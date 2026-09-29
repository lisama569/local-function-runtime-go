package fnruntime

import (
	"context"
	"fmt"
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"time"
)

// outcome 是执行单元的内部终态（成功输出或结构化失败）。
type outcome struct {
	output []byte
	err    *CallError
}

// Invoke 调用已注册的函数。
//
// 成功（包括命中幂等去重的成功终态）返回 *Result；失败返回包装了
// *CallError 的 error，可用 errors.As 提取并按 Reason 区分失败原因。
// 命中幂等去重的失败终态同样以 *CallError 返回，且与首次失败原因一致。
func (r *Runtime) Invoke(ctx context.Context, name string, payload []byte, opts ...InvokeOption) (*Result, error) {
	o := InvokeOptions{}
	for _, opt := range opts {
		opt(&o)
	}
	return r.invokeWithKey(ctx, name, o.IdempotencyKey, payload, o.Timeout)
}

// invokeWithKey 是真正的调用流程。
//
// 幂等路径下本 goroutine 既可能是 leader（负责真实执行），也可能在准入
// 遇到瞬时拒绝（busy）时放弃占位并重试成为 follower；重试有界，避免活锁。
func (r *Runtime) invokeWithKey(ctx context.Context, name, key string, payload []byte, timeout time.Duration) (*Result, error) {
	const maxAttempts = 32

	for attempt := 0; attempt < maxAttempts; attempt++ {
		var rec *idemRecord
		leader := true

		// ---- 1. 状态检查 + 幂等占位（同一把锁，保证顺序）----
		r.mu.Lock()
		if r.state != stateRunning {
			r.mu.Unlock()
			return nil, newCallError(name, ReasonRuntimeStopped, nil)
		}
		reg, ok := r.regs[name]
		if !ok {
			r.mu.Unlock()
			return nil, newCallError(name, ReasonFunctionNotFound, nil)
		}
		if key != "" {
			now := time.Now()
			r.idem.deleteExpired(now)
			if existing, hit := r.idem.get(key, now); hit {
				rec = existing
				leader = false
			} else {
				rec = &idemRecord{
					done:     make(chan struct{}),
					expireAt: now.Add(r.cfg.IdempotencyTTL),
				}
				r.idem.records[key] = rec
			}
		}
		if leader {
			// WG.Add 与状态检查在同一临界区内：Shutdown 之后不会再有
			// 被 WaitGroup 遗漏的在途执行单元。
			r.inFlight.Add(1)
		}
		r.mu.Unlock()

		// ---- 2. follower：等待 leader 的终态并去重复用 ----
		if !leader {
			return r.waitOutcome(ctx, name, key, rec)
		}

		// ---- 3. leader：两级额度的非阻塞准入（先函数、后全局）----
		oc := r.admit(reg, rec, key)
		if oc != nil {
			if key != "" && isTransientReason(oc.err.Reason) {
				// 准入被瞬时拒绝：把拒绝终态发布给已经加入等待的 follower
				// （它们同样收到明确的 busy 拒绝，而不是阻塞），再从 map
				// 删除占位，让后续同 key 调用重新参与准入。
				runtime.Gosched()
				r.mu.Lock()
				if cur, ok := r.idem.records[key]; ok && cur == rec {
					rec.outcome = oc
					close(rec.done)
					delete(r.idem.records, key)
				}
				r.inFlight.Done()
				r.mu.Unlock()
				continue
			}
			// 非瞬时拒绝（如运行时被停机）直接作为终态返回。
			r.finishLeader(key, rec, oc)
			return nil, oc.err
		}

		// ---- 4. 准入成功，后台执行并等待终态 ----
		if timeout == 0 {
			timeout = r.cfg.DefaultTimeout
		}
		// 捕获本轮令牌通道引用：虽然 Start 必须等上一轮全部收尾后才会重建
		// 通道，这里仍只使用局部快照，避免 goroutine 存活期间的字段解引用。
		fnSlots := reg.slots
		globalSlots := r.globalSlots
		execDone := make(chan *outcome, 1)
		go r.runExecution(reg, payload, timeout, fnSlots, globalSlots, execDone)

		r.cfg.Logger.Printf("fnruntime: admitted name=%s idempotencyKey=%q fnConcurrency=%d/%d globalConcurrency=%d/%d",
			name, key, len(reg.slots), cap(reg.slots), len(r.globalSlots), cap(r.globalSlots))

		select {
		case oc := <-execDone:
			r.finishLeader(key, rec, oc)
			if oc.err != nil {
				return nil, oc.err
			}
			return &Result{Output: oc.output}, nil
		case <-ctx.Done():
			// 调用方不再等待；执行单元仍在后台跑到自身超时并归还额度，
			// 其终态仍写入幂等记录供后续同 key 调用复用。
			r.cfg.Logger.Printf("fnruntime: caller canceled waiting name=%s idempotencyKey=%q", name, key)
			go func() {
				oc := <-execDone
				r.finishLeader(key, rec, oc)
			}()
			return nil, newCallError(name, ReasonCallerCanceled, ctx.Err())
		}
	}

	return nil, newCallError(name, ReasonFunctionBusy,
		fmt.Errorf("idempotency admission contended after %d attempts", maxAttempts))
}

// admit 以非阻塞方式尝试两级额度：先占函数槽，再占全局槽。
// 先函数后全局意味着被慢调用占满自身额度的函数无法囤积全局额度，
// 其他函数仍可拿到全局额度，从而不会被一起拖死。
// 任何拒绝路径都不留下已占用的令牌。
func (r *Runtime) admit(reg *registration, rec *idemRecord, key string) *outcome {
	select {
	case reg.slots <- struct{}{}:
	default:
		r.cfg.Logger.Printf("fnruntime: rejected name=%s reason=%s idempotencyKey=%q fnConcurrency=%d/%d",
			reg.name, ReasonFunctionBusy, key, len(reg.slots), cap(reg.slots))
		return &outcome{err: newCallError(reg.name, ReasonFunctionBusy, nil)}
	}

	select {
	case r.globalSlots <- struct{}{}:
	default:
		<-reg.slots // 回滚函数槽，绝不漏放
		r.cfg.Logger.Printf("fnruntime: rejected name=%s reason=%s idempotencyKey=%q globalConcurrency=%d/%d",
			reg.name, ReasonGlobalBusy, key, len(r.globalSlots), cap(r.globalSlots))
		return &outcome{err: newCallError(reg.name, ReasonGlobalBusy, nil)}
	}
	return nil
}

// waitOutcome 让 follower 等待 leader 终态。调用方自身 ctx 取消时返回
// caller_canceled，但不影响 leader 与幂等记录。
func (r *Runtime) waitOutcome(ctx context.Context, name, key string, rec *idemRecord) (*Result, error) {
	select {
	case <-rec.done:
		oc := rec.outcome
		r.cfg.Logger.Printf("fnruntime: deduped name=%s idempotencyKey=%q verdict=%s",
			name, key, verdictOf(oc))
		if oc.err != nil {
			return nil, oc.err
		}
		return &Result{Output: oc.output, Deduped: true}, nil
	case <-ctx.Done():
		return nil, newCallError(name, ReasonCallerCanceled, ctx.Err())
	}
}

// finishLeader 把终态发布给等待者：写入幂等记录（如有）、释放调用方占住的
// WaitGroup 计数。执行单元自身的额度归还在 runExecution 内通过 defer 完成，
// 与本方法解耦，保证任何路径都不会重复占用或漏放。
func (r *Runtime) finishLeader(key string, rec *idemRecord, oc *outcome) {
	if key != "" && rec != nil {
		r.mu.Lock()
		rec.outcome = oc
		rec.expireAt = time.Now().Add(r.cfg.IdempotencyTTL)
		close(rec.done)
		r.mu.Unlock()
	}
	r.inFlight.Done()
	r.cfg.Logger.Printf("fnruntime: finalized idempotencyKey=%q verdict=%s", key, verdictOf(oc))
}

// runExecution 在独立 goroutine 中执行处理函数，负责：
//   - 建立不继承调用方取消的执行上下文，以单次调用超时为界；
//   - recover 处理函数 panic，转化为 panic 失败；
//   - 任何退出路径（成功/失败/panic/超时）都通过 defer 完整归还两级额度，
//     并恰好一次向 execDone 发送终态（缓冲为 1，不会阻塞/泄漏）。
func (r *Runtime) runExecution(reg *registration, payload []byte, timeout time.Duration,
	fnSlots, globalSlots chan struct{}, execDone chan *outcome) {
	// result 是本次执行唯一的终态发布点：内部执行 goroutine 与超时分支
	// 通过 CompareAndSwap 竞争首发权，任何一方之后都不再触碰该指针，
	// 因此不存在数据竞争。
	var result atomic.Pointer[outcome]

	defer func() {
		<-globalSlots
		<-fnSlots
		execDone <- result.Load()
	}()

	var execCtx context.Context
	var cancel context.CancelFunc
	if timeout > 0 {
		execCtx, cancel = context.WithTimeout(context.Background(), timeout)
	} else {
		execCtx, cancel = context.WithCancel(context.Background())
	}
	defer cancel()

	// publish 只有首个调用者成功写入终态；迟到结果（如超时后的返回/panic）
	// 被丢弃。
	publish := func(oc *outcome) bool { return result.CompareAndSwap(nil, oc) }

	finished := make(chan struct{})

	go func() {
		defer close(finished)
		defer func() {
			if p := recover(); p != nil {
				if publish(&outcome{err: newCallError(reg.name, ReasonPanic,
					fmt.Errorf("%v\n%s", p, debug.Stack()))}) {
					r.cfg.Logger.Printf("fnruntime: panic recovered name=%s panic=%v", reg.name, p)
				}
			}
		}()
		out, err := reg.fn(execCtx, payload)
		if err != nil {
			// handler 因执行上下文到期而返回（即使它比超时分支早几百纳秒
			// 发布），语义上仍是调用超时，保证失败原因判定与时序无关。
			reason := ReasonHandlerError
			if execCtx.Err() == context.DeadlineExceeded {
				reason = ReasonTimeout
			}
			publish(&outcome{err: newCallError(reg.name, reason, err)})
			return
		}
		publish(&outcome{output: out})
	}()

	select {
	case <-execCtx.Done():
		if publish(&outcome{err: newCallError(reg.name, ReasonTimeout, execCtx.Err())}) {
			// 立即对外给出 timeout 终态并归还额度；
			// 若处理函数不响应 ctx，其 goroutine 仍会在自然结束后退出
			// （finished 被一个回收等待者排空），但不会再触碰任何额度或终态。
		}
		go func() { <-finished }()
	case <-finished:
		// finished 关闭建立 happens-before：内部 goroutine 已发布终态，
		// 这里 Load 必然可见。
		if result.Load() == nil {
			publish(&outcome{err: newCallError(reg.name, ReasonHandlerError,
				fmt.Errorf("handler finished without a result"))})
		}
	}
}

// isTransientReason 判断一个拒绝原因是否属于瞬时容量原因。
// 这类原因不应作为幂等终态缓存（下次调用可能就有额度）。
func isTransientReason(reason FailureReason) bool {
	return reason == ReasonFunctionBusy || reason == ReasonGlobalBusy
}

// verdictOf 生成单测/排障可读的最终判定描述。
func verdictOf(oc *outcome) string {
	if oc.err != nil {
		return "failure:" + string(oc.err.Reason)
	}
	return "success"
}
