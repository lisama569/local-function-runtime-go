package localfunctionruntime

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Runtime 本地函数运行时。零值不可用，请使用 New + Start。
type Runtime struct {
	mu         sync.Mutex
	state      runtimeState
	cfg        config
	sched      *scheduler
	dedup      *dedupStore
	regMu      sync.RWMutex
	funcs      map[string]*functionEntry
	wg         sync.WaitGroup
	inFlight   atomic.Int64
	logger     Logger
	baseCtx    context.Context
	baseCancel context.CancelFunc
}

type runtimeState int

const (
	stateNew runtimeState = iota
	stateRunning
	stateShuttingDown
	stateStopped
)

func (s runtimeState) String() string {
	switch s {
	case stateNew:
		return "new"
	case stateRunning:
		return "running"
	case stateShuttingDown:
		return "shutting_down"
	default:
		return "stopped"
	}
}

// Result 一次调用的明确结果。
type Result struct {
	Function string
	Value    []byte
	// Deduped 命中同幂等键的在途调用，本次未产生新的函数执行。
	Deduped bool
	// Cached 命中同幂等键已完成且未过期的结果。
	Cached bool
}

// FunctionStats 单个函数的额度快照。
type FunctionStats struct {
	Registered bool
	Capacity   int
	Held       int
}

// Stats 运行时快照，供观测与测试日志使用。
type Stats struct {
	State          string
	GlobalCapacity int
	GlobalHeld     int
	InFlight       int64
	Functions      map[string]FunctionStats
}

// New 创建运行时（尚未启动）。
func New(opts ...Option) (*Runtime, error) {
	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.globalConcurrency < 1 {
		return nil, fmt.Errorf("%w: global concurrency must be >= 1", ErrInvalidConfig)
	}
	if cfg.defaultTimeout <= 0 {
		return nil, fmt.Errorf("%w: default timeout must be > 0", ErrInvalidConfig)
	}
	return &Runtime{
		state:  stateNew,
		cfg:    cfg,
		logger: cfg.logger,
	}, nil
}

// Start 将运行时从 new/stopped 状态转入运行中。重复 Start 返回 ErrInvalidConfig 之外的安全错误。
func (r *Runtime) Start() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch r.state {
	case stateNew, stateStopped:
		r.sched = newScheduler(r.cfg.globalConcurrency)
		r.dedup = newDedupStore(r.cfg.idempotencyTTL)
		r.dedup.start(time.Now)
		r.funcs = make(map[string]*functionEntry)
		r.baseCtx, r.baseCancel = context.WithCancel(context.Background())
		r.state = stateRunning
		return nil
	case stateRunning:
		return fmt.Errorf("%w: already running", ErrInvalidConfig)
	default:
		return fmt.Errorf("%w: shutdown in progress", ErrShuttingDown)
	}
}

// Shutdown 在 ctx 截止时间内等待全部在途调用结束，随后停止后台清理。
// 停机开始后新调用一律被拒绝（ErrShuttingDown）；超过截止时间返回 ErrShutdownTimeout，
// 此时在途 goroutine 仍会自然结算（归还额度、落盘幂等结果），不会泄漏。
// 停止后可再次 Start。
func (r *Runtime) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	if r.state != stateRunning {
		state := r.state
		r.mu.Unlock()
		if state == stateShuttingDown {
			return ErrShuttingDown
		}
		return nil
	}
	dedup := r.dedup
	baseCancel := r.baseCancel
	r.state = stateShuttingDown
	r.mu.Unlock()

	// 停机立即向所有在途函数广播取消，促使它们尽快结束。
	if baseCancel != nil {
		baseCancel()
	}

	waitErr := make(chan struct{}, 1)
	go func() {
		r.wg.Wait()
		waitErr <- struct{}{}
	}()

	var err error
	timedOut := false
	select {
	case <-ctx.Done():
		err = fmt.Errorf("%w: %v", ErrShutdownTimeout, ctx.Err())
		timedOut = true
	case <-waitErr:
		err = nil
	}

	r.mu.Lock()
	dedup.close()
	r.state = stateStopped
	r.mu.Unlock()
	if timedOut {
		// 在途 goroutine 仍会自然完成（wg.Done、额度归还、幂等落盘），不泄漏；
		// 仅不阻塞调用方继续等待。
		go func() { <-waitErr }()
	}
	return err
}

// Register 注册函数。仅在运行中可注册；MaxConcurrency 会被收敛到 [1, 全局上限]。
func (r *Runtime) Register(name string, fn Function, fcfg FunctionConfig) error {
	if name == "" || fn == nil {
		return fmt.Errorf("%w: name and function are required", ErrInvalidConfig)
	}
	r.mu.Lock()
	if r.state != stateRunning {
		r.mu.Unlock()
		if r.state == stateShuttingDown {
			return ErrShuttingDown
		}
		return ErrNotRunning
	}
	cap := fcfg.maxConcurrency(r.cfg.globalConcurrency)
	if cap > r.cfg.globalConcurrency {
		cap = r.cfg.globalConcurrency
	}
	r.sched.register(name, cap)
	entry := &functionEntry{name: name, fn: fn, cfg: fcfg}
	r.regMu.Lock()
	r.funcs[name] = entry
	r.regMu.Unlock()
	r.mu.Unlock()
	return nil
}

// Unregister 移除函数：移除后新调用返回 ErrFunctionNotFound，已在途调用照常完成。
func (r *Runtime) Unregister(name string) error {
	r.mu.Lock()
	running := r.state == stateRunning
	if running {
		r.sched.unregister(name)
	}
	r.mu.Unlock()
	if !running {
		return ErrNotRunning
	}
	r.regMu.Lock()
	if _, ok := r.funcs[name]; !ok {
		r.regMu.Unlock()
		return ErrFunctionNotFound
	}
	delete(r.funcs, name)
	r.regMu.Unlock()
	return nil
}

// Invoke 调用已注册函数。返回值与错误互斥表达明确判定：
// 额度拒绝、未注册、停机为可区分的哨兵错误；函数自身返回的 error 原样透传。
func (r *Runtime) Invoke(ctx context.Context, name string, payload []byte, opts ...InvokeOption) (*Result, error) {
	iopts := invokeOptions{}
	for _, o := range opts {
		o(&iopts)
	}

	r.mu.Lock()
	switch r.state {
	case stateNew, stateStopped:
		r.mu.Unlock()
		return nil, ErrNotRunning
	case stateShuttingDown:
		r.mu.Unlock()
		return nil, ErrShuttingDown
	}
	r.regMu.RLock()
	entry, ok := r.funcs[name]
	r.regMu.RUnlock()
	if !ok {
		r.mu.Unlock()
		return nil, ErrFunctionNotFound
	}
	// Add 与状态翻转互斥：要么在停机前计入 wg，要么直接被拒绝，不会有 Wait/Add 竞态。
	r.wg.Add(1)
	r.mu.Unlock()

	baseCtx := r.baseCtx
	resp := make(chan invokeResponse, 1)
	go r.runInvocation(baseCtx, ctx, entry, payload, iopts, resp)

	select {
	case res := <-resp:
		return res.result, res.err
	case <-ctx.Done():
		// 调用方提前放弃，但执行结算（额度归还、幂等落盘）仍在后台完成，wg 覆盖。
		return nil, ctx.Err()
	}
}

type invokeResponse struct {
	result *Result
	err    error
}

func (r *Runtime) runInvocation(
	baseCtx, callerCtx context.Context,
	entry *functionEntry,
	payload []byte,
	iopts invokeOptions,
	resp chan<- invokeResponse,
) {
	defer r.wg.Done()
	r.inFlight.Add(1)
	defer r.inFlight.Add(-1)

	name := entry.name
	key := iopts.idempotencyKey
	verdict := "executed"
	deduped, cached := false, false
	var finalErr error
	defer func() {
		r.logInvocation(name, key, verdict, finalErr)
	}()

	// 1) 幂等路径：缓存 / leader / follower。
	if key != "" {
		now := time.Now()
		if out, ok := r.dedup.snapshot(key, now); ok {
			cached = true
			verdict = "cached"
			finalErr = out.err
			resp <- invokeResponse{result: &Result{Function: name, Value: out.value, Cached: true}, err: out.err}
			return
		}
		leader, wait := r.dedup.acquire(key, now)
		if !leader {
			verdict = "deduped"
			deduped = true
			select {
			case <-wait:
				out := r.dedup.waitResult(key)
				finalErr = out.err
				resp <- invokeResponse{result: &Result{Function: name, Value: out.value, Deduped: true}, err: out.err}
			case <-callerCtx.Done():
				finalErr = callerCtx.Err()
				resp <- invokeResponse{err: callerCtx.Err()}
			}
			return
		}
	}

	// 2) 两级非阻塞准入。
	permit, admitErr := r.sched.tryAdmit(name)
	if admitErr != nil {
		if key != "" {
			r.dedup.discard(key, admitErr)
		}
		verdict = "rejected"
		finalErr = admitErr
		resp <- invokeResponse{err: admitErr}
		return
	}

	// 3) 执行：超时与 panic 均隔离；额度在函数 goroutine 真正结束后才归还。
	// 超时解析优先级：调用选项 > 函数配置 > 运行时默认。
	timeout := iopts.timeout
	if timeout <= 0 {
		timeout = entry.cfg.timeout(r.cfg.defaultTimeout)
	}
	// 同时受调用方 ctx、运行时停机 ctx 与调用超时约束。
	// 两个来源独立传播；监听器在 execCtx 结束后退出，不依赖调用方 ctx 被取消。
	linkedCtx, linkedCancel := context.WithCancel(context.Background())
	stopWatch := make(chan struct{})
	go func() {
		select {
		case <-callerCtx.Done():
			linkedCancel()
		case <-stopWatch:
		}
	}()
	go func() {
		select {
		case <-baseCtx.Done():
			linkedCancel()
		case <-stopWatch:
		}
	}()
	execCtx, cancel := context.WithTimeout(linkedCtx, timeout)
	defer func() {
		cancel()
		close(stopWatch)
	}()

	type execOutcome struct {
		value    []byte
		err      error
		panicked any
	}
	done := make(chan execOutcome, 1)
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				done <- execOutcome{panicked: rec}
				return
			}
		}()
		v, err := entry.fn(execCtx, payload)
		done <- execOutcome{value: v, err: err}
	}()

	var out dedupOutcome
	var resValue []byte
	select {
	case oc := <-done:
		if oc.panicked != nil {
			out.err = fmt.Errorf("%w: %v", ErrInvocationPanic, oc.panicked)
			verdict = "panic"
		} else {
			out.value, out.err = oc.value, oc.err
			if oc.err != nil {
				verdict = "function_error"
			}
		}
		resValue = out.value
		if key != "" {
			r.dedup.complete(key, out, time.Now())
		}
		permit.Release()
	case <-execCtx.Done():
		var cancelErr error
		switch {
		case callerCtx.Err() != nil:
			// 调用方提前放弃。
			cancelErr = callerCtx.Err()
			verdict = "caller_cancelled"
		case baseCtx.Err() != nil:
			// 运行时停机。
			cancelErr = ErrShuttingDown
			verdict = "shutdown_cancelled"
		default:
			// 单次调用超时。
			cancelErr = fmt.Errorf("%w after %s", ErrInvocationTimeout, timeout)
			verdict = "timeout"
		}
		// 先给调用方明确的取消/超时结论。
		finalErr = cancelErr
		resp <- invokeResponse{err: cancelErr}
		// 再等待函数 goroutine 自然结束：真实结算结果才是幂等事实，
		// follower / 重试方必须看到同一份结果，而不是空结果。
		oc := <-done
		permit.Release()
		if oc.panicked != nil {
			out.err = fmt.Errorf("%w: %v", ErrInvocationPanic, oc.panicked)
		} else if oc.err != nil {
			out.value, out.err = oc.value, oc.err
		} else {
			out.err = cancelErr
		}
		if key != "" {
			r.dedup.complete(key, out, time.Now())
		}
		return
	}

	finalErr = out.err
	resp <- invokeResponse{result: &Result{Function: name, Value: resValue, Deduped: deduped, Cached: cached}, err: out.err}
}

func (r *Runtime) logInvocation(name, key, verdict string, err error) {
	if r.logger == nil {
		return
	}
	r.mu.Lock()
	sched := r.sched
	r.mu.Unlock()
	if sched == nil {
		// 已停机且未重启（或日志来自停机超时后的延迟结算）。
		if key == "" {
			key = "-"
		}
		r.logger.Printf("invoke function=%s idempotency-key=%s verdict=%s error=%q (scheduler stopped)",
			name, key, verdict, errString(err))
		return
	}
	st := sched.stats()
	held := 0
	if fs, ok := st.Functions[name]; ok {
		held = fs.Held
	}
	if key == "" {
		key = "-"
	}
	switch {
	case err == nil:
		r.logger.Printf("invoke function=%s idempotency-key=%s verdict=%s global=%d/%d function-held=%d",
			name, key, verdict, st.GlobalHeld, st.GlobalCapacity, held)
	default:
		r.logger.Printf("invoke function=%s idempotency-key=%s verdict=%s error=%q global=%d/%d function-held=%d",
			name, key, verdict, err.Error(), st.GlobalHeld, st.GlobalCapacity, held)
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// Stats 返回调度与在途状态快照。
func (r *Runtime) Stats() Stats {
	r.mu.Lock()
	sched, state := r.sched, r.state
	r.mu.Unlock()
	if sched == nil {
		return Stats{State: state.String()}
	}
	st := sched.stats()
	return Stats{
		State:          state.String(),
		GlobalCapacity: st.GlobalCapacity,
		GlobalHeld:     st.GlobalHeld,
		InFlight:       r.inFlight.Load(),
		Functions:      st.Functions,
	}
}
