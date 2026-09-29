package fnruntime

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestPerFunctionFairness：函数 A 用大量慢调用占满自身额度时，
// 函数 B 必须照常被接纳与执行，且 A 的超额调用拿到 function_busy
// 而不是阻塞或静默排队。
func TestPerFunctionFairness(t *testing.T) {
	r := newStartedRuntime(t, Config{GlobalConcurrency: 8, DefaultTimeout: 5 * time.Second})

	const aLimit = 2
	release := make(chan struct{})
	var execA, execB atomic.Int64
	startedA := make(chan struct{}, 16)
	startedB := make(chan struct{}, 16)

	r.MustRegister("slowA", slowHandler(startedA, release, &execA), FunctionConfig{MaxConcurrency: aLimit})
	r.MustRegister("fastB", func(ctx context.Context, payload []byte) ([]byte, error) {
		execB.Add(1)
		startedB <- struct{}{}
		return []byte("b"), nil
	}, FunctionConfig{MaxConcurrency: 4})

	// 1) 占满 slowA 的函数额度。
	var wg sync.WaitGroup
	for i := 0; i < aLimit; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = r.Invoke(context.Background(), "slowA", nil) }()
	}
	for i := 0; i < aLimit; i++ {
		select {
		case <-startedA:
		case <-time.After(2 * time.Second):
			t.Fatal("slowA in-flight calls never started")
		}
	}

	// 2) 对 slowA 的超额调用必须立即得到 function_busy。
	var busyA, otherA atomic.Int64
	for i := 0; i < 10; i++ {
		_, err := r.Invoke(context.Background(), "slowA", nil)
		if err != nil {
			if reasonOf(err) == ReasonFunctionBusy {
				busyA.Add(1)
			} else {
				otherA.Add(1)
			}
		}
	}
	if busyA.Load() != 10 {
		t.Fatalf("expected 10 function_busy rejections, got busy=%d other=%d", busyA.Load(), otherA.Load())
	}
	t.Logf("verdict: slowA fnConcurrency=%d/%d rejected=%d reason=function_busy",
		aLimit, aLimit, busyA.Load())

	// 3) B 不受影响：多轮调用都成功。
	for i := 0; i < 8; i++ {
		res, err := r.Invoke(context.Background(), "fastB", nil)
		if err != nil {
			t.Fatalf("fastB call %d should not be starved: %v", i, err)
		}
		<-startedB
		if string(res.Output) != "b" {
			t.Fatalf("unexpected fastB output %q", res.Output)
		}
	}
	t.Logf("verdict: fastB served=%d executions while slowA saturated, execA=%d", execB.Load(), execA.Load())
	if execB.Load() != 8 {
		t.Fatalf("expected 8 fastB executions, got %d", execB.Load())
	}

	// 4) 释放慢调用后额度必须完整归还：A 可再次被接纳。
	close(release)
	wg.Wait()
	startedA2 := make(chan struct{}, 4)
	release2 := make(chan struct{})
	// 重新注册一个带新 release 的观察处理器来验证额度已归还。
	r.MustRegister("slowA2", slowHandler(startedA2, release2, nil), FunctionConfig{MaxConcurrency: aLimit})
	var wg2 sync.WaitGroup
	for i := 0; i < aLimit; i++ {
		wg2.Add(1)
		go func() { defer wg2.Done(); _, _ = r.Invoke(context.Background(), "slowA2", nil) }()
	}
	for i := 0; i < aLimit; i++ {
		select {
		case <-startedA2:
		case <-time.After(2 * time.Second):
			t.Fatal("slots were not released after slow calls finished")
		}
	}
	close(release2)
	wg2.Wait()
	t.Logf("verdict: per-function quota fully returned after completion")
}

// TestGlobalLimit：当全局额度被占满时，新调用得到 global_busy；
// 释放后全局额度恢复，全程不应有额度泄漏。
func TestGlobalLimit(t *testing.T) {
	const global = 3
	r := newStartedRuntime(t, Config{GlobalConcurrency: global, DefaultTimeout: 5 * time.Second})

	release := make(chan struct{})
	started := make(chan struct{}, 16)
	var exec atomic.Int64
	// 三个函数，各自额度 4，但全局只有 3。
	for _, name := range []string{"g1", "g2", "g3"} {
		r.MustRegister(name, slowHandler(started, release, &exec), FunctionConfig{MaxConcurrency: 4})
	}

	var wg sync.WaitGroup
	for i := 0; i < global; i++ {
		wg.Add(1)
		name := []string{"g1", "g2", "g3"}[i]
		go func(n string) { defer wg.Done(); _, _ = r.Invoke(context.Background(), n, nil) }(name)
	}
	for i := 0; i < global; i++ {
		<-started
	}

	// 全局占满：任意函数的新调用都应被拒绝为 global_busy。
	var globalBusy atomic.Int64
	for i := 0; i < 12; i++ {
		name := []string{"g1", "g2", "g3"}[i%3]
		_, err := r.Invoke(context.Background(), name, nil)
		if err != nil && reasonOf(err) == ReasonGlobalBusy {
			globalBusy.Add(1)
		} else if err == nil {
			t.Fatal("unexpected success while global slots saturated")
		} else {
			t.Fatalf("expected global_busy, got %v", err)
		}
	}
	t.Logf("verdict: globalConcurrency=%d/%d rejected=%d reason=global_busy",
		global, global, globalBusy.Load())

	close(release)
	wg.Wait()

	// 额度归还后：又能恰好接纳 global 个调用（验证无泄漏）。
	started2 := make(chan struct{}, 16)
	release2 := make(chan struct{})
	r.MustRegister("g4", slowHandler(started2, release2, nil), FunctionConfig{MaxConcurrency: 16})
	var wg2 sync.WaitGroup
	for i := 0; i < global; i++ {
		wg2.Add(1)
		go func() { defer wg2.Done(); _, _ = r.Invoke(context.Background(), "g4", nil) }()
	}
	for i := 0; i < global; i++ {
		select {
		case <-started2:
		case <-time.After(2 * time.Second):
			t.Fatal("global slots leaked: cannot re-admit full capacity")
		}
	}
	if _, err := r.Invoke(context.Background(), "g4", nil); err == nil || reasonOf(err) != ReasonGlobalBusy {
		t.Fatalf("expected global_busy at full capacity, got %v", err)
	}
	close(release2)
	wg2.Wait()
	t.Logf("verdict: global quota fully returned, no leak")
}

// TestConcurrentContentionNoLeak：高并发混合负载下反复争抢，
// 结束后所有额度必须归零（race detector 下同时验证无数据竞争）。
func TestConcurrentContentionNoLeak(t *testing.T) {
	r := newStartedRuntime(t, Config{GlobalConcurrency: 4, DefaultTimeout: 200 * time.Millisecond})
	gate := make(chan struct{})
	var realExec atomic.Int64

	for _, name := range []string{"c1", "c2", "c3"} {
		n := name
		_ = n
		r.MustRegister(name, func(ctx context.Context, payload []byte) ([]byte, error) {
			realExec.Add(1)
			select {
			case <-gate:
			case <-ctx.Done():
			}
			return []byte("ok"), nil
		}, FunctionConfig{MaxConcurrency: 2})
	}

	var wg sync.WaitGroup
	for i := 0; i < 60; i++ {
		wg.Add(1)
		name := []string{"c1", "c2", "c3"}[i%3]
		go func(n string) {
			defer wg.Done()
			_, _ = r.Invoke(context.Background(), n, nil, WithTimeout(150*time.Millisecond))
		}(name)
	}
	time.Sleep(60 * time.Millisecond)
	close(gate)
	wg.Wait()

	// 所有执行单元结束后，两级额度都应空闲。
	r.mu.Lock()
	used := len(r.globalSlots)
	var perFn []int
	for _, reg := range r.regs {
		perFn = append(perFn, len(reg.slots))
	}
	r.mu.Unlock()
	if used != 0 {
		t.Fatalf("global slots leaked: %d in use", used)
	}
	for i, n := range perFn {
		if n != 0 {
			t.Fatalf("function %d slots leaked: %d in use", i, n)
		}
	}
	t.Logf("verdict: contention settled, realExecutions=%d globalConcurrency=0/%d all fn quotas idle",
		realExec.Load(), cap2(r))
}

func reasonOf(err error) FailureReason {
	if ce, ok := err.(*CallError); ok {
		return ce.Reason
	}
	return ""
}

func cap2(r *Runtime) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return cap(r.globalSlots)
}
