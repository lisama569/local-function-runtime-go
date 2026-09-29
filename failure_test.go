package fnruntime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestInvokeTimeout(t *testing.T) {
	r := newStartedRuntime(t, Config{GlobalConcurrency: 4, DefaultTimeout: time.Second})

	release := make(chan struct{})
	started := make(chan struct{}, 4)
	var exec atomic.Int64
	r.MustRegister("slow", slowHandler(started, release, &exec), FunctionConfig{MaxConcurrency: 1})

	_, err := r.Invoke(context.Background(), "slow", nil, WithTimeout(80*time.Millisecond))
	assertReason(t, err, ReasonTimeout)
	t.Logf("verdict: timeout returned to caller")

	// 超时终态后函数额度必须已归还：第二个调用立刻被接纳（同样慢、随后超时）。
	_, err2 := r.Invoke(context.Background(), "slow", nil, WithTimeout(80*time.Millisecond))
	assertReason(t, err2, ReasonTimeout)
	t.Logf("verdict: per-function quota returned after timeout, second call admitted")

	// 释放后让被隔离在后台的执行自然结束（它们尊重 ctx，在超时后也很快退出）。
	close(release)
	time.Sleep(120 * time.Millisecond)
	if exec.Load() != 2 {
		t.Fatalf("expected 2 real executions, got %d", exec.Load())
	}
}

func TestInvokePanic(t *testing.T) {
	r := newStartedRuntime(t, Config{})
	var calls atomic.Int64
	r.MustRegister("boom", func(ctx context.Context, payload []byte) ([]byte, error) {
		calls.Add(1)
		panic("kaboom")
	}, FunctionConfig{MaxConcurrency: 2})

	for i := 0; i < 3; i++ {
		_, err := r.Invoke(context.Background(), "boom", nil)
		assertReason(t, err, ReasonPanic)
	}
	if calls.Load() != 3 {
		t.Fatalf("runtime must survive panics and admit all 3 calls, executed=%d", calls.Load())
	}

	// panic 后同一函数仍可正常服务，且额度全部归还。
	r.MustRegister("alive", func(context.Context, []byte) ([]byte, error) {
		return []byte("still-alive"), nil
	}, FunctionConfig{MaxConcurrency: 1})
	res, err := r.Invoke(context.Background(), "alive", nil)
	if err != nil || string(res.Output) != "still-alive" {
		t.Fatalf("runtime not healthy after panic: res=%v err=%v", res, err)
	}
	t.Logf("verdict: panic isolated, runtime still healthy output=%q", res.Output)
}

func TestInvokeHandlerError(t *testing.T) {
	r := newStartedRuntime(t, Config{})
	myErr := errors.New("business boom")
	r.MustRegister("biz", func(context.Context, []byte) ([]byte, error) {
		return nil, myErr
	}, FunctionConfig{})

	_, err := r.Invoke(context.Background(), "biz", nil)
	assertReason(t, err, ReasonHandlerError)
	if !errors.Is(err, myErr) {
		t.Fatalf("underlying handler error should be unwrappable, got %v", err)
	}
	t.Logf("verdict: handler_error distinguished and unwrapped")
}

func TestUnregisterDuringFlight(t *testing.T) {
	r := newStartedRuntime(t, Config{DefaultTimeout: 3 * time.Second})
	release := make(chan struct{})
	started := make(chan struct{}, 2)
	var completed atomic.Int64
	r.MustRegister("moving", func(ctx context.Context, payload []byte) ([]byte, error) {
		started <- struct{}{}
		select {
		case <-release:
			completed.Add(1)
			return []byte("done"), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, FunctionConfig{MaxConcurrency: 2})

	var got atomic.Value
	go func() {
		res, err := r.Invoke(context.Background(), "moving", nil)
		if err != nil {
			got.Store(err)
			return
		}
		got.Store(string(res.Output))
	}()
	<-started

	// 运行期间移除：新调用 not_found，在途调用继续跑。
	if !r.Unregister("moving") {
		t.Fatal("unregister should report true")
	}
	if r.Unregister("moving") {
		t.Fatal("second unregister should report false")
	}
	_, err := r.Invoke(context.Background(), "moving", nil)
	assertReason(t, err, ReasonFunctionNotFound)

	close(release)
	deadline := time.After(2 * time.Second)
	for {
		if v := got.Load(); v != nil {
			if v != "done" {
				t.Fatalf("in-flight call should finish normally, got %v", v)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("in-flight call result lost after unregister")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if completed.Load() != 1 {
		t.Fatalf("in-flight handler should complete once, got %d", completed.Load())
	}
	t.Logf("verdict: unregister isolated, new calls rejected, in-flight result delivered")
}

func TestCallerCanceled(t *testing.T) {
	r := newStartedRuntime(t, Config{DefaultTimeout: 5 * time.Second})
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	r.MustRegister("hold", slowHandler(started, release, nil), FunctionConfig{MaxConcurrency: 2})

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { _, err := r.Invoke(ctx, "hold", nil); errc <- err }()
	<-started
	cancel()
	assertReason(t, <-errc, ReasonCallerCanceled)

	// 调用方取消不杀执行；释放后额度归还，运行时仍可接纳新调用。
	close(release)
	time.Sleep(80 * time.Millisecond)
	r.MustRegister("after", func(context.Context, []byte) ([]byte, error) { return []byte("ok"), nil },
		FunctionConfig{})
	res, err := r.Invoke(context.Background(), "after", nil)
	if err != nil || string(res.Output) != "ok" {
		t.Fatalf("runtime unhealthy after caller cancel: %v %v", res, err)
	}
	t.Logf("verdict: caller_canceled distinguished, execution drained safely")
}
