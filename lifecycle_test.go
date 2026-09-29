package localfunctionruntime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestInvocationTimeout(t *testing.T) {
	r := newRunningRuntime(t)
	release := make(chan struct{})
	_ = r.Register("slow", func(ctx context.Context, _ []byte) ([]byte, error) {
		select {
		case <-release:
			return []byte("done"), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, FunctionConfig{MaxConcurrency: 4})

	start := time.Now()
	_, err := r.Invoke(context.Background(), "slow", nil, WithInvokeTimeout(50*time.Millisecond))
	if !errors.Is(err, ErrInvocationTimeout) {
		t.Fatalf("want ErrInvocationTimeout, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("timeout should return promptly, took %v", elapsed)
	}
	close(release)

	// 超时调用的 goroutine 结束后额度必须完整归还：下一次调用可以立即准入成功。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if st := r.Stats(); st.GlobalHeld == 0 && st.Functions["slow"].Held == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("permits leaked after timeout: %+v", r.Stats())
}

func TestInvocationPanicIsolated(t *testing.T) {
	r := newRunningRuntime(t)
	var calls atomic.Int64
	_ = r.Register("panicky", func(ctx context.Context, _ []byte) ([]byte, error) {
		calls.Add(1)
		panic("kaboom")
	}, FunctionConfig{})

	for i := 0; i < 5; i++ {
		_, err := r.Invoke(context.Background(), "panicky", nil)
		if !errors.Is(err, ErrInvocationPanic) {
			t.Fatalf("call %d want ErrInvocationPanic, got %v", i, err)
		}
	}
	if calls.Load() != 5 {
		t.Fatalf("runtime must survive panics and serve all calls, got %d", calls.Load())
	}

	// 同函数在 panic 后仍能正常服务。
	_ = r.Register("healthy", func(ctx context.Context, _ []byte) ([]byte, error) {
		return []byte("fine"), nil
	}, FunctionConfig{})
	res, err := r.Invoke(context.Background(), "healthy", nil)
	if err != nil || string(res.Value) != "fine" {
		t.Fatalf("runtime unhealthy after panic: %v %v", res, err)
	}

	// 所有 panic 调用结束后额度归零。
	st := r.Stats()
	if st.GlobalHeld != 0 {
		t.Fatalf("permits leaked after panics: globalHeld=%d", st.GlobalHeld)
	}
}

func TestUnregisterDoesNotAffectInFlight(t *testing.T) {
	r := newRunningRuntime(t, WithGlobalConcurrency(4))
	started := make(chan struct{}, 1)
	proceed := make(chan struct{})
	var ran atomic.Bool
	_ = r.Register("ephemeral", func(ctx context.Context, _ []byte) ([]byte, error) {
		started <- struct{}{}
		<-proceed
		ran.Store(true)
		return []byte("finished"), nil
	}, FunctionConfig{MaxConcurrency: 1, Timeout: 10 * time.Second})

	resCh := make(chan resultOrErr, 1)
	go func() {
		res, err := r.Invoke(context.Background(), "ephemeral", nil)
		resCh <- resultOrErr{res, err}
	}()
	<-started

	if err := r.Unregister("ephemeral"); err != nil {
		t.Fatalf("Unregister: %v", err)
	}
	// 在途期间新调用被明确拒绝。
	if _, err := r.Invoke(context.Background(), "ephemeral", nil); !errors.Is(err, ErrFunctionNotFound) {
		t.Fatalf("want ErrFunctionNotFound after unregister, got %v", err)
	}
	// 注销不存在的函数。
	if err := r.Unregister("ghost"); !errors.Is(err, ErrFunctionNotFound) {
		t.Fatalf("want ErrFunctionNotFound, got %v", err)
	}
	close(proceed)
	roe := <-resCh
	if roe.err != nil || string(roe.res.Value) != "finished" {
		t.Fatalf("in-flight invocation must finish normally, got %+v %v", roe.res, roe.err)
	}
	if !ran.Load() {
		t.Fatal("in-flight function body did not run to completion")
	}
}

type resultOrErr struct {
	res *Result
	err error
}

func TestFunctionConfigTimeoutApplies(t *testing.T) {
	r := newRunningRuntime(t, WithDefaultTimeout(10*time.Second))
	_ = r.Register("slow-cfg", func(ctx context.Context, _ []byte) ([]byte, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}, FunctionConfig{Timeout: 40 * time.Millisecond})
	start := time.Now()
	_, err := r.Invoke(context.Background(), "slow-cfg", nil)
	if !errors.Is(err, ErrInvocationTimeout) {
		t.Fatalf("want ErrInvocationTimeout from FunctionConfig, got %v", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("timeout took too long: %v", d)
	}
}

func TestCallerContextCancelPropagates(t *testing.T) {
	r := newRunningRuntime(t)
	ctxStarted := make(chan struct{})
	_ = r.Register("cancellable", func(ctx context.Context, _ []byte) ([]byte, error) {
		close(ctxStarted)
		<-ctx.Done()
		return nil, ctx.Err()
	}, FunctionConfig{Timeout: 10 * time.Second})

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := r.Invoke(ctx, "cancellable", nil)
		errCh <- err
	}()
	<-ctxStarted
	cancel()

	select {
	case err := <-errCh:
		if err == nil || errors.Is(err, ErrInvocationTimeout) {
			t.Fatalf("want caller context error propagated, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("caller cancellation did not unblock Invoke")
	}

	// 取消后额度必须归还。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if r.Stats().GlobalHeld == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("permits leaked after caller cancel: %+v", r.Stats())
}
