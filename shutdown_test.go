package localfunctionruntime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestShutdownWaitsForInFlight(t *testing.T) {
	r, err := New(WithLogger(newTestLogger(t)))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
	var completed atomic.Bool
	_ = r.Register("graceful", func(ctx context.Context, _ []byte) ([]byte, error) {
		<-ctx.Done() // 收到停机取消信号后才收尾
		time.Sleep(50 * time.Millisecond)
		completed.Store(true)
		return []byte("final"), nil
	}, FunctionConfig{Timeout: 10 * time.Second})

	done := make(chan resultOrErr, 1)
	go func() {
		res, err := r.Invoke(context.Background(), "graceful", nil)
		done <- resultOrErr{res, err}
	}()
	time.Sleep(30 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.Shutdown(ctx); err != nil {
		t.Fatalf("graceful shutdown: %v", err)
	}
	if !completed.Load() {
		t.Fatal("in-flight function must run to completion during shutdown")
	}
	roe := <-done
	// 函数随停机取消而收尾，调用方可预期地收到 ErrShuttingDown。
	if !errors.Is(roe.err, ErrShuttingDown) {
		t.Fatalf("in-flight caller want ErrShuttingDown, got %v", roe.err)
	}

	// 停机后新调用被拒绝。
	if _, err := r.Invoke(context.Background(), "graceful", nil); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("want ErrNotRunning after stop, got %v", err)
	}
}

func TestShutdownDeadlineExceeded(t *testing.T) {
	r, err := New(WithLogger(newTestLogger(t)))
	if err != nil {
		t.Fatal(err)
	}
	_ = r.Start()
	started := make(chan struct{})
	finished := make(chan struct{})
	_ = r.Register("stuck", func(ctx context.Context, _ []byte) ([]byte, error) {
		// 故意忽略取消，模拟无法中断的函数。
		close(started)
		time.Sleep(500 * time.Millisecond)
		close(finished)
		return nil, nil
	}, FunctionConfig{Timeout: 10 * time.Second})
	go func() { _, _ = r.Invoke(context.Background(), "stuck", nil) }()
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	if err := r.Shutdown(ctx); !errors.Is(err, ErrShutdownTimeout) {
		t.Fatalf("want ErrShutdownTimeout, got %v", err)
	}
	cancel()

	// 超时后运行时不再等待，但在途 goroutine 依旧会自然结算；这里等它结束，
	// 验证额度/幂等状态最终收敛、无后台单元遗留。
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("orphaned in-flight goroutine never finished")
	}
}

func TestNewCallsRejectedDuringShutdownRace(t *testing.T) {
	r, err := New(WithGlobalConcurrency(4), WithLogger(newTestLogger(t)))
	if err != nil {
		t.Fatal(err)
	}
	_ = r.Start()
	proceed := make(chan struct{})
	_ = r.Register("busy", func(ctx context.Context, _ []byte) ([]byte, error) {
		<-proceed
		return nil, nil
	}, FunctionConfig{MaxConcurrency: 4, Timeout: 10 * time.Second})
	for i := 0; i < 2; i++ {
		go func() { _, _ = r.Invoke(context.Background(), "busy", nil) }()
	}
	time.Sleep(30 * time.Millisecond)

	// 高并发发起新调用，同时停机：每个新调用要么成功执行（额度内），
	// 要么拿到可区分的拒绝错误，绝不允许阻塞或 panic。
	const N = 100
	errs := make(chan error, N)
	for i := 0; i < N; i++ {
		go func() {
			_, e := r.Invoke(context.Background(), "busy", nil)
			errs <- e
		}()
	}
	shutdownDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		shutdownDone <- r.Shutdown(ctx)
	}()

	rejected := 0
	executed := 0
	for i := 0; i < N; i++ {
		e := <-errs
		switch {
		case e == nil:
			executed++
		case errors.Is(e, ErrFunctionConcurrencyLimit),
			errors.Is(e, ErrGlobalConcurrencyLimit),
			errors.Is(e, ErrShuttingDown),
			errors.Is(e, ErrNotRunning):
			rejected++
		default:
			t.Fatalf("unexpected error class: %v", e)
		}
	}
	close(proceed)
	if err := <-shutdownDone; err != nil {
		t.Fatalf("shutdown should drain after proceed: %v", err)
	}
	t.Logf("shutdown race: executed=%d rejected=%d total=%d", executed, rejected, N)
	if executed+rejected != N {
		t.Fatal("every concurrent call must be accounted for")
	}
}

func TestRestartCycle(t *testing.T) {
	r, err := New(WithLogger(newTestLogger(t)))
	if err != nil {
		t.Fatal(err)
	}
	for cycle := 0; cycle < 3; cycle++ {
		if err := r.Start(); err != nil {
			t.Fatalf("cycle %d Start: %v", cycle, err)
		}
		_ = r.Register("ping", func(ctx context.Context, _ []byte) ([]byte, error) {
			return []byte("pong"), nil
		}, FunctionConfig{})
		res, err := r.Invoke(context.Background(), "ping", nil)
		if err != nil || string(res.Value) != "pong" {
			t.Fatalf("cycle %d invoke: %v %v", cycle, res, err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := r.Shutdown(ctx); err != nil {
			t.Fatalf("cycle %d Shutdown: %v", cycle, err)
		}
		cancel()
	}

	// 再启动后旧注册表已清空，需重新注册。
	_ = r.Start()
	if _, err := r.Invoke(context.Background(), "ping", nil); !errors.Is(err, ErrFunctionNotFound) {
		t.Fatalf("registry should reset after restart, got %v", err)
	}
}

func TestDoubleShutdownAndStartState(t *testing.T) {
	r, err := New(WithLogger(newTestLogger(t)))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := r.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown before start should be no-op: %v", err)
	}
	_ = r.Start()
	c1, cancel1 := context.WithTimeout(ctx, time.Second)
	defer cancel1()
	if err := r.Shutdown(c1); err != nil {
		t.Fatalf("first shutdown: %v", err)
	}
	// 重复停机是安全的 no-op。
	c2, cancel2 := context.WithTimeout(ctx, time.Second)
	defer cancel2()
	if err := r.Shutdown(c2); err != nil {
		t.Fatalf("double shutdown should be safe: %v", err)
	}
}

func TestShutdownInFlightIdempotencyFollowersConsistent(t *testing.T) {
	r, err := New(WithGlobalConcurrency(4), WithIdempotencyTTL(time.Minute), WithLogger(newTestLogger(t)))
	if err != nil {
		t.Fatal(err)
	}
	_ = r.Start()
	started := make(chan struct{})
	_ = r.Register("fin", func(ctx context.Context, _ []byte) ([]byte, error) {
		close(started)
		<-ctx.Done()
		return nil, nil
	}, FunctionConfig{MaxConcurrency: 4, Timeout: 10 * time.Second})

	leaderErr := make(chan error, 1)
	followerErr := make(chan error, 1)
	go func() {
		_, e := r.Invoke(context.Background(), "fin", nil, WithIdempotencyKey("ship-key"))
		leaderErr <- e
	}()
	<-started
	go func() {
		_, e := r.Invoke(context.Background(), "fin", nil, WithIdempotencyKey("ship-key"))
		followerErr <- e
	}()
	// 等 follower 挂上在途去重。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && r.Stats().InFlight < 2 {
		time.Sleep(2 * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	le := <-leaderErr
	fe := <-followerErr
	if !errors.Is(le, ErrShuttingDown) || !errors.Is(fe, ErrShuttingDown) {
		t.Fatalf("leader and follower must both get ErrShuttingDown, got leader=%v follower=%v", le, fe)
	}
}
