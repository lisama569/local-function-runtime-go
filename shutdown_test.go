package fnruntime

import (
	"context"

	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestGracefulShutdownWaitsForInFlight：停机时已在途的调用必须在截止时间内
// 跑完并向调用方交付结果；停机期间新调用被拒绝。
func TestGracefulShutdownWaitsForInFlight(t *testing.T) {
	r := newStartedRuntimeManual(t, Config{GlobalConcurrency: 8, DefaultTimeout: 5 * time.Second})

	release := make(chan struct{})
	started := make(chan struct{}, 8)
	var doneOK atomic.Int64
	r.MustRegister("s", slowHandler(started, release, nil), FunctionConfig{MaxConcurrency: 8})
	const n = 4
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := r.Invoke(context.Background(), "s", []byte("payload"))
			if err != nil {
				t.Errorf("in-flight call failed during shutdown: %v", err)
				return
			}
			if string(res.Output) != "payload" {
				t.Errorf("unexpected output %q", res.Output)
			}
			doneOK.Add(1)
		}()
	}
	for i := 0; i < n; i++ {
		<-started
	}

	// 开始停机：新调用立即被拒绝。
	shutdownErr := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		shutdownErr <- r.Shutdown(ctx)
	}()

	// 给 Shutdown 一点时间切到 draining，然后验证拒绝门禁。
	time.Sleep(60 * time.Millisecond)
	_, err := r.Invoke(context.Background(), "s", nil)
	assertReason(t, err, ReasonRuntimeStopped)

	select {
	case <-shutdownErr:
		t.Fatal("shutdown must not return while in-flight calls are still running")
	default:
	}

	close(release)
	wg.Wait()

	select {
	case err := <-shutdownErr:
		if err != nil {
			t.Fatalf("graceful shutdown should succeed: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not return after in-flight calls finished")
	}
	if doneOK.Load() != n {
		t.Fatalf("all %d in-flight results must be delivered, got %d", n, doneOK.Load())
	}
	t.Logf("verdict: graceful shutdown drained %d/%d in-flight calls, new calls rejected", doneOK.Load(), n)
}

// TestShutdownDeadlineExceeded：截止时间到达仍有慢调用时，Shutdown 返回
// DeadlineExceeded；之后可以重新 Start（Start 等待后台收尾），重启后服务正常。
func TestShutdownDeadlineExceeded(t *testing.T) {
	r := newStartedRuntimeManual(t, Config{GlobalConcurrency: 4, DefaultTimeout: 10 * time.Second})

	release := make(chan struct{})
	started := make(chan struct{}, 4)
	r.MustRegister("slow", slowHandler(started, release, nil), FunctionConfig{MaxConcurrency: 4})
	go func() { _, _ = r.Invoke(context.Background(), "slow", nil) }()
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	err := r.Shutdown(ctx)
	cancel()
	if err != context.DeadlineExceeded {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
	t.Logf("verdict: shutdown returned deadline_exceeded while call still in flight")

	// 后台在途仍受调用超时约束；释放 handler 阻塞让其自然结束。
	close(release)

	// 重新 Start 必须等待后台收尾结束后成功，且服务可用。
	done := make(chan error, 1)
	go func() { done <- r.Start() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("restart after forced shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start blocked forever waiting background drain")
	}

	r.MustRegister("post", func(context.Context, []byte) ([]byte, error) { return []byte("up"), nil },
		FunctionConfig{})
	res, err := r.Invoke(context.Background(), "post", nil)
	if err != nil || string(res.Output) != "up" {
		t.Fatalf("runtime not serving after restart: %v %v", res, err)
	}
	t.Logf("verdict: restarted cleanly after deadline-exceeded shutdown, serving again")
}

// TestShutdownRaceWithInvokes：大量调用与 Shutdown 并发，在 race detector
// 下必须无竞争、无 WaitGroup 泄漏；每个调用要么拿到终态结果，要么拿到
// runtime_stopped，二者皆为可预期判定。
func TestShutdownRaceWithInvokes(t *testing.T) {
	r := newStartedRuntimeManual(t, Config{GlobalConcurrency: 4, DefaultTimeout: 300 * time.Millisecond})

	gate := make(chan struct{})
	var executions atomic.Int64
	r.MustRegister("race-fn", func(ctx context.Context, payload []byte) ([]byte, error) {
		executions.Add(1)
		select {
		case <-gate:
		case <-ctx.Done():
		}
		return []byte("ok"), nil
	}, FunctionConfig{MaxConcurrency: 2})

	var wg sync.WaitGroup
	var success, stopped, other atomic.Int64
	start := make(chan struct{})
	for i := 0; i < 80; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			res, err := r.Invoke(context.Background(), "race-fn", nil, WithTimeout(250*time.Millisecond))
			switch {
			case err == nil:
				_ = res
				success.Add(1)
			case reasonOf(err) == ReasonRuntimeStopped:
				stopped.Add(1)
			default:
				other.Add(1) // busy/timeout/panic 也都是明确可区分的判定
			}
		}()
	}
	close(start)

	shutdownDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		shutdownDone <- r.Shutdown(ctx)
	}()
	close(gate)
	wg.Wait()
	serr := <-shutdownDone
	if serr != nil {
		t.Fatalf("shutdown after concurrent burst: %v", serr)
	}
	t.Logf("verdict: shutdown race settled success=%d runtime_stopped=%d otherClassified=%d executions=%d",
		success.Load(), stopped.Load(), other.Load(), executions.Load())
}

// newStartedRuntimeManual 与 newStartedRuntime 相同，但不注册 t.Cleanup、
// 也不预置函数，供需要自己掌控注册与 Shutdown 时序的停机测试使用。
func newStartedRuntimeManual(t *testing.T, cfg Config) *Runtime {
	t.Helper()
	cfg.Logger = newTestLogger(t)
	r := New(cfg)
	if err := r.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	return r
}
