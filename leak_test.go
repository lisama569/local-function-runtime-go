package fnruntime

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// TestNoGoroutineLeakAfterForcedShutdown：超时停机后，尚未自然结束的
// 执行 goroutine 必须最终全部退出，运行时收尾 goroutine 也不得残留。
func TestNoGoroutineLeakAfterForcedShutdown(t *testing.T) {
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	before := runtime.NumGoroutine()

	r := newStartedRuntimeManual(t, Config{GlobalConcurrency: 4, DefaultTimeout: 2 * time.Second})

	release := make(chan struct{})
	started := make(chan struct{}, 8)
	var executions atomic.Int64
	// 处理器一开始不响应 ctx（模拟无法中断的慢调用），测试显式释放后才退出。
	r.MustRegister("leak", func(ctx context.Context, payload []byte) ([]byte, error) {
		executions.Add(1)
		started <- struct{}{}
		<-release
		return []byte("ok"), nil
	}, FunctionConfig{MaxConcurrency: 4})

	const n = 3
	errc := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			_, err := r.Invoke(context.Background(), "leak", nil, WithTimeout(2*time.Second))
			errc <- err
		}()
	}
	for i := 0; i < n; i++ {
		<-started
	}

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	if err := r.Shutdown(ctx); err != context.DeadlineExceeded {
		cancel()
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	cancel()
	// 停机超时不取消在途调用：调用方仍在等待执行终态。
	// 显式释放卡住的处理器后，调用方拿到成功结果。
	close(release)
	for i := 0; i < n; i++ {
		select {
		case err := <-errc:
			if err != nil {
				t.Fatalf("in-flight result after release should be success, got %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("in-flight result never delivered after release")
		}
	}
	t.Logf("verdict: %d in-flight results delivered post forced-shutdown, executions=%d", n, executions.Load())

	// 所有 goroutine（执行单元、WaitGroup 等待者、drained 补发者）都应回收。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		if runtime.NumGoroutine() <= before+1 {
			t.Logf("verdict: goroutines settled before=%d after=%d", before, runtime.NumGoroutine())
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatalf("goroutine leak: before=%d after=%d", before, runtime.NumGoroutine())
}
