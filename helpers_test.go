package fnruntime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testLogger 同时把运行时日志输出到测试日志（-v 可见）。
// 运行时日志本身已包含并发额度、幂等标识与最终判定结果。
type testLogger struct {
	t  *testing.T
	mu sync.Mutex
}

func newTestLogger(t *testing.T) *testLogger {
	t.Helper()
	return &testLogger{t: t}
}

func (l *testLogger) Printf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.t.Logf("[runtime] "+format, args...)
}

// assertReason 断言 err 是 *CallError 且原因为 want。
func assertReason(t *testing.T, err error, want FailureReason) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected failure %q, got success", want)
	}
	var ce *CallError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *CallError with reason %q, got %v", want, err)
	}
	if ce.Reason != want {
		t.Fatalf("expected reason %q, got %q (err=%v)", want, ce.Reason, ce.Err)
	}
	t.Logf("verdict: failure reason=%q function=%q matches expectation", ce.Reason, ce.Function)
}

// slowHandler 返回一个尊重 ctx、在 release 关闭前阻塞的处理函数。
// 每次真实获得执行权都会向 started 发信号（缓冲由调用方保证），
// 并使 execCount 自增，便于统计真实执行次数。
func slowHandler(started chan<- struct{}, release <-chan struct{}, execCount *atomic.Int64) HandlerFunc {
	return func(ctx context.Context, payload []byte) ([]byte, error) {
		if execCount != nil {
			execCount.Add(1)
		}
		if started != nil {
			started <- struct{}{}
		}
		select {
		case <-release:
			return payload, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// newStartedRuntime 构造并启动一个注入测试日志的运行时。
func newStartedRuntime(t *testing.T, cfg Config) *Runtime {
	t.Helper()
	cfg.Logger = newTestLogger(t)
	r := New(cfg)
	if err := r.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.Shutdown(ctx)
	})
	return r
}
