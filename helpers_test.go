package localfunctionruntime

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// testLogger 单测日志器：打印并发额度、幂等标识与最终判定。
// 内部带缓冲与互斥，测试结束后仍有在途 goroutine 落日志时也不会触发
// testing 的 "Log in goroutine after test completed" panic。
type testLogger struct {
	mu      sync.Mutex
	t       *testing.T
	records []string
	closed  bool
}

func newTestLogger(t *testing.T) *testLogger {
	l := &testLogger{t: t}
	t.Cleanup(func() {
		l.mu.Lock()
		l.closed = true
		l.t = nil
		l.mu.Unlock()
	})
	return l
}

func (l *testLogger) Printf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.t == nil {
		l.records = append(l.records, msg)
		return
	}
	l.t.Logf("[runtime] %s", msg)
}

func newRunningRuntime(t *testing.T, opts ...Option) *Runtime {
	t.Helper()
	r, err := New(append(opts, WithLogger(newTestLogger(t)))...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := r.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.Shutdown(ctx)
	})
	return r
}
