package localfunctionruntime

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestInvokeBasicSuccess(t *testing.T) {
	r := newRunningRuntime(t)
	if err := r.Register("echo", func(ctx context.Context, p []byte) ([]byte, error) {
		return append([]byte("ok:"), p...), nil
	}, FunctionConfig{}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	res, err := r.Invoke(context.Background(), "echo", []byte("x"))
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if string(res.Value) != "ok:x" || res.Function != "echo" {
		t.Fatalf("unexpected result: %+v", res)
	}
}

func TestInvokeFunctionErrorTransparent(t *testing.T) {
	r := newRunningRuntime(t)
	sentinel := errors.New("boom")
	_ = r.Register("fail", func(ctx context.Context, _ []byte) ([]byte, error) {
		return nil, sentinel
	}, FunctionConfig{})
	_, err := r.Invoke(context.Background(), "fail", nil)
	if !errors.Is(err, sentinel) {
		t.Fatalf("want function error transparently, got %v", err)
	}
}

func startBlocker(t *testing.T, r *Runtime, name string, n int, maxFn int) (release func()) {
	t.Helper()
	proceed := make(chan struct{})
	started := make(chan struct{}, n)
	if err := r.Register(name, func(ctx context.Context, _ []byte) ([]byte, error) {
		started <- struct{}{}
		select {
		case <-proceed:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, FunctionConfig{MaxConcurrency: maxFn, Timeout: 10 * time.Second}); err != nil {
		t.Fatalf("Register %s: %v", name, err)
	}
	for i := 0; i < n; i++ {
		go func() { _, _ = r.Invoke(context.Background(), name, nil) }()
	}
	for i := 0; i < n; i++ {
		<-started
	}
	return func() { close(proceed) }
}

func TestFailuresAreClassified(t *testing.T) {
	// 全局 4，两个函数。公平份额：每个函数最多 4-(2-1)=3 槽。
	r := newRunningRuntime(t, WithGlobalConcurrency(4))

	// 函数级上限 1：第二次调用必须返回 ErrFunctionConcurrencyLimit。
	rel1 := startBlocker(t, r, "only", 1, 1)
	_, err := r.Invoke(context.Background(), "only", nil)
	if !errors.Is(err, ErrFunctionConcurrencyLimit) {
		t.Fatalf("want ErrFunctionConcurrencyLimit, got %v", err)
	}

	// 另一个函数函数级额度充足（8），但受公平份额 3 槽约束：占 3 后第 4 次必须被全局原因拒绝。
	rel2 := startBlocker(t, r, "hog", 3, 8)
	_, err = r.Invoke(context.Background(), "hog", nil)
	if !errors.Is(err, ErrGlobalConcurrencyLimit) {
		t.Fatalf("want ErrGlobalConcurrencyLimit (fair share), got %v", err)
	}

	// 未注册。
	_, err = r.Invoke(context.Background(), "missing", nil)
	if !errors.Is(err, ErrFunctionNotFound) {
		t.Fatalf("want ErrFunctionNotFound, got %v", err)
	}

	rel1()
	rel2()
}

func TestFairnessOtherFunctionStillRuns(t *testing.T) {
	// 全局 4：hog 拿到公平份额 3 槽时，另一个函数仍必须能拿到预留的 1 槽并成功执行。
	r := newRunningRuntime(t, WithGlobalConcurrency(4))
	rel := startBlocker(t, r, "hog", 3, 8)
	defer rel()

	_ = r.Register("small", func(ctx context.Context, _ []byte) ([]byte, error) {
		return []byte("alive"), nil
	}, FunctionConfig{MaxConcurrency: 1})
	res, err := r.Invoke(context.Background(), "small", nil)
	if err != nil {
		t.Fatalf("small function must remain schedulable: %v", err)
	}
	if string(res.Value) != "alive" {
		t.Fatalf("unexpected value %q", res.Value)
	}
}

func TestNotRunningAndRegisterValidation(t *testing.T) {
	r, err := New()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Invoke(context.Background(), "x", nil); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("want ErrNotRunning, got %v", err)
	}
	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
	if err := r.Register("", func(context.Context, []byte) ([]byte, error) { return nil, nil }, FunctionConfig{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("want ErrInvalidConfig, got %v", err)
	}
	if err := r.Register("nil", nil, FunctionConfig{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("want ErrInvalidConfig, got %v", err)
	}
}
