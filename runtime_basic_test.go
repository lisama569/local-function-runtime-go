package fnruntime

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestInvokeBasicSuccess(t *testing.T) {
	r := newStartedRuntime(t, Config{})
	r.MustRegister("echo", func(ctx context.Context, payload []byte) ([]byte, error) {
		return append([]byte("ok:"), payload...), nil
	}, FunctionConfig{MaxConcurrency: 2})

	res, err := r.Invoke(context.Background(), "echo", []byte("ping"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(res.Output) != "ok:ping" || res.Deduped {
		t.Fatalf("unexpected result: %q deduped=%v", res.Output, res.Deduped)
	}
	t.Logf("verdict: success output=%q", res.Output)
}

func TestInvokeBeforeStart(t *testing.T) {
	r := New(Config{Logger: newTestLogger(t)})
	// 未注册时调用，应返回 runtime_stopped（先于 not_found 的状态门禁）。
	_, err := r.Invoke(context.Background(), "nope", nil)
	assertReason(t, err, ReasonRuntimeStopped)

	if err := r.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	_, err = r.Invoke(context.Background(), "nope", nil)
	assertReason(t, err, ReasonFunctionNotFound)
}

func TestRegisterValidation(t *testing.T) {
	r := New(Config{Logger: newTestLogger(t)})

	if err := r.Register("", func(context.Context, []byte) ([]byte, error) { return nil, nil }, FunctionConfig{}); !errors.Is(err, errEmptyName) {
		t.Fatalf("expected errEmptyName, got %v", err)
	}
	if err := r.Register("nil", nil, FunctionConfig{}); !errors.Is(err, errNilHandler) {
		t.Fatalf("expected errNilHandler, got %v", err)
	}
	if err := r.Register("bad", func(context.Context, []byte) ([]byte, error) { return nil, nil },
		FunctionConfig{MaxConcurrency: -1}); err == nil {
		t.Fatalf("expected invalid config error")
	}
	// 默认并发度被规范化为 1。
	if err := r.Register("f", func(context.Context, []byte) ([]byte, error) { return nil, nil },
		FunctionConfig{}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := r.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := r.Start(); !errors.Is(err, errAlreadyStarted) {
		t.Fatalf("expected errAlreadyStarted, got %v", err)
	}
	if err := r.Register("dup", func(context.Context, []byte) ([]byte, error) { return nil, nil },
		FunctionConfig{}); err != nil {
		t.Fatalf("register dup: %v", err)
	}
	if err := r.Register("dup", func(context.Context, []byte) ([]byte, error) { return nil, nil },
		FunctionConfig{}); !errors.Is(err, errDuplicateName) {
		t.Fatalf("expected errDuplicateName, got %v", err)
	}
}

func TestRestartCycle(t *testing.T) {
	r := New(Config{Logger: newTestLogger(t)})
	h := func(ctx context.Context, payload []byte) ([]byte, error) { return []byte("v"), nil }
	r.MustRegister("f", h, FunctionConfig{MaxConcurrency: 1})

	for cycle := 0; cycle < 3; cycle++ {
		if err := r.Start(); err != nil {
			t.Fatalf("cycle %d start: %v", cycle, err)
		}
		res, err := r.Invoke(context.Background(), "f", nil)
		if err != nil {
			t.Fatalf("cycle %d invoke: %v", cycle, err)
		}
		t.Logf("cycle=%d verdict=success output=%q", cycle, res.Output)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := r.Shutdown(ctx); err != nil {
			cancel()
			t.Fatalf("cycle %d shutdown: %v", cycle, err)
		}
		cancel()
		// 停机后新调用被拒绝。
		_, err = r.Invoke(context.Background(), "f", nil)
		assertReason(t, err, ReasonRuntimeStopped)
	}
}

func TestShutdownWithoutStart(t *testing.T) {
	r := New(Config{Logger: newTestLogger(t)})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown without start: %v", err)
	}
	// 之后仍可正常启动。
	if err := r.Start(); err != nil {
		t.Fatalf("restart after shutdown-new: %v", err)
	}
}
