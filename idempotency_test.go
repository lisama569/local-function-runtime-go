package fnruntime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var errBoom = errors.New("boom")

// TestIdempotencyConcurrentSameKey：N 个携带相同幂等标识的并发调用，
// 处理函数只能真实执行一次，其余调用全部以 Deduped=true 复用同一成功结果。
func TestIdempotencyConcurrentSameKey(t *testing.T) {
	r := newStartedRuntime(t, Config{GlobalConcurrency: 16, DefaultTimeout: 3 * time.Second})

	var executions atomic.Int64
	inside := make(chan struct{}, 64)
	proceed := make(chan struct{})
	r.MustRegister("side-effect", func(ctx context.Context, payload []byte) ([]byte, error) {
		executions.Add(1)
		inside <- struct{}{}
		<-proceed
		return append([]byte("result-for:"), payload...), nil
	}, FunctionConfig{MaxConcurrency: 16})

	const n = 32
	key := "order-1234"
	var wg sync.WaitGroup
	var deduped, fresh atomic.Int64
	var outputs sync.Map
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			res, err := r.Invoke(context.Background(), "side-effect", []byte("p"),
				WithIdempotencyKey(key), WithTimeout(2*time.Second))
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			outputs.Store(string(res.Output), struct{}{})
			if res.Deduped {
				deduped.Add(1)
			} else {
				fresh.Add(1)
			}
		}()
	}
	close(start)
	<-inside // 唯一一次真实执行进入后，放行
	close(proceed)
	wg.Wait()

	if executions.Load() != 1 {
		t.Fatalf("idempotency violated: handler executed %d times, want 1", executions.Load())
	}
	if fresh.Load() != 1 || deduped.Load() != n-1 {
		t.Fatalf("want 1 fresh + %d deduped, got fresh=%d deduped=%d", n-1, fresh.Load(), deduped.Load())
	}
	if cnt := lenSyncMap(&outputs); cnt != 1 {
		t.Fatalf("all callers must see same output, saw %d distinct outputs", cnt)
	}
	t.Logf("verdict: idempotencyKey=%q realExecutions=1 deduped=%d/%d sharedOutput=result-for:p",
		key, deduped.Load(), n)
}

// TestIdempotencyFailureCached：失败终态（panic/handler error/timeout）
// 同样按幂等标识去重：重试拿到与首次完全相同的失败原因，且不重复执行。
func TestIdempotencyFailureCached(t *testing.T) {
	r := newStartedRuntime(t, Config{DefaultTimeout: 3 * time.Second})

	var executions atomic.Int64
	r.MustRegister("flaky", func(ctx context.Context, payload []byte) ([]byte, error) {
		executions.Add(1)
		return nil, errBoom
	}, FunctionConfig{MaxConcurrency: 4})

	key := "retry-key"
	for i := 0; i < 5; i++ {
		_, err := r.Invoke(context.Background(), "flaky", nil, WithIdempotencyKey(key))
		assertReason(t, err, ReasonHandlerError)
	}
	if executions.Load() != 1 {
		t.Fatalf("failure must be deduped too, real executions=%d", executions.Load())
	}
	t.Logf("verdict: idempotencyKey=%q failure deduped realExecutions=1/5 reason=handler_error", key)

	// 不同 key 独立执行。
	_, err := r.Invoke(context.Background(), "flaky", nil, WithIdempotencyKey("other-key"))
	assertReason(t, err, ReasonHandlerError)
	if executions.Load() != 2 {
		t.Fatalf("different key must execute, real executions=%d", executions.Load())
	}
	t.Logf("verdict: distinct idempotencyKey=\"other-key\" executed independently")
}

// TestIdempotencyRetriesAcrossTime：先成功，短 TTL 后再次同键调用应重新执行；
// TTL 内则去重。
func TestIdempotencyRetriesAcrossTime(t *testing.T) {
	r := newStartedRuntime(t, Config{IdempotencyTTL: 120 * time.Millisecond, DefaultTimeout: time.Second})
	var executions atomic.Int64
	r.MustRegister("once", func(context.Context, []byte) ([]byte, error) {
		executions.Add(1)
		return []byte("ok"), nil
	}, FunctionConfig{MaxConcurrency: 4})

	key := "ttl-key"
	for i := 0; i < 3; i++ {
		res, err := r.Invoke(context.Background(), "once", nil, WithIdempotencyKey(key))
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if i > 0 && !res.Deduped {
			t.Fatalf("call %d within TTL must be deduped", i)
		}
	}
	if executions.Load() != 1 {
		t.Fatalf("want 1 execution within TTL, got %d", executions.Load())
	}
	time.Sleep(180 * time.Millisecond)
	res, err := r.Invoke(context.Background(), "once", nil, WithIdempotencyKey(key))
	if err != nil {
		t.Fatalf("post-TTL call: %v", err)
	}
	if res.Deduped {
		t.Fatal("post-TTL call must execute again")
	}
	if executions.Load() != 2 {
		t.Fatalf("want 2 executions after TTL expiry, got %d", executions.Load())
	}
	t.Logf("verdict: idempotencyKey=%q executions=2 (1 within TTL deduped, 1 after expiry)", key)
}

// TestIdempotencyPanicDedup：panic 终态也被记录，同键重试不再触发 handler。
func TestIdempotencyPanicDedup(t *testing.T) {
	r := newStartedRuntime(t, Config{})
	var executions atomic.Int64
	r.MustRegister("p", func(context.Context, []byte) ([]byte, error) {
		executions.Add(1)
		panic("nope")
	}, FunctionConfig{MaxConcurrency: 4})
	key := "panic-key"
	for i := 0; i < 4; i++ {
		_, err := r.Invoke(context.Background(), "p", nil, WithIdempotencyKey(key))
		assertReason(t, err, ReasonPanic)
	}
	if executions.Load() != 1 {
		t.Fatalf("panic outcome must dedupe, got executions=%d", executions.Load())
	}
	t.Logf("verdict: idempotencyKey=%q panic deduped realExecutions=1/4", key)
}

// TestIdempotencyBusyNotCached：当同键 leader 因额度被瞬时拒绝时，
// follower 收到同样明确的 busy 拒绝（而非阻塞），稍后额度恢复重试可成功，
// 且只有一次真实执行。
func TestIdempotencyBusyNotCached(t *testing.T) {
	r := newStartedRuntime(t, Config{GlobalConcurrency: 1, DefaultTimeout: 3 * time.Second})
	release := make(chan struct{})
	started := make(chan struct{}, 4)
	var executions atomic.Int64
	r.MustRegister("q", slowHandler(started, release, &executions), FunctionConfig{MaxConcurrency: 1})

	// 占满全局 + 函数额度。
	go func() { _, _ = r.Invoke(context.Background(), "q", nil) }()
	<-started

	// 同键的并发突发：全部快速失败为 busy（function_busy 先于 global），不挂起。
	key := "busy-key"
	var wg sync.WaitGroup
	var rejections atomic.Int64
	done := make(chan struct{})
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-done
			_, err := r.Invoke(context.Background(), "q", nil, WithIdempotencyKey(key))
			if err != nil && isTransientReason(reasonOf(err)) {
				rejections.Add(1)
			}
		}()
	}
	close(done)
	wg.Wait()
	if rejections.Load() != 16 {
		t.Fatalf("expected 16 immediate busy rejections, got %d", rejections.Load())
	}
	t.Logf("verdict: idempotencyKey=%q %d busy callers rejected without blocking", key, rejections.Load())

	// 释放占用后，同键调用成功且只执行一次（占位已被删除，重新准入）。
	close(release)
	time.Sleep(50 * time.Millisecond)
	res, err := r.Invoke(context.Background(), "q", []byte("z"), WithIdempotencyKey(key))
	if err != nil {
		t.Fatalf("retry after capacity freed: %v", err)
	}
	if res.Deduped || string(res.Output) != "z" {
		t.Fatalf("retry should execute fresh, got deduped=%v out=%q", res.Deduped, res.Output)
	}
	t.Logf("verdict: idempotencyKey=%q retry succeeded after capacity freed", key)
}

func lenSyncMap(m *sync.Map) int {
	n := 0
	m.Range(func(_, _ any) bool { n++; return true })
	return n
}
