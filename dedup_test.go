package localfunctionruntime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestIdempotencyConcurrentDedup(t *testing.T) {
	r := newRunningRuntime(t, WithIdempotencyTTL(time.Minute))
	var executions atomic.Int64
	gate := make(chan struct{})
	leaderIn := make(chan struct{})
	_ = r.Register("charge", func(ctx context.Context, p []byte) ([]byte, error) {
		n := executions.Add(1)
		close(leaderIn)
		<-gate
		return []byte(fmt.Sprintf("charge#%d", n)), nil
	}, FunctionConfig{MaxConcurrency: 16})

	const N = 10
	var wg sync.WaitGroup
	results := make([]string, N)
	deduped := make([]bool, N)
	errs := make([]error, N)
	start := make(chan struct{})
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			res, err := r.Invoke(context.Background(), "charge", nil, WithIdempotencyKey("order-123"))
			if err != nil {
				errs[i] = err
				return
			}
			results[i] = string(res.Value)
			deduped[i] = res.Deduped
		}(i)
	}
	close(start)
	<-leaderIn
	// 轮询直到 N 个调用全部进入在途状态（1 个执行中 + N-1 个 follower 等待）。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if r.Stats().InFlight == int64(N) {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if r.Stats().InFlight != int64(N) {
		close(gate)
		t.Fatalf("want %d in-flight followers, stats=%+v", N, r.Stats())
	}
	close(gate)
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := executions.Load(); got != 1 {
		t.Fatalf("same idempotency key must execute exactly once, got %d", got)
	}
	leaders := 0
	for i, v := range results {
		if v != "charge#1" {
			t.Fatalf("call %d got %q, want shared result charge#1", i, v)
		}
		if !deduped[i] {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("want exactly 1 leader execution and %d deduped followers, got leaders=%d", N-1, leaders)
	}
}

func TestIdempotencyCacheThenTTLExpiry(t *testing.T) {
	r := newRunningRuntime(t, WithIdempotencyTTL(60*time.Millisecond))
	var executions atomic.Int64
	_ = r.Register("charge2", func(ctx context.Context, _ []byte) ([]byte, error) {
		return []byte(fmt.Sprintf("v%d", executions.Add(1))), nil
	}, FunctionConfig{})

	call := func() (*Result, error) {
		return r.Invoke(context.Background(), "charge2", nil, WithIdempotencyKey("k-ttl"))
	}
	res, err := call()
	if err != nil || string(res.Value) != "v1" {
		t.Fatalf("first call: %v %v", res, err)
	}
	res, err = call()
	if err != nil || string(res.Value) != "v1" || !res.Cached {
		t.Fatalf("cached call should return v1 with Cached flag, got %v %v", res, err)
	}
	if got := executions.Load(); got != 1 {
		t.Fatalf("want 1 execution within TTL, got %d", got)
	}
	time.Sleep(120 * time.Millisecond)
	res, err = call()
	if err != nil || string(res.Value) != "v2" {
		t.Fatalf("post-TTL retry should execute again, got %v %v", res, err)
	}
}

func TestIdempotencyZeroTTLRetries(t *testing.T) {
	r := newRunningRuntime(t, WithIdempotencyTTL(0))
	var executions atomic.Int64
	_ = r.Register("fn", func(ctx context.Context, _ []byte) ([]byte, error) {
		return []byte(fmt.Sprintf("v%d", executions.Add(1))), nil
	}, FunctionConfig{})
	for i := 0; i < 3; i++ {
		_, err := r.Invoke(context.Background(), "fn", nil, WithIdempotencyKey("k0"))
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := executions.Load(); got != 3 {
		t.Fatalf("TTL=0 only dedups in-flight; want 3 executions, got %d", got)
	}
}

func TestIdempotencyFailureDedup(t *testing.T) {
	// 失败结果（含 timeout/panic 类不确定结果）同样按 TTL 去重，
	// 避免"至少一次投递"下调用方重试造成重复副作用。
	r := newRunningRuntime(t, WithIdempotencyTTL(time.Minute))
	var executions atomic.Int64
	boom := errors.New("downstream wrote but failed")
	_ = r.Register("sideeffect", func(ctx context.Context, _ []byte) ([]byte, error) {
		executions.Add(1)
		return nil, boom
	}, FunctionConfig{})
	for i := 0; i < 3; i++ {
		_, err := r.Invoke(context.Background(), "sideeffect", nil, WithIdempotencyKey("fail-key"))
		if !errors.Is(err, boom) {
			t.Fatalf("call %d: want boom, got %v", i, err)
		}
	}
	if got := executions.Load(); got != 1 {
		t.Fatalf("failed result with same key must still dedup, got %d", got)
	}
}

func TestIdempotencyAdmissionRejectNotCached(t *testing.T) {
	r := newRunningRuntime(t, WithGlobalConcurrency(2), WithIdempotencyTTL(time.Minute))
	proceed := make(chan struct{})
	started := make(chan struct{}, 2)
	_ = r.Register("full", func(ctx context.Context, _ []byte) ([]byte, error) {
		started <- struct{}{}
		<-proceed
		return []byte("ok"), nil
	}, FunctionConfig{MaxConcurrency: 2, Timeout: 10 * time.Second})

	go func() { _, _ = r.Invoke(context.Background(), "full", nil, WithIdempotencyKey("a")) }()
	go func() { _, _ = r.Invoke(context.Background(), "full", nil, WithIdempotencyKey("b")) }()
	<-started
	<-started

	// 同 key 被拒绝（额度满）不应缓存：释放额度后用同一 key 重试必须真正执行成功。
	_, err := r.Invoke(context.Background(), "full", nil, WithIdempotencyKey("retry-me"))
	if !errors.Is(err, ErrFunctionConcurrencyLimit) {
		t.Fatalf("want rejection, got %v", err)
	}
	close(proceed)
	// 等待两个占用槽位释放。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if r.Stats().GlobalHeld == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	res, err := r.Invoke(context.Background(), "full", nil, WithIdempotencyKey("retry-me"))
	if err != nil {
		t.Fatalf("retry after transient rejection must execute: %v", err)
	}
	if string(res.Value) != "ok" {
		t.Fatalf("unexpected %q", res.Value)
	}
}
