package localfunctionruntime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConcurrentContentionNoLeak(t *testing.T) {
	r := newRunningRuntime(t, WithGlobalConcurrency(6), WithIdempotencyTTL(200*time.Millisecond))

	var executions atomic.Int64
	mk := func(delay time.Duration, panicEvery int64, counter *atomic.Int64) Function {
		var localCounter atomic.Int64
		c := counter
		if c == nil {
			c = &localCounter
		}
		return func(ctx context.Context, p []byte) ([]byte, error) {
			executions.Add(1)
			n := c.Add(1)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			if panicEvery > 0 && n%panicEvery == 0 {
				panic("synthetic")
			}
			return p, nil
		}
	}
	var aCounter atomic.Int64
	_ = r.Register("a", mk(2*time.Millisecond, 3, &aCounter), FunctionConfig{MaxConcurrency: 2, Timeout: time.Second})
	_ = r.Register("b", mk(1*time.Millisecond, 0, nil), FunctionConfig{MaxConcurrency: 4, Timeout: time.Second})
	_ = r.Register("c", mk(5*time.Millisecond, 0, nil), FunctionConfig{MaxConcurrency: 1, Timeout: 100 * time.Millisecond})
	// d 慢于自身超时：已准入的调用会产生确定性的超时样本。
	_ = r.Register("d", mk(80*time.Millisecond, 0, nil), FunctionConfig{MaxConcurrency: 4, Timeout: 20 * time.Millisecond})

	const perFunc = 120
	var wg sync.WaitGroup
	var ok, rejected, timeouts, panics, other int64
	for _, name := range []string{"a", "b", "c"} {
		for i := 0; i < perFunc; i++ {
			wg.Add(1)
			i := i
			go func(name string, idx int) {
				defer wg.Done()
				time.Sleep(time.Duration(idx%20) * time.Millisecond)
				opts := []InvokeOption{}
				if name == "b" && i%3 == 0 {
					opts = append(opts, WithIdempotencyKey(periodicKey(i)))
				}
				_, err := r.Invoke(context.Background(), name, []byte("x"), opts...)
				switch {
				case err == nil:
					atomic.AddInt64(&ok, 1)
				case errors.Is(err, ErrFunctionConcurrencyLimit), errors.Is(err, ErrGlobalConcurrencyLimit):
					atomic.AddInt64(&rejected, 1)
				case errors.Is(err, ErrInvocationTimeout):
					atomic.AddInt64(&timeouts, 1)
				case errors.Is(err, ErrInvocationPanic):
					atomic.AddInt64(&panics, 1)
				default:
					atomic.AddInt64(&other, 1)
					t.Errorf("unexpected error class for %s: %v", name, err)
				}
			}(name, i)
		}
	}
	wg.Wait()

	// 风暴结束后再发起确定性超时样本：d 慢于自身超时（80ms vs 20ms）。
	const timeoutSamples = 3
	var wg2 sync.WaitGroup
	for i := 0; i < timeoutSamples; i++ {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			_, err := r.Invoke(context.Background(), "d", nil)
			switch {
			case errors.Is(err, ErrInvocationTimeout):
				atomic.AddInt64(&timeouts, 1)
			case err == nil:
				atomic.AddInt64(&ok, 1)
			default:
				atomic.AddInt64(&other, 1)
			}
		}()
	}
	wg2.Wait()
	total := perFunc*3 + timeoutSamples
	t.Logf("stress total=%d ok=%d rejected=%d timeouts=%d panics=%d other=%d executions=%d",
		total, ok, rejected, timeouts, panics, other, executions.Load())
	if ok+rejected+timeouts+panics+other != int64(total) {
		t.Fatal("every invocation must be accounted for")
	}
	if ok == 0 || rejected == 0 || panics == 0 || timeouts == 0 {
		t.Fatalf("stress should mix success/rejection/timeout/panic outcomes, got ok=%d rejected=%d timeouts=%d panics=%d",
			ok, rejected, timeouts, panics)
	}

	// 全部结算后额度必须完全归还。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		st := r.Stats()
		if st.GlobalHeld == 0 && st.InFlight == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("quota leak after storm: %+v", r.Stats())
}

func periodicKey(i int) string {
	// 少量 key 周期性复用：在 TTL 窗口内并发复用会去重，跨 TTL 重新执行。
	return "key-" + string(rune('a'+i%5))
}
