package fnruntime_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	fnruntime "github.com/lisama569/local-function-runtime-go"
)

func Example() {
	rt := fnruntime.New(fnruntime.Config{
		GlobalConcurrency: 8,
		DefaultTimeout:    2 * time.Second,
		IdempotencyTTL:    time.Minute,
	})
	if err := rt.Start(); err != nil {
		panic(err)
	}

	rt.MustRegister("echo", func(ctx context.Context, payload []byte) ([]byte, error) {
		return append([]byte("echo:"), payload...), nil
	}, fnruntime.FunctionConfig{MaxConcurrency: 4})

	res, err := rt.Invoke(context.Background(), "echo", []byte("hi"))
	fmt.Printf("first:  output=%q err=%v\n", res.Output, err)

	// 携带相同幂等标识的重试不会产生第二次真实执行。
	res2, err := rt.Invoke(context.Background(), "echo", []byte("hi"),
		fnruntime.WithIdempotencyKey("call-001"))
	fmt.Printf("leader: output=%q deduped=%v err=%v\n", res2.Output, res2.Deduped, err)

	res3, err := rt.Invoke(context.Background(), "echo", []byte("hi"),
		fnruntime.WithIdempotencyKey("call-001"))
	fmt.Printf("retry:  output=%q deduped=%v err=%v\n", res3.Output, res3.Deduped, err)

	// 按失败原因区分错误。
	_, err = rt.Invoke(context.Background(), "missing", nil)
	var ce *fnruntime.CallError
	if errors.As(err, &ce) {
		fmt.Printf("missing call reason: %s\n", ce.Reason)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = rt.Shutdown(ctx)

	// Output:
	// first:  output="echo:hi" err=<nil>
	// leader: output="echo:hi" deduped=false err=<nil>
	// retry:  output="echo:hi" deduped=true err=<nil>
	// missing call reason: function_not_found
}
