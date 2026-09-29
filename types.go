package fnruntime

import (
	"context"
	"time"
)

// HandlerFunc 是注册到运行时的处理函数形态。
type HandlerFunc func(ctx context.Context, payload []byte) ([]byte, error)

// FunctionConfig 描述单个函数的注册配置。
type FunctionConfig struct {
	// MaxConcurrency 是该函数自身的并发上限，必须 > 0；为 0 时取默认值 1。
	MaxConcurrency int
}

// Config 是运行时级别的配置，零值会在 New 时填充默认值。
type Config struct {
	// GlobalConcurrency 全局并发上限，默认 32。
	GlobalConcurrency int
	// DefaultTimeout 单次调用的默认超时，默认 5s。
	DefaultTimeout time.Duration
	// IdempotencyTTL 幂等结果的保留时长，默认 5m。
	IdempotencyTTL time.Duration
	// Logger 用于输出调度与判定日志，默认丢弃。
	Logger Logger
}

// InvokeOptions 控制单次调用行为。
type InvokeOptions struct {
	// Timeout 本次调用的超时；为 0 时取运行时默认值，<0 表示不超时。
	Timeout time.Duration
	// IdempotencyKey 非空时开启幂等去重。
	IdempotencyKey string
}

// InvokeOption 以可选参数形式覆盖 InvokeOptions。
type InvokeOption func(*InvokeOptions)

// Result 是一次调用成功（或命中去重）后的结构化结果。
type Result struct {
	// Output 处理函数返回的输出。
	Output []byte
	// Deduped 为 true 表示本次没有真正执行处理函数，
	// 而是复用了同一幂等标识下此前调用的终态结果。
	Deduped bool
}

// Logger 是运行时接受的最小日志接口。
type Logger interface {
	Printf(format string, args ...any)
}

// noopLogger 在未提供 Logger 时使用。
type noopLogger struct{}

func (noopLogger) Printf(string, ...any) {}

// WithTimeout 覆盖单次调用超时。
func WithTimeout(d time.Duration) InvokeOption {
	return func(o *InvokeOptions) { o.Timeout = d }
}

// WithIdempotencyKey 附带幂等标识。
func WithIdempotencyKey(key string) InvokeOption {
	return func(o *InvokeOptions) { o.IdempotencyKey = key }
}
