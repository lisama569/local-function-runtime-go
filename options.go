package localfunctionruntime

import "time"

// Logger 运行时内部日志接口（默认静默丢弃）。
type Logger interface {
	Printf(format string, args ...any)
}

const (
	defaultGlobalConcurrency = 16
	defaultTimeout           = 30 * time.Second
	defaultIdempotencyTTL    = 5 * time.Minute
)

type config struct {
	globalConcurrency int
	defaultTimeout    time.Duration
	idempotencyTTL    time.Duration
	logger            Logger
}

// Option 运行时级配置项。
type Option func(*config)

func defaultConfig() config {
	return config{
		globalConcurrency: defaultGlobalConcurrency,
		defaultTimeout:    defaultTimeout,
		idempotencyTTL:    defaultIdempotencyTTL,
		logger:            nopLogger{},
	}
}

// WithGlobalConcurrency 设置全局并发上限（必须 >=1）。
func WithGlobalConcurrency(n int) Option {
	return func(c *config) {
		if n >= 1 {
			c.globalConcurrency = n
		}
	}
}

// WithDefaultTimeout 设置调用默认超时（必须 >0）。
func WithDefaultTimeout(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.defaultTimeout = d
		}
	}
}

// WithIdempotencyTTL 设置幂等结果在完成后的保留时长（0 表示完成后立即清理）。
func WithIdempotencyTTL(d time.Duration) Option {
	return func(c *config) {
		if d >= 0 {
			c.idempotencyTTL = d
		}
	}
}

// WithLogger 注入日志实现。
func WithLogger(l Logger) Option {
	return func(c *config) {
		if l != nil {
			c.logger = l
		}
	}
}

type nopLogger struct{}

func (nopLogger) Printf(string, ...any) {}

type invokeOptions struct {
	timeout        time.Duration
	idempotencyKey string
}

// InvokeOption 单次调用可选项。
type InvokeOption func(*invokeOptions)

// WithInvokeTimeout 覆盖本次调用的超时时间。
func WithInvokeTimeout(d time.Duration) InvokeOption {
	return func(o *invokeOptions) {
		if d > 0 {
			o.timeout = d
		}
	}
}

// WithIdempotencyKey 附带幂等标识：相同标识的并发/重试调用只执行一次函数。
func WithIdempotencyKey(key string) InvokeOption {
	return func(o *invokeOptions) {
		o.idempotencyKey = key
	}
}
