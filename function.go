package localfunctionruntime

import (
	"context"
	"time"
)

// Function 用户注册的本地处理函数。
//
// 函数应尊重 ctx.Done()：超时或停机时 ctx 会被取消，
// 函数若无视取消继续执行，其后台 goroutine 将无法被回收（运行时无法强杀 goroutine）。
type Function func(ctx context.Context, payload []byte) ([]byte, error)

// FunctionConfig 单个函数的配置。
type FunctionConfig struct {
	// MaxConcurrency 该函数独立并发上限；<=0 时等于全局并发上限。
	MaxConcurrency int
	// Timeout 该函数的单次调用默认超时；0 使用运行时默认超时。
	Timeout time.Duration
}

func (c FunctionConfig) maxConcurrency(fallback int) int {
	if c.MaxConcurrency <= 0 {
		return fallback
	}
	return c.MaxConcurrency
}

func (c FunctionConfig) timeout(fallback time.Duration) time.Duration {
	if c.Timeout <= 0 {
		return fallback
	}
	return c.Timeout
}

// functionEntry 注册表中的函数条目。
// 每个条目对应独立的 per-function 额度单元；条目从注册表移除后，
// 已在途调用持有的 permit 仍引用该条目，额度可被安全归还。
type functionEntry struct {
	name    string
	fn      Function
	cfg     FunctionConfig
	removed bool
}
