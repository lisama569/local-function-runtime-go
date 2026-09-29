package localfunctionruntime

import "errors"

// 失败原因分类：调用方可用 errors.Is 精确区分。
var (
	// ErrFunctionNotFound 函数未注册（或已在调用前被移除）。
	ErrFunctionNotFound = errors.New("function not found")
	// ErrFunctionConcurrencyLimit 超过该函数自身的并发上限。
	ErrFunctionConcurrencyLimit = errors.New("function concurrency limit exceeded")
	// ErrGlobalConcurrencyLimit 超过全局并发上限（含公平份额限制）。
	ErrGlobalConcurrencyLimit = errors.New("global concurrency limit exceeded")
	// ErrShuttingDown 运行时正在停机，拒绝新调用。
	ErrShuttingDown = errors.New("runtime is shutting down")
	// ErrNotRunning 运行时尚未启动或已关闭。
	ErrNotRunning = errors.New("runtime is not running")
	// ErrInvocationTimeout 单次调用在限定时间内未完成。
	ErrInvocationTimeout = errors.New("invocation timeout")
	// ErrInvocationPanic 函数执行期间发生 panic，已被隔离恢复。
	ErrInvocationPanic = errors.New("invocation panic")
	// ErrShutdownTimeout 停机等待在途调用超过截止时间。
	ErrShutdownTimeout = errors.New("shutdown deadline exceeded")
	// ErrInvalidConfig 配置非法。
	ErrInvalidConfig = errors.New("invalid config")
)
