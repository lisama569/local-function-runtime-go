package fnruntime

import "fmt"

// FailureReason 是一次调用失败的可区分原因。
type FailureReason string

const (
	// ReasonRuntimeStopped 运行时尚未启动、正在停机或已经关闭。
	ReasonRuntimeStopped FailureReason = "runtime_stopped"
	// ReasonFunctionNotFound 被调用的函数未注册（或已在运行期间被移除）。
	ReasonFunctionNotFound FailureReason = "function_not_found"
	// ReasonFunctionBusy 该函数自身的并发额度已用尽。
	ReasonFunctionBusy FailureReason = "function_busy"
	// ReasonGlobalBusy 运行时全局并发额度已用尽。
	ReasonGlobalBusy FailureReason = "global_busy"
	// ReasonTimeout 单次调用在给定截止时间内没有完成。
	ReasonTimeout FailureReason = "timeout"
	// ReasonCallerCanceled 调用方在拿到结果前主动取消了等待。
	ReasonCallerCanceled FailureReason = "caller_canceled"
	// ReasonPanic 处理函数在执行过程中发生 panic，已被运行时恢复。
	ReasonPanic FailureReason = "panic"
	// ReasonHandlerError 处理函数正常返回了一个业务错误。
	ReasonHandlerError FailureReason = "handler_error"
	// ReasonInvalidConfig 注册或运行时配置不合法。
	ReasonInvalidConfig FailureReason = "invalid_config"
)

// CallError 携带一次调用的结构化失败信息。
type CallError struct {
	// Function 是失败发生时被调用的函数名。
	Function string
	// Reason 是可机读、可区分的失败原因。
	Reason FailureReason
	// Err 是可选的底层错误。
	Err error
}

func (e *CallError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("fnruntime: call to %q failed: %s: %v", e.Function, e.Reason, e.Err)
	}
	return fmt.Sprintf("fnruntime: call to %q failed: %s", e.Function, e.Reason)
}

func (e *CallError) Unwrap() error { return e.Err }

func newCallError(name string, reason FailureReason, err error) *CallError {
	return &CallError{Function: name, Reason: reason, Err: err}
}
