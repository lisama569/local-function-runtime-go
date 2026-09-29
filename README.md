# local-function-runtime-go

一个可被其他 Go 代码直接集成（`New` → `Start` → `Register` → `Invoke`）的**本地函数运行时**。
它托管若干注册进来的处理函数，对外提供统一调用入口；调用方拿到的要么是明确的
`*Result`，要么是可用 `errors.Is` 精确区分原因的失败。

- 多函数并发调用、每函数独立并发上限 + 全局资源上限、公平调度、超额立即拒绝；
- 单次调用超时、函数 panic、运行期注销等异常完全隔离，额度任何路径都不漏放/重复放；
- 幂等标识下"至少一次投递"的重试去重（同标识只产生一次执行）；
- 带截止时间的优雅停机，可安全地反复启动 / 关闭；
- 纯标准库、无外部依赖；Go ≥ 1.25。

## 快速开始

```go
rt, err := localfunctionruntime.New(
    localfunctionruntime.WithGlobalConcurrency(16),
    localfunctionruntime.WithDefaultTimeout(2*time.Second),
)
if err != nil { return err }
if err := rt.Start(); err != nil { return err }
defer func() { _ = rt.Shutdown(ctx) }()

rt.Register("orders.create", func(ctx context.Context, payload []byte) ([]byte, error) {
    // ctx 会在超时 / 停机时被取消，函数应及时响应 ctx.Done()
    return createOrder(ctx, payload)
}, localfunctionruntime.FunctionConfig{
    MaxConcurrency: 4,                  // 该函数独立并发上限
    Timeout:        500 * time.Millisecond,
})

res, err := rt.Invoke(ctx, "orders.create", payload,
    localfunctionruntime.WithIdempotencyKey(orderID), // 相同标识只执行一次
    localfunctionruntime.WithInvokeTimeout(time.Second),
)
if err != nil {
    switch {
    case errors.Is(err, localfunctionruntime.ErrFunctionConcurrencyLimit): // 函数额度满
    case errors.Is(err, localfunctionruntime.ErrGlobalConcurrencyLimit):   // 全局/公平额度满
    case errors.Is(err, localfunctionruntime.ErrInvocationTimeout):        // 单次超时
    case errors.Is(err, localfunctionruntime.ErrInvocationPanic):          // 函数 panic
    case errors.Is(err, localfunctionruntime.ErrFunctionNotFound):         // 未注册/已注销
    case errors.Is(err, localfunctionruntime.ErrShuttingDown):             // 停机中
    }
}
_ = res.Value
```

## 调度与并发模型

两级**非阻塞**准入（同一把锁内原子判定，超额立即拒绝，绝不排队、绝不阻塞调用方）：

1. **函数级额度**：每个函数有独立上限 `FunctionConfig.MaxConcurrency`，
   未配置时等于全局上限。占满 → `ErrFunctionConcurrencyLimit`。
2. **公平份额**：为每个其他**活跃函数**至少预留 1 个全局槽位，因此单个函数可占用的
   有效上限为

   ```
   effectiveCap = min(函数自身上限, max(1, 全局并发上限 - (活跃函数数 - 1)))
   ```

   一个函数被大量慢调用占满时，只能拿到"全局槽位 - 其他函数保留槽位"，
   无法把其他函数一起拖死。公平份额占满但函数自身额度未满 → `ErrGlobalConcurrencyLimit`。
3. **全局额度**：全运行时在途调用不超过 `WithGlobalConcurrency`，占满 →
   `ErrGlobalConcurrencyLimit`。

额度句柄 `permit` 通过 `sync.Once` 保证 `Release` 幂等；并且只有在函数 goroutine
真正结束（正常返回 / panic 恢复 / 超时取消后收尾）之后才归还，因此**任何路径都不会
重复占用或漏放**。注销函数时其账本保留至在途额度全部归还（惰性摘除），归还不会误记
到同名重新注册的新账本上。

## 失败分类

所有失败均为包级哨兵错误，用 `errors.Is` 判定：

| 错误 | 含义 |
| --- | --- |
| `ErrFunctionNotFound` | 函数未注册，或已被 `Unregister`（在途开始后注销不影响已在途调用） |
| `ErrFunctionConcurrencyLimit` | 超过该函数自身的并发上限（瞬时状态，可重试） |
| `ErrGlobalConcurrencyLimit` | 超过全局上限或触发公平份额预留（瞬时状态，可重试） |
| `ErrInvocationTimeout` | 单次调用超过超时（调用选项 > 函数配置 > 运行时默认） |
| `ErrInvocationPanic` | 函数体内 panic，已被 recover 隔离，错误消息附带 panic 值 |
| `ErrShuttingDown` | 停机开始后发起的新调用，或在途调用因停机取消而收尾 |
| `ErrNotRunning` | 尚未 `Start` 或已停机 |
| `ErrShutdownTimeout` | `Shutdown(ctx)` 超过给定截止时间 |
| `ErrInvalidConfig` | 配置非法（名为空、函数为 nil、并发上限 < 1 等） |

> 函数自身 `return err` 的业务错误**原样透传**，不与上述运行时错误混淆。

超时 / panic 不会击穿运行时：执行在独立 goroutine 中进行，超时后立即向调用方返回
`ErrInvocationTimeout`，同时等待函数 goroutine 自然结束后再归还额度并落盘幂等结果，
保证账面与事实一致。

## 幂等语义（至少一次投递下的重试去重）

用 `WithIdempotencyKey(key)` 给调用附带幂等标识：

- **在途去重**：相同 key 的并发调用只有首个（leader）真正执行函数，其余（follower）
  共享同一份结果，`Result.Deduped == true`，不会产生第二次副作用；
- **完成缓存**：leader 完成后，结果在 `WithIdempotencyTTL`（默认 5 分钟）内直接返回，
  `Result.Cached == true`。成功与失败都会缓存——超时 / panic / 下游报错无法确认副作用
  是否已发生，缓存失败可避免"调用方重试 → 重复副作用"，由业务方决定如何处理该失败；
- **TTL 过期**后同 key 允许重新执行；`TTL=0` 表示只去重在途并发、完成结果不缓存；
- **额度拒绝不缓存**：`ErrFunctionConcurrencyLimit` / `ErrGlobalConcurrencyLimit` 属于
  瞬时资源状态，leader 被拒会立即唤醒 follower 返回同一拒绝原因，随后用相同 key 重试
  会重新真正执行。

幂等的作用域是"同一次逻辑请求"。请把 key 设为业务侧稳定标识（订单号、请求 ID 等），
而不是每次随机生成。

## 停机行为

- `Shutdown(ctx)` 开始后状态立即翻转为 `shutting_down`：**新调用一律返回
  `ErrShuttingDown`**，与在途结算严格互斥，不会有 Wait/Add 竞态；
- 同时取消运行时基础 context，向所有在途函数广播取消信号（配合函数响应
  `ctx.Done()` 可快速收尾）；
- 在 `ctx` 截止时间内等待全部在途调用完成并归还额度、落盘幂等结果；
- 超时后返回 `ErrShutdownTimeout`：调用方不再被阻塞，但**在途 goroutine 仍会自然
  结算**，不会泄漏额度或后台单元；
- 停止后可再次 `Start()`（注册表与幂等存储重建），`Start`/`Shutdown` 可安全反复
  调用，对未启动运行时执行 `Shutdown` 是 no-op。

注意：Go 无法强杀 goroutine。如果函数完全无视 `ctx.Done()`，其 goroutine 只能等待
函数自身返回；这属于用户函数契约，文档与类型注释中已明确。

## 可观测性

- `WithLogger(Logger)` 注入日志器；每次调用结束都会打印
  `function`、`idempotency-key`、`verdict`（executed / deduped / cached / rejected /
  timeout / panic / function_error / shutdown_cancelled / caller_cancelled）、
  **全局与函数级并发额度占用**及错误原因。
- `Runtime.Stats()` 返回 `Stats{State, GlobalCapacity, GlobalHeld, InFlight, Functions}`
  快照，可用于健康检查与测试断言。

## 本地验证

```bash
# 常规测试
go test ./... -v

# 数据竞争检测（所有测试必须在 -race 下通过，且无额度泄漏）
go test ./... -race -count=10

go vet ./...
```

测试覆盖：

- 并发争抢与公平调度（慢函数占满时其他函数仍可调用）、函数级 / 全局级拒绝原因区分；
- 单次超时（运行时默认、函数配置、调用选项三级优先级）与额度归还；
- 函数 panic 隔离（连续 panic 后运行时与其他函数仍然可用）；
- 运行期 `Unregister` 不影响在途调用、在途结束后账本正确清理；
- 幂等：并发只执行一次、TTL 缓存与过期重执、`TTL=0`、失败去重、额度拒绝不缓存；
- 停机竞态：大量新调用与 `Shutdown` 并发时每个结果都可归类、无阻塞无 panic；
- 截止时间超时、停机中拒绝新调用、反复启停与重复停机；
- 混合流量压力测试（成功 / 拒绝 / 超时 / panic 同时出现），结束后全局在途与
  函数在途额度全部归零。

单测日志样例：

```
[runtime] invoke function=charge idempotency-key=order-123 verdict=deduped global=0/16 function-held=0
[runtime] invoke function=hog idempotency-key=- verdict=rejected error="global concurrency limit exceeded" global=4/4 function-held=3
stress total=363 ok=... rejected=... timeouts=... panics=... executions=...
```

## 包结构

| 文件 | 职责 |
| --- | --- |
| `runtime.go` | 运行时生命周期、注册表、调用流水线、超时 / panic 隔离、日志与 Stats |
| `scheduler.go` | 两级并发准入器与公平份额、幂等的 `permit` 额度句柄 |
| `dedup.go` | 幂等 leader/follower 去重、TTL 缓存与过期清理 |
| `function.go` | `Function` 类型与 `FunctionConfig` |
| `options.go` | 运行时 / 调用级配置项与默认值 |
| `errors.go` | 失败分类哨兵错误 |
