# local-function-runtime-go

一个零依赖、可被任意 Go 程序直接集成的**本地函数运行时**：把若干处理函数按名称注册进来，
通过统一的 `Invoke` 入口调用。运行时负责并发调度、失败分类、超时/panic 隔离、
幂等去重与优雅停机，调用方只需要面对明确的成功结果或可区分原因的失败。

- 模块路径：`github.com/lisama569/local-function-runtime-go`
- Go 版本：1.25+
- 第三方依赖：无（仅标准库）

## 快速开始

```go
rt := fnruntime.New(fnruntime.Config{
    GlobalConcurrency: 8,           // 全局并发上限，默认 32
    DefaultTimeout:    5 * time.Second, // 单次调用默认超时
    IdempotencyTTL:    5 * time.Minute, // 幂等终态保留时长
})
if err := rt.Start(); err != nil { /* ... */ }

rt.MustRegister("place-order", func(ctx context.Context, payload []byte) ([]byte, error) {
    // 业务逻辑；应当尊重 ctx.Done()，以便在调用超时后及时退出
    return orderID, nil
}, fnruntime.FunctionConfig{MaxConcurrency: 4}) // 该函数独立并发上限

res, err := rt.Invoke(ctx, "place-order", payload,
    fnruntime.WithIdempotencyKey("payment-2026-09-30-0001"), // 开启幂等去重
    fnruntime.WithTimeout(2*time.Second),                    // 覆盖默认超时
)

// 优雅停机：拒绝新调用，等待在途调用结束
shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
_ = rt.Shutdown(shutdownCtx)
```

## 调度与并发模型

两级令牌池，全部采用**非阻塞准入**（通道的 `select default`），超出额度立即返回结构化拒绝，
绝不阻塞调用方、也不做静默排队：

1. **每函数令牌池**：容量为注册时的 `MaxConcurrency`（缺省 1）。函数被大量慢调用占满时，
   超额调用立刻得到 `function_busy`。
2. **全局令牌池**：容量为 `Config.GlobalConcurrency`（缺省 32）。全局耗尽时得到 `global_busy`。

准入顺序为**先函数、后全局**：函数 A 只有在拿到自身令牌后才会申请全局令牌，
因此 A 即使被慢调用打满，也无法囤积全局额度——函数 B、C 仍能正常获得执行机会
（公平性/防互相拖累）。申请全局令牌失败时会回滚已取得的函数令牌，任何拒绝路径都不留残余占用。

额度归还：每个被接纳的调用在独立 goroutine 中执行，结束路径（成功 / 业务错误 / panic /
超时）统一通过 `defer` 先归还全局令牌、再归还函数令牌，恰好一次，不重放、不漏放。
调用方提前放弃等待（取消自己的 context）不会终止执行，也不影响额度归还。

## 失败分类

所有失败都返回 `*CallError`，用 `errors.As` 提取，按 `Reason` 字段区分：

| Reason | 触发场景 |
| --- | --- |
| `runtime_stopped` | 未启动、正在停机（draining）、或已关闭时发起调用 |
| `function_not_found` | 函数未注册，或在运行期间已被 `Unregister` 移除 |
| `function_busy` | 该函数自身并发额度已用尽（瞬时容量原因，不建议重试退避之外缓存） |
| `global_busy` | 全局并发额度已用尽（瞬时容量原因） |
| `timeout` | 调用在 `Timeout` 截止时间内没有完成；handler 因执行 ctx 到期返回的错误也归为该类 |
| `caller_canceled` | 调用方在拿到终态前取消了自己的 context（不影响后台执行） |
| `panic` | 处理函数 panic，已被运行时 recover，运行时本身继续可用 |
| `handler_error` | 处理函数正常 `return err`，底层错误可用 `errors.Is/As` 解包 |
| `invalid_config` | 注册/运行时配置非法（如负并发度） |

`function_busy` / `global_busy` 属于瞬时容量拒绝：在幂等路径下不会被缓存为终态，
携带同一幂等键的后续调用仍可在额度恢复后成功执行。

## 调用生命周期与异常隔离

- **超时**：执行使用独立的 `context`（不继承调用方的取消），以单次调用超时为界。
  终态通过原子指针竞争发布，超时与 handler 同时返回时判定与时序无关。
  超时后调用方立即拿到 `timeout` 并归还额度；若 handler 不响应 ctx，其 goroutine
  会在自然结束后退出，但不会再触碰任何额度。
- **panic**：执行 goroutine 内置 recover，panic 转化为 `panic` 失败；
  单次调用的崩溃不会影响运行时或其他函数。
- **运行期间移除函数**：`Unregister` 后新调用得到 `function_not_found`；
  已在途的调用持有函数注册信息，继续执行、正常交付结果并归还额度。
- 调用方 ctx 与执行 ctx 分离：调用方取消只决定"还要不要等结果"，不杀执行。

## 幂等语义（至少一次投递下的至多一次副作用）

`WithIdempotencyKey(key)` 开启服务端去重，语义为**整轮运行期间同一 key 至多一次真实执行**：

- 首个携带该 key 的调用（leader）真实执行 handler；并发到达的同 key 调用（follower）
  不执行 handler，而是等待并复用 leader 的终态——成功结果与失败终态（含 panic/超时/业务错误）
  一律去重，重试拿到的失败原因与首次完全一致。
- 终态保留 `IdempotencyTTL`（缺省 5 分钟），采用惰性过期清理，无后台清理 goroutine；
  仍在执行中的记录不受 TTL 影响，避免长执行被误清理而产生重复副作用。
- leader 被瞬时容量拒绝时，已在等待的 follower 会收到同样明确的 busy 拒绝（不会挂起），
  占位随即释放；额度恢复后同 key 调用可正常执行。
- 幂等记录保存在内存中，随 `Start` 周期重建；它不替代持久化的精确一次语义，
  但足以保证单进程内"重试不产生重复可观察副作用"。

## 停机行为与生命周期

`Runtime` 可被**反复安全地启动和关闭**：

- `New` 得到未启动实例；`Start` 开始服务。`Shutdown(ctx)` 立即把状态切为 draining，
  新调用得到 `runtime_stopped`，同时等待所有已接纳的在途调用结束。
- 在途调用在 ctx 截止时间内全部结束时，`Shutdown` 返回 `nil`。
- 截止时间到达仍有在途调用时，`Shutdown` 返回 `context.DeadlineExceeded`：
  新调用继续被拒绝，在途调用在后台跑到自身超时/自然结束，其结果依旧通过原等待通道交付，
  额度照常归还；全部收尾完成后运行时发出可复用信号。
- 此后再次 `Start()` 会先等待后台收尾完成，再重建令牌池与幂等表（已注册函数保留），
  返回一个干净的服务周期。`WaitGroup` 计数与状态检查在同一临界区内完成，
  停机接纳之间不存在窗口期竞态。

## 本地验证

```bash
# 常规测试
go test ./... -count=1 -v

# 开启数据竞争检测（验收并发安全与额度归还的关键命令）
go test -race ./... -count=1

# 多轮压测，放大调度/停机竞态
go test -race ./... -count=10

go vet ./...
```

测试覆盖：

- 每函数/全局两级额度争抢、慢函数占满时的公平性、额度归还无泄漏；
- 调用超时、handler panic、业务错误、调用方取消、运行中移除函数；
- 同 key 并发去重、失败终态去重、TTL 过期重执行、busy 不缓存；
- 优雅停机、停机超时后重启、停机与大量调用并发竞态、goroutine 泄漏检查。

单测通过注入的 `Logger` 输出结构化日志，`go test -v` 时可见每次准入的
**并发额度**（`fnConcurrency=n/limit`、`globalConcurrency=n/limit`）、
**幂等标识**（`idempotencyKey=...`）以及**最终判定**（`verdict=success` /
`verdict=failure:<reason>` / `deduped`）。
