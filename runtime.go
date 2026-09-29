package fnruntime

import (
	"context"
	"errors"
	"sync"
	"time"
)

// runtimeState 描述运行时生命周期状态。
type runtimeState int

const (
	stateNew      runtimeState = iota // 已创建，尚未 Start
	stateRunning                      // 正常服务中
	stateDraining                     // 已收到停机请求，拒绝新调用，等待在途完成
	stateClosed                       // 停机截止时间已到，仍有在途调用在后台收尾
)

// registration 是一个已注册函数的运行期表示。
type registration struct {
	name  string
	fn    HandlerFunc
	cfg   FunctionConfig
	slots chan struct{} // 每函数并发令牌，容量即 MaxConcurrency
}

// Runtime 是可被直接集成的本地函数运行时。零值不可用，请通过 New 创建。
type Runtime struct {
	mu sync.Mutex

	state runtimeState

	// regs 在任意状态下都可能被读取（在途调用持有 reg 指针），
	// 仅在 stateNew/stateRunning 下通过 Register/Unregister 变化。
	regs map[string]*registration

	// idem 保存幂等记录，随每次 Start 重建。
	idem *idemStore

	// globalSlots 是全局并发令牌池，随每次 Start 重建。
	globalSlots chan struct{}

	// inFlight 跟踪所有已被接纳、尚未终态的执行单元。
	inFlight sync.WaitGroup

	// drained 在一次停机周期内全部在途执行单元结束后关闭，
	// 供超时停机后的 Start 等待复用。
	drained chan struct{}

	cfg Config
}

// New 创建一个尚未启动的运行时。
func New(cfg Config) *Runtime {
	if cfg.GlobalConcurrency <= 0 {
		cfg.GlobalConcurrency = 32
	}
	if cfg.DefaultTimeout <= 0 {
		cfg.DefaultTimeout = 5 * time.Second
	}
	if cfg.IdempotencyTTL <= 0 {
		cfg.IdempotencyTTL = 5 * time.Minute
	}
	if cfg.Logger == nil {
		cfg.Logger = noopLogger{}
	}
	return &Runtime{
		state:   stateNew,
		regs:    make(map[string]*registration),
		cfg:     cfg,
		drained: make(chan struct{}),
	}
}

// Start 将运行时置为可服务状态。New 之后或一次完整停机之后均可调用；
// 若上一次停机超时、后台仍有在途调用在收尾，Start 会阻塞等待收尾结束后
// 再重新装配，保证运行时可以被安全地反复启动和关闭。
func (r *Runtime) Start() error {
	// 先在锁外判断是否需要等待上一轮超时停机的后台收尾。
	// drained 在构造时即为已可关闭状态，因此全新 Runtime 不会被阻塞。
	if wait := r.drainWait(); wait != nil {
		<-wait
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	switch r.state {
	case stateRunning:
		return errAlreadyStarted
	case stateNew, stateDraining, stateClosed:
		// stateNew：首次启动；
		// stateDraining/stateClosed 出现在等待 drained 之后，
		// 能走到这里说明在途已全部结束，可以开启新一轮。
	}

	r.resetLocked()
	r.state = stateRunning
	r.cfg.Logger.Printf("fnruntime: started (globalConcurrency=%d defaultTimeout=%s idempotencyTTL=%s)",
		r.cfg.GlobalConcurrency, r.cfg.DefaultTimeout, r.cfg.IdempotencyTTL)
	return nil
}

// drainWait 返回需要等待的收尾信号；当前没有停机周期时返回 nil。
// 调用时不能持有 r.mu。
func (r *Runtime) drainWait() <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state == stateNew || r.state == stateRunning {
		return nil
	}
	return r.drained
}

// Shutdown 在截止时间 ctx 内优雅停机：立即把状态切为 draining 并拒绝新调用，
// 随后等待所有在途调用结束。ctx 超时后返回 context.DeadlineExceeded，
// 仍在途的调用在后台继续跑完（其结果已通过调用方的等待通道交付），
// 跑完后关闭 drained；再次 Start 会先等待该信号。
func (r *Runtime) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	switch r.state {
	case stateNew:
		// 从未启动，直接进入已关闭，重建一个干净周期供后续 Start。
		r.state = stateClosed
		r.resetLocked()
		close(r.drained)
		r.mu.Unlock()
		return nil
	case stateDraining, stateClosed:
		wait := r.drained
		r.mu.Unlock()
		<-wait
		return nil
	}

	r.state = stateDraining
	r.mu.Unlock()
	r.cfg.Logger.Printf("fnruntime: shutdown initiated, rejecting new invocations")

	// 用 goroutine + select 等待 WaitGroup，避免 ctx 取消后泄漏等待者：
	// WaitGroup 完成时 done 被关闭，等待 goroutine 随即退出。
	done := make(chan struct{})
	go func() {
		r.inFlight.Wait()
		close(done)
	}()

	var err error
	select {
	case <-done:
		r.mu.Lock()
		r.state = stateClosed
		close(r.drained)
		r.mu.Unlock()
		r.cfg.Logger.Printf("fnruntime: shutdown complete, all in-flight calls finished")
	case <-ctx.Done():
		err = ctx.Err()
		r.mu.Lock()
		r.state = stateClosed
		// 后台收尾结束后补发 drained 信号：用新 goroutine 等待，
		// 关闭后下一轮 Start 即可复用。
		go func() {
			<-done
			r.mu.Lock()
			select {
			case <-r.drained:
				// 已关闭（理论上不会发生），避免重复 close。
			default:
				close(r.drained)
			}
			r.mu.Unlock()
		}()
		r.mu.Unlock()
		r.cfg.Logger.Printf("fnruntime: shutdown deadline exceeded, draining continues in background")
	}
	return err
}

// Register 注册一个处理函数。仅在运行时未启动或运行中可用。
func (r *Runtime) Register(name string, fn HandlerFunc, cfg FunctionConfig) error {
	if name == "" {
		return errEmptyName
	}
	if fn == nil {
		return errNilHandler
	}
	if cfg.MaxConcurrency < 0 {
		return newCallError(name, ReasonInvalidConfig, errors.New("MaxConcurrency must be > 0"))
	}
	if cfg.MaxConcurrency == 0 {
		cfg.MaxConcurrency = 1
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state == stateDraining || r.state == stateClosed {
		return errNotStarted
	}
	if _, exists := r.regs[name]; exists {
		return errDuplicateName
	}
	reg := &registration{
		name:  name,
		fn:    fn,
		cfg:   cfg,
		slots: make(chan struct{}, cfg.MaxConcurrency),
	}
	r.regs[name] = reg
	r.cfg.Logger.Printf("fnruntime: function registered name=%s maxConcurrency=%d", name, cfg.MaxConcurrency)
	return nil
}

// MustRegister 注册失败时 panic，适合启动期装配。
func (r *Runtime) MustRegister(name string, fn HandlerFunc, cfg FunctionConfig) {
	if err := r.Register(name, fn, cfg); err != nil {
		panic(err)
	}
}

// Unregister 移除一个函数：之后的新调用返回 function_not_found，
// 已经在途的调用持有 registration 指针，继续执行并正常归还额度，不受影响。
func (r *Runtime) Unregister(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.regs[name]; !ok {
		return false
	}
	delete(r.regs, name)
	r.cfg.Logger.Printf("fnruntime: function unregistered name=%s", name)
	return true
}

// resetLocked 重建一次启动周期内的可变结构，调用时需持有 mu。
// 已注册的函数集合（regs）在重启之间保留，但每函数令牌池需重建。
func (r *Runtime) resetLocked() {
	r.idem = newIdemStore()
	r.globalSlots = make(chan struct{}, r.cfg.GlobalConcurrency)
	r.drained = make(chan struct{})
	r.inFlight = sync.WaitGroup{}
	for _, reg := range r.regs {
		reg.slots = make(chan struct{}, reg.cfg.MaxConcurrency)
	}
}

var (
	errAlreadyStarted = errors.New("fnruntime: runtime is already started")
	errNotStarted     = errors.New("fnruntime: runtime is not started")
	errDuplicateName  = errors.New("fnruntime: function already registered")
	errEmptyName      = errors.New("fnruntime: function name must not be empty")
	errNilHandler     = errors.New("fnruntime: handler must not be nil")
)
