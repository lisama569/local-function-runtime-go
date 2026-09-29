package localfunctionruntime

import (
	"sync"
)

// funcState 单个函数的额度账本。即使函数被注销，在途 permit 仍持有其指针，
// 归还时可安全计数，不影响账本一致性。
type funcState struct {
	cap     int
	held    int
	removed bool
}

// permit 一次准入获得的额度句柄。Release 必须被调用，
// 且内部保证真正的释放逻辑只执行一次（任何路径都不会重复归还或漏放）。
type permit struct {
	once      sync.Once
	releaseFn func()
}

// Release 归还本次准入占用的全部额度（函数级 + 全局级），幂等。
func (p *permit) Release() {
	p.once.Do(func() {
		if p.releaseFn != nil {
			p.releaseFn()
		}
	})
}

// scheduler 两级并发准入器。
//
// 准入顺序（同一把锁内原子完成，不阻塞、不排队）：
//  1. 函数级额度：该函数在途数不得超过其独立上限；
//  2. 公平份额额度：为每个其他活跃函数至少预留 1 个全局槽位，
//     单函数可占上限 = max(1, 全局容量-其他活跃函数数)，防止慢函数占满全局；
//  3. 全局额度：全局在途数不得超过全局容量。
//
// 注销的函数保留账本直到其在途额度全部归还（惰性摘除），期间拒绝一切新准入。
type scheduler struct {
	mu         sync.Mutex
	capacity   int
	globalHeld int
	funcs      map[string]*funcState
}

func newScheduler(globalCapacity int) *scheduler {
	if globalCapacity < 1 {
		globalCapacity = 1
	}
	return &scheduler{
		capacity: globalCapacity,
		funcs:    make(map[string]*funcState),
	}
}

func (s *scheduler) register(name string, maxConcurrency int) *funcState {
	if maxConcurrency < 1 {
		maxConcurrency = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	fs := &funcState{cap: maxConcurrency}
	s.funcs[name] = fs
	return fs
}

// unregister 标记函数已注销：新准入立即收到 ErrFunctionNotFound；
// 账本保留至在途 permit 全部归还后惰性删除，保证额度不漏放。
func (s *scheduler) unregister(name string) {
	s.mu.Lock()
	if fs, ok := s.funcs[name]; ok {
		fs.removed = true
	}
	s.mu.Unlock()
}

// activeFuncsLocked 统计参与公平份额预留的活跃（未注销）函数数。
func (s *scheduler) activeFuncsLocked() int {
	n := 0
	for _, fs := range s.funcs {
		if !fs.removed {
			n++
		}
	}
	return n
}

// tryAdmit 非阻塞地尝试准入。
// 成功：返回 permit，调用方必须在调用结束时 Release。
// 失败：返回 ErrFunctionNotFound / ErrFunctionConcurrencyLimit / ErrGlobalConcurrencyLimit。
func (s *scheduler) tryAdmit(name string) (*permit, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	fs, ok := s.funcs[name]
	if !ok || fs.removed {
		return nil, ErrFunctionNotFound
	}

	// 公平份额：为除自己外的每个活跃函数至少预留 1 个全局槽位。
	fairCap := s.capacity - (s.activeFuncsLocked() - 1)
	if fairCap < 1 {
		fairCap = 1
	}
	effectiveCap := fs.cap
	if fairCap < effectiveCap {
		effectiveCap = fairCap
	}

	if fs.held >= fs.cap {
		return nil, ErrFunctionConcurrencyLimit
	}
	if fs.held >= effectiveCap {
		// 函数自身额度未满，但公平份额（为其他函数预留槽位）已占满。
		return nil, ErrGlobalConcurrencyLimit
	}
	if s.globalHeld >= s.capacity {
		return nil, ErrGlobalConcurrencyLimit
	}

	fs.held++
	s.globalHeld++
	return &permit{releaseFn: func() {
		s.mu.Lock()
		fs.held--
		s.globalHeld--
		if fs.removed && fs.held == 0 {
			// 仅当账本仍指向本条目时才摘除，避免误删同名重新注册后的新账本。
			if cur, ok := s.funcs[name]; ok && cur == fs {
				delete(s.funcs, name)
			}
		}
		s.mu.Unlock()
	}}, nil
}

type schedulerStats struct {
	GlobalCapacity int
	GlobalHeld     int
	Functions      map[string]FunctionStats
}

func (s *scheduler) stats() schedulerStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	fns := make(map[string]FunctionStats, len(s.funcs))
	for name, fs := range s.funcs {
		fns[name] = FunctionStats{Registered: !fs.removed, Capacity: fs.cap, Held: fs.held}
	}
	return schedulerStats{
		GlobalCapacity: s.capacity,
		GlobalHeld:     s.globalHeld,
		Functions:      fns,
	}
}

func (s *scheduler) held(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if fs, ok := s.funcs[name]; ok {
		return fs.held
	}
	return 0
}
