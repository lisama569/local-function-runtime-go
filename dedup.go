package localfunctionruntime

import (
	"sync"
	"time"
)

// dedupOutcome 一次幂等执行的最终结果（成功值或失败原因）。
type dedupOutcome struct {
	value []byte
	err   error
}

type dedupEntry struct {
	// done 只作为"结果已就绪"的广播信号；结果本身存于 out，供任意多个 follower 重复读取。
	done        chan struct{}
	out         dedupOutcome
	completedAt time.Time
	// discarded 额度拒绝等瞬时失败：广播给在途 follower，但同 key 后续 acquire 立即重新占位。
	discarded bool
}

// dedupStore 幂等记录存储。
//
// 语义：
//   - 相同 key 的首个调用（leader）真正执行函数，其余并发调用（follower）
//     等待同一份结果，不产生第二次执行；
//   - 执行完成后的结果在 TTL 内作为缓存返回（包括失败结果——超时类失败
//     无法确认下游是否已生效，缓存失败可避免重试造成重复副作用）；
//   - TTL 过期后允许再次执行。TTL=0 表示完成结果不做缓存（只去重在途并发）。
type dedupStore struct {
	mu      sync.Mutex
	entries map[string]*dedupEntry
	ttl     time.Duration
	stopCh  chan struct{}
	done    chan struct{}
}

func newDedupStore(ttl time.Duration) *dedupStore {
	return &dedupStore{
		entries: make(map[string]*dedupEntry),
		ttl:     ttl,
	}
}

// start 启动后台过期清理（TTL>0 时）。start/close 可随运行时反复启停。
func (d *dedupStore) start(now func() time.Time) {
	d.stopCh = make(chan struct{})
	d.done = make(chan struct{})
	if d.ttl <= 0 {
		close(d.done)
		return
	}
	tick := d.ttl / 2
	if tick < 10*time.Millisecond {
		tick = 10 * time.Millisecond
	}
	go d.janitor(tick, now)
}

func (d *dedupStore) janitor(tick time.Duration, now func() time.Time) {
	defer close(d.done)
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-d.stopCh:
			return
		case <-t.C:
			d.sweep(now())
		}
	}
}

func (d *dedupStore) sweep(now time.Time) {
	d.mu.Lock()
	for k, e := range d.entries {
		if e.discarded || (!e.completedAt.IsZero() && now.Sub(e.completedAt) >= d.ttl) {
			delete(d.entries, k)
		}
	}
	d.mu.Unlock()
}

// snapshot 读取已完成且未过期的缓存结果。TTL=0 与被拒绝的记录不参与缓存。
func (d *dedupStore) snapshot(key string, now time.Time) (dedupOutcome, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	e, ok := d.entries[key]
	if !ok || e.discarded || e.completedAt.IsZero() || d.ttl == 0 {
		return dedupOutcome{}, false
	}
	if now.Sub(e.completedAt) >= d.ttl {
		delete(d.entries, key)
		return dedupOutcome{}, false
	}
	return e.out, true
}

// acquire 占位或加入等待。
// leader=true：调用方负责执行函数并最终调用 complete/discard；
// leader=false：等待 wait 关闭后调用 waitResult 读取共享结果。
func (d *dedupStore) acquire(key string, now time.Time) (leader bool, wait <-chan struct{}) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e, ok := d.entries[key]; ok {
		expired := e.discarded || (!e.completedAt.IsZero() && (d.ttl == 0 || now.Sub(e.completedAt) >= d.ttl))
		if !expired {
			return false, e.done
		}
	}
	e := &dedupEntry{done: make(chan struct{})}
	d.entries[key] = e
	return true, e.done
}

// waitResult 在 wait 关闭后读取共享结果。
func (d *dedupStore) waitResult(key string) dedupOutcome {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e, ok := d.entries[key]; ok {
		return e.out
	}
	return dedupOutcome{}
}

// complete 由 leader 调用：落盘结果并唤醒全部 follower。
func (d *dedupStore) complete(key string, out dedupOutcome, now time.Time) {
	d.mu.Lock()
	e, ok := d.entries[key]
	if ok {
		e.out = out
		e.completedAt = now
		close(e.done) // 先写 out 再广播，happen-before 保证 follower 读到 out
	}
	d.mu.Unlock()
}

// discard 放弃本次幂等占位（如额度拒绝）：广播拒绝原因给在途 follower，
// 不做缓存，同 key 的下一次 acquire 立即成为新 leader。
func (d *dedupStore) discard(key string, err error) {
	d.mu.Lock()
	if e, ok := d.entries[key]; ok && e.completedAt.IsZero() && !e.discarded {
		e.out = dedupOutcome{err: err}
		e.discarded = true
		close(e.done)
	}
	d.mu.Unlock()
}

// close 停止后台清理。不清空条目：停机超时后仍可能有在途 leader 落盘结果，
// 清空 map 会与之数据竞争；下一次 Start 会创建全新存储，旧存储自然回收。
func (d *dedupStore) close() {
	if d.stopCh != nil {
		close(d.stopCh)
		<-d.done
		d.stopCh = nil
	}
}
