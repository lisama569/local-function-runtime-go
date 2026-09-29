package fnruntime

import "time"

// idemRecord 是同一幂等标识下调用的终态记录。
//
// 首个携带该标识的调用（leader）负责执行处理函数，并在终态时写入
// outcome、关闭 done；并发到达的重复调用（follower）在 done 上等待并
// 复用同一终态，因此整个运行时内同一幂等标识至多产生一次真实执行。
type idemRecord struct {
	done     chan struct{}
	outcome  *outcome
	expireAt time.Time
}

// finished 判断记录是否已经进入终态。
func (rec *idemRecord) finished() bool {
	select {
	case <-rec.done:
		return true
	default:
		return false
	}
}

// idemStore 保存幂等标识 -> 终态记录。
// map 本身只在持有 runtime.mu 时访问，因此这里不再加锁。
type idemStore struct {
	records map[string]*idemRecord
}

func newIdemStore() *idemStore {
	return &idemStore{records: make(map[string]*idemRecord)}
}

// get 返回未过期的记录。
//
//   - 记录不存在：返回 false；
//   - 记录仍在执行（leader 尚未终态）：无论时间一律返回，follower 必须加入等待，
//     否则长执行可能被 TTL 误清理而触发重复副作用；
//   - 记录已终态且超过 expireAt：惰性删除并返回 false。
func (s *idemStore) get(key string, now time.Time) (*idemRecord, bool) {
	rec, ok := s.records[key]
	if !ok {
		return nil, false
	}
	if rec.finished() && !now.Before(rec.expireAt) {
		delete(s.records, key)
		return nil, false
	}
	return rec, true
}

// deleteExpired 清理所有已终态且过期的记录；在途记录保留。
func (s *idemStore) deleteExpired(now time.Time) {
	for k, rec := range s.records {
		if rec.finished() && !now.Before(rec.expireAt) {
			delete(s.records, k)
		}
	}
}
