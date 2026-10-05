package engine

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// ErrClosed 表示引擎已关闭。
var ErrClosed = errors.New("engine: closed")

// obsoleteFile 记录已被清单移除、等待安全删除的文件。
type obsoleteFile struct {
	meta      FileMeta
	removedAt uint64 // 从该清单版本起不再可见
}

// Engine 是分区存储引擎。
//
// 一致性语义：清单版本不可变，查询先获取当前版本的快照（引用计数），
// 后台的合并/迁移/淘汰通过原子清单切换生效，被移除的文件只有在
// 不再被任何活跃快照引用后才会真正删除。因此读取方在任一时刻看到的
// 都是某个完整一致的版本，不会看到中间状态。
type Engine struct {
	mu   sync.RWMutex
	cfg  Config
	root string

	manifest *Manifest
	ms       *manifestStore
	js       *journalStore

	snaps         map[uint64]int // 活跃快照：版本 -> 引用数
	pendingDelete []obsoleteFile

	sched  *scheduler
	closed bool
	nowFn  func() int64 // 时钟，测试可注入
}

// Open 打开或创建引擎，并执行崩溃恢复。
func Open(root string, cfg Config) (*Engine, error) {
	cfg = cfg.withDefaults()
	e := &Engine{
		cfg:   cfg,
		root:  root,
		ms:    newManifestStore(filepath.Join(root, "manifest")),
		js:    newJournalStore(filepath.Join(root, "journal")),
		snaps: map[uint64]int{},
		nowFn: func() int64 { return time.Now().UnixNano() },
	}
	for _, d := range []string{string(TierHot), string(TierCold)} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			return nil, err
		}
	}
	m, err := e.ms.load()
	if err != nil {
		return nil, err
	}
	e.manifest = m
	if err := e.recover(); err != nil {
		return nil, err
	}
	e.sched = newScheduler(e)
	e.sched.start()
	return e, nil
}

// recover 重放操作日志并清扫孤儿文件。
func (e *Engine) recover() error {
	entries, err := e.js.list()
	if err != nil {
		return err
	}
	for _, ent := range entries {
		committed := ent.Committed
		if !committed && len(ent.Add) > 0 {
			// 崩溃可能发生在清单提交之后、journal 标记完成之前：
			// 若清单已包含全部新增文件，视为已提交，做前滚。
			allIn := true
			for _, a := range ent.Add {
				if _, ok := e.manifest.Files[a.ID]; !ok {
					allIn = false
					break
				}
			}
			committed = allIn
		}
		if committed {
			for _, r := range ent.Remove {
				os.Remove(filepath.Join(e.root, r.Path))
			}
		} else {
			for _, a := range ent.Add {
				os.Remove(filepath.Join(e.root, a.Path))
			}
		}
		_ = e.js.remove(ent.OpID)
	}
	return e.sweepOrphans()
}

// sweepOrphans 删除不在清单中的数据文件与临时文件。
func (e *Engine) sweepOrphans() error {
	known := make(map[string]bool, len(e.manifest.Files))
	for _, m := range e.manifest.Files {
		known[m.Path] = true
	}
	for _, tier := range []string{string(TierHot), string(TierCold)} {
		base := filepath.Join(e.root, tier)
		err := filepath.Walk(base, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return nil
			}
			rel, rerr := filepath.Rel(e.root, path)
			if rerr != nil {
				return nil
			}
			if strings.HasSuffix(path, ".tmp") || !known[rel] {
				os.Remove(path)
			}
			return nil
		})
		if err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// Close 停止后台任务并释放资源，可重复调用。
func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.mu.Unlock()
	e.sched.stop()
	return nil
}

func (e *Engine) config() Config {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.cfg
}

// SetConfig 更新配置，仅影响后续调度，已落盘数据不受影响。
func (e *Engine) SetConfig(cfg Config) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.cfg = cfg.withDefaults()
}

// Write 持久化写入一批事件：按分区分组，每组写一个小文件，
// 然后一次清单提交使整批数据原子可见。
func (e *Engine) Write(events ...Event) error {
	if len(events) == 0 {
		return nil
	}
	cfg := e.config()
	byPart := map[string][]Event{}
	for _, ev := range events {
		p := partitionOf(ev.ID, ev.Timestamp, cfg)
		byPart[p] = append(byPart[p], ev)
	}
	var metas []FileMeta
	for p, evs := range byPart {
		meta, err := writeEventFile(e.root, p, TierHot, evs)
		if err != nil {
			for _, m := range metas { // 清理已写出的孤儿文件
				os.Remove(filepath.Join(e.root, m.Path))
			}
			return err
		}
		metas = append(metas, meta)
	}
	return e.applyChange("write", metas, nil)
}

// applyChange 是统一的清单变更提交路径：journal(未提交) → 清单提交 →
// journal(已提交) → 登记待删除文件并尝试回收。
func (e *Engine) applyChange(kind string, add, remove []FileMeta) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrClosed
	}
	nm := e.manifest.clone()
	for _, r := range remove {
		delete(nm.Files, r.ID)
	}
	for _, a := range add {
		nm.Files[a.ID] = a
	}
	nm.Version++
	op := journalEntry{OpID: newFileID(), Kind: kind, Add: add, Remove: remove}
	if err := e.js.write(op); err != nil {
		return err
	}
	if err := e.ms.commit(nm); err != nil {
		return err
	}
	e.manifest = nm
	op.Committed = true
	if err := e.js.write(op); err == nil {
		_ = e.js.remove(op.OpID)
	}
	for _, r := range remove {
		e.pendingDelete = append(e.pendingDelete, obsoleteFile{meta: r, removedAt: nm.Version})
	}
	e.gcLocked()
	return nil
}

// acquire 获取当前清单版本的快照。
func (e *Engine) acquire() (*Manifest, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, ErrClosed
	}
	v := e.manifest
	e.snaps[v.Version]++
	return v, nil
}

func (e *Engine) release(m *Manifest) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.snaps[m.Version] > 1 {
		e.snaps[m.Version]--
	} else {
		delete(e.snaps, m.Version)
	}
	e.gcLocked()
}

// gcLocked 删除不再被任何活跃快照引用的废弃文件。
func (e *Engine) gcLocked() {
	minActive := uint64(math.MaxUint64)
	for v, n := range e.snaps {
		if n > 0 && v < minActive {
			minActive = v
		}
	}
	keep := e.pendingDelete[:0]
	for _, ob := range e.pendingDelete {
		if len(e.snaps) == 0 || ob.removedAt <= minActive {
			os.Remove(filepath.Join(e.root, ob.meta.Path))
		} else {
			keep = append(keep, ob)
		}
	}
	e.pendingDelete = keep
}

// Query 按条件查询，结果按 (Timestamp, ID) 排序，与全量扫描逐行一致。
func (e *Engine) Query(q Query) ([]Event, error) {
	snap, err := e.acquire()
	if err != nil {
		return nil, err
	}
	defer e.release(snap)

	cfg := e.config()
	// 分区级裁剪
	parts := map[string]bool{}
	for _, m := range snap.Files {
		parts[m.Partition] = true
	}
	var names []string
	for p := range parts {
		names = append(names, p)
	}
	keep := map[string]bool{}
	for _, p := range prunePartitions(names, q, cfg) {
		keep[p] = true
	}
	keySet := map[string]bool{}
	for _, k := range q.Keys {
		keySet[k] = true
	}
	var out []Event
	for _, m := range snap.Files {
		if !keep[m.Partition] {
			continue
		}
		// 文件级时间裁剪
		if m.MaxTS < q.Start || m.MinTS >= q.End {
			continue
		}
		events, err := readEventFile(e.root, m)
		if err != nil {
			return nil, err
		}
		for _, ev := range events {
			if ev.Timestamp < q.Start || ev.Timestamp >= q.End {
				continue
			}
			if len(keySet) > 0 && !keySet[ev.ID] {
				continue
			}
			out = append(out, ev)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Timestamp != out[j].Timestamp {
			return out[i].Timestamp < out[j].Timestamp
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}

// Status 返回各分区后台任务状态。
func (e *Engine) Status() []PartitionStatus { return e.sched.status() }

// CompactNow 手动触发一次合并调度（测试用）。
func (e *Engine) CompactNow() { e.sched.kick() }

// SweepNow 手动触发一次保留期淘汰与分层迁移（测试用）。
func (e *Engine) SweepNow() { e.sched.sweepNow() }

// partitionFileCounts 统计每个分区当前的文件数（供调度与观测使用）。
func (e *Engine) partitionFileCounts() map[string]int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := map[string]int{}
	for _, m := range e.manifest.Files {
		out[m.Partition]++
	}
	return out
}
