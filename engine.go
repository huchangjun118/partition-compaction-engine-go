package engine

import (
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// obsoleteFile 记录已从清单摘除、等待所有读者释放后物理删除的段文件。
type obsoleteFile struct {
	id   string
	path string
}

// task 是提交给后台 worker 的工作单元。
type task struct {
	id   string
	kind string // "compact" | "migrate" | "expire"
	pk   PartitionKey
	seg  string // migrate 专用：段 ID
}

// Engine 是本地分区存储引擎。
type Engine struct {
	dir   string
	store *manifestStore
	nowFn atomic.Value // func() time.Time，原子存取便于测试注入时钟

	mu       sync.Mutex
	cfg      Config
	m        *manifest
	refs     map[string]int // 段 ID -> 活跃读者数
	obsolete []obsoleteFile
	closed   bool

	stopCh  chan struct{}
	wg      sync.WaitGroup
	workCh  chan task
	partBsy map[PartitionKey]bool // 某分区是否有合并任务在途
	migrBsy map[string]bool       // 某段是否有迁移任务在途

	tasksMu sync.Mutex
	tasks   []*TaskInfo
	taskSeq int
}

// Open 打开（或创建）引擎，并执行崩溃恢复：
// 清理临时文件与孤儿段文件，校验清单引用的文件都存在。
func Open(dir string, cfg Config) (*Engine, error) {
	cfg = cfg.withDefaults()
	store := newManifestStore(filepath.Join(dir, "manifest.json"))
	m, err := store.load()
	if err != nil {
		return nil, fmt.Errorf("load manifest: %w", err)
	}
	e := &Engine{
		dir:     dir,
		store:   store,
		cfg:     cfg,
		m:       m,
		refs:    map[string]int{},
		stopCh:  make(chan struct{}),
		workCh:  make(chan task, 1024),
		partBsy: map[PartitionKey]bool{},
		migrBsy: map[string]bool{},
	}
	e.nowFn.Store(time.Now)
	if err := e.recover(); err != nil {
		return nil, err
	}
	for i := 0; i < cfg.CompactionConcurrency; i++ {
		e.wg.Add(1)
		go e.worker()
	}
	e.wg.Add(1)
	go e.scheduler()
	return e, nil
}

// recover 启动恢复：删除临时文件与未被清单引用的孤儿段文件。
func (e *Engine) recover() error {
	live := map[string]bool{}
	for _, s := range e.m.Segments {
		live[s.ID+".seg"] = true
	}
	for _, dir := range []string{e.hotDir(), e.coldDir()} {
		entries, err := os.ReadDir(dir)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		for _, ent := range entries {
			name := ent.Name()
			if strings.HasSuffix(name, ".tmp") {
				os.Remove(filepath.Join(dir, name))
				continue
			}
			if strings.HasSuffix(name, ".seg") && !live[name] {
				os.Remove(filepath.Join(dir, name))
			}
		}
	}
	// 清单引用的文件必须存在，否则视为损坏。
	for _, s := range e.m.Segments {
		if _, err := os.Stat(e.segmentPath(s)); err != nil {
			return fmt.Errorf("segment %s referenced by manifest is missing: %w", s.ID, err)
		}
	}
	return nil
}

// now 返回当前时钟（可被测试替换）。
func (e *Engine) now() time.Time { return e.nowFn.Load().(func() time.Time)() }

// setNow 替换时钟，仅供测试使用。
func (e *Engine) setNow(f func() time.Time) { e.nowFn.Store(f) }

func (e *Engine) hotDir() string  { return filepath.Join(e.dir, "data", "hot") }
func (e *Engine) coldDir() string { return filepath.Join(e.dir, "data", "cold") }

func (e *Engine) segmentPath(s *SegmentMeta) string {
	if s.Tier == TierCold {
		return filepath.Join(e.coldDir(), s.ID+".seg")
	}
	return filepath.Join(e.hotDir(), s.ID+".seg")
}

func keyBucketOf(cfg Config, key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(cfg.KeyBuckets))
}

func timeBucketOf(cfg Config, ts int64) int64 {
	w := cfg.TimeBucketWindow.Milliseconds()
	return ts / w
}

// Close 关闭引擎，停止后台任务。
func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.mu.Unlock()
	close(e.stopCh)
	e.wg.Wait()
	return nil
}

// SetConfig 更新配置，只影响后续调度，不影响已落盘数据。
func (e *Engine) SetConfig(cfg Config) {
	e.mu.Lock()
	e.cfg = cfg.withDefaults()
	e.mu.Unlock()
}

func (e *Engine) getConfig() Config {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.cfg
}

// commitLocked 在持有 e.mu 时原子推进清单版本。
func (e *Engine) commitLocked(mutate func(m *manifest)) error {
	next := e.m.clone()
	mutate(next)
	next.Version = e.m.Version + 1
	if err := e.store.commit(next); err != nil {
		return err
	}
	e.m = next
	return nil
}

// allocSegmentIDs 预留一批段 ID。
func (e *Engine) allocSegmentIDs(n int) (uint64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return 0, fmt.Errorf("engine closed")
	}
	base := e.m.NextID
	if err := e.commitLocked(func(m *manifest) { m.NextID += uint64(n) }); err != nil {
		return 0, err
	}
	return base, nil
}

// Write 写入一批记录。按分区排序后每组落一个段文件，再一次清单提交发布。
func (e *Engine) Write(records []Record) error {
	if len(records) == 0 {
		return nil
	}
	cfg := e.getConfig()
	groups := map[PartitionKey][]Record{}
	for _, r := range records {
		pk := PartitionKey{TimeBucket: timeBucketOf(cfg, r.Timestamp), KeyBucket: keyBucketOf(cfg, r.Key)}
		groups[pk] = append(groups[pk], r)
	}
	base, err := e.allocSegmentIDs(len(groups))
	if err != nil {
		return err
	}
	metas := make([]*SegmentMeta, 0, len(groups))
	i := uint64(0)
	for pk, recs := range groups {
		sort.Slice(recs, func(a, b int) bool {
			if recs[a].Key != recs[b].Key {
				return recs[a].Key < recs[b].Key
			}
			return recs[a].Timestamp < recs[b].Timestamp
		})
		meta := SegmentMeta{
			ID:        fmt.Sprintf("seg-%d", base+i),
			Partition: pk,
			Tier:      TierHot,
			CreatedAt: e.now().UnixMilli(),
		}
		i++
		written, err := writeSegment(e.hotDir(), meta, recs)
		if err != nil {
			return err
		}
		m := written
		metas = append(metas, &m)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return fmt.Errorf("engine closed")
	}
	return e.commitLocked(func(m *manifest) {
		m.Segments = append(m.Segments, metas...)
	})
}

// acquireSnapshot 固定当前清单版本并为每个段计入读者引用。
func (e *Engine) acquireSnapshot() (*manifest, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, fmt.Errorf("engine closed")
	}
	snap := e.m.clone()
	for _, s := range snap.Segments {
		e.refs[s.ID]++
	}
	return snap, nil
}

// releaseSnapshot 释放引用，并物理删除不再被任何读者引用的废弃段文件。
func (e *Engine) releaseSnapshot(snap *manifest) {
	e.mu.Lock()
	for _, s := range snap.Segments {
		e.refs[s.ID]--
		if e.refs[s.ID] <= 0 {
			delete(e.refs, s.ID)
		}
	}
	e.mu.Unlock()
	e.sweepObsolete()
}

// sweepObsolete 物理删除所有已不被读者引用的废弃段文件。
func (e *Engine) sweepObsolete() {
	e.mu.Lock()
	var deletable []string
	kept := e.obsolete[:0]
	for _, ob := range e.obsolete {
		if e.refs[ob.id] == 0 {
			deletable = append(deletable, ob.path)
		} else {
			kept = append(kept, ob)
		}
	}
	e.obsolete = kept
	e.mu.Unlock()
	for _, p := range deletable {
		removeSegmentFile(p)
	}
}

// markObsoleteLocked 将段文件标记为废弃，无读者引用时由 releaseSnapshot 删除。
func (e *Engine) markObsoleteLocked(segs ...*SegmentMeta) {
	for _, s := range segs {
		e.obsolete = append(e.obsolete, obsoleteFile{id: s.ID, path: e.segmentPath(s)})
	}
}

// Query 按条件查询。先做分区裁剪与段级 min/max 裁剪，再逐行过滤，
// 结果与全量扫描逐行一致。
func (e *Engine) Query(q Query) ([]Record, error) {
	snap, err := e.acquireSnapshot()
	if err != nil {
		return nil, err
	}
	defer e.releaseSnapshot(snap)

	cfg := e.getConfig()
	keySet := map[string]bool{}
	buckets := map[int]bool{}
	var minKey, maxKey string
	for i, k := range q.Keys {
		keySet[k] = true
		buckets[keyBucketOf(cfg, k)] = true
		if i == 0 || k < minKey {
			minKey = k
		}
		if i == 0 || k > maxKey {
			maxKey = k
		}
	}

	var out []Record
	for _, seg := range snap.Segments {
		if !segmentMayMatch(cfg, seg, q, buckets, minKey, maxKey) {
			continue
		}
		recs, err := readSegment(e.segmentPath(seg))
		if err != nil {
			return nil, err
		}
		for _, r := range recs {
			if r.Timestamp < q.StartTS || r.Timestamp > q.EndTS {
				continue
			}
			if len(keySet) > 0 && !keySet[r.Key] {
				continue
			}
			out = append(out, r)
		}
	}
	sort.Slice(out, func(a, b int) bool {
		if out[a].Key != out[b].Key {
			return out[a].Key < out[b].Key
		}
		if out[a].Timestamp != out[b].Timestamp {
			return out[a].Timestamp < out[b].Timestamp
		}
		return string(out[a].Value) < string(out[b].Value)
	})
	return out, nil
}

// segmentMayMatch 分区裁剪 + 段级 min/max 裁剪。
func segmentMayMatch(cfg Config, seg *SegmentMeta, q Query, buckets map[int]bool, minKey, maxKey string) bool {
	// 分区时间桶裁剪
	tb := seg.Partition.TimeBucket
	if tb < timeBucketOf(cfg, q.StartTS) || tb > timeBucketOf(cfg, q.EndTS) {
		return false
	}
	// 分区主键桶裁剪
	if len(buckets) > 0 && !buckets[seg.Partition.KeyBucket] {
		return false
	}
	// 段级时间范围裁剪
	if seg.MaxTS < q.StartTS || seg.MinTS > q.EndTS {
		return false
	}
	// 段级主键范围裁剪
	if len(buckets) > 0 && (seg.MaxKey < minKey || seg.MinKey > maxKey) {
		return false
	}
	return true
}

// Tasks 返回后台任务状态快照。
func (e *Engine) Tasks() []TaskInfo {
	e.tasksMu.Lock()
	defer e.tasksMu.Unlock()
	out := make([]TaskInfo, len(e.tasks))
	for i, t := range e.tasks {
		out[i] = *t
	}
	return out
}

func (e *Engine) newTaskLocked(kind string, pk PartitionKey) *TaskInfo {
	e.taskSeq++
	t := &TaskInfo{
		ID:        fmt.Sprintf("task-%d", e.taskSeq),
		Partition: pk,
		Kind:      kind,
		State:     TaskPending,
		CreatedAt: e.now(),
	}
	e.tasks = append(e.tasks, t)
	return t
}

func (e *Engine) finishTask(t *TaskInfo, err error) {
	e.tasksMu.Lock()
	defer e.tasksMu.Unlock()
	t.EndedAt = e.now()
	if err != nil {
		t.State = TaskFailed
		t.Err = err.Error()
	} else {
		t.State = TaskDone
	}
}

func (e *Engine) setTaskRunning(t *TaskInfo) {
	e.tasksMu.Lock()
	t.State = TaskRunning
	e.tasksMu.Unlock()
}

// scheduler 周期调度后台工作。
func (e *Engine) scheduler() {
	defer e.wg.Done()
	cfg := e.getConfig()
	tick := time.NewTicker(cfg.ScheduleInterval)
	defer tick.Stop()
	for {
		select {
		case <-e.stopCh:
			return
		case <-tick.C:
			for _, t := range e.schedule() {
				select {
				case e.workCh <- t:
				case <-e.stopCh:
					return
				}
			}
		}
	}
}

// worker 消费后台任务。
func (e *Engine) worker() {
	defer e.wg.Done()
	for {
		select {
		case <-e.stopCh:
			return
		case t := <-e.workCh:
			e.execute(t)
		}
	}
}

// schedule 扫描当前清单，返回需要执行的任务（合并/迁移/淘汰）。
// 被选中执行的分区/段会被标记为在途，避免重复调度。
func (e *Engine) schedule() []task {
	e.mu.Lock()
	defer e.mu.Unlock()
	cfg := e.cfg
	now := e.now()
	var tasks []task

	// 1. 小文件合并：分区内段数达到阈值
	byPart := map[PartitionKey]int{}
	for _, s := range e.m.Segments {
		byPart[s.Partition]++
	}
	for pk, n := range byPart {
		if n >= cfg.CompactionThreshold && !e.partBsy[pk] {
			e.partBsy[pk] = true
			tasks = append(tasks, task{id: e.mustNewTask("compact", pk).ID, kind: "compact", pk: pk})
		}
	}
	// 2. 分层迁移：热层段超过 ColdAfter
	if cfg.ColdAfter > 0 {
		for _, s := range e.m.Segments {
			if s.Tier == TierHot && !e.migrBsy[s.ID] &&
				now.Sub(time.UnixMilli(s.CreatedAt)) > cfg.ColdAfter {
				e.migrBsy[s.ID] = true
				tasks = append(tasks, task{id: e.mustNewTask("migrate", s.Partition).ID, kind: "migrate", pk: s.Partition, seg: s.ID})
			}
		}
	}
	// 3. 保留期淘汰：整段超出保留窗口
	if cfg.RetentionWindow > 0 {
		cutoff := now.Add(-cfg.RetentionWindow).UnixMilli()
		expired := false
		for _, s := range e.m.Segments {
			if s.MaxTS < cutoff {
				expired = true
				break
			}
		}
		if expired {
			tasks = append(tasks, task{id: e.mustNewTask("expire", PartitionKey{}).ID, kind: "expire"})
		}
	}
	return tasks
}

// mustNewTask 在持有 e.mu 时登记任务。
func (e *Engine) mustNewTask(kind string, pk PartitionKey) *TaskInfo {
	e.tasksMu.Lock()
	defer e.tasksMu.Unlock()
	return e.newTaskLocked(kind, pk)
}

// execute 执行一个后台任务并更新其状态。
func (e *Engine) execute(t task) {
	ti := e.findTask(t.id)
	if ti == nil {
		return
	}
	e.setTaskRunning(ti)
	var err error
	switch t.kind {
	case "compact":
		err = runCompaction(e, t.pk)
		e.mu.Lock()
		delete(e.partBsy, t.pk)
		e.mu.Unlock()
	case "migrate":
		err = runMigration(e, t.seg)
		e.mu.Lock()
		delete(e.migrBsy, t.seg)
		e.mu.Unlock()
	case "expire":
		err = runExpiration(e)
	}
	e.finishTask(ti, err)
}

func (e *Engine) findTask(id string) *TaskInfo {
	e.tasksMu.Lock()
	defer e.tasksMu.Unlock()
	for _, t := range e.tasks {
		if t.ID == id {
			return t
		}
	}
	return nil
}

// CompactNow 同步执行一轮调度（合并/迁移/淘汰），主要用于测试与手动触发。
func (e *Engine) CompactNow() {
	for _, t := range e.schedule() {
		e.execute(t)
	}
}
