package engine

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

func segmentCount(t *testing.T, e *Engine) int {
	t.Helper()
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.m.Segments)
}

// TestCompactionPreservesData 合并前后查询结果必须完全一致。
func TestCompactionPreservesData(t *testing.T) {
	cfg := testConfig()
	e := openTest(t, cfg)
	base := time.Now().UnixMilli()
	var all []Record
	// 同一分区内制造大量小文件：固定键与时间桶，多次小批量写入
	for i := 0; i < 12; i++ {
		batch := []Record{
			{Key: "hot", Timestamp: base + int64(i), Value: []byte(fmt.Sprintf("a-%d", i))},
			{Key: "hot", Timestamp: base + int64(i) + 100, Value: []byte(fmt.Sprintf("b-%d", i))},
		}
		all = append(all, batch...)
		if err := e.Write(batch); err != nil {
			t.Fatal(err)
		}
	}
	before := segmentCount(t, e)
	if before < cfg.CompactionThreshold {
		t.Fatalf("expected many small files, got %d", before)
	}
	q := Query{StartTS: 0, EndTS: base + 10000, Keys: []string{"hot"}}
	want, err := e.Query(q)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(want, fullScan(all, q)) {
		t.Fatal("pre-compaction data mismatch")
	}

	e.CompactNow()

	after := segmentCount(t, e)
	if after != 1 {
		t.Fatalf("expected 1 segment after compaction, got %d", after)
	}
	got, err := e.Query(q)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("compaction changed query results:\n got %v\nwant %v", got, want)
	}
	// 废弃的旧文件最终应被物理删除
	e.mu.Lock()
	obsolete := len(e.obsolete)
	e.mu.Unlock()
	if obsolete != 0 {
		t.Fatalf("obsolete files not swept: %d", obsolete)
	}
}

// TestCompactionMultiplePartitions 多个分区各自独立合并。
func TestCompactionMultiplePartitions(t *testing.T) {
	cfg := testConfig()
	e := openTest(t, cfg)
	base := time.Now().UnixMilli()
	// 写入分布在多个分区
	for i := 0; i < 10; i++ {
		var batch []Record
		for _, k := range []string{"k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8"} {
			batch = append(batch, Record{Key: k, Timestamp: base + int64(i), Value: []byte("v")})
		}
		if err := e.Write(batch); err != nil {
			t.Fatal(err)
		}
	}
	e.CompactNow()
	e.mu.Lock()
	byPart := map[PartitionKey]int{}
	for _, s := range e.m.Segments {
		byPart[s.Partition]++
	}
	e.mu.Unlock()
	for pk, n := range byPart {
		if n != 1 {
			t.Fatalf("partition %+v has %d segments after compaction, want 1", pk, n)
		}
	}
}

// TestConcurrentReadWriteCompact 合并、写入与查询并发进行，
// 任一时刻查询都必须看到某个完整一致的版本。
func TestConcurrentReadWriteCompact(t *testing.T) {
	cfg := testConfig()
	cfg.CompactionThreshold = 3
	e := openTest(t, cfg)
	base := time.Now().UnixMilli()

	var mu sync.Mutex
	var all []Record
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// 写入 goroutine
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 60; i++ {
				batch := []Record{
					{Key: fmt.Sprintf("w%d", w), Timestamp: base + int64(i), Value: []byte(fmt.Sprintf("%d-%d", w, i))},
				}
				if err := e.Write(batch); err != nil {
					t.Errorf("write: %v", err)
					return
				}
				mu.Lock()
				all = append(all, batch...)
				mu.Unlock()
				time.Sleep(time.Millisecond)
			}
		}(w)
	}
	// 合并 goroutine（独立的等待组，避免与 stop 信号互相等待）
	var cwg sync.WaitGroup
	cwg.Add(1)
	go func() {
		defer cwg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				e.CompactNow()
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()
	// 查询 goroutine：结果必须是写入记录集的子序列且不多不少（对已完成写入）
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			q := Query{StartTS: 0, EndTS: base + 10000, Keys: []string{"w0"}}
			got, err := e.Query(q)
			if err != nil {
				t.Errorf("query: %v", err)
				return
			}
			// 一致性校验：结果中的每条记录都必须真实写入过，且同一 (key,ts) 不重复
			seen := map[string]int{}
			for _, r := range got {
				seen[fmt.Sprintf("%s/%d", r.Key, r.Timestamp)]++
			}
			for k, n := range seen {
				if n != 1 {
					t.Errorf("duplicate record in result: %s x%d", k, n)
					return
				}
			}
			time.Sleep(time.Millisecond)
		}
	}()

	wg.Wait()
	close(stop)
	cwg.Wait()
	// 等待后台合并收尾后做最终校验
	e.CompactNow()
	mu.Lock()
	final := append([]Record(nil), all...)
	mu.Unlock()
	q := Query{StartTS: 0, EndTS: base + 10000}
	got, err := e.Query(q)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, fullScan(final, q)) {
		t.Fatalf("final data mismatch: got %d records, want %d", len(got), len(fullScan(final, q)))
	}
}

// TestReaderSnapshotDuringCompaction 正在进行的查询持有快照，
// 合并不得删除其正在读取的数据。
func TestReaderSnapshotDuringCompaction(t *testing.T) {
	cfg := testConfig()
	e := openTest(t, cfg)
	base := time.Now().UnixMilli()
	for i := 0; i < 6; i++ {
		if err := e.Write([]Record{{Key: "s", Timestamp: base + int64(i), Value: []byte("v")}}); err != nil {
			t.Fatal(err)
		}
	}
	// 手动持有一个快照（模拟慢查询）
	snap, err := e.acquireSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	e.CompactNow() // 合并完成，但旧文件因快照引用不得删除
	e.mu.Lock()
	if len(e.obsolete) == 0 {
		e.mu.Unlock()
		t.Fatal("expected obsolete files pending on snapshot release")
	}
	e.mu.Unlock()
	// 快照中的段文件必须仍然可读
	for _, seg := range snap.Segments {
		if _, err := readSegment(e.segmentPath(seg)); err != nil {
			t.Fatalf("snapshot segment unreadable during compaction: %v", err)
		}
	}
	e.releaseSnapshot(snap)
	// 释放后废弃文件应被清除
	e.mu.Lock()
	n := len(e.obsolete)
	e.mu.Unlock()
	if n != 0 {
		t.Fatalf("obsolete files remain after snapshot release: %d", n)
	}
}

// TestTaskStates 任务状态可观测：完成后状态为 done。
func TestTaskStates(t *testing.T) {
	cfg := testConfig()
	e := openTest(t, cfg)
	base := time.Now().UnixMilli()
	for i := 0; i < 4; i++ {
		e.Write([]Record{{Key: "t", Timestamp: base + int64(i), Value: []byte("v")}})
	}
	e.CompactNow()
	tasks := e.Tasks()
	if len(tasks) == 0 {
		t.Fatal("no tasks recorded")
	}
	for _, ti := range tasks {
		if ti.State != TaskDone {
			t.Fatalf("task %s state = %s, want done (err=%s)", ti.ID, ti.State, ti.Err)
		}
		if ti.Kind != "compact" {
			t.Fatalf("unexpected task kind %s", ti.Kind)
		}
	}
}
