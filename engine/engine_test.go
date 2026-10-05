package engine

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"
)

func testConfig(root string) Config {
	return Config{
		TimeBucket:       time.Hour,
		HashBuckets:      4,
		CompactThreshold: 4,
		CompactWorkers:   2,
		SweepInterval:    time.Hour, // 测试中一律手动触发
	}
}

func openTest(t *testing.T, cfg Config) *Engine {
	t.Helper()
	e, err := Open(t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

// fullScan 绕过所有裁剪，直接读清单里的全部文件再过滤，作为正确性基线。
func fullScan(t *testing.T, e *Engine, q Query) []Event {
	t.Helper()
	snap, err := e.acquire()
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer e.release(snap)
	keySet := map[string]bool{}
	for _, k := range q.Keys {
		keySet[k] = true
	}
	var out []Event
	for _, m := range snap.Files {
		evs, err := readEventFile(e.root, m)
		if err != nil {
			t.Fatalf("read %s: %v", m.Path, err)
		}
		for _, ev := range evs {
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
	return out
}

func mustWrite(t *testing.T, e *Engine, evs ...Event) {
	t.Helper()
	if err := e.Write(evs...); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func mustQuery(t *testing.T, e *Engine, q Query) []Event {
	t.Helper()
	out, err := e.Query(q)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	return out
}

func hour(n int64) int64 { return n * int64(time.Hour) }

// TestPartitionPruning 随机数据 + 随机查询，裁剪结果必须与全量扫描逐行一致。
func TestPartitionPruning(t *testing.T) {
	e := openTest(t, testConfig(""))
	rng := rand.New(rand.NewSource(42))
	var all []Event
	for i := 0; i < 200; i++ {
		all = append(all, Event{
			ID:        fmt.Sprintf("key-%d", rng.Intn(20)),
			Timestamp: hour(rng.Int63n(10)) + rng.Int63n(int64(time.Hour)),
			Payload:   []byte(fmt.Sprintf("payload-%d", i)),
		})
	}
	// 分多批写入，制造多个小文件与多个分区
	for i := 0; i < len(all); i += 7 {
		j := i + 7
		if j > len(all) {
			j = len(all)
		}
		mustWrite(t, e, all[i:j]...)
	}
	for i := 0; i < 100; i++ {
		q := Query{Start: hour(rng.Int63n(8)), End: hour(rng.Int63n(8) + 3)}
		if rng.Intn(2) == 0 {
			n := rng.Intn(3) + 1
			for k := 0; k < n; k++ {
				q.Keys = append(q.Keys, fmt.Sprintf("key-%d", rng.Intn(20)))
			}
		}
		got := mustQuery(t, e, q)
		want := fullScan(t, e, q)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("query %+v mismatch:\n got %d rows\n want %d rows", q, len(got), len(want))
		}
	}
}

// TestCompaction 合并后文件数减少，且查询结果与合并前完全一致。
func TestCompaction(t *testing.T) {
	e := openTest(t, testConfig(""))
	var all []Event
	for i := 0; i < 12; i++ { // 同一分区 12 个小文件
		ev := Event{ID: fmt.Sprintf("k%d", i%3), Timestamp: hour(1) + int64(i), Payload: []byte{byte(i)}}
		all = append(all, ev)
		mustWrite(t, e, ev)
	}
	q := Query{Start: 0, End: hour(2)}
	before := mustQuery(t, e, q)
	var total int
	for _, n := range e.partitionFileCounts() {
		total += n
	}
	if total < 12 {
		t.Fatalf("expected >=12 files before compaction, got %d", total)
	}
	e.SweepNow()
	after := mustQuery(t, e, q)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("results changed after compaction:\nbefore %v\nafter %v", before, after)
	}
	// 每个分区合并为 1 个文件
	for p, n := range e.partitionFileCounts() {
		if n != 1 {
			t.Fatalf("partition %s: expected 1 file after compaction, got %d", p, n)
		}
	}
	// 状态可观测
	st := e.Status()
	found := false
	for _, s := range st {
		if s.State == JobDone {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a done partition status, got %+v", st)
	}
}

// TestCompactionBelowThreshold 未达到阈值不合并。
func TestCompactionBelowThreshold(t *testing.T) {
	e := openTest(t, testConfig(""))
	for i := 0; i < 3; i++ {
		mustWrite(t, e, Event{ID: "a", Timestamp: hour(1) + int64(i)})
	}
	e.SweepNow()
	var total int
	for _, n := range e.partitionFileCounts() {
		total += n
	}
	if total != 3 {
		t.Fatalf("expected 3 files (no compaction), got %d", total)
	}
}

func countFilesOnDisk(t *testing.T, root string) int {
	t.Helper()
	n := 0
	for _, tier := range []string{"hot", "cold"} {
		filepath.Walk(filepath.Join(root, tier), func(p string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() {
				n++
			}
			return nil
		})
	}
	return n
}
