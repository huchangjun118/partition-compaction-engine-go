package engine

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"
)

func testConfig() Config {
	return Config{
		KeyBuckets:            4,
		TimeBucketWindow:      time.Hour,
		CompactionThreshold:   3,
		CompactionConcurrency: 2,
		ScheduleInterval:      time.Hour, // 测试中默认手动调度
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

// fullScan 参考实现：在内存记录集上逐行过滤。
func fullScan(recs []Record, q Query) []Record {
	keySet := map[string]bool{}
	for _, k := range q.Keys {
		keySet[k] = true
	}
	var out []Record
	for _, r := range recs {
		if r.Timestamp < q.StartTS || r.Timestamp > q.EndTS {
			continue
		}
		if len(keySet) > 0 && !keySet[r.Key] {
			continue
		}
		out = append(out, r)
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
	return out
}

func TestWriteQueryBasic(t *testing.T) {
	e := openTest(t, testConfig())
	recs := []Record{
		{Key: "a", Timestamp: 1000, Value: []byte("v1")},
		{Key: "b", Timestamp: 2000, Value: []byte("v2")},
		{Key: "a", Timestamp: 3000, Value: []byte("v3")},
	}
	if err := e.Write(recs); err != nil {
		t.Fatal(err)
	}
	got, err := e.Query(Query{StartTS: 0, EndTS: 4000})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 records, got %d", len(got))
	}
	got, err = e.Query(Query{StartTS: 0, EndTS: 4000, Keys: []string{"a"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 records for key a, got %d", len(got))
	}
	got, err = e.Query(Query{StartTS: 1500, EndTS: 2500})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Key != "b" {
		t.Fatalf("time range query wrong: %+v", got)
	}
}

// TestQueryMatchesFullScan 随机数据 + 随机查询，引擎结果必须与全量扫描逐行一致。
func TestQueryMatchesFullScan(t *testing.T) {
	e := openTest(t, testConfig())
	rng := rand.New(rand.NewSource(42))
	var all []Record
	keys := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"}
	base := time.Now().UnixMilli()
	for i := 0; i < 50; i++ {
		var batch []Record
		for j := 0; j < 20; j++ {
			r := Record{
				Key:       keys[rng.Intn(len(keys))],
				Timestamp: base + rng.Int63n(6*time.Hour.Milliseconds()),
				Value:     []byte(fmt.Sprintf("v-%d-%d", i, j)),
			}
			batch = append(batch, r)
			all = append(all, r)
		}
		if err := e.Write(batch); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 200; i++ {
		start := base + rng.Int63n(5*time.Hour.Milliseconds())
		end := start + rng.Int63n(2*time.Hour.Milliseconds())
		var q Query
		q.StartTS, q.EndTS = start, end
		if rng.Intn(2) == 0 {
			n := 1 + rng.Intn(3)
			for j := 0; j < n; j++ {
				q.Keys = append(q.Keys, keys[rng.Intn(len(keys))])
			}
		}
		got, err := e.Query(q)
		if err != nil {
			t.Fatal(err)
		}
		want := fullScan(all, q)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("query %d %+v mismatch:\n got %d records\nwant %d records", i, q, len(got), len(want))
		}
	}
}

// TestPartitionPruning 验证裁剪确实跳过了无关分区的段。
func TestPartitionPruning(t *testing.T) {
	cfg := testConfig()
	e := openTest(t, cfg)
	base := int64(0)
	// 每个时间桶、每个键写一条
	for h := 0; h < 4; h++ {
		for _, k := range []string{"k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8"} {
			ts := base + int64(h)*cfg.TimeBucketWindow.Milliseconds() + 1
			if err := e.Write([]Record{{Key: k, Timestamp: ts, Value: []byte("x")}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	snap, err := e.acquireSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer e.releaseSnapshot(snap)

	// 单键 + 单时间桶查询：命中的段必须全部属于该键桶与该时间桶
	q := Query{StartTS: 1, EndTS: cfg.TimeBucketWindow.Milliseconds(), Keys: []string{"k3"}}
	wantBucket := keyBucketOf(cfg, "k3")
	matched := 0
	for _, seg := range snap.Segments {
		buckets := map[int]bool{wantBucket: true}
		if segmentMayMatch(cfg, seg, q, buckets, "k3", "k3") {
			matched++
			if seg.Partition.KeyBucket != wantBucket {
				t.Fatalf("segment from wrong key bucket matched: %+v", seg.Partition)
			}
			if seg.Partition.TimeBucket != 0 {
				t.Fatalf("segment from wrong time bucket matched: %+v", seg.Partition)
			}
		}
	}
	if matched == 0 {
		t.Fatal("pruning dropped all segments, would lose data")
	}
	// 查询结果正确性
	got, err := e.Query(q)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Key != "k3" {
		t.Fatalf("want exactly k3 record, got %+v", got)
	}
}
