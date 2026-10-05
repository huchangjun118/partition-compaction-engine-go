package engine

import (
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// TestRetention 超出保留窗口的数据被淘汰，窗口内数据不受影响。
func TestRetention(t *testing.T) {
	cfg := testConfig("")
	cfg.Retention = 2 * time.Hour
	e := openTest(t, cfg)

	oldEv := Event{ID: "old", Timestamp: hour(0), Payload: []byte("old")}
	newEv := Event{ID: "new", Timestamp: hour(10), Payload: []byte("new")}
	mustWrite(t, e, oldEv)
	mustWrite(t, e, newEv)

	// 时钟拨到 hour(12)：cutoff = hour(10)，old 文件 MaxTS=hour(0) 过期
	e.nowFn = func() int64 { return hour(12) }
	e.SweepNow()

	got := mustQuery(t, e, Query{Start: 0, End: hour(100)})
	if !reflect.DeepEqual(got, []Event{newEv}) {
		t.Fatalf("after retention: got %+v", got)
	}
	// 过期文件必须从磁盘消失
	if n := countFilesOnDisk(t, e.root); n != 1 {
		t.Fatalf("expected 1 file on disk, got %d", n)
	}
	// 清单中也不得有过期文件
	for _, m := range e.manifest.Files {
		if m.MaxTS < hour(10) {
			t.Fatalf("expired file still in manifest: %+v", m)
		}
	}
}

// TestRetentionDisabled 未配置保留窗口时不淘汰。
func TestRetentionDisabled(t *testing.T) {
	e := openTest(t, testConfig(""))
	mustWrite(t, e, Event{ID: "a", Timestamp: hour(0)})
	e.nowFn = func() int64 { return hour(10000) }
	e.SweepNow()
	if got := mustQuery(t, e, Query{Start: 0, End: hour(100)}); len(got) != 1 {
		t.Fatalf("retention disabled but data lost: %+v", got)
	}
}

// TestTiering 超出热窗口的数据迁移到冷层，读取方无感知。
func TestTiering(t *testing.T) {
	cfg := testConfig("")
	cfg.HotWindow = time.Hour
	e := openTest(t, cfg)

	coldEv := Event{ID: "cold", Timestamp: hour(0), Payload: []byte("c")}
	hotEv := Event{ID: "hot", Timestamp: hour(5), Payload: []byte("h")}
	mustWrite(t, e, coldEv)
	mustWrite(t, e, hotEv)

	e.nowFn = func() int64 { return hour(6) } // cutoff = hour(5)
	e.SweepNow()

	// 清单层：coldEv 的文件在冷层，hotEv 的在热层
	var coldMeta, hotMeta FileMeta
	for _, m := range e.manifest.Files {
		if m.Partition == partitionOf("cold", hour(0), e.config()) {
			coldMeta = m
		} else {
			hotMeta = m
		}
	}
	if coldMeta.Tier != TierCold {
		t.Fatalf("expected cold tier, got %+v", coldMeta)
	}
	if hotMeta.Tier != TierHot {
		t.Fatalf("expected hot tier, got %+v", hotMeta)
	}
	if _, err := osStat(filepath.Join(e.root, string(TierCold), coldMeta.Partition, filepath.Base(coldMeta.Path))); err != nil {
		t.Fatalf("cold file missing on disk: %v", err)
	}
	// 读取方不需要知道层级：查询结果不变
	got := mustQuery(t, e, Query{Start: 0, End: hour(100)})
	if len(got) != 2 {
		t.Fatalf("expected 2 events across tiers, got %+v", got)
	}
	want := fullScan(t, e, Query{Start: 0, End: hour(100)})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("tiered read mismatch")
	}
}

// TestConfigChange 配置变更只影响后续调度：调低阈值后新写入触发合并；
// 修改 HashBuckets 后旧分区仍按各自模数正确裁剪。
func TestConfigChange(t *testing.T) {
	cfg := testConfig("")
	e := openTest(t, cfg)

	// 修改 HashBuckets 前写入一批
	var old []Event
	for i := 0; i < 10; i++ {
		ev := Event{ID: key(i), Timestamp: hour(1) + int64(i)}
		old = append(old, ev)
		mustWrite(t, e, ev)
	}
	// 变更配置：HashBuckets 4 -> 8，阈值 4 -> 3
	c2 := e.config()
	c2.HashBuckets = 8
	c2.CompactThreshold = 3
	e.SetConfig(c2)
	// 新写入进入新模数的分区
	for i := 0; i < 5; i++ {
		mustWrite(t, e, Event{ID: key(i), Timestamp: hour(2) + int64(i)})
	}
	// 任意主键查询都必须与全量扫描一致（跨模数裁剪正确性）
	for i := 0; i < 10; i++ {
		q := Query{Start: 0, End: hour(100), Keys: []string{key(i)}}
		got := mustQuery(t, e, q)
		want := fullScan(t, e, q)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("key %s: got %+v want %+v", key(i), got, want)
		}
	}
	// 调低阈值后，hour(2) 的分区（5 文件 >= 3）应被合并
	e.SweepNow()
	for p, n := range e.partitionFileCounts() {
		if n >= 3 {
			t.Fatalf("partition %s not compacted after threshold change: %d files", p, n)
		}
	}
}

func key(i int) string { return "key-" + string(rune('a'+i)) }

// TestTimeBucketChange 修改 TimeBucket 后，旧分区仍按原桶宽正确裁剪。
func TestTimeBucketChange(t *testing.T) {
	e := openTest(t, testConfig("")) // TimeBucket = 1h
	var all []Event
	for i := 0; i < 20; i++ {
		ev := Event{ID: key(i % 5), Timestamp: hour(int64(i%6)) + int64(i)}
		all = append(all, ev)
		mustWrite(t, e, ev)
	}
	// 变更时间桶宽度后继续写入
	c2 := e.config()
	c2.TimeBucket = 10 * time.Minute
	e.SetConfig(c2)
	for i := 0; i < 10; i++ {
		mustWrite(t, e, Event{ID: key(i % 5), Timestamp: hour(7) + int64(i)*int64(time.Minute)})
	}
	// 任意时间窗 + 主键查询都必须与全量扫描一致
	for start := int64(0); start < hour(8); start += hour(1) / 2 {
		for _, ks := range [][]string{nil, {key(1)}, {key(0), key(3)}} {
			q := Query{Start: start, End: start + hour(1) + 1, Keys: ks}
			got := mustQuery(t, e, q)
			want := fullScan(t, e, q)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("q=%+v: got %d rows want %d", q, len(got), len(want))
			}
		}
	}
}
