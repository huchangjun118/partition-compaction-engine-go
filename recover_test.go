package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func countFiles(t *testing.T, dir string) (seg, tmp int) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return 0, 0
	}
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".seg" {
			seg++
		} else if filepath.Ext(e.Name()) == ".tmp" {
			tmp++
		}
	}
	return
}

// TestRecoveryCleansOrphans 崩溃遗留的临时文件与孤儿段文件在重启后被清理，
// 清单与数据保持完整，不重复累计也不丢数据。
func TestRecoveryCleansOrphans(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig()
	e, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UnixMilli()
	var all []Record
	for i := 0; i < 5; i++ {
		batch := []Record{{Key: "r", Timestamp: base + int64(i), Value: []byte(fmt.Sprintf("v%d", i))}}
		all = append(all, batch...)
		if err := e.Write(batch); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	// 模拟崩溃现场：半成品临时文件 + 未被清单引用的孤儿段文件
	hot := filepath.Join(dir, "data", "hot")
	os.WriteFile(filepath.Join(hot, "seg-999.seg.tmp"), []byte("partial"), 0o644)
	os.WriteFile(filepath.Join(hot, "seg-999.seg"), []byte("orphan"), 0o644)
	os.WriteFile(filepath.Join(dir, "manifest.json.tmp"), []byte("{}"), 0o644)

	// 重启恢复
	e2, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	seg, tmp := countFiles(t, hot)
	if tmp != 0 {
		t.Fatalf("tmp files not cleaned: %d", tmp)
	}
	if seg != 5 {
		t.Fatalf("orphan seg not cleaned or live seg lost: %d files", seg)
	}
	q := Query{StartTS: 0, EndTS: base + 10000}
	got, err := e2.Query(q)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, fullScan(all, q)) {
		t.Fatalf("data changed across recovery: got %d records", len(got))
	}
}

// TestRecoveryAfterCompactionCommit 合并提交后重启：清单指向新段，旧段文件被清理。
func TestRecoveryAfterCompactionCommit(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig()
	e, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UnixMilli()
	var all []Record
	for i := 0; i < 6; i++ {
		b := []Record{{Key: "c", Timestamp: base + int64(i), Value: []byte("v")}}
		all = append(all, b...)
		e.Write(b)
	}
	e.CompactNow()
	e.Close()

	e2, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if n := segmentCount(t, e2); n != 1 {
		t.Fatalf("want 1 segment after reopen, got %d", n)
	}
	q := Query{StartTS: 0, EndTS: base + 10000}
	got, _ := e2.Query(q)
	if !reflect.DeepEqual(got, fullScan(all, q)) {
		t.Fatal("data mismatch after compaction + restart")
	}
	hot := filepath.Join(dir, "data", "hot")
	seg, _ := countFiles(t, hot)
	if seg != 1 {
		t.Fatalf("stale segment files remain: %d", seg)
	}
}

// TestRetentionExpiration 超出保留窗口的整段数据被淘汰，窗口内数据不受影响。
func TestRetentionExpiration(t *testing.T) {
	cfg := testConfig()
	cfg.RetentionWindow = 2 * time.Hour
	e := openTest(t, cfg)

	now := time.Now()
	e.setNow(func() time.Time { return now })
	old := now.Add(-3 * time.Hour).UnixMilli()
	recent := now.Add(-30 * time.Minute).UnixMilli()
	e.Write([]Record{{Key: "old", Timestamp: old, Value: []byte("x")}})
	e.Write([]Record{{Key: "new", Timestamp: recent, Value: []byte("y")}})
	if n := segmentCount(t, e); n != 2 {
		t.Fatalf("want 2 segments, got %d", n)
	}

	e.CompactNow()

	if n := segmentCount(t, e); n != 1 {
		t.Fatalf("expired segment not removed, %d remain", n)
	}
	got, _ := e.Query(Query{StartTS: 0, EndTS: now.UnixMilli()})
	if len(got) != 1 || got[0].Key != "new" {
		t.Fatalf("retention removed wrong data: %+v", got)
	}
	// 淘汰后重启依然一致
	e.Close()
	dir := e.dir
	e2, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	got, _ = e2.Query(Query{StartTS: 0, EndTS: now.UnixMilli()})
	if len(got) != 1 || got[0].Key != "new" {
		t.Fatalf("retention result not stable across restart: %+v", got)
	}
}

// TestTierMigration 冷数据迁移到冷层，读取方无感知。
func TestTierMigration(t *testing.T) {
	cfg := testConfig()
	cfg.ColdAfter = time.Minute
	e := openTest(t, cfg)

	now := time.Now()
	e.setNow(func() time.Time { return now })
	base := now.UnixMilli()
	e.Write([]Record{{Key: "m", Timestamp: base, Value: []byte("v1")}})
	e.Write([]Record{{Key: "m", Timestamp: base + 1, Value: []byte("v2")}})

	// 时间前进，段变冷
	later := now.Add(2 * time.Minute)
	e.setNow(func() time.Time { return later })
	e.CompactNow()

	e.mu.Lock()
	for _, s := range e.m.Segments {
		if s.Tier != TierCold {
			t.Fatalf("segment %s not migrated to cold tier", s.ID)
		}
		if _, err := os.Stat(filepath.Join(e.coldDir(), s.ID+".seg")); err != nil {
			t.Fatalf("segment file missing in cold dir: %v", err)
		}
	}
	e.mu.Unlock()
	// 热层不应残留已迁移文件
	if n, _ := countFiles(t, e.hotDir()); n != 0 {
		t.Fatalf("hot tier still holds %d files", n)
	}
	// 读取方无需感知层级
	got, err := e.Query(Query{StartTS: 0, EndTS: base + 10, Keys: []string{"m"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("query after migration returned %d records, want 2", len(got))
	}
}

// TestConfigChange 配置变更只影响后续调度，已落盘数据保持有效。
func TestConfigChange(t *testing.T) {
	cfg := testConfig()
	cfg.CompactionThreshold = 100 // 不触发合并
	e := openTest(t, cfg)
	base := time.Now().UnixMilli()
	var all []Record
	for i := 0; i < 6; i++ {
		b := []Record{{Key: "cfg", Timestamp: base + int64(i), Value: []byte("v")}}
		all = append(all, b...)
		e.Write(b)
	}
	e.CompactNow()
	if n := segmentCount(t, e); n != 6 {
		t.Fatalf("threshold=100 should not compact, got %d segments", n)
	}
	// 调低阈值，后续调度生效
	newCfg := cfg
	newCfg.CompactionThreshold = 3
	e.SetConfig(newCfg)
	e.CompactNow()
	if n := segmentCount(t, e); n != 1 {
		t.Fatalf("new threshold not applied, got %d segments", n)
	}
	q := Query{StartTS: 0, EndTS: base + 10000}
	got, _ := e.Query(Query{StartTS: q.StartTS, EndTS: q.EndTS})
	if !reflect.DeepEqual(got, fullScan(all, q)) {
		t.Fatal("data invalidated by config change")
	}
}
