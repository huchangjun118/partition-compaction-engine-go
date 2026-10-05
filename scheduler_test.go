package engine

import (
	"testing"
	"time"
)

// TestBackgroundScheduler 后台调度器自动推进合并与迁移，无需手动触发。
func TestBackgroundScheduler(t *testing.T) {
	cfg := testConfig()
	cfg.ScheduleInterval = 10 * time.Millisecond
	cfg.CompactionThreshold = 3
	cfg.ColdAfter = 50 * time.Millisecond
	e := openTest(t, cfg)

	now := time.Now()
	e.setNow(func() time.Time { return now })
	base := now.UnixMilli()
	for i := 0; i < 6; i++ {
		e.Write([]Record{{Key: "bg", Timestamp: base + int64(i), Value: []byte("v")}})
	}
	// 等待自动合并：段数应显著下降（合并只处理调度时刻的快照，
	// 与写入并发时可能残留未达到阈值的新段，属正常行为）
	deadline := time.Now().Add(5 * time.Second)
	for segmentCount(t, e) >= 6 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := segmentCount(t, e); n >= 6 {
		t.Fatalf("background compaction did not run, %d segments", n)
	}
	// 时间前进（持续走的偏移时钟，保证 ColdAfter 对新段也会生效），等待自动迁移到冷层
	e.setNow(func() time.Time { return time.Now().Add(time.Hour) })
	deadline = time.Now().Add(5 * time.Second)
	allCold := func() bool {
		e.mu.Lock()
		defer e.mu.Unlock()
		for _, s := range e.m.Segments {
			if s.Tier != TierCold {
				return false
			}
		}
		return true
	}
	for !allCold() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !allCold() {
		t.Fatal("background migration did not run")
	}
	// 数据始终可查
	got, err := e.Query(Query{StartTS: 0, EndTS: base + 1000, Keys: []string{"bg"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 6 {
		t.Fatalf("got %d records, want 6", len(got))
	}
	// 任务状态可观测：等待所有任务收尾后，不应有失败任务
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		busy := false
		for _, ti := range e.Tasks() {
			if ti.State == TaskPending || ti.State == TaskRunning {
				busy = true
			}
		}
		if !busy {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	tasks := e.Tasks()
	if len(tasks) == 0 {
		t.Fatal("no tasks recorded")
	}
	for _, ti := range tasks {
		if ti.State == TaskFailed {
			t.Fatalf("task %s (%s) failed: %s", ti.ID, ti.Kind, ti.Err)
		}
		if ti.State == TaskPending || ti.State == TaskRunning {
			t.Fatalf("task %s (%s) still %s after quiesce wait", ti.ID, ti.Kind, ti.State)
		}
	}
}

// TestSlowPartitionDoesNotBlockOthers 一个分区合并进行中时，
// 其他分区的写入与查询不被阻塞。
func TestSlowPartitionDoesNotBlockOthers(t *testing.T) {
	cfg := testConfig()
	cfg.CompactionThreshold = 3
	e := openTest(t, cfg)
	base := time.Now().UnixMilli()

	// 分区 A：大量数据
	for i := 0; i < 10; i++ {
		e.Write([]Record{{Key: "heavy", Timestamp: base + int64(i), Value: make([]byte, 1024)}})
	}
	done := make(chan struct{})
	go func() {
		e.CompactNow() // 触发 heavy 分区合并
		close(done)
	}()
	// 分区 B：写入与查询应立即可用
	for i := 0; i < 20; i++ {
		if err := e.Write([]Record{{Key: "light", Timestamp: base + int64(i), Value: []byte("v")}}); err != nil {
			t.Fatalf("write to other partition blocked: %v", err)
		}
		if _, err := e.Query(Query{StartTS: 0, EndTS: base + 1000, Keys: []string{"light"}}); err != nil {
			t.Fatalf("query on other partition blocked: %v", err)
		}
	}
	<-done
	got, _ := e.Query(Query{StartTS: 0, EndTS: base + 1000, Keys: []string{"light"}})
	if len(got) != 20 {
		t.Fatalf("light partition data wrong: %d records", len(got))
	}
}
