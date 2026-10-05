package engine

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestConcurrentReadWriteCompact 写入、查询与合并并发进行：
// 查询不得报错、不得返回过滤条件之外的数据；最终全量数据必须完整可查。
func TestConcurrentReadWriteCompact(t *testing.T) {
	cfg := testConfig("")
	cfg.CompactThreshold = 3
	e := openTest(t, cfg)

	const batches = 40
	var wg, readers sync.WaitGroup
	stop := make(chan struct{})

	// 写入协程
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < batches; i++ {
			ev := Event{ID: fmt.Sprintf("w%d", i%5), Timestamp: hour(1) + int64(i)}
			if err := e.Write(ev); err != nil {
				t.Errorf("write: %v", err)
				return
			}
		}
	}()

	// 查询协程 ×2：结果中不得出现范围外数据
	for r := 0; r < 2; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				out, err := e.Query(Query{Start: hour(1), End: hour(2)})
				if err != nil {
					t.Errorf("query: %v", err)
					return
				}
				for _, ev := range out {
					if ev.Timestamp < hour(1) || ev.Timestamp >= hour(2) {
						t.Errorf("out-of-range event: %+v", ev)
						return
					}
				}
			}
		}()
	}

	// 合并触发协程
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			e.SweepNow()
			time.Sleep(time.Millisecond)
		}
	}()

	wg.Wait()      // 等写入与合并结束
	close(stop)    // 通知查询协程退出
	readers.Wait() // 等查询协程退出

	// 最终数据完整性：40 条全部可查
	out := mustQuery(t, e, Query{Start: hour(1), End: hour(2)})
	if len(out) != batches {
		t.Fatalf("expected %d events, got %d", batches, len(out))
	}
	want := fullScan(t, e, Query{Start: hour(1), End: hour(2)})
	if len(out) != len(want) {
		t.Fatalf("query vs fullscan: %d != %d", len(out), len(want))
	}
}

// TestSnapshotIsolation 长查询持有快照期间发生合并与淘汰，
// 快照释放前旧文件不得被物理删除。
func TestSnapshotIsolation(t *testing.T) {
	cfg := testConfig("")
	cfg.Retention = time.Hour
	e := openTest(t, cfg)
	for i := 0; i < 6; i++ {
		mustWrite(t, e, Event{ID: "a", Timestamp: hour(1) + int64(i)})
	}
	// 手动持有一个快照
	snap, err := e.acquire()
	if err != nil {
		t.Fatal(err)
	}
	paths := map[string]bool{}
	for _, m := range snap.Files {
		paths[m.Path] = true
	}
	// 触发合并 + 淘汰（淘汰不会命中，因为 nowFn 默认是当前时间；
	// 这里把时钟拨快，使文件同时满足淘汰条件）
	e.nowFn = func() int64 { return time.Now().Add(2 * time.Hour).UnixNano() }
	e.SweepNow()
	// 快照仍引用的文件必须还在
	for p := range paths {
		if _, err := fileStat(e.root, p); err != nil {
			t.Fatalf("snapshot file %s deleted while pinned: %v", p, err)
		}
	}
	e.release(snap)
	// 释放后再触发一轮，旧文件应被回收
	e.SweepNow()
	for p := range paths {
		if _, err := fileStat(e.root, p); err == nil {
			t.Fatalf("obsolete file %s not collected after release", p)
		}
	}
}

func fileStat(root, rel string) (bool, error) {
	_, err := osStat(root + "/" + rel)
	if err != nil {
		return false, err
	}
	return true, nil
}
