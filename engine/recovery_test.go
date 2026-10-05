package engine

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// reopen 关闭并重新打开同一根目录的引擎（模拟进程重启）。
func reopen(t *testing.T, e *Engine) *Engine {
	t.Helper()
	root := e.root
	cfg := e.config()
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	ne, err := Open(root, cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { ne.Close() })
	return ne
}

// 直接改清单（绕过 GC，模拟崩溃留下的现场）。
func forceCommit(t *testing.T, e *Engine, mutate func(m *Manifest)) {
	t.Helper()
	e.mu.Lock()
	defer e.mu.Unlock()
	nm := e.manifest.clone()
	mutate(nm)
	nm.Version++
	if err := e.ms.commit(nm); err != nil {
		t.Fatalf("commit: %v", err)
	}
	e.manifest = nm
}

// TestRecoveryRollback 崩溃发生在清单提交之前：新文件不在清单中，
// 重启后应回滚——半成品文件被删除，原数据完整。
func TestRecoveryRollback(t *testing.T) {
	e := openTest(t, testConfig(""))
	ev := Event{ID: "a", Timestamp: hour(1), Payload: []byte("x")}
	mustWrite(t, e, ev)

	// 模拟一次崩溃的合并：新文件已写出，journal 未提交，清单未更新
	half, err := writeEventFile(e.root, partitionOf("a", hour(1), e.config()), TierHot, []Event{ev})
	if err != nil {
		t.Fatal(err)
	}
	var old []FileMeta
	for _, m := range e.manifest.Files {
		old = append(old, m)
	}
	if err := e.js.write(journalEntry{OpID: "op-crash1", Kind: "compact", Add: []FileMeta{half}, Remove: old}); err != nil {
		t.Fatal(err)
	}

	e = reopen(t, e)

	if _, err := osStat(filepath.Join(e.root, half.Path)); !os.IsNotExist(err) {
		t.Fatalf("half-written file should be rolled back, stat err=%v", err)
	}
	got := mustQuery(t, e, Query{Start: 0, End: hour(2)})
	if !reflect.DeepEqual(got, []Event{ev}) {
		t.Fatalf("data lost after rollback: %+v", got)
	}
	if ents, _ := e.js.list(); len(ents) != 0 {
		t.Fatalf("journal not cleaned: %+v", ents)
	}
}

// TestRecoveryForward 崩溃发生在清单提交之后、journal 标记完成之前：
// 重启后应前滚——旧文件被清理，新文件保留，数据不重复不丢失。
func TestRecoveryForward(t *testing.T) {
	e := openTest(t, testConfig(""))
	ev1 := Event{ID: "a", Timestamp: hour(1), Payload: []byte("1")}
	ev2 := Event{ID: "b", Timestamp: hour(1) + 1, Payload: []byte("2")}
	mustWrite(t, e, ev1)
	mustWrite(t, e, ev2)

	// 模拟合并：写出合并文件，清单已切换，但 journal 未来得及标记 committed
	merged, err := writeEventFile(e.root, partitionOf("a", hour(1), e.config()), TierHot, []Event{ev1, ev2})
	if err != nil {
		t.Fatal(err)
	}
	var old []FileMeta
	for _, m := range e.manifest.Files {
		old = append(old, m)
	}
	if err := e.js.write(journalEntry{OpID: "op-crash2", Kind: "compact", Add: []FileMeta{merged}, Remove: old}); err != nil {
		t.Fatal(err)
	}
	forceCommit(t, e, func(m *Manifest) {
		for _, o := range old {
			delete(m.Files, o.ID)
		}
		m.Files[merged.ID] = merged
	})
	oldPaths := make([]string, 0, len(old))
	for _, o := range old {
		oldPaths = append(oldPaths, o.Path)
	}

	e = reopen(t, e)

	for _, p := range oldPaths {
		if _, err := osStat(filepath.Join(e.root, p)); !os.IsNotExist(err) {
			t.Fatalf("obsolete file %s should be collected by forward recovery", p)
		}
	}
	// 数据恰好一份：不重复累计也不丢
	got := mustQuery(t, e, Query{Start: 0, End: hour(2)})
	if !reflect.DeepEqual(got, []Event{ev1, ev2}) {
		t.Fatalf("after forward recovery: %+v", got)
	}
}

// TestRecoveryCommittedJournal 已标记 committed 的 journal（崩溃在删除
// journal 文件之前）：重启后完成清理并移除 journal。
func TestRecoveryCommittedJournal(t *testing.T) {
	e := openTest(t, testConfig(""))
	ev := Event{ID: "a", Timestamp: hour(1)}
	mustWrite(t, e, ev)
	var old FileMeta
	for _, m := range e.manifest.Files {
		old = m
	}
	if err := e.js.write(journalEntry{OpID: "op-crash3", Kind: "retention", Remove: []FileMeta{old}, Committed: true}); err != nil {
		t.Fatal(err)
	}
	forceCommit(t, e, func(m *Manifest) { delete(m.Files, old.ID) })

	e = reopen(t, e)

	if _, err := osStat(filepath.Join(e.root, old.Path)); !os.IsNotExist(err) {
		t.Fatalf("removed file should be collected, stat err=%v", err)
	}
	if ents, _ := e.js.list(); len(ents) != 0 {
		t.Fatalf("journal not cleaned")
	}
}

// TestOrphanSweep 未知数据文件与临时文件在重启时被清扫。
func TestOrphanSweep(t *testing.T) {
	e := openTest(t, testConfig(""))
	mustWrite(t, e, Event{ID: "a", Timestamp: hour(1)})
	root := e.root
	orphan := filepath.Join(root, "hot", "t=1", "h4=0", "d-deadbeef.jsonl")
	os.MkdirAll(filepath.Dir(orphan), 0o755)
	os.WriteFile(orphan, []byte("{}\n"), 0o644)
	tmp := filepath.Join(root, "hot", "t=1", "h4=0", "d-x.jsonl.tmp")
	os.WriteFile(tmp, []byte("partial"), 0o644)

	e = reopen(t, e)

	for _, p := range []string{orphan, tmp} {
		if _, err := osStat(p); !os.IsNotExist(err) {
			t.Fatalf("orphan %s not swept", p)
		}
	}
	if got := mustQuery(t, e, Query{Start: 0, End: hour(2)}); len(got) != 1 {
		t.Fatalf("data lost: %+v", got)
	}
}

// TestRestartDataPersistent 正常重启后数据完整可查。
func TestRestartDataPersistent(t *testing.T) {
	e := openTest(t, testConfig(""))
	evs := []Event{
		{ID: "a", Timestamp: hour(1)},
		{ID: "b", Timestamp: hour(2)},
		{ID: "c", Timestamp: hour(3)},
	}
	mustWrite(t, e, evs...)
	e.SweepNow() // 触发一次合并（不足阈值则不合并，不影响）
	e = reopen(t, e)
	got := mustQuery(t, e, Query{Start: 0, End: hour(100)})
	if !reflect.DeepEqual(got, evs) {
		t.Fatalf("got %+v want %+v", got, evs)
	}
}
