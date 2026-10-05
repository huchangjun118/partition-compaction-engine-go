package engine

import (
	"os"
	"path/filepath"
	"sort"
)

// compactPartition 合并一个分区内的小文件。
//
// 流程：获取清单快照（钉住待读文件）→ 读取全部事件 → 写出合并文件 →
// 通过 applyChange 原子切换清单。旧文件在所有引用它们的快照释放后才删除，
// 因此合并前后查询结果完全一致，且并发读不会看到中间状态。
func (e *Engine) compactPartition(partition string) error {
	snap, err := e.acquire()
	if err != nil {
		return err
	}
	var files []FileMeta
	for _, m := range snap.Files {
		if m.Partition == partition {
			files = append(files, m)
		}
	}
	if len(files) < 2 {
		e.release(snap)
		return nil
	}
	sort.Slice(files, func(i, j int) bool { return files[i].ID < files[j].ID })

	var events []Event
	for _, m := range files {
		evs, err := readEventFile(e.root, m)
		if err != nil {
			e.release(snap)
			return err
		}
		events = append(events, evs...)
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].Timestamp != events[j].Timestamp {
			return events[i].Timestamp < events[j].Timestamp
		}
		return events[i].ID < events[j].ID
	})

	tier := TierCold
	for _, m := range files {
		if m.Tier == TierHot {
			tier = TierHot
			break
		}
	}
	meta, err := writeEventFile(e.root, partition, tier, events)
	if err != nil {
		e.release(snap)
		return err
	}
	err = e.applyChange("compact", []FileMeta{meta}, files)
	e.release(snap)
	if err != nil {
		os.Remove(filepath.Join(e.root, meta.Path))
		return err
	}
	return nil
}
