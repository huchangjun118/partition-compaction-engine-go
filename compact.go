package engine

import (
	"fmt"
	"sort"
)

// runCompaction 合并一个分区内的全部小段：
// 1. 快照当前分区的段集合（输入）；
// 2. 归并读取所有输入段，写出一个新段（先写临时文件再 rename）；
// 3. 一次清单原子提交：摘除输入段、发布新段；
// 4. 输入段文件标记为废弃，等所有读者释放后物理删除。
// 合并不改写数据：输出是输入记录的全集归并，不丢不去重。
func runCompaction(e *Engine, pk PartitionKey) error {
	// 1. 选取输入段
	e.mu.Lock()
	var inputs []*SegmentMeta
	for _, s := range e.m.Segments {
		if s.Partition == pk {
			cp := *s
			inputs = append(inputs, &cp)
		}
	}
	if len(inputs) < 2 {
		e.mu.Unlock()
		return nil // 无需合并
	}
	if err := e.commitLocked(func(m *manifest) { m.NextID++ }); err != nil {
		e.mu.Unlock()
		return err
	}
	outID := fmt.Sprintf("seg-%d", e.m.NextID-1)
	e.mu.Unlock()

	// 2. 归并
	var all []Record
	tier := TierCold
	for _, in := range inputs {
		recs, err := readSegment(e.segmentPath(in))
		if err != nil {
			return fmt.Errorf("compact read %s: %w", in.ID, err)
		}
		all = append(all, recs...)
		if in.Tier == TierHot {
			tier = TierHot
		}
	}
	sort.Slice(all, func(a, b int) bool {
		if all[a].Key != all[b].Key {
			return all[a].Key < all[b].Key
		}
		if all[a].Timestamp != all[b].Timestamp {
			return all[a].Timestamp < all[b].Timestamp
		}
		return string(all[a].Value) < string(all[b].Value)
	})
	dir := e.hotDir()
	if tier == TierCold {
		dir = e.coldDir()
	}
	meta := SegmentMeta{
		ID:        outID,
		Partition: pk,
		Tier:      tier,
		CreatedAt: e.now().UnixMilli(),
	}
	written, err := writeSegment(dir, meta, all)
	if err != nil {
		return err
	}

	// 3. 原子提交：摘除输入、发布输出
	e.mu.Lock()
	inputIDs := map[string]bool{}
	for _, in := range inputs {
		inputIDs[in.ID] = true
	}
	out := written
	err = e.commitLocked(func(m *manifest) {
		m.remove(inputIDs)
		m.Segments = append(m.Segments, &out)
	})
	if err != nil {
		return err
	}
	// 4. 输入段转为废弃，等待读者释放后删除
	e.markObsoleteLocked(inputs...)
	e.mu.Unlock()
	e.sweepObsolete()
	return nil
}
