package engine

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// runMigration 将段从热层迁移到冷层：
// 1. 复制文件到冷层（临时文件 + rename）；
// 2. 清单原子提交更新 Tier；
// 3. 热层副本标记废弃，等读者释放后删除。
// 读取方通过清单快照定位段所在层，无需感知迁移。
func runMigration(e *Engine, segID string) error {
	e.mu.Lock()
	seg := e.m.find(segID)
	if seg == nil || seg.Tier == TierCold {
		e.mu.Unlock()
		return nil
	}
	cp := *seg
	e.mu.Unlock()

	src := e.segmentPath(&cp)
	cp.Tier = TierCold
	dst := e.segmentPath(&cp)
	if err := copyFileAtomic(src, dst); err != nil {
		return fmt.Errorf("migrate %s: %w", segID, err)
	}

	e.mu.Lock()
	cur := e.m.find(segID)
	if cur == nil || cur.Tier == TierCold {
		e.mu.Unlock()
		removeSegmentFile(dst) // 段已消失或已迁移，清理多余副本
		return nil
	}
	err := e.commitLocked(func(m *manifest) {
		m.find(segID).Tier = TierCold
	})
	if err != nil {
		e.mu.Unlock()
		return err
	}
	old := *cur
	old.Tier = TierHot
	e.markObsoleteLocked(&old)
	e.mu.Unlock()
	e.sweepObsolete()
	return nil
}

// copyFileAtomic 复制文件：先写临时文件 + fsync + rename。
func copyFileAtomic(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp := dst + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, dst)
}

// runExpiration 淘汰整段超出保留窗口的段：
// 清单原子提交摘除后，段文件标记废弃，等读者释放后删除。
func runExpiration(e *Engine) error {
	cfg := e.getConfig()
	if cfg.RetentionWindow <= 0 {
		return nil
	}
	cutoff := e.now().Add(-cfg.RetentionWindow).UnixMilli()

	e.mu.Lock()
	var expired []*SegmentMeta
	expiredIDs := map[string]bool{}
	for _, s := range e.m.Segments {
		if s.MaxTS < cutoff {
			cp := *s
			expired = append(expired, &cp)
			expiredIDs[s.ID] = true
		}
	}
	if len(expired) == 0 {
		return nil
	}
	if err := e.commitLocked(func(m *manifest) { m.remove(expiredIDs) }); err != nil {
		e.mu.Unlock()
		return err
	}
	e.markObsoleteLocked(expired...)
	e.mu.Unlock()
	e.sweepObsolete()
	return nil
}
