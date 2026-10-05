package engine

import (
	"io"
	"os"
	"path/filepath"
)

// sweepRetention 淘汰超出保留窗口的文件：MaxTS 早于 now-Retention 的文件
// 从清单中移除，并在不再被任何快照引用后物理删除。
func (e *Engine) sweepRetention() error {
	cfg := e.config()
	if cfg.Retention <= 0 {
		return nil
	}
	cutoff := e.nowFn() - cfg.Retention.Nanoseconds()
	e.mu.RLock()
	var expired []FileMeta
	for _, m := range e.manifest.Files {
		if m.MaxTS < cutoff {
			expired = append(expired, m)
		}
	}
	e.mu.RUnlock()
	if len(expired) == 0 {
		return nil
	}
	return e.applyChange("retention", nil, expired)
}

// sweepTiering 将超出热窗口的热层文件迁移到冷层。
// 迁移 = 复制文件到冷层（新文件 ID）+ 原子清单切换 + 安全删除旧文件，
// 读取方无感知。崩溃恢复依赖 journal：未提交则删冷层副本，已提交则删热层原件。
func (e *Engine) sweepTiering() error {
	cfg := e.config()
	if cfg.HotWindow <= 0 {
		return nil
	}
	cutoff := e.nowFn() - cfg.HotWindow.Nanoseconds()
	e.mu.RLock()
	var candidates []FileMeta
	for _, m := range e.manifest.Files {
		if m.Tier == TierHot && m.MaxTS < cutoff {
			candidates = append(candidates, m)
		}
	}
	e.mu.RUnlock()
	for _, old := range candidates {
		if err := e.migrateToCold(old); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) migrateToCold(old FileMeta) error {
	// 钉住当前版本，保证迁移过程中旧文件不被回收。
	snap, err := e.acquire()
	if err != nil {
		return err
	}
	defer e.release(snap)

	newMeta := old
	newMeta.ID = newFileID()
	newMeta.Tier = TierCold
	newMeta.Path = filepath.Join(string(TierCold), old.Partition, "d-"+newMeta.ID+".jsonl")
	if err := copyFileAtomic(filepath.Join(e.root, old.Path), filepath.Join(e.root, newMeta.Path)); err != nil {
		return err
	}
	if err := e.applyChange("tier", []FileMeta{newMeta}, []FileMeta{old}); err != nil {
		os.Remove(filepath.Join(e.root, newMeta.Path))
		return err
	}
	return nil
}

// copyFileAtomic 复制文件（tmp+rename+fsync），目标已存在则覆盖。
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
		os.Remove(tmp)
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		os.Remove(tmp)
		return err
	}
	syncDir(filepath.Dir(dst))
	return nil
}
