package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// manifest 是引擎的持久化清单，记录版本与全部段。
type manifest struct {
	Version  uint64         `json:"version"`
	NextID   uint64         `json:"next_id"`
	Segments []*SegmentMeta `json:"segments"`
}

func (m *manifest) clone() *manifest {
	out := &manifest{Version: m.Version, NextID: m.NextID, Segments: make([]*SegmentMeta, len(m.Segments))}
	for i, s := range m.Segments {
		cp := *s
		out.Segments[i] = &cp
	}
	return out
}

func (m *manifest) find(id string) *SegmentMeta {
	for _, s := range m.Segments {
		if s.ID == id {
			return s
		}
	}
	return nil
}

func (m *manifest) remove(ids map[string]bool) {
	out := m.Segments[:0]
	for _, s := range m.Segments {
		if !ids[s.ID] {
			out = append(out, s)
		}
	}
	m.Segments = out
}

// manifestStore 负责清单的原子加载与提交。
// 提交采用「写临时文件 + fsync + rename」，崩溃时要么看到旧版本要么看到新版本，
// 不会看到半成品。
type manifestStore struct {
	path string
}

func newManifestStore(path string) *manifestStore { return &manifestStore{path: path} }

// load 读取清单；不存在时返回空清单。
func (s *manifestStore) load() (*manifest, error) {
	data, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return &manifest{}, nil
	}
	if err != nil {
		return nil, err
	}
	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// commit 原子写入新版本清单。
func (s *manifestStore) commit(m *manifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return err
	}
	// fsync 目录，保证 rename 持久化。
	if d, err := os.Open(filepath.Dir(s.path)); err == nil {
		d.Sync()
		d.Close()
	}
	return nil
}
