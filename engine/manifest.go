package engine

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

// Manifest 是某一版本的完整文件清单，版本内不可变。
type Manifest struct {
	Version uint64              `json:"version"`
	Files   map[string]FileMeta `json:"files"` // fileID -> meta
}

func newManifest() *Manifest {
	return &Manifest{Version: 0, Files: map[string]FileMeta{}}
}

// clone 深拷贝清单，保证已发布版本不可变。
func (m *Manifest) clone() *Manifest {
	out := &Manifest{Version: m.Version, Files: make(map[string]FileMeta, len(m.Files))}
	for k, v := range m.Files {
		out.Files[k] = v
	}
	return out
}

// manifestStore 负责清单的原子提交与加载。
// 布局：<root>/manifest/manifest.json，通过 tmp+rename+fsync 原子替换。
type manifestStore struct {
	dir string
}

func newManifestStore(dir string) *manifestStore { return &manifestStore{dir: dir} }

func (s *manifestStore) path() string { return filepath.Join(s.dir, "manifest.json") }

func (s *manifestStore) load() (*Manifest, error) {
	data, err := os.ReadFile(s.path())
	if errors.Is(err, os.ErrNotExist) {
		return newManifest(), nil
	}
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	if m.Files == nil {
		m.Files = map[string]FileMeta{}
	}
	return &m, nil
}

// commit 原子落盘清单（tmp+rename+fsync），调用方负责递增版本号。
func (s *manifestStore) commit(m *Manifest) error {
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	tmp := s.path() + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if f, err := os.Open(tmp); err == nil {
		_ = f.Sync()
		_ = f.Close()
	}
	if err := os.Rename(tmp, s.path()); err != nil {
		os.Remove(tmp)
		return err
	}
	syncDir(s.dir)
	return nil
}
