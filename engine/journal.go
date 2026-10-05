package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
)

// journalEntry 记录一次合并/迁移/淘汰操作的意图与结果，用于崩溃恢复。
// 协议：先写 journal(Committed=false) → 提交清单 → 写 journal(Committed=true)
// → 删除 journal。重启时按 Committed 决定前滚（清理 Remove）或回滚（清理 Add）。
type journalEntry struct {
	OpID      string     `json:"op_id"`
	Kind      string     `json:"kind"` // compact | tier | retention
	Add       []FileMeta `json:"add"`
	Remove    []FileMeta `json:"remove"`
	Committed bool       `json:"committed"`
}

type journalStore struct {
	dir string
}

func newJournalStore(dir string) *journalStore { return &journalStore{dir: dir} }

func (s *journalStore) path(opID string) string {
	return filepath.Join(s.dir, "op-"+opID+".json")
}

func (s *journalStore) write(e journalEntry) error {
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	tmp := s.path(e.OpID) + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if f, err := os.Open(tmp); err == nil {
		_ = f.Sync()
		_ = f.Close()
	}
	if err := os.Rename(tmp, s.path(e.OpID)); err != nil {
		os.Remove(tmp)
		return err
	}
	syncDir(s.dir)
	return nil
}

func (s *journalStore) remove(opID string) error {
	err := os.Remove(s.path(opID))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func (s *journalStore) list() ([]journalEntry, error) {
	ents, err := os.ReadDir(s.dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []journalEntry
	for _, ent := range ents {
		name := ent.Name()
		if ent.IsDir() || !strings.HasPrefix(name, "op-") || !strings.HasSuffix(name, ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(s.dir, name))
		if err != nil {
			return nil, err
		}
		var e journalEntry
		if err := json.Unmarshal(data, &e); err != nil {
			continue // 损坏的 journal 忽略，孤儿文件由启动清扫处理
		}
		out = append(out, e)
	}
	return out, nil
}
