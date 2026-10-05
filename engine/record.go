package engine

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// 数据文件格式：JSON Lines，每行一个 Event。

func newFileID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// writeEventFile 将一批事件原子写入对应层级目录（tmp+rename+fsync），
// 返回文件元信息。调用方需保证 events 非空且属于同一分区。
func writeEventFile(root, partition string, tier Tier, events []Event) (FileMeta, error) {
	id := newFileID()
	rel := filepath.Join(string(tier), partition, "d-"+id+".jsonl")
	abs := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return FileMeta{}, err
	}
	tmp := abs + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return FileMeta{}, err
	}
	w := bufio.NewWriter(f)
	meta := FileMeta{ID: id, Partition: partition, Tier: tier, Path: rel, Count: len(events)}
	for i, ev := range events {
		if i == 0 || ev.Timestamp < meta.MinTS {
			meta.MinTS = ev.Timestamp
		}
		if i == 0 || ev.Timestamp > meta.MaxTS {
			meta.MaxTS = ev.Timestamp
		}
		line, err := json.Marshal(ev)
		if err != nil {
			f.Close()
			os.Remove(tmp)
			return FileMeta{}, err
		}
		if _, err := w.Write(append(line, '\n')); err != nil {
			f.Close()
			os.Remove(tmp)
			return FileMeta{}, err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		os.Remove(tmp)
		return FileMeta{}, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return FileMeta{}, err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return FileMeta{}, err
	}
	if err := os.Rename(tmp, abs); err != nil {
		os.Remove(tmp)
		return FileMeta{}, err
	}
	if st, err := os.Stat(abs); err == nil {
		meta.Size = st.Size()
	}
	syncDir(filepath.Dir(abs))
	return meta, nil
}

// readEventFile 读取一个数据文件中的全部事件。
func readEventFile(root string, meta FileMeta) ([]Event, error) {
	f, err := os.Open(filepath.Join(root, meta.Path))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []Event
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 16<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var ev Event
		if err := json.Unmarshal(line, &ev); err != nil {
			return nil, fmt.Errorf("decode %s: %w", meta.Path, err)
		}
		out = append(out, ev)
	}
	return out, sc.Err()
}

func syncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = d.Sync()
	_ = d.Close()
}
