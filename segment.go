package engine

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

var segmentMagic = []byte("SEG1")

// SegmentMeta 是清单中记录的段文件元数据。
type SegmentMeta struct {
	ID        string       `json:"id"`
	Partition PartitionKey `json:"partition"`
	MinTS     int64        `json:"min_ts"`
	MaxTS     int64        `json:"max_ts"`
	MinKey    string       `json:"min_key"`
	MaxKey    string       `json:"max_key"`
	Count     int          `json:"count"`
	Size      int64        `json:"size"`
	Tier      Tier         `json:"tier"`
	CreatedAt int64        `json:"created_at"` // 毫秒
}

// writeSegment 将记录（调用方保证已按 (Key,Timestamp) 排序）写入 dir，
// 先写临时文件再原子 rename，返回补全统计信息后的元数据。
func writeSegment(dir string, meta SegmentMeta, records []Record) (SegmentMeta, error) {
	if len(records) == 0 {
		return SegmentMeta{}, fmt.Errorf("empty segment")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return SegmentMeta{}, err
	}
	final := filepath.Join(dir, meta.ID+".seg")
	tmp := final + ".tmp"

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return SegmentMeta{}, err
	}
	w := bufio.NewWriter(f)
	if _, err := w.Write(segmentMagic); err != nil {
		f.Close()
		return SegmentMeta{}, err
	}
	buf := make([]byte, binary.MaxVarintLen64)
	putUvarint := func(v uint64) error {
		n := binary.PutUvarint(buf, v)
		_, err := w.Write(buf[:n])
		return err
	}
	for _, r := range records {
		if err := putUvarint(uint64(len(r.Key))); err != nil {
			f.Close()
			return SegmentMeta{}, err
		}
		if _, err := w.WriteString(r.Key); err != nil {
			f.Close()
			return SegmentMeta{}, err
		}
		if err := putUvarint(uint64(r.Timestamp)); err != nil {
			f.Close()
			return SegmentMeta{}, err
		}
		if err := putUvarint(uint64(len(r.Value))); err != nil {
			f.Close()
			return SegmentMeta{}, err
		}
		if _, err := w.Write(r.Value); err != nil {
			f.Close()
			return SegmentMeta{}, err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return SegmentMeta{}, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return SegmentMeta{}, err
	}
	if err := f.Close(); err != nil {
		return SegmentMeta{}, err
	}
	if err := os.Rename(tmp, final); err != nil {
		return SegmentMeta{}, err
	}

	meta.Count = len(records)
	meta.MinTS, meta.MaxTS = records[0].Timestamp, records[0].Timestamp
	meta.MinKey, meta.MaxKey = records[0].Key, records[0].Key
	for _, r := range records {
		if r.Timestamp < meta.MinTS {
			meta.MinTS = r.Timestamp
		}
		if r.Timestamp > meta.MaxTS {
			meta.MaxTS = r.Timestamp
		}
		if r.Key < meta.MinKey {
			meta.MinKey = r.Key
		}
		if r.Key > meta.MaxKey {
			meta.MaxKey = r.Key
		}
	}
	if st, err := os.Stat(final); err == nil {
		meta.Size = st.Size()
	}
	return meta, nil
}

// readSegment 读取段文件全部记录。
func readSegment(path string) ([]Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	magic := make([]byte, len(segmentMagic))
	if _, err := io.ReadFull(r, magic); err != nil {
		return nil, fmt.Errorf("read magic: %w", err)
	}
	if string(magic) != string(segmentMagic) {
		return nil, fmt.Errorf("bad segment magic in %s", path)
	}
	var out []Record
	for {
		keyLen, err := binary.ReadUvarint(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(r, key); err != nil {
			return nil, err
		}
		ts, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		valLen, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		val := make([]byte, valLen)
		if _, err := io.ReadFull(r, val); err != nil {
			return nil, err
		}
		out = append(out, Record{Key: string(key), Timestamp: int64(ts), Value: val})
	}
	return out, nil
}

// removeSegmentFile 删除段文件（幂等）。
func removeSegmentFile(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
