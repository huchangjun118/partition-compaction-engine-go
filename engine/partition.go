package engine

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
)

// 分区命名：t=<bucketWidthNS>:<bucketIndex>/h<mod>=<hashBucket>。
// 时间桶宽度与哈希模数都编码在分区名中，因此修改 TimeBucket 或
// HashBuckets 配置后，已有分区仍按各自写入时的参数正确裁剪，
// 已落盘数据无需迁移或重建。

// partitionOf 计算事件所属分区：时间桶 + 主键哈希桶。
func partitionOf(id string, ts int64, cfg Config) string {
	width := cfg.TimeBucket.Nanoseconds()
	return fmt.Sprintf("t=%d:%d/h%d=%d", width, ts/width, cfg.HashBuckets, hashBucket(id, cfg.HashBuckets))
}

func hashBucket(id string, n int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return int(h.Sum32() % uint32(n))
}

// parsePartition 解析分区名，返回时间桶宽度、时间桶下标、哈希模数与哈希桶号。
func parsePartition(p string) (width, timeBucket int64, mod int, hash int, ok bool) {
	parts := strings.Split(p, "/")
	if len(parts) != 2 || !strings.HasPrefix(parts[0], "t=") || !strings.HasPrefix(parts[1], "h") {
		return 0, 0, 0, 0, false
	}
	tkv := strings.SplitN(parts[0][2:], ":", 2)
	if len(tkv) != 2 {
		return 0, 0, 0, 0, false
	}
	w, err0 := strconv.ParseInt(tkv[0], 10, 64)
	tb, err1 := strconv.ParseInt(tkv[1], 10, 64)
	hkv := strings.SplitN(parts[1][1:], "=", 2)
	if len(hkv) != 2 {
		return 0, 0, 0, 0, false
	}
	m, err2 := strconv.Atoi(hkv[0])
	h, err3 := strconv.Atoi(hkv[1])
	if err0 != nil || err1 != nil || err2 != nil || err3 != nil || w <= 0 || m <= 0 {
		return 0, 0, 0, 0, false
	}
	return w, tb, m, h, true
}

// prunePartitions 根据查询条件从分区集合中裁剪出候选分区：
//   - 时间裁剪：按分区自身记录的桶宽计算覆盖范围，与 [Start, End) 不相交则排除；
//   - 主键裁剪：Keys 非空时，按分区自身记录的模数计算 key 的桶号，
//     不在集合中则排除。
func prunePartitions(partitions []string, q Query, cfg Config) []string {
	var out []string
	for _, p := range partitions {
		width, tb, mod, h, ok := parsePartition(p)
		if !ok {
			out = append(out, p) // 无法解析的分区保守保留
			continue
		}
		lo, hi := tb*width, (tb+1)*width
		if hi <= q.Start || lo >= q.End {
			continue
		}
		if len(q.Keys) > 0 {
			match := false
			for _, k := range q.Keys {
				if hashBucket(k, mod) == h {
					match = true
					break
				}
			}
			if !match {
				continue
			}
		}
		out = append(out, p)
	}
	return out
}
