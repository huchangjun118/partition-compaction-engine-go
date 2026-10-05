// Package engine 实现本地分区存储引擎：按时间范围与主键分区写入，
// 支持分区裁剪查询、小文件合并、保留期淘汰与冷热分级存储。
package engine

import "time"

// Event 是一条事件记录，ID 为主键，Timestamp 为 UnixNano 时间戳。
type Event struct {
	ID        string `json:"id"`
	Timestamp int64  `json:"ts"`
	Payload   []byte `json:"payload,omitempty"`
}

// Query 描述一次查询：时间范围 [Start, End)（UnixNano），
// Keys 非空时仅匹配这些主键（等值集合），为空表示不限主键。
type Query struct {
	Start int64
	End   int64
	Keys  []string
}

// Tier 表示存储层级。
type Tier string

const (
	TierHot  Tier = "hot"
	TierCold Tier = "cold"
)

// FileMeta 描述一个数据文件的元信息。
type FileMeta struct {
	ID        string `json:"id"`
	Partition string `json:"partition"`
	Tier      Tier   `json:"tier"`
	Path      string `json:"path"` // 相对引擎根目录
	MinTS     int64  `json:"min_ts"`
	MaxTS     int64  `json:"max_ts"`
	Count     int    `json:"count"`
	Size      int64  `json:"size"`
}

// JobState 表示分区后台任务状态。
type JobState string

const (
	JobIdle    JobState = "idle"
	JobPending JobState = "pending"
	JobRunning JobState = "running"
	JobFailed  JobState = "failed"
	JobDone    JobState = "done"
)

// PartitionStatus 是分区的可观测状态。
type PartitionStatus struct {
	Partition string    `json:"partition"`
	State     JobState  `json:"state"`
	FileCount int       `json:"file_count"`
	LastError string    `json:"last_error,omitempty"`
	LastRun   time.Time `json:"last_run,omitempty"`
}

// Config 是引擎配置。配置变更只影响后续调度，不影响已落盘数据。
type Config struct {
	// TimeBucket 是分区的时间桶宽度，默认 24h。
	TimeBucket time.Duration
	// HashBuckets 是主键哈希桶数，默认 16。注意：落盘后修改会影响
	// 新写入的分区归属，已有分区仍可被正确读取（按元数据裁剪）。
	HashBuckets int
	// CompactThreshold：分区内文件数达到该值触发合并，默认 8。
	CompactThreshold int
	// CompactWorkers 是合并并发度，默认 2。
	CompactWorkers int
	// Retention 是保留窗口，MaxTS 早于 now-Retention 的文件被淘汰；0 表示不淘汰。
	Retention time.Duration
	// HotWindow：MaxTS 早于 now-HotWindow 的文件迁移到冷层；0 表示不分层。
	HotWindow time.Duration
	// SweepInterval 是后台调度周期，默认 1s。
	SweepInterval time.Duration
}

func (c Config) withDefaults() Config {
	if c.TimeBucket <= 0 {
		c.TimeBucket = 24 * time.Hour
	}
	if c.HashBuckets <= 0 {
		c.HashBuckets = 16
	}
	if c.CompactThreshold <= 0 {
		c.CompactThreshold = 8
	}
	if c.CompactWorkers <= 0 {
		c.CompactWorkers = 2
	}
	if c.SweepInterval <= 0 {
		c.SweepInterval = time.Second
	}
	return c
}
