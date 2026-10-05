package engine

import "time"

// Record 是一条事件数据。
type Record struct {
	Key       string `json:"key"`
	Timestamp int64  `json:"ts"` // 毫秒时间戳
	Value     []byte `json:"value"`
}

// Query 描述一次查询条件：时间范围 + 主键集合（为空表示不限主键）。
type Query struct {
	StartTS int64
	EndTS   int64 // 包含
	Keys    []string
}

// Tier 表示存储层级。
type Tier int

const (
	TierHot Tier = iota
	TierCold
)

func (t Tier) String() string {
	if t == TierCold {
		return "cold"
	}
	return "hot"
}

// TaskState 表示后台任务（合并/迁移/淘汰）状态。
type TaskState int

const (
	TaskPending TaskState = iota
	TaskRunning
	TaskDone
	TaskFailed
)

func (s TaskState) String() string {
	switch s {
	case TaskPending:
		return "pending"
	case TaskRunning:
		return "running"
	case TaskDone:
		return "done"
	case TaskFailed:
		return "failed"
	}
	return "unknown"
}

// TaskInfo 是后台任务的可观测信息。
type TaskInfo struct {
	ID        string
	Partition PartitionKey
	Kind      string // "compact" | "migrate" | "expire"
	State     TaskState
	Err       string
	CreatedAt time.Time
	EndedAt   time.Time
}

// PartitionKey 标识一个分区：时间桶 + 主键桶。
type PartitionKey struct {
	TimeBucket int64 `json:"time_bucket"`
	KeyBucket  int   `json:"key_bucket"`
}

// Config 是引擎配置。配置变更只影响后续调度，不影响已落盘数据。
type Config struct {
	// 分区
	KeyBuckets       int           // 主键哈希桶数
	TimeBucketWindow time.Duration // 时间桶宽度
	// 合并
	CompactionThreshold   int // 分区内小文件数达到该值触发合并
	CompactionConcurrency int // 合并 worker 并发度
	// 保留期与分层
	RetentionWindow time.Duration // 超过该窗口的数据淘汰
	ColdAfter       time.Duration // 段文件创建超过该时长迁移到冷层
	// 调度
	ScheduleInterval time.Duration // 后台调度周期
}

func (c *Config) withDefaults() Config {
	out := *c
	if out.KeyBuckets <= 0 {
		out.KeyBuckets = 4
	}
	if out.TimeBucketWindow <= 0 {
		out.TimeBucketWindow = time.Hour
	}
	if out.CompactionThreshold <= 0 {
		out.CompactionThreshold = 4
	}
	if out.CompactionConcurrency <= 0 {
		out.CompactionConcurrency = 2
	}
	if out.ScheduleInterval <= 0 {
		out.ScheduleInterval = 100 * time.Millisecond
	}
	return out
}
