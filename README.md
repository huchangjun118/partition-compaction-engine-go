# partition-compaction-engine-go

本地分区存储引擎，面向事件类数据的长期存储与按条件查询。数据按
**时间范围 + 主键哈希** 二级分区写入，支持分区裁剪查询、小文件合并、
保留期淘汰与冷热分级存储，全部后台操作可与写入/查询并发进行，
并具备崩溃恢复能力。

## 快速开始

```go
e, err := engine.Open("/data/events", engine.Config{
    TimeBucket:       24 * time.Hour, // 分区时间桶宽度
    HashBuckets:      16,             // 主键哈希桶数
    CompactThreshold: 8,              // 分区内文件数达到该值触发合并
    CompactWorkers:   2,              // 合并并发度
    Retention:        30 * 24 * time.Hour, // 保留窗口，0 表示不淘汰
    HotWindow:        7 * 24 * time.Hour,  // 热窗口，0 表示不分层
    SweepInterval:    time.Second,    // 后台调度周期
})
defer e.Close()

err = e.Write(engine.Event{ID: "user-1", Timestamp: time.Now().UnixNano(), Payload: []byte("...")})

events, err := e.Query(engine.Query{
    Start: start.UnixNano(),   // [Start, End)
    End:   end.UnixNano(),
    Keys:  []string{"user-1"}, // 可选，主键等值集合
})
```

## 数据布局

```
root/
  hot/  t=<桶宽ns>:<时间桶>/h<模数>=<哈希桶>/d-<文件ID>.jsonl   # 热层数据
  cold/ t=<桶宽ns>:<时间桶>/h<模数>=<哈希桶>/d-<文件ID>.jsonl   # 冷层数据
  manifest/manifest.json                              # 文件清单（原子替换）
  journal/op-<ID>.json                                # 后台操作日志（崩溃恢复）
```

- 每次 `Write` 按分区分组，每个分区写出一个 JSON Lines 小文件，
  随后一次清单提交使整批数据原子可见。
- 分区名中编码了写入时的时间桶宽度与哈希模数，因此修改
  `TimeBucket` / `HashBuckets` 后，旧分区仍按各自写入时的参数
  正确裁剪，已落盘数据无需迁移或重建。

## 一致性语义

- **快照读**：清单按版本管理且版本内不可变。查询先获取当前版本快照
  （引用计数），整个查询期间看到的就是该版本的完整视图，
  不会看到合并/迁移/淘汰的中间状态。
- **原子切换**：合并、迁移、淘汰都先写出新文件，再通过
  tmp+rename+fsync 原子替换清单生效；旧文件只有在不再被任何
  活跃快照引用后才物理删除。正在被读取的数据不会被删除或替换。
- **查询正确性**：查询结果按 `(Timestamp, ID)` 排序，与绕过裁剪的
  全量扫描逐行一致——分区裁剪（时间桶 + 主键哈希桶 + 文件级时间范围）
  只减少读取量，不改变结果集。
- **崩溃恢复**：每个后台操作先写 journal（意图）→ 提交清单 →
  标记 journal 完成。重启时重放 journal：
  - 清单未包含新文件 → 回滚：删除半成品文件；
  - 清单已包含新文件 → 前滚：清理应删除的旧文件；
  - 最后清扫所有不在清单中的孤儿文件与临时文件。
  全程幂等，不重复累计也不丢数据。
- **并发**：合并按分区独立推进（每分区互斥 + 全局并发度上限），
  慢分区不阻塞其他分区的写入与查询。

## 配置项

| 配置 | 默认 | 说明 |
|---|---|---|
| `TimeBucket` | 24h | 分区时间桶宽度 |
| `HashBuckets` | 16 | 主键哈希桶数（变更只影响新写入的分区归属） |
| `CompactThreshold` | 8 | 分区内文件数达到该值触发合并 |
| `CompactWorkers` | 2 | 合并任务并发度 |
| `Retention` | 0（不淘汰） | 保留窗口，`MaxTS < now-Retention` 的文件被淘汰 |
| `HotWindow` | 0（不分层） | 热窗口，`MaxTS < now-HotWindow` 的热层文件迁移到冷层 |
| `SweepInterval` | 1s | 后台调度周期 |

`SetConfig` 可在运行时变更配置，只影响后续调度决策，
已落盘数据与清单无需任何迁移或重建。

## 可观测性

`Status()` 返回各分区后台任务状态：`idle / pending / running / done /
failed`，附带文件数、最近错误与最近执行时间，可区分等待、进行中与失败。

## 测试

```sh
go test ./... -race
```

覆盖：分区裁剪正确性（随机数据对拍全量扫描）、合并前后结果一致、
并发读写与合并、快照隔离、保留期淘汰、分层迁移、配置变更、
崩溃回滚/前滚/孤儿清扫、重启持久性。
