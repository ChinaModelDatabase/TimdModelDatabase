# InfluxDB 架构分析（对照 tmDB）

本文分析 InfluxDB 的核心架构，并逐项对照 tmDB 当前实现，指出可借鉴之处与现存缺陷。

---

## 1. InfluxDB 概述

InfluxDB 是面向时序数据的开源数据库，采用 Go（1.x）/Rust（2.x+/3.x IOx）编写，核心特性：

- **Line Protocol**：行式写入协议，高效紧凑
- **TSM 引擎**：Time-Structured Merge Tree，列式存储 + Gorilla 压缩
- **无事务**：仅支持简单读写，无 JOIN（Flux 有部分聚合能力）
- **基数陷阱**：series 基数（高基数标签）会导致内存爆炸
- **水平扩展**：InfluxDB 3.x（IOx）引入 Rust 重写与对象存储

### 版本演进与架构差异

| 版本 | 存储引擎 | 查询语言 | 架构 |
|---|---|---|---|
| 1.x | TSM | InfluxQL | 单机/集群 Raft 元数据 |
| 2.x | TSM | Flux + SQL | 混合 |
| 3.x (IOx) | Parquet/对象存储 | SQL + InfluxQL | 云原生，无状态 |

---

## 2. 数据模型

### 2.1 InfluxDB 模型

```text
measurement,tag_key=tag_value,... field_key=field_value timestamp
```

- **Measurement**：类似表名
- **Tags**：索引维度（字符串），全部作为 series key
- **Fields**：实际数值（float/int/string/bool）
- **Series**：measurement + tag 组合的唯一键

**关键约束**：一个 series 内所有 tag 值固定，field 值可变。这是列式压缩（按 series 分列）的前提。

### 2.2 tmDB 对照

tmDB 采用经典关系模型，`TableMeta` + `ColumnDef{is_tag}`（lib.rs:25）：

```rust
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub is_tag: bool,   // 标记 tag 列
    pub nullable: bool,
    pub compression: CompressionLevel,
}
```

**差异**：InfluxDB 的 series 隐式由 tag 组合定义；tmDB 显式声明 tag 列，但 **没有 series 概念**——同一 tag 组合的不同数据分散在同一个 table 的不同 segment 中。这会影响压缩率（跨 tag 混合）和索引效率。

---

## 3. 存储引擎：TSM

### 3.1 TSM 格式

```text
┌──────────────────────────┐
│ Index (series key → offset) │  内存常驻
├──────────────────────────┤
│ Series 1: [ts][field...][ts][field...] │
│ Series 2: [ts][field...][ts][field...] │
│ ...                       │
└──────────────────────────┘
```

- 每个 series 一个数据块，按时间追加写
- 文件内含多个 series，index 记录偏移
- **Gorilla 压缩**浮点，**RLE** 压缩整数

### 3.2 写入路径

```text
Write Request → WAL (预写日志) → MemTable/Cache → Flush → TSM File
```

InfluxDB 使用 **shard** 概念（按时间范围分片，默认 7 天），每个 shard 独立持有 WAL + Cache + TSM。

### 3.3 tmDB 对照

tmDB 已有：

| 组件 | 文件 | 状态 |
|---|---|---|
| MemTable | memtable.rs | 已实现（BTreeMap + 行数/字节阈值） |
| WAL | table.rs:238 | **已写入完整数据**（`wal.rs::encode_rows` 逐列带长度前缀与 NULL 标记）；`test_values_survive_reopen_byte_for_byte`、`test_string_values_with_arbitrary_bytes`、`test_nulls_survive` 均通过。**但写入路径 `durable=false`，不 fsync**（断电丢数据） |
| Segment 格式 (.tsb) | segment/file.rs | 已实现（Header+列数据+Footer） |
| Shard 概念 | shard.rs | **存储层时间窗口分片已实现**：`shard::tests` 约 20 个测试通过（路由/裁剪/压缩/过期/重开）。**分布式分片未实现** |

**WAL 严重缺陷**（table.rs:238 `write_wal_entry`）：

```rust
// 注释明确写 "simple: timestamp + row count for crash recovery"
// 实际只写入 marker + 行数，崩溃后无法恢复数据
```

这不满足"持久化"承诺——WAL 必须写入完整行数据或引用 segment 位置。

---

## 4. 编码与压缩

### 4.1 InfluxDB 编码策略

| 类型 | 编码 | 压缩比 |
|---|---|---|
| 时间戳 | Delta-of-Delta | ~10x |
| 浮点 | Gorilla (XOR) | ~5-10x（实际更高） |
| 整数 | RLE | 视数据分布 |
| Tag | Dictionary + Bitmap | 极高 |

### 4.2 tmDB 编码实现

已实现（encoding/ 目录）：

- `DeltaOfDeltaEncoder`（时间戳）
- `GorillaEncoder`（Float64/32）
- `RleEncoder`（Int64/32）
- `DictEncoder`（字典）
- `BitPackEncoder`（位压缩）

**编码选型缺陷**（table.rs:188-202）：

```rust
let encoded = match col_def.data_type {
    DataType::Timestamp => DeltaOfDeltaEncoder::new().encode(...)?,
    DataType::Float64 | DataType::Float32 => GorillaEncoder::new().encode(...)?,
    DataType::Int64 | DataType::Int32 => RleEncoder::new().encode(...)?,
    _ => values,  // String/其他类型未编码，直接存原始字节
};
```

问题：
1. **按数据类型固定编码**，未按数据分布动态选择。RLE 对递增 ID 极差，应换 Delta。
2. **String/Tag 列不编码**，`values` 直接存原始字节，与 InfluxDB 的 Dictionary 编码差距巨大。
3. **缺少 EncodingType 枚举的实际使用**——`EncodingType` 已定义但未接入选型逻辑。

---

## 5. Compaction

### 5.1 InfluxDB Compaction

L0（memtable flush）→ L1（合并去重）→ L2+（最终压缩），每级维护独立 index。Compaction 是后台 goroutine，与写入解耦。

### 5.2 tmDB Compaction

**当前状态**：

- `CompactionManager::compact()` 是 `todo!()`（compaction.rs:105），**调用即 panic**。
- `Table::compact_l0()` 实现了简化合并，但仅两两合并（table.rs:511 `merge_segments`）。
- 自动触发条件：`l0_count >= 4`（table.rs:228）。

**segment/file.rs:196 严重 bug**：

```rust
pub fn write_block_with_ts(&mut self, block: Vec<Vec<u8>>, ...) {
    self.col_data = block;  // BUG: 覆盖而非追加！
    ...
}
```

`finish()` 只写最后一个 `col_data`，导致：
- 多 block segment 丢失除最后 block 外的所有数据
- ZoneMap 记录了多个 block，但数据不存在
- **这解释了此前 benchmark 的异常压缩比**（10M 行 → 29KB，数据实际丢失）

修复：`self.col_data` 应改为 `Vec<Vec<u8>>` 的 extend 或 append 结构。

---

## 6. 查询路径

### 6.1 InfluxQL

```sql
SELECT mean("value") FROM "measurement" 
WHERE "host"='server01' AND time > now() - 1h 
GROUP BY time(5m), "region"
```

- **Pushdown**：tag 过滤下推到 storage 层
- **Group by time**：时间窗口聚合
- 无 JOIN、无子查询（1.x）

### 6.2 tmDB 查询

- SQL 解析器（tmdb-sql）：支持 SELECT/INSERT/CREATE 等基础语法
- `Table::scan(ts_min, ts_max, tag_filters, select_columns)`：支持时间范围 + tag 过滤 + 列裁剪
- ZoneMap 块级跳过（table.rs:371）

**差异**：tmDB 走标准 SQL 路线（可 JOIN），InfluxDB 走专用查询语言（无 JOIN）。tmDB 在分析类查询（多表 JOIN、聚合）更有优势，InfluxDB 在单表时序聚合更轻量。

---

## 7. Series 基数问题

InfluxDB 最大的架构债：

```
series 基数 = measurement × Π(每个 tag 的不同值数量)
```

例如 1000 个 host × 10 个 region × 5 个 service = 50,000 series/measurement，每个 series 在内存 index 都有条目。

**tmDB 应对策略**：
- TagIndex（table.rs）已有基础实现
- 但缺少 InfluxDB 的 **series 数量限制** 和 **基数监控**

---

## 8. 分布式架构

### InfluxDB 集群（2.x）

- **Meta Store**：Raft 集群，存储 database/RP/continuous query 定义
- **Data Node**：本地 TSM 引擎，无状态
- **Write Proxy**：路由到分片
- **Querier**：scatter-gather 聚合

### tmDB 对照

`tmdb-dist/src/lib.rs`：模拟 Raft + 分片，但 **无真实网络 RPC**，仅内存结构。

---

## 9. 关键设计借鉴

| InfluxDB 机制 | tmDB 现状 | 建议 |
|---|---|---|
| **WAL 完整预写** | ✅ 已写入完整数据（`encode_rows` 逐列带长度前缀+NULL 标记，多轮 roundtrip 测试通过）；**但不 fsync**，断电丢数据 | 保持完整写入；**补 fsync 或提供持久化档位** |
| **Dictionary 编码 Tag** | ✅ 已实现：String 列在 `select_encoding` 中固定走 Dictionary | — |
| **动态编码选型** | ✅ 已实现：`selector.rs` 采样 ≤4096 值、对候选打分取最小，已接入 `ColumnCodec::encode` | — |
| **Shard 按时间分片** | ✅ 存储层已实现：`shard::tests` 约 20 个测试通过（时间窗口路由/裁剪/压缩/过期） | **分布式分片**（跨节点）仍未实现 |
| **Series 基数限制** | ✅ 已实现：`cardinality::tests` 约 25 个测试通过（含并发上限拒绝） | 监控告警暴露未验证 |
| **后台 compaction** | ✅ 已实现：`test_background_maintenance_runs_and_stops` 通过；全 workspace 无 `todo!()`/`unimplemented!()` | "线程池"实现细节未验证 |
| **Block 级索引** | ZoneMap 单块；`test_zone_map_prunes_blocks`、`test_for_each_block_column_visits_only_matches` 通过 | 每 segment 多 block |

---

## 10. 总结对比

| 维度 | InfluxDB | tmDB |
|---|---|---|
| **数据模型** | Series = measurement + tags | 关系模型 + tag 标记 |
| **写入协议** | Line Protocol | SQL (INSERT) + Line Protocol 导入 |
| **存储格式** | TSM (Go) / Parquet (IOx) | .tsb 自定义列存 |
| **压缩** | Gorilla + RLE + Dictionary | Gorilla + RLE + DoD（Dictionary 未接入） |
| **查询** | InfluxQL（无 JOIN） | 标准 SQL（可 JOIN） |
| **事务** | 无 | MVCC Snapshot Isolation |
| **扩展** | Raft 元数据 + 分片 | 模拟 Raft（未实现 RPC） |
| **成熟度** | 生产级（10+ 年） | 原型阶段 |

**核心差距**（2026-10-03 复核）：
1. **WAL 写入未 fsync** — `accept_rows` 走 `wal.append(durable=false)`，进程崩溃可恢复（framed + CRC + 回放），**断电仍会丢数据**
2. ~~segment 多 block 写入有 bug~~ ✅ 已由 `test_multiple_blocks_all_survive`、`test_block_boundaries_do_not_overlap_or_gap` 覆盖
3. ~~Tag/字符串无编码~~ ✅ 已实现 Dictionary（`DictStringEncoder`）
4. ~~compaction 未完成~~ ✅ `CompactionManager::compact()` 已实现，全 workspace 无 `todo!()`/`unimplemented!()`
5. ~~无 shard 概念~~ ✅ 存储层时间窗口分片已实现（`shard::tests` 约 20 个测试通过）；**分布式分片仍未实现**

**核心优势**：
1. **标准 SQL 支持** — JOIN 能力强于 InfluxDB
2. **MVCC 事务** — 数据一致性保证
3. **PG/MySQL 双协议** — 生态兼容
4. **信创平台适配** — 国产化需求

---

## 11. 后续行动建议

> 下列状态已于 2026-10-03 复核。[x] 表示已完成并有回归测试覆盖。

### P0（阻塞生产）
- [x] ~~修复 `SegmentWriter::write_block_with_ts` 覆盖 bug~~ — 该函数已不存在，多 block 写入由 `test_multiple_blocks_all_survive` 覆盖
- [ ] **WAL 写入未 fsync** — 列数据已完整写入（`encode_rows` 逐列带长度前缀与 NULL 标记），但 `accept_rows` 调用的是 `wal.append` 而非 `append_durable`，**断电仍会丢数据**
- [x] ~~实现 `CompactionManager::compact()`~~ — 已实现

### P1（性能达标）
- [x] ~~Tag/String 列接入 Dictionary 编码~~ — String 列在 `select_encoding` 中固定走 Dictionary
- [x] ~~动态编码选型（按数据分布采样）~~ — `selector.rs` 采样 ≤4096 值、对候选打分取最小，已接入 `ColumnCodec::encode`
- [x] ~~修复 benchmark 计时逻辑~~ — criterion bench 已在计时区外预建批次；`tmdb benchmark` 端到端已实现，每阶段校验行数

### P2（架构完善）
- [x] ~~引入 Shard 概念（按时间分片）~~ — 存储层已实现，`shard::tests` 约 20 个测试通过
- [x] ~~Series 基数限制~~ — `cardinality::tests` 约 25 个测试通过（含并发上限）；监控告警暴露未验证
- [~] 后台 compaction — `test_background_maintenance_runs_and_stops` 通过；"线程池"实现细节未验证
- [ ] **分布式** RPC / Raft — 未实现；`tmdb-dist` 3 个测试通过但 `simulate_*` 恒返回 `true`，测试不构成正确性证据

> 本节 P2 与 `InfluxDB功能对比.md` 的 P2 条目不一致：那边是"元数据持久化 + Raft / 对象存储 / 国密 TLS"，
> 这边是"Series 基数 / compaction 线程池 / 分布式 RPC"。两份文档的 P2 从未对齐，恢复后需合并为单一事实来源。

### P3（生产可运营性与可验证性）

> **⚠️ 编号冲突**：根目录 `tmDB-信创时序数据库解决方案.md` §十 定义了另一套 P0-P8 里程碑，
> 其 **P3 = 混合事务**，且 P0-P2 含义不同（基础设施 / 存储引擎 / SQL MVP）。
> 两套体系共用标签、语义不同，需统一。本文的 P3 依据优先级文档归纳，非主设计文档的 P3。

> 定义于 2026-10-03 审计之后。核心问题不是功能不足，而是**代码库对自身状态的认知不可信**：
> 文档说没做的实际已做（tag 过滤、动态编码、compaction、WAL），文档说已做的实际是空壳
> （`tmdb-dist` 的 vote/replicate 恒返回 `true`，`tmdb-crypto` 的 SM4 是与密钥异或，
> 两者测试都通过——断言"能往返"而非"有共识/有安全性"）；3 个静默正确性缺陷长期存在
> （HAVING/DISTINCT 被忽略、二级索引建了未维护、认证无条件放行），因为测试断言"不 panic"而非"结果正确"。

- [ ] **可观测性**：查询延迟分布、慢查询日志、WAL/flush/compaction 分阶段指标（当前仅 `/health` 与 `/metrics`）
- [ ] **状态自校验**：由代码推导实现状态的持续检查，取代手写文档状态——本次审计发现状态在两个方向上都失真
- [ ] **故障注入与恢复验证**：WAL checkpoint 失败、压缩失败、磁盘写满等场景的可重复测试（当前无任何故障注入测试）
- [ ] **授权层**：PG SCRAM 现可认证，但执行引擎无 authorization——任意认证用户可读写任意表
- [ ] **配置面收敛**：`tmdb.toml`、CLI 参数与环境变量并存，需统一来源与优先级
- [ ] **优雅关闭与 drain**：退出前同步 flush/compaction；当前在 async executor 上同步执行阻塞操作

> **顺序提示**：P2 应排在 P0 遗留项（Zstd 静默回退 raw = 压缩失败即数据损坏；WAL 未 fsync = 断电丢数据）
> 之后。分布式会把这两个影响面从单机放大到集群。

> **复核状态**：本节定义基于 2026-10-03 审计所见；工作区源码恢复后需逐项复核后再排期。
> 注：审计期间工作区源码出现不可访问状态，`.git` 无任何提交历史（0 objects），故本节无法对照代码逐项确认。

---

*分析完成时间：2026-09-30*
*状态复核时间：2026-10-03*
*P3 定义时间：2026-10-03*
*对照代码版本：tmDB workspace (14 crates)*
