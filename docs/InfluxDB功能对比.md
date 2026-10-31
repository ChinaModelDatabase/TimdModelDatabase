# tmDB 与 InfluxDB 功能对比

> 本文基于 tmDB workspace 当前代码（14 crates，约 15.9k 行 Rust）逐项对照 InfluxDB。
> 每条结论都标注了**已验证**（读过实现或跑过测试）或**未实现**，不做推测。

---

## 0. 阅读须知：本文取代 `InfluxDB架构分析.md`

仓库中较早的 `docs/InfluxDB架构分析.md`（2026-09-30）已过时，其中多条 P0 结论
在随后完成的工作中已经不成立。**不要再按那份文档的待办列表排期**：

| 旧文档结论 | 当前实际状态 |
|---|---|
| WAL 仅记录 ts + 行数，崩溃丢数据 | **已修复**。`wal.rs` 为 framed record：`len:u32 + crc32:u32 + payload`，读取在截断或 CRC 不符处停止；含损坏尾修复、checkpoint |
| `CompactionManager::compact()` 是 `todo!()`，调用即 panic | **已修复**。`compaction.rs:238` 为真实实现；全 workspace 无 `todo!()`/`unimplemented!()` |
| `write_block_with_ts` 覆盖 `col_data` 导致多 block 数据丢失 | **已修复**。该 API 已移除，改为 append 语义；zone map 与多 block 一致 |
| 「10M 行 → 29KB，数据实际丢失」 | 该 benchmark 结论作废 |
| 「Tag/String 列不编码」 | 部分修复：String 已接 Dictionary 编码路径，但**未按数据分布动态选型**（见 §4） |

---

## 1. 版本演进与定位

| | InfluxDB 1.x | InfluxDB 2.x | InfluxDB 3.x (Core) | **tmDB** |
|---|---|---|---|---|
| 引擎 | TSM | TSM + WAL | Rust + Apache Arrow / Parquet | 自研 `.tsb` 列存 |
| 查询语言 | InfluxQL | Flux + InfluxQL | SQL (DataFusion) + InfluxQL | 标准 SQL（sqlparser AST） |
| 存储 | 本地文件 | 本地 / 共享 | 对象存储 (S3 等) | 本地文件 |
| 元数据 | 单机 / Raft | Raft 集群 | 无状态（对象存储） | 内存 DashMap，**无持久化元数据** |
| Flux | — | 一等公民 | **3.0 已移除** | 不计划实现 |

**定位差异**：tmDB 走「关系模型 + 标准 SQL」路线，InfluxDB 走「series key + 专用查询语言」路线。
这不是同一条路上的竞速——目标工作负载不同，因此**功能对齐的重点不应是照搬 InfluxDB 的 API 表面**。

---

## 2. 数据模型

**InfluxDB**：`measurement,tag=val,... field=val timestamp`。series key 隐式由 tag 组合决定，
一个 series 内 tag 固定、field 可变——这是按 series 分列压缩的前提。

**tmDB**：`TableMeta` + `ColumnDef { is_tag }`，显式声明 tag 列。

**已验证的差距**：
- tmDB 有 series 概念（`table.rs:554 find_series_by_tags`、`CardinalityGuard::admit_batch`），
  这点**强于**旧文档所述的「没有 series 概念」。
- 但 tag 值同时写入 tag index 和 String 数据列（`lp/src/mapping.rs`），
  属于冗余存储，是正确性优先于压缩率的取舍。
- **无 shard 概念**：全 workspace 未找到 shard 路由代码。单表无限增长，
  所有 segment 常驻同一 `Table`，与 InfluxDB 的按时间分片差距明显。

---

## 3. 写入路径

| 能力 | InfluxDB | tmDB | 状态 |
|---|---|---|---|
| Line Protocol | 原生 | `tmdb-lp` crate | **已实现**（33 测试 + 1 doctest） |
| WAL | 有，崩溃可恢复 | framed + CRC32 + checkpoint | **已实现** |
| MemTable | cache | `memtable.rs` | **已实现** |
| Flush | → TSM/shard file | → `.tsb` segment | **已实现** |
| 写入去重 | 同 series 同 ts 覆盖 | checkpoint marker + segment marker 取 max | **已实现** |
| 基数保护 | 有 series 上限与监控 | `CardinalityGuard` 原子批量准入 | **已实现** |
| 乱序写入 | 支持 | 支持（memtable 按 ts 排序） | **已实现** |

**已验证的 P0 缺陷** — `tmdb-lp/src/parser.rs`：
1. quoted field 内的 `,` 和 `=` 会被错误切分（切分器不跟踪引号状态）
2. quoted string 按单字节构造 `String`，**非 ASCII UTF-8 会被破坏**
3. `IngestReport.unknown_fields` / `type_mismatches` 恒为 0，策略生效但不计数
4. map error 行号按 parsed-point 下标，非原始文本行号

---

## 4. 编码与压缩

**InfluxDB**：时间戳 Delta-of-Delta，浮点 Gorilla，整数 RLE，Tag Dictionary+Bitmap。

**tmDB** 已实现 `DeltaOfDelta` / `Gorilla` / `Rle` / `Dict` / `BitPack`，`EncodingType`
枚举已接入。

**差距**：
- 选型仍**按数据类型固定**，未按数据分布采样动态选择（递增 ID 用 RLE 效果极差）。
- `Dictionary` 对 tag 的收益是最大的，目前未针对 tag 列强制启用。

**已验证的 P0 缺陷** — `encoding/mod.rs:749` `compress_page` / `:769` `decompress_page`：
```
compress_page  : zstd 失败 → 回退 data.to_vec()（未压缩原文）
decompress_page: 一律按 zstd 解压，并猜测容量 max(len*8, 256KiB)
```
压缩失败写出原文、读取时却按 zstd 解压 → **该行数据不可恢复**；容量猜测在压缩比高的页上会截断。
且无任何永久 roundtrip 测试。这是当前最危险的未修项。

---

## 5. 查询

**InfluxDB**：InfluxQL，tag 过滤下推，`GROUP BY time()`，无 JOIN。

**tmDB（本轮刚完成，34 测试全绿）**：
- `tmdb-exec/src/query.rs` 基于 sqlparser AST 的单表执行器：
  projection、WHERE（比较/IN/LIKE/BETWEEN/NULL 三值逻辑/算术）、
  聚合（count/sum/avg/min/max/first/last）、`GROUP BY`（列 + `time()` 桶）、
  ORDER BY（别名/序号）、LIMIT
- `engine.rs` 重写为 AST dispatch；未知语句返回错误而非静默 `Ok(Empty)`
- `time(1m)` 经 token 流折叠后支持（`fold_duration_literals`）
- 桶宽按 `TableMeta.ts_unit` 换算，修正了毫秒表上 `1m` 退化为单桶的问题

**已验证的 P0 缺陷** — `table.rs:814` `Table::scan`：
```rust
let _series_ids = if !tag_filters.is_empty() {
    self.tag_index.find_series(tag_filters).unwrap_or_default()
```
查询出 series ID 后**完全未使用**。tag 过滤被静默忽略，返回全部行。
当前 `query.rs` 只能绕过它逐行比较，存储层下推缺失。

**明确未实现**（返回错误，不返回错误结果）：`HAVING`、`DISTINCT`、JOIN、子查询、
`ALTER TABLE`、`CREATE INDEX`（仅解析校验后明确报「尚无二级索引」）。

---

## 6. 结果类型与线缆协议

这一项是本轮新发现的问题，InfluxDB 侧不存在对应缺陷。

`QueryResult::Rows` 过去只带列名不带类型，消费者只能**按字节长度猜类型**：
- `tmdb-server` 把 8 字节值在 `f == i as f64 && i < 1000000` 时当整数，否则按 f64 打印 → 时间戳被打成 `0.00`
- PG 协议把所有列声明为 `int4`，并把**小端原始字节当 text 格式**发上线
- MySQL 协议同样把原始字节当 text 发送

现已改为 `QueryResult::Rows { columns, types, rows }`，PG/MySQL 按真实类型 OID 声明并
渲染文本。旧测试之所以通过，是因为旧引擎把时间戳写成了浮点。

**仍未实现**：InfluxDB 兼容的 HTTP write/query API。`tmdb-server/src/http.rs`
仅 60 行，只有 `/metrics` 和 `/health`，缺 `/api/v2/write`、`/api/v2/query`、认证与错误格式。

---

## 7. 安全（**最严重的未修项**）

**已验证**：
- `tmdb-wire/src/pg/mod.rs:95` `handle_scram_final` 注释写着
  `// Simplified: accept all SCRAM responses`，随后**无条件** `self.authenticated = true`。
  → **任何密码都能登录**。
- `tmdb-crypto`：`sm4_encrypt` 的 `mode` 参数被命名为 `_mode` 并忽略；
  `TlsConfig::new` 读了证书路径但未见实际握手。
- 无 RBAC、无 TLS 终结、无审计日志。

**结论：tmDB 目前不具备任何可称为「安全」的能力。** 在修掉 SCRAM 之前，
不应部署到任何非本机环境。

---

## 8. 分布式

| | InfluxDB | tmDB |
|---|---|---|
| 元数据一致性 | Raft | 无（内存 DashMap，重启即失） |
| 数据分片 | 按时间 shard | **无** |
| 副本 / 高可用 | 多节点 | 无 |
| 查询 | scatter-gather | 单进程 |

`tmdb-dist`（348 行）自述为内存模拟 Raft，无真实 RPC。
`tmdb-txn` 仅 283 行，MVCC 快照隔离远达不到生产级。

---

## 9. 成熟度总表

| 维度 | InfluxDB | tmDB | 差距 |
|---|---|---|---|
| 崩溃数据安全 | 工业级 | framed WAL + CRC + checkpoint | 基本达标 |
| 查询正确性 | 工业级 | AST 执行器；HAVING/DISTINCT 已实现 | 缺 JOIN |
| tag 过滤下推 | 有 | **已实现**（memtable + segment 双侧过滤） | — |
| 压缩正确性 | Gorilla/RLE/Dict | **动态选型已实现**（按分布采样打分） | — |
| Zstd 路径 | — | **压缩失败即数据损坏**（静默回退 raw） | **P0** |
| 写入协议 | Line Protocol | LP 已实现，2 个解析 bug | **P0** |
| HTTP API | 完整 | 仅 /health /metrics | **P0** |
| 认证 / TLS | 完整 | PG 已真实 SCRAM；MySQL 未实现；无 TLS | **P0** |
| 二级索引 | 有 | CREATE/DROP/维护已实现，**优化器未使用** | P1 |
| 分片 / 副本 | Raft + shard | 存储层时间窗口分片已实现（`shard::tests` 约 20 个测试）；**分布式/Raft 未实现** | P2 |
| 对象存储 | S3 | 无 | P2 |
| Flux | 已移除 | 不实现 | — |

代码规模（`Get-ChildItem -Recurse *.rs` 实测）：
```
tmdb-timestore   11300 行 / 22 文件   ← 唯一实质实现
tmdb-exec         3472 行 /  6 文件
tmdb-wire         2605 行 /  8 文件
tmdb-lp           1423 行 /  4 文件
tmdb-server        743 行 /  5 文件
tmDB CLI           303 行 /  1 文件
tmdb-dist          348 行 /  1 文件
tmdb-txn           283 行 /  1 文件
其余 6 crate          均 < 200 行（占位为主）
```

---

## 10. 优先级建议

### P0 — 正确性与安全（阻塞任何非本机部署）
1. ~~修 `Table::scan` 忽略 `tag_filters`~~ ✅ 已完成（memtable 与 segment 双侧过滤，5 个回归测试）
2. 修 Zstd 压缩/解压不对称（`encoding/mod.rs:749`/`:769`）：`compress_page` 失败时静默回退 raw，
   而 `decompress_page` 恒按 Zstd 解码 → **压缩失败等于数据损坏**；另有 `len*8` 容量猜测会拒绝高膨胀数据
3. 修 `tmdb-lp` quoted `,`/`=` 切分与非 ASCII 字符串破坏（`parse_field_value` 逐 byte 转 `char`）；
   另 `SchemaMapper` 只产出 value columns，与 `insert_batch_with_tags` 的完整 schema 契约不一致
4. ~~实现真正的 SCRAM 校验~~ ✅ PG 侧已完成（RFC 7677 §3 向量逐字节复现，凭据配置强制启动失败）；
   **MySQL `caching_sha2_password` 未实现**，且需先决定 TLS 还是 RSA 公钥方案
5. 实现 `/api/v2/write` 与 `/api/v2/query`（当前 HTTP 有 `/health` 与 `/metrics`：`http::tests::test_metrics_endpoint` 与 `metrics::tests::test_prometheus_*` 约 10 个测试通过；**无 `/ready`、无写入/查询端点**）

### P1 — 性能达标
6. ~~动态编码选型~~ ✅ 已完成（`selector.rs` 采样 ≤4096 值、对候选打分取最小，并接入 `ColumnCodec::encode`）。
   String 列固定走 Dictionary，tag 即 String，故 tag 侧天然满足；无独立的"tag 强制"开关
7. ~~`HAVING` / `DISTINCT`~~ ✅ 已完成（含 HAVING 引用未投影聚合、HAVING 先于 LIMIT、DISTINCT 保序）；
   ~~二级索引~~ ✅ CREATE/DROP/INSERT 维护已完成（10 个测试），**优化器尚未使用索引**，查询仍全表扫描
8. ~~端到端 benchmark~~ ✅ 已完成（`tmdb benchmark`，走完整 SQL 路径，每阶段校验行数）。
   50K 点 / 25 series / 4 列实测：插入 32 K 行/s，全表扫描 235 K 行/s，WHERE 227 K 行/s，
   GROUP BY+HAVING 259 K 行/s → **SQL 插入路径约为扫描的 1/7，是主要瓶颈**

### P2 — 架构完整
9. ~~shard 概念（按时间分片）与 TTL~~ ✅ 存储层已实现（`shard::tests` 约 20 个测试：时间窗口路由/裁剪/压缩/过期；`ttl::tests` 6 个测试通过）；**分布式分片未实现**
10. 元数据持久化 + Raft（注：`tmdb-dist` 的 `test_raft_leader_election`/`test_raft_log_replication`/`test_shard_migration` 通过，但 `simulate_vote_request` 恒返回 `true`，**测试不构成正确性证据**）
11. 对象存储后端 / Parquet 互操作
12. 真实国密（SM3/SM4）与 TLS 终结

### P3 — 生产可运营性与可验证性

> **⚠️ 编号冲突**：根目录 `tmDB-信创时序数据库解决方案.md` §十 定义了另一套 P0-P8 里程碑，
> 其中 **P3 = 混合事务**（关系表 + MVCC + 跨表 Join + time_bucket），P0-P2 含义也不同
> （基础设施 / 存储引擎 / SQL MVP）。两套体系**共用标签、语义不同**。
> 本文的 P3 是依据优先级文档归纳的，**不是**主设计文档的 P3。命名需在源码恢复后统一。

> 定义于 2026-10-03 审计之后。P0/P1/P2 分别覆盖正确性、性能、架构，但审计暴露了一个贯穿性问题：
> **代码库对自身状态的认知不可信**。文档声称未做的 P0/P1 项实际已完成（tag 过滤下推、动态编码、compaction、WAL）；
> 文档声称已实现的 P2 项实际是不可用脚手架（`tmdb-dist` 的 `simulate_vote_request`/`simulate_append_entries`
> 恒返回 `true`，`tmdb-crypto` 的 SM4 是与密钥异或而非加密——两者测试都通过，断言的是"能往返"而非"有共识/有安全性"）；
> 3 个静默正确性缺陷（HAVING/DISTINCT 被忽略、二级索引建了未维护、认证无条件放行）长期存在，
> 因为测试断言"不 panic"而非"结果正确"。
>
> 因此 P3 不是"更多功能"，而是让系统**能被信任**的能力。

13. **可观测性**：查询延迟分布、慢查询日志、WAL/flush/compaction 分阶段指标；当前 HTTP 仅 `/health` 与 `/metrics`
14. **状态自校验**：由代码推导实现状态的持续检查，取代手写文档状态——本次审计发现状态在**两个方向**上都失真
15. **故障注入与恢复验证**：WAL checkpoint 失败、压缩失败、磁盘写满等场景的可重复测试；当前无任何故障注入测试
16. **授权层**：PG SCRAM 现可认证，但执行引擎无任何 authorization——任意认证用户可读写任意表
17. **配置面收敛**：`tmdb.toml`、CLI 参数与环境变量并存，需统一来源与优先级
18. **优雅关闭与 drain**：退出前同步 flush/compaction；当前在 async executor 上同步执行阻塞操作

**顺序提示**：P2 项（9-12）应排在 P0 遗留项之后。分布式/分片会把 Zstd 静默回退 raw（= 压缩失败即数据损坏）
与 WAL 未 fsync（= 断电丢数据）的影响面从单机放大到集群。

> **复核状态**：本节定义基于 2026-10-03 审计所见；工作区源码恢复后需逐项复核后再排期。

---

*生成时间：2026-10-03*
*对照代码：tmDB workspace，14 crates；`cargo test --workspace` 354 测试 + 1 doctest 全绿*
