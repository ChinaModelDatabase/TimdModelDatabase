# tmDB — 信创时序数据库解决方案

> 纯 Rust · 标准 SQL · **PostgreSQL + MySQL 双协议原生** · 时序表与关系表同库同事务
> 版本 v1.1 | 日期 2026-09-29

---

## 一、对标结论：抄谁的长板

| 能力 | 对标对象 | 采纳的设计 | 不采纳的部分 |
|---|---|---|---|
| 标签/高基数 | **TDengine** | tag 表与测点列物理分离、超级表 STable、tag 值字典化 + 倒排 | C/C++ 引擎、专用 SQL 方言 |
| 列存 + 压缩 | **VictoriaMetrics** | 时序专用编码（delta-of-delta + Gorilla）、无外部依赖、小文件、极低资源占用 | 只做指标、不支持关系表与 Join |
| 关系融合/事务 | **TimescaleDB** | 时序表与普通表同库共事务、可 Join | 依赖 PG 全部特性、写放大 |
| 生态兼容 | **InfluxDB** | SQL 化主入口、Prometheus/Line 协议可导入 | 非 SQL 优先、易基数爆炸的 tag 设计 |
| 存算一体/低门槛 | **TDengine / IoTDB** | 单机即完整系统、边缘可独立部署 | — |
| 统一写入层 | **IoTDB** | WAL + 段文件 + 压缩合并的简单 LSM | 树模型、复杂自定义算子 |

**一句话定位**：

> 存算一体、纯 Rust、**PostgreSQL（5432）+ MySQL（3306）双协议同端口族**、**标准 SQL**、**时序表与关系表同库同事务可 Join**、面向国产 CPU/OS 的轻量级 TSDB。

这条“同库同事务 Join”是绝大多数国产 TSDB 都没有的差异点（TDengine / IoTDB 无跨表 Join 与事务），也是最容易在信创项目里拿到验收分的点。

第二条差异化是 **协议层**：国产信创现场往往同时存在 MySQL 生态（老业务）与 PG 生态（新兴工具），单一协议会导致一半工具连不上。tmDB 在**同一个进程、同一个存储、同一个事务系统**上同时提供 MySQL 和 PostgreSQL 协议，客户端用哪个都能连、看到的是同一份数据 —— 这是达梦/人大金仓/OceanBase 都在做但时序产品都没做的事。

---

## 二、目标与非目标

### 2.1 功能目标

1. **标准 SQL**：DDL / DML / DQL / TCL，语法对齐 PostgreSQL 与 MySQL 双方言
2. **双协议同源**：单进程同时监听 **PostgreSQL 5432** 与 **MySQL 3306**，共享同一 Catalog / 同一 WAL / 同一事务，两端可跨协议互相连（MySQL 客户端读 PG 建的表，反之亦然）
3. **方言互转**：内置 PG ⇄ MySQL 双向方言翻译层，SQL 归一到统一内部 AST 后共用同一套优化器与执行器
4. **时序增强**：`time_bucket()`、`first/last/argmax/slope/histogram/percentile`、连续聚合、TTL 保留（两端同义函数名）
5. **混合负载**：时序表 ↔ 普通表 Join，同事务可见性
6. **高基数**：tag 字典化 + 倒排，不因 series 数爆炸 OOM
7. **零外部依赖**：单个静态二进制，`tar -xzf` 即可跑
8. **国产工具链直连**：Smartbi、帆软 FineBI、DBeaver、DataEase、Navicat、Grafana、DataX 均免改造

### 2.2 信创适配矩阵（硬指标）

| 类别 | 目标平台 |
|---|---|
| CPU | 飞腾 S2500/D2000 (ARM64)、鲲鹏 920 (ARM64)、申威 3231 (ARM64)、龙芯 3A5000/3C5000 (**LoongArch64**)、海光 7000 / 兆芯 (x86_64) |
| OS | 银河麒麟 V10 SP3（ARM/LoongArch）、统信 UOS Server 20、openEuler 22.03 LTS-SP3、龙蜥 Anolis OS 23 |
| 兼容库 | 达梦 DM8、人大金仓 KingBaseES V9、GaussDB、OceanBase 社区版、TDSQL |
| 协议 | **PostgreSQL wire 3.0 (5432)**、**MySQL protocol 4.1 / COM_QUERY (3306)**、HTTP/REST (8080) |
| 驱动 | JDBC(pg/mysql 双 jar)、ODBC(PSQLODBC/MySQL ODBC)、.NET MySqlConnector、Npgsql、go-sql-driver/mysql、rust-postgres、mysql_async、R语言 DBI |
| 工具链 | Smartbi、帆软 FineBI、DBeaver、DataEase、Navicat、Grafana、DataX、Canal/Debezium（CDC）、DolphinScheduler、DataXWriter |
| 密码 | 传输 SM2/SM3/SM4（国密套件），静态 SM4 加密 + SM3 校验 |
| 交叉编译 | `cross-rs` + LLVM 15+（LoongArch 后端）、`cargo-vet` 供应链锁定 |

### 2.3 非目标（v1 明确不做）

- 机器学习 / UDF 插件框架（v2 再说）
- 跨库 2PC 分布式事务
- 全文检索、多维空间索引
- 亿级 series 的长周期实时降采样（v2）
- MySQL 独有高级特性：存储过程/触发器/事件调度器/`ON DUPLICATE KEY` 之外的存储引擎特性、Binlog 复制、GROUP BY 隐式排序优化
- MySQL 8.0 `caching_sha2_password` 全流程（v1 支持 `mysql_native_password` 与 `sha256_password`，v1.1 补 `caching_sha2_password` 快速路径）

---

## 三、整体架构

```
┌──────────────────────────────────────────────────────────────────────────┐
│ 接入层    ┌ PostgreSQL Wire :5432 ─┐  ┌ MySQL Wire :3306 ─┐  ┌HTTP:8080┐ │
│          │  Startup/Query/Bind/    │  │  HandshakeV10/     │  │REST/    │ │
│          │  Parse/Describe/Portal  │  │  COM_QUERY/        │  │PromQL/  │ │
│          │  扩展协议(PG 协议)       │  │  COM_STMT_*/RESULT │  │Line     │ │
│          └───────────┬──────────────┘  └──────────┬─────────┘  └────┬────┘ │
│                      │  会话对象(统一抽象)          │  会话对象(统一)  │      │
│                      └──────────────┬───────────────┴───────────────┘      │
│  认证    md5/scram(国密可选)        │        mysql_native / sha256 / SM3     │
│  语法识别  PG 方言  ←── 归一化 ──→  MySQL 方言      （DialectDetector）    │
├──────────────────────────────────────────────────────────────────────────┤
│  SQL 层   Lexer → Parser(双方言) → **方言归一 DialectNormalizer**           │
│           → AST(统一) → Analyzer(Binder) → Planner                        │
│           语义检查 / 类型推导 / 名称解析 / 权限(RBAC) / 隐式类型转换        │
│           规则重写: 谓词下推 · 聚合下推 · 常量折叠 · 分区裁剪 · 反连接改写  │
├──────────────────────────────────────────────────────────────────────────┤
│  执行层   Volcano(控制流) + 向量化(数据流) 混合                            │
│           Scans / HashJoin / MergeJoin / Agg / Sort / Limit                │
│           Exchange 算子(分布式收/发) · Vector 8列×1024行                   │
├──────────────────────────────────────────────────────────────────────────┤
│  存储层   Catalog(系统表) │ Chunk 分区管理 │ Schema 版本                   │
│           ├─ 时序存储引擎  Segment File + 压缩合并(类LSM)                   │
│           │    ├─ TagStore   字典 + 倒排 SeriesIndex + Bloom                │
│           │    └ DataStore  列组/Page/ZoneMap/编码器                       │
│           └─ 关系存储引擎  行存 + 索引(BTree/Hash) + MVCC                   │
├──────────────────────────────────────────────────────────────────────────┤
│  事务层   WAL(Group Commit) · MVCC · 快照隔离 · 2PC(单节点)                 │
│           锁管理(DMVCC latch) · 恢复(Redo Undo)                            │
├──────────────────────────────────────────────────────────────────────────┤
│  存储抽象   PageCache(强依赖) · BufferPool(Mmap) · Block IO                 │
│  节点层   Raft 复制 · 分片路由 · 心跳/成员变更 · 分区再平衡                  │
│  系统层   mmap/直接IO、NUMA 亲和、CPU 绑核、jemalloc 分配器                  │
└──────────────────────────────────────────────────────────────────────────┘
```

**关键：双协议只在最上面的“接入层 + 方言归一”分叉，SQL 核心、执行引擎、存储引擎、事务系统完全单份。**
代价极小（协议层是全项目最标准化、最容易复用的部分），收益极大（一套代码服务两类生态）。

**设计原则：复杂度换可靠性。**

明确采用 **“WAL + 追加写 Segment + 按时间/行数 roll + 后台 Compaction”** 的类 LSM 设计，而非完整 RocksDB 移植。这是有意的取舍：

- TDengine / IoTDB 都是这个路线，proven
- 避免 LSM 读放大与 compaction 抖动
- 时序场景写多为追加，天然适合
- 事务/快照由独立 WAL 层保证，存储层无 MVCC 负担

---

## 四、SQL 层设计

### 4.1 语法子集（对齐 PostgreSQL）

```sql
-- 库/表
CREATE DATABASE smart_grid;
CREATE TABLE schema_name.dev_power (
    ts          TIMESTAMPTZ  NOT NULL,
    device_id   BIGINT       NOT NULL,   -- 标签列(高基数)
    line_id     INT          NOT NULL,   -- 标签列(低基数)
    volt        DOUBLE  PRECISION,
    current_a   DOUBLE  PRECISION,
    PRIMARY KEY (ts, device_id)
)
PARTITION BY RANGE (ts)                    -- 扩展语法
WITH ( chunk_interval = '1 day',           -- 扩展
       retention_period = '180 days',      -- 扩展
       tag_columns      = ('device_id','line_id'),  -- 关键: 标签分离
       compression      = 'zstd' );

ALTER TABLE dev_power SET ( retention_period = '90 days' );
DROP PARTITION dev_power_20240101;

-- 写入
INSERT INTO dev_power (ts, device_id, line_id, volt, current_a)
VALUES (now(), 10086, 7, 220.1, 3.4), (now(), 10087, 7, 219.8, 3.1);

-- 查询: 标准 SQL + 时序函数
SELECT line_id,
       time_bucket('1 min', ts)                        AS bucket,
       avg(volt)      AS v_avg,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY volt) AS v_p95,
       last(volt)     AS v_last          -- Timescale 风格窗口函数
FROM dev_power
WHERE ts >= now() - INTERVAL '7 days'
  AND line_id = 7
GROUP BY 1, 2
ORDER BY 2;

-- ★ 核心差异化: 时序表 ↔ 关系表 同事务 Join
SELECT d.name, m.v_avg, d.capacity_kw
FROM   meter_meta d                                   -- 普通关系表
JOIN   (SELECT device_id, avg(volt) AS v_avg
        FROM dev_power
        WHERE ts BETWEEN '2026-09-01' AND '2026-09-02'
        GROUP BY 1) m
ON     d.device_id = m.device_id
WHERE  d.station = '杭州 500kV';

-- TCL
BEGIN; INSERT INTO dev_power ...; UPDATE meter_meta ...; COMMIT;
VACUUM ANALYZE dev_power;
```

### 4.1.1 同一张表，MySQL 方言写法（3306 端口直连）

tmDB 内部只有一份 AST。下面两段语句**建出的是完全相同的表**：

```sql
-- 通过 MySQL 协议（mysql -h ip -P 3306）执行
CREATE DATABASE IF NOT EXISTS smart_grid DEFAULT CHARACTER SET utf8mb4;

CREATE TABLE `dev_power` (
  `ts`        DATETIME(3)   NOT NULL COMMENT '时间戳，毫秒精度',
  `device_id` BIGINT        NOT NULL COMMENT '设备号(高基数标签)',
  `line_id`   INT           NOT NULL COMMENT '线路号(低基数标签)',
  `volt`      DOUBLE             DEFAULT NULL,
  `current_a` DOUBLE             DEFAULT NULL,
  PRIMARY KEY (`ts`,`device_id`),
  KEY `idx_line_ts` (`line_id`, `ts`)
) ENGINE=tmdb
  PARTITION BY RANGE COLUMNS(`ts`) (
    PARTITION p202609 VALUES LESS THAN ('2026-10-01'),
    PARTITION p202610 VALUES LESS THAN ('2026-11-01'),
    PARTITION pmax     VALUES LESS THAN (MAXVALUE)
  )
  /* tmdb: tag_columns=('device_id','line_id'),
             chunk_interval='1 day',
             retention_period='180 days' */;

INSERT INTO `dev_power` (`ts`,`device_id`,`line_id`,`volt`,`current_a`)
VALUES (NOW(3), 10086, 7, 220.1, 3.4)
ON DUPLICATE KEY UPDATE `volt`=VALUES(`volt`);   -- 幂等补写

SELECT `line_id`,
       time_bucket('1 minute', `ts`)  AS bucket,   -- MySQL 端同义函数
       avg(`volt`)                   AS v_avg,
       approx_percentile(`volt`, 0.95) AS v_p95,  -- MySQL 端同义函数
       last(`volt`)                  AS v_last
FROM `dev_power`
WHERE `ts` >= NOW() - INTERVAL 7 DAY
  AND `line_id` = 7
GROUP BY `line_id`, bucket
ORDER BY bucket DESC;

-- ★ 跨协议同会话可见：MySQL 端建的表，PG 端立刻可查
-- (PG 5432 端) SELECT count(*) FROM dev_power;
```

**注意 `ENGINE=tmdb`**：MySQL 驱动/JDBC 习惯带 engine 子句，解析器直接接受并忽略，保证老代码零改动跑通。

### 4.2 关键语法扩展（保持标准 SQL 为主）

| 扩展 | 位置 | 说明 |
|---|---|---|
| `PARTITION BY RANGE` | DDL | 声明式时间分区（向 PG11+ 靠拢） |
| `RANGE COLUMNS` 分区 | DDL | 接受 MySQL 风格分区，翻译为内部 Range 分区 |
| `TAG_COLUMNS` / `/* tmdb: … */` | DDL 属性 / 注释 | 标签列声明，决定列存分组与倒排 |
| `time_bucket(ts, width[,origin])` | 函数 | 对齐 PG13+ / Timescale |
| `first/last/argmin/argmax/slope` | 窗口函数 | 缺口填充、时间插值 |
| `interpolate(volt, gap => '30s')` | 后处理 | 前向/线性插值 |
| `hint /*+ SKIP_TAG_DICT */` | 注释 | 保留逃逸口 |

> **原则：不发明方言。** 所有新增能力用 SQL 标准能表达的优先表达（`time_bucket` 就是 PG 标准），扩展只加在“标准无法表达”的地方（分区间隔、标签声明）。

### 4.3 方言归一层（DialectNormalizer）—— 双协议的核心设计

统一 AST（`tmdb-sql::ast`）是**唯一中间表示**。三段式：

```
                    ┌──────────── PG Parser ───────────┐
SQL 文本 ──Dialect──┤                                   ├──→ 统一 AST
       Detector      ├─────────── MySQL Parser ──────────┤        │
                    └───────────────────────────────────┘        │
                                                     Analyzer/Binder
                                                              │
                                                    ┌─────────┴─────────┐
                                              PG Result 编码     MySQL Result 编码
```

```rust
pub enum Dialect { Postgres, MySql, Auto }

/// 协议层嗅探：首包特征 + 认证插件名 + 客户端指纹
pub struct DialectDetector {
    startup: StartupMsgKind,      // SSLRequest / Startup / GSSENCRequest
    auth_plugin: Option<String>,  // mysql_native_password / caching_sha2 / scram-sha-256
    fingerprint: ClientFingerprint, // MySQL Connector/J, Navicat, mysql CLI, DBeaver...
}

pub trait Normalizer {
    /// MySQL → 统一 AST（消方言差异）
    fn normalize_from_mysql(stmt: &ast::Statement) -> Result<ast::Statement>;
    /// 统一 AST → MySQL 可解析形式（错误信息、EXPLAIN、SHOW 类回显）
    fn denormalize_to_mysql(stmt: &ast::Statement) -> Result<ast::Statement>;
}
```

### 4.4 PostgreSQL ⇄ MySQL 语法映射表

**归一后执行层只认统一 AST，映射表就是维护面。**

| 语义 | PostgreSQL 写法 | MySQL 写法 | 归一后（内部统一 AST） |
|---|---|---|---|
| 自增主键 | `GENERATED ALWAYS AS IDENTITY` | `AUTO_INCREMENT` | `IdentityColumn` |
| 布尔 | `BOOLEAN`（`t/f`） | `TINYINT(1)` | `Bool` |
| 字符串 | `TEXT` / `VARCHAR(n)` | `VARCHAR(n)` / `LONGTEXT` | `Varchar(n)` / `Text` |
| 文本排序规则 | `COLLATE "zh_CN.UTF-8"` | `utf8mb4_0900_ai_ci` | `Collation{ zh_cn, binary, ci }` |
| 时间类型 | `TIMESTAMPTZ` | `DATETIME(3)` | `TimestampTz{precision:3, tz}` |
| 时间间隔 | `INTERVAL '7 days'` | `INTERVAL 7 DAY` | `IntervalLiteral` |
| 字符串拼接 | `a \|\| b` | `concat(a,b)` / `a,b`（`sql_mode`） | `Concat` |
| 分页 | `LIMIT n OFFSET m` | `LIMIT m, n` | `Limit{offset,limit}` |
| 整除 | `a / b`（int/int=int） | `a div b` / `/` 恒为 float | `IntDiv` / `FloatDiv` |
| 幂运算 | `power(a,b)` / `a^b` | `pow(a,b)` | `Pow` |
| 大小写标识符 | `"Col"` 敏感 | `` `col` `` 敏感 | `Identifier{quote:Double\|Backtick\|None}` |
| 空值合并 | `coalesce` / `a IS NULL` | `ifnull` / `a <=> null` | `Coalesce` / `IsNull` |
| 范围条件 | `ts BETWEEN a AND b` | `ts BETWEEN a AND b` | `Between{ inclusive }` |
| Upsert | `ON CONFLICT (…) DO UPDATE` | `ON DUPLICATE KEY UPDATE` | `Upsert{ keys, update_set }` |
| 删表 | `DROP TABLE t` | `DROP TABLE IF EXISTS t` | 同 |
| 改表 | `ALTER TABLE t ALTER COLUMN …` | `ALTER TABLE t MODIFY COLUMN …` | `AlterColumn` |
| 分组序号 | `GROUP BY 1,2` | `GROUP BY 1,2` | 同（MySQL 端隐式别名也支持） |
| 窗口函数 | `OVER (PARTITION BY …)` | `OVER (PARTITION BY …)` | 同 |
| CTE | `WITH x AS (…)` | `WITH x AS (…)` / `WITH RECURSIVE` | `Cte{recursive}` |
| 索引 | `CREATE INDEX … ON t USING btree (a)` | `KEY idx (a) IN TABLE` | `Index{cols, method }` |
| 引擎子句 | 无 | `ENGINE=InnoDB DEFAULT CHARSET=utf8mb4` | **解析后丢弃** |
| 分区子句 | `PARTITION BY RANGE (ts)` | `PARTITION BY RANGE COLUMNS(ts)(…)` | `PartitionSpec::Range{col,interval}` |
| 统计信息 | `ANALYZE t` | `ANALYZE TABLE t` | `Analyze` |
| 批量导入 | `COPY t FROM STDIN` | `LOAD DATA LOCAL INFILE` | `BulkLoad{src}` |
| 事务 | `BEGIN / COMMIT / ROLLBACK` | `START TRANSACTION / COMMIT / ROLLBACK` | `TxnStmt` |
| 保存点 | `SAVEPOINT s` | `SAVEPOINT s` | `Savepoint` |
| 占位符 | `$1 / $2` | `?` | `Param(ordinal)` |

### 4.5 函数别名表（同一函数，两种方言名）

执行层只有一份 UDF 注册表，`FunctionRegistry` 挂两个名字：

| 统一实现 | PostgreSQL 名 | MySQL 名 | 用途 |
|---|---|---|---|
| `time_bucket` | `time_bucket(1, ts)` | `time_bucket(INTERVAL 1 MINUTE, ts)` | 时间分桶聚合 |
| `window_duration` | `width_bucket(1, ts, a, b)` | `width_bucket(1, ts, a, b)` | 窗口编号 |
| `percentile_cont` | `percentile_cont(0.95) WITHIN GROUP (ORDER BY v)` | `approx_percentile(v, 0.95)` | 分位数 |
| `last_value` | `last(v) OVER (ORDER BY ts)` | `last(v)` | 取最新值 |
| `first_value` | `first(v) OVER (ORDER BY ts)` | `first(v)` | 取最早值 |
| `argmax` | `argmax(v, ts) OVER w` | `arg_max(ts, v) OVER w` | 极值对应时间 |
| `slope` | `slope(v, ts) OVER w` | `slope(v, ts) OVER w` | 趋势斜率 |
| `histogram` | `histogram(v, 0, 100, 10)` | `histogram(v, 0, 100, 10)` | 直方图聚合 |
| `interpolate` | `interpolate(v, gap => INTERVAL '30s')` | `interpolate(v, INTERVAL 30 SECOND)` | 缺口插值 |
| `date_trunc` | `date_trunc('hour', ts)` | `date_trunc('hour', ts)` | 时间截断（MySQL 侧同义注册） |
| `retention_step` | `tmdb_step` | `tmdb_step` | 预聚合 Rollup 标记 |

> 同时**保留双名注册**（PG 端可调 `approx_percentile`，MySQL 端可调 `percentile_cont`），最大程度降低老代码迁移成本。

### 4.6 MySQL 协议层实现要点

| 环节 | PG 侧 | MySQL 侧 | 差异处理 |
|---|---|---|---|
| 握手 | StartupMessage + SCRAM-SHA-256 | HandshakeV10 + `mysql_native_password` / `caching_sha2_password` / `sha256_password` | 插件协商独立实现 |
| 认证 | HBA / pgbouncer 兼容 | `mysql.user` 视图（映射到 Catalog） | 统一走 Catalog 的角色表 |
| 传输 | 简单 query / 扩展 query | 简单 query / `COM_QUERY` / Prepared Statement(`COM_STMT_PREPARE/EXECUTE/FETCH/CLOSE`) | Prepared 参数化差异最大，需单独适配 |
| 结果集 | RowDescription(二进制) + DataRow | ColumnCount41 + ColumnDefinition41 + Text Row | 结果编码器分两套 |
| 类型 OID | PG OID(23/20/1114/701…) | MySQL type(3/8/12/246…) | 双向映射表 |
| 结果集元数据 | 列名 + table OID | 列名 + table + 库名 + 原始表名 + 原类型 | MySQL 端需回填 table 字段，否则 BI 工具报“未知列” |
| 错误码 | SQLSTATE(5 字符) + 位置 | errno(4 字节) + SQLSTATE 标记 + message | 统一错误 → 双方码表 |
| 事务 | `BEGIN` 显式 | autocommit / `START TRANSACTION` | 服务端自动按协议设置 autocommit |
| 隐式转换 | 严格类型 | 宽松模式（`sql_mode`） | **v1 采用严格模式**，避免精度/溢出歧义；可配置放开 |
| 时间返回 | `timestamptz` 文本 ISO8601 | `DATETIME` 文本 `YYYY-MM-DD HH:MM:SS.mmm` | 由连接 `time_zone` 统一设置 |
| NULL 表示 | 列长度 -1 | `0xFB` | 结果编码器处理 |
| 存储过程 | 不支持（v1） | 不支持（v1） | 一致列入非目标 |
| `COM_FIELD_LIST` / `COM_STATISTICS` / `COM_CHANGE_USER` | — | 需最小实现 | 遗漏会导致老驱动断连 |
| KILL / `COM_PROCESS_KILL` | 取消请求 | `KILL <id>` | 统一到查询管理器 |
| 多结果集 | PG 支持 | MySQL 需 `CLIENT_MULTI_STATEMENTS` | 显式按 flag 分支 |
| 大字段 | 不用 | 需正确置 `0xFC/0xFD/0xFE` 特殊长度标记 | ⚠ 常见 BUG 源 |
| 预处理语句大字段 | — | **禁止**（MySQL 协议硬限制 16MB） | 需在执行前拦截并降级为 `COM_QUERY` |

> MySQL 协议规范：`Handshake Protocol 41`、`COM_*` 命令、Resultset 编码、两阶段认证。
> 必测用例：`mysql` CLI / `mysqlsh` / Navicat / DBeaver / MySQL Connector-J 8.x / `mysql-connector-python` / `go-sql-driver` / `mysql_async`。

### 4.7 兼容性测试策略（MySQL）

```
tests/
├── sql/
│   ├── pg/          # SQLLogicTest (PG 侧)
│   ├── mysql/       # MySQLLogicTest 裁剪版 (基于 MySQL 官方 test suite)
│   └── parity/      # ★ 双端同语句结果一致性对拍
├── wire/
│   ├── mysql/       # 握手/认证/结果集/边界(大字段/NULL/时区) 抓包级用例
│   └── cross/       # PG 写入 → MySQL 读、MySQL 写入 → PG 读
├── client/
│   ├── navicat/  dbeaver/  mysql-cli/  connector-j/  smartbi/
└── fuzz/
    └── protocol/     # 畸形包 → 不得 panic (FuzzedDataProvider)
```

**★ Parity 对拍是双协议质量的生命线**：
同一份 DDL/DML 脚本分别在 5432 与 3306 执行，结果集逐行 diff（浮点容差 1e-12，时间归一到 UTC 毫秒）。CI 中作为强制卡口。

### 4.8 Parser 与类型系统实现

- 基础：`sqlparser-rs`（纯 Rust，MIT）做 PG 侧骨架，**MySQL 侧以 `GenericDialect` + 自研 Visitor 扩展**
- 扩展：自研 `Node` trait 层，用 `visit_mut` 模式注入扩展节点，避免 fork 维护
- 归一：`DialectNormalizer` 负责 `ON DUPLICATE KEY` → `Upsert`、`RANGE COLUMNS` → `PartitionSpec` 等收敛
- 类型系统：自研 `TypeId`，**同时持有 PG OID 与 MySQL type 编号**，结果编码器与驱动协商均由此驱动
- 名称解析：三级 catalog → schema → table/column；MySQL 端因无 schema 概念，用 `database` 映射为 PG 的 `schema` 层（`smart_grid.dev_power`）
- 引号规则：MySQL 端 `` ` `` 与 PG 端 `"` 各自保真存储标识符大小写；**不强制统一大小写**（遵循各自协议行为，规避兼容性陷阱）

---

## 五、存储引擎（核心）

### 5.1 两种存储形态，双协议同一 Catalog 同一 WAL

```
Database
├── Catalog (系统表：pg_class/pg_attribute/pg_type  ── PG 协议视图
│                        + information_schema      ── 双协议标准视图
│                        + mysql.* 系统表虚拟视图    ── MySQL 协议视图)
│
├── [关系表]  Row Store  ────────────────┐
│   table → row groups (128KB) → MVCC     │  同一事务
│   BTree / Hash Index / MVCC undo        │  同一 WAL
│   用于: 字典表、维表、元数据            │  同一快照
│                                           │  ★ PG 5432 与 MySQL 3306
└── [时序表]  Column Store  ──────────────┘    看到的是同一份数据
    table = Tag Set (STable) + Series
    series → 按时间切分 Chunk → Segment File
    Chunk = 1天 × 该 tag 组合的所有 series
```

**系统表虚拟视图（Catalog 之上的只读投影，不落物理存储）**：

| 内部真实系统表 | PG 协议暴露 | MySQL 协议暴露 |
|---|---|---|
| `tmdb_class` | `pg_class`、`pg_attribute`、`pg_type`、`pg_namespace` | `information_schema.tables/columns` |
| `tmdb_index` | `pg_index`、`pg_indexes` | `information_schema.statistics` |
| `tmdb_user` | `pg_roles`、`pg_user` | `mysql.user`、`mysql.db` |
| `tmdb_settings` | `pg_settings`（部分） | `@@version`、`@@sql_mode`、`@@time_zone` 等系统变量 |
| `tmdb_processlist` | `pg_stat_activity` | `information_schema.processlist` |
| — | `SHOW ALL TABLES` | `SHOW TABLES` |

> 这些视图由 `Catalog → VirtualView` 的投影算子实时生成，保证任何协议下的元数据自浏览（Navicat / DBeaver / BI 工具都依赖它）都正确可用。

### 5.2 时序数据模型（对标 TDengine STable）

```
STable: dev_power
├── tag 列(高基数)  device_id  BIGINT   ──┐
├── tag 列(低基数)  line_id    INT      ──┤→ TagStore
└── 度量列(数值)    volt / current_a /…  ──┘→ DataStore
                                     │
                            series_key = hash(device_id, line_id)
                                     │
                    同一 series_key 的所有 point 物理连续存放
                                    ┌────┴────┐
                              Chunk(1天)  Chunk(1天)
                             ┌──────┐    ┌──────┐
                            段文件 段文件 段文件 ...
```

**收益**：

- 查询 `WHERE device_id IN (...)` → 先查倒排拿 series_id，再点查对应数据块，**不做全量扫描**
- `line_id = 7` 这类低基数 tag → TagStore 内 bitmap 过滤，秒级
- 列存自然压缩：同一 series 的 `volt` 列全是 220V 附近 → Gorilla 压到 ~0.3 B/point

### 5.3 段文件（Segment File）格式 `.tsb`

自研格式，借鉴 TDengine TSM + Arrow 的可读性：

```
┌─────────────────────────────────────────────────────────────┐
│ File Header (256B)                                          │
│  magic "CTSB" | version | table_id | ts_unit | col_count   │
│  created_at | row_count | checksum(SM3)                     │
├─────────────────────────────────────────────────────────────┤
│ Data Block 0   (列组)                                       │
│   col: ts        → Page[] (每 Page 64KB)                     │
│   col: volt      → Page[]                                   │
│   col: current_a → Page[]                                   │
│   col: quality   → Page[] (位图压缩)                         │
│   BlockFooter: 每列 {page_offset, row_count, enc,           │
│                      zone_map: [min,max,null_cnt]}          │
│   NullBitmap   → 1 bit/row                                  │
├─────────────────────────────────────────────────────────────┤
│ Block 1 ... Block N   (每 Block 4096 行 或 256KB)            │
├─────────────────────────────────────────────────────────────┤
│ Footer Index (4KB 末页)                                     │
│  列名 → {block_offset, page_count, min_ts, max_ts,          │
│          null_ratio, distinct_est}                          │
│  Series Bitmap: 本段包含哪些 series_id (RoaringBitmap)       │
└─────────────────────────────────────────────────────────────┘
```

**关键设计**：

- **ZoneMap**：每 Page 记录 min/max，谓词 `ts BETWEEN` 和 `volt > 240` 可整页跳过
- **Footer 即目录**：随机点查只读 4KB Footer + 1 个 Block，IO 放大可控
- **Row Group = 4096 行**：兼顾扫描并行度与压缩率（对标 Parquet）
- **Schema 演进**：Header 存列名而非列序号，`ADD COLUMN` 只写新列列组

### 5.4 编码器（压缩率的胜负手）

| 类型 | 编码 | 预期字节/点 |
|---|---|---|
| `TIMESTAMPTZ` | delta-of-delta + ZigZag Varint | 0.5 ~ 1.0 |
| `BIGINT`（tag 值/测点号） | delta + FOR 帧 + BitPacking | 0.5 ~ 2 |
| `DOUBLE`（度量） | **Gorilla**（前后值 XOR + leading-zero + trailing-zero 复用） | 0.8 ~ 2.5 |
| `FLOAT` | Gorilla 简化版 | 0.6 ~ 1.5 |
| 低基数 INT（状态码/开关量） | Dictionary + RLE | 0.05 |
| 字符串（低基数，Line 协议兼容） | Dictionary + FST | 0.3 |
| NULL | RoaringBitmap 稀疏表示 | ~0 |

**编码流水线**（可组合，一列可叠多层）：

```
原始值
  → [1] 差分预处理   (按列类型可选)
  → [2] 通用编码     Gorilla / BitPack / RLE / Dictionary
  → [3] 块压缩       zstd level 1~3 (可关) / LZ4 (低延迟档)
  → Page (64KB) → 文件
```

**配置档位**（写入吞吐 vs 压缩率，用户可选）：

| 档位 | 编码 | zstd | 适用 |
|---|---|---|---|
| `speed` | delta | off | 高频遥测、边端 |
| `balanced`（默认） | Gorilla | LZ4 | 通用 |
| `archive` | Gorilla + 数值重编码 | zstd-3 | 长期留存 |

**目标压缩比**：24 B/点 原始 → **1.2 ~ 2.5 B/点（10~20x）**，对齐 VictoriaMetrics 区间。

> Rust 生态可用的压缩 crate：`zstd`（bindgen C，需交叉编译；备选 `ruzstd` 纯 Rust）、`lz4_flex`。
> **信创要求下优先 `ruzstd`**，避免 C 依赖导致龙芯/申威交叉编译失败。

### 5.5 索引体系

| 索引 | 结构 | 用途 |
|---|---|---|
| **SeriesIndex** | tag 值 → series_id 的倒排 + RoaringBitmap | 时序查询入口，**最关键** |
| **TagDictionary** | 字典编码 + FST 前缀树 | tag 精确/前缀/范围匹配 |
| **BloomFilter** | Page 级，64 B/Page | `volt = 220.1` 等值点查跳过 |
| **ChunkMinMax** | Chunk 级 ts min/max | 分区/Chunk 裁剪 |
| **关系表索引** | BTree（PostgreSQL 兼容 opclass）、Hash、BRIN(ts) | 关系表 Join/范围扫 |

### 5.6 Compaction（后台合并）

```
Segment 写入流：
  Insert → WAL(commit) → MemTable(Buffer) → 满(256MB/60s) → Flush 段文件 L0
  L0 → 合并 → L1（按 series_id + ts 区间对齐，重写压缩）
  L1 → 合并 → L2（终态，只读，TTL 到期直接整文件删除）
```

- **合并策略**：只合并 ts 区间重叠的段；合并时做 delta 重排 + 重新编码（收益最大）
- **TTL 优化**：整段全过期 → 直接 unlink，不读不写（对齐 IoTDB/TDengine 的优势）
- **删除**（GDPR/修数据）：`DELETE` 走墓碑 + 段级 zone map 跳过；不立即重写
- **优先级控制**：Compaction 与查询共享线程池但独立限流，避免 compaction 打满 IO

### 5.7 事务与 MVCC（保证 Join 正确性的根基）

**这是最容易做错的地方，必须显式设计。**

```rust
// 事务核心结构
pub struct Txn {
    id:       TxnId,        // 全局单调
    start_ts: u64,          // 快照读版本
    commit_ts: u64,         // 提交版本
    status:   AtomicU8,     // ACTIVE / COMMITTED / ABORTED
    wal_lsn:  u64,          // 预分配 WAL LSN，避免提交冲突
    reads:    RefCell<HashSet<(RelationId, PageId)>>,      // read set (SI 冲突检测)
    writes:   RefCell<HashMap<(RelationId, PageId), Vec<u8>>>, // undo
}
```

| 机制 | 方案 | 说明 |
|---|---|---|
| 隔离级别 | **Snapshot Isolation**（可串行化仅对同 key 冲突时提升） | 时序追加写为主，SI 足够 |
| 时序表可见性 | 段文件内每行隐式 `commit_ts` delta 列；查询按快照过滤 | 时序表天然 append，可延迟可见 |
| 关系表可见性 | 行版本 + Undo Log，页头 `xmin/xmax` | 对齐 PG 语义 |
| **跨表一致性** | 同一事务写入时序表 + 关系表，用同一 `commit_ts`；快照读时两侧都按该版本过滤 | ★ 卖点 |
| 提交原子性 | WAL group commit：所有 write set 落盘 → 分配 commit_ts → 写 commit record | 崩溃后靠 redo 恢复 |
| 冲突检测 | 时序表：同 `(series_id, ts)` 才冲突（天然低冲突）；关系表：read set 交集 | 乐观锁，避免全局锁 |
| 恢复 | WAL replay（redo 已提交）+ 段文件尾部 `xmin` 校验（undo 未提交） | 启动 < 30s（100GB 数据） |

### 5.8 写路径（关键性能点）

```
1. Parse/SQL → Expr 树，绑定到列
2. 提取 tag → TagStore 反查 series_id（内存 Dict / 缓存）
3. 编码：按 (series_id, chunk_id) 路由到对应 MemTable
4. MemTable：追加列式缓冲 + 局部 sort（乱序容忍窗口，如 30min）
5. 预分配 WAL LSN → 并行写 WAL（组提交，fsync 按 durability 档位）
6. 分配 commit_ts → 标记可见
7. 满 → 后台 flush 段文件（L0）→ compaction 链
```

**durability 档位**（信创项目普遍可调）：

| 档位 | 行为 | 适用 |
|---|---|---|
| `max` | `fsync` 每提交 | 金融 / 电网 SCADA |
| `normal` | 组提交 fsync（默认） | 10 万 TPS 级别无压力 |
| `os_cache` | 不 fsync，靠 OS | 边端 / 可容忍丢失 |

**乱序写**：允许 `max_late_arrival = '2 hours'`，超出进 `errors` 队列并在日志告警 —— 工业场景必须显式处理，不能静默丢弃。

---

## 六、执行引擎

### 6.1 混合模型

```
控制流：Volcano/Iterator   →  逻辑计划树，schema 协商，动态分区裁剪
数据流：向量化             →  Vector(2048 行 × N 列) + SelectionVector
```

参考 DataFusion 的设计：**批边界 + 状态机 + 选择向量**，但自研以保证信创可控性。

### 6.1.1 双协议下的执行差异

执行引擎本身**完全不感知协议**。只有三处需要协议适配，全部集中在边界的 `ProtocolAdapter`：

| 环节 | 差异 | 处理方式 |
|---|---|---|
| 函数名解析 | 同一 UDF 两个名字 | 归一阶段已在 AST 层统一，执行期零成本 |
| 隐式类型转换 | MySQL 宽松、PG 严格 | `CastPolicy { Strict, MySqlLenient }`，由 `ProtocolAdapter` 在 Bind 前注入；**仅影响隐式转换，不放宽存储类型检查** |
| 结果集排序 | PG 无隐式 `ORDER BY` 语义 | 执行计划共享，MySQL 端保留其既有行为（`sql_mode` 可关），不额外加 Sort 算子 |

### 6.2 关键优化

| 优化 | 说明 |
|---|---|
| 分区/Chunk 裁剪 | `ts` 谓词直接剪掉整段文件，不进 IO |
| **Late Materialization** | 先只读 `ts` + tag 相关列做过滤/聚合，最后取度量列 → I/O 降 3~5x |
| Tag 先过滤 | 谓词里 tag 条件先在 TagStore 求值（bitmap），得到 series 白名单 |
| 聚合下推 | 预聚合 Rollup 表（对标 IoTDB 降采样） |
| **Prepared Statement 复用** | 缓存归一后的逻辑计划，PG/MySQL 两端共享同一份 plan cache（按方言 key 隔离） |
| 并行度 | `core × 2`，扫描按 (chunk, block) 并行 |
| NUMA 亲和 | 绑核 + 每 NUMA 独立 BufferPool |

### 6.3 SIMD 与国产 CPU

```rust
// 编译期门控：跨架构可选
#[cfg(target_arch = "x86_64")]
mod simd { /* avx2: delta-of-delta / sum / min-max；带 is_x86_feature_detected! 运行时检测 */ }

#[cfg(target_arch = "aarch64")]
mod simd { /* neon: 同上 */ }

#[cfg(target_arch = "loongarch64")]
mod simd { /* lsx/mmx: 同上 */ }

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64", target_arch = "loongarch64")))]
mod simd { /* 纯标量回退：与 SIMD 共享同一套 trait，语义等价 */ }
```

**原则**：所有算子定义 `Kernel` trait，标量实现是基准，SIMD 是优化，**结果必须 bit-exact 等价**（用随机对拍测试保证）。这直接决定龙芯/申威上性能是否可控。

---

## 七、分布式与高可用

### 7.1 部署形态（v1 存算一体）

```
       ┌──────────── Coordinator(无状态) ────────────┐
       │  解析 / 计划 / 分布式调度 / 权限              │
       └───┬──────────────┬──────────────┬───────────┘
           │ Exchange      │              │
     ┌─────▼─────┐  ┌─────▼─────┐  ┌────▼────┐
     │ DataNode1 │  │ DataNode2 │  │ DataNode3│  Raft Group
     │  分片 A,B │  │  分片 C   │  │  分片 A  │  (每分片 3 副本)
     └───────────┘  └───────────┘  └─────────┘
```

- **分片粒度**：库（租户）→ STable → 时间 Chunk。**先按时间切，再按分片切**，天然避免大表热点
- **副本**：Raft 强一致（`raft-rs`，纯 Rust）。时序追加可扩展为 **quorum 写 + Raft 成员管理**，降低写放大
- **再平衡**：Chunk 粒度迁移，网络限速 + 可暂停
- **查询路由**：单分片 → 直推；多分片 → Coordinator 归并。**跨分片大 Join 不支持**（明确写入文档，避免信创项目现场踩坑）

### 7.2 v2 演进：存算分离

分析型重负载（历史数据聚合）时，把 Parquet 格式冷数据卸载到对象存储（华为 OBS / 浪潮天穹 / MinIO），Coordinator 直读。存储层预留 `ExternalScan` 接口但 v1 不实现。

---

## 八、信创专项工程（重点）

### 8.1 供应链与许可证

| 项 | 要求 | 工具 |
|---|---|---|
| 依赖许可证 | **禁用 GPL/AGPL 传染性依赖**；Apache-2.0 / MIT / BSD / MPL-2.0 | `cargo-deny`（CI 卡口） |
| 源码自主率 | 核心模块 100% 自研，整体 ≥ 95% | 交付报告 + `cargo-vet` 审计 |
| 供应链锁定 | 依赖树哈希锁定，防止投毒 | `cargo-vet` + 离线 vendor |
| 第三方库清单 | 交付《开源组件清单及许可证表》 | CI 自动生成 |

**依赖选型（全部纯 Rust 或可选特性）**：

| 用途 | crate | 许可 | 备注 |
|---|---|---|---|
| SQL 解析 | `sqlparser-rs` | Apache-2.0 | |
| 存储压缩 | `ruzstd`（纯 Rust） | MIT | 备选 `zstd`（需交叉编译 C） |
| LZ4 | `lz4_flex` | MIT | |
| Raft | `raft-rs` | MIT | |
| 分配器 | `jemallocator` / `mimalloc` | MIT/Apache-2.0 | 内存密集，收益大 |
| 序列化 | `serde` + `bincode` | MIT/Apache-2.0 | |
| 列存编码 | **自研** | — | 核心知识产权 |
| 索引 | `fst` / `roaring` | MIT / Apache-2.0 | |
| 加密 | `sm3` / `sm4` crate 或自研 | — | 国密 |
| async | `tokio` | MIT | |

### 8.2 交叉编译矩阵（CI 必过）

```yaml
# .github/workflows/build.yml (示意)
targets:
  - x86_64-unknown-linux-gnu      # 海光/兆芯
  - aarch64-unknown-linux-gnu     # 飞腾/鲲鹏/申威
  - loongarch64-unknown-linux-gnu # 龙芯 3A5000/3C5000
  - aarch64-unknown-linux-musl    # 静态链接，边缘/麒麟最小化部署
images:
  - openeuler:22.03   # aarch64
  - openeuler:22.03   # loongarch64
  - kylin:v10-sp3     # aarch64 / loongarch64
  - uos-server-20     # x86_64 / aarch64
checks:
  - cargo test --all-features        # 单元 + 编码器对拍
  - cargo nextest + fault injection  # WAL 崩溃恢复
  - SIMD/标量 结果 bit-exact 对拍
  - 混合架构上跑同一份 SQL 结果一致性校验
```

> **LoongArch 是最大风险点**：`loongarch64-unknown-linux-gnu` 需要 LLVM 15+ 且 crates.io 上部分依赖未发布预编译产物。
> 策略：全部依赖 **禁止使用 C 扩展**，`ruzstd` 而非 `zstd`；CI 早期把 LoongArch 目标跑通再堆功能。

### 8.3 性能与稳定性验证报告

交付物需包含（在国产实机上实测）：

| 报告 | 指标 |
|---|---|
| 兼容性测试报告 | 麒麟/统信 × 飞腾/龙芯/海光 组合矩阵，逐项通过 |
| 功能测试报告 | SQL 标准符合度（选 GB/T 35176 相关用例） |
| **协议兼容报告** | MySQL Connector/J 8.x / Navicat / DBeaver / mysql CLI / 国产 JDBC 驱动 × 全部 DDL/DML/DQL 用例通过率；PG 端 Npgsql/JDBC 同步出具 |
| **双端一致性报告** | Parity 对拍 100% 通过，附差异清单（应为空） |
| 性能测试报告 | 写入 TPS、查询 P99、压缩比、国产平台达成率；**分协议统计**（`mysql` CLI 批写 vs JDBC 批写 vs COPY 吞吐差异） |
| 稳定性测试报告 | 72h 满载、断电/断网/磁盘满故障注入；双协议并发连接压测 |
| 源码自主性报告 | 自研代码量与占比、第三方清单 |

**性能目标（单节点，飞腾 S2500 / 32C）**：

| 场景 | 目标 |
|---|---|
| 单行批量写 | ≥ 100 万 points/s |
| 写入 P99 延迟 | ≤ 5 ms（normal durability） |
| **MySQL 协议端写入** | ≥ 60 万 points/s（协议开销约为原生 COPY 的 1/3，需实测标定） |
| **PG 协议端写入** | ≥ 80 万 points/s |
| **COPY / LOAD DATA 批量导入** | ≥ 300 万行/s |
| 1 亿点 + 10 万 series，1min 聚合 | ≤ 1.5 s |
| 1 亿点 Tag 过滤聚合 | ≤ 3 s |
| 压缩比 | ≥ 10:1 |
| 启动恢复（100GB） | ≤ 60 s |
| 稳态内存（1e9 点） | ≤ 8 GB（依赖 OS page cache） |
| 双协议并发（MySQL 500 + PG 500 连接） | 各自 P99 不劣化 > 15% |

---

## 九、工程结构

```
tmdb/
├── crates/
│   ├── tmdb/                    # 主二进制、CLI、配置
│   ├── tmdb-server/             # 连接管理、查询管理器、协议路由
│   ├── tmdb-sql/                # AST + Parser + ★ 方言归一层
│   │   └── src/
│   │       ├── ast/                 # 统一 AST（唯一中间表示）
│   │       ├── parser/  visitor/
│   │       └── dialect/             # detector / normalizer / pg2mysql / mysql2pg / func_alias
│   ├── tmdb-catalog/            # 元数据、系统表、schema 版本
│   │   └── src/  virtual_view.rs    # pg_* / mysql.* / information_schema 投影
│   ├── tmdb-txn/                # WAL、MVCC、快照、锁、恢复
│   ├── tmdb-timestore/          # ★ 段文件、编码器、compaction
│   │   └── src/
│   │       ├── segment/             # .tsb 读写、footer、zone map
│   │       ├── encoding/            # gorilla, delta_of_delta, rle, dict, bitpack
│   │       ├── memtable.rs
│   │       ├── compaction.rs
│   │       └── ttl.rs
│   ├── tmdb-tagstore/           # ★ 字典 + 倒排 + bloom
│   ├── tmdb-rowstore/           # 关系表、BTree、MVCC
│   ├── tmdb-exec/               # Volcano + 向量算子
│   │   └── src/  ops/  vector/  kernel/
│   ├── tmdb-dist/               # Raft、分片、再平衡
│   ├── tmdb-crypto/             # SM2/SM3/SM4、SM4 静态加密
│   └── tmdb-wire/               # ★ 只做协议，不做语义
│       ├── pg/                      # startup, auth(scram/md5/sm3), simple+extended, copy
│       ├── mysql/                   # handshake41, auth plugins, COM_*, resultset, text/binary
│       ├── http/                    # REST、PromQL、Influx Line
│       └── shared/                  # Session/QueryId/ResultStream/ErrorCode 抽象
├── tests/
│   ├── sql/           # pg/ + mysql/ + parity/ 双方言 + 双端对拍
│   ├── wire/          # pg/ + mysql/ 协议级用例 + cross/ 跨协议读写
│   ├── torture/       # 随机 SQL fuzz + 对拍
│   ├── crash/         # 故障注入（kill -9 / 磁盘满 / 网络分区）
│   └── cross-arch/    # 多架构结果一致性
├── benches/           # 编码器、扫描、写入吞吐、协议开销对比
├── docs/
│   ├── 信创适配手册.md   # 部署、调优、验收
│   ├── MySQL兼容说明.md  # 支持/不支持清单、驱动版本矩阵、迁移指南
│   └── 开源组件清单.md
└── .cargo/config.toml # 交叉编译 target
```

> **架构约束（写入 `docs/架构约束.md` 并在 CI 检查）**：
> `tmdb-wire` **不得依赖** `tmdb-timestore` / `tmdb-exec`；
> 所有协议层能力必须通过 `tmdb-server` 暴露的 `SessionApi` 访问。
> 这是保证双协议不会退化成两套实现的关键工程纪律。

### 9.1 二进制与配置

```toml
# tmdb.toml
[server]
listen           = "0.0.0.0:5432"        # PostgreSQL 协议
listen_mysql     = "0.0.0.0:3306"        # MySQL 协议
listen_http      = "0.0.0.0:8080"        # REST / PromQL / Line
max_connections  = 4096
default_dialect  = "auto"                # auto | postgres | mysql

[wire.pg]
enabled          = true
auth_methods     = ["scram-sha-256", "md5", "sm3"]
max_bind_params  = 65535
copy_enabled     = true

[wire.mysql]
enabled                 = true
auth_plugins            = ["mysql_native_password", "sha256_password",
                           "caching_sha2_password", "sm3"]
default_charset         = "utf8mb4"
collation               = "utf8mb4_bin"
sql_mode                = "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"
autocommit_default      = true
max_allowed_packet      = "64MB"
prepared_stmt_enabled   = true
prepared_stmt_max_size  = "16MB"       # 协议硬上限，超限自动降级为 COM_QUERY
compress_enabled        = false        # zlib 协议压缩，v1.1
lower_case_table_names  = 0            # 0 敏感 / 1 不敏感 / 2 存原样比

[wire.mysql.system_vars]
@@version              = "8.0.35-tmdb-1.0"
@@version_comment      = "tmDB 信创时序数据库"
@@time_zone            = "+08:00"
@@character_set_server = "utf8mb4"

[storage]
data_dir        = "/data/tmdb"
chunk_interval  = "1 day"
segment_target  = "256MB"
row_group_rows  = 4096
page_size       = "64KB"
default_encoding = "balanced"

[query]
default_timeout   = "30s"
max_memory_per_query = "2GB"
spill_to_disk     = true
plan_cache_size   = "1024"        # 按方言隔离

[distributed]
enabled       = true
shard_key     = "database"
replication   = 3
rebalance_rate = "50MB/s"

[security]
sm_tls        = true          # 国密 TLS（双协议共用同一密钥库）
sm_encrypt    = true          # 静态 SM4
audit_log     = "/var/log/tmdb/audit.log"
audit_dialect = true          # 审计日志记录客户端协议与工具指纹
```

---

## 十、里程碑

| 阶段 | 周期 | 交付 | 验收线 |
|---|---|---|---|
| **P0 基础设施** | 4 周 | WAL、PageCache、错误模型、CI 多架构编译 | LoongArch/aarch64/x86 三架构绿灯 |
| **P1 存储引擎** | 8 周 | 段文件、5 种编码器、memtable、compaction、TTL | 压缩比 ≥10:1，1e7 点可写可查 |
| **P2 SQL MVP** | 6 周 | Parser/Binder/Planner + 基础算子，单机可用 | SQLLogicTest 基础集通过 |
| **P3 混合事务** | 6 周 | 关系表 + MVCC + 跨表 Join + time_bucket | ★ 同事务写入 + Join 结果一致 |
| **P4 优化与高基数** | 6 周 | Late materialization、Tag 剪枝、预聚合 | 1e9 点 / 1e5 series 达性能目标 |
| **P5 MySQL 协议层** | 8 周 | Handshake41、认证插件、COM_\*、结果集、Prepared Statement、虚拟系统表 | ★ Navicat/DBeaver/Connector-J 全绿；Parity 对拍 0 差异 |
| **P6 分布式** | 8 周 | Raft 副本、分片、再平衡、分布式查询 | 单节点故障 RTO < 30s |
| **P7 信创适配** | 6 周 | 交叉编译、国密、双协议认证、迁移工具 | **三类国产 CPU × 两种国产 OS × 双协议 全绿** |
| **P8 生态与验收** | 4 周 | Smartbi/DBeaver/Grafana 认证、压测报告 | 出具全套信创测试报告 |

**总计约 56 周 / 8~10 人**（2 存储、2 引擎、1 分布式、1 协议层（MySQL/PG 双协议）、1 信创适配、1 测试、1 PM）。

> 相比 v1（单协议）增加 8 周，主要来自 MySQL 协议层的 Prepared Statement、认证插件、系统表虚拟视图、跨协议对拍测试。
> 协议层可**并行招聘/外包**，不阻塞存储引擎主线。

---

## 十一、风险与对策

| 风险 | 影响 | 对策 |
|---|---|---|
| **LoongArch 生态不成熟**，部分 crate 编译失败 | 阻塞交付 | P0 阶段第一周就打通；禁 C 扩展依赖；准备预编译 vendor 分支 |
| 列存引擎调试成本高（脏数据/编码 bug） | 数据不可信 | 编码器**属性测试**（proptest）+ 标量对拍 + 崩溃注入测试常态化 |
| 分布式一致性 bug | 严重事故 | 先做**单机完全可用**，分布式延后；Raft 用 battle-tested `raft-rs` 不自研共识 |
| 性能不达预期 | 验收失败 | 每个阶段设量化卡口，不达标不进入下一阶段 |
| **MySQL 协议兼容坑多**（大字段长度标记、Prepared 16MB 限制、隐式类型转换歧义） | 驱动连不上/数据错乱 | 协议层按 MySQL 官方 spec 逐条实现 + Wireshark 抓包对拍 + 老驱动真机矩阵测试；**不做"差不多"实现** |
| **双端语义分叉**（同一 SQL 两端结果不同） | 数据不可信，现场事故 | Parity 对拍做 CI 强制卡口；`ProtocolAdapter` 之外禁止任何方言分支 |
| **双协议退化成两套实现** | 维护成本翻倍、长期失控 | 架构约束：wire 层不得依赖存储/执行层；crate 依赖方向 CI 检查 |
| 与现有国产库定位冲突（达梦/金仓/TDSQL 也做时序） | 市场 | 差异化：**纯时序性能** + **PG + MySQL 双协议** + **混合 Join**；并提供到达梦/金仓/PolarDB 的数据同步 |
| 团队 Rust 经验不足 | 进度 | 前 1 个月做编码器 spike（POC），验证开发效率 |

---

## 十二、建议的第一步

不要先写 SQL 层，**先把 P1 的存储引擎原型跑通**，因为它是唯一的真难点：

```
Step 1  段文件读写 + 5 种编码器 + 压缩率对拍报告（2 周）
Step 2  memtable + WAL + 崩溃恢复 + 单机 1e7 点 benchmark（2 周）
Step 3  在飞腾 S2500 + 麒麟 V10 上重跑 benchmark，拿性能基线（1 周）
Step 4  拿 Step 3 的数据决定是否继续投入
```

压缩比和国产平台性能达成率是这个项目的**生死线**，先验证再堆功能。

---

## 附录 A：迁移路径

| 来源 | 迁移方式 |
|---|---|
| **MySQL / MariaDB** | ★ 直接 `mysql -h tmdb -P 3306` 接入；应用层零改动（改连接串即可）；存量表 `mysqldump` → `tmdb-import mysql --sql` 走 MySQL 协议重放，DDL 走方言归一 |
| **PolarDB / OceanBase / TDSQL / TiDB** | 兼容 MySQL 协议，直连 3306；`tmdb-compat` 做 MySQL 方言 DDL 重放（TiDB 的 `AUTO_RANDOM`、`shard_row_id_bits` 等扩展语法需剥离） |
| **Prometheus / VictoriaMetrics** | `tmdb-import prom --url=http://… --table=…` 或远程写直连 |
| **InfluxDB** | Line 协议写入端点（`POST /write`）或 `influx_inspect` dump 转换 |
| **TDengine** | 导出 CSV/TAOS dump → `COPY` 批量导入，STable → 时序表自动映射 |
| **IoTDB** | `export-csv` → `COPY` |
| **PostgreSQL / openGauss / GaussDB** | `pg_dump` → `tmdb-import pg --sql` 走 5432 重放；PG 专有语法降级映射表见 4.4 |
| **达梦 / 金仓** | JDBC 读取 → 关系表 `COPY` 导入，异构 Join 通过 FDW / 同步服务实现 |
| **→ MySQL / TDSQL** | 归档数据导出 Parquet/CSV → 对方 `LOAD DATA`；或 Kafka Connect 增量同步（Binlog 模拟 CDC 从 WAL 派生） |
| **→ 达梦 / 金仓** | 归档数据导出 Parquet/CSV → 对方 `COPY`；或 Kafka Connect 增量同步 |

### A.1 MySQL 应用零改动迁移检查表

| 项 | 动作 | 说明 |
|---|---|---|
| 连接串 | `jdbc:mysql://host:3306/db` → 保持不变 | 只需换 IP/端口 |
| 驱动 | MySQL Connector/J 8.x 保持不变 | 认证插件已支持 |
| 方言 | MySQL 方言直接支持 | 归一层处理差异 |
| 存储引擎 | `ENGINE=InnoDB` 解析后忽略 | 无需改表 |
| 字符集 | `utf8mb4` / `utf8mb4_general_ci` 支持 | 排序规则映射见 4.4 |
| 自增 | `AUTO_INCREMENT` 支持 | |
| 索引 | `KEY idx(a) IN TABLE`、`UNIQUE KEY`、`PRIMARY KEY` 支持 | |
| 视图 | `CREATE VIEW` 支持（v1.1） | |
| 触发器/存储过程/事件 | ✗ 需改造为应用层逻辑或定时任务 | 列入迁移报告 |
| `ON DUPLICATE KEY UPDATE` | ✓ 支持 | |
| `REPLACE INTO` | ✓ 支持 | |
| `LOAD DATA LOCAL INFILE` | ✓ 支持（需服务端 `local_infile=1`） | |
| 存储过程调用 | ✗ | 应用层改写为事务内多条 SQL |
| 读写分离 | ✓ 可配多 DataNode 只读连接 | |
| 字符集 `utf8mb3` | ⚠ 自动升级为 `utf8mb4` 并在迁移报告标注 | |
| 隐式类型转换 | 严格模式，可能暴露原库遗留脏数据 | 迁移报告列出被拒绝的语句 |
| `sql_mode` 依赖 | 逐项对照，提供 `sql_mode` 映射建议 | |

---

## 附录 B：国产数据库横向对比

| 维度 | InfluxDB | TimescaleDB | VictoriaMetrics | TDengine | IoTDB | **tmDB** |
|---|---|---|---|---|---|---|
| 实现语言 | Go | C（PG 扩展） | Go | C/C++ | Java | **Rust** |
| SQL 方言 | 专用（InfluxQL/Flux） | 标准 SQL | PromQL/MetricsQL | 专用 TDengine SQL | 树模型/类 SQL | **标准 SQL，PG + MySQL 双方言** |
| 客户端协议 | HTTP/Line | PostgreSQL wire | HTTP/PromQL | REST/JDBC | Thrift/REST | **PG 5432 + MySQL 3306 + HTTP 8080** |
| 应用迁移成本 | 高 | 中（换协议） | 高 | 高 | 高 | **MySQL 应用改连接串即可（零改动）** |
| 时序↔关系表 Join | ✗ | ✓ 同事务 | ✗ | ✗ | ✗ | **✓ 同事务同快照，跨协议同样可见** |
| 高基数标签 | 弱 | 中（PG 膨胀） | 强 | 强 | 中 | **强（字典+倒排+Roaring）** |
| 压缩比 | 中（~8x） | 中（~5x） | 极强（~20x） | 强（~15x） | 强（~12x） | **目标 ~15x（可配置）** |
| 单机写入 TPS | 高 | 中 | 中高 | 极高 | 极高 | **目标 100w points/s** |
| 边缘/嵌入式 | 弱 | 弱 | 中 | 强 | 强 | **强（静态二进制，无依赖）** |
| 国产 CPU/OS | 需移植 | 需移植 | 需移植 | 部分 | 部分 | **原生支持（飞腾/龙芯/海光/申威）** |
| 国产 BI 直连 | 弱 | 好（PG 协议） | 弱 | 弱 | 弱 | **好（双协议，Smartbi/Navicat/DBeaver 全通）** |
| 国密 | ✗ | ✗ | ✗ | ✗ | ✗ | **✓ SM2/SM3/SM4（双协议共用密钥库）** |
| 依赖外部库 | 少量 | PG 强依赖 | 无 | glibc 强依赖 | JVM | **无（单静态二进制）** |
| 许可证友好度 | MIT/商业 | Apache + 商业 | Apache-2.0 | AGPL v3 | Apache-2.0 | **Apache-2.0 + 全栈可控** |

---

*文档结束*
