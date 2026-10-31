# tmDB Java 使用示例

## 项目结构

```
examples/java/
├── pom.xml                          # Maven 配置
├── src/main/java/com/tmdb/example/
│   ├── Main.java                    # 入口
│   ├── PostgreSQLExample.java       # PostgreSQL JDBC 示例
│   ├── MySQLExample.java            # MySQL JDBC 示例
│   ├── ConnectionPoolExample.java   # HikariCP 连接池示例
│   ├── SensorDataRepository.java    # CRUD 仓储
│   ├── SensorData.java              # 数据模型
│   └── SensorStats.java             # 统计模型
└── README.md
```

## 快速开始

### 1. 前置条件

```bash
# 启动 tmDB
./tmdb start

# 创建数据库
psql -h localhost -p 5432 -U tmdb -c "CREATE DATABASE smart_grid;"
mysql -h localhost -P 3306 -u tmdb -e "CREATE DATABASE smart_grid;"
```

### 2. 编译运行

```bash
cd examples/java

# 编译
mvn clean compile

# 运行所有示例
mvn exec:java

# 只运行 PostgreSQL 示例
mvn exec:java -Dexec.mainClass="com.tmdb.example.PostgreSQLExample"

# 打包
mvn clean package
java -jar target/tmdb-java-example-1.0.0.jar
```

## 示例说明

### PostgreSQL 连接（端口 5432）

```java
// JDBC URL
String url = "jdbc:postgresql://localhost:5432/smart_grid";
Connection conn = DriverManager.getConnection(url, "tmdb", "password");

// 创建表
stmt.execute("CREATE TABLE sensor_data (ts TIMESTAMPTZ, device_id BIGINT, temperature DOUBLE)");

// 插入数据
stmt.execute("INSERT INTO sensor_data VALUES (NOW(), 1001, 25.5)");

// 查询数据
ResultSet rs = stmt.executeQuery("SELECT * FROM sensor_data");
```

### MySQL 连接（端口 3306）

```java
// JDBC URL
String url = "jdbc:mysql://localhost:3306/smart_grid";
Connection conn = DriverManager.getConnection(url, "tmdb", "password");

// 创建表
stmt.execute("CREATE TABLE device_metrics (id BIGINT AUTO_INCREMENT PRIMARY KEY, ts DATETIME(3), device_id BIGINT)");

// 插入数据
stmt.execute("INSERT INTO device_metrics (ts, device_id, metric_value) VALUES (NOW(3), 1001, 25.5)");
```

### HikariCP 连接池

```java
HikariConfig config = new HikariConfig();
config.setJdbcUrl("jdbc:postgresql://localhost:5432/smart_grid");
config.setUsername("tmdb");
config.setPassword("password");
config.setMaximumPoolSize(20);
config.setMinimumIdle(5);

HikariDataSource dataSource = new HikariDataSource(config);
Connection conn = dataSource.getConnection();
```

### 批量插入

```java
String sql = "INSERT INTO sensor_data (ts, device_id, temperature) VALUES (?, ?, ?)";
try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
    for (int i = 0; i < 10000; i++) {
        pstmt.setTimestamp(1, Timestamp.from(Instant.now()));
        pstmt.setLong(2, i);
        pstmt.setDouble(3, 20.0 + i * 0.01);
        pstmt.addBatch();
    }
    pstmt.executeBatch();  // 批量提交
}
```

### 预编译语句

```java
String sql = "SELECT * FROM sensor_data WHERE device_id = ? AND ts > ?";
try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
    pstmt.setLong(1, 1001);
    pstmt.setTimestamp(2, Timestamp.from(Instant.now().minusSeconds(3600)));
    ResultSet rs = pstmt.executeQuery();
}
```

### 事务

```java
conn.setAutoCommit(false);
try {
    // 执行多个操作
    stmt.executeUpdate("INSERT INTO sensor_data ...");
    stmt.executeUpdate("UPDATE device_meta ...");
    conn.commit();
} catch (Exception e) {
    conn.rollback();
    throw e;
}
```

### 连接池并发测试

```java
ExecutorService executor = Executors.newFixedThreadPool(10);
CountDownLatch latch = new CountDownLatch(10);

for (int i = 0; i < 10; i++) {
    executor.submit(() -> {
        for (int q = 0; q < 100; q++) {
            try (Connection conn = pool.getConnection()) {
                // 执行查询
            }
        }
        latch.countDown();
    });
}
latch.await();
```

## 依赖版本

| 依赖 | 版本 | 用途 |
|---|---|---|
| PostgreSQL JDBC | 42.7.1 | PG 协议连接 |
| MySQL JDBC | 8.3.0 | MySQL 协议连接 |
| HikariCP | 5.1.0 | 连接池 |
| SLF4J + Logback | 2.0.9 / 1.4.14 | 日志 |
| Gson | 2.10.1 | JSON 处理 |
| JUnit 5 | 5.10.1 | 单元测试 |

## 性能参考

| 操作 | 数据量 | 耗时 |
|---|---|---|
| 单条插入 | 1 row | <1ms |
| 批量插入 | 1000 rows | ~50ms |
| 批量插入 | 10000 rows | ~400ms |
| 查询 | 1000 rows | ~10ms |
| 并发查询 | 10 threads × 100 queries | ~500ms |
