package com.tmdb.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.util.List;

/**
 * MySQL JDBC Example - Connect to tmDB via MySQL protocol (port 3306)
 */
public class MySQLExample {
    private static final Logger log = LoggerFactory.getLogger(MySQLExample.class);

    private static final String URL = Main.MYSQL_URL;
    private static final String USER = Main.MYSQL_USER;
    private static final String PASSWORD = Main.MYSQL_PASSWORD;

    public void run() throws Exception {
        log.info("=== MySQL JDBC Example ===");
        log.info("Connecting to: {}", URL);

        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD)) {
            log.info("Connected successfully!");

            testConnection(conn);
            createTable(conn);
            insertData(conn);
            queryData(conn);
            preparedInsert(conn);
            upsertExample(conn);
        }
    }

    private void testConnection(Connection conn) throws SQLException {
        log.info("--- Test Connection ---");
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT 1");
            if (rs.next()) {
                log.info("Connection test passed: {}", rs.getInt(1));
            }
        }
    }

    private void createTable(Connection conn) throws SQLException {
        log.info("--- Create Table ---");
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS device_metrics");

            // Create table (MySQL dialect)
            String sql = """
                CREATE TABLE device_metrics (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    ts DATETIME(3) NOT NULL,
                    device_id BIGINT NOT NULL,
                    metric_name VARCHAR(100) NOT NULL,
                    metric_value DOUBLE NOT NULL,
                    INDEX idx_device_ts (device_id, ts),
                    INDEX idx_metric (metric_name)
                ) ENGINE=tmdb
                """;
            stmt.execute(sql);
            log.info("Table 'device_metrics' created successfully");
        }
    }

    private void insertData(Connection conn) throws SQLException {
        log.info("--- Insert Data ---");
        try (Statement stmt = conn.createStatement()) {
            String sql = """
                INSERT INTO device_metrics (ts, device_id, metric_name, metric_value)
                VALUES
                    (NOW(3), 1001, 'temperature', 25.5),
                    (NOW(3), 1001, 'humidity', 60.2),
                    (NOW(3), 1002, 'temperature', 26.1),
                    (NOW(3), 1002, 'humidity', 58.8),
                    (NOW(3), 1003, 'temperature', 24.8),
                    (NOW(3), 1003, 'voltage', 220.0)
                """;
            int rows = stmt.executeUpdate(sql);
            log.info("Inserted {} rows", rows);
        }
    }

    private void queryData(Connection conn) throws SQLException {
        log.info("--- Query Data ---");
        String sql = "SELECT id, ts, device_id, metric_name, metric_value FROM device_metrics ORDER BY device_id, metric_name";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();

            StringBuilder header = new StringBuilder();
            for (int i = 1; i <= columnCount; i++) {
                header.append(String.format("%-15s", meta.getColumnName(i)));
            }
            log.info(header.toString());
            log.info("-".repeat(columnCount * 15));

            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int i = 1; i <= columnCount; i++) {
                    row.append(String.format("%-15s", rs.getString(i)));
                }
                log.info(row.toString());
            }
        }
    }

    private void preparedInsert(Connection conn) throws SQLException {
        log.info("--- Prepared Statement Insert ---");
        String sql = "INSERT INTO device_metrics (ts, device_id, metric_name, metric_value) VALUES (?, ?, ?, ?)";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            long startTime = System.currentTimeMillis();
            int batchSize = 1000;

            for (int i = 0; i < batchSize; i++) {
                pstmt.setTimestamp(1, Timestamp.from(Instant.now()));
                pstmt.setLong(2, 3000 + i);
                pstmt.setString(3, i % 2 == 0 ? "temperature" : "pressure");
                pstmt.setDouble(4, 100.0 + (i % 50) * 0.5);
                pstmt.addBatch();
            }
            pstmt.executeBatch();

            long elapsed = System.currentTimeMillis() - startTime;
            log.info("Batch inserted {} rows in {}ms ({} rows/s)", batchSize, elapsed,
                    batchSize * 1000L / Math.max(elapsed, 1));
        }
    }

    private void upsertExample(Connection conn) throws SQLException {
        log.info("--- Upsert (ON DUPLICATE KEY UPDATE) ---");
        String sql = """
            INSERT INTO device_metrics (ts, device_id, metric_name, metric_value)
            VALUES (NOW(3), 1001, 'temperature', 28.0)
            ON DUPLICATE KEY UPDATE metric_value = VALUES(metric_value)
            """;

        try (Statement stmt = conn.createStatement()) {
            int rows = stmt.executeUpdate(sql);
            log.info("Upsert affected {} rows", rows);
        }
    }
}
