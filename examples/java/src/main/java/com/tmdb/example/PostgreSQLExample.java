package com.tmdb.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * PostgreSQL JDBC Example - Connect to tmDB via PG protocol (port 5432)
 */
public class PostgreSQLExample {
    private static final Logger log = LoggerFactory.getLogger(PostgreSQLExample.class);

    private static final String URL = Main.PG_URL;
    private static final String USER = Main.PG_USER;
    private static final String PASSWORD = Main.PG_PASSWORD;

    public void run() throws Exception {
        log.info("=== PostgreSQL JDBC Example ===");
        log.info("Connecting to: {}", URL);

        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD)) {
            log.info("Connected successfully!");

            // Test connection
            testConnection(conn);

            // Create table
            createTable(conn);

            // Insert data
            insertData(conn);

            // Query data
            queryData(conn);

            // Batch insert
            batchInsert(conn);

            // Prepared statement
            preparedStatementExample(conn);

            // Transaction example
            transactionExample(conn);

            // Aggregation query
            aggregationExample(conn);
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
            // Drop if exists
            stmt.execute("DROP TABLE IF EXISTS sensor_data");

            // Create table (PostgreSQL dialect)
            String sql = """
                CREATE TABLE sensor_data (
                    ts TIMESTAMPTZ NOT NULL,
                    device_id BIGINT NOT NULL,
                    temperature DOUBLE PRECISION,
                    humidity DOUBLE PRECISION,
                    status VARCHAR(50),
                    PRIMARY KEY (ts, device_id)
                )
                """;
            stmt.execute(sql);
            log.info("Table 'sensor_data' created successfully");
        }
    }

    private void insertData(Connection conn) throws SQLException {
        log.info("--- Insert Data ---");
        try (Statement stmt = conn.createStatement()) {
            String sql = """
                INSERT INTO sensor_data (ts, device_id, temperature, humidity, status)
                VALUES
                    (NOW(), 1001, 25.5, 60.2, 'active'),
                    (NOW(), 1002, 26.1, 58.8, 'active'),
                    (NOW(), 1003, 24.8, 62.1, 'standby'),
                    (NOW(), 1004, 27.3, 55.4, 'active'),
                    (NOW(), 1005, 23.9, 64.7, 'maintenance')
                """;
            int rows = stmt.executeUpdate(sql);
            log.info("Inserted {} rows", rows);
        }
    }

    private void queryData(Connection conn) throws SQLException {
        log.info("--- Query Data ---");
        String sql = "SELECT ts, device_id, temperature, humidity, status FROM sensor_data ORDER BY device_id";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();

            // Print header
            StringBuilder header = new StringBuilder();
            for (int i = 1; i <= columnCount; i++) {
                header.append(String.format("%-20s", meta.getColumnName(i)));
            }
            log.info(header.toString());
            log.info("-".repeat(columnCount * 20));

            // Print rows
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int i = 1; i <= columnCount; i++) {
                    row.append(String.format("%-20s", rs.getString(i)));
                }
                log.info(row.toString());
            }
        }
    }

    private void batchInsert(Connection conn) throws SQLException {
        log.info("--- Batch Insert ---");
        String sql = "INSERT INTO sensor_data (ts, device_id, temperature, humidity, status) VALUES (?, ?, ?, ?, ?)";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            long startTime = System.currentTimeMillis();
            int batchSize = 1000;

            for (int i = 0; i < batchSize; i++) {
                pstmt.setTimestamp(1, Timestamp.from(Instant.now()));
                pstmt.setLong(2, 2000 + i);
                pstmt.setDouble(3, 20.0 + (i % 100) * 0.1);
                pstmt.setDouble(4, 50.0 + (i % 50) * 0.2);
                pstmt.setString(5, i % 3 == 0 ? "active" : "standby");
                pstmt.addBatch();

                if (i % 100 == 0) {
                    pstmt.executeBatch();
                }
            }
            pstmt.executeBatch();

            long elapsed = System.currentTimeMillis() - startTime;
            log.info("Batch inserted {} rows in {}ms ({} rows/s)", batchSize, elapsed,
                    batchSize * 1000L / Math.max(elapsed, 1));
        }
    }

    private void preparedStatementExample(Connection conn) throws SQLException {
        log.info("--- Prepared Statement ---");
        String sql = "SELECT device_id, temperature FROM sensor_data WHERE device_id = ? ORDER BY ts DESC LIMIT ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, 1001);
            pstmt.setInt(2, 5);

            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    log.info("Device: {}, Temperature: {}", rs.getLong(1), rs.getDouble(2));
                }
            }
        }
    }

    private void transactionExample(Connection conn) throws SQLException {
        log.info("--- Transaction Example ---");
        boolean autoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);

        try {
            // Insert in transaction
            try (PreparedStatement pstmt = conn.prepareStatement(
                    "INSERT INTO sensor_data (ts, device_id, temperature, humidity, status) VALUES (?, ?, ?, ?, ?)")) {
                pstmt.setTimestamp(1, Timestamp.from(Instant.now()));
                pstmt.setLong(2, 9999);
                pstmt.setDouble(3, 30.0);
                pstmt.setDouble(4, 40.0);
                pstmt.setString(5, "test");
                pstmt.executeUpdate();
            }

            // Update in same transaction
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("UPDATE sensor_data SET temperature = 31.0 WHERE device_id = 9999");
            }

            conn.commit();
            log.info("Transaction committed successfully");

        } catch (Exception e) {
            conn.rollback();
            log.error("Transaction rolled back: {}", e.getMessage());
            throw e;
        } finally {
            conn.setAutoCommit(autoCommit);
        }
    }

    private void aggregationExample(Connection conn) throws SQLException {
        log.info("--- Aggregation Example ---");
        String sql = """
            SELECT
                status,
                COUNT(*) as count,
                AVG(temperature) as avg_temp,
                MAX(temperature) as max_temp,
                MIN(temperature) as min_temp
            FROM sensor_data
            GROUP BY status
            ORDER BY status
            """;

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                log.info("Status: {}, Count: {}, Avg: {:.2f}, Max: {:.2f}, Min: {:.2f}",
                        rs.getString(1), rs.getInt(2),
                        rs.getDouble(3), rs.getDouble(4), rs.getDouble(5));
            }
        }
    }
}
