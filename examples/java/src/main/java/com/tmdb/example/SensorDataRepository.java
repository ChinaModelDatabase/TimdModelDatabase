package com.tmdb.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Sensor Data Repository - CRUD operations for sensor data
 */
public class SensorDataRepository {
    private static final Logger log = LoggerFactory.getLogger(SensorDataRepository.class);

    private final Connection connection;
    private final boolean useMySQL;

    public SensorDataRepository(Connection connection, boolean useMySQL) {
        this.connection = connection;
        this.useMySQL = useMySQL;
    }

    /**
     * Create sensor data table
     */
    public void createTable() throws SQLException {
        String sql = useMySQL ?
            """
            CREATE TABLE IF NOT EXISTS sensor_data (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                ts DATETIME(3) NOT NULL,
                device_id BIGINT NOT NULL,
                temperature DOUBLE,
                humidity DOUBLE,
                status VARCHAR(50),
                INDEX idx_device_ts (device_id, ts)
            ) ENGINE=tmdb
            """ :
            """
            CREATE TABLE IF NOT EXISTS sensor_data (
                ts TIMESTAMPTZ NOT NULL,
                device_id BIGINT NOT NULL,
                temperature DOUBLE PRECISION,
                humidity DOUBLE PRECISION,
                status VARCHAR(50),
                PRIMARY KEY (ts, device_id)
            )
            """;

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
            log.info("Sensor data table created/verified");
        }
    }

    /**
     * Insert sensor data
     */
    public boolean insert(SensorData data) throws SQLException {
        String sql = useMySQL ?
            "INSERT INTO sensor_data (ts, device_id, temperature, humidity, status) VALUES (?, ?, ?, ?, ?)" :
            "INSERT INTO sensor_data (ts, device_id, temperature, humidity, status) VALUES (?, ?, ?, ?, ?)";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setTimestamp(1, Timestamp.from(data.getTimestamp()));
            pstmt.setLong(2, data.getDeviceId());
            pstmt.setDouble(3, data.getTemperature());
            pstmt.setDouble(4, data.getHumidity());
            pstmt.setString(5, data.getStatus());
            return pstmt.executeUpdate() > 0;
        }
    }

    /**
     * Batch insert sensor data
     */
    public int batchInsert(List<SensorData> dataList) throws SQLException {
        String sql = "INSERT INTO sensor_data (ts, device_id, temperature, humidity, status) VALUES (?, ?, ?, ?, ?)";
        int totalInserted = 0;

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            for (SensorData data : dataList) {
                pstmt.setTimestamp(1, Timestamp.from(data.getTimestamp()));
                pstmt.setLong(2, data.getDeviceId());
                pstmt.setDouble(3, data.getTemperature());
                pstmt.setDouble(4, data.getHumidity());
                pstmt.setString(5, data.getStatus());
                pstmt.addBatch();
            }
            int[] results = pstmt.executeBatch();
            for (int r : results) {
                totalInserted += r;
            }
        }
        return totalInserted;
    }

    /**
     * Find sensor data by device ID
     */
    public List<SensorData> findByDeviceId(long deviceId, int limit) throws SQLException {
        String sql = "SELECT ts, device_id, temperature, humidity, status FROM sensor_data WHERE device_id = ? ORDER BY ts DESC LIMIT ?";

        List<SensorData> results = new ArrayList<>();
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setLong(1, deviceId);
            pstmt.setInt(2, limit);

            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    SensorData data = new SensorData();
                    data.setTimestamp(rs.getTimestamp(1).toInstant());
                    data.setDeviceId(rs.getLong(2));
                    data.setTemperature(rs.getDouble(3));
                    data.setHumidity(rs.getDouble(4));
                    data.setStatus(rs.getString(5));
                    results.add(data);
                }
            }
        }
        return results;
    }

    /**
     * Find sensor data by time range
     */
    public List<SensorData> findByTimeRange(Instant start, Instant end) throws SQLException {
        String sql = "SELECT ts, device_id, temperature, humidity, status FROM sensor_data WHERE ts BETWEEN ? AND ? ORDER BY ts";

        List<SensorData> results = new ArrayList<>();
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setTimestamp(1, Timestamp.from(start));
            pstmt.setTimestamp(2, Timestamp.from(end));

            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    SensorData data = new SensorData();
                    data.setTimestamp(rs.getTimestamp(1).toInstant());
                    data.setDeviceId(rs.getLong(2));
                    data.setTemperature(rs.getDouble(3));
                    data.setHumidity(rs.getDouble(4));
                    data.setStatus(rs.getString(5));
                    results.add(data);
                }
            }
        }
        return results;
    }

    /**
     * Find latest sensor data for a device
     */
    public Optional<SensorData> findLatestByDevice(long deviceId) throws SQLException {
        String sql = "SELECT ts, device_id, temperature, humidity, status FROM sensor_data WHERE device_id = ? ORDER BY ts DESC LIMIT 1";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setLong(1, deviceId);

            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    SensorData data = new SensorData();
                    data.setTimestamp(rs.getTimestamp(1).toInstant());
                    data.setDeviceId(rs.getLong(2));
                    data.setTemperature(rs.getDouble(3));
                    data.setHumidity(rs.getDouble(4));
                    data.setStatus(rs.getString(5));
                    return Optional.of(data);
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Get sensor statistics
     */
    public SensorStats getStats(Instant start, Instant end) throws SQLException {
        String sql = """
            SELECT COUNT(*) as count,
                   AVG(temperature) as avg_temp,
                   MAX(temperature) as max_temp,
                   MIN(temperature) as min_temp,
                   AVG(humidity) as avg_humidity
            FROM sensor_data
            WHERE ts BETWEEN ? AND ?
            """;

        SensorStats stats = new SensorStats();
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setTimestamp(1, Timestamp.from(start));
            pstmt.setTimestamp(2, Timestamp.from(end));

            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    stats.setCount(rs.getLong(1));
                    stats.setAvgTemperature(rs.getDouble(2));
                    stats.setMaxTemperature(rs.getDouble(3));
                    stats.setMinTemperature(rs.getDouble(4));
                    stats.setAvgHumidity(rs.getDouble(5));
                }
            }
        }
        return stats;
    }

    /**
     * Delete sensor data older than specified days
     */
    public int deleteOlderThan(int days) throws SQLException {
        String sql = "DELETE FROM sensor_data WHERE ts < NOW() - INTERVAL '" + days + " days'";

        try (Statement stmt = connection.createStatement()) {
            return stmt.executeUpdate(sql);
        }
    }

    /**
     * Count total records
     */
    public long count() throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM sensor_data")) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        }
        return 0;
    }
}
