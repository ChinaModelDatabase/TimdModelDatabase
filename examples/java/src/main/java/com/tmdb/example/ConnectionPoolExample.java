package com.tmdb.example;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Connection Pool Example - HikariCP with tmDB
 */
public class ConnectionPoolExample {
    private static final Logger log = LoggerFactory.getLogger(ConnectionPoolExample.class);

    private static final String PG_URL = Main.PG_URL;
    private static final String MYSQL_URL = Main.MYSQL_URL;

    public void run() throws Exception {
        log.info("=== Connection Pool Example (HikariCP) ===");

        // PostgreSQL connection pool
        HikariDataSource pgPool = createPgPool();
        try {
            testPool(pgPool, "PostgreSQL");
            concurrentQueries(pgPool, "PostgreSQL");
        } finally {
            pgPool.close();
        }

        // MySQL connection pool
        HikariDataSource mysqlPool = createMysqlPool();
        try {
            testPool(mysqlPool, "MySQL");
        } finally {
            mysqlPool.close();
        }
    }

    private HikariDataSource createPgPool() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(PG_URL);
        config.setUsername(Main.PG_USER);
        config.setPassword(Main.PG_PASSWORD);
        config.setDriverClassName("org.postgresql.Driver");

        // Pool configuration
        config.setMaximumPoolSize(20);
        config.setMinimumIdle(5);
        config.setConnectionTimeout(30000);
        config.setIdleTimeout(600000);
        config.setMaxLifetime(1800000);
        config.setPoolName("tmdb-pg-pool");

        // Performance tuning
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        log.info("Created PostgreSQL connection pool (max=20, min=5)");
        return new HikariDataSource(config);
    }

    private HikariDataSource createMysqlPool() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(MYSQL_URL);
        config.setUsername(Main.MYSQL_USER);
        config.setPassword(Main.MYSQL_PASSWORD);
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");

        // Pool configuration
        config.setMaximumPoolSize(20);
        config.setMinimumIdle(5);
        config.setConnectionTimeout(30000);
        config.setPoolName("tmdb-mysql-pool");

        log.info("Created MySQL connection pool (max=20, min=5)");
        return new HikariDataSource(config);
    }

    private void testPool(HikariDataSource pool, String name) throws Exception {
        log.info("--- {} Connection Pool Test ---", name);
        log.info("Active connections: {}", pool.getHikariPoolMXBean().getActiveConnections());
        log.info("Idle connections: {}", pool.getHikariPoolMXBean().getIdleConnections());
        log.info("Total connections: {}", pool.getHikariPoolMXBean().getTotalConnections());

        // Test basic query
        try (Connection conn = pool.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1")) {
            if (rs.next()) {
                log.info("{} pool test passed", name);
            }
        }
    }

    private void concurrentQueries(HikariDataSource pool, String name) throws Exception {
        log.info("--- {} Concurrent Queries Test ---", name);

        int threads = 10;
        int queriesPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        AtomicLong totalQueries = new AtomicLong(0);
        AtomicLong totalErrors = new AtomicLong(0);

        long startTime = System.currentTimeMillis();

        CountDownLatch latch = new CountDownLatch(threads);
        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    for (int q = 0; q < queriesPerThread; q++) {
                        try (Connection conn = pool.getConnection();
                             Statement stmt = conn.createStatement();
                             ResultSet rs = stmt.executeQuery("SELECT 1")) {
                            if (rs.next()) {
                                totalQueries.incrementAndGet();
                            }
                        }
                    }
                } catch (Exception e) {
                    totalErrors.incrementAndGet();
                    log.error("Query error: {}", e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(60, TimeUnit.SECONDS);
        executor.shutdown();

        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Concurrent test completed:");
        log.info("  Threads: {}", threads);
        log.info("  Queries: {} ({} errors)", totalQueries.get(), totalErrors.get());
        log.info("  Time: {}ms", elapsed);
        log.info("  Throughput: {} queries/s", totalQueries.get() * 1000L / Math.max(elapsed, 1));
    }
}
