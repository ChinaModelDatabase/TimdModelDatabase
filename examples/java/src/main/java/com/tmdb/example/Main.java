package com.tmdb.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * tmDB Java Example - Entry Point
 *
 * Demonstrates how to connect to tmDB via:
 * 1. PostgreSQL JDBC (port 5432)
 * 2. MySQL JDBC (port 3306)
 * 3. Connection pooling (HikariCP)
 */
public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    // PostgreSQL connection (tmDB PG protocol)
    public static final String PG_URL = "jdbc:postgresql://localhost:5432/smart_grid";
    public static final String PG_USER = "tmdb";
    public static final String PG_PASSWORD = "password";

    // MySQL connection (tmDB MySQL protocol)
    public static final String MYSQL_URL = "jdbc:mysql://localhost:3306/smart_grid?useSSL=false&serverTimezone=UTC";
    public static final String MYSQL_USER = "tmdb";
    public static final String MYSQL_PASSWORD = "password";

    public static void main(String[] args) {
        log.info("=== tmDB Java Example ===");

        // Run PostgreSQL example
        try {
            PostgreSQLExample example = new PostgreSQLExample();
            example.run();
        } catch (Exception e) {
            log.error("PostgreSQL example failed: {}", e.getMessage(), e);
        }

        // Run MySQL example
        try {
            MySQLExample example = new MySQLExample();
            example.run();
        } catch (Exception e) {
            log.error("MySQL example failed: {}", e.getMessage(), e);
        }

        // Run connection pool example
        try {
            ConnectionPoolExample example = new ConnectionPoolExample();
            example.run();
        } catch (Exception e) {
            log.error("Connection pool example failed: {}", e.getMessage(), e);
        }

        log.info("=== All examples completed ===");
    }
}
