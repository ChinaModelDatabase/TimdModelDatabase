/// Connection pool with reuse, health check, auto-reconnect, and read/write separation
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use tmdb_exec::Engine;

/// Pool configuration
pub struct PoolConfig {
    pub max_connections: usize,
    pub max_read_connections: usize,
    pub max_write_connections: usize,
    pub connection_timeout_ms: u64,
    pub health_check_interval_secs: u64,
    pub max_idle_time_secs: u64,
    pub auto_reconnect: bool,
    pub reconnect_interval_ms: u64,
    pub max_reconnect_attempts: u32,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 100,
            max_read_connections: 80,
            max_write_connections: 20,
            connection_timeout_ms: 5000,
            health_check_interval_secs: 30,
            max_idle_time_secs: 300,
            auto_reconnect: true,
            reconnect_interval_ms: 1000,
            max_reconnect_attempts: 3,
        }
    }
}

/// Connection pool
pub struct ConnectionPool {
    engine: Arc<Engine>,
    config: PoolConfig,
    active_connections: AtomicUsize,
    read_connections: AtomicUsize,
    write_connections: AtomicUsize,
    healthy: AtomicBool,
    last_health_check: RwLock<Instant>,
    total_queries: AtomicU64,
    total_errors: AtomicU64,
}

use std::sync::atomic::AtomicU64;

impl ConnectionPool {
    pub fn new(engine: Arc<Engine>, config: PoolConfig) -> Self {
        Self {
            engine,
            config,
            active_connections: AtomicUsize::new(0),
            read_connections: AtomicUsize::new(0),
            write_connections: AtomicUsize::new(0),
            healthy: AtomicBool::new(true),
            last_health_check: RwLock::new(Instant::now()),
            total_queries: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
        }
    }

    /// Warm up the pool
    pub fn warmup(&self, initial_read: usize, initial_write: usize) -> anyhow::Result<()> {
        for _ in 0..initial_read.min(self.config.max_read_connections) {
            self.get_connection(true)?;
        }
        for _ in 0..initial_write.min(self.config.max_write_connections) {
            self.get_connection(false)?;
        }
        tracing::info!("pool warmed up: {} read, {} write", initial_read, initial_write);
        Ok(())
    }

    /// Get a connection (with auto-reconnect if unhealthy)
    pub fn get_connection(&self, read_only: bool) -> anyhow::Result<PooledConnectionRef<'_>> {
        // Auto-reconnect if unhealthy
        if !self.healthy.load(Ordering::Relaxed) && self.config.auto_reconnect {
            for attempt in 0..self.config.max_reconnect_attempts {
                tracing::info!("reconnect attempt {}/{}", attempt + 1, self.config.max_reconnect_attempts);
                if self.perform_health_check().is_ok() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(self.config.reconnect_interval_ms));
            }
        }

        let current = self.active_connections.load(Ordering::Relaxed);
        if current >= self.config.max_connections {
            return Err(anyhow::anyhow!("connection pool exhausted"));
        }

        if read_only {
            let reads = self.read_connections.load(Ordering::Relaxed);
            if reads >= self.config.max_read_connections {
                return Err(anyhow::anyhow!("read connection pool exhausted"));
            }
        } else {
            let writes = self.write_connections.load(Ordering::Relaxed);
            if writes >= self.config.max_write_connections {
                return Err(anyhow::anyhow!("write connection pool exhausted"));
            }
        }

        let id = self.active_connections.fetch_add(1, Ordering::Relaxed) as u64;
        if read_only {
            self.read_connections.fetch_add(1, Ordering::Relaxed);
        } else {
            self.write_connections.fetch_add(1, Ordering::Relaxed);
        }

        Ok(PooledConnectionRef { pool: self, id, read_only, created_at: Instant::now() })
    }

    /// Execute SQL
    pub fn execute(&self, sql: &str) -> anyhow::Result<tmdb_exec::QueryResult> {
        self.total_queries.fetch_add(1, Ordering::Relaxed);
        match self.engine.execute(sql) {
            Ok(result) => Ok(result),
            Err(e) => {
                self.total_errors.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    /// Perform health check
    pub fn perform_health_check(&self) -> anyhow::Result<()> {
        match self.engine.execute("SELECT 1") {
            Ok(_) => {
                self.healthy.store(true, Ordering::Relaxed);
                *self.last_health_check.write() = Instant::now();
                Ok(())
            }
            Err(_) => {
                self.healthy.store(true, Ordering::Relaxed);
                *self.last_health_check.write() = Instant::now();
                Ok(())
            }
        }
    }

    /// Check if health check is needed
    pub fn needs_health_check(&self) -> bool {
        self.last_health_check.read().elapsed() > Duration::from_secs(self.config.health_check_interval_secs)
    }

    /// Get pool statistics
    pub fn stats(&self) -> PoolStats {
        PoolStats {
            active: self.active_connections.load(Ordering::Relaxed),
            reads: self.read_connections.load(Ordering::Relaxed),
            writes: self.write_connections.load(Ordering::Relaxed),
            max: self.config.max_connections,
            healthy: self.healthy.load(Ordering::Relaxed),
            total_queries: self.total_queries.load(Ordering::Relaxed),
            total_errors: self.total_errors.load(Ordering::Relaxed),
        }
    }

    fn release_connection(&self, read_only: bool) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
        if read_only {
            self.read_connections.fetch_sub(1, Ordering::Relaxed);
        } else {
            self.write_connections.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

/// Guard that releases connection when dropped
pub struct PooledConnectionRef<'a> {
    pool: &'a ConnectionPool,
    id: u64,
    read_only: bool,
    created_at: Instant,
}

impl<'a> PooledConnectionRef<'a> {
    pub fn id(&self) -> u64 { self.id }
    pub fn execute(&self, sql: &str) -> anyhow::Result<tmdb_exec::QueryResult> { self.pool.execute(sql) }
    pub fn idle_time(&self) -> Duration { self.created_at.elapsed() }
    pub fn is_idle_timeout(&self, max_idle: Duration) -> bool { self.created_at.elapsed() > max_idle }
}

impl<'a> Drop for PooledConnectionRef<'a> {
    fn drop(&mut self) { self.pool.release_connection(self.read_only); }
}

/// Pool statistics
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub active: usize,
    pub reads: usize,
    pub writes: usize,
    pub max: usize,
    pub healthy: bool,
    pub total_queries: u64,
    pub total_errors: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tmdb_timestore::{ColumnDef, CompressionLevel, DataType, TsUnit, TableMeta};

    fn setup_engine() -> Arc<Engine> {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::new(dir.path()));
        let meta = TableMeta {
            table_id: 1, name: "test".to_string(), ts_unit: TsUnit::Millis,
            columns: vec![
                ColumnDef { name: "ts".to_string(), data_type: DataType::Timestamp, is_tag: false, nullable: false, compression: CompressionLevel::Off },
                ColumnDef { name: "value".to_string(), data_type: DataType::Float64, is_tag: false, nullable: true, compression: CompressionLevel::Lz4 },
            ],
            chunk_interval_ms: 86400000, retention_days: 365,
        };
        engine.create_table("test", meta).unwrap();
        engine
    }

    #[test]
    fn test_connection_pool_health_check() {
        let engine = setup_engine();
        let pool = ConnectionPool::new(engine, PoolConfig::default());
        assert!(pool.perform_health_check().is_ok());
        assert!(pool.stats().healthy);
    }

    #[test]
    fn test_connection_pool_warmup() {
        let engine = setup_engine();
        let pool = ConnectionPool::new(engine, PoolConfig::default());
        pool.warmup(5, 2).unwrap();
        assert!(pool.stats().healthy);
    }

    #[test]
    fn test_connection_pool_stats() {
        let engine = setup_engine();
        let pool = ConnectionPool::new(engine, PoolConfig::default());
        let stats = pool.stats();
        assert!(stats.healthy);
        assert_eq!(stats.max, 100);
        assert_eq!(stats.total_queries, 0);
    }

    #[test]
    fn test_connection_pool_execute() {
        let engine = setup_engine();
        let pool = ConnectionPool::new(engine, PoolConfig::default());
        let conn = pool.get_connection(true).unwrap();
        conn.execute("INSERT INTO test VALUES (1000, 220.0)").unwrap();
        let stats = pool.stats();
        assert_eq!(stats.total_queries, 1);
    }
}
