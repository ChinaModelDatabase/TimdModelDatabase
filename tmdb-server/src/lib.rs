/// Simple server that accepts connections and executes SQL via the engine
pub mod metrics;
pub mod pool;
pub mod cache;
pub mod http;

use std::sync::Arc;

use tmdb_exec::{Engine, QueryResult};
use tmdb_timestore::{DataType, TableMeta};

pub use metrics::{Metrics, Timer};

/// Server configuration
pub struct ServerConfig {
    pub pg_listen: String,
    pub mysql_listen: String,
    pub data_dir: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            pg_listen: "0.0.0.0:5432".to_string(),
            mysql_listen: "0.0.0.0:3306".to_string(),
            data_dir: "/tmp/tmdb_data".to_string(),
        }
    }
}

/// Simple TCP server (demo version)
pub struct Server {
    engine: Arc<Engine>,
    metrics: Arc<Metrics>,
}

impl Server {
    pub fn new(config: ServerConfig) -> Self {
        let engine = Arc::new(Engine::new(&config.data_dir));
        let metrics = Metrics::new();
        Self { engine, metrics }
    }

    /// Execute SQL and return result as string
    pub fn execute_sql(&self, sql: &str) -> String {
        let timer = Timer::start();
        let result = self.engine.execute(sql);
        let duration = timer.elapsed_ms();
        self.metrics.record_query(duration);

        match result {
            Ok(QueryResult::Empty) => "OK".to_string(),
            Ok(QueryResult::Affected(n)) => {
                self.metrics.record_write(n, 0);
                format!("Affected {} rows", n)
            }
            Ok(QueryResult::Rows { columns, types, rows }) => {
                let row_count = rows.len() as u64;
                self.metrics.record_rows(row_count);
                self.metrics.record_segments(self.engine.tables.iter().map(|t| t.segment_count() as u64).sum());

                let mut result = String::new();
                result.push_str(&columns.join(" | "));
                result.push('\n');
                result.push_str(&"-".repeat(columns.len() * 15));
                result.push('\n');
                for row in &rows {
                    let vals: Vec<String> = row
                        .iter()
                        .enumerate()
                        .map(|(i, v)| {
                            // The declared column type decides how the bytes are
                            // read. Guessing from the byte length would render a
                            // timestamp as a denormal float and a bigint as a
                            // double, so both would come back as "0.00".
                            let ty = types.get(i).copied().unwrap_or(DataType::String);
                            match tmdb_exec::Value::decode(&ty, v.as_deref()) {
                                tmdb_exec::Value::Null => "NULL".to_string(),
                                decoded => decoded.to_text(),
                            }
                        })
                        .collect();
                    result.push_str(&vals.join(" | "));
                    result.push('\n');
                }
                result
            }
            Err(e) => format!("Error: {}", e),
        }
    }

    /// Get metrics endpoint
    pub fn metrics_endpoint(&self) -> String {
        self.metrics.to_prometheus()
    }

    /// Get engine reference
    pub fn engine(&self) -> &Arc<Engine> {
        &self.engine
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_execute() {
        let server = Server::new(ServerConfig {
            data_dir: tempfile::tempdir().unwrap().into_path().to_string_lossy().to_string(),
            ..Default::default()
        });

        // Create table
        let result = server.execute_sql("CREATE TABLE test (ts TIMESTAMP, value DOUBLE)");
        assert_eq!(result, "OK");

        // Insert
        let result = server.execute_sql("INSERT INTO test VALUES (1000, 220.0)");
        assert!(result.contains("Affected"));

        // Flush
        server.engine().flush_table("test").unwrap();

        // Select
        let result = server.execute_sql("SELECT * FROM test");
        assert!(result.contains("1000"));
        assert!(result.contains("220"));
    }
}
