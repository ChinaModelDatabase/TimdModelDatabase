/// Metrics collection with alerting rules
use std::sync::atomic::{AtomicU64, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Alert severity
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
}

/// Alert rule
#[derive(Debug, Clone)]
pub struct AlertRule {
    pub name: String,
    pub condition: AlertCondition,
    pub severity: AlertSeverity,
    pub message: String,
}

#[derive(Debug, Clone)]
pub enum AlertCondition {
    Threshold { metric: String, op: AlertOp, value: f64 },
    Rate { metric: String, window_secs: u64, threshold: f64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertOp {
    GreaterThan,
    LessThan,
    Equal,
}

/// Metrics collector
#[derive(Debug, Default)]
pub struct Metrics {
    pub writes_total: AtomicU64,
    pub writes_bytes: AtomicU64,
    pub flushes_total: AtomicU64,
    pub flush_duration_ms: AtomicU64,
    pub queries_total: AtomicU64,
    pub query_duration_ms: AtomicU64,
    pub segments_total: AtomicU64,
    pub compactions_total: AtomicU64,
    pub rows_total: AtomicU64,
    pub errors_total: AtomicU64,
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub connections_active: AtomicU64,
    pub connections_total: AtomicU64,
}

impl Metrics {
    pub fn new() -> Arc<Self> { Arc::new(Self::default()) }

    pub fn record_write(&self, rows: u64, bytes: u64) {
        self.writes_total.fetch_add(rows, Ordering::Relaxed);
        self.writes_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
    pub fn record_flush(&self, duration_ms: u64) {
        self.flushes_total.fetch_add(1, Ordering::Relaxed);
        self.flush_duration_ms.fetch_add(duration_ms, Ordering::Relaxed);
    }
    pub fn record_query(&self, duration_ms: u64) {
        self.queries_total.fetch_add(1, Ordering::Relaxed);
        self.query_duration_ms.fetch_add(duration_ms, Ordering::Relaxed);
    }
    pub fn record_error(&self) { self.errors_total.fetch_add(1, Ordering::Relaxed); }
    pub fn record_compaction(&self) { self.compactions_total.fetch_add(1, Ordering::Relaxed); }
    pub fn record_rows(&self, count: u64) { self.rows_total.fetch_add(count, Ordering::Relaxed); }
    pub fn record_segments(&self, count: u64) { self.segments_total.fetch_add(count, Ordering::Relaxed); }
    pub fn record_cache_hit(&self) { self.cache_hits.fetch_add(1, Ordering::Relaxed); }
    pub fn record_cache_miss(&self) { self.cache_misses.fetch_add(1, Ordering::Relaxed); }
    pub fn record_connection(&self) { self.connections_total.fetch_add(1, Ordering::Relaxed); }

    /// Export as Prometheus text format
    pub fn to_prometheus(&self) -> String {
        format!(
            "# HELP tmdb_writes_total Total rows written\n\
             # TYPE tmdb_writes_total counter\n\
             tmdb_writes_total {}\n\n\
             # HELP tmdb_writes_bytes Total bytes written\n\
             # TYPE tmdb_writes_bytes counter\n\
             tmdb_writes_bytes {}\n\n\
             # HELP tmdb_flushes_total Total flush operations\n\
             # TYPE tmdb_flushes_total counter\n\
             tmdb_flushes_total {}\n\n\
             # HELP tmdb_queries_total Total queries executed\n\
             # TYPE tmdb_queries_total counter\n\
             tmdb_queries_total {}\n\n\
             # HELP tmdb_query_duration_ms Total query duration in ms\n\
             # TYPE tmdb_query_duration_ms counter\n\
             tmdb_query_duration_ms {}\n\n\
             # HELP tmdb_segments_total Total segments\n\
             # TYPE tmdb_segments_total gauge\n\
             tmdb_segments_total {}\n\n\
             # HELP tmdb_compactions_total Total compactions\n\
             # TYPE tmdb_compactions_total counter\n\
             tmdb_compactions_total {}\n\n\
             # HELP tmdb_rows_total Total rows stored\n\
             # TYPE tmdb_rows_total gauge\n\
             tmdb_rows_total {}\n\n\
             # HELP tmdb_errors_total Total errors\n\
             # TYPE tmdb_errors_total counter\n\
             tmdb_errors_total {}\n\n\
             # HELP tmdb_cache_hits Total cache hits\n\
             # TYPE tmdb_cache_hits counter\n\
             tmdb_cache_hits {}\n\n\
             # HELP tmdb_cache_misses Total cache misses\n\
             # TYPE tmdb_cache_misses counter\n\
             tmdb_cache_misses {}\n",
            self.writes_total.load(Ordering::Relaxed),
            self.writes_bytes.load(Ordering::Relaxed),
            self.flushes_total.load(Ordering::Relaxed),
            self.queries_total.load(Ordering::Relaxed),
            self.query_duration_ms.load(Ordering::Relaxed),
            self.segments_total.load(Ordering::Relaxed),
            self.compactions_total.load(Ordering::Relaxed),
            self.rows_total.load(Ordering::Relaxed),
            self.errors_total.load(Ordering::Relaxed),
            self.cache_hits.load(Ordering::Relaxed),
            self.cache_misses.load(Ordering::Relaxed),
        )
    }

    /// Check alerting rules
    pub fn check_alerts(&self, rules: &[AlertRule]) -> Vec<Alert> {
        let mut alerts = Vec::new();
        for rule in rules {
            if let Some(alert) = self.check_alert_rule(rule) {
                alerts.push(alert);
            }
        }
        alerts
    }

    fn check_alert_rule(&self, rule: &AlertRule) -> Option<Alert> {
        match &rule.condition {
            AlertCondition::Threshold { metric, op, value } => {
                let current = self.get_metric_value(metric);
                let triggered = match op {
                    AlertOp::GreaterThan => current > *value,
                    AlertOp::LessThan => current < *value,
                    AlertOp::Equal => (current - *value).abs() < 0.001,
                };
                if triggered {
                    Some(Alert {
                        rule: rule.clone(),
                        current_value: current,
                        timestamp: Instant::now(),
                    })
                } else {
                    None
                }
            }
            AlertCondition::Rate { metric: _, window_secs: _, threshold: _ } => {
                // TODO: implement rate-based alerting
                None
            }
        }
    }

    fn get_metric_value(&self, metric: &str) -> f64 {
        match metric {
            "writes_total" => self.writes_total.load(Ordering::Relaxed) as f64,
            "queries_total" => self.queries_total.load(Ordering::Relaxed) as f64,
            "errors_total" => self.errors_total.load(Ordering::Relaxed) as f64,
            "segments_total" => self.segments_total.load(Ordering::Relaxed) as f64,
            "rows_total" => self.rows_total.load(Ordering::Relaxed) as f64,
            "cache_hit_rate" => {
                let hits = self.cache_hits.load(Ordering::Relaxed) as f64;
                let misses = self.cache_misses.load(Ordering::Relaxed) as f64;
                if hits + misses > 0.0 { hits / (hits + misses) } else { 0.0 }
            }
            _ => 0.0,
        }
    }
}

/// Alert
#[derive(Debug, Clone)]
pub struct Alert {
    pub rule: AlertRule,
    pub current_value: f64,
    pub timestamp: Instant,
}

/// Timer helper
pub struct Timer { start: Instant }
impl Timer {
    pub fn start() -> Self { Self { start: Instant::now() } }
    pub fn elapsed_ms(&self) -> u64 { self.start.elapsed().as_millis() as u64 }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_basic() {
        let metrics = Metrics::new();
        metrics.record_write(100, 8000);
        metrics.record_flush(50);
        metrics.record_query(10);
        metrics.record_error();
        metrics.record_cache_hit();
        metrics.record_cache_miss();

        let prom = metrics.to_prometheus();
        assert!(prom.contains("tmdb_writes_total 100"));
        assert!(prom.contains("tmdb_flushes_total 1"));
        assert!(prom.contains("tmdb_errors_total 1"));
        assert!(prom.contains("tmdb_cache_hits 1"));
    }

    #[test]
    fn test_alerting() {
        let metrics = Metrics::new();
        metrics.record_error();

        let rules = vec![AlertRule {
            name: "high_errors".to_string(),
            condition: AlertCondition::Threshold {
                metric: "errors_total".to_string(),
                op: AlertOp::GreaterThan,
                value: 0.0,
            },
            severity: AlertSeverity::Warning,
            message: "Too many errors".to_string(),
        }];

        let alerts = metrics.check_alerts(&rules);
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].rule.severity, AlertSeverity::Warning);
    }
}
