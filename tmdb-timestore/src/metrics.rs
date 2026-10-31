//! Table metrics for observability.
//!
//! # Scope
//!
//! These counters are per-table and in-process. They describe what this instance
//! has done since it started, not cumulative lifetime totals, and they are not
//! shared between replicas. Anything that needs a durable or cluster-wide view
//! has to count the log or ask the other nodes.
//!
//! # Gauges versus counters
//!
//! Counters ([`MetricsSnapshot::points_written`] and friends) only ever increase.
//! Gauges ([`MetricsSnapshot::row_count`], `memtable_rows`, `segment_count`)
//! describe current state and can go down after a flush, a compaction or a
//! purge. A scrape that treats all of these as monotonic will produce nonsense
//! rate graphs, so the split is explicit here rather than left to the caller.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Cumulative counters, incremented with relaxed ordering.
#[derive(Debug)]
pub struct TableMetrics {
    points_written: AtomicU64,
    points_scanned: AtomicU64,
    bytes_written: AtomicU64,
    bytes_read: AtomicU64,
    flushes: AtomicU64,
    flush_failures: AtomicU64,
    compactions: AtomicU64,
    compaction_failures: AtomicU64,
    compaction_bytes_in: AtomicU64,
    compaction_bytes_out: AtomicU64,
    rows_merged: AtomicU64,
    wal_appends: AtomicU64,
    wal_bytes: AtomicU64,
    wal_recovered_rows: AtomicU64,
    rows_purged: AtomicU64,
    points_rejected: AtomicU64,
    started_at: Instant,
}

impl Default for TableMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl TableMetrics {
    pub fn new() -> Self {
        Self {
            points_written: AtomicU64::new(0),
            points_scanned: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            flushes: AtomicU64::new(0),
            flush_failures: AtomicU64::new(0),
            compactions: AtomicU64::new(0),
            compaction_failures: AtomicU64::new(0),
            compaction_bytes_in: AtomicU64::new(0),
            compaction_bytes_out: AtomicU64::new(0),
            rows_merged: AtomicU64::new(0),
            wal_appends: AtomicU64::new(0),
            wal_bytes: AtomicU64::new(0),
            wal_recovered_rows: AtomicU64::new(0),
            rows_purged: AtomicU64::new(0),
            points_rejected: AtomicU64::new(0),
            started_at: Instant::now(),
        }
    }

    /// Time since this table was opened.
    pub fn uptime(&self) -> Duration {
        self.started_at.elapsed()
    }

    pub fn record_write(&self, points: u64) {
        self.points_written.fetch_add(points, Ordering::Relaxed);
    }

    pub fn record_scan(&self, points: u64) {
        self.points_scanned.fetch_add(points, Ordering::Relaxed);
    }

    pub fn record_bytes_written(&self, bytes: u64) {
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_bytes_read(&self, bytes: u64) {
        self.bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_flush(&self, ok: bool) {
        if ok {
            self.flushes.fetch_add(1, Ordering::Relaxed);
        } else {
            self.flush_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_compaction(&self, ok: bool) {
        if ok {
            self.compactions.fetch_add(1, Ordering::Relaxed);
        } else {
            self.compaction_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record one completed merge. `in`/`out` make the ratio observable, so a
    /// compaction that inflates the data is visible instead of silent.
    pub fn record_merge(&self, bytes_in: u64, bytes_out: u64) {
        self.compaction_bytes_in.fetch_add(bytes_in, Ordering::Relaxed);
        self.compaction_bytes_out.fetch_add(bytes_out, Ordering::Relaxed);
    }

    pub fn record_rows_merged(&self, rows: u64) {
        self.rows_merged.fetch_add(rows, Ordering::Relaxed);
    }

    pub fn record_wal_append(&self, bytes: u64) {
        self.wal_appends.fetch_add(1, Ordering::Relaxed);
        self.wal_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_wal_recovery(&self, rows: u64) {
        self.wal_recovered_rows.fetch_add(rows, Ordering::Relaxed);
    }

    pub fn record_purge(&self, rows: u64) {
        self.rows_purged.fetch_add(rows, Ordering::Relaxed);
    }

    pub fn record_rejection(&self, points: u64) {
        self.points_rejected.fetch_add(points, Ordering::Relaxed);
    }

    pub fn points_written(&self) -> u64 {
        self.points_written.load(Ordering::Relaxed)
    }

    pub fn points_scanned(&self) -> u64 {
        self.points_scanned.load(Ordering::Relaxed)
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }

    pub fn bytes_read(&self) -> u64 {
        self.bytes_read.load(Ordering::Relaxed)
    }

    pub fn flushes(&self) -> u64 {
        self.flushes.load(Ordering::Relaxed)
    }

    pub fn flush_failures(&self) -> u64 {
        self.flush_failures.load(Ordering::Relaxed)
    }

    pub fn compactions(&self) -> u64 {
        self.compactions.load(Ordering::Relaxed)
    }

    pub fn compaction_failures(&self) -> u64 {
        self.compaction_failures.load(Ordering::Relaxed)
    }

    pub fn compaction_bytes_in(&self) -> u64 {
        self.compaction_bytes_in.load(Ordering::Relaxed)
    }

    pub fn compaction_bytes_out(&self) -> u64 {
        self.compaction_bytes_out.load(Ordering::Relaxed)
    }

    pub fn rows_merged(&self) -> u64 {
        self.rows_merged.load(Ordering::Relaxed)
    }

    pub fn wal_appends(&self) -> u64 {
        self.wal_appends.load(Ordering::Relaxed)
    }

    pub fn wal_bytes(&self) -> u64 {
        self.wal_bytes.load(Ordering::Relaxed)
    }

    pub fn wal_recovered_rows(&self) -> u64 {
        self.wal_recovered_rows.load(Ordering::Relaxed)
    }

    pub fn rows_purged(&self) -> u64 {
        self.rows_purged.load(Ordering::Relaxed)
    }

    pub fn points_rejected(&self) -> u64 {
        self.points_rejected.load(Ordering::Relaxed)
    }
}

/// A point-in-time reading of all metrics.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricsSnapshot {
    /// Seconds since the table was opened.
    pub uptime_secs: f64,
    /// Cumulative counters.
    pub points_written: u64,
    pub points_scanned: u64,
    pub bytes_written: u64,
    pub bytes_read: u64,
    pub flushes: u64,
    pub flush_failures: u64,
    pub compactions: u64,
    pub compaction_failures: u64,
    pub compaction_bytes_in: u64,
    pub compaction_bytes_out: u64,
    pub rows_merged: u64,
    pub wal_appends: u64,
    pub wal_bytes: u64,
    pub wal_recovered_rows: u64,
    pub rows_purged: u64,
    pub points_rejected: u64,
    /// Current gauges.
    pub row_count: u64,
    pub memtable_rows: u64,
    pub memtable_bytes: u64,
    pub segment_count: u64,
    pub segment_bytes: u64,
    pub wal_size_bytes: u64,
    pub disk_bytes: u64,
    pub series_count: u64,
    pub tag_cardinality: Vec<(String, u64)>,
}

impl MetricsSnapshot {
    /// Rows written per second since startup.
    pub fn write_rate(&self) -> f64 {
        if self.uptime_secs <= 0.0 {
            0.0
        } else {
            self.points_written as f64 / self.uptime_secs
        }
    }

    /// Rows scanned per second since startup.
    pub fn scan_rate(&self) -> f64 {
        if self.uptime_secs <= 0.0 {
            0.0
        } else {
            self.points_scanned as f64 / self.uptime_secs
        }
    }

    /// How much smaller a compaction made the data.
    ///
    /// `None` when no compaction has run. A value below `1.0` means merges are
    /// inflating the data, which is worth alerting on.
    pub fn compaction_ratio(&self) -> Option<f64> {
        if self.compaction_bytes_in == 0 {
            None
        } else {
            Some(self.compaction_bytes_in as f64 / self.compaction_bytes_out.max(1) as f64)
        }
    }

    /// Overall on-disk compression against an 8-bytes-per-value baseline.
    pub fn compression_ratio(&self) -> Option<f64> {
        let raw = self.row_count.checked_mul(8)?;
        if self.disk_bytes == 0 {
            None
        } else {
            Some(raw as f64 / self.disk_bytes as f64)
        }
    }

    /// Fraction of writes refused for cardinality, in `0.0..=1.0`.
    pub fn rejection_rate(&self) -> f64 {
        let attempted = self.points_written + self.points_rejected;
        if attempted == 0 {
            0.0
        } else {
            self.points_rejected as f64 / attempted as f64
        }
    }

    /// `key=value` lines, one per metric.
    ///
    /// No timestamps: the scrape time belongs to the collector, and repeating
    /// it here would make two collectors of the same snapshot disagree.
    pub fn to_prometheus(&self, prefix: &str) -> String {
        let mut out = String::with_capacity(1024);
        let mut line = |name: &str, value: String| {
            out.push_str(&format!("{}{} {}\n", prefix, name, value));
        };

        line("uptime_seconds", format!("{:.3}", self.uptime_secs));
        line("points_written_total", self.points_written.to_string());
        line("points_scanned_total", self.points_scanned.to_string());
        line("bytes_written_total", self.bytes_written.to_string());
        line("bytes_read_total", self.bytes_read.to_string());
        line("flushes_total", self.flushes.to_string());
        line("flush_failures_total", self.flush_failures.to_string());
        line("compactions_total", self.compactions.to_string());
        line("compaction_failures_total", self.compaction_failures.to_string());
        line("compaction_bytes_in_total", self.compaction_bytes_in.to_string());
        line("compaction_bytes_out_total", self.compaction_bytes_out.to_string());
        line("rows_merged_total", self.rows_merged.to_string());
        line("wal_appends_total", self.wal_appends.to_string());
        line("wal_bytes_total", self.wal_bytes.to_string());
        line("wal_recovered_rows_total", self.wal_recovered_rows.to_string());
        line("rows_purged_total", self.rows_purged.to_string());
        line("points_rejected_total", self.points_rejected.to_string());
        line("row_count", self.row_count.to_string());
        line("memtable_rows", self.memtable_rows.to_string());
        line("memtable_bytes", self.memtable_bytes.to_string());
        line("segment_count", self.segment_count.to_string());
        line("segment_bytes", self.segment_bytes.to_string());
        line("wal_size_bytes", self.wal_size_bytes.to_string());
        line("disk_bytes", self.disk_bytes.to_string());
        line("series_count", self.series_count.to_string());
        line("write_rate", format!("{:.3}", self.write_rate()));
        line("scan_rate", format!("{:.3}", self.scan_rate()));
        line("rejection_rate", format!("{:.6}", self.rejection_rate()));
        if let Some(r) = self.compression_ratio() {
            line("compression_ratio", format!("{:.3}", r));
        }
        if let Some(r) = self.compaction_ratio() {
            line("compaction_ratio", format!("{:.3}", r));
        }
        for (name, count) in &self.tag_cardinality {
            out.push_str(&format!(
                "{}tag_cardinality{{tag=\"{}\"}} {}\n",
                prefix, name, count
            ));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counters_start_at_zero() {
        let m = TableMetrics::new();
        let s = MetricsSnapshot {
            uptime_secs: 1.0,
            points_written: m.points_written.load(Ordering::Relaxed),
            points_scanned: 0,
            bytes_written: 0,
            bytes_read: 0,
            flushes: 0,
            flush_failures: 0,
            compactions: 0,
            compaction_failures: 0,
            compaction_bytes_in: 0,
            compaction_bytes_out: 0,
            rows_merged: 0,
            wal_appends: 0,
            wal_bytes: 0,
            wal_recovered_rows: 0,
            rows_purged: 0,
            points_rejected: 0,
            row_count: 0,
            memtable_rows: 0,
            memtable_bytes: 0,
            segment_count: 0,
            segment_bytes: 0,
            wal_size_bytes: 0,
            disk_bytes: 0,
            series_count: 0,
            tag_cardinality: vec![],
        };
        assert_eq!(s.write_rate(), 0.0);
        assert_eq!(s.rejection_rate(), 0.0);
        assert_eq!(s.compaction_ratio(), None);
        assert_eq!(s.compression_ratio(), None);
    }

    #[test]
    fn test_rates_use_uptime() {
        let s = snapshot(|s| {
            s.uptime_secs = 2.0;
            s.points_written = 1000;
            s.points_scanned = 200;
        });
        assert_eq!(s.write_rate(), 500.0);
        assert_eq!(s.scan_rate(), 100.0);
    }

    #[test]
    fn test_zero_uptime_does_not_divide_by_zero() {
        let s = snapshot(|s| {
            s.uptime_secs = 0.0;
            s.points_written = 1000;
        });
        assert_eq!(s.write_rate(), 0.0);
        assert!(s.write_rate().is_finite());
    }

    #[test]
    fn test_compaction_ratio_reports_shrinkage() {
        let s = snapshot(|s| {
            s.compaction_bytes_in = 1000;
            s.compaction_bytes_out = 250;
        });
        assert_eq!(s.compaction_ratio(), Some(4.0));
    }

    #[test]
    fn test_compaction_ratio_is_none_before_any_merge() {
        let s = snapshot(|_| {});
        assert_eq!(s.compaction_ratio(), None);
    }

    #[test]
    fn test_compaction_ratio_flags_inflation() {
        let s = snapshot(|s| {
            s.compaction_bytes_in = 100;
            s.compaction_bytes_out = 400;
        });
        let r = s.compaction_ratio().unwrap();
        assert!(r < 1.0, "a ratio below 1 means the merge grew the data");
    }

    #[test]
    fn test_compression_ratio_uses_row_count() {
        let s = snapshot(|s| {
            s.row_count = 1000;
            s.disk_bytes = 50;
        });
        assert_eq!(s.compression_ratio(), Some(160.0));
    }

    #[test]
    fn test_compression_ratio_none_when_empty() {
        let s = snapshot(|_| {});
        assert_eq!(s.compression_ratio(), None);
    }

    #[test]
    fn test_rejection_rate() {
        let s = snapshot(|s| {
            s.points_written = 90;
            s.points_rejected = 10;
        });
        assert!((s.rejection_rate() - 0.1).abs() < 1e-9);
    }

    #[test]
    fn test_rejection_rate_zero_when_nothing_written() {
        let s = snapshot(|_| {});
        assert_eq!(s.rejection_rate(), 0.0);
    }

    #[test]
    fn test_prometheus_output_has_one_line_per_metric() {
        let s = snapshot(|s| {
            s.points_written = 42;
            s.row_count = 100;
            s.tag_cardinality = vec![("host".to_string(), 3)];
        });
        let text = s.to_prometheus("tmdb_table_");
        let lines: Vec<&str> = text.lines().collect();

        assert!(lines.contains(&"tmdb_table_points_written_total 42"));
        assert!(lines.contains(&"tmdb_table_row_count 100"));
        assert!(lines
            .iter()
            .any(|l| *l == "tmdb_table_tag_cardinality{tag=\"host\"} 3"));
        assert!(text.ends_with('\n'), "exposition format needs a trailing newline");
    }

    #[test]
    fn test_prometheus_omits_undefined_ratios() {
        let s = snapshot(|_| {});
        let text = s.to_prometheus("t_");
        assert!(!text.contains("compaction_ratio"), "undefined ratios must be absent");
        assert!(!text.contains("compression_ratio"));
    }

    #[test]
    fn test_prometheus_prefix_is_applied_everywhere() {
        let s = snapshot(|_| {});
        for line in s.to_prometheus("x_").lines() {
            assert!(line.starts_with("x_"), "line missing prefix: {}", line);
        }
    }

    #[test]
    fn test_counters_only_increase() {
        let m = TableMetrics::new();
        m.record_write(10);
        m.record_write(5);
        m.record_scan(3);
        m.record_flush(true);
        m.record_flush(false);
        m.record_compaction(true);
        m.record_merge(1000, 400);
        m.record_rows_merged(50);
        m.record_wal_append(20);
        m.record_wal_recovery(7);
        m.record_purge(9);
        m.record_rejection(1);

        assert_eq!(m.points_written.load(Ordering::Relaxed), 15);
        assert_eq!(m.points_scanned.load(Ordering::Relaxed), 3);
        assert_eq!(m.flushes.load(Ordering::Relaxed), 1);
        assert_eq!(m.flush_failures.load(Ordering::Relaxed), 1);
        assert_eq!(m.compactions.load(Ordering::Relaxed), 1);
        assert_eq!(m.compaction_bytes_in.load(Ordering::Relaxed), 1000);
        assert_eq!(m.compaction_bytes_out.load(Ordering::Relaxed), 400);
        assert_eq!(m.rows_merged.load(Ordering::Relaxed), 50);
        assert_eq!(m.wal_appends.load(Ordering::Relaxed), 1);
        assert_eq!(m.wal_bytes.load(Ordering::Relaxed), 20);
        assert_eq!(m.wal_recovered_rows.load(Ordering::Relaxed), 7);
        assert_eq!(m.rows_purged.load(Ordering::Relaxed), 9);
        assert_eq!(m.points_rejected.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_uptime_is_monotonic() {
        let m = TableMetrics::new();
        let first = m.uptime();
        let second = m.uptime();
        assert!(second >= first);
        assert!(first < Duration::from_secs(5));
    }

    /// Build a snapshot, then apply `f` to it.
    fn snapshot(f: impl FnOnce(&mut MetricsSnapshot)) -> MetricsSnapshot {
        let mut s = MetricsSnapshot {
            uptime_secs: 1.0,
            points_written: 0,
            points_scanned: 0,
            bytes_written: 0,
            bytes_read: 0,
            flushes: 0,
            flush_failures: 0,
            compactions: 0,
            compaction_failures: 0,
            compaction_bytes_in: 0,
            compaction_bytes_out: 0,
            rows_merged: 0,
            wal_appends: 0,
            wal_bytes: 0,
            wal_recovered_rows: 0,
            rows_purged: 0,
            points_rejected: 0,
            row_count: 0,
            memtable_rows: 0,
            memtable_bytes: 0,
            segment_count: 0,
            segment_bytes: 0,
            wal_size_bytes: 0,
            disk_bytes: 0,
            series_count: 0,
            tag_cardinality: vec![],
        };
        f(&mut s);
        s
    }
}
