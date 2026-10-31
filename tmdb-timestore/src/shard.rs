//! Time-window sharding for a time-series table.
//!
//! # Why
//!
//! One segment file per table stops scaling. A year of one-second data is ~31M
//! rows, which is too much to rewrite when a merge is needed and too much to
//! prune when retention bites. Splitting the timeline into fixed windows bounds
//! how much any one operation touches: a compaction only rewrites one window,
//! and a TTL sweep deletes whole windows at a time.
//!
//! # Routing
//!
//! A window is `window_start <= ts < window_start + window_ms`, where
//! `window_start` is `ts` floored to a multiple of `window_ms`. Flooring rather
//! than counting from an epoch anchor means the mapping is stateless: the same
//! timestamp always lands in the same window with no coordination, so a restart
//! needs no manifest.
//!
//! Late data — a timestamp that belongs to a window already flushed — is
//! accepted and reported, because rejecting it would silently lose data. The
//! caller decides whether to reopen the window or append to it.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use parking_lot::RwLock;

use crate::table::Table;
use crate::TableMeta;

/// A table partitioned by time window.
pub struct ShardedTable {
    meta: TableMeta,
    window_ms: u64,
    root: PathBuf,
    /// Windows by start timestamp, so range scans visit them in time order.
    windows: RwLock<BTreeMap<i64, Arc<Table>>>,
}

impl ShardedTable {
    /// Create a sharded table under `root`.
    ///
    /// `window_ms` is clamped to at least 1 so a misconfigured zero cannot cause
    /// a division by zero in the routing arithmetic.
    pub fn create(meta: TableMeta, root: impl AsRef<Path>, window_ms: u64) -> anyhow::Result<Self> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(&root)
            .with_context(|| format!("create shard root {}", root.display()))?;

        let window_ms = window_ms.max(1);
        let mut windows = BTreeMap::new();

        // Reuse whatever previous runs left, so a restart keeps serving the same
        // windows instead of orphaning their files.
        for entry in std::fs::read_dir(&root)?.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let Some(name) = path.file_name() else {
                continue;
            };
            let Some(start) = parse_window_start(&name.to_string_lossy()) else {
                continue;
            };
            let Ok(table) = Table::open(meta.clone(), &path) else {
                tracing::warn!("shard {} could not be opened, skipping", path.display());
                continue;
            };
            windows.insert(start, Arc::new(table));
        }

        tracing::info!(
            "sharded table '{}' opened with {} existing window(s) of {}ms",
            meta.name,
            windows.len(),
            window_ms
        );

        Ok(Self {
            meta,
            window_ms,
            root,
            windows: RwLock::new(windows),
        })
    }

    pub fn meta(&self) -> &TableMeta {
        &self.meta
    }

    pub fn window_ms(&self) -> u64 {
        self.window_ms
    }

    /// Window start that `ts` belongs to, in the table's own timestamp unit.
    ///
    /// Arithmetic happens in milliseconds so a width is expressible in the same
    /// units regardless of `TsUnit`, then the result is converted back. Returning
    /// native units keeps the value directly comparable with a caller's `ts`.
    pub fn window_start(&self, ts: i64) -> i64 {
        let ms = self.meta.ts_unit.to_millis(ts);
        let width = self.window_ms as i64;
        self.meta.ts_unit.from_millis(ms.div_euclid(width) * width)
    }

    /// Window width expressed in the table's own timestamp unit.
    pub fn window_width(&self) -> i64 {
        self.meta.ts_unit.from_millis(self.window_ms as i64).max(1)
    }

    /// Directory for one window.
    pub fn window_dir(&self, start: i64) -> PathBuf {
        self.root.join(format!("w{:020}", start))
    }

    /// Table for a window, creating it on first write.
    fn window_table(&self, start: i64) -> anyhow::Result<Arc<Table>> {
        if let Some(t) = self.windows.read().get(&start) {
            return Ok(Arc::clone(t));
        }

        // Create outside the read lock: opening scans the directory.
        let dir = self.window_dir(start);
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("create window dir {}", dir.display()))?;
        let table = Arc::new(Table::open(self.meta.clone(), &dir)?);

        let mut windows = self.windows.write();
        // Another writer may have won the race; keep one table per window so
        // two handles never write the same files.
        Ok(Arc::clone(
            windows.entry(start).or_insert(table),
        ))
    }

    /// Write rows, routing each to its window.
    ///
    /// Returns how many rows each window received.
    pub fn write(&self, rows: &[(i64, Vec<Option<Vec<u8>>>)]) -> anyhow::Result<BTreeMap<i64, usize>> {
        let mut grouped: BTreeMap<i64, Vec<(i64, Vec<Option<Vec<u8>>>)>> = BTreeMap::new();
        for row in rows {
            grouped
                .entry(self.window_start(row.0))
                .or_default()
                .push(row.clone());
        }

        let mut written = BTreeMap::new();
        for (start, batch) in grouped {
            let table = self.window_table(start)?;
            table.insert_batch(&batch);
            written.insert(start, batch.len());
        }
        Ok(written)
    }

    /// Read a time range, visiting only overlapping windows.
    pub fn read(
        &self,
        ts_min: i64,
        ts_max: i64,
        select_columns: Option<&[usize]>,
    ) -> anyhow::Result<Vec<(i64, Vec<Option<Vec<u8>>>)>> {
        if ts_max < ts_min {
            // An inverted range matches nothing. Building a BTree range from it
            // would panic, so answer directly.
            return Ok(Vec::new());
        }

        let first = self.window_start(ts_min);
        let last = self.window_start(ts_max);

        let candidates: Vec<Arc<Table>> = self
            .windows
            .read()
            .range(first..=last)
            .map(|(_, t)| Arc::clone(t))
            .collect();

        let mut rows = Vec::new();
        for table in candidates {
            rows.extend(table.scan(ts_min, ts_max, &[], select_columns)?);
        }
        rows.sort_by_key(|r| r.0);
        Ok(rows)
    }

    /// Flush every window that has buffered rows.
    pub fn flush(&self) -> anyhow::Result<usize> {
        let tables: Vec<Arc<Table>> = self.windows.read().values().map(Arc::clone).collect();
        let mut flushed = 0;
        for table in tables {
            if table.should_flush() || table.memtable_size() > 0 {
                table.flush()?;
                flushed += 1;
            }
        }
        Ok(flushed)
    }

    /// Compact every window, which is safe to do in parallel because windows
    /// share no files.
    pub fn compact(&self) -> anyhow::Result<usize> {
        let tables: Vec<Arc<Table>> = self.windows.read().values().map(Arc::clone).collect();
        let mut compacted = 0;
        for table in tables {
            table.compact()?;
            compacted += 1;
        }
        Ok(compacted)
    }

    /// Drop windows that retention has expired, deleting their files.
    ///
    /// Only whole windows are removed, and only when the window's last instant is
    /// already outside the retention window. A window that straddles the cutoff
    /// is kept, so a boundary row is never deleted.
    ///
    /// `now` and the window keys are both in the table's own `TsUnit`, so the
    /// retention span is scaled to that unit rather than assumed to be
    /// milliseconds. Window keys are not milliseconds, so a seconds table
    /// compared them against a millisecond cutoff.
    pub fn purge_expired(&self, now: i64) -> anyhow::Result<Vec<PathBuf>> {
        let retention_days = self.meta.retention_days;
        if retention_days == 0 {
            return Ok(Vec::new());
        }
        let unit = self.meta.ts_unit;
        let cutoff = now.saturating_sub((retention_days as i64).saturating_mul(unit.per_day()));
        // A window shorter than one unit still occupies a whole key, so clamp to
        // at least one unit rather than collapsing the span to nothing.
        let window_span = unit.from_millis(self.window_ms as i64).max(1);

        let expired: Vec<i64> = self
            .windows
            .read()
            .keys()
            .copied()
            .filter(|start| start.saturating_add(window_span) <= cutoff)
            .collect();

        let mut removed = Vec::new();
        for start in expired {
            let dir = self.window_dir(start);
            if let Err(e) = std::fs::remove_dir_all(&dir) {
                tracing::warn!("could not remove window {}: {}", dir.display(), e);
                continue;
            }
            self.windows.write().remove(&start);
            removed.push(dir);
        }
        Ok(removed)
    }

    /// Number of live windows.
    pub fn window_count(&self) -> usize {
        self.windows.read().len()
    }

    /// Windows present, by start timestamp.
    pub fn windows(&self) -> Vec<i64> {
        self.windows.read().keys().copied().collect()
    }

    /// Total rows across all windows.
    pub fn row_count(&self) -> u64 {
        self.windows.read().values().map(|t| t.row_count()).sum()
    }

    /// Bytes on disk across all windows.
    pub fn disk_bytes(&self) -> u64 {
        self.windows
            .read()
            .values()
            .map(|t| t.disk_bytes())
            .sum()
    }
}

/// `w00000000001700000000000` -> `1_700_000_000_000`.
fn parse_window_start(name: &str) -> Option<i64> {
    name.strip_prefix('w')?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColumnDef, CompressionLevel, DataType, TsUnit};

    fn meta() -> TableMeta {
        TableMeta {
            table_id: 1,
            name: "sharded".to_string(),
            ts_unit: TsUnit::Millis,
            columns: vec![
                ColumnDef {
                    name: "ts".to_string(),
                    data_type: DataType::Timestamp,
                    is_tag: false,
                    nullable: false,
                    compression: CompressionLevel::Off,
                },
                ColumnDef {
                    name: "value".to_string(),
                    data_type: DataType::Float64,
                    is_tag: false,
                    nullable: true,
                    compression: CompressionLevel::Lz4,
                },
            ],
            chunk_interval_ms: 3_600_000,
            retention_days: 0,
        }
    }

    fn row(ts: i64, v: f64) -> (i64, Vec<Option<Vec<u8>>>) {
        (ts, vec![Some(ts.to_le_bytes().to_vec()), Some(v.to_le_bytes().to_vec())])
    }

    #[test]
    fn test_window_routing_floors_to_boundaries() {
        let t = ShardedTable::create(meta(), std::env::temp_dir().join("unused"), 1000).unwrap();
        // Never opened on disk, only used for the pure routing arithmetic.
        assert_eq!(t.window_start(0), 0);
        assert_eq!(t.window_start(999), 0);
        assert_eq!(t.window_start(1000), 1000);
        assert_eq!(t.window_start(1999), 1000);
        assert_eq!(t.window_start(1_700_000_123_456), 1_700_000_123_000);
    }

    #[test]
    fn test_negative_timestamps_floor_downwards() {
        let t = ShardedTable::create(meta(), std::env::temp_dir().join("unused2"), 1000).unwrap();
        // Rust's `/` truncates towards zero, which would put -1 in window 0
        // instead of -1000.
        assert_eq!(t.window_start(-1), -1000);
        assert_eq!(t.window_start(-1000), -1000);
        assert_eq!(t.window_start(-1001), -2000);
    }

    #[test]
    fn test_zero_window_width_does_not_panic() {
        let t = ShardedTable::create(meta(), std::env::temp_dir().join("unused3"), 0).unwrap();
        assert_eq!(t.window_ms(), 1, "width must be clamped");
        assert_eq!(t.window_start(1234), 1234);
    }

    #[test]
    fn test_write_routes_rows_to_separate_windows() {
        let dir = tempfile::tempdir().unwrap();
        let t = ShardedTable::create(meta(), dir.path(), 1000).unwrap();

        let rows: Vec<_> = (0..30).map(|i| row(i * 100, i as f64)).collect();
        let written = t.write(&rows).unwrap();

        assert_eq!(t.window_count(), 3, "0..2900 spans three 1000ms windows");
        assert_eq!(written.values().sum::<usize>(), 30);
    }

    #[test]
    fn test_read_returns_rows_in_time_order_across_windows() {
        let dir = tempfile::tempdir().unwrap();
        let t = ShardedTable::create(meta(), dir.path(), 1000).unwrap();

        // Write out of order to prove the read path sorts.
        let rows = vec![row(2500, 2.0), row(100, 1.0), row(1800, 3.0)];
        t.write(&rows).unwrap();
        t.flush().unwrap();

        let got = t.read(0, 5000, None).unwrap();
        let timestamps: Vec<i64> = got.iter().map(|r| r.0).collect();
        assert_eq!(timestamps, vec![100, 1800, 2500]);
    }

    #[test]
    fn test_read_spans_multiple_windows() {
        let dir = tempfile::tempdir().unwrap();
        let t = ShardedTable::create(meta(), dir.path(), 1000).unwrap();

        let rows: Vec<_> = (0..50).map(|i| row(i * 200, i as f64)).collect();
        t.write(&rows).unwrap();
        t.flush().unwrap();

        assert_eq!(t.read(0, 20_000, None).unwrap().len(), 50);
    }

    #[test]
    fn test_read_prunes_windows_outside_the_range() {
        let dir = tempfile::tempdir().unwrap();
        let t = ShardedTable::create(meta(), dir.path(), 1000).unwrap();

        t.write(&(0..20).map(|i| row(i * 500, i as f64)).collect::<Vec<_>>())
            .unwrap();
        t.flush().unwrap();
        assert_eq!(t.window_count(), 10);

        // Only the window holding ts=2500 is consulted.
        let got = t.read(2400, 2600, None).unwrap();
        let timestamps: Vec<i64> = got.iter().map(|r| r.0).collect();
        assert_eq!(timestamps, vec![2500], "only ts 2500 falls in the range");
        assert!(got.iter().all(|r| r.0 >= 2400 && r.0 <= 2600));
    }

    #[test]
    fn test_flush_writes_segments_per_window() {
        let dir = tempfile::tempdir().unwrap();
        let t = ShardedTable::create(meta(), dir.path(), 1000).unwrap();

        // 0, 300, ..., 2700 spans three 1000ms windows.
        t.write(&(0..10).map(|i| row(i * 300, i as f64)).collect::<Vec<_>>())
            .unwrap();
        assert_eq!(t.flush().unwrap(), 3);

        for start in t.windows() {
            let seg_dir = t.window_dir(start);
            let files = std::fs::read_dir(&seg_dir)
                .unwrap()
                .flatten()
                .filter(|e| e.path().extension().map(|x| x == "tsb").unwrap_or(false))
                .count();
            assert!(files >= 1, "window {} produced no segment", start);
        }
    }

    #[test]
    fn test_reopen_reuses_existing_windows() {
        let dir = tempfile::tempdir().unwrap();
        {
            let t = ShardedTable::create(meta(), dir.path(), 1000).unwrap();
            t.write(&(0..20).map(|i| row(i * 400, i as f64)).collect::<Vec<_>>())
                .unwrap();
            t.flush().unwrap();
        }

        let t = ShardedTable::create(meta(), dir.path(), 1000).unwrap();
        assert_eq!(t.window_count(), 8, "all windows must be reused, not recreated");
        assert_eq!(t.row_count(), 20);
        assert_eq!(t.read(0, 20_000, None).unwrap().len(), 20);
    }

    #[test]
    fn test_writes_after_reopen_go_to_the_same_window() {
        let dir = tempfile::tempdir().unwrap();
        {
            let t = ShardedTable::create(meta(), dir.path(), 1000).unwrap();
            t.write(&[row(100, 1.0)]).unwrap();
            t.flush().unwrap();
        }

        let t = ShardedTable::create(meta(), dir.path(), 1000).unwrap();
        t.write(&[row(200, 2.0)]).unwrap();
        t.flush().unwrap();

        assert_eq!(t.window_count(), 1, "ts 100 and 200 share a window");
        assert_eq!(t.row_count(), 2);
    }

    #[test]
    fn test_purge_removes_only_fully_expired_windows() {
        let dir = tempfile::tempdir().unwrap();
        let mut m = meta();
        m.retention_days = 1;

        let t = ShardedTable::create(m, dir.path(), 86_400_000).unwrap();
        let now = 1_700_000_000_000i64;
        let day = 86_400_000i64;

        // One window two days old, one current.
        t.write(&[row(now - 2 * day, 1.0), row(now, 2.0)]).unwrap();
        t.flush().unwrap();
        assert_eq!(t.window_count(), 2);

        let removed = t.purge_expired(now).unwrap();
        assert_eq!(removed.len(), 1, "only the stale window is deleted");
        assert_eq!(t.window_count(), 1);
        assert_eq!(t.row_count(), 1);

        // The surviving window's files are still readable.
        assert_eq!(t.read(0, now + day, None).unwrap().len(), 1);
    }

    #[test]
    fn test_purge_keeps_window_straddling_the_cutoff() {
        let dir = tempfile::tempdir().unwrap();
        let mut m = meta();
        m.retention_days = 1;

        let t = ShardedTable::create(m, dir.path(), 7 * 86_400_000).unwrap();
        let now = 1_700_000_000_000i64;

        // A wide window whose start is older than the cutoff but which still
        // contains recent data. Deleting it would lose that data.
        let old_start = (now / (7 * 86_400_000)) * (7 * 86_400_000);
        t.write(&[row(old_start, 1.0), row(now, 2.0)]).unwrap();
        t.flush().unwrap();

        let removed = t.purge_expired(now).unwrap();
        assert!(removed.is_empty(), "a straddling window must be kept");
        assert_eq!(t.row_count(), 2);
    }

    #[test]
    fn test_purge_disabled_when_retention_is_zero() {
        let dir = tempfile::tempdir().unwrap();
        let t = ShardedTable::create(meta(), dir.path(), 1000).unwrap();
        t.write(&[row(1, 1.0)]).unwrap();
        t.flush().unwrap();

        assert!(t.purge_expired(i64::MAX / 2).unwrap().is_empty());
        assert_eq!(t.window_count(), 1, "retention 0 means keep everything");
    }

    #[test]
    fn test_compact_runs_per_window() {
        let dir = tempfile::tempdir().unwrap();
        let t = ShardedTable::create(meta(), dir.path(), 1000).unwrap();

        for round in 0..5 {
            t.write(&(0..10).map(|i| row(round * 5000 + i * 100, i as f64)).collect::<Vec<_>>())
                .unwrap();
            t.flush().unwrap();
        }

        assert_eq!(t.compact().unwrap(), t.window_count());
        assert_eq!(t.row_count(), 50, "compaction must not lose rows");
        assert_eq!(t.read(0, 1_000_000, None).unwrap().len(), 50);
    }

    #[test]
    fn test_second_precision_tables_route_on_real_time() {
        let dir = tempfile::tempdir().unwrap();
        let mut m = meta();
        m.ts_unit = TsUnit::Seconds;

        let t = ShardedTable::create(m, dir.path(), 3_600_000).unwrap();
        // One hour expressed in milliseconds, routed on second-precision input.
        let width = t.window_width();
        assert_eq!(width, 3_600, "an hour is 3600 seconds");

        let start = t.window_start(1_700_000_000);
        assert_eq!(start % width, 0, "window start must be aligned to the width");
        assert!(
            start <= 1_700_000_000 && 1_700_000_000 < start + width,
            "ts must fall inside its own window"
        );
    }

    #[test]
    fn test_nanosecond_tables_route_on_real_time() {
        let dir = tempfile::tempdir().unwrap();
        let mut m = meta();
        m.ts_unit = TsUnit::Nanos;

        let t = ShardedTable::create(m, dir.path(), 60_000).unwrap();
        let width = t.window_width();
        assert_eq!(width, 60_000_000_000, "a minute is 60e9 nanoseconds");

        let ns = 1_700_000_123_456_789_000i64;
        let start = t.window_start(ns);
        assert_eq!(start % width, 0, "must align to a minute in nanos");
        assert!(start <= ns && ns < start + width);
    }

    #[test]
    fn test_empty_read_returns_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let t = ShardedTable::create(meta(), dir.path(), 1000).unwrap();
        assert!(t.read(0, 10_000, None).unwrap().is_empty());
    }

    #[test]
    fn test_inverted_range_returns_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let t = ShardedTable::create(meta(), dir.path(), 1000).unwrap();
        t.write(&[row(500, 1.0)]).unwrap();
        t.flush().unwrap();

        assert!(t.read(9000, 1000, None).unwrap().is_empty());
    }

    #[test]
    fn test_window_name_round_trips() {
        assert_eq!(parse_window_start("w00000000001700000000000"), Some(1_700_000_000_000));
        assert_eq!(parse_window_start("nonsense"), None);
        assert_eq!(parse_window_start("wabc"), None);
    }

    #[test]
    fn test_disk_bytes_reflects_flushed_data() {
        let dir = tempfile::tempdir().unwrap();
        let t = ShardedTable::create(meta(), dir.path(), 1000).unwrap();

        t.write(&(0..100).map(|i| row(i * 100, i as f64)).collect::<Vec<_>>())
            .unwrap();
        t.flush().unwrap();

        assert!(t.disk_bytes() > 0, "flushed windows must occupy space");
    }

    #[test]
    fn test_no_l1_segments_lose_rows_after_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let t = ShardedTable::create(meta(), dir.path(), 100_000).unwrap();

        // Everything in one window, so compaction actually has work to do.
        for round in 0..8 {
            t.write(&(0..20).map(|i| row(round * 100 + i, (round * 100 + i) as f64)).collect::<Vec<_>>())
                .unwrap();
            t.flush().unwrap();
        }

        t.compact().unwrap();
        assert_eq!(t.window_count(), 1);

        let got = t.read(0, 10_000, None).unwrap();
        assert_eq!(got.len(), 160, "compaction lost rows");

        let timestamps: Vec<i64> = got.iter().map(|r| r.0).collect();
        let mut sorted = timestamps.clone();
        sorted.sort_unstable();
        assert_eq!(timestamps, sorted, "compaction must leave rows time-ordered");

        // Every value was written as its own timestamp, so the read-back pair
        // must match row for row. This catches a merge that reorders one column
        // relative to another.
        for (i, r) in got.iter().enumerate() {
            let bytes = r.1[1].as_ref().unwrap();
            let value = f64::from_le_bytes(bytes[..8].try_into().unwrap());
            assert_eq!(
                value, timestamps[i] as f64,
                "row {} carries the value of a different row", i
            );
        }
    }
}
