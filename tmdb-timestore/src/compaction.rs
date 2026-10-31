//! Compaction: merging segments to reduce read amplification and reclaim space.
//!
//! # Levels
//!
//! - `L0` — written straight from a memtable flush. Cheap to produce, may
//!   overlap any other segment in time.
//! - `L1` — the product of an L0 merge. Sorted by time, one segment per group.
//! - `L2` — final, read-only, non-overlapping with other L2 segments.
//!
//! Because a query has to open every segment whose time range overlaps it,
//! overlapping L0 segments directly multiply read amplification. Merging them
//! into a single sorted L1 segment is the whole point of this module.
//!
//! # Crash safety
//!
//! A merge writes a *new* file first and only then deletes the inputs, so an
//! interrupted compaction leaves both copies on disk rather than losing data.
//! [`CompactionManager::recover_orphans`] reclaims those leftovers at startup.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

use crate::encoding::{ColumnData, CompressionLevel, DataType};
use crate::segment::file::{SegmentReader, SegmentWriter};
use crate::TsUnit;

/// Suffix used for segments produced by compaction, so orphan recovery can tell
/// merged output from flush output.
pub const COMPACTED_SUFFIX: &str = "c.tsb";

/// Compaction level
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CompactionLevel {
    L0, // Fresh from flush, may overlap
    L1, // Merged, non-overlapping
    L2, // Final, read-only
}

impl CompactionLevel {
    fn tag(&self) -> &'static str {
        match self {
            CompactionLevel::L0 => "L0",
            CompactionLevel::L1 => "L1",
            CompactionLevel::L2 => "L2",
        }
    }
}

/// A segment file reference
#[derive(Debug, Clone)]
pub struct SegmentRef {
    pub path: PathBuf,
    pub level: CompactionLevel,
    pub min_ts: i64,
    pub max_ts: i64,
    pub row_count: u64,
    pub byte_size: u64,
}

impl SegmentRef {
    /// Do this segment and `other` share any timestamp?
    pub fn overlaps(&self, other: &SegmentRef) -> bool {
        self.min_ts <= other.max_ts && other.min_ts <= self.max_ts
    }
}

/// Everything a merge needs to know about the table being compacted.
#[derive(Debug, Clone)]
pub struct CompactionSpec {
    pub table_id: u64,
    pub ts_unit: TsUnit,
    pub column_types: Vec<DataType>,
    pub compression: CompressionLevel,
    pub rows_per_block: usize,
}

impl CompactionSpec {
    pub fn new(table_id: u64, ts_unit: TsUnit, column_types: Vec<DataType>) -> Self {
        Self {
            table_id,
            ts_unit,
            column_types,
            compression: CompressionLevel::Lz4,
            rows_per_block: crate::segment::file::DEFAULT_ROWS_PER_BLOCK,
        }
    }
}

/// Result of one merge.
#[derive(Debug, Clone)]
pub struct MergeResult {
    pub segment: SegmentRef,
    pub inputs: Vec<SegmentRef>,
    /// Bytes of input vs. output, for logging the actual saving.
    pub bytes_in: u64,
    pub bytes_out: u64,
}

impl MergeResult {
    /// Ratio of input bytes to output bytes; `None` when the input was empty.
    pub fn compression_ratio(&self) -> Option<f64> {
        if self.bytes_in == 0 {
            None
        } else {
            Some(self.bytes_in as f64 / self.bytes_out.max(1) as f64)
        }
    }
}

/// Compaction manager
pub struct CompactionManager {
    segments: RwLock<Vec<SegmentRef>>,
    data_dir: PathBuf,
    retention_days: u32,
    /// Unit of the timestamps in `SegmentRef::min_ts` and `max_ts`.
    ///
    /// Retention is stored in days, so comparing it against a timestamp means
    /// converting one to the other. Assuming milliseconds here made every
    /// non-millisecond table purge on the wrong schedule: a seconds table read
    /// `now - retention_days*86_400_000` against second-resolution timestamps,
    /// which never expired, and a nanosecond table expired almost immediately.
    ts_unit: TsUnit,
    seq: AtomicU64,
    /// Counters for `metrics.rs` to expose.
    compactions: AtomicU64,
    rows_merged: AtomicU64,
}

impl CompactionManager {
    pub fn new(data_dir: impl AsRef<Path>, retention_days: u32) -> Self {
        Self::with_unit(data_dir, retention_days, TsUnit::Millis)
    }

    /// A manager that interprets segment timestamps in `ts_unit`.
    pub fn with_unit(data_dir: impl AsRef<Path>, retention_days: u32, ts_unit: TsUnit) -> Self {
        Self {
            segments: RwLock::new(Vec::new()),
            data_dir: data_dir.as_ref().to_path_buf(),
            retention_days,
            ts_unit,
            seq: AtomicU64::new(0),
            compactions: AtomicU64::new(0),
            rows_merged: AtomicU64::new(0),
        }
    }

    /// The unit segment timestamps are expressed in.
    pub fn ts_unit(&self) -> TsUnit {
        self.ts_unit
    }

    /// How far back data is kept, in this manager's timestamp unit.
    fn retention_span(&self) -> i64 {
        (self.retention_days as i64).saturating_mul(self.ts_unit.per_day())
    }

    /// Register a new segment after flush
    pub fn register(&self, segment: SegmentRef) {
        self.segments.write().push(segment);
    }

    /// Get segments that should be compacted (L0 with overlaps)
    pub fn get_compaction_candidates(&self) -> Vec<SegmentRef> {
        let segments = self.segments.read();
        segments
            .iter()
            .filter(|s| s.level == CompactionLevel::L0)
            .cloned()
            .collect()
    }

    /// Pick a merge group.
    ///
    /// Returns up to `max_group` L0 segments, oldest first, or `None` when there
    /// is nothing worth merging.
    ///
    /// Grouping is *not* restricted to overlapping segments. Time-ordered
    /// flushes produce L0 files that do not overlap at all, and if only
    /// overlapping files were merged, L0 would grow without bound and every read
    /// would have to open more files. Segments that do overlap the growing range
    /// are pulled in first, because leaving one out would leave the overlap — and
    /// the read amplification it causes — in place after the merge.
    pub fn select_merge_group(&self, max_group: usize) -> Option<Vec<SegmentRef>> {
        let max_group = max_group.max(2);
        let mut candidates = self.get_compaction_candidates();
        if candidates.len() < 2 {
            return None;
        }
        candidates.sort_by_key(|s| (s.min_ts, s.max_ts));

        let mut group: Vec<SegmentRef> = Vec::with_capacity(max_group);
        let mut lo = i64::MAX;
        let mut hi = i64::MIN;
        let mut rest: Vec<SegmentRef> = Vec::new();

        for seg in candidates {
            if group.len() >= max_group {
                rest.push(seg);
                continue;
            }
            // Overlaps what the group already covers, or is the first member.
            let overlaps = group.is_empty() || seg.min_ts <= hi;
            if overlaps {
                lo = lo.min(seg.min_ts);
                hi = hi.max(seg.max_ts);
                group.push(seg);
            } else {
                rest.push(seg);
            }
        }

        // Fill any leftover slots with the next segments in time order, so a run
        // of non-overlapping files still gets merged.
        for seg in rest {
            if group.len() >= max_group {
                break;
            }
            lo = lo.min(seg.min_ts);
            hi = hi.max(seg.max_ts);
            group.push(seg);
        }

        if group.len() < 2 {
            None
        } else {
            Some(group)
        }
    }

    /// Merge `candidates` into a single sorted L1 segment.
    ///
    /// Rows are decoded through the typed column path, sorted by timestamp and
    /// re-encoded block by block. Writing the output before deleting the inputs
    /// means an interrupted merge costs disk space but never data.
    pub fn compact(
        &self,
        candidates: Vec<SegmentRef>,
        spec: &CompactionSpec,
    ) -> anyhow::Result<MergeResult> {
        anyhow::ensure!(candidates.len() >= 2, "compaction needs at least two segments");
        let total_in: u64 = candidates.iter().map(|s| s.row_count).sum();
        if total_in == 0 {
            anyhow::bail!("refusing to compact empty segments");
        }

        // Decode every input into typed columns, one column at a time.
        let mut cols: Vec<ColumnData> = spec
            .column_types
            .iter()
            .map(|t| ColumnData::new(*t, 0))
            .collect();

        // The merged segment must claim every sequence its inputs claimed, or a
        // later restart would treat those rows as unflushed and replay them.
        let mut flushed_upto_seq: u64 = 0;
        let mut timestamps: Vec<i64> = Vec::new();
        for seg in &candidates {
            let reader = SegmentReader::open(&seg.path)?;
            flushed_upto_seq = flushed_upto_seq.max(reader.footer().flushed_upto_seq);
            for block_id in 0..reader.block_count() as u32 {
                let block_cols = reader.read_block(block_id)?;
                for (i, src) in block_cols.iter().enumerate() {
                    if let Some(dst) = cols.get_mut(i) {
                        crate::encoding::concat(dst, src)?;
                    }
                }
                // Timestamps drive the sort; record one entry per row.
                for i in 0..block_cols[0].len() {
                    if let Some(ts) = block_cols[0].as_i64(i) {
                        timestamps.push(ts);
                    }
                }
            }
        }

        anyhow::ensure!(
            timestamps.len() == total_in as usize,
            "decoded {} rows but the segments claim {}",
            timestamps.len(),
            total_in
        );

        // Sort every column by timestamp together.
        let order = sort_order(&timestamps);
        let sorted: Vec<ColumnData> = cols
            .iter()
            .map(|c| reorder(c, &order))
            .collect::<anyhow::Result<_>>()?;

        // Block bounds and segment bounds must come from the sorted order, not
        // the order the inputs happened to be listed in.
        let mut sorted_ts = timestamps.clone();
        sorted_ts.sort_unstable();

        let min_ts = sorted_ts[0];
        let max_ts = *sorted_ts.last().unwrap();
        let row_count = timestamps.len() as u32;

        let path = self.next_compacted_path();
        let mut writer = SegmentWriter::new(&path, spec.table_id, spec.ts_unit, spec.column_types.clone(), spec.compression)
            .with_rows_per_block(spec.rows_per_block)
            .with_flushed_upto_seq(flushed_upto_seq);

        let rows_per_block = writer.rows_per_block().max(1);
        let mut start = 0usize;
        while start < row_count as usize {
            let end = (start + rows_per_block).min(row_count as usize);
            let block: Vec<ColumnData> = sorted
                .iter()
                .map(|c| slice(c, start, end))
                .collect::<anyhow::Result<_>>()?;

            // Block bounds come from the actual timestamps in the block.
            let mut lo = i64::MAX;
            let mut hi = i64::MIN;
            for ts in &sorted_ts[start..end] {
                lo = lo.min(*ts);
                hi = hi.max(*ts);
            }
            writer.push_columns(&block, (end - start) as u32, lo, hi)?;
            start = end;
        }
        writer.finish()?;

        let byte_size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        let bytes_in: u64 = candidates.iter().map(|s| s.byte_size).sum();

        let segment = SegmentRef {
            path,
            level: CompactionLevel::L1,
            min_ts,
            max_ts,
            row_count: row_count as u64,
            byte_size,
        };

        // Inputs are only unlinked once the new segment is complete on disk.
        let input_paths: Vec<PathBuf> = candidates.iter().map(|s| s.path.clone()).collect();
        let mut segments = self.segments.write();
        segments.retain(|s| !input_paths.contains(&s.path));
        segments.push(segment.clone());
        segments.sort_by_key(|s| (s.min_ts, s.max_ts));
        drop(segments);

        for p in &input_paths {
            if let Err(e) = std::fs::remove_file(p) {
                tracing::warn!("failed to remove compacted input {}: {}", p.display(), e);
            }
        }

        self.compactions.fetch_add(1, Ordering::Relaxed);
        self.rows_merged.fetch_add(row_count as u64, Ordering::Relaxed);

        Ok(MergeResult {
            segment,
            inputs: candidates,
            bytes_in,
            bytes_out: byte_size,
        })
    }

    /// Allocate the next compacted-segment file name.
    fn next_compacted_path(&self) -> PathBuf {
        let n = self.seq.fetch_add(1, Ordering::Relaxed);
        self.data_dir
            .join(format!("seg_{:06}_{}.{}", n, CompactionLevel::L1.tag(), COMPACTED_SUFFIX))
    }

    /// Get segments that have expired (TTL)
    pub fn get_expired_segments(&self, now_ts: i64) -> Vec<SegmentRef> {
        let retention = self.retention_span();
        let segments = self.segments.read();
        segments
            .iter()
            .filter(|s| now_ts.saturating_sub(s.max_ts) > retention)
            .cloned()
            .collect()
    }

    /// Delete expired segments, returning how many were removed and how many
    /// rows they held.
    ///
    /// The row count matters: the purge metric previously recorded zero, so a
    /// table could delete its entire dataset and still report that it had not
    /// purged a single row.
    pub fn purge_expired(&self, now_ts: i64) -> anyhow::Result<(usize, u64)> {
        let expired = self.get_expired_segments(now_ts);
        let count = expired.len();
        let rows: u64 = expired.iter().map(|s| s.row_count).sum();

        for segment in &expired {
            if segment.path.exists() {
                std::fs::remove_file(&segment.path)?;
            }
        }

        let retention = self.retention_span();
        let mut segments = self.segments.write();
        segments.retain(|s| now_ts.saturating_sub(s.max_ts) <= retention);

        Ok((count, rows))
    }

    /// Delete files on disk that are no longer tracked.
    ///
    /// An interrupted merge leaves its inputs behind (they were never removed
    /// from the list) or its output orphaned (the process died before the list
    /// was updated). This removes either once the caller has reloaded the
    /// surviving set.
    pub fn recover_orphans(&self, live: &HashSet<PathBuf>) -> anyhow::Result<Vec<PathBuf>> {
        let mut removed = Vec::new();
        let entries = match std::fs::read_dir(&self.data_dir) {
            Ok(e) => e,
            Err(_) => return Ok(removed),
        };

        for entry in entries.flatten() {
            let path = entry.path();
            let is_segment = path
                .extension()
                .map(|e| e.eq_ignore_ascii_case("tsb"))
                .unwrap_or(false);
            if is_segment && !live.contains(&path) && path.exists() {
                if let Err(e) = std::fs::remove_file(&path) {
                    tracing::warn!("failed to remove orphan {}: {}", path.display(), e);
                } else {
                    removed.push(path);
                }
            }
        }
        Ok(removed)
    }

    /// Load every segment found in `data_dir`, sorted by time.
    ///
    /// A file whose header is unreadable is skipped with a warning rather than
    /// aborting startup, so one corrupt segment does not take the table down.
    pub fn load_from_disk(&self) -> Vec<SegmentRef> {
        let mut out = Vec::new();
        let entries = match std::fs::read_dir(&self.data_dir) {
            Ok(e) => e,
            Err(_) => return out,
        };

        let mut paths: Vec<PathBuf> = entries
            .flatten()
            .map(|e| e.path())
            .filter(|p| {
                p.extension()
                    .map(|e| e.eq_ignore_ascii_case("tsb"))
                    .unwrap_or(false)
            })
            .collect();
        paths.sort();

        for path in paths {
            let compacted = path
                .file_name()
                .map(|n| n.to_string_lossy().ends_with(COMPACTED_SUFFIX))
                .unwrap_or(false);
            match SegmentReader::open(&path) {
                Ok(reader) => {
                    let size = reader.file_size();
                    out.push(SegmentRef {
                        path,
                        level: if compacted { CompactionLevel::L1 } else { CompactionLevel::L0 },
                        min_ts: reader.min_ts(),
                        max_ts: reader.max_ts(),
                        row_count: reader.row_count(),
                        byte_size: size,
                    });
                }
                Err(e) => tracing::error!("skipping unreadable segment {}: {}", path.display(), e),
            }
        }

        out.sort_by_key(|s| (s.min_ts, s.max_ts));
        out
    }

    /// Replace the tracked segment list.
    pub fn set_segments(&self, segments: Vec<SegmentRef>) {
        *self.segments.write() = segments;
    }

    pub fn segments(&self) -> Vec<SegmentRef> {
        self.segments.read().clone()
    }

    /// Get total number of segments
    pub fn segment_count(&self) -> usize {
        self.segments.read().len()
    }

    /// Get total row count across all segments
    pub fn total_rows(&self) -> u64 {
        self.segments.read().iter().map(|s| s.row_count).sum()
    }

    /// Bytes currently occupied by tracked segments.
    pub fn total_bytes(&self) -> u64 {
        self.segments.read().iter().map(|s| s.byte_size).sum()
    }

    /// Number of completed merges.
    pub fn compaction_count(&self) -> u64 {
        self.compactions.load(Ordering::Relaxed)
    }

    /// Rows rewritten by merges so far.
    pub fn rows_merged(&self) -> u64 {
        self.rows_merged.load(Ordering::Relaxed)
    }
}

/// Indices that sort `timestamps`, ties broken by original position.
fn sort_order(timestamps: &[i64]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..timestamps.len()).collect();
    order.sort_by_key(|&i| (timestamps[i], i));
    order
}

/// Apply a permutation to one column.
fn reorder(col: &ColumnData, order: &[usize]) -> anyhow::Result<ColumnData> {
    let mut out = ColumnData::new(col.data_type(), 0);
    for &i in order {
        out.extend_from(&slice_one(col, i, i + 1)?)?;
    }
    Ok(out)
}

/// Rows `[start, end)` of one column.
fn slice(col: &ColumnData, start: usize, end: usize) -> anyhow::Result<ColumnData> {
    slice_one(col, start, end)
}

/// Rows `[start, end)` of one column, via the raw row representation.
fn slice_one(col: &ColumnData, start: usize, end: usize) -> anyhow::Result<ColumnData> {
    let mut out = ColumnData::new(col.data_type(), 0);
    for i in start..end.min(col.len()) {
        match &mut out {
            ColumnData::Timestamp(v) => v.push(col.as_i64(i)),
            ColumnData::Int64(v) => v.push(col.as_i64(i)),
            ColumnData::Int32(v) => v.push(col.as_i64(i).map(|x| x as i32)),
            ColumnData::Float64(v) => v.push(col.as_f64(i)),
            ColumnData::Float32(v) => v.push(col.as_f64(i).map(|x| x as f32)),
            ColumnData::Bool(v) => v.push(col.as_i64(i).map(|x| x != 0)),
            ColumnData::String(v) => v.push(col.as_str(i).map(|s| s.to_string())),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::ColumnCodec;

    fn spec() -> CompactionSpec {
        CompactionSpec::new(
            1,
            TsUnit::Millis,
            vec![DataType::Timestamp, DataType::Float64, DataType::String],
        )
    }

    fn write_segment(
        dir: &Path,
        name: &str,
        base_ts: i64,
        rows: usize,
    ) -> SegmentRef {
        let path = dir.join(name);
        let ts: Vec<Option<i64>> = (0..rows).map(|i| Some(base_ts + i as i64 * 10)).collect();
        let vals: Vec<Option<f64>> = (0..rows).map(|i| Some(20.0 + i as f64 * 0.5)).collect();
        let tags: Vec<Option<String>> = (0..rows)
            .map(|i| Some(format!("device-{}", i % 3)))
            .collect();

        let mut w = SegmentWriter::new(
            &path,
            1,
            TsUnit::Millis,
            spec().column_types,
            CompressionLevel::Lz4,
        );
        w.push_columns(
            &[
                ColumnData::Timestamp(ts),
                ColumnData::Float64(vals),
                ColumnData::String(tags),
            ],
            rows as u32,
            base_ts,
            base_ts + (rows as i64 - 1) * 10,
        )
        .unwrap();
        w.finish().unwrap();

        let byte_size = std::fs::metadata(&path).unwrap().len();
        SegmentRef {
            path,
            level: CompactionLevel::L0,
            min_ts: base_ts,
            max_ts: base_ts + (rows as i64 - 1) * 10,
            row_count: rows as u64,
            byte_size,
        }
    }

    #[test]
    fn test_compaction_manager() {
        let cm = CompactionManager::new("/tmp/test", 365);

        for i in 0..5 {
            cm.register(SegmentRef {
                path: PathBuf::from(format!("/tmp/test/seg_{}.tsb", i)),
                level: CompactionLevel::L0,
                min_ts: i * 1000,
                max_ts: (i + 1) * 1000,
                row_count: 1000,
                byte_size: 1024 * 1024,
            });
        }

        assert_eq!(cm.segment_count(), 5);
        assert_eq!(cm.total_rows(), 5000);

        let candidates = cm.get_compaction_candidates();
        assert_eq!(candidates.len(), 5);
    }

    #[test]
    fn test_select_merge_group_respects_cap() {
        let dir = tempfile::tempdir().unwrap();
        let cm = CompactionManager::new(dir.path(), 365);

        for i in 0..10 {
            cm.register(SegmentRef {
                path: dir.path().join(format!("seg_{}.tsb", i)),
                level: CompactionLevel::L0,
                min_ts: i * 100,           // all overlap the chain
                max_ts: i * 100 + 500,
                row_count: 10,
                byte_size: 10,
            });
        }

        let group = cm.select_merge_group(4).unwrap();
        assert_eq!(group.len(), 4, "group must respect the cap");

        // A single segment is not worth a merge.
        let single = CompactionManager::new(dir.path(), 365);
        single.register(SegmentRef {
            path: dir.path().join("only.tsb"),
            level: CompactionLevel::L0,
            min_ts: 0,
            max_ts: 10,
            row_count: 1,
            byte_size: 1,
        });
        assert!(single.select_merge_group(8).is_none());
    }

    #[test]
    fn test_compact_preserves_every_row() {
        let dir = tempfile::tempdir().unwrap();
        let cm = CompactionManager::new(dir.path(), 365);

        let a = write_segment(dir.path(), "seg_000000.tsb", 1000, 100);
        let b = write_segment(dir.path(), "seg_000001.tsb", 2000, 100);
        let total = a.row_count + b.row_count;
        cm.register(a.clone());
        cm.register(b.clone());

        let result = cm.compact(vec![a.clone(), b.clone()], &spec()).unwrap();

        assert_eq!(result.segment.row_count, total, "rows lost during merge");
        assert_eq!(result.inputs.len(), 2);
        assert_eq!(cm.compaction_count(), 1);
        assert_eq!(cm.rows_merged(), total);

        // Inputs are gone, output exists.
        assert!(!a.path.exists(), "input A should be deleted");
        assert!(!b.path.exists(), "input B should be deleted");
        assert!(result.segment.path.exists());

        // And the output is readable with the right values.
        let reader = SegmentReader::open(&result.segment.path).unwrap();
        assert_eq!(reader.row_count(), total);
        let cols = reader.read_all_columns().unwrap();
        for i in 0..total as usize {
            // 1000 + k*10 for k in 0..200
            assert_eq!(cols[0].as_i64(i), Some(1000 + i as i64 * 10), "row {}", i);
            assert!(cols[2].as_str(i).is_some(), "string lost at row {}", i);
        }
    }

    #[test]
    fn test_compact_orders_rows_by_timestamp() {
        let dir = tempfile::tempdir().unwrap();
        let cm = CompactionManager::new(dir.path(), 365);

        // Interleave two ranges so the merge has real work to do.
        let a = write_segment(dir.path(), "seg_000000.tsb", 5000, 50);
        let b = write_segment(dir.path(), "seg_000001.tsb", 1000, 50);
        cm.register(a);
        cm.register(b);

        let result = cm.compact(
            cm.get_compaction_candidates(),
            &spec(),
        )
        .unwrap();

        let reader = SegmentReader::open(&result.segment.path).unwrap();
        let cols = reader.read_all_columns().unwrap();
        let ts: Vec<i64> = (0..cols[0].len()).map(|i| cols[0].as_i64(i).unwrap()).collect();
        assert!(ts.windows(2).all(|w| w[0] <= w[1]), "output is not sorted");
        assert_eq!(ts[0], 1000);
        assert_eq!(*ts.last().unwrap(), 5000 + 49 * 10);
    }

    #[test]
    fn test_compact_rejects_single_segment() {
        let dir = tempfile::tempdir().unwrap();
        let cm = CompactionManager::new(dir.path(), 365);
        let a = write_segment(dir.path(), "seg_000000.tsb", 1000, 10);
        assert!(cm.compact(vec![a], &spec()).is_err());
    }

    #[test]
    fn test_load_from_disk_finds_and_classifies_segments() {
        let dir = tempfile::tempdir().unwrap();
        let cm = CompactionManager::new(dir.path(), 365);
        write_segment(dir.path(), "seg_000000.tsb", 1000, 20);

        let loaded = cm.load_from_disk();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].level, CompactionLevel::L0);
        assert_eq!(loaded[0].row_count, 20);
        assert_eq!(loaded[0].min_ts, 1000);
    }

    #[test]
    fn test_load_from_disk_skips_corrupt_segment() {
        let dir = tempfile::tempdir().unwrap();
        let cm = CompactionManager::new(dir.path(), 365);
        write_segment(dir.path(), "seg_000000.tsb", 1000, 20);
        std::fs::write(dir.path().join("seg_000001.tsb"), b"not a segment").unwrap();

        let loaded = cm.load_from_disk();
        assert_eq!(loaded.len(), 1, "corrupt file should be skipped, not fatal");
    }

    #[test]
    fn test_recover_orphans_removes_untracked_files() {
        let dir = tempfile::tempdir().unwrap();
        let cm = CompactionManager::new(dir.path(), 365);
        let a = write_segment(dir.path(), "seg_000000.tsb", 1000, 20);
        write_segment(dir.path(), "seg_000001.tsb", 2000, 20);

        let live: HashSet<PathBuf> = [a.path.clone()].into_iter().collect();
        let removed = cm.recover_orphans(&live).unwrap();

        assert_eq!(removed.len(), 1);
        assert!(a.path.exists());
        assert!(!dir.path().join("seg_000001.tsb").exists());
    }

    #[test]
    fn test_purge_expired_removes_only_old_segments() {
        let dir = tempfile::tempdir().unwrap();
        // One day of retention: the 1970 segment is expired, the current one
        // (about 100 ms old) is not.
        let cm = CompactionManager::new(dir.path(), 1);

        let old = write_segment(dir.path(), "seg_000000.tsb", 1000, 10);
        let new = write_segment(dir.path(), "seg_000001.tsb", 9_000_000_000_000, 10);
        cm.register(old.clone());
        cm.register(new.clone());

        // One hour after the newer segment's last row: inside the retention
        // window, while the 1970 data is far outside it.
        let now = 9_000_003_600_000;
        let removed = cm.purge_expired(now).unwrap();

        assert_eq!(removed.0, 1, "one segment should have been purged");
        assert_eq!(removed.1, old.row_count, "purged row count should be reported");
        assert!(!old.path.exists());
        assert!(new.path.exists());
        assert_eq!(cm.segment_count(), 1);
    }

    /// Retention is stored in days, so a table in seconds, microseconds or
    /// nanoseconds has to scale the window to its own unit. Comparing raw days
    /// against a seconds-resolution timestamp purges almost everything, and
    /// against a nanosecond timestamp purges almost nothing.
    #[test]
    fn test_retention_respects_the_timestamp_unit() {
        // One day of retention, and a segment 100 ticks old. In every unit 100
        // ticks is a tiny fraction of a day, so all of them must expire; a
        // millisecond assumption would leave the seconds case unexpired.
        for (unit, now, old_ts) in [
            (TsUnit::Seconds, 9_000_003_600i64, 100i64),
            (TsUnit::Millis, 9_000_003_600_000i64, 100i64),
            (TsUnit::Micros, 9_000_003_600_000_000i64, 100i64),
            (TsUnit::Nanos, 9_000_003_600_000_000_000i64, 100i64),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let cm = CompactionManager::with_unit(dir.path(), 1, unit);
            let path = dir.path().join("seg_000000.tsb");
            std::fs::write(&path, b"x").unwrap();
            cm.register(SegmentRef {
                path,
                level: CompactionLevel::L0,
                min_ts: old_ts,
                max_ts: old_ts,
                row_count: 7,
                byte_size: 1,
            });

            let (removed, rows) = cm.purge_expired(now).unwrap();
            assert_eq!(removed, 1, "{:?}: expired segment was not purged", unit);
            assert_eq!(rows, 7, "{:?}: purged rows were not reported", unit);
            assert_eq!(cm.segment_count(), 0, "{:?}: segment list still holds it", unit);
        }
    }

    /// A segment well inside the retention window must survive in every unit.
    #[test]
    fn test_recent_segments_survive_in_every_unit() {
        for unit in [
            TsUnit::Seconds,
            TsUnit::Millis,
            TsUnit::Micros,
            TsUnit::Nanos,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let cm = CompactionManager::with_unit(dir.path(), 365, unit);
            let path = dir.path().join("seg_000000.tsb");
            std::fs::write(&path, b"x").unwrap();
            // Written a minute ago, in this unit.
            let recent = unit.now() - unit.per_day() / 1_440;
            cm.register(SegmentRef {
                path,
                level: CompactionLevel::L0,
                min_ts: recent,
                max_ts: recent,
                row_count: 7,
                byte_size: 1,
            });

            let purged = cm.purge_expired(unit.now()).unwrap();
            assert_eq!(purged.0, 0, "{:?}: purged data that is an hour old", unit);
            assert_eq!(cm.segment_count(), 1, "{:?}: lost a live segment", unit);
        }
    }

    #[test]
    fn test_compaction_shrinks_merged_segment() {
        let dir = tempfile::tempdir().unwrap();
        let cm = CompactionManager::new(dir.path(), 365);
        let a = write_segment(dir.path(), "seg_000000.tsb", 1000, 500);
        let b = write_segment(dir.path(), "seg_000001.tsb", 6000, 500);

        let result = cm.compact(vec![a, b], &spec()).unwrap();
        // 1000 rows of 3 columns should not grow versus the inputs.
        assert!(
            result.bytes_out < result.bytes_in,
            "merge grew the data: {} -> {}",
            result.bytes_in,
            result.bytes_out
        );
        assert!(result.compression_ratio().unwrap() > 1.0);
    }

    #[test]
    fn test_roundtrip_through_merge_for_every_type() {
        let dir = tempfile::tempdir().unwrap();
        let cm = CompactionManager::new(dir.path(), 365);

        let types = vec![
            DataType::Timestamp,
            DataType::Int32,
            DataType::Bool,
            DataType::String,
            DataType::Float32,
        ];
        let sp = CompactionSpec::new(7, TsUnit::Millis, types.clone());

        let build = |base: i64| -> Vec<ColumnData> {
            vec![
                ColumnData::Timestamp((0..40).map(|i| Some(base + i as i64)).collect()),
                ColumnData::Int32((0..40).map(|i| Some(i as i32 * 3 - 40)).collect()),
                ColumnData::Bool((0..40).map(|i| Some(i % 3 == 0)).collect()),
                ColumnData::String((0..40).map(|i| Some(format!("s{}", i % 5))).collect()),
                ColumnData::Float32((0..40).map(|i| Some(i as f32 * 1.5)).collect()),
            ]
        };

        let mut inputs = Vec::new();
        for (n, base) in [1000i64, 5000].iter().enumerate() {
            let base = *base;
            let path = dir.path().join(format!("seg_{:06}.tsb", n));
            let mut w = SegmentWriter::new(
                &path,
                7,
                TsUnit::Millis,
                types.clone(),
                CompressionLevel::Off,
            );
            w.push_columns(&build(base), 40, base, base + 39).unwrap();
            w.finish().unwrap();
            inputs.push(SegmentRef {
                byte_size: std::fs::metadata(&path).unwrap().len(),
                path,
                level: CompactionLevel::L0,
                min_ts: base,
                max_ts: base + 39,
                row_count: 40,
            });
        }

        let result = cm.compact(inputs, &sp).unwrap();
        let reader = SegmentReader::open(&result.segment.path).unwrap();
        let cols = reader.read_all_columns().unwrap();

        assert_eq!(cols[0].len(), 80, "merge dropped rows");

        // First segment holds 1000..1039, second 5000..5039, merged sorted.
        let first: Vec<i64> = (0..40).map(|i| 1000 + i).collect();
        let second: Vec<i64> = (0..40).map(|i| 5000 + i).collect();
        let want: Vec<i64> = first.into_iter().chain(second).collect();
        let got: Vec<i64> = (0..80).map(|i| cols[0].as_i64(i).unwrap()).collect();
        assert_eq!(got, want, "timestamps wrong after merge");

        // Every other column survived with all its values.
        for i in 0..cols[1].len() {
            assert!(cols[1].as_i64(i).is_some(), "int32 lost at {}", i);
            assert!(cols[2].as_i64(i).is_some(), "bool lost at {}", i);
            assert!(cols[3].as_str(i).is_some(), "string lost at {}", i);
            assert!(cols[4].as_f64(i).is_some(), "float lost at {}", i);
        }

        // Re-encoding a merged column must still round-trip.
        let _ = ColumnCodec::encode(&cols[3]).unwrap();
    }
}
