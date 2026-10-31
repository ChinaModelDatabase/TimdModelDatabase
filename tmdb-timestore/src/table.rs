use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::Context;
use parking_lot::RwLock;

use crate::compaction::{CompactionLevel, CompactionManager, CompactionSpec, SegmentRef};
use crate::encoding::{ColumnData, CompressionLevel, DataType};
use crate::cardinality::{CardinalityError, CardinalityGuard, CardinalityLimits};
use crate::memtable::MemTable;
use crate::metrics::{MetricsSnapshot, TableMetrics};
use crate::segment::file::{SegmentReader, SegmentWriter};
use crate::wal::Wal;
use crate::{ColumnDef, TableMeta};

/// Column types in schema order, as the segment header records them.
fn schema_types(columns: &[ColumnDef]) -> Vec<DataType> {
    columns.iter().map(|c| c.data_type).collect()
}

/// L0 segments that trigger a merge.
pub const DEFAULT_COMPACTION_THRESHOLD: usize = 4;

/// Total segment bytes kept open for reuse before the oldest are closed.
const READER_CACHE_BYTES: usize = 256 * 1024 * 1024;

/// A segment reader plus the file identity it was built from.
struct CachedReader {
    reader: SegmentReader,
    len: u64,
    modified: Option<std::time::SystemTime>,
}

/// Open segment readers, kept across queries.
///
/// Opening a segment reads the whole file and deserialises a footer describing
/// every block in it. A point query wants one block, so repeating that per query
/// cost more than the decode it was supposed to be enabling.
///
/// A written segment is never modified, so a reader stays valid for as long as
/// the file behind it does. The length and mtime are re-checked on every borrow,
/// which is what makes a replacement or a deleted file a cache miss instead of a
/// stale read.
#[derive(Default)]
struct ReaderCache {
    /// Oldest first, so the front is what gets evicted.
    entries: Vec<(PathBuf, Arc<CachedReader>)>,
    bytes: usize,
}

impl ReaderCache {
    /// A reader for `path`, opening the file only if there is not a usable one.
    fn borrow(&mut self, path: &Path) -> anyhow::Result<Arc<CachedReader>> {
        let meta = std::fs::metadata(path).ok();
        let len = meta.as_ref().map(|m| m.len()).unwrap_or(0);
        let modified = meta.as_ref().and_then(|m| m.modified().ok());

        if let Some(pos) = self.entries.iter().position(|(p, _)| p == path) {
            let (key, entry) = self.entries.remove(pos);
            if entry.len == len && entry.modified == modified {
                // Reuse, and move to the back so the working set survives.
                self.entries.push((key, Arc::clone(&entry)));
                return Ok(entry);
            }
            // The file changed underneath us, so the reader describes something
            // that no longer exists.
            self.bytes = self.bytes.saturating_sub(entry.len as usize);
        }

        let reader = SegmentReader::open(path)
            .map_err(|e| anyhow::anyhow!("open segment {}: {}", path.display(), e))?;
        let entry = Arc::new(CachedReader {
            reader,
            len,
            modified,
        });
        self.entries.push((path.to_path_buf(), Arc::clone(&entry)));
        self.bytes += len as usize;

        while self.bytes > READER_CACHE_BYTES && self.entries.len() > 1 {
            let (_, evicted) = self.entries.remove(0);
            self.bytes = self.bytes.saturating_sub(evicted.len as usize);
        }
        Ok(entry)
    }

    /// Drop everything, so a caller that replaced segments does not hold their
    /// data any longer than it must.
    fn clear(&mut self) {
        self.entries.clear();
        self.bytes = 0;
    }
}

/// Numeric prefix of a `seg_NNNNNN*.tsb` name, if present.
///
/// Used to continue the file-name counter above whatever already exists, so a
/// restart cannot overwrite a live segment.
fn segment_index(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    let rest = name.strip_prefix("seg_")?;
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

/// Rows `[start, end)` of one column, preserving NULLs.
fn slice_column(col: &ColumnData, start: usize, end: usize) -> anyhow::Result<ColumnData> {
    // Copy the typed slice directly. Going through `get_raw` first would
    // allocate a fresh `Vec<u8>` for every single value, parse it straight back
    // and throw it away: six million throwaway allocations to slice two million
    // rows, which measured as half a second of pure waste.
    let range = start..end;
    Ok(match col {
        ColumnData::Timestamp(v) => ColumnData::Timestamp(v[range].to_vec()),
        ColumnData::Int64(v) => ColumnData::Int64(v[range].to_vec()),
        ColumnData::Int32(v) => ColumnData::Int32(v[range].to_vec()),
        ColumnData::Float64(v) => ColumnData::Float64(v[range].to_vec()),
        ColumnData::Float32(v) => ColumnData::Float32(v[range].to_vec()),
        ColumnData::Bool(v) => ColumnData::Bool(v[range].to_vec()),
        // A `String` owns its bytes, so these still have to be cloned one by one.
        ColumnData::String(v) => ColumnData::String(v[range].to_vec()),
    })
}

pub struct Table {
    pub meta: TableMeta,
    memtable: RwLock<MemTable>,
    segments: RwLock<Vec<SegmentRef>>,
    compaction: CompactionManager,
    data_dir: std::path::PathBuf,
    flush_needed: Arc<AtomicBool>,
    tag_index: TagIndex,
    /// Monotonic segment file-name counter.
    segment_seq: std::sync::atomic::AtomicU64,
    /// Highest sequence any on-disk segment has already flushed.
    max_segment_flushed: std::sync::atomic::AtomicU64,
    wal: Wal,
    /// Monotonic WAL sequence number.
    wal_seq: std::sync::atomic::AtomicU64,
    /// Counters for observability.
    metrics: TableMetrics,
    /// Enforces tag and series limits.
    cardinality: CardinalityGuard,
    /// L0 segments that trigger a merge.
    compaction_threshold: usize,
    /// Rows per block in segments this table writes.
    rows_per_block: usize,
    /// Open segment readers, shared with concurrent scans.
    readers: Arc<RwLock<ReaderCache>>,
    /// Makes the next WAL append fail, so tests can drive the log-failure path.
    /// Breaking the log by locking the file is platform-specific and racy; a log
    /// failure is a real condition that must be handled, and this is the only
    /// reliable way to produce one on demand.
    #[cfg(test)]
    fail_next_wal_append: std::sync::atomic::AtomicBool,
}

/// What one [`Table::maintenance_pass`] did.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MaintenanceOutcome {
    pub flushed: bool,
    pub compacted: bool,
    pub segments_purged: u64,
    /// Rows deleted by the TTL purge, not just the files.
    pub rows_purged: u64,
    pub segments_before: u64,
    pub segments_after: u64,
    pub flush_error: Option<String>,
    pub compaction_error: Option<String>,
}

// --- TagIndex (inline, simple) ---
pub struct TagIndex {
    inner: parking_lot::RwLock<std::collections::HashMap<String, std::collections::HashMap<String, Vec<u64>>>>,
}

impl TagIndex {
    pub fn new() -> Self {
        Self {
            inner: parking_lot::RwLock::new(std::collections::HashMap::new()),
        }
    }

    pub fn get_or_create_series(&self, tags: &[(String, String)], series_id: u64) {
        let mut inner = self.inner.write();
        for (name, value) in tags {
            let dict = inner.entry(name.clone()).or_insert_with(std::collections::HashMap::new);
            dict.entry(value.clone()).or_insert_with(Vec::new).push(series_id);
        }
    }

    pub fn find_series(&self, tag_filters: &[(String, String)]) -> Option<Vec<u64>> {
        if tag_filters.is_empty() {
            return None;
        }
        let inner = self.inner.read();
        let mut result: Option<Vec<u64>> = None;
        for (name, value) in tag_filters {
            if let Some(dict) = inner.get(name) {
                if let Some(ids) = dict.get(value) {
                    match &mut result {
                        None => result = Some(ids.clone()),
                        Some(existing) => {
                            existing.retain(|id| ids.contains(id));
                        }
                    }
                } else {
                    return Some(vec![]);
                }
            } else {
                return Some(vec![]);
            }
        }
        result
    }
}

impl Default for TagIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl Table {
    pub fn new(meta: TableMeta, data_dir: impl AsRef<Path>) -> Self {
        let data_dir = data_dir.as_ref().to_path_buf();
        // A table that cannot read its directory still has to exist, or callers
        // get a panic instead of an error. Start empty and let the caller
        // discover the missing data through `row_count`.
        Self::open(meta.clone(), &data_dir)
            .unwrap_or_else(|e| {
                tracing::error!("could not open table in {}: {}", data_dir.display(), e);
                Self::empty(meta, &data_dir)
            })
    }

    /// Open a table, loading its segments and replaying its log.
    pub fn open(meta: TableMeta, data_dir: impl AsRef<Path>) -> anyhow::Result<Self> {
        Self::open_with_limits(meta, data_dir, CardinalityLimits::default())
    }

    /// Open a table with explicit cardinality limits.
    pub fn open_with_limits(
        meta: TableMeta,
        data_dir: impl AsRef<Path>,
        limits: CardinalityLimits,
    ) -> anyhow::Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir)
            .with_context(|| format!("create table dir {}", data_dir.display()))?;

        let compaction = CompactionManager::with_unit(&data_dir, meta.retention_days, meta.ts_unit);

        // Pick up whatever previous runs left behind: segments on disk first,
        // then any rows the log still holds because a flush never finished.
        let loaded = compaction.load_from_disk();
        compaction.set_segments(loaded.clone());

        // Determine the highest sequence that any existing segment already
        // contains. If a flush succeeded in writing a segment but failed to
        // checkpoint the log (or the checkpoint was lost), the WAL checkpoint is
        // stale and would cause us to replay rows that are already in the
        // segment. The segment footer carries the durable marker, so we must
        // treat it as the authoritative floor for replay.
        let mut max_segment_flushed: u64 = 0;
        for s in &loaded {
            if let Ok(r) = crate::segment::file::SegmentReader::open(&s.path) {
                max_segment_flushed = max_segment_flushed.max(r.footer().flushed_upto_seq);
            }
        }

        // The name counter must not reuse a name already on disk, or a restart
        // would overwrite a live segment.
        let next_seq = loaded
            .iter()
            .filter_map(|s| segment_index(&s.path))
            .max()
            .map(|n| n + 1)
            .unwrap_or(0);

        let table = Self {
            compaction_threshold: DEFAULT_COMPACTION_THRESHOLD,
            rows_per_block: crate::segment::DEFAULT_ROWS_PER_BLOCK,
            readers: Arc::new(RwLock::new(ReaderCache::default())),
            #[cfg(test)]
            fail_next_wal_append: std::sync::atomic::AtomicBool::new(false),
            meta,
            memtable: RwLock::new(MemTable::new(0, 32768, 256 * 1024 * 1024)), // 32K rows per batch
            segments: RwLock::new(loaded),
            compaction,
            wal: Wal::new(data_dir.join("tmdb.wal")),
            wal_seq: std::sync::atomic::AtomicU64::new(0),
            metrics: TableMetrics::new(),
            cardinality: CardinalityGuard::new(limits),
            data_dir,
            flush_needed: Arc::new(AtomicBool::new(false)),
            tag_index: TagIndex::new(),
            segment_seq: std::sync::atomic::AtomicU64::new(next_seq),
            max_segment_flushed: std::sync::atomic::AtomicU64::new(max_segment_flushed),
        };

        table.recover_wal();
        Ok(table)
    }

    /// A table with no on-disk state, used when opening fails.
    fn empty(meta: TableMeta, data_dir: &Path) -> Self {
        let compaction = CompactionManager::with_unit(data_dir, meta.retention_days, meta.ts_unit);
        Self {
            compaction_threshold: DEFAULT_COMPACTION_THRESHOLD,
            rows_per_block: crate::segment::DEFAULT_ROWS_PER_BLOCK,
            readers: Arc::new(RwLock::new(ReaderCache::default())),
            #[cfg(test)]
            fail_next_wal_append: std::sync::atomic::AtomicBool::new(false),
            meta,
            memtable: RwLock::new(MemTable::new(0, 32768, 256 * 1024 * 1024)),
            segments: RwLock::new(Vec::new()),
            compaction,
            wal: Wal::new(data_dir.join("tmdb.wal")),
            wal_seq: std::sync::atomic::AtomicU64::new(0),
            metrics: TableMetrics::new(),
            cardinality: CardinalityGuard::new(CardinalityLimits::default()),
            data_dir: data_dir.to_path_buf(),
            flush_needed: Arc::new(AtomicBool::new(false)),
            max_segment_flushed: std::sync::atomic::AtomicU64::new(0),
            tag_index: TagIndex::new(),
            segment_seq: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Replay rows the log still holds into the memtable.
    ///
    /// Called during [`Table::new`]. A damaged tail is expected after a crash and
    /// is truncated. Batches a flush already checkpointed are skipped, so a
    /// restart cannot duplicate rows that are safely in a segment.
    fn recover_wal(&self) {
        let recovery = match self.wal.recover() {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("WAL unreadable for table {}: {}", self.meta.table_id, e);
                return;
            }
        };

        if !matches!(recovery.stopped_because, crate::wal::WalStopReason::Eof) {
            tracing::warn!(
                "WAL recovery stopped early ({}), discarding the torn tail",
                recovery.stopped_because
            );
            if let Err(e) = self.wal.truncate_to_last_good() {
                tracing::error!("could not repair WAL: {}", e);
            }
        }

        // Continue the sequence above anything this table already flushed.
        let checkpoint = recovery.checkpoints.get(&self.meta.table_id).copied().unwrap_or(0);
        let segment_floor = self
            .max_segment_flushed
            .load(std::sync::atomic::Ordering::Relaxed);
        let base = checkpoint.max(segment_floor);
        self.wal_seq
            .fetch_max(base + 1, Ordering::Relaxed);

        let records = recovery.pending_for(self.meta.table_id);
        if records.is_empty() {
            return;
        }

        let mut replayed = 0usize;
        for record in &records {
            if record.seq <= base {
                // This record's rows are already in a segment on disk, so replaying
                // them would create duplicates. The WAL checkpoint alone was stale.
                continue;
            }
            for (ts, values) in &record.rows {
                self.memtable.write().insert(*ts, values.clone());
                replayed += 1;
            }
            self.wal_seq.fetch_max(record.seq + 1, Ordering::Relaxed);
        }

        self.flush_needed.store(true, Ordering::Relaxed);
        self.metrics.record_wal_recovery(replayed as u64);
        tracing::warn!(
            "recovered {} unflushed rows for table {}",
            replayed,
            self.meta.table_id
        );
    }

    /// Rows the log still holds for this table that are not yet in a segment.
    pub fn wal_pending_rows(&self) -> usize {
        self.wal
            .recover()
            .map(|r| r.pending_rows(self.meta.table_id))
            .unwrap_or(0)
    }

    /// Size of the log in bytes.
    pub fn wal_size(&self) -> u64 {
        self.wal.size()
    }

    /// Force the log to disk without writing a segment.
    ///
    /// Exposed for diagnostics that need to time the log fsync on its own; a
    /// flush pays this cost before it writes anything.
    pub fn flush_wal_only(&self) {
        let _ = self.wal.sync();
    }

    pub fn insert_with_tags(&self, ts: i64, values: Vec<Option<Vec<u8>>>, tags: &[(String, String)]) {
        // The id comes from the guard rather than a second counter, so the tag
        // index and the cardinality guard cannot end up numbering the same
        // series differently.
        match self.admit_tags(tags) {
            Ok(series_id) => {
                if !tags.is_empty() {
                    self.tag_index.get_or_create_series(tags, series_id);
                }
            }
            Err(e) => {
                self.metrics.record_rejection(1);
                tracing::warn!("write refused: {}", e);
                return;
            }
        }

        let accepted = self.accept_rows(std::slice::from_ref(&(ts, values)));
        self.metrics.record_write(accepted as u64);
    }

    pub fn insert_batch_with_tags(&self, batch: &[(i64, Vec<Option<Vec<u8>>>, Vec<(String, String)>)]) {
        // Admit the whole batch in one call, or none of it.
        //
        // Admitting row by row looked equivalent but was not: the loop committed
        // each row's tags as it went, so a batch refused on its third row had
        // already consumed headroom for the first two. The rows themselves were
        // correctly not written, so the data and the index disagreed -- and the
        // headroom was gone for good.
        let tag_sets: Vec<Vec<(String, String)>> =
            batch.iter().map(|(_, _, tags)| tags.clone()).collect();

        let series_ids = match self.cardinality.admit_batch(&tag_sets) {
            Ok(ids) => ids,
            Err(e) => {
                self.metrics.record_rejection(batch.len() as u64);
                tracing::warn!("batch of {} refused: {}", batch.len(), e);
                return;
            }
        };

        for ((_, _, tags), series_id) in batch.iter().zip(series_ids) {
            if !tags.is_empty() {
                self.tag_index.get_or_create_series(tags, series_id);
            }
        }

        let rows: Vec<(i64, Vec<Option<Vec<u8>>>)> =
            batch.iter().map(|(ts, values, _)| (*ts, values.clone())).collect();

        let accepted = self.accept_rows(&rows);
        self.metrics.record_write(accepted as u64);
        if self.memtable.read().should_flush() {
            self.flush_needed.store(true, Ordering::Relaxed);
        }
    }

    /// Admit one row's tags, returning why it was refused.
    pub fn admit_tags(&self, tags: &[(String, String)]) -> Result<u64, CardinalityError> {
        self.cardinality.admit(tags)
    }

    pub fn insert(&self, ts: i64, values: Vec<Option<Vec<u8>>>) {
        let accepted = self.accept_rows(std::slice::from_ref(&(ts, values)));
        self.metrics.record_write(accepted as u64);
    }

    pub fn insert_batch(&self, batch: &[(i64, Vec<Option<Vec<u8>>>)]) {
        let accepted = self.accept_rows(batch);
        self.metrics.record_write(accepted as u64);
    }

    /// Bulk insert - optimized for high throughput
    pub fn bulk_insert(&self, rows: &[(i64, Vec<Option<Vec<u8>>>)]) -> u64 {
        let batch_size = 10000;
        let mut total = 0u64;
        for chunk in rows.chunks(batch_size) {
            let accepted = self.accept_rows(chunk);
            self.metrics.record_write(accepted as u64);
            total += accepted as u64;
        }
        total
    }

    /// Consume a pending injected log failure, if a test armed one.
    #[cfg(test)]
    fn injected_wal_failure(&self) -> Option<anyhow::Error> {
        if self.fail_next_wal_append.swap(false, Ordering::Relaxed) {
            Some(anyhow::anyhow!("injected WAL failure"))
        } else {
            None
        }
    }

    /// No fault injection outside tests. Kept as a separate `cfg` arm rather than
    /// a `cfg!(test)` branch, which would still type-check the field in release
    /// builds where it does not exist.
    #[cfg(not(test))]
    fn injected_wal_failure(&self) -> Option<anyhow::Error> {
        None
    }

    /// Log a batch and buffer it, returning how many rows were accepted.
    ///
    /// The sequence number is claimed and the rows are buffered under a single
    /// memtable write lock. That lock is the one `flush` drains, so any sequence
    /// a flush observes is guaranteed to be in the rows it drained. Claiming the
    /// sequence outside the lock (as this used to) let a concurrent batch be
    /// checkpointed as durable while still sitting in the memtable, and it was
    /// then lost on restart.
    ///
    /// A logging failure rejects the whole batch rather than buffering rows that
    /// no crash could recover.
    fn accept_rows(&self, rows: &[(i64, Vec<Option<Vec<u8>>>)]) -> usize {
        if rows.is_empty() {
            return 0;
        }
        let memtable = self.memtable.write();
        let seq = self.wal_seq.fetch_add(1, Ordering::Relaxed);
        let before = self.wal.size();
        let logged = match self.injected_wal_failure() {
            Some(e) => Err(e),
            None => self.wal.append(self.meta.table_id, seq, rows),
        };
        if let Err(e) = logged {
            tracing::error!("WAL append failed for table {}: {}", self.meta.table_id, e);
            // Put the sequence back so the next attempt is not skipped.
            self.wal_seq.fetch_sub(1, Ordering::Relaxed);
            return 0;
        }
        self.metrics
            .record_wal_append(self.wal.size().saturating_sub(before));
        let should_flush = memtable.insert_batch(rows);
        drop(memtable);
        if should_flush {
            self.flush_needed.store(true, Ordering::Relaxed);
        }
        rows.len()
    }

    pub fn find_series_by_tags(&self, tag_filters: &[(String, String)]) -> Option<Vec<u64>> {
        self.tag_index.find_series(tag_filters)
    }

    pub fn should_flush(&self) -> bool {
        self.flush_needed.load(Ordering::Relaxed)
    }

    /// Write the memtable to a new segment.
    ///
    /// Order matters. The rows are already in the log from when they were
    /// accepted; this flush fsyncs them, writes the segment, and only then
    /// checkpoints the log. A crash before the checkpoint leaves the rows
    /// replayable, and a crash after it finds them in the segment.
    pub fn flush(&self) -> anyhow::Result<()> {
        // Claim the rows and the sequence they cover under one lock. `accept_rows`
        // claims a sequence and buffers its rows under this same lock, so every
        // sequence at or below `upto_seq` is present in `rows`. Reading the
        // sequence separately would let a concurrent batch be checkpointed as
        // durable while still only in the memtable, and it would be lost.
        let (rows, upto_seq) = {
            let memtable = self.memtable.write();
            let upto_seq = self.wal_seq.load(Ordering::Relaxed).saturating_sub(1);
            (memtable.drain(), upto_seq)
        };
        if rows.is_empty() {
            return Ok(());
        }

        match self.write_segment(&rows, upto_seq) {
            Ok(()) => Ok(()),
            Err(e) => {
                // The rows are still in the log and were never checkpointed, but
                // they are no longer buffered either. Put them back so a retry
                // can pick them up and a scan can still see them.
                self.memtable.write().insert_batch(&rows);
                self.metrics.record_flush(false);
                Err(e)
            }
        }
    }

    /// Write buffered rows out as a segment, then checkpoint the log.
    fn write_segment(
        &self,
        rows: &[(i64, Vec<Option<Vec<u8>>>)],
        upto_seq: u64,
    ) -> anyhow::Result<()> {
        let segment_path = self.next_segment_path();
        let table_id = self.meta.table_id;
        let ts_unit = self.meta.ts_unit;
        let columns: Vec<ColumnDef> = self.meta.columns.clone();
        let default_compression = self.default_compression();

        // Durability first: make the logged rows survive a machine crash before
        // the checkpoint below claims they do.
        let t0 = std::time::Instant::now();
        self.wal.sync()?;
        let t_sync = t0.elapsed();

        let t0 = std::time::Instant::now();
        let (column_data, min_ts, max_ts) = Self::build_columns(&columns, rows);
        let row_count = rows.len() as u32;
        let t_build = t0.elapsed();

        let t0 = std::time::Instant::now();
        let mut writer = SegmentWriter::new(&segment_path, table_id, ts_unit, schema_types(&columns), default_compression)
            .with_rows_per_block(self.rows_per_block)
            .with_flushed_upto_seq(upto_seq);
        Self::write_blocks(&mut writer, &column_data, row_count, min_ts, max_ts)?;
        let t_blocks = t0.elapsed();

        let t0 = std::time::Instant::now();
        writer.finish()?;
        let t_finish = t0.elapsed();

        if std::env::var_os("CDB_FLUSH_TRACE").is_some() {
            eprintln!(
                "FLUSH_TRACE rows={row_count} wal_sync={:?} build={:?} blocks={:?} finish={:?}",
                t_sync, t_build, t_blocks, t_finish
            );
        }

        let byte_size = std::fs::metadata(&segment_path).map(|m| m.len()).unwrap_or(0);

        self.segments.write().push(SegmentRef {
            path: segment_path,
            level: CompactionLevel::L0,
            min_ts,
            max_ts,
            row_count: row_count as u64,
            byte_size,
        });
        // A compaction or a purge may have deleted the files any cached reader
        // was holding, so release them rather than wait for the size cap.
        self.readers.write().clear();

        self.flush_needed.store(false, Ordering::Relaxed);
        self.metrics.record_flush(true);
        self.metrics.record_bytes_written(byte_size);

        // The segment is complete and fsynced, so these rows are durable and the
        // log copy is redundant. A checkpoint is fsynced; if it fails the rows
        // simply replay into the memtable on the next start, which is safe
        // because they are already in the segment and will be rewritten once.
        let t0 = std::time::Instant::now();
        if let Err(e) = self.wal.checkpoint(table_id, upto_seq) {
            tracing::warn!("could not checkpoint WAL after flush: {}", e);
        }
        let t_ckpt = t0.elapsed();

        let t0 = std::time::Instant::now();
        if let Err(e) = self.wal.compact() {
            tracing::warn!("could not reclaim WAL space after flush: {}", e);
        }
        let t_compact = t0.elapsed();

        if std::env::var_os("CDB_FLUSH_TRACE").is_some() {
            eprintln!(
                "FLUSH_TRACE checkpoint={:?} wal_compact={:?}",
                t_ckpt, t_compact
            );
        }

        // Auto-trigger compaction if enough L0 segments
        let l0_count = self
            .segments
            .read()
            .iter()
            .filter(|s| s.level == CompactionLevel::L0)
            .count();
        if l0_count >= self.compaction_threshold {
            if let Err(e) = self.compact_l0() {
                tracing::error!("auto-compaction failed: {}", e);
            }
        }

        Ok(())
    }

    /// Compression used for a segment when no column asks for a specific level.
    fn default_compression(&self) -> CompressionLevel {
        self.meta
            .columns
            .iter()
            .map(|c| c.compression)
            .find(|c| *c != CompressionLevel::Off)
            .unwrap_or(CompressionLevel::Lz4)
    }

    /// Allocate the next segment file name.
    ///
    /// The counter is monotonic rather than `segments.len()`, because
    /// compaction removes segments from the list and would otherwise hand the
    /// same file name to two different segments.
    fn next_segment_path(&self) -> PathBuf {
        let n = self.segment_seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.data_dir.join(format!("seg_{:06}.tsb", n))
    }

    /// Split raw rows into typed columns.
    ///
    /// Values arrive as the public API's raw byte slices; `ColumnData::push_raw`
    /// interprets them at the width their declared type actually needs, which is
    /// what the old `flat_map` + `i * 8` path got wrong for `Int32`, `Bool` and
    /// `String`.
    fn build_columns(
        columns: &[ColumnDef],
        rows: &[(i64, Vec<Option<Vec<u8>>>)],
    ) -> (Vec<ColumnData>, i64, i64) {
        let mut cols: Vec<ColumnData> = columns
            .iter()
            .map(|c| ColumnData::new(c.data_type, 0))
            .collect();

        for (ts, values) in rows {
            for (i, col) in cols.iter_mut().enumerate() {
                match values.get(i) {
                    Some(Some(raw)) => {
                        col.push_raw(Some(raw)).expect("value does not match column type");
                    }
                    _ => col.push_none(),
                }
            }
            // Column 0 is the timestamp; keep it authoritative for block bounds.
            if cols.is_empty() {
                let _ = ts;
            }
        }

        let min_ts = rows.first().map(|r| r.0).unwrap_or(0);
        let max_ts = rows.last().map(|r| r.0).unwrap_or(0);
        (cols, min_ts, max_ts)
    }

    /// Split typed columns into blocks of `DEFAULT_ROWS_PER_BLOCK` rows.
    fn write_blocks(
        writer: &mut SegmentWriter,
        cols: &[ColumnData],
        row_count: u32,
        min_ts: i64,
        max_ts: i64,
    ) -> anyhow::Result<()> {
        if row_count == 0 {
            return Ok(());
        }
        let block_ts = |start: usize, end: usize| -> (i64, i64) {
            let mut lo = i64::MAX;
            let mut hi = i64::MIN;
            for c in cols {
                if let Some(ColumnData::Timestamp(ts)) = Some(c) {
                    for v in ts[start..end].iter().flatten() {
                        lo = lo.min(*v);
                        hi = hi.max(*v);
                    }
                }
            }
            if lo == i64::MAX {
                (min_ts, max_ts)
            } else {
                (lo, hi)
            }
        };

        let rows_per_block = writer.rows_per_block().max(1);
        let mut start = 0usize;
        let trace = std::env::var_os("CDB_FLUSH_TRACE").is_some();
        let mut t_slice = std::time::Duration::ZERO;
        let mut t_push = std::time::Duration::ZERO;
        while start < row_count as usize {
            let end = (start + rows_per_block).min(row_count as usize);
            let (bmin, bmax) = block_ts(start, end);
            let t0 = std::time::Instant::now();
            let sliced: Vec<ColumnData> = cols
                .iter()
                .map(|c| slice_column(c, start, end))
                .collect::<anyhow::Result<_>>()?;
            t_slice += t0.elapsed();
            let t0 = std::time::Instant::now();
            writer.push_columns(&sliced, (end - start) as u32, bmin, bmax)?;
            t_push += t0.elapsed();
            start = end;
        }
        if trace {
            eprintln!(
                "FLUSH_TRACE write_blocks rows={} blocks={} slice={:?} encode+write={:?}",
                row_count,
                (row_count as usize).div_ceil(rows_per_block),
                t_slice,
                t_push
            );
        }
        Ok(())
    }

    /// Scan with column selection, ZoneMap pruning, and tag filtering.
    ///
    /// `tag_filters` is applied to the stored tag columns: a row is returned
    /// only if every filter matches its own column value. A row whose tag
    /// column is NULL never matches a filter, which is what SQL `WHERE` means
    /// by an unknown comparison.
    ///
    /// The tag index is not consulted here. It can say which series exist, but
    /// rows carry no series id, so it cannot say which *rows* belong to one --
    /// filtering has to happen on the decoded values.
    ///
    /// Returns an error if a filter names something that is not a tag column of
    /// this table, rather than quietly returning every row: a caller who asks
    /// to filter on a column that does not exist has a bug, and answering with
    /// unfiltered data would hide it.
    pub fn scan(
        &self,
        ts_min: i64,
        ts_max: i64,
        tag_filters: &[(String, String)],
        select_columns: Option<&[usize]>,
    ) -> anyhow::Result<Vec<(i64, Vec<Option<Vec<u8>>>)>> {
        let tag_constraints = self.resolve_tag_filters(tag_filters)?;

        let segments = self.segments.read();
        let col_count = self.meta.columns.len();
        let cols_to_decode: Vec<usize> = match select_columns {
            Some(cols) => cols.to_vec(),
            None => (0..col_count).collect(),
        };

        // A tag column needed only for filtering still has to be decoded, even
        // when the caller projected it away.
        let mut decode_set = cols_to_decode.clone();
        for &(col, _) in &tag_constraints {
            if !decode_set.contains(&col) {
                decode_set.push(col);
            }
        }

        let mut all_rows = Vec::new();

        let mut readers = self.readers.write();
        for seg_ref in segments.iter() {
            // ZoneMap pruning: skip segments that don't overlap time range
            if seg_ref.max_ts < ts_min || seg_ref.min_ts > ts_max {
                continue;
            }

            let cached = readers.borrow(&seg_ref.path)?;
            let reader = &cached.reader;

            // Walk only the blocks that overlap the query, and within each block
            // only the requested columns. This is what per-column chunk lengths
            // in the footer make possible.
            //
            // `matching_blocks` filters the metadata in a single pass. Probing
            // every block instead meant a point query tested all 489 of them
            // to find the one it wanted.
            for block in reader.matching_blocks(ts_min, ts_max) {
                let block_id = block.block_id;
                let ts_col = match reader.read_block_column(block_id, 0)? {
                    c @ ColumnData::Timestamp(_) => c,
                    _ => continue,
                };
                let block_rows = ts_col.len();

                let mut decoded: Vec<Option<ColumnData>> = vec![None; col_count];
                for &col_idx in &decode_set {
                    if col_idx == 0 || col_idx >= col_count {
                        continue;
                    }
                    decoded[col_idx] = Some(reader.read_block_column(block_id, col_idx)?);
                }

                for i in 0..block_rows {
                    let ts = match ts_col.as_i64(i) {
                        Some(t) => t,
                        None => continue,
                    };
                    if ts < ts_min || ts > ts_max {
                        continue;
                    }
                    if !tag_constraints.is_empty() {
                        let matched = tag_constraints.iter().all(|&(col, ref want)| {
                            let got = match col {
                                0 => ts_col.get_raw(i),
                                _ => decoded[col].as_ref().and_then(|c| c.get_raw(i)),
                            };
                            got.as_deref() == Some(want.as_slice())
                        });
                        if !matched {
                            continue;
                        }
                    }

                    let mut values = Vec::with_capacity(cols_to_decode.len());
                    for &col_idx in &cols_to_decode {
                        values.push(match col_idx {
                            0 => ts_col.get_raw(i),
                            _ => decoded[col_idx].as_ref().and_then(|c| c.get_raw(i)),
                        });
                    }
                    all_rows.push((ts, values));
                }
            }
        }

        all_rows.sort_by_key(|r| r.0);

        // Rows still buffered in memory have not reached a segment yet, and
        // callers expect `scan` to see them.
        if self.memtable.read().row_count() > 0 {
            for (ts, values) in self.memtable.read().scan(ts_min, ts_max) {
                let matched = tag_constraints.iter().all(|&(col, ref want)| {
                    values.get(col).and_then(|v| v.as_deref()) == Some(want.as_slice())
                });
                if !matched {
                    continue;
                }
                let projected: Vec<Option<Vec<u8>>> = cols_to_decode
                    .iter()
                    .map(|&c| values.get(c).cloned().flatten())
                    .collect();
                all_rows.push((ts, projected));
            }
            all_rows.sort_by_key(|r| r.0);
        }

        self.metrics.record_scan(all_rows.len() as u64);
        Ok(all_rows)
    }

    /// Turn `tag_filters` into (column index, expected raw bytes) pairs.
    fn resolve_tag_filters(
        &self,
        tag_filters: &[(String, String)],
    ) -> anyhow::Result<Vec<(usize, Vec<u8>)>> {
        let mut out = Vec::with_capacity(tag_filters.len());
        for (key, value) in tag_filters {
            let found = self
                .meta
                .columns
                .iter()
                .position(|c| c.is_tag && c.name.eq_ignore_ascii_case(key));
            let idx = match found {
                Some(i) => i,
                None => anyhow::bail!(
                    "tag filter '{}' does not name a tag column of table '{}'",
                    key,
                    self.meta.name
                ),
            };
            // Tag values are text; anything else would compare bytes against a
            // string that was never encoded this way.
            if self.meta.columns[idx].data_type != DataType::String {
                anyhow::bail!(
                    "tag column '{}' has type {}, which cannot be compared to a tag value",
                    key,
                    self.meta.columns[idx].data_type
                );
            }
            out.push((idx, value.as_bytes().to_vec()));
        }
        Ok(out)
    }

    pub fn row_count(&self) -> u64 {
        self.segments.read().iter().map(|s| s.row_count).sum::<u64>()
            + self.memtable.read().row_count()
    }

    pub fn segment_count(&self) -> usize {
        self.segments.read().len()
    }

    pub fn memtable_size(&self) -> u64 {
        self.memtable.read().byte_size()
    }

    /// Bytes this table occupies on disk, segments and log included.
    pub fn disk_bytes(&self) -> u64 {
        self.segments.read().iter().map(|s| s.byte_size).sum::<u64>() + self.wal_size()
    }

    /// Snapshot of the tracked segments.
    pub fn segments(&self) -> Vec<SegmentRef> {
        self.segments.read().clone()
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Start async flush background thread
    pub fn start_async_flush(table: Arc<Table>, interval_ms: u64) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(interval_ms));
            loop {
                interval.tick().await;
                if table.should_flush() {
                    match table.flush() {
                        Ok(_) => {}
                        Err(e) => tracing::error!("async flush failed: {}", e),
                    }
                }
            }
        });
    }

    /// Start async compaction background thread (L0 toL1 merge)
    pub fn start_async_compaction(table: Arc<Table>, interval_ms: u64) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(interval_ms));
            loop {
                interval.tick().await;
                let l0_count = table.segments.read().iter().filter(|s| s.level == CompactionLevel::L0).count();
                if l0_count >= 4 {
                    // Trigger compaction: merge L0 segments into L1
                    tracing::info!("compaction triggered: {} L0 segments", l0_count);
                    match table.compact_l0() {
                        Ok(_) => tracing::info!("compaction completed"),
                        Err(e) => tracing::error!("compaction failed: {}", e),
                    }
                }
            }
        });
    }

    /// One pass of background maintenance: flush, compact, then purge.
    ///
    /// Exposed so a caller can run maintenance on its own schedule instead of
    /// taking the background thread.
    pub fn maintenance_pass(&self, now: i64) -> anyhow::Result<MaintenanceOutcome> {
        let mut outcome = MaintenanceOutcome::default();

        if self.should_flush() || self.memtable.read().row_count() > 0 {
            match self.flush() {
                Ok(()) => outcome.flushed = true,
                Err(e) => {
                    self.metrics.record_flush(false);
                    tracing::error!("maintenance flush failed: {}", e);
                    outcome.flush_error = Some(e.to_string());
                }
            }
        }

        // Compact any mergeable L0 pair. The in-flush trigger normally merges
        // first and keeps L0 below the threshold, so waiting for the threshold
        // here would leave this branch unreachable in normal operation. The
        // threshold governs the in-flush backstop for callers that never run
        // maintenance; a maintenance pass merges whatever is available.
        let l0 = self
            .segments
            .read()
            .iter()
            .filter(|s| s.level == CompactionLevel::L0)
            .count();
        if l0 >= 2 {
            outcome.compacted = true;
            outcome.segments_before = self.segments.read().len() as u64;
            match self.compact_l0() {
                Ok(()) => outcome.segments_after = self.segments.read().len() as u64,
                Err(e) => {
                    self.metrics.record_compaction(false);
                    tracing::error!("maintenance compaction failed: {}", e);
                    outcome.compaction_error = Some(e.to_string());
                }
            }
        }

        if self.meta.retention_days > 0 {
            self.compaction.set_segments(self.segments.read().clone());
            match self.compaction.purge_expired(now) {
                Ok((removed, rows)) if removed > 0 => {
                    *self.segments.write() = self.compaction.segments();
                    self.readers.write().clear();
                    outcome.segments_purged = removed as u64;
                    outcome.rows_purged = rows;
                    self.metrics.record_purge(rows);
                }
                Ok(_) => {}
                Err(e) => tracing::warn!("purge failed: {}", e),
            }
        }

        Ok(outcome)
    }

    /// Start a background thread doing flush, compaction and TTL purges.
    ///
    /// Returns a handle that stops the loop when dropped, so a test or a clean
    /// shutdown does not leave a task running against a closed table.
    pub fn start_maintenance(
        table: Arc<Table>,
        interval_ms: u64,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(interval_ms));
            loop {
                interval.tick().await;

                // TTL is compared against wall-clock time, so the maintenance
                // loop has to be told what "now" is rather than assuming the
                // caller's clock matches the data.
                let now = table.now_ms();
                if let Err(e) = table.maintenance_pass(now) {
                    tracing::error!("maintenance pass failed: {}", e);
                }
            }
        })
    }

    /// Current wall-clock time in the table's own unit, best effort.
    fn now_ms(&self) -> i64 {
        self.meta.ts_unit.now()
    }

    /// Compact L0 segments into L1.
    ///
    /// Delegates to [`CompactionManager`], which owns the merge: it selects a
    /// bounded group, writes the merged segment before unlinking its inputs, and
    /// updates the tracked list atomically.
    fn compact_l0(&self) -> anyhow::Result<()> {
        // Keep the manager's view in step with the table's before selecting.
        self.compaction.set_segments(self.segments.read().clone());

        const MAX_GROUP: usize = 8;
        let mut merged_any = false;

        while let Some(group) = self.compaction.select_merge_group(MAX_GROUP) {
            match self.compaction.compact(group, &self.compaction_spec()) {
                Ok(result) => {
                    self.metrics.record_compaction(true);
                    self.metrics.record_merge(result.bytes_in, result.bytes_out);
                    self.metrics
                        .record_rows_merged(result.segment.row_count);
                    tracing::info!(
                        "compacted {} segments into {} ({} rows, {} -> {} bytes)",
                        result.inputs.len(),
                        result.segment.path.display(),
                        result.segment.row_count,
                        result.bytes_in,
                        result.bytes_out,
                    );
                    merged_any = true;
                }
                Err(e) => {
                    self.metrics.record_compaction(false);
                    tracing::error!("compaction failed: {}", e);
                    break;
                }
            }
        }

        if merged_any {
            *self.segments.write() = self.compaction.segments();
            // The merged inputs are gone and the output is a new file, so no
            // cached reader still describes live data.
            self.readers.write().clear();
        }
        Ok(())
    }

    /// Merge description handed to [`CompactionManager`].
    fn compaction_spec(&self) -> CompactionSpec {
        let mut spec = CompactionSpec::new(
            self.meta.table_id,
            self.meta.ts_unit,
            schema_types(&self.meta.columns),
        );
        spec.compression = self.default_compression();
        spec
    }

    /// Public entry point for background compaction.
    pub fn compact(&self) -> anyhow::Result<()> {
        self.compact_l0()
    }

    /// Number of completed compactions.
    pub fn compaction_count(&self) -> u64 {
        self.compaction.compaction_count()
    }

    /// L0 segment count that makes `flush` merge on its own.
    ///
    /// This is the backstop for callers that never run
    /// [`Table::maintenance_pass`]. A maintenance pass merges any available
    /// L0 pair regardless of this value.
    pub fn compaction_threshold(&self) -> usize {
        self.compaction_threshold
    }

    /// Change how many L0 segments make `flush` merge on its own.
    ///
    /// Values below 2 are raised to 2, since a merge needs at least two inputs.
    pub fn set_compaction_threshold(&mut self, segments: usize) {
        self.compaction_threshold = segments.max(2);
    }

    /// Rows per block written into new segments.
    pub fn rows_per_block(&self) -> usize {
        self.rows_per_block
    }

    /// Change the block size for segments written from now on.
    ///
    /// Larger blocks encode and compress better but give zone-map pruning less to
    /// work with on a wide scan. Existing segments keep the size they were
    /// written with, so this is safe to change at any time.
    ///
    /// Values below 1 are raised to 1, since a block holds at least one row.
    pub fn set_rows_per_block(&mut self, rows: usize) {
        self.rows_per_block = rows.max(1);
    }

    /// Access the cardinality guard for direct inspection.
    pub fn cardinality(&self) -> &CardinalityGuard {
        &self.cardinality
    }

    /// Point-in-time reading of every counter and gauge.
    pub fn metrics(&self) -> MetricsSnapshot {
        let segments = self.segments.read();
        let segment_bytes: u64 = segments.iter().map(|s| s.byte_size).sum();
        let segment_count = segments.len() as u64;
        drop(segments);

        let memtable = self.memtable.read();
        let memtable_rows = memtable.row_count();
        let memtable_bytes = memtable.byte_size();
        drop(memtable);

        let disk_bytes = segment_bytes + self.wal_size();

        MetricsSnapshot {
            uptime_secs: self.metrics.uptime().as_secs_f64(),
            points_written: self.metrics.points_written(),
            points_scanned: self.metrics.points_scanned(),
            bytes_written: self.metrics.bytes_written(),
            bytes_read: self.metrics.bytes_read(),
            flushes: self.metrics.flushes(),
            flush_failures: self.metrics.flush_failures(),
            compactions: self.metrics.compactions(),
            compaction_failures: self.metrics.compaction_failures(),
            compaction_bytes_in: self.metrics.compaction_bytes_in(),
            compaction_bytes_out: self.metrics.compaction_bytes_out(),
            rows_merged: self.metrics.rows_merged(),
            wal_appends: self.metrics.wal_appends(),
            wal_bytes: self.metrics.wal_bytes(),
            wal_recovered_rows: self.metrics.wal_recovered_rows(),
            rows_purged: self.metrics.rows_purged(),
            points_rejected: self.metrics.points_rejected(),
            row_count: self.row_count(),
            memtable_rows,
            memtable_bytes,
            segment_count,
            segment_bytes,
            wal_size_bytes: self.wal_size(),
            disk_bytes,
            series_count: self.cardinality.series_count(),
            tag_cardinality: self
                .cardinality
                .tag_names()
                .into_iter()
                .map(|n| {
                    let count = self.cardinality.tag_cardinality(&n);
                    (n, count)
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColumnDef, CompressionLevel, DataType, TsUnit};

    fn test_meta() -> TableMeta {
        TableMeta {
            table_id: 1,
            name: "test_table".to_string(),
            ts_unit: TsUnit::Millis,
            columns: vec![
                ColumnDef { name: "ts".to_string(), data_type: DataType::Timestamp, is_tag: false, nullable: false, compression: CompressionLevel::Off },
                ColumnDef { name: "value".to_string(), data_type: DataType::Float64, is_tag: false, nullable: true, compression: CompressionLevel::Lz4 },
            ],
            chunk_interval_ms: 86400000,
            retention_days: 365,
        }
    }

    #[test]
    fn test_insert_flush_scan() {
        let dir = tempfile::tempdir().unwrap();
        let table = Table::new(test_meta(), dir.path());

        for i in 0..100 {
            let ts = 1000 + i * 1000;
            let val = (220.0 + i as f64 * 0.01).to_le_bytes().to_vec();
            table.insert(ts, vec![Some(ts.to_le_bytes().to_vec()), Some(val)]);
        }

        table.flush().unwrap();
        assert_eq!(table.row_count(), 100);

        let rows = table.scan(1000, 100_000, &[], None).unwrap();
        assert_eq!(rows.len(), 100);
        assert!(rows[0].1[1].is_some());
    }

    /// A schema whose tag columns are stored alongside the value columns, which
    /// is what `Table::scan`'s tag filter reads.
    fn tagged_meta() -> TableMeta {
        TableMeta {
            table_id: 1,
            name: "tagged".to_string(),
            ts_unit: TsUnit::Millis,
            columns: vec![
                ColumnDef { name: "ts".to_string(), data_type: DataType::Timestamp, is_tag: false, nullable: false, compression: CompressionLevel::Off },
                ColumnDef { name: "host".to_string(), data_type: DataType::String, is_tag: true, nullable: false, compression: CompressionLevel::Lz4 },
                ColumnDef { name: "region".to_string(), data_type: DataType::String, is_tag: true, nullable: false, compression: CompressionLevel::Lz4 },
                ColumnDef { name: "value".to_string(), data_type: DataType::Float64, is_tag: false, nullable: true, compression: CompressionLevel::Lz4 },
            ],
            chunk_interval_ms: 86400000,
            retention_days: 365,
        }
    }

    fn tagged_row(ts: i64, host: &str, region: &str, v: f64) -> (i64, Vec<Option<Vec<u8>>>) {
        (
            ts,
            vec![
                Some(ts.to_le_bytes().to_vec()),
                Some(host.as_bytes().to_vec()),
                Some(region.as_bytes().to_vec()),
                Some(v.to_le_bytes().to_vec()),
            ],
        )
    }

    /// `scan` accepted a tag filter and ignored it, so a query asking for one
    /// host got every host. It must filter, both before and after a flush.
    #[test]
    fn scan_applies_tag_filters_before_and_after_a_flush() {
        let dir = tempfile::tempdir().unwrap();
        let table = Table::new(tagged_meta(), dir.path());

        let rows = vec![
            tagged_row(1000, "a", "eu", 1.0),
            tagged_row(2000, "b", "eu", 2.0),
            tagged_row(3000, "a", "us", 3.0),
        ];
        table.insert_batch(&rows);

        let f = |k: &str, v: &str| vec![(k.to_string(), v.to_string())];
        let scan = |table: &Table, filters: &[(String, String)]| {
            table
                .scan(0, i64::MAX, filters, None)
                .unwrap()
                .into_iter()
                .map(|(ts, _)| ts)
                .collect::<Vec<i64>>()
        };

        // Still in the memtable.
        assert_eq!(scan(&table, &f("host", "a")), vec![1000, 3000]);
        assert_eq!(scan(&table, &f("region", "us")), vec![3000]);
        assert_eq!(scan(&table, &[]), vec![1000, 2000, 3000]);

        table.flush().unwrap();

        // And from segments, where the filtering has to happen on decoded
        // column values instead.
        assert_eq!(scan(&table, &f("host", "a")), vec![1000, 3000]);
        assert_eq!(scan(&table, &f("region", "us")), vec![3000]);
        assert_eq!(
            scan(&table, &[("host".into(), "a".into()), ("region".into(), "us".into())]),
            vec![3000]
        );
        assert_eq!(scan(&table, &f("host", "nobody")), Vec::<i64>::new());
    }

    /// Two filters must both hold, not either one.
    #[test]
    fn tag_filters_are_conjunctive() {
        let dir = tempfile::tempdir().unwrap();
        let table = Table::new(tagged_meta(), dir.path());
        table.insert_batch(&[
            tagged_row(1000, "a", "eu", 1.0),
            tagged_row(2000, "a", "us", 2.0),
            tagged_row(3000, "b", "eu", 3.0),
        ]);
        table.flush().unwrap();

        let both = vec![
            ("host".to_string(), "a".to_string()),
            ("region".to_string(), "eu".to_string()),
        ];
        let rows = table.scan(0, i64::MAX, &both, None).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, 1000);
    }

    /// A tag column the caller did not project must still be available for the
    /// filter; otherwise `SELECT value WHERE host = 'a'` silently matches all.
    #[test]
    fn tag_filter_works_when_the_tag_column_is_not_projected() {
        let dir = tempfile::tempdir().unwrap();
        let table = Table::new(tagged_meta(), dir.path());
        table.insert_batch(&[
            tagged_row(1000, "a", "eu", 1.0),
            tagged_row(2000, "b", "eu", 2.0),
        ]);
        table.flush().unwrap();

        let filters = vec![("host".to_string(), "a".to_string())];
        // Only ts and value.
        let rows = table.scan(0, i64::MAX, &filters, Some(&[0, 3])).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, 1000);
        assert_eq!(rows[0].1.len(), 2);
    }

    /// Returning unfiltered rows would hide the caller's bug, so a filter on a
    /// column that is not a tag is an error.
    #[test]
    fn a_filter_on_a_non_tag_column_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let table = Table::new(tagged_meta(), dir.path());
        table.insert_batch(&[tagged_row(1000, "a", "eu", 1.0)]);

        let err = table
            .scan(0, i64::MAX, &[("nosuch".to_string(), "x".to_string())], None)
            .unwrap_err();
        assert!(err.to_string().contains("nosuch"), "got: {}", err);

        // A value column is not a tag column either.
        let err = table
            .scan(0, i64::MAX, &[("value".to_string(), "1".to_string())], None)
            .unwrap_err();
        assert!(err.to_string().contains("value"), "got: {}", err);
    }

    /// The filter key is matched case-insensitively, as column names are
    /// elsewhere, so `WHERE Host = 'a'` behaves like `WHERE host = 'a'`.
    #[test]
    fn tag_filter_keys_match_case_insensitively() {
        let dir = tempfile::tempdir().unwrap();
        let table = Table::new(tagged_meta(), dir.path());
        table.insert_batch(&[tagged_row(1000, "a", "eu", 1.0)]);
        let rows = table
            .scan(0, i64::MAX, &[("HOST".to_string(), "a".to_string())], None)
            .unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_insert_with_tags() {
        let dir = tempfile::tempdir().unwrap();
        let table = Table::new(test_meta(), dir.path());

        table.insert_with_tags(1000, vec![Some(1000i64.to_le_bytes().to_vec()), Some(220.0f64.to_le_bytes().to_vec())],
            &[("device_id".to_string(), "10086".to_string()), ("line_id".to_string(), "7".to_string())]);
        table.insert_with_tags(2000, vec![Some(2000i64.to_le_bytes().to_vec()), Some(221.0f64.to_le_bytes().to_vec())],
            &[("device_id".to_string(), "10087".to_string()), ("line_id".to_string(), "7".to_string())]);
        table.insert_with_tags(3000, vec![Some(3000i64.to_le_bytes().to_vec()), Some(222.0f64.to_le_bytes().to_vec())],
            &[("device_id".to_string(), "10086".to_string()), ("line_id".to_string(), "8".to_string())]);

        let ids = table.find_series_by_tags(&[("device_id".to_string(), "10086".to_string())]);
        assert_eq!(ids.unwrap().len(), 2);

        let ids = table.find_series_by_tags(&[
            ("device_id".to_string(), "10086".to_string()),
            ("line_id".to_string(), "7".to_string()),
        ]);
        assert_eq!(ids.unwrap().len(), 1);
    }

    fn row(ts: i64, v: f64) -> (i64, Vec<Option<Vec<u8>>>) {
        (ts, vec![Some(ts.to_le_bytes().to_vec()), Some(v.to_le_bytes().to_vec())])
    }

    #[test]
    fn test_wal_file_created() {
        let dir = tempfile::tempdir().unwrap();
        let table = Table::new(test_meta(), dir.path());

        table.insert(1000, vec![Some(1000i64.to_le_bytes().to_vec()), Some(220.0f64.to_le_bytes().to_vec())]);
        table.flush().unwrap();

        let wal_path = dir.path().join("tmdb.wal");
        assert!(wal_path.exists());
    }

    #[test]
    fn test_rows_are_logged_when_accepted() {
        let dir = tempfile::tempdir().unwrap();
        let table = Table::new(test_meta(), dir.path());

        table.insert_batch(&(0..50).map(|i| row(1000 + i, i as f64)).collect::<Vec<_>>());
        assert_eq!(
            table.wal_pending_rows(),
            50,
            "accepted rows must be in the log before any flush"
        );
    }

    #[test]
    fn test_flush_clears_the_log() {
        let dir = tempfile::tempdir().unwrap();
        let table = Table::new(test_meta(), dir.path());

        table.insert_batch(&(0..50).map(|i| row(1000 + i, i as f64)).collect::<Vec<_>>());
        assert_eq!(table.wal_pending_rows(), 50);

        table.flush().unwrap();
        assert_eq!(table.wal_pending_rows(), 0, "log must be trimmed once durable");
        assert_eq!(table.row_count(), 50);
    }

    #[test]
    fn test_partial_flush_leaves_later_rows_pending() {
        let dir = tempfile::tempdir().unwrap();
        let table = Table::new(test_meta(), dir.path());

        table.insert_batch(&(0..50).map(|i| row(1000 + i, i as f64)).collect::<Vec<_>>());
        table.flush().unwrap();
        assert_eq!(table.wal_pending_rows(), 0);

        // A second batch arrives after the checkpoint.
        table.insert_batch(&(50..80).map(|i| row(9000 + i, i as f64)).collect::<Vec<_>>());
        assert_eq!(table.wal_pending_rows(), 30, "the newer batch is not yet durable");
        assert_eq!(table.row_count(), 80);
    }

    #[test]
    fn test_log_stays_bounded_across_many_flushes() {
        let dir = tempfile::tempdir().unwrap();
        let table = Table::new(test_meta(), dir.path());

        for round in 0..20 {
            table.insert_batch(
                &(0..100)
                    .map(|i| row(round * 1000 + i, i as f64))
                    .collect::<Vec<_>>(),
            );
            table.flush().unwrap();
        }

        assert_eq!(table.wal_pending_rows(), 0);
        assert_eq!(table.row_count(), 2000);

        // 2000 rows fully logged would be well over 50 KB if nothing was
        // reclaimed; an empty log is a few bytes.
        assert!(
            table.wal_size() < 64,
            "log grew to {} bytes despite 20 flushes",
            table.wal_size()
        );
    }

    #[test]
    fn test_reopen_sees_flushed_rows() {
        let dir = tempfile::tempdir().unwrap();
        {
            let table = Table::new(test_meta(), dir.path());
            table.insert_batch(&(0..200).map(|i| row(1000 + i * 10, i as f64)).collect::<Vec<_>>());
            table.flush().unwrap();
        }

        let reopened = Table::new(test_meta(), dir.path());
        assert_eq!(reopened.row_count(), 200);
        assert_eq!(reopened.segment_count(), 1);

        let rows = reopened.scan(0, 1_000_000, &[], None).unwrap();
        assert_eq!(rows.len(), 200);
        assert_eq!(rows[0].0, 1000);
        assert_eq!(rows[199].0, 1000 + 199 * 10);
    }

    #[test]
    fn test_unflushed_rows_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            // Insert and never flush: a crash leaves these only in the log.
            let table = Table::new(test_meta(), dir.path());
            table.insert_batch(&(0..30).map(|i| row(5000 + i * 10, i as f64)).collect::<Vec<_>>());
            assert_eq!(table.segment_count(), 0, "nothing on disk yet");
        }

        let reopened = Table::new(test_meta(), dir.path());
        assert_eq!(reopened.row_count(), 30, "log replay must restore the rows");

        // The replayed rows are queryable before any further flush.
        let rows = reopened.scan(0, 1_000_000, &[], None).unwrap();
        assert_eq!(rows.len(), 30);
        assert_eq!(rows[0].0, 5000);

        // And they become a normal segment on the next flush.
        reopened.flush().unwrap();
        assert_eq!(reopened.row_count(), 30);
        let rows = reopened.scan(0, 1_000_000, &[], None).unwrap();
        assert_eq!(rows.len(), 30);
    }

    #[test]
    fn test_reopen_does_not_duplicate_replayed_rows() {
        let dir = tempfile::tempdir().unwrap();
        {
            let table = Table::new(test_meta(), dir.path());
            table.insert_batch(&(0..40).map(|i| row(1000 + i, i as f64)).collect::<Vec<_>>());
            table.flush().unwrap();
            table.insert_batch(&(100..140).map(|i| row(9000 + i, i as f64)).collect::<Vec<_>>());
            // flush() failed to run: the second batch is only in the log.
        }

        let reopened = Table::new(test_meta(), dir.path());
        assert_eq!(reopened.row_count(), 80, "40 flushed + 40 replayed");
        assert_eq!(reopened.segment_count(), 1);
    }

    #[test]
    fn test_torn_log_tail_is_discarded_not_fatal() {
        let dir = tempfile::tempdir().unwrap();
        {
            let table = Table::new(test_meta(), dir.path());
            table.insert_batch(&(0..20).map(|i| row(1000 + i, i as f64)).collect::<Vec<_>>());
            table.flush().unwrap();
            table.insert_batch(&(0..20).map(|i| row(7000 + i, i as f64)).collect::<Vec<_>>());
            table.flush().unwrap();
            table.insert_batch(&(0..20).map(|i| row(13_000 + i, i as f64)).collect::<Vec<_>>());
            table.flush().unwrap();
        }

        // Nothing is pending after the three flushes, so the log is just a header and a
        // truncated tail costs nothing.
        let wal = dir.path().join("tmdb.wal");
        let data = std::fs::read(&wal).unwrap();
        std::fs::write(&wal, &data[..data.len().saturating_sub(9)]).unwrap();

        let reopened = Table::new(test_meta(), dir.path());
        assert_eq!(reopened.row_count(), 60, "all three segments survive");

        // The repaired log accepts new writes.
        reopened.insert_batch(&(0..10).map(|i| row(20_000 + i, i as f64)).collect::<Vec<_>>());
        assert_eq!(reopened.wal_pending_rows(), 10);
        reopened.flush().unwrap();
        assert_eq!(reopened.row_count(), 70);
    }

    #[test]
    fn test_pending_rows_replay_after_a_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        {
            let table = Table::new(test_meta(), dir.path());
            table.insert_batch(&(0..20).map(|i| row(1000 + i, i as f64)).collect::<Vec<_>>());
            table.flush().unwrap();

            // Three separate logged batches, none flushed: all pending.
            for batch in 0..3 {
                table.insert_batch(
                    &(0..10)
                        .map(|i| row(7000 + batch * 100 + i, i as f64))
                        .collect::<Vec<_>>(),
                );
            }
            assert_eq!(table.wal_pending_rows(), 30);
        }

        // Cut the last record in half, as a crash mid-append would.
        let wal = dir.path().join("tmdb.wal");
        let data = std::fs::read(&wal).unwrap();
        std::fs::write(&wal, &data[..data.len() - 7]).unwrap();

        let reopened = Table::new(test_meta(), dir.path());
        assert_eq!(
            reopened.row_count(),
            40,
            "20 flushed plus the 20 pending rows whose records survived"
        );
        assert_eq!(reopened.segment_count(), 1);
    }

    #[test]
    fn test_corrupt_log_is_survivable() {
        let dir = tempfile::tempdir().unwrap();
        {
            let table = Table::new(test_meta(), dir.path());
            table.insert_batch(&(0..10).map(|i| row(1000 + i, i as f64)).collect::<Vec<_>>());
            table.flush().unwrap();
        }

        // Flip bytes in the middle of the record payload.
        let wal = dir.path().join("tmdb.wal");
        let mut data = std::fs::read(&wal).unwrap();
        for b in data.iter_mut().skip(8).take(4) {
            *b ^= 0xA5;
        }
        std::fs::write(&wal, &data).unwrap();

        let reopened = Table::new(test_meta(), dir.path());
        assert_eq!(
            reopened.row_count(),
            10,
            "rows already in a segment must not be lost to a bad log"
        );
    }

    #[test]
    fn test_restart_does_not_overwrite_existing_segments() {
        let dir = tempfile::tempdir().unwrap();
        for round in 0..3 {
            let table = Table::new(test_meta(), dir.path());
            table.insert_batch(
                &(0..10)
                    .map(|i| row(1000 * (round + 1) + i, i as f64))
                    .collect::<Vec<_>>(),
            );
            table.flush().unwrap();
        }

        let reopened = Table::new(test_meta(), dir.path());
        assert_eq!(reopened.segment_count(), 3, "each round produced a segment");
        assert_eq!(reopened.row_count(), 30);

        // Distinct file names, all still present.
        let files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .filter(|e| e.path().extension().map(|x| x == "tsb").unwrap_or(false))
            .collect();
        assert_eq!(files.len(), 3);
    }

    #[test]
    fn test_multiple_flushes_stay_queryable_across_restart() {
        let dir = tempfile::tempdir().unwrap();
        let total = 500i64;
        {
            let table = Table::new(test_meta(), dir.path());
            for chunk in 0..5i64 {
                let base = chunk * 100;
                table.insert_batch(
                    &(0..100)
                        .map(|i| row(base + i, (base + i) as f64))
                        .collect::<Vec<_>>(),
                );
                table.flush().unwrap();
            }
        }

        let reopened = Table::new(test_meta(), dir.path());
        assert_eq!(reopened.row_count(), total as u64);

        let rows = reopened.scan(0, i64::MAX, &[], None).unwrap();
        assert_eq!(rows.len() as i64, total);
        let timestamps: Vec<i64> = rows.iter().map(|r| r.0).collect();
        let mut sorted = timestamps.clone();
        sorted.sort_unstable();
        assert_eq!(timestamps, sorted, "scan must return time-ordered rows");
        assert_eq!(timestamps[0], 0);
        assert_eq!(*timestamps.last().unwrap(), total - 1);
    }

    #[test]
    fn test_cardinality_limit_refuses_new_tag_values() {
        let dir = tempfile::tempdir().unwrap();
        let limits = CardinalityLimits {
            per_tag: 3,
            per_series: 100,
            max_series: 0,
        };
        let table = Table::open_with_limits(test_meta(), dir.path(), limits).unwrap();

        for i in 0..3i64 {
            table.insert_with_tags(
                1000 + i,
                vec![
                    Some((1000 + i).to_le_bytes().to_vec()),
                    Some(1.0f64.to_le_bytes().to_vec()),
                ],
                &[("host".to_string(), format!("h{}", i))],
            );
        }
        assert_eq!(table.row_count(), 3);

        // The fourth host is a new series and must be refused.
        table.insert_with_tags(
            2000,
            vec![Some(2000i64.to_le_bytes().to_vec()), Some(2.0f64.to_le_bytes().to_vec())],
            &[("host".to_string(), "h3".to_string())],
        );
        assert_eq!(table.row_count(), 3, "the refused row must not be stored");
        assert_eq!(table.metrics().points_rejected, 1);

        // An existing host still writes.
        table.insert_with_tags(
            3000,
            vec![Some(3000i64.to_le_bytes().to_vec()), Some(3.0f64.to_le_bytes().to_vec())],
            &[("host".to_string(), "h0".to_string())],
        );
        assert_eq!(table.row_count(), 4);
    }

    #[test]
    fn test_refused_batch_stores_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let limits = CardinalityLimits {
            per_tag: 2,
            per_series: 100,
            max_series: 0,
        };
        let table = Table::open_with_limits(test_meta(), dir.path(), limits).unwrap();

        let batch: Vec<_> = (0..4i64)
            .map(|i| {
                (
                    1000 + i,
                    vec![
                        Some((1000 + i).to_le_bytes().to_vec()),
                        Some((i as f64).to_le_bytes().to_vec()),
                    ],
                    vec![("host".to_string(), format!("h{}", i))],
                )
            })
            .collect();
        table.insert_batch_with_tags(&batch);

        assert_eq!(
            table.row_count(),
            0,
            "a batch is all-or-nothing; the third host is over the limit"
        );
        assert_eq!(table.wal_pending_rows(), 0, "nothing may be logged either");
    }

    /// A refused batch must not consume any cardinality headroom either.
    ///
    /// The rows were correctly not written, so the existing test could not tell
    /// the difference. But the admission loop committed each row's tags as it
    /// went, so the two hosts that fitted had already been counted against a
    /// limit of two. The table then believed it was full and refused writes for
    /// hosts that had never been stored.
    #[test]
    fn test_refused_batch_burns_no_headroom() {
        let dir = tempfile::tempdir().unwrap();
        let limits = CardinalityLimits {
            per_tag: 2,
            per_series: 100,
            max_series: 0,
        };
        let table = Table::open_with_limits(test_meta(), dir.path(), limits).unwrap();

        let with_host = |h: &str| -> (i64, Vec<Option<Vec<u8>>>, Vec<(String, String)>) {
            (
                1000,
                vec![
                    Some(1000i64.to_le_bytes().to_vec()),
                    Some(1.0f64.to_le_bytes().to_vec()),
                ],
                vec![("host".to_string(), h.to_string())],
            )
        };

        // Refused: three distinct hosts against a limit of two.
        let refused = vec![with_host("a"), with_host("b"), with_host("c")];
        table.insert_batch_with_tags(&refused);
        assert_eq!(table.row_count(), 0);

        // The sharp part: nothing was written, so the table holds no series at
        // all and the whole limit is still free. A host that was merely *seen*
        // in the refused batch must not have used any of it up.
        table.insert_batch_with_tags(&[with_host("w")]);
        assert_eq!(
            table.row_count(),
            1,
            "a host nobody stored was refused, so the refused batch burned headroom"
        );

        // And the limit is still exactly two distinct hosts.
        table.insert_batch_with_tags(&[with_host("v")]);
        assert_eq!(table.row_count(), 2);
        table.insert_batch_with_tags(&[with_host("u")]);
        assert_eq!(table.row_count(), 2, "the third host is over the limit");
    }

    /// The tag index and the cardinality guard must agree on series identity.
    ///
    /// Both used to number series from independent counters, so the same tag set
    /// had one id in the guard and a different one in the index. Anything that
    /// resolved tags through one and looked them up in the other would miss.
    #[test]
    fn test_tag_index_and_guard_agree_on_series_ids() {
        let dir = tempfile::tempdir().unwrap();
        let table = Table::open_with_limits(
            test_meta(),
            dir.path(),
            CardinalityLimits::unlimited(),
        )
        .unwrap();

        let tags = vec![("host".to_string(), "a".to_string())];
        table.insert_with_tags(1000, row(1000, 1.0).1, &tags);

        let from_guard = table.admit_tags(&tags).unwrap();
        let from_index = table.find_series_by_tags(&tags).unwrap()[0];
        assert_eq!(
            from_guard, from_index,
            "the guard and the tag index numbered the same series differently"
        );
    }

    #[test]
    fn test_metrics_count_writes_scans_and_flushes() {
        let dir = tempfile::tempdir().unwrap();
        let table = Table::new(test_meta(), dir.path());

        table.insert_batch(&(0..100).map(|i| row(1000 + i, i as f64)).collect::<Vec<_>>());
        assert_eq!(table.metrics().points_written, 100);
        assert_eq!(table.metrics().memtable_rows, 100);

        table.flush().unwrap();
        let m = table.metrics();
        assert_eq!(m.flushes, 1);
        assert_eq!(m.segment_count, 1);
        assert!(m.bytes_written > 0);
        assert!(m.wal_appends >= 1);
        assert!(m.wal_bytes > 0);
        assert_eq!(m.row_count, 100);

        table.scan(0, i64::MAX, &[], None).unwrap();
        assert_eq!(table.metrics().points_scanned, 100);
    }

    #[test]
    fn test_metrics_report_compression() {
        let dir = tempfile::tempdir().unwrap();
        let table = Table::new(test_meta(), dir.path());

        table.insert_batch(&(0..1000).map(|i| row(1000 + i, 20.0)).collect::<Vec<_>>());
        table.flush().unwrap();

        let m = table.metrics();
        let ratio = m.compression_ratio().expect("flushed data must have a ratio");
        assert!(
            ratio > 1.0,
            "repeating floats should compress, got {}x",
            ratio
        );
    }

    #[test]
    fn test_metrics_count_recovery_on_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let table = Table::new(test_meta(), dir.path());
            table.insert_batch(&(0..40).map(|i| row(1000 + i, i as f64)).collect::<Vec<_>>());
            // No flush: the rows live only in the log.
        }

        let reopened = Table::new(test_meta(), dir.path());
        assert_eq!(reopened.metrics().wal_recovered_rows, 40);
    }

    #[test]
    fn test_metrics_prometheus_text_is_well_formed() {
        let dir = tempfile::tempdir().unwrap();
        let table = Table::new(test_meta(), dir.path());
        table.insert_batch(&(0..10).map(|i| row(1000 + i, i as f64)).collect::<Vec<_>>());
        table.flush().unwrap();

        let text = table.metrics().to_prometheus("tmdb_table_");
        assert!(text.contains("tmdb_table_points_written_total 10"));
        assert!(text.contains("tmdb_table_row_count 10"));
        assert!(text.ends_with('\n'));
    }

    #[test]
    fn test_metrics_track_compactions() {
        let dir = tempfile::tempdir().unwrap();
        let table = Table::new(test_meta(), dir.path());

        for round in 0..8 {
            table.insert_batch(
                &(0..20)
                    .map(|i| row(round * 100 + i, (round * 100 + i) as f64))
                    .collect::<Vec<_>>(),
            );
            table.flush().unwrap();
        }

        let m = table.metrics();
        assert!(m.compactions >= 1, "8 L0 segments should have been merged");
        assert!(m.rows_merged > 0);
        assert!(m.compaction_bytes_in > 0);
        assert!(m.compaction_bytes_out > 0);
        assert!(m.compaction_ratio().is_some());
    }

    #[test]
    fn test_maintenance_flushes_and_compacts() {
        let dir = tempfile::tempdir().unwrap();
        let mut table = Table::new(test_meta(), dir.path());
        // Out of the way of the in-flush auto-compaction, so this test measures
        // what the maintenance pass itself does. The pass checks the same field,
        // so the count of L0 segments is arranged to clear *this* threshold.
        table.set_compaction_threshold(4);

        for round in 0..4i64 {
            table.insert_batch(
                &(0..20)
                    .map(|i| row(round * 100 + i, (round * 100 + i) as f64))
                    .collect::<Vec<_>>(),
            );
            table.flush().unwrap();
        }
        // The fourth flush itself triggered a merge, so top the count back up
        // without going over the threshold again.
        assert!(table.segment_count() <= 4);

        // A batch still in the memtable, so the pass has something to flush too.
        table.insert_batch(&(500..520).map(|i| row(i, i as f64)).collect::<Vec<_>>());

        let outcome = table.maintenance_pass(0).unwrap();
        assert!(outcome.flushed, "the pending batch must be flushed");
        assert_eq!(table.row_count(), 100, "maintenance must not lose rows");
        assert_eq!(table.scan(0, i64::MAX, &[], None).unwrap().len(), 100);
    }

    #[test]
    fn test_maintenance_is_idempotent_on_an_empty_table() {
        let dir = tempfile::tempdir().unwrap();
        let table = Table::new(test_meta(), dir.path());

        let first = table.maintenance_pass(0).unwrap();
        let second = table.maintenance_pass(0).unwrap();
        assert!(!first.flushed, "nothing to flush");
        assert!(!second.flushed);
        assert_eq!(table.segment_count(), 0);
    }

    #[test]
    fn test_maintenance_purges_expired_segments() {
        let dir = tempfile::tempdir().unwrap();
        let mut m = test_meta();
        m.retention_days = 1;
        let table = Table::new(m, dir.path());

        let day = 24 * 60 * 60 * 1000i64;
        let now = 1_700_000_000_000i64;
        table.insert_batch(&(0..10).map(|i| row(now - 5 * day + i, i as f64)).collect::<Vec<_>>());
        table.flush().unwrap();
        assert_eq!(table.segment_count(), 1);

        let outcome = table.maintenance_pass(now).unwrap();
        assert_eq!(outcome.segments_purged, 1, "data older than retention goes");
        assert_eq!(table.segment_count(), 0);
        assert_eq!(table.row_count(), 0);
    }

    #[test]
    fn test_maintenance_keeps_fresh_segments() {
        let dir = tempfile::tempdir().unwrap();
        let mut m = test_meta();
        m.retention_days = 7;
        let table = Table::new(m, dir.path());

        let now = 1_700_000_000_000i64;
        table.insert_batch(&(0..10).map(|i| row(now + i, i as f64)).collect::<Vec<_>>());
        table.flush().unwrap();

        let outcome = table.maintenance_pass(now).unwrap();
        assert_eq!(outcome.segments_purged, 0);
        assert_eq!(table.segment_count(), 1);
    }

    #[test]
    fn test_maintenance_can_compact_without_a_flush() {
        let dir = tempfile::tempdir().unwrap();
        let table = Table::new(test_meta(), dir.path());

        // Each flush appends an L0 segment, so the count climbs monotonically
        // until the threshold trips. That point is where a pass should merge
        // with nothing left in the memtable to flush.
        let mut compacted_without_flush = false;
        for round in 0..6i64 {
            table.insert_batch(
                &(0..10)
                    .map(|i| row(round * 100 + i, i as f64))
                    .collect::<Vec<_>>(),
            );
            table.flush().unwrap();

            let outcome = table.maintenance_pass(0).unwrap();
            if outcome.compacted && !outcome.flushed {
                compacted_without_flush = true;
            }
        }

        assert!(
            compacted_without_flush,
            "maintenance never compacted with an empty memtable"
        );
        assert_eq!(table.row_count(), 60);
    }

    /// A flush must never checkpoint a sequence whose rows it did not write.
    ///
    /// The sequence used to be read after the memtable snapshot, so a batch that
    /// logged in that window was covered by the checkpoint while still sitting
    /// only in the memtable. The segment would not contain it and the log would
    /// no longer replay it, so a restart silently lost those rows. Writes racing
    /// a flush is the normal case, not an exotic one, so this is a real test
    /// rather than a synthetic edge case.
    #[test]
    fn test_concurrent_writes_are_not_lost_across_flush() {
        let dir = tempfile::tempdir().unwrap();
        let table = Arc::new(Table::new(test_meta(), dir.path()));

        let per_batch = 500usize;
        let batches = 200usize;

        let writer = {
            let table = Arc::clone(&table);
            std::thread::spawn(move || {
                for b in 0..batches {
                    let rows: Vec<(i64, Vec<Option<Vec<u8>>>)> = (0..per_batch)
                        .map(|k| {
                            let i = (b * per_batch + k) as i64;
                            row(i, i as f64)
                        })
                        .collect();
                    table.insert_batch(&rows);
                }
            })
        };

        // Flush repeatedly while the writer runs, so flushes land in the middle
        // of logged-but-unflushed batches.
        let flusher = {
            let table = Arc::clone(&table);
            std::thread::spawn(move || {
                for _ in 0..40 {
                    table.flush().unwrap();
                    std::thread::yield_now();
                }
                table.flush().unwrap();
            })
        };

        writer.join().unwrap();
        flusher.join().unwrap();
        table.flush().unwrap();

        let expected = (per_batch * batches) as u64;
        assert_eq!(table.row_count(), expected, "row count before reopen");

        // Reopen: the checkpoint must not have claimed any row the segments lack.
        drop(table);
        let reopened = Table::new(test_meta(), dir.path());
        assert_eq!(
            reopened.row_count(),
            expected,
            "rows were lost across a restart racing flush"
        );

        let rows = reopened.scan(i64::MIN, i64::MAX, &[], None).unwrap();
        assert_eq!(rows.len() as u64, expected);
        let timestamps: std::collections::BTreeSet<i64> = rows.iter().map(|r| r.0).collect();
        assert_eq!(
            timestamps.len() as u64,
            expected,
            "duplicate or missing timestamps after reopen"
        );
    }

    /// A batch that cannot be logged must not be buffered.
    ///
    /// The old code logged the batch, logged the error, and then buffered the
    /// rows anyway, so a caller saw a write succeed that no crash could recover.
    #[test]
    fn test_unlogged_batch_is_not_buffered() {
        let dir = tempfile::tempdir().unwrap();
        let table = Table::new(test_meta(), dir.path());
        table.insert(1, vec![Some(1.0f64.to_le_bytes().to_vec())]);
        assert_eq!(table.row_count(), 1);

        table
            .fail_next_wal_append
            .store(true, Ordering::Relaxed);
        let rows: Vec<(i64, Vec<Option<Vec<u8>>>)> = vec![row(2, 2.0), row(3, 3.0)];
        let accepted = table.accept_rows(&rows);

        assert_eq!(accepted, 0, "a batch that could not be logged was accepted");
        assert_eq!(
            table.row_count(),
            1,
            "rows that were never logged were buffered anyway"
        );
        assert!(
            table.scan(i64::MIN, i64::MAX, &[], None).unwrap().len() == 1,
            "unlogged rows are visible to reads"
        );

        // The sequence must not have been consumed, so a retry is logged and
        // read back normally.
        let accepted = table.accept_rows(&rows);
        assert_eq!(accepted, 2, "the batch should be accepted once logging works");
        assert_eq!(table.row_count(), 3);

        table.flush().unwrap();
        drop(table);
        let reopened = Table::new(test_meta(), dir.path());
        assert_eq!(reopened.row_count(), 3, "the retried batch was not durable");
    }

    #[test]
    fn test_flush_auto_compacts_past_the_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let table = Table::new(test_meta(), dir.path());

        // The default threshold is 4, so repeated flushes must eventually merge
        // without anyone calling `compact` explicitly.
        for round in 0..12i64 {
            table.insert_batch(
                &(0..20)
                    .map(|i| row(round * 100 + i, i as f64))
                    .collect::<Vec<_>>(),
            );
            table.flush().unwrap();
        }

        assert!(table.compaction_count() >= 1, "auto-compaction never ran");
        assert!(
            table.segment_count() < 12,
            "L0 grew to {} segments; compaction is not keeping up",
            table.segment_count()
        );
        assert_eq!(table.row_count(), 240, "compaction must not lose rows");
    }

    #[test]
    fn test_background_maintenance_runs_and_stops() {
        let dir = tempfile::tempdir().unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();

        // Retention is disabled so the only thing this test can observe is the
        // flush. With retention on, the pass also purges segments older than the
        // policy window, and these timestamps are from 1970, so the segment
        // written above would be deleted again in the same pass.
        let mut meta = test_meta();
        meta.retention_days = 0;

        // `start_maintenance` spawns onto the ambient runtime, so the runtime has
        // to exist before the call rather than being created afterwards.
        runtime.block_on(async {
            let table = Arc::new(Table::new(meta, dir.path()));
            table.insert_batch(&(0..50).map(|i| row(1000 + i, i as f64)).collect::<Vec<_>>());

            let handle = Table::start_maintenance(Arc::clone(&table), 10);
            for _ in 0..100 {
                if table.segment_count() > 0 {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            handle.abort();

            assert!(table.segment_count() >= 1, "background flush did not run");
            assert_eq!(table.row_count(), 50, "flush must not lose rows");
            assert_eq!(table.metrics().flushes, 1);
        });
    }

    #[test]
    fn test_compaction_threshold_can_be_raised() {
        let dir = tempfile::tempdir().unwrap();
        let mut table = Table::new(test_meta(), dir.path());
        assert_eq!(table.compaction_threshold(), 4);

        table.set_compaction_threshold(10);
        assert_eq!(table.compaction_threshold(), 10);

        // Below 2 is meaningless for a merge and is raised.
        table.set_compaction_threshold(1);
        assert_eq!(table.compaction_threshold(), 2);
    }

    /// Changing the block size must not change what a round trip returns.
    ///
    /// The block size is a storage decision, not a semantic one, so a table
    /// written in 500-row blocks has to read back identically to the same rows
    /// written in one block.
    #[test]
    fn test_block_size_does_not_change_reads() {
        let mut reference: Option<Vec<(i64, Vec<Option<Vec<u8>>>)>> = None;

        for rows_per_block in [1usize, 7, 512, 16_384, 100_000] {
            let dir = tempfile::tempdir().unwrap();
            let mut table = Table::new(test_meta(), dir.path());
            table.set_rows_per_block(rows_per_block);
            assert_eq!(table.rows_per_block(), rows_per_block);

            let rows: Vec<(i64, Vec<Option<Vec<u8>>>)> =
                (0..2_000i64).map(|i| row(i, i as f64 * 1.5)).collect();
            table.insert_batch(&rows);
            table.flush().unwrap();

            let read = table.scan(i64::MIN, i64::MAX, &[], None).unwrap();
            assert_eq!(read.len(), 2_000, "rows_per_block={}", rows_per_block);
            for (i, (ts, values)) in read.iter().enumerate() {
                assert_eq!(*ts, i as i64);
                let v = f64::from_le_bytes(values[1].as_ref().unwrap()[..8].try_into().unwrap());
                assert_eq!(v, i as f64 * 1.5, "rows_per_block={} row={}", rows_per_block, i);
            }

            // Reopening must read the same blocks back the same way.
            drop(table);
            let reopened = Table::new(test_meta(), dir.path());
            let after = reopened.scan(i64::MIN, i64::MAX, &[], None).unwrap();
            assert_eq!(after, read, "reopen differs at rows_per_block={}", rows_per_block);

            match &reference {
                None => reference = Some(after),
                Some(first) => assert_eq!(&after, first, "block size changed the data"),
            }
        }
    }

    /// A reused segment reader must never outlive the file it describes.
    ///
    /// Readers are cached across queries, so the cache has to notice when a
    /// file is replaced or deleted. Getting this wrong would not fail loudly --
    /// it would serve another segment's rows under the right timestamps.
    #[test]
    fn test_reader_cache_never_serves_a_stale_segment() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seg.tsb");

        let write = |p: &Path, ts_base: i64, n: usize| {
            let ts: Vec<i64> = (0..n as i64).map(|i| ts_base + i).collect();
            let mut w = SegmentWriter::new(
                p,
                1,
                TsUnit::Millis,
                vec![DataType::Timestamp, DataType::Float64],
                CompressionLevel::Off,
            );
            w.push_columns(
                &[
                    ColumnData::Timestamp(ts.iter().map(|t| Some(*t)).collect()),
                    ColumnData::Float64(vec![Some(1.0); n]),
                ],
                n as u32,
                ts_base,
                ts_base + n as i64 - 1,
            )
            .unwrap();
            w.finish().unwrap();
        };

        write(&path, 1000, 10);
        let first = std::fs::read(&path).unwrap();

        let mut cache = ReaderCache::default();
        let a = cache.borrow(&path).unwrap();
        assert_eq!(a.reader.min_ts(), 1000);

        // Same file, same length, so the cached reader is legitimately reused.
        let b = cache.borrow(&path).unwrap();
        assert_eq!(b.reader.min_ts(), 1000);
        assert!(Arc::ptr_eq(&a, &b), "unchanged file should reuse the reader");

        // Replace it with different data. The cache must not hand back the old
        // reader, even though the path is identical.
        write(&path, 5000, 10);
        let second = std::fs::read(&path).unwrap();
        assert_ne!(first, second, "the rewrite should differ on disk");

        let c = cache.borrow(&path).unwrap();
        assert_eq!(
            c.reader.min_ts(),
            5000,
            "cache served a reader for the file that used to be there"
        );

        // And a missing file is an error rather than a stale hit.
        std::fs::remove_file(&path).unwrap();
        assert!(
            cache.borrow(&path).is_err(),
            "a deleted segment should not resolve from the cache"
        );
    }

    #[test]
    fn test_block_size_below_one_is_raised() {        let dir = tempfile::tempdir().unwrap();
        let mut table = Table::new(test_meta(), dir.path());
        table.set_rows_per_block(0);
        assert_eq!(table.rows_per_block(), 1, "a block must hold at least one row");

        table.insert(1, vec![Some(1.0f64.to_le_bytes().to_vec())]);
        table.flush().unwrap();
        assert_eq!(table.row_count(), 1);
    }

    #[test]
    fn test_values_survive_reopen_byte_for_byte() {
        let dir = tempfile::tempdir().unwrap();
        let inputs: Vec<f64> = (0..100).map(|i| i as f64 * 1.23456789).collect();
        {
            let table = Table::new(test_meta(), dir.path());
            table.insert_batch(&inputs.iter().enumerate().map(|(i, v)| row(1000 + i as i64, *v)).collect::<Vec<_>>());
            table.flush().unwrap();
        }

        let reopened = Table::new(test_meta(), dir.path());
        let rows = reopened.scan(0, i64::MAX, &[], None).unwrap();
        assert_eq!(rows.len(), inputs.len());

        for (i, want) in inputs.iter().enumerate() {
            let bytes = rows[i].1[1].as_ref().unwrap();
            let got = f64::from_le_bytes(bytes[..8].try_into().unwrap());
            assert_eq!(&got, want, "value {} changed across the round trip", i);
        }
    }
}
