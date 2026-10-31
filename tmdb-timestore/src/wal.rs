//! Write-ahead log for the storage layer.
//!
//! # Why this is not the transaction WAL
//!
//! `tmdb-txn` records committed transactions so the MVCC layer can undo or
//! redo them. That is not enough here: a row that has been acknowledged to the
//! client lives in a memtable until `Table::flush` runs, and if the process dies
//! before then the row is gone. This WAL closes that gap by logging rows when
//! they are accepted, not when they are flushed.
//!
//! # Records
//!
//! ```text
//! magic:u32 = 0x57414C31 ("WAL1")   -- start of file only
//! per record:
//!   record_len:u32        bytes of payload that follow
//!   crc32:u32             CRC32 of payload, little-endian
//!   payload
//! ```
//!
//! Two payload kinds:
//!
//! ```text
//! kind:u8 = 0x01  row batch
//!   table_id:u64
//!   seq:u64
//!   rows * [ts:i64][col_count:u32][col_count * (len:u32 + bytes | 0xFFFFFFFF for NULL)]
//!
//! kind:u8 = 0x02  flush checkpoint
//!   table_id:u64
//!   upto_seq:u64           every row of this table with seq <= upto_seq is in a segment
//! ```
//!
//! # Recovery
//!
//! A checkpoint is written only after the segment holding those rows is fsynced,
//! so on startup every row batch newer than the last checkpoint is still missing
//! from disk and is replayed. Rows already covered by a checkpoint are skipped,
//! which is what stops a restart from duplicating data.
//!
//! Reading stops at the first truncated record or CRC mismatch — exactly what a
//! crash mid-write leaves behind. Everything before the damage is usable and the
//! torn record is discarded; those rows were never acknowledged as durable.
//!
//! # Durability tiers
//!
//! `append` puts the record in the OS page cache, so a *process* crash loses
//! nothing. `append_durable` additionally fsyncs, which is what protects against
//! machine loss and is reserved for flushes. Syncing every row would put an
//! fsync on the insert path and destroy write throughput.

use std::collections::HashMap;
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};

use anyhow::Context;

/// File magic, `"WAL1"` little-endian.
pub const WAL_MAGIC: u32 = 0x314C_4157;
/// Bytes of header at the start of the file.
pub const WAL_HEADER_LEN: usize = 4;
/// Marks a NULL value in a row.
const NULL_MARKER: u32 = u32::MAX;

const KIND_ROW_BATCH: u8 = 0x01;
const KIND_CHECKPOINT: u8 = 0x02;

/// Where one record sits in the log, and what it claims.
///
/// Describing a record without decoding its rows is what makes compaction cheap.
/// The first 17 bytes of every payload carry the kind, the table and the
/// sequence, so a scan never has to turn 96 MB of rows back into
/// `Vec<Option<Vec<u8>>>` just to learn that it is about to delete them.
struct RecordSpan {
    /// Offset of the record's 8-byte length-and-CRC header.
    offset: u64,
    /// Payload length, excluding the 8-byte header.
    len: u64,
    table_id: u64,
    seq: u64,
    is_checkpoint: bool,
}

/// The result of one pass over the log.
struct Scan {
    spans: Vec<RecordSpan>,
    /// Highest checkpointed sequence per table.
    checkpoints: HashMap<u64, u64>,
    /// Offset just past the last intact record.
    clean_end: u64,
    file_len: u64,
    stopped: WalStopReason,
}

/// A row batch read back from the log.
#[derive(Debug, Clone, PartialEq)]
pub struct WalRecord {
    pub table_id: u64,
    pub seq: u64,
    pub rows: Vec<(i64, Vec<Option<Vec<u8>>>)>,
}

impl WalRecord {
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }
}

/// A durability boundary read back from the log.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WalCheckpoint {
    pub table_id: u64,
    pub upto_seq: u64,
}

/// Why recovery stopped early.
#[derive(Debug, Clone, PartialEq)]
pub enum WalStopReason {
    /// Clean end of file.
    Eof,
    /// The final record was cut short; its rows were never durable.
    TruncatedTail { offset: u64 },
    /// The record's CRC did not match, so it was not written completely.
    ChecksumMismatch { offset: u64 },
}

impl std::fmt::Display for WalStopReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalStopReason::Eof => write!(f, "clean end of file"),
            WalStopReason::TruncatedTail { offset } => {
                write!(f, "truncated record at offset {}", offset)
            }
            WalStopReason::ChecksumMismatch { offset } => {
                write!(f, "checksum mismatch at offset {}", offset)
            }
        }
    }
}

/// Outcome of a recovery pass.
#[derive(Debug, Clone)]
pub struct Recovery {
    /// Row batches not yet covered by a checkpoint, in write order.
    pub pending: Vec<WalRecord>,
    /// Highest checkpointed sequence per table id.
    pub checkpoints: HashMap<u64, u64>,
    /// Why scanning stopped.
    pub stopped_because: WalStopReason,
    /// Bytes of the log that were usable.
    pub bytes_recovered: u64,
    /// Records skipped because a checkpoint already made them durable.
    pub records_already_durable: usize,
}

impl Recovery {
    /// Rows still missing from disk for one table.
    pub fn pending_rows(&self, table_id: u64) -> usize {
        self.pending
            .iter()
            .filter(|r| r.table_id == table_id)
            .map(|r| r.row_count())
            .sum()
    }

    /// Replayable batches for one table.
    pub fn pending_for(&self, table_id: u64) -> Vec<&WalRecord> {
        self.pending.iter().filter(|r| r.table_id == table_id).collect()
    }

    /// Total rows still missing from disk across all tables.
    pub fn total_pending_rows(&self) -> usize {
        self.pending.iter().map(|r| r.row_count()).sum()
    }

    /// True when nothing is waiting to be replayed.
    pub fn is_clean(&self) -> bool {
        self.pending.is_empty()
    }
}

/// Append-only write-ahead log.
pub struct Wal {
    path: PathBuf,
    /// The open log, if any, plus the lock that serialises every mutation.
    ///
    /// These are one field rather than a lock beside a cache because the lock is
    /// what makes the cached handle safe: appends, compaction and truncation all
    /// exclude each other, so a handle can never be in use while something is
    /// replacing the file underneath it.
    ///
    /// Holding the handle open matters because this is the hottest path in the
    /// system. Reopening per batch cost a stat, an open, three writes and a close
    /// on every append, all to reach the same file.
    state: std::sync::Mutex<Option<OpenWal>>,
}

/// An open log, kept open between appends.
struct OpenWal {
    file: std::fs::File,
    /// Whether the magic has been written, tracked instead of stat-ing the file.
    header_written: bool,
    /// Length of the log, tracked so `Wal::size` needs no syscall either.
    len: u64,
}

impl Wal {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            state: std::sync::Mutex::new(None),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append a row batch and leave it in the page cache.
    ///
    /// Survives a process crash; does not guarantee survival of a machine crash.
    pub fn append(&self, table_id: u64, seq: u64, rows: &[(i64, Vec<Option<Vec<u8>>>)]) -> anyhow::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let payload = encode_rows(table_id, seq, rows);
        self.write_record(&payload, false)
    }

    /// Append a row batch and fsync it.
    pub fn append_durable(
        &self,
        table_id: u64,
        seq: u64,
        rows: &[(i64, Vec<Option<Vec<u8>>>)],
    ) -> anyhow::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let payload = encode_rows(table_id, seq, rows);
        self.write_record(&payload, true)
    }

    /// Record that every row of `table_id` up to `upto_seq` is now in a segment.
    ///
    /// Fsynced, because the guarantee it asserts depends on a segment that was
    /// itself fsynced first.
    pub fn checkpoint(&self, table_id: u64, upto_seq: u64) -> anyhow::Result<()> {
        let mut payload = Vec::with_capacity(17);
        payload.push(KIND_CHECKPOINT);
        payload.extend_from_slice(&table_id.to_le_bytes());
        payload.extend_from_slice(&upto_seq.to_le_bytes());
        self.write_record(&payload, true)
    }

    fn write_record(&self, payload: &[u8], durable: bool) -> anyhow::Result<()> {
        // Taking the cache slot doubles as taking the lock, so there is one
        // critical section rather than a lock and a cache that can disagree.
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if guard.is_none() {
            if let Some(parent) = self.path.parent() {
                std::fs::create_dir_all(parent).ok();
            }
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)
                .with_context(|| format!("open WAL {}", self.path.display()))?;
            // An existing log was written by an earlier process, so its header is
            // already there. Only a log we are creating needs one.
            let len = file.metadata().map(|m| m.len()).unwrap_or(0);
            *guard = Some(OpenWal {
                file,
                header_written: len >= WAL_HEADER_LEN as u64,
                len,
            });
        }

        let open = guard.as_mut().expect("just opened");
        if !open.header_written {
            open.file
                .write_all(&WAL_MAGIC.to_le_bytes())
                .with_context(|| format!("write WAL header {}", self.path.display()))?;
            open.header_written = true;
            open.len += WAL_HEADER_LEN as u64;
        }

        // Length, checksum and payload go out in one write. Three separate
        // writes meant three syscalls and three chances to be interrupted
        // mid-record for the cost of a memcpy.
        let mut framed = Vec::with_capacity(payload.len() + 8);
        framed.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        framed.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
        framed.extend_from_slice(payload);

        open.file
            .write_all(&framed)
            .with_context(|| format!("append to WAL {}", self.path.display()))?;
        open.len += framed.len() as u64;

        if durable {
            open.file
                .sync_data()
                .with_context(|| format!("fsync WAL {}", self.path.display()))?;
        }
        Ok(())
    }

    /// Read every intact record and separate pending rows from durable ones.
    ///
    /// Never fails on a damaged tail — that is the expected state after a crash,
    /// not an error.
    pub fn recover(&self) -> anyhow::Result<Recovery> {
        let mut data = Vec::new();
        if self.path.exists() {
            let mut f = std::fs::File::open(&self.path)
                .with_context(|| format!("open WAL {}", self.path.display()))?;
            f.read_to_end(&mut data)?;
        }

        let mut pending: Vec<WalRecord> = Vec::new();
        let mut checkpoints: HashMap<u64, u64> = HashMap::new();
        let mut records_already_durable = 0usize;
        let mut bytes_recovered = 0usize;

        if data.len() < WAL_HEADER_LEN {
            return Ok(Recovery {
                pending,
                checkpoints,
                stopped_because: WalStopReason::Eof,
                bytes_recovered: 0,
                records_already_durable,
            });
        }

        let magic = u32::from_le_bytes(data[0..4].try_into()?);
        if magic != WAL_MAGIC {
            anyhow::bail!("not a WAL file: magic {:#x}, expected {:#x}", magic, WAL_MAGIC);
        }

        let mut pos = WAL_HEADER_LEN;
        // Assigned on every exit path below; the loop always breaks before the
        // value is read, so it is never observed unset.
        #[allow(unused_assignments)]
        let mut stopped = WalStopReason::Eof;

        loop {
            let record_start = pos;
            if pos == data.len() {
                stopped = WalStopReason::Eof;
                break;
            }
            if pos + 8 > data.len() {
                stopped = WalStopReason::TruncatedTail { offset: record_start as u64 };
                break;
            }

            let len = u32::from_le_bytes(data[pos..pos + 4].try_into()?) as usize;
            let expected_crc = u32::from_le_bytes(data[pos + 4..pos + 8].try_into()?);
            pos += 8;

            if pos + len > data.len() {
                stopped = WalStopReason::TruncatedTail { offset: record_start as u64 };
                break;
            }

            let payload = &data[pos..pos + len];
            if crc32fast::hash(payload) != expected_crc {
                stopped = WalStopReason::ChecksumMismatch { offset: record_start as u64 };
                break;
            }

            match decode_payload(payload) {
                Ok(Item::Rows(record)) => {
                    bytes_recovered += len + 8;
                    pending.push(record);
                }
                Ok(Item::Checkpoint(cp)) => {
                    bytes_recovered += len + 8;
                    // Everything at or below this sequence is now in a segment.
                    let slot = checkpoints.entry(cp.table_id).or_insert(cp.upto_seq);
                    *slot = (*slot).max(cp.upto_seq);
                    records_already_durable += pending
                        .iter()
                        .filter(|r| r.table_id == cp.table_id && r.seq <= cp.upto_seq)
                        .count();
                }
                Err(e) => {
                    // A structurally invalid but CRC-valid record means the
                    // writer and reader disagree; stop rather than guess.
                    tracing::error!("WAL record at {} could not be decoded: {}", record_start, e);
                    stopped = WalStopReason::ChecksumMismatch { offset: record_start as u64 };
                    break;
                }
            }

            pos += len;
        }

        // Drop what the checkpoints already made durable. A table with no checkpoint
        // has nothing durable, so every batch must replay — comparing against a
        // default of 0 would wrongly drop batch 0.
        pending.retain(|r| match checkpoints.get(&r.table_id) {
            Some(upto) => r.seq > *upto,
            None => true,
        });

        Ok(Recovery {
            pending,
            checkpoints,
            stopped_because: stopped,
            bytes_recovered: bytes_recovered as u64,
            records_already_durable,
        })
    }

    /// Rewrite the log keeping only batches that are still pending.
    ///
    /// Checkpointed rows are already in segments, and the segments on disk imply
    /// the checkpoints, so both are dropped. Returns the number of batches
    /// discarded.
    ///
    /// The rewrite is atomic with respect to appends: the whole scan, copy and
    /// rename happen under [`Wal::state`], so a concurrent [`Wal::append`] either
    /// lands before the scan and is carried over, or waits and lands afterwards.
    /// It cannot be lost in between.
    pub fn compact(&self) -> anyhow::Result<usize> {
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let scan = self.scan()?;
        let dropped = self.rewrite_from_scan(&scan, None)?;
        // The rename just replaced the file, so the handle points at the old,
        // now unlinked inode. Writing to it would report success and lose the
        // record.
        *guard = None;
        Ok(dropped)
    }

    /// Walk the log once, recording every intact record and the checkpoints.
    ///
    /// The record payload is read into one reused buffer and checksummed, because
    /// a bad checksum means the records after it cannot be trusted. But the rows
    /// themselves are never decoded: only the 17-byte prefix is interpreted.
    fn scan(&self) -> anyhow::Result<Scan> {
        let mut spans = Vec::new();
        let mut checkpoints: HashMap<u64, u64> = HashMap::new();
        let mut clean_end = WAL_HEADER_LEN as u64;

        let file_len = std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0);
        if file_len < WAL_HEADER_LEN as u64 {
            return Ok(Scan {
                spans,
                checkpoints,
                clean_end,
                file_len,
                stopped: WalStopReason::Eof,
            });
        }

        let mut f = std::fs::File::open(&self.path)
            .with_context(|| format!("open WAL {}", self.path.display()))?;

        let mut magic = [0u8; 4];
        f.read_exact(&mut magic)?;
        let magic = u32::from_le_bytes(magic);
        if magic != WAL_MAGIC {
            anyhow::bail!(
                "not a WAL file: magic {:#x}, expected {:#x}",
                magic,
                WAL_MAGIC
            );
        }

        // One allocation, reused for every record. A batch is the unit of work
        // here, not a row, so this stays small even for a multi-gigabyte log.
        let mut payload: Vec<u8> = Vec::new();
        let mut header = [0u8; 8];
        let mut pos = WAL_HEADER_LEN as u64;
        // Assigned on every break path below; the loop always breaks before it is
        // read, so it is never observed unset.
        #[allow(unused_assignments)]
        let mut stopped = WalStopReason::Eof;

        loop {
            if pos == file_len {
                stopped = WalStopReason::Eof;
                break;
            }
            if pos + 8 > file_len {
                stopped = WalStopReason::TruncatedTail { offset: pos };
                break;
            }
            f.read_exact(&mut header)?;
            let len = u32::from_le_bytes(header[0..4].try_into()?) as u64;
            let expected_crc = u32::from_le_bytes(header[4..8].try_into()?);

            if pos + 8 + len > file_len {
                stopped = WalStopReason::TruncatedTail { offset: pos };
                break;
            }
            if len < 17 {
                tracing::error!("WAL record at {} is too short: {} bytes", pos, len);
                stopped = WalStopReason::ChecksumMismatch { offset: pos };
                break;
            }

            payload.clear();
            payload.resize(len as usize, 0);
            f.read_exact(&mut payload)?;
            if crc32fast::hash(&payload) != expected_crc {
                stopped = WalStopReason::ChecksumMismatch { offset: pos };
                break;
            }

            let kind = payload[0];
            let table_id = u64::from_le_bytes(payload[1..9].try_into()?);
            let seq = u64::from_le_bytes(payload[9..17].try_into()?);
            match kind {
                KIND_ROW_BATCH => {}
                KIND_CHECKPOINT => {
                    let slot = checkpoints.entry(table_id).or_insert(seq);
                    *slot = (*slot).max(seq);
                }
                other => {
                    tracing::error!("WAL record at {} has unknown kind {:#x}", pos, other);
                    stopped = WalStopReason::ChecksumMismatch { offset: pos };
                    break;
                }
            }

            pos += 8 + len;
            clean_end = pos;
            spans.push(RecordSpan {
                offset: pos - 8 - len,
                len,
                table_id,
                seq,
                is_checkpoint: kind == KIND_CHECKPOINT,
            });
        }

        Ok(Scan {
            spans,
            checkpoints,
            clean_end,
            file_len,
            stopped,
        })
    }

    /// Rewrite the log from a scan, copying the records `keep` accepts verbatim.
    ///
    /// Kept records are copied as raw bytes rather than decoded and re-encoded.
    /// The payload is already in the on-disk format, so re-encoding it could only
    /// introduce a difference, and doing so was most of the cost of a flush.
    ///
    /// Returns how many records were dropped.
    fn rewrite_from_scan(&self, scan: &Scan, exclude_table: Option<u64>) -> anyhow::Result<usize> {
        let keep = |s: &RecordSpan| -> bool {
            if s.is_checkpoint {
                // Checkpoints describe the log itself. Once the log has been
                // rewritten, the segments on disk already imply them, so keeping
                // one would only suppress a replay that is no longer needed.
                return false;
            }
            if Some(s.table_id) == exclude_table {
                return false;
            }
            // No checkpoint means nothing is durable, so every batch must replay.
            // Comparing against a default of 0 would wrongly drop batch 0.
            match scan.checkpoints.get(&s.table_id) {
                Some(upto) => s.seq > *upto,
                None => true,
            }
        };

        let dropped = scan.spans.iter().filter(|s| !keep(s)).count();
        let has_trailing_garbage = scan.clean_end < scan.file_len;
        if dropped == 0 && !has_trailing_garbage {
            return Ok(0);
        }

        if scan.clean_end < scan.file_len {
            // The rewrite below copies only intact records, so the damaged tail
            // is dropped here. That is the same recovery a restart would apply,
            // but it happens without anybody asking, so say so.
            tracing::warn!(
                "compacting WAL {}: discarding {} bytes after offset {} ({:?})",
                self.path.display(),
                scan.file_len - scan.clean_end,
                scan.clean_end,
                scan.stopped
            );
        }

        let tmp = self.path.with_extension("wal.tmp");
        {
            let mut src = std::fs::File::open(&self.path)
                .with_context(|| format!("open WAL {}", self.path.display()))?;
            let mut out = std::fs::File::create(&tmp)?;
            out.write_all(&WAL_MAGIC.to_le_bytes())?;

            let mut buf = vec![0u8; 1 << 20];
            for s in scan.spans.iter().filter(|s| keep(s)) {
                // The header is copied as well, so the record lands byte-identical
                // to how it was written.
                let mut remaining = s.len + 8;
                let mut at = s.offset;
                while remaining > 0 {
                    let want = remaining.min(buf.len() as u64) as usize;
                    src.seek(std::io::SeekFrom::Start(at))?;
                    src.read_exact(&mut buf[..want])?;
                    out.write_all(&buf[..want])?;
                    at += want as u64;
                    remaining -= want as u64;
                }
            }
            out.sync_all()?;
        }
        std::fs::rename(&tmp, &self.path)?;
        Ok(dropped)
    }

    /// Drop everything for `table_id`, keeping other tables' records.
    pub fn truncate_table(&self, table_id: u64) -> anyhow::Result<usize> {
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let scan = self.scan()?;
        let dropped = self.rewrite_from_scan(&scan, Some(table_id))?;
        *guard = None;
        Ok(dropped)
    }

    /// Remove the log file entirely.
    pub fn reset(&self) -> anyhow::Result<()> {
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        *guard = None;
        if self.path.exists() {
            std::fs::remove_file(&self.path)?;
        }
        Ok(())
    }

    /// Current size in bytes, including the header.
    pub fn size(&self) -> u64 {
        // The open handle knows the length, so a hot caller needs no syscall.
        // Falls back to the file when nothing has been opened yet.
        if let Ok(guard) = self.state.lock() {
            if let Some(open) = guard.as_ref() {
                return open.len;
            }
        }
        std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0)
    }

    /// Flush buffered records to the device.
    ///
    /// Must be called before a checkpoint asserts that a row survives a machine
    /// crash; [`Wal::append`] only reaches the page cache.
    pub fn sync(&self) -> anyhow::Result<()> {
        let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let open = match guard.as_ref() {
            // Nothing has been appended through this handle, so there is nothing
            // of ours still sitting in the page cache.
            None => return Ok(()),
            Some(open) => open,
        };
        open.file
            .sync_data()
            .with_context(|| format!("fsync WAL {}", self.path.display()))?;
        Ok(())
    }

    /// Cut the file back to the last intact record.
    ///
    /// Returns the new length when it changed, or `None` when the log was
    /// already intact.
    pub fn truncate_to_last_good(&self) -> anyhow::Result<Option<u64>> {
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let recovery = self.recover()?;
        match recovery.stopped_because {
            WalStopReason::Eof => Ok(None),
            _ => {
                let good_len = WAL_HEADER_LEN as u64 + recovery.bytes_recovered;
                let f = std::fs::File::options().write(true).open(&self.path)?;
                f.set_len(good_len)?;
                f.sync_all()?;
                // The length changed behind the cached handle.
                *guard = None;
                Ok(Some(good_len))
            }
        }
    }
}

enum Item {
    Rows(WalRecord),
    Checkpoint(WalCheckpoint),
}

fn encode_rows(table_id: u64, seq: u64, rows: &[(i64, Vec<Option<Vec<u8>>>)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(32 + rows.len() * 32);

    out.push(KIND_ROW_BATCH);
    out.extend_from_slice(&table_id.to_le_bytes());
    out.extend_from_slice(&seq.to_le_bytes());

    for (ts, values) in rows {
        out.extend_from_slice(&ts.to_le_bytes());
        out.extend_from_slice(&(values.len() as u32).to_le_bytes());
        for v in values {
            match v {
                Some(bytes) => {
                    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                    out.extend_from_slice(bytes);
                }
                None => out.extend_from_slice(&NULL_MARKER.to_le_bytes()),
            }
        }
    }

    out
}

fn decode_payload(payload: &[u8]) -> anyhow::Result<Item> {
    if payload.len() < 9 {
        anyhow::bail!("payload too small: {} bytes", payload.len());
    }
    let kind = payload[0];
    let table_id = u64::from_le_bytes(payload[1..9].try_into()?);

    match kind {
        KIND_CHECKPOINT => {
            if payload.len() < 17 {
                anyhow::bail!("checkpoint truncated at {} bytes", payload.len());
            }
            let upto_seq = u64::from_le_bytes(payload[9..17].try_into()?);
            Ok(Item::Checkpoint(WalCheckpoint { table_id, upto_seq }))
        }
        KIND_ROW_BATCH => {
            if payload.len() < 17 {
                anyhow::bail!("row batch truncated at {} bytes", payload.len());
            }
            let seq = u64::from_le_bytes(payload[9..17].try_into()?);

            let mut pos = 17;
            let mut rows = Vec::new();
            loop {
                if pos == payload.len() {
                    break;
                }
                if pos + 12 > payload.len() {
                    anyhow::bail!("row header truncated at {}", pos);
                }
                let ts = i64::from_le_bytes(payload[pos..pos + 8].try_into()?);
                let col_count = u32::from_le_bytes(payload[pos + 8..pos + 12].try_into()?) as usize;
                pos += 12;

                let mut values = Vec::with_capacity(col_count);
                for _ in 0..col_count {
                    if pos + 4 > payload.len() {
                        anyhow::bail!("value length truncated at {}", pos);
                    }
                    let len = u32::from_le_bytes(payload[pos..pos + 4].try_into()?);
                    pos += 4;

                    if len == NULL_MARKER {
                        values.push(None);
                        continue;
                    }
                    if pos + len as usize > payload.len() {
                        anyhow::bail!("value body truncated at {}", pos);
                    }
                    values.push(Some(payload[pos..pos + len as usize].to_vec()));
                    pos += len as usize;
                }
                rows.push((ts, values));
            }

            Ok(Item::Rows(WalRecord { table_id, seq, rows }))
        }
        other => anyhow::bail!("unknown record kind {:#x}", other),
    }
}

/// Replayable rows for one table, keyed by table id.
pub fn recover(path: impl AsRef<Path>) -> anyhow::Result<HashMap<u64, Vec<(i64, Vec<Option<Vec<u8>>>)>>> {
    let recovery = Wal::new(path).recover()?;
    let mut out: HashMap<u64, Vec<(i64, Vec<Option<Vec<u8>>>)>> = HashMap::new();
    for record in recovery.pending {
        out.entry(record.table_id).or_default().extend(record.rows);
    }
    Ok(out)
}

/// Cut off a torn tail so the next append starts from a clean record boundary.
pub fn repair(path: impl AsRef<Path>) -> anyhow::Result<bool> {
    Ok(Wal::new(path).truncate_to_last_good()?.is_some())
}

/// Log size in bytes, for metrics.
pub fn wal_size(path: impl AsRef<Path>) -> u64 {
    Wal::new(path).size()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(n: usize, base: i64) -> Vec<(i64, Vec<Option<Vec<u8>>>)> {
        (0..n)
            .map(|i| {
                (
                    base + i as i64,
                    vec![
                        Some((base + i as i64).to_le_bytes().to_vec()),
                        Some((i as f64 * 1.5).to_le_bytes().to_vec()),
                        if i % 4 == 0 {
                            None
                        } else {
                            Some(format!("tag{}", i % 3).into_bytes())
                        },
                    ],
                )
            })
            .collect()
    }

    #[test]
    fn test_roundtrip_single_batch() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Wal::new(dir.path().join("test.wal"));

        let r = rows(100, 1000);
        wal.append(1, 0, &r).unwrap();

        let rec = wal.recover().unwrap();
        assert_eq!(rec.pending.len(), 1);
        assert_eq!(rec.pending[0].rows, r);
        assert_eq!(rec.total_pending_rows(), 100);
    }

    #[test]
    fn test_roundtrip_multiple_batches_and_sequences() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Wal::new(dir.path().join("test.wal"));

        for seq in 0..5u64 {
            wal.append(1, seq, &rows(50, 1000 + seq as i64 * 100)).unwrap();
        }

        let rec = wal.recover().unwrap();
        assert_eq!(rec.pending.len(), 5);
        for (i, r) in rec.pending.iter().enumerate() {
            assert_eq!(r.seq, i as u64);
            assert_eq!(r.row_count(), 50);
        }
    }

    #[test]
    fn test_checkpoint_marks_rows_durable() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Wal::new(dir.path().join("test.wal"));

        wal.append(1, 0, &rows(10, 100)).unwrap();
        wal.append(1, 1, &rows(10, 200)).unwrap();
        wal.checkpoint(1, 0).unwrap();
        wal.append(1, 2, &rows(10, 300)).unwrap();

        let rec = wal.recover().unwrap();
        assert_eq!(rec.pending_rows(1), 20, "only batches after the checkpoint remain");
        assert_eq!(rec.records_already_durable, 1);
        assert_eq!(rec.checkpoints[&1], 0);
    }

    #[test]
    fn test_replay_after_checkpoint_does_not_duplicate() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let wal = Wal::new(&path);

        wal.append(1, 0, &rows(10, 100)).unwrap();
        wal.append(1, 1, &rows(10, 200)).unwrap();
        wal.checkpoint(1, 1).unwrap();

        let rec = wal.recover().unwrap();
        assert!(rec.is_clean(), "everything is in a segment");
        assert_eq!(rec.total_pending_rows(), 0);
    }

    #[test]
    fn test_checkpoint_is_per_table() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Wal::new(dir.path().join("test.wal"));

        wal.append(1, 0, &rows(10, 100)).unwrap();
        wal.append(2, 0, &rows(10, 200)).unwrap();
        wal.checkpoint(1, 0).unwrap();

        let rec = wal.recover().unwrap();
        assert_eq!(rec.pending_rows(1), 0);
        assert_eq!(rec.pending_rows(2), 10, "table 2 was not flushed");
    }

    #[test]
    fn test_highest_checkpoint_wins() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Wal::new(dir.path().join("test.wal"));

        for seq in 0..5u64 {
            wal.append(1, seq, &rows(10, 100 + seq as i64 * 10)).unwrap();
        }
        wal.checkpoint(1, 2).unwrap();
        wal.checkpoint(1, 4).unwrap();

        let rec = wal.recover().unwrap();
        assert!(rec.is_clean());
    }

    #[test]
    fn test_nulls_survive() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Wal::new(dir.path().join("test.wal"));

        let r = vec![
            (1i64, vec![Some(vec![1u8]), None, Some(vec![])]),
            (2i64, vec![None, None, Some(vec![7u8, 8, 9])]),
        ];
        wal.append(1, 0, &r).unwrap();
        assert_eq!(wal.recover().unwrap().pending[0].rows, r, "NULL markers were not preserved");
    }

    #[test]
    fn test_multiple_tables_are_separated() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Wal::new(dir.path().join("test.wal"));

        wal.append(1, 0, &rows(10, 100)).unwrap();
        wal.append(2, 0, &rows(20, 200)).unwrap();
        wal.append(1, 1, &rows(30, 300)).unwrap();

        let rec = wal.recover().unwrap();
        assert_eq!(rec.pending_for(1).len(), 2);
        assert_eq!(rec.pending_for(2).len(), 1);
        assert_eq!(rec.total_pending_rows(), 60);
    }

    #[test]
    fn test_empty_log_recovers_cleanly() {
        let dir = tempfile::tempdir().unwrap();
        let rec = Wal::new(dir.path().join("missing.wal")).recover().unwrap();
        assert!(rec.is_clean());
        assert_eq!(rec.stopped_because, WalStopReason::Eof);
    }

    #[test]
    fn test_empty_batch_is_not_written() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Wal::new(dir.path().join("test.wal"));
        wal.append(1, 0, &[]).unwrap();
        assert!(!wal.path().exists(), "an empty batch must not create a record");
    }

    #[test]
    fn test_crc_detects_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let wal = Wal::new(&path);

        wal.append(1, 0, &rows(20, 100)).unwrap();
        wal.append(1, 1, &rows(20, 200)).unwrap();

        let mut data = std::fs::read(&path).unwrap();
        data[WAL_HEADER_LEN + 12] ^= 0xFF;
        std::fs::write(&path, &data).unwrap();

        let rec = wal.recover().unwrap();
        assert!(rec.pending.is_empty(), "corrupt record must not be returned");
        assert!(matches!(rec.stopped_because, WalStopReason::ChecksumMismatch { .. }));
    }

    #[test]
    fn test_truncated_tail_discards_only_the_last_record() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let wal = Wal::new(&path);

        wal.append(1, 0, &rows(20, 100)).unwrap();
        wal.append(1, 1, &rows(20, 200)).unwrap();

        let data = std::fs::read(&path).unwrap();
        std::fs::write(&path, &data[..data.len() - 10]).unwrap();

        let rec = wal.recover().unwrap();
        assert_eq!(rec.pending.len(), 1, "the intact record must survive");
        assert_eq!(rec.pending[0].seq, 0);
        assert!(matches!(rec.stopped_because, WalStopReason::TruncatedTail { .. }));
    }

    #[test]
    fn test_truncated_checkpoint_replays_previous_batch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let wal = Wal::new(&path);

        wal.append(1, 0, &rows(20, 100)).unwrap();
        wal.checkpoint(1, 0).unwrap();

        // Cut the log so the checkpoint never finished: the batch must replay.
        let data = std::fs::read(&path).unwrap();
        std::fs::write(&path, &data[..data.len() - 4]).unwrap();

        let rec = wal.recover().unwrap();
        assert_eq!(rec.pending_rows(1), 20, "unfinished checkpoint must not claim durability");
    }

    #[test]
    fn test_truncate_to_last_good_repairs_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let wal = Wal::new(&path);

        wal.append(1, 0, &rows(20, 100)).unwrap();
        wal.append(1, 1, &rows(20, 200)).unwrap();
        wal.append(1, 2, &rows(20, 300)).unwrap();

        // Damage the middle record: the first must survive, the rest must not.
        let mut data = std::fs::read(&path).unwrap();
        let first_len =
            u32::from_le_bytes(data[WAL_HEADER_LEN..WAL_HEADER_LEN + 4].try_into().unwrap()) as usize;
        let second_start = WAL_HEADER_LEN + 8 + first_len;
        data[second_start + 8 + 12] ^= 0xFF;
        std::fs::write(&path, &data).unwrap();

        let rec = wal.recover().unwrap();
        assert_eq!(rec.pending.len(), 1, "the intact first record survived corruption");
        assert_eq!(rec.pending[0].seq, 0);

        let good = Wal::new(&path).truncate_to_last_good().unwrap().unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), good);

        wal.append(1, 2, &rows(5, 300)).unwrap();
        let rec = wal.recover().unwrap();
        assert_eq!(rec.pending.len(), 2);
        assert_eq!(rec.stopped_because, WalStopReason::Eof);
    }

    #[test]
    fn test_wrong_magic_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("not.wal");
        std::fs::write(&path, b"junkjunkjunk").unwrap();
        assert!(Wal::new(&path).recover().is_err());
    }

    #[test]
    fn test_truncate_table_keeps_other_tables() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Wal::new(dir.path().join("test.wal"));

        wal.append(1, 0, &rows(10, 100)).unwrap();
        wal.append(2, 0, &rows(10, 200)).unwrap();
        wal.append(1, 1, &rows(10, 300)).unwrap();

        assert_eq!(wal.truncate_table(1).unwrap(), 2);

        let rec = wal.recover().unwrap();
        assert_eq!(rec.pending_rows(1), 0, "table 1 should be gone");
        assert_eq!(rec.pending_rows(2), 10, "table 2 must survive");
    }

    #[test]
    fn test_truncate_table_on_absent_table_is_a_noop() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Wal::new(dir.path().join("test.wal"));
        wal.append(1, 0, &rows(10, 100)).unwrap();

        assert_eq!(wal.truncate_table(99).unwrap(), 0);
        assert_eq!(wal.recover().unwrap().pending_rows(1), 10);
    }

    #[test]
    fn test_reset_removes_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Wal::new(dir.path().join("test.wal"));
        wal.append(1, 0, &rows(10, 100)).unwrap();
        assert!(wal.path().exists());
        wal.reset().unwrap();
        assert!(!wal.path().exists());
        assert_eq!(wal.size(), 0);
    }

    #[test]
    fn test_convenience_helper_returns_rows_by_table() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let wal = Wal::new(&path);
        let r = rows(7, 42);
        wal.append(3, 0, &r).unwrap();

        assert_eq!(recover(&path).unwrap()[&3], r);
        assert!(wal_size(&path) > 0);
    }

    #[test]
    fn test_large_batch_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Wal::new(dir.path().join("test.wal"));
        let r = rows(50_000, 1_000_000);
        wal.append(1, 0, &r).unwrap();
        assert_eq!(wal.recover().unwrap().pending[0].rows, r);
    }

    #[test]
    fn test_append_after_recovery_keeps_earlier_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let wal = Wal::new(&path);

        wal.append(1, 0, &rows(10, 100)).unwrap();
        let _ = wal.recover().unwrap();
        wal.append(1, 1, &rows(10, 200)).unwrap();

        assert_eq!(wal.recover().unwrap().pending.len(), 2, "recovery must not lose the header state");
    }

    #[test]
    fn test_corrupt_length_field_does_not_panic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let wal = Wal::new(&path);
        wal.append(1, 0, &rows(10, 100)).unwrap();

        let mut data = std::fs::read(&path).unwrap();
        data[WAL_HEADER_LEN..WAL_HEADER_LEN + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        std::fs::write(&path, &data).unwrap();

        let rec = wal.recover().unwrap();
        assert!(rec.is_clean());
        assert!(matches!(rec.stopped_because, WalStopReason::TruncatedTail { .. }));
    }

    #[test]
    fn test_string_values_with_arbitrary_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Wal::new(dir.path().join("test.wal"));

        let r = vec![(
            1i64,
            vec![Some(vec![0xFF, 0x00, 0xFE]), Some(b"line\nbreak\0inside".to_vec())],
        )];
        wal.append(1, 0, &r).unwrap();
        assert_eq!(wal.recover().unwrap().pending[0].rows, r);
    }

    #[test]
    fn test_mixed_batches_and_checkpoints_in_one_pass() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Wal::new(dir.path().join("test.wal"));

        wal.append(1, 0, &rows(5, 100)).unwrap();
        wal.append(2, 0, &rows(5, 200)).unwrap();
        wal.append(1, 1, &rows(5, 300)).unwrap();
        wal.checkpoint(1, 1).unwrap();
        wal.append(2, 1, &rows(5, 400)).unwrap();

        let rec = wal.recover().unwrap();
        assert_eq!(rec.pending_rows(1), 0);
        assert_eq!(rec.pending_rows(2), 10);
        assert_eq!(rec.total_pending_rows(), 10);
    }

    /// Every batch handed to a shared log must be recoverable, whoever wrote it.
    ///
    /// This is the test for the two races the log had. The header was written
    /// after stat-ing the file, so concurrent appends to a fresh log could both
    /// decide it was empty and both write the magic. And a record was written as
    /// three separate writes, so two threads could interleave a length, a
    /// checksum and a payload into something no reader could parse. Either one
    /// shows up here as a recovery that stops early or loses rows.
    #[test]
    fn test_concurrent_appends_to_one_log_are_all_recoverable() {
        use std::sync::Arc;

        const THREADS: u64 = 8;
        const PER_THREAD: usize = 40;

        let dir = tempfile::tempdir().unwrap();
        let wal = Arc::new(Wal::new(dir.path().join("test.wal")));

        // Every table is checkpointed at the end, so recovery must come back
        // empty. A truncated log or a mangled record would show up as leftover
        // rows rather than as a panic.
        let workers: Vec<_> = (0..THREADS)
            .map(|t| {
                let wal = Arc::clone(&wal);
                std::thread::spawn(move || {
                    for i in 0..PER_THREAD {
                        wal.append(t, i as u64, &rows(4, (t * 1000 + i as u64) as i64)).unwrap();
                    }
                })
            })
            .collect();
        for w in workers {
            w.join().unwrap();
        }
        for t in 0..THREADS {
            wal.checkpoint(t, PER_THREAD as u64).unwrap();
        }

        let rec = wal.recover().unwrap();
        assert_eq!(
            rec.stopped_because,
            WalStopReason::Eof,
            "log did not read back cleanly"
        );
        assert_eq!(rec.total_pending_rows(), 0, "every batch was checkpointed");
        // Every byte of every record was read, which is what proves nothing
        // interleaved and no header was duplicated.
        assert_eq!(
            rec.bytes_recovered,
            wal.size() - WAL_HEADER_LEN as u64,
            "recovery did not account for the whole log"
        );
    }

    /// Compaction must not swallow a batch that arrives while it runs.
    ///
    /// Compaction renames a rewritten file over the live path. An append that
    /// landed in between the scan and the rename would simply disappear, and
    /// since it was never checkpointed, nothing would replay it. The batch is
    /// given a sequence above the checkpoint so it is pending and must survive.
    #[test]
    fn test_compaction_does_not_lose_concurrent_appends() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let wal = Arc::new(Wal::new(dir.path().join("test.wal")));

        // Pending rows that compaction must carry over, plus a checkpoint to
        // give it something to drop.
        wal.append(1, 0, &rows(4, 100)).unwrap();
        wal.checkpoint(1, 0).unwrap();
        wal.append(1, 1, &rows(4, 200)).unwrap();

        let stop = Arc::new(AtomicBool::new(false));
        let writer = {
            let wal = Arc::clone(&wal);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                // Keep appending past the checkpoint, so a lost write is visible
                // as missing pending rows rather than as a length mismatch.
                let mut seq = 2u64;
                while !stop.load(Ordering::Relaxed) {
                    wal.append(1, seq, &rows(4, (900 + seq) as i64)).unwrap();
                    seq += 1;
                }
                seq
            })
        };

        for _ in 0..25 {
            wal.compact().unwrap();
        }
        stop.store(true, Ordering::Relaxed);
        let final_seq = writer.join().unwrap();

        let rec = wal.recover().unwrap();
        assert_eq!(
            rec.stopped_because,
            WalStopReason::Eof,
            "compaction left a malformed log"
        );
        assert_eq!(
            rec.pending_rows(1),
            (final_seq as usize - 1) * 4,
            "an append was lost across a compaction"
        );
    }

    /// An append after a compaction has to reach the file, not the old inode.
    ///
    /// The log handle stays open between appends to keep the write path cheap,
    /// and compaction replaces the file by renaming over it. A handle that
    /// outlives the rename points at an unlinked inode, so the write succeeds,
    /// reports success, and lands nowhere. This is the quietest possible data
    /// loss, and it is why every operation that replaces the file drops the
    /// cached handle.
    #[test]
    fn test_append_after_compaction_reaches_the_new_file() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Wal::new(dir.path().join("test.wal"));

        wal.append(1, 0, &rows(2, 100)).unwrap();
        wal.checkpoint(1, 0).unwrap();
        wal.append(1, 1, &rows(2, 200)).unwrap();

        // The first append opens and caches the handle; compaction then replaces
        // the file underneath it.
        wal.compact().unwrap();
        wal.append(1, 2, &rows(2, 300)).unwrap();

        // Read the bytes back with a completely independent reader, which is
        // what a restart would do.
        let raw = std::fs::read(wal.path()).unwrap();
        let rec = Wal::new(dir.path().join("test.wal")).recover().unwrap();
        assert_eq!(
            rec.stopped_because,
            WalStopReason::Eof,
            "the rewritten log is malformed ({} bytes on disk)", raw.len()
        );
        assert_eq!(
            rec.pending_rows(1),
            4,
            "the post-compaction batch did not land in the file"
        );

        // And a fresh handle must not carry a stale length from the old file.
        assert_eq!(wal.size() as usize, raw.len(), "cached length went stale");
    }

    /// The header must appear exactly once, however the log was created.
    #[test]
    fn test_header_is_written_once_across_many_appends() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Wal::new(dir.path().join("test.wal"));

        for i in 0..5i64 {
            wal.append(1, i as u64, &rows(3, i)).unwrap();
        }

        let raw = std::fs::read(wal.path()).unwrap();
        let magic = WAL_MAGIC.to_le_bytes();
        let magic_positions: Vec<usize> = raw
            .windows(magic.len())
            .enumerate()
            .filter(|(_, w)| *w == magic)
            .map(|(i, _)| i)
            .collect();
        assert_eq!(
            magic_positions,
            vec![0],
            "the magic should appear once, at the start, but was found at {:?}",
            magic_positions
        );

        let rec = wal.recover().unwrap();
        assert_eq!(rec.stopped_because, WalStopReason::Eof);
        assert_eq!(rec.pending_rows(1), 15);
    }
}