/// In-memory buffer for incoming writes before flushing to segment files.
///
/// MemTable accumulates rows in columnar format, sorted by timestamp.
/// When full (by row count or byte size), it is flushed to disk as a segment file.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

/// A single row of data
#[derive(Debug, Clone)]
pub struct Row {
    pub ts: i64,
    pub values: Vec<Option<Vec<u8>>>,
}

/// MemTable for a single series (tag combination)
pub struct MemTable {
    series_id: u64,
    rows: RwLock<BTreeMap<i64, Vec<Option<Vec<u8>>>>>,
    row_count: AtomicU64,
    byte_size: AtomicU64,
    max_rows: usize,
    max_bytes: usize,
}

impl MemTable {
    pub fn new(series_id: u64, max_rows: usize, max_bytes: usize) -> Self {
        Self {
            series_id,
            rows: RwLock::new(BTreeMap::new()),
            row_count: AtomicU64::new(0),
            byte_size: AtomicU64::new(0),
            max_rows,
            max_bytes,
        }
    }

    /// Batch insert: lock once, insert all rows
    pub fn insert_batch(&self, batch: &[(i64, Vec<Option<Vec<u8>>>)]) -> bool {
        let mut rows = self.rows.write();
        let mut size_delta = 0u64;
        for (ts, values) in batch {
            let estimated_size = 8 + values.iter().map(|v| v.as_ref().map_or(0, |b| b.len())).sum::<usize>();
            rows.insert(*ts, values.clone());
            size_delta += estimated_size as u64;
        }
        self.row_count.fetch_add(batch.len() as u64, Ordering::Relaxed);
        self.byte_size.fetch_add(size_delta, Ordering::Relaxed);
        self.row_count.load(Ordering::Relaxed) >= self.max_rows as u64
            || self.byte_size.load(Ordering::Relaxed) >= self.max_bytes as u64
    }

    /// Insert a row into the memtable
    pub fn insert(&self, ts: i64, values: Vec<Option<Vec<u8>>>) -> bool {
        let estimated_size = 8 + values.iter().map(|v| v.as_ref().map_or(0, |b| b.len())).sum::<usize>();

        let mut rows = self.rows.write();
        rows.insert(ts, values);
        self.row_count.fetch_add(1, Ordering::Relaxed);
        self.byte_size.fetch_add(estimated_size as u64, Ordering::Relaxed);

        // Return true if should flush
        self.row_count.load(Ordering::Relaxed) >= self.max_rows as u64
            || self.byte_size.load(Ordering::Relaxed) >= self.max_bytes as u64
    }

    /// Check if the memtable should be flushed
    pub fn should_flush(&self) -> bool {
        self.row_count.load(Ordering::Relaxed) >= self.max_rows as u64
            || self.byte_size.load(Ordering::Relaxed) >= self.max_bytes as u64
    }

    /// Get the number of rows
    pub fn row_count(&self) -> u64 {
        self.row_count.load(Ordering::Relaxed)
    }

    /// Get the estimated byte size
    pub fn byte_size(&self) -> u64 {
        self.byte_size.load(Ordering::Relaxed)
    }

    /// Get the series ID
    pub fn series_id(&self) -> u64 {
        self.series_id
    }

    /// Scan rows in the given timestamp range
    pub fn scan(&self, ts_min: i64, ts_max: i64) -> Vec<(i64, Vec<Option<Vec<u8>>>)> {
        let rows = self.rows.read();
        rows.range(ts_min..=ts_max)
            .map(|(&ts, values)| (ts, values.clone()))
            .collect()
    }

    /// Get all rows sorted by timestamp
    pub fn get_all(&self) -> Vec<(i64, Vec<Option<Vec<u8>>>)> {
        let rows = self.rows.read();
        rows.iter().map(|(&ts, values)| (ts, values.clone())).collect()
    }

    /// Take every row out, leaving the memtable empty.
    ///
    /// The caller owns the returned rows. Compared to `get_all` this moves the
    /// values instead of deep-cloning them, which is the difference between one
    /// copy of the memtable and one allocation per value per row. A flush over a
    /// multi-million row memtable spent more time cloning than encoding.
    pub fn drain(&self) -> Vec<(i64, Vec<Option<Vec<u8>>>)> {
        let mut rows = self.rows.write();
        let drained = std::mem::take(&mut *rows);
        self.row_count.store(0, Ordering::Relaxed);
        self.byte_size.store(0, Ordering::Relaxed);
        drained.into_iter().collect()
    }

    /// Clear the memtable
    pub fn clear(&self) {
        self.rows.write().clear();
        self.row_count.store(0, Ordering::Relaxed);
        self.byte_size.store(0, Ordering::Relaxed);
    }
}

/// Global write buffer managing multiple memtables
pub struct WriteBuffer {
    memtables: parking_lot::RwLock<Vec<Box<MemTable>>>,
    /// How many memtables may be live at once.
    ///
    /// Nothing enforces this yet: the buffer is only ever written through
    /// [`WriteBuffer::add`], and callers drain it with
    /// [`WriteBuffer::get_flushable`], so the count stays at whatever the
    /// flusher leaves behind.
    #[allow(dead_code)]
    max_memtables: usize,
    max_rows_per_table: usize,
    max_bytes_per_table: usize,
}

impl WriteBuffer {
    pub fn new(max_memtables: usize, max_rows: usize, max_bytes: usize) -> Self {
        Self {
            memtables: parking_lot::RwLock::new(Vec::new()),
            max_memtables,
            max_rows_per_table: max_rows,
            max_bytes_per_table: max_bytes,
        }
    }

    /// Create a new memtable for a series
    pub fn create_memtable(&self, series_id: u64) -> Box<MemTable> {
        Box::new(MemTable::new(
            series_id,
            self.max_rows_per_table,
            self.max_bytes_per_table,
        ))
    }

    /// Get memtables that need flushing
    pub fn get_flushable(&self) -> Vec<Box<MemTable>> {
        let tables = self.memtables.read();
        tables
            .iter()
            .filter(|t| t.should_flush())
            .map(|t| {
                let mt = Box::new(MemTable::new(
                    t.series_id(),
                    self.max_rows_per_table,
                    self.max_bytes_per_table,
                ));
                // Copy data (simplified)
                for (ts, values) in t.get_all() {
                    mt.insert(ts, values);
                }
                mt
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memtable_insert_and_scan() {
        let mt = MemTable::new(1, 1000, 1024 * 1024);

        // Insert some rows
        for i in 0..100 {
            let ts = 1000 + i * 100;
            let values = vec![Some(format!("value_{}", i).into_bytes())];
            mt.insert(ts, values);
        }

        assert_eq!(mt.row_count(), 100);

        // Scan range
        let rows = mt.scan(1000, 5000);
        assert_eq!(rows.len(), 41); // 1000, 1100, ..., 5000

        // Scan empty range
        let rows = mt.scan(20000, 30000);
        assert!(rows.is_empty());
    }

    #[test]
    fn test_memtable_flush_threshold() {
        let mt = MemTable::new(1, 5, 1024 * 1024);

        for i in 0..5 {
            assert!(!mt.should_flush());
            mt.insert(i, vec![Some(vec![0u8; 100])]);
        }

        assert!(mt.should_flush());
    }
}
