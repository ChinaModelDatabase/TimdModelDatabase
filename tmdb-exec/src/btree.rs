/// B-tree index with automatic maintenance on INSERT/UPDATE/DELETE
use std::collections::BTreeMap;
use std::sync::Arc;
use parking_lot::RwLock;

/// Index entry
#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub key: Vec<u8>,
    pub row_id: u64,
}

/// B-tree index with maintenance
pub struct BTreeIndex {
    name: String,
    columns: Vec<String>,
    entries: RwLock<BTreeMap<Vec<u8>, Vec<u64>>>, // key -> list of row_ids
    entry_count: AtomicUsize,
}

use std::sync::atomic::{AtomicUsize, Ordering};

impl BTreeIndex {
    pub fn new(name: String, columns: Vec<String>) -> Self {
        Self {
            name,
            columns,
            entries: RwLock::new(BTreeMap::new()),
            entry_count: AtomicUsize::new(0),
        }
    }

    /// Insert entry (called by engine on INSERT)
    pub fn insert(&self, key: Vec<u8>, row_id: u64) {
        let mut entries = self.entries.write();
        entries.entry(key).or_insert_with(Vec::new).push(row_id);
        self.entry_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Update entry (called by engine on UPDATE)
    pub fn update(&self, old_key: &[u8], new_key: Vec<u8>, row_id: u64) {
        let mut entries = self.entries.write();
        // Remove from old key
        if let Some(ids) = entries.get_mut(old_key) {
            ids.retain(|&id| id != row_id);
            if ids.is_empty() {
                entries.remove(old_key);
            }
        }
        // Insert to new key
        entries.entry(new_key).or_insert_with(Vec::new).push(row_id);
    }

    /// Delete entry (called by engine on DELETE)
    pub fn delete(&self, key: &[u8], row_id: u64) {
        let mut entries = self.entries.write();
        if let Some(ids) = entries.get_mut(key) {
            ids.retain(|&id| id != row_id);
            if ids.is_empty() {
                entries.remove(key);
            }
            self.entry_count.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Search by key
    pub fn search(&self, key: &[u8]) -> Vec<u64> {
        let entries = self.entries.read();
        entries.get(key).cloned().unwrap_or_default()
    }

    /// Range scan
    pub fn range_scan(&self, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, Vec<u64>)> {
        let entries = self.entries.read();
        entries.range::<Vec<u8>, _>(start.to_vec()..=end.to_vec())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Get index name
    pub fn name(&self) -> &str { &self.name }

    /// Get indexed columns
    pub fn columns(&self) -> &[String] { &self.columns }

    /// Get entry count
    pub fn entry_count(&self) -> usize { self.entry_count.load(Ordering::Relaxed) }

    /// Check if key exists
    pub fn contains(&self, key: &[u8]) -> bool {
        self.entries.read().contains_key(key)
    }
}

/// Index manager - manages all indexes for a table
pub struct IndexManager {
    indexes: RwLock<Vec<Arc<BTreeIndex>>>,
}

impl IndexManager {
    pub fn new() -> Self {
        Self {
            indexes: RwLock::new(Vec::new()),
        }
    }

    /// Create a new index
    pub fn create_index(&self, name: String, columns: Vec<String>) -> Arc<BTreeIndex> {
        let index = Arc::new(BTreeIndex::new(name, columns));
        self.indexes.write().push(index.clone());
        index
    }

    /// Get index by name
    pub fn get_index(&self, name: &str) -> Option<Arc<BTreeIndex>> {
        self.indexes.read().iter().find(|i| i.name() == name).cloned()
    }

    /// Get all indexes
    pub fn get_all_indexes(&self) -> Vec<Arc<BTreeIndex>> {
        self.indexes.read().clone()
    }

    /// Drop index
    pub fn drop_index(&self, name: &str) -> bool {
        let mut indexes = self.indexes.write();
        let len_before = indexes.len();
        indexes.retain(|i| i.name() != name);
        indexes.len() < len_before
    }
}

impl Default for IndexManager {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_insert_search() {
        let idx = BTreeIndex::new("idx_ts".to_string(), vec!["ts".to_string()]);
        idx.insert(vec![1, 2, 3], 100);
        idx.insert(vec![1, 2, 3], 200);
        idx.insert(vec![4, 5, 6], 300);

        assert_eq!(idx.search(&[1, 2, 3]), vec![100, 200]);
        assert_eq!(idx.search(&[4, 5, 6]), vec![300]);
        assert!(idx.search(&[7, 8, 9]).is_empty());
    }

    #[test]
    fn test_index_update() {
        let idx = BTreeIndex::new("idx_up".to_string(), vec!["col".to_string()]);
        idx.insert(vec![1], 100);
        idx.update(&[1], vec![2], 100);
        assert!(idx.search(&[1]).is_empty());
        assert_eq!(idx.search(&[2]), vec![100]);
    }

    #[test]
    fn test_index_delete() {
        let idx = BTreeIndex::new("idx_del".to_string(), vec!["col".to_string()]);
        idx.insert(vec![1], 100);
        idx.insert(vec![1], 200);
        idx.delete(&[1], 100);
        assert_eq!(idx.search(&[1]), vec![200]);
        idx.delete(&[1], 200);
        assert!(idx.search(&[1]).is_empty());
    }

    #[test]
    fn test_index_manager() {
        let mgr = IndexManager::new();
        let idx = mgr.create_index("idx1".to_string(), vec!["col1".to_string()]);
        assert_eq!(idx.columns.len(), 1);
        assert_eq!(mgr.get_all_indexes().len(), 1);
        assert!(mgr.get_index("idx1").is_some());
        assert!(mgr.drop_index("idx1"));
        assert!(mgr.get_all_indexes().is_empty());
    }
}
