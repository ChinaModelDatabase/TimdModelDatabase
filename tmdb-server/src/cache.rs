/// LRU query cache for hot query results
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use std::time::{Duration, Instant};

/// Cache entry
#[derive(Debug, Clone)]
pub struct CacheEntry {
    pub result: Vec<(i64, Vec<Option<Vec<u8>>>)>,
    pub created_at: Instant,
    pub hit_count: u64,
}

/// LRU query cache
pub struct QueryCache {
    entries: RwLock<HashMap<String, CacheEntry>>,
    max_entries: usize,
    ttl: Duration,
    hits: std::sync::atomic::AtomicU64,
    misses: std::sync::atomic::AtomicU64,
}

impl QueryCache {
    pub fn new(max_entries: usize, ttl_secs: u64) -> Arc<Self> {
        Arc::new(Self {
            entries: RwLock::new(HashMap::new()),
            max_entries,
            ttl: Duration::from_secs(ttl_secs),
            hits: std::sync::atomic::AtomicU64::new(0),
            misses: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Get cached result
    pub fn get(&self, key: &str) -> Option<Vec<(i64, Vec<Option<Vec<u8>>>)>> {
        let mut entries = self.entries.write();
        if let Some(entry) = entries.get_mut(key) {
            if entry.created_at.elapsed() < self.ttl {
                entry.hit_count += 1;
                self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Some(entry.result.clone());
            }
        }
        self.misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        None
    }

    /// Put result in cache
    pub fn put(&self, key: String, result: Vec<(i64, Vec<Option<Vec<u8>>>)>) {
        let mut entries = self.entries.write();
        if entries.len() >= self.max_entries {
            // Evict LRU (simplified: remove oldest)
            if let Some(oldest_key) = entries.keys().next().cloned() {
                entries.remove(&oldest_key);
            }
        }
        entries.insert(key, CacheEntry {
            result,
            created_at: Instant::now(),
            hit_count: 0,
        });
    }

    /// Invalidate cache for a table
    pub fn invalidate_table(&self, table: &str) {
        self.entries.write().retain(|k, _| !k.starts_with(table));
    }

    /// Clear all cache
    pub fn clear(&self) { self.entries.write().clear(); }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        let entries = self.entries.read();
        CacheStats {
            entries: entries.len(),
            max_entries: self.max_entries,
            hits: self.hits.load(std::sync::atomic::Ordering::Relaxed),
            misses: self.misses.load(std::sync::atomic::Ordering::Relaxed),
        }
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub entries: usize,
    pub max_entries: usize,
    pub hits: u64,
    pub misses: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_cache_basic() {
        let cache = QueryCache::new(100, 60);
        assert!(cache.get("test").is_none());
        cache.put("test".to_string(), vec![(1000, vec![Some(vec![1, 2, 3])])]);
        let result = cache.get("test");
        assert!(result.is_some());
        assert_eq!(cache.stats().hits, 1);
    }

    #[test]
    fn test_query_cache_invalidate() {
        let cache = QueryCache::new(100, 60);
        cache.put("table1.row1".to_string(), vec![]);
        cache.put("table2.row1".to_string(), vec![]);
        cache.invalidate_table("table1");
        assert!(cache.get("table1.row1").is_none());
        assert!(cache.get("table2.row1").is_some());
    }
}
