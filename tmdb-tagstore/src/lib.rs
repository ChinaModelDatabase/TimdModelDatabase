use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use ahash::AHashMap;
use parking_lot::RwLock;
use roaring::RoaringBitmap;

pub struct TagStore {
    dictionaries: RwLock<HashMap<String, AHashMap<String, RoaringBitmap>>>,
    next_series_id: AtomicU64,
}

impl TagStore {
    pub fn new() -> Self {
        Self {
            dictionaries: RwLock::new(HashMap::new()),
            next_series_id: AtomicU64::new(1),
        }
    }

    pub fn get_or_create_series(&self, tags: &[(String, String)]) -> u64 {
        if let Some(series_id) = self.find_series(tags) {
            return series_id;
        }

        let series_id = self.next_series_id.fetch_add(1, Ordering::Relaxed);
        let mut dicts = self.dictionaries.write();

        for (name, value) in tags {
            let dict = dicts.entry(name.clone()).or_insert_with(AHashMap::new);
            dict.entry(value.clone())
                .or_insert_with(RoaringBitmap::new)
                .insert(series_id as u32);
        }

        series_id
    }

    pub fn find_series(&self, tags: &[(String, String)]) -> Option<u64> {
        if tags.is_empty() {
            return None;
        }

        let dicts = self.dictionaries.read();

        let first = &tags[0];
        let mut result: RoaringBitmap = dicts.get(&first.0)?.get(&first.1)?.clone();

        for (name, value) in &tags[1..] {
            let tag_series = dicts.get(name)?.get(value)?.clone();
            result = result & tag_series;
            if result.is_empty() {
                return None;
            }
        }

        result.iter().next().map(|id| id as u64)
    }

    pub fn find_series_in(&self, tag_name: &str, values: &[String]) -> RoaringBitmap {
        let dicts = self.dictionaries.read();
        let mut result = RoaringBitmap::new();

        if let Some(dict) = dicts.get(tag_name) {
            for value in values {
                if let Some(bm) = dict.get(value) {
                    result = result | bm;
                }
            }
        }

        result
    }

    pub fn get_tag_values(&self, tag_name: &str) -> Vec<String> {
        let dicts = self.dictionaries.read();
        dicts
            .get(tag_name)
            .map(|d| d.keys().cloned().collect())
            .unwrap_or_default()
    }

    pub fn series_count(&self) -> u64 {
        self.next_series_id.load(Ordering::Relaxed) - 1
    }
}

impl Default for TagStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tag_store_basic() {
        let store = TagStore::new();

        let id1 = store.get_or_create_series(&[
            ("device_id".to_string(), "10086".to_string()),
            ("line_id".to_string(), "7".to_string()),
        ]);

        let id2 = store.get_or_create_series(&[
            ("device_id".to_string(), "10087".to_string()),
            ("line_id".to_string(), "7".to_string()),
        ]);

        assert_ne!(id1, id2);
        assert_eq!(store.series_count(), 2);

        let found = store.find_series(&[
            ("device_id".to_string(), "10086".to_string()),
            ("line_id".to_string(), "7".to_string()),
        ]);
        assert_eq!(found, Some(id1));

        let bitmap = store.find_series_in("device_id", &["10086".to_string(), "10087".to_string()]);
        assert_eq!(bitmap.len(), 2);
    }

    #[test]
    fn test_duplicate_series() {
        let store = TagStore::new();

        let id1 = store.get_or_create_series(&[
            ("device_id".to_string(), "10086".to_string()),
        ]);
        let id2 = store.get_or_create_series(&[
            ("device_id".to_string(), "10086".to_string()),
        ]);

        assert_eq!(id1, id2);
        assert_eq!(store.series_count(), 1);
    }
}
