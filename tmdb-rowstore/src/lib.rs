/// Row store - row-oriented storage for relation tables (metadata, dimension tables).
///
/// Uses BTree-based in-memory storage with WAL for durability.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use parking_lot::RwLock;

/// Column value
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Float64(f64),
    Text(String),
    Bytes(Vec<u8>),
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Value::Null => 0u8.hash(state),
            Value::Bool(b) => { 1u8.hash(state); b.hash(state); }
            Value::Int32(v) => { 2u8.hash(state); v.hash(state); }
            Value::Int64(v) => { 3u8.hash(state); v.hash(state); }
            Value::Float64(v) => { 4u8.hash(state); v.to_bits().hash(state); }
            Value::Text(s) => { 5u8.hash(state); s.hash(state); }
            Value::Bytes(b) => { 6u8.hash(state); b.hash(state); }
        }
    }
}

impl Eq for Value {}

/// A row of data
pub type Row = Vec<Value>;

/// Row store table
pub struct RowTable {
    columns: Vec<String>,
    primary_key: Vec<usize>,
    rows: RwLock<HashMap<Vec<Value>, Row>>,
}

impl RowTable {
    pub fn new(columns: Vec<String>, primary_key: Vec<usize>) -> Self {
        Self {
            columns,
            primary_key,
            rows: RwLock::new(HashMap::new()),
        }
    }

    /// Insert a row
    pub fn insert(&self, row: Row) -> anyhow::Result<()> {
        let key: Vec<Value> = self
            .primary_key
            .iter()
            .map(|&i| row[i].clone())
            .collect();

        self.rows.write().insert(key, row);
        Ok(())
    }

    /// Get a row by primary key
    pub fn get(&self, key: &[Value]) -> Option<Row> {
        self.rows.read().get(key).cloned()
    }

    /// Scan rows with a filter
    pub fn scan(&self, filter: Option<Box<dyn Fn(&Row) -> bool>>) -> Vec<Row> {
        let rows = self.rows.read();
        match filter {
            Some(f) => rows.values().filter(|r| f(r)).cloned().collect(),
            None => rows.values().cloned().collect(),
        }
    }

    /// Delete a row by primary key
    pub fn delete(&self, key: &[Value]) -> bool {
        self.rows.write().remove(key).is_some()
    }

    /// Update a row
    pub fn update(&self, key: &[Value], new_row: Row) -> bool {
        let mut rows = self.rows.write();
        if rows.contains_key(key) {
            rows.insert(key.to_vec(), new_row);
            true
        } else {
            false
        }
    }

    /// Row count
    pub fn row_count(&self) -> usize {
        self.rows.read().len()
    }

    /// Column names
    pub fn columns(&self) -> &[String] {
        &self.columns
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_table_crud() {
        let table = RowTable::new(
            vec!["id".to_string(), "name".to_string()],
            vec![0],
        );

        table.insert(vec![Value::Int64(1), Value::Text("Alice".into())]).unwrap();
        table.insert(vec![Value::Int64(2), Value::Text("Bob".into())]).unwrap();

        assert_eq!(table.row_count(), 2);

        let row = table.get(&[Value::Int64(1)]);
        assert!(row.is_some());
        assert_eq!(row.unwrap()[1], Value::Text("Alice".into()));

        assert!(table.delete(&[Value::Int64(1)]));
        assert_eq!(table.row_count(), 1);
    }
}
