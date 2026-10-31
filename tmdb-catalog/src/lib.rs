/// Catalog - metadata management for tables, columns, schemas.
///
/// Provides virtual views for both PostgreSQL and MySQL protocols.

use std::sync::Arc;
use dashmap::DashMap;

/// Table metadata
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TableInfo {
    pub schema_name: String,
    pub table_name: String,
    pub table_type: TableType,
    pub columns: Vec<ColumnInfo>,
    pub row_count: u64,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TableType {
    BaseTable,
    View,
    SystemTable,
}

/// Column metadata
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub ordinal_position: u32,
    pub data_type: String,
    pub nullable: bool,
    pub is_primary_key: bool,
    pub default_value: Option<String>,
}

/// Database schema
#[derive(Debug, Clone)]
pub struct Schema {
    pub name: String,
    pub tables: DashMap<String, TableInfo>,
}

/// Catalog - the root metadata store
pub struct Catalog {
    schemas: DashMap<String, Arc<Schema>>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            schemas: DashMap::new(),
        }
    }

    /// Create a new schema (database)
    pub fn create_schema(&self, name: &str) -> anyhow::Result<()> {
        if self.schemas.contains_key(name) {
            return Err(anyhow::anyhow!("schema '{}' already exists", name));
        }
        self.schemas.insert(
            name.to_string(),
            Arc::new(Schema {
                name: name.to_string(),
                tables: DashMap::new(),
            }),
        );
        Ok(())
    }

    /// Register a table
    pub fn register_table(&self, schema: &str, table: TableInfo) -> anyhow::Result<()> {
        let s = self
            .schemas
            .get(schema)
            .ok_or_else(|| anyhow::anyhow!("schema '{}' not found", schema))?;
        s.tables.insert(table.table_name.clone(), table);
        Ok(())
    }

    /// Get a table
    pub fn get_table(&self, schema: &str, table: &str) -> Option<TableInfo> {
        self.schemas.get(schema)?.tables.get(table).map(|t| t.clone())
    }

    /// List all schemas
    pub fn list_schemas(&self) -> Vec<String> {
        self.schemas.iter().map(|s| s.key().clone()).collect()
    }

    /// List all tables in a schema
    pub fn list_tables(&self, schema: &str) -> Vec<String> {
        self.schemas
            .get(schema)
            .map(|s| s.tables.iter().map(|t| t.key().clone()).collect())
            .unwrap_or_default()
    }

    /// Drop a table
    pub fn drop_table(&self, schema: &str, table: &str) -> anyhow::Result<()> {
        let s = self
            .schemas
            .get(schema)
            .ok_or_else(|| anyhow::anyhow!("schema '{}' not found", schema))?;
        s.tables
            .remove(table)
            .ok_or_else(|| anyhow::anyhow!("table '{}' not found", table))?;
        Ok(())
    }

    /// Generate PostgreSQL system catalog view
    pub fn pg_class_view(&self) -> Vec<PgClassRow> {
        let mut rows = Vec::new();
        for schema in self.schemas.iter() {
            for table in schema.tables.iter() {
                rows.push(PgClassRow {
                    relname: table.table_name.clone(),
                    relnamespace: schema.name.clone(),
                    relkind: 'r',
                });
            }
        }
        rows
    }

    /// Generate MySQL information_schema.tables view
    pub fn mysql_tables_view(&self) -> Vec<MysqlTableRow> {
        let mut rows = Vec::new();
        for schema in self.schemas.iter() {
            for table in schema.tables.iter() {
                rows.push(MysqlTableRow {
                    table_catalog: "tmdb".to_string(),
                    table_schema: schema.name.clone(),
                    table_name: table.table_name.clone(),
                    table_type: "BASE TABLE".to_string(),
                    engine: "tmdb".to_string(),
                });
            }
        }
        rows
    }
}

/// PostgreSQL pg_class row
#[derive(Debug)]
pub struct PgClassRow {
    pub relname: String,
    pub relnamespace: String,
    pub relkind: char,
}

/// MySQL information_schema.tables row
#[derive(Debug)]
pub struct MysqlTableRow {
    pub table_catalog: String,
    pub table_schema: String,
    pub table_name: String,
    pub table_type: String,
    pub engine: String,
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_catalog_crud() {
        let catalog = Catalog::new();
        catalog.create_schema("test_db").unwrap();

        let table = TableInfo {
            schema_name: "test_db".to_string(),
            table_name: "metrics".to_string(),
            table_type: TableType::BaseTable,
            columns: vec![ColumnInfo {
                name: "ts".to_string(),
                ordinal_position: 1,
                data_type: "TIMESTAMPTZ".to_string(),
                nullable: false,
                is_primary_key: true,
                default_value: None,
            }],
            row_count: 0,
            created_at: 0,
        };

        catalog.register_table("test_db", table).unwrap();

        let t = catalog.get_table("test_db", "metrics").unwrap();
        assert_eq!(t.table_name, "metrics");
        assert_eq!(t.columns.len(), 1);

        let tables = catalog.list_tables("test_db");
        assert_eq!(tables, vec!["metrics".to_string()]);
    }
}
