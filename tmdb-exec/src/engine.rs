use std::sync::Arc;

use tmdb_timestore::{ColumnDef, CompressionLevel, DataType, Table, TableMeta, TsUnit};
use sqlparser::ast::{
    ColumnOption, CreateTable, DataType as SqlDataType, Expr, ObjectName, Query, SetExpr,
    Statement, TableFactor,
};
use sqlparser::dialect::{Dialect, GenericDialect};
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};

use crate::btree::BTreeIndex;
use crate::query::execute_select;
use crate::values::Value;

/// Simple SQL executor with query cache
pub struct Engine {
    pub tables: dashmap::DashMap<String, Arc<Table>>,
    pub data_dir: std::path::PathBuf,
    query_cache: Arc<QueryCache>,
    /// Secondary indexes, keyed by table name. Each index is maintained on
    /// every INSERT that touches its table, so a lookup can never see a row
    /// the table does not have.
    indexes: dashmap::DashMap<String, Vec<Arc<BTreeIndex>>>,
}

/// LRU query cache (inline for simplicity)
pub struct QueryCache {
    entries: parking_lot::RwLock<std::collections::HashMap<String, CachedRows>>,
    max_entries: usize,
}

/// A cached result. The column list is stored alongside the rows, because a
/// projection can produce a different set of columns per statement and reusing
/// another table's schema would mislabel every value.
#[derive(Debug, Clone)]
pub struct CachedRows {
    pub columns: Vec<String>,
    pub types: Vec<DataType>,
    pub rows: Vec<Vec<Option<Vec<u8>>>>,
}

impl QueryCache {
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: parking_lot::RwLock::new(std::collections::HashMap::new()),
            max_entries,
        }
    }

    pub fn get(&self, key: &str) -> Option<CachedRows> {
        self.entries.read().get(key).cloned()
    }

    pub fn put(&self, key: String, result: CachedRows) {
        let mut entries = self.entries.write();
        if entries.len() >= self.max_entries && !entries.contains_key(&key) {
            if let Some(k) = entries.keys().next().cloned() {
                entries.remove(&k);
            }
        }
        entries.insert(key, result);
    }

    /// Drop every cached plan that reads from `table`.
    pub fn invalidate_table(&self, table: &str) {
        self.entries.write().retain(|k, _| !k.contains(table));
    }
}

impl Engine {
    pub fn new(data_dir: impl AsRef<std::path::Path>) -> Self {
        let data_dir = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir).ok();
        let tables = dashmap::DashMap::new();
        for table in reopen_tables(&data_dir) {
            tables.insert(table.meta.name.clone(), Arc::new(table));
        }
        Self {
            tables,
            data_dir,
            query_cache: Arc::new(QueryCache::new(1000)),
            indexes: dashmap::DashMap::new(),
        }
    }

    /// Execute SQL, reusing a cached result for a repeated SELECT.
    pub fn execute_cached(&self, sql: &str) -> anyhow::Result<QueryResult> {
        if let Some(stmt) = self.parse_one(sql)? {
            let key = stmt.to_string();
            if is_read_only(&stmt) {
                if let Some(cached) = self.query_cache.get(&key) {
                    return Ok(QueryResult::Rows {
                        columns: cached.columns,
                        types: cached.types,
                        rows: cached.rows,
                    });
                }
                let result = self.execute_statement(&stmt)?;
                if let QueryResult::Rows { columns, types, rows } = &result {
                    self.query_cache.put(
                        key,
                        CachedRows {
                            columns: columns.clone(),
                            types: types.clone(),
                            rows: rows.clone(),
                        },
                    );
                }
                return Ok(result);
            }
        }
        self.execute(sql)
    }

    pub fn create_table(&self, name: &str, meta: TableMeta) -> anyhow::Result<()> {
        let table_dir = self.data_dir.join(name);
        let table = Arc::new(Table::new(meta, &table_dir));
        // The schema is what lets the table come back after a restart. It has
        // to be on disk before the table is visible, or a crash between the two
        // would leave rows nobody can read.
        table.save_meta()?;
        self.tables.insert(name.to_string(), table);
        Ok(())
    }

    /// Parse one statement, rejecting an empty or multi-statement string so a
    /// caller cannot believe it ran something it did not.
    pub fn parse_one(&self, sql: &str) -> anyhow::Result<Option<Statement>> {
        let mut stmts = parse_statements(&GenericDialect {}, sql)?;
        match stmts.len() {
            0 => Ok(None),
            1 => Ok(Some(stmts.remove(0))),
            n => anyhow::bail!("expected one statement, found {}", n),
        }
    }

    pub fn execute(&self, sql: &str) -> anyhow::Result<QueryResult> {
        match self.parse_one(sql)? {
            None => Ok(QueryResult::Empty),
            Some(stmt) => self.execute_statement(&stmt),
        }
    }

    fn execute_statement(&self, stmt: &Statement) -> anyhow::Result<QueryResult> {
        match stmt {
            Statement::Query(query) => self.run_select(query),
            Statement::CreateTable(create) => self.execute_create_table(create),
            Statement::Insert(insert) => self.execute_insert(insert),
            Statement::Drop {
                object_type,
                if_exists,
                names,
                ..
            } => self.execute_drop(*object_type, *if_exists, names),
            Statement::CreateIndex(create) => self.execute_create_index(create),
            Statement::AlterTable { .. } => {
                anyhow::bail!("ALTER TABLE is not supported yet")
            }
            other => anyhow::bail!("unsupported statement: {}", other),
        }
    }

    fn run_select(&self, query: &Query) -> anyhow::Result<QueryResult> {
        let select = match query.body.as_ref() {
            SetExpr::Select(s) => s.as_ref(),
            other => anyhow::bail!("unsupported query body: {}", other),
        };
        let name = match &select.from.first().map(|f| &f.relation) {
            Some(TableFactor::Table { name, .. }) => object_name(name),
            Some(other) => anyhow::bail!("unsupported table factor: {}", other),
            None => anyhow::bail!("SELECT without FROM is not supported"),
        };
        let table = self
            .tables
            .get(&name)
            .ok_or_else(|| anyhow::anyhow!("table '{}' not found", name))?;
        let (columns, types, rows) = execute_select(table.as_ref(), query)?;
        Ok(QueryResult::Rows { columns, types, rows })
    }

    fn execute_create_table(&self, create: &CreateTable) -> anyhow::Result<QueryResult> {
        if create.like.is_some() || create.clone.is_some() || create.query.is_some() {
            anyhow::bail!("CREATE TABLE ... AS/LIKE is not supported yet");
        }
        let table_name = object_name(&create.name);
        if create.columns.is_empty() {
            anyhow::bail!("CREATE TABLE needs at least one column");
        }

        let mut columns = Vec::with_capacity(create.columns.len());
        for def in &create.columns {
            let data_type = map_data_type(&def.data_type)?;
            let mut nullable = true;
            let mut compression = CompressionLevel::Lz4;
            for opt in &def.options {
                match &opt.option {
                    ColumnOption::NotNull => nullable = false,
                    ColumnOption::Null => nullable = true,
                    ColumnOption::Unique { is_primary, .. } if *is_primary => nullable = false,
                    _ => {}
                }
            }
            // A column is a tag when the DDL says so, or when it is one of the
            // conventional tag names. This is decided per column: an earlier
            // tag must not silently tag every column after it.
            let is_tag = def.name.value.eq_ignore_ascii_case("device_id")
                || def.name.value.eq_ignore_ascii_case("line_id")
                || def.name.value.to_ascii_lowercase().starts_with("tag_");
            let _ = &mut compression;
            columns.push(ColumnDef {
                name: def.name.value.clone(),
                data_type,
                is_tag,
                nullable,
                compression,
            });
        }

        // Every read path assumes column 0 is the timestamp, so a table without
        // one would scan into nonsense rather than fail at creation.
        if columns[0].data_type != DataType::Timestamp {
            anyhow::bail!(
                "the first column of '{}' must be TIMESTAMP, found {} '{}'",
                table_name,
                columns[0].data_type,
                columns[0].name
            );
        }
        let mut seen = std::collections::HashSet::new();
        for c in &columns {
            if !seen.insert(c.name.to_ascii_lowercase()) {
                anyhow::bail!("duplicate column '{}'", c.name);
            }
        }

        let meta = TableMeta {
            table_id: self.tables.len() as u64 + 1,
            name: table_name.clone(),
            ts_unit: TsUnit::Millis,
            columns,
            chunk_interval_ms: 86_400_000,
            retention_days: 365,
        };
        self.create_table(&table_name, meta)?;
        Ok(QueryResult::Empty)
    }

    fn execute_drop(
        &self,
        object_type: sqlparser::ast::ObjectType,
        if_exists: bool,
        names: &[ObjectName],
    ) -> anyhow::Result<QueryResult> {
        match object_type {
            sqlparser::ast::ObjectType::Table => {
                if names.len() != 1 {
                    anyhow::bail!("expected exactly one table name in DROP TABLE");
                }
                let name = object_name(&names[0]);
                if self.tables.remove(&name).is_none() {
                    if if_exists {
                        return Ok(QueryResult::Empty);
                    }
                    return Err(anyhow::anyhow!("table '{}' not found", name));
                }
                // The indexes are keyed by table name, so a dropped table must
                // take them with it. Leaving them behind would keep serving
                // lookups for rows that no longer exist.
                self.indexes.remove(&name);
                let _ = std::fs::remove_dir_all(self.data_dir.join(&name));
                self.query_cache.invalidate_table(&name);
                Ok(QueryResult::Empty)
            }
            sqlparser::ast::ObjectType::Index => {
                for name in names {
                    let index_name = object_name(name);
                    if !self.drop_index(&index_name) {
                        if if_exists {
                            continue;
                        }
                        return Err(anyhow::anyhow!("index '{}' not found", index_name));
                    }
                }
                Ok(QueryResult::Empty)
            }
            other => anyhow::bail!("only DROP TABLE and DROP INDEX are supported, not {}", other),
        }
    }

    /// Remove an index by name, wherever it is registered.
    fn drop_index(&self, index_name: &str) -> bool {
        let mut removed = false;
        self.indexes.retain(|_, indexes| {
            let before = indexes.len();
            indexes.retain(|i| !i.name().eq_ignore_ascii_case(index_name));
            if indexes.len() < before {
                removed = true;
            }
            !indexes.is_empty()
        });
        removed
    }

    /// Indexes registered on a table: name, indexed columns and entry count.
    pub fn index_stats(&self, table: &str) -> Vec<(String, Vec<String>, usize)> {
        self.indexes
            .get(table)
            .map(|indexes| {
                indexes
                    .iter()
                    .map(|i| (i.name().to_string(), i.columns().to_vec(), i.entry_count()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Encode one row's indexed columns into an unambiguous key.
    ///
    /// Each value is length-prefixed and preceded by a presence marker, so a
    /// NULL and an empty string cannot produce the same key.
    fn encode_index_key(
        values: &[Option<Vec<u8>>],
        columns: &[String],
        meta: &TableMeta,
    ) -> anyhow::Result<Vec<u8>> {
        let mut out = Vec::new();
        for name in columns {
            let slot = meta
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(name))
                .ok_or_else(|| anyhow::anyhow!("column '{}' is not in the schema", name))?;
            match values.get(slot).and_then(|v| v.as_deref()) {
                Some(raw) => {
                    out.push(1);
                    out.extend_from_slice(&(raw.len() as u32).to_le_bytes());
                    out.extend_from_slice(raw);
                }
                None => out.push(0),
            }
        }
        Ok(out)
    }

    /// Record newly written rows in every index on the table.
    ///
    /// Called for every INSERT, in the same call that writes the rows, so an
    /// index can never lag the table it describes.
    fn maintain_indexes(&self, table_name: &str, batch: &[(i64, Vec<Option<Vec<u8>>>)]) {
        let Some(indexes) = self.indexes.get(table_name) else {
            return;
        };
        if indexes.is_empty() {
            return;
        }
        let Some(table) = self.tables.get(table_name) else {
            return;
        };
        let meta = &table.meta;
        for (ts, values) in batch {
            for index in indexes.iter() {
                // The columns were validated when the index was created, so a
                // failure here means the schema changed underneath us; skip
                // rather than corrupt the index with a key it cannot interpret.
                if let Ok(key) = Self::encode_index_key(values, index.columns(), meta) {
                    index.insert(key, *ts as u64);
                }
            }
        }
    }

    fn execute_create_index(
        &self,
        create: &sqlparser::ast::CreateIndex,
    ) -> anyhow::Result<QueryResult> {
        let name = create
            .name
            .as_ref()
            .map(object_name)
            .unwrap_or_default();
        if name.is_empty() {
            anyhow::bail!("CREATE INDEX needs an index name");
        }
        let table_name = object_name(&create.table_name);
        let table = self
            .tables
            .get(&table_name)
            .ok_or_else(|| anyhow::anyhow!("table '{}' not found", table_name))?;
        let meta = &table.meta;

        // Each column is an OrderByExpr, so it displays as `col`, `col ASC` or
        // `col DESC`. Take the identifier and drop the ordering keyword.
        let mut columns: Vec<String> = Vec::with_capacity(create.columns.len());
        for c in &create.columns {
            let text = c.to_string();
            let raw = text
                .split_whitespace()
                .next()
                .unwrap_or_default()
                .trim_matches(|ch| ch == '"' || ch == '`' || ch == '\'')
                .to_string();
            if raw.is_empty() {
                anyhow::bail!("CREATE INDEX has an empty column name");
            }
            if !meta.columns.iter().any(|c| c.name.eq_ignore_ascii_case(&raw)) {
                anyhow::bail!(
                    "column '{}' is not in table '{}'",
                    raw,
                    table_name
                );
            }
            columns.push(raw);
        }
        if columns.is_empty() {
            anyhow::bail!("CREATE INDEX must name at least one column");
        }

        if let Some(existing) = self.indexes.get(&table_name) {
            if existing.iter().any(|i| i.name().eq_ignore_ascii_case(&name)) {
                anyhow::bail!("index '{}' already exists on '{}'", name, table_name);
            }
        }

        // Build over everything already in the table. Sorting by timestamp
        // first keeps the construction order deterministic.
        let index = Arc::new(BTreeIndex::new(name.clone(), columns.clone()));
        let mut scanned = table.scan(i64::MIN, i64::MAX, &[], None)?;
        scanned.sort_by_key(|r| r.0);
        let built = scanned.len();
        for (ts, values) in &scanned {
            let key = Self::encode_index_key(values, &columns, meta)?;
            index.insert(key, *ts as u64);
        }

        self.indexes.entry(table_name.clone()).or_default().push(index);

        Ok(QueryResult::Affected(built as u64))
    }

    fn execute_insert(&self, insert: &sqlparser::ast::Insert) -> anyhow::Result<QueryResult> {
        if insert.on.is_some() {
            anyhow::bail!("INSERT ... ON CONFLICT is not supported yet");
        }
        let table_name = object_name(&insert.table_name);
        let table = self
            .tables
            .get(&table_name)
            .ok_or_else(|| anyhow::anyhow!("table '{}' not found", table_name))?;

        let source = insert
            .source
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("only INSERT ... VALUES is supported"))?;
        let values = match source.body.as_ref() {
            SetExpr::Values(v) => v,
            other => anyhow::bail!("only INSERT ... VALUES is supported, got {}", other),
        };

        let meta = &table.meta;
        // Either every row names the columns, or none does; a partial list would
        // silently shift values into the wrong columns.
        let targets: Vec<usize> = if insert.columns.is_empty() {
            (0..meta.columns.len()).collect()
        } else {
            let mut idx = Vec::with_capacity(insert.columns.len());
            for ident in &insert.columns {
                let (i, _) = meta
                    .columns
                    .iter()
                    .enumerate()
                    .find(|(_, c)| c.name.eq_ignore_ascii_case(&ident.value))
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "unknown column '{}' in table '{}'",
                            ident.value,
                            meta.name
                        )
                    })?;
                idx.push(i);
            }
            idx
        };

        let mut batch: Vec<(i64, Vec<Option<Vec<u8>>>)> = Vec::with_capacity(values.rows.len());
        for (row_no, row) in values.rows.iter().enumerate() {
            if row.len() != targets.len() {
                anyhow::bail!(
                    "row {} has {} values but {} columns were named",
                    row_no + 1,
                    row.len(),
                    targets.len()
                );
            }
            let mut encoded: Vec<Option<Vec<u8>>> = vec![None; meta.columns.len()];
            for (slot, expr) in targets.iter().zip(row) {
                let col = &meta.columns[*slot];
                encoded[*slot] = insert_value(expr, col).map_err(|e| {
                    anyhow::anyhow!("row {}: {}", row_no + 1, e)
                })?;
            }
            let ts = match encoded.first().and_then(|v| v.as_deref()) {
                Some(bytes) if bytes.len() == 8 => {
                    let mut buf = [0u8; 8];
                    buf.copy_from_slice(bytes);
                    i64::from_le_bytes(buf)
                }
                _ => anyhow::bail!(
                    "row {} has no timestamp; column 0 must be TIMESTAMP",
                    row_no + 1
                ),
            };
            batch.push((ts, encoded));
        }

        if batch.is_empty() {
            return Ok(QueryResult::Affected(0));
        }
        let count = batch.len() as u64;
        table.insert_batch(&batch);
        // Index maintenance happens in the same call that writes the rows, so
        // an index can never observe a state the table has moved past.
        self.maintain_indexes(&table_name, &batch);
        self.query_cache.invalidate_table(&table_name);
        Ok(QueryResult::Affected(count))
    }

    pub fn flush_table(&self, name: &str) -> anyhow::Result<()> {
        if let Some(table) = self.tables.get(name) {
            table.flush()?;
        }
        Ok(())
    }

    pub fn row_count(&self, name: &str) -> u64 {
        self.tables
            .get(name)
            .map(|t| t.row_count())
            .unwrap_or(0)
    }
}

/// Every table that a previous run left in `data_dir`.
///
/// A restart that cannot see its own tables leaves perfectly good segments
/// orphaned: the rows are on disk, but no name points at them. Each table
/// directory holds its schema next to its segments, so reopening one is a
/// matter of reading that schema back.
///
/// A directory that will not open is reported and skipped rather than failing
/// the whole startup: one unreadable table should not take down the others.
fn reopen_tables(data_dir: &std::path::Path) -> Vec<Table> {
    let Ok(entries) = std::fs::read_dir(data_dir) else {
        return Vec::new();
    };

    let mut tables = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        match Table::open_stored(&path) {
            Ok(Some(table)) => tables.push(table),
            Ok(None) => {}
            Err(e) => tracing::warn!(
                "skipping {}: {}; its rows stay on disk but are not queryable",
                path.display(),
                e
            ),
        }
    }
    tables
}

/// Query result
#[derive(Debug)]
pub enum QueryResult {
    Empty,
    Affected(u64),
    Rows {
        columns: Vec<String>,
        /// Declared type of each output column. Carried explicitly because the
        /// byte encoding alone cannot distinguish a timestamp from a double.
        types: Vec<DataType>,
        rows: Vec<Vec<Option<Vec<u8>>>>,
    },
}

fn is_read_only(stmt: &Statement) -> bool {
    matches!(stmt, Statement::Query(_) | Statement::Explain { .. })
}

/// Parse SQL, after folding duration literals into single number tokens.
///
/// InfluxQL writes a bucket width as `time(1m)`, but SQL has no such literal:
/// the tokenizer emits `1` and then `m`, and the expression parser then stops at
/// `1` and rejects the `)`. Rewriting the text would mean re-implementing enough
/// of a lexer to avoid corrupting string literals, so the fix is applied to the
/// token stream instead, where a string literal is already a single opaque
/// token.
fn parse_statements(dialect: &dyn Dialect, sql: &str) -> anyhow::Result<Vec<Statement>> {
    let tokens = Tokenizer::new(dialect, sql).tokenize()?;
    Ok(Parser::new(dialect)
        .with_tokens(fold_duration_literals(tokens))
        .parse_statements()?)
}

/// Merge `Number("1"), Word("m")` into `Number("1m")` inside a `time(` call.
fn fold_duration_literals(tokens: Vec<Token>) -> Vec<Token> {
    let mut out: Vec<Token> = Vec::with_capacity(tokens.len());
    let mut i = 0;
    while i < tokens.len() {
        // Only inside `time(`, so an unrelated `1 m` elsewhere cannot be fused.
        let in_time_call = matches!(out.last(), Some(Token::LParen))
            && matches!(out.get(out.len().wrapping_sub(2)), Some(Token::Word(w)) if w.value.eq_ignore_ascii_case("time"));
        match (&tokens[i], tokens.get(i + 1)) {
            (Token::Number(n, long), Some(Token::Word(w)))
                if in_time_call && !w.quote_style.is_some() && is_duration_unit(&w.value) =>
            {
                out.push(Token::Number(format!("{}{}", n, w.value), *long));
                i += 2;
                continue;
            }
            _ => {}
        }
        out.push(tokens[i].clone());
        i += 1;
    }
    out
}

fn is_duration_unit(s: &str) -> bool {
    matches!(
        s,
        "ns" | "us" | "\u{b5}s" | "\u{3bc}s" | "ms" | "s" | "m" | "h" | "d" | "w"
    )
}

/// The last part of a possibly-qualified name, so `db.t` and `t` both resolve to
/// `t`.
fn object_name(name: &ObjectName) -> String {
    name.0
        .last()
        .map(|p| p.value.clone())
        .unwrap_or_default()
}

fn map_data_type(ty: &SqlDataType) -> anyhow::Result<DataType> {
    // Strip the ClickHouse-style wrappers before matching, so `Nullable(Float64)`
    // and `LowCardinality(String)` land on the type they wrap.
    let ty = match ty {
        SqlDataType::Nullable(inner) | SqlDataType::LowCardinality(inner) => inner,
        other => other,
    };
    Ok(match ty {
        SqlDataType::Timestamp(_, _)
        | SqlDataType::Date
        | SqlDataType::Date32
        | SqlDataType::Time(_, _)
        | SqlDataType::Datetime(_)
        | SqlDataType::Datetime64(_, _) => DataType::Timestamp,
        SqlDataType::Double
        | SqlDataType::DoublePrecision
        | SqlDataType::Float64
        | SqlDataType::Float8
        | SqlDataType::Real
        | SqlDataType::Float(_)
        | SqlDataType::Numeric(_)
        | SqlDataType::Decimal(_)
        | SqlDataType::Dec(_)
        | SqlDataType::BigNumeric(_)
        | SqlDataType::BigDecimal(_) => DataType::Float64,
        SqlDataType::Float32 | SqlDataType::Float4 => DataType::Float32,
        SqlDataType::BigInt(_)
        | SqlDataType::Int8(_)
        | SqlDataType::Int16
        | SqlDataType::Int64
        | SqlDataType::Int128
        | SqlDataType::Int256
        | SqlDataType::UnsignedBigInt(_)
        | SqlDataType::UnsignedInt8(_)
        | SqlDataType::UInt64
        | SqlDataType::UInt128
        | SqlDataType::UInt256 => DataType::Int64,
        SqlDataType::Int(_)
        | SqlDataType::Int4(_)
        | SqlDataType::Int32
        | SqlDataType::Integer(_)
        | SqlDataType::SmallInt(_)
        | SqlDataType::Int2(_)
        | SqlDataType::TinyInt(_)
        | SqlDataType::MediumInt(_)
        | SqlDataType::UnsignedTinyInt(_)
        | SqlDataType::UnsignedSmallInt(_)
        | SqlDataType::UnsignedInt(_)
        | SqlDataType::UnsignedInt2(_)
        | SqlDataType::UnsignedInt4(_)
        | SqlDataType::UnsignedInteger(_)
        | SqlDataType::UnsignedMediumInt(_)
        | SqlDataType::UInt8
        | SqlDataType::UInt16
        | SqlDataType::UInt32 => DataType::Int32,
        SqlDataType::Bool | SqlDataType::Boolean => DataType::Bool,
        SqlDataType::Text
        | SqlDataType::String(_)
        | SqlDataType::FixedString(_)
        | SqlDataType::Char(_)
        | SqlDataType::Character(_)
        | SqlDataType::Varchar(_)
        | SqlDataType::Nvarchar(_)
        | SqlDataType::CharacterVarying(_)
        | SqlDataType::CharVarying(_)
        | SqlDataType::CharacterLargeObject(_)
        | SqlDataType::CharLargeObject(_)
        | SqlDataType::Clob(_)
        | SqlDataType::Uuid
        | SqlDataType::Bytea => DataType::String,
        // Anything with a name the caller spelled out is still worth accepting
        // when it names a type this engine understands.
        SqlDataType::Custom(name, _) => match object_name(name).to_ascii_lowercase().as_str() {
            "text" | "string" | "varchar" | "utf8" | "tag" => DataType::String,
            "timestamp" | "timestamptz" | "datetime" | "time" => DataType::Timestamp,
            "double" | "float64" | "float8" | "real" => DataType::Float64,
            "float" | "float32" | "float4" => DataType::Float32,
            "bigint" | "int64" | "long" => DataType::Int64,
            "int" | "integer" | "int32" => DataType::Int32,
            "bool" | "boolean" => DataType::Bool,
            other => anyhow::bail!("unsupported column type '{}'", other),
        },
        other => anyhow::bail!("unsupported column type {}", other),
    })
}

/// Turn one INSERT value expression into bytes for its column, applying the
/// column's type. `encode` does the widening and range checks, so a value that
/// does not fit becomes an error rather than bytes that a later read would
/// decode as nonsense.
fn insert_value(expr: &Expr, col: &ColumnDef) -> anyhow::Result<Option<Vec<u8>>> {
    let value = literal_value(expr, col)?;
    if value.is_null() && !col.nullable {
        anyhow::bail!("column '{}' is NOT NULL", col.name);
    }
    value.encode(&col.data_type)
}

/// Read a literal, without applying the column type.
fn literal_value(expr: &Expr, col: &ColumnDef) -> anyhow::Result<Value> {
    Ok(match expr {
        Expr::Value(v) => match v {
            sqlparser::ast::Value::Null => Value::Null,
            sqlparser::ast::Value::Boolean(b) => Value::Bool(*b),
            sqlparser::ast::Value::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Value::Int(i)
                } else if let Ok(f) = n.parse::<f64>() {
                    Value::Float(f)
                } else {
                    anyhow::bail!("'{}' is not a number", n)
                }
            }
            sqlparser::ast::Value::SingleQuotedString(s)
            | sqlparser::ast::Value::DoubleQuotedString(s)
            | sqlparser::ast::Value::EscapedStringLiteral(s)
            | sqlparser::ast::Value::NationalStringLiteral(s) => Value::Str(s.clone()),
            other => anyhow::bail!("unsupported literal {} for column '{}'", other, col.name),
        },
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Minus,
            expr,
        } => match literal_value(expr, col)? {
            Value::Int(i) => Value::Int(-i),
            Value::Float(f) => Value::Float(-f),
            Value::Null => Value::Null,
            other => anyhow::bail!("cannot negate {} for column '{}'", other.to_text(), col.name),
        },
        other => anyhow::bail!(
            "INSERT values must be literals, got {} for column '{}'",
            other,
            col.name
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn engine_with_metrics(dir: &std::path::Path) -> Engine {
        let engine = Engine::new(dir);
        engine
            .execute(
                "CREATE TABLE metrics (ts TIMESTAMP, host VARCHAR, region VARCHAR, value DOUBLE)",
            )
            .unwrap();
        engine
    }

    fn rows(result: QueryResult) -> (Vec<String>, Vec<Vec<Option<Vec<u8>>>>) {
        match result {
            QueryResult::Rows { columns, rows, .. } => (columns, rows),
            other => panic!("expected Rows, got {:?}", other),
        }
    }

    fn f64_at(row: &[Option<Vec<u8>>], i: usize) -> f64 {
        let bytes = row[i].as_ref().expect("value");
        f64::from_le_bytes(bytes[..8].try_into().unwrap())
    }

    fn i64_at(row: &[Option<Vec<u8>>], i: usize) -> i64 {
        let bytes = row[i].as_ref().expect("value");
        i64::from_le_bytes(bytes[..8].try_into().unwrap())
    }

    fn str_at(row: &[Option<Vec<u8>>], i: usize) -> String {
        String::from_utf8(row[i].as_ref().expect("value").clone()).unwrap()
    }

    #[test]
    fn projection_returns_only_the_named_columns() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        engine
            .execute("INSERT INTO metrics VALUES (1000, 'a', 'eu', 1.5), (2000, 'b', 'us', 2.5)")
            .unwrap();

        let (columns, out) = rows(engine.execute("SELECT host, value FROM metrics").unwrap());
        assert_eq!(columns, vec!["host", "value"]);
        assert_eq!(out.len(), 2);
        assert_eq!(str_at(&out[0], 0), "a");
        assert!((f64_at(&out[0], 1) - 1.5).abs() < 1e-9);
    }

    #[test]
    fn where_filters_rows_that_the_old_scan_returned() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        engine
            .execute(
                "INSERT INTO metrics VALUES (1000, 'a', 'eu', 1.0), (2000, 'b', 'us', 2.0), \
                 (3000, 'c', 'eu', 3.0)",
            )
            .unwrap();

        let (columns, out) = rows(
            engine
                .execute("SELECT value FROM metrics WHERE value > 1.5")
                .unwrap(),
        );
        assert_eq!(columns, vec!["value"]);
        assert_eq!(out.len(), 2, "the predicate must actually remove rows");
        assert!((f64_at(&out[0], 0) - 2.0).abs() < 1e-9);
    }

    #[test]
    fn string_equality_on_a_tag_column_filters() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        engine
            .execute(
                "INSERT INTO metrics VALUES (1000, 'a', 'eu', 1.0), (2000, 'b', 'us', 2.0)",
            )
            .unwrap();

        let (_, out) = rows(engine.execute("SELECT host FROM metrics WHERE region = 'eu'").unwrap());
        assert_eq!(out.len(), 1);
        assert_eq!(str_at(&out[0], 0), "a");
    }

    #[test]
    fn aggregate_without_group_by_returns_one_row() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        engine
            .execute("INSERT INTO metrics VALUES (1000, 'a', 'eu', 1.0), (2000, 'b', 'eu', 3.0)")
            .unwrap();

        let (columns, out) = rows(
            engine
                .execute("SELECT count(*), avg(value) FROM metrics")
                .unwrap(),
        );
        assert_eq!(columns.len(), 2);
        assert_eq!(out.len(), 1);
        assert_eq!(i64_at(&out[0], 0), 2);
        assert!((f64_at(&out[0], 1) - 2.0).abs() < 1e-9);
    }

    #[test]
    fn group_by_tag_produces_one_row_per_tag() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        engine
            .execute(
                "INSERT INTO metrics VALUES (1000, 'a', 'eu', 1.0), (2000, 'a', 'eu', 3.0), \
                 (3000, 'b', 'us', 5.0)",
            )
            .unwrap();

        let (columns, out) = rows(
            engine
                .execute("SELECT region, sum(value) FROM metrics GROUP BY region")
                .unwrap(),
        );
        assert_eq!(columns, vec!["region", "sum(value)"]);
        assert_eq!(out.len(), 2);
        // The tag column leads the output, is not duplicated by the projection,
        // and each group carries the sum of its own rows.
        let mut seen = Vec::new();
        for row in &out {
            let region = str_at(row, 0);
            let total = f64_at(row, 1);
            match region.as_str() {
                "eu" => {
                    assert!((total - 4.0).abs() < 1e-9, "eu total was {}", total);
                }
                "us" => {
                    assert!((total - 5.0).abs() < 1e-9, "us total was {}", total);
                }
                other => panic!("unexpected region '{}'", other),
            }
            seen.push(region);
        }
        seen.sort();
        assert_eq!(seen, vec!["eu", "us"]);
    }

    #[test]
    fn time_buckets_respect_the_table_unit() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        // The table is in milliseconds, so a one-minute bucket is 60_000 units.
        engine
            .execute(
                "INSERT INTO metrics VALUES (0, 'a', 'eu', 1.0), (30000, 'a', 'eu', 2.0), \
                 (61000, 'a', 'eu', 4.0)",
            )
            .unwrap();

        let (columns, out) = rows(
            engine
                .execute("SELECT time(1m), sum(value) FROM metrics GROUP BY time(1m)")
                .unwrap(),
        );
        assert_eq!(columns, vec!["time", "sum(value)"]);
        assert_eq!(out.len(), 2, "two distinct minutes, got {:?}", out);
        let first = i64_at(&out[0], 0);
        assert_eq!(first % 60_000, 0, "bucket must align to the minute");
        assert!((f64_at(&out[0], 1) - 3.0).abs() < 1e-9);
    }

    #[test]
    fn order_by_and_limit_apply_to_the_projection() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        engine
            .execute(
                "INSERT INTO metrics VALUES (1000, 'a', 'eu', 5.0), (2000, 'b', 'eu', 1.0), \
                 (3000, 'c', 'eu', 3.0)",
            )
            .unwrap();

        let (_, out) = rows(
            engine
                .execute("SELECT value FROM metrics ORDER BY value DESC LIMIT 2")
                .unwrap(),
        );
        assert_eq!(out.len(), 2);
        assert!((f64_at(&out[0], 0) - 5.0).abs() < 1e-9);
        assert!((f64_at(&out[1], 0) - 3.0).abs() < 1e-9);
    }

    #[test]
    fn null_literals_round_trip_and_count_is_null_aware() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        engine
            .execute("INSERT INTO metrics VALUES (1000, 'a', 'eu', NULL), (2000, 'b', 'eu', 4.0)")
            .unwrap();

        let (_, out) = rows(
            engine
                .execute("SELECT count(*), count(value), sum(value) FROM metrics")
                .unwrap(),
        );
        assert_eq!(i64_at(&out[0], 0), 2, "count(*) counts every row");
        assert_eq!(i64_at(&out[0], 1), 1, "count(col) skips NULL");
        assert!((f64_at(&out[0], 2) - 4.0).abs() < 1e-9);
    }

    #[test]
    fn insert_honours_a_named_column_list() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        engine
            .execute("INSERT INTO metrics (value, ts) VALUES (7.0, 4000)")
            .unwrap();
        let (_, out) = rows(engine.execute("SELECT ts, value FROM metrics").unwrap());
        assert_eq!(i64_at(&out[0], 0), 4000);
        assert!((f64_at(&out[0], 1) - 7.0).abs() < 1e-9);
    }

    #[test]
    fn a_table_without_a_leading_timestamp_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path());
        let err = engine
            .execute("CREATE TABLE bad (value DOUBLE, ts TIMESTAMP)")
            .unwrap_err();
        assert!(
            err.to_string().contains("first column"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn an_unknown_column_is_an_error_not_an_empty_result() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        let err = engine.execute("SELECT nope FROM metrics").unwrap_err();
        assert!(err.to_string().contains("nope"), "unexpected: {}", err);
    }

    #[test]
    fn a_mismatched_type_is_rejected_rather_than_written_as_garbage() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        assert!(engine
            .execute("INSERT INTO metrics VALUES (1000, 'a', 'eu', 'not-a-number')")
            .is_err());
    }

    #[test]
    fn a_restarted_engine_still_sees_the_tables_it_created() {
        let dir = tempfile::tempdir().unwrap();
        {
            let engine = engine_with_metrics(dir.path());
            engine
                .execute("INSERT INTO metrics VALUES (1000, 'a', 'eu', 1.5)")
                .unwrap();
            engine.flush_table("metrics").unwrap();
        }

        let restarted = Engine::new(dir.path());
        let (_, out) = rows(restarted.execute("SELECT host FROM metrics").unwrap());
        assert_eq!(out.len(), 1);
        assert_eq!(str_at(&out[0], 0), "a");
    }

    #[test]
    fn a_restarted_engine_recovers_rows_that_were_never_flushed() {
        let dir = tempfile::tempdir().unwrap();
        {
            let engine = engine_with_metrics(dir.path());
            engine
                .execute(
                    "INSERT INTO metrics VALUES (1000, 'a', 'eu', 1.5), (2000, 'b', 'us', 2.5)",
                )
                .unwrap();
        }

        let restarted = Engine::new(dir.path());
        let (_, out) = rows(restarted.execute("SELECT count(*) FROM metrics").unwrap());
        assert_eq!(i64_at(&out[0], 0), 2);
    }

    #[test]
    fn a_dropped_table_does_not_come_back_after_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let engine = engine_with_metrics(dir.path());
            engine.execute("DROP TABLE metrics").unwrap();
        }

        let restarted = Engine::new(dir.path());
        assert!(restarted.execute("SELECT * FROM metrics").is_err());
    }

    #[test]
    fn a_restarted_engine_still_refuses_a_value_the_schema_rejects() {
        let dir = tempfile::tempdir().unwrap();
        {
            let engine = engine_with_metrics(dir.path());
            engine.flush_table("metrics").unwrap();
            drop(engine);
        }

        let restarted = Engine::new(dir.path());
        assert!(restarted
            .execute("INSERT INTO metrics VALUES (3000, 'c', 'eu', 'not-a-number')")
            .is_err());
    }

    /// One damaged schema must not take the healthy tables down with it: the
    /// rows of every other table are still readable.
    #[test]
    fn a_table_with_a_damaged_schema_does_not_hide_the_others() {
        let dir = tempfile::tempdir().unwrap();
        {
            let engine = engine_with_metrics(dir.path());
            engine
                .execute("CREATE TABLE broken (ts TIMESTAMP, v DOUBLE)")
                .unwrap();
            engine
                .execute("INSERT INTO metrics VALUES (1000, 'a', 'eu', 1.5)")
                .unwrap();
            engine.flush_table("metrics").unwrap();
            std::fs::write(
                dir.path()
                    .join("broken")
                    .join(tmdb_timestore::META_FILE_NAME),
                "{ not json",
            )
            .unwrap();
            drop(engine);
        }

        let restarted = Engine::new(dir.path());
        let (_, out) = rows(restarted.execute("SELECT count(*) FROM metrics").unwrap());
        assert_eq!(i64_at(&out[0], 0), 1);
        assert!(restarted.execute("SELECT * FROM broken").is_err());
    }

    #[test]
    fn results_survive_a_flush_to_disk() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        engine
            .execute("INSERT INTO metrics VALUES (1000, 'a', 'eu', 1.0), (2000, 'b', 'us', 2.0)")
            .unwrap();
        engine.flush_table("metrics").unwrap();

        let (_, out) = rows(
            engine
                .execute("SELECT host FROM metrics WHERE value = 2.0")
                .unwrap(),
        );
        assert_eq!(out.len(), 1);
        assert_eq!(str_at(&out[0], 0), "b");
    }

    #[test]
    fn a_cached_select_is_invalidated_by_a_write() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        engine
            .execute("INSERT INTO metrics VALUES (1000, 'a', 'eu', 1.0)")
            .unwrap();

        let (_, before) = rows(engine.execute_cached("SELECT count(*) FROM metrics").unwrap());
        assert_eq!(i64_at(&before[0], 0), 1);

        engine
            .execute("INSERT INTO metrics VALUES (2000, 'b', 'eu', 2.0)")
            .unwrap();
        let (_, after) = rows(engine.execute_cached("SELECT count(*) FROM metrics").unwrap());
        assert_eq!(i64_at(&after[0], 0), 2, "a write must not be masked by cache");
    }

    #[test]
    fn cached_columns_match_the_projection() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        engine
            .execute("INSERT INTO metrics VALUES (1000, 'a', 'eu', 1.0)")
            .unwrap();
        let (columns, _) = rows(engine.execute_cached("SELECT host, value FROM metrics").unwrap());
        assert_eq!(columns, vec!["host", "value"]);
    }

    #[test]
    fn an_unknown_statement_is_an_error_instead_of_silently_empty() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path());
        assert!(engine.execute("UPDATE metrics SET value = 1").is_err());
        assert!(engine.execute("SELECT * FROM missing_table").is_err());
        assert!(engine.execute("this is not sql").is_err());
    }

    #[test]
    fn group_by_column_must_be_selected_or_aggregated() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        engine
            .execute("INSERT INTO metrics VALUES (1000, 'a', 'eu', 1.0)")
            .unwrap();
        let err = engine
            .execute("SELECT host, count(*) FROM metrics GROUP BY region")
            .unwrap_err();
        assert!(err.to_string().contains("GROUP BY"), "unexpected: {}", err);
    }

    // -----------------------------------------------------------------------
    // HAVING
    // -----------------------------------------------------------------------

    /// WHERE removes rows and HAVING removes groups. This is the case that
    /// cannot be expressed with WHERE alone.
    #[test]
    fn having_filters_groups_that_where_cannot() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        engine
            .execute(
                "INSERT INTO metrics VALUES \
                 (1000, 'a', 'eu', 1.0), (2000, 'b', 'eu', 2.0), \
                 (3000, 'c', 'us', 3.0)",
            )
            .unwrap();

        // WHERE count(*) is not even legal; this group survives WHERE and is
        // removed by HAVING.
        let (_, out) = rows(
            engine
                .execute("SELECT region, count(*) FROM metrics GROUP BY region HAVING count(*) > 1")
                .unwrap(),
        );
        assert_eq!(out.len(), 1, "only the two-row group survives");
        assert_eq!(i64_at(&out[0], 1), 2);
    }

    /// HAVING may name an aggregate the SELECT list does not project.
    #[test]
    fn having_may_reference_an_aggregate_that_is_not_selected() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        engine
            .execute(
                "INSERT INTO metrics VALUES \
                 (1000, 'a', 'eu', 1.0), (2000, 'b', 'eu', 2.0), \
                 (3000, 'c', 'us', 3.0)",
            )
            .unwrap();

        let (columns, out) = rows(
            engine
                .execute("SELECT region FROM metrics GROUP BY region HAVING count(*) > 1")
                .unwrap(),
        );
        // The helper aggregate must not leak into the result.
        assert_eq!(columns, vec!["region"]);
        assert_eq!(out.len(), 1);
        assert_eq!(str_at(&out[0], 0), "eu");
    }

    /// HAVING runs before LIMIT, so a group it drops does not consume a slot.
    #[test]
    fn having_runs_before_limit() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        engine
            .execute(
                "INSERT INTO metrics VALUES \
                 (1000, 'a', 'big', 1.0), (2000, 'b', 'big', 2.0), \
                 (3000, 'c', 'small', 3.0)",
            )
            .unwrap();

        let (_, out) = rows(
            engine
                .execute(
                    "SELECT region, count(*) FROM metrics GROUP BY region \
                     HAVING count(*) > 1 LIMIT 1",
                )
                .unwrap(),
        );
        assert_eq!(out.len(), 1);
        assert_eq!(str_at(&out[0], 0), "big");
    }

    /// A NULL predicate drops the group, as SQL requires.
    #[test]
    fn a_null_having_predicate_drops_the_group() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        engine
            .execute(
                "INSERT INTO metrics VALUES \
                 (1000, 'a', 'eu', 1.0), (2000, 'b', 'us', 2.0)",
            )
            .unwrap();

        // value is not grouped, so it must be aggregated; min > 100 is NULL
        // for every group and every group is dropped.
        let (_, out) = rows(
            engine
                .execute(
                    "SELECT region, count(*) FROM metrics GROUP BY region \
                     HAVING min(value) > 100",
                )
                .unwrap(),
        );
        assert_eq!(out.len(), 0);
    }

    /// HAVING may reference a GROUP BY column, not only aggregates.
    #[test]
    fn having_can_filter_on_a_group_column() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        engine
            .execute(
                "INSERT INTO metrics VALUES \
                 (1000, 'a', 'eu', 1.0), (2000, 'b', 'eu', 2.0), \
                 (3000, 'c', 'us', 3.0)",
            )
            .unwrap();

        let (_, out) = rows(
            engine
                .execute(
                    "SELECT region, count(*) FROM metrics GROUP BY region HAVING region = 'us'",
                )
                .unwrap(),
        );
        assert_eq!(out.len(), 1);
        assert_eq!(str_at(&out[0], 0), "us");
    }

    #[test]
    fn an_unknown_having_column_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        let err = engine
            .execute(
                "SELECT region, count(*) FROM metrics GROUP BY region HAVING nosuch > 1",
            )
            .unwrap_err();
        assert!(err.to_string().contains("nosuch"), "unexpected: {}", err);
    }

    /// HAVING over a non-aggregate SELECT must not silently pass rows through.
    #[test]
    fn an_aggregate_in_having_without_group_by_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        engine
            .execute("INSERT INTO metrics VALUES (1000, 'a', 'eu', 1.0)")
            .unwrap();
        // count(*) in HAVING makes this an aggregate query, so the bare
        // `host` column is no longer legal.
        let err = engine
            .execute("SELECT host FROM metrics HAVING count(*) > 5")
            .unwrap_err();
        assert!(err.to_string().contains("GROUP BY"), "unexpected: {}", err);
    }

    // -----------------------------------------------------------------------
    // DISTINCT
    // -----------------------------------------------------------------------

    #[test]
    fn distinct_removes_duplicate_rows() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        engine
            .execute(
                "INSERT INTO metrics VALUES \
                 (1000, 'a', 'eu', 1.0), (2000, 'a', 'eu', 1.0), \
                 (3000, 'b', 'us', 2.0)",
            )
            .unwrap();

        let (columns, out) = rows(
            engine
                .execute("SELECT DISTINCT region FROM metrics")
                .unwrap(),
        );
        assert_eq!(columns, vec!["region"]);
        assert_eq!(out.len(), 2);
        let mut got: Vec<String> = out.iter().map(|r| str_at(r, 0)).collect();
        got.sort();
        assert_eq!(got, vec!["eu", "us"]);
    }

    /// DISTINCT applies to the projected result, so two rows that differ only
    /// in a column that was not selected really are duplicates.
    #[test]
    fn distinct_applies_after_projection() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        engine
            .execute(
                "INSERT INTO metrics VALUES \
                 (1000, 'a', 'eu', 1.0), (2000, 'b', 'eu', 2.0)",
            )
            .unwrap();

        // host differs but region does not, so projecting only region leaves
        // two identical rows.
        let (_, out) = rows(
            engine
                .execute("SELECT DISTINCT region FROM metrics")
                .unwrap(),
        );
        assert_eq!(out.len(), 1, "host was not selected, so the rows collapse");
    }

    #[test]
    fn distinct_keeps_the_first_occurrence_and_stays_with_order_by() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        engine
            .execute(
                "INSERT INTO metrics VALUES \
                 (3000, 'c', 'us', 3.0), (1000, 'a', 'eu', 1.0), \
                 (2000, 'b', 'eu', 2.0), (4000, 'd', 'us', 4.0)",
            )
            .unwrap();

        let (_, out) = rows(
            engine
                .execute(
                    "SELECT DISTINCT region FROM metrics ORDER BY region DESC",
                )
                .unwrap(),
        );
        assert_eq!(out.len(), 2);
        assert_eq!(str_at(&out[0], 0), "us");
        assert_eq!(str_at(&out[1], 0), "eu");
    }

    #[test]
    fn distinct_on_is_refused_rather_than_silently_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        let err = engine
            .execute("SELECT DISTINCT ON (region) region FROM metrics")
            .unwrap_err();
        assert!(err.to_string().contains("DISTINCT ON"), "unexpected: {}", err);
    }

    /// A regression guard: DISTINCT must not reorder the result.
    #[test]
    fn distinct_preserves_first_seen_order() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        engine
            .execute(
                "INSERT INTO metrics VALUES \
                 (1000, 'a', 'us', 1.0), (2000, 'b', 'eu', 2.0), \
                 (3000, 'c', 'us', 3.0)",
            )
            .unwrap();

        let (_, out) = rows(
            engine
                .execute("SELECT DISTINCT region FROM metrics")
                .unwrap(),
        );
        let got: Vec<String> = out.iter().map(|r| str_at(r, 0)).collect();
        // 'us' appears first in timestamp order, so it must stay first.
        assert_eq!(got, vec!["us", "eu"]);
    }

    // -----------------------------------------------------------------------
    // Secondary indexes
    // -----------------------------------------------------------------------

    #[test]
    fn create_index_covers_rows_that_already_existed() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        engine
            .execute(
                "INSERT INTO metrics VALUES \
                 (1000, 'a', 'eu', 1.0), (2000, 'b', 'us', 2.0), \
                 (3000, 'c', 'us', 3.0)",
            )
            .unwrap();

        let affected = engine
            .execute("CREATE INDEX idx_region ON metrics (region)")
            .unwrap();
        match affected {
            QueryResult::Affected(n) => assert_eq!(n, 3),
            other => panic!("expected Affected(3), got {:?}", other),
        }

        let stats = engine.index_stats("metrics");
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].0, "idx_region");
        assert_eq!(stats[0].1, vec!["region"]);
        assert_eq!(stats[0].2, 3, "index must cover the pre-existing rows");
    }

    /// A row written after the index exists must reach the index too, or a
    /// lookup would silently miss it.
    #[test]
    fn insert_maintains_the_index() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        engine
            .execute("INSERT INTO metrics VALUES (1000, 'a', 'eu', 1.0)")
            .unwrap();
        engine
            .execute("CREATE INDEX idx_host ON metrics (host)")
            .unwrap();
        assert_eq!(engine.index_stats("metrics")[0].2, 1);

        engine
            .execute("INSERT INTO metrics VALUES (2000, 'b', 'us', 2.0)")
            .unwrap();
        assert_eq!(
            engine.index_stats("metrics")[0].2,
            2,
            "an index that lags the table is worse than no index"
        );
    }

    #[test]
    fn multi_column_indexes_count_each_row_once() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        engine
            .execute(
                "INSERT INTO metrics VALUES \
                 (1000, 'a', 'eu', 1.0), (2000, 'b', 'eu', 2.0), \
                 (3000, 'c', 'us', 3.0)",
            )
            .unwrap();
        engine
            .execute("CREATE INDEX idx_pair ON metrics (region, host)")
            .unwrap();

        let stats = engine.index_stats("metrics");
        assert_eq!(stats[0].1, vec!["region", "host"]);
        assert_eq!(stats[0].2, 3);
    }

    #[test]
    fn a_duplicate_index_name_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        engine
            .execute("CREATE INDEX idx ON metrics (host)")
            .unwrap();
        let err = engine
            .execute("CREATE INDEX idx ON metrics (region)")
            .unwrap_err();
        assert!(err.to_string().contains("already exists"), "unexpected: {}", err);
    }

    #[test]
    fn an_index_on_an_unknown_column_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        let err = engine
            .execute("CREATE INDEX idx ON metrics (nosuch)")
            .unwrap_err();
        assert!(err.to_string().contains("nosuch"), "unexpected: {}", err);
    }

    #[test]
    fn an_index_on_an_unknown_table_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path());
        let err = engine
            .execute("CREATE INDEX idx ON missing (host)")
            .unwrap_err();
        assert!(err.to_string().contains("not found"), "unexpected: {}", err);
    }

    #[test]
    fn drop_index_removes_it_and_its_rows() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        engine
            .execute("INSERT INTO metrics VALUES (1000, 'a', 'eu', 1.0)")
            .unwrap();
        engine
            .execute("CREATE INDEX idx ON metrics (host)")
            .unwrap();
        assert_eq!(engine.index_stats("metrics").len(), 1);

        engine.execute("DROP INDEX idx").unwrap();
        assert!(
            engine.index_stats("metrics").is_empty(),
            "dropped index is still registered"
        );
    }

    /// Dropping a table must take its indexes with it, or a lookup would keep
    /// returning rows for a table that is gone.
    #[test]
    fn drop_table_removes_its_indexes() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        engine
            .execute("INSERT INTO metrics VALUES (1000, 'a', 'eu', 1.0)")
            .unwrap();
        engine
            .execute("CREATE INDEX idx ON metrics (host)")
            .unwrap();
        assert_eq!(engine.index_stats("metrics").len(), 1);

        engine.execute("DROP TABLE metrics").unwrap();
        assert!(engine.index_stats("metrics").is_empty());
    }

    #[test]
    fn dropping_an_unknown_index_is_an_error_unless_if_exists() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        let err = engine.execute("DROP INDEX nosuch").unwrap_err();
        assert!(err.to_string().contains("not found"), "unexpected: {}", err);
        engine.execute("DROP INDEX IF EXISTS nosuch").unwrap();
    }

    /// A NULL and an empty string must not share a key, or the index would
    /// return rows that do not match the lookup.
    #[test]
    fn null_and_empty_string_produce_different_index_keys() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        engine
            .execute("INSERT INTO metrics VALUES (1000, '', 'eu', 1.0)")
            .unwrap();
        engine
            .execute("INSERT INTO metrics VALUES (2000, NULL, 'eu', 2.0)")
            .unwrap();
        engine
            .execute("CREATE INDEX idx ON metrics (host)")
            .unwrap();

        // Two distinct keys, so two entries.
        assert_eq!(
            engine.index_stats("metrics")[0].2,
            2,
            "NULL and empty string must not collide"
        );
    }

    /// The index is built from the same encoded values the table stores, so
    /// decoding a lookup key must yield the value that was indexed.
    #[test]
    fn an_index_key_round_trips_through_the_schema() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with_metrics(dir.path());
        engine
            .execute("INSERT INTO metrics VALUES (1000, 'abc', 'eu', 1.0)")
            .unwrap();

        let table = engine.tables.get("metrics").unwrap();
        let meta = table.meta.clone();
        // The schema is ts, host, region, value, so `host` sits at slot 1.
        let key = Engine::encode_index_key(
            &[None, Some(b"abc".to_vec()), None, None],
            &["host".to_string()],
            &meta,
        )
        .unwrap();
        // 1 (present) + 3 (length) + "abc"
        assert_eq!(key, vec![1, 3, 0, 0, 0, b'a', b'b', b'c']);
    }
}
