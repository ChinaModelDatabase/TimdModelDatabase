//! Line protocol for tmDB.
//!
//! Parses the InfluxDB 1.x line protocol that Telegraf and the community
//! clients emit, and maps each line onto a table's fixed column schema.
//!
//! ```
//! use tmdb_lp::{ingest, IngestOptions, Precision};
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let dir = tempfile::tempdir()?;
//! # let meta = tmdb_lp::meta_for_fields(1, "cpu", &["usage".to_string()], tmdb_timestore::TsUnit::Seconds);
//! # let table = tmdb_timestore::Table::new(meta, dir.path());
//! let report = ingest(
//!     &table,
//!     "cpu,host=a,region=w usage=93.5 1700000000",
//!     IngestOptions { precision: Precision::Seconds, ..Default::default() },
//! );
//! assert_eq!(report.written, 1);
//! assert!(report.is_clean());
//! # Ok(()) }
//! ```

mod ingest;
mod mapping;
mod parser;

pub use ingest::{ingest, validate, IngestOptions, IngestReport};
pub use mapping::{
    meta_for_fields, meta_for_fields_with_tags, MapError, MappedRow, SchemaMapper, TypePolicy,
    UnknownFieldPolicy,
};
pub use parser::{parse_batch, parse_line, FieldValue, LpError, LpPoint, Precision};