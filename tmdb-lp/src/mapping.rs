//! Map parsed points onto a table's fixed schema.
//!
//! tmDB tables have a fixed set of typed columns, so line protocol fields
//! are matched by name against that schema rather than growing a column on the
//! fly. A field with no matching column is an error: silently dropping it would
//! mean a Telegraf config typo loses data with no visible sign.

use tmdb_timestore::encoding::DataType;
use tmdb_timestore::{ColumnDef, TableMeta, TsUnit};

use crate::parser::{FieldValue, LpPoint, Precision};

/// A point that could not be mapped onto the table.
#[derive(Debug, Clone, PartialEq)]
pub enum MapError {
    /// The measurement did not name this table.
    WrongMeasurement {
        got: String,
        wanted: String,
    },
    /// A field in the line has no column in the schema.
    UnknownField { field: String },
    /// The same field appeared twice in one line.
    DuplicateField { field: String },
    /// A field could not be represented in its column's type.
    TypeMismatch {
        field: String,
        column_type: DataType,
        value: FieldValue,
    },
    /// A column's value overflowed its storage width.
    Overflow { field: String, column_type: DataType },
    /// The line had no timestamp and the caller supplied no default.
    MissingTimestamp,
    /// The timestamp could not be rescaled into the table's unit.
    TimestampOverflow,
}

impl std::fmt::Display for MapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MapError::WrongMeasurement { got, wanted } => {
                write!(f, "line names measurement {:?} but table is {:?}", got, wanted)
            }
            MapError::UnknownField { field } => write!(
                f,
                "field {:?} has no column in the table schema",
                field
            ),
            MapError::DuplicateField { field } => {
                write!(f, "field {:?} appears twice in one line", field)
            }
            MapError::TypeMismatch {
                field,
                column_type,
                value,
            } => write!(
                f,
                "field {:?} value {:?} does not fit column type {}",
                field,
                value,
                column_type
            ),
            MapError::Overflow { field, column_type } => write!(
                f,
                "field {:?} does not fit in a {} column",
                field,
                column_type
            ),
            MapError::MissingTimestamp => write!(
                f,
                "line has no timestamp and the ingest call supplied no default"
            ),
            MapError::TimestampOverflow => {
                write!(f, "timestamp does not fit the table's time unit")
            }
        }
    }
}

impl std::error::Error for MapError {}

/// How to treat a field that is not in the schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnknownFieldPolicy {
    /// Reject the line.
    Reject,
    /// Skip the field and count it.
    Skip,
}

/// How to treat a field whose value does not fit its column type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypePolicy {
    /// Reject the line.
    Reject,
    /// Store a NULL and count it.
    Nullify,
}

/// A row ready for `Table::insert_batch_with_tags`.
#[derive(Debug, Clone, PartialEq)]
pub struct MappedRow {
    pub ts: i64,
    pub values: Vec<Option<Vec<u8>>>,
    pub tags: Vec<(String, String)>,
}

/// Maps points onto one table's schema.
pub struct SchemaMapper<'a> {
    meta: &'a TableMeta,
    precision: Precision,
    unknown: UnknownFieldPolicy,
    types: TypePolicy,
}

impl<'a> SchemaMapper<'a> {
    pub fn new(meta: &'a TableMeta, precision: Precision) -> Self {
        Self {
            meta,
            precision,
            unknown: UnknownFieldPolicy::Reject,
            types: TypePolicy::Reject,
        }
    }

    pub fn with_unknown_field_policy(mut self, policy: UnknownFieldPolicy) -> Self {
        self.unknown = policy;
        self
    }

    pub fn with_type_policy(mut self, policy: TypePolicy) -> Self {
        self.types = policy;
        self
    }

    /// Map one point.
    ///
    /// `default_ts` is used when the line carried no timestamp, which is the
    /// normal case for a line written by hand but rare for Telegraf.
    pub fn map_point(
        &self,
        point: &LpPoint,
        default_ts: Option<i64>,
    ) -> Result<MappedRow, MapError> {
        if point.measurement != self.meta.name {
            return Err(MapError::WrongMeasurement {
                got: point.measurement.clone(),
                wanted: self.meta.name.clone(),
            });
        }

        let raw_ts = match point.timestamp.or(default_ts) {
            Some(t) => t,
            None => return Err(MapError::MissingTimestamp),
        };
        let ts = self
            .precision
            .to_ts_unit(raw_ts, self.meta.ts_unit)
            .ok_or(MapError::TimestampOverflow)?;

        // `Table::build_columns` reads `values[i]` as `meta.columns[i]`, and
        // `Table::scan` returns values in that same order. So this vector must
        // be aligned to the *whole* schema, tag columns included. Sizing it to
        // the value columns alone shifts every column after the first tag, and
        // `build_columns` then either panics on the type mismatch or writes one
        // field's bytes into another field's column.
        let mut values: Vec<Option<Vec<u8>>> = vec![None; self.meta.columns.len()];

        // Tag columns are not carried in `values` -- tags live in the tag index
        // -- but writing them through as well keeps a plain column scan able to
        // evaluate a tag predicate without a second lookup.
        for (name, value) in &point.tags {
            if let Some(pos) = self.meta.columns.iter().position(|c| c.is_tag && c.name == *name) {
                if matches!(self.meta.columns[pos].data_type, DataType::String) {
                    values[pos] = Some(value.clone().into_bytes());
                }
            }
        }

        let mut seen: Vec<&str> = Vec::with_capacity(point.fields.len());
        for (key, value) in &point.fields {
            if seen.contains(&key.as_str()) {
                return Err(MapError::DuplicateField {
                    field: key.clone(),
                });
            }
            seen.push(key.as_str());

            let pos = match self.meta.columns.iter().position(|c| c.name == *key) {
                Some(p) => p,
                None => match self.unknown {
                    UnknownFieldPolicy::Reject => {
                        return Err(MapError::UnknownField { field: key.clone() })
                    }
                    UnknownFieldPolicy::Skip => continue,
                },
            };
            // A tag column cannot also be a field target: the tag index already
            // owns that name, and letting both write it would make the row and
            // the index disagree.
            if self.meta.columns[pos].is_tag {
                return Err(MapError::TypeMismatch {
                    field: key.clone(),
                    column_type: self.meta.columns[pos].data_type,
                    value: value.clone(),
                });
            }
            let col = &self.meta.columns[pos];

            match encode_value(value, col.data_type) {
                Ok(bytes) => values[pos] = Some(bytes),
                Err(EncodeError::Type) => match self.types {
                    TypePolicy::Reject => {
                        return Err(MapError::TypeMismatch {
                            field: key.clone(),
                            column_type: col.data_type,
                            value: value.clone(),
                        })
                    }
                    TypePolicy::Nullify => values[pos] = None,
                },
                Err(EncodeError::Overflow) => {
                    return Err(MapError::Overflow {
                        field: key.clone(),
                        column_type: col.data_type,
                    })
                }
            }
        }

        // The timestamp column, if the schema declares one, mirrors the row's
        // timestamp so a plain column scan can read it back.
        if let Some(ts_col) = self.meta.ts_column() {
            if let Some(pos) = self.meta.columns.iter().position(|c| c.name == ts_col.name) {
                if values[pos].is_none() {
                    values[pos] = Some(ts.to_le_bytes().to_vec());
                }
            }
        }

        Ok(MappedRow {
            ts,
            values,
            tags: point.tags.clone(),
        })
    }
}

enum EncodeError {
    Type,
    Overflow,
}

/// Encode one line-protocol value into the byte form the column expects.
///
/// The widths match what `ColumnData::push_raw` reads back, so a value written
/// here decodes to exactly what the caller sent.
fn encode_value(v: &FieldValue, ty: DataType) -> Result<Vec<u8>, EncodeError> {
    match (v, ty) {
        // Integer-typed columns.
        (FieldValue::Int(i), DataType::Int64 | DataType::Timestamp) => {
            Ok((*i).to_le_bytes().to_vec())
        }
        (FieldValue::Int(i), DataType::Int32) => i32::try_from(*i)
            .map(|n| n.to_le_bytes().to_vec())
            .map_err(|_| EncodeError::Overflow),
        (FieldValue::UInt(u), DataType::Int64 | DataType::Timestamp) => i64::try_from(*u)
            .map(|n| n.to_le_bytes().to_vec())
            .map_err(|_| EncodeError::Overflow),
        (FieldValue::UInt(u), DataType::Int32) => i32::try_from(*u)
            .map(|n| n.to_le_bytes().to_vec())
            .map_err(|_| EncodeError::Overflow),

        // Integer fields into float columns. Line protocol marks integers with
        // a suffix, but the caller may well have declared a DOUBLE column for
        // a metric that is only ever whole numbers (`humidity=71i`), so this is
        // the common path rather than a corner case. Above 2^53 the conversion
        // rounds, which is inherent to a DOUBLE column and not something the
        // caller can detect afterwards.
        (FieldValue::Int(i), DataType::Float64) => Ok((*i as f64).to_le_bytes().to_vec()),
        (FieldValue::Int(i), DataType::Float32) => Ok((*i as f32).to_le_bytes().to_vec()),
        (FieldValue::UInt(u), DataType::Float64) => Ok((*u as f64).to_le_bytes().to_vec()),
        (FieldValue::UInt(u), DataType::Float32) => Ok((*u as f32).to_le_bytes().to_vec()),

        // Float-typed columns.
        (FieldValue::Float(f), DataType::Float64) => Ok(f.to_le_bytes().to_vec()),
        (FieldValue::Float(f), DataType::Float32) => Ok(((*f as f32).to_le_bytes()).to_vec()),
        // A float written without a decimal point is integral, so widening it
        // into an integer column loses nothing.
        (FieldValue::Float(f), DataType::Int64 | DataType::Timestamp) => {
            if f.fract() != 0.0 || !f.is_finite() {
                return Err(EncodeError::Type);
            }
            if *f < i64::MIN as f64 || *f > i64::MAX as f64 {
                return Err(EncodeError::Overflow);
            }
            Ok((*f as i64).to_le_bytes().to_vec())
        }
        (FieldValue::Float(f), DataType::Int32) => {
            if f.fract() != 0.0 || !f.is_finite() {
                return Err(EncodeError::Type);
            }
            i32::try_from(*f as i64)
                .map(|n| n.to_le_bytes().to_vec())
                .map_err(|_| EncodeError::Overflow)
        }

        (FieldValue::Bool(b), DataType::Bool) => Ok(vec![if *b { 1 } else { 0 }]),
        (FieldValue::Str(s), DataType::String) => Ok(s.as_bytes().to_vec()),

        _ => Err(EncodeError::Type),
    }
}

/// Build the schema a table needs to accept a given set of fields.
///
/// This exists so an application can create the table first and then accept
/// Telegraf traffic, without hand-writing a `TableMeta`. Fields become
/// `Float64` columns and get a leading `Timestamp` column, which is the shape
/// line protocol data almost always has.
pub fn meta_for_fields(
    table_id: u64,
    table_name: &str,
    fields: &[String],
    ts_unit: TsUnit,
) -> TableMeta {
    meta_for_fields_with_tags(table_id, table_name, fields, &[], ts_unit)
}

/// As [`meta_for_fields`], but also declares tag columns.
///
/// Tags go in front of the value columns to match how a tag-first schema is
/// normally written, which is also the layout that exposes column alignment
/// mistakes: a mapper that sized its row to the value columns alone would shift
/// every field left by the number of tags.
pub fn meta_for_fields_with_tags(
    table_id: u64,
    table_name: &str,
    fields: &[String],
    tags: &[String],
    ts_unit: TsUnit,
) -> TableMeta {
    let lz4 = tmdb_timestore::encoding::CompressionLevel::Lz4;
    let mut columns = vec![ColumnDef {
        name: "time".to_string(),
        data_type: DataType::Timestamp,
        is_tag: false,
        nullable: false,
        compression: lz4,
    }];
    for t in tags {
        columns.push(ColumnDef {
            name: t.clone(),
            data_type: DataType::String,
            is_tag: true,
            nullable: false,
            compression: lz4,
        });
    }
    for f in fields {
        columns.push(ColumnDef {
            name: f.clone(),
            data_type: DataType::Float64,
            is_tag: false,
            nullable: true,
            compression: lz4,
        });
    }
    TableMeta {
        table_id,
        name: table_name.to_string(),
        ts_unit,
        columns,
        chunk_interval_ms: 0,
        retention_days: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_line;

    fn meta() -> TableMeta {
        meta_for_fields(
            1,
            "weather",
            &["temperature".to_string(), "humidity".to_string()],
            TsUnit::Millis,
        )
    }

    #[test]
    fn maps_a_line_onto_the_schema() {
        let m = meta();
        let mapper = SchemaMapper::new(&m, Precision::Seconds);
        let p = parse_line("weather,loc=us temperature=82,humidity=71 1700000000").unwrap();
        let row = mapper.map_point(&p, None).unwrap();

        assert_eq!(row.ts, 1_700_000_000_000);
        assert_eq!(row.tags, vec![("loc".to_string(), "us".to_string())]);
        // time, temperature, humidity
        assert_eq!(row.values.len(), 3);
        assert_eq!(row.values[0].as_ref().unwrap(), &1_700_000_000_000i64.to_le_bytes());
        assert_eq!(row.values[1].as_ref().unwrap(), &82.0f64.to_le_bytes());
    }

    #[test]
    fn an_absent_field_stays_null_and_does_not_shift_columns() {
        let m = meta();
        let mapper = SchemaMapper::new(&m, Precision::Seconds);
        let p = parse_line("weather temperature=82 1").unwrap();
        let row = mapper.map_point(&p, None).unwrap();
        assert!(row.values[1].is_some(), "temperature");
        assert_eq!(row.values[2], None, "humidity must be NULL, not shifted");
    }

    #[test]
    fn integer_field_widens_into_a_float_column() {
        let m = meta();
        let mapper = SchemaMapper::new(&m, Precision::Seconds);
        let p = parse_line("weather temperature=82i 1").unwrap();
        let row = mapper.map_point(&p, None).unwrap();
        assert_eq!(row.values[1].as_ref().unwrap(), &82.0f64.to_le_bytes());
    }

    #[test]
    fn integral_float_narrows_into_an_int_column() {
        let m = TableMeta {
            columns: vec![
                ColumnDef {
                    name: "time".into(),
                    data_type: DataType::Timestamp,
                    is_tag: false,
                    nullable: false,
                    compression: tmdb_timestore::encoding::CompressionLevel::Lz4,
                },
                ColumnDef {
                    name: "count".into(),
                    data_type: DataType::Int64,
                    is_tag: false,
                    nullable: true,
                    compression: tmdb_timestore::encoding::CompressionLevel::Lz4,
                },
            ],
            ..meta()
        };
        let mapper = SchemaMapper::new(&m, Precision::Seconds);
        let p = parse_line("weather count=7 1").unwrap();
        let row = mapper.map_point(&p, None).unwrap();
        assert_eq!(row.values[1].as_ref().unwrap(), &7i64.to_le_bytes());
    }

    #[test]
    fn a_fractional_value_will_not_silently_truncate_into_an_int_column() {
        let m = TableMeta {
            columns: vec![
                ColumnDef {
                    name: "time".into(),
                    data_type: DataType::Timestamp,
                    is_tag: false,
                    nullable: false,
                    compression: tmdb_timestore::encoding::CompressionLevel::Lz4,
                },
                ColumnDef {
                    name: "count".into(),
                    data_type: DataType::Int64,
                    is_tag: false,
                    nullable: true,
                    compression: tmdb_timestore::encoding::CompressionLevel::Lz4,
                },
            ],
            ..meta()
        };
        let mapper = SchemaMapper::new(&m, Precision::Seconds);
        let p = parse_line("weather count=7.5 1").unwrap();
        assert!(matches!(
            mapper.map_point(&p, None),
            Err(MapError::TypeMismatch { .. })
        ));

        // The caller can opt into a NULL instead of losing the row.
        let lenient = SchemaMapper::new(&m, Precision::Seconds).with_type_policy(TypePolicy::Nullify);
        let row = lenient.map_point(&p, None).unwrap();
        assert_eq!(row.values[1], None);
    }

    #[test]
    fn unknown_field_is_rejected_by_default_and_skippable_on_request() {
        let m = meta();
        let mapper = SchemaMapper::new(&m, Precision::Seconds);
        let p = parse_line("weather temperature=1,nosuch=2 1").unwrap();
        assert_eq!(
            mapper.map_point(&p, None),
            Err(MapError::UnknownField {
                field: "nosuch".into()
            })
        );

        let lenient =
            SchemaMapper::new(&m, Precision::Seconds).with_unknown_field_policy(UnknownFieldPolicy::Skip);
        assert!(lenient.map_point(&p, None).is_ok());
    }

    #[test]
    fn a_line_naming_another_measurement_is_refused() {
        let m = meta();
        let mapper = SchemaMapper::new(&m, Precision::Seconds);
        let p = parse_line("cpu temperature=1 1").unwrap();
        assert!(matches!(
            mapper.map_point(&p, None),
            Err(MapError::WrongMeasurement { .. })
        ));
    }

    #[test]
    fn a_missing_timestamp_needs_a_default() {
        let m = meta();
        let mapper = SchemaMapper::new(&m, Precision::Seconds);
        let p = parse_line("weather temperature=1").unwrap();
        assert_eq!(mapper.map_point(&p, None), Err(MapError::MissingTimestamp));
        assert_eq!(mapper.map_point(&p, Some(42)).unwrap().ts, 42_000);
    }

    #[test]
    fn int32_overflow_is_reported() {
        let m = TableMeta {
            columns: vec![
                ColumnDef {
                    name: "time".into(),
                    data_type: DataType::Timestamp,
                    is_tag: false,
                    nullable: false,
                    compression: tmdb_timestore::encoding::CompressionLevel::Lz4,
                },
                ColumnDef {
                    name: "small".into(),
                    data_type: DataType::Int32,
                    is_tag: false,
                    nullable: true,
                    compression: tmdb_timestore::encoding::CompressionLevel::Lz4,
                },
            ],
            ..meta()
        };
        let mapper = SchemaMapper::new(&m, Precision::Seconds);
        let p = parse_line("weather small=5000000000i 1").unwrap();
        assert!(matches!(
            mapper.map_point(&p, None),
            Err(MapError::Overflow { .. })
        ));
    }

    #[test]
    fn values_stay_aligned_when_the_schema_declares_tags() {
        let m = meta_for_fields_with_tags(
            1,
            "weather",
            &["temperature".to_string(), "humidity".to_string()],
            &["loc".to_string(), "sensor".to_string()],
            TsUnit::Millis,
        );
        // time, loc, sensor, temperature, humidity
        assert_eq!(m.columns.len(), 5);

        let mapper = SchemaMapper::new(&m, Precision::Seconds);
        let p = parse_line("weather,loc=us,sensor=a temperature=82,humidity=71 1").unwrap();
        let row = mapper.map_point(&p, None).unwrap();

        assert_eq!(
            row.values.len(),
            m.columns.len(),
            "a short row would shift every field left and build_columns would \
             then write one field's bytes into another field's column"
        );
        assert_eq!(row.values[3].as_ref().unwrap(), &82.0f64.to_le_bytes());
        assert_eq!(row.values[4].as_ref().unwrap(), &71.0f64.to_le_bytes());
        // Tags ride along in the row as well as the index.
        assert_eq!(row.values[1].as_deref(), Some(&b"us"[..]));
        assert_eq!(row.values[2].as_deref(), Some(&b"a"[..]));
    }

    #[test]
    fn a_field_may_not_overwrite_a_tag_column() {
        let m = meta_for_fields_with_tags(
            1,
            "weather",
            &["temperature".to_string()],
            &["loc".to_string()],
            TsUnit::Millis,
        );
        let mapper = SchemaMapper::new(&m, Precision::Seconds);
        let p = parse_line("weather,loc=us loc=\"spoofed\",temperature=1 1").unwrap();
        assert!(
            matches!(mapper.map_point(&p, None), Err(MapError::TypeMismatch { .. })),
            "the tag index owns this name; a field must not silently overwrite it"
        );
    }
}