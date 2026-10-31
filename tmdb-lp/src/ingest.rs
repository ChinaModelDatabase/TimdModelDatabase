//! Turn a line-protocol payload into table writes.

use tmdb_timestore::Table;

use crate::mapping::{MapError, MappedRow, SchemaMapper, TypePolicy, UnknownFieldPolicy};
use crate::parser::{parse_batch, LpError, Precision};

/// What an ingest call did.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct IngestReport {
    /// Lines that became rows.
    pub written: u64,
    /// Lines skipped for a structural reason (see `parse_errors`).
    pub parse_errors: u64,
    /// Lines skipped because they did not fit the schema.
    pub map_errors: u64,
    /// Fields dropped under [`UnknownFieldPolicy::Skip`].
    pub unknown_fields: u64,
    /// Values replaced with NULL under [`TypePolicy::Nullify`].
    pub type_mismatches: u64,
    /// One entry per rejected line, for the caller's error report.
    pub parse_errors_detail: Vec<(usize, LpError)>,
    pub map_errors_detail: Vec<(usize, MapError)>,
}

impl IngestReport {
    pub fn rejected(&self) -> u64 {
        self.parse_errors + self.map_errors
    }

    pub fn is_clean(&self) -> bool {
        self.rejected() == 0 && self.unknown_fields == 0 && self.type_mismatches == 0
    }
}

/// How an ingest call should treat input it cannot use.
#[derive(Debug, Clone, Copy)]
pub struct IngestOptions {
    pub precision: Precision,
    pub unknown: UnknownFieldPolicy,
    pub types: TypePolicy,
    /// Timestamp for lines that carry none. `None` rejects them.
    pub default_ts: Option<i64>,
    /// Refuse the whole batch if any line fails.
    ///
    /// The default is to accept the good lines and report the bad ones, which
    /// is what a continuous Telegraf stream needs: one malformed line should
    /// not stall the pipeline behind it.
    pub all_or_nothing: bool,
}

impl Default for IngestOptions {
    fn default() -> Self {
        Self {
            precision: Precision::Nanoseconds,
            unknown: UnknownFieldPolicy::Reject,
            types: TypePolicy::Reject,
            default_ts: None,
            all_or_nothing: false,
        }
    }
}

/// Parse `payload` and write the rows it describes into `table`.
///
/// Bad lines are reported rather than aborting the batch unless
/// [`IngestOptions::all_or_nothing`] is set, in which case the batch is
/// validated first and nothing is written if anything is wrong.
pub fn ingest(table: &Table, payload: &str, opts: IngestOptions) -> IngestReport {
    let meta = table.meta.clone();
    let mapper = SchemaMapper::new(&meta, opts.precision)
        .with_unknown_field_policy(opts.unknown)
        .with_type_policy(opts.types);

    let (points, parse_errors) = parse_batch(payload);
    let mut report = IngestReport {
        parse_errors: parse_errors.len() as u64,
        parse_errors_detail: parse_errors,
        ..Default::default()
    };

    let mut rows: Vec<MappedRow> = Vec::with_capacity(points.len());
    for (i, point) in points.iter().enumerate() {
        match mapper.map_point(point, opts.default_ts) {
            Ok(row) => rows.push(row),
            Err(e) => report.map_errors_detail.push((i + 1, e)),
        }
    }

    if opts.all_or_nothing && report.rejected() > 0 {
        report.map_errors = report.map_errors_detail.len() as u64;
        return report;
    }

    report.map_errors = report.map_errors_detail.len() as u64;
    if rows.is_empty() {
        return report;
    }

    let batch: Vec<(i64, Vec<Option<Vec<u8>>>, Vec<(String, String)>)> = rows
        .into_iter()
        .map(|r| (r.ts, r.values, r.tags))
        .collect();

    table.insert_batch_with_tags(&batch);
    report.written = batch.len() as u64;
    report
}

/// Count what a mapper would reject, without writing anything.
///
/// Useful for a dry run before switching a live pipeline over, and used by the
/// tests to assert that the counters in [`IngestReport`] are honest.
pub fn validate(table: &Table, payload: &str, opts: IngestOptions) -> IngestReport {
    let meta = table.meta.clone();
    let mapper = SchemaMapper::new(&meta, opts.precision)
        .with_unknown_field_policy(opts.unknown)
        .with_type_policy(opts.types);

    let (points, parse_errors) = parse_batch(payload);
    let mut report = IngestReport {
        parse_errors: parse_errors.len() as u64,
        parse_errors_detail: parse_errors,
        ..Default::default()
    };
    let mut ok = 0u64;
    for (i, point) in points.iter().enumerate() {
        match mapper.map_point(point, opts.default_ts) {
            Ok(_) => ok += 1,
            Err(e) => report.map_errors_detail.push((i + 1, e)),
        }
    }
    report.map_errors = report.map_errors_detail.len() as u64;
    report.written = ok;
    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapping::{meta_for_fields, meta_for_fields_with_tags};
    use tmdb_timestore::TsUnit;

    fn table(dir: &tempfile::TempDir) -> Table {
        let meta = meta_for_fields(
            1,
            "weather",
            &["temperature".to_string(), "humidity".to_string()],
            TsUnit::Millis,
        );
        Table::new(meta, dir.path())
    }

    #[test]
    fn ingests_a_multi_line_payload() {
        let dir = tempfile::tempdir().unwrap();
        let t = table(&dir);
        let payload = "\
# telegraf
weather,loc=us temperature=82,humidity=71 1700000000000000000
weather,loc=us temperature=83,humidity=72 1700000001000000000
weather,loc=eu temperature=18 1700000002000000000
";
        let report = ingest(
            &t,
            payload,
            IngestOptions {
                precision: Precision::Nanoseconds,
                ..Default::default()
            },
        );
        assert_eq!(report.written, 3);
        assert_eq!(report.rejected(), 0);
        assert!(report.is_clean());
        assert_eq!(t.row_count(), 3);

        t.flush().unwrap();
        let rows = t.scan(0, i64::MAX, &[], None).unwrap();
        assert_eq!(rows.len(), 3);
        // Stored in the table's own unit, not the wire unit.
        assert!(rows.iter().all(|r| r.0 >= 1_700_000_000_000));
    }

    #[test]
    fn a_bad_line_does_not_stop_the_batch() {
        let dir = tempfile::tempdir().unwrap();
        let t = table(&dir);
        let payload = "\
weather temperature=1 1700000000000000000
weather temperature=notanumber 1700000001000000000
weather temperature=2 1700000002000000000
";
        let report = ingest(
            &t,
            payload,
            IngestOptions {
                precision: Precision::Nanoseconds,
                ..Default::default()
            },
        );
        assert_eq!(report.written, 2);
        assert_eq!(report.parse_errors, 1);
        assert_eq!(report.parse_errors_detail[0].0, 2);
        assert_eq!(t.row_count(), 2);
    }

    #[test]
    fn all_or_nothing_writes_nothing_when_a_line_is_bad() {
        let dir = tempfile::tempdir().unwrap();
        let t = table(&dir);
        let payload = "\
weather temperature=1 1700000000000000000
weather temperature=oops 1700000001000000000
";
        let report = ingest(
            &t,
            payload,
            IngestOptions {
                precision: Precision::Nanoseconds,
                all_or_nothing: true,
                ..Default::default()
            },
        );
        assert_eq!(report.written, 0);
        assert_eq!(report.rejected(), 1);
        assert_eq!(t.row_count(), 0, "a rejected batch must not leave rows behind");
    }

    #[test]
    fn an_unknown_field_is_reported_with_its_line_number() {
        let dir = tempfile::tempdir().unwrap();
        let t = table(&dir);
        let payload = "\
weather temperature=1 1700000000000000000
weather temperature=1,nope=2 1700000001000000000
";
        let report = ingest(
            &t,
            payload,
            IngestOptions {
                precision: Precision::Nanoseconds,
                ..Default::default()
            },
        );
        assert_eq!(report.written, 1);
        assert_eq!(report.map_errors, 1);
        assert!(matches!(
            report.map_errors_detail[0].1,
            MapError::UnknownField { .. }
        ));
    }

    #[test]
    fn validate_predicts_ingest_without_writing() {
        let dir = tempfile::tempdir().unwrap();
        let t = table(&dir);
        let payload = "weather temperature=1 1700000000000000000\nbroken line\n";
        let report = validate(
            &t,
            payload,
            IngestOptions {
                precision: Precision::Nanoseconds,
                ..Default::default()
            },
        );
        assert_eq!(report.written, 1);
        assert_eq!(report.parse_errors, 1);
        assert_eq!(t.row_count(), 0, "validate must not write");
    }

    #[test]
    fn tags_reach_the_series_index() {
        let dir = tempfile::tempdir().unwrap();
        let t = table(&dir);
        ingest(
            &t,
            "weather,loc=us,sensor=a temperature=1 1700000000000000000\n\
             weather,loc=us,sensor=b temperature=2 1700000000000000000\n",
            IngestOptions {
                precision: Precision::Nanoseconds,
                ..Default::default()
            },
        );
        t.flush().unwrap();
        let found = t.find_series_by_tags(&[("loc".to_string(), "us".to_string())]);
        assert_eq!(found.map(|v| v.len()), Some(2), "both sensors are one series group");
    }

    /// The end-to-end guard on column alignment.
    ///
    /// With a tag column declared, a row sized to the value columns alone is
    /// shifted left by one. `build_columns` then feeds `temperature`'s bytes to
    /// the tag column and panics on the type mismatch -- or, when the types
    /// happen to line up, writes the wrong field into the wrong column and
    /// returns wrong data with no error at all.
    #[test]
    fn a_tag_column_schema_round_trips_through_a_segment() {
        let dir = tempfile::tempdir().unwrap();
        let meta = meta_for_fields_with_tags(
            1,
            "weather",
            &["temperature".to_string(), "humidity".to_string()],
            &["loc".to_string()],
            TsUnit::Millis,
        );
        let t = Table::new(meta.clone(), dir.path());

        let report = ingest(
            &t,
            "weather,loc=us temperature=82.5,humidity=71 1700000000000",
            IngestOptions {
                precision: Precision::Milliseconds,
                ..Default::default()
            },
        );
        assert_eq!(report.written, 1);
        assert!(report.is_clean(), "{:?}", report.map_errors_detail);

        // Force the row through build_columns, where a shift would blow up.
        t.flush().unwrap();

        let rows = t.scan(0, i64::MAX, &[], None).unwrap();
        assert_eq!(rows.len(), 1);
        let (_, values) = &rows[0];
        assert_eq!(values.len(), meta.columns.len());

        let at = |name: &str| {
            meta.columns
                .iter()
                .position(|c| c.name == name)
                .expect("column exists")
        };
        assert_eq!(values[at("loc")].as_deref(), Some(&b"us"[..]));
        assert_eq!(
            values[at("temperature")].as_ref().unwrap(),
            &82.5f64.to_le_bytes()
        );
        assert_eq!(
            values[at("humidity")].as_ref().unwrap(),
            &71.0f64.to_le_bytes()
        );
    }
}