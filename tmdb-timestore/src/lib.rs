pub mod encoding;
pub mod segment;
pub mod memtable;
pub mod compaction;
pub mod cardinality;
pub mod metrics;
pub mod shard;
pub mod wal;
pub mod ttl;
pub mod table;

pub use encoding::{CompressionLevel, DataType, EncodingType, ZoneMap};
pub use memtable::MemTable;
pub use compaction::{CompactionManager, CompactionLevel, SegmentRef};
pub use cardinality::{CardinalityGuard, CardinalityLimits, CardinalityError};
pub use metrics::TableMetrics;
pub use shard::ShardedTable;
pub use wal::{Wal, WalRecord, WalStopReason, Recovery as WalRecovery};
pub use ttl::{TtlManager, TtlConfig};
pub use table::Table;

/// File holding a table's schema inside its data directory.
///
/// Segments and the log carry rows but not the column list they belong to, so
/// this file is the only thing that lets a table be reopened after a restart.
pub const META_FILE_NAME: &str = "tmdb.meta";

/// Timestamp unit
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TsUnit {
    Seconds = 0,
    Millis = 1,
    Micros = 2,
    Nanos = 3,
}

impl TsUnit {
    /// Number of this unit in one millisecond.
    ///
    /// Kept as `f64`-free integer math so window arithmetic stays exact.
    pub fn per_milli(self) -> i64 {
        match self {
            TsUnit::Seconds => 0,
            TsUnit::Millis => 1,
            TsUnit::Micros => 1_000,
            TsUnit::Nanos => 1_000_000,
        }
    }

    /// Convert a timestamp to milliseconds.
    ///
    /// Seconds are scaled rather than rejected, so a second-precision table can
    /// still be partitioned by the same window arithmetic.
    pub fn to_millis(self, ts: i64) -> i64 {
        match self {
            TsUnit::Seconds => ts.saturating_mul(1_000),
            TsUnit::Millis => ts,
            TsUnit::Micros => ts / 1_000,
            TsUnit::Nanos => ts / 1_000_000,
        }
    }

    /// Convert milliseconds back into this unit.
    pub fn from_millis(self, ms: i64) -> i64 {
        match self {
            TsUnit::Seconds => ms / 1_000,
            TsUnit::Millis => ms,
            TsUnit::Micros => ms.saturating_mul(1_000),
            TsUnit::Nanos => ms.saturating_mul(1_000_000),
        }
    }

    /// Number of this unit in one day.
    ///
    /// Retention is expressed in days but compared against timestamps in this
    /// unit. Converting through milliseconds loses sub-millisecond precision and
    /// rounds wrong for negative values, so day arithmetic multiplies directly.
    pub fn per_day(self) -> i64 {
        const SECONDS_PER_DAY: i64 = 24 * 60 * 60;
        match self {
            TsUnit::Seconds => SECONDS_PER_DAY,
            TsUnit::Millis => SECONDS_PER_DAY * 1_000,
            TsUnit::Micros => SECONDS_PER_DAY * 1_000_000,
            TsUnit::Nanos => SECONDS_PER_DAY * 1_000_000_000,
        }
    }

    /// The current wall-clock time expressed in this unit.
    pub fn now(self) -> i64 {
        let d = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        match self {
            TsUnit::Seconds => d as i64,
            TsUnit::Millis => (d as i64).saturating_mul(1_000),
            TsUnit::Micros => (d as i64).saturating_mul(1_000_000),
            TsUnit::Nanos => (d as i64).saturating_mul(1_000_000_000),
        }
    }
}

/// Column definition for a time-series table
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub is_tag: bool,
    pub nullable: bool,
    pub compression: CompressionLevel,
}

/// Table metadata
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TableMeta {
    pub table_id: u64,
    pub name: String,
    pub ts_unit: TsUnit,
    pub columns: Vec<ColumnDef>,
    pub chunk_interval_ms: u64,
    pub retention_days: u32,
}

impl TableMeta {
    pub fn ts_column(&self) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.data_type == DataType::Timestamp)
    }

    pub fn tag_columns(&self) -> Vec<&ColumnDef> {
        self.columns.iter().filter(|c| c.is_tag).collect()
    }

    pub fn value_columns(&self) -> Vec<&ColumnDef> {
        self.columns.iter().filter(|c| !c.is_tag).collect()
    }

    /// Write the schema to `path`, replacing whatever was there atomically.
    ///
    /// A half-written schema is worse than none: the table would reopen with a
    /// column list that agrees with neither the new nor the old definition.
    /// Writing to a sibling and renaming means a crash leaves one or the other.
    pub fn write_to_file(&self, path: &std::path::Path) -> anyhow::Result<()> {
        use anyhow::Context;

        let tmp = path.with_extension("tmp");
        let mut file = std::fs::File::create(&tmp)
            .with_context(|| format!("create schema file {}", tmp.display()))?;
        serde_json::to_writer(&mut file, self)
            .with_context(|| format!("serialize schema to {}", tmp.display()))?;
        file.sync_all()
            .with_context(|| format!("fsync schema file {}", tmp.display()))?;
        drop(file);
        std::fs::rename(&tmp, path)
            .with_context(|| format!("install schema file {}", path.display()))?;
        if let Some(dir) = path.parent() {
            // The rename itself has to reach the disk, or a crash can leave a
            // directory entry pointing at a file that was never written.
            let dir_file =
                std::fs::File::open(dir).with_context(|| format!("open {}", dir.display()))?;
            dir_file
                .sync_all()
                .with_context(|| format!("fsync {}", dir.display()))?;
        }
        Ok(())
    }

    /// Read a schema written by [`TableMeta::write_to_file`].
    pub fn read_from_file(path: &std::path::Path) -> anyhow::Result<Self> {
        use anyhow::Context;

        let bytes =
            std::fs::read(path).with_context(|| format!("read schema file {}", path.display()))?;
        serde_json::from_slice(&bytes)
            .with_context(|| format!("parse schema file {}", path.display()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use encoding::{
        BitPackEncoder, ColumnCodec, ColumnData, DeltaOfDeltaEncoder, DictEncoder, Encoder,
        GorillaEncoder, RleEncoder,
    };

    /// Every encoder must round-trip *varying* data exactly. The previous
    /// version of this test used only constant and perfectly regular values,
    /// which is precisely the input the broken encoders happened to handle.
    #[test]
    fn test_encoding_roundtrip_on_varying_data() {
        // Timestamps with jitter and gaps.
        let timestamps: Vec<i64> = (0..1000)
            .map(|i| 1_700_000_000_000 + i as i64 * 1000 + (i % 11) as i64 * 37)
            .collect();
        let nulls = vec![false; timestamps.len()];

        let enc = DeltaOfDeltaEncoder::new()
            .encode_i64(&timestamps, &nulls)
            .unwrap();
        let dec = DeltaOfDeltaEncoder::new().decode_i64(&enc, timestamps.len()).unwrap();
        assert_eq!(dec, timestamps, "delta-of-delta lost timestamps");

        // Drifting floats, not a constant.
        let values: Vec<f64> = (0..1000).map(|i| 20.0 + (i as f64).sin() * 3.5).collect();
        let enc = GorillaEncoder::new().encode_f64(&values, &nulls).unwrap();
        let dec = GorillaEncoder::new().decode_f64(&enc, values.len()).unwrap();
        for (i, (a, b)) in dec.iter().zip(values.iter()).enumerate() {
            assert_eq!(a.to_bits(), b.to_bits(), "gorilla lost float at {}", i);
        }

        // Mostly-repeating integers.
        let values: Vec<i64> = (0..1000).map(|i| if i % 100 == 0 { 7 } else { 42 }).collect();
        let enc = RleEncoder::new().encode_i64(&values, &nulls).unwrap();
        let dec = RleEncoder::new().decode_i64(&enc, values.len()).unwrap();
        assert_eq!(dec, values, "rle lost integers");

        // Low-cardinality tags.
        let values: Vec<i64> = (0..1000).map(|i| (i % 5) as i64).collect();
        let enc = DictEncoder::new().encode_i64(&values, &nulls).unwrap();
        let dec = DictEncoder::new().decode_i64(&enc, values.len()).unwrap();
        assert_eq!(dec, values, "dictionary lost integers");

        // Small non-negative values.
        let values: Vec<i64> = (0..1000).map(|i| (i % 16) as i64).collect();
        let enc = BitPackEncoder::new().encode_i64(&values, &nulls).unwrap();
        let dec = BitPackEncoder::new().decode_i64(&enc, values.len()).unwrap();
        assert_eq!(dec, values, "bitpack lost integers");
    }

    #[test]
    fn test_constant_series_still_collapses() {
        let values = vec![42i64; 1000];
        let enc = RleEncoder::new()
            .encode_i64(&values, &vec![false; 1000])
            .unwrap();
        assert_eq!(enc.len(), 13, "1 marker + 4 count + 8 value");

        let values = vec![3.14f64; 1000];
        let enc = GorillaEncoder::new()
            .encode_f64(&values, &vec![false; 1000])
            .unwrap();
        assert!(enc.len() < 200, "constant floats took {} bytes", enc.len());
    }

    /// Every datatype must survive a full codec round trip, including
    /// variable-width strings and nulls.
    #[test]
    fn test_column_codec_all_types() {
        let cases = vec![
            ColumnData::Timestamp((0..500).map(|i| Some(i as i64 * 100)).collect()),
            ColumnData::Int64((0..500).map(|i| Some(i as i64)).collect()),
            ColumnData::Int32((0..500).map(|i| Some(i as i32)).collect()),
            ColumnData::Float64((0..500).map(|i| Some(i as f64 * 0.25)).collect()),
            ColumnData::Float32((0..500).map(|i| Some(i as f32 * 0.5)).collect()),
            ColumnData::Bool((0..500).map(|i| Some(i % 3 == 0)).collect()),
            ColumnData::String(
                (0..500)
                    .map(|i| if i % 17 == 0 { None } else { Some(format!("v{}", i % 11)) })
                    .collect(),
            ),
        ];

        for col in cases {
            let enc = ColumnCodec::encode(&col).unwrap();
            let dec = ColumnCodec::decode(&enc, col.data_type()).unwrap();
            assert_eq!(dec, col, "codec lost data for {}", col.data_type());
        }
    }

    fn sample_meta() -> TableMeta {
        TableMeta {
            table_id: 7,
            name: "metrics".to_string(),
            ts_unit: TsUnit::Millis,
            chunk_interval_ms: 86_400_000,
            retention_days: 30,
            columns: vec![
                ColumnDef {
                    name: "ts".to_string(),
                    data_type: DataType::Timestamp,
                    is_tag: false,
                    nullable: false,
                    compression: CompressionLevel::Lz4,
                },
                ColumnDef {
                    name: "host".to_string(),
                    data_type: DataType::String,
                    is_tag: true,
                    nullable: true,
                    compression: CompressionLevel::Zstd(3),
                },
            ],
        }
    }

    /// The schema is the only thing a restart cannot reconstruct from rows, so
    /// every field of it has to survive the trip through the table directory.
    #[test]
    fn a_schema_round_trips_through_the_table_directory() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(META_FILE_NAME);

        let meta = sample_meta();
        meta.write_to_file(&path).unwrap();

        let back = TableMeta::read_from_file(&path).unwrap();
        assert_eq!(back.table_id, meta.table_id);
        assert_eq!(back.name, meta.name);
        assert_eq!(back.ts_unit, meta.ts_unit);
        assert_eq!(back.chunk_interval_ms, meta.chunk_interval_ms);
        assert_eq!(back.retention_days, meta.retention_days);
        assert_eq!(back.columns.len(), meta.columns.len());
        for (a, b) in back.columns.iter().zip(meta.columns.iter()) {
            assert_eq!(a.name, b.name);
            assert_eq!(a.data_type, b.data_type);
            assert_eq!(a.is_tag, b.is_tag);
            assert_eq!(a.nullable, b.nullable);
            assert_eq!(a.compression, b.compression);
        }
    }

    /// Rewriting a schema must not leave a partially written file behind: a
    /// reader either sees the old schema or the new one, never a truncated mix.
    #[test]
    fn a_rewritten_schema_replaces_the_old_one_completely() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(META_FILE_NAME);

        let mut meta = sample_meta();
        meta.write_to_file(&path).unwrap();

        meta.columns.truncate(1);
        meta.retention_days = 1;
        meta.write_to_file(&path).unwrap();

        let back = TableMeta::read_from_file(&path).unwrap();
        assert_eq!(back.columns.len(), 1, "old columns survived the rewrite");
        assert_eq!(back.retention_days, 1);

        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|n| n.ends_with(".tmp"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "temporary schema files left: {:?}",
            leftovers
        );
    }

    /// A directory that is not a table must be reported as such, not as an
    /// error: the data directory can hold anything else a deployment puts there.
    #[test]
    fn a_directory_without_a_schema_is_not_a_table() {
        let dir = tempfile::tempdir().unwrap();
        assert!(Table::open_stored(dir.path()).unwrap().is_none());
    }
}
