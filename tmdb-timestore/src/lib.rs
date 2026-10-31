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
}
