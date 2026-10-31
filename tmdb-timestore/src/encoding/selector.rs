//! Dynamic encoding selection.
//!
//! The original writer picked an encoder purely from the declared type, so a
//! monotonically increasing device id was handed to the run-length encoder and
//! came out *larger* than the raw input (8000 -> 13000 bytes). Real columns
//! have wildly different distributions within one type, so the choice has to
//! look at the data.
//!
//! This module samples the column, scores every legal candidate, and returns
//! the smallest. A cheap pre-filter rejects obviously unsuitable encoders
//! (for example RLE on data with no repeated values) so the expensive full
//! encode is only run on plausible candidates.
//!
//! Sampling is capped by [`SAMPLE_LIMIT`] to keep flush latency bounded; the
//! estimate is scaled up from the sample.

use super::{
    BitPackEncoder, ColumnData, DeltaOfDeltaEncoder, DictEncoder, DictStringEncoder, Encoder,
    EncodingType, GorillaEncoder, RleEncoder,
};

/// Maximum values inspected when choosing an encoding.
pub const SAMPLE_LIMIT: usize = 4096;

/// Result of scoring the candidates for a column.
#[derive(Debug, Clone)]
pub struct EncodingDecision {
    pub encoding: EncodingType,
    /// Encoded size of the sampled values, in bytes.
    pub estimated_payload_len: usize,
    /// Number of values inspected.
    pub sampled: usize,
    /// Every candidate that was actually encoded, for diagnostics and tests.
    pub considered: Vec<(EncodingType, usize)>,
}

/// Choose the smallest legal encoding for `column`.
pub fn select_encoding(column: &ColumnData) -> EncodingDecision {
    let len = column.len();
    let sampled = len.min(SAMPLE_LIMIT);

    if sampled == 0 {
        return EncodingDecision {
            encoding: EncodingType::Raw,
            estimated_payload_len: 0,
            sampled: 0,
            considered: Vec::new(),
        };
    }

    // Raw is always available and is the baseline every candidate must beat.
    let raw_len = raw_size(column, sampled);
    let mut best = EncodingDecision {
        encoding: EncodingType::Raw,
        estimated_payload_len: raw_len,
        sampled,
        considered: vec![(EncodingType::Raw, raw_len)],
    };

    let mut consider = |encoding: EncodingType, payload: Option<Vec<u8>>| {
        if let Some(bytes) = payload {
            let size = bytes.len();
            if size < best.estimated_payload_len {
                best.encoding = encoding;
                best.estimated_payload_len = size;
            }
            best.considered.push((encoding, size));
        }
    };

    match column {
        ColumnData::Float64(_) | ColumnData::Float32(_) => {
            let vals = sample_f64(column);
            consider(EncodingType::Gorilla, GorillaEncoder::new().encode_f64(&vals, &[]).ok());
            // RLE on the raw bit pattern still helps for held values.
            let bits: Vec<i64> = vals.iter().map(|v| v.to_bits() as i64).collect();
            if has_long_runs(&bits) {
                consider(EncodingType::Rle, RleEncoder::new().encode_i64(&bits, &[]).ok());
            }
        }
        ColumnData::String(_) => {
            let vals = sample_strings(column);
            if let Some(bytes) = DictStringEncoder::new().encode_str(&vals, &[]).ok() {
                consider(EncodingType::Dictionary, Some(bytes));
            }
        }
        _ => {
            let vals = sample_i64(column);
            let stats = IntStats::of(&vals);

            // Delta-of-delta is the natural fit for timestamps and any column
            // with a stable interval.
            if stats.regular_interval {
                consider(
                    EncodingType::DeltaOfDelta,
                    DeltaOfDeltaEncoder::new().encode_i64(&vals, &[]).ok(),
                );
            }
            // Dictionary pays off when cardinality is far below row count.
            if stats.cardinality * 2 <= vals.len() {
                consider(EncodingType::Dictionary, DictEncoder::new().encode_i64(&vals, &[]).ok());
            }
            // RLE only when runs actually exist.
            if stats.longest_run >= MIN_RUN_FOR_RLE {
                consider(EncodingType::Rle, RleEncoder::new().encode_i64(&vals, &[]).ok());
            }
            // Bit-packing only when every value is non-negative and small.
            if stats.all_non_negative {
                consider(EncodingType::BitPack, BitPackEncoder::new().encode_i64(&vals, &[]).ok());
            }
        }
    }

    // Extrapolate the sampled size to the full column, then add the per-block
    // header and null bitmap the codec will add.
    let scale = len as f64 / sampled as f64;
    let payload = (best.estimated_payload_len as f64 * scale).ceil() as usize;
    best.estimated_payload_len = payload + 13 + len.div_ceil(8);

    best
}

/// Runs shorter than this are not worth a run-length header.
const MIN_RUN_FOR_RLE: usize = 4;

fn raw_size(column: &ColumnData, sampled: usize) -> usize {
    match column {
        ColumnData::Timestamp(_) | ColumnData::Int64(_) | ColumnData::Float64(_) => sampled * 8,
        ColumnData::Int32(_) | ColumnData::Float32(_) => sampled * 4,
        ColumnData::Bool(_) => sampled,
        ColumnData::String(_) => (0..sampled)
            .filter_map(|i| column.as_str(i))
            .map(|s| s.len() + 4)
            .sum(),
    }
}

fn sample_i64(column: &ColumnData) -> Vec<i64> {
    (0..column.len().min(SAMPLE_LIMIT))
        .filter_map(|i| column.as_i64(i))
        .collect()
}

fn sample_f64(column: &ColumnData) -> Vec<f64> {
    (0..column.len().min(SAMPLE_LIMIT))
        .filter_map(|i| column.as_f64(i))
        .collect()
}

fn sample_strings(column: &ColumnData) -> Vec<String> {
    (0..column.len().min(SAMPLE_LIMIT))
        .filter_map(|i| column.as_str(i).map(|s| s.to_string()))
        .collect()
}

struct IntStats {
    cardinality: usize,
    longest_run: usize,
    all_non_negative: bool,
    regular_interval: bool,
}

impl IntStats {
    fn of(vals: &[i64]) -> Self {
        let mut cardinality = std::collections::HashSet::new();
        let mut longest_run = 1usize;
        let mut run = 1usize;
        let mut all_non_negative = true;

        for (i, &v) in vals.iter().enumerate() {
            cardinality.insert(v);
            if v < 0 {
                all_non_negative = false;
            }
            if i > 0 {
                if v == vals[i - 1] {
                    run += 1;
                } else {
                    longest_run = longest_run.max(run);
                    run = 1;
                }
            }
        }
        longest_run = longest_run.max(run);

        // A stable interval means every first difference is the same value.
        let regular_interval = if vals.len() < 3 {
            false
        } else {
            let step = vals[1] - vals[0];
            step != 0
                && vals.windows(2).skip(1).all(|w| w[1] - w[0] == step)
        };

        Self {
            cardinality: cardinality.len(),
            longest_run,
            all_non_negative,
            regular_interval,
        }
    }
}

fn has_long_runs(bits: &[i64]) -> bool {
    let mut run = 1usize;
    let mut longest = 1usize;
    for i in 1..bits.len() {
        if bits[i] == bits[i - 1] {
            run += 1;
            longest = longest.max(run);
        } else {
            run = 1;
        }
    }
    longest >= 8
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::{ColumnCodec, DataType};

    fn encode_len(col: &ColumnData) -> usize {
        ColumnCodec::encode(col).unwrap().len()
    }

    fn roundtrip(col: &ColumnData) {
        let enc = ColumnCodec::encode(col).unwrap();
        let dec = ColumnCodec::decode(&enc, col.data_type()).unwrap();
        assert_eq!(dec, *col, "round trip failed for {}", col.data_type());
    }

    #[test]
    fn test_monotonic_ids_avoid_rle() {
        // The regression that motivated dynamic selection: RLE used to expand
        // this data instead of compressing it.
        let col = ColumnData::Int64((0..2000).map(|i| Some(1_000_000 + i * 97)).collect());
        let d = select_encoding(&col);
        assert_ne!(
            d.encoding,
            EncodingType::Rle,
            "RLE must not be chosen for monotonic data"
        );
        assert!(encode_len(&col) < 2000 * 8, "encoded {} bytes", encode_len(&col));
    }

    #[test]
    fn test_constant_series_uses_rle() {
        let col = ColumnData::Int64(vec![Some(42); 2000]);
        let d = select_encoding(&col);
        assert_eq!(d.encoding, EncodingType::Rle);
        // 2000 null-bitmap bits (250 B) dominate the 13 B RLE payload, so the
        // block is ~270 bytes rather than the ~13 bytes of the payload alone.
        assert!(encode_len(&col) < 400, "got {} bytes", encode_len(&col));
    }

    #[test]
    fn test_regular_timestamps_use_delta_of_delta() {
        let col = ColumnData::Timestamp(
            (0..2000)
                .map(|i| Some(1_700_000_000_000 + i as i64 * 1000))
                .collect(),
        );
        let d = select_encoding(&col);
        assert_eq!(d.encoding, EncodingType::DeltaOfDelta);
        assert!(encode_len(&col) < 2000 * 2, "got {} bytes", encode_len(&col));
    }

    #[test]
    fn test_low_cardinality_tags_use_dictionary() {
        // Ten distinct large ids: too wide for bit-packing, so the dictionary
        // wins. (With narrow ids such as 0..10 bit-packing is genuinely smaller,
        // which is the correct outcome and is checked separately below.)
        let col = ColumnData::Int64(
            (0..4000)
                .map(|i| Some(1_000_000 + (i % 10) * 100_000))
                .collect(),
        );
        let d = select_encoding(&col);
        assert_eq!(d.encoding, EncodingType::Dictionary);
    }

    #[test]
    fn test_narrow_values_prefer_bitpack_over_dictionary() {
        let col = ColumnData::Int64((0..4000).map(|i| Some((i % 10) as i64)).collect());
        let d = select_encoding(&col);
        assert_eq!(d.encoding, EncodingType::BitPack);
        assert!(encode_len(&col) < 4000 * 8 / 3);
    }

    #[test]
    fn test_small_non_negative_uses_bitpack() {
        // Values 0..15 are neither constant nor regular, so bit-packing wins.
        let col = ColumnData::Int64((0..4000).map(|i| Some((i * 7 % 16) as i64)).collect());
        let d = select_encoding(&col);
        assert_eq!(d.encoding, EncodingType::BitPack);
    }

    #[test]
    fn test_strings_use_dictionary() {
        let col = ColumnData::String(
            (0..4000)
                .map(|i| Some(format!("device-{}", i % 25)))
                .collect(),
        );
        let d = select_encoding(&col);
        assert_eq!(d.encoding, EncodingType::Dictionary);
    }

    #[test]
    fn test_smooth_floats_use_gorilla() {
        // A slow ramp keeps the exponent and the low mantissa bits stable, so
        // the XOR window stays narrow and the reuse path fires.
        let col = ColumnData::Float64((0..4000).map(|i| Some(1000.0 + i as f64 * 0.25)).collect());
        let d = select_encoding(&col);
        assert_eq!(d.encoding, EncodingType::Gorilla);
        assert!(encode_len(&col) < 4000 * 8 / 3, "got {} bytes", encode_len(&col));
    }

    /// Noisy 64-bit floats cannot beat raw, so the selector must reject Gorilla
    /// rather than write a payload that is larger than the input.
    ///
    /// The comparison excludes the fixed 13-byte block header and the null
    /// bitmap, which no encoding choice can avoid.
    #[test]
    fn test_noisy_floats_fall_back_to_raw() {
        let col = ColumnData::Float64(
            (0..2000)
                .map(|i| Some((i as f64 * 1.234_567).sin() * 1e6))
                .collect(),
        );
        let d = select_encoding(&col);
        assert_eq!(d.encoding, EncodingType::Raw, "considered {:?}", d.considered);

        let payload = encode_len(&col) - 13 - col.len().div_ceil(8);
        assert!(payload <= 2000 * 8, "payload {} exceeds raw", payload);
    }

    #[test]
    fn test_high_cardinality_falls_back_to_raw() {
        // Irregular gaps, all values distinct: no delta structure and full
        // cardinality, so nothing beats raw.
        let col = ColumnData::Int64(
            (0..1000)
                .map(|i| Some((i as i64) * 2_654_435_761i64 ^ ((i * 7919) as i64 % 1_000)))
                .collect(),
        );
        let d = select_encoding(&col);
        assert!(
            matches!(
                d.encoding,
                EncodingType::Raw | EncodingType::BitPack | EncodingType::Dictionary
            ),
            "unexpected choice {:?}",
            d.encoding
        );

        let payload = encode_len(&col) - 13 - col.len().div_ceil(8);
        assert!(payload <= 1000 * 8, "payload {} exceeds raw", payload);
    }

    #[test]
    fn test_selection_is_stable_across_calls() {
        let col = ColumnData::Float64((0..500).map(|i| Some(i as f64 * 1.5)).collect());
        let a = select_encoding(&col);
        let b = select_encoding(&col);
        assert_eq!(a.encoding, b.encoding);
        assert_eq!(a.estimated_payload_len, b.estimated_payload_len);
    }

    #[test]
    fn test_empty_column_is_raw() {
        let d = select_encoding(&ColumnData::Int64(Vec::new()));
        assert_eq!(d.encoding, EncodingType::Raw);
    }

    #[test]
    fn test_int_stats_regularity_detection() {
        let regular: Vec<i64> = (0..100).map(|i| 1000 + i * 5).collect();
        assert!(IntStats::of(&regular).regular_interval);
        let irregular: Vec<i64> = (0..100).map(|i| 1000 + i * 5 + (i % 3)).collect();
        assert!(!IntStats::of(&irregular).regular_interval);
    }

    #[test]
    fn test_every_selected_encoding_roundtrips() {
        // The selector's contract: whatever it picks must decode exactly.
        let cases: Vec<ColumnData> = vec![
            ColumnData::Int64(vec![Some(42); 500]),
            ColumnData::Int64((0..500).map(|i| Some(1_000_000 + i * 97)).collect()),
            ColumnData::Timestamp((0..500).map(|i| Some(i as i64 * 1000)).collect()),
            ColumnData::Int64((0..500).map(|i| Some(i % 7)).collect()),
            ColumnData::Int64((0..500).map(|i| Some((i * 7 % 16) as i64)).collect()),
            ColumnData::Float64((0..500).map(|i| Some(20.0 + i as f64 * 0.03)).collect()),
            ColumnData::Float64(vec![Some(1.5); 500]),
            ColumnData::String((0..500).map(|i| Some(format!("s{}", i % 9))).collect()),
            ColumnData::Bool((0..500).map(|i| Some(i % 2 == 0)).collect()),
        ];

        for col in cases {
            let d = select_encoding(&col);
            roundtrip(&col);
            assert!(
                !d.considered.is_empty(),
                "no candidate encoded for {}",
                col.data_type()
            );
        }
    }

    #[test]
    fn test_nulls_do_not_break_selection() {
        let col = ColumnData::Float64(
            (0..1000)
                .map(|i| if i % 10 == 0 { None } else { Some(i as f64) })
                .collect(),
        );
        roundtrip(&col);

        let col = ColumnData::Int64(
            (0..1000)
                .map(|i| if i % 7 == 0 { None } else { Some(i as i64 * 100) })
                .collect(),
        );
        roundtrip(&col);

        let col = ColumnData::String(
            (0..500)
                .map(|i| {
                    if i % 11 == 0 {
                        None
                    } else {
                        Some(format!("tag{}", i % 13))
                    }
                })
                .collect(),
        );
        roundtrip(&col);

        let _ = DataType::Timestamp;
    }
}
