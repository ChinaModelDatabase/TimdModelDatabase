//! Run-length encoding for repeated values.
//!
//! Constant series (a stuck sensor, a fixed status tag) collapse to a handful
//! of bytes. Literal runs keep the format usable for non-repeating data, but
//! the previous implementation always paid a 4-byte length prefix per literal
//! value, so monotonically increasing data *grew* from 8000 to 13000 bytes.
//! Literals are now stored as a packed byte array with a single length prefix.
//!
//! # Format
//!
//! ```text
//! repeated runs  : 0x01 + count:u32 + value:i64
//! literal section: 0x00 + count:u32 + count * i64
//! ```
//!
//! Runs and literal sections alternate, starting with a run. Decoding walks
//! the stream until it has produced `count` values or the input is exhausted.

use super::Encoder;
use crate::encoding::EncodingType;

/// Marks a repeated run.
const MARK_RUN: u8 = 0x01;
/// Marks a packed literal section.
const MARK_LITERAL: u8 = 0x00;

/// A literal section shorter than this is not worth its 4-byte prefix.
const MIN_RUN: usize = 3;

pub struct RleEncoder;

impl RleEncoder {
    pub fn new() -> Self {
        Self
    }
}

impl Encoder for RleEncoder {
    fn encoding_type(&self) -> EncodingType {
        EncodingType::Rle
    }

    fn encode_i64(&self, values: &[i64], nulls: &[bool]) -> anyhow::Result<Vec<u8>> {
        self.encode_impl(values, nulls)
    }

    fn decode_i64(&self, data: &[u8], count: usize) -> anyhow::Result<Vec<i64>> {
        self.decode_impl(data, count)
    }

    fn encode_f64(&self, values: &[f64], nulls: &[bool]) -> anyhow::Result<Vec<u8>> {
        let bits: Vec<i64> = values.iter().map(|v| v.to_bits() as i64).collect();
        self.encode_impl(&bits, nulls)
    }

    fn decode_f64(&self, data: &[u8], count: usize) -> anyhow::Result<Vec<f64>> {
        Ok(self
            .decode_impl(data, count)?
            .into_iter()
            .map(|v| f64::from_bits(v as u64))
            .collect())
    }

    fn encode_str(&self, _values: &[String], _nulls: &[bool]) -> anyhow::Result<Vec<u8>> {
        Err(super::bitio::EncodingError::Corrupt("rle cannot encode strings".into()).into())
    }

    fn decode_str(&self, _data: &[u8], _count: usize) -> anyhow::Result<Vec<String>> {
        Err(super::bitio::EncodingError::Corrupt("rle cannot decode strings".into()).into())
    }
}

impl RleEncoder {
    fn encode_impl(&self, values: &[i64], nulls: &[bool]) -> anyhow::Result<Vec<u8>> {
        let vals = super::delta::compact(values, nulls);
        if vals.is_empty() {
            return Ok(Vec::new());
        }

        // Worst case is all literals plus one header.
        let mut out = Vec::with_capacity(5 + vals.len() * 8);
        let mut i = 0;

        while i < vals.len() {
            let val = vals[i];
            let mut run_len = 1;
            while i + run_len < vals.len() && vals[i + run_len] == val {
                run_len += 1;
            }

            if run_len >= MIN_RUN {
                out.push(MARK_RUN);
                out.extend_from_slice(&(run_len as u32).to_le_bytes());
                out.extend_from_slice(&val.to_le_bytes());
            } else {
                // Gather a maximal span of sub-threshold values into one
                // literal section, including short runs, so that non-
                // repetitive data costs one header instead of one per value.
                let start = i;
                while i < vals.len() {
                    let v = vals[i];
                    let mut len = 1;
                    while i + len < vals.len() && vals[i + len] == v {
                        len += 1;
                    }
                    if len >= MIN_RUN {
                        break;
                    }
                    i += len;
                }
                let n = i - start;
                out.push(MARK_LITERAL);
                out.extend_from_slice(&(n as u32).to_le_bytes());
                for &v in &vals[start..i] {
                    out.extend_from_slice(&v.to_le_bytes());
                }
                continue;
            }

            i += run_len;
        }

        Ok(out)
    }

    fn decode_impl(&self, data: &[u8], count: usize) -> anyhow::Result<Vec<i64>> {
        let mut out = Vec::with_capacity(count);
        let mut pos = 0;

        while pos < data.len() && out.len() < count {
            let marker = data[pos];
            pos += 1;

            if pos + 4 > data.len() {
                break;
            }
            let n = u32::from_le_bytes(data[pos..pos + 4].try_into()?) as usize;
            pos += 4;

            if marker == MARK_RUN {
                if pos + 8 > data.len() {
                    break;
                }
                let v = i64::from_le_bytes(data[pos..pos + 8].try_into()?);
                pos += 8;
                for _ in 0..n {
                    if out.len() >= count {
                        break;
                    }
                    out.push(v);
                }
            } else {
                let need = n.saturating_mul(8);
                if pos + need > data.len() {
                    // Truncated tail: decode whatever is complete.
                    let avail = (data.len() - pos) / 8;
                    for c in data[pos..pos + avail * 8].chunks_exact(8) {
                        out.push(i64::from_le_bytes(c.try_into().unwrap()));
                    }
                    return Ok(out);
                }
                for c in data[pos..pos + need].chunks_exact(8) {
                    out.push(i64::from_le_bytes(c.try_into().unwrap()));
                }
                pos += need;
            }
        }

        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(vals: &[i64]) -> usize {
        let enc = RleEncoder::new().encode_i64(vals, &vec![false; vals.len()]).unwrap();
        let dec = RleEncoder::new().decode_i64(&enc, vals.len()).unwrap();
        assert_eq!(dec.len(), vals.len(), "length mismatch for {} values", vals.len());
        for (i, (a, b)) in dec.iter().zip(vals.iter()).enumerate() {
            assert_eq!(a, b, "mismatch at index {}: got {} want {}", i, a, b);
        }
        enc.len()
    }

    #[test]
    fn test_constant_series_is_tiny() {
        let vals = vec![42i64; 1000];
        assert_eq!(roundtrip(&vals), 13);
    }

    #[test]
    fn test_mixed_runs_roundtrip() {
        let mut vals = Vec::new();
        vals.extend(std::iter::repeat(1i64).take(100));
        vals.extend(std::iter::repeat(2i64).take(50));
        vals.extend(std::iter::repeat(3i64).take(200));
        roundtrip(&vals);
    }

    #[test]
    fn test_monotonic_does_not_explode() {
        // Previously 8000 bytes of input became 13000 bytes.
        let vals: Vec<i64> = (0..1000).map(|i| 1_000_000 + i * 97).collect();
        let size = roundtrip(&vals);
        let raw = vals.len() * 8;
        assert!(
            size <= raw + 8,
            "RLE expanded monotonic data: {} vs raw {}",
            size,
            raw
        );
    }

    #[test]
    fn test_alternating_two_values() {
        let vals: Vec<i64> = (0..1000).map(|i| if i % 2 == 0 { 1 } else { 2 }).collect();
        roundtrip(&vals);
    }

    #[test]
    fn test_run_boundaries() {
        for n in 1..12 {
            let vals = vec![7i64; n];
            roundtrip(&vals);
        }
    }

    #[test]
    fn test_runs_and_literals_interleaved() {
        let mut vals = Vec::new();
        vals.extend([1i64, 2]); // literal span
        vals.extend(std::iter::repeat(5i64).take(10)); // run
        vals.extend([9i64, 8, 7]); // literal span
        vals.extend(std::iter::repeat(0i64).take(4)); // run
        roundtrip(&vals);
    }

    #[test]
    fn test_large_run_count() {
        let vals = vec![1i64; 100_000];
        assert_eq!(roundtrip(&vals), 13);
    }

    #[test]
    fn test_empty_and_single() {
        assert!(RleEncoder::new().encode_i64(&[], &[]).unwrap().is_empty());
        roundtrip(&[42]);
    }

    #[test]
    fn test_nulls_compacted() {
        let vals = vec![1i64, 1, 1, 1, 1];
        let nulls = vec![false, false, true, false, true];
        let enc = RleEncoder::new().encode_i64(&vals, &nulls).unwrap();
        let dec = RleEncoder::new().decode_i64(&enc, 3).unwrap();
        assert_eq!(dec, vec![1, 1, 1]);
    }
}
