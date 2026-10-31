//! Bit-packing for small non-negative integers.
//!
//! When every value fits in `k` bits, storing `k` bits per value instead of
//! 64 is a large win. The previous implementation derived the width from the
//! maximum absolute value but then packed the values as *signed* bits, which
//! produced wrong results for any value whose high bit was set. This version
//! requires non-negative input, states that in the width byte, and round-trips
//! exactly.
//!
//! # Format
//!
//! ```text
//! bits_per_value: u8   (1..=64)
//! count: u32
//! count * bits_per_value bits, MSB first
//! ```

use super::bitio::{BitReader, EncodingError};
use super::Encoder;
use crate::encoding::EncodingType;

/// Signals that the column held a negative value and bit-packing was skipped.
pub const BITPACK_UNSIGNED_FLAG: u8 = 0xFF;

pub struct BitPackEncoder;

impl BitPackEncoder {
    pub fn new() -> Self {
        Self
    }

    /// Width needed to represent `max`, at least 1 bit.
    fn width_for(max: u64) -> usize {
        if max == 0 {
            1
        } else {
            (64 - max.leading_zeros()) as usize
        }
    }
}

impl Encoder for BitPackEncoder {
    fn encoding_type(&self) -> EncodingType {
        EncodingType::BitPack
    }

    fn encode_i64(&self, values: &[i64], nulls: &[bool]) -> anyhow::Result<Vec<u8>> {
        self.encode_impl(values, nulls)
    }

    fn decode_i64(&self, data: &[u8], count: usize) -> anyhow::Result<Vec<i64>> {
        self.decode_impl(data, count)
    }

    fn encode_f64(&self, values: &[f64], nulls: &[bool]) -> anyhow::Result<Vec<u8>> {
        // Bit-packing a float's bit pattern is almost never a win, but it is
        // lossless, so expose it rather than failing.
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
        Err(EncodingError::Corrupt("bitpack cannot encode strings".into()).into())
    }

    fn decode_str(&self, _data: &[u8], _count: usize) -> anyhow::Result<Vec<String>> {
        Err(EncodingError::Corrupt("bitpack cannot decode strings".into()).into())
    }
}

impl BitPackEncoder {
    fn encode_impl(&self, values: &[i64], nulls: &[bool]) -> anyhow::Result<Vec<u8>> {
        let vals = super::delta::compact(values, nulls);
        if vals.is_empty() {
            return Ok(Vec::new());
        }

        // Signed values are out of scope; fall back to plain 8-byte storage.
        if vals.iter().any(|&v| v < 0) {
            let mut out = Vec::with_capacity(9 + vals.len() * 8);
            out.push(BITPACK_UNSIGNED_FLAG);
            out.extend_from_slice(&(vals.len() as u32).to_le_bytes());
            for v in &vals {
                out.extend_from_slice(&v.to_le_bytes());
            }
            return Ok(out);
        }

        let max = vals.iter().copied().max().unwrap_or(0) as u64;
        let width = Self::width_for(max);

        let mut w = super::bitio::BitWriter::with_capacity(vals.len() * width / 8 + 8);
        for &v in &vals {
            w.write_bits(v as u64, width);
        }
        let packed = w.finish();

        let mut out = Vec::with_capacity(5 + packed.len());
        out.push(width as u8);
        out.extend_from_slice(&(vals.len() as u32).to_le_bytes());
        out.extend_from_slice(&packed);
        Ok(out)
    }

    fn decode_impl(&self, data: &[u8], count: usize) -> anyhow::Result<Vec<i64>> {
        if count == 0 || data.is_empty() {
            return Ok(Vec::new());
        }
        if data.len() < 5 {
            return Err(EncodingError::UnexpectedEof.into());
        }

        let width = data[0];
        let stored_count = u32::from_le_bytes(data[1..5].try_into()?) as usize;

        if width == BITPACK_UNSIGNED_FLAG {
            // Signed fallback path.
            let mut out = Vec::with_capacity(count);
            let mut pos = 5;
            for _ in 0..count {
                if pos + 8 > data.len() {
                    break;
                }
                out.push(i64::from_le_bytes(data[pos..pos + 8].try_into()?));
                pos += 8;
            }
            return Ok(out);
        }

        if width == 0 || width > 64 {
            return Err(EncodingError::Corrupt(format!("invalid width {}", width)).into());
        }
        let width = width as usize;

        let mut r = BitReader::new(&data[5..]);
        let mut out = Vec::with_capacity(count.min(stored_count));
        for _ in 0..stored_count.min(count) {
            if r.bits_remaining() < width {
                break;
            }
            let v = r.read_bits(width)?;
            out.push(v as i64);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(vals: &[i64]) {
        let enc = BitPackEncoder::new()
            .encode_i64(vals, &vec![false; vals.len()])
            .unwrap();
        let dec = BitPackEncoder::new().decode_i64(&enc, vals.len()).unwrap();
        assert_eq!(dec.len(), vals.len(), "length mismatch for {} values", vals.len());
        for (i, (a, b)) in dec.iter().zip(vals.iter()).enumerate() {
            assert_eq!(a, b, "mismatch at index {}: got {} want {}", i, a, b);
        }
    }

    #[test]
    fn test_small_values_roundtrip() {
        let vals: Vec<i64> = (0..5000).map(|i| i % 16).collect();
        roundtrip(&vals);
        let enc = BitPackEncoder::new().encode_i64(&vals, &vec![false; 5000]).unwrap();
        assert!(enc.len() < 5000 * 8 / 8, "got {}", enc.len());
    }

    #[test]
    fn test_width_boundaries() {
        for bits in 1..=17 {
            let max = (1i64 << bits) - 1;
            let vals: Vec<i64> = (0..200).map(|i| i % (max + 1)).collect();
            roundtrip(&vals);
        }
    }

    #[test]
    fn test_maximum_values() {
        roundtrip(&[0, 1, 255, 256, 65535, 65536, u32::MAX as i64]);
    }

    #[test]
    fn test_all_zeros() {
        roundtrip(&vec![0i64; 1000]);
    }

    #[test]
    fn test_single_value() {
        roundtrip(&[7]);
    }

    #[test]
    fn test_negative_falls_back_and_roundtrips() {
        let vals = vec![-1i64, -5, 3, -100, 0, 7];
        roundtrip(&vals);
        let enc = BitPackEncoder::new().encode_i64(&vals, &vec![false; 6]).unwrap();
        assert_eq!(enc[0], BITPACK_UNSIGNED_FLAG);
    }

    #[test]
    fn test_full_u64_range_positive() {
        let vals = vec![i64::MAX, 0, 1, i64::MAX - 1];
        roundtrip(&vals);
    }

    #[test]
    fn test_corrupt_width_rejected() {
        let mut enc = BitPackEncoder::new().encode_i64(&[1, 2, 3], &[false; 3]).unwrap();
        enc[0] = 100;
        assert!(BitPackEncoder::new().decode_i64(&enc, 3).is_err());
    }

    #[test]
    fn test_truncated_errors_or_short_reads() {
        let vals: Vec<i64> = (0..100).map(|i| i % 8).collect();
        let enc = BitPackEncoder::new().encode_i64(&vals, &vec![false; 100]).unwrap();
        let dec = BitPackEncoder::new().decode_i64(&enc[..6], 100).unwrap();
        assert!(dec.len() < 100);
    }

    #[test]
    fn test_empty() {
        assert!(BitPackEncoder::new().encode_i64(&[], &[]).unwrap().is_empty());
    }
}
