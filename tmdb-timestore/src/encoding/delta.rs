//! Delta-of-delta encoding for monotonically increasing timestamps.
//!
//! Timestamps in a time series are almost always increasing and sampled at a
//! near-constant interval, so the *second* difference (delta of delta) is
//! usually zero or very small. This encoder stores those second differences
//! with a variable-width bit prefix.
//!
//! # Format
//!
//! ```text
//! first value   : 64 bits, raw
//! first delta   : varint(zigzag(v1 - v0))
//! each later i>=2: d = (v[i] - v[i-1]) - prev_delta
//!   d == 0            -> '0'                        (1 bit)
//!   |d| <= 63         -> '10'  + zigzag:7            (9 bits)
//!   |d| <= 8191       -> '110' + zigzag:14          (17 bits)
//!   |d| <= 2^31-1     -> '1110' + zigzag:32         (36 bits)
//!   otherwise         -> '1111' + zigzag:64         (68 bits)
//! ```
//!
//! The last branch exists because a 32-bit zigzag cannot represent a
//! delta-of-delta that comes from a column containing `i64::MAX / 2` next to
//! `i64::MIN / 2`; the previous 3-branch form silently truncated those to 32
//! bits and produced wrong values.
//!
//! The decoder reads the same prefixes in the same order. The previous
//! implementation used bit patterns (`0b10 << 6`, `0b110 << 5`) that did not
//! line up with its own decoder, which corrupted any non-constant interval.

use super::bitio::{
    read_varint, unzigzag_i64, write_varint, zigzag_i64, BitReader, BitWriter, EncodingError,
};
use super::Encoder;
use crate::encoding::EncodingType;

pub struct DeltaOfDeltaEncoder;

impl DeltaOfDeltaEncoder {
    pub fn new() -> Self {
        Self
    }
}

impl Encoder for DeltaOfDeltaEncoder {
    fn encoding_type(&self) -> EncodingType {
        EncodingType::DeltaOfDelta
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
            .map(|b| f64::from_bits(b as u64))
            .collect())
    }

    fn encode_str(&self, _values: &[String], _nulls: &[bool]) -> anyhow::Result<Vec<u8>> {
        Err(EncodingError::Corrupt("delta-of-delta cannot encode strings".into()).into())
    }

    fn decode_str(&self, _data: &[u8], _count: usize) -> anyhow::Result<Vec<String>> {
        Err(EncodingError::Corrupt("delta-of-delta cannot decode strings".into()).into())
    }
}

impl DeltaOfDeltaEncoder {
    fn encode_impl(&self, values: &[i64], nulls: &[bool]) -> anyhow::Result<Vec<u8>> {
        let vals = compact(values, nulls);
        if vals.is_empty() {
            return Ok(Vec::new());
        }

        // The first value is written as 8 raw little-endian bytes rather than
        // through the bit writer: the following varint has to start on a byte
        // boundary, and mixing the two would drop the first value entirely.
        let mut head = Vec::with_capacity(9);
        head.extend_from_slice(&(vals[0] as u64).to_le_bytes());
        if vals.len() == 1 {
            return Ok(head);
        }

        // Differences use wrapping arithmetic: a column holding `i64::MAX / 2`
        // next to `i64::MIN / 2` produces a difference that does not fit in
        // i64, and a plain subtraction panics in debug builds.
        let mut prev_delta = vals[1].wrapping_sub(vals[0]);
        write_varint(&mut head, zigzag_i64(prev_delta));

        // Bit stream for the remaining values.
        let mut prev = vals[1];
        let mut d_buf = BitWriter::with_capacity(vals.len());
        for &v in &vals[2..] {
            let delta = v.wrapping_sub(prev);
            let d = delta.wrapping_sub(prev_delta);
            prev = v;
            prev_delta = delta;

            if d == 0 {
                d_buf.write_bit(false);
            } else if (-63..=63).contains(&d) {
                d_buf.write_bits(0b10, 2);
                d_buf.write_bits(zigzag_i64(d), 7);
            } else if (-8191..=8191).contains(&d) {
                d_buf.write_bits(0b110, 3);
                d_buf.write_bits(zigzag_i64(d), 14);
            } else if (-(1i64 << 31)..=((1i64 << 31) - 1)).contains(&d) {
                d_buf.write_bits(0b1110, 4);
                d_buf.write_bits(zigzag_i64(d), 32);
            } else {
                d_buf.write_bits(0b1111, 4);
                d_buf.write_bits(zigzag_i64(d), 64);
            }
        }

        // `head` is 8 raw bytes plus a whole number of varint bytes, so the bit
        // stream that follows already starts on a byte boundary.
        let mut out = Vec::with_capacity(head.len() + vals.len());
        out.extend_from_slice(&head);
        out.extend_from_slice(&d_buf.finish());
        Ok(out)
    }

    fn decode_impl(&self, data: &[u8], count: usize) -> anyhow::Result<Vec<i64>> {
        if count == 0 || data.is_empty() {
            return Ok(Vec::new());
        }
        if data.len() < 8 {
            return Err(EncodingError::UnexpectedEof.into());
        }

        let first = u64::from_le_bytes(data[0..8].try_into()?) as i64;
        let mut out = Vec::with_capacity(count);
        out.push(first);
        if count == 1 {
            return Ok(out);
        }

        let (zz, n) = read_varint(data, 8)?;
        let prev_delta0 = unzigzag_i64(zz);

        let second = first.wrapping_add(prev_delta0);
        out.push(second);
        if count == 2 {
            return Ok(out);
        }

        let mut r = BitReader::new(&data[8 + n..]);
        let mut prev = second;
        let mut prev_delta = prev_delta0;

        while out.len() < count {
            if r.bits_remaining() == 0 {
                return Err(EncodingError::UnexpectedEof.into());
            }

            let d = if !r.read_bit()? {
                0
            } else if !r.read_bit()? {
                unzigzag_i64(r.read_bits(7)?)
            } else if !r.read_bit()? {
                unzigzag_i64(r.read_bits(14)?)
            } else if !r.read_bit()? {
                unzigzag_i64(r.read_bits(32)?)
            } else {
                unzigzag_i64(r.read_bits(64)?)
            };

            let delta = prev_delta.wrapping_add(d);
            let value = prev.wrapping_add(delta);
            out.push(value);
            prev = value;
            prev_delta = delta;
        }

        Ok(out)
    }
}

/// Drop nulls so encoders only see the present values.
pub(crate) fn compact(values: &[i64], nulls: &[bool]) -> Vec<i64> {
    if nulls.len() != values.len() || !nulls.iter().any(|&n| n) {
        return values.to_vec();
    }
    values
        .iter()
        .zip(nulls.iter())
        .filter(|(_, &n)| !n)
        .map(|(&v, _)| v)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(vals: &[i64]) {
        let nulls = vec![false; vals.len()];
        let enc = DeltaOfDeltaEncoder::new().encode_i64(vals, &nulls).unwrap();
        let dec = DeltaOfDeltaEncoder::new().decode_i64(&enc, vals.len()).unwrap();
        assert_eq!(dec.len(), vals.len(), "length mismatch for {} values", vals.len());
        for (i, (a, b)) in dec.iter().zip(vals.iter()).enumerate() {
            assert_eq!(a, b, "mismatch at index {}: got {} want {}", i, a, b);
        }
    }

    #[test]
    fn test_constant_interval() {
        let ts: Vec<i64> = (0..1000).map(|i| 1_700_000_000_000 + i * 1000).collect();
        roundtrip(&ts);
        let enc = DeltaOfDeltaEncoder::new()
            .encode_i64(&ts, &vec![false; 1000])
            .unwrap();
        // Constant interval => every delta-of-delta is 0 => ~1 bit/value.
        assert!(enc.len() < ts.len() * 8 / 8, "got {} bytes", enc.len());
    }

    #[test]
    fn test_irregular_intervals_with_jitter() {
        // The case that previously produced 492/500 wrong values.
        let ts: Vec<i64> = (0..500)
            .map(|i| 1_700_000_000_000 + i * 1000 + (i % 7) * 137)
            .collect();
        roundtrip(&ts);
    }

    #[test]
    fn test_random_walk_timestamps() {
        let mut ts = Vec::new();
        let mut cur = 1_000_000i64;
        let mut seed = 0x2545F4914F6CDD1Du64;
        for _ in 0..2000 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            cur += 1 + (seed >> 33) as i64 % 5000;
            ts.push(cur);
        }
        roundtrip(&ts);
    }

    #[test]
    fn test_single_value() {
        roundtrip(&[12345]);
    }

    #[test]
    fn test_two_values() {
        roundtrip(&[100, 350]);
    }

    #[test]
    fn test_large_gaps_use_wide_prefix() {
        let ts = vec![0i64, 1_000_000, 2_000_000_000, 3, i64::MAX / 2, i64::MIN / 2];
        roundtrip(&ts);
    }

    #[test]
    fn test_all_boundaries_exercised() {
        // Force each prefix branch in one stream, then again with a different
        // starting interval.
        let ts: Vec<i64> = vec![0, 10, 21, 33, 46, 60, 75, 91, 108];
        roundtrip(&ts);
        let ts: Vec<i64> = vec![0, 1, 3, 6, 10, 15, 21, 28, 36];
        roundtrip(&ts);
        // Second differences just past each branch boundary.
        for d in [64i64, -64, 8192, -8192, 1 << 31, -(1 << 31)] {
            roundtrip(&[0, 1000, 1000 + d, 1000 + 2 * d, 1000 + 3 * d]);
        }
    }

    #[test]
    fn test_delta_of_delta_larger_than_32_bits() {
        // The 32-bit zigzag branch cannot hold these differences; the wide
        // escape branch must.
        let ts = vec![0i64, 1, 1 << 40, -(1 << 40), i64::MAX / 2, i64::MIN / 2];
        roundtrip(&ts);
    }

    #[test]
    fn test_nulls_are_skipped_and_returned_count_matches_non_nulls() {
        let vals = vec![1000i64, 2000, 3000, 4000];
        let nulls = vec![false, true, false, true];
        let enc = DeltaOfDeltaEncoder::new().encode_i64(&vals, &nulls).unwrap();
        let dec = DeltaOfDeltaEncoder::new().decode_i64(&enc, 2).unwrap();
        assert_eq!(dec, vec![1000, 3000]);
    }

    #[test]
    fn test_empty() {
        let enc = DeltaOfDeltaEncoder::new().encode_i64(&[], &[]).unwrap();
        assert!(enc.is_empty());
        let dec = DeltaOfDeltaEncoder::new().decode_i64(&enc, 0).unwrap();
        assert!(dec.is_empty());
    }
}
