//! Gorilla encoding for floating-point values.
//!
//! Successive samples of a sensor reading are usually close together, so the
//! XOR of two adjacent IEEE-754 bit patterns has many leading and trailing
//! zero bits. Gorilla stores only the significant middle window.
//!
//! # Format
//!
//! ```text
//! first value: 64 bits
//! each later value:
//!   xor = cur ^ prev
//!   xor == 0                  -> '0'                        (1 bit)
//!   xor != 0:
//!     leading = xor.leading_zeros(), trailing = xor.trailing_zeros()
//!     window fits previous    -> '10' + significant_bits    (1 + 1 + w)
//!     otherwise               -> '11' + lead:6 + len:7 + significant_bits
//! ```
//!
//! `leading` needs 6 bits because it ranges over 0..=63, and `len` needs 7 bits
//! because it ranges over 1..=64 — a full-width XOR (`leading == 0`,
//! `trailing == 0`) has `len == 64`, which does not fit in 6 bits. Storing
//! these in 5 and 6 bits truncated both fields for high-entropy series.
//!
//! The reuse window is only taken when the new XOR's leading zeros are *at
//! least* the previous window's leading zeros and its trailing zeros are *at
//! least* the previous window's trailing zeros, which guarantees the stored
//! window fully covers the significant bits. The previous implementation
//! recomputed `trailing` from `leading + significant_bits`, which silently
//! produced wrong values for any varying series.

use super::bitio::{BitReader, BitWriter, EncodingError};
use super::Encoder;
use crate::encoding::EncodingType;

/// `leading` spans 0..=63, so 6 bits.
const LEADING_BITS: usize = 6;
/// `len` spans 1..=64, so 7 bits.
const LEN_BITS: usize = 7;

pub struct GorillaEncoder;

impl GorillaEncoder {
    pub fn new() -> Self {
        Self
    }
}

impl Encoder for GorillaEncoder {
    fn encoding_type(&self) -> EncodingType {
        EncodingType::Gorilla
    }

    fn encode_i64(&self, values: &[i64], nulls: &[bool]) -> anyhow::Result<Vec<u8>> {
        let bits: Vec<f64> = values.iter().map(|&v| f64::from_bits(v as u64)).collect();
        self.encode_impl(&bits, nulls)
    }

    fn decode_i64(&self, data: &[u8], count: usize) -> anyhow::Result<Vec<i64>> {
        Ok(self
            .decode_impl(data, count)?
            .into_iter()
            .map(|v| v.to_bits() as i64)
            .collect())
    }

    fn encode_f64(&self, values: &[f64], nulls: &[bool]) -> anyhow::Result<Vec<u8>> {
        self.encode_impl(values, nulls)
    }

    fn decode_f64(&self, data: &[u8], count: usize) -> anyhow::Result<Vec<f64>> {
        self.decode_impl(data, count)
    }

    fn encode_str(&self, _values: &[String], _nulls: &[bool]) -> anyhow::Result<Vec<u8>> {
        Err(EncodingError::Corrupt("gorilla cannot encode strings".into()).into())
    }

    fn decode_str(&self, _data: &[u8], _count: usize) -> anyhow::Result<Vec<String>> {
        Err(EncodingError::Corrupt("gorilla cannot decode strings".into()).into())
    }
}

impl GorillaEncoder {
    fn encode_impl(&self, values: &[f64], nulls: &[bool]) -> anyhow::Result<Vec<u8>> {
        let vals = compact_f64(values, nulls);
        if vals.is_empty() {
            return Ok(Vec::new());
        }

        let mut w = BitWriter::with_capacity(vals.len() * 4);
        let mut prev = vals[0].to_bits();
        w.write_u64(prev);

        // No previous window yet.
        let mut prev_leading: u32 = 0;
        let mut prev_len: u32 = 0;
        let mut have_window = false;

        for &v in &vals[1..] {
            let cur = v.to_bits();
            let xor = cur ^ prev;
            prev = cur;

            if xor == 0 {
                w.write_bit(false);
                continue;
            }

            let leading = xor.leading_zeros();
            let trailing = xor.trailing_zeros();
            let len = 64 - leading - trailing;

            // Reuse the previous window only if it fully contains this XOR's
            // significant bits.
            if have_window
                && leading >= prev_leading
                && trailing >= 64 - prev_leading - prev_len
                && prev_len > 0
            {
                w.write_bits(0b10, 2);
                w.write_bits(xor >> (64 - prev_leading - prev_len), prev_len as usize);
            } else {
                w.write_bits(0b11, 2);
                w.write_bits(leading as u64, LEADING_BITS);
                w.write_bits(len as u64, LEN_BITS);
                w.write_bits(xor >> trailing, len as usize);
                prev_leading = leading;
                prev_len = len;
                have_window = true;
            }
        }

        Ok(w.finish())
    }

    fn decode_impl(&self, data: &[u8], count: usize) -> anyhow::Result<Vec<f64>> {
        if count == 0 || data.is_empty() {
            return Ok(Vec::new());
        }

        let mut r = BitReader::new(data);
        let mut out = Vec::with_capacity(count);

        let mut prev = r.read_u64()?;
        out.push(f64::from_bits(prev));
        if count == 1 {
            return Ok(out);
        }

        // Window state starts unset; a reuse frame ('10') is only ever emitted
        // after a new-window frame ('11') has established it.
        let mut prev_leading: u32 = 0;
        let mut prev_len: u32 = 0;

        while out.len() < count {
            if !r.read_bit()? {
                out.push(f64::from_bits(prev));
                continue;
            }

            let (leading, len) = if !r.read_bit()? {
                // Reuse window: leading and len come from the previous frame.
                (prev_leading, prev_len)
            } else {
                let l = r.read_bits(LEADING_BITS)? as u32;
                let n = r.read_bits(LEN_BITS)? as u32;
                (l, n)
            };

            if leading + len > 64 {
                return Err(EncodingError::Corrupt(format!(
                    "gorilla window out of range: leading={} len={}",
                    leading, len
                ))
                .into());
            }

            let trailing = 64 - leading - len;
            let significant = r.read_bits(len as usize)?;
            let xor = significant << trailing;
            prev ^= xor;

            out.push(f64::from_bits(prev));
            prev_leading = leading;
            prev_len = len;
        }

        Ok(out)
    }
}

pub(crate) fn compact_f64(values: &[f64], nulls: &[bool]) -> Vec<f64> {
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

    fn roundtrip(vals: &[f64]) {
        let nulls = vec![false; vals.len()];
        let enc = GorillaEncoder::new().encode_f64(vals, &nulls).unwrap();
        let dec = GorillaEncoder::new().decode_f64(&enc, vals.len()).unwrap();
        assert_eq!(dec.len(), vals.len(), "length mismatch for {} values", vals.len());
        for (i, (a, b)) in dec.iter().zip(vals.iter()).enumerate() {
            assert_eq!(
                a.to_bits(),
                b.to_bits(),
                "mismatch at index {}: got {} want {}",
                i,
                a,
                b
            );
        }
    }

    #[test]
    fn test_constant_series() {
        let vals = vec![3.14f64; 1000];
        roundtrip(&vals);
        let enc = GorillaEncoder::new().encode_f64(&vals, &vec![false; 1000]).unwrap();
        // 64 bits for the first value + 1 bit per repeat.
        assert!(enc.len() < 200, "constant series encoded to {} bytes", enc.len());
    }

    #[test]
    fn test_smooth_ramp_exact() {
        // The case that previously produced 499/500 wrong values.
        let vals: Vec<f64> = (0..500).map(|i| 20.0 + i as f64 * 0.037).collect();
        roundtrip(&vals);
    }

    #[test]
    fn test_noisy_sensor_readings() {
        let mut seed = 0x9E3779B97F4A7C15u64;
        let mut vals = Vec::new();
        let mut cur = 25.0f64;
        for _ in 0..4000 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let noise = ((seed >> 33) as f64 / (1u64 << 31) as f64) - 0.5;
            cur += noise * 0.1;
            vals.push(cur);
        }
        roundtrip(&vals);
    }

    #[test]
    fn test_alternating_extremes() {
        let mut vals = Vec::new();
        for i in 0..500 {
            vals.push(if i % 2 == 0 { f64::MIN_POSITIVE } else { f64::MAX });
        }
        roundtrip(&vals);
    }

    #[test]
    fn test_negative_and_zero() {
        roundtrip(&[0.0, -0.0, 1.0, -1.0, 0.5, -0.5]);
    }

    #[test]
    fn test_nan_and_infinity_preserved_bitwise() {
        let vals = vec![f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 1.0];
        roundtrip(&vals);
    }

    #[test]
    fn test_windows_that_overflow_narrow_fields() {
        // `leading` above 31 and `len` above 63 are both representable, and the
        // previous 5/6-bit fields truncated them.
        let vals = vec![0.0f64, -0.0, 1.0, 1.0, 0.0];
        roundtrip(&vals);

        // A full-width XOR (leading 0, trailing 0 => len 64).
        roundtrip(&[f64::from_bits(0), f64::from_bits(u64::MAX)]);

        // Leading zeros of 32 and above, which need 6 bits.
        roundtrip(&[f64::from_bits(0), f64::from_bits(1), f64::from_bits(0), f64::from_bits(1)]);
        roundtrip(&[1.0f64, f64::from_bits(1), 1.0, f64::from_bits(1)]);
    }

    #[test]
    fn test_repeated_window_reuse_is_exact() {
        // Forces the '10' reuse path repeatedly, then a wider window, then
        // back to reuse.
        let mut vals = Vec::new();
        for i in 0..200 {
            vals.push(f64::from_bits(if i % 3 == 0 { 0x40400000 } else { 0x4040_0000 | (i as u64 & 0xFFF) }));
        }
        roundtrip(&vals);
    }

    #[test]
    fn test_random_bits_stress() {
        let mut seed = 0xDEADBEEFCAFEF00Du64;
        let vals: Vec<f64> = (0..3000)
            .map(|_| {
                seed = seed
                    .wrapping_mul(2862933555777941757)
                    .wrapping_add(3037000493);
                f64::from_bits(seed)
            })
            .collect();
        roundtrip(&vals);
    }

    #[test]
    fn test_single_value() {
        roundtrip(&[42.0]);
    }

    #[test]
    fn test_empty() {
        let enc = GorillaEncoder::new().encode_f64(&[], &[]).unwrap();
        assert!(enc.is_empty());
    }

    #[test]
    fn test_truncated_stream_errors_instead_of_corrupting() {
        let vals: Vec<f64> = (0..200).map(|i| i as f64 * 1.7).collect();
        let enc = GorillaEncoder::new().encode_f64(&vals, &vec![false; 200]).unwrap();
        let truncated = &enc[..enc.len() / 2];
        match GorillaEncoder::new().decode_f64(truncated, 200) {
            Ok(v) => panic!("expected an error, got {} values", v.len()),
            Err(_) => {}
        }
    }

    #[test]
    fn test_smooth_data_compresses_better_than_raw() {
        let vals: Vec<f64> = (0..2000).map(|i| 20.0 + (i as f64).sin() * 0.5).collect();
        let enc = GorillaEncoder::new().encode_f64(&vals, &vec![false; 2000]).unwrap();
        let raw = vals.len() * 8;
        assert!(
            enc.len() * 10 < raw * 9,
            "smooth data compressed poorly: {} vs {}",
            enc.len(),
            raw
        );
    }

    /// Why Gorilla cannot do better on arbitrary f64 series.
    ///
    /// Around 20.0 the exponent is fixed and the XOR window spans the whole
    /// mantissa, so a frame costs `2 + 6 + 7 + 53` bits — more than the 64 bits
    /// a raw value would take. This is inherent to 64-bit floats with noisy low
    /// mantissa bits, and it is why [`super::selector`] scores Gorilla against a
    /// raw baseline instead of assuming it wins.
    #[test]
    fn test_gorilla_can_expand_on_noisy_mantissa() {
        let vals: Vec<f64> = (0..500).map(|i| (i as f64 * 1.234_567).sin() * 1e6).collect();
        let enc = GorillaEncoder::new().encode_f64(&vals, &vec![false; 500]).unwrap();
        // Correctness still holds even when the encoding is not a win.
        let dec = GorillaEncoder::new().decode_f64(&enc, 500).unwrap();
        assert_eq!(dec.len(), 500);
        for (a, b) in dec.iter().zip(vals.iter()) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
    }

    /// A series whose values share low-order mantissa bits (a slow ramp) is
    /// where the window-reuse path actually pays off.
    #[test]
    fn test_ramp_with_stable_trailing_zeros_compresses_well() {
        let vals: Vec<f64> = (0..2000).map(|i| 1000.0 + i as f64 * 0.25).collect();
        let enc = GorillaEncoder::new().encode_f64(&vals, &vec![false; 2000]).unwrap();
        let raw = vals.len() * 8;
        assert!(
            enc.len() * 3 < raw,
            "ramp should compress via window reuse: {} vs {}",
            enc.len(),
            raw
        );
    }

    #[test]
    fn test_nulls_compacted() {
        let vals = vec![1.0f64, 2.0, 3.0, 4.0];
        let nulls = vec![false, true, false, true];
        let enc = GorillaEncoder::new().encode_f64(&vals, &nulls).unwrap();
        let dec = GorillaEncoder::new().decode_f64(&enc, 2).unwrap();
        assert_eq!(dec, vec![1.0, 3.0]);
    }
}
