//! Dictionary encoding for low-cardinality columns.
//!
//! Tags, device ids and status strings repeat heavily, so replacing each value
//! with an index into a small dictionary is far cheaper than storing the value
//! itself. Integers use fixed-width dictionary entries; strings use length
//! prefixed entries and therefore handle variable-width data correctly, which
//! the previous flat-bytes implementation could not.
//!
//! # Integer format
//!
//! ```text
//! dict_len: u32
//! dict_len * i64
//! count * varint(index)
//! ```
//!
//! # String format
//!
//! ```text
//! dict_len: u32
//! dict_len * (len: u32 + bytes)
//! count * varint(index)
//! ```

use std::collections::HashMap;

use super::bitio::{read_varint, write_varint, EncodingError};
use super::Encoder;
use crate::encoding::EncodingType;

pub struct DictEncoder;

impl DictEncoder {
    pub fn new() -> Self {
        Self
    }
}

impl Encoder for DictEncoder {
    fn encoding_type(&self) -> EncodingType {
        EncodingType::Dictionary
    }

    fn encode_i64(&self, values: &[i64], nulls: &[bool]) -> anyhow::Result<Vec<u8>> {
        self.encode_int_impl(values, nulls)
    }

    fn decode_i64(&self, data: &[u8], count: usize) -> anyhow::Result<Vec<i64>> {
        self.decode_int_impl(data, count)
    }

    fn encode_f64(&self, values: &[f64], nulls: &[bool]) -> anyhow::Result<Vec<u8>> {
        let bits: Vec<i64> = values.iter().map(|v| v.to_bits() as i64).collect();
        self.encode_int_impl(&bits, nulls)
    }

    fn decode_f64(&self, data: &[u8], count: usize) -> anyhow::Result<Vec<f64>> {
        Ok(self
            .decode_int_impl(data, count)?
            .into_iter()
            .map(|v| f64::from_bits(v as u64))
            .collect())
    }

    fn encode_str(&self, values: &[String], nulls: &[bool]) -> anyhow::Result<Vec<u8>> {
        DictStringEncoder::new().encode_str(values, nulls)
    }

    fn decode_str(&self, data: &[u8], count: usize) -> anyhow::Result<Vec<String>> {
        DictStringEncoder::new().decode_str(data, count)
    }
}

impl DictEncoder {
    fn encode_int_impl(&self, values: &[i64], nulls: &[bool]) -> anyhow::Result<Vec<u8>> {
        let vals = super::delta::compact(values, nulls);
        if vals.is_empty() {
            return Ok(Vec::new());
        }

        let mut dict: Vec<i64> = Vec::new();
        let mut index: HashMap<i64, u32> = HashMap::new();
        for &v in &vals {
            let next = dict.len() as u32;
            index.entry(v).or_insert_with(|| {
                dict.push(v);
                next
            });
        }

        let mut out = Vec::with_capacity(4 + dict.len() * 8 + vals.len());
        out.extend_from_slice(&(dict.len() as u32).to_le_bytes());
        for &v in &dict {
            out.extend_from_slice(&v.to_le_bytes());
        }
        for &v in &vals {
            write_varint(&mut out, index[&v] as u64);
        }
        Ok(out)
    }

    fn decode_int_impl(&self, data: &[u8], count: usize) -> anyhow::Result<Vec<i64>> {
        if count == 0 || data.is_empty() {
            return Ok(Vec::new());
        }
        if data.len() < 4 {
            return Err(EncodingError::UnexpectedEof.into());
        }

        let dict_len = u32::from_le_bytes(data[0..4].try_into()?) as usize;
        let dict_bytes = dict_len
            .checked_mul(8)
            .ok_or_else(|| EncodingError::Corrupt("dictionary length overflow".into()))?;
        if 4 + dict_bytes > data.len() {
            return Err(EncodingError::UnexpectedEof.into());
        }

        let dict: Vec<i64> = data[4..4 + dict_bytes]
            .chunks_exact(8)
            .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
            .collect();

        let mut out = Vec::with_capacity(count);
        let mut pos = 4 + dict_bytes;
        for _ in 0..count {
            let (idx, n) = read_varint(data, pos)?;
            pos += n;
            let v = *dict
                .get(idx as usize)
                .ok_or_else(|| EncodingError::Corrupt(format!("dict index {} out of range", idx)))?;
            out.push(v);
        }
        Ok(out)
    }
}

/// Dictionary encoding for variable-width UTF-8 strings.
pub struct DictStringEncoder;

impl DictStringEncoder {
    pub fn new() -> Self {
        Self
    }
}

impl Encoder for DictStringEncoder {
    fn encoding_type(&self) -> EncodingType {
        EncodingType::Dictionary
    }

    fn encode_str(&self, values: &[String], nulls: &[bool]) -> anyhow::Result<Vec<u8>> {
        self.encode_str_impl(values, nulls)
    }

    fn decode_str(&self, data: &[u8], count: usize) -> anyhow::Result<Vec<String>> {
        self.decode_str_impl(data, count)
    }

    fn encode_i64(&self, _values: &[i64], _nulls: &[bool]) -> anyhow::Result<Vec<u8>> {
        Err(EncodingError::Corrupt(
            "string dictionary cannot encode integers".into(),
        )
        .into())
    }

    fn decode_i64(&self, _data: &[u8], _count: usize) -> anyhow::Result<Vec<i64>> {
        Err(EncodingError::Corrupt(
            "string dictionary cannot decode integers".into(),
        )
        .into())
    }

    fn encode_f64(&self, _values: &[f64], _nulls: &[bool]) -> anyhow::Result<Vec<u8>> {
        Err(EncodingError::Corrupt(
            "string dictionary cannot encode floats".into(),
        )
        .into())
    }

    fn decode_f64(&self, _data: &[u8], _count: usize) -> anyhow::Result<Vec<f64>> {
        Err(EncodingError::Corrupt(
            "string dictionary cannot decode floats".into(),
        )
        .into())
    }
}

impl DictStringEncoder {
    fn encode_str_impl(&self, values: &[String], nulls: &[bool]) -> anyhow::Result<Vec<u8>> {
        let vals = compact_str(values, nulls);
        if vals.is_empty() {
            return Ok(Vec::new());
        }

        let mut dict: Vec<&str> = Vec::new();
        let mut index: HashMap<&str, u32> = HashMap::new();
        for v in &vals {
            let next = dict.len() as u32;
            index.entry(v.as_str()).or_insert_with(|| {
                dict.push(v.as_str());
                next
            });
        }

        let mut dict_part = Vec::new();
        dict_part.extend_from_slice(&(dict.len() as u32).to_le_bytes());
        for d in &dict {
            dict_part.extend_from_slice(&(d.len() as u32).to_le_bytes());
            dict_part.extend_from_slice(d.as_bytes());
        }

        let mut idx_part = Vec::with_capacity(vals.len());
        for v in &vals {
            write_varint(&mut idx_part, index[v.as_str()] as u64);
        }

        let mut out = Vec::with_capacity(4 + dict_part.len() + idx_part.len());
        out.extend_from_slice(&(dict_part.len() as u32).to_le_bytes());
        out.extend_from_slice(&dict_part);
        out.extend_from_slice(&idx_part);
        Ok(out)
    }

    fn decode_str_impl(&self, data: &[u8], count: usize) -> anyhow::Result<Vec<String>> {
        if count == 0 || data.is_empty() {
            return Ok(Vec::new());
        }
        if data.len() < 4 {
            return Err(EncodingError::UnexpectedEof.into());
        }

        let dict_part_len = u32::from_le_bytes(data[0..4].try_into()?) as usize;
        let dict_end = 4usize
            .checked_add(dict_part_len)
            .ok_or_else(|| EncodingError::Corrupt("dictionary section overflow".into()))?;
        if dict_end > data.len() {
            return Err(EncodingError::UnexpectedEof.into());
        }

        let dict_bytes = &data[4..dict_end];
        if dict_bytes.len() < 4 {
            return Err(EncodingError::UnexpectedEof.into());
        }
        let dict_len = u32::from_le_bytes(dict_bytes[0..4].try_into()?) as usize;

        let mut dict: Vec<&str> = Vec::with_capacity(dict_len);
        let mut dpos = 4;
        for _ in 0..dict_len {
            if dpos + 4 > dict_bytes.len() {
                return Err(EncodingError::UnexpectedEof.into());
            }
            let len = u32::from_le_bytes(dict_bytes[dpos..dpos + 4].try_into()?) as usize;
            dpos += 4;
            if dpos + len > dict_bytes.len() {
                return Err(EncodingError::UnexpectedEof.into());
            }
            dict.push(
                std::str::from_utf8(&dict_bytes[dpos..dpos + len])
                    .map_err(|e| EncodingError::Corrupt(format!("invalid utf-8 in dictionary: {}", e)))?,
            );
            dpos += len;
        }

        // Indexes start right after the dictionary section; track that position
        // separately from the dictionary walk above, which is relative to
        // `dict_bytes`.
        let mut pos = dict_end;
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            let (idx, n) = read_varint(data, pos)?;
            pos += n;
            let s = *dict.get(idx as usize).ok_or_else(|| {
                EncodingError::Corrupt(format!("string dict index {} out of range", idx))
            })?;
            out.push(s.to_string());
        }
        Ok(out)
    }
}

pub(crate) fn compact_str(values: &[String], nulls: &[bool]) -> Vec<String> {
    if nulls.len() != values.len() || !nulls.iter().any(|&n| n) {
        return values.to_vec();
    }
    values
        .iter()
        .zip(nulls.iter())
        .filter(|(_, &n)| !n)
        .map(|(v, _)| v.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_int_low_cardinality_roundtrip() {
        let vals: Vec<i64> = (0..5000).map(|i| (i % 5) as i64).collect();
        let nulls = vec![false; 5000];
        let enc = DictEncoder::new().encode_i64(&vals, &nulls).unwrap();
        let dec = DictEncoder::new().decode_i64(&enc, 5000).unwrap();
        assert_eq!(dec, vals);

        let raw = vals.len() * 8;
        assert!(enc.len() * 2 < raw, "got {} vs raw {}", enc.len(), raw);
    }

    #[test]
    fn test_int_monotonic_ids_roundtrip() {
        // Previously RLE expanded this to 13000 bytes; dictionary must at
        // least not corrupt it.
        let vals: Vec<i64> = (0..1000).map(|i| 1_000_000 + i * 97).collect();
        let enc = DictEncoder::new().encode_i64(&vals, &vec![false; 1000]).unwrap();
        let dec = DictEncoder::new().decode_i64(&enc, 1000).unwrap();
        assert_eq!(dec, vals);
    }

    #[test]
    fn test_string_variable_width_roundtrip() {
        // The case that previously produced "alphabeg" / "<truncated>".
        let vals = vec![
            "alpha".to_string(),
            "be".to_string(),
            "gamma!".to_string(),
            "x".to_string(),
            "a".repeat(300),
        ];
        let enc = DictStringEncoder::new().encode_str(&vals, &vec![false; 5]).unwrap();
        let dec = DictStringEncoder::new().decode_str(&enc, 5).unwrap();
        assert_eq!(dec, vals);
    }

    #[test]
    fn test_string_tag_cardinality_roundtrip() {
        let vals: Vec<String> = (0..5000)
            .map(|i| format!("device-{}", i % 20))
            .collect();
        let enc = DictStringEncoder::new().encode_str(&vals, &vec![false; 5000]).unwrap();
        let dec = DictStringEncoder::new().decode_str(&enc, 5000).unwrap();
        assert_eq!(dec, vals);

        let raw: usize = vals.iter().map(|s| s.len() + 8).sum();
        assert!(enc.len() * 4 < raw, "got {} vs raw {}", enc.len(), raw);
    }

    #[test]
    fn test_string_empty_and_unicode() {
        let vals = vec![
            String::new(),
            "温度".to_string(),
            "\u{1F321}\u{FE0F}".to_string(),
            "\u{0}".to_string(),
        ];
        let enc = DictStringEncoder::new().encode_str(&vals, &vec![false; 4]).unwrap();
        let dec = DictStringEncoder::new().decode_str(&enc, 4).unwrap();
        assert_eq!(dec, vals);
    }

    #[test]
    fn test_string_all_unique() {
        let vals: Vec<String> = (0..200).map(|i| format!("u{}", i)).collect();
        let enc = DictStringEncoder::new().encode_str(&vals, &vec![false; 200]).unwrap();
        let dec = DictStringEncoder::new().decode_str(&enc, 200).unwrap();
        assert_eq!(dec, vals);
    }

    #[test]
    fn test_corrupt_index_errors() {
        let vals = vec![1i64, 2, 3];
        let mut enc = DictEncoder::new().encode_i64(&vals, &vec![false; 3]).unwrap();
        // Point the last index far past the dictionary.
        let last = enc.len() - 1;
        enc[last] = 0x7F;
        assert!(DictEncoder::new().decode_i64(&enc, 3).is_err());
    }

    #[test]
    fn test_truncated_int_stream_errors() {
        let vals: Vec<i64> = (0..100).map(|i| i % 7).collect();
        let enc = DictEncoder::new().encode_i64(&vals, &vec![false; 100]).unwrap();
        assert!(DictEncoder::new().decode_i64(&enc[..4], 100).is_err());
    }

    #[test]
    fn test_empty_inputs() {
        assert!(DictEncoder::new().encode_i64(&[], &[]).unwrap().is_empty());
        assert!(DictStringEncoder::new().encode_str(&[], &[]).unwrap().is_empty());
    }
}
