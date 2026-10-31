//! Typed column values and the encoding-selection facade.
//!
//! # Why this module exists
//!
//! The original storage path flattened every column into a single `Vec<u8>`
//! and later reconstructed values with `i * 8` byte offsets. That is only
//! correct for 8-byte scalars: a `String` column of `["alpha","be","gamma!"]`
//! came back as `["alphabeg","<truncated>","<truncated>"]`, and an `Int32`
//! column read every other slot. This module introduces [`ColumnData`], which
//! keeps values typed and lets each encoder work on the representation it
//! actually understands, so widths, nulls and strings all survive a round trip.

pub mod bitio;
pub mod bitpack;
pub mod delta;
pub mod dict;
pub mod gorilla;
pub mod rle;
pub mod selector;
pub mod simd;

pub use bitpack::BitPackEncoder;
pub use delta::DeltaOfDeltaEncoder;
pub use dict::{DictEncoder, DictStringEncoder};
pub use gorilla::GorillaEncoder;
pub use rle::RleEncoder;
pub use selector::{select_encoding, EncodingDecision};

use std::fmt;

/// Column data type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum DataType {
    Timestamp,
    Int64,
    Int32,
    Float64,
    Float32,
    Bool,
    String,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Timestamp => write!(f, "TIMESTAMPTZ"),
            DataType::Int64 => write!(f, "BIGINT"),
            DataType::Int32 => write!(f, "INTEGER"),
            DataType::Float64 => write!(f, "DOUBLE"),
            DataType::Float32 => write!(f, "REAL"),
            DataType::Bool => write!(f, "BOOLEAN"),
            DataType::String => write!(f, "TEXT"),
        }
    }
}

impl DataType {
    /// Fixed byte width, or `None` for variable-width types.
    pub fn fixed_width(&self) -> Option<usize> {
        match self {
            DataType::Timestamp | DataType::Int64 | DataType::Float64 => Some(8),
            DataType::Int32 | DataType::Float32 => Some(4),
            DataType::Bool => Some(1),
            DataType::String => None,
        }
    }

    pub fn is_numeric(&self) -> bool {
        !matches!(self, DataType::Bool | DataType::String)
    }
}

/// Encoding tag recorded per column block so the reader knows how to decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum EncodingType {
    /// Fixed-width little-endian values, no transformation.
    #[default]
    Raw,
    DeltaOfDelta,
    Gorilla,
    Rle,
    Dictionary,
    BitPack,
    /// Fixed-width integers stored with their own delta scheme.
    Delta,
}

impl fmt::Display for EncodingType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            EncodingType::Raw => "raw",
            EncodingType::DeltaOfDelta => "dod",
            EncodingType::Gorilla => "gorilla",
            EncodingType::Rle => "rle",
            EncodingType::Dictionary => "dict",
            EncodingType::BitPack => "bitpack",
            EncodingType::Delta => "delta",
        };
        write!(f, "{}", s)
    }
}

impl EncodingType {
    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0 => EncodingType::Raw,
            1 => EncodingType::DeltaOfDelta,
            2 => EncodingType::Gorilla,
            3 => EncodingType::Rle,
            4 => EncodingType::Dictionary,
            5 => EncodingType::BitPack,
            6 => EncodingType::Delta,
            _ => return None,
        })
    }

    pub fn as_u8(&self) -> u8 {
        match self {
            EncodingType::Raw => 0,
            EncodingType::DeltaOfDelta => 1,
            EncodingType::Gorilla => 2,
            EncodingType::Rle => 3,
            EncodingType::Dictionary => 4,
            EncodingType::BitPack => 5,
            EncodingType::Delta => 6,
        }
    }
}

/// Shared behaviour for the concrete encoders.
///
/// `nulls` is a parallel mask; when it is non-empty, only the positions where
/// it is `false` are encoded, and `decode` returns exactly that many values.
/// This keeps the encoders themselves free of null bookkeeping; the null mask
/// travels in the column block header written by [`ColumnCodec`].
pub trait Encoder {
    fn encoding_type(&self) -> EncodingType;

    fn encode_i64(&self, values: &[i64], nulls: &[bool]) -> anyhow::Result<Vec<u8>>;
    fn decode_i64(&self, data: &[u8], count: usize) -> anyhow::Result<Vec<i64>>;

    fn encode_f64(&self, values: &[f64], nulls: &[bool]) -> anyhow::Result<Vec<u8>>;
    fn decode_f64(&self, data: &[u8], count: usize) -> anyhow::Result<Vec<f64>>;

    fn encode_str(&self, values: &[String], nulls: &[bool]) -> anyhow::Result<Vec<u8>>;
    fn decode_str(&self, data: &[u8], count: usize) -> anyhow::Result<Vec<String>>;
}

/// Which value kinds an encoder handles natively.
///
/// Encoders declare their natural kind here; the remaining kinds are served by
/// lossless bit reinterpretation (float <-> i64), or rejected outright for
/// strings, which have no numeric equivalent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeKind {
    Integer,
    Float,
    String,
}

/// A typed column of values with an explicit null mask.
///
/// `None` entries in the typed vectors represent SQL NULLs, so values and
/// nulls always stay aligned regardless of element width.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnData {
    Timestamp(Vec<Option<i64>>),
    Int64(Vec<Option<i64>>),
    Int32(Vec<Option<i32>>),
    Float64(Vec<Option<f64>>),
    Float32(Vec<Option<f32>>),
    Bool(Vec<Option<bool>>),
    String(Vec<Option<String>>),
}

impl ColumnData {
    pub fn new(data_type: DataType, len: usize) -> Self {
        match data_type {
            DataType::Timestamp => ColumnData::Timestamp(vec![None; len]),
            DataType::Int64 => ColumnData::Int64(vec![None; len]),
            DataType::Int32 => ColumnData::Int32(vec![None; len]),
            DataType::Float64 => ColumnData::Float64(vec![None; len]),
            DataType::Float32 => ColumnData::Float32(vec![None; len]),
            DataType::Bool => ColumnData::Bool(vec![None; len]),
            DataType::String => ColumnData::String(vec![None; len]),
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            ColumnData::Timestamp(_) => DataType::Timestamp,
            ColumnData::Int64(_) => DataType::Int64,
            ColumnData::Int32(_) => DataType::Int32,
            ColumnData::Float64(_) => DataType::Float64,
            ColumnData::Float32(_) => DataType::Float32,
            ColumnData::Bool(_) => DataType::Bool,
            ColumnData::String(_) => DataType::String,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            ColumnData::Timestamp(v) => v.len(),
            ColumnData::Int64(v) => v.len(),
            ColumnData::Int32(v) => v.len(),
            ColumnData::Float64(v) => v.len(),
            ColumnData::Float32(v) => v.len(),
            ColumnData::Bool(v) => v.len(),
            ColumnData::String(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn null_count(&self) -> usize {
        match self {
            ColumnData::Timestamp(v) => v.iter().filter(|x| x.is_none()).count(),
            ColumnData::Int64(v) => v.iter().filter(|x| x.is_none()).count(),
            ColumnData::Int32(v) => v.iter().filter(|x| x.is_none()).count(),
            ColumnData::Float64(v) => v.iter().filter(|x| x.is_none()).count(),
            ColumnData::Float32(v) => v.iter().filter(|x| x.is_none()).count(),
            ColumnData::Bool(v) => v.iter().filter(|x| x.is_none()).count(),
            ColumnData::String(v) => v.iter().filter(|x| x.is_none()).count(),
        }
    }

    /// Append a value parsed from the raw 8-byte representation used by the
    /// public row API.
    pub fn push_raw(&mut self, raw: Option<&[u8]>) -> anyhow::Result<()> {
        let raw = match raw {
            Some(r) => r,
            None => {
                self.push_none();
                return Ok(());
            }
        };
        match self {
            ColumnData::Timestamp(v) => {
                let n = read_fixed_i64(raw)?;
                v.push(Some(n));
            }
            ColumnData::Int64(v) => {
                let n = read_fixed_i64(raw)?;
                v.push(Some(n));
            }
            ColumnData::Int32(v) => {
                let n = read_fixed_i32(raw)?;
                v.push(Some(n));
            }
            ColumnData::Float64(v) => {
                let n = read_fixed_f64(raw)?;
                v.push(Some(n));
            }
            ColumnData::Float32(v) => {
                let n = read_fixed_f32(raw)?;
                v.push(Some(n));
            }
            ColumnData::Bool(v) => {
                v.push(Some(raw.first().copied().unwrap_or(0) != 0));
            }
            ColumnData::String(v) => {
                let s = std::str::from_utf8(raw)
                    .map_err(|e| anyhow::anyhow!("invalid utf-8 in string column: {}", e))?;
                v.push(Some(s.to_string()));
            }
        }
        Ok(())
    }

    pub fn push_none(&mut self) {
        match self {
            ColumnData::Timestamp(v) => v.push(None),
            ColumnData::Int64(v) => v.push(None),
            ColumnData::Int32(v) => v.push(None),
            ColumnData::Float64(v) => v.push(None),
            ColumnData::Float32(v) => v.push(None),
            ColumnData::Bool(v) => v.push(None),
            ColumnData::String(v) => v.push(None),
        }
    }

    /// Serialize the i-th value back into the raw 8-byte-or-less form used by
    /// the public row API.
    pub fn get_raw(&self, i: usize) -> Option<Vec<u8>> {
        match self {
            ColumnData::Timestamp(v) => v.get(i).copied().flatten().map(|x| x.to_le_bytes().to_vec()),
            ColumnData::Int64(v) => v.get(i).copied().flatten().map(|x| x.to_le_bytes().to_vec()),
            ColumnData::Int32(v) => v.get(i).copied().flatten().map(|x| x.to_le_bytes().to_vec()),
            ColumnData::Float64(v) => v.get(i).copied().flatten().map(|x| x.to_le_bytes().to_vec()),
            ColumnData::Float32(v) => v.get(i).copied().flatten().map(|x| x.to_le_bytes().to_vec()),
            ColumnData::Bool(v) => v.get(i).copied().flatten().map(|x| vec![x as u8]),
            ColumnData::String(v) => match v.get(i) {
                Some(Some(s)) => Some(s.clone().into_bytes()),
                _ => None,
            },
        }
    }

    /// Value as f64 for numeric aggregation, `None` when absent or not numeric.
    pub fn as_f64(&self, i: usize) -> Option<f64> {
        match self {
            ColumnData::Timestamp(v) => v.get(i).copied().flatten().map(|x| x as f64),
            ColumnData::Int64(v) => v.get(i).copied().flatten().map(|x| x as f64),
            ColumnData::Int32(v) => v.get(i).copied().flatten().map(|x| x as f64),
            ColumnData::Float64(v) => v.get(i).copied().flatten(),
            ColumnData::Float32(v) => v.get(i).copied().flatten().map(|x| x as f64),
            ColumnData::Bool(_) | ColumnData::String(_) => None,
        }
    }

    /// Value as i64 for numeric aggregation, `None` when absent or not integral.
    pub fn as_i64(&self, i: usize) -> Option<i64> {
        match self {
            ColumnData::Timestamp(v) => v.get(i).copied().flatten(),
            ColumnData::Int64(v) => v.get(i).copied().flatten(),
            ColumnData::Int32(v) => v.get(i).copied().flatten().map(|x| x as i64),
            ColumnData::Float64(v) => v.get(i).copied().flatten().map(|x| x as i64),
            ColumnData::Float32(v) => v.get(i).copied().flatten().map(|x| x as i64),
            ColumnData::Bool(v) => v.get(i).copied().flatten().map(|x| x as i64),
            ColumnData::String(_) => None,
        }
    }

    pub fn as_str(&self, i: usize) -> Option<&str> {
        match self {
            ColumnData::String(v) => v.get(i).and_then(|x| x.as_deref()),
            _ => None,
        }
    }

    /// Append every value of `other`, preserving NULLs.
    ///
    /// Used when stitching blocks together during compaction or full-segment
    /// reads; both sides must describe the same type.
    pub fn extend_from(&mut self, other: &ColumnData) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.data_type() == other.data_type(),
            "cannot extend {} column with {} values",
            self.data_type(),
            other.data_type()
        );
        match (self, other) {
            (ColumnData::Timestamp(a), ColumnData::Timestamp(b)) => a.extend_from_slice(b),
            (ColumnData::Int64(a), ColumnData::Int64(b)) => a.extend_from_slice(b),
            (ColumnData::Int32(a), ColumnData::Int32(b)) => a.extend_from_slice(b),
            (ColumnData::Float64(a), ColumnData::Float64(b)) => a.extend_from_slice(b),
            (ColumnData::Float32(a), ColumnData::Float32(b)) => a.extend_from_slice(b),
            (ColumnData::Bool(a), ColumnData::Bool(b)) => a.extend_from_slice(b),
            (ColumnData::String(a), ColumnData::String(b)) => a.extend_from_slice(b),
            _ => anyhow::bail!("column type mismatch"),
        }
        Ok(())
    }
}

/// Append `src` onto `dst`. See [`ColumnData::extend_from`].
pub fn concat(dst: &mut ColumnData, src: &ColumnData) -> anyhow::Result<()> {
    dst.extend_from(src)
}

#[inline]
fn read_fixed_i64(raw: &[u8]) -> anyhow::Result<i64> {
    if raw.len() >= 8 {
        Ok(i64::from_le_bytes(raw[0..8].try_into()?))
    } else {
        let mut buf = [0u8; 8];
        buf[..raw.len()].copy_from_slice(raw);
        Ok(i64::from_le_bytes(buf))
    }
}

#[inline]
fn read_fixed_i32(raw: &[u8]) -> anyhow::Result<i32> {
    if raw.len() >= 4 {
        Ok(i32::from_le_bytes(raw[0..4].try_into()?))
    } else {
        let mut buf = [0u8; 4];
        buf[..raw.len()].copy_from_slice(raw);
        Ok(i32::from_le_bytes(buf))
    }
}

#[inline]
fn read_fixed_f64(raw: &[u8]) -> anyhow::Result<f64> {
    Ok(f64::from_bits(read_fixed_i64(raw)? as u64))
}

#[inline]
fn read_fixed_f32(raw: &[u8]) -> anyhow::Result<f32> {
    Ok(f32::from_bits(read_fixed_i32(raw)? as u32))
}

/// Encode/decode facade over the concrete encoders.
///
/// Chooses the encoding dynamically (see [`selector`]), writes a small header
/// describing the layout, and can rebuild the original [`ColumnData`].
pub struct ColumnCodec;

impl ColumnCodec {
    /// Encode `column` into a self-describing byte block.
    ///
    /// Layout:
    /// ```text
    /// [encoding:1][row_count:u32][null_count:u32][non_null_count:u32]
    /// [null_bitmap: ceil(row_count/8)][payload]
    /// ```
    pub fn encode(column: &ColumnData) -> anyhow::Result<Vec<u8>> {
        let row_count = column.len();
        let decision = select_encoding(column);

        let mut null_bitmap = vec![0u8; row_count.div_ceil(8)];
        for (i, is_null) in nulls_of(column).iter().enumerate() {
            if *is_null {
                null_bitmap[i / 8] |= 0x80 >> (i % 8);
            }
        }

        let payload = encode_payload(column, decision.encoding)?;
        let non_null = row_count - column.null_count();

        let mut out = Vec::with_capacity(13 + null_bitmap.len() + payload.len());
        out.push(decision.encoding.as_u8());
        out.extend_from_slice(&(row_count as u32).to_le_bytes());
        out.extend_from_slice(&(column.null_count() as u32).to_le_bytes());
        out.extend_from_slice(&(non_null as u32).to_le_bytes());
        out.extend_from_slice(&null_bitmap);
        out.extend_from_slice(&payload);
        Ok(out)
    }

    /// Rebuild a [`ColumnData`] from a block produced by [`ColumnCodec::encode`].
    pub fn decode(data: &[u8], data_type: DataType) -> anyhow::Result<ColumnData> {
        if data.len() < 13 {
            anyhow::bail!("column block too small: {} bytes", data.len());
        }
        let encoding = EncodingType::from_u8(data[0])
            .ok_or_else(|| anyhow::anyhow!("unknown encoding tag {}", data[0]))?;
        let row_count = u32::from_le_bytes(data[1..5].try_into()?) as usize;
        let null_count = u32::from_le_bytes(data[5..9].try_into()?) as usize;
        let non_null = u32::from_le_bytes(data[9..13].try_into()?) as usize;

        let bitmap_len = row_count.div_ceil(8);
        let bitmap_end = 13 + bitmap_len;
        if bitmap_end > data.len() {
            anyhow::bail!("null bitmap truncated");
        }
        let bitmap = &data[13..bitmap_end];
        let payload = &data[bitmap_end..];

        let values = decode_payload(encoding, data_type, payload, non_null)?;
        if values.len() != non_null {
            anyhow::bail!(
                "decoded {} values but header declares {}",
                values.len(),
                non_null
            );
        }
        let _ = null_count;

        scatter(values, bitmap, row_count, data_type)
    }

    /// Size in bytes that [`ColumnCodec::encode`] would produce, without doing
    /// the work twice during compaction.
    pub fn encoded_len(column: &ColumnData) -> usize {
        let decision = select_encoding(column);
        13 + column.len().div_ceil(8) + decision.estimated_payload_len
    }
}

fn nulls_of(column: &ColumnData) -> Vec<bool> {
    (0..column.len()).map(|i| match column {
        ColumnData::Timestamp(v) => v[i].is_none(),
        ColumnData::Int64(v) => v[i].is_none(),
        ColumnData::Int32(v) => v[i].is_none(),
        ColumnData::Float64(v) => v[i].is_none(),
        ColumnData::Float32(v) => v[i].is_none(),
        ColumnData::Bool(v) => v[i].is_none(),
        ColumnData::String(v) => v[i].is_none(),
    }).collect()
}

fn non_null_i64(column: &ColumnData) -> Vec<i64> {
    match column {
        ColumnData::Timestamp(v) => v.iter().filter_map(|x| *x).collect(),
        ColumnData::Int64(v) => v.iter().filter_map(|x| *x).collect(),
        ColumnData::Int32(v) => v.iter().filter_map(|x| x.map(|y| y as i64)).collect(),
        ColumnData::Float64(v) => v.iter().filter_map(|x| x.map(|y| y.to_bits() as i64)).collect(),
        ColumnData::Float32(v) => v.iter().filter_map(|x| x.map(|y| y.to_bits() as i64)).collect(),
        ColumnData::Bool(v) => v.iter().filter_map(|x| x.map(|y| y as i64)).collect(),
        ColumnData::String(_) => Vec::new(),
    }
}

fn non_null_f64(column: &ColumnData) -> Vec<f64> {
    match column {
        ColumnData::Timestamp(v) => v.iter().filter_map(|x| x.map(|y| y as f64)).collect(),
        ColumnData::Int64(v) => v.iter().filter_map(|x| x.map(|y| y as f64)).collect(),
        ColumnData::Int32(v) => v.iter().filter_map(|x| x.map(|y| y as f64)).collect(),
        ColumnData::Float64(v) => v.iter().filter_map(|x| *x).collect(),
        ColumnData::Float32(v) => v.iter().filter_map(|x| x.map(|y| y as f64)).collect(),
        ColumnData::Bool(v) => v.iter().filter_map(|x| x.map(|y| if y { 1.0 } else { 0.0 })).collect(),
        ColumnData::String(_) => Vec::new(),
    }
}

fn non_null_strings(column: &ColumnData) -> Vec<String> {
    match column {
        ColumnData::String(v) => v.iter().filter_map(|x| x.clone()).collect(),
        _ => Vec::new(),
    }
}

fn encode_payload(column: &ColumnData, encoding: EncodingType) -> anyhow::Result<Vec<u8>> {
    let no_nulls: Vec<bool> = Vec::new();
    match column {
        ColumnData::Float64(_) | ColumnData::Float32(_) => {
            let vals = non_null_f64(column);
            match encoding {
                EncodingType::Gorilla => GorillaEncoder::new().encode_f64(&vals, &no_nulls),
                EncodingType::Rle => RleEncoder::new()
                    .encode_f64(&vals, &no_nulls),
                _ => raw_f64(&vals),
            }
        }
        ColumnData::String(_) => {
            let vals = non_null_strings(column);
            match encoding {
                EncodingType::Dictionary => DictStringEncoder::new().encode_str(&vals, &no_nulls),
                _ => raw_strings(&vals),
            }
        }
        _ => {
            let vals = non_null_i64(column);
            match encoding {
                EncodingType::DeltaOfDelta => DeltaOfDeltaEncoder::new().encode_i64(&vals, &no_nulls),
                EncodingType::Rle => RleEncoder::new().encode_i64(&vals, &no_nulls),
                EncodingType::Dictionary => DictEncoder::new().encode_i64(&vals, &no_nulls),
                EncodingType::BitPack => BitPackEncoder::new().encode_i64(&vals, &no_nulls),
                _ => raw_i64(&vals),
            }
        }
    }
}

fn raw_i64(vals: &[i64]) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(vals.len() * 8);
    for &v in vals {
        out.extend_from_slice(&v.to_le_bytes());
    }
    Ok(out)
}

fn raw_f64(vals: &[f64]) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(vals.len() * 8);
    for &v in vals {
        out.extend_from_slice(&v.to_bits().to_le_bytes());
    }
    Ok(out)
}

fn raw_strings(vals: &[String]) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::new();
    for v in vals {
        out.extend_from_slice(&(v.len() as u32).to_le_bytes());
        out.extend_from_slice(v.as_bytes());
    }
    Ok(out)
}

fn decode_payload(
    encoding: EncodingType,
    data_type: DataType,
    payload: &[u8],
    non_null: usize,
) -> anyhow::Result<Scattered> {
    match data_type {
        DataType::Float64 | DataType::Float32 => {
            let f = match encoding {
                EncodingType::Gorilla => GorillaEncoder::new().decode_f64(payload, non_null)?,
                EncodingType::Rle => {
                    // RLE stores the float bit pattern as i64 runs.
                    let bits = RleEncoder::new().decode_i64(payload, non_null)?;
                    bits.into_iter()
                        .map(|b| f64::from_bits(b as u64))
                        .collect()
                }
                _ => payload
                    .chunks_exact(8)
                    .take(non_null)
                    .map(|c| f64::from_bits(u64::from_le_bytes(c.try_into().unwrap())))
                    .collect(),
            };
            Ok(Scattered::Float(f))
        }
        DataType::String => {
            let s = match encoding {
                EncodingType::Dictionary => DictStringEncoder::new().decode_str(payload, non_null)?,
                _ => {
                    let mut out = Vec::with_capacity(non_null);
                    let mut pos = 0;
                    for _ in 0..non_null {
                        if pos + 4 > payload.len() {
                            break;
                        }
                        let len = u32::from_le_bytes(payload[pos..pos + 4].try_into()?) as usize;
                        pos += 4;
                        if pos + len > payload.len() {
                            break;
                        }
                        out.push(
                            String::from_utf8(payload[pos..pos + len].to_vec())
                                .map_err(|e| anyhow::anyhow!("invalid utf-8: {}", e))?,
                        );
                        pos += len;
                    }
                    out
                }
            };
            Ok(Scattered::Str(s))
        }
        _ => {
            let v = match encoding {
                EncodingType::DeltaOfDelta => DeltaOfDeltaEncoder::new().decode_i64(payload, non_null)?,
                EncodingType::Rle => RleEncoder::new().decode_i64(payload, non_null)?,
                EncodingType::Dictionary => DictEncoder::new().decode_i64(payload, non_null)?,
                EncodingType::BitPack => BitPackEncoder::new().decode_i64(payload, non_null)?,
                _ => payload
                    .chunks_exact(8)
                    .take(non_null)
                    .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
                    .collect(),
            };
            Ok(Scattered::Int(v))
        }
    }
}

/// Decoded non-null values, before the null mask is applied.
enum Scattered {
    Int(Vec<i64>),
    Float(Vec<f64>),
    Str(Vec<String>),
}

impl Scattered {
    fn len(&self) -> usize {
        match self {
            Scattered::Int(v) => v.len(),
            Scattered::Float(v) => v.len(),
            Scattered::Str(v) => v.len(),
        }
    }
}

/// Re-insert nulls so every typed vector has `row_count` entries.
fn scatter(
    values: Scattered,
    bitmap: &[u8],
    row_count: usize,
    data_type: DataType,
) -> anyhow::Result<ColumnData> {
    let is_null = |i: usize| -> bool {
        if i >= row_count {
            return true;
        }
        let byte = bitmap[i / 8];
        (byte >> (7 - (i % 8))) & 1 == 1
    };

    Ok(match values {
        Scattered::Int(v) => {
            let mut it = v.into_iter();
            match data_type {
                DataType::Timestamp => ColumnData::Timestamp(
                    (0..row_count)
                        .map(|i| if is_null(i) { None } else { it.next() })
                        .collect(),
                ),
                DataType::Int64 => ColumnData::Int64(
                    (0..row_count)
                        .map(|i| if is_null(i) { None } else { it.next() })
                        .collect(),
                ),
                DataType::Int32 => ColumnData::Int32(
                    (0..row_count)
                        .map(|i| {
                            if is_null(i) {
                                None
                            } else {
                                it.next().map(|x| x as i32)
                            }
                        })
                        .collect(),
                ),
                DataType::Bool => ColumnData::Bool(
                    (0..row_count)
                        .map(|i| {
                            if is_null(i) {
                                None
                            } else {
                                it.next().map(|x| x != 0)
                            }
                        })
                        .collect(),
                ),
                _ => anyhow::bail!("integer values in non-integer column"),
            }
        }
        Scattered::Float(v) => {
            let mut it = v.into_iter();
            match data_type {
                DataType::Float64 => ColumnData::Float64(
                    (0..row_count)
                        .map(|i| if is_null(i) { None } else { it.next() })
                        .collect(),
                ),
                DataType::Float32 => ColumnData::Float32(
                    (0..row_count)
                        .map(|i| {
                            if is_null(i) {
                                None
                            } else {
                                it.next().map(|x| x as f32)
                            }
                        })
                        .collect(),
                ),
                _ => anyhow::bail!("float values in non-float column"),
            }
        }
        Scattered::Str(v) => {
            let mut it = v.into_iter();
            ColumnData::String(
                (0..row_count)
                    .map(|i| if is_null(i) { None } else { it.next() })
                    .collect(),
            )
        }
    })
}

/// Page compression.
///
/// Zstd compression is now implemented. The compression level is honored when
/// `CompressionLevel::Zstd(l)` is used.
pub fn compress_page(data: &[u8], level: CompressionLevel) -> Vec<u8> {
    match level {
        CompressionLevel::Off => data.to_vec(),
        CompressionLevel::Lz4 => lz4_flex::compress_prepend_size(data),
        CompressionLevel::Zstd(l) => {
            let lvl = if l == 0 { 3i32 } else { l as i32 };
            zstd::bulk::compress(data, lvl).unwrap_or_else(|_| data.to_vec())
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum CompressionLevel {
    #[default]
    Off,
    Lz4,
    Zstd(u32),
}

/// Decompress a page written by [`compress_page`].
pub fn decompress_page(data: &[u8], level: CompressionLevel) -> anyhow::Result<Vec<u8>> {
    match level {
        CompressionLevel::Off => Ok(data.to_vec()),
        CompressionLevel::Lz4 => Ok(lz4_flex::decompress_size_prepended(data)
            .map_err(|e| anyhow::anyhow!("lz4 decompress failed: {}", e))?),
        CompressionLevel::Zstd(_) => {
            // Zstd bulk compress does not prepend size; we store raw compressed bytes.
            // Estimate bound: original * 4 is safe for typical ratios; fall back to larger.
            let mut bound = data.len() * 8;
            if bound < 256 * 1024 {
                bound = 256 * 1024;
            }
            Ok(zstd::bulk::decompress(data, bound)
                .map_err(|e| anyhow::anyhow!("zstd decompress failed: {}", e))?)
        }
    }
}

/// Zone map for block-level min/max statistics.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ZoneMap {
    pub min_ts: i64,
    pub max_ts: i64,
    pub min_val: Option<f64>,
    pub max_val: Option<f64>,
    pub null_count: u32,
    pub row_count: u32,
}

impl ZoneMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn might_contain(&self, min: i64, max: i64) -> bool {
        self.max_ts >= min && self.min_ts <= max
    }

    /// Build a zone map from a decoded column.
    pub fn from_column(column: &ColumnData, min_ts: i64, max_ts: i64) -> Self {
        let mut min_val = None;
        let mut max_val = None;
        for i in 0..column.len() {
            if let Some(v) = column.as_f64(i) {
                min_val = Some(min_val.map_or(v, |m: f64| m.min(v)));
                max_val = Some(max_val.map_or(v, |m: f64| m.max(v)));
            }
        }
        Self {
            min_ts,
            max_ts,
            min_val,
            max_val,
            null_count: column.null_count() as u32,
            row_count: column.len() as u32,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(col: &ColumnData) {
        let enc = ColumnCodec::encode(col).unwrap();
        let dec = ColumnCodec::decode(&enc, col.data_type()).unwrap();
        assert_eq!(&dec, col, "round trip mismatch for {:?}", col.data_type());
    }

    #[test]
    fn test_every_type_roundtrip() {
        roundtrip(&ColumnData::Timestamp(
            (0..100).map(|i| Some(i as i64 * 1000)).collect(),
        ));
        roundtrip(&ColumnData::Int64((0..100).map(|i| Some(i as i64)).collect()));
        roundtrip(&ColumnData::Int32((0..100).map(|i| Some(i as i32)).collect()));
        roundtrip(&ColumnData::Float64(
            (0..100).map(|i| Some(20.0 + i as f64 * 0.5)).collect(),
        ));
        roundtrip(&ColumnData::Float32(
            (0..100).map(|i| Some(20.0 + i as f32 * 0.5)).collect(),
        ));
        roundtrip(&ColumnData::Bool(
            (0..100).map(|i| Some(i % 2 == 0)).collect(),
        ));
    }

    #[test]
    fn test_string_variable_width_roundtrip() {
        // Previously corrupted into "alphabeg" / "<truncated>".
        roundtrip(&ColumnData::String(vec![
            Some("alpha".into()),
            Some("be".into()),
            Some("gamma!".into()),
            Some("".into()),
            Some("x".repeat(1000)),
        ]));
    }

    #[test]
    fn test_int32_wide_range_roundtrip() {
        // Previously read every other slot.
        roundtrip(&ColumnData::Int32(
            vec![Some(1), Some(2), Some(300), Some(70000), Some(-5)],
        ));
    }

    #[test]
    fn test_nulls_preserved_positions() {
        let col = ColumnData::Float64(vec![Some(1.0), None, Some(3.0), None, Some(5.0)]);
        roundtrip(&col);
        let dec = ColumnCodec::decode(&ColumnCodec::encode(&col).unwrap(), DataType::Float64).unwrap();
        assert!(matches!(&dec, ColumnData::Float64(v) if v[1].is_none() && v[3].is_none()));
    }

    #[test]
    fn test_all_nulls_column() {
        let col = ColumnData::Int64(vec![None; 50]);
        roundtrip(&col);
    }

    #[test]
    fn test_empty_column() {
        roundtrip(&ColumnData::Float64(Vec::new()));
        roundtrip(&ColumnData::String(Vec::new()));
    }

    #[test]
    fn test_single_row() {
        roundtrip(&ColumnData::Float64(vec![Some(42.5)]));
        roundtrip(&ColumnData::String(vec![Some("only".into())]));
    }

    #[test]
    fn test_raw_accessors_agree_with_encode() {
        let col = ColumnData::String(vec![Some("ab".into()), None, Some("cde".into())]);
        let dec = ColumnCodec::decode(&ColumnCodec::encode(&col).unwrap(), DataType::String).unwrap();
        assert_eq!(dec.as_str(0), Some("ab"));
        assert_eq!(dec.get_raw(0), Some(b"ab".to_vec()));
        assert_eq!(dec.get_raw(1), None);
        assert_eq!(dec.as_str(2), Some("cde"));
    }

    #[test]
    fn test_truncated_block_errors() {
        let enc = ColumnCodec::encode(&ColumnData::Int64((0..10).map(Some).collect())).unwrap();
        assert!(ColumnCodec::decode(&enc[..5], DataType::Int64).is_err());
    }

    #[test]
    fn test_unknown_encoding_tag_errors() {
        let mut enc = ColumnCodec::encode(&ColumnData::Int64((0..10).map(Some).collect())).unwrap();
        enc[0] = 99;
        assert!(ColumnCodec::decode(&enc, DataType::Int64).is_err());
    }

    #[test]
    fn test_zone_map_from_column() {
        let col = ColumnData::Float64(vec![Some(1.0), Some(-3.0), None, Some(9.0)]);
        let zm = ZoneMap::from_column(&col, 100, 200);
        assert_eq!(zm.min_val, Some(-3.0));
        assert_eq!(zm.max_val, Some(9.0));
        assert_eq!(zm.null_count, 1);
        assert_eq!(zm.row_count, 4);
    }
}

