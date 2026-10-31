//! Decoding, comparison and encoding of stored column values.
//!
//! `Table::scan` hands back raw bytes whose meaning is fixed by the column's
//! `DataType`. Anything that wants to look at a value -- a filter, an aggregate,
//! an ORDER BY -- has to decode it first, and has to re-encode it in the same
//! type to hand a result back. Centralising that here keeps the byte widths in
//! one place; getting them subtly different between the filter and the
//! aggregate is how a query starts returning confidently wrong answers.

use std::cmp::Ordering;

use tmdb_timestore::DataType;

/// A decoded column value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Int(i64),
    Float(f64),
    Bool(bool),
    Str(String),
}

impl Value {
    /// Decode one stored value.
    ///
    /// Short input is zero-extended rather than rejected, matching how the
    /// segment reader already tolerates a truncated fixed-width column: a
    /// partially written value is still the number it was being written as.
    pub fn decode(ty: &DataType, raw: Option<&[u8]>) -> Value {
        let raw = match raw {
            None => return Value::Null,
            Some(r) => r,
        };
        match ty {
            DataType::Timestamp | DataType::Int64 => {
                Value::Int(read_i64(raw))
            }
            DataType::Int32 => Value::Int(read_i32(raw) as i64),
            DataType::Float64 => Value::Float(f64::from_bits(read_i64(raw) as u64)),
            DataType::Float32 => Value::Float(f32::from_bits(read_i32(raw) as u32) as f64),
            DataType::Bool => Value::Bool(raw.first().copied().unwrap_or(0) != 0),
            DataType::String => Value::Str(String::from_utf8_lossy(raw).into_owned()),
        }
    }

    /// Encode into the byte form the column type expects.
    pub fn encode(&self, ty: &DataType) -> anyhow::Result<Option<Vec<u8>>> {
        let null = || Ok(None);
        match (self, ty) {
            (Value::Null, _) => null(),
            (Value::Bool(b), DataType::Bool) => Ok(Some(vec![if *b { 1 } else { 0 }])),
            (Value::Str(s), DataType::String) => Ok(Some(s.as_bytes().to_vec())),
            (Value::Int(i), DataType::Int64) | (Value::Int(i), DataType::Timestamp) => {
                Ok(Some(i.to_le_bytes().to_vec()))
            }
            (Value::Int(i), DataType::Int32) => i32::try_from(*i)
                .map(|n| Some(n.to_le_bytes().to_vec()))
                .map_err(|_| anyhow::anyhow!("value {} overflows INTEGER", i)),
            (Value::Int(i), DataType::Float64) => Ok(Some((*i as f64).to_le_bytes().to_vec())),
            (Value::Int(i), DataType::Float32) => {
                Ok(Some((*i as f32).to_le_bytes().to_vec()))
            }
            (Value::Float(f), DataType::Float64) => Ok(Some(f.to_le_bytes().to_vec())),
            (Value::Float(f), DataType::Float32) => Ok(Some((*f as f32).to_le_bytes().to_vec())),
            // SQL widens an integer to a real on comparison; writing one back
            // into an integer column has to round the same way.
            (Value::Float(f), DataType::Int64) | (Value::Float(f), DataType::Timestamp) => {
                let r = f.trunc();
                if !r.is_finite() || r < i64::MIN as f64 || r > i64::MAX as f64 {
                    anyhow::bail!("value {} does not fit a BIGINT column", f);
                }
                Ok(Some((r as i64).to_le_bytes().to_vec()))
            }
            (Value::Float(f), DataType::Int32) => {
                let r = f.trunc();
                if !r.is_finite() {
                    anyhow::bail!("value {} does not fit an INTEGER column", f);
                }
                i32::try_from(r as i64)
                    .map(|n| Some(n.to_le_bytes().to_vec()))
                    .map_err(|_| anyhow::anyhow!("value {} overflows INTEGER", f))
            }
            (other, ty) => anyhow::bail!("cannot store {:?} in a {} column", other, ty),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Numeric view of the value, for aggregates.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Int(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
            _ => None,
        }
    }

    /// Three-valued comparison: `None` when either side is NULL, matching SQL's
    /// rule that a comparison against NULL is unknown rather than false.
    pub fn compare(&self, other: &Value) -> Option<Ordering> {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => None,
            (Value::Str(a), Value::Str(b)) => Some(a.cmp(b)),
            (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
            (Value::Int(a), Value::Int(b)) => Some(a.cmp(b)),
            _ => {
                // Mixed int/float: go through f64. This is where a large i64
                // loses precision against a float, which is the same loss the
                // database's own DOUBLE columns already carry.
                match (self.as_f64(), other.as_f64()) {
                    (Some(a), Some(b)) => a.partial_cmp(&b),
                    _ => None,
                }
            }
        }
    }

    /// Total order used for sorting, where NULLs need a defined position.
    pub fn sort_key(&self) -> (u8, f64, String) {
        match self {
            Value::Null => (0, 0.0, String::new()),
            Value::Int(i) => (1, *i as f64, String::new()),
            Value::Float(f) => (1, *f, String::new()),
            // Booleans sort false before true, which numeric keys already give.
            Value::Bool(b) => (1, if *b { 1.0 } else { 0.0 }, String::new()),
            Value::Str(s) => (2, 0.0, s.clone()),
        }
    }

    /// Text form, for a wire protocol that has no type tags.
    pub fn to_text(&self) -> String {
        match self {
            Value::Null => String::new(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Str(s) => s.clone(),
        }
    }
}

fn read_i64(raw: &[u8]) -> i64 {
    let mut buf = [0u8; 8];
    let n = raw.len().min(8);
    buf[..n].copy_from_slice(&raw[..n]);
    i64::from_le_bytes(buf)
}

fn read_i32(raw: &[u8]) -> i32 {
    let mut buf = [0u8; 4];
    let n = raw.len().min(4);
    buf[..n].copy_from_slice(&raw[..n]);
    i32::from_le_bytes(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_every_column_type() {
        let cases = vec![
            (DataType::Timestamp, Value::Int(-1_700_000_000_123)),
            (DataType::Int64, Value::Int(i64::MIN)),
            (DataType::Int32, Value::Int(-7)),
            (DataType::Float64, Value::Float(1.5)),
            (DataType::Float32, Value::Float(0.5)),
            (DataType::Bool, Value::Bool(true)),
            (DataType::String, Value::Str("hello".into())),
        ];
        for (ty, v) in cases {
            let bytes = v.encode(&ty).unwrap().expect("not null");
            assert_eq!(Value::decode(&ty, Some(&bytes)), v, "{:?}", ty);
        }
    }

    #[test]
    fn null_round_trips_as_null() {
        for ty in [
            DataType::Timestamp,
            DataType::Int64,
            DataType::Int32,
            DataType::Float64,
            DataType::Float32,
            DataType::Bool,
            DataType::String,
        ] {
            assert!(Value::Null.encode(&ty).unwrap().is_none());
            assert_eq!(Value::decode(&ty, None), Value::Null);
        }
    }

    #[test]
    fn comparison_with_null_is_unknown() {
        assert_eq!(Value::Null.compare(&Value::Int(1)), None);
        assert_eq!(Value::Int(1).compare(&Value::Null), None);
    }

    #[test]
    fn integers_and_floats_compare_numerically() {
        assert_eq!(
            Value::Int(1).compare(&Value::Float(1.5)),
            Some(Ordering::Less)
        );
        assert_eq!(Value::Int(2).compare(&Value::Float(2.0)), Some(Ordering::Equal));
    }

    #[test]
    fn an_oversized_integer_is_refused_rather_than_truncated() {
        let err = Value::Int(i64::MAX).encode(&DataType::Int32).unwrap_err();
        assert!(err.to_string().contains("overflows"), "{}", err);
    }

    #[test]
    fn short_input_reads_as_the_number_being_written() {
        assert_eq!(Value::decode(&DataType::Int64, Some(&[1, 0])), Value::Int(1));
        assert_eq!(Value::decode(&DataType::Bool, Some(&[])), Value::Bool(false));
    }
}
