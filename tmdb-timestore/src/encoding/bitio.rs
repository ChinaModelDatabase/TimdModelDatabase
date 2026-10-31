//! Bit-level readers and writers shared by the bit-oriented encoders.
//!
//! Both `DeltaOfDeltaEncoder` and `GorillaEncoder` emit a continuous bit
//! stream rather than byte-aligned records, so they share this MSB-first
//! bit packing implementation. Keeping it in one place guarantees that
//! `encode` and `decode` agree on bit order, which is where the previous
//! implementation diverged and silently corrupted data.

/// MSB-first bit writer.
pub struct BitWriter {
    buf: Vec<u8>,
    current: u8,
    pos: u8,
}

impl BitWriter {
    pub fn new() -> Self {
        Self {
            buf: Vec::new(),
            current: 0,
            pos: 0,
        }
    }

    /// Reserve `bits` extra bytes; used to guarantee a minimum output size.
    pub fn with_capacity(bytes: usize) -> Self {
        Self {
            buf: Vec::with_capacity(bytes),
            current: 0,
            pos: 0,
        }
    }

    #[inline]
    pub fn write_bit(&mut self, bit: bool) {
        if bit {
            self.current |= 1 << (7 - self.pos);
        }
        self.pos += 1;
        if self.pos == 8 {
            self.buf.push(self.current);
            self.current = 0;
            self.pos = 0;
        }
    }

    /// Write the low `count` bits of `value`, most significant first.
    ///
    /// Bits above `count` in `value` are ignored, so `count > 64` is a
    /// programming error rather than a silent truncation.
    #[inline]
    pub fn write_bits(&mut self, value: u64, count: usize) {
        assert!(count <= 64, "cannot write {} bits at once", count);
        for i in (0..count).rev() {
            self.write_bit((value >> i) & 1 == 1);
        }
    }

    #[inline]
    pub fn write_u64(&mut self, value: u64) {
        self.write_bits(value, 64);
    }

    /// Flush any partial byte.
    pub fn finish(mut self) -> Vec<u8> {
        if self.pos > 0 {
            self.buf.push(self.current);
        }
        self.buf
    }
}

impl Default for BitWriter {
    fn default() -> Self {
        Self::new()
    }
}

/// MSB-first bit reader. Mirrors [`BitWriter`] exactly.
pub struct BitReader<'a> {
    data: &'a [u8],
    byte_pos: usize,
    bit_pos: u8,
}

impl<'a> BitReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            byte_pos: 0,
            bit_pos: 0,
        }
    }

    #[inline]
    pub fn read_bit(&mut self) -> Result<bool, EncodingError> {
        if self.byte_pos >= self.data.len() {
            return Err(EncodingError::UnexpectedEof);
        }
        let bit = (self.data[self.byte_pos] >> (7 - self.bit_pos)) & 1 == 1;
        self.bit_pos += 1;
        if self.bit_pos == 8 {
            self.byte_pos += 1;
            self.bit_pos = 0;
        }
        Ok(bit)
    }

    /// Read `count` bits, most significant first.
    ///
    /// The final byte of a stream may hold padding bits. Those are readable —
    /// a decoder is told how many values to reconstruct, so it never has to
    /// guess where padding starts — but running past the buffer is an error.
    pub fn read_bits(&mut self, count: usize) -> Result<u64, EncodingError> {
        assert!(count <= 64, "cannot read {} bits at once", count);
        let mut result: u64 = 0;
        for _ in 0..count {
            result = (result << 1) | self.read_bit()? as u64;
        }
        Ok(result)
    }

    #[inline]
    pub fn read_u64(&mut self) -> Result<u64, EncodingError> {
        self.read_bits(64)
    }

    #[inline]
    pub fn bits_remaining(&self) -> usize {
        (self.data.len() - self.byte_pos) * 8 - self.bit_pos as usize
    }
}

/// Error type for bit-level decoding failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncodingError {
    UnexpectedEof,
    Corrupt(String),
}

impl std::fmt::Display for EncodingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EncodingError::UnexpectedEof => write!(f, "unexpected end of encoded stream"),
            EncodingError::Corrupt(msg) => write!(f, "corrupt encoded stream: {}", msg),
        }
    }
}

impl std::error::Error for EncodingError {}

// --- Zigzag helpers -------------------------------------------------------
//
// Zigzag maps signed integers onto unsigned ones while keeping small
// magnitudes small, which is essential for the varint and bit-packed paths.

#[inline]
pub fn zigzag_i64(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

#[inline]
pub fn unzigzag_i64(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

// --- LEB128 varint -------------------------------------------------------

#[inline]
pub fn write_varint(buf: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            buf.push(byte | 0x80);
        } else {
            buf.push(byte);
            return;
        }
    }
}

pub fn read_varint(data: &[u8], start: usize) -> Result<(u64, usize), EncodingError> {
    let mut result = 0u64;
    let mut shift = 0u32;
    let mut pos = start;
    loop {
        if pos >= data.len() {
            return Err(EncodingError::UnexpectedEof);
        }
        let byte = data[pos];
        pos += 1;
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok((result, pos - start));
        }
        shift += 7;
        if shift > 63 {
            return Err(EncodingError::Corrupt("varint overflow".into()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zigzag_roundtrip() {
        for v in [0i64, -1, 1, 63, -64, 64, -65, i64::MAX, i64::MIN, -8192, 8191] {
            assert_eq!(unzigzag_i64(zigzag_i64(v)), v, "zigzag failed for {}", v);
        }
    }

    #[test]
    fn zigzag_keeps_small_values_small() {
        assert!(zigzag_i64(0) == 0);
        assert!(zigzag_i64(-1) == 1);
        assert!(zigzag_i64(1) == 2);
        assert!(zigzag_i64(63) == 126);
        assert!(zigzag_i64(64) == 128);
    }

    #[test]
    fn varint_roundtrip() {
        for v in [0u64, 1, 127, 128, 300, u32::MAX as u64, u64::MAX] {
            let mut buf = Vec::new();
            write_varint(&mut buf, v);
            let (got, n) = read_varint(&buf, 0).unwrap();
            assert_eq!(got, v);
            assert_eq!(n, buf.len());
        }
    }

    #[test]
    fn bit_roundtrip_widths() {
        for count in [1usize, 3, 7, 8, 13, 32, 63, 64] {
            let value = if count == 64 {
                0xDEAD_BEEF_CAFE_BABE
            } else {
                0b1011_0110 & ((1u64 << count) - 1)
            };
            let mut w = BitWriter::new();
            w.write_bits(value, count);
            let buf = w.finish();
            let mut r = BitReader::new(&buf);
            assert_eq!(r.read_bits(count).unwrap(), value, "count {}", count);
        }
    }

    #[test]
    fn bit_writer_read_exhaustion() {
        // The last byte is padded, so 3 written bits still occupy a whole byte.
        // Reading past the *buffer* must fail; reading the padding is allowed.
        let mut w = BitWriter::new();
        w.write_bits(0b101, 3);
        let buf = w.finish();
        assert_eq!(buf.len(), 1);

        let mut r = BitReader::new(&buf);
        assert_eq!(r.read_bits(3).unwrap(), 0b101);

        let mut r = BitReader::new(&buf);
        assert!(r.read_bits(9).is_err(), "must not read past the buffer");
    }
}
