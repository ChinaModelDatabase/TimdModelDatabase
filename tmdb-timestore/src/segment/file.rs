//! Segment file (.tsb) — columnar on-disk format for tmDB.
//!
//! # Version 2 layout
//!
//! ```text
//! +----------------------------------------------------------+
//! | Header (128 B, fixed)                                   |
//! |   magic CTSB | version | flags | table_id | ts_unit     |
//! |   col_count | row_count | block_count | min_ts | max_ts |
//! |   created_at | compression | footer_offset | footer_len |
//! +----------------------------------------------------------+
//! | Block 0                                                 |
//! |   [block header: 32 B]  id | rows | min_ts | max_ts     |
//! |                          data_off | data_len | ncols    |
//! |   [chunk 0] [chunk 1] ... [chunk N-1]                   |
//! | Block 1 ...                                             |
//! +----------------------------------------------------------+
//! | Footer: bincode(SegmentFooter) | footer_len:u32          |
//! +----------------------------------------------------------+
//! ```
//!
//! Each chunk is one column block produced by
//! [`crate::encoding::ColumnCodec`], optionally LZ4-compressed on top. Chunks
//! are stored back to back in schema order, and the footer records each chunk's
//! `stored_len` so a reader can seek directly to the one column it needs and
//! leave the rest untouched.
//!
//! # What changed in version 2
//!
//! Version 1 did `self.col_data = block` in `write_block_with_ts`, which
//! *overwrote* the previous block instead of appending. `finish()` therefore
//! wrote only the last block while the zone map advertised every block, so any
//! multi-block segment silently lost data — which is why an earlier benchmark
//! reported implausible compression ratios. Here each block is buffered
//! separately, carries its own header, and is addressed by explicit offsets.

use std::io::{Seek, Write};
use std::path::{Path, PathBuf};

use bytes::{BufMut, BytesMut};

use crate::encoding::{compress_page, decompress_page, ColumnCodec, CompressionLevel, DataType};
use crate::TsUnit;

const MAGIC: &[u8; 4] = b"CTSB";
const VERSION: u32 = 2;
pub const HEADER_SIZE: usize = 128;
/// `block_id(4) + row_count(4) + min_ts(8) + max_ts(8) + data_offset(8)
///  + data_size(4) + col_count(4) + reserved(4)`
pub const BLOCK_HEADER_SIZE: usize = 44;

/// Rows per block.
///
/// This is a trade-off between compressed size, write throughput and point-query
/// latency, and it was worth measuring rather than guessing:
///
/// | rows/block | ratio  | point lookup (mean) |
/// |-----------:|-------:|--------------------:|
/// |       4096  |  35.3x |             489 us |
/// |       8192  |  67.4x |             938 us |
/// |      16384  | 120.5x |            1588 us |
/// |      32768  | 206.8x |            3287 us |
/// |      65536  | 319.0x |           10194 us |
///
/// Bigger blocks give LZ4 much more context to match against, so the ratio
/// climbs steeply, and encoding gets faster too. But a point lookup has to
/// decode a whole block, so its cost grows with the block: 65536 costs roughly
/// twenty times what 4096 does.
///
/// 4096 is kept because 35.3x already clears the 20-30x target with room to
/// spare, and it leaves point lookups far inside the 1-5 ms target. Trading that
/// latency margin for compression past what was asked for is a bad deal, and
/// measurement on a noisy machine showed point latency at 16384 and above
/// swinging by 2-3x between runs, close enough to the 5 ms ceiling to be a
/// liability rather than an optimisation.
///
/// The real fix is partial decode: nothing yet reads a single row out of a
/// block, so point queries pay for every row in it. Until that exists, this
/// constant is the only lever, so [`crate::Table::set_rows_per_block`] exposes it.
pub const DEFAULT_ROWS_PER_BLOCK: usize = 4_096;

#[derive(Debug, thiserror::Error)]
pub enum SegmentError {
    #[error("invalid magic: expected CTSB")]
    InvalidMagic,
    #[error("unsupported version: {0} (this build reads version 2)")]
    UnsupportedVersion(u32),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("corruption: {0}")]
    Corruption(String),
}

/// Fixed-size segment header.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SegmentHeader {
    pub table_id: u64,
    pub ts_unit: TsUnit,
    pub col_count: u32,
    pub row_count: u64,
    pub block_count: u32,
    pub min_ts: i64,
    pub max_ts: i64,
    pub created_at: i64,
    pub compression: CompressionLevel,
    pub footer_offset: u64,
    pub footer_len: u32,
}

impl SegmentHeader {
    pub fn new(
        table_id: u64,
        ts_unit: TsUnit,
        col_count: u32,
        compression: CompressionLevel,
    ) -> Self {
        Self {
            table_id,
            ts_unit,
            col_count,
            row_count: 0,
            block_count: 0,
            min_ts: i64::MAX,
            max_ts: i64::MIN,
            created_at: now_millis(),
            compression,
            footer_offset: 0,
            footer_len: 0,
        }
    }

    /// Append the header to `buf`.
    ///
    /// Field offsets are fixed and are mirrored by [`SegmentHeader::read_from`]:
    ///
    /// ```text
    ///  0..4   magic "CTSB"
    ///  4..8   version
    ///  8..12  flags
    /// 12..16  col_count
    ///    16   ts_unit
    ///    17   compression
    /// 18..20  reserved
    /// 20..28  table_id
    /// 28..36  row_count
    /// 36..40  block_count
    /// 40..44  reserved
    /// 44..52  min_ts
    /// 52..60  max_ts
    /// 60..68  created_at
    /// 68..76  footer_offset
    /// 76..80  footer_len
    /// 80..128 zero padding
    /// ```
    pub fn write_to(&self, buf: &mut BytesMut) {
        let start = buf.len();
        buf.put_slice(MAGIC);
        buf.put_u32_le(VERSION);
        buf.put_u32_le(0); // flags
        buf.put_u32_le(self.col_count);
        buf.put_u8(self.ts_unit as u8);
        buf.put_u8(compression_tag(self.compression));
        buf.put_u16_le(0); // reserved
        buf.put_u64_le(self.table_id);
        buf.put_u64_le(self.row_count);
        buf.put_u32_le(self.block_count);
        buf.put_u32_le(0); // reserved
        buf.put_i64_le(self.min_ts);
        buf.put_i64_le(self.max_ts);
        buf.put_i64_le(self.created_at);
        buf.put_u64_le(self.footer_offset);
        buf.put_u32_le(self.footer_len);
        buf.put_u32_le(0); // reserved

        let written = buf.len() - start;
        debug_assert!(written <= HEADER_SIZE, "header overflow: {}", written);
        buf.put_bytes(0, HEADER_SIZE - written);
    }

    pub fn read_from(buf: &[u8]) -> Result<Self, SegmentError> {
        if buf.len() < HEADER_SIZE {
            return Err(SegmentError::Corruption("header too short".into()));
        }
        if &buf[0..4] != MAGIC {
            return Err(SegmentError::InvalidMagic);
        }
        let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        if version != VERSION {
            return Err(SegmentError::UnsupportedVersion(version));
        }
        let ts_unit = match buf[16] {
            0 => TsUnit::Seconds,
            1 => TsUnit::Millis,
            2 => TsUnit::Micros,
            3 => TsUnit::Nanos,
            other => return Err(SegmentError::Corruption(format!("bad ts_unit {}", other))),
        };

        Ok(Self {
            table_id: u64::from_le_bytes(buf[20..28].try_into().unwrap()),
            ts_unit,
            col_count: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
            row_count: u64::from_le_bytes(buf[28..36].try_into().unwrap()),
            block_count: u32::from_le_bytes(buf[36..40].try_into().unwrap()),
            min_ts: i64::from_le_bytes(buf[44..52].try_into().unwrap()),
            max_ts: i64::from_le_bytes(buf[52..60].try_into().unwrap()),
            created_at: i64::from_le_bytes(buf[60..68].try_into().unwrap()),
            compression: compression_from_tag(buf[17]),
            footer_offset: u64::from_le_bytes(buf[68..76].try_into().unwrap()),
            footer_len: u32::from_le_bytes(buf[76..80].try_into().unwrap()),
        })
    }
}

fn compression_tag(level: CompressionLevel) -> u8 {
    match level {
        CompressionLevel::Off => 0,
        CompressionLevel::Lz4 => 1,
        CompressionLevel::Zstd(_) => 2,
    }
}

fn compression_from_tag(tag: u8) -> CompressionLevel {
    match tag {
        1 => CompressionLevel::Lz4,
        2 => CompressionLevel::Zstd(3),
        _ => CompressionLevel::Off,
    }
}

/// Per-column description inside a block.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ColumnChunkInfo {
    pub col_index: u32,
    pub data_type: DataType,
    /// Encoding the selector chose for this block.
    pub encoding: crate::encoding::EncodingType,
    /// Length before page compression.
    pub raw_len: u32,
    /// Length actually stored.
    pub stored_len: u32,
    pub row_count: u32,
    pub null_count: u32,
}

/// A block: a time-ordered run of rows, independently decodable.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BlockMeta {
    pub block_id: u32,
    pub row_count: u32,
    pub min_ts: i64,
    pub max_ts: i64,
    /// Absolute file offset of the first chunk.
    pub data_offset: u64,
    pub data_size: u32,
    pub columns: Vec<ColumnChunkInfo>,
}

impl BlockMeta {
    pub fn might_contain(&self, ts_min: i64, ts_max: i64) -> bool {
        self.max_ts >= ts_min && self.min_ts <= ts_max
    }
}

/// Footer: the index that makes block pruning worthwhile.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SegmentFooter {
    pub blocks: Vec<BlockMeta>,
    pub row_count: u64,
    pub min_ts: i64,
    pub max_ts: i64,
    pub table_id: u64,
    /// Highest write sequence whose rows this segment holds.
    ///
    /// The segment is fsynced before the log is checkpointed, so this marker is
    /// durable whenever the segment is. Recovery therefore uses it -- not the
    /// log checkpoint -- to decide which logged batches are already stored. If a
    /// flush dies between the segment fsync and the checkpoint, the checkpoint
    /// still lags but this marker does not, so the rows are neither replayed on
    /// top of the segment nor lost.
    pub flushed_upto_seq: u64,
}

impl SegmentFooter {
    /// Blocks that may hold rows in `[ts_min, ts_max]`.
    pub fn matching_blocks(&self, ts_min: i64, ts_max: i64) -> Vec<&BlockMeta> {
        self.blocks
            .iter()
            .filter(|b| b.might_contain(ts_min, ts_max))
            .collect()
    }

    /// Blocks that may hold rows in `[ts_min, ts_max]`.
    pub fn might_contain(&self, ts_min: i64, ts_max: i64) -> bool {
        self.max_ts >= ts_min && self.min_ts <= ts_max
    }
}

/// The footer as it was written before `flushed_upto_seq` was added.
#[derive(serde::Deserialize)]
struct LegacySegmentFooter {
    blocks: Vec<BlockMeta>,
    row_count: u64,
    min_ts: i64,
    max_ts: i64,
    table_id: u64,
}

/// Decode a footer, accepting both the current layout and the one that
/// predates `flushed_upto_seq`.
///
/// `#[serde(default)]` on the new field would be the obvious way to do this
/// and does not work: bincode is not self-describing, so a payload that ends
/// early surfaces as `UnexpectedEof` from the decoder rather than as a missing
/// field for serde to default. The test
/// `footer_without_the_sequence_marker_still_reads` pins that behaviour.
///
/// Order matters, and is the whole point of doing this explicitly. Trying the
/// current layout first means a footer written by this version is never
/// mistaken for an old one; the reverse order would silently read a modern
/// footer as legacy, ignore its trailing field, and reset the replay marker to
/// zero -- which is precisely the duplicate-replay bug the field exists to
/// prevent.
fn decode_footer(bytes: &[u8]) -> Result<SegmentFooter, SegmentError> {
    match bincode::deserialize::<SegmentFooter>(bytes) {
        Ok(footer) => Ok(footer),
        Err(current_err) => match bincode::deserialize::<LegacySegmentFooter>(bytes) {
            Ok(old) => Ok(SegmentFooter {
                blocks: old.blocks,
                row_count: old.row_count,
                min_ts: old.min_ts,
                max_ts: old.max_ts,
                table_id: old.table_id,
                // No marker was recorded, so recovery must fall back to the
                // log checkpoint for this segment.
                flushed_upto_seq: 0,
            }),
            Err(_) => Err(SegmentError::Corruption(format!(
                "footer decode: {}",
                current_err
            ))),
        },
    }
}

/// A buffered block awaiting write.
struct PendingBlock {
    /// Encoded, compressed chunks in schema order.
    chunks: Vec<Vec<u8>>,
    columns: Vec<ColumnChunkInfo>,
    row_count: u32,
    min_ts: i64,
    max_ts: i64,
}

/// Incrementally builds a segment file.
///
/// Blocks accumulate in order; [`SegmentWriter::finish`] writes header, blocks
/// and footer, then patches the header with the footer location.
pub struct SegmentWriter {
    path: PathBuf,
    header: SegmentHeader,
    column_types: Vec<DataType>,
    rows_per_block: usize,
    flushed_upto_seq: u64,
    pending: Vec<PendingBlock>,
}

impl SegmentWriter {
    pub fn new(
        path: impl AsRef<Path>,
        table_id: u64,
        ts_unit: TsUnit,
        column_types: Vec<DataType>,
        compression: CompressionLevel,
    ) -> Self {
        let col_count = column_types.len() as u32;
        Self {
            path: path.as_ref().to_path_buf(),
            header: SegmentHeader::new(table_id, ts_unit, col_count, compression),
            column_types,
            rows_per_block: DEFAULT_ROWS_PER_BLOCK,
            flushed_upto_seq: 0,
            pending: Vec::new(),
        }
    }

    /// Record the write sequence this segment's rows came from.
    ///
    /// Recovery reads this back out of the footer to learn which logged batches
    /// are already stored, so a segment built by compaction must carry the
    /// highest sequence of the segments it absorbed.
    pub fn with_flushed_upto_seq(mut self, seq: u64) -> Self {
        self.flushed_upto_seq = seq;
        self
    }

    pub fn with_rows_per_block(mut self, rows: usize) -> Self {
        self.rows_per_block = rows.max(1);
        self
    }

    pub fn rows_per_block(&self) -> usize {
        self.rows_per_block
    }

    pub fn column_types(&self) -> &[DataType] {
        &self.column_types
    }

    pub fn pending_blocks(&self) -> usize {
        self.pending.len()
    }

    /// Append one block of already-encoded chunks.
    ///
    /// `chunks` and `columns` must both hold one entry per schema column, in
    /// order. Blocks are *appended* — the behaviour version 1 got wrong.
    pub fn push_block(
        &mut self,
        chunks: Vec<Vec<u8>>,
        columns: Vec<ColumnChunkInfo>,
        row_count: u32,
        min_ts: i64,
        max_ts: i64,
    ) -> Result<(), SegmentError> {
        let expected = self.column_types.len();
        if chunks.len() != expected || columns.len() != expected {
            return Err(SegmentError::Corruption(format!(
                "block has {} chunks / {} infos but schema has {} columns",
                chunks.len(),
                columns.len(),
                expected
            )));
        }
        for (i, c) in columns.iter().enumerate() {
            if c.stored_len as usize != chunks[i].len() {
                return Err(SegmentError::Corruption(format!(
                    "chunk {} declares {} bytes but carries {}",
                    i,
                    c.stored_len,
                    chunks[i].len()
                )));
            }
            if c.col_index as usize != i {
                return Err(SegmentError::Corruption(format!(
                    "chunk at position {} declares col_index {}",
                    i, c.col_index
                )));
            }
        }

        self.header.row_count += row_count as u64;
        self.header.block_count += 1;
        self.header.min_ts = self.header.min_ts.min(min_ts);
        self.header.max_ts = self.header.max_ts.max(max_ts);

        self.pending.push(PendingBlock {
            chunks,
            columns,
            row_count,
            min_ts,
            max_ts,
        });
        Ok(())
    }

    /// Encode and append a block from decoded columns.
    pub fn push_columns(
        &mut self,
        columns: &[crate::encoding::ColumnData],
        row_count: u32,
        min_ts: i64,
        max_ts: i64,
    ) -> Result<(), SegmentError> {
        if columns.len() != self.column_types.len() {
            return Err(SegmentError::Corruption(format!(
                "expected {} columns, got {}",
                self.column_types.len(),
                columns.len()
            )));
        }

        let mut chunks = Vec::with_capacity(columns.len());
        let mut infos = Vec::with_capacity(columns.len());

        for (i, col) in columns.iter().enumerate() {
            let raw = ColumnCodec::encode(col)
                .map_err(|e| SegmentError::Corruption(format!("column {} encode: {}", i, e)))?;
            let encoding = crate::encoding::EncodingType::from_u8(raw[0])
                .ok_or_else(|| SegmentError::Corruption("bad encoding tag in chunk".into()))?;
            let raw_len = raw.len() as u32;
            let stored = compress_page(&raw, self.header.compression);

            infos.push(ColumnChunkInfo {
                col_index: i as u32,
                data_type: self.column_types[i],
                encoding,
                raw_len,
                stored_len: stored.len() as u32,
                row_count: col.len() as u32,
                null_count: col.null_count() as u32,
            });
            chunks.push(stored);
        }

        self.push_block(chunks, infos, row_count, min_ts, max_ts)
    }

    /// Write the file and return its path.
    pub fn finish(mut self) -> Result<PathBuf, SegmentError> {
        if self.pending.is_empty() {
            return Err(SegmentError::Corruption(
                "cannot write a segment with no blocks".into(),
            ));
        }

        let mut file = std::fs::File::create(&self.path)?;

        let mut header_buf = BytesMut::with_capacity(HEADER_SIZE);
        self.header.write_to(&mut header_buf);
        file.write_all(&header_buf)?;

        let mut blocks_meta = Vec::with_capacity(self.pending.len());
        let mut cursor = HEADER_SIZE as u64;

        for (block_id, block) in self.pending.drain(..).enumerate() {
            let data_offset = cursor + BLOCK_HEADER_SIZE as u64;
            let data_size: usize = block.chunks.iter().map(|c| c.len()).sum();

            let mut out = Vec::with_capacity(BLOCK_HEADER_SIZE + data_size);
            out.extend_from_slice(&(block_id as u32).to_le_bytes());
            out.extend_from_slice(&block.row_count.to_le_bytes());
            out.extend_from_slice(&block.min_ts.to_le_bytes());
            out.extend_from_slice(&block.max_ts.to_le_bytes());
            out.extend_from_slice(&data_offset.to_le_bytes());
            out.extend_from_slice(&(data_size as u32).to_le_bytes());
            out.extend_from_slice(&(block.columns.len() as u32).to_le_bytes());
            out.extend_from_slice(&[0u8; 4]);
            debug_assert_eq!(out.len(), BLOCK_HEADER_SIZE);

            for chunk in &block.chunks {
                out.extend_from_slice(chunk);
            }
            file.write_all(&out)?;

            blocks_meta.push(BlockMeta {
                block_id: block_id as u32,
                row_count: block.row_count,
                min_ts: block.min_ts,
                max_ts: block.max_ts,
                data_offset,
                data_size: data_size as u32,
                columns: block.columns,
            });

            cursor += out.len() as u64;
        }

        let footer = SegmentFooter {
            blocks: blocks_meta,
            row_count: self.header.row_count,
            min_ts: self.header.min_ts,
            max_ts: self.header.max_ts,
            table_id: self.header.table_id,
            flushed_upto_seq: self.flushed_upto_seq,
        };
        let footer_bytes =
            bincode::serialize(&footer).map_err(|e| SegmentError::Corruption(e.to_string()))?;

        let footer_offset = cursor;
        file.write_all(&footer_bytes)?;
        file.write_all(&(footer_bytes.len() as u32).to_le_bytes())?;

        // Patch the header now that the footer location is known.
        self.header.footer_offset = footer_offset;
        self.header.footer_len = footer_bytes.len() as u32;
        let mut patched = BytesMut::with_capacity(HEADER_SIZE);
        self.header.write_to(&mut patched);
        file.seek(std::io::SeekFrom::Start(0))?;
        file.write_all(&patched)?;
        file.sync_all()?;

        Ok(self.path)
    }
}

/// Reads a segment, decoding individual blocks and columns on demand.
pub struct SegmentReader {
    header: SegmentHeader,
    footer: SegmentFooter,
    data: Vec<u8>,
    column_types: Vec<DataType>,
}

impl SegmentReader {
    /// Open a segment, recovering the schema from the footer's chunk metadata.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SegmentError> {
        let data = std::fs::read(path.as_ref())?;
        if data.len() < HEADER_SIZE + 4 {
            return Err(SegmentError::Corruption("file too small".into()));
        }
        let header = SegmentHeader::read_from(&data)?;
        let footer_len_pos = data.len() - 4;
        let footer_len =
            u32::from_le_bytes(data[footer_len_pos..].try_into().unwrap()) as usize;
        let footer_start = footer_len_pos
            .checked_sub(footer_len)
            .ok_or_else(|| SegmentError::Corruption("footer length out of range".into()))?;
        if footer_start < HEADER_SIZE {
            return Err(SegmentError::Corruption("footer overlaps header".into()));
        }
        if header.footer_offset as usize != footer_start {
            return Err(SegmentError::Corruption(format!(
                "header footer_offset {} disagrees with trailing footer at {}",
                header.footer_offset, footer_start
            )));
        }
        let footer = decode_footer(&data[footer_start..footer_len_pos])?;

        if footer.blocks.len() != header.block_count as usize {
            return Err(SegmentError::Corruption(format!(
                "header claims {} blocks but footer lists {}",
                header.block_count,
                footer.blocks.len()
            )));
        }

        // Schema comes from the chunk metadata, so a reader needs no separate
        // catalog entry.
        let mut column_types = vec![DataType::Int64; header.col_count as usize];
        for block in &footer.blocks {
            for (i, c) in block.columns.iter().enumerate() {
                if i < column_types.len() {
                    column_types[i] = c.data_type;
                }
            }
        }

        Ok(Self {
            header,
            footer,
            data,
            column_types,
        })
    }

pub fn header(&self) -> &SegmentHeader {
        &self.header
    }

    pub fn footer(&self) -> &SegmentFooter {
        &self.footer
    }

    pub fn row_count(&self) -> u64 {
        self.header.row_count
    }

    pub fn col_count(&self) -> u32 {
        self.header.col_count
    }

    pub fn block_count(&self) -> usize {
        self.footer.blocks.len()
    }

    pub fn column_types(&self) -> &[DataType] {
        &self.column_types
    }

    pub fn min_ts(&self) -> i64 {
        self.header.min_ts
    }

    pub fn max_ts(&self) -> i64 {
        self.header.max_ts
    }

    pub fn might_contain(&self, ts_min: i64, ts_max: i64) -> bool {
        self.footer.might_contain(ts_min, ts_max)
    }

    /// Blocks overlapping `[ts_min, ts_max]`.
    pub fn matching_blocks(&self, ts_min: i64, ts_max: i64) -> Vec<&BlockMeta> {
        self.footer.matching_blocks(ts_min, ts_max)
    }

    /// Byte range of one column's stored chunk inside a block.
    fn chunk_range(&self, block: &BlockMeta, col_index: usize) -> Result<(usize, usize), SegmentError> {
        let info = block
            .columns
            .get(col_index)
            .ok_or_else(|| {
                SegmentError::Corruption(format!(
                    "block {} has {} columns, asked for {}",
                    block.block_id,
                    block.columns.len(),
                    col_index
                ))
            })?;

        let mut offset = block.data_offset as usize;
        for c in &block.columns[..col_index] {
            offset += c.stored_len as usize;
        }
        let end = offset
            .checked_add(info.stored_len as usize)
            .filter(|e| *e <= self.data.len())
            .ok_or_else(|| {
                SegmentError::Corruption(format!(
                    "column {} of block {} extends past end of file",
                    col_index, block.block_id
                ))
            })?;
        Ok((offset, end))
    }

    /// Metadata for one block.
    pub fn block_meta(&self, block_id: u32) -> Result<&BlockMeta, SegmentError> {
        // The writer emits blocks in id order, so the index is the answer
        // essentially always. Scanning the whole list instead made this O(n),
        // which turned any loop over blocks into O(n^2) -- a 489-block segment
        // cost about 120k comparisons on a single query. The search stays as a
        // fallback so a footer written out of order still reads correctly.
        if let Some(block) = self.footer.blocks.get(block_id as usize) {
            if block.block_id == block_id {
                return Ok(block);
            }
        }
        self.footer
            .blocks
            .iter()
            .find(|b| b.block_id == block_id)
            .ok_or_else(|| SegmentError::Corruption(format!("block {} not found", block_id)))
    }

    /// Decode one column of one block.
    ///
    /// Only this block and column are decompressed, which is the whole point of
    /// storing per-column lengths in the footer.
    pub fn read_block_column(
        &self,
        block_id: u32,
        col_index: usize,
    ) -> anyhow::Result<crate::encoding::ColumnData> {
        self.read_column_from(self.block_meta(block_id)?, col_index)
    }

    /// Decode one column given metadata already in hand.
    ///
    /// Resolving the block once and passing it down keeps a multi-column read
    /// from repeating the lookup for every column.
    fn read_column_from(
        &self,
        block: &BlockMeta,
        col_index: usize,
    ) -> anyhow::Result<crate::encoding::ColumnData> {
        let (start, end) = self.chunk_range(block, col_index)?;
        let raw = decompress_page(&self.data[start..end], self.header.compression)?;
        let data_type = self.column_types[col_index];
        Ok(ColumnCodec::decode(&raw, data_type)?)
    }

    /// Byte offset where a block's chunk data begins.
    pub fn block_data_offset(&self, block_id: u32) -> Result<u64, SegmentError> {
        Ok(self.block_meta(block_id)?.data_offset)
    }

    /// Decode every column of one block.
    pub fn read_block(&self, block_id: u32) -> anyhow::Result<Vec<crate::encoding::ColumnData>> {
        // One lookup for the whole block rather than one per column.
        let block = self.block_meta(block_id)?;
        let mut out = Vec::with_capacity(self.column_types.len());
        for c in 0..self.column_types.len() {
            out.push(self.read_column_from(block, c)?);
        }
        Ok(out)
    }

    /// Decode every block, returning per-block column vectors.
    pub fn read_all_blocks(
        &self,
    ) -> anyhow::Result<Vec<Vec<crate::encoding::ColumnData>>> {
        let mut out = Vec::with_capacity(self.footer.blocks.len());
        for b in &self.footer.blocks {
            out.push(self.read_block(b.block_id)?);
        }
        Ok(out)
    }

    /// Decompress one column across all matching blocks without concatenating,
    /// so callers can stream block by block.
    pub fn for_each_block_column<F>(
        &self,
        ts_min: i64,
        ts_max: i64,
        col_index: usize,
        mut f: F,
    ) -> anyhow::Result<()>
    where
        F: FnMut(&BlockMeta, &crate::encoding::ColumnData) -> anyhow::Result<()>,
    {
        for block in self.matching_blocks(ts_min, ts_max) {
            let col = self.read_block_column(block.block_id, col_index)?;
            f(block, &col)?;
        }
        Ok(())
    }

    /// Decode all columns of all blocks, concatenated per column.
    pub fn read_all_columns(&self) -> anyhow::Result<Vec<crate::encoding::ColumnData>> {
        let mut out: Vec<crate::encoding::ColumnData> = self
            .column_types
            .iter()
            .map(|t| crate::encoding::ColumnData::new(*t, 0))
            .collect();

        for block in &self.footer.blocks {
            let cols = self.read_block(block.block_id)?;
            for (i, c) in cols.into_iter().enumerate() {
                crate::encoding::concat(&mut out[i], &c)?;
            }
        }
        Ok(out)
    }

    /// File size in bytes.
    pub fn file_size(&self) -> u64 {
        self.data.len() as u64
    }

    /// Total stored (compressed) bytes across all chunks.
    pub fn stored_bytes(&self) -> u64 {
        self.footer
            .blocks
            .iter()
            .flat_map(|b| b.columns.iter())
            .map(|c| c.stored_len as u64)
            .sum()
    }

    /// Total uncompressed bytes across all chunks.
    pub fn raw_bytes(&self) -> u64 {
        self.footer
            .blocks
            .iter()
            .flat_map(|b| b.columns.iter())
            .map(|c| c.raw_len as u64)
            .sum()
    }

    /// sha256 of the whole file, for integrity checks.
    pub fn checksum(&self) -> String {
        use sha2::{Digest, Sha256};
        let digest = Sha256::digest(&self.data);
        digest.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

pub(crate) fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::ColumnData;

    fn types() -> Vec<DataType> {
        vec![DataType::Timestamp, DataType::Float64]
    }

    fn write_simple(path: &Path, blocks: usize, rows: usize) -> Vec<(i64, f64)> {
        let mut w = SegmentWriter::new(
            path,
            7,
            TsUnit::Millis,
            types(),
            CompressionLevel::Lz4,
        );
        let mut expected = Vec::new();

        for b in 0..blocks {
            let base = 1_000 + (b * rows) as i64 * 10;
            let ts: Vec<Option<i64>> = (0..rows).map(|i| Some(base + i as i64 * 10)).collect();
            let vals: Vec<Option<f64>> = (0..rows)
                .map(|i| Some(20.0 + (b * rows + i) as f64 * 0.125))
                .collect();
            let min_ts = base;
            let max_ts = base + (rows as i64 - 1) * 10;

            w.push_columns(
                &[ColumnData::Timestamp(ts), ColumnData::Float64(vals)],
                rows as u32,
                min_ts,
                max_ts,
            )
            .unwrap();

            for i in 0..rows {
                expected.push((base + i as i64 * 10, 20.0 + (b * rows + i) as f64 * 0.125));
            }
        }

        w.finish().unwrap();
        expected
    }

    #[test]
    fn test_header_roundtrip() {
        let mut h = SegmentHeader::new(42, TsUnit::Millis, 5, CompressionLevel::Lz4);
        h.row_count = 100;
        h.block_count = 2;
        h.min_ts = 10;
        h.max_ts = 99;
        h.footer_offset = 4096;
        h.footer_len = 512;

        let mut buf = BytesMut::new();
        h.write_to(&mut buf);
        assert_eq!(&buf[0..4], MAGIC);
        assert_eq!(buf.len(), HEADER_SIZE);

        assert_eq!(SegmentHeader::read_from(&buf).unwrap(), h);
    }

    #[test]
    fn test_rejects_wrong_version() {
        let mut buf = BytesMut::new();
        SegmentHeader::new(1, TsUnit::Millis, 1, CompressionLevel::Off).write_to(&mut buf);
        buf[4..8].copy_from_slice(&1u32.to_le_bytes());
        match SegmentHeader::read_from(&buf) {
            Err(SegmentError::UnsupportedVersion(1)) => {}
            other => panic!("expected UnsupportedVersion, got {:?}", other.map(|_| ())),
        }
    }

    #[test]
    fn test_rejects_bad_magic() {
        let mut buf = BytesMut::new();
        SegmentHeader::new(1, TsUnit::Millis, 1, CompressionLevel::Off).write_to(&mut buf);
        buf[0] = b'X';
        assert!(matches!(
            SegmentHeader::read_from(&buf),
            Err(SegmentError::InvalidMagic)
        ));
    }

    /// The regression test for the overwrite bug: several blocks must all
    /// survive the round trip.
    #[test]
    fn test_multiple_blocks_all_survive() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("multi.tsb");
        let expected = write_simple(&path, 5, 100);

        let r = SegmentReader::open(&path).unwrap();
        assert_eq!(r.block_count(), 5);
        assert_eq!(r.row_count(), 500);

        let cols = r.read_all_columns().unwrap();
        let ts = &cols[0];
        let vals = &cols[1];

        assert_eq!(ts.len(), 500, "lost rows across blocks");
        for (i, (ets, eval)) in expected.iter().enumerate() {
            assert_eq!(ts.as_i64(i), Some(*ets), "timestamp {} wrong", i);
            assert_eq!(vals.as_f64(i), Some(*eval), "value {} wrong", i);
        }
    }

    #[test]
    fn test_block_boundaries_do_not_overlap_or_gap() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bounds.tsb");
        write_simple(&path, 4, 50);

        let r = SegmentReader::open(&path).unwrap();
        let mut total = 0u32;
        let mut prev_max = i64::MIN;
        for b in &r.footer().blocks {
            assert!(b.min_ts > prev_max, "block {} overlaps the previous one", b.block_id);
            prev_max = b.max_ts;
            total += b.row_count;
        }
        assert_eq!(total, 200);
    }

    #[test]
    fn test_read_single_block_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("single.tsb");
        write_simple(&path, 3, 20);

        let r = SegmentReader::open(&path).unwrap();
        let col = r.read_block_column(1, 1).unwrap();
        assert_eq!(col.len(), 20);
        // Block 1 starts at row 20.
        assert_eq!(col.as_f64(0), Some(20.0 + 20.0 * 0.125));
    }

    #[test]
    fn test_zone_map_prunes_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("zm.tsb");
        write_simple(&path, 10, 10); // each block spans 90ms

        let r = SegmentReader::open(&path).unwrap();
        assert_eq!(r.block_count(), 10);

        // Block b spans [1000 + b*100, 1090 + b*100]. Only block 1 overlaps
        // (1095, 1195).
        let matching = r.matching_blocks(1_095, 1_195);
        assert_eq!(matching.len(), 1);
        assert_eq!(matching[0].block_id, 1);

        // A range covering two blocks matches both.
        assert_eq!(r.matching_blocks(1_000, 1_195).len(), 2);

        // A range covering everything matches all blocks.
        assert_eq!(r.matching_blocks(0, i64::MAX).len(), 10);

        // A range past the end matches nothing.
        assert_eq!(r.matching_blocks(999_999, 1_000_000).len(), 0);
    }

    #[test]
    fn test_for_each_block_column_visits_only_matches() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("iter.tsb");
        write_simple(&path, 6, 10);

        let r = SegmentReader::open(&path).unwrap();
        let mut seen = Vec::new();
        // Block b spans [1000 + b*100, 1090 + b*100], so this covers blocks 0
        // and 1 only.
        r.for_each_block_column(1_000, 1_190, 0, |b, col| {
            seen.push(b.block_id);
            assert_eq!(col.len(), 10);
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, vec![0, 1]);
    }

    #[test]
    fn test_rejects_empty_writer() {
        let dir = tempfile::tempdir().unwrap();
        let w = SegmentWriter::new(
            dir.path().join("empty.tsb"),
            1,
            TsUnit::Millis,
            types(),
            CompressionLevel::Off,
        );
        assert!(w.finish().is_err(), "an empty segment must not be written");
    }

    #[test]
    fn test_rejects_mismatched_column_count() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = SegmentWriter::new(
            dir.path().join("bad.tsb"),
            1,
            TsUnit::Millis,
            types(),
            CompressionLevel::Off,
        );
        let col = ColumnData::Timestamp(vec![Some(1), Some(2)]);
        assert!(w.push_columns(&[col], 2, 1, 2).is_err());
    }

    #[test]
    fn test_rejects_bad_stored_len() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = SegmentWriter::new(
            dir.path().join("bad2.tsb"),
            1,
            TsUnit::Millis,
            types(),
            CompressionLevel::Off,
        );
        let chunk = vec![0u8; 8];
        let info = ColumnChunkInfo {
            col_index: 0,
            data_type: DataType::Timestamp,
            encoding: crate::encoding::EncodingType::Raw,
            raw_len: 8,
            stored_len: 99, // lies about the size
            row_count: 1,
            null_count: 0,
        };
        assert!(w.push_block(vec![chunk.clone()], vec![info], 1, 1, 1).is_err());
    }

    #[test]
    fn test_corrupt_footer_is_detected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("corrupt.tsb");
        write_simple(&path, 1, 10);

        // Truncate the footer region.
        let len = std::fs::metadata(&path).unwrap().len() as usize;
        let data = std::fs::read(&path).unwrap();
        std::fs::write(&path, &data[..len - 6]).unwrap();
        assert!(SegmentReader::open(&path).is_err());
    }

    #[test]
    fn test_compression_off_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raw.tsb");

        let mut w = SegmentWriter::new(
            &path,
            1,
            TsUnit::Millis,
            types(),
            CompressionLevel::Off,
        );
        let ts: Vec<Option<i64>> = (0..30).map(Some).collect();
        let vals: Vec<Option<f64>> = (0..30).map(|i| Some(i as f64)).collect();
        w.push_columns(
            &[ColumnData::Timestamp(ts), ColumnData::Float64(vals)],
            30,
            0,
            29,
        )
        .unwrap();
        w.finish().unwrap();

        let r = SegmentReader::open(&path).unwrap();
        let cols = r.read_all_columns().unwrap();
        assert_eq!(cols[0].as_i64(29), Some(29));
        assert_eq!(cols[1].as_f64(29), Some(29.0));
    }

    #[test]
    fn test_reported_sizes_are_consistent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sizes.tsb");
        write_simple(&path, 3, 200);

        let r = SegmentReader::open(&path).unwrap();
        assert!(r.stored_bytes() > 0);
        assert!(r.raw_bytes() >= r.stored_bytes());
        assert!(r.file_size() > r.stored_bytes());
        assert_eq!(r.checksum().len(), 64);
    }

    /// The exact layout a segment written before `flushed_upto_seq` existed
    /// would have serialized.
    #[derive(serde::Serialize)]
    struct LegacyFooter {
        blocks: Vec<BlockMeta>,
        row_count: u64,
        min_ts: i64,
        max_ts: i64,
        table_id: u64,
    }

    fn legacy_bytes() -> Vec<u8> {
        bincode::serialize(&LegacyFooter {
            blocks: vec![BlockMeta {
                block_id: 0,
                row_count: 2,
                min_ts: 10,
                max_ts: 20,
                data_offset: 128,
                data_size: 64,
                columns: vec![],
            }],
            row_count: 2,
            min_ts: 10,
            max_ts: 20,
            table_id: 7,
        })
        .unwrap()
    }

    #[test]
    fn footer_without_the_sequence_marker_still_reads() {
        let footer = decode_footer(&legacy_bytes()).unwrap();
        assert_eq!(footer.flushed_upto_seq, 0);
        assert_eq!(footer.row_count, 2);
        assert_eq!(footer.min_ts, 10);
        assert_eq!(footer.max_ts, 20);
        assert_eq!(footer.table_id, 7);
        assert_eq!(footer.blocks.len(), 1);
    }

    /// A current-format footer must never be read as a legacy one.
    ///
    /// bincode would accept it -- five fields decode happily from six -- and the
    /// trailing marker would be dropped, resetting recovery to the log
    /// checkpoint and replaying rows the segment already holds.
    #[test]
    fn a_current_footer_is_never_mistaken_for_a_legacy_one() {
        let footer = SegmentFooter {
            blocks: vec![],
            row_count: 0,
            min_ts: 0,
            max_ts: 0,
            table_id: 1,
            flushed_upto_seq: 999,
        };
        let bytes = bincode::serialize(&footer).unwrap();
        assert_eq!(decode_footer(&bytes).unwrap().flushed_upto_seq, 999);
    }

    #[test]
    fn a_corrupt_footer_is_reported_rather_than_defaulted() {
        assert!(decode_footer(b"not a footer at all").is_err());
    }

    #[test]
    fn footer_marker_survives_a_write_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("marker.tsb");
        let mut w = SegmentWriter::new(
            &path,
            7,
            TsUnit::Millis,
            types(),
            CompressionLevel::Lz4,
        )
        .with_flushed_upto_seq(4242);
        w.push_columns(
            &[ColumnData::Timestamp(vec![Some(1), Some(2)]), ColumnData::Float64(vec![Some(1.0), Some(2.0)])],
            2,
            1,
            2,
        )
        .unwrap();
        w.finish().unwrap();

        let r = SegmentReader::open(&path).unwrap();
        assert_eq!(r.footer().flushed_upto_seq, 4242);
    }

    #[test]
    fn footer_matching_blocks_filters_by_range() {
        let mk = |block_id: u32, min_ts: i64, max_ts: i64| BlockMeta {
            block_id,
            row_count: 1,
            min_ts,
            max_ts,
            data_offset: block_id as u64 * 100,
            data_size: 1,
            columns: vec![],
        };
        let footer = SegmentFooter {
            blocks: vec![mk(0, 0, 99), mk(1, 100, 199), mk(2, 200, 299)],
            row_count: 3,
            min_ts: 0,
            max_ts: 299,
            table_id: 1,
            flushed_upto_seq: 0,
        };

        assert!(footer.might_contain(50, 60));
        assert!(!footer.might_contain(1000, 2000));
        assert_eq!(footer.matching_blocks(150, 250).len(), 2);
        assert_eq!(footer.matching_blocks(150, 250)[0].block_id, 1);
        assert_eq!(footer.matching_blocks(1000, 2000).len(), 0);
    }
}