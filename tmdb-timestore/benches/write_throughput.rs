//! Write-path benchmarks.
//!
//! These are `#[ignore]`d so a normal `cargo test` does not spend minutes
//! writing gigabytes to disk. Run them with:
//!
//! ```text
//! cargo test -p tmdb-timestore --release --bench write_throughput -- --ignored --nocapture
//! ```
//!
//! Use `--release`. In a debug build the encoders are several times slower and
//! the numbers say nothing about production behaviour.
//!
//! # What is actually measured
//!
//! Every figure below is computed from data that was written and read back.
//! The previous version of this file printed a hardcoded "10M rows simulation"
//! ratio of 8x/64x/8x per column, which asserted a result rather than
//! measuring one. There is no simulation here.
//!
//! # Correctness gate
//!
//! Each benchmark verifies the data it wrote before reporting a rate. A fast
//! path that corrupts or drops rows is not a fast path, and a throughput number
//! without a correctness check next to it is not evidence.

use std::time::Instant;

use tmdb_timestore::encoding::{compress_page, select_encoding, ColumnCodec, ColumnData, DataType};
use tmdb_timestore::{ColumnDef, CompressionLevel, Table, TableMeta, TsUnit};

/// Points to write. Large enough that per-call overhead is noise.
const POINTS: usize = 2_000_000;
/// Rows per `insert_batch` call.
const BATCH: usize = 50_000;
/// Distinct devices, i.e. the `device_id` cardinality.
const DEVICES: i64 = 1_000;

fn test_meta() -> TableMeta {
    TableMeta {
        table_id: 1,
        name: "benchmark_table".to_string(),
        ts_unit: TsUnit::Millis,
        columns: vec![
            ColumnDef {
                name: "ts".to_string(),
                data_type: DataType::Timestamp,
                is_tag: false,
                nullable: false,
                compression: CompressionLevel::Off,
            },
            ColumnDef {
                name: "volt".to_string(),
                data_type: DataType::Float64,
                is_tag: false,
                nullable: true,
                compression: CompressionLevel::Lz4,
            },
            ColumnDef {
                name: "device_id".to_string(),
                data_type: DataType::Int64,
                is_tag: false,
                nullable: false,
                compression: CompressionLevel::Lz4,
            },
        ],
        chunk_interval_ms: 86_400_000,
        retention_days: 365,
    }
}

/// Voltage for point `i`: a slowly drifting value, as a sensor would produce.
fn volt_at(i: i64) -> f64 {
    220.0 + ((i % 500) as f64) * 0.01
}

fn device_at(i: i64) -> i64 {
    i % DEVICES
}

/// Bytes a row occupies before any encoding: ts + volt + device_id.
const RAW_BYTES_PER_ROW: usize = 8 + 8 + 8;

fn batch(start: i64, n: usize) -> Vec<(i64, Vec<Option<Vec<u8>>>)> {
    (0..n as i64)
        .map(|k| {
            let i = start + k;
            (
                1_000 + i,
                vec![
                    Some((1_000 + i).to_le_bytes().to_vec()),
                    Some(volt_at(i).to_le_bytes().to_vec()),
                    Some(device_at(i).to_le_bytes().to_vec()),
                ],
            )
        })
        .collect()
}

/// The log write path on its own.
///
/// Every point is written here before it reaches a segment, so this is a floor
/// under the write throughput the table can offer. The handle is kept open and
/// each record goes out in a single write; measured before that change it cost a
/// stat, an open, three writes and a close per batch.
#[test]
#[ignore = "benchmark; run with --release --ignored"]
fn bench_wal_append() {
    use tmdb_timestore::wal::Wal;

    const ROWS: usize = 2_000_000;
    const PER_BATCH: usize = 10_000;

    println!("=== WAL append ({} rows) ===", ROWS);

    let rows: Vec<(i64, Vec<Option<Vec<u8>>>)> = (0..PER_BATCH as i64)
        .map(|k| (k, vec![Some(vec![7u8; 16])]))
        .collect();

    let dir = tempfile::tempdir().expect("temp dir");
    let wal = Wal::new(dir.path().join("bench.wal"));

    let start = Instant::now();
    for batch in 0..(ROWS / PER_BATCH) {
        wal.append(1, batch as u64, &rows).expect("append");
    }
    let elapsed = start.elapsed();

    let on_disk = std::fs::metadata(dir.path().join("bench.wal"))
        .map(|m| m.len())
        .unwrap_or(0);
    println!(
        "  append     {:>7.3}s  {:>7.2} M rows/s  {:>6.1} MB logged",
        elapsed.as_secs_f64(),
        ROWS as f64 / elapsed.as_secs_f64() / 1e6,
        on_disk as f64 / 1e6,
    );

    // A durable append fsyncs, which is what a checkpoint costs.
    let start = Instant::now();
    for batch in 0..200 {
        wal.append_durable(1, batch as u64, &rows).expect("append_durable");
    }
    let durable = start.elapsed();
    println!(
        "  durable    {:>7.3}s  {:>7.2} fsync/s",
        durable.as_secs_f64(),
        200.0 / durable.as_secs_f64()
    );

    let start = Instant::now();
    for _ in 0..20 {
        wal.compact().expect("compact");
    }
    let compact = start.elapsed();
    println!(
        "  compact    {:>7.3}s  {:>6.1} MB/s",
        compact.as_secs_f64() / 20.0,
        on_disk as f64 / 1e6 / compact.as_secs_f64()
    );
}

/// Column encode/decode throughput and ratio, per column type.
#[test]
#[ignore = "benchmark; run with --release --ignored"]
fn bench_column_codec() {
    let n = 1_000_000;
    println!("=== Column encode/decode ({} rows) ===", n);

    // Regular one-second timestamps: the DeltaOfDelta best case.
    let ts: Vec<Option<i64>> = (0..n as i64).map(|i| Some(1_700_000_000_000 + i * 1000)).collect();
    measure("timestamp (regular)", ColumnData::Timestamp(ts), DataType::Timestamp, n);

    // Jittered timestamps: closer to real network and sensor data.
    let jittered: Vec<Option<i64>> = (0..n as i64)
        .map(|i| Some(1_700_000_000_000 + i * 1000 + (i % 7) * 137))
        .collect();
    measure("timestamp (jittered)", ColumnData::Timestamp(jittered), DataType::Timestamp, n);

    // Slowly varying readings: the Gorilla best case.
    let volts: Vec<Option<f64>> = (0..n as i64).map(|i| Some(volt_at(i))).collect();
    measure("float64 (drifting)", ColumnData::Float64(volts), DataType::Float64, n);

    // Constant readings: RLE territory.
    let flat: Vec<Option<f64>> = vec![Some(220.0); n];
    measure("float64 (constant)", ColumnData::Float64(flat), DataType::Float64, n);

    // Bounded integer cardinality: BitPack territory.
    let devices: Vec<Option<i64>> = (0..n as i64).map(|i| Some(device_at(i))).collect();
    measure("int64 (device ids)", ColumnData::Int64(devices), DataType::Int64, n);

    // Low-cardinality tags: Dictionary territory.
    let tags: Vec<Option<String>> = (0..n)
        .map(|i| Some(format!("device-{}", i as i64 % DEVICES)))
        .collect();
    measure(
        "string (tags)",
        ColumnData::String(tags),
        DataType::String,
        n,
    );

    // A column with holes, since NULLs cost a bitmap and change every choice.
    let sparse: Vec<Option<f64>> = (0..n as i64)
        .map(|i| if i % 17 == 0 { None } else { Some(volt_at(i)) })
        .collect();
    measure("float64 (7% null)", ColumnData::Float64(sparse), DataType::Float64, n);

    block_granularity();
}

/// Block size trades three things against each other, and they disagree:
///
/// * encoding and LZ4 both get faster and smaller with bigger blocks,
/// * a point lookup has to decode a whole block, so it gets slower,
/// * zone-map pruning on a wide scan gets coarser.
///
/// Picking a default by intuition is how the previous value ended up at 4096.
/// This measures the curve so the default is a decision rather than a guess.
#[test]
#[ignore = "benchmark; run with --release --ignored"]
fn bench_block_size_tradeoff() {
    const POINTS: usize = 1_000_000;
    const PROBES: usize = 2_000;

    println!("=== Block size trade-off ({} points) ===", POINTS);
    println!(
        "  {:>9}  {:>10}  {:>9}  {:>8}  {:>12}  {:>12}",
        "rows", "ratio", "flush M/s", "scan M/s", "point us", "point p99 us"
    );

    for rows_per_block in [4096usize, 8192, 16384, 32768, 65536] {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut table = Table::new(test_meta(), dir.path());
        table.set_rows_per_block(rows_per_block);

        let start = Instant::now();
        for chunk in (0..POINTS).collect::<Vec<_>>().chunks(BATCH) {
            table.insert_batch(&batch(chunk[0] as i64, chunk.len()));
        }
        table.flush().expect("flush");
        let flush = start.elapsed();

        let disk = table.metrics().segment_bytes;
        let ratio = (POINTS * RAW_BYTES_PER_ROW) as f64 / disk.max(1) as f64;

        let start = Instant::now();
        let rows = table.scan(i64::MIN, i64::MAX, &[], None).expect("scan");
        let scan = start.elapsed();
        assert_eq!(rows.len(), POINTS, "scan returned the wrong count");

        let mut samples = Vec::with_capacity(PROBES);
        for k in 0..PROBES {
            let i = (k as i64 * 499) % POINTS as i64;
            let t0 = Instant::now();
            let hit = table
                .scan(1_000 + i, 1_000 + i, &[], None)
                .expect("point scan");
            samples.push(t0.elapsed().as_secs_f64() * 1e6);
            assert_eq!(hit.len(), 1, "point lookup {i} missed");
        }
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let mean = samples.iter().sum::<f64>() / samples.len() as f64;
        let p99 = samples[samples.len() - samples.len() / 100];

        println!(
            "  {:>9}  {:>9.1}x  {:>9.2}  {:>8.2}  {:>12.1}  {:>12.1}",
            rows_per_block,
            ratio,
            POINTS as f64 / flush.as_secs_f64() / 1e6,
            POINTS as f64 / scan.as_secs_f64() / 1e6,
            mean,
            p99,
        );
    }
}

/// A segment encodes one block at a time, then LZ4s each chunk. Isolating those
/// two steps says whether block size or the second compression pass is the cost.
fn block_granularity() {
    const N: usize = 2_000_000;
    let volts: Vec<Option<f64>> = (0..N as i64).map(|i| Some(volt_at(i))).collect();

    println!("\n=== Encode cost by block size ({} rows) ===", N);
    for rows_per_block in [4096usize, 65536, 1_048_576, N] {
        let mut encoded_bytes = 0usize;
        let mut stored_bytes = 0usize;
        let mut blocks = 0usize;

        let start = Instant::now();
        for chunk in volts.chunks(rows_per_block) {
            let col = ColumnData::Float64(chunk.to_vec());
            let bytes = ColumnCodec::encode(&col).expect("encode");
            encoded_bytes += bytes.len();
            blocks += 1;
        }
        let encode_only = start.elapsed();

        let start = Instant::now();
        for chunk in volts.chunks(rows_per_block) {
            let col = ColumnData::Float64(chunk.to_vec());
            let bytes = ColumnCodec::encode(&col).expect("encode");
            stored_bytes += compress_page(&bytes, CompressionLevel::Lz4).len();
        }
        let with_lz4 = start.elapsed();

        println!(
            "  {:>9} rows/block  {:>4} blocks  encode {:>5.2}s ({:>5.2} M/s)  +lz4 {:>5.2}s ({:>5.2} M/s)  lz4 ratio {:>4.2}x",
            rows_per_block,
            blocks,
            encode_only.as_secs_f64(),
            N as f64 / encode_only.as_secs_f64() / 1e6,
            with_lz4.as_secs_f64(),
            N as f64 / with_lz4.as_secs_f64() / 1e6,
            encoded_bytes as f64 / stored_bytes as f64,
        );
    }
}

/// Encode, decode, verify, and report one column.
fn measure(name: &str, column: ColumnData, data_type: DataType, n: usize) {
    let decision = select_encoding(&column);
    let raw = n * data_type.fixed_width().unwrap_or(8);

    let start = Instant::now();
    let encoded = ColumnCodec::encode(&column).expect("encode");
    let encode = start.elapsed();

    let start = Instant::now();
    let decoded = ColumnCodec::decode(&encoded, data_type).expect("decode");
    let decode = start.elapsed();

    // Correctness first: a ratio computed from corrupt output is meaningless.
    assert_eq!(decoded.len(), n, "{}: row count changed", name);
    assert_eq!(decoded.null_count(), column.null_count(), "{}: NULLs changed", name);
    for i in 0..n {
        assert_eq!(
            decoded.get_raw(i),
            column.get_raw(i),
            "{}: value at row {} changed",
            name,
            i
        );
    }

    println!(
        "  {:24} {:14} raw={:5}KB enc={:5}KB {:6.2}x  enc {:6.2} M/s  dec {:6.2} M/s",
        name,
        format!("{:?}", decision.encoding),
        raw / 1024,
        encoded.len() / 1024,
        raw as f64 / encoded.len().max(1) as f64,
        n as f64 / encode.as_secs_f64() / 1e6,
        n as f64 / decode.as_secs_f64() / 1e6,
    );
}

/// End-to-end write, flush and read on real files, with a correctness check.
#[test]
#[ignore = "benchmark; writes several GB; run with --release --ignored"]
fn bench_table_write_flush_scan() {
    let dir = tempfile::tempdir().expect("temp dir");
    let table = Table::new(test_meta(), dir.path());

    println!("=== Table write / flush / scan ({} points) ===", POINTS);

    // Build every batch up front. Generating the rows allocates three `Vec`s per
    // point, so doing it inside the timed region measured this benchmark's own
    // data generator rather than the insert path -- enough to make the write
    // target look out of reach when it is not.
    let batches: Vec<Vec<(i64, Vec<Option<Vec<u8>>>)>> = (0..(POINTS as i64 / BATCH as i64))
        .map(|c| batch(c * BATCH as i64, BATCH))
        .collect();

    // --- Write ---
    let start = Instant::now();
    for rows in &batches {
        table.insert_batch(rows);
    }
    let write_time = start.elapsed();
    assert_eq!(batches.iter().map(|b| b.len()).sum::<usize>(), POINTS);

    // --- Flush ---
    let start = Instant::now();
    table.flush().expect("flush");
    let flush_time = start.elapsed();

    // Segment bytes only: the log is transient and would flatter the ratio.
    let metrics = table.metrics();
    let disk_bytes = metrics.segment_bytes;
    let raw_bytes = (POINTS * RAW_BYTES_PER_ROW) as u64;

    println!("  write      {:>8.2}s  {:>7.2} M points/s", write_time.as_secs_f64(), POINTS as f64 / write_time.as_secs_f64() / 1e6);
    println!("  flush      {:>8.2}s  {:>7.2} M points/s", flush_time.as_secs_f64(), POINTS as f64 / flush_time.as_secs_f64() / 1e6);
    println!("  raw        {:>8.1} MB", raw_bytes as f64 / 1e6);
    println!("  on disk    {:>8.1} MB  ({} segments)", disk_bytes as f64 / 1e6, metrics.segment_count);
    println!("  ratio      {:>8.2}x", raw_bytes as f64 / disk_bytes.max(1) as f64);

    // --- Read back and verify ---
    let start = Instant::now();
    let rows = table
        .scan(i64::MIN, i64::MAX, &[], None)
        .expect("scan");
    let scan_time = start.elapsed();

    assert_eq!(rows.len(), POINTS, "scan returned the wrong number of rows");
    for (i, (ts, values)) in rows.iter().enumerate() {
        let i = i as i64;
        assert_eq!(*ts, 1_000 + i, "row {} has the wrong timestamp", i);

        let volt = f64::from_le_bytes(values[1].as_ref().unwrap()[..8].try_into().unwrap());
        assert_eq!(volt, volt_at(i), "row {} has the wrong voltage", i);

        let device = i64::from_le_bytes(values[2].as_ref().unwrap()[..8].try_into().unwrap());
        assert_eq!(device, device_at(i), "row {} has the wrong device", i);
    }

    println!(
        "  scan       {:>8.2}s  {:>7.2} M points/s  (all {} rows verified)",
        scan_time.as_secs_f64(),
        POINTS as f64 / scan_time.as_secs_f64() / 1e6,
        rows.len()
    );

    // --- Point lookup ---
    let start = Instant::now();
    let probes = 10_000usize;
    for k in 0..probes {
        let i = (k as i64 * 197) % POINTS as i64;
        let found = table.scan(1_000 + i, 1_000 + i, &[], None).expect("point scan");
        assert_eq!(found.len(), 1, "point lookup for {i} returned {} rows", found.len());
    }
    let lookup_time = start.elapsed();
    println!(
        "  point scan {:>8.2}s  {:>7.2} M lookups/s  ({:.1} us each)",
        lookup_time.as_secs_f64(),
        probes as f64 / lookup_time.as_secs_f64() / 1e6,
        lookup_time.as_secs_f64() * 1e6 / probes as f64
    );
}

/// Many small flushes, then compaction: the write-heavy steady state.
#[test]
#[ignore = "benchmark; run with --release --ignored"]
fn bench_flush_and_compaction() {
    let dir = tempfile::tempdir().expect("temp dir");
    let table = Table::new(test_meta(), dir.path());

    let per_flush = 100_000usize;
    let flushes = 20usize;
    println!("=== Flush and compaction ({} flushes x {} rows) ===", flushes, per_flush);

    let start = Instant::now();
    for f in 0..flushes {
        let rows = batch(f as i64 * per_flush as i64, per_flush);
        table.insert_batch(&rows);
        table.flush().expect("flush");
    }
    let total_time = start.elapsed();
    let total = per_flush * flushes;

    let metrics = table.metrics();
    println!("  total      {:>8.2}s  {:>7.2} M points/s", total_time.as_secs_f64(), total as f64 / total_time.as_secs_f64() / 1e6);
    println!("  flushes    {}", metrics.flushes);
    println!("  compactions{}", metrics.compactions);
    println!("  segments   {}", metrics.segment_count);
    println!("  rows merged{}", metrics.rows_merged);
    if let Some(ratio) = metrics.compaction_ratio() {
        println!("  merge ratio{:>8.2}x", ratio);
    }
    println!("  on disk    {:>8.1} MB", metrics.segment_bytes as f64 / 1e6);

    assert_eq!(table.row_count(), total as u64, "compaction lost rows");

    // The data must still read back correctly after every merge.
    let rows = table.scan(i64::MIN, i64::MAX, &[], None).expect("scan");
    assert_eq!(rows.len(), total, "scan after compaction returned the wrong count");
    for (i, (ts, _)) in rows.iter().enumerate() {
        assert_eq!(*ts, 1_000 + i as i64, "row {} has the wrong timestamp", i);
    }
    println!("  verified   all {} rows after compaction", rows.len());
}

/// Where flush time actually goes.
///
/// `flush` fsyncs the log, encodes, writes and fsyncs a segment, then fsyncs a
/// checkpoint and rewrites the log. Those are very different costs and the total
/// alone does not say which one to fix.
#[test]
#[ignore = "benchmark; run with --release --ignored"]
fn bench_flush_phase_breakdown() {
    let n = 2_000_000usize;
    println!("=== Flush phase breakdown ({} points) ===", n);

    let dir = tempfile::tempdir().expect("temp dir");
    let table = Table::new(test_meta(), dir.path());

    let rows: Vec<(i64, Vec<Option<Vec<u8>>>)> = batch(0, n);

    let start = Instant::now();
    table.bulk_insert(&rows);
    let insert = start.elapsed();
    let wal_bytes = table.wal_size();

    // Isolate the log fsync, which flush pays before it writes anything.
    let start = Instant::now();
    table.flush_wal_only();
    let wal_sync = start.elapsed();

    let start = Instant::now();
    table.flush().expect("flush");
    let flush = start.elapsed();

    // A second flush has nothing to write, so it isolates the fixed overhead.
    let start = Instant::now();
    table.flush().expect("empty flush");
    let empty_flush = start.elapsed();

    let metrics = table.metrics();
    println!("  insert        {:>7.2}s  ({:.0} MB logged)", insert.as_secs_f64(), wal_bytes as f64 / 1e6);
    println!("  log fsync     {:>7.2}s", wal_sync.as_secs_f64());
    println!("  flush total   {:>7.2}s  (fsync above is included)", flush.as_secs_f64());
    println!("  flush minus fsync {:>5.2}s", (flush - wal_sync).as_secs_f64());
    println!("  empty flush   {:>7.2}s  (fixed overhead)", empty_flush.as_secs_f64());
    println!("  segments      {}", metrics.segment_count);
    assert_eq!(table.row_count(), n as u64);
}

/// Reopen cost: how long a restart takes to be queryable again.
#[test]
#[ignore = "benchmark; run with --release --ignored"]
fn bench_reopen() {
    let dir = tempfile::tempdir().expect("temp dir");
    let per_flush = 200_000usize;
    let flushes = 10usize;

    {
        let table = Table::new(test_meta(), dir.path());
        for f in 0..flushes {
            table.insert_batch(&batch(f as i64 * per_flush as i64, per_flush));
            table.flush().expect("flush");
        }
    }

    let start = Instant::now();
    let table = Table::new(test_meta(), dir.path());
    let open_time = start.elapsed();

    let expected = (per_flush * flushes) as u64;
    assert_eq!(table.row_count(), expected, "reopen lost rows");

    let start = Instant::now();
    let rows = table.scan(i64::MIN, i64::MAX, &[], None).expect("scan");
    let first_scan = start.elapsed();
    assert_eq!(rows.len() as u64, expected);

    println!("=== Reopen ({} rows, {} segments) ===", expected, table.segment_count());
    println!("  open       {:>8.2}s", open_time.as_secs_f64());
    println!("  first scan {:>8.2}s  {:>7.2} M rows/s", first_scan.as_secs_f64(), expected as f64 / first_scan.as_secs_f64() / 1e6);
}
