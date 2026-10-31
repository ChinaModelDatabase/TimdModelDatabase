//! Encoding diagnostics.
//!
//! Exercises the typed `ColumnData` / `ColumnCodec` path with realistic sensor
//! data and prints the compression each encoding achieves. The old version of
//! this example called the pre-rewrite per-encoder API, which no longer exists,
//! and it documented bugs (variable-width `i * 8` offsets, 32-bit delta
//! truncation) that are now fixed and covered by unit tests. Those cases are kept
//! here as round-trip checks rather than as demonstrations of corruption.
//!
//! Run with: `cargo run -p tmdb-timestore --example diag_encoding`

use std::time::Instant;

use tmdb_timestore::encoding::{
    select_encoding, ColumnCodec, ColumnData, DataType, EncodingType,
};

fn main() {
    section("1. DeltaOfDelta on jittered sensor timestamps");
    // Irregular cadence: a fixed step plus per-row jitter.
    let ts: Vec<i64> = (0..500)
        .map(|i| 1_700_000_000_000 + i * 1000 + (i % 7) * 137)
        .collect();
    let column = ColumnData::Timestamp(ts.iter().map(|v| Some(*v)).collect());
    report(&column, DataType::Timestamp);

    section("2. Gorilla on smoothly varying float readings");
    let vals: Vec<f64> = (0..500).map(|i| 20.0 + i as f64 * 0.037).collect();
    let column = ColumnData::Float64(vals.iter().map(|v| Some(*v)).collect());
    report(&column, DataType::Float64);

    section("3. Dictionary on low-cardinality tag values");
    let tags: Vec<String> = (0..1000).map(|i| format!("device-{}", i % 12)).collect();
    let column = ColumnData::String(tags.iter().map(|v| Some(v.clone())).collect());
    report(&column, DataType::String);

    section("4. RLE on repeated values");
    let repeated: Vec<i64> = (0..1000).map(|_| 42).collect();
    let column = ColumnData::Int64(repeated.iter().map(|v| Some(*v)).collect());
    report(&column, DataType::Int64);

    section("5. Variable-length strings survive a round trip (old i*8 bug)");
    let strs = vec![
        Some("alpha".to_string()),
        Some("be".to_string()),
        Some("gamma!".to_string()),
        None,
    ];
    let column = ColumnData::String(strs.clone());
    let encoded = ColumnCodec::encode(&column).expect("encode");
    let decoded = ColumnCodec::decode(&encoded, DataType::String).expect("decode");
    let got: Vec<Option<String>> = (0..strs.len()).map(|i| decoded.as_str(i).map(String::from)).collect();
    println!("  raw strings : {:?}", strs);
    println!("  round trip  : {:?}", got);
    println!("  identical   : {}", got == strs);

    section("6. Int32 uses its own width, not an 8-byte stride (old bug)");
    let i32s = vec![Some(1i32), Some(2), Some(300), Some(70_000), Some(-5), None];
    let column = ColumnData::Int32(i32s.clone());
    let encoded = ColumnCodec::encode(&column).expect("encode");
    let decoded = ColumnCodec::decode(&encoded, DataType::Int32).expect("decode");
    let got: Vec<Option<i32>> = (0..i32s.len())
        .map(|i| decoded.get_raw(i).map(|b| i32::from_le_bytes(b[..4].try_into().unwrap())))
        .collect();
    println!("  values   : {:?}", i32s);
    println!("  roundtrip: {:?}", got);
    println!("  identical: {}", got == i32s);

    section("7. Large deltas past 32 bits (old truncation bug)");
    // A gap wider than i32::MAX forces the escape path.
    let big: Vec<Option<i64>> = vec![Some(0), Some(i64::from(i32::MAX as i64) + 5_000_000_000)];
    let column = ColumnData::Timestamp(big.clone());
    let encoded = ColumnCodec::encode(&column).expect("encode");
    let decoded = ColumnCodec::decode(&encoded, DataType::Timestamp).expect("decode");
    let got: Vec<Option<i64>> = (0..2).map(|i| decoded.as_i64(i)).collect();
    println!("  values   : {:?}", big);
    println!("  roundtrip: {:?}", got);
    println!("  identical: {}", got == big);

    section("8. NULLs are preserved and stay in place");
    let with_nulls: Vec<Option<f64>> = (0..20)
        .map(|i| if i % 3 == 0 { None } else { Some(i as f64) })
        .collect();
    let column = ColumnData::Float64(with_nulls.clone());
    let encoded = ColumnCodec::encode(&column).expect("encode");
    let decoded = ColumnCodec::decode(&encoded, DataType::Float64).expect("decode");
    let got: Vec<Option<f64>> = (0..with_nulls.len()).map(|i| decoded.as_f64(i)).collect();
    println!("  nulls before={} after={}", column.null_count(), decoded.null_count());
    println!("  identical : {}", got == with_nulls);

    section("9. Encoding choice per column shape");
    for (name, column) in [
        ("timestamps", ColumnData::Timestamp((0..100).map(|i| Some(i * 1000)).collect())),
        ("timestamps jittered", ColumnData::Timestamp((0..100).map(|i| Some(i * 997 + (i % 5))).collect())),
        ("constant f64", ColumnData::Float64(vec![Some(7.5); 100])),
        ("random strings", ColumnData::String((0..100).map(|i| Some(format!("{:032x}", i * 7919))).collect())),
        ("repeating int", ColumnData::Int64(vec![Some(1); 100])),
    ] {
        let d = select_encoding(&column);
        println!("  {:22} -> {:?}", name, d.encoding);
    }

    section("10. Encode timing (100k rows)");
    let n = 100_000;
    let column = ColumnData::Timestamp((0..n).map(|i| Some(1_700_000_000_000 + i as i64 * 1000)).collect());
    let start = Instant::now();
    let encoded = ColumnCodec::encode(&column).expect("encode");
    let encode_time = start.elapsed();
    let start = Instant::now();
    let _ = ColumnCodec::decode(&encoded, DataType::Timestamp).expect("decode");
    let decode_time = start.elapsed();

    let raw = n * 8;
    println!("  rows      : {}", n);
    println!("  raw       : {} bytes", raw);
    println!("  encoded   : {} bytes ({:.2}x)", encoded.len(), raw as f64 / encoded.len() as f64);
    println!("  encode    : {:?}", encode_time);
    println!("  decode    : {:?}", decode_time);
    println!("  throughput: {:.2} M rows/s encode", n as f64 / encode_time.as_secs_f64() / 1e6);
}

fn section(title: &str) {
    println!("\n=== {} ===", title);
}

/// Encode, decode, verify, and print the ratio.
fn report(column: &ColumnData, data_type: DataType) {
    let decision = select_encoding(column);
    let raw = column.len() * data_type.fixed_width().unwrap_or(8);

    let encoded = match ColumnCodec::encode(column) {
        Ok(e) => e,
        Err(e) => {
            println!("  encode ERROR: {}", e);
            return;
        }
    };

    let decoded = match ColumnCodec::decode(&encoded, data_type) {
        Ok(d) => d,
        Err(e) => {
            println!("  encode ok, {} -> {} bytes", raw, encoded.len());
            println!("  decode ERROR: {}", e);
            return;
        }
    };

    let identical = columns_equal(column, &decoded);
    let ratio = raw as f64 / encoded.len().max(1) as f64;
    println!(
        "  {:16} raw={:5}B enc={:5}B ({:.2}x) rows={:4} nulls={} roundtrip={}",
        format!("{:?}", decision.encoding),
        raw,
        encoded.len(),
        ratio,
        column.len(),
        column.null_count(),
        if identical { "ok" } else { "MISMATCH" }
    );

    if decision.encoding == EncodingType::Raw && ratio < 1.0 {
        println!("  note: Raw selected but the block is smaller than fixed width");
    }
}

fn columns_equal(a: &ColumnData, b: &ColumnData) -> bool {
    if a.len() != b.len() {
        return false;
    }
    (0..a.len()).all(|i| a.get_raw(i) == b.get_raw(i))
}
