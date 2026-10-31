use clap::{Parser, Subcommand};
use std::time::{Duration, Instant};

/// tmDB - 信创时序数据库
#[derive(Parser)]
#[command(name = "tmdb", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Configuration file path
    #[arg(short, long, default_value = "tmdb.toml")]
    config: String,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the server
    Start {
        /// Listen address for PostgreSQL wire
        #[arg(long, default_value = "0.0.0.0:5432")]
        listen: String,

        /// Listen address for MySQL wire
        #[arg(long, default_value = "0.0.0.0:3306")]
        listen_mysql: String,

        /// Listen address for HTTP
        #[arg(long, default_value = "0.0.0.0:8080")]
        listen_http: String,
    },

    /// Import data
    Import {
        /// Import format (csv, influx, prometheus)
        #[arg(short, long)]
        format: String,

        /// Input file path
        #[arg(short, long)]
        file: String,

        /// Target table name
        #[arg(short, long)]
        table: String,
    },

    /// Export data
    Export {
        /// Export format (csv, parquet)
        #[arg(short, long)]
        format: String,

        /// Output file path
        #[arg(short, long)]
        file: String,

        /// Source table name
        #[arg(short, long)]
        table: String,
    },

    /// Run benchmark
    Benchmark {
        /// Number of points
        #[arg(long, default_value = "1000000")]
        points: u64,

        /// Number of series
        #[arg(long, default_value = "1000")]
        series: u64,

        /// Number of columns
        #[arg(long, default_value = "5")]
        columns: u32,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Start {
            listen,
            listen_mysql,
            listen_http,
        } => {
            tracing::info!("tmDB starting...");
            tracing::info!("PG wire: {}", listen);
            tracing::info!("MySQL wire: {}", listen_mysql);
            tracing::info!("HTTP: {}", listen_http);

            // TODO: bind listeners and accept connections
            tracing::info!("Server started. Press Ctrl+C to stop.");

            // Keep the server running
            tokio::signal::ctrl_c().await?;
            tracing::info!("Shutting down...");
        }
        Commands::Import {
            format,
            file,
            table,
        } => {
            let fmt = match format.as_str() {
                "csv" => tmdb_compat::ImportFormat::Csv,
                "influx" => tmdb_compat::ImportFormat::InfluxLine,
                "prometheus" => tmdb_compat::ImportFormat::Prometheus,
                "tdengine" => tmdb_compat::ImportFormat::Tdengine,
                _ => return Err(anyhow::anyhow!("unknown format: {}", format)),
            };
            let count = tmdb_compat::Importer::import_file(fmt, &file, &table)?;
            tracing::info!("Imported {} rows into '{}'", count, table);
        }
        Commands::Export {
            format,
            file,
            table,
        } => {
            let fmt = match format.as_str() {
                "csv" => tmdb_compat::ExportFormat::Csv,
                "parquet" => tmdb_compat::ExportFormat::Parquet,
                _ => return Err(anyhow::anyhow!("unknown format: {}", format)),
            };
            let count = tmdb_compat::Exporter::export_file(fmt, &file, &table)?;
            tracing::info!("Exported {} rows from '{}'", count, table);
        }
        Commands::Benchmark {
            points,
            series,
            columns,
        } => run_benchmark(points, series, columns)?,
    }

    Ok(())
}

/// End-to-end benchmark through the public SQL surface.
///
/// Every phase goes through `Engine::execute`, so the numbers include parsing,
/// planning and the storage path -- the costs a user actually pays. Stages that
/// are not the thing being measured (data generation) are prepared beforehand,
/// because timing the generator makes any write target look out of reach when
/// it is not.
///
/// Each phase verifies its own row count before reporting, so a benchmark that
/// silently drops rows cannot produce a fast-looking number.
fn run_benchmark(points: u64, series: u64, columns: u32) -> anyhow::Result<()> {
let dir = std::env::temp_dir().join(format!("tmdb-bench-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let result = bench_run(&dir, points, series, columns);
    // Remove the data directory either way: a failed run still leaves a table
    // behind, and a benchmark is not a fixture.
    let _ = std::fs::remove_dir_all(&dir);
    result
        .map_err(|e| anyhow::anyhow!("benchmark failed (data dir {} removed): {}", dir.display(), e))
}

fn bench_run(
    dir: &std::path::Path,
    points: u64,
    series: u64,
    columns: u32,
) -> anyhow::Result<()> {
    if points == 0 || series == 0 || columns == 0 {
        anyhow::bail!("points, series and columns must all be greater than zero");
    }

    let engine = tmdb_exec::Engine::new(dir);
    let mut column_defs = vec!["ts TIMESTAMP".to_string(), "host VARCHAR".to_string()];
    for i in 0..columns {
        column_defs.push(format!("v{} DOUBLE", i));
    }
    let table_sql = format!("CREATE TABLE bench ({})", column_defs.join(", "));

    let ddl = Instant::now();
    engine.execute(&table_sql)?;
    let ddl_time = ddl.elapsed();

    // --- Insert -------------------------------------------------------------
    // Prepared outside the timed region: the statement text is not the insert
    // path, and generating it per row would dominate the measurement.
    let batch_size = 1000u64;
    let batches = points.div_ceil(batch_size);
    let mut statements: Vec<String> = Vec::with_capacity(batches as usize);
    for b in 0..batches {
        let start = b * batch_size;
        let end = (start + batch_size).min(points);
        let mut rows = Vec::with_capacity((end - start) as usize);
for i in start..end {
            let ts = 1_000 + i as i64;
            // A string literal must be quoted, or `host-0` parses as
            // subtraction rather than as the value it is meant to be.
            let host = format!("'host-{}'", i % series);
            let mut values = vec![host];
            for c in 0..columns {
                values.push(format!("{:.6}", (i as f64) * 0.25 + c as f64));
            }
            rows.push(format!("({}, {})", ts, values.join(", ")));
        }
        statements.push(format!("INSERT INTO bench VALUES {}", rows.join(", ")));
    }

    let insert = Instant::now();
    for sql in &statements {
        engine.execute(sql)?;
    }
    let insert_time = insert.elapsed();

    // Verify the write phase before trusting its rate.
    let count_rows = match engine.execute("SELECT count(*) FROM bench")? {
        tmdb_exec::QueryResult::Rows { rows, .. } => rows,
        other => anyhow::bail!("count(*) returned {:?}", other),
    };
    let counted = i64_from(&count_rows[0][0]) as u64;
    if counted != points {
        anyhow::bail!(
            "inserted {} of {} points; refusing to report a rate for a partial write",
            counted,
            points
        );
    }

    // --- Read ---------------------------------------------------------------
    let scan = Instant::now();
    let scanned = match engine.execute("SELECT ts, host, v0 FROM bench")? {
        tmdb_exec::QueryResult::Rows { rows, .. } => rows,
        other => anyhow::bail!("full scan returned {:?}", other),
    };
    let scan_time = scan.elapsed();
    if scanned.len() as u64 != points {
        anyhow::bail!(
            "full scan returned {} of {} rows",
            scanned.len(),
            points
        );
    }

    let filter = Instant::now();
    let filtered = match engine.execute("SELECT ts, host, v0 FROM bench WHERE v0 > 1000.0")? {
        tmdb_exec::QueryResult::Rows { rows, .. } => rows,
        other => anyhow::bail!("filtered scan returned {:?}", other),
    };
    let filter_time = filter.elapsed();

    let group = Instant::now();
    let grouped = match engine.execute(
        "SELECT host, count(*) FROM bench GROUP BY host HAVING count(*) > 0",
    )? {
        tmdb_exec::QueryResult::Rows { rows, .. } => rows,
        other => anyhow::bail!("grouped scan returned {:?}", other),
    };
    let group_time = group.elapsed();
    if grouped.len() as u64 != series {
        anyhow::bail!(
            "grouped {} hosts into {} groups, expected {}",
            series,
            grouped.len(),
            series
        );
    }

    // --- Report -------------------------------------------------------------
    println!("=== end-to-end SQL benchmark ===");
    println!(
        "  points     {:>12}   series {:>8}   value columns {:>4}",
        points, series, columns
    );
    println!("  table      {}", table_sql);
    println!();
    println!(
        "  create     {:>10}   {:>12.2} ms",
        "",
        ms(ddl_time)
    );
    println!(
        "  insert     {:>10}   {:>12.2} ms   {:>10.2} K rows/s   ({} statements, {} verified)",
        "",
        ms(insert_time),
        rate(points, insert_time),
        statements.len(),
        counted
    );
    println!(
        "  scan *     {:>10}   {:>12.2} ms   {:>10.2} K rows/s   ({} rows verified)",
        "",
        ms(scan_time),
        rate(points, scan_time),
        scanned.len()
    );
    println!(
        "  scan where {:>10}   {:>12.2} ms   {:>10.2} K rows/s   ({} rows matched)",
        "",
        ms(filter_time),
        rate(points, filter_time),
        filtered.len()
    );
    println!(
        "  group+hv   {:>10}   {:>12.2} ms   {:>10.2} K rows/s   ({} groups verified)",
        "",
        ms(group_time),
        rate(points, group_time),
        grouped.len()
    );
    println!();
    println!(
        "  total      {:>10}   {:>12.2} ms",
        "",
        ms(insert_time + scan_time + filter_time + group_time)
    );

    Ok(())
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

fn rate(rows: u64, d: Duration) -> f64 {
    if d.is_zero() {
        return f64::INFINITY;
    }
    rows as f64 / d.as_secs_f64() / 1e3
}

/// Decode an Int64 column value.
fn i64_from(v: &Option<Vec<u8>>) -> i64 {
    let bytes = v.as_ref().expect("count(*) must not be NULL");
    i64::from_le_bytes(bytes[..8].try_into().unwrap())
}
