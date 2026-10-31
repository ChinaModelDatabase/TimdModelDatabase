/// Migration: Influx/Prometheus/TDengine/MySQL/PG import.

use std::path::Path;

/// Import format
#[derive(Debug, Clone, Copy)]
pub enum ImportFormat {
    InfluxLine,
    Prometheus,
    Csv,
    Tdengine,
    Mysql,
    Postgres,
}

/// Importer
pub struct Importer;

impl Importer {
    /// Import from file
    pub fn import_file(
        format: ImportFormat,
        path: impl AsRef<Path>,
        table: &str,
    ) -> anyhow::Result<u64> {
        match format {
            ImportFormat::Csv => Self::import_csv(path.as_ref(), table),
            ImportFormat::InfluxLine => Self::import_influx_line(path.as_ref(), table),
            _ => Err(anyhow::anyhow!("format {:?} not yet implemented", format)),
        }
    }

    fn import_csv(path: &Path, table: &str) -> anyhow::Result<u64> {
        let content = std::fs::read_to_string(path)?;
        let lines: Vec<&str> = content.lines().collect();
        tracing::info!(
            "Importing {} CSV lines into table '{}'",
            lines.len(),
            table
        );
        // TODO: parse CSV and insert rows
        Ok(lines.len() as u64)
    }

    fn import_influx_line(path: &Path, table: &str) -> anyhow::Result<u64> {
        let content = std::fs::read_to_string(path)?;
        let lines: Vec<&str> = content.lines().collect();
        tracing::info!(
            "Importing {} InfluxDB line protocol lines into table '{}'",
            lines.len(),
            table
        );
        // TODO: parse line protocol and insert rows
        Ok(lines.len() as u64)
    }
}

/// Export format
#[derive(Debug, Clone, Copy)]
pub enum ExportFormat {
    Csv,
    Parquet,
    Json,
}

/// Exporter
pub struct Exporter;

impl Exporter {
    pub fn export_file(
        format: ExportFormat,
        path: impl AsRef<Path>,
        table: &str,
    ) -> anyhow::Result<u64> {
        tracing::info!("Exporting table '{}' to {:?}", table, format);
        // TODO: implement export
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_import_csv() {
        let dir = tempfile::tempdir().unwrap();
        let csv_path = dir.path().join("test.csv");
        std::fs::write(&csv_path, "ts,value\n1000,1.0\n2000,2.0\n").unwrap();

        let count = Importer::import_file(ImportFormat::Csv, &csv_path, "metrics").unwrap();
        assert_eq!(count, 3); // header + 2 data lines
    }
}
