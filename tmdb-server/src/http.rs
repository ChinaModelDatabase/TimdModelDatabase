/// HTTP server for Prometheus metrics endpoint
use std::sync::Arc;
use tmdb_exec::Engine;
use crate::metrics::Metrics;

/// HTTP server
pub struct HttpServer {
    engine: Arc<Engine>,
    metrics: Arc<Metrics>,
    addr: String,
}

impl HttpServer {
    pub fn new(engine: Arc<Engine>, metrics: Arc<Metrics>, addr: String) -> Self {
        Self { engine, metrics, addr }
    }

    /// Start HTTP server
    pub async fn start(&self) -> anyhow::Result<()> {
        let addr = self.addr.clone();
        let metrics = self.metrics.clone();

        let listener = tokio::net::TcpListener::bind(&addr).await?;
        tracing::info!("HTTP server listening on {}", addr);

        loop {
            let (stream, _) = listener.accept().await?;
            let metrics = metrics.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_http_connection(stream, &metrics).await {
                    tracing::error!("HTTP connection error: {}", e);
                }
            });
        }
    }
}

/// Handle HTTP connection
async fn handle_http_connection(
    mut stream: tokio::net::TcpStream,
    metrics: &Metrics,
) -> anyhow::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await?;

    let request = String::from_utf8_lossy(&buf[..n]);
    let lines: Vec<&str> = request.lines().collect();

    if let Some(first_line) = lines.first() {
        let parts: Vec<&str> = first_line.split_whitespace().collect();
        if parts.len() >= 2 {
            let path = parts[1];
            let body = match path {
                "/metrics" => metrics.to_prometheus(),
                "/health" => "OK".to_string(),
                "/ready" => "OK".to_string(),
                _ => "Not Found".to_string(),
            };

            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_endpoint() {
        let metrics = Metrics::new();
        metrics.record_write(100, 8000);
        let prom = metrics.to_prometheus();
        assert!(prom.contains("tmdb_writes_total 100"));
    }
}
