//! Streaming table copy under an exported snapshot.

use crate::client::PgClient;
use crate::errors::from_pg;
use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use pgsync_core::config::{TableName, quote_ident, quote_literal};
use pgsync_core::metrics::Metrics;
use pgsync_core::{Error, Result};
use std::sync::Arc;
use tracing::debug;

/// Start a read-only REPEATABLE READ transaction on `src` that sees `snapshot_name`.
/// Must be the first statements on the connection's transaction.
pub async fn import_snapshot(src: &PgClient, snapshot_name: &str) -> Result<()> {
    if snapshot_name.is_empty()
        || !snapshot_name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-')
    {
        return Err(Error::internal(format!(
            "suspicious snapshot name {snapshot_name:?}"
        )));
    }
    src.batch_execute(&format!(
        "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET TRANSACTION SNAPSHOT {};",
        quote_literal(snapshot_name)
    ))
    .await
    .map_err(|e| from_pg("import snapshot", e))
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CopyStats {
    pub rows: u64,
    pub bytes: u64,
}

/// Stream `table` from `src` (inside its snapshot transaction) into `dst` (inside the
/// caller's target transaction) with `COPY ... TO STDOUT` / `COPY ... FROM STDIN`.
/// Memory is bounded by `buffer_size` plus one server chunk.
pub async fn copy_table(
    src: &PgClient,
    dst: &PgClient,
    table: &TableName,
    columns: &[String],
    buffer_size: usize,
    metrics: &Arc<Metrics>,
) -> Result<CopyStats> {
    let cols: Vec<String> = columns.iter().map(|c| quote_ident(c)).collect();
    let col_list = cols.join(", ");
    let out_sql = format!("COPY {} ({col_list}) TO STDOUT", table.quoted());
    let in_sql = format!("COPY {} ({col_list}) FROM STDIN", table.quoted());

    let out = src
        .copy_out(&out_sql)
        .await
        .map_err(|e| from_pg(&format!("COPY OUT {table}"), e))?;
    let sink = dst
        .copy_in::<_, Bytes>(&in_sql)
        .await
        .map_err(|e| from_pg(&format!("COPY IN {table}"), e))?;
    let mut out = std::pin::pin!(out);
    let mut sink = std::pin::pin!(sink);
    let mut buf = BytesMut::with_capacity(buffer_size);
    let mut stats = CopyStats::default();

    while let Some(chunk) = out.next().await {
        let chunk = chunk.map_err(|e| from_pg(&format!("COPY OUT {table}"), e))?;
        stats.bytes += chunk.len() as u64;
        metrics.add(&metrics.bytes_copied, chunk.len() as u64);
        buf.extend_from_slice(&chunk);
        if buf.len() >= buffer_size {
            sink.send(buf.split().freeze())
                .await
                .map_err(|e| from_pg(&format!("COPY IN {table}"), e))?;
        }
    }
    if !buf.is_empty() {
        sink.send(buf.split().freeze())
            .await
            .map_err(|e| from_pg(&format!("COPY IN {table}"), e))?;
    }
    stats.rows = sink
        .as_mut()
        .finish()
        .await
        .map_err(|e| from_pg(&format!("COPY IN {table}"), e))?;
    metrics.add(&metrics.rows_copied, stats.rows);
    debug!(%table, rows = stats.rows, bytes = stats.bytes, "table copied");
    Ok(stats)
}
