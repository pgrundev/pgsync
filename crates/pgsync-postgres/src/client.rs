//! Ordinary (non-replication) connections through tokio-postgres.

use crate::errors::from_pg;
use crate::tls;
use crate::url::{ConnectionInfo, TlsPolicy};
use pgsync_core::config::SafetyConfig;
use pgsync_core::{Error, Result};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_postgres::Client;
use tracing::debug;

/// A client plus the task driving its socket. Dropping it closes the connection.
pub struct PgClient {
    pub client: Client,
    task: JoinHandle<()>,
    pub info: ConnectionInfo,
}

impl Drop for PgClient {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl std::ops::Deref for PgClient {
    type Target = Client;
    fn deref(&self) -> &Client {
        &self.client
    }
}

pub async fn connect(
    info: &ConnectionInfo,
    safety: &SafetyConfig,
    timeout: Duration,
    application_name: &str,
) -> Result<PgClient> {
    let mut config = info.config.clone();
    config.application_name(application_name);
    config.connect_timeout(timeout);
    let label = info.redacted();

    let fut = async {
        match info.tls {
            TlsPolicy::Disabled => {
                config.ssl_mode(tokio_postgres::config::SslMode::Disable);
                let (client, conn) = config
                    .connect(tokio_postgres::NoTls)
                    .await
                    .map_err(|e| from_pg(&label, e))?;
                Ok::<_, Error>((client, tokio::spawn(drive(conn))))
            }
            TlsPolicy::Preferred { .. } | TlsPolicy::Required { .. } => {
                config.ssl_mode(match info.tls {
                    TlsPolicy::Required { .. } => tokio_postgres::config::SslMode::Require,
                    _ => tokio_postgres::config::SslMode::Prefer,
                });
                let tls_config = tls::client_config(info.tls, safety, info.tls_ca_file.as_deref())?;
                let connector =
                    tokio_postgres_rustls::MakeRustlsConnect::new((*tls_config).clone());
                let (client, conn) = config
                    .connect(connector)
                    .await
                    .map_err(|e| from_pg(&label, e))?;
                Ok((client, tokio::spawn(drive(conn))))
            }
        }
    };
    let (client, task) = tokio::time::timeout(timeout + Duration::from_secs(1), fut)
        .await
        .map_err(|_| {
            Error::transient(format!("{label}: connection timed out after {timeout:?}"))
        })??;
    debug!(target = %label, "connected");
    Ok(PgClient {
        client,
        task,
        info: info.clone(),
    })
}

async fn drive<S, T>(conn: tokio_postgres::Connection<S, T>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    T: tokio_postgres::tls::TlsStream + Unpin + Send + 'static,
{
    if let Err(e) = conn.await {
        debug!(error = %e, "connection task ended");
    }
}
