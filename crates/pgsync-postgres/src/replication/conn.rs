//! Minimal PostgreSQL wire-protocol client for the replication connection.
//!
//! The published `tokio-postgres` cannot open a `replication=database` session, so this
//! module implements just enough of the protocol: startup, authentication
//! (trust / cleartext / MD5 / SCRAM-SHA-256), simple queries, and the CopyBoth stream used
//! by `START_REPLICATION`. Message encoding/decoding is delegated to `postgres-protocol`.

use crate::errors::{from_io, kind_for_sqlstate};
use crate::tls;
use crate::url::{ConnectionInfo, TlsPolicy};
use bytes::{Buf, Bytes, BytesMut};
use fallible_iterator::FallibleIterator;
use pgsync_core::config::SafetyConfig;
use pgsync_core::{Error, ErrorKind, Result};
use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256};
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use std::pin::Pin;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::debug;

type Io = Pin<Box<dyn AsyncReadWrite + Send>>;

pub trait AsyncReadWrite: AsyncRead + AsyncWrite {}
impl<T: AsyncRead + AsyncWrite> AsyncReadWrite for T {}

const COPY_BOTH_RESPONSE_TAG: u8 = b'W';

/// A decoded simple-query result set.
#[derive(Debug, Default, Clone)]
pub struct SimpleResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
}

impl SimpleResult {
    pub fn get(&self, row: usize, col: &str) -> Option<&str> {
        let idx = self.columns.iter().position(|c| c == col)?;
        self.rows.get(row)?.get(idx)?.as_deref()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Idle,
    CopyBoth,
    Closed,
}

pub struct ReplicationConnection {
    io: Io,
    read_buf: BytesMut,
    write_buf: BytesMut,
    mode: Mode,
    label: String,
    pub server_version: Option<String>,
    /// Largest backend message accepted (`resources.max_message_bytes`). Enforced before
    /// the message is buffered, so memory never silently exceeds the budget.
    max_message_bytes: usize,
}

enum Backend {
    CopyBothResponse,
    Msg(Message),
}

impl ReplicationConnection {
    /// Open a logical-replication connection to `info.dbname`.
    pub async fn connect(
        info: &ConnectionInfo,
        safety: &SafetyConfig,
        timeout: Duration,
    ) -> Result<Self> {
        let label = info.redacted();
        let fut = Self::connect_inner(info, safety, &label);
        tokio::time::timeout(timeout, fut).await.map_err(|_| {
            Error::transient(format!(
                "{label}: replication connection timed out after {timeout:?}"
            ))
        })?
    }

    async fn connect_inner(
        info: &ConnectionInfo,
        safety: &SafetyConfig,
        label: &str,
    ) -> Result<Self> {
        let addr = format!("{}:{}", info.host, info.port);
        let mut tcp = TcpStream::connect(&addr)
            .await
            .map_err(|e| from_io(&format!("{label}: connect"), e))?;
        tcp.set_nodelay(true).ok();

        let io: Io = if info.tls.uses_tls() {
            let mut buf = BytesMut::new();
            frontend::ssl_request(&mut buf);
            tcp.write_all(&buf).await.map_err(|e| from_io(label, e))?;
            let mut answer = [0u8; 1];
            tcp.read_exact(&mut answer)
                .await
                .map_err(|e| from_io(label, e))?;
            match (answer[0], info.tls) {
                (b'S', _) => {
                    let config = tls::client_config(info.tls, safety, info.tls_ca_file.as_deref())?;
                    let connector = tokio_rustls::TlsConnector::from(config);
                    let name = tls::server_name(&info.host)?;
                    let stream = connector.connect(name, tcp).await.map_err(|e| {
                        Error::with_source(
                            ErrorKind::Config,
                            format!("{label}: TLS handshake failed"),
                            e,
                        )
                    })?;
                    Box::pin(stream)
                }
                (b'N', TlsPolicy::Required { .. }) => {
                    return Err(Error::config(format!(
                        "{label}: server does not support TLS but TLS is required"
                    )));
                }
                (b'N', _) => {
                    debug!(
                        target = label,
                        "server declined TLS; continuing in plain text (loopback)"
                    );
                    Box::pin(tcp)
                }
                (other, _) => {
                    return Err(Error::internal(format!(
                        "{label}: unexpected SSL negotiation byte {other:#x}"
                    )));
                }
            }
        } else {
            Box::pin(tcp)
        };

        let mut conn = ReplicationConnection {
            io,
            read_buf: BytesMut::with_capacity(64 * 1024),
            write_buf: BytesMut::with_capacity(8 * 1024),
            mode: Mode::Idle,
            label: label.to_owned(),
            server_version: None,
            max_message_bytes: usize::MAX,
        };
        conn.startup(info).await?;
        Ok(conn)
    }

    async fn startup(&mut self, info: &ConnectionInfo) -> Result<()> {
        let params = [
            ("user", info.user.as_str()),
            ("database", info.dbname.as_str()),
            ("replication", "database"),
            ("application_name", "pgsync"),
            ("client_encoding", "UTF8"),
        ];
        frontend::startup_message(params, &mut self.write_buf)
            .map_err(|e| from_io(&self.label, e))?;
        self.flush().await?;

        let password = info.password().map(|p| p.to_vec());
        loop {
            match self.read_backend().await? {
                Backend::Msg(Message::AuthenticationOk) => break,
                Backend::Msg(Message::AuthenticationCleartextPassword) => {
                    let pw = password.as_deref().ok_or_else(Self::need_password)?;
                    frontend::password_message(pw, &mut self.write_buf)
                        .map_err(|e| from_io(&self.label, e))?;
                    self.flush().await?;
                }
                Backend::Msg(Message::AuthenticationMd5Password(body)) => {
                    let pw = password.as_deref().ok_or_else(Self::need_password)?;
                    let hashed = postgres_protocol::authentication::md5_hash(
                        info.user.as_bytes(),
                        pw,
                        body.salt(),
                    );
                    frontend::password_message(hashed.as_bytes(), &mut self.write_buf)
                        .map_err(|e| from_io(&self.label, e))?;
                    self.flush().await?;
                }
                Backend::Msg(Message::AuthenticationSasl(body)) => {
                    let pw = password.as_deref().ok_or_else(Self::need_password)?;
                    let mut mechanisms = body.mechanisms();
                    let mut has_scram = false;
                    while let Some(m) = mechanisms.next().map_err(|e| from_io(&self.label, e))? {
                        if m == "SCRAM-SHA-256" {
                            has_scram = true;
                        }
                    }
                    if !has_scram {
                        return Err(Error::config(format!(
                            "{}: server offers no supported SASL mechanism",
                            self.label
                        )));
                    }
                    // Channel binding is declared unsupported ("n,,"); the server accepts this
                    // unless it is configured to require it.
                    let mut scram = ScramSha256::new(pw, ChannelBinding::unsupported());
                    frontend::sasl_initial_response(
                        "SCRAM-SHA-256",
                        scram.message(),
                        &mut self.write_buf,
                    )
                    .map_err(|e| from_io(&self.label, e))?;
                    self.flush().await?;
                    match self.read_backend().await? {
                        Backend::Msg(Message::AuthenticationSaslContinue(body)) => {
                            scram.update(body.data()).map_err(|e| {
                                Error::with_source(
                                    ErrorKind::Config,
                                    format!("{}: SCRAM failed", self.label),
                                    e,
                                )
                            })?;
                        }
                        other => return Err(self.unexpected(other, "AuthenticationSASLContinue")),
                    }
                    frontend::sasl_response(scram.message(), &mut self.write_buf)
                        .map_err(|e| from_io(&self.label, e))?;
                    self.flush().await?;
                    match self.read_backend().await? {
                        Backend::Msg(Message::AuthenticationSaslFinal(body)) => {
                            scram.finish(body.data()).map_err(|e| {
                                Error::with_source(
                                    ErrorKind::Config,
                                    format!("{}: SCRAM verification failed", self.label),
                                    e,
                                )
                            })?;
                        }
                        other => return Err(self.unexpected(other, "AuthenticationSASLFinal")),
                    }
                }
                Backend::Msg(Message::ErrorResponse(body)) => {
                    return Err(self.db_error(body.fields()));
                }
                other => return Err(self.unexpected(other, "authentication")),
            }
        }

        loop {
            match self.read_backend().await? {
                Backend::Msg(Message::ParameterStatus(body)) => {
                    if body.name().map_err(|e| from_io(&self.label, e))? == "server_version" {
                        self.server_version = Some(
                            body.value()
                                .map_err(|e| from_io(&self.label, e))?
                                .to_owned(),
                        );
                    }
                }
                Backend::Msg(Message::BackendKeyData(_)) => {}
                Backend::Msg(Message::NoticeResponse(_)) => {}
                Backend::Msg(Message::ReadyForQuery(_)) => return Ok(()),
                Backend::Msg(Message::ErrorResponse(body)) => {
                    return Err(self.db_error(body.fields()));
                }
                other => return Err(self.unexpected(other, "ReadyForQuery")),
            }
        }
    }

    /// Refuse backend messages larger than `bytes` (see `resources.max_message_bytes`).
    pub fn set_max_message_bytes(&mut self, bytes: usize) {
        self.max_message_bytes = bytes.max(1024);
    }

    fn need_password() -> Error {
        Error::config("server requested a password but the connection URL has none")
    }

    /// Run a simple-query command (replication commands or SQL) and collect the result.
    pub async fn simple_query(&mut self, sql: &str) -> Result<SimpleResult> {
        self.ensure_idle()?;
        frontend::query(sql, &mut self.write_buf).map_err(|e| from_io(&self.label, e))?;
        self.flush().await?;
        let mut result = SimpleResult::default();
        let mut error: Option<Error> = None;
        loop {
            match self.read_backend().await? {
                Backend::Msg(Message::RowDescription(body)) => {
                    let mut fields = body.fields();
                    result.columns.clear();
                    while let Some(f) = fields.next().map_err(|e| from_io(&self.label, e))? {
                        result.columns.push(f.name().to_owned());
                    }
                }
                Backend::Msg(Message::DataRow(body)) => {
                    let mut row = Vec::with_capacity(result.columns.len());
                    let buf = body.buffer();
                    let mut ranges = body.ranges();
                    while let Some(r) = ranges.next().map_err(|e| from_io(&self.label, e))? {
                        row.push(r.map(|r| String::from_utf8_lossy(&buf[r]).into_owned()));
                    }
                    result.rows.push(row);
                }
                Backend::Msg(Message::CommandComplete(_))
                | Backend::Msg(Message::EmptyQueryResponse) => {}
                Backend::Msg(Message::NoticeResponse(body)) => {
                    self.log_notice(body.fields());
                }
                Backend::Msg(Message::ErrorResponse(body)) => {
                    error = Some(self.db_error(body.fields()));
                }
                Backend::Msg(Message::ReadyForQuery(_)) => break,
                Backend::CopyBothResponse => {
                    return Err(Error::internal(format!(
                        "{}: unexpected CopyBothResponse to {sql:?}",
                        self.label
                    )));
                }
                other => return Err(self.unexpected(other, "simple query response")),
            }
        }
        match error {
            Some(e) => Err(e),
            None => Ok(result),
        }
    }

    /// Issue `START_REPLICATION ...` and switch to CopyBoth mode.
    pub async fn start_replication(&mut self, command: &str) -> Result<()> {
        self.ensure_idle()?;
        frontend::query(command, &mut self.write_buf).map_err(|e| from_io(&self.label, e))?;
        self.flush().await?;
        loop {
            match self.read_backend().await? {
                Backend::CopyBothResponse => {
                    self.mode = Mode::CopyBoth;
                    return Ok(());
                }
                Backend::Msg(Message::NoticeResponse(body)) => self.log_notice(body.fields()),
                Backend::Msg(Message::ErrorResponse(body)) => {
                    let err = self.db_error(body.fields());
                    // Consume the trailing ReadyForQuery so the connection stays usable.
                    loop {
                        if let Backend::Msg(Message::ReadyForQuery(_)) = self.read_backend().await?
                        {
                            break;
                        }
                    }
                    return Err(err);
                }
                other => return Err(self.unexpected(other, "CopyBothResponse")),
            }
        }
    }

    /// Next CopyData payload from the server. Cancel-safe: a partially received frame
    /// stays buffered. Returns an error when the stream ends.
    pub async fn next_copy_data(&mut self) -> Result<Bytes> {
        if self.mode != Mode::CopyBoth {
            return Err(Error::internal(format!(
                "{}: not in CopyBoth mode",
                self.label
            )));
        }
        loop {
            match self.read_backend().await? {
                Backend::Msg(Message::CopyData(body)) => {
                    return Ok(body.into_bytes());
                }
                // A shutting-down logical walsender ends the stream with CommandComplete
                // (no CopyDone); a physical one sends CopyDone first. Both mean "reconnect".
                Backend::Msg(Message::CopyDone)
                | Backend::Msg(Message::CommandComplete(_))
                | Backend::Msg(Message::ReadyForQuery(_)) => {
                    self.mode = Mode::Closed;
                    return Err(Error::transient(format!(
                        "{}: server ended the replication stream (shutdown or timeline change)",
                        self.label
                    )));
                }
                Backend::Msg(Message::NoticeResponse(body)) => self.log_notice(body.fields()),
                Backend::Msg(Message::ErrorResponse(body)) => {
                    self.mode = Mode::Closed;
                    return Err(self.db_error(body.fields()));
                }
                other => return Err(self.unexpected(other, "CopyData")),
            }
        }
    }

    /// Send a CopyData frame (standby status update) to the server.
    pub async fn send_copy_data(&mut self, payload: &[u8]) -> Result<()> {
        if self.mode != Mode::CopyBoth {
            return Err(Error::internal(format!(
                "{}: not in CopyBoth mode",
                self.label
            )));
        }
        let msg = frontend::CopyData::new(payload).map_err(|e| from_io(&self.label, e))?;
        msg.write(&mut self.write_buf);
        self.flush().await
    }

    fn ensure_idle(&self) -> Result<()> {
        match self.mode {
            Mode::Idle => Ok(()),
            Mode::CopyBoth => Err(Error::internal(format!(
                "{}: connection is streaming",
                self.label
            ))),
            Mode::Closed => Err(Error::transient(format!(
                "{}: connection closed",
                self.label
            ))),
        }
    }

    /// Cancel-safe: bytes not yet written stay in `write_buf` and are sent by the next call.
    async fn flush(&mut self) -> Result<()> {
        while !self.write_buf.is_empty() {
            let n = self
                .io
                .write(&self.write_buf)
                .await
                .map_err(|e| from_io(&self.label, e))?;
            if n == 0 {
                return Err(Error::transient(format!(
                    "{}: write returned 0 bytes",
                    self.label
                )));
            }
            self.write_buf.advance(n);
        }
        self.io.flush().await.map_err(|e| from_io(&self.label, e))
    }

    async fn read_backend(&mut self) -> Result<Backend> {
        loop {
            if self.read_buf.len() >= 5 {
                let tag = self.read_buf[0];
                let len = u32::from_be_bytes([
                    self.read_buf[1],
                    self.read_buf[2],
                    self.read_buf[3],
                    self.read_buf[4],
                ]) as usize;
                if len < 4 {
                    return Err(Error::internal(format!(
                        "{}: invalid message length {len}",
                        self.label
                    )));
                }
                if len + 1 > self.max_message_bytes {
                    self.mode = Mode::Closed;
                    return Err(Error::resource(format!(
                        "{}: server sent a {} byte message, above resources.max_message_bytes ({}); \
                         nothing was acknowledged. Raise the limit (and memory_budget_mb if needed) \
                         and `run` to apply the transaction",
                        self.label,
                        len + 1,
                        self.max_message_bytes
                    )));
                }
                if self.read_buf.len() > len {
                    if tag == COPY_BOTH_RESPONSE_TAG {
                        self.read_buf.advance(len + 1);
                        return Ok(Backend::CopyBothResponse);
                    }
                    let msg = Message::parse(&mut self.read_buf).map_err(|e| {
                        Error::with_source(
                            ErrorKind::Internal,
                            format!("{}: protocol error", self.label),
                            e,
                        )
                    })?;
                    if let Some(m) = msg {
                        return Ok(Backend::Msg(m));
                    }
                    return Err(Error::internal(format!(
                        "{}: parser consumed nothing",
                        self.label
                    )));
                }
                self.read_buf.reserve(len + 1 - self.read_buf.len());
            } else {
                self.read_buf.reserve(64 * 1024);
            }
            let n = self
                .io
                .read_buf(&mut self.read_buf)
                .await
                .map_err(|e| from_io(&self.label, e))?;
            if n == 0 {
                self.mode = Mode::Closed;
                return Err(Error::transient(format!(
                    "{}: connection closed by server",
                    self.label
                )));
            }
        }
    }

    fn db_error(&self, fields: postgres_protocol::message::backend::ErrorFields<'_>) -> Error {
        let (code, message, detail) = collect_error_fields(fields);
        let kind = stream_error_kind(&code, &message);
        let mut msg = format!("{}: {message} (SQLSTATE {code})", self.label);
        if let Some(d) = detail {
            msg.push_str(&format!(": {d}"));
        }
        Error::new(kind, msg)
    }

    fn log_notice(&self, fields: postgres_protocol::message::backend::ErrorFields<'_>) {
        let (code, message, _) = collect_error_fields(fields);
        debug!(target = %self.label, code, "server notice: {message}");
    }

    fn unexpected(&self, got: Backend, expected: &str) -> Error {
        let what = match got {
            Backend::CopyBothResponse => "CopyBothResponse".to_owned(),
            Backend::Msg(m) => message_name(&m).to_owned(),
        };
        Error::internal(format!("{}: expected {expected}, got {what}", self.label))
    }

    pub async fn close(mut self) {
        let mut buf = BytesMut::new();
        frontend::terminate(&mut buf);
        let _ = self.io.write_all(&buf).await;
        let _ = self.io.shutdown().await;
    }
}

/// Errors on the replication connection that mean the slot can no longer deliver every
/// transaction: the slot was invalidated (`max_slot_wal_keep_size`) or its WAL is gone.
/// Both need a resynchronisation, not a retry.
pub fn stream_error_kind(code: &str, message: &str) -> ErrorKind {
    match code {
        // object_not_in_prerequisite_state: "can no longer get changes from replication
        // slot ... This slot has been invalidated because ..."
        "55000" if message.contains("replication slot") || message.contains("invalidated") => {
            ErrorKind::ResyncRequired
        }
        // undefined_file: "requested WAL segment ... has already been removed"
        "58P01" => ErrorKind::ResyncRequired,
        _ => kind_for_sqlstate(code),
    }
}

fn collect_error_fields(
    mut fields: postgres_protocol::message::backend::ErrorFields<'_>,
) -> (String, String, Option<String>) {
    let mut code = String::from("XX000");
    let mut message = String::from("unknown error");
    let mut detail = None;
    while let Ok(Some(f)) = fields.next() {
        match f.type_() {
            b'C' => code = String::from_utf8_lossy(f.value_bytes()).into_owned(),
            b'M' => message = String::from_utf8_lossy(f.value_bytes()).into_owned(),
            b'D' => detail = Some(String::from_utf8_lossy(f.value_bytes()).into_owned()),
            _ => {}
        }
    }
    (code, message, detail)
}

fn message_name(m: &Message) -> &'static str {
    match m {
        Message::AuthenticationOk => "AuthenticationOk",
        Message::AuthenticationCleartextPassword => "AuthenticationCleartextPassword",
        Message::AuthenticationMd5Password(_) => "AuthenticationMD5Password",
        Message::AuthenticationSasl(_) => "AuthenticationSASL",
        Message::AuthenticationSaslContinue(_) => "AuthenticationSASLContinue",
        Message::AuthenticationSaslFinal(_) => "AuthenticationSASLFinal",
        Message::BackendKeyData(_) => "BackendKeyData",
        Message::CommandComplete(_) => "CommandComplete",
        Message::CopyData(_) => "CopyData",
        Message::CopyDone => "CopyDone",
        Message::CopyInResponse(_) => "CopyInResponse",
        Message::CopyOutResponse(_) => "CopyOutResponse",
        Message::DataRow(_) => "DataRow",
        Message::EmptyQueryResponse => "EmptyQueryResponse",
        Message::ErrorResponse(_) => "ErrorResponse",
        Message::NoticeResponse(_) => "NoticeResponse",
        Message::ParameterStatus(_) => "ParameterStatus",
        Message::ReadyForQuery(_) => "ReadyForQuery",
        Message::RowDescription(_) => "RowDescription",
        _ => "other",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalidated_slot_and_removed_wal_need_resync() {
        assert_eq!(
            stream_error_kind(
                "55000",
                "can no longer get changes from replication slot \"s\""
            ),
            ErrorKind::ResyncRequired
        );
        assert_eq!(
            stream_error_kind(
                "58P01",
                "requested WAL segment 000000010000000000000003 has already been removed"
            ),
            ErrorKind::ResyncRequired
        );
        assert_eq!(
            stream_error_kind("55006", "replication slot \"s\" is active for PID 1"),
            ErrorKind::Transient
        );
        assert_eq!(
            stream_error_kind("55000", "something else"),
            ErrorKind::Internal
        );
        assert_eq!(
            stream_error_kind("57P01", "terminating connection"),
            ErrorKind::Transient
        );
    }
}
