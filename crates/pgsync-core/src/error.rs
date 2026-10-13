//! Structured, categorised errors.
//!
//! Every runtime failure in PGSync carries a [`ErrorKind`] so the engine can decide
//! whether to retry, stop, or demand operator action without string matching.

use std::fmt;

/// How the engine should react to an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    /// Network, timeout, or server restart. Retry with backoff.
    Transient,
    /// Configuration problem. Stop; operator fixes config.
    Config,
    /// Source does not meet prerequisites (wal_level, privileges, missing publication...).
    Preflight,
    /// A change cannot be applied safely (unsupported type/operation, missing key, DDL).
    /// Stop without acknowledging. Operator intervention required.
    Unsupported,
    /// Replication continuity cannot be proven (slot missing, WAL lost, identity changed).
    /// Job must be resynchronised from a new snapshot.
    ResyncRequired,
    /// Memory or resource budget exceeded. Stop without acknowledging; may be recoverable
    /// after raising limits.
    Resource,
    /// Source and target have diverged (e.g. an UPDATE matched no row). Stop.
    Consistency,
    /// Protocol violation or internal invariant failure. Stop.
    Internal,
    /// Operation cancelled by shutdown request.
    Cancelled,
}

impl ErrorKind {
    pub fn is_retryable(self) -> bool {
        matches!(self, ErrorKind::Transient)
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ErrorKind::Transient => "transient",
            ErrorKind::Config => "config",
            ErrorKind::Preflight => "preflight",
            ErrorKind::Unsupported => "unsupported",
            ErrorKind::ResyncRequired => "resync_required",
            ErrorKind::Resource => "resource",
            ErrorKind::Consistency => "consistency",
            ErrorKind::Internal => "internal",
            ErrorKind::Cancelled => "cancelled",
        };
        f.write_str(s)
    }
}

/// The PGSync error type.
#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    message: String,
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

impl Error {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Error {
            kind,
            message: message.into(),
            source: None,
        }
    }

    pub fn with_source(
        kind: ErrorKind,
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Error {
            kind,
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    pub fn transient(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Transient, message)
    }
    pub fn config(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Config, message)
    }
    pub fn preflight(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Preflight, message)
    }
    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Unsupported, message)
    }
    pub fn resync_required(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::ResyncRequired, message)
    }
    pub fn resource(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Resource, message)
    }
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Internal, message)
    }
    pub fn consistency(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Consistency, message)
    }
    pub fn cancelled() -> Self {
        Self::new(ErrorKind::Cancelled, "operation cancelled")
    }

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn is_retryable(&self) -> bool {
        self.kind.is_retryable()
    }

    /// Prefix the message with context, preserving kind and source.
    pub fn context(mut self, ctx: impl fmt::Display) -> Self {
        self.message = format!("{ctx}: {}", self.message);
        self
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.kind, self.message)?;
        if let Some(src) = &self.source {
            write!(f, ": {src}")?;
        }
        Ok(())
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|e| e as &(dyn std::error::Error + 'static))
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Extension to attach context to results.
pub trait ResultExt<T> {
    fn context(self, ctx: impl fmt::Display) -> Result<T>;
}

impl<T> ResultExt<T> for Result<T> {
    fn context(self, ctx: impl fmt::Display) -> Result<T> {
        self.map_err(|e| e.context(ctx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_includes_kind_and_source() {
        let io = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset by peer");
        let e = Error::with_source(ErrorKind::Transient, "source connection lost", io);
        assert_eq!(
            e.to_string(),
            "[transient] source connection lost: reset by peer"
        );
        assert!(e.is_retryable());
        assert!(std::error::Error::source(&e).is_some());
    }

    #[test]
    fn only_transient_is_retryable() {
        assert!(Error::transient("x").is_retryable());
        for e in [
            Error::config("x"),
            Error::preflight("x"),
            Error::unsupported("x"),
            Error::resync_required("x"),
            Error::resource("x"),
            Error::internal("x"),
            Error::consistency("x"),
            Error::cancelled(),
        ] {
            assert!(!e.is_retryable(), "{e}");
        }
    }

    #[test]
    fn context_prefixes_message_and_keeps_kind() {
        let e = Error::unsupported("no replica identity").context("table public.t");
        assert_eq!(e.kind(), ErrorKind::Unsupported);
        assert_eq!(e.message(), "table public.t: no replica identity");
    }
}
