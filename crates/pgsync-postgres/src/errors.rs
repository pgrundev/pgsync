//! Mapping of PostgreSQL errors to PGSync error kinds.

use pgsync_core::{Error, ErrorKind};

/// Classify a SQLSTATE code.
pub fn kind_for_sqlstate(code: &str) -> ErrorKind {
    match code {
        // connection exceptions, admin shutdown, cannot connect now, crash shutdown
        c if c.starts_with("08") => ErrorKind::Transient,
        "57P01" | "57P02" | "57P03" | "57P04" | "57014" => ErrorKind::Transient,
        // too many connections / conflicts
        "53300" | "40001" | "40P01" | "55P03" => ErrorKind::Transient,
        // object_in_use: slot already active (stale walsender after our crash) -> retry
        "55006" => ErrorKind::Transient,
        // authentication / authorization
        c if c.starts_with("28") => ErrorKind::Config,
        "42501" => ErrorKind::Preflight,
        // undefined object (slot / publication missing)
        "42704" | "42P01" | "42703" => ErrorKind::ResyncRequired,
        // feature not supported
        "0A000" => ErrorKind::Unsupported,
        // insufficient resources (disk, memory)
        c if c.starts_with("53") => ErrorKind::Resource,
        // data exceptions / integrity violations: divergence or unsupported data
        c if c.starts_with("22") || c.starts_with("23") => ErrorKind::Consistency,
        _ => ErrorKind::Internal,
    }
}

/// Convert a `tokio_postgres::Error` into a categorised [`Error`].
pub fn from_pg(context: &str, e: tokio_postgres::Error) -> Error {
    if let Some(db) = e.as_db_error() {
        let kind = kind_for_sqlstate(db.code().code());
        let mut msg = format!(
            "{context}: {} (SQLSTATE {})",
            db.message(),
            db.code().code()
        );
        if let Some(d) = db.detail() {
            msg.push_str(&format!(": {d}"));
        }
        return Error::new(kind, msg);
    }
    if e.is_closed() {
        return Error::transient(format!("{context}: connection closed"));
    }
    let s = e.to_string();
    // tokio-postgres wraps I/O and timeout errors without a DbError.
    let kind = if s.contains("timed out")
        || s.contains("timeout")
        || s.contains("error communicating")
        || s.contains("connection reset")
        || s.contains("broken pipe")
        || s.contains("connection refused")
        || s.contains("db error: FATAL")
        || s.contains("unexpected EOF")
    {
        ErrorKind::Transient
    } else if s.contains("error connecting")
        || s.contains("Connection refused")
        || s.contains("dns")
    {
        ErrorKind::Transient
    } else {
        ErrorKind::Internal
    };
    Error::with_source(kind, context.to_owned(), e)
}

pub fn from_io(context: &str, e: std::io::Error) -> Error {
    Error::with_source(ErrorKind::Transient, context.to_owned(), e)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_common_sqlstates() {
        assert_eq!(kind_for_sqlstate("08006"), ErrorKind::Transient);
        assert_eq!(kind_for_sqlstate("57P01"), ErrorKind::Transient);
        assert_eq!(kind_for_sqlstate("55006"), ErrorKind::Transient);
        assert_eq!(kind_for_sqlstate("28P01"), ErrorKind::Config);
        assert_eq!(kind_for_sqlstate("42501"), ErrorKind::Preflight);
        assert_eq!(kind_for_sqlstate("42704"), ErrorKind::ResyncRequired);
        assert_eq!(kind_for_sqlstate("23505"), ErrorKind::Consistency);
        assert_eq!(kind_for_sqlstate("22P02"), ErrorKind::Consistency);
        assert_eq!(kind_for_sqlstate("0A000"), ErrorKind::Unsupported);
        assert_eq!(kind_for_sqlstate("53100"), ErrorKind::Resource);
        assert_eq!(kind_for_sqlstate("XX000"), ErrorKind::Internal);
    }
}
