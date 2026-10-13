//! Connection URL handling and TLS policy derivation.

use pgsync_core::config::SafetyConfig;
use pgsync_core::{Error, Result};
use std::str::FromStr;
use tokio_postgres::config::{Host, SslMode};

/// How TLS is negotiated for a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsPolicy {
    /// Plain TCP (loopback or Unix socket only).
    Disabled,
    /// Try TLS, fall back to plain if the server refuses (loopback only).
    Preferred { verify: bool },
    /// TLS is mandatory.
    Required { verify: bool },
}

impl TlsPolicy {
    pub fn uses_tls(self) -> bool {
        !matches!(self, TlsPolicy::Disabled)
    }
    pub fn verify(self) -> bool {
        match self {
            TlsPolicy::Disabled => false,
            TlsPolicy::Preferred { verify } | TlsPolicy::Required { verify } => verify,
        }
    }
}

/// Parsed connection parameters plus the derived TLS policy. Never printed with the
/// password; use [`ConnectionInfo::redacted`] for logs.
#[derive(Clone)]
pub struct ConnectionInfo {
    pub config: tokio_postgres::Config,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub dbname: String,
    pub tls: TlsPolicy,
    pub is_loopback: bool,
    /// `sslrootcert=<path>` from the URL, trusted in addition to `safety.tls_ca_file`.
    pub tls_ca_file: Option<String>,
}

/// libpq parameters that tokio-postgres does not know. Verification modes collapse to
/// `require` because pgsync always verifies the chain and host name for non-loopback hosts;
/// `sslrootcert` is honoured; parameters that would silently weaken or change the
/// connection are refused; the rest are ignored.
struct Normalised {
    url: String,
    tls_ca_file: Option<String>,
}

fn normalise_url(url: &str) -> Result<Normalised> {
    let Ok(mut parsed) = url::Url::parse(url) else {
        // key=value form, or something tokio-postgres will reject with its own message.
        return Ok(Normalised {
            url: url.to_owned(),
            tls_ca_file: None,
        });
    };
    if !matches!(parsed.scheme(), "postgres" | "postgresql") {
        return Ok(Normalised {
            url: url.to_owned(),
            tls_ca_file: None,
        });
    }
    let mut tls_ca_file = None;
    let mut kept: Vec<(String, String)> = Vec::new();
    for (k, v) in parsed.query_pairs() {
        let (k, v) = (k.into_owned(), v.into_owned());
        match k.as_str() {
            "sslmode" => {
                let mapped = match v.as_str() {
                    "verify-full" | "verify-ca" => "require",
                    "allow" => "prefer",
                    other => other,
                };
                kept.push((k, mapped.to_owned()));
            }
            "sslrootcert" => {
                if v != "system" {
                    tls_ca_file = Some(v);
                }
            }
            "sslcert" | "sslkey" | "sslpassword" => {
                return Err(Error::config(format!(
                    "connection URL parameter {k} is not supported: client certificate \
                     authentication is not available in this version of pgsync"
                )));
            }
            "sslcrl" | "sslcrldir" => {
                return Err(Error::config(format!(
                    "connection URL parameter {k} is not supported (certificate revocation \
                     lists are not checked); remove it or use safety.tls_ca_file with a CA \
                     that does not need it"
                )));
            }
            "passfile" | "service" | "requirepeer" => {
                return Err(Error::config(format!(
                    "connection URL parameter {k} is not supported; put the password in the URL \
                     held by the environment variable"
                )));
            }
            // Harmless libpq knobs with no equivalent here.
            "sslcompression"
            | "sslsni"
            | "sslcertmode"
            | "gssencmode"
            | "krbsrvname"
            | "gsslib"
            | "ssl_min_protocol_version"
            | "ssl_max_protocol_version"
            | "client_encoding"
            | "fallback_application_name" => {}
            _ => kept.push((k, v)),
        }
    }
    parsed.set_query(None);
    if !kept.is_empty() {
        let mut q = parsed.query_pairs_mut();
        for (k, v) in &kept {
            q.append_pair(k, v);
        }
    }
    Ok(Normalised {
        url: parsed.to_string(),
        tls_ca_file,
    })
}

impl std::fmt::Debug for ConnectionInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.redacted())
    }
}

impl ConnectionInfo {
    /// Parse a `postgres://` URL (or key=value string) read from the environment variable
    /// `env_name`, and derive the TLS policy from `safety`.
    pub fn from_env(env_name: &str, safety: &SafetyConfig) -> Result<Self> {
        let url = std::env::var(env_name).map_err(|_| {
            Error::config(format!(
                "environment variable {env_name} is not set (connection URL)"
            ))
        })?;
        Self::parse(&url, safety).map_err(|e| e.context(env_name))
    }

    pub fn parse(url: &str, safety: &SafetyConfig) -> Result<Self> {
        let normalised = normalise_url(url)?;
        let config = tokio_postgres::Config::from_str(&normalised.url)
            .map_err(|e| Error::config(format!("invalid connection URL: {e}")))?;
        let hosts = config.get_hosts();
        if hosts.len() != 1 {
            return Err(Error::config(format!(
                "connection URL must name exactly one host (found {})",
                hosts.len()
            )));
        }
        let (host, is_loopback) = match &hosts[0] {
            Host::Tcp(h) => (h.clone(), is_loopback_host(h)),
            #[cfg(unix)]
            Host::Unix(p) => (p.display().to_string(), true),
        };
        let port = config.get_ports().first().copied().unwrap_or(5432);
        let user = config
            .get_user()
            .ok_or_else(|| Error::config("connection URL must include a user"))?
            .to_owned();
        let dbname = config
            .get_dbname()
            .ok_or_else(|| Error::config("connection URL must include a database name"))?
            .to_owned();

        let verify = !safety.insecure_skip_tls_verify;
        let tls = match (config.get_ssl_mode(), is_loopback, safety.require_tls) {
            (SslMode::Disable, false, true) => {
                return Err(Error::config(
                    "sslmode=disable is not allowed for a non-loopback host while safety.require_tls is true",
                ));
            }
            (SslMode::Disable, _, _) => TlsPolicy::Disabled,
            (_, false, true) => TlsPolicy::Required { verify: true },
            (SslMode::Require, _, _) => TlsPolicy::Required { verify },
            (_, true, _) => TlsPolicy::Preferred { verify },
            (_, false, false) => TlsPolicy::Preferred { verify },
        };
        if safety.insecure_skip_tls_verify && !is_loopback {
            return Err(Error::config(
                "safety.insecure_skip_tls_verify is only honoured for loopback hosts",
            ));
        }
        Ok(ConnectionInfo {
            config,
            host,
            port,
            user,
            dbname,
            tls,
            is_loopback,
            tls_ca_file: normalised.tls_ca_file,
        })
    }

    pub fn redacted(&self) -> String {
        format!(
            "postgres://{}@{}:{}/{}",
            self.user, self.host, self.port, self.dbname
        )
    }

    pub fn password(&self) -> Option<&[u8]> {
        self.config.get_password()
    }
}

pub fn is_loopback_host(h: &str) -> bool {
    if h.eq_ignore_ascii_case("localhost") {
        return true;
    }
    match h.parse::<std::net::IpAddr>() {
        Ok(ip) => ip.is_loopback(),
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn safety(require_tls: bool, insecure: bool) -> SafetyConfig {
        SafetyConfig {
            require_tls,
            insecure_skip_tls_verify: insecure,
            ..Default::default()
        }
    }

    #[test]
    fn loopback_hosts_may_skip_tls() {
        let i = ConnectionInfo::parse("postgres://u:p@localhost:5433/db", &safety(true, false))
            .unwrap();
        assert!(i.is_loopback);
        assert_eq!(i.tls, TlsPolicy::Preferred { verify: true });
        assert_eq!(i.redacted(), "postgres://u@localhost:5433/db");
        assert_eq!(i.password(), Some(&b"p"[..]));
        let i = ConnectionInfo::parse(
            "postgres://u@127.0.0.1/db?sslmode=disable",
            &safety(true, false),
        )
        .unwrap();
        assert_eq!(i.tls, TlsPolicy::Disabled);
    }

    #[test]
    fn remote_hosts_require_verified_tls() {
        let i = ConnectionInfo::parse("postgres://u:p@db.example.com/db", &safety(true, false))
            .unwrap();
        assert!(!i.is_loopback);
        assert_eq!(i.tls, TlsPolicy::Required { verify: true });
        let err = ConnectionInfo::parse(
            "postgres://u:p@db.example.com/db?sslmode=disable",
            &safety(true, false),
        )
        .unwrap_err();
        assert!(err.message().contains("sslmode=disable"));
        let err = ConnectionInfo::parse("postgres://u:p@db.example.com/db", &safety(true, true))
            .unwrap_err();
        assert!(err.message().contains("loopback"));
    }

    #[test]
    fn insecure_override_only_on_loopback() {
        let i = ConnectionInfo::parse(
            "postgres://u:p@localhost/db?sslmode=require",
            &safety(true, true),
        )
        .unwrap();
        assert_eq!(i.tls, TlsPolicy::Required { verify: false });
    }

    #[test]
    fn libpq_tls_parameters_are_normalised() {
        // verify-full / verify-ca collapse to require (pgsync always verifies remote hosts).
        let i = ConnectionInfo::parse(
            "postgres://u:p@db.example.com:5432/db?sslmode=verify-full&sslrootcert=/etc/ca.pem&sslcompression=0",
            &safety(true, false),
        )
        .unwrap();
        assert_eq!(i.tls, TlsPolicy::Required { verify: true });
        assert_eq!(i.tls_ca_file.as_deref(), Some("/etc/ca.pem"));
        let i = ConnectionInfo::parse(
            "postgresql://u:p@db.example.com/db?sslmode=verify-ca&sslrootcert=system&application_name=x",
            &safety(true, false),
        )
        .unwrap();
        assert_eq!(i.tls, TlsPolicy::Required { verify: true });
        assert_eq!(i.tls_ca_file, None);
        assert_eq!(i.config.get_application_name(), Some("x"));
        // Parameters that would change the security model are refused, not ignored.
        for p in [
            "sslcert=/c.pem",
            "sslkey=/k.pem",
            "sslcrl=/crl.pem",
            "passfile=/p",
        ] {
            let err = ConnectionInfo::parse(
                &format!("postgres://u:p@db.example.com/db?{p}"),
                &safety(true, false),
            )
            .unwrap_err();
            assert!(err.message().contains("not supported"), "{p}: {err}");
        }
        // Unknown parameters still fail loudly.
        assert!(
            ConnectionInfo::parse("postgres://u:p@localhost/db?bogus=1", &safety(true, false))
                .is_err()
        );
    }

    #[test]
    fn rejects_incomplete_urls() {
        assert!(ConnectionInfo::parse("postgres://localhost/db", &safety(true, false)).is_err());
        assert!(ConnectionInfo::parse("postgres://u@localhost", &safety(true, false)).is_err());
        assert!(ConnectionInfo::parse("not a url", &safety(true, false)).is_err());
    }
}
