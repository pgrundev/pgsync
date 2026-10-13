//! TLS behaviour against a real TLS-only PostgreSQL on a non-loopback address
//! (`scripts/tls-lab.sh`). Skipped unless `PGSYNC_TLS_LAB=1`.
//!
//! Environment (printed by `scripts/tls-lab.sh env`): PGSYNC_TLS_CA, PGSYNC_TLS_LAN_IP,
//! PGSYNC_TLS_LAB_DIR (datadirs + certs), PGSYNC_TLS_PG_BIN (for pg_ctl reload).

use pgsync_core::ErrorKind;
use pgsync_core::config::SafetyConfig;
use pgsync_postgres::client;
use pgsync_postgres::replication::conn::ReplicationConnection;
use pgsync_postgres::replication::slot::identify_system;
use pgsync_postgres::url::{ConnectionInfo, TlsPolicy};
use std::time::Duration;

struct Lab {
    ca: String,
    ip: String,
    dir: String,
    pg_bin: String,
}

fn lab() -> Option<Lab> {
    if std::env::var("PGSYNC_TLS_LAB").ok()? != "1" {
        return None;
    }
    Some(Lab {
        ca: std::env::var("PGSYNC_TLS_CA").ok()?,
        ip: std::env::var("PGSYNC_TLS_LAN_IP").ok()?,
        dir: std::env::var("PGSYNC_TLS_LAB_DIR").ok()?,
        pg_bin: std::env::var("PGSYNC_TLS_PG_BIN").unwrap_or_else(|_| "/usr/bin".into()),
    })
}

macro_rules! lab_guard {
    () => {
        match lab() {
            Some(l) => l,
            None => {
                eprintln!("skipped: PGSYNC_TLS_LAB=1 and scripts/tls-lab.sh required");
                return;
            }
        }
    };
}

const T: Duration = Duration::from_secs(10);

/// `unwrap_err` without a `Debug` bound on the success type.
fn expect_err<T>(r: Result<T, pgsync_core::Error>, what: &str) -> pgsync_core::Error {
    match r {
        Ok(_) => panic!("{what}: expected an error, got a connection"),
        Err(e) => e,
    }
}

fn safety(ca: Option<&str>) -> SafetyConfig {
    SafetyConfig {
        tls_ca_file: ca.map(str::to_owned),
        ..SafetyConfig::default()
    }
}

async fn sql_connect(url: &str, s: &SafetyConfig) -> Result<client::PgClient, pgsync_core::Error> {
    let info = ConnectionInfo::parse(url, s)?;
    client::connect(&info, s, T, "tls-test").await
}

async fn repl_connect(
    url: &str,
    s: &SafetyConfig,
) -> Result<ReplicationConnection, pgsync_core::Error> {
    let info = ConnectionInfo::parse(url, s)?;
    ReplicationConnection::connect(&info, s, T).await
}

#[tokio::test]
async fn trusted_certificate_is_accepted_on_sql_and_replication_connections() {
    let l = lab_guard!();
    let s = safety(Some(&l.ca));
    let url = format!("postgres://pgsync:pgsync@{}:5833/app?sslmode=require", l.ip);
    let info = ConnectionInfo::parse(&url, &s).unwrap();
    assert!(!info.is_loopback);
    assert_eq!(info.tls, TlsPolicy::Required { verify: true });
    // SQL connection: encrypted session, SCRAM over TLS.
    let c = sql_connect(&url, &s).await.unwrap();
    let row = c
        .query_one(
            "SELECT ssl, version FROM pg_stat_ssl WHERE pid = pg_backend_pid()",
            &[],
        )
        .await
        .unwrap();
    assert!(row.get::<_, bool>(0));
    assert!(row.get::<_, String>(1).starts_with("TLSv1."));
    // Replication connection: TLS negotiated with the same policy.
    let mut r = repl_connect(&url, &s).await.unwrap();
    let sys = identify_system(&mut r).await.unwrap();
    assert_eq!(sys.dbname.as_deref(), Some("app"));
    r.close().await;
    // The CA may also come from the URL (libpq style), with sslmode=verify-full.
    let url = format!(
        "postgres://pgsync:pgsync@{}:5833/app?sslmode=verify-full&sslrootcert={}",
        l.ip, l.ca
    );
    sql_connect(&url, &safety(None)).await.unwrap();
    repl_connect(&url, &safety(None))
        .await
        .unwrap()
        .close()
        .await;
}

#[tokio::test]
async fn untrusted_issuer_is_rejected_on_both_connection_kinds() {
    let l = lab_guard!();
    let url = format!("postgres://pgsync:pgsync@{}:5833/app?sslmode=require", l.ip);
    // No CA configured: the lab certificate is unknown to the system store.
    let err = expect_err(sql_connect(&url, &safety(None)).await, "sql untrusted");
    assert!(
        err.to_string().to_lowercase().contains("certificate")
            || err.to_string().contains("UnknownIssuer"),
        "{err}"
    );
    let err = expect_err(repl_connect(&url, &safety(None)).await, "repl untrusted");
    assert_eq!(err.kind(), ErrorKind::Config, "{err}");
    assert!(err.to_string().contains("TLS handshake failed"), "{err}");
    // A different CA: still rejected.
    let other = format!("{}/certs/otherca.crt", l.dir);
    assert!(sql_connect(&url, &safety(Some(&other))).await.is_err());
    assert!(repl_connect(&url, &safety(Some(&other))).await.is_err());
}

#[tokio::test]
async fn hostname_mismatch_is_rejected() {
    let l = lab_guard!();
    // Swap the target's certificate for one issued by the same CA but for another name,
    // reload, connect, and restore. The chain is valid; only the name does not match.
    let datadir = format!("{}/target", l.dir);
    let swap = |cert: &str, key: &str| {
        std::fs::copy(
            format!("{}/certs/{cert}", l.dir),
            format!("{datadir}/server.crt"),
        )
        .unwrap();
        std::fs::copy(
            format!("{}/certs/{key}", l.dir),
            format!("{datadir}/server.key"),
        )
        .unwrap();
        let st = std::process::Command::new(format!("{}/pg_ctl", l.pg_bin))
            .args(["-D", &datadir, "reload"])
            .stdout(std::process::Stdio::null())
            .status()
            .unwrap();
        assert!(st.success());
    };
    swap("other.crt", "other.key");
    tokio::time::sleep(Duration::from_millis(500)).await;
    let url = format!(
        "postgres://postgres:postgres@{}:5834/app?sslmode=require",
        l.ip
    );
    let s = safety(Some(&l.ca));
    let sql = sql_connect(&url, &s).await;
    let repl = repl_connect(&url, &s).await;
    swap("server.crt", "server.key");
    let e = sql.err().expect("hostname mismatch must be rejected (SQL)");
    assert!(
        e.to_string().contains("NotValidForName") || e.to_string().to_lowercase().contains("name"),
        "{e}"
    );
    let e = repl
        .err()
        .expect("hostname mismatch must be rejected (replication)");
    assert!(
        e.to_string().contains("NotValidForName") || e.to_string().to_lowercase().contains("name"),
        "{e}"
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    sql_connect(&url, &s)
        .await
        .expect("restored certificate accepted again");
}

#[tokio::test]
async fn plaintext_to_a_remote_host_is_refused_before_connecting() {
    let l = lab_guard!();
    let url = format!("postgres://pgsync:pgsync@{}:5833/app?sslmode=disable", l.ip);
    let err = ConnectionInfo::parse(&url, &safety(Some(&l.ca))).unwrap_err();
    assert!(err.message().contains("sslmode=disable"), "{err}");
    // insecure_skip_tls_verify cannot be used to bypass verification for a remote host.
    let s = SafetyConfig {
        insecure_skip_tls_verify: true,
        ..safety(None)
    };
    let url = format!("postgres://pgsync:pgsync@{}:5833/app", l.ip);
    let err = ConnectionInfo::parse(&url, &s).unwrap_err();
    assert!(err.message().contains("loopback"), "{err}");
}

#[tokio::test]
async fn tls_required_against_a_server_without_tls_fails_clearly() {
    let l = lab_guard!();
    // The plain PG18 pair (scripts/local-pg.sh) listens on loopback without ssl; asking
    // for TLS there must fail instead of silently continuing in clear text.
    let Ok(url) = std::env::var("PGSYNC_TLS_PLAIN_URL") else {
        eprintln!("skipped: PGSYNC_TLS_PLAIN_URL (a loopback server with ssl=off) not set");
        return;
    };
    let url = format!("{url}?sslmode=require");
    let s = safety(Some(&l.ca));
    let err = expect_err(
        repl_connect(&url, &s).await,
        "tls required, server without tls",
    );
    assert!(err.to_string().contains("does not support TLS"), "{err}");
    assert!(sql_connect(&url, &s).await.is_err());
}

#[tokio::test]
async fn reconnect_over_tls_after_the_server_drops_the_session() {
    let l = lab_guard!();
    let s = safety(Some(&l.ca));
    let admin = format!(
        "postgres://postgres:postgres@{}:5833/app?sslmode=require",
        l.ip
    );
    let url = format!("postgres://pgsync:pgsync@{}:5833/app?sslmode=require", l.ip);
    let a = sql_connect(&admin, &s).await.unwrap();
    let mut r = repl_connect(&url, &s).await.unwrap();
    identify_system(&mut r).await.unwrap();
    a.execute(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'pgsync' AND backend_type = 'walsender'",
        &[],
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let err = identify_system(&mut r).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Transient, "{err}");
    // A fresh TLS session works immediately.
    let mut r2 = repl_connect(&url, &s).await.unwrap();
    identify_system(&mut r2).await.unwrap();
    r2.close().await;
}

#[tokio::test]
async fn secrets_never_appear_in_connection_labels() {
    let l = lab_guard!();
    let url = format!(
        "postgres://pgsync:s3cret-pw@{}:5833/app?sslmode=require",
        l.ip
    );
    let info = ConnectionInfo::parse(&url, &safety(Some(&l.ca))).unwrap();
    assert!(!info.redacted().contains("s3cret"));
    assert!(!format!("{info:?}").contains("s3cret"));
    let err = expect_err(
        sql_connect(&url, &safety(Some(&l.ca))).await,
        "wrong password",
    );
    assert!(!err.to_string().contains("s3cret"), "{err}");
}
