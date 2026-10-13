//! End-to-end harness: drives the real `pgsync` binary against the Docker Compose
//! source/target pair. Each test gets its own schema, publication, slot and job.
//!
//! Required environment (defaults match docker-compose.yml):
//!   PGSYNC_E2E=1                       enable the suite
//!   PGSYNC_E2E_SOURCE_ADMIN_URL        superuser on the source
//!   PGSYNC_E2E_SOURCE_URL              replication role on the source
//!   PGSYNC_E2E_TARGET_URL              superuser on the target
#![allow(dead_code)]

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use tokio_postgres::{Client, NoTls};

pub const DEFAULT_SOURCE_ADMIN: &str = "postgres://postgres:postgres@localhost:5433/app";
pub const DEFAULT_SOURCE: &str = "postgres://pgsync:pgsync@localhost:5433/app";
pub const DEFAULT_TARGET: &str = "postgres://postgres:postgres@localhost:5434/app";

pub fn enabled() -> bool {
    std::env::var("PGSYNC_E2E")
        .map(|v| v == "1")
        .unwrap_or(false)
}

/// Skip the test unless the suite is enabled.
#[macro_export]
macro_rules! e2e_guard {
    () => {
        if !$crate::common::enabled() {
            eprintln!("skipped: set PGSYNC_E2E=1 with docker compose running");
            return;
        }
    };
}

pub async fn connect(url: &str) -> Client {
    let (client, conn) = tokio_postgres::connect(url, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
}

pub fn env_url(var: &str, default: &str) -> String {
    std::env::var(var).unwrap_or_else(|_| default.to_owned())
}

pub struct Job {
    pub name: String,
    pub schema: String,
    pub publication: String,
    pub slot: String,
    pub job_id: String,
    pub config_path: PathBuf,
    pub log_dir: PathBuf,
    pub src: Client,
    pub tgt: Client,
    pub source_url: String,
    pub target_url: String,
    pub source_admin_url: String,
    pub log_seq: usize,
}

impl Job {
    /// Create an isolated job: schema `e2e_<name>` on the source, publication, config.
    /// `resources_extra` / `safety_extra` are extra YAML lines (2-space indented) appended
    /// to the respective sections.
    pub async fn new(
        name: &str,
        tables_ddl: &str,
        tables: &[&str],
        resources_extra: &str,
        safety_extra: &str,
    ) -> Job {
        let source_admin_url = env_url("PGSYNC_E2E_SOURCE_ADMIN_URL", DEFAULT_SOURCE_ADMIN);
        let source_url = env_url("PGSYNC_E2E_SOURCE_URL", DEFAULT_SOURCE);
        let target_url = env_url("PGSYNC_E2E_TARGET_URL", DEFAULT_TARGET);
        let src = connect(&source_admin_url).await;
        let tgt = connect(&target_url).await;
        let schema = format!("e2e_{name}");
        let publication = format!("pub_{name}");
        let slot = format!("slot_{name}");
        let job_id = format!("e2e-{name}");
        let dir = std::env::var("PGSYNC_E2E_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir().join("pgsync-e2e"));
        std::fs::create_dir_all(&dir).unwrap();
        let config_path = dir.join(format!("{name}.yaml"));

        // Clean leftovers from a previous run.
        let _ = src
            .batch_execute(&format!("DROP PUBLICATION IF EXISTS {publication}"))
            .await;
        let _ = src.execute("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1 AND NOT active", &[&slot]).await;
        let _ = src
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .await;
        let _ = tgt
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .await;
        let _ = tgt
            .execute("DELETE FROM pgsync.job WHERE job_id = $1", &[&job_id])
            .await;

        src.batch_execute(&format!(
            "CREATE SCHEMA {schema}; SET search_path = {schema}; {tables_ddl}"
        ))
        .await
        .expect("create tables");
        src.batch_execute(&format!(
            "GRANT USAGE ON SCHEMA {schema} TO pgsync; GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO pgsync; \
             ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT SELECT ON TABLES TO pgsync;"
        ))
        .await
        .unwrap();
        let qualified: Vec<String> = tables.iter().map(|t| format!("{schema}.{t}")).collect();
        src.batch_execute(&format!(
            "CREATE PUBLICATION {publication} FOR TABLE {}",
            qualified.join(", ")
        ))
        .await
        .unwrap();

        let job = Job {
            name: name.to_owned(),
            schema,
            publication,
            slot,
            job_id,
            config_path,
            log_dir: dir,
            src,
            tgt,
            source_url,
            target_url,
            source_admin_url,
            log_seq: 0,
        };
        job.write_config(tables, resources_extra, safety_extra);
        job
    }

    pub fn write_config(&self, tables: &[&str], resources_extra: &str, safety_extra: &str) {
        let tables_yaml: Vec<String> = tables
            .iter()
            .map(|t| format!("    - {}.{t}", self.schema))
            .collect();
        let yaml = format!(
            "job:\n  id: {}\nsource:\n  url_env: E2E_SOURCE_URL\n  publication: {}\n  slot: {}\ntarget:\n  url_env: E2E_TARGET_URL\nreplication:\n  tables:\n{}\nresources:\n  connection_timeout: 5s\n  feedback_interval: 1s\n  retry_backoff:\n    initial: 200ms\n    max: 2s\n{}\nsafety:\n  monitor_interval: 2s\n{}\n",
            self.job_id,
            self.publication,
            self.slot,
            tables_yaml.join("\n"),
            resources_extra,
            safety_extra
        );
        std::fs::write(&self.config_path, yaml).unwrap();
    }

    fn command(&mut self, args: &[&str]) -> (Command, PathBuf) {
        self.log_seq += 1;
        let log = self
            .log_dir
            .join(format!("{}-{}-{}.log", self.name, self.log_seq, args[0]));
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_pgsync"));
        cmd.arg("--config")
            .arg(&self.config_path)
            .args(args)
            .env("E2E_SOURCE_URL", &self.source_url)
            .env("E2E_TARGET_URL", &self.target_url)
            .env("PGSYNC_LOG", "info")
            .stdin(Stdio::null())
            .stdout(std::fs::File::create(&log).unwrap())
            .stderr(std::fs::File::options().append(true).open(&log).unwrap());
        (cmd, log)
    }

    /// Start a long-running command (bootstrap / run).
    pub fn spawn(&mut self, args: &[&str]) -> Proc {
        let (mut cmd, log) = self.command(args);
        let child = cmd.spawn().expect("spawn pgsync");
        Proc { child, log }
    }

    /// Run a command to completion and return (exit code, combined output).
    pub fn run(&mut self, args: &[&str]) -> (i32, String) {
        let (mut cmd, log) = self.command(args);
        let status = cmd.status().expect("run pgsync");
        let out = std::fs::read_to_string(&log).unwrap_or_default();
        (status.code().unwrap_or(-1), out)
    }

    pub async fn job_state(&self) -> Option<String> {
        // A fresh target has no pgsync schema until the first command creates it.
        match self
            .tgt
            .query_opt(
                "SELECT state FROM pgsync.job WHERE job_id = $1",
                &[&self.job_id],
            )
            .await
        {
            Ok(row) => row.map(|r| r.get(0)),
            Err(e) if e.code() == Some(&tokio_postgres::error::SqlState::UNDEFINED_TABLE) => None,
            Err(e) => panic!("job_state: {e}"),
        }
    }

    pub async fn job_row(&self) -> Option<(String, i64, String, String, Option<String>)> {
        self.tgt
            .query_opt(
                "SELECT state, txns_applied, committed_lsn::text, acked_lsn::text, last_error FROM pgsync.job WHERE job_id = $1",
                &[&self.job_id],
            )
            .await
            .unwrap()
            .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4)))
    }

    pub async fn wait_state(&self, state: &str, timeout: Duration) {
        let start = Instant::now();
        loop {
            if self.job_state().await.as_deref() == Some(state) {
                return;
            }
            if start.elapsed() > timeout {
                panic!(
                    "job {} did not reach {state} within {timeout:?} (now {:?})",
                    self.job_id,
                    self.job_state().await
                );
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Order-independent content hash of a table (all columns, text form).
    pub async fn hash(client: &Client, schema: &str, table: &str, order_by: &str) -> String {
        let sql = format!(
            "SELECT coalesce(md5(string_agg(t::text, '|' ORDER BY {order_by})), 'empty') || ':' || count(*) FROM {schema}.{table} t"
        );
        client.query_one(&sql, &[]).await.expect("hash").get(0)
    }

    pub async fn hashes(&self, tables: &[(&str, &str)]) -> (Vec<String>, Vec<String>) {
        let mut s = vec![];
        let mut t = vec![];
        for (table, order) in tables {
            s.push(Self::hash(&self.src, &self.schema, table, order).await);
            t.push(Self::hash(&self.tgt, &self.schema, table, order).await);
        }
        (s, t)
    }

    /// Wait until the source and target contents match for all tables and the job is
    /// STREAMING. Compares full contents, not counts.
    pub async fn wait_converged(&self, tables: &[(&str, &str)], timeout: Duration) {
        let start = Instant::now();
        loop {
            let (s, t) = self.hashes(tables).await;
            let state = self.job_state().await;
            if s == t && state.as_deref() == Some("STREAMING") {
                return;
            }
            if start.elapsed() > timeout {
                panic!(
                    "no convergence within {timeout:?}: state={state:?}\nsource={s:?}\ntarget={t:?}\njob={:?}",
                    self.job_row().await
                );
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    }

    pub async fn source_wal_lsn(&self) -> String {
        self.src
            .query_one("SELECT pg_current_wal_lsn()::text", &[])
            .await
            .unwrap()
            .get(0)
    }

    pub async fn slot_exists(&self) -> bool {
        self.src
            .query_opt(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1",
                &[&self.slot],
            )
            .await
            .unwrap()
            .is_some()
    }

    pub async fn slot_active_pid(&self) -> Option<i32> {
        self.src
            .query_opt(
                "SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1",
                &[&self.slot],
            )
            .await
            .unwrap()
            .and_then(|r| r.get(0))
    }

    /// Drop everything the test created. Slots are dropped only when inactive.
    pub async fn teardown(&self) {
        let _ = self
            .src
            .batch_execute(&format!("DROP PUBLICATION IF EXISTS {}", self.publication))
            .await;
        let _ = self
            .src
            .execute("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1 AND NOT active", &[&self.slot])
            .await;
        let _ = self
            .src
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {} CASCADE", self.schema))
            .await;
        let _ = self
            .tgt
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {} CASCADE", self.schema))
            .await;
        let _ = self
            .tgt
            .execute("DELETE FROM pgsync.job WHERE job_id = $1", &[&self.job_id])
            .await;
    }
}

pub struct Proc {
    pub child: Child,
    pub log: PathBuf,
}

impl Proc {
    pub fn pid(&self) -> u32 {
        self.child.id()
    }
    /// SIGKILL: simulates a crash.
    pub fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
    /// SIGTERM: graceful shutdown; returns the exit code.
    pub fn terminate(&mut self) -> i32 {
        let _ = Command::new("kill")
            .arg("-TERM")
            .arg(self.pid().to_string())
            .status();
        self.child
            .wait()
            .map(|s| s.code().unwrap_or(-1))
            .unwrap_or(-1)
    }
    pub fn wait(&mut self) -> i32 {
        self.child
            .wait()
            .map(|s| s.code().unwrap_or(-1))
            .unwrap_or(-1)
    }
    pub fn try_wait(&mut self) -> Option<i32> {
        self.child
            .try_wait()
            .ok()
            .flatten()
            .map(|s| s.code().unwrap_or(-1))
    }
    pub fn log(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }
}

impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub fn docker(args: &[&str]) {
    let status = Command::new("docker").args(args).status().expect("docker");
    assert!(status.success(), "docker {args:?} failed");
}

/// Standard two-table dataset used by most scenarios.
pub const ACCOUNTS_DDL: &str = r#"
CREATE TABLE accounts (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email       text NOT NULL UNIQUE,
    name        varchar(80),
    active      boolean NOT NULL DEFAULT true,
    balance     numeric(14,2) NOT NULL DEFAULT 0,
    uid         uuid NOT NULL DEFAULT gen_random_uuid(),
    meta        jsonb,
    tags        text[],
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamp,
    blob        bytea
);
CREATE TABLE events (
    id          bigserial PRIMARY KEY,
    account_id  bigint NOT NULL REFERENCES accounts (id),
    kind        text NOT NULL,
    amount      numeric(14,2),
    payload     json,
    note        text
);
"#;

pub const ACCOUNTS_TABLES: &[(&str, &str)] = &[("accounts", "id"), ("events", "id")];

pub async fn seed_accounts(client: &Client, schema: &str, n: i64) {
    client
        .batch_execute(&format!(
            "INSERT INTO {schema}.accounts (email, name, balance, meta, tags, blob) \
             SELECT format('u%s@x.test', g), format('User %s', g), (g * 37 % 1000)::numeric / 10, \
             jsonb_build_object('n', g, 'odd', g % 2 = 1), ARRAY[format('t%s', g % 5), 'all'], decode(lpad(to_hex(g), 8, '0'), 'hex') \
             FROM generate_series(1, {n}) g; \
             INSERT INTO {schema}.events (account_id, kind, amount, payload, note) \
             SELECT (g % {n}) + 1, (ARRAY['open','close','tx'])[(g % 3) + 1], g * 1.5, json_build_object('g', g), \
             CASE WHEN g % 7 = 0 THEN NULL ELSE format('note %s', g) END FROM generate_series(1, {n} * 3) g;"
        ))
        .await
        .expect("seed");
}
