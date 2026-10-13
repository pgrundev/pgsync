//! Job configuration: YAML schema, defaults, and validation.
//!
//! Connection URLs are never stored in the file; they are read from the environment
//! variables named by `url_env` so that secrets stay out of config and logs.

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt;
use std::time::Duration;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub job: JobConfig,
    pub source: SourceConfig,
    pub target: TargetConfig,
    pub replication: ReplicationConfig,
    #[serde(default)]
    pub schema: SchemaConfig,
    #[serde(default)]
    pub resources: ResourcesConfig,
    #[serde(default)]
    pub safety: SafetyConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JobConfig {
    /// Stable identifier; also the primary key of the checkpoint row in the target.
    pub id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    /// Environment variable holding the source connection URL.
    pub url_env: String,
    /// Existing publication to stream from.
    pub publication: String,
    /// Logical replication slot name (created by `bootstrap`, used by `run`).
    pub slot: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TargetConfig {
    pub url_env: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplicationMode {
    /// Initial snapshot followed by continuous CDC (default).
    SnapshotAndCdc,
    /// Reserved. Rejected by validation: pgsync cannot prove that an existing target copy is
    /// consistent with a freshly created slot, so there is no safe way to skip the snapshot.
    CdcOnly,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicationConfig {
    #[serde(default = "default_mode")]
    pub mode: ReplicationMode,
    /// Fully qualified `schema.table` names. Must match the publication contents.
    pub tables: Vec<TableName>,
}

fn default_mode() -> ReplicationMode {
    ReplicationMode::SnapshotAndCdc
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SchemaMode {
    /// Run `pg_dump --schema-only` for the selected tables and apply it to the target.
    #[default]
    PgDump,
    /// The target schema already exists; PGSync verifies columns and proceeds.
    Existing,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct SchemaConfig {
    #[serde(default)]
    pub mode: SchemaMode,
    /// Path to the `pg_dump` binary (default: resolved from `PATH`).
    #[serde(default)]
    pub pg_dump_path: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct ResourcesConfig {
    pub memory_budget_mb: u64,
    pub buffer_size_kb: u64,
    pub max_inflight_bytes: u64,
    pub channel_capacity: usize,
    pub max_batch_rows: usize,
    pub max_batch_bytes: u64,
    /// Largest single replication message accepted from the server (one row change can be
    /// up to 1 GB in PostgreSQL). Larger messages stop the job with a `resource` error
    /// before anything is buffered; nothing is acknowledged. `None` derives a value from
    /// the remaining memory budget, capped at 32 MiB.
    pub max_message_bytes: Option<u64>,
    pub max_parallel_copies: usize,
    #[serde(with = "duration_text")]
    pub connection_timeout: Duration,
    pub retry_backoff: RetryBackoff,
    #[serde(with = "duration_text")]
    pub snapshot_timeout: Duration,
    #[serde(with = "duration_text")]
    pub feedback_interval: Duration,
    pub max_replication_lag_bytes: u64,
}

impl Default for ResourcesConfig {
    fn default() -> Self {
        ResourcesConfig {
            memory_budget_mb: 128,
            buffer_size_kb: 256,
            max_inflight_bytes: 16 * 1024 * 1024,
            channel_capacity: 4096,
            max_batch_rows: 500,
            max_batch_bytes: 4 * 1024 * 1024,
            max_message_bytes: None,
            max_parallel_copies: 2,
            connection_timeout: Duration::from_secs(10),
            retry_backoff: RetryBackoff::default(),
            snapshot_timeout: Duration::from_secs(6 * 3600),
            feedback_interval: Duration::from_secs(10),
            max_replication_lag_bytes: 1024 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct RetryBackoff {
    #[serde(with = "duration_text")]
    pub initial: Duration,
    #[serde(with = "duration_text")]
    pub max: Duration,
    pub multiplier: f64,
    /// `0` means retry forever.
    pub max_attempts: u32,
}

pub const MAX_MESSAGE_BYTES_CAP: u64 = 32 * 1024 * 1024;

impl ResourcesConfig {
    /// Memory reserved for channel, batch and COPY buffers.
    pub fn fixed_budget_bytes(&self) -> u64 {
        self.max_inflight_bytes
            + self.max_batch_bytes
            + (self.max_parallel_copies as u64) * 2 * self.buffer_size_kb * 1024
    }

    /// Effective message limit: the configured value, or half of what the budget leaves
    /// after the fixed buffers (a message is held twice while it is applied: decoded and
    /// serialised as statement parameters), capped at 32 MiB and never below 1 MiB.
    pub fn effective_max_message_bytes(&self) -> u64 {
        match self.max_message_bytes {
            Some(v) => v,
            None => {
                let budget = self.memory_budget_mb * 1024 * 1024;
                let spare = budget.saturating_sub(self.fixed_budget_bytes()) / 2;
                spare.clamp(1024 * 1024, MAX_MESSAGE_BYTES_CAP)
            }
        }
    }
}

impl Default for RetryBackoff {
    fn default() -> Self {
        RetryBackoff {
            initial: Duration::from_secs(1),
            max: Duration::from_secs(30),
            multiplier: 2.0,
            max_attempts: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum WalRetentionAction {
    #[default]
    Warn,
    Stop,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct SafetyConfig {
    pub require_tls: bool,
    /// Accept any server certificate. Only honoured for loopback hosts.
    pub insecure_skip_tls_verify: bool,
    /// PEM file with CA certificate(s) to trust in addition to the system store.
    pub tls_ca_file: Option<String>,
    pub auto_create_publication: bool,
    pub auto_drop_slot: bool,
    pub wal_retention_alarm_bytes: u64,
    pub wal_retention_action: WalRetentionAction,
    #[serde(with = "duration_text")]
    pub monitor_interval: Duration,
    /// How often the running job writes its runtime row (`pgsync.job_runtime`) to the
    /// target, for `pgsync status` and external monitoring.
    #[serde(with = "duration_text")]
    pub heartbeat_interval: Duration,
}

impl Default for SafetyConfig {
    fn default() -> Self {
        SafetyConfig {
            require_tls: true,
            insecure_skip_tls_verify: false,
            tls_ca_file: None,
            auto_create_publication: false,
            auto_drop_slot: false,
            wal_retention_alarm_bytes: 1024 * 1024 * 1024,
            wal_retention_action: WalRetentionAction::Warn,
            monitor_interval: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(10),
        }
    }
}

/// A `schema.table` pair. Serialised as `schema.table`; quoted identifiers are not
/// supported in Beta 1 (names must be plain lower-case identifiers).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableName {
    pub schema: String,
    pub name: String,
}

impl TableName {
    pub fn new(schema: impl Into<String>, name: impl Into<String>) -> Self {
        TableName {
            schema: schema.into(),
            name: name.into(),
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        let (schema, name) = s
            .split_once('.')
            .ok_or_else(|| Error::config(format!("table {s:?} must be written as schema.table")))?;
        for part in [schema, name] {
            if !is_plain_identifier(part) {
                return Err(Error::config(format!(
                    "table {s:?}: {part:?} is not a plain identifier (lower-case letters, digits, underscore)"
                )));
            }
        }
        Ok(TableName::new(schema, name))
    }

    /// Double-quoted, escaped `"schema"."table"` for use in SQL.
    pub fn quoted(&self) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.name))
    }
}

impl fmt::Display for TableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.schema, self.name)
    }
}

impl Serialize for TableName {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for TableName {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        TableName::parse(&s).map_err(|e| serde::de::Error::custom(e.message().to_owned()))
    }
}

pub fn is_plain_identifier(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_lowercase() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_') && s.len() <= 63
}

/// Quote an SQL identifier, doubling embedded quotes.
pub fn quote_ident(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        if c == '"' {
            out.push('"');
        }
        out.push(c);
    }
    out.push('"');
    out
}

/// Quote an SQL string literal (single quotes doubled). Used only for replication
/// commands, which do not support parameters.
pub fn quote_literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        if c == '\'' {
            out.push('\'');
        }
        out.push(c);
    }
    out.push('\'');
    out
}

impl Config {
    pub fn from_yaml(text: &str) -> Result<Config> {
        let cfg: Config = serde_yaml_ng::from_str(text)
            .map_err(|e| Error::config(format!("invalid configuration: {e}")))?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        let bad = |m: String| Err(Error::config(m));
        if self.job.id.is_empty() || self.job.id.len() > 64 {
            return bad("job.id must be 1-64 characters".into());
        }
        if !self
            .job
            .id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return bad("job.id may only contain letters, digits, '-' and '_'".into());
        }
        for (field, value) in [
            ("source.url_env", &self.source.url_env),
            ("target.url_env", &self.target.url_env),
        ] {
            if value.is_empty() {
                return bad(format!("{field} must name an environment variable"));
            }
        }
        if !is_plain_identifier(&self.source.publication) {
            return bad(format!(
                "source.publication {:?} must be a plain identifier",
                self.source.publication
            ));
        }
        if !is_slot_name(&self.source.slot) {
            return bad(format!(
                "source.slot {:?} must contain only lower-case letters, digits and underscores (max 63)",
                self.source.slot
            ));
        }
        if self.replication.tables.is_empty() {
            return bad("replication.tables must list at least one table".into());
        }
        if self.replication.mode == ReplicationMode::CdcOnly {
            return bad(
                "replication.mode: cdc_only is not supported in this version: pgsync cannot verify \
                 that the target already holds a copy consistent with a new slot's snapshot, and \
                 streaming into an unverified copy would diverge silently. Use snapshot_and_cdc."
                    .into(),
            );
        }
        let mut seen = BTreeSet::new();
        for t in &self.replication.tables {
            if !seen.insert(t) {
                return bad(format!("replication.tables lists {t} more than once"));
            }
        }
        let r = &self.resources;
        if r.memory_budget_mb < 16 {
            return bad("resources.memory_budget_mb must be at least 16".into());
        }
        if r.buffer_size_kb == 0 || r.buffer_size_kb > 64 * 1024 {
            return bad("resources.buffer_size_kb must be between 1 and 65536".into());
        }
        if r.max_inflight_bytes < 64 * 1024 {
            return bad("resources.max_inflight_bytes must be at least 65536".into());
        }
        if r.channel_capacity == 0 {
            return bad("resources.channel_capacity must be positive".into());
        }
        if r.max_batch_rows == 0 || r.max_batch_bytes == 0 {
            return bad("resources.max_batch_rows and max_batch_bytes must be positive".into());
        }
        if r.max_parallel_copies == 0 || r.max_parallel_copies > 64 {
            return bad("resources.max_parallel_copies must be between 1 and 64".into());
        }
        let budget = r.memory_budget_mb * 1024 * 1024;
        let needed = r.fixed_budget_bytes();
        if needed > budget {
            return bad(format!(
                "resources.memory_budget_mb ({} MiB) is smaller than max_inflight_bytes + max_batch_bytes + copy buffers ({} bytes)",
                r.memory_budget_mb, needed
            ));
        }
        if let Some(m) = r.max_message_bytes {
            if m < 64 * 1024 {
                return bad("resources.max_message_bytes must be at least 65536".into());
            }
            if needed + 2 * m > budget {
                return bad(format!(
                    "resources.memory_budget_mb ({} MiB) cannot hold the fixed buffers ({} bytes) plus two copies of a max_message_bytes message ({} bytes)",
                    r.memory_budget_mb, needed, m
                ));
            }
        }
        if r.connection_timeout.is_zero() {
            return bad("resources.connection_timeout must be positive".into());
        }
        if r.retry_backoff.initial.is_zero() || r.retry_backoff.max < r.retry_backoff.initial {
            return bad("resources.retry_backoff: initial must be positive and <= max".into());
        }
        if !(r.retry_backoff.multiplier >= 1.0 && r.retry_backoff.multiplier <= 10.0) {
            return bad("resources.retry_backoff.multiplier must be between 1 and 10".into());
        }
        if r.snapshot_timeout.is_zero() {
            return bad("resources.snapshot_timeout must be positive".into());
        }
        if r.feedback_interval.is_zero() || r.feedback_interval > Duration::from_secs(60) {
            return bad("resources.feedback_interval must be between 1ms and 60s".into());
        }
        if r.max_replication_lag_bytes == 0 {
            return bad("resources.max_replication_lag_bytes must be positive".into());
        }
        let s = &self.safety;
        if s.wal_retention_alarm_bytes == 0 {
            return bad("safety.wal_retention_alarm_bytes must be positive".into());
        }
        if s.monitor_interval.is_zero() {
            return bad("safety.monitor_interval must be positive".into());
        }
        if s.heartbeat_interval.is_zero() || s.heartbeat_interval > Duration::from_secs(300) {
            return bad("safety.heartbeat_interval must be between 1ms and 5m".into());
        }
        Ok(())
    }
}

pub fn is_slot_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 63
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// Human durations such as `500ms`, `10s`, `5m`, `2h`.
pub fn parse_duration(s: &str) -> std::result::Result<Duration, String> {
    let s = s.trim();
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let n: u64 = num.parse().map_err(|_| format!("invalid duration {s:?}"))?;
    let d = match unit.trim() {
        "ms" => Duration::from_millis(n),
        "s" | "" => Duration::from_secs(n),
        "m" => Duration::from_secs(n * 60),
        "h" => Duration::from_secs(n * 3600),
        _ => return Err(format!("invalid duration unit in {s:?} (use ms, s, m, h)")),
    };
    Ok(d)
}

pub fn format_duration(d: Duration) -> String {
    let ms = d.as_millis();
    if ms % 3_600_000 == 0 && ms > 0 {
        format!("{}h", ms / 3_600_000)
    } else if ms % 60_000 == 0 && ms > 0 {
        format!("{}m", ms / 60_000)
    } else if ms % 1000 == 0 {
        format!("{}s", ms / 1000)
    } else {
        format!("{ms}ms")
    }
}

mod duration_text {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(d: &Duration, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&format_duration(*d))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> std::result::Result<Duration, D::Error> {
        let s = String::deserialize(d)?;
        parse_duration(&s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = r#"
job:
  id: demo
source:
  url_env: SRC
  publication: pub1
  slot: slot_1
target:
  url_env: DST
replication:
  tables: [public.users, public.orders]
"#;

    #[test]
    fn minimal_config_gets_defaults() {
        let c = Config::from_yaml(MINIMAL).unwrap();
        assert_eq!(c.replication.mode, ReplicationMode::SnapshotAndCdc);
        assert_eq!(c.resources.memory_budget_mb, 128);
        assert_eq!(c.resources.connection_timeout, Duration::from_secs(10));
        assert!(c.safety.require_tls);
        assert_eq!(c.schema.mode, SchemaMode::PgDump);
        assert_eq!(c.replication.tables[0], TableName::new("public", "users"));
    }

    #[test]
    fn spec_example_parses() {
        let yaml = r#"
job:
  id: demo-replication
source:
  url_env: PGSYNC_SOURCE_URL
  publication: pgsync_demo
  slot: pgsync_demo_slot
target:
  url_env: PGSYNC_TARGET_URL
replication:
  mode: snapshot_and_cdc
  tables:
    - public.users
    - public.orders
resources:
  memory_budget_mb: 128
  buffer_size_kb: 256
  max_inflight_bytes: 16777216
  max_parallel_copies: 2
  connection_timeout: 5s
  retry_backoff:
    initial: 500ms
    max: 1m
  snapshot_timeout: 2h
safety:
  require_tls: true
  auto_create_publication: false
  auto_drop_slot: false
"#;
        let c = Config::from_yaml(yaml).unwrap();
        assert_eq!(
            c.resources.retry_backoff.initial,
            Duration::from_millis(500)
        );
        assert_eq!(c.resources.retry_backoff.max, Duration::from_secs(60));
        assert_eq!(c.resources.snapshot_timeout, Duration::from_secs(7200));
        assert_eq!(c.resources.connection_timeout, Duration::from_secs(5));
    }

    #[test]
    fn rejects_unknown_fields_and_bad_values() {
        let cases = [
            (
                MINIMAL.replace("tables: [public.users, public.orders]", "tables: []"),
                "at least one",
            ),
            (
                MINIMAL.replace("public.orders", "public.users"),
                "more than once",
            ),
            (MINIMAL.replace("slot_1", "Slot-1"), "source.slot"),
            (
                format!("{MINIMAL}\n")
                    .replace("replication:\n", "replication:\n  mode: cdc_only\n"),
                "cdc_only is not supported",
            ),
            (MINIMAL.replace("id: demo", "id: 'demo job'"), "job.id"),
            (
                format!("{MINIMAL}\nresources:\n  memory_budget_mb: 4\n"),
                "memory_budget_mb",
            ),
            (
                format!("{MINIMAL}\nresources:\n  max_parallel_copies: 0\n"),
                "max_parallel_copies",
            ),
            (
                format!("{MINIMAL}\nresources:\n  connection_timeout: 10x\n"),
                "duration",
            ),
            (
                format!("{MINIMAL}\nresources:\n  typo: 1\n"),
                "unknown field",
            ),
            (
                format!(
                    "{MINIMAL}\nresources:\n  memory_budget_mb: 16\n  max_inflight_bytes: 20000000\n"
                ),
                "smaller than",
            ),
        ];
        for (yaml, expect) in cases {
            let err = Config::from_yaml(&yaml).expect_err(&format!("should reject: {expect}"));
            assert!(err.message().contains(expect), "{expect}: got {err}");
        }
    }

    #[test]
    fn message_limit_is_derived_from_the_budget_or_validated() {
        let c = Config::from_yaml(MINIMAL).unwrap();
        // 128 MiB - (16 + 4 + 1) MiB = 107 MiB, half is 53.5 MiB, capped at 32 MiB.
        assert_eq!(c.resources.effective_max_message_bytes(), 32 * 1024 * 1024);
        let c =
            Config::from_yaml(&format!("{MINIMAL}\nresources:\n  memory_budget_mb: 24\n")).unwrap();
        assert_eq!(c.resources.effective_max_message_bytes(), 1536 * 1024);
        let c = Config::from_yaml(&format!(
            "{MINIMAL}\nresources:\n  max_message_bytes: 8388608\n"
        ))
        .unwrap();
        assert_eq!(c.resources.effective_max_message_bytes(), 8 * 1024 * 1024);
        let err = Config::from_yaml(&format!(
            "{MINIMAL}\nresources:\n  max_message_bytes: 67108864\n"
        ))
        .unwrap_err();
        assert!(err.message().contains("two copies"), "{err}");
    }

    #[test]
    fn table_name_parsing_and_quoting() {
        assert!(TableName::parse("users").is_err());
        assert!(TableName::parse("Public.users").is_err());
        assert!(TableName::parse("public.users;drop").is_err());
        let t = TableName::parse("app_1.orders_2").unwrap();
        assert_eq!(t.quoted(), "\"app_1\".\"orders_2\"");
        assert_eq!(quote_ident("we\"ird"), "\"we\"\"ird\"");
        assert_eq!(quote_literal("it's"), "'it''s'");
    }

    #[test]
    fn durations_round_trip() {
        for (s, d) in [
            ("500ms", Duration::from_millis(500)),
            ("10s", Duration::from_secs(10)),
            ("5m", Duration::from_secs(300)),
            ("2h", Duration::from_secs(7200)),
            ("7", Duration::from_secs(7)),
        ] {
            assert_eq!(parse_duration(s).unwrap(), d, "{s}");
        }
        assert_eq!(format_duration(Duration::from_secs(300)), "5m");
        assert_eq!(format_duration(Duration::from_millis(1500)), "1500ms");
        assert!(parse_duration("abc").is_err());
        assert!(parse_duration("10d").is_err());
    }
}
