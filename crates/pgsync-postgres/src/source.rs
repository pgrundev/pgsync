//! Source inspection and preflight checks (ordinary SQL connection, read-only).

use crate::client::PgClient;
use crate::errors::from_pg;
use crate::replication::slot::{SlotInfo, slot_info_from_json};
use pgsync_core::config::TableName;
use pgsync_core::{Error, Lsn, Result};
use serde::Serialize;
use std::collections::BTreeSet;

#[derive(Debug, Clone, Serialize)]
pub struct ServerInfo {
    pub version_num: i32,
    pub version: String,
    pub wal_level: String,
    pub max_replication_slots: i32,
    pub max_wal_senders: i32,
    pub max_slot_wal_keep_size: String,
    pub wal_sender_timeout: String,
    pub wal_sender_timeout_ms: i64,
    pub current_wal_lsn: Lsn,
    pub slots_in_use: i64,
    pub system_identifier: String,
    pub dbname: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct RoleInfo {
    pub name: String,
    pub replication: bool,
    pub superuser: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct PublicationTable {
    pub table: TableName,
    pub column_list: Option<Vec<String>>,
    pub row_filter: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PublicationInfo {
    pub name: String,
    pub all_tables: bool,
    pub insert: bool,
    pub update: bool,
    pub delete: bool,
    pub truncate: bool,
    pub via_partition_root: bool,
    pub tables: Vec<PublicationTable>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ColumnInfo {
    pub attnum: i16,
    pub name: String,
    pub type_name: String,
    pub not_null: bool,
    pub generated: bool,
    pub identity: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TableInfo {
    pub table: TableName,
    pub oid: u32,
    pub relkind: String,
    pub replica_identity: String,
    pub has_primary_key: bool,
    pub key_columns: Vec<String>,
    pub columns: Vec<ColumnInfo>,
    pub estimated_rows: f64,
    pub size_bytes: i64,
    pub can_select: bool,
}

impl TableInfo {
    pub fn column_names(&self) -> Vec<String> {
        self.columns
            .iter()
            .filter(|c| !c.generated)
            .map(|c| c.name.clone())
            .collect()
    }
}

pub async fn server_info(c: &PgClient) -> Result<ServerInfo> {
    let row = c
        .query_one(
            "SELECT current_setting('server_version_num')::int, version(), current_setting('wal_level'), \
             current_setting('max_replication_slots')::int, current_setting('max_wal_senders')::int, \
             current_setting('max_slot_wal_keep_size'), current_setting('wal_sender_timeout'), \
             pg_current_wal_lsn()::text, (SELECT count(*) FROM pg_replication_slots), \
             (SELECT system_identifier::text FROM pg_control_system()), current_database(), \
             (SELECT setting::bigint FROM pg_settings WHERE name = 'wal_sender_timeout')",
            &[],
        )
        .await
        .map_err(|e| from_pg("source server info", e))?;
    let lsn: String = row.get(7);
    Ok(ServerInfo {
        version_num: row.get(0),
        version: row.get(1),
        wal_level: row.get(2),
        max_replication_slots: row.get(3),
        max_wal_senders: row.get(4),
        max_slot_wal_keep_size: row.get(5),
        wal_sender_timeout: row.get(6),
        current_wal_lsn: lsn
            .parse()
            .map_err(|e| Error::internal(format!("pg_current_wal_lsn: {e}")))?,
        slots_in_use: row.get(8),
        system_identifier: row.get(9),
        dbname: row.get(10),
        wal_sender_timeout_ms: row.get(11),
    })
}

pub async fn role_info(c: &PgClient) -> Result<RoleInfo> {
    let row = c
        .query_one("SELECT rolname::text, rolreplication, rolsuper FROM pg_roles WHERE rolname = current_user", &[])
        .await
        .map_err(|e| from_pg("source role info", e))?;
    Ok(RoleInfo {
        name: row.get(0),
        replication: row.get(1),
        superuser: row.get(2),
    })
}

pub async fn publication_info(c: &PgClient, name: &str) -> Result<Option<PublicationInfo>> {
    let row = c
        .query_opt(
            "SELECT puballtables, pubinsert, pubupdate, pubdelete, pubtruncate, pubviaroot FROM pg_publication WHERE pubname = $1",
            &[&name],
        )
        .await
        .map_err(|e| from_pg("publication info", e))?;
    let Some(row) = row else { return Ok(None) };
    let rows = c
        .query(
            "SELECT schemaname::text, tablename::text, attnames::text[], rowfilter, \
             (SELECT count(*) FROM pg_attribute a JOIN pg_class cl ON cl.oid = a.attrelid JOIN pg_namespace n ON n.oid = cl.relnamespace \
              WHERE n.nspname = pt.schemaname AND cl.relname = pt.tablename AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = '') \
             FROM pg_publication_tables pt WHERE pubname = $1 ORDER BY 1, 2",
            &[&name],
        )
        .await
        .map_err(|e| from_pg("publication tables", e))?;
    let tables = rows
        .iter()
        .map(|r| {
            let attnames: Option<Vec<String>> = r.get(2);
            let total: i64 = r.get(4);
            let column_list = attnames.filter(|a| (a.len() as i64) != total);
            PublicationTable {
                table: TableName::new(r.get::<_, String>(0), r.get::<_, String>(1)),
                column_list,
                row_filter: r.get(3),
            }
        })
        .collect();
    Ok(Some(PublicationInfo {
        name: name.to_owned(),
        all_tables: row.get(0),
        insert: row.get(1),
        update: row.get(2),
        delete: row.get(3),
        truncate: row.get(4),
        via_partition_root: row.get(5),
        tables,
    }))
}

pub async fn table_info(c: &PgClient, t: &TableName) -> Result<Option<TableInfo>> {
    let row = c
        .query_opt(
            "SELECT c.oid, c.relkind::text, c.relreplident::text, \
             EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary), \
             c.reltuples::float8, pg_table_size(c.oid), has_table_privilege(c.oid, 'SELECT') \
             FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relname = $2",
            &[&t.schema, &t.name],
        )
        .await
        .map_err(|e| from_pg(&format!("table info {t}"), e))?;
    let Some(row) = row else { return Ok(None) };
    let oid: u32 = row.get(0);
    let relkind: String = row.get(1);
    let replica_identity: String = row.get(2);
    let cols = c
        .query(
            "SELECT a.attnum, a.attname::text, format_type(a.atttypid, a.atttypmod), a.attnotnull, \
             a.attgenerated <> '', a.attidentity::text \
             FROM pg_attribute a WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum",
            &[&oid],
        )
        .await
        .map_err(|e| from_pg(&format!("columns of {t}"), e))?;
    let columns = cols
        .iter()
        .map(|r| ColumnInfo {
            attnum: r.get(0),
            name: r.get(1),
            type_name: r.get(2),
            not_null: r.get(3),
            generated: r.get(4),
            identity: r.get(5),
        })
        .collect();
    let keys = c
        .query(
            "SELECT a.attname::text FROM pg_index i \
             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey) \
             WHERE i.indrelid = $1 AND ((i.indisprimary AND $2 = 'd') OR (i.indisreplident AND $2 = 'i')) \
             ORDER BY a.attnum",
            &[&oid, &replica_identity],
        )
        .await
        .map_err(|e| from_pg(&format!("key columns of {t}"), e))?;
    Ok(Some(TableInfo {
        table: t.clone(),
        oid,
        relkind,
        replica_identity,
        has_primary_key: row.get(3),
        key_columns: keys.iter().map(|r| r.get::<_, String>(0)).collect(),
        columns,
        estimated_rows: row.get(4),
        size_bytes: row.get(5),
        can_select: row.get(6),
    }))
}

pub async fn slot_info(c: &PgClient, slot: &str) -> Result<Option<SlotInfo>> {
    let row = c
        .query_opt(
            "SELECT to_jsonb(s)::text FROM pg_replication_slots s WHERE s.slot_name = $1",
            &[&slot],
        )
        .await
        .map_err(|e| from_pg("slot info", e))?;
    match row {
        Some(r) => Ok(Some(slot_info_from_json(&r.get::<_, String>(0))?)),
        None => Ok(None),
    }
}

pub async fn current_wal_lsn(c: &PgClient) -> Result<Lsn> {
    let row = c
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .map_err(|e| from_pg("pg_current_wal_lsn", e))?;
    row.get::<_, String>(0)
        .parse()
        .map_err(|e| Error::internal(format!("pg_current_wal_lsn: {e}")))
}

// ---- preflight ----------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CheckStatus {
    Pass,
    Warn,
    Fail,
}

#[derive(Debug, Clone, Serialize)]
pub struct CheckResult {
    pub name: String,
    pub status: CheckStatus,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct PreflightReport {
    pub checks: Vec<CheckResult>,
}

impl PreflightReport {
    pub fn add(&mut self, name: &str, status: CheckStatus, detail: impl Into<String>) {
        self.checks.push(CheckResult {
            name: name.to_owned(),
            status,
            detail: detail.into(),
        });
    }
    pub fn pass(&mut self, name: &str, detail: impl Into<String>) {
        self.add(name, CheckStatus::Pass, detail);
    }
    pub fn warn(&mut self, name: &str, detail: impl Into<String>) {
        self.add(name, CheckStatus::Warn, detail);
    }
    pub fn fail(&mut self, name: &str, detail: impl Into<String>) {
        self.add(name, CheckStatus::Fail, detail);
    }
    pub fn failed(&self) -> bool {
        self.checks.iter().any(|c| c.status == CheckStatus::Fail)
    }
    pub fn failures(&self) -> Vec<&CheckResult> {
        self.checks
            .iter()
            .filter(|c| c.status == CheckStatus::Fail)
            .collect()
    }
}

/// Which lifecycle step the preflight is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreflightMode {
    /// `check` / `bootstrap`: the slot must not exist yet.
    Bootstrap,
    /// `run`: the slot must exist.
    Run,
}

pub struct SourcePreflight {
    pub server: ServerInfo,
    pub role: RoleInfo,
    pub publication: Option<PublicationInfo>,
    pub tables: Vec<TableInfo>,
    pub slot: Option<SlotInfo>,
    pub report: PreflightReport,
}

/// Inspect the source and evaluate every Beta 1 prerequisite. Never modifies the source.
pub async fn preflight(
    c: &PgClient,
    publication: &str,
    slot: &str,
    tables: &[TableName],
    mode: PreflightMode,
    feedback_interval: std::time::Duration,
) -> Result<SourcePreflight> {
    let mut r = PreflightReport::default();
    let server = server_info(c).await?;
    let role = role_info(c).await?;

    // Server
    if server.version_num < 140000 {
        r.fail(
            "server_version",
            format!("{} is not supported (need 16+)", server.version),
        );
    } else if server.version_num < 160000 {
        r.warn(
            "server_version",
            format!(
                "{} is untested; supported versions are 16, 17, 18",
                server.version
            ),
        );
    } else {
        r.pass("server_version", server.version.clone());
    }
    if server.wal_level == "logical" {
        r.pass("wal_level", "logical");
    } else {
        r.fail(
            "wal_level",
            format!(
                "is {:?}; must be 'logical' (requires a server restart; PGSync never changes it)",
                server.wal_level
            ),
        );
    }
    if server.max_replication_slots <= 0 {
        r.fail("max_replication_slots", "is 0");
    } else if server.slots_in_use >= server.max_replication_slots as i64
        && mode == PreflightMode::Bootstrap
    {
        r.fail(
            "max_replication_slots",
            format!("all {} slots are in use", server.max_replication_slots),
        );
    } else {
        r.pass(
            "max_replication_slots",
            format!(
                "{} ({} in use)",
                server.max_replication_slots, server.slots_in_use
            ),
        );
    }
    if server.max_wal_senders <= 0 {
        r.fail("max_wal_senders", "is 0");
    } else {
        r.pass("max_wal_senders", server.max_wal_senders.to_string());
    }
    if server.max_slot_wal_keep_size == "-1" {
        r.warn(
            "max_slot_wal_keep_size",
            "unlimited: a stalled consumer can fill the source disk with retained WAL",
        );
    } else {
        r.pass(
            "max_slot_wal_keep_size",
            format!(
                "{} (slot is invalidated beyond this)",
                server.max_slot_wal_keep_size
            ),
        );
    }
    let fb_ms = feedback_interval.as_millis() as i64;
    if server.wal_sender_timeout_ms > 0 && fb_ms * 2 > server.wal_sender_timeout_ms {
        r.warn(
            "wal_sender_timeout",
            format!(
                "{} is less than twice resources.feedback_interval ({fb_ms} ms): the walsender may drop \
                 the connection while the applier is blocked; lower feedback_interval",
                server.wal_sender_timeout
            ),
        );
    } else {
        r.pass("wal_sender_timeout", server.wal_sender_timeout.clone());
    }

    // Role
    if role.replication || role.superuser {
        r.pass(
            "role",
            format!(
                "{} has REPLICATION{}",
                role.name,
                if role.superuser { " (superuser)" } else { "" }
            ),
        );
    } else {
        r.fail(
            "role",
            format!("{} lacks the REPLICATION attribute", role.name),
        );
    }
    if role.superuser {
        r.warn(
            "least_privilege",
            "connecting as a superuser; prefer a dedicated REPLICATION role with SELECT only",
        );
    }

    // Publication
    let publication = publication_info(c, publication).await?;
    let configured: BTreeSet<&TableName> = tables.iter().collect();
    match &publication {
        None => r.fail("publication", format!("publication {publication:?} does not exist (PGSync does not create it on a source without approval)")),
        Some(p) => {
            r.pass("publication", format!("{} exists", p.name));
            let mut missing = vec![];
            if !p.insert { missing.push("insert") }
            if !p.update { missing.push("update") }
            if !p.delete { missing.push("delete") }
            if missing.is_empty() {
                r.pass("publication_operations", format!("insert, update, delete{}", if p.truncate { ", truncate" } else { "" }));
            } else {
                r.fail("publication_operations", format!("publication does not publish {}; changes would be silently missing", missing.join(", ")));
            }
            if p.all_tables {
                r.warn("publication_all_tables", "FOR ALL TABLES: new tables appear in the stream and will stop replication until configured");
            }
            if p.via_partition_root {
                r.fail("publish_via_partition_root", "not supported in Beta 1");
            }
            let published: BTreeSet<&TableName> = p.tables.iter().map(|t| &t.table).collect();
            let not_published: Vec<String> = configured.difference(&published).map(|t| t.to_string()).collect();
            let not_configured: Vec<String> = published.difference(&configured).map(|t| t.to_string()).collect();
            if not_published.is_empty() && not_configured.is_empty() {
                r.pass("publication_tables", format!("{} tables match replication.tables", published.len()));
            } else {
                if !not_published.is_empty() {
                    r.fail("publication_tables", format!("configured tables not in publication: {}", not_published.join(", ")));
                }
                if !not_configured.is_empty() {
                    r.fail("publication_tables", format!("publication contains tables not in replication.tables: {} (their changes cannot be applied)", not_configured.join(", ")));
                }
            }
            for t in &p.tables {
                if t.column_list.is_some() {
                    r.fail("column_list", format!("{}: publication column lists are not supported in Beta 1", t.table));
                }
                if t.row_filter.is_some() {
                    r.fail("row_filter", format!("{}: publication row filters are not supported in Beta 1", t.table));
                }
            }
        }
    }

    // Tables
    let mut infos = Vec::new();
    for t in tables {
        match table_info(c, t).await? {
            None => r.fail("table", format!("{t} does not exist")),
            Some(info) => {
                let name = format!("table {t}");
                match info.relkind.as_str() {
                    "r" => {}
                    "p" => r.fail(&name, "partitioned tables are not supported in Beta 1"),
                    "f" => r.fail(&name, "foreign tables cannot be replicated"),
                    "v" | "m" => r.fail(&name, "views cannot be replicated"),
                    k => r.fail(&name, format!("unsupported relkind {k:?}")),
                }
                if !info.can_select {
                    r.fail(
                        &name,
                        format!(
                            "role {} lacks SELECT (needed for the initial snapshot)",
                            role.name
                        ),
                    );
                }
                match info.replica_identity.as_str() {
                    "d" if info.has_primary_key => r.pass(&name, format!("primary key ({})", info.key_columns.join(", "))),
                    "d" => r.fail(&name, "no primary key and REPLICA IDENTITY DEFAULT: UPDATE/DELETE would fail on the source itself"),
                    "i" => r.pass(&name, format!("REPLICA IDENTITY USING INDEX ({})", info.key_columns.join(", "))),
                    "f" => r.warn(&name, "REPLICA IDENTITY FULL: supported, but every UPDATE/DELETE ships the whole old row and matches without an index"),
                    "n" => r.fail(&name, "REPLICA IDENTITY NOTHING: UPDATE/DELETE cannot be replicated"),
                    other => r.fail(&name, format!("unknown replica identity {other:?}")),
                }
                let generated: Vec<_> = info
                    .columns
                    .iter()
                    .filter(|c| c.generated)
                    .map(|c| c.name.as_str())
                    .collect();
                if !generated.is_empty() {
                    r.pass(
                        &name,
                        format!(
                            "generated columns are recomputed on the target: {}",
                            generated.join(", ")
                        ),
                    );
                }
                infos.push(info);
            }
        }
    }

    // Slot
    let slot_info = slot_info(c, slot).await?;
    match (&slot_info, mode) {
        (None, PreflightMode::Bootstrap) => r.pass("slot", format!("{slot:?} does not exist yet; bootstrap will create it")),
        (Some(s), PreflightMode::Bootstrap) => r.fail("slot", format!("{slot:?} already exists (plugin {:?}, active {}); choose another name or run `pgsync cleanup`", s.plugin, s.active)),
        (None, PreflightMode::Run) => r.fail("slot", format!("{slot:?} does not exist; the job needs a new bootstrap")),
        (Some(s), PreflightMode::Run) => {
            if s.wal_status.as_deref() == Some("lost") || s.invalidation_reason.is_some() {
                r.fail("slot", format!("{slot:?} is invalid (wal_status {:?}, reason {:?}); resynchronisation required", s.wal_status, s.invalidation_reason));
            } else {
                let retained = s.restart_lsn.map(|l| server.current_wal_lsn.bytes_since(l)).unwrap_or(0);
                r.pass("slot", format!("{slot:?} exists, active={}, wal_status={:?}, retained_wal_bytes={retained}", s.active, s.wal_status));
            }
        }
    }

    Ok(SourcePreflight {
        server,
        role,
        publication,
        tables: infos,
        slot: slot_info,
        report: r,
    })
}
