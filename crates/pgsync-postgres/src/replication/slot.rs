//! Replication commands and slot inspection over a replication connection.

use super::conn::{ReplicationConnection, SimpleResult};
use pgsync_core::config::{quote_ident, quote_literal};
use pgsync_core::{Error, Lsn, Result};

#[derive(Debug, Clone)]
pub struct SystemInfo {
    pub system_id: String,
    pub timeline: u64,
    pub xlogpos: Lsn,
    pub dbname: Option<String>,
}

pub async fn identify_system(conn: &mut ReplicationConnection) -> Result<SystemInfo> {
    let r = conn.simple_query("IDENTIFY_SYSTEM").await?;
    let get = |c: &str| r.get(0, c).map(str::to_owned);
    Ok(SystemInfo {
        system_id: get("systemid")
            .ok_or_else(|| Error::internal("IDENTIFY_SYSTEM: missing systemid"))?,
        timeline: get("timeline").and_then(|t| t.parse().ok()).unwrap_or(0),
        xlogpos: get("xlogpos")
            .ok_or_else(|| Error::internal("IDENTIFY_SYSTEM: missing xlogpos"))?
            .parse()
            .map_err(|e| Error::internal(format!("IDENTIFY_SYSTEM: {e}")))?,
        dbname: get("dbname"),
    })
}

#[derive(Debug, Clone)]
pub struct CreatedSlot {
    pub slot_name: String,
    pub consistent_point: Lsn,
    pub snapshot_name: String,
}

/// `CREATE_REPLICATION_SLOT ... LOGICAL pgoutput EXPORT_SNAPSHOT`.
///
/// The exported snapshot stays valid only until the next command on `conn`.
pub async fn create_slot_with_snapshot(
    conn: &mut ReplicationConnection,
    slot: &str,
) -> Result<CreatedSlot> {
    let cmd = format!(
        "CREATE_REPLICATION_SLOT {} LOGICAL pgoutput EXPORT_SNAPSHOT",
        quote_ident(slot)
    );
    let r = conn.simple_query(&cmd).await?;
    let consistent_point: Lsn = r
        .get(0, "consistent_point")
        .ok_or_else(|| Error::internal("CREATE_REPLICATION_SLOT: missing consistent_point"))?
        .parse()
        .map_err(|e| Error::internal(format!("CREATE_REPLICATION_SLOT: {e}")))?;
    let snapshot_name = r
        .get(0, "snapshot_name")
        .ok_or_else(|| Error::internal("CREATE_REPLICATION_SLOT: missing snapshot_name"))?
        .to_owned();
    Ok(CreatedSlot {
        slot_name: slot.to_owned(),
        consistent_point,
        snapshot_name,
    })
}

pub async fn drop_slot(conn: &mut ReplicationConnection, slot: &str) -> Result<()> {
    conn.simple_query(&format!("DROP_REPLICATION_SLOT {} WAIT", quote_ident(slot)))
        .await?;
    Ok(())
}

/// Version-independent view of `pg_replication_slots` for one slot.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct SlotInfo {
    pub slot_name: String,
    pub plugin: Option<String>,
    pub slot_type: Option<String>,
    pub database: Option<String>,
    pub active: bool,
    pub active_pid: Option<i64>,
    pub restart_lsn: Option<Lsn>,
    pub confirmed_flush_lsn: Option<Lsn>,
    pub wal_status: Option<String>,
    pub safe_wal_size: Option<i64>,
    pub invalidation_reason: Option<String>,
    pub inactive_since: Option<String>,
}

pub const SLOT_QUERY: &str =
    "SELECT to_jsonb(s)::text AS j FROM pg_catalog.pg_replication_slots s WHERE s.slot_name = ";

pub fn slot_info_from_json(json: &str) -> Result<SlotInfo> {
    let v: serde_json::Value = serde_json::from_str(json)
        .map_err(|e| Error::internal(format!("pg_replication_slots: {e}")))?;
    let s = |k: &str| v.get(k).and_then(|x| x.as_str()).map(str::to_owned);
    let lsn = |k: &str| -> Result<Option<Lsn>> {
        match s(k) {
            Some(t) => Ok(Some(
                t.parse()
                    .map_err(|e| Error::internal(format!("{k}: {e}")))?,
            )),
            None => Ok(None),
        }
    };
    Ok(SlotInfo {
        slot_name: s("slot_name").unwrap_or_default(),
        plugin: s("plugin"),
        slot_type: s("slot_type"),
        database: s("database"),
        active: v.get("active").and_then(|x| x.as_bool()).unwrap_or(false),
        active_pid: v.get("active_pid").and_then(|x| x.as_i64()),
        restart_lsn: lsn("restart_lsn")?,
        confirmed_flush_lsn: lsn("confirmed_flush_lsn")?,
        wal_status: s("wal_status"),
        safe_wal_size: v.get("safe_wal_size").and_then(|x| x.as_i64()),
        invalidation_reason: s("invalidation_reason"),
        inactive_since: s("inactive_since"),
    })
}

pub async fn read_slot(conn: &mut ReplicationConnection, slot: &str) -> Result<Option<SlotInfo>> {
    let r: SimpleResult = conn
        .simple_query(&format!("{SLOT_QUERY}{}", quote_literal(slot)))
        .await?;
    match r.get(0, "j") {
        Some(json) => Ok(Some(slot_info_from_json(json)?)),
        None => Ok(None),
    }
}

/// Build the `START_REPLICATION` command for pgoutput protocol version 1.
pub fn start_replication_command(slot: &str, from: Lsn, publication: &str) -> String {
    format!(
        "START_REPLICATION SLOT {} LOGICAL {} (proto_version '1', publication_names {})",
        quote_ident(slot),
        from,
        quote_literal(publication)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_slot_json_with_optional_columns() {
        let json = r#"{"slot_name":"s1","plugin":"pgoutput","slot_type":"logical","database":"app","active":true,"active_pid":123,"restart_lsn":"0/16B3748","confirmed_flush_lsn":"0/16B3780","wal_status":"reserved","safe_wal_size":null,"two_phase":false}"#;
        let s = slot_info_from_json(json).unwrap();
        assert_eq!(s.plugin.as_deref(), Some("pgoutput"));
        assert!(s.active);
        assert_eq!(s.restart_lsn, Some(Lsn(0x16B3748)));
        assert_eq!(s.confirmed_flush_lsn, Some(Lsn(0x16B3780)));
        assert_eq!(s.invalidation_reason, None);
        let pg17 = r#"{"slot_name":"s1","active":false,"invalidation_reason":"wal_removed","wal_status":"lost","restart_lsn":null}"#;
        let s = slot_info_from_json(pg17).unwrap();
        assert_eq!(s.invalidation_reason.as_deref(), Some("wal_removed"));
        assert_eq!(s.restart_lsn, None);
    }

    #[test]
    fn start_command_quotes_safely() {
        let c = start_replication_command("my_slot", Lsn(0x16B3748), "pub1");
        assert_eq!(
            c,
            "START_REPLICATION SLOT \"my_slot\" LOGICAL 0/16B3748 (proto_version '1', publication_names 'pub1')"
        );
    }
}
