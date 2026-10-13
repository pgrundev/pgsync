//! `EventStream` / `StreamFactory` implementations over a replication connection.

use super::conn::ReplicationConnection;
use super::pgoutput::{LogicalMessage, ReplicationMessage, decode, encode_standby_status};
use super::slot::{identify_system, read_slot, start_replication_command};
use crate::url::ConnectionInfo;
use pgsync_core::checkpoint::Checkpoint;
use pgsync_core::config::SafetyConfig;
use pgsync_core::event::{Op, SourceEvent, now_pg_micros};
use pgsync_core::traits::{EventStream, Feedback, StreamFactory};
use pgsync_core::{Error, Lsn, Result};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info};

pub struct PgEventStream {
    conn: ReplicationConnection,
    /// Bound on one feedback write; a peer that stops reading is treated as gone.
    write_timeout: Duration,
}

impl PgEventStream {
    pub fn new(conn: ReplicationConnection, write_timeout: Duration) -> Self {
        PgEventStream {
            conn,
            write_timeout,
        }
    }

    pub async fn close(self) {
        self.conn.close().await;
    }
}

impl EventStream for PgEventStream {
    async fn next_event(&mut self) -> Result<SourceEvent> {
        loop {
            let data = self.conn.next_copy_data().await?;
            match decode(data)? {
                ReplicationMessage::PrimaryKeepalive { wal_end, .. } => {
                    return Ok(SourceEvent::Keepalive { wal_end });
                }
                ReplicationMessage::XLogData { body, .. } => match body {
                    LogicalMessage::Begin {
                        final_lsn,
                        commit_ts,
                        xid,
                    } => {
                        return Ok(SourceEvent::Begin {
                            final_lsn,
                            commit_ts,
                            xid,
                        });
                    }
                    LogicalMessage::Commit {
                        flags,
                        commit_lsn,
                        end_lsn,
                        commit_ts,
                    } => {
                        if flags != 0 {
                            return Err(Error::unsupported(format!(
                                "commit message with flags {flags:#x}"
                            )));
                        }
                        return Ok(SourceEvent::Commit {
                            commit_lsn,
                            end_lsn,
                            commit_ts,
                        });
                    }
                    LogicalMessage::Relation(r) => return Ok(SourceEvent::Relation(Arc::new(r))),
                    LogicalMessage::Insert { relation, new } => {
                        return Ok(SourceEvent::Op(Op::Insert { relation, new }));
                    }
                    LogicalMessage::Update {
                        relation,
                        key,
                        old,
                        new,
                    } => {
                        return Ok(SourceEvent::Op(Op::Update {
                            relation,
                            key,
                            old,
                            new,
                        }));
                    }
                    LogicalMessage::Delete { relation, key, old } => {
                        return Ok(SourceEvent::Op(Op::Delete { relation, key, old }));
                    }
                    LogicalMessage::Truncate {
                        relations,
                        cascade,
                        restart_identity,
                    } => {
                        return Ok(SourceEvent::Op(Op::Truncate {
                            relations,
                            cascade,
                            restart_identity,
                        }));
                    }
                    LogicalMessage::Origin { .. } | LogicalMessage::Type { .. } => continue,
                    LogicalMessage::Message {
                        prefix,
                        transactional,
                        ..
                    } => {
                        debug!(
                            prefix,
                            transactional, "ignoring logical decoding message (not a data change)"
                        );
                        continue;
                    }
                    LogicalMessage::Unsupported { tag } => {
                        return Err(Error::unsupported(format!(
                            "pgoutput message {:?} is not supported by this version of PGSync",
                            tag as char
                        )));
                    }
                },
            }
        }
    }

    async fn send_feedback(&mut self, fb: Feedback) -> Result<()> {
        let payload = encode_standby_status(
            fb.written,
            fb.flushed,
            fb.applied,
            now_pg_micros(),
            fb.reply_requested,
        );
        match tokio::time::timeout(self.write_timeout, self.conn.send_copy_data(&payload)).await {
            Ok(r) => r,
            Err(_) => Err(Error::transient(format!(
                "feedback write did not complete within {:?}; source not reading",
                self.write_timeout
            ))),
        }
    }
}

/// Opens replication streams for `run`, verifying source identity and slot continuity.
pub struct PgStreamFactory {
    pub info: ConnectionInfo,
    pub safety: SafetyConfig,
    pub timeout: Duration,
    pub slot: String,
    pub publication: String,
    pub max_message_bytes: usize,
}

impl StreamFactory for PgStreamFactory {
    type Stream = PgEventStream;

    async fn connect(&mut self, cp: &Checkpoint) -> Result<(PgEventStream, Lsn)> {
        let mut conn =
            ReplicationConnection::connect(&self.info, &self.safety, self.timeout).await?;
        conn.set_max_message_bytes(self.max_message_bytes);
        let sys = identify_system(&mut conn).await?;
        if sys.system_id != cp.source_system_id {
            return Err(Error::resync_required(format!(
                "source system identifier changed (checkpoint {}, server {}); continuity cannot be proven",
                cp.source_system_id, sys.system_id
            )));
        }
        if sys.dbname.as_deref() != Some(cp.source_dbname.as_str()) {
            return Err(Error::preflight(format!(
                "connected to database {:?} but the job was bootstrapped against {:?}",
                sys.dbname, cp.source_dbname
            )));
        }
        let Some(slot) = read_slot(&mut conn, &self.slot).await? else {
            return Err(Error::resync_required(format!(
                "replication slot {:?} does not exist; the job must be re-bootstrapped",
                self.slot
            )));
        };
        verify_slot_continuity(&slot, cp)?;
        let cmd = start_replication_command(&self.slot, cp.committed_lsn, &self.publication);
        conn.start_replication(&cmd).await?;
        info!(slot = %self.slot, from = %cp.committed_lsn, confirmed_flush = ?slot.confirmed_flush_lsn, "START_REPLICATION accepted");
        Ok((PgEventStream::new(conn, self.timeout), sys.xlogpos))
    }
}

/// Fail with `ResyncRequired` whenever the slot cannot deliver every transaction after the
/// checkpoint, or with `Preflight` when it is the wrong kind of slot.
pub fn verify_slot_continuity(slot: &super::slot::SlotInfo, cp: &Checkpoint) -> Result<()> {
    if slot.slot_type.as_deref() != Some("logical") || slot.plugin.as_deref() != Some("pgoutput") {
        return Err(Error::preflight(format!(
            "slot {:?} is not a logical pgoutput slot (type {:?}, plugin {:?})",
            slot.slot_name, slot.slot_type, slot.plugin
        )));
    }
    if slot.database.as_deref() != Some(cp.source_dbname.as_str()) {
        return Err(Error::preflight(format!(
            "slot {:?} belongs to database {:?}, expected {:?}",
            slot.slot_name, slot.database, cp.source_dbname
        )));
    }
    if slot.wal_status.as_deref() == Some("lost") || slot.invalidation_reason.is_some() {
        return Err(Error::resync_required(format!(
            "slot {:?} has lost WAL (wal_status={:?}, invalidation_reason={:?}); resynchronisation required",
            slot.slot_name, slot.wal_status, slot.invalidation_reason
        )));
    }
    if let Some(confirmed) = slot.confirmed_flush_lsn
        && confirmed > cp.acked_lsn
    {
        return Err(Error::resync_required(format!(
            "slot {:?} confirmed_flush_lsn {} is beyond the checkpoint's acknowledged position {}; \
             another consumer advanced the slot and transactions may be missing",
            slot.slot_name, confirmed, cp.acked_lsn
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::slot::SlotInfo;
    use pgsync_core::ErrorKind;

    fn slot() -> SlotInfo {
        SlotInfo {
            slot_name: "s".into(),
            plugin: Some("pgoutput".into()),
            slot_type: Some("logical".into()),
            database: Some("app".into()),
            confirmed_flush_lsn: Some(Lsn(100)),
            wal_status: Some("reserved".into()),
            ..Default::default()
        }
    }
    fn cp() -> Checkpoint {
        let mut cp = Checkpoint::new_job("j", "sys", "app", "s", "p");
        cp.record_commit(Lsn(90), Lsn(100), 1, 1);
        cp
    }

    #[test]
    fn continuity_checks() {
        verify_slot_continuity(&slot(), &cp()).unwrap();
        let mut s = slot();
        s.confirmed_flush_lsn = Some(Lsn(101));
        assert_eq!(
            verify_slot_continuity(&s, &cp()).unwrap_err().kind(),
            ErrorKind::ResyncRequired
        );
        let mut s = slot();
        s.wal_status = Some("lost".into());
        assert_eq!(
            verify_slot_continuity(&s, &cp()).unwrap_err().kind(),
            ErrorKind::ResyncRequired
        );
        let mut s = slot();
        s.invalidation_reason = Some("wal_removed".into());
        assert_eq!(
            verify_slot_continuity(&s, &cp()).unwrap_err().kind(),
            ErrorKind::ResyncRequired
        );
        let mut s = slot();
        s.plugin = Some("wal2json".into());
        assert_eq!(
            verify_slot_continuity(&s, &cp()).unwrap_err().kind(),
            ErrorKind::Preflight
        );
        let mut s = slot();
        s.database = Some("other".into());
        assert_eq!(
            verify_slot_continuity(&s, &cp()).unwrap_err().kind(),
            ErrorKind::Preflight
        );
    }
}
