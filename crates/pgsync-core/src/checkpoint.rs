//! Durable replication progress.

use crate::lsn::Lsn;
use crate::state::JobState;
use serde::{Deserialize, Serialize};

/// Everything needed to identify a job's source and resume it safely.
///
/// Invariants maintained by the engine:
/// - `commit_lsn <= committed_lsn <= acked_lsn`
/// - the slot's `confirmed_flush_lsn` never exceeds `acked_lsn`
/// - `committed_lsn` only moves inside the target transaction that applied the data
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    pub job_id: String,
    pub state: JobState,
    /// `IDENTIFY_SYSTEM.systemid` of the source cluster.
    pub source_system_id: String,
    pub source_dbname: String,
    pub slot: String,
    pub publication: String,
    /// Whether PGSync created the slot (and may therefore drop it in `cleanup`).
    pub slot_owned: bool,
    /// `consistent_point` returned at slot creation.
    pub consistent_point: Option<Lsn>,
    /// `commit_lsn` of the last applied transaction (`Begin.final_lsn` of a redelivered
    /// transaction is compared against this).
    pub commit_lsn: Lsn,
    /// `end_lsn` of the last applied transaction.
    pub committed_lsn: Lsn,
    /// Highest position reported to the server as flushed.
    pub acked_lsn: Lsn,
    pub last_xid: Option<u32>,
    /// PostgreSQL timestamp (µs since 2000-01-01) of the last applied commit.
    pub last_commit_ts: Option<i64>,
    pub txns_applied: u64,
    pub last_error: Option<String>,
}

impl Checkpoint {
    pub fn new_job(
        job_id: &str,
        source_system_id: &str,
        source_dbname: &str,
        slot: &str,
        publication: &str,
    ) -> Self {
        Checkpoint {
            job_id: job_id.to_owned(),
            state: JobState::Initializing,
            source_system_id: source_system_id.to_owned(),
            source_dbname: source_dbname.to_owned(),
            slot: slot.to_owned(),
            publication: publication.to_owned(),
            slot_owned: false,
            consistent_point: None,
            commit_lsn: Lsn::ZERO,
            committed_lsn: Lsn::ZERO,
            acked_lsn: Lsn::ZERO,
            last_xid: None,
            last_commit_ts: None,
            txns_applied: 0,
            last_error: None,
        }
    }

    /// True when a transaction whose commit record is at `final_lsn` has already been
    /// applied and must be skipped.
    pub fn already_applied(&self, final_lsn: Lsn) -> bool {
        final_lsn <= self.commit_lsn
    }

    /// Record a transaction commit. Keeps the invariants.
    pub fn record_commit(&mut self, commit_lsn: Lsn, end_lsn: Lsn, xid: u32, commit_ts: i64) {
        debug_assert!(commit_lsn > self.commit_lsn);
        self.commit_lsn = commit_lsn;
        self.committed_lsn = self.committed_lsn.max(end_lsn);
        self.acked_lsn = self.acked_lsn.max(self.committed_lsn);
        self.last_xid = Some(xid);
        self.last_commit_ts = Some(commit_ts);
        self.txns_applied += 1;
    }

    /// Record a keepalive position as safe to acknowledge. Returns true if it advanced.
    pub fn record_keepalive(&mut self, wal_end: Lsn) -> bool {
        if wal_end > self.acked_lsn {
            self.acked_lsn = wal_end;
            true
        } else {
            false
        }
    }

    pub fn check_invariants(&self) -> Result<(), String> {
        if self.commit_lsn > self.committed_lsn {
            return Err(format!(
                "commit_lsn {} > committed_lsn {}",
                self.commit_lsn, self.committed_lsn
            ));
        }
        if self.committed_lsn > self.acked_lsn {
            return Err(format!(
                "committed_lsn {} > acked_lsn {}",
                self.committed_lsn, self.acked_lsn
            ));
        }
        Ok(())
    }
}

/// Persists [`Checkpoint`]s. The PostgreSQL implementation stores them in the target.
pub trait CheckpointStore: Send {
    fn load(
        &mut self,
        job_id: &str,
    ) -> impl Future<Output = crate::Result<Option<Checkpoint>>> + Send;
    fn save(&mut self, cp: &Checkpoint) -> impl Future<Output = crate::Result<()>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cp() -> Checkpoint {
        Checkpoint::new_job("j", "sys", "db", "slot", "pub")
    }

    #[test]
    fn commit_advances_all_positions() {
        let mut c = cp();
        c.record_commit(Lsn(100), Lsn(120), 7, 1);
        assert_eq!(c.commit_lsn, Lsn(100));
        assert_eq!(c.committed_lsn, Lsn(120));
        assert_eq!(c.acked_lsn, Lsn(120));
        assert_eq!(c.txns_applied, 1);
        c.check_invariants().unwrap();
        assert!(c.already_applied(Lsn(100)));
        assert!(c.already_applied(Lsn(50)));
        assert!(!c.already_applied(Lsn(101)));
    }

    #[test]
    fn keepalive_only_moves_ack_forward() {
        let mut c = cp();
        c.record_commit(Lsn(100), Lsn(120), 7, 1);
        assert!(c.record_keepalive(Lsn(500)));
        assert_eq!(c.acked_lsn, Lsn(500));
        assert!(!c.record_keepalive(Lsn(400)));
        assert_eq!(c.committed_lsn, Lsn(120));
        c.check_invariants().unwrap();
        // A later commit below the acked position keeps acked where it is.
        c.record_commit(Lsn(300), Lsn(320), 8, 2);
        assert_eq!(c.acked_lsn, Lsn(500));
        c.check_invariants().unwrap();
    }

    #[test]
    fn serde_round_trip() {
        let mut c = cp();
        c.record_commit(Lsn(0x1_0000_0010), Lsn(0x1_0000_0020), 9, 123);
        let json = serde_json::to_string(&c).unwrap();
        assert!(json.contains("\"1/10\""));
        let back: Checkpoint = serde_json::from_str(&json).unwrap();
        assert_eq!(back, c);
    }
}
