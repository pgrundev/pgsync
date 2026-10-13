//! Process-wide counters, cheap to update from any task and snapshot-able as JSON.

use crate::lsn::Lsn;
use crate::state::JobState;
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[derive(Default)]
pub struct Metrics {
    pub events_received: AtomicU64,
    pub bytes_received: AtomicU64,
    pub txns_applied: AtomicU64,
    pub txns_skipped: AtomicU64,
    pub rows_applied: AtomicU64,
    pub rows_copied: AtomicU64,
    pub bytes_copied: AtomicU64,
    pub feedback_sent: AtomicU64,
    pub reconnects: AtomicU64,
    pub last_received_lsn: AtomicU64,
    pub server_wal_end: AtomicU64,
    pub committed_lsn: AtomicU64,
    pub acked_lsn: AtomicU64,
    pub inflight_items: AtomicU64,
    pub inflight_bytes: AtomicU64,
    /// Unix microseconds of the last target commit (0 = none yet).
    pub last_apply_unix_micros: AtomicU64,
    state: AtomicU64,
    connection: AtomicU64,
    started: Option<Instant>,
}

/// State of the source replication connection as seen by the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionState {
    /// No stream opened yet, or the job is not running.
    Disconnected,
    Connected,
    /// A transient failure is being retried with backoff.
    Reconnecting,
}

const CONNECTIONS: [ConnectionState; 3] = [
    ConnectionState::Disconnected,
    ConnectionState::Connected,
    ConnectionState::Reconnecting,
];

#[derive(Debug, Clone, Serialize)]
pub struct MetricsSnapshot {
    pub state: JobState,
    pub uptime_secs: u64,
    pub events_received: u64,
    pub bytes_received: u64,
    pub txns_applied: u64,
    pub txns_skipped: u64,
    pub rows_applied: u64,
    pub rows_copied: u64,
    pub bytes_copied: u64,
    pub feedback_sent: u64,
    pub reconnects: u64,
    pub last_received_lsn: Lsn,
    pub server_wal_end: Lsn,
    pub committed_lsn: Lsn,
    pub acked_lsn: Lsn,
    /// `server_wal_end - acked_lsn`, bytes: WAL the server has produced that this job has
    /// not yet durably covered (applied, or acknowledged as carrying nothing for it).
    pub apply_lag_bytes: u64,
    pub inflight_items: u64,
    pub inflight_bytes: u64,
    pub rss_bytes: Option<u64>,
    pub last_apply_unix_micros: Option<u64>,
    pub connection: ConnectionState,
}

const STATES: [JobState; 7] = [
    JobState::Initializing,
    JobState::Snapshotting,
    JobState::CatchingUp,
    JobState::Streaming,
    JobState::Paused,
    JobState::Error,
    JobState::ResyncRequired,
];

impl Metrics {
    pub fn new() -> Self {
        Metrics {
            started: Some(Instant::now()),
            ..Default::default()
        }
    }

    pub fn set_state(&self, s: JobState) {
        let idx = STATES.iter().position(|x| *x == s).unwrap_or(0) as u64;
        self.state.store(idx, Ordering::Relaxed);
    }

    pub fn state(&self) -> JobState {
        STATES[self.state.load(Ordering::Relaxed) as usize % STATES.len()]
    }

    pub fn set_connection(&self, c: ConnectionState) {
        let idx = CONNECTIONS.iter().position(|x| *x == c).unwrap_or(0) as u64;
        self.connection.store(idx, Ordering::Relaxed);
    }

    pub fn connection(&self) -> ConnectionState {
        CONNECTIONS[self.connection.load(Ordering::Relaxed) as usize % CONNECTIONS.len()]
    }

    pub fn mark_applied_now(&self) {
        let micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);
        self.last_apply_unix_micros.store(micros, Ordering::Relaxed);
    }

    pub fn add(&self, counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Ordering::Relaxed);
    }

    pub fn set_lsn(&self, slot: &AtomicU64, lsn: Lsn) {
        slot.fetch_max(lsn.0, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let g = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let wal_end = g(&self.server_wal_end);
        let committed = g(&self.committed_lsn);
        let acked = g(&self.acked_lsn);
        MetricsSnapshot {
            state: self.state(),
            uptime_secs: self.started.map(|s| s.elapsed().as_secs()).unwrap_or(0),
            events_received: g(&self.events_received),
            bytes_received: g(&self.bytes_received),
            txns_applied: g(&self.txns_applied),
            txns_skipped: g(&self.txns_skipped),
            rows_applied: g(&self.rows_applied),
            rows_copied: g(&self.rows_copied),
            bytes_copied: g(&self.bytes_copied),
            feedback_sent: g(&self.feedback_sent),
            reconnects: g(&self.reconnects),
            last_received_lsn: Lsn(g(&self.last_received_lsn)),
            server_wal_end: Lsn(wal_end),
            committed_lsn: Lsn(committed),
            acked_lsn: Lsn(acked),
            apply_lag_bytes: wal_end.saturating_sub(acked.max(committed)),
            inflight_items: g(&self.inflight_items),
            inflight_bytes: g(&self.inflight_bytes),
            rss_bytes: process_rss_bytes(),
            last_apply_unix_micros: match g(&self.last_apply_unix_micros) {
                0 => None,
                v => Some(v),
            },
            connection: self.connection(),
        }
    }
}

/// Resident set size of this process, if the platform exposes it cheaply.
pub fn process_rss_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
        let pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
        return Some(pages * 4096);
    }
    #[cfg(target_os = "macos")]
    {
        // `ps` is cheap enough for periodic status output and avoids an FFI dependency.
        let out = std::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &std::process::id().to_string()])
            .output()
            .ok()?;
        let kb: u64 = String::from_utf8_lossy(&out.stdout).trim().parse().ok()?;
        return Some(kb * 1024);
    }
    #[allow(unreachable_code)]
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_reflects_counters_and_lag() {
        let m = Metrics::new();
        m.set_state(JobState::Streaming);
        m.add(&m.txns_applied, 3);
        m.set_lsn(&m.server_wal_end, Lsn(1000));
        m.set_lsn(&m.committed_lsn, Lsn(400));
        m.set_lsn(&m.committed_lsn, Lsn(300)); // never regresses
        let s = m.snapshot();
        assert_eq!(s.state, JobState::Streaming);
        assert_eq!(s.txns_applied, 3);
        assert_eq!(s.committed_lsn, Lsn(400));
        assert_eq!(s.apply_lag_bytes, 600);
        // Keepalive-acknowledged WAL (nothing for this publication) is not lag.
        m.set_lsn(&m.acked_lsn, Lsn(900));
        assert_eq!(m.snapshot().apply_lag_bytes, 100);
        let json = serde_json::to_value(&s).unwrap();
        assert_eq!(json["state"], "STREAMING");
    }

    #[test]
    fn rss_is_reported_on_supported_platforms() {
        if cfg!(any(target_os = "linux", target_os = "macos")) {
            assert!(process_rss_bytes().unwrap() > 0);
        }
    }
}
