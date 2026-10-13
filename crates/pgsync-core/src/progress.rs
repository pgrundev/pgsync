//! Human-facing progress reporting, decoupled from logging.
//!
//! The engine and the bootstrap flow call these hooks; the CLI renders them for a
//! terminal. The default implementation does nothing, so library users pay nothing.

use crate::lsn::Lsn;
use std::sync::Arc;
use std::time::Duration;

/// One applied source transaction, summarised with bounded memory.
#[derive(Debug, Clone, Default)]
pub struct TxnSummary {
    pub xid: u32,
    pub end_lsn: Lsn,
    /// Per (table, operation) counts, in first-seen order.
    pub ops: Vec<OpCount>,
    /// Up to `MAX_KEYS` `table key=value` samples, only when key reporting is enabled.
    pub keys: Vec<String>,
    pub rows: u64,
    /// Time from BEGIN to COMMIT on the target.
    pub apply_time: Duration,
}

#[derive(Debug, Clone)]
pub struct OpCount {
    pub table: String,
    pub kind: &'static str,
    pub count: u64,
}

pub const MAX_KEYS: usize = 8;
pub const MAX_OP_GROUPS: usize = 64;

impl TxnSummary {
    pub fn record(&mut self, table: &str, kind: &'static str) {
        self.rows += 1;
        if let Some(c) = self
            .ops
            .iter_mut()
            .find(|c| c.table == table && c.kind == kind)
        {
            c.count += 1;
        } else if self.ops.len() < MAX_OP_GROUPS {
            self.ops.push(OpCount {
                table: table.to_owned(),
                kind,
                count: 1,
            });
        }
    }

    pub fn record_key(&mut self, sample: String) {
        if self.keys.len() < MAX_KEYS {
            self.keys.push(sample);
        }
    }
}

pub trait Progress: Send + Sync {
    /// A completed setup step (`✓ ...`).
    fn step(&self, _message: &str) {}
    /// A table copy started.
    fn copy_started(&self, _table: &str) {}
    /// A table copy finished.
    fn copy_done(&self, _table: &str, _rows: u64, _bytes: u64, _elapsed: Duration) {}
    /// Streaming started at `from`.
    fn streaming(&self, _from: Lsn) {}
    /// A transaction was committed on the target.
    fn txn(&self, _summary: &TxnSummary) {}
    /// A redelivered transaction was skipped.
    fn skipped(&self, _xid: u32, _end_lsn: Lsn) {}
    /// Whether key samples should be collected (costs allocations per row).
    fn wants_keys(&self) -> bool {
        false
    }
}

pub struct NoProgress;
impl Progress for NoProgress {}

pub type ProgressHandle = Arc<dyn Progress>;

pub fn none() -> ProgressHandle {
    Arc::new(NoProgress)
}
