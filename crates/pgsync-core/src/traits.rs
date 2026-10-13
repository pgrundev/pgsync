//! Boundaries between the engine and database-specific adapters.

use crate::Result;
use crate::checkpoint::Checkpoint;
use crate::event::{Op, Relation, SourceEvent};
use crate::lsn::Lsn;
use std::future::Future;
use std::sync::Arc;

/// Standby status update sent to the source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Feedback {
    pub written: Lsn,
    pub flushed: Lsn,
    pub applied: Lsn,
    pub reply_requested: bool,
}

/// An open logical replication stream.
pub trait EventStream: Send {
    /// Next event in commit order. Must be cancel-safe: dropping the future must not
    /// lose or corrupt a partially received message.
    fn next_event(&mut self) -> impl Future<Output = Result<SourceEvent>> + Send;

    /// Report progress to the server. Only positions that are durable in the target may be
    /// reported as flushed.
    fn send_feedback(&mut self, fb: Feedback) -> impl Future<Output = Result<()>> + Send;
}

/// Produces streams; called again after a transient failure.
pub trait StreamFactory: Send {
    type Stream: EventStream;
    /// Open a stream starting at `from` (the server may start later, at its confirmed
    /// flush position). Returns the stream and the server's current WAL end.
    fn connect(&mut self, from: Lsn) -> impl Future<Output = Result<(Self::Stream, Lsn)>> + Send;
}

/// Applies transactions to a destination and persists checkpoints with them.
pub trait ReplicationSink: Send {
    /// Load the job's checkpoint; `None` if the job has never been bootstrapped.
    fn load_checkpoint(
        &mut self,
        job_id: &str,
    ) -> impl Future<Output = Result<Option<Checkpoint>>> + Send;

    /// Persist a checkpoint outside of any data transaction (autocommit).
    fn save_checkpoint(&mut self, cp: &Checkpoint) -> impl Future<Output = Result<()>> + Send;

    /// Start a destination transaction.
    fn begin(&mut self) -> impl Future<Output = Result<()>> + Send;

    /// Relation metadata; may arrive inside or outside a transaction and must be cached.
    fn relation(&mut self, rel: Arc<Relation>) -> impl Future<Output = Result<()>> + Send;

    /// Apply one change inside the open transaction. Implementations may batch.
    fn apply(&mut self, op: Op) -> impl Future<Output = Result<()>> + Send;

    /// Flush pending changes, write `cp`, and commit atomically. After `Ok`, the data and
    /// the checkpoint are durable.
    fn commit(&mut self, cp: &Checkpoint) -> impl Future<Output = Result<()>> + Send;

    /// Abort the open transaction, if any. Best effort; must not panic.
    fn rollback(&mut self) -> impl Future<Output = Result<()>> + Send;
}

/// Produces sinks; called again after a transient failure.
pub trait SinkFactory: Send {
    type Sink: ReplicationSink;
    fn connect(&mut self) -> impl Future<Output = Result<Self::Sink>> + Send;
}
