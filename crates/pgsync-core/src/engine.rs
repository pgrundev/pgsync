//! The replication engine: reader task, applier loop, and reconnect policy.
//!
//! Guarantees:
//! - Events are applied in the order the source produced them.
//! - A source transaction is applied atomically together with its checkpoint.
//! - WAL positions are acknowledged only after the corresponding target commit returned.
//! - Redelivered transactions (after crash/reconnect) are skipped by LSN comparison.
//! - Memory held by undelivered events is bounded by the channel limits.

use crate::channel;
use crate::checkpoint::Checkpoint;
use crate::config::ResourcesConfig;
use crate::error::{Error, ErrorKind, Result};
use crate::event::SourceEvent;
use crate::lsn::Lsn;
use crate::metrics::Metrics;
use crate::retry::Backoff;
use crate::state::JobState;
use crate::traits::{EventStream, Feedback, ReplicationSink, SinkFactory, StreamFactory};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Position the reader reports to the server. Only `acked` is ever reported as flushed.
#[derive(Debug, Clone, Copy, Default)]
pub struct AckPosition {
    pub acked: Lsn,
}

pub struct ApplyOutcome {
    pub checkpoint: Checkpoint,
}

/// Runs one connected stream until cancellation or error.
///
/// `catchup_target` is the server WAL end observed when the stream was opened; once the
/// acknowledged position reaches it the job state becomes `STREAMING`.
pub async fn run_apply_loop<S, K>(
    stream: S,
    mut sink: K,
    mut cp: Checkpoint,
    catchup_target: Lsn,
    resources: &ResourcesConfig,
    metrics: Arc<Metrics>,
    cancel: CancellationToken,
) -> Result<ApplyOutcome>
where
    S: EventStream + 'static,
    K: ReplicationSink,
{
    cp.check_invariants().map_err(Error::internal)?;
    let (tx, mut rx) = channel::bounded::<SourceEvent>(
        resources.channel_capacity,
        resources.max_inflight_bytes as usize,
    );
    let (ack_tx, ack_rx) = watch::channel(AckPosition {
        acked: cp.acked_lsn,
    });
    let reader_cancel = cancel.child_token();
    let reader = tokio::spawn(reader_task(
        stream,
        tx,
        ack_rx,
        resources.feedback_interval,
        metrics.clone(),
        reader_cancel.clone(),
    ));

    metrics.set_state(cp.state);
    metrics.set_lsn(&metrics.committed_lsn, cp.committed_lsn);
    metrics.set_lsn(&metrics.acked_lsn, cp.acked_lsn);

    let result = applier_loop(
        &mut rx,
        &mut sink,
        &mut cp,
        catchup_target,
        &ack_tx,
        &metrics,
    )
    .await;

    // Stop the reader regardless of why we exited, then surface the most useful error.
    reader_cancel.cancel();
    rx.close();
    let reader_result = match reader.await {
        Ok(r) => r,
        Err(e) => Err(Error::internal(format!("reader task panicked: {e}"))),
    };

    let applier_err = result.err();
    let reader_err = reader_result.err();
    if applier_err.is_some() || reader_err.is_some() {
        let _ = sink.rollback().await;
    }
    match (applier_err, reader_err) {
        (None, None) => Ok(ApplyOutcome { checkpoint: cp }),
        // A real applier failure (not a cancellation) is the root cause.
        (Some(e), _) if e.kind() != ErrorKind::Cancelled => Err(e),
        // Otherwise the reader's failure explains why the channel closed.
        (_, Some(e)) => Err(e),
        // Applier saw the channel close with a transaction open, because we were cancelled.
        (Some(_), None) => Ok(ApplyOutcome { checkpoint: cp }),
    }
}

struct OpenTxn {
    final_lsn: Lsn,
    xid: u32,
    skipping: bool,
}

async fn applier_loop<K: ReplicationSink>(
    rx: &mut channel::Receiver<SourceEvent>,
    sink: &mut K,
    cp: &mut Checkpoint,
    catchup_target: Lsn,
    ack_tx: &watch::Sender<AckPosition>,
    metrics: &Metrics,
) -> Result<()> {
    let mut open: Option<OpenTxn> = None;
    let mut rows_in_txn: u64 = 0;

    while let Some(envelope) = rx.recv().await {
        let event = envelope.into_inner();
        metrics
            .inflight_items
            .store(0, std::sync::atomic::Ordering::Relaxed);
        match event {
            SourceEvent::Begin { final_lsn, xid, .. } => {
                if open.is_some() {
                    return Err(Error::internal(format!(
                        "BEGIN for xid {xid} while transaction {} is open",
                        open.as_ref().map(|o| o.xid).unwrap_or(0)
                    )));
                }
                let skipping = cp.already_applied(final_lsn);
                if skipping {
                    debug!(xid, %final_lsn, committed = %cp.commit_lsn, "skipping already applied transaction");
                    metrics.add(&metrics.txns_skipped, 1);
                } else {
                    sink.begin().await?;
                }
                rows_in_txn = 0;
                open = Some(OpenTxn {
                    final_lsn,
                    xid,
                    skipping,
                });
            }
            SourceEvent::Relation(rel) => {
                sink.relation(rel).await?;
            }
            SourceEvent::Op(op) => {
                let Some(txn) = open.as_ref() else {
                    return Err(Error::internal(format!(
                        "{} outside of a transaction",
                        op.kind()
                    )));
                };
                if !txn.skipping {
                    rows_in_txn += 1;
                    sink.apply(op).await?;
                }
            }
            SourceEvent::Commit {
                commit_lsn,
                end_lsn,
                commit_ts,
            } => {
                let Some(txn) = open.take() else {
                    return Err(Error::internal("COMMIT outside of a transaction"));
                };
                if txn.final_lsn != commit_lsn {
                    return Err(Error::internal(format!(
                        "COMMIT lsn {commit_lsn} does not match BEGIN final_lsn {}",
                        txn.final_lsn
                    )));
                }
                if txn.skipping {
                    // Already durable. Make sure the ack position covers it.
                    if cp.record_keepalive(end_lsn) {
                        sink.save_checkpoint(cp).await?;
                    }
                } else {
                    cp.record_commit(commit_lsn, end_lsn, txn.xid, commit_ts);
                    if cp.state == JobState::CatchingUp && cp.acked_lsn >= catchup_target {
                        cp.state = JobState::Streaming;
                        info!(lsn = %cp.committed_lsn, "caught up; now streaming");
                    }
                    sink.commit(cp).await?;
                    metrics.add(&metrics.txns_applied, 1);
                    metrics.add(&metrics.rows_applied, rows_in_txn);
                    metrics.set_lsn(&metrics.committed_lsn, cp.committed_lsn);
                    metrics.set_state(cp.state);
                }
                metrics.set_lsn(&metrics.acked_lsn, cp.acked_lsn);
                ack_tx.send_replace(AckPosition {
                    acked: cp.acked_lsn,
                });
            }
            SourceEvent::Keepalive { wal_end } => {
                if open.is_some() {
                    continue;
                }
                let mut changed = cp.record_keepalive(wal_end);
                if cp.state == JobState::CatchingUp && cp.acked_lsn >= catchup_target {
                    cp.state = JobState::Streaming;
                    info!(lsn = %cp.acked_lsn, "caught up; now streaming");
                    changed = true;
                }
                if changed {
                    sink.save_checkpoint(cp).await?;
                    metrics.set_lsn(&metrics.acked_lsn, cp.acked_lsn);
                    metrics.set_state(cp.state);
                    ack_tx.send_replace(AckPosition {
                        acked: cp.acked_lsn,
                    });
                }
            }
        }
    }
    if let Some(txn) = open {
        if !txn.skipping {
            warn!(xid = txn.xid, "stream ended mid-transaction; rolling back");
        }
        return Err(Error::cancelled());
    }
    Ok(())
}

async fn reader_task<S: EventStream>(
    mut stream: S,
    tx: channel::Sender<SourceEvent>,
    ack_rx: watch::Receiver<AckPosition>,
    feedback_interval: Duration,
    metrics: Arc<Metrics>,
    cancel: CancellationToken,
) -> Result<()> {
    let mut ticker = tokio::time::interval(feedback_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await; // first tick completes immediately; skip it
    let mut last_received = Lsn::ZERO;
    // Position last reported while reading. Unsolicited reports are sent only when it
    // changed: a server that hears from us every tick never sends the keepalive that
    // carries the position of WAL it skipped (unpublished tables, vacuum), and the slot
    // would never advance over it while the publication is idle.
    let mut last_sent_ack = Lsn::ZERO;

    let feedback = |ack: &watch::Receiver<AckPosition>, last_received: Lsn, reply: bool| {
        let acked = ack.borrow().acked;
        Feedback {
            written: last_received.max(acked),
            flushed: acked,
            applied: acked,
            reply_requested: reply,
        }
    };

    loop {
        let event = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                // Final progress report so the slot reflects everything durable.
                let _ = stream.send_feedback(feedback(&ack_rx, last_received, false)).await;
                return Ok(());
            }
            _ = ticker.tick() => {
                let fb = feedback(&ack_rx, last_received, false);
                if fb.flushed != last_sent_ack {
                    stream.send_feedback(fb).await?;
                    last_sent_ack = fb.flushed;
                    metrics.add(&metrics.feedback_sent, 1);
                }
                continue;
            }
            r = stream.next_event() => r?,
        };

        metrics.add(&metrics.events_received, 1);
        metrics.add(&metrics.bytes_received, event.byte_len() as u64);
        match &event {
            SourceEvent::Commit { end_lsn, .. } => {
                last_received = last_received.max(*end_lsn);
                metrics.set_lsn(&metrics.last_received_lsn, *end_lsn);
            }
            SourceEvent::Keepalive { wal_end } => {
                last_received = last_received.max(*wal_end);
                metrics.set_lsn(&metrics.server_wal_end, *wal_end);
                // Answer keepalives promptly (the server may have requested a reply) with
                // the last durable position.
                let fb = feedback(&ack_rx, last_received, false);
                stream.send_feedback(fb).await?;
                last_sent_ack = fb.flushed;
                metrics.add(&metrics.feedback_sent, 1);
            }
            _ => {}
        }

        let bytes = event.byte_len();
        let permit = loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    let _ = stream.send_feedback(feedback(&ack_rx, last_received, false)).await;
                    return Ok(());
                }
                _ = ticker.tick() => {
                    // Applier is slow (backpressure) and the socket is not being read, so
                    // server pings cannot be answered: send an unconditional heartbeat with
                    // the last durable position.
                    let fb = feedback(&ack_rx, last_received, false);
                    stream.send_feedback(fb).await?;
                    last_sent_ack = fb.flushed;
                    metrics.add(&metrics.feedback_sent, 1);
                }
                p = tx.reserve(bytes) => match p {
                    Ok(p) => break p,
                    Err(_) => return Ok(()), // applier gone; it reports the reason
                },
            }
        };
        metrics
            .inflight_items
            .store(tx.inflight_items(), std::sync::atomic::Ordering::Relaxed);
        permit.send(event);
    }
}

/// Reconnecting driver: opens a stream and a sink, runs the apply loop, and on transient
/// failures reloads the checkpoint from the sink (the source of truth) and reconnects.
pub async fn run_replication<SF, KF>(
    job_id: &str,
    mut streams: SF,
    mut sinks: KF,
    resources: &ResourcesConfig,
    metrics: Arc<Metrics>,
    cancel: CancellationToken,
) -> Result<Checkpoint>
where
    SF: StreamFactory,
    SF::Stream: 'static,
    KF: SinkFactory,
{
    let mut backoff = Backoff::new(&resources.retry_backoff);
    loop {
        if cancel.is_cancelled() {
            return Err(Error::cancelled());
        }
        let attempt = async {
            let mut sink = sinks.connect().await?;
            let cp = sink
                .load_checkpoint(job_id)
                .await?
                .ok_or_else(|| Error::config(format!("job {job_id:?} has no checkpoint; run `pgsync bootstrap` first")))?;
            if !cp.state.is_resumable() {
                return Err(Error::new(
                    if cp.state == JobState::ResyncRequired { ErrorKind::ResyncRequired } else { ErrorKind::Preflight },
                    format!("job {job_id:?} is in state {} and cannot be resumed", cp.state),
                ));
            }
            let mut cp = cp;
            if matches!(cp.state, JobState::Error | JobState::Paused) {
                cp.state = JobState::CatchingUp;
                cp.last_error = None;
            }
            let (stream, wal_end) = streams.connect(&cp).await?;
            metrics.set_lsn(&metrics.server_wal_end, wal_end);
            info!(from = %cp.committed_lsn, acked = %cp.acked_lsn, server_wal_end = %wal_end, state = %cp.state, "replication stream opened");
            let outcome = run_apply_loop(stream, sink, cp, wal_end, resources, metrics.clone(), cancel.clone()).await?;
            Ok::<_, Error>(outcome.checkpoint)
        }
        .await;

        match attempt {
            Ok(cp) => return Ok(cp),
            Err(e) if e.is_retryable() && !cancel.is_cancelled() => {
                let Some(delay) = backoff.next_delay() else {
                    error!(error = %e, "retry attempts exhausted");
                    return Err(e);
                };
                metrics.add(&metrics.reconnects, 1);
                warn!(error = %e, attempt = backoff.attempt(), delay_ms = delay.as_millis() as u64, "transient failure; reconnecting");
                tokio::select! {
                    _ = cancel.cancelled() => return Err(Error::cancelled()),
                    _ = tokio::time::sleep(delay) => {}
                }
            }
            Err(e) => {
                // Record the failure in the target when we can; never advance positions.
                if let Ok(mut sink) = sinks.connect().await
                    && let Ok(Some(mut cp)) = sink.load_checkpoint(job_id).await
                {
                    cp.state = match e.kind() {
                        ErrorKind::ResyncRequired => JobState::ResyncRequired,
                        ErrorKind::Cancelled => cp.state,
                        _ => JobState::Error,
                    };
                    if e.kind() != ErrorKind::Cancelled {
                        cp.last_error = Some(truncate(&e.to_string(), 1000));
                    }
                    let _ = sink.save_checkpoint(&cp).await;
                }
                return Err(e);
            }
        }
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_owned()
    } else {
        format!("{}…", &s[..max])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Op, Relation, ReplicaIdentity, Tuple, Value};
    use std::collections::VecDeque;
    use std::sync::Mutex;

    // ---- fakes -------------------------------------------------------------------

    #[derive(Clone)]
    enum Script {
        Event(SourceEvent),
        Fail(ErrorKind),
    }

    struct FakeStream {
        script: VecDeque<Script>,
        feedback: Arc<Mutex<Vec<Feedback>>>,
        fail_at: Option<tokio::time::Instant>,
    }

    impl FakeStream {
        fn new(script: Vec<Script>, feedback: Arc<Mutex<Vec<Feedback>>>) -> Self {
            FakeStream {
                script: script.into(),
                feedback,
                fail_at: None,
            }
        }
    }

    impl EventStream for FakeStream {
        // Cancel-safe: nothing is consumed until the result is actually returned.
        async fn next_event(&mut self) -> Result<SourceEvent> {
            match self.script.front().cloned() {
                Some(Script::Event(e)) => {
                    self.script.pop_front();
                    Ok(e)
                }
                Some(Script::Fail(kind)) => {
                    // Give the applier and the feedback ticker time to run first.
                    let at = *self.fail_at.get_or_insert_with(|| {
                        tokio::time::Instant::now() + Duration::from_millis(60)
                    });
                    tokio::time::sleep_until(at).await;
                    self.script.pop_front();
                    self.fail_at = None;
                    Err(Error::new(kind, "scripted failure"))
                }
                None => std::future::pending().await,
            }
        }
        async fn send_feedback(&mut self, fb: Feedback) -> Result<()> {
            self.feedback.lock().unwrap().push(fb);
            Ok(())
        }
    }

    #[derive(Default)]
    struct SinkLog {
        calls: Vec<String>,
        /// Checkpoints as they were committed (durable).
        committed: Vec<Checkpoint>,
        saved: Vec<Checkpoint>,
        fail_commit_at: Option<usize>,
        fail_commit_kind: Option<ErrorKind>,
        durable: Option<Checkpoint>,
    }

    #[derive(Clone)]
    struct FakeSink(Arc<Mutex<SinkLog>>);

    impl ReplicationSink for FakeSink {
        async fn load_checkpoint(&mut self, _job: &str) -> Result<Option<Checkpoint>> {
            Ok(self.0.lock().unwrap().durable.clone())
        }
        async fn save_checkpoint(&mut self, cp: &Checkpoint) -> Result<()> {
            let mut l = self.0.lock().unwrap();
            l.calls.push(format!("save {}", cp.acked_lsn.0));
            l.saved.push(cp.clone());
            l.durable = Some(cp.clone());
            Ok(())
        }
        async fn begin(&mut self) -> Result<()> {
            self.0.lock().unwrap().calls.push("begin".into());
            Ok(())
        }
        async fn relation(&mut self, rel: Arc<Relation>) -> Result<()> {
            self.0
                .lock()
                .unwrap()
                .calls
                .push(format!("relation {}", rel.id));
            Ok(())
        }
        async fn apply(&mut self, op: Op) -> Result<()> {
            self.0
                .lock()
                .unwrap()
                .calls
                .push(format!("apply {}", op.kind()));
            Ok(())
        }
        async fn commit(&mut self, cp: &Checkpoint) -> Result<()> {
            let mut l = self.0.lock().unwrap();
            let n = l.committed.len();
            if l.fail_commit_at == Some(n) {
                l.calls.push("commit FAILED".into());
                return Err(Error::new(
                    l.fail_commit_kind.unwrap_or(ErrorKind::Transient),
                    "commit failed",
                ));
            }
            l.calls.push(format!("commit {}", cp.committed_lsn.0));
            l.committed.push(cp.clone());
            l.durable = Some(cp.clone());
            Ok(())
        }
        async fn rollback(&mut self) -> Result<()> {
            self.0.lock().unwrap().calls.push("rollback".into());
            Ok(())
        }
    }

    fn begin(lsn: u64, xid: u32) -> Script {
        Script::Event(SourceEvent::Begin {
            final_lsn: Lsn(lsn),
            commit_ts: 1,
            xid,
        })
    }
    fn commit(lsn: u64) -> Script {
        Script::Event(SourceEvent::Commit {
            commit_lsn: Lsn(lsn),
            end_lsn: Lsn(lsn + 8),
            commit_ts: 1,
        })
    }
    fn insert() -> Script {
        Script::Event(SourceEvent::Op(Op::Insert {
            relation: 1,
            new: Tuple {
                values: vec![Value::text("1")],
            },
        }))
    }
    fn keepalive(lsn: u64) -> Script {
        Script::Event(SourceEvent::Keepalive { wal_end: Lsn(lsn) })
    }
    fn relation() -> Script {
        Script::Event(SourceEvent::Relation(Arc::new(Relation {
            id: 1,
            schema: "public".into(),
            name: "t".into(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![],
        })))
    }

    fn resources() -> ResourcesConfig {
        ResourcesConfig {
            feedback_interval: Duration::from_millis(20),
            channel_capacity: 4,
            max_inflight_bytes: 1 << 20,
            ..Default::default()
        }
    }

    fn checkpoint(state: JobState) -> Checkpoint {
        let mut cp = Checkpoint::new_job("job", "sys", "db", "slot", "pub");
        cp.state = state;
        cp
    }

    async fn run(
        script: Vec<Script>,
        cp: Checkpoint,
        sink: FakeSink,
    ) -> (Result<ApplyOutcome>, Arc<Mutex<Vec<Feedback>>>) {
        let feedback = Arc::new(Mutex::new(vec![]));
        let stream = FakeStream::new(script, feedback.clone());
        let cancel = CancellationToken::new();
        let metrics = Arc::new(Metrics::new());
        let res = tokio::time::timeout(
            Duration::from_secs(5),
            run_apply_loop(stream, sink, cp, Lsn(1000), &resources(), metrics, cancel),
        )
        .await
        .expect("apply loop should finish");
        (res, feedback)
    }

    // ---- tests -------------------------------------------------------------------

    #[tokio::test]
    async fn applies_transactions_in_order_and_acks_after_commit() {
        let sink = FakeSink(Default::default());
        let script = vec![
            relation(),
            begin(100, 1),
            insert(),
            insert(),
            commit(100),
            begin(200, 2),
            insert(),
            commit(200),
            Script::Fail(ErrorKind::Transient),
        ];
        let (res, feedback) = run(script, checkpoint(JobState::Streaming), sink.clone()).await;
        assert_eq!(res.err().map(|e| e.kind()), Some(ErrorKind::Transient));
        let log = sink.0.lock().unwrap();
        assert_eq!(
            log.calls,
            vec![
                "relation 1",
                "begin",
                "apply insert",
                "apply insert",
                "commit 108",
                "begin",
                "apply insert",
                "commit 208",
                "rollback",
            ]
        );
        assert_eq!(log.committed[0].commit_lsn, Lsn(100));
        assert_eq!(log.committed[1].committed_lsn, Lsn(208));
        assert_eq!(log.committed[1].acked_lsn, Lsn(208));
        assert_eq!(log.committed[1].txns_applied, 2);
        // Periodic feedback carries the durable position only.
        let fb = feedback.lock().unwrap();
        assert!(fb.iter().all(|f| f.flushed <= Lsn(208)));
        assert_eq!(fb.last().unwrap().flushed, Lsn(208));
    }

    #[tokio::test]
    async fn skips_redelivered_transactions_without_touching_sink() {
        let sink = FakeSink(Default::default());
        let mut cp = checkpoint(JobState::Streaming);
        cp.record_commit(Lsn(200), Lsn(208), 2, 1);
        let script = vec![
            begin(100, 1),
            insert(),
            commit(100), // older: skip
            begin(200, 2),
            insert(),
            commit(200), // equal: skip
            begin(300, 3),
            insert(),
            commit(300), // new: apply
            Script::Fail(ErrorKind::Transient),
        ];
        let (_, _) = run(script, cp, sink.clone()).await;
        let log = sink.0.lock().unwrap();
        assert_eq!(
            log.calls,
            vec!["begin", "apply insert", "commit 308", "rollback"]
        );
        assert_eq!(log.committed[0].txns_applied, 2);
    }

    #[tokio::test]
    async fn keepalive_between_transactions_persists_and_advances_ack() {
        let sink = FakeSink(Default::default());
        let script = vec![
            begin(100, 1),
            insert(),
            keepalive(104),
            commit(100),    // mid-txn keepalive ignored
            keepalive(500), // idle: persisted
            keepalive(400), // regress: ignored
            Script::Fail(ErrorKind::Transient),
        ];
        let (_, feedback) = run(script, checkpoint(JobState::Streaming), sink.clone()).await;
        let log = sink.0.lock().unwrap();
        assert_eq!(
            log.calls,
            vec![
                "begin",
                "apply insert",
                "commit 108",
                "save 500",
                "rollback"
            ]
        );
        assert_eq!(log.saved[0].acked_lsn, Lsn(500));
        assert_eq!(log.saved[0].committed_lsn, Lsn(108));
        assert_eq!(feedback.lock().unwrap().last().unwrap().flushed, Lsn(500));
    }

    #[tokio::test]
    async fn catching_up_becomes_streaming_once_target_reached() {
        let sink = FakeSink(Default::default());
        let script = vec![
            begin(100, 1),
            commit(100),
            begin(2000, 2),
            commit(2000),
            Script::Fail(ErrorKind::Transient),
        ];
        let (_, _) = run(script, checkpoint(JobState::CatchingUp), sink.clone()).await;
        let log = sink.0.lock().unwrap();
        assert_eq!(log.committed[0].state, JobState::CatchingUp);
        assert_eq!(log.committed[1].state, JobState::Streaming);
    }

    #[tokio::test]
    async fn commit_failure_rolls_back_and_does_not_ack() {
        let sink = FakeSink(Default::default());
        sink.0.lock().unwrap().fail_commit_at = Some(0);
        sink.0.lock().unwrap().fail_commit_kind = Some(ErrorKind::Unsupported);
        let script = vec![
            begin(100, 1),
            insert(),
            commit(100),
            begin(200, 2),
            commit(200),
        ];
        let (res, feedback) = run(script, checkpoint(JobState::Streaming), sink.clone()).await;
        assert_eq!(res.err().map(|e| e.kind()), Some(ErrorKind::Unsupported));
        let log = sink.0.lock().unwrap();
        assert_eq!(
            log.calls,
            vec!["begin", "apply insert", "commit FAILED", "rollback"]
        );
        assert!(log.committed.is_empty());
        assert!(
            feedback
                .lock()
                .unwrap()
                .iter()
                .all(|f| f.flushed == Lsn::ZERO)
        );
    }

    #[tokio::test]
    async fn protocol_violations_are_internal_errors() {
        let sink = FakeSink(Default::default());
        let (res, _) = run(
            vec![insert()],
            checkpoint(JobState::Streaming),
            sink.clone(),
        )
        .await;
        assert_eq!(res.err().map(|e| e.kind()), Some(ErrorKind::Internal));
        let (res, _) = run(
            vec![begin(1, 1), begin(2, 2)],
            checkpoint(JobState::Streaming),
            sink.clone(),
        )
        .await;
        assert_eq!(res.err().map(|e| e.kind()), Some(ErrorKind::Internal));
        let (res, _) = run(
            vec![begin(100, 1), commit(101)],
            checkpoint(JobState::Streaming),
            sink,
        )
        .await;
        assert_eq!(res.err().map(|e| e.kind()), Some(ErrorKind::Internal));
    }

    #[tokio::test]
    async fn idle_reader_reports_only_changed_positions() {
        // One keepalive, then silence: the position is reported when it changes, never on
        // every tick, so the server will ping us with the skipped-WAL position.
        let sink = FakeSink(Default::default());
        let feedback = Arc::new(Mutex::new(vec![]));
        let stream = FakeStream::new(vec![keepalive(500)], feedback.clone());
        let cancel = CancellationToken::new();
        let handle = tokio::spawn(run_apply_loop(
            stream,
            sink,
            checkpoint(JobState::Streaming),
            Lsn(1000),
            &*Box::leak(Box::new(resources())),
            Arc::new(Metrics::new()),
            cancel.clone(),
        ));
        tokio::time::sleep(Duration::from_millis(200)).await; // ~10 ticks at 20ms
        cancel.cancel();
        let _ = handle.await;
        let fb = feedback.lock().unwrap();
        // keepalive answer + one tick after the applier persisted 500 + final report on
        // cancel; never one per tick.
        assert!(fb.len() <= 3, "unsolicited feedback every tick: {fb:?}");
        assert_eq!(fb.last().unwrap().flushed, Lsn(500));
    }

    #[tokio::test]
    async fn feedback_keeps_flowing_while_applier_is_blocked() {
        // Sink that blocks on apply forever: the reader must still send periodic feedback
        // with the last durable position while the channel is full.
        struct BlockingSink(FakeSink);
        impl ReplicationSink for BlockingSink {
            async fn load_checkpoint(&mut self, j: &str) -> Result<Option<Checkpoint>> {
                self.0.load_checkpoint(j).await
            }
            async fn save_checkpoint(&mut self, cp: &Checkpoint) -> Result<()> {
                self.0.save_checkpoint(cp).await
            }
            async fn begin(&mut self) -> Result<()> {
                self.0.begin().await
            }
            async fn relation(&mut self, r: Arc<Relation>) -> Result<()> {
                self.0.relation(r).await
            }
            async fn apply(&mut self, _op: Op) -> Result<()> {
                std::future::pending().await
            }
            async fn commit(&mut self, cp: &Checkpoint) -> Result<()> {
                self.0.commit(cp).await
            }
            async fn rollback(&mut self) -> Result<()> {
                self.0.rollback().await
            }
        }
        let feedback = Arc::new(Mutex::new(vec![]));
        let mut script: Vec<Script> = vec![begin(100, 1)];
        script.extend(std::iter::repeat_n(insert(), 50));
        let stream = FakeStream::new(script, feedback.clone());
        let cancel = CancellationToken::new();
        let metrics = Arc::new(Metrics::new());
        let mut cp = checkpoint(JobState::Streaming);
        cp.record_commit(Lsn(50), Lsn(58), 1, 1);
        let handle = tokio::spawn(run_apply_loop(
            stream,
            BlockingSink(FakeSink(Default::default())),
            cp,
            Lsn(1000),
            &*Box::leak(Box::new(resources())),
            metrics.clone(),
            cancel.clone(),
        ));
        tokio::time::sleep(Duration::from_millis(150)).await;
        let sent = feedback.lock().unwrap().len();
        assert!(
            sent >= 3,
            "expected periodic feedback under backpressure, got {sent}"
        );
        assert!(
            feedback
                .lock()
                .unwrap()
                .iter()
                .all(|f| f.flushed == Lsn(58))
        );
        assert!(
            metrics.snapshot().inflight_items >= 3,
            "channel should be backed up"
        );
        cancel.cancel();
        // The applier is stuck in `apply`; cancellation is observed by the reader, which
        // closes the channel. The blocked applier can only be unblocked by the sink itself,
        // so we abort the task here as a process shutdown would.
        handle.abort();
    }

    // ---- reconnect driver ----------------------------------------------------------

    struct ScriptedStreams {
        rounds: VecDeque<Vec<Script>>,
        feedback: Arc<Mutex<Vec<Feedback>>>,
        connects: Vec<Lsn>,
    }
    impl StreamFactory for ScriptedStreams {
        type Stream = FakeStream;
        async fn connect(&mut self, cp: &Checkpoint) -> Result<(FakeStream, Lsn)> {
            self.connects.push(cp.committed_lsn);
            let script = self
                .rounds
                .pop_front()
                .unwrap_or_else(|| vec![Script::Fail(ErrorKind::Config)]);
            Ok((FakeStream::new(script, self.feedback.clone()), Lsn(1000)))
        }
    }
    struct Sinks(FakeSink);
    impl SinkFactory for Sinks {
        type Sink = FakeSink;
        async fn connect(&mut self) -> Result<FakeSink> {
            Ok(self.0.clone())
        }
    }

    #[tokio::test]
    async fn reconnects_after_transient_error_and_resumes_from_durable_checkpoint() {
        let sink = FakeSink(Default::default());
        let mut cp = checkpoint(JobState::Streaming);
        cp.record_commit(Lsn(10), Lsn(18), 0, 0);
        sink.0.lock().unwrap().durable = Some(cp);
        let streams = ScriptedStreams {
            rounds: vec![
                // Round 1: one txn applied, then connection drops mid second txn.
                vec![
                    begin(100, 1),
                    insert(),
                    commit(100),
                    begin(200, 2),
                    insert(),
                    Script::Fail(ErrorKind::Transient),
                ],
                // Round 2: server redelivers from its confirmed position.
                vec![
                    begin(100, 1),
                    insert(),
                    commit(100),
                    begin(200, 2),
                    insert(),
                    commit(200),
                    Script::Fail(ErrorKind::Config),
                ],
            ]
            .into(),
            feedback: Default::default(),
            connects: vec![],
        };
        let resources = ResourcesConfig {
            retry_backoff: crate::config::RetryBackoff {
                initial: Duration::from_millis(5),
                max: Duration::from_millis(5),
                multiplier: 1.0,
                max_attempts: 0,
            },
            ..resources()
        };
        let metrics = Arc::new(Metrics::new());
        let res = run_replication(
            "job",
            streams,
            Sinks(sink.clone()),
            &resources,
            metrics.clone(),
            CancellationToken::new(),
        )
        .await;
        assert_eq!(res.err().map(|e| e.kind()), Some(ErrorKind::Config));
        let log = sink.0.lock().unwrap();
        assert_eq!(
            log.calls,
            vec![
                "begin",
                "apply insert",
                "commit 108",
                "begin",
                "apply insert",
                "rollback",
                "begin",
                "apply insert",
                "commit 208",
                "rollback",
                "save 208", // final state recorded as ERROR
            ]
        );
        assert_eq!(log.durable.as_ref().unwrap().state, JobState::Error);
        assert!(log.durable.as_ref().unwrap().last_error.is_some());
        assert_eq!(metrics.snapshot().reconnects, 1);
        assert_eq!(metrics.snapshot().txns_skipped, 1);
    }

    #[tokio::test]
    async fn refuses_to_run_without_checkpoint_or_in_resync_state() {
        let sink = FakeSink(Default::default());
        let streams = ScriptedStreams {
            rounds: VecDeque::new(),
            feedback: Default::default(),
            connects: vec![],
        };
        let res = run_replication(
            "job",
            streams,
            Sinks(sink.clone()),
            &resources(),
            Arc::new(Metrics::new()),
            CancellationToken::new(),
        )
        .await;
        assert_eq!(res.err().map(|e| e.kind()), Some(ErrorKind::Config));

        sink.0.lock().unwrap().durable = Some(checkpoint(JobState::ResyncRequired));
        let streams = ScriptedStreams {
            rounds: VecDeque::new(),
            feedback: Default::default(),
            connects: vec![],
        };
        let res = run_replication(
            "job",
            streams,
            Sinks(sink),
            &resources(),
            Arc::new(Metrics::new()),
            CancellationToken::new(),
        )
        .await;
        assert_eq!(res.err().map(|e| e.kind()), Some(ErrorKind::ResyncRequired));
    }
}
