//! Job orchestration: bootstrap (snapshot + handoff), run, status, cleanup, and the WAL
//! safety monitor.

use crate::client::{self, PgClient};
use crate::errors::from_pg;
use crate::replication::conn::ReplicationConnection;
use crate::replication::slot::{create_slot_with_snapshot, identify_system};
use crate::replication::stream::PgStreamFactory;
use crate::schema;
use crate::sink::{self, PgSink, SinkLimits};
use crate::snapshot;
use crate::source::{self, PreflightMode, PreflightReport, SourcePreflight};
use crate::url::ConnectionInfo;
use pgsync_core::checkpoint::Checkpoint;
use pgsync_core::config::{Config, ReplicationMode, SchemaMode, TableName, WalRetentionAction};
use pgsync_core::engine::run_replication;
use pgsync_core::metrics::Metrics;
use pgsync_core::state::JobState;
use pgsync_core::traits::SinkFactory;
use pgsync_core::{Error, ErrorKind, Lsn, Result};
use serde::Serialize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// Everything a command needs.
pub struct JobContext {
    pub config: Config,
    pub source: ConnectionInfo,
    pub target: ConnectionInfo,
    pub metrics: Arc<Metrics>,
    pub cancel: CancellationToken,
    pub progress: pgsync_core::progress::ProgressHandle,
}

impl JobContext {
    pub fn new(config: Config, cancel: CancellationToken) -> Result<Self> {
        let source = ConnectionInfo::from_env(&config.source.url_env, &config.safety)?;
        let target = ConnectionInfo::from_env(&config.target.url_env, &config.safety)?;
        Ok(JobContext {
            config,
            source,
            target,
            metrics: Arc::new(Metrics::new()),
            cancel,
            progress: pgsync_core::progress::none(),
        })
    }

    fn timeout(&self) -> Duration {
        self.config.resources.connection_timeout
    }

    async fn source_client(&self, name: &str) -> Result<PgClient> {
        client::connect(&self.source, &self.config.safety, self.timeout(), name).await
    }

    async fn target_client(&self, name: &str) -> Result<PgClient> {
        client::connect(&self.target, &self.config.safety, self.timeout(), name).await
    }

    fn limits(&self) -> SinkLimits {
        SinkLimits {
            max_batch_rows: self.config.resources.max_batch_rows,
            max_batch_bytes: self.config.resources.max_batch_bytes as usize,
        }
    }

    pub fn stream_factory(&self) -> PgStreamFactory {
        PgStreamFactory {
            info: self.source.clone(),
            safety: self.config.safety.clone(),
            timeout: self.timeout(),
            slot: self.config.source.slot.clone(),
            publication: self.config.source.publication.clone(),
        }
    }
}

pub struct PgSinkFactory {
    pub target: ConnectionInfo,
    pub safety: pgsync_core::config::SafetyConfig,
    pub timeout: Duration,
    pub max_batch_rows: usize,
    pub max_batch_bytes: usize,
}

impl SinkFactory for PgSinkFactory {
    type Sink = PgSink;
    async fn connect(&mut self) -> Result<PgSink> {
        let c = client::connect(&self.target, &self.safety, self.timeout, "pgsync-apply").await?;
        PgSink::new(
            c,
            SinkLimits {
                max_batch_rows: self.max_batch_rows,
                max_batch_bytes: self.max_batch_bytes,
            },
        )
        .await
    }
}

// ---- check -------------------------------------------------------------------------

#[derive(Serialize)]
pub struct CheckOutput {
    pub source: PreflightReport,
    pub target: PreflightReport,
    pub ok: bool,
}

pub async fn check(
    ctx: &JobContext,
    mode: PreflightMode,
) -> Result<(CheckOutput, Option<SourcePreflight>)> {
    let mut target_report = PreflightReport::default();
    let src = match ctx.source_client("pgsync-check").await {
        Ok(c) => Some(c),
        Err(e) => {
            let mut r = PreflightReport::default();
            r.fail("source_connection", e.to_string());
            return Ok((
                CheckOutput {
                    source: r,
                    target: target_report,
                    ok: false,
                },
                None,
            ));
        }
    };
    let src = src.unwrap();
    let mut pre = source::preflight(
        &src,
        &ctx.config.source.publication,
        &ctx.config.source.slot,
        &ctx.config.replication.tables,
        mode,
    )
    .await?;
    // Replication protocol connectivity (pg_hba, REPLICATION attribute in practice).
    match ReplicationConnection::connect(&ctx.source, &ctx.config.safety, ctx.timeout()).await {
        Ok(mut conn) => match identify_system(&mut conn).await {
            Ok(sys) => {
                pre.report.pass(
                    "replication_connection",
                    format!("system {} timeline {}", sys.system_id, sys.timeline),
                );
                conn.close().await;
            }
            Err(e) => pre.report.fail("replication_connection", e.to_string()),
        },
        Err(e) => pre.report.fail(
            "replication_connection",
            format!(
                "{e} (check pg_hba.conf allows replication connections for this role/database)"
            ),
        ),
    }
    match session_tls(&src).await {
        Ok(true) => pre
            .report
            .pass("source_tls", format!("encrypted ({:?})", ctx.source.tls)),
        Ok(false) if ctx.source.is_loopback => {
            pre.report.warn("source_tls", "plain connection (loopback)")
        }
        Ok(false) => pre.report.fail("source_tls", "connection is not encrypted"),
        Err(e) => pre
            .report
            .warn("source_tls", format!("cannot determine: {e}")),
    }

    match ctx.target_client("pgsync-check").await {
        Ok(tgt) => {
            let row = tgt
                .query_one("SELECT version(), current_setting('server_version_num')::int, (SELECT rolsuper FROM pg_roles WHERE rolname = current_user)", &[])
                .await
                .map_err(|e| from_pg("target version", e))?;
            let version: String = row.get(0);
            let vnum: i32 = row.get(1);
            let superuser: bool = row.get(2);
            if vnum < 140000 {
                target_report.fail("target_version", version);
            } else {
                target_report.pass("target_version", version);
            }
            if superuser {
                target_report.pass(
                    "target_role",
                    "superuser (session_replication_role = replica available)",
                );
            } else {
                target_report.warn("target_role", "not a superuser: target triggers and FK checks stay active during apply; tables must be ordered parent-first");
            }
            match tgt
                .batch_execute("CREATE SCHEMA IF NOT EXISTS pgsync")
                .await
            {
                Ok(()) => target_report.pass("target_pgsync_schema", "writable"),
                Err(e) => target_report.fail(
                    "target_pgsync_schema",
                    from_pg("create schema", e).to_string(),
                ),
            }
            if ctx.config.schema.mode == SchemaMode::PgDump {
                let bin = ctx
                    .config
                    .schema
                    .pg_dump_path
                    .clone()
                    .unwrap_or_else(|| "pg_dump".into());
                match tokio::process::Command::new(&bin)
                    .arg("--version")
                    .output()
                    .await
                {
                    Ok(o) if o.status.success() => target_report.pass(
                        "pg_dump",
                        String::from_utf8_lossy(&o.stdout).trim().to_owned(),
                    ),
                    _ => target_report.fail(
                        "pg_dump",
                        format!(
                            "{bin} not runnable (set schema.pg_dump_path or schema.mode: existing)"
                        ),
                    ),
                }
            }
            for t in &ctx.config.replication.tables {
                match schema::target_columns(&tgt, &t.schema, &t.name).await? {
                    Some(cols) => {
                        if ctx.config.schema.mode == SchemaMode::PgDump
                            && mode == PreflightMode::Bootstrap
                        {
                            target_report.fail(&format!("target table {t}"), "already exists; schema.mode is pg_dump (use --restart-snapshot to drop and recreate, or schema.mode: existing)");
                        } else if let Some(info) = pre.tables.iter().find(|i| &i.table == t) {
                            match schema::verify_columns(t, &info.columns, &cols) {
                                Ok(w) if w.is_empty() => target_report
                                    .pass(&format!("target table {t}"), "columns match"),
                                Ok(w) => {
                                    target_report.warn(&format!("target table {t}"), w.join("; "))
                                }
                                Err(e) => target_report
                                    .fail(&format!("target table {t}"), e.message().to_owned()),
                            }
                        }
                    }
                    None => {
                        if ctx.config.schema.mode == SchemaMode::Existing {
                            target_report.fail(
                                &format!("target table {t}"),
                                "does not exist and schema.mode is existing",
                            );
                        } else if mode == PreflightMode::Run {
                            target_report
                                .fail(&format!("target table {t}"), "does not exist on the target");
                        } else {
                            target_report.pass(
                                &format!("target table {t}"),
                                "will be created by pg_dump schema bootstrap",
                            );
                        }
                    }
                }
            }
            match session_tls(&tgt).await {
                Ok(true) => {
                    target_report.pass("target_tls", format!("encrypted ({:?})", ctx.target.tls))
                }
                Ok(false) if ctx.target.is_loopback => {
                    target_report.warn("target_tls", "plain connection (loopback)")
                }
                Ok(false) => target_report.fail("target_tls", "connection is not encrypted"),
                Err(e) => target_report.warn("target_tls", format!("cannot determine: {e}")),
            }
        }
        Err(e) => target_report.fail("target_connection", e.to_string()),
    }
    let ok = !pre.report.failed() && !target_report.failed();
    Ok((
        CheckOutput {
            source: pre.report.clone(),
            target: target_report,
            ok,
        },
        Some(pre),
    ))
}

async fn session_tls(c: &PgClient) -> Result<bool> {
    let row = c
        .query_one(
            "SELECT coalesce((SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()), false)",
            &[],
        )
        .await
        .map_err(|e| from_pg("pg_stat_ssl", e))?;
    Ok(row.get(0))
}

fn preflight_error(out: &CheckOutput) -> Error {
    let mut lines = Vec::new();
    for c in out.source.failures() {
        lines.push(format!("source {}: {}", c.name, c.detail));
    }
    for c in out.target.failures() {
        lines.push(format!("target {}: {}", c.name, c.detail));
    }
    Error::preflight(format!("preflight failed:\n  {}", lines.join("\n  ")))
}

// ---- bootstrap ---------------------------------------------------------------------

pub async fn bootstrap(ctx: &JobContext, restart_snapshot: bool) -> Result<()> {
    let cfg = &ctx.config;
    let job_id = cfg.job.id.clone();
    ctx.metrics.set_state(JobState::Initializing);

    // Existing job?
    let tgt = ctx.target_client("pgsync-bootstrap").await?;
    tgt.batch_execute(sink::SCHEMA_SQL)
        .await
        .map_err(|e| from_pg("create pgsync schema", e))?;
    let mut probe = PgSink::new(ctx.target_client("pgsync-bootstrap").await?, ctx.limits()).await?;
    let existing =
        pgsync_core::traits::ReplicationSink::load_checkpoint(&mut probe, &job_id).await?;
    drop(probe);
    if let Some(cp) = &existing {
        if !restart_snapshot {
            return Err(Error::config(format!(
                "job {job_id:?} already exists on the target in state {}. Use `pgsync run` to resume, \
                 `pgsync bootstrap --restart-snapshot` to discard the incomplete copy and start over, \
                 or `pgsync cleanup` to remove it.",
                cp.state
            )));
        }
        info!(state = %cp.state, "restarting snapshot: discarding previous copy");
        restart_cleanup(ctx, &tgt, Some(cp)).await?;
    } else if restart_snapshot {
        info!("restarting snapshot: no job metadata; resetting target tables only");
        restart_cleanup(ctx, &tgt, None).await?;
    }

    // Preflight (never modifies the source).
    let (out, pre) = check(ctx, PreflightMode::Bootstrap).await?;
    if !out.ok {
        return Err(preflight_error(&out));
    }
    let pre = pre.expect("preflight present when ok");
    let src_system_id = pre.server.system_identifier.clone();
    let warns = out
        .source
        .checks
        .iter()
        .chain(out.target.checks.iter())
        .filter(|c| c.status == source::CheckStatus::Warn)
        .count();
    ctx.progress.step(&format!(
        "connected to source {} · system {}",
        short_version(&pre.server.version),
        src_system_id
    ));
    ctx.progress.step(&format!(
        "publication {} · {} tables",
        cfg.source.publication,
        pre.tables.len()
    ));
    ctx.progress.step(&format!(
        "preflight: {} checks passed{}",
        out.source.checks.len() + out.target.checks.len(),
        if warns > 0 {
            format!(" ({warns} warnings)")
        } else {
            String::new()
        }
    ));

    // Slot + exported snapshot. The replication connection must stay idle until all copy
    // sessions have imported the snapshot.
    let mut repl = ReplicationConnection::connect(&ctx.source, &cfg.safety, ctx.timeout()).await?;
    let sys = identify_system(&mut repl).await?;
    if sys.system_id != src_system_id {
        return Err(Error::internal(
            "system identifier differs between SQL and replication connections",
        ));
    }
    let created = create_slot_with_snapshot(&mut repl, &cfg.source.slot).await?;
    info!(slot = %created.slot_name, consistent_point = %created.consistent_point, snapshot = %created.snapshot_name, "replication slot created with exported snapshot");
    ctx.progress.step(&format!(
        "replication slot {} created (pgoutput) · snapshot at {}",
        created.slot_name, created.consistent_point
    ));

    let mut cp = Checkpoint::new_job(
        &job_id,
        &src_system_id,
        &pre.server.dbname,
        &cfg.source.slot,
        &cfg.source.publication,
    );
    cp.state = JobState::Snapshotting;
    cp.slot_owned = true;
    cp.consistent_point = Some(created.consistent_point);
    cp.commit_lsn = created.consistent_point;
    cp.committed_lsn = created.consistent_point;
    cp.acked_lsn = created.consistent_point;
    sink::upsert_checkpoint(&tgt, &cp).await?;
    ctx.metrics.set_state(JobState::Snapshotting);

    let snapshot_result = run_snapshot(ctx, &tgt, &pre, &created.snapshot_name).await;
    // The exported snapshot is no longer needed once the copies have committed (or failed).
    repl.close().await;
    match snapshot_result {
        Ok(()) => {}
        Err(e) => {
            error!(error = %e, "snapshot failed; job stays in SNAPSHOTTING and needs `bootstrap --restart-snapshot`");
            let _ =
                sink::set_job_state(&tgt, &job_id, JobState::Snapshotting, Some(&e.to_string()))
                    .await;
            return Err(e);
        }
    }

    cp.state = JobState::CatchingUp;
    sink::upsert_checkpoint(&tgt, &cp).await?;
    info!(consistent_point = %created.consistent_point, "snapshot complete; switching to CDC");
    drop(tgt);

    if cfg.replication.mode == ReplicationMode::CdcOnly {
        return Ok(());
    }
    run(ctx).await.map(|_| ())
}

async fn restart_cleanup(ctx: &JobContext, tgt: &PgClient, cp: Option<&Checkpoint>) -> Result<()> {
    if let Some(cp) = cp
        && cp.slot_owned
    {
        let src = ctx.source_client("pgsync-bootstrap").await?;
        if source::slot_info(&src, &cp.slot).await?.is_some() {
            src.execute("SELECT pg_drop_replication_slot($1)", &[&cp.slot])
                .await
                .map_err(|e| from_pg("drop slot", e))?;
            info!(slot = %cp.slot, "dropped replication slot owned by this job");
        }
    }
    for t in &ctx.config.replication.tables {
        let sql = match ctx.config.schema.mode {
            SchemaMode::PgDump => format!("DROP TABLE IF EXISTS {} CASCADE", t.quoted()),
            SchemaMode::Existing => format!("TRUNCATE TABLE {} CASCADE", t.quoted()),
        };
        tgt.batch_execute(&sql)
            .await
            .map_err(|e| from_pg(&format!("reset target table {t}"), e))?;
    }
    if let Some(cp) = cp {
        sink::delete_job(tgt, &cp.job_id).await?;
    }
    Ok(())
}

async fn run_snapshot(
    ctx: &JobContext,
    tgt: &PgClient,
    pre: &SourcePreflight,
    snapshot_name: &str,
) -> Result<()> {
    let cfg = &ctx.config;
    let started = Instant::now();

    if cfg.schema.mode == SchemaMode::PgDump {
        let sql = schema::dump_schema(
            cfg.schema.pg_dump_path.as_deref(),
            &ctx.source,
            &cfg.safety,
            &cfg.replication.tables,
        )
        .await?;
        schema::apply_schema(tgt, &cfg.replication.tables, &sql).await?;
        info!("target schema created from pg_dump");
        ctx.progress.step(&format!(
            "schema: {} tables created on target (pg_dump)",
            cfg.replication.tables.len()
        ));
    }
    for info in &pre.tables {
        let cols = schema::target_columns(tgt, &info.table.schema, &info.table.name)
            .await?
            .ok_or_else(|| {
                Error::preflight(format!(
                    "{} missing on target after schema bootstrap",
                    info.table
                ))
            })?;
        for w in schema::verify_columns(&info.table, &info.columns, &cols)? {
            warn!("{w}");
        }
    }

    // Copy tables with bounded parallelism. Each worker owns one source connection (with
    // the exported snapshot) and one target connection (one transaction per table).
    let sem = Arc::new(Semaphore::new(cfg.resources.max_parallel_copies));
    let mut set: JoinSet<Result<(TableName, snapshot::CopyStats)>> = JoinSet::new();
    let deadline = tokio::time::Instant::now() + cfg.resources.snapshot_timeout;
    let buffer = (cfg.resources.buffer_size_kb * 1024) as usize;
    let job_id = cfg.job.id.clone();
    for info in &pre.tables {
        let table = info.table.clone();
        let columns = info.column_names();
        let sem = sem.clone();
        let source = ctx.source.clone();
        let target = ctx.target.clone();
        let safety = cfg.safety.clone();
        let timeout = ctx.timeout();
        let metrics = ctx.metrics.clone();
        let snapshot_name = snapshot_name.to_owned();
        let cancel = ctx.cancel.clone();
        let job_id = job_id.clone();
        let progress = ctx.progress.clone();
        set.spawn(async move {
            let _permit = sem.acquire().await.map_err(|_| Error::cancelled())?;
            if cancel.is_cancelled() {
                return Err(Error::cancelled());
            }
            let src = client::connect(&source, &safety, timeout, "pgsync-copy").await?;
            snapshot::import_snapshot(&src, &snapshot_name).await?;
            let dst = client::connect(&target, &safety, timeout, "pgsync-copy").await?;
            if let Err(e) = dst
                .batch_execute("SET session_replication_role = replica")
                .await
            {
                warn!(error = %e, %table, "copying with triggers and FK checks active");
            }
            dst.batch_execute("BEGIN")
                .await
                .map_err(|e| from_pg("BEGIN copy", e))?;
            progress.copy_started(&table.to_string());
            let t0 = Instant::now();
            let stats = tokio::select! {
                r = snapshot::copy_table(&src, &dst, &table, &columns, buffer, &metrics) => r?,
                _ = cancel.cancelled() => return Err(Error::cancelled()),
            };
            PgSink::record_table_copied(
                &dst,
                &job_id,
                &table.schema,
                &table.name,
                stats.rows,
                stats.bytes,
            )
            .await?;
            dst.batch_execute("COMMIT")
                .await
                .map_err(|e| from_pg("COMMIT copy", e))?;
            src.batch_execute("COMMIT")
                .await
                .map_err(|e| from_pg("end snapshot txn", e))?;
            info!(%table, rows = stats.rows, bytes = stats.bytes, "copied");
            progress.copy_done(&table.to_string(), stats.rows, stats.bytes, t0.elapsed());
            Ok((table, stats))
        });
    }
    let mut total_rows = 0u64;
    let mut total_bytes = 0u64;
    while let Some(res) = tokio::select! {
        r = set.join_next() => r,
        _ = tokio::time::sleep_until(deadline) => {
            set.abort_all();
            return Err(Error::resource(format!("snapshot exceeded snapshot_timeout ({:?})", cfg.resources.snapshot_timeout)));
        }
    } {
        match res {
            Ok(Ok((_, stats))) => {
                total_rows += stats.rows;
                total_bytes += stats.bytes;
            }
            Ok(Err(e)) => {
                set.abort_all();
                return Err(e);
            }
            Err(e) => {
                set.abort_all();
                return Err(Error::internal(format!("copy task failed: {e}")));
            }
        }
    }
    let secs = started.elapsed().as_secs_f64();
    info!(
        tables = pre.tables.len(),
        rows = total_rows,
        bytes = total_bytes,
        seconds = format!("{secs:.1}"),
        mb_per_s = format!("{:.1}", total_bytes as f64 / 1e6 / secs.max(0.001)),
        "initial copy finished"
    );
    ctx.progress.step(&format!(
        "initial copy complete: {} rows in {secs:.1}s",
        group_digits(total_rows)
    ));
    Ok(())
}

fn short_version(v: &str) -> String {
    // "PostgreSQL 18.6 (Debian ...) on ..." -> "PostgreSQL 18.6"
    v.split(" (")
        .next()
        .unwrap_or(v)
        .split(" on ")
        .next()
        .unwrap_or(v)
        .to_owned()
}

pub fn group_digits(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out
}

// ---- run -----------------------------------------------------------------------------

pub async fn run(ctx: &JobContext) -> Result<Checkpoint> {
    let cfg = &ctx.config;
    let monitor_cancel = ctx.cancel.child_token();
    let monitor_reason: Arc<Mutex<Option<Error>>> = Arc::new(Mutex::new(None));
    let monitor = tokio::spawn(monitor_task(
        ctx.source.clone(),
        cfg.safety.clone(),
        ctx.timeout(),
        cfg.source.slot.clone(),
        cfg.safety.wal_retention_alarm_bytes,
        cfg.resources.max_replication_lag_bytes,
        cfg.safety.wal_retention_action,
        cfg.safety.monitor_interval,
        ctx.metrics.clone(),
        monitor_cancel.clone(),
        ctx.cancel.clone(),
        monitor_reason.clone(),
    ));
    let reporter = tokio::spawn(report_task(ctx.metrics.clone(), monitor_cancel.clone()));

    let result = run_replication(
        &cfg.job.id,
        ctx.stream_factory(),
        PgSinkFactory {
            target: ctx.target.clone(),
            safety: cfg.safety.clone(),
            timeout: ctx.timeout(),
            max_batch_rows: cfg.resources.max_batch_rows,
            max_batch_bytes: cfg.resources.max_batch_bytes as usize,
        },
        &cfg.resources,
        ctx.metrics.clone(),
        ctx.cancel.clone(),
        ctx.progress.clone(),
    )
    .await;
    monitor_cancel.cancel();
    let _ = monitor.await;
    let _ = reporter.await;

    let monitor_reason = monitor_reason.lock().unwrap().take();
    if let Some(reason) = monitor_reason {
        // The monitor stopped the job: surface its reason and record the state.
        if let Ok(tgt) = ctx.target_client("pgsync-run").await {
            let state = if reason.kind() == ErrorKind::ResyncRequired {
                JobState::ResyncRequired
            } else {
                JobState::Paused
            };
            let _ = sink::set_job_state(&tgt, &cfg.job.id, state, Some(&reason.to_string())).await;
        }
        return Err(reason);
    }
    match result {
        Ok(cp) => {
            info!(committed = %cp.committed_lsn, acked = %cp.acked_lsn, "replication stopped cleanly");
            Ok(cp)
        }
        Err(e) if e.kind() == ErrorKind::Cancelled => {
            // Cancelled before the loop ran (e.g. during connect). Reload the checkpoint.
            let mut s = PgSink::new(ctx.target_client("pgsync-run").await?, ctx.limits()).await?;
            pgsync_core::traits::ReplicationSink::load_checkpoint(&mut s, &cfg.job.id)
                .await?
                .ok_or_else(|| Error::config("job has no checkpoint"))
        }
        Err(e) => Err(e),
    }
}

#[allow(clippy::too_many_arguments)]
async fn monitor_task(
    source: ConnectionInfo,
    safety: pgsync_core::config::SafetyConfig,
    timeout: Duration,
    slot: String,
    retention_alarm: u64,
    lag_alarm: u64,
    action: WalRetentionAction,
    interval: Duration,
    metrics: Arc<Metrics>,
    cancel: CancellationToken,
    job_cancel: CancellationToken,
    reason: Arc<Mutex<Option<Error>>>,
) {
    let mut client: Option<PgClient> = None;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(interval) => {}
        }
        if client.is_none() {
            match client::connect(&source, &safety, timeout, "pgsync-monitor").await {
                Ok(c) => client = Some(c),
                Err(e) => {
                    warn!(error = %e, "monitor: cannot connect to source");
                    continue;
                }
            }
        }
        let c = client.as_ref().unwrap();
        let (slot_info, current) = match (
            source::slot_info(c, &slot).await,
            source::current_wal_lsn(c).await,
        ) {
            (Ok(s), Ok(l)) => (s, l),
            (Err(e), _) | (_, Err(e)) => {
                warn!(error = %e, "monitor: source query failed");
                client = None;
                continue;
            }
        };
        metrics.set_lsn(&metrics.server_wal_end, current);
        let Some(s) = slot_info else {
            let e = Error::resync_required(format!("replication slot {slot:?} disappeared"));
            error!("{e}");
            *reason.lock().unwrap() = Some(e);
            job_cancel.cancel();
            return;
        };
        if s.wal_status.as_deref() == Some("lost") || s.invalidation_reason.is_some() {
            let e = Error::resync_required(format!(
                "replication slot {slot:?} invalidated (wal_status={:?}, reason={:?})",
                s.wal_status, s.invalidation_reason
            ));
            error!("{e}");
            *reason.lock().unwrap() = Some(e);
            job_cancel.cancel();
            return;
        }
        let retained = s.restart_lsn.map(|l| current.bytes_since(l)).unwrap_or(0);
        let lag = s
            .confirmed_flush_lsn
            .map(|l| current.bytes_since(l))
            .unwrap_or(0);
        let snap = metrics.snapshot();
        info!(
            slot = %slot,
            active = s.active,
            wal_status = ?s.wal_status,
            retained_wal_bytes = retained,
            slot_lag_bytes = lag,
            apply_lag_bytes = snap.apply_lag_bytes,
            safe_wal_size = ?s.safe_wal_size,
            "wal monitor"
        );
        if lag > lag_alarm {
            warn!(
                slot_lag_bytes = lag,
                threshold = lag_alarm,
                "ALARM: replication lag exceeds max_replication_lag_bytes"
            );
        }
        if retained > retention_alarm {
            warn!(
                retained_wal_bytes = retained,
                threshold = retention_alarm,
                "ALARM: WAL retained by the slot exceeds safety.wal_retention_alarm_bytes"
            );
            if action == WalRetentionAction::Stop {
                let e = Error::resource(format!(
                    "WAL retention {retained} bytes exceeds safety.wal_retention_alarm_bytes ({retention_alarm}); stopping as configured. \
                     Stopping does not release WAL: resume with `pgsync run`, or `pgsync cleanup` to drop the slot."
                ));
                error!("{e}");
                *reason.lock().unwrap() = Some(e);
                job_cancel.cancel();
                return;
            }
        }
    }
}

async fn report_task(metrics: Arc<Metrics>, cancel: CancellationToken) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(Duration::from_secs(30)) => {}
        }
        let s = metrics.snapshot();
        info!(
            state = %s.state,
            txns_applied = s.txns_applied,
            rows_applied = s.rows_applied,
            events = s.events_received,
            bytes_received = s.bytes_received,
            committed_lsn = %s.committed_lsn,
            acked_lsn = %s.acked_lsn,
            apply_lag_bytes = s.apply_lag_bytes,
            inflight_items = s.inflight_items,
            rss_mb = s.rss_bytes.map(|b| b / (1024 * 1024)),
            reconnects = s.reconnects,
            "status"
        );
    }
}

// ---- status / cleanup ----------------------------------------------------------------

#[derive(Serialize)]
pub struct StatusOutput {
    pub job: Option<Checkpoint>,
    pub slot: Option<crate::replication::slot::SlotInfo>,
    pub source_wal_lsn: Option<Lsn>,
    pub slot_lag_bytes: Option<u64>,
    pub retained_wal_bytes: Option<u64>,
    pub tables: Vec<TableSyncRow>,
}

#[derive(Serialize)]
pub struct TableSyncRow {
    pub table: String,
    pub state: String,
    pub rows_copied: i64,
    pub bytes_copied: i64,
}

pub async fn status(ctx: &JobContext) -> Result<StatusOutput> {
    let tgt = ctx.target_client("pgsync-status").await?;
    let mut s = PgSink::new(tgt, ctx.limits()).await?;
    let job =
        pgsync_core::traits::ReplicationSink::load_checkpoint(&mut s, &ctx.config.job.id).await?;
    let rows = s
        .client()
        .query(
            "SELECT schema_name || '.' || table_name, state, rows_copied, bytes_copied FROM pgsync.table_sync WHERE job_id = $1 ORDER BY 1",
            &[&ctx.config.job.id],
        )
        .await
        .map_err(|e| from_pg("table_sync", e))?;
    let tables = rows
        .iter()
        .map(|r| TableSyncRow {
            table: r.get(0),
            state: r.get(1),
            rows_copied: r.get(2),
            bytes_copied: r.get(3),
        })
        .collect();
    let (slot, source_wal_lsn) = match ctx.source_client("pgsync-status").await {
        Ok(src) => (
            source::slot_info(&src, &ctx.config.source.slot).await?,
            Some(source::current_wal_lsn(&src).await?),
        ),
        Err(e) => {
            warn!(error = %e, "source unreachable");
            (None, None)
        }
    };
    let slot_lag_bytes = match (&slot, source_wal_lsn) {
        (Some(s), Some(cur)) => s.confirmed_flush_lsn.map(|l| cur.bytes_since(l)),
        _ => None,
    };
    let retained_wal_bytes = match (&slot, source_wal_lsn) {
        (Some(s), Some(cur)) => s.restart_lsn.map(|l| cur.bytes_since(l)),
        _ => None,
    };
    Ok(StatusOutput {
        job,
        slot,
        source_wal_lsn,
        slot_lag_bytes,
        retained_wal_bytes,
        tables,
    })
}

/// Drop the slot PGSync owns and delete the job metadata. Target data tables are kept.
pub async fn cleanup(ctx: &JobContext) -> Result<Vec<String>> {
    let mut actions = Vec::new();
    let tgt = ctx.target_client("pgsync-cleanup").await?;
    let mut s = PgSink::new(tgt, ctx.limits()).await?;
    let job =
        pgsync_core::traits::ReplicationSink::load_checkpoint(&mut s, &ctx.config.job.id).await?;
    let slot_owned =
        job.as_ref().map(|j| j.slot_owned).unwrap_or(false) || ctx.config.safety.auto_drop_slot;
    let src = ctx.source_client("pgsync-cleanup").await?;
    if let Some(info) = source::slot_info(&src, &ctx.config.source.slot).await? {
        if slot_owned {
            if info.active {
                return Err(Error::preflight(format!(
                    "slot {:?} is active (pid {:?}); stop the consumer first",
                    ctx.config.source.slot, info.active_pid
                )));
            }
            src.execute(
                "SELECT pg_drop_replication_slot($1)",
                &[&ctx.config.source.slot],
            )
            .await
            .map_err(|e| from_pg("drop slot", e))?;
            actions.push(format!(
                "dropped replication slot {:?} on the source",
                ctx.config.source.slot
            ));
        } else {
            actions.push(format!(
                "left replication slot {:?} in place (not owned by this job; set safety.auto_drop_slot to override)",
                ctx.config.source.slot
            ));
        }
    } else {
        actions.push(format!(
            "replication slot {:?} does not exist",
            ctx.config.source.slot
        ));
    }
    if job.is_some() {
        sink::delete_job(s.client(), &ctx.config.job.id).await?;
        actions.push(format!(
            "deleted job {:?} metadata from the target (data tables kept)",
            ctx.config.job.id
        ));
    } else {
        actions.push(format!(
            "job {:?} has no metadata on the target",
            ctx.config.job.id
        ));
    }
    Ok(actions)
}
