//! `pgsync` command line interface.

use anyhow::{Context, bail};
use clap::{Parser, Subcommand};
use pgsync_core::config::Config;
use pgsync_core::{ErrorKind, JobState};
use pgsync_postgres::job::{self, JobContext};
use pgsync_postgres::source::{CheckStatus, PreflightMode};
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

#[derive(Parser)]
#[command(
    name = "pgsync",
    version,
    about = "PostgreSQL logical replication engine (PGRun)"
)]
struct Cli {
    /// Path to the job configuration (YAML).
    #[arg(short, long, env = "PGSYNC_CONFIG", default_value = "pgsync.yaml")]
    config: PathBuf,
    /// Emit JSON (logs and command output).
    #[arg(long, env = "PGSYNC_JSON")]
    json: bool,
    /// Log filter, e.g. `info`, `debug`, `pgsync_core=trace`.
    #[arg(long, env = "PGSYNC_LOG", default_value = "info")]
    log: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Validate source, target and replication prerequisites without changing anything.
    Check,
    /// Create the slot, take a consistent snapshot, copy tables, then stream changes.
    Bootstrap {
        /// Discard an incomplete snapshot (drops the owned slot and the target copies).
        #[arg(long)]
        restart_snapshot: bool,
    },
    /// Resume a bootstrapped job from its durable checkpoint.
    Run,
    /// Report job state, positions and lag.
    Status,
    /// Drop the replication slot owned by the job and delete its metadata.
    Cleanup {
        /// Confirm the removal.
        #[arg(long)]
        yes: bool,
    },
}

fn init_logging(json: bool, filter: &str) {
    // tokio-postgres logs statement parameters at DEBUG, which would expose row values.
    // Keep it at WARN unless the operator names it explicitly.
    let mut filter = filter.to_owned();
    if !filter.contains("tokio_postgres") {
        filter.push_str(",tokio_postgres=warn");
    }
    let filter = tracing_subscriber::EnvFilter::try_new(&filter)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,tokio_postgres=warn"));
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_writer(std::io::stderr);
    if json {
        builder.json().flatten_event(true).init();
    } else {
        builder.init();
    }
}

fn shutdown_token() -> CancellationToken {
    let token = CancellationToken::new();
    let t = token.clone();
    tokio::spawn(async move {
        let ctrl_c = tokio::signal::ctrl_c();
        #[cfg(unix)]
        {
            let mut term =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("SIGTERM handler");
            tokio::select! {
                _ = ctrl_c => {}
                _ = term.recv() => {}
            }
        }
        #[cfg(not(unix))]
        {
            let _ = ctrl_c.await;
        }
        info!("shutdown requested; finishing the current transaction");
        t.cancel();
    });
    token
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    init_logging(cli.json, &cli.log);
    let code = match real_main(cli).await {
        Ok(()) => 0,
        Err(e) => {
            error!("{e:#}");
            match e.downcast_ref::<pgsync_core::Error>().map(|e| e.kind()) {
                Some(ErrorKind::Config) => 2,
                Some(ErrorKind::Preflight) => 3,
                Some(ErrorKind::ResyncRequired) => 4,
                Some(ErrorKind::Unsupported) | Some(ErrorKind::Consistency) => 5,
                _ => 1,
            }
        }
    };
    std::process::exit(code);
}

async fn real_main(cli: Cli) -> anyhow::Result<()> {
    let text = std::fs::read_to_string(&cli.config)
        .with_context(|| format!("reading {}", cli.config.display()))?;
    let config = Config::from_yaml(&text)?;
    let cancel = shutdown_token();
    let ctx = JobContext::new(config, cancel)?;
    info!(job = %ctx.config.job.id, source = %ctx.source.redacted(), target = %ctx.target.redacted(), "pgsync {}", env!("CARGO_PKG_VERSION"));

    match cli.command {
        Command::Check => {
            let (out, _) = job::check(&ctx, PreflightMode::Bootstrap).await?;
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                for (label, report) in [("source", &out.source), ("target", &out.target)] {
                    println!("{label}:");
                    for c in &report.checks {
                        let tag = match c.status {
                            CheckStatus::Pass => "PASS",
                            CheckStatus::Warn => "WARN",
                            CheckStatus::Fail => "FAIL",
                        };
                        println!("  [{tag}] {:<28} {}", c.name, c.detail);
                    }
                }
                println!(
                    "{}",
                    if out.ok {
                        "OK: prerequisites satisfied"
                    } else {
                        "FAILED: fix the items marked FAIL"
                    }
                );
            }
            if !out.ok {
                bail!(pgsync_core::Error::preflight("preflight failed"));
            }
        }
        Command::Bootstrap { restart_snapshot } => {
            job::bootstrap(&ctx, restart_snapshot).await?;
        }
        Command::Run => {
            job::run(&ctx).await?;
        }
        Command::Status => {
            let out = job::status(&ctx).await?;
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                match &out.job {
                    None => println!("job {:?}: not bootstrapped", ctx.config.job.id),
                    Some(j) => {
                        println!("job {}: {}", j.job_id, j.state);
                        println!(
                            "  source system   {} db {}",
                            j.source_system_id, j.source_dbname
                        );
                        println!("  slot            {} (owned: {})", j.slot, j.slot_owned);
                        println!("  publication     {}", j.publication);
                        println!(
                            "  consistent pt   {}",
                            j.consistent_point
                                .map(|l| l.to_string())
                                .unwrap_or_default()
                        );
                        println!("  committed lsn   {}", j.committed_lsn);
                        println!("  acked lsn       {}", j.acked_lsn);
                        println!("  txns applied    {}", j.txns_applied);
                        if let Some(e) = &j.last_error {
                            println!("  last error      {e}");
                        }
                        if j.state == JobState::ResyncRequired {
                            println!(
                                "  action          run `pgsync cleanup --yes` then `pgsync bootstrap`"
                            );
                        }
                    }
                }
                match &out.slot {
                    None => println!("slot: missing on source"),
                    Some(s) => println!(
                        "slot: active={} wal_status={} confirmed_flush={} restart={} invalidation={}",
                        s.active,
                        s.wal_status.clone().unwrap_or_default(),
                        s.confirmed_flush_lsn
                            .map(|l| l.to_string())
                            .unwrap_or_default(),
                        s.restart_lsn.map(|l| l.to_string()).unwrap_or_default(),
                        s.invalidation_reason
                            .clone()
                            .unwrap_or_else(|| "none".into())
                    ),
                }
                if let Some(l) = out.source_wal_lsn {
                    println!(
                        "source wal: {l}  slot lag: {} bytes  retained wal: {} bytes",
                        out.slot_lag_bytes.unwrap_or(0),
                        out.retained_wal_bytes.unwrap_or(0)
                    );
                }
                for t in &out.tables {
                    println!(
                        "  table {:<32} {} rows={} bytes={}",
                        t.table, t.state, t.rows_copied, t.bytes_copied
                    );
                }
            }
        }
        Command::Cleanup { yes } => {
            if !yes {
                bail!(
                    "cleanup drops the replication slot owned by job {:?} on the source and deletes its metadata on the target; re-run with --yes to confirm",
                    ctx.config.job.id
                );
            }
            for a in job::cleanup(&ctx).await? {
                println!("{a}");
            }
        }
    }
    Ok(())
}
