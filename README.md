<h1 align="center">pgsync</h1>

<p align="center">
  <strong>Memory-bounded logical replication for PostgreSQL, in Rust.</strong><br>
  One binary takes a transactionally consistent snapshot of the tables you name,
  then streams every committed change through native <code>pgoutput</code> into a second
  PostgreSQL — one target transaction per source transaction, checkpoint committed with the data.<br>
  No extension on the source, no broker, no memory that grows with your database.
</p>

<p align="center">
  <a href="https://github.com/pgrundev/pgsync/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/pgrundev/pgsync/actions/workflows/ci.yml/badge.svg"></a>
  <a href="LICENSE"><img alt="License: Apache-2.0" src="https://img.shields.io/badge/license-Apache--2.0-blue"></a>
  <img alt="PostgreSQL 16–18" src="https://img.shields.io/badge/postgres-16%E2%80%9318-336791">
  <img alt="Rust stable" src="https://img.shields.io/badge/rust-stable-orange">
</p>

<p align="center">
  <a href="#quickstart">Quickstart</a> ·
  <a href="#install">Install</a> ·
  <a href="#setup--a-replication-role-that-can-only-read">Setup</a> ·
  <a href="#commands-and-flags">Commands</a> ·
  <a href="#configuration">Configuration</a> ·
  <a href="#how-it-stays-correct">Correctness</a> ·
  <a href="#memory-and-throughput">Benchmarks</a> ·
  <a href="#troubleshooting">Troubleshooting</a> ·
  <a href="docs/architecture.md">Architecture</a>
</p>

> **Status: controlled technical beta (v0.1.0).** Synthetic or explicitly approved
> non-sensitive data only. There is no data masking yet, so pgsync must not be pointed at
> production customer data. What is and is not supported is spelled out in
> [`docs/features.md`](docs/features.md); the evidence behind every claim below is in
> [`docs/test-report.md`](docs/test-report.md) and [`docs/benchmarks.md`](docs/benchmarks.md).

---

## Quickstart

Two PostgreSQL 18 containers, a publication, and a 90-second first run:

```sh
docker compose up -d --wait                       # source :5433 (wal_level=logical), target :5434
export PGSYNC_SOURCE_URL='postgres://pgsync:pgsync@localhost:5433/app'
export PGSYNC_TARGET_URL='postgres://postgres:postgres@localhost:5434/app'

cargo build --release
./target/release/pgsync --config examples/pgsync.yaml check       # 22 checks, changes nothing
./target/release/pgsync --config examples/pgsync.yaml bootstrap   # slot + snapshot + copy, then streams
```

In a second shell, write to the source and read it back from the target:

```sh
psql postgres://postgres:postgres@localhost:5433/app \
  -c "INSERT INTO users (email, full_name) VALUES ('demo@example.test', 'Demo')"
psql "$PGSYNC_TARGET_URL" -c "SELECT id, email, full_name FROM users WHERE email = 'demo@example.test'"
```

Stop pgsync with Ctrl-C (it finishes the transaction in flight), make more changes, then
resume from the checkpoint it left in the target:

```sh
./target/release/pgsync --config examples/pgsync.yaml run
./target/release/pgsync --config examples/pgsync.yaml status
```

Connection URLs come from the environment variables named in the config — never from the
file, never from the command line — so they stay out of `ps`, logs, and git.
`make check bootstrap run status cleanup` wrap the same commands.

## Why pgsync

| | |
|---|---|
| **Correct before fast** | Each source transaction becomes exactly one target transaction, applied in commit order. The checkpoint row (`pgsync.job`) is updated *inside* that transaction, so data and position are durable together or not at all. WAL is acknowledged to the source only after the commit returns. |
| **Crash-proof by construction** | After a SIGKILL, a dropped connection, or a server restart, pgsync reloads the checkpoint from the target and resumes. Redelivered transactions are recognised by LSN and skipped; nothing is applied twice. A 15-round random-kill loop under continuous writes converges to identical content hashes. |
| **Memory does not scale with data** | One bounded, byte-accounted channel between reader and applier, bounded statement batches, fixed-size COPY buffers, and no transaction buffering. Measured RSS: 33 MB peak snapshotting 1 GB, ~15 MB steady during CDC, 9.5 MB per process with 20 jobs. |
| **No silent skipping, ever** | If a change cannot be applied safely — a column the target lacks, an UPDATE that matches no row, a slot someone else advanced — the job stops visibly (`ERROR` or `RESYNC_REQUIRED`) with a reason. There is no "start from current LSN" fallback in the code. |
| **Gentle with the source** | Native `pgoutput`, no extensions, no event triggers, no DDL, no DML. A `REPLICATION` role with `SELECT` is enough. The only object pgsync creates on the source is the slot you named, and only during `bootstrap`. |
| **Operable** | 22 preflight checks before anything is created, retained-WAL and lag alarms, explicit `cleanup --yes`, structured logs with `--json`, scriptable exit codes. |

<details>
<summary><strong>How it compares to built-in subscriptions, pgstream, Debezium</strong></summary>

A built-in `CREATE SUBSCRIPTION` is the right tool when the target is a PostgreSQL you
administer and you want the server to own the process. pgsync exists because PGRun needs
the replication engine *outside* the database: a separate process whose crash cannot take
the runtime down, that will later rewrite rows (masking) between decode and apply, and
that can feed non-PostgreSQL storage. It keeps the consistency properties of a
subscription (exported snapshot, commit-ordered atomic apply) while owning the checkpoint.

[pgstream](https://github.com/xataio/pgstream) (Go) was the primary reference; the audit
is in [`docs/research/pgstream-audit.md`](docs/research/pgstream-audit.md). pgsync differs
on purpose: `pgoutput` instead of wal2json, transaction boundaries preserved instead of
batched across them, checkpoint stored transactionally instead of only in the slot, and
strict stop instead of drop-and-continue on apply errors.

Debezium and other broker-based CDC pipelines solve fan-out to many consumers. pgsync has
one consumer per job and no broker, by design, for Beta 1.

</details>

## Requirements

- Source: PostgreSQL 16, 17 or 18 with `wal_level = logical`, a free replication slot and
  WAL sender, and a publication containing exactly the tables you configure.
- Target: PostgreSQL 16–18. A superuser (or a role allowed to `SET session_replication_role`)
  lets pgsync skip target triggers and FK checks during apply, as a subscription worker does.
- Every replicated table needs a primary key or a unique replica-identity index
  (`REPLICA IDENTITY FULL` works, slower). Partitioned and foreign tables, views, column
  lists, row filters and `publish_via_partition_root` are rejected by `check`.
- `pg_dump` of at least the source's major version on `PATH` (or `schema.mode: existing`).
- Rust stable (1.85+) to build; Docker with Compose for the test environment.

## See it

**`pgsync check`** — every prerequisite, graded, before anything is created:

```
source:
  [PASS] server_version               PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2) on aarch64-unknown-linux-gnu ...
  [PASS] wal_level                    logical
  [PASS] max_replication_slots        16 (0 in use)
  [PASS] max_wal_senders              16
  [PASS] max_slot_wal_keep_size       1GB (slot is invalidated beyond this)
  [PASS] wal_sender_timeout           30s
  [PASS] role                         pgsync has REPLICATION
  [PASS] publication                  pgsync_demo exists
  [PASS] publication_operations       insert, update, delete, truncate
  [PASS] publication_tables           2 tables match replication.tables
  [PASS] table public.users           primary key (id)
  [PASS] table public.orders          primary key (id)
  [PASS] slot                         "pgsync_demo_slot" does not exist yet; bootstrap will create it
  [PASS] replication_connection       system 7696030015139909676 timeline 1
  [WARN] source_tls                   plain connection (loopback)
target:
  [PASS] target_version               PostgreSQL 18.6 ...
  [PASS] target_role                  superuser (session_replication_role = replica available)
  [PASS] target_pgsync_schema         writable
  [PASS] pg_dump                      pg_dump (PostgreSQL) 18.2
  [PASS] target table public.users    will be created by pg_dump schema bootstrap
  [PASS] target table public.orders   will be created by pg_dump schema bootstrap
  [WARN] target_tls                   plain connection (loopback)
OK: prerequisites satisfied
```

**`pgsync bootstrap`** — the handoff, as logged (passwords and row values never appear):

```
INFO replication slot created with exported snapshot slot=pgsync_demo_slot consistent_point=0/6A97678 snapshot=0000007E-00000014-1
INFO schema dumped tables=2 bytes=2805
INFO target schema created from pg_dump
INFO copied table=public.orders rows=1000 bytes=92232
INFO copied table=public.users rows=200 bytes=26886
INFO initial copy finished tables=2 rows=1200 bytes=119118 seconds="0.7"
INFO snapshot complete; switching to CDC consistent_point=0/6A97678
INFO START_REPLICATION accepted slot=pgsync_demo_slot from=0/6A97678 confirmed_flush=Some(Lsn(0/6A97678))
INFO replication stream opened from=0/6A97678 acked=0/6A97678 server_wal_end=0/6A97EE0 state=CATCHING_UP
INFO caught up; now streaming lsn=0/6A97EE0
```

**`pgsync status`** — positions on both sides and what the slot is holding:

```
job demo-replication: STREAMING
  source system   7696030015139909676 db app
  slot            pgsync_demo_slot (owned: true)
  publication     pgsync_demo
  consistent pt   0/6A97678
  committed lsn   0/6A9BB90
  acked lsn       0/6A9BB90
  txns applied    1
slot: active=true wal_status=reserved confirmed_flush=0/6A97EE0 restart=0/6A97EA8 invalidation=none
source wal: 0/6A9BB90  slot lag: 15536 bytes  retained wal: 15592 bytes
  table public.orders                    COPIED rows=1000 bytes=92232
  table public.users                     COPIED rows=200 bytes=26886
```

`--json` turns both the logs and the command output into JSON.

## Commands and flags

Every command reads `--config` (or `$PGSYNC_CONFIG`, default `pgsync.yaml`) and the two
URL environment variables the config names.

| Command | What it does |
|---|---|
| `check` | validate source, target and replication prerequisites — 22 checks, nothing is created or modified |
| `bootstrap` | create the slot with an exported snapshot, bootstrap the target schema (`pg_dump --schema-only`), stream every table with `COPY`, write the checkpoint, then keep streaming changes |
| `bootstrap --restart-snapshot` | discard an incomplete copy: drop the slot pgsync owns, drop (or truncate) the target tables, start over |
| `run` | resume a bootstrapped job from the checkpoint in the target; refuses if continuity cannot be proven |
| `status` | job state and positions, slot state, lag and retained WAL, per-table copy progress |
| `cleanup --yes` | drop the slot pgsync owns and delete the job metadata; target data is kept |

| Global flag | |
|---|---|
| `--config <path>` | job configuration file (YAML) |
| `--json` | JSON logs and JSON command output |
| `--log <filter>` | tracing filter, e.g. `debug` or `pgsync_core=trace` (statement parameters are never logged) |

Exit codes are a scriptable contract: `0` done or stopped by signal · `1` runtime error ·
`2` configuration · `3` preflight · `4` resynchronisation required · `5` unsupported change
or consistency violation.

Job states, as stored in `pgsync.job` and shown by `status`: `INITIALIZING` →
`SNAPSHOTTING` → `CATCHING_UP` → `STREAMING`, plus `PAUSED` (stopped by a safety threshold),
`ERROR` (resumable after the operator fixes the cause) and `RESYNC_REQUIRED` (not resumable;
`cleanup --yes` then `bootstrap`).

## Install

No release binaries yet — build from source with Rust stable:

```sh
git clone https://github.com/pgrundev/pgsync
cd pgsync
cargo build --release          # ./target/release/pgsync
```

Dependencies are pinned in `Cargo.lock` and come from crates.io only (`tokio-postgres`,
`postgres-protocol`, `rustls`, `clap`, `serde`). The replication protocol client is in-house;
see [`docs/research/rust-libraries.md`](docs/research/rust-libraries.md) for why.

## Setup — a replication role that can only read

pgsync never needs DML, DDL or superuser on the source. Run this once as an administrator:

```sql
CREATE ROLE pgsync WITH LOGIN REPLICATION PASSWORD '…';
GRANT CONNECT ON DATABASE app TO pgsync;
GRANT USAGE ON SCHEMA public TO pgsync;
GRANT SELECT ON public.users, public.orders TO pgsync;      -- the initial COPY needs it

-- The publication is an explicit administrative action; pgsync does not create it.
CREATE PUBLICATION pgsync_demo FOR TABLE public.users, public.orders;
```

`pg_hba.conf` must allow `pgsync` to connect to the database itself (logical replication
uses the database entry, not `replication`). `wal_level = logical` requires a restart;
pgsync will tell you, not change it. The full list, including what adding a keyless table
to a publication does to *your own* UPDATEs, is in
[`docs/operations.md`](docs/operations.md).

## Point pgsync at your databases

```sh
export PGSYNC_SOURCE_URL='postgres://pgsync:…@db.internal:5432/app?sslmode=require'
export PGSYNC_TARGET_URL='postgres://pgsync_writer:…@golden.internal:5432/app'
```

Any libpq-style URL works. TLS policy is derived from the host:

- Non-loopback hosts require TLS **with certificate verification** while
  `safety.require_tls` is true (the default). `sslmode=disable` is refused for them.
  Add a private CA with `safety.tls_ca_file`.
- `localhost`, `127.0.0.1`, `::1` and Unix sockets may use plain connections.
  `safety.insecure_skip_tls_verify` is honoured for loopback hosts only.

`check` reports whether each session actually ended up encrypted (`pg_stat_ssl`), not what
was requested.

## Configuration

[`examples/pgsync.yaml`](examples/pgsync.yaml), annotated. Unknown keys and out-of-range
values are rejected with a message naming the field.

```yaml
job:
  id: demo-replication            # primary key of the checkpoint row in the target

source:
  url_env: PGSYNC_SOURCE_URL      # env var holding the URL, never the URL itself
  publication: pgsync_demo        # must already exist
  slot: pgsync_demo_slot          # created by `bootstrap`, owned by this job

target:
  url_env: PGSYNC_TARGET_URL

replication:
  mode: snapshot_and_cdc          # or cdc_only
  tables: [public.users, public.orders]   # must equal the publication's table set

schema:
  mode: pg_dump                   # or `existing` (target tables already there; columns verified)

resources:
  memory_budget_mb: 128           # must cover the three bounds below plus COPY buffers
  max_inflight_bytes: 16777216    # reader → applier channel byte budget (backpressure)
  max_batch_rows: 500             # statements pipelined per flush inside a target transaction
  max_batch_bytes: 4194304
  buffer_size_kb: 256             # COPY chunk size during the snapshot
  max_parallel_copies: 2
  connection_timeout: 10s
  retry_backoff: { initial: 1s, max: 30s, multiplier: 2.0, max_attempts: 0 }
  snapshot_timeout: 6h
  feedback_interval: 10s
  max_replication_lag_bytes: 1073741824

safety:
  require_tls: true
  insecure_skip_tls_verify: false
  auto_create_publication: false
  auto_drop_slot: false
  wal_retention_alarm_bytes: 1073741824
  wal_retention_action: warn      # or `stop` → job goes to PAUSED (WAL is still retained)
  monitor_interval: 30s
```

## How it stays correct

```
source PostgreSQL ──pgoutput──▶ reader ──bounded channel──▶ applier ──▶ target PostgreSQL
                                   ▲                           │ BEGIN · DML · UPDATE pgsync.job · COMMIT
                                   └──── feedback: flushed = last durable position ◀──┘
```

1. `bootstrap` opens one replication connection and runs
   `CREATE_REPLICATION_SLOT … LOGICAL pgoutput EXPORT_SNAPSHOT`. PostgreSQL guarantees that
   everything visible in that snapshot is excluded from the stream starting at the returned
   `consistent_point`, and everything committed later is included.
2. Copy sessions import the snapshot (`SET TRANSACTION SNAPSHOT` under `REPEATABLE READ`)
   and stream each table with `COPY … TO STDOUT` → `COPY … FROM STDIN`, in bounded chunks,
   one target transaction per table.
3. Streaming starts at `consistent_point`. The applier opens a target transaction on
   `BEGIN`, pipelines DML in bounded batches, writes the checkpoint row, and commits.
   Only then is the position published to the reader, which reports it as flushed.
4. On restart, the checkpoint in the target is the truth. The server resumes from the
   slot's confirmed position; transactions whose commit LSN is at or below the checkpoint
   are consumed and skipped.
5. On every `run`, pgsync verifies the source system identifier, the slot's plugin and
   database, `wal_status`, and that the slot has not been advanced past the checkpoint by
   someone else. Any mismatch is `RESYNC_REQUIRED`.

Rows travel in PostgreSQL text format and are cast server-side to the target column's
declared type; identifiers are quoted, values are always bound parameters, INSERTs use
`OVERRIDING SYSTEM VALUE`, and unchanged TOAST values are left untouched. Details and the
trade-offs: [`docs/architecture.md`](docs/architecture.md).

## What it replicates

INSERT, UPDATE, DELETE, TRUNCATE (with `CASCADE` / `RESTART IDENTITY`), for ordinary tables
with a primary key or unique replica identity, including identity columns, generated columns
(recomputed on the target), TOAST, arrays, json/jsonb, bytea, numeric, uuid, timestamps,
intervals, NULLs. Rolled-back transactions never reach the stream.

Not replicated, by design or not yet: DDL (a column change stops the job and names the
column; apply it on the target and `run`), sequences, large objects, two-phase commit,
in-progress transaction streaming (protocol v2), partitioned tables, row filters, column
lists. The full matrix: [`docs/features.md`](docs/features.md).

## Version support

| Role | Versions | Notes |
|---|---|---|
| Source | 16, 17, 18 | CI matrix runs the whole end-to-end suite on each; 18.6 is the reference and the only version run locally so far. 14–15 pass `check` with a warning and are untested. |
| Target | 16, 17, 18 | `pg_dump` must be at least the source's major version. |

## Memory and throughput

Measured on an Apple M1 Pro with source, target and pgsync on the same machine (so
conservative), PostgreSQL 18.6, release build, default configuration. Raw samples and the
scripts are in [`docs/benchmarks.md`](docs/benchmarks.md) and `bench-results/`.

| Scenario | Result |
|---|---|
| Snapshot, 1 GB (2 × 4 M rows) | 32 s · 36.5 MB/s · RSS peak 33 MB |
| Snapshot, 100 MB | 5 s · RSS peak 29 MB |
| Catch-up of a 5,000-transaction backlog (575 k row ops) | 8 s · 625 txn/s · ≈72 k rows/s · one core saturated |
| 20 concurrent jobs (one process each) | 9.5 MB RSS per process, all 20 consistent |
| 40-minute soak, 9.2 M rows applied | RSS 14.6–14.9 MB in every quarter after the first minute · 0 reconnects |

The applier is single-threaded and CPU-bound at ~70 k row operations/s; multi-row
coalescing is the planned next step. No comparison with other tools has been run, so none
is claimed.

## Operating it

- **The slot retains WAL.** Stopping pgsync does not release it; `max_slot_wal_keep_size`
  on the source eventually invalidates the slot instead. `status` and the periodic
  `wal monitor` log line show retained bytes; `safety.wal_retention_alarm_bytes` raises an
  alarm, and `wal_retention_action: stop` parks the job in `PAUSED` so the condition cannot
  be missed. Keepalive positions are acknowledged while the publication is idle, so the
  slot keeps advancing over unpublished WAL.
- **Resynchronising** is explicit: `cleanup --yes` (drops the owned slot and the job row),
  then `bootstrap --restart-snapshot`. There is deliberately no shortcut.
- **Schema changes** on a published table stop the job with the column name in
  `last_error`; apply the DDL on the target first next time.
- **Signals**: SIGTERM / Ctrl-C finishes or rolls back the transaction in flight, sends a
  final position report, and exits 0.

[`docs/operations.md`](docs/operations.md) has the complete runbook.

## CI integration

```sh
make lint          # cargo fmt --check, clippy -D warnings
make test          # 63 unit tests, no database needed
make e2e           # live protocol test + 18 end-to-end scenarios against docker compose (~1–6 min)
make e2e-slow      # container pause/restart, random crash loop, 1.2 M-row snapshot
make bench         # resource and throughput benchmarks
```

[`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs lint and unit tests, then the
end-to-end suite against `postgres:16`, `:17` and `:18`, and the disruptive scenarios on 18.
Scenarios compare full table contents (MD5 over every row and column), never row counts.

## Roadmap and non-goals

- **Beta 2 — production-safe golden copies.** Deterministic masking between decode and
  apply (same values across snapshot and CDC, FK-consistent, fail-closed, versioned policies
  with forced rebuilds); DDL detection with a strict pause mode; protocol v2 streaming with
  bounded disk spill for very large transactions; ctid-range parallel copy; multi-row
  statement coalescing; Prometheus metrics.
- **Beta 3 — PGXStore integration.** Versioned golden copies with a defined consistency
  boundary; PGRun control-plane integration (Rails orchestrates, pgsync moves data).
- **Non-goals for now.** Multi-node HA, non-PostgreSQL sinks, source failover handling,
  automatic recovery from lost WAL, replicating databases without replica identities.
  pgsync will never write to source application tables or alter source configuration.

## Troubleshooting

<details>
<summary><strong><code>[FAIL] slot "…" already exists</code> on <code>check</code> or <code>bootstrap</code></strong></summary>

A slot with that name exists on the source — from an earlier bootstrap or another tool.
If it is this job's slot and you want to start over: `pgsync cleanup --yes` (drops it only
when inactive and owned), then `bootstrap`. Otherwise pick another `source.slot`.
`bootstrap` never reuses a slot because an existing slot has no exported snapshot to copy
from.

</details>

<details>
<summary><strong><code>[FAIL] target table … already exists; schema.mode is pg_dump</code></strong></summary>

The target already has the tables and `pg_dump` mode would try to create them. Either you
pre-created them on purpose — set `schema.mode: existing` and pgsync verifies the columns —
or they are leftovers: `bootstrap --restart-snapshot` drops and recreates them.

</details>

<details>
<summary><strong><code>publication contains tables not in replication.tables</code></strong></summary>

Beta 1 requires the configured table set and the publication's table set to be identical.
Changes to an unconfigured published table would arrive and could not be applied. Add the
table to the config or remove it from the publication (`ALTER PUBLICATION … DROP TABLE`).

</details>

<details>
<summary><strong>Job is in <code>ERROR</code> after a column was added on the source</strong></summary>

Expected. `last_error` (in `status`) names the column. Apply the same DDL on the target,
then `pgsync run`. The transaction that failed was rolled back and nothing before it was
lost; it is redelivered on resume.

</details>

<details>
<summary><strong>Job is in <code>RESYNC_REQUIRED</code>, exit code 4</strong></summary>

Continuity could not be proven: the slot is missing or invalidated (`wal_status = lost`),
its confirmed position is beyond the checkpoint (another consumer advanced it), or the
source cluster's system identifier changed. `run` will keep refusing. Recover with
`pgsync cleanup --yes` followed by `pgsync bootstrap`. There is no flag to skip the gap.

</details>

<details>
<summary><strong><code>sslmode=disable is not allowed for a non-loopback host</code></strong></summary>

`safety.require_tls` is on and the host is not loopback. Use TLS (add the CA with
`safety.tls_ca_file` if it is private). For a throwaway lab on a remote host, set
`safety.require_tls: false` explicitly in the config — never the default.

</details>

<details>
<summary><strong>Retained WAL keeps growing while pgsync is stopped</strong></summary>

That is the slot doing its job. Either resume with `run` so it advances, or
`cleanup --yes` to drop it if the job is abandoned. `max_slot_wal_keep_size` on the source
is the backstop; once it triggers, the job needs a resync.

</details>

## Privacy

pgsync talks to two databases you name and nothing else — no telemetry, no outbound
calls. Connection URLs are read from the environment and printed with the password
redacted. Row values never appear in logs at any level: the one library that logs
statement parameters (`tokio-postgres`) is pinned to WARN even under `--log debug`.
The checkpoint stored in the target holds positions and identifiers, not data.

## Contributing

Issues and PRs welcome. The invariants that are load-bearing: apply order and atomicity,
acknowledge-after-durable-commit, bounded memory, no silent skipping, no source
modification beyond the slot. The dev loop is `make lint`, `make test`, and `make e2e`
with `docker compose up -d`; CI runs the same three. Architecture decisions are recorded
in [`docs/architecture.md`](docs/architecture.md), and the research that led to them in
[`docs/research/`](docs/research/).

## Security

pgsync handles database credentials and replication slots. To report a vulnerability, use
GitHub's private vulnerability reporting (Security → Report a vulnerability) rather than a
public issue.

## License

[Apache-2.0](LICENSE). No third-party code is vendored.
