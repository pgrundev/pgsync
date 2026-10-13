# PGSync architecture (Beta 1.1, v0.1.1)

PGSync is a standalone Rust process that replicates a selected set of tables from a source
PostgreSQL (16, 17, 18) to a target PostgreSQL using native logical replication (`pgoutput`),
with a transactionally consistent initial snapshot, atomic per-transaction apply, and
checkpoints stored in the target database in the same transaction as the data.

This document records what is reused, what is implemented independently, why each library
was selected, the consistency design, known limitations, and how the beta evolves without a
rewrite. Research inputs: [`research/pgstream-audit.md`](research/pgstream-audit.md),
[`research/rust-libraries.md`](research/rust-libraries.md),
[`protocol-reference.md`](protocol-reference.md).

## 1. Reused vs. implemented

| Concern | Decision |
|---|---|
| Ordinary SQL connections (catalog queries, `COPY`, target apply, checkpoints) | **Reused:** `tokio-postgres` 0.7.18 + `tokio-postgres-rustls` 0.14 (crates.io, pinned). |
| Wire-level message encoding, SCRAM-SHA-256 / MD5 authentication, backend message parsing | **Reused:** `postgres-protocol` 0.6.12. |
| TLS | **Reused:** `rustls` 0.23 (`ring` backend), `tokio-rustls`, `rustls-native-certs`. |
| Replication connection (`replication=database` startup, `IDENTIFY_SYSTEM`, `CREATE_REPLICATION_SLOT`, `START_REPLICATION`, CopyBoth stream, keepalive/feedback) | **Implemented** in `pgsync-postgres::replication` (~700 lines). The published `tokio-postgres` cannot open a replication connection; see library assessment. |
| `pgoutput` decoder (proto v1 message set) | **Implemented** in `pgsync-postgres::pgoutput`, unit-tested from byte fixtures. |
| Snapshot export / import, streaming `COPY` | **Implemented** on top of `tokio-postgres` `copy_out` / `copy_in`. |
| Schema bootstrap | **Reused tool:** `pg_dump --schema-only` for the selected tables; PGSync executes the SQL. Optional `schema.mode: existing` skips it. |
| Transaction model, checkpoint contract, budgets, backpressure, retry, lifecycle | **Implemented** in `pgsync-core`. |
| CLI, config loading, logging | **Implemented** in `pgsync-cli` with `clap`, `serde_yaml_ng`, `tracing`. |

No code is copied from pgstream (Go) or any Rust fork. Attribution is therefore not required;
the Apache-2.0 licence of this repository applies.

### Why these libraries

- `tokio-postgres` is the mature async PostgreSQL client for Rust, pinned to a crates.io
  release. Its lack of replication support is worked around by a small adapter instead of a
  git fork, so the whole client stack stays on published versions.
- `postgres-protocol` already implements SCRAM and the message grammar; PGSync adds the one
  missing backend tag (`CopyBothResponse`) and the `replication=database` startup parameter.
- `rustls` with `ring` avoids a C toolchain dependency (`aws-lc-rs` needs cmake) and gives
  certificate + hostname verification by default.
- `serde_yaml_ng` is the maintained successor of the archived `serde_yaml`.
- No message broker, no `sqlx`, no ORM.

## 2. Crate layout

```
crates/
  pgsync-core       lifecycle, config + validation, Lsn, errors, event model,
                    checkpoint contract, trait boundaries, budgets, retry policy
  pgsync-postgres   replication connection, pgoutput decoder, source preflight,
                    snapshot reader, PostgreSQL sink + checkpoint store, WAL monitor
  pgsync-cli        `pgsync` binary: check / bootstrap / run / status / cleanup
```

Boundaries (traits in `pgsync-core::traits`):

- `ReplicationSource` — yields ordered `SourceEvent`s (`Begin`, `Op`, `Commit`, `Keepalive`).
- `SnapshotSource` — exports a snapshot, lists tables, streams a table as `COPY` bytes.
- `ReplicationSink` — opens a target transaction per source transaction, applies ops,
  commits together with the checkpoint, exposes `CheckpointStore`.
- `CheckpointStore` — load/save `Checkpoint`.
- `ReplicationEngine` — orchestrates bootstrap, catch-up, streaming, reconnects.

Traits exist only where a second implementation is plausible (PGX / PGXStore sinks, test
doubles). Everything else is concrete.

## 3. Data flow

```
          ┌───────────────── source PostgreSQL ─────────────────┐
          │  replication conn (pgoutput)        N copy conns     │
          └───────────┬─────────────────────────────┬───────────┘
                      │ CopyData 'w'/'k'            │ COPY ... TO STDOUT
                      ▼                             ▼
              ReplicationReader task         SnapshotCopier (bounded parallelism)
              (decode, byte-accounted,                │  COPY ... FROM STDIN
               bounded mpsc, feedback timer)          │  per table, one target txn
                      │                             ▼
                      ▼ SourceEvent             target PostgreSQL
              Applier task  ─── BEGIN / DML batches / UPDATE pgsync.checkpoint / COMMIT ──▶
                      │
                      ▼ watch<AckPosition>
              feedback 'r' (flushed = durable position)
```

Two long-lived tasks communicate over one bounded channel. The reader never buffers a
transaction: each decoded message becomes one `SourceEvent` and is forwarded immediately.
The applier opens a target transaction on `Begin`, executes DML in bounded batches, and on
`Commit` updates the checkpoint row and commits. Memory therefore depends on batch and
channel limits, never on transaction or database size.

### Backpressure

- The channel is bounded in items (`resources.channel_capacity`) and bytes
  (`resources.max_inflight_bytes`, a semaphore the reader must acquire per event).
- When the applier is slow the reader blocks on the channel, which stops reading the socket,
  which stops the walsender (TCP backpressure).
- While blocked the reader still services a feedback timer so `wal_sender_timeout` is never
  tripped; the reported position is always the last durable one.

## 4. Consistency design

### 4.1 Snapshot and CDC handoff

1. Open the replication connection; `IDENTIFY_SYSTEM` (record `systemid`, timeline).
2. `CREATE_REPLICATION_SLOT <slot> LOGICAL pgoutput EXPORT_SNAPSHOT` → `consistent_point`,
   `snapshot_name`. The connection then stays **idle**; the exported snapshot is valid until
   the next command on that connection.
3. Persist `pgsync.job` in the target with state `SNAPSHOTTING`, the system id, slot,
   publication, and `consistent_point`.
4. Bootstrap the schema (`pg_dump --schema-only`) unless `schema.mode: existing`.
5. For each table (bounded parallelism, FK-aware ordering): on a copy connection
   `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET TRANSACTION SNAPSHOT '<name>'`,
   then `COPY (SELECT cols FROM t) TO STDOUT` streamed into `COPY t (cols) FROM STDIN` inside
   a target transaction that also records the table as copied. Both sides stream in
   `buffer_size_kb` chunks.
6. When all tables are copied, set state `CATCHING_UP`, `committed_lsn = consistent_point`.
7. `START_REPLICATION SLOT <slot> LOGICAL <consistent_point> (proto_version '1',
   publication_names '<pub>')` on the same connection.

Per the PostgreSQL documentation, everything visible in the exported snapshot is excluded
from the stream starting at `consistent_point`, and everything committed later is included.
There is no overlap and no gap, so the copy needs no idempotency tricks.

If the snapshot fails, the job stays in `SNAPSHOTTING`. `pgsync bootstrap --restart-snapshot`
drops the slot PGSync created, truncates the copied tables, and starts over. There is no
partial resume in Beta 1.

### 4.2 Transaction apply and checkpoint

`pgsync.job` (one row per job) holds `committed_lsn` (the `end_lsn` of the last applied
transaction), `commit_lsn`, `acked_lsn` (the highest position ever reported as flushed),
`last_xid`, `last_commit_ts`, `state`, and source identity.

- On `Begin(final_lsn)`: if `final_lsn <= job.commit_lsn` the transaction was already applied
  (redelivery after crash/reconnect); it is consumed and skipped without touching the target.
- Otherwise `BEGIN` on the target, apply ops, and on `Commit(commit_lsn, end_lsn)`:
  `UPDATE pgsync.job SET committed_lsn = end_lsn, commit_lsn = commit_lsn, acked_lsn = end_lsn
  ...` in the same transaction, then `COMMIT`.
- Only after the commit returns does the applier publish `end_lsn` to the feedback channel.
  Feedback reports `flushed = applied = acked_lsn`. **WAL is never acknowledged before the
  data is durable in the target.**
- `Keepalive(wal_end)` events travel through the same ordered channel. The applier handles
  them only between transactions: it records `acked_lsn = max(acked_lsn, wal_end)` in the
  target (autocommit) and then publishes it. This lets the slot advance while the publication
  is idle (source writes to unpublished tables, other databases) without retaining WAL, and
  keeps the invariant `slot.confirmed_flush_lsn <= job.acked_lsn`.
- On start, `run` checks: slot exists, plugin is `pgoutput`, `wal_status` not `lost`,
  `systemid` matches, and `confirmed_flush_lsn <= job.acked_lsn`. Any violation →
  `RESYNC_REQUIRED` (or `PREFLIGHT`), never a silent skip. PGSync requests streaming from
  `job.committed_lsn`; the server starts at `max(requested, confirmed_flush_lsn)`.

### 4.3 Unsupported changes

`Truncate` is applied as `TRUNCATE ... [CASCADE] [RESTART IDENTITY]` (tables in the
publication only). `Message` events are ignored only if non-transactional and are logged;
transactional `Message` payloads are not data changes. Any other message tag (streaming,
two-phase, unknown) stops the job with `unsupported` and the transaction is not acknowledged.
An UPDATE/DELETE without a usable key, a relation that does not exist on the target, a column
mismatch, or a server error during apply stops the job (`ERROR` state) with the transaction
rolled back. A relation outside `replication.tables` (publication changed after bootstrap)
stops the job with `RESYNC_REQUIRED` before anything is applied. Parameters are cast to the
target column's *base* type (`$1::character varying`, not `::character varying(80)`): the
column's assignment rules then raise for values that do not fit instead of truncating or
rounding them as an explicit modified-type cast would. No automatic skipping exists anywhere.

## 5. Resource management

Configurable and validated (`resources` section):

| Setting | Default | Effect |
|---|---|---|
| `memory_budget_mb` | 128 | Soft cap; `max_inflight_bytes + batch bytes + buffers` must fit. |
| `buffer_size_kb` | 256 | COPY chunk size and socket read buffer. |
| `max_inflight_bytes` | 16 MiB | Byte semaphore on the reader→applier channel. |
| `channel_capacity` | 4096 | Item bound on the channel. |
| `max_batch_rows` / `max_batch_bytes` | 500 / 4 MiB | Statements pipelined per flush inside a target transaction. |
| `max_message_bytes` | derived (≤ 32 MiB) | Largest single replication message; enforced before buffering, larger ones stop the job (`resource`, nothing acknowledged). |
| `max_parallel_copies` | 2 | Concurrent table copies during the snapshot. |
| `connection_timeout` | 10s | TCP + startup timeout. |
| `retry_backoff` | 1s → 30s, ×2 | Reconnect policy for transient errors. |
| `snapshot_timeout` | 6h | Abort the snapshot if it runs longer. |
| `max_replication_lag_bytes` | 1 GiB | Alarm threshold on `wal_end - applied`. |
| `wal_retention_alarm_bytes` | 1 GiB | Alarm threshold on `pg_current_wal_lsn - restart_lsn`. |

Measured values (RSS, CPU, throughput, lag) live in `docs/benchmarks.md` once recorded.

## 6. Source safety

- `run` never creates publications or slots. `bootstrap` creates the slot named in the
  config (an explicit, operator-approved action) and records that PGSync owns it — the
  record is written before the slot exists, and the slot is dropped again if that
  bootstrap's snapshot does not complete. Publication
  creation requires `safety.auto_create_publication: true` and is refused for hosts that are
  not loopback unless `safety.allow_remote_publication_create: true`.
- No DDL on the source, no event triggers, no extensions, no DML. The replication role needs
  `REPLICATION`, `LOGIN`, and `SELECT` on the published tables only.
- TLS with certificate verification is required for non-loopback hosts
  (`safety.require_tls: true`). `safety.insecure_skip_tls_verify` is accepted only for
  loopback hosts.
- Passwords are read from the URL in an environment variable and never logged; row values are
  never logged.
- The running process writes a heartbeat row (`pgsync.job_runtime`) to the target every
  `safety.heartbeat_interval`; `status` reads it (see `docs/operations.md`).
- `check` and the runtime monitor report slot state, lag, retained WAL, `wal_status`, and
  `invalidation_reason`. Exceeding `wal_retention_alarm_bytes` raises an alarm and, with
  `safety.wal_retention_action: stop`, stops the job. Stopping never advances the slot.
- Stopping a consumer does **not** release WAL. `pgsync cleanup` drops the slot PGSync owns
  after explicit confirmation (`--yes`), and the job must then be re-bootstrapped.

## 7. Known limitations (Beta 1)

- pgoutput protocol version 1 only: no in-progress transaction streaming, no two-phase
  commit, no `origin` filtering. Large transactions are decoded by the server at commit (the
  server spills to disk under `logical_decoding_work_mem`); PGSync streams them straight into
  one target transaction, so PGSync memory stays bounded but the target holds a long
  transaction.
- DDL is not replicated. Any schema change on a published table that alters the column set
  causes a mismatch error at apply time (`ERROR`, nothing skipped). Operator applies the DDL
  to the target and resumes.
- Sequences are not replicated (identity values are, via `OVERRIDING SYSTEM VALUE`).
- Partitioned tables, foreign tables, views, large objects, row filters, and column lists are
  rejected in preflight.
- Tables need a primary key or a unique replica identity index; `REPLICA IDENTITY FULL` is
  accepted; `NOTHING` is rejected.
- Snapshot restart is all-or-nothing; the exported-snapshot transaction holds the source
  xmin for the duration of the copy (documented operational risk; `snapshot_timeout` bounds it).
- Single node, single job per process. Run several processes for several jobs.
- Text-format transfer only (`binary` option unused). Every value is cast server-side with
  the target column's declared type.

## 8. Evolution path (no rewrite required)

- **Beta 2 masking:** insert a `Transform` stage between decoder and sink operating on the
  `Op` model; the snapshot copier switches from raw `COPY` passthrough to the same row model
  when a policy applies to a table.
- **Large transactions / spill:** the applier's batch buffer can be backed by a bounded
  disk spool without touching the reader or the protocol layer.
- **Streaming protocol v2+:** the decoder gains `Stream*` variants; the applier gains a
  per-xid staging area. The channel and checkpoint contracts stay the same.
- **Parallel copy by ctid ranges:** `SnapshotSource::copy_table` takes a range; the copier
  fans out ranges under the same semaphore.
- **DDL (strict mode):** compare the `Relation` message to the cached target columns and
  pause with `RESYNC_REQUIRED`/`PAUSED` instead of failing at apply.
- **PGX / PGXStore sinks:** implement `ReplicationSink` + `CheckpointStore`.
- **Metrics:** the `Metrics` struct already counts bytes/rows/lag; a Prometheus exporter is
  an additive feature.
