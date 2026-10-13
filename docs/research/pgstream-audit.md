# Xata pgstream audit

Reviewed 2026-10-12. Source: https://github.com/xataio/pgstream (read only; not built).

- Default branch `main` HEAD: `7cf534b0dcdbfa7c8e7936c37648d850445a9230` (2026-10-08).
- Latest stable release: `v1.5.0` → `825a944464c7dc129df53eb0340828f4c4b46cdf` (2026-09-10).
- Stack: Go 1.26, `jackc/pgx/v5`, `jackc/pglogrepl`, `cenkalti/backoff/v4`.
- License: Apache-2.0 (`LICENSE`, SPDX headers in every file). No `NOTICE` file. One MIT file:
  `migrations/postgres/injector/1_create_pgstream_xid.up.sql` (Copyright 2022 Rasmus Holm, pg-xid).

**PGSync reuses no pgstream code.** The items below were studied for design and operational
lessons only. Where PGSync adopts an idea, it is noted as *adopted*; where PGSync deliberately
differs, it is noted as *diverged*.

## WAL reader (`pkg/wal/listener/postgres/`, `pkg/wal/replication/postgres/`)

- Plugin: **wal2json only** (`format-version=2`, `include-transaction=0`). pgoutput is an open
  enhancement (#577). *Diverged:* PGSync uses native pgoutput; no customer extension needed.
- Starts from the slot's `confirmed_flush_lsn` (falls back to `restart_lsn`), requires the
  slot to exist already (`pgstream init` creates it). *Adopted:* `run` never creates slots.
- The receive loop blocks in `ReceiveMessage` with no deadline. Keepalives without
  `ReplyRequested` are dropped; with it, a data-less event is pushed through the batch
  pipeline and acked once the batch drains. No independent feedback timer.
  *Diverged:* PGSync sends standby status on a timer independent of apply progress, using
  the last durably committed LSN, so slow apply never trips `wal_sender_timeout`.
- Each XLogData is acked as `WALStart + len(data)` (open bug #1254: this can exceed
  `pg_current_wal_lsn()` and, after restart, skip committed rows). *Diverged:* PGSync acks only
  server-supplied `end_lsn` values from Commit messages, after the target commit is durable.

## Snapshot (`pkg/snapshot/generator/postgres/data/`, `.../schema/pgdumprestore/`)

- **Not slot-based.** The slot is created at `init`; the snapshot later records
  `confirmed_flush_lsn`, runs the copy, then starts replication from that LSN. Overlap is
  expected and tolerated through `on_conflict_action` (tutorials use `nothing`).
  *Diverged:* PGSync uses `CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT` and starts streaming
  from `consistent_point`, so the copy and the stream are disjoint by construction.
- Per schema: one `REPEATABLE READ READ ONLY` transaction exports a snapshot, worker
  transactions import it with `SET TRANSACTION SNAPSHOT`, tables are split into ctid page
  ranges (`batch_bytes`, default 80 MiB) read with `SELECT ... WHERE ctid BETWEEN ...`.
  Consistency is per schema, not global. *Adopted later:* ctid range parallelism is the natural
  Beta 2 optimisation. *Diverged:* Beta 1 uses one global snapshot and streaming `COPY`.
- Schema bootstrap shells out to `pg_dump --schema-only` and `psql`, splitting the dump so
  indexes/constraints are restored after data. *Adopted:* `pg_dump --schema-only` for the
  selected tables (PGSync applies the SQL itself, no `psql`).
- Snapshot progress is recorded per table in `pgstream.snapshot_requests` for resumption.
  *Deferred:* Beta 1 discards and restarts incomplete snapshots.

## PostgreSQL sink (`pkg/wal/processor/postgres/`)

- Batch writer: 1 s / 100 msgs / 1.5 MiB defaults, 100 MiB memory guard, concurrency 1.
  Coalesces consecutive same-table same-action DML into multi-row INSERT / `pk = ANY($1)`
  DELETE. *Adopted:* bounded batches with byte accounting; multi-row coalescing is a Beta 2
  throughput item.
- SQL generation: identifiers quoted, values always `$n` parameters, INSERT uses
  `OVERRIDING SYSTEM VALUE`, generated columns dropped, optional
  `session_replication_role = replica`. *Adopted* in full.
- Row identity: old key from wal2json when present, else PK, else the event is dropped.
  Tables without identity are silently skipped. *Diverged:* PGSync rejects such tables in
  preflight and stops (never skips) if an UPDATE/DELETE arrives without a usable key.
- Error policy: non-internal errors (constraint, missing relation, data exception) are
  isolated, dropped with a `DATALOSS` log, and the checkpoint still advances unless
  `strict_mode`. *Diverged:* PGSync is always strict.
- Source transaction boundaries are not preserved; the target can observe partial source
  transactions. *Diverged:* PGSync applies each source transaction atomically.

## Checkpoints

Progress lives only in the slot's `confirmed_flush_lsn`. Apply and ack are not atomic
(at-least-once, idempotency via ON CONFLICT). *Diverged:* PGSync stores the checkpoint in the
target in the same transaction as the data and uses it to skip redelivered transactions.

## Transformers (`pkg/wal/processor/transformer/`)

Decorator chain: sanitizer → transformer → injector → filter → instrumentation. Rules are
keyed by `schema.table.column`, validated against the source catalog, with strict/relaxed
modes that block DDL introducing untransformed columns and a uniqueness checker. Worth
revisiting for Beta 2 masking; PGSync's ordered `Transaction → Op` event model leaves a clean
insertion point between decoder and sink.

## Recovery

Exponential backoff (500 ms – 30 s) around `ReceiveMessage`; every retry reconnects and
restarts from `confirmed_flush_lsn`. Missing slot is a hard startup error. Lost WAL
(`wal_status`) is not checked at runtime. *Adopted:* reconnect with backoff.
*Extended:* PGSync checks `wal_status`/`invalidation_reason` in preflight and periodically.

## DDL tracking

Event triggers (`ddl_command_end`, `sql_drop`) installed by `pgstream init` call a
`SECURITY DEFINER` function that emits DDL text via `pg_logical_emit_message`. Requires
superuser-level privileges on the source; the target replays raw DDL with its own privileges.
Out of scope for Beta 1 (no event triggers on customer sources). The transactional logical
message approach is the right Beta 2 candidate for an opt-in mode.

## Operational lessons recorded

1. Ack only server-supplied LSNs at commit boundaries (#1254).
2. Keep transaction boundaries or accept partial visibility and at-least-once apply.
3. Bytea must be hex-decoded before binding (#1089); empty string vs NULL in text COPY (#1050).
4. Identity columns are not generated columns (#753); need `OVERRIDING SYSTEM VALUE`.
5. Per-statement replay cannot keep up during catch-up after a long snapshot (#769).
6. Snapshot must run against a single instance; load balancers break `SET TRANSACTION SNAPSHOT` (#1012).
7. Explicit sequences are not advanced by CDC (#1203).
