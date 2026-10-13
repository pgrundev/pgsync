# Operations guide

## Source prerequisites

- `wal_level = logical` (server restart; PGSync never changes it).
- `max_replication_slots` and `max_wal_senders` with headroom for one slot and one sender
  per job (plus one sender while `check` probes the replication connection).
- A role with `LOGIN`, `REPLICATION`, `CONNECT` on the database, `USAGE` on the schemas and
  `SELECT` on the published tables. No DML, no DDL, no superuser.
- `pg_hba.conf` must allow the role to connect to the database (logical replication uses the
  database's own entry, not `replication`).
- A publication containing exactly the configured tables, publishing insert, update and
  delete (truncate recommended).
- Each table needs a primary key or a unique replica-identity index. Adding a table without
  one to a publication that publishes UPDATE/DELETE makes **the source application's own**
  updates fail; `pgsync check` reports this before anything is created.

Target: a role that can create the `pgsync` schema and the replicated tables. A superuser
lets PGSync set `session_replication_role = replica` (triggers and FK checks skipped, as the
built-in subscription worker does). Without it, triggers fire and FK order matters.

## What PGSync creates and when

| Where | Object | Created by | Removed by |
|---|---|---|---|
| Source | logical replication slot (`source.slot`, plugin `pgoutput`) | `bootstrap` | `cleanup --yes`, `bootstrap --restart-snapshot` |
| Target | schema `pgsync` with tables `job`, `table_sync` | first command touching the target | never (drop manually) |
| Target | replicated tables (pg_dump mode) | `bootstrap` | `bootstrap --restart-snapshot` |

Nothing else: no publications (unless `safety.auto_create_publication`, unused in Beta 1),
no event triggers, no extensions, no changes to GUCs.

## WAL retention risk

A logical slot pins WAL from its `restart_lsn`. If PGSync (or anything else) stops consuming,
WAL accumulates until `max_slot_wal_keep_size` invalidates the slot (then the job needs a
resync) or, with the default `-1`, until the disk fills. **Stopping PGSync does not release
WAL.** The slot does.

PGSync reports retained bytes (`pg_current_wal_lsn() - restart_lsn`) and slot lag in
`pgsync status`, in the periodic `wal monitor` log line, and alarms above
`safety.wal_retention_alarm_bytes`. With `wal_retention_action: stop` it stops the job
(`PAUSED`), which still does not release WAL; it only makes the condition impossible to miss.

Keepalive positions are acknowledged while the publication is idle, so the slot advances
even when all source writes go to unpublished tables or other databases.

Decommissioning a job: stop PGSync, then `pgsync cleanup --yes` (drops the slot, deletes the
`pgsync.job` row, keeps target data). To drop manually:
`SELECT pg_drop_replication_slot('<slot>')` on the source once `active = false`.

## The exported snapshot

`bootstrap` holds the slot-creating replication connection idle for the whole copy; the
copy sessions hold `REPEATABLE READ` transactions. Both pin `xmin`, so autovacuum cannot
remove rows newer than the snapshot until the copy ends. `resources.snapshot_timeout`
(default 6 h) aborts the copy; a failed copy leaves the job in `SNAPSHOTTING`.
`pgsync status` shows per-table progress in `table_sync`.

## Resynchronisation

Required when continuity cannot be proven: slot missing, `wal_status = lost`,
`invalidation_reason` set, slot `confirmed_flush_lsn` beyond the checkpoint, or the source
`systemid` changed. The job is marked `RESYNC_REQUIRED`, `run` exits 4 and refuses to start.

```
pgsync cleanup --yes                  # drops the owned slot (if any) and the job row
pgsync bootstrap --restart-snapshot   # drops/truncates target tables and copies again
```

There is deliberately no option to skip missing WAL or start from the current LSN.

## Schema changes on the source

Not replicated. Adding a column to a published table makes the next change to that table
stop the job with `ERROR` and `last_error` naming the column. Apply the same DDL on the
target, then `pgsync run`. Dropping or renaming a column behaves the same way. Additive
changes applied on the target **before** the source are harmless.

## Failure and exit codes

| Exit | Meaning | Job state | Action |
|---|---|---|---|
| 0 | stopped by signal, or command done | unchanged | `run` to resume |
| 1 | runtime error (transient errors are retried before this) | `ERROR` | inspect logs, `run` |
| 2 | configuration | unchanged | fix config |
| 3 | preflight | unchanged | fix prerequisite |
| 4 | continuity lost | `RESYNC_REQUIRED` | `cleanup --yes`, `bootstrap` |
| 5 | unsupported change / consistency violation | `ERROR` | fix target schema or investigate divergence, then `run` |

The `pgsync.job` row carries `last_error`. The state transitions `ERROR → CATCHING_UP` when
`run` starts and `CATCHING_UP → STREAMING` once the acknowledged position reaches the server
WAL end observed at connect time.

## Logging and secrets

Structured logs (`--json` for JSON) on stderr. Connection URLs are read from the environment
and printed without passwords. Row values are never logged; `tokio_postgres` statement
parameter logging is forced to WARN level even under `--log debug`.

## Memory

Bounded by configuration: `max_inflight_bytes` (undelivered events) + `max_batch_bytes`
(pipelined statements) + `max_parallel_copies × 2 × buffer_size_kb` (COPY), plus connection
buffers. Server-side, PostgreSQL decodes large transactions under `logical_decoding_work_mem`
and spills to disk on the source; PGSync streams them into one target transaction without
buffering, so a 10 GB source transaction costs PGSync no additional memory but holds a long
target transaction.
