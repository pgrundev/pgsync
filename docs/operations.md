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
| Source | logical replication slot (`source.slot`, plugin `pgoutput`) | `bootstrap` | `cleanup --yes`, `bootstrap --restart-snapshot`, and `bootstrap` itself when its snapshot fails or is interrupted (see below) |
| Target | schema `pgsync` with tables `job`, `job_runtime`, `table_sync` | first command touching the target | never (drop manually) |
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
(default 6 h) aborts the copy. `pgsync status` shows per-table progress in `table_sync`
and the heartbeat row (rows and bytes copied so far).

**Slot lifecycle during bootstrap.** The job row is written (state `INITIALIZING`,
`slot_owned = true`) *before* the slot is created, so a crash between the two leaves a
record that marks the slot as droppable: the next `bootstrap` drops it and continues, and
`cleanup --yes` drops it too. If the copy fails or is interrupted (error, timeout, Ctrl-C),
the slot created by that bootstrap is dropped immediately — the copy is all-or-nothing, so
the slot could only retain WAL — and `last_error` records both the failure and the fact
that the slot is gone. The job stays `SNAPSHOTTING`; `bootstrap --restart-snapshot` starts
over. A slot that predates the bootstrap is never touched.

Copy and apply sessions reset `statement_timeout`, `idle_in_transaction_session_timeout`,
`idle_session_timeout` and `transaction_timeout` on the target, so timeouts configured on
the target role or database cannot cancel a long copy or a large transaction on every
attempt.

## Resynchronisation

Required when continuity cannot be proven: slot missing, `wal_status = lost`,
`invalidation_reason` set (the source exceeded `max_slot_wal_keep_size` while the consumer
was behind — detected both at `run` and, while streaming, by the monitor and by the server's
own error), slot `confirmed_flush_lsn` beyond the checkpoint, the source `systemid`
changed, or a change for a published table that is not in `replication.tables`. The job is
marked `RESYNC_REQUIRED`, `run` exits 4 and refuses to start.

**Pausing a consumer versus releasing WAL.** Stopping pgsync (SIGTERM, `PAUSED` after a
retention alarm, `ERROR`) pauses the consumer: the slot keeps every byte of WAL since its
`restart_lsn`, and the source keeps growing it until the consumer resumes or
`max_slot_wal_keep_size` invalidates the slot. Only dropping the slot releases that WAL —
`cleanup --yes`, or `pg_drop_replication_slot` by an administrator — and after that the job
can only be rebuilt from a new snapshot. pgsync never drops a slot on its own except the one
it created in a bootstrap whose snapshot did not complete.

```
pgsync cleanup --yes                  # drops the owned slot (if any) and the job row
pgsync bootstrap --restart-snapshot   # drops/truncates target tables and copies again
```

There is deliberately no option to skip missing WAL or start from the current LSN.

## Schema changes on the source

Not replicated. What happens depends on the change (all verified by end-to-end tests):

| Source change | Effect |
|---|---|
| Column added (or renamed: a new name the target lacks) | next change to the table stops the job with `ERROR` naming the column; apply the DDL on the target and `pgsync run` |
| Column dropped | replication continues; the target keeps the column and new rows get its default (an error if it is `NOT NULL` without a default) |
| Column type changed | continues while every value still converts to the target type; a value that does not fit (too long, out of range) stops the job with a consistency error (exit 5) and nothing altered — values are never truncated or rounded to fit |
| Table added to the publication (`ALTER PUBLICATION ... ADD TABLE`, `FOR ALL TABLES`) | its first change stops the job with `RESYNC_REQUIRED`: the table was never snapshotted, and pgoutput resolves membership from the historic catalog, so removing it from the publication later does not filter the change. Add it to `replication.tables`, `cleanup --yes`, `bootstrap --restart-snapshot` |

Additive changes applied on the target **before** the source are harmless.

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

## Monitoring a running job

Every `safety.heartbeat_interval` (default 10 s) the process writes `pgsync.job_runtime`
in the target: pid, host, pgsync version, state, connection state (`connected`,
`reconnecting`, `disconnected`), source version, received and acknowledged positions,
apply lag in bytes, counters, transactions and rows per second over the last interval,
reconnects, RSS, last apply time, and `stopped_at` on a clean stop. `pgsync status`
(`--json` for the full structure) reads it and flags the row as stale when no heartbeat
arrived for three intervals without a recorded stop — the process is probably gone. Both
the table and the JSON are stable interfaces for external monitoring.

`status` also reports: seconds since the commit time of the last applied source
transaction, snapshot progress (tables copied / rows / bytes), slot lag and retained WAL,
`resync_required`, and `action` — the operator command for every state that needs one.

## Logging and secrets

Structured logs (`--json` for JSON) on stderr. Connection URLs are read from the environment
and printed without passwords. Row values are never logged; `tokio_postgres` statement
parameter logging is forced to WARN level even under `--log debug`.

## Memory

Bounded by configuration: `max_inflight_bytes` (undelivered events) + `max_batch_bytes`
(pipelined statements) + `max_parallel_copies × 2 × buffer_size_kb` (COPY) + two copies of
the largest single message (`max_message_bytes`: decoded, and serialised as statement
parameters), plus the runtime baseline (about 10 MB). `memory_budget_mb` must cover the sum;
validation refuses a configuration that cannot.

`resources.max_message_bytes` is enforced before a message is buffered: a single change
(one row) larger than the limit stops the job with a `resource` error naming the size,
nothing is acknowledged, and `run` applies it after the limit (and if necessary the
budget) is raised. Unset, the limit is derived from the budget and capped at 32 MiB. A
PostgreSQL row can be up to 1 GB; if your data has such rows, set the limit explicitly.

Server-side, PostgreSQL decodes large transactions under `logical_decoding_work_mem` and
spills to disk on the source; PGSync streams them into one target transaction without
buffering, so a 10 GB source transaction costs PGSync no additional memory but holds a long
target transaction.
