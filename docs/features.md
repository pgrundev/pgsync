# Supported features and limitations (v0.1.0)

## PostgreSQL versions

| Role | Versions | Notes |
|---|---|---|
| Source | 16, 17, 18 | Tested in CI against `postgres:16`, `:17`, `:18` images. 14/15 pass preflight with a warning but are untested. |
| Target | 16, 17, 18 | Same major as the source is the reference setup. `pg_dump` must be at least the source's major version. |

Reference environment: PostgreSQL 18 → 18 (`docker-compose.yml`).

## Operations

| Operation | Status | Notes |
|---|---|---|
| INSERT | Supported | `OVERRIDING SYSTEM VALUE` so identity values are preserved. |
| UPDATE | Supported | Key from the old-key tuple when the key changed, else from the new tuple. Unchanged TOAST columns are left untouched. `GENERATED ALWAYS AS IDENTITY` columns are never assigned. |
| DELETE | Supported | By replica identity key, or by full row for `REPLICA IDENTITY FULL`. |
| TRUNCATE | Supported | `CASCADE` and `RESTART IDENTITY` flags honoured. |
| Transaction boundaries | Supported | One target transaction per source transaction; checkpoint updated inside it. |
| Rolled-back source transactions | Supported | Never reach the stream. |
| Logical decoding messages (`pg_logical_emit_message`) | Ignored with a debug log | Not data changes. |
| Streaming of in-progress transactions (proto v2+) | Not supported | Protocol v1 only; the server sends whole transactions at commit. |
| Two-phase commit | Not supported | Detected: the job stops with `unsupported`. |
| DDL | Not replicated | Column set changes stop the job with `ERROR` naming the column; apply the DDL on the target and `run`. |
| Sequences | Not replicated | Identity/serial values are carried in the rows. |
| Large objects | Not supported | PostgreSQL limitation. |

## Tables

| Requirement | Enforced by |
|---|---|
| Ordinary tables (`relkind = r`) | preflight (partitioned, foreign, views rejected) |
| Primary key, or `REPLICA IDENTITY USING INDEX`, or `REPLICA IDENTITY FULL` | preflight (`NOTHING`, and `DEFAULT` without PK rejected) |
| `SELECT` privilege for the replication role | preflight |
| Table set equals the publication's table set | preflight |
| No publication column lists or row filters | preflight |
| No `publish_via_partition_root` | preflight |
| Publication publishes insert, update and delete | preflight |

Generated columns are not published by default and are recomputed on the target.

## Data types

Values travel in PostgreSQL text format and are cast server-side to the target column's
declared type, so any type with text input/output works. Tested end to end
(`null_and_common_types_round_trip`): smallint, integer, bigint, real, double precision,
numeric (incl. NaN), boolean, text, varchar, char, timestamp (incl. infinity), timestamptz,
date, time, interval, uuid, json, jsonb, bytea (hex), integer[], text[] (with quotes, commas,
NULL elements), inet, 300 kB TOAST text, NULLs in every column, special characters
(tabs, newlines, backslashes, quotes).

Not tested: user-defined types, enums, composite types, ranges, arrays of composites,
`binary` wire format (unused).

## Snapshot

- `CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT`; copy sessions import the snapshot with
  `SET TRANSACTION SNAPSHOT` under `REPEATABLE READ READ ONLY`.
- `COPY ... TO STDOUT` → `COPY ... FROM STDIN` in text format, streamed in `buffer_size_kb`
  chunks; one target transaction per table.
- Bounded parallelism (`max_parallel_copies`), each worker with its own pair of connections.
- Schema from `pg_dump --schema-only` (tables, constraints, indexes, identity sequences) or
  `schema.mode: existing`.
- Failure: the job stays `SNAPSHOTTING`; `bootstrap --restart-snapshot` drops the owned slot
  and the target tables (pg_dump mode) or truncates them (existing mode) and starts over.
  No partial resume.

## Recovery

| Situation | Behaviour |
|---|---|
| Source connection lost / walsender terminated / source restart | Reconnect with backoff; resume from `confirmed_flush_lsn`; redelivered transactions skipped by LSN. |
| Target connection lost / target restart | Rollback, reconnect, reload checkpoint from the target, reconnect to the source. |
| PGSync killed (SIGKILL) | On `run`: checkpoint from the target decides; nothing lost or duplicated. |
| SIGTERM / Ctrl-C | Current transaction finishes or rolls back; final feedback sent; exit 0. |
| Slot missing / invalidated (`wal_status = lost`) | `RESYNC_REQUIRED`, exit 4. `cleanup --yes` then `bootstrap`. |
| Slot advanced past the checkpoint (another consumer) | `RESYNC_REQUIRED`. |
| Source `systemid` changed | `RESYNC_REQUIRED`. |
| UPDATE/DELETE matches 0 or >1 rows on the target | `ERROR` (consistency), exit 5, transaction rolled back. |
| Unknown column / table on the target | `ERROR` (unsupported), exit 5. |
| WAL retention above `wal_retention_alarm_bytes` | Alarm log; with `wal_retention_action: stop` the job pauses (`PAUSED`). |

## Not in Beta 1

Masking, DDL replication, multi-node HA, PGXStore/PGX sinks, Rails integration, non-PostgreSQL
sinks, source failover, partial snapshot resume, disk spill for in-flight transactions,
Prometheus endpoint, parallel ctid-range copy, binary wire format, channel binding for SCRAM.
