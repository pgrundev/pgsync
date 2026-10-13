# PostgreSQL Logical Replication (pgoutput) — Implementation Reference

Compiled 2026-10-12 from the PostgreSQL 18 documentation (`/docs/current/`), with version
deltas checked against the 13–17 pages. This is the byte-level reference the
`pgsync-postgres` crate is written against. All integers are **big-endian**; `String` is
NUL-terminated; timestamps are `Int64` **microseconds since 2000-01-01 00:00:00 UTC**
(`unix_us = pg_us + 946_684_800_000_000`). LSNs are `Int64` on the wire and `X/Y` (hex) as text.

## 1. Replication connection and commands

- Startup parameter `replication=database` plus `dbname=<db>` enters **logical walsender
  mode**. Replication commands *and* ordinary SQL are accepted, but only via the **simple
  query protocol**.
- `IDENTIFY_SYSTEM` → one row: `systemid` (text), `timeline` (int8), `xlogpos` (text, current
  flush LSN), `dbname`.
- `CREATE_REPLICATION_SLOT name LOGICAL pgoutput (SNAPSHOT 'export')` (PG15+ syntax) or
  `CREATE_REPLICATION_SLOT name LOGICAL pgoutput EXPORT_SNAPSHOT` (legacy, works on all
  versions) → one row: `slot_name`, `consistent_point` (text LSN; earliest point streaming can
  start), `snapshot_name` (text), `output_plugin`.
  **The exported snapshot is valid until a new command is executed on this connection or the
  connection is closed.** Keep the slot-creating connection idle until every copy session has
  executed `SET TRANSACTION SNAPSHOT`.
- `START_REPLICATION SLOT name LOGICAL X/Y (proto_version '1', publication_names 'p1,"P 2"')`
  → server replies `CopyBothResponse` (`'W'`) then `CopyData` frames. Streaming begins at
  **max(requested LSN, slot `confirmed_flush_lsn`)**.
- `DROP_REPLICATION_SLOT name [WAIT]`.

## 2. CopyBoth framing

Outer frame: `'d'` + `Int32 len` (includes itself) + payload. The payload's first byte selects:

### XLogData (server→client) `'w'`, 25-byte header
| offset | size | field |
|---|---|---|
| 0 | 1 | `'w'` |
| 1 | 8 | WAL start of this message |
| 9 | 8 | current end of WAL on server |
| 17 | 8 | server clock (pg µs) |
| 25 | n | one pgoutput message |

### Primary keepalive (server→client) `'k'`, 18 bytes
| offset | size | field |
|---|---|---|
| 0 | 1 | `'k'` |
| 1 | 8 | current end of WAL |
| 9 | 8 | server clock |
| 17 | 1 | `1` = reply immediately or risk disconnect |

### Standby status update (client→server) `'r'`, 34 bytes
| offset | size | field |
|---|---|---|
| 0 | 1 | `'r'` |
| 1 | 8 | last WAL byte + 1 written |
| 9 | 8 | last WAL byte + 1 flushed (**this advances `confirmed_flush_lsn`**) |
| 17 | 8 | last WAL byte + 1 applied |
| 25 | 8 | client clock (pg µs) |
| 33 | 1 | `1` = request reply |

## 3. pgoutput

Options: `proto_version` (1 all; 2 PG14+ streaming; 3 PG15+ two-phase; 4 PG16+ parallel),
`publication_names` (required), `binary` (PG14+), `messages` (PG14+), `streaming`
(`off`/`on`/`parallel`), `two_phase` (proto ≥3), `origin` (`none`/`any`, PG16+).

Flow rules: transactions arrive whole and in commit order as `Begin … Commit` (proto 1, or
`streaming=off`). A `Relation` message precedes the first DML for a relation and is re-sent
whenever its definition changes; cache it. `Type` precedes `Relation` for non-built-in types.

Message layouts (`[Int32 xid]` is present only inside Stream Start/Stop blocks):

| Msg | Tag | Layout |
|---|---|---|
| Begin | `B` | Int64 final_lsn, Int64 commit_ts, Int32 xid |
| Commit | `C` | Int8 flags(0), Int64 commit_lsn, Int64 end_lsn, Int64 commit_ts |
| Origin | `O` | Int64 origin_commit_lsn, String name |
| Relation | `R` | [xid], Int32 oid, String ns (`""`=pg_catalog), String name, Int8 replident, Int16 ncols, per col: Int8 flags(1=key), String name, Int32 typoid, Int32 typmod |
| Type | `Y` | [xid], Int32 oid, String ns, String name |
| Insert | `I` | [xid], Int32 oid, `'N'`, TupleData |
| Update | `U` | [xid], Int32 oid, optional (`'K'` TupleData \| `'O'` TupleData), `'N'`, TupleData |
| Delete | `D` | [xid], Int32 oid, (`'K'` TupleData \| `'O'` TupleData) |
| Truncate | `T` | [xid], Int32 nrels, Int8 opts (1=CASCADE, 2=RESTART IDENTITY), Int32 oid × n |
| Message | `M` | [xid], Int8 flags, Int64 lsn, String prefix, Int32 len, bytes |
| Stream Start/Stop/Commit/Abort | `S`/`E`/`c`/`A` | proto ≥2 |
| Begin Prepare/Prepare/Commit Prepared/Rollback Prepared/Stream Prepare | `b`/`P`/`K`/`r`/`p` | proto ≥3 |

TupleData: `Int16 ncols`, then per column `'n'` (NULL), `'u'` (unchanged TOAST, not sent),
`'t'` Int32 len + bytes (text), `'b'` Int32 len + bytes (binary, PG14+ with `binary`).

Replica identity char: `d` default (PK), `n` nothing, `f` full, `i` index. Update sends `K`
only if a replica-identity column changed, `O` only for FULL; Delete sends exactly one.

## 4. Exported snapshot semantics

Creating a logical slot over the replication protocol exports a snapshot that shows
**exactly the state after which all changes will be included in the change stream**.
Everything visible in the snapshot is excluded from the stream starting at
`consistent_point`; everything committed later is included.

`SET TRANSACTION SNAPSHOT 'id'` must run inside an explicit transaction that is
REPEATABLE READ or SERIALIZABLE, before any query. Use
`BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET TRANSACTION SNAPSHOT '...';`.

Slot crash semantics: slot position is persisted only at checkpoint; after a server crash
the server may resend changes. **Consumers must track the last applied LSN and skip
already-applied transactions.**

## 5. Restrictions worth remembering

No DDL, no sequences, no large objects; only tables/partitioned tables. TRUNCATE is
replicated. A table whose publication publishes UPDATE/DELETE must have a replica identity
(PK, unique index via `REPLICA IDENTITY USING INDEX`, or FULL); otherwise the **publisher**
errors on UPDATE/DELETE.

## 6. Server configuration and slot monitoring

| GUC | notes |
|---|---|
| `wal_level` | must be `logical` |
| `max_replication_slots` | default 10 |
| `max_wal_senders` | default 10 |
| `wal_sender_timeout` | default 60 s; send status updates well within it |
| `max_slot_wal_keep_size` | default -1 (unlimited); slot becomes `lost` when exceeded |
| `idle_replication_slot_timeout` | PG18; invalidates idle slots |

`pg_replication_slots`: `active`, `restart_lsn`, `confirmed_flush_lsn`, `wal_status`
(`reserved`/`extended`/`unreserved`/`lost`, PG13+), `safe_wal_size` (PG13+),
`conflicting` (PG16+), `invalidation_reason` + `inactive_since` (PG17+).
Lag = `pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)`.

## 7. Privileges

Role needs `REPLICATION` + `LOGIN`, a matching `pg_hba.conf` entry for the database, and
`SELECT` on each published table (or `pg_read_all_data`). Publisher checks privileges once at
connection start. Row-level security policies run as the replication role unless
`options=-crow_security=off` is passed.
