# Rust PostgreSQL replication library assessment

Reviewed 2026-10-12 against crates.io, docs.rs, and shallow clones of the repositories.

| Crate | Version | License | Status | Replication support |
|---|---|---|---|---|
| `tokio-postgres` / `postgres-protocol` | 0.7.18 / 0.6.12 (2026-06) | MIT/Apache-2.0 | Active (repo `rust-postgres/rust-postgres`, pushed 2026-10-03) | **None.** No `replication=database` startup parameter, no `CopyBothResponse`, no pgoutput parser. `Lsn` is `type Lsn = u64`. Git master is identical; support exists only in unmerged PRs #778 (2021–) and #1182. |
| `postgres-replication` | 0.6.7 | MIT/Apache-2.0 | Fork-only (MaterializeInc, supabase forks of rust-postgres) | pgoutput v1 parser + `LogicalReplicationStream`, but requires the forked `tokio-postgres` for the entire client stack. **Not on crates.io.** |
| `etl` (ex `pg_replicate`, supabase/etl) | 0.1.0, unpublished | Apache-2.0 | Active, pre-stable, MSRV 1.95 | Full pipeline (table-sync workers + apply worker, ctid-range COPY, `USE_SNAPSHOT`), pgoutput v1, destinations BigQuery/Iceberg/ClickHouse; no PostgreSQL destination. Depends on the supabase rust-postgres fork and `sqlx`. ~42k LOC. |
| `pg_walstream` | 0.9.0 (2026-09) | BSD-3-Clause | Single maintainer, CI on PG 15–18 | Own wire client (or libpq), pgoutput v1–v4, snapshot handoff, slot management. Default features pull `aws-lc-rs` (cmake). ~48k LOC. |
| `pgwire-replication` | 0.4.1 (2026-09) | MIT/Apache-2.0 | Single maintainer | Transport only (connect, SCRAM/MD5, start replication, feedback). No pgoutput decoding. ~5k LOC. |
| `pgoutput` | 0.0.7 | MIT | 0 stars, no README | Parser only; untested. |
| `sqlx` | 0.9.0 | MIT/Apache-2.0 | Active | No replication protocol. |
| `tokio-postgres-rustls` | 0.14.0 (2026-05) | MIT | Active | TLS for tokio-postgres with chain + hostname verification by default; `tls-server-end-point` channel binding. |
| `pg_query` | 6.2.1 | MIT | Active | libpg_query bindings. Only relevant for DDL parsing (Beta 2). |

## Decision

**Option (a): crates.io `tokio-postgres` 0.7.18 for ordinary SQL, plus an in-house replication
connection built on `postgres-protocol` 0.6.12.**

`postgres-protocol` already provides: startup message encoding, SCRAM-SHA-256 (`sasl`) and
MD5 helpers, simple query encoding, `CopyData` framing, and parsing of every backend message
PGSync needs except `CopyBothResponse` (`'W'`), which PGSync parses itself (one tag). The
adapter therefore consists of: socket + optional `tokio-rustls` TLS, startup with
`replication=database`, authentication, simple-query result decoding, CopyBoth streaming, and
a pgoutput decoder. This is roughly 900 lines, fully unit-testable from byte fixtures, and
pinned to published crate versions.

Rejected:
- Fork dependencies (`postgres-replication`, `etl`): the whole database client would come from
  an unpublished git revision that tracks upstream manually.
- `pg_walstream`: viable, but a single-maintainer 48k-line surface for a component where PGSync
  needs byte-level control over acknowledgement semantics; the BSD-3 licence would be fine.
- `pgwire-replication`: would still require the pgoutput decoder; saves little.

Borrowed ideas (no code): `etl`'s per-table state machine and ctid-range copy for Beta 2;
`postgres-replication`'s message enum shape.
