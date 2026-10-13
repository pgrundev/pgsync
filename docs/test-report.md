# Test report — PGSync v0.1.0

Commit: `__COMMIT__` (see `git log`). Date: 2026-10-13.
Machine: Apple M1 Pro, 10 cores, 16 GiB RAM, macOS 14.4 (Darwin 23.4.0).
Docker Desktop 28.5.1 (Linux VM, 10 CPUs, 11.7 GiB) for the end-to-end suite;
host-local PostgreSQL 18.6 (Homebrew) for the large-table scenario and benchmarks because the
Docker VM disk was full.

## Commands

```bash
cargo test --workspace                                   # unit tests, no database
docker compose up -d --wait
PGSYNC_TEST_SOURCE_URL=postgres://pgsync:pgsync@localhost:5433/app \
  cargo test -p pgsync-postgres --test live_replication  # live protocol test
PGSYNC_E2E=1 PGSYNC_E2E_DIR=target/e2e \
  cargo test -p pgsync-cli --test e2e -- --test-threads=1            # 17 fast scenarios
PGSYNC_E2E=1 PGSYNC_E2E_DIR=target/e2e \
  cargo test -p pgsync-cli --test e2e -- --test-threads=1 --ignored  # 5 disruptive scenarios
```

## Unit tests (`cargo test --workspace`)

| Crate | Tests | Covers |
|---|---|---|
| pgsync-core | 39 | LSN parsing/ordering/serde, config validation (defaults, bad values, unknown fields, budgets, durations), error categories, job states, checkpoint invariants and serde, byte-accounted channel (item/byte limits, oversized items, permit release), backoff, metrics, engine: ordering, ack-after-commit, skip of redelivered transactions, keepalive handling, catch-up transition, commit failure rollback, protocol violations, feedback under backpressure, reconnect driver with checkpoint reload, refusal without checkpoint / in RESYNC_REQUIRED |
| pgsync-postgres | 23 | SQLSTATE classification, URL/TLS policy (loopback vs remote, insecure override), pgoutput decoder from byte fixtures (keepalive, begin/commit, relation, insert/update/delete tuple variants, binary values, truncation, truncate/origin/type/message, unsupported tags), standby status layout, slot JSON parsing across versions, START_REPLICATION quoting, slot continuity checks, pg_dump filtering, column verification, SQL generation (OVERRIDING SYSTEM VALUE, identity/TOAST-aware UPDATE, key selection, FULL identity, missing key, generated columns) |

Result: 62 passed, 0 failed (39 + 23), plus `cargo clippy --workspace --all-targets -- -D warnings` and `cargo fmt --check` clean.

## Live protocol test (`live_replication`)

Opens a real replication connection (SCRAM-SHA-256), `IDENTIFY_SYSTEM`, creates a slot with
an exported snapshot, starts streaming, decodes an insert with eight column types, sends
feedback with reply request, receives the keepalive, verifies `confirmed_flush_lsn` moved,
drops the slot. Result: pass (PostgreSQL 18.6).

## End-to-end scenarios (`e2e`)

Each scenario creates its own schema, publication, slot and job, drives the real `pgsync`
binary as a subprocess, and verifies by comparing an MD5 over **full row contents** (all
columns, text form, ordered by key) between source and target, never just row counts.

| # | Spec scenario | Test | Result |
|---|---|---|---|
| 1 | Empty source table | `empty_tables_then_cdc` | pass |
| 2 | Initial snapshot with existing rows | `snapshot_existing_rows_then_cdc_and_restart` (2,000 rows, graceful restart, resume) | pass |
| 3–5 | Inserts, updates, deletes during the snapshot | `writes_during_snapshot_are_not_lost_or_duplicated` (80k rows copied while 200 mixed transactions run) | pass |
| 6 | Concurrent source transactions | `concurrent_transactions_preserve_order` (6 writers × 100 interleaved transactions on shared rows) | pass |
| 7 | Large tables | `large_table_snapshot` (1.2 M rows, host clusters) | pass (1.2 M rows snapshotted in 6.1 s, converged) |
| 8 | Large rows and TOAST | `large_rows_and_toast_updates` (400 kB bytea, 5 MB text, unchanged-TOAST updates) | pass |
| 9 | NULL and common types | `null_and_common_types_round_trip` (23 types incl. arrays, json/jsonb, bytea, intervals, infinity, NaN, special characters) | pass |
| 10 | Transaction rollback | `transaction_rollback_and_truncate` (also TRUNCATE, RESTART IDENTITY CASCADE) | pass |
| 11 | Source disconnect | `source_walsender_killed_reconnects` (`pg_terminate_backend` on the walsender) | pass |
| 11b | Source restart | `source_restart_is_survived` (`docker restart`) | pass (after fixing walsender-shutdown handling) |
| 12 | Destination disconnect | `target_connection_killed_reconnects` | pass |
| 13 | Replication worker crash | `crash_sigkill_and_resume_without_loss_or_duplication` (two SIGKILLs under a 400-transaction non-idempotent write load) | pass |
| 14 | Restart and resume from checkpoint | covered by 2, 13, 20 | pass |
| 15 | Destination unavailable temporarily | `destination_restart_is_survived` (`docker restart`), `destination_paused_backpressure_then_recovers` | pass |
| 16 | Unsupported schema change | `unsupported_schema_change_stops_visibly` (ADD COLUMN → ERROR naming the column, exit 5, nothing applied; DDL on target → resume) | pass |
| 17 | Missing replication slot | `missing_slot_requires_resync` (exit 4, RESYNC_REQUIRED, stays refused, cleanup works) | pass |
| 17b | Slot advanced by another consumer | `slot_advanced_by_another_consumer_requires_resync` | pass |
| 18 | WAL retention threshold | `wal_retention_threshold_stops_job` (alarm, stop → PAUSED, resume after raising the limit) | pass |
| 19 | Slow destination backpressure | `destination_paused_backpressure_then_recovers` (target container paused 20 s with a 64 KiB channel budget while 3,000 rows are written; walsender stays connected; convergence after unpause) | pass |
| 20 | Repeated interruption and restart | `repeated_random_crashes_converge` (15 rounds, random 100–2500 ms, 70 % SIGKILL / 30 % SIGTERM, continuous writer: 9,800 writer transactions + 196 bulk updates = 9,996 applied, hashes equal) | pass |
| — | Idle publication: slot advances over unpublished WAL | `idle_slot_advances_over_unpublished_wal` (20 k rows into an unpublished table; `confirmed_flush_lsn` reaches the WAL head) | pass |
| — | Preflight rejections | `preflight_rejects_bad_tables_and_publications` (no PK, REPLICA IDENTITY NOTHING, missing table; bootstrap refused without creating a slot) | pass |
| — | CLI status/cleanup | `status_and_cleanup_commands` | pass |
| — | Duplicate bootstrap / incomplete snapshot | `bootstrap_refuses_duplicate_and_incomplete_snapshot_restart` | pass |

Final run of the fast suite after all fixes: 18 passed, 0 failed, 5 ignored (61 s on Docker). Disruptive suite: 5 passed after the
fixes described below.

## Spec failure points (section 9)

| Failure point | How it is exercised | Outcome |
|---|---|---|
| Before applying a transaction | SIGKILL loop (20); reader error between transactions (11) | redelivered and applied once |
| During transaction apply | SIGKILL loop (20); target connection killed mid-stream (12) | target rolls back; redelivered |
| After target commit, before WAL ack | unit test `skips_redelivered_transactions_without_touching_sink`; SIGKILL loop hits this window statistically (`txns_skipped` metric) | skipped by LSN comparison |
| During reconnect | 11, 11b, 12, 15 | resumed |
| While transferring snapshot data | 3–5; `bootstrap_refuses_duplicate_and_incomplete_snapshot_restart` simulates a dead snapshot | restart required, never marked complete |
| After a PostgreSQL restart | 11b (source), 15 (target) | resumed |
| Temporary network loss | 11, 12, 19 (frozen peer) | resumed; feedback keeps the walsender alive |

## Bugs found by the suite (fixed before release)

1. UPDATE assigned `GENERATED ALWAYS AS IDENTITY` columns (SQLSTATE 428C9). Fixed by never
   assigning identity-always columns and skipping key columns when the key did not change.
2. A logical walsender shutting down ends the stream with `CommandComplete`, not `CopyDone`;
   this was classified as an internal error and left the job in `ERROR`. Now transient.
3. Resuming from `PAUSED` never returned to `STREAMING`.
4. `bootstrap --restart-snapshot` after `cleanup` did not reset leftover target tables.
5. Engine: a reader failure mid-transaction was masked by the applier's "cancelled" result.
6. `tokio_postgres` logs statement parameters at DEBUG; forced to WARN so row values never
   reach logs.
7. Found by the 40-minute soak: periodic unsolicited feedback prevented the walsender's
   keepalive, so an idle publication never acknowledged skipped WAL (autovacuum, unpublished
   tables) and the slot would retain it. The reader now reports only changed positions
   while reading and keeps the unconditional heartbeat only while blocked by backpressure.

## Not covered

- PostgreSQL 16 and 17 as source/target are in the CI matrix but were not executed in this
  session (only 18.6 was run locally).
- TLS end to end (certificate verification code path is unit-tested for policy only; no
  TLS-enabled server was available).
- User-defined types, enums, ranges, composite types.
- Multi-hour soak (see benchmarks for the longest run performed).
