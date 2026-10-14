# Test report — PGSync v0.1.1 (Beta 1.1)

Commits: Beta 1 baseline `8f54509`; Beta 1.1 `a6d6c07` … `HEAD` (see `git log 8f54509..`).
Date: 2026-10-13/14.
Machine: Apple M1 Pro, 10 cores, 16 GiB RAM, macOS 14.4 (Darwin 23.4.0), Apple SSD.
Databases: host-local Homebrew clusters, PostgreSQL 16.11, 17.11 and 18.6, matching
source/target majors, started by `scripts/local-pg.sh`; a TLS pair (18.6) by
`scripts/tls-lab.sh`. Docker was not usable (VM disk full), so the Compose pair was not
run; CI still runs the fast suite against `postgres:16/17/18` images.

## Commands

```bash
# Unit tests (no database): 41 core + 25 adapter
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all -- --check

# Host-local pairs (PG_BIN per version; ports as used in this report)
PG_BIN=/opt/homebrew/opt/postgresql@18/bin ./scripts/local-pg.sh start $PWD/target/local-pg18          # 5533/5534
PG_BIN=/opt/homebrew/opt/postgresql@16/bin SRC_PORT=51633 TGT_PORT=51634 ./scripts/local-pg.sh start $PWD/target/local-pg16
PG_BIN=/opt/homebrew/opt/postgresql@17/bin SRC_PORT=51733 TGT_PORT=51734 ./scripts/local-pg.sh start $PWD/target/local-pg17

# Per version: live protocol test, fast suite (25 scenarios), disruptive suite (5 scenarios)
export PGSYNC_E2E=1 PGSYNC_E2E_DIR=$PWD/target/e2e-pg18 \
  PGSYNC_E2E_SOURCE_ADMIN_URL=postgres://postgres@localhost:5533/app \
  PGSYNC_E2E_SOURCE_URL=postgres://pgsync:pgsync@localhost:5533/app \
  PGSYNC_E2E_TARGET_URL=postgres://postgres@localhost:5534/app \
  PGSYNC_E2E_SOURCE_DATADIR=$PWD/target/local-pg18/source PGSYNC_E2E_TARGET_DATADIR=$PWD/target/local-pg18/target \
  PGSYNC_E2E_PG_BIN=/opt/homebrew/opt/postgresql@18/bin PGSYNC_E2E_CRASH_ROUNDS=15
PGSYNC_TEST_SOURCE_URL=$PGSYNC_E2E_SOURCE_URL PGSYNC_TEST_SOURCE_ADMIN_URL=$PGSYNC_E2E_SOURCE_ADMIN_URL \
  cargo test -p pgsync-postgres --test live_replication
cargo test -p pgsync-cli --test e2e -- --test-threads=1
cargo test -p pgsync-cli --test e2e -- --test-threads=1 --ignored

# TLS lab (PostgreSQL 18, ssl=on, hostssl + scram on the LAN address)
PG_BIN=/opt/homebrew/opt/postgresql@18/bin ./scripts/tls-lab.sh start
eval "$(./scripts/tls-lab.sh env)"
PGSYNC_TLS_LAB=1 PGSYNC_TLS_LAB_DIR=$PWD/target/tls-lab PGSYNC_TLS_PG_BIN=/opt/homebrew/opt/postgresql@18/bin \
  PGSYNC_TLS_PLAIN_URL=postgres://postgres@127.0.0.1:5533/app \
  cargo test -p pgsync-postgres --test tls_lab -- --test-threads=1
PGSYNC_TEST_SOURCE_URL=$PGSYNC_E2E_SOURCE_URL PGSYNC_TEST_SOURCE_ADMIN_URL=$PGSYNC_E2E_SOURCE_ADMIN_URL \
  cargo test -p pgsync-postgres --test live_replication          # over TLS
PGSYNC_E2E=1 PGSYNC_E2E_DIR=$PWD/target/e2e-tls cargo test -p pgsync-cli --test e2e -- --test-threads=1   # over TLS

# Benchmarks and soak: see docs/performance.md
```

## Results by suite

| Suite | PG 16.11 | PG 17.11 | PG 18.6 | PG 18.6 over TLS |
|---|---|---|---|---|
| Unit (66) | pass | pass | pass | — |
| Live protocol (1) | pass | pass | pass | pass |
| Fast e2e (25) | 25 pass | 25 pass | 25 pass | 24 pass + scenario 30 run separately: pass |
| Disruptive e2e (5) | 5 pass | 5 pass | 5 pass | — |
| TLS lab (7) | — | — | — | 7 pass |

Final runs after the last code change (`7761425`, the cast fix touches every statement the
sink emits): PG 18 fast suite 25/25, PG 17 fast suite 25/25. Soak (68 min, 7 interruptions)
and benchmarks: `docs/performance.md`; every run ended with identical contents except the
first 10 M-row run, whose hash step overflowed and was repeated at 3 M rows.

## Scenarios

Each scenario creates its own schema, publication, slot and job, drives the real `pgsync`
binary as a subprocess, and verifies by comparing an MD5 over **full row contents** between
source and target. Scenarios 1–23 are those of the Beta 1 report (unchanged); 24–30 are
new in Beta 1.1 and each pins a bug or a behaviour that was previously unverified.

| # | Scenario | Test | What it proves |
|---|---|---|---|
| 1–23 | Beta 1 scenarios (snapshot, CDC, types, TOAST, rollback/truncate, concurrency, crash, reconnects, DDL refusal, missing/advanced slot, preflight, retention alarm, status/cleanup, duplicate bootstrap, idle slot, backpressure, restarts, random crash loop, large table) | as in the Beta 1 report | unchanged; all pass on 16/17/18 |
| 24 | Published but unconfigured table | `published_but_unconfigured_table_stops_visibly` | a table added to the publication after bootstrap is refused with `RESYNC_REQUIRED` (exit 4), nothing applied, `run` keeps refusing, recovery by re-bootstrap with the table configured |
| 25 | Interrupted bootstrap | `bootstrap_recovers_from_interrupted_bootstrap` | an `INITIALIZING` leftover with an orphan slot is cleaned by the next `bootstrap`; `cleanup` drops it too |
| 26 | Snapshot failure | `snapshot_failure_drops_the_fresh_slot` | a failed copy (`snapshot_timeout`) drops the slot it created, records it, `run` refuses, `--restart-snapshot` recovers |
| 27 | Inherited target timeouts | `inherited_target_timeouts_do_not_stall_copy_or_apply` | `statement_timeout` / `idle_in_transaction_session_timeout` on the target database cannot cancel the copy or the apply |
| 28 | Oversized message | `oversized_message_is_refused_without_ack_and_resumes_after_raising_the_limit` | a 3 MB row with a 1 MiB limit stops the job (`resource`, exit 1) **before** buffering, the slot still holds the transaction, raising the limit and `run` applies it |
| 29 | Slot invalidation | `invalidated_slot_requires_resync_stopped_and_streaming` | `max_slot_wal_keep_size` exceeded while stopped and while streaming (target frozen) → `RESYNC_REQUIRED`, exit 4, keeps refusing, `status` names the recovery |
| 30 | Source column drop / type change | `source_column_drop_and_type_change_behaviour_is_explicit` | dropped column: target keeps it with the default; widened type keeps replicating; a value that no longer fits stops the job (exit 5, `22001`) with nothing altered; widening the target and `run` applies it |

TLS lab (`tests/tls_lab.rs`): trusted CA accepted on SQL and replication connections
(TLS 1.3, SCRAM); unknown issuer rejected on both; hostname mismatch rejected on both
(certificate swapped live); `sslmode=disable` and `insecure_skip_tls_verify` refused for a
remote host; TLS required against `ssl=off` fails clearly; reconnect over TLS after the
server terminates the session; no password in labels or errors. Plus the whole fast suite
and the live protocol test over the lab. 254 run logs were scanned for passwords and row
values: none.

## Spec failure points (Beta 1.1 §3)

| Crash / failure window | Exercised by | Outcome |
|---|---|---|
| Before transaction application | random crash loop (15 rounds × 3 versions), walsender kill, source restart | redelivered, applied once |
| During transaction application | crash loop; target backend killed; target frozen (SIGSTOP) then resumed | target rolls back; redelivered |
| After destination commit, before WAL ack | unit `skips_redelivered_transactions_without_touching_sink`; crash loop (`txns_skipped` > 0 observed) | skipped by LSN; never applied twice |
| Before WAL acknowledgment with keepalive-only progress | `idle_slot_advances_over_unpublished_wal` | position persisted before it is reported |
| During checkpoint recovery | `run` after SIGKILL in every crash scenario; invariants checked on load | resumes from the stored checkpoint; a violating row is refused |
| Bootstrap: between slot creation and job row | scenario 25 | recovered (no orphan slot) |
| Bootstrap: during copy | scenario 26 | slot dropped, restart required, nothing marked complete |
| Last applied transaction and checkpoint atomic | `PgSink::commit` writes `pgsync.job` inside the data transaction; scenarios 13, 20, 30 | a crash after `COMMIT` returns leaves both; before, neither |

## Edge cases (Beta 1.1 §3)

| Case | Covered by | Result |
|---|---|---|
| INSERT / UPDATE / DELETE / TRUNCATE (CASCADE, RESTART IDENTITY) | 1–10 | replicated |
| Unchanged TOAST, large TOAST (5 MB), NULLs | 8, 9, 28 | replicated; `u` values leave the target untouched |
| Primary-key update | `concurrent_transactions_preserve_order`, unit `where_prefers_old_key_and_handles_full_identity` | old key used in WHERE |
| Missing primary key / REPLICA IDENTITY NOTHING | preflight scenario | rejected before bootstrap |
| Empty and idle publication, unpublished WAL | `idle_slot_advances_over_unpublished_wal` | slot advances over skipped WAL |
| Long-running transaction on the target | 27 (timeouts reset) | not cancelled |
| Rollback | 10 | never reaches the stream |
| Schema changes | 16, 24, 30 | refused or explicitly tolerated as documented, never silently altered |
| Relation metadata changes (new `Relation` message mid-stream) | 16, 30 | re-resolved against the target before apply |
| Connection and slot loss | 11, 11b, 12, 15, 17, 17b, 29 | reconnect, or `RESYNC_REQUIRED` |
| Unsupported pgoutput messages (streaming, two-phase) | unit `truncate_origin_type_message_unsupported` | job stops with `unsupported` |

## Bugs found by Beta 1.1 (all fixed, each with a regression test)

1. **Silent truncation/rounding of values on apply** (`7761425`). Parameters were cast to
   the target column's declared type with modifier; `$1::varchar(80)` truncates and
   `$1::numeric(12,2)` rounds without error. Found by scenario 30 (a 200-character name
   arrived as 80 characters with the job `STREAMING`). Fix: cast to the base type.
2. **Published-but-unconfigured tables applied to a same-named target table**
   (`a6d6c07`). Fix: refuse with `RESYNC_REQUIRED`.
3. **Orphaned slot after a crash between slot creation and the job row** (`7fc2298`).
   Fix: job row first; recovery from `INITIALIZING`.
4. **Failed snapshot left its slot retaining WAL indefinitely** (`7fc2298`). Fix: drop it.
5. **Inherited target timeouts stalled the job forever** (57014 retried) (`7fc2298`).
   Fix: sessions reset them.
6. **Oversized messages silently exceeded the memory budget** (`ee900d8`). Fix:
   `max_message_bytes` enforced before buffering.
7. **Slot invalidation / removed WAL on the stream reported as an internal error** instead
   of `RESYNC_REQUIRED` (`ee900d8`).
8. **libpq `sslmode=verify-full|verify-ca` and `sslrootcert=` URLs rejected** by the URL
   parser; `sslcert`/`sslkey` would have been rejected with an unhelpful message
   (`ee900d8`).
9. **`pg_dump` could not verify a publicly trusted certificate** (no `PGSSLROOTCERT`)
   (`ee900d8`); and then refused `sslrootcert=system` with `sslmode=prefer` (fixed in the
   same commit after scenario 28 caught it on loopback).
10. **TLS failures reported as a generic internal "error performing TLS handshake"**
    and retried (`ec30fed`). Fix: reason surfaced, classified as configuration.
11. **Shutdown or reconnect could block on a reader stuck writing to a dead peer**
    (`ee900d8`). Fix: bounded join and feedback-write timeout.
12. **`apply_lag_bytes` counted keepalive-acknowledged WAL as lag** (`acb65e6`).
13. **`replication.mode: cdc_only` ran the snapshot and exited without streaming**
    (`7761425`). Fix: rejected by validation with the reason.
14. Documentation: "dropping a column stops the job" was wrong (it does not); corrected
    with the tested behaviour.

## Not covered

- Mixed PostgreSQL majors; PostgreSQL 14/15; Linux (recipe in `docs/linux-benchmark.md`).
- MD5 and clear-text authentication against a server; GSSAPI; client certificates.
- Network partitions other than peer freeze/restart/backend termination.
- Datasets above 10 M rows per table (disk); see `docs/performance.md`.
- User-defined types, enums, ranges, composites (text format should carry them; untested).
