# Beta 1.1 assessment — PGSync v0.1.1

Baseline: Beta 1 at `8f54509` (assessed as `61eb49f`). Beta 1.1 commits: `a6d6c07`,
`7fc2298`, `ee900d8`, `ec30fed`, `949dee0`, `acb65e6`, `7761425`, `a61cc6b`, `6b82996`,
`1b0ed12`, `ac9ca2c` and later documentation commits (`git log 8f54509..`). Nothing pushed.
Date: 2026-10-13/14. Evidence: `docs/test-report.md`, `docs/performance.md`,
`docs/compatibility.md`, `bench-results/`.

## Verdict

**VERDICT_PLACEHOLDER**

Meaning: engineering readiness to start the masking phase on this engine. It is **not**
permission to connect unsanitised customer production data; that remains blocked until
Beta 2 masking exists (`docs/production-readiness-gaps.md`).

## What was tested

| Area (spec §) | Method |
|---|---|
| Correctness audit (§3) | Line-by-line review of the engine, protocol client, decoder, sink, bootstrap, preflight, URL/TLS policy; every finding turned into a fix with a regression test or into a documented limitation |
| PostgreSQL 16/17/18 (§4) | Host-local clusters per version: unit, live protocol, 25 fast and 5 disruptive end-to-end scenarios, slot invalidation; matching majors only |
| TLS (§5) | A TLS-only PostgreSQL on a non-loopback address with a private CA and SCRAM: 7 negative/positive tests on both connection kinds, the live protocol test and the whole fast suite over TLS |
| Soak and recovery (§6) | `scripts/soak.sh`: phased workload (steady, burst, idle, large transactions), fixed SIGKILL/SIGTERM/walsender-kill schedule, 10 s sampling, final chunked content hashes |
| Memory and concurrency (§7) | Message-size limit enforcement test; 1/5/10/20-job matrix with idle and active phases and incremental per-job cost |
| Larger dataset (§8) | 2 × 10 M rows (2.9 GB COPY) with writers during the copy; catch-up on a 4 GB dataset; baseline-sized rerun |
| WAL and source safety (§9) | Slot invalidation while stopped and streaming, stalled target (SIGSTOP), slow consumer (backpressure), source/target restarts, walsender termination, orphaned/failed-bootstrap slots, publication changes |
| Diagnostics (§10) | Heartbeat row and `status --json`, verified end to end |

## What passed

- Unit: 66 tests. Live protocol: pass on 16, 17, 18 and over TLS.
- End to end: 25 fast scenarios × 3 versions, 5 disruptive scenarios × 3 versions
  (including a 15-round random SIGKILL/SIGTERM loop per version), 25 fast scenarios over
  TLS, 7 TLS-lab tests. All contents compared by full-row hashes, never counts.
- No password or row value in 254 run logs.
- Benchmarks: all runs ended consistent (except the first 10 M-row run whose hash step itself
  failed on size; repeated with a chunked hash, see `docs/performance.md`).
- Soak: SOAK_PLACEHOLDER

## What failed (and was fixed)

Fourteen defects, the first two of which could have caused silent divergence on customer
data (`docs/test-report.md`, "Bugs found"):

1. Values that no longer fit the target column were **silently truncated or rounded**
   (explicit modified-type casts). Now a consistency stop.
2. A table added to the publication after bootstrap was **applied to a same-named target
   table that was never snapshotted**. Now `RESYNC_REQUIRED`.
3. Orphaned slot after a crash in bootstrap; 4. failed snapshot retained WAL forever;
   5. inherited target timeouts stalled the job; 6. oversized messages exceeded the memory
   budget silently; 7. slot loss on the stream reported as an internal error; 8–9. libpq TLS
   URL parameters and `pg_dump` root-certificate handling; 10. TLS failure reasons hidden
   and retried; 11. shutdown could block on a dead peer; 12. lag metric counted
   keepalive-acknowledged WAL; 13. `cdc_only` did not do what it said; 14. a wrong
   statement in the operations guide about dropped columns.

No test in the Beta 1 suite regressed.

## What remains unsupported (unchanged unless noted)

pgoutput protocol v1 only (no in-progress streaming, no two-phase), no DDL replication,
no sequences or large objects, no partitioned tables, row filters or column lists, no
`REPLICA IDENTITY NOTHING`, no partial snapshot resume, no disk spill, no masking, single
process, no client certificates, no SCRAM channel binding on the replication connection,
**`replication.mode: cdc_only` now rejected** (new), mixed PostgreSQL majors untested.

## Operational risks that remain

1. **WAL retention** while a consumer is stopped or slow is inherent to logical slots;
   pgsync detects, alarms, can pause, and reports invalidation as a resync, but only an
   operator (or `max_slot_wal_keep_size`) releases WAL. Set `max_slot_wal_keep_size`.
2. **Long target transactions** for large source transactions (bounded pgsync memory, but
   locks and bloat on the target). Beta 2: protocol v2 with spill.
3. **Snapshot pins xmin** for the copy duration (minutes for GBs, hours for TBs).
4. **Single-threaded apply** (~70 k row ops/s on in-memory data, I/O bound on large
   targets); a sustained faster source grows slot lag until invalidation.
5. **No supervisor**: a crashed process stays down; `job_runtime.stale` is the signal.
6. **Lossy-but-valid conversions** (numeric scale, timestamp precision) under
   `schema.mode: existing` with narrower target types are applied like `COPY` would.
7. **Linux and ≥ 10 GB datasets unmeasured**; numbers are macOS/M1 Pro.

## Can the implementation proceed to masking?

Yes, on engineering grounds: the single apply path (`PgSink::relation`/`apply`), the
`Op`/`Tuple` model shared by decoder and sink, byte-accounted channel, checkpoint-with-data
atomicity, acknowledgement after commit and the message-size limit are all independent of
what values contain, and Beta 1.1 added the hooks masking needs (configured-table refusal,
base-type casts, heartbeat for publication gating). The proposal is in
`docs/design/beta2-masking.md`.

## Benchmark summary

| Metric | Beta 1 baseline | Beta 1.1 result |
|---|---|---|
| Snapshot MB/s | 36.5 (1 GB) | 32.5 (2.9 GB with concurrent writers); 38.1 (100 MB) |
| Peak snapshot RSS | 33 MB | 40.1 MB (2.9 GB, includes catch-up with a full channel) |
| CDC transactions/sec | 625 | CATCHUP_PLACEHOLDER |
| CDC row operations/sec | ~72 K | CATCHUP_ROWS_PLACEHOLDER |
| 20-job memory | 9.5 MB/job reported | 9.7–11.4 MB incremental per job (aggregate 204–220 MB for 20) |
| Longest soak | 35 minutes | SOAK_DURATION_PLACEHOLDER |
| Largest snapshot | 1 GB | 2.9 GB COPY (2 × 10 M rows) |

Hardware: Apple M1 Pro, 16 GiB, macOS 14.4, PostgreSQL 18.6 host-local; see
`docs/performance.md` for conditions and why some pairs are not directly comparable.

## Test commands

See `docs/test-report.md` (exact commands and environment variables) and
`docs/linux-benchmark.md` (benchmark recipe).
