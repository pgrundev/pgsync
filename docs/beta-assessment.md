# Beta 1 assessment — PGSync v0.1.0

Commit: `61eb49f`. Date: 2026-10-13.

## Verdict: READY for Controlled Technical Beta (synthetic / approved non-sensitive data only)

NOT READY for any production customer data: no masking, no DDL handling, single node.

## Acceptance criteria (spec §16)

| Criterion | Evidence |
|---|---|
| Rust implementation builds reproducibly | `Cargo.lock` committed, crates.io dependencies only, `rust-toolchain.toml` stable, CI `cargo build`/`clippy -D warnings` |
| End-to-end PostgreSQL → PostgreSQL replication works | 22 e2e scenarios, README quick start executed manually |
| Initial snapshot and CDC handoff are consistent | `CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT` + `SET TRANSACTION SNAPSHOT`; `writes_during_snapshot_are_not_lost_or_duplicated` |
| INSERT, UPDATE and DELETE are replicated | all scenarios; TRUNCATE too |
| Transactions preserve atomicity and order | one target transaction per source transaction; `concurrent_transactions_preserve_order`; engine unit tests |
| Checkpoints are durable | `pgsync.job` row updated inside the data transaction, `synchronous_commit = on` |
| Restart does not lose transactions | scenarios 2, 13, 20 |
| Crash/reconnect tests pass | 11, 11b, 12, 13, 15, 19, 20 |
| Unsupported changes fail visibly and safely | 16, 17, 17b, preflight; no skip path exists in the code |
| No unbounded in-memory channels | one bounded channel (`channel_capacity`, `max_inflight_bytes`); batches bounded by rows/bytes; no transaction buffering |
| Initial copy uses streaming I/O | `COPY TO STDOUT` → `COPY FROM STDIN` in `buffer_size_kb` chunks |
| WAL feedback is sent only after durable apply | `Feedback.flushed = acked_lsn`, which only moves after `COMMIT` returns or for keepalives between transactions; unit-tested |
| Source replication prerequisites and risks are documented | `docs/operations.md`, `pgsync check` |
| Memory and throughput benchmarks are recorded | `docs/benchmarks.md` |
| Docker end-to-end example works | README quick start, `docker-compose.yml` |
| Limitations are documented | `docs/features.md`, `docs/architecture.md` §7 |
| No real customer data is required for testing | all datasets synthetic (`generate_series`) |

## Remaining risks

1. **Long transactions on the target.** A very large source transaction is applied as one
   target transaction (as PostgreSQL's own subscription worker does). PGSync memory stays
   flat, but locks and bloat on the target follow the transaction size. Mitigation in Beta 2:
   protocol v2 streaming with a disk-backed stage.
2. **Snapshot duration pins xmin on the source** for the whole copy (documented; bounded by
   `snapshot_timeout`). Beta 2: ctid-range parallel copy and per-table snapshot sessions.
3. **DDL** stops the job; an operator must apply it on the target. Acceptable for a
   controlled beta, not for unattended production.
4. **REPLICA IDENTITY FULL** tables match by all columns without index support; slow on big
   tables.
5. **TLS** policy and certificate verification code were not exercised against a real
   TLS-enabled server in this session.
6. **Versions 16/17** are in CI but were not run locally in this session.
7. **Soak duration** is limited to what was run (see benchmarks); a multi-hour run with RSS
   sampling is still required before any production claim.
8. **SCRAM channel binding** is not implemented (declared unsupported to the server); the
   server accepts this unless configured to require channel binding.

## Recommendations for Beta 2

1. Masking stage between decoder and sink on the `Op` model, with a versioned policy and a
   forced full rebuild on policy change; reuse the PGRun classification model.
2. pgoutput protocol v2 (`streaming = on`) with bounded disk spill for in-flight transactions.
3. Strict-mode DDL detection: compare `Relation` messages with cached target columns and
   pause before apply instead of failing at apply.
4. ctid-range parallel copy and per-table resumable snapshots.
5. Coalesced multi-row INSERT / `= ANY($1)` DELETE batches for catch-up throughput.
6. Prometheus `/metrics`; the `Metrics` struct already carries the counters.
7. CI job with a TLS-enabled PostgreSQL (self-signed CA) to exercise `safety.tls_ca_file`
   and `require_tls` end to end.
8. Multi-hour soak and 100 GB dataset on a Linux host with real disk, before TB-scale claims.
