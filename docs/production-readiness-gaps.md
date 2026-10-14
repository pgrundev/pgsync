# Production readiness gaps — PGSync v0.1.1 (Beta 1.1)

What still stands between this engine and unattended operation on customer production
data, ordered by the damage the gap can do. "Beta 2" items are the planned next phase;
"later" items have no committed phase. Nothing here is a known bug: every known bug found
in Beta 1.1 was fixed (see `docs/test-report.md`).

## Blocking for any customer data

| Gap | Consequence | Plan |
|---|---|---|
| No data masking | Raw customer values reach the target. | Beta 2 (`docs/design/beta2-masking.md`), fail-closed policy. |
| No DDL handling beyond stop-and-tell | A schema change on the source stops the job until an operator applies it on the target; a dropped column or a widened type is tolerated silently (documented, tested), which a masking policy must treat as a new column. | Beta 2 strict mode: pause before apply on any `Relation` difference. |
| Single process, no supervisor | A crashed process stays down until something restarts it; WAL accumulates meanwhile. The checkpoint makes restarts safe, but nothing performs them. | PGRun control plane (Beta 3) or a systemd unit with `Restart=always`; `pgsync.job_runtime.stale` is the signal. |

## Operational risks that remain with the current design

| Risk | Mitigation in place | Remaining exposure |
|---|---|---|
| **WAL retention by the slot** while the consumer is stopped, paused or slow | `max_slot_wal_keep_size` preflight warning; retained-WAL alarm and `wal_retention_action: stop`; slot dropped automatically only for a bootstrap whose snapshot failed; invalidation detected and reported as `RESYNC_REQUIRED` | A long outage of the consumer still fills the source disk when `max_slot_wal_keep_size = -1`. Operators must set it. Nothing drops a production slot automatically, by design. |
| **Large source transactions** are applied as one target transaction | pgsync memory stays flat (measured); the target holds locks and bloat for the transaction's duration | A multi-GB transaction holds a target transaction open for minutes; `max_message_bytes` bounds one row, not one transaction. Beta 2: protocol v2 streaming with bounded disk spill. |
| **Snapshot pins `xmin`** on the source for the whole copy | `snapshot_timeout`; failed copies release everything | A 10 GB copy at ~37 MB/s pins `xmin` for ~5 minutes; a 1 TB copy for hours — autovacuum cannot clean rows newer than the snapshot meanwhile. Beta 2: ctid-range parallel copy with per-table snapshots. |
| **Single-threaded applier**, ~70 k row operations/s | Enough for the write rates measured; backpressure is correct under overload | A source writing faster than the applier for long enough accumulates slot lag until `max_slot_wal_keep_size` invalidates the slot. Multi-row coalescing planned. |
| **`REPLICA IDENTITY FULL`** tables | Supported with a full-row match | Every UPDATE/DELETE scans without an index; duplicate rows make the match ambiguous and stop the job. Document per table; prefer a key. |
| **Target timeouts, triggers, FKs** | Sessions reset timeouts; superuser sessions use `session_replication_role = replica` | Without superuser on the target, triggers fire and FK order matters during the copy (parallel copies). Preflight warns. |
| **Type drift between source and target** (`schema.mode: existing`) | Preflight warns on declared-type differences; values that do not fit stop the job instead of being truncated or rounded (fixed in Beta 1.1) | A *lossy but valid* conversion (numeric scale reduction, timestamp precision) is applied without error, like `COPY` would. Keep target types identical. |
| **Publication changes** | Added tables are refused (`RESYNC_REQUIRED`); `FOR ALL TABLES` publications are warned about | Removing a configured table from the publication stops changes silently from pgsync's point of view (they are not produced). Operators must keep the two sets equal; `check` verifies it. |

## Security and secrets

| Gap | Status |
|---|---|
| TLS chain and host-name verification for non-loopback hosts | Done, tested on real TLS (`docs/compatibility.md`). |
| SCRAM channel binding on the replication connection | Not implemented; the server accepts a client without it. Add when `postgres-protocol` exposes the TLS exporter for the in-house connection. |
| Client-certificate authentication | Not implemented; refused with a clear message. |
| Passwords | Only from the environment; never in logs, labels, errors or `ps` (`pg_dump` receives it via `PGPASSWORD` in its own environment). |
| Row values in logs | Never at any log level (`tokio_postgres` pinned to WARN; `--show-keys` is explicit opt-in for key samples on a terminal). |
| Secrets at rest | The checkpoint and heartbeat rows in the target hold positions and identifiers only. |

## Validation gaps (what was not measured)

| Item | Status after Beta 1.1 |
|---|---|
| Linux | Not run. Reproducible recipe in `docs/linux-benchmark.md`. All numbers are macOS/M1 Pro. |
| 10 GB snapshot | Not run: the development disk had 17 GB free. Largest run: 10 M rows per table (see `docs/performance.md`). |
| 100 GB, 1 TB | Not run. |
| Mixed PostgreSQL majors | Not run, not claimed. |
| Network partition (packet loss, half-open connections) | Only peer freezes (SIGSTOP), restarts and terminated backends were tested. `tcp_user_timeout` / keepalive settings in the URL are passed through for the SQL client; the replication connection relies on `wal_sender_timeout` on the server and on feedback write timeouts. |
| Multi-hour soak | 68 minutes with 7 interruptions, consistent; the 4-hour target was cut short by the development disk (`docs/performance.md`). |
| Catch-up throughput on an uncontended machine | 454–500 txn/s measured at load average 20–27 versus 625 in Beta 1 on an idle machine; rerun pending. |
| MD5 and clear-text password authentication | Code paths exist; not exercised against a server. |

## Ready-for-Beta-2 criteria met

- Every `Relation` reaches the sink through one code path (`PgSink::relation`) where a
  policy check can be inserted; the configured-table check added in Beta 1.1 is that hook.
- Every value reaches the target through `Op` tuples (CDC) or `COPY` chunks (snapshot);
  the design doc shows the snapshot path switching to the same tuple representation for
  policied tables without touching the engine.
- Checkpoint atomicity, acknowledgement after commit, bounded channel, and the message size
  limit are independent of what the values contain.
