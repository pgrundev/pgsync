# Performance — PGSync v0.1.1 (Beta 1.1)

Every number below was measured; raw samples, logs and JSON summaries are in
`bench-results/` (file names carry the UTC stamp). Beta 1 numbers come from
`docs/benchmarks.md` (commit `cfadb53`); Beta 1.1 numbers from the commits named in each
section. Hardware and conditions differ between runs and are stated for each; the
"Comparability" column says what a difference means.

## Environment

| Item | Value |
|---|---|
| Machine | Apple M1 Pro, 10 cores (8 performance + 2 efficiency), 16 GiB RAM, macOS 14.4 (Darwin 23.4.0), Apple SSD (APFS, encrypted) |
| PostgreSQL | 18.6 (Homebrew) for the PG18 pair, 17.11 for the PG17 pair; host-local clusters from `scripts/local-pg.sh` |
| Source config | `wal_level=logical`, `max_slot_wal_keep_size=4GB`, `wal_sender_timeout=30s`, `shared_buffers=256MB`, `fsync=on`, defaults otherwise |
| Target config | `shared_buffers=256MB`, `fsync=on`, defaults otherwise |
| pgsync | release build (`lto = thin`), default resources (`max_inflight_bytes` 16 MiB, `max_batch_rows` 500, `max_batch_bytes` 4 MiB, `buffer_size_kb` 256, `max_parallel_copies` 2, `max_message_bytes` derived = 32 MiB) |
| Sampling | `ps -o rss,%cpu` of the `pgsync` process every second (benchmarks) or every 10 s (soak) |
| Linux | **not run** (`docs/linux-benchmark.md` has the recipe); none of these numbers is a Linux figure |

Source, target and pgsync share the machine; every benchmark competed for CPU and disk with
the two PostgreSQL servers and the writers, and some ran while a test suite was executing
on another cluster pair (noted). Throughput figures are therefore conservative; memory
figures are not affected by contention.

## Summary table (Beta 1 baseline → Beta 1.1)

| Metric | Beta 1 baseline | Beta 1.1 result | Comparability |
|---|---|---|---|
| Snapshot MB/s | 36.5 (1 GB, 2 × 4 M rows, no concurrent writes) | **32.5** (2.9 GB COPY, 2 × 10 M rows, **two writers running during the copy**, PG17 e2e suite running on another pair) ; 38.1 at 100 MB (PG17 pair, see below) | same machine; the 2.9 GB run paid for concurrent writers and a parallel test suite |
| Peak snapshot RSS | 33 MB (1 GB) | **40.1 MB** (2.9 GB; the phase includes the 17 s catch-up with a full 16 MiB channel) · 21.0 MB average | same machine, same limits; growth is the channel filling during catch-up, not data size |
| CDC transactions/sec (catch-up, no writer bottleneck) | 625 txn/s (100 MB dataset, 118 MB WAL backlog) | **131.6 txn/s on a 4 GB dataset with 2.2 GB of WAL backlog (59 MB/s of WAL, 12.5 k rows/s, 25.6 % CPU: target-I/O bound, not pgsync-bound)** · 100 MB dataset rerun: see "Catch-up on the baseline dataset" | not comparable across dataset sizes; the baseline-sized rerun is the like-for-like number |
| CDC row operations/sec | ≈72 k (same run) | 12.5 k on the 4 GB dataset (I/O bound) · baseline-sized rerun below | same caveat |
| 20-job memory | 9.5 MB/job "per process RSS" (shared pages counted 20 times) | **incremental 9.7–11.4 MB per additional job** (aggregate differences, idle and active), aggregate 204 MB active / 220 MB idle for 20 jobs, 291/348 MB peak | Beta 1.1 measures what one more job actually costs; the Beta 1 figure divided a shared-page-inflated RSS |
| Longest soak | 35 min (40-min run) | see "Soak" | — |
| Largest snapshot | 1 GB (1.17 GB COPY) | **2.9 GB COPY (3.9 GB on disk, 2 × 10 M rows)** | disk-limited (17 GB free on the development machine); 10 GB not run |

## Snapshot, 2 × 10 M rows (commit `7761425`, PG 18 pair, `bench-20261013T223335Z`)

`WRITERS_DURING_SNAPSHOT=2 CDC_SECONDS=120 ./scripts/bench.sh 10000000`

| Metric | Value |
|---|---|
| Source dataset | 2 tables × 10,000,000 rows, 3.94 GB on disk, 2,926,394,924 COPY bytes |
| Copy (slot created → all tables copied) | 90.0 s → **32.5 MB/s** |
| Catch-up (copy done → `STREAMING`) | 17.0 s |
| Writers during the copy | 2 (`UPDATE` 20 rows + `INSERT` 30 rows per transaction, as fast as `psql` can spawn) |
| WAL generated on the source during copy + catch-up | 609 MB; retained-WAL peak observed by the slot 596 MB |
| RSS during copy + catch-up | average 21.0 MB, **peak 40.1 MB** (105 samples) |
| CPU during copy + catch-up | 7.3 % average of one core (the servers do the work) |
| CDC phase (120 s, 4 writers) | 9,589 transactions, 507,079 row operations applied; RSS average 14.6 MB, peak 21.6 MB; CPU 10.2 % |
| Drain after writers stopped | converged within the 2 s polling interval |
| Content comparison | **not performed in this run**: the whole-table `string_agg` hash exceeded PostgreSQL's 1 GB string buffer and aborted the script before the JSON summary (fixed: chunked hash, `1b0ed12`+). A repeat on the PG17 pair with the fixed scripts is recorded below. |
| Disk | 16.9 GiB free before, 8.9 GiB after (both clusters) |

The 1 GB baseline copied at 36.5 MB/s with an idle source; this run copied at 32.5 MB/s
with two writers hammering the same tables and a 25-scenario test suite running against
another cluster pair on the same machine. Memory did not follow data size: 33 MB peak at
1 GB, 40 MB at 2.9 GB, and the extra 7 MB is the reader→applier channel (16 MiB budget)
filling during the catch-up, which the 1 GB run did not have.

## Catch-up throughput on the 4 GB dataset (`catchup-20261013T224044Z`)

With the 10 M-row job stopped, 5,000 committed transactions (100 inserts + 10 random updates
+ 5 deletes each) were generated, then `pgsync run` was timed until the WAL head was
acknowledged.

| Metric | Value |
|---|---|
| Backlog | 5,000 transactions, 2,247,875,256 WAL bytes (random updates on a 4 GB table produce full-page images) |
| Catch-up time | 38.0 s |
| Transactions/s | 131.6 |
| Row operations/s | 12,526 |
| WAL consumed | 59.2 MB/s |
| RSS | average 16.7 MB, peak 29.3 MB |
| CPU (pgsync) | 25.6 % of one core |
| Contents after catch-up | identical (`ledger`, chunked hash) |

pgsync was not the bottleneck here (25 % CPU, 59 MB/s of WAL decoded and acknowledged);
the target spent its time on random index and heap pages of a 4 GB table with 256 MB of
shared buffers. The Beta 1 figure of 625 txn/s was measured on a 100 MB dataset that fits in
memory on both sides.

## Baseline dataset on the PG 17 pair (2 × 400 k rows, `bench-20261013T225030Z`, `catchup-20261013T225202Z`)

Run **while the 4-hour soak (four writers, pgsync, two more PostgreSQL servers) was
executing on the same machine and SSD**; pgsync's CPU share shows it was waiting on the
database, not working.

| Metric | Beta 1 (idle machine) | Beta 1.1 (under the soak's load) |
|---|---|---|
| Copy, 114 MB COPY bytes | 5.0 s, 22.8 MB/s | 10.0 s, 11.4 MB/s (+2.0 s catch-up) |
| Copy RSS avg / peak | 20.0 / 28.7 MB | 17.4 / 26.6 MB |
| CDC, 60 s, 4 writers | 7,138 txn, 375,625 rows | 4,189 txn, 224,942 rows; RSS 14.4 / 21.8 MB |
| Catch-up of 5,000 transactions (115 MB WAL) | 8.0 s, 625 txn/s, ≈72 k rows/s, 98 % CPU | 66.0 s, 75.8 txn/s, 8,243 rows/s, **11.1 % CPU**, RSS 15.1 / 19.0 MB |
| Contents | identical | identical |

The catch-up figure is not an engine regression measurement: at 11 % CPU the applier was
blocked on the target's commits competing with the soak's writers for the same disk
(`fsync=on`). Nothing in the Beta 1.1 changes touches the per-statement apply path (the
cast type string changed, the heartbeat writes one row every 10 s). An idle-machine rerun
on the PG 18 pair is scheduled after the soak and recorded below when available.

### Catch-up rerun on an idle machine

Pending.

## Concurrency: 1, 5, 10, 20 jobs (`scale-matrix-20261013T224251Z`)

`./scripts/scale-matrix.sh 20000 60`: one process per job, each with its own schema, slot
and publication (20,000 rows per job), 30 s idle (streaming, no writes) then 60 s with a
writer touching every job. All jobs consistent at the end of every run.

| Jobs | Idle aggregate RSS avg / peak | Active aggregate RSS avg / peak | Idle CPU (sum) | Active CPU (sum) | Per-process RSS avg (shared pages counted N times) |
|---|---|---|---|---|---|
| 1 | 8.9 / 14.2 MB | 9.4 / 10.8 MB | 0.5 % | 1.4 % | 8.9 MB |
| 5 | 61.0 / 90.5 MB | 53.2 / 73.0 MB | 2.8 % | 4.7 % | 12.2 MB |
| 10 | 106.0 / 172.5 MB | 107.4 / 148.2 MB | 8.9 % | 7.4 % | 10.6 MB |
| 20 | 220.2 / 348.2 MB | 203.9 / 290.6 MB | 26.6 % | 13.0 % | 11.0 MB |

Incremental cost of one more job (aggregate differences):

| From → to | Idle | Active | Active peak |
|---|---|---|---|
| 1 → 5 | 13.0 MB | 11.0 MB | 15.6 MB |
| 5 → 10 | 9.0 MB | 10.8 MB | 15.0 MB |
| 10 → 20 | 11.4 MB | 9.7 MB | 14.2 MB |

**Capacity reading:** about **10–11 MB of RSS per additional job** at this workload, 15 MB
at peak, linear from 1 to 20 jobs; aggregate memory is a straight line, not a curve. The
idle peaks (e.g. 348 MB for 20 jobs) are the tail of the bootstrap phase caught by the first
idle samples, not steady state. CPU for 20 streaming jobs: 13 % of one core under the
writer, 27 % when idle — the idle figure is dominated by 20 heartbeat and monitor
connections on a 10 s / 30 s cadence and by `ps`-based RSS sampling inside each process; it
is a fixed cost that would be amortised by running several jobs per process (not a Beta 1.1
goal). Twenty slots and walsenders on one source stayed healthy.

The Beta 1 claim of "9.5 MB per process" was a per-process RSS that counts the shared
binary and library pages once per process; the incremental figure above is what a node
actually pays. The two happen to be close on macOS because the shared part of a pgsync
process is small (~3 MB).

## Soak (`soak-20261013T230339Z`, PG 18 pair, commit `04b614f`)

`DURATION_SECS=14400 ROWS=400000 ./scripts/soak.sh` — planned for 4 hours, **ran 68
minutes (4,100 s)**: the script's free-disk guard stopped it cleanly when free space on the
development machine fell to 1.1 GB (another session on the same machine was writing
several GB to its own scratch space during the run; pgsync's own footprint on disk was the
0.3 GB dataset plus its growth). A longer run was not feasible on this machine in this
session; the 68 minutes are reported as such, not as a 4-hour pass.

Workload: 2 × 400 k rows; four writers in 20-minute cycles of 12 min steady (20 updates +
30 inserts + 5 deletes per transaction, as fast as `psql` spawns), 2 min bulk-update bursts,
2 min idle, 2 min with one 200 k-row transaction; sampled every 10 s.

Interruptions, all from the fixed schedule, all recovered without operator action:

| Minute | Event | Outcome |
|---|---|---|
| 7, 23, 41, 67 | SIGKILL pgsync, `run` 2 s later | resumed from the checkpoint |
| 15, 53 | SIGTERM pgsync, `run` 2 s later | finished the transaction in flight, exit 0, resumed |
| 31 | `pg_terminate_backend` on the walsender | one reconnect, resumed |
| — | unexpected exits | 0 |

| Metric | Value |
|---|---|
| Transactions applied | 113,726 (source hashes: `items` 400,000 rows, `ledger` 3,905,025 rows) |
| Contents at the end | **identical** on both tables (chunked MD5 over every row and column) |
| Drain after writers stopped | 0 s (already converged) |
| RSS average / peak | 13.5 MB / 59.0 MB |
| RSS per quarter (avg / peak) | 14.4 / 59.0 · 14.0 / 42.4 · 11.5 / 31.7 · 14.3 / 51.9 MB |
| RSS first 10 min vs last 10 min | 11.9 MB vs 11.1 MB — **no growth** |
| CPU | 4.3 % of one core average |
| Apply lag | p50 0.02 MB, p99 4.1 MB, max 16.0 MB; never above 64 MB |
| Retained WAL (slot) max | 105.7 MB (during the 200 k-row transactions and right after kills) |
| Connections | 2 on the source (walsender + monitor), 2 on the target (apply + heartbeat), constant; no leak across 7 restarts |

The peaks (up to 59 MB) coincide with the 200 k-row transactions: the reader→applier
channel fills to its 16 MiB budget and the applier holds a 4 MiB batch, on top of the
~12 MB baseline and allocator slack; between them RSS returns to 11–14 MB. The 40-minute
Beta 1 soak showed 14.6–14.9 MB; this run is consistent with it under a harder workload
with interruptions.

## Not measured

- 10 GB or larger snapshots (disk); 100 GB, 1 TB.
- Linux.
- Separate disk and network throughput (the bench reports COPY bytes and WAL bytes only).
- A comparison with other tools.
