# Reproducing the benchmarks on Linux

Every measurement in `docs/performance.md` was taken on macOS (Apple M1 Pro). Linux
validation is **pending**: the Docker Desktop VM on the development machine had no free disk,
and no other Linux host was available in the Beta 1.1 session. The scripts are portable
(bash, `psql`, `ps`, `python3`); this page is the exact recipe so that the same numbers can be
produced on a Linux box and compared.

## Host requirements

- Linux x86_64 or aarch64, kernel 5.x or newer, 8+ cores and 16 GB RAM to match the
  reference run; a local SSD with 25 GB free for the 10 M-row dataset (60 GB for 10 GB).
- PostgreSQL 18 server and client packages (`postgresql-18`, `postgresql-client-18` from
  apt.postgresql.org, or the distribution's), `python3`, `bc`, `bash` ≥ 4.
- Rust stable (`rustup`), `git`.

```sh
sudo apt-get install -y postgresql-common
sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y
sudo apt-get install -y postgresql-18 postgresql-client-18 python3 bc
export PG_BIN=/usr/lib/postgresql/18/bin
export PATH=$PG_BIN:$PATH
```

## Build

```sh
git clone <repo> pgsync && cd pgsync
cargo build --release
```

## Clusters

Two host-local clusters, identical configuration to the macOS runs (`wal_level=logical`,
`max_slot_wal_keep_size=4GB`, `shared_buffers=256MB`, `fsync=on`):

```sh
PG_BIN=$PG_BIN ./scripts/local-pg.sh start /var/tmp/pgsync-pg18     # :5533 source, :5534 target
export SOURCE_URL=postgres://pgsync:pgsync@localhost:5533/app
export TARGET_URL=postgres://postgres@localhost:5534/app
export ADMIN_URL=postgres://postgres@localhost:5533/app
export BIN=$PWD/target/release/pgsync
```

## Runs (same parameters as the reference)

```sh
# 1. Snapshot + CDC, 10 M rows per table (~2.9 GB COPY), two writers during the copy.
WRITERS_DURING_SNAPSHOT=2 CDC_SECONDS=120 ./scripts/bench.sh 10000000
# 10 GB variant (needs ~35 GB free during the run):
#   WRITERS_DURING_SNAPSHOT=2 CDC_SECONDS=120 ./scripts/bench.sh 35000000

# 2. Catch-up throughput: 5,000 committed transactions applied from a stopped job.
TXNS=5000 ./scripts/catchup-bench.sh

# 3. Concurrency: 1, 5, 10, 20 jobs, idle and active phases, incremental RSS per job.
./scripts/scale-matrix.sh 20000 60

# 4. Soak (4 h; set DURATION_SECS for longer).
DURATION_SECS=14400 ./scripts/soak.sh
```

Each script prints a JSON summary and stores samples under `bench-results/`. Record with
the results: `lscpu | head -20`, `free -g`, `uname -r`, the storage device and filesystem
(`lsblk -o NAME,MODEL,ROTA,SIZE`, `df -T .`), `psql -c 'select version()'`, and the pgsync
commit (`git rev-parse HEAD`). `scripts/bench.sh` already embeds cores, kernel, RAM and free
disk in its JSON.

## Memory on Linux

`ps -o rss` on Linux counts shared library pages in every process, like on macOS. For the
incremental cost per job use `scripts/scale-matrix.sh` (aggregate differences). For a
per-process figure that excludes shared pages, read `Pss` from `/proc/<pid>/smaps_rollup`
while the matrix runs; `pgsync status` reports RSS from `/proc/self/statm`.

## What to compare

| Metric | macOS reference (`docs/performance.md`) | Linux |
|---|---|---|
| Snapshot copy MB/s and peak RSS | — | — |
| Catch-up txn/s, rows/s, CPU | — | — |
| Incremental RSS per job (idle / active) | — | — |
| Soak: RSS first vs last 10 min, lag p99, drain time, consistency | — | — |

Differences to expect: Linux `fsync` on a real SSD versus Apple's, glibc/jemalloc-less
allocation (pgsync uses the system allocator), and no virtualisation. None of the results
on macOS should be used as a Linux capacity number.
