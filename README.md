# PGSync

PGSync is a standalone Rust service that replicates selected tables from a source PostgreSQL
(16, 17, 18) to a target PostgreSQL using native logical replication (`pgoutput`). It takes a
transactionally consistent initial snapshot, then applies every committed source transaction
atomically on the target, storing its checkpoint in the same transaction as the data.

**Status: v0.1.0, controlled technical beta.** Synthetic or explicitly approved non-sensitive
data only. No data masking yet, so it must not be pointed at production customer data.

- Architecture and design decisions: [`docs/architecture.md`](docs/architecture.md)
- Supported features and limitations: [`docs/features.md`](docs/features.md)
- Operations (slots, WAL retention, resync, failure modes): [`docs/operations.md`](docs/operations.md)
- Test report and benchmarks: [`docs/test-report.md`](docs/test-report.md), [`docs/benchmarks.md`](docs/benchmarks.md)
- Beta assessment: [`docs/beta-assessment.md`](docs/beta-assessment.md)

## Guarantees

- Source transactions are applied in commit order and atomically; the target never shows a
  partially applied transaction.
- WAL is acknowledged to the source only after the corresponding target commit is durable.
- After a crash, reconnect or restart, no transaction is skipped and none is applied twice.
- Memory is bounded by configured limits (channel bytes, batch bytes, COPY buffers), not by
  transaction or database size.
- Anything PGSync cannot apply safely stops the job visibly (`ERROR` / `RESYNC_REQUIRED`).
  There is no silent skipping and no automatic "start from the current LSN".
- The source is never modified except for the replication slot that `bootstrap` creates.

## Quick start (reproducible first run)

Prerequisites: Rust stable, Docker with Compose, and `pg_dump` 16+ on `PATH`
(`brew install libpq` or `apt install postgresql-client`).

```bash
# 1. Start two PostgreSQL 18 instances (source on 5433 with wal_level=logical, target on 5434).
docker compose up -d --wait

# 2. The publication already exists; the init script creates it as an explicit admin step:
docker/source-init/03-publication.sql   # CREATE PUBLICATION pgsync_demo FOR TABLE public.users, public.orders;

# 3. Configure PGSync. Connection URLs come from the environment, never from the file.
export PGSYNC_SOURCE_URL='postgres://pgsync:pgsync@localhost:5433/app'
export PGSYNC_TARGET_URL='postgres://postgres:postgres@localhost:5434/app'
cargo build --release
./target/release/pgsync --config examples/pgsync.yaml check

# 4. Bootstrap: create the slot, export a snapshot, copy both tables, switch to streaming.
./target/release/pgsync --config examples/pgsync.yaml bootstrap
#    (keeps running; open a second terminal for the next steps)

# 5. Insert a row on the source.
psql "$PGSYNC_SOURCE_URL" -c "INSERT INTO users (email, full_name) VALUES ('demo@example.test', 'Demo')"
#    The pgsync role is read-only; use the admin URL to write:
psql postgres://postgres:postgres@localhost:5433/app -c "INSERT INTO users (email, full_name) VALUES ('demo@example.test', 'Demo')"

# 6. Verify the row on the target.
psql "$PGSYNC_TARGET_URL" -c "SELECT id, email, full_name FROM users WHERE email = 'demo@example.test'"

# 7. Restart PGSync: Ctrl-C the bootstrap process (it finishes the current transaction),
#    write more rows while it is down, then resume.
psql postgres://postgres:postgres@localhost:5433/app -c "UPDATE users SET full_name = 'Demo 2' WHERE email = 'demo@example.test'"
./target/release/pgsync --config examples/pgsync.yaml run

# 8. Verify the update arrived and the checkpoint advanced.
psql "$PGSYNC_TARGET_URL" -c "SELECT full_name FROM users WHERE email = 'demo@example.test'"
./target/release/pgsync --config examples/pgsync.yaml status
```

`make check bootstrap run status cleanup` wrap the same commands.

## Commands

| Command | Effect |
|---|---|
| `pgsync check` | Validates source and target prerequisites (22 checks). Changes nothing. |
| `pgsync bootstrap [--restart-snapshot]` | Creates the slot with an exported snapshot, bootstraps the schema (`pg_dump --schema-only`), streams every table with `COPY`, records the checkpoint, then streams changes. `--restart-snapshot` discards an incomplete copy. |
| `pgsync run` | Resumes from the checkpoint stored in the target. Refuses to run if continuity cannot be proven. |
| `pgsync status [--json]` | Job state, positions, slot state, lag and retained WAL. |
| `pgsync cleanup --yes` | Drops the slot PGSync owns and deletes job metadata. Keeps target data. |

Exit codes: `0` ok, `1` runtime error, `2` configuration, `3` preflight, `4` resync required,
`5` unsupported change or consistency violation.

Job states: `INITIALIZING`, `SNAPSHOTTING`, `CATCHING_UP`, `STREAMING`, `PAUSED`, `ERROR`,
`RESYNC_REQUIRED`.

## Configuration

See [`examples/pgsync.yaml`](examples/pgsync.yaml). Every value is validated with a
descriptive error. Highlights:

- `source.publication` and `source.slot` must be provisioned explicitly (the slot by
  `bootstrap`, the publication by an administrator).
- `resources.*` bound memory: `max_inflight_bytes` (reader → applier), `max_batch_bytes`,
  `buffer_size_kb` (COPY chunks), `max_parallel_copies`. `memory_budget_mb` must cover them.
- `safety.require_tls` enforces certificate-verified TLS for non-loopback hosts.
  `safety.insecure_skip_tls_verify` is honoured for loopback hosts only.
- `safety.wal_retention_alarm_bytes` + `wal_retention_action: warn|stop`.

## Development

```bash
make lint          # fmt + clippy -D warnings
make test          # unit tests (no database needed)
make e2e           # live protocol test + 17 end-to-end scenarios (Docker, ~6 min)
make e2e-slow      # container pause/restart, random crash loop, large table (~10 min)
make bench         # resource/throughput benchmarks (see docs/benchmarks.md)
```

Logs go to stderr; `--json` switches logs and command output to JSON. Row values and
passwords are never logged.

## License

Apache-2.0. No third-party code is vendored; see `docs/research/` for what was studied.
