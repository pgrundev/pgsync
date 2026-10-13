# Compatibility — PGSync v0.1.1 (Beta 1.1)

What was actually run, on which PostgreSQL versions and connection types, and what is
therefore claimed. Anything not listed here was not tested and is not claimed.

## PostgreSQL versions

All runs on one machine (Apple M1 Pro, macOS 14.4), host-local Homebrew clusters started by
`scripts/local-pg.sh` with matching source and target majors. The Docker Compose pair is
equivalent but was not used in this session (the Docker VM had no free disk).

| Suite | PostgreSQL 16.11 | PostgreSQL 17.11 | PostgreSQL 18.6 |
|---|---|---|---|
| Unit tests (`cargo test --workspace`, no database) | 76 pass | 76 pass | 76 pass |
| Live protocol test (`live_replication`: SCRAM, IDENTIFY_SYSTEM, slot with exported snapshot, stream, feedback, keepalive) | pass | pass | pass |
| Fast end-to-end suite (24 scenarios, `e2e`) | 24 pass | 24 pass | 24 pass |
| Disruptive suite (5 scenarios: target frozen, target restart, source restart, 15-round random crash loop, 1.2 M-row snapshot) | see test report | see test report | 5 pass |
| Slot invalidation (`max_slot_wal_keep_size`), stopped and streaming | pass | pass | pass |

The fast suite covers: initial snapshot, logical replication of INSERT/UPDATE/DELETE/
TRUNCATE, reconnection (walsender and target backend terminated), crash recovery (SIGKILL),
checkpointing, slot management (missing, advanced by another consumer, invalidated),
WAL feedback (idle slot advances over unpublished WAL), 23 common data types, TOAST,
NULLs, rollback, DDL refusal, bootstrap interruption, oversized messages, inherited
timeouts, published-but-unconfigured tables.

### Version-specific notes

- `pg_replication_slots.invalidation_reason` and `inactive_since` exist from 17; on 16 only
  `wal_status = 'lost'` marks an invalidated slot. pgsync checks both (JSON row, missing
  columns tolerated).
- `SET transaction_timeout = 0` exists from 17; pgsync issues it separately and ignores the
  error on 16.
- `pg_dump` from PostgreSQL 18 was used for every target (`pg_dump` must be at least the
  source's major). Its output applied cleanly to 16, 17 and 18 targets after filtering
  (`\restrict`, `SET transaction_timeout`, tablespace and access-method settings).
- pgoutput protocol version 1 behaves identically on 16–18 for the message set pgsync uses.

### Not tested, not claimed

- Mixed majors (e.g. 16 → 18). Nothing in the protocol prevents it, and the `pg_dump`
  rule above applies, but no mixed pair was run.
- PostgreSQL 14 and 15 as source (preflight warns; untested). PostgreSQL 19 beta.
- Linux (see `docs/performance.md`: a reproducible script exists; no Linux run yet).
- Windows: not a target.

## TLS and authentication

Tested against `scripts/tls-lab.sh`: PostgreSQL 18.6 with `ssl = on`, a private CA, a
server certificate carrying this machine's LAN address as an IP SAN, `hostssl`-only
`pg_hba.conf` entries for that address with `scram-sha-256`, and plain connections rejected.
pgsync treats the LAN address as non-loopback, so these are the production rules.

| Case | SQL connection (tokio-postgres + rustls) | Replication connection (in-house + rustls) |
|---|---|---|
| Trusted CA (`safety.tls_ca_file` or `sslrootcert=`), TLS 1.3, SCRAM-SHA-256 | accepted; `pg_stat_ssl.ssl = true` | accepted; IDENTIFY_SYSTEM, slot, stream, feedback all over TLS |
| Unknown issuer (no CA configured, or a different CA) | rejected (`UnknownIssuer`) | rejected (`UnknownIssuer`) |
| Hostname mismatch (valid chain, certificate for another name) | rejected (`NotValidForName`) | rejected (`NotValidForName`) |
| `sslmode=disable` on a non-loopback host | refused by URL validation while `safety.require_tls` (default) | same |
| `safety.insecure_skip_tls_verify` on a non-loopback host | refused by URL validation | same |
| TLS required, server has `ssl = off` | fails (`server does not support TLS`) | fails (`server does not support TLS but TLS is required`) |
| Walsender/backend terminated under TLS | transient error, reconnects | transient error, reconnects |
| Whole fast e2e suite over TLS (24 scenarios, SCRAM) | pass | pass |
| Password in logs, labels, error messages | never (checked over 254 run logs) | never |

Certificate validation is always on for non-loopback hosts: there is no option that turns
it off for them. `safety.insecure_skip_tls_verify` only applies to `localhost`, `127.0.0.1`,
`::1` and Unix sockets.

### libpq URL parameters

pgsync accepts libpq-style URLs and maps them as follows.

| Parameter | Handling |
|---|---|
| `sslmode=verify-full`, `verify-ca` | treated as `require` with verification (pgsync always verifies chain **and** host name; `verify-ca` is therefore stricter than in libpq) |
| `sslmode=require` | TLS with verification for non-loopback hosts; for loopback, TLS without verification unless a CA is configured |
| `sslmode=prefer` / `allow` | loopback only (non-loopback hosts always require TLS); falls back to plain if the server declines |
| `sslmode=disable` | loopback only |
| `sslrootcert=<file>` | trusted in addition to the system store and `safety.tls_ca_file` |
| `sslrootcert=system` | default behaviour, accepted |
| `sslcert`, `sslkey`, `sslpassword` | refused: client-certificate authentication is not implemented |
| `sslcrl`, `sslcrldir` | refused: revocation lists are not checked, so the parameter would be silently ignored |
| `passfile`, `service`, `requirepeer` | refused |
| `sslcompression`, `sslsni`, `gssencmode`, `krbsrvname`, `ssl_min/max_protocol_version` | ignored |
| `application_name`, `connect_timeout`, `options`, `keepalives*`, `target_session_attrs`, `channel_binding` | passed to the SQL client |
| anything else | error naming the parameter |

`pg_dump` (schema bootstrap) receives `PGSSLMODE=verify-full` and `PGSSLROOTCERT` set to the
configured CA file, or `system` when none is configured. `sslrootcert=system` needs libpq 16
or newer; with an older `pg_dump` and a publicly trusted server certificate, set
`safety.tls_ca_file` to the CA bundle explicitly.

### Authentication methods

| Method | SQL connection | Replication connection |
|---|---|---|
| SCRAM-SHA-256 | tested (TLS lab, Docker images) | tested (TLS lab, Docker images) |
| SCRAM-SHA-256-PLUS (channel binding) | used automatically by tokio-postgres-rustls when offered | **not implemented**: the client advertises no channel binding (`n,,`); servers accept this unless a client-side `channel_binding=require` policy is desired, which pgsync cannot honour on the replication connection |
| MD5 | code path present, unit-level only; not exercised against a server in this session | same |
| trust | tested (host-local clusters) | tested |
| password (clear text) | code path present; only sensible over TLS; not exercised | same |
| GSSAPI / Kerberos, client certificates, LDAP/RADIUS (server-side password methods behave like password) | not supported | not supported |

## Operating system and toolchain

- Built and run on macOS 14.4 (arm64) with Rust 1.92 stable; CI (`.github/workflows/ci.yml`)
  builds on `ubuntu-latest` and runs the fast suite on `postgres:16/17/18` images.
- `~/.local/bin/cc` on the development machine shadows the system compiler; the worktree
  carries a git-excluded `.cargo/config.toml` pointing the linker at `/usr/bin/cc`. Nothing in
  the repository depends on it.
