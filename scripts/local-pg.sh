#!/usr/bin/env bash
# Start (or stop) two host-local PostgreSQL clusters equivalent to docker-compose.yml, for
# benchmarks on machines where the Docker VM disk is too small.
#
#   PG_BIN=/opt/homebrew/opt/postgresql@18/bin ./scripts/local-pg.sh start [datadir]
#   ./scripts/local-pg.sh stop [datadir]
#
# Source: port $SRC_PORT (5533), target: port $TGT_PORT (5534). Same roles, demo schema and
# publication as Docker. Extra server settings for the source: $SRC_EXTRA_CONF (one line,
# e.g. "ssl = on"); files to append to pg_hba.conf: $SRC_HBA / $TGT_HBA.
# Several version pairs can run side by side with different datadirs and ports, e.g.
#   PG_BIN=/opt/homebrew/opt/postgresql@16/bin SRC_PORT=5633 TGT_PORT=5634 \
#     ./scripts/local-pg.sh start target/local-pg16
set -euo pipefail
CMD=${1:-start}
BASE=${2:-${PGSYNC_LOCAL_DIR:-$PWD/target/local-pg}}
PG_BIN=${PG_BIN:-$(dirname "$(command -v postgres)")}
SRC_PORT=${SRC_PORT:-5533}; TGT_PORT=${TGT_PORT:-5534}
SRC=$BASE/source; TGT=$BASE/target
HERE=$(cd "$(dirname "$0")/.." && pwd)

start_one() { # dir port extra-conf
  if [ ! -f "$1/PG_VERSION" ]; then
    mkdir -p "$1"
    "$PG_BIN/initdb" -D "$1" -U postgres --auth=trust --no-sync -E UTF8 >/dev/null
    cat >> "$1/postgresql.conf" <<CONF
port = $2
listen_addresses = 'localhost'
unix_socket_directories = ''
fsync = on
shared_buffers = 256MB
log_min_messages = warning
$3
CONF
  fi
  "$PG_BIN/pg_ctl" -D "$1" -l "$1/server.log" -w start >/dev/null
}

case "$CMD" in
  start)
    start_one "$SRC" "$SRC_PORT" "wal_level = logical
max_replication_slots = 32
max_wal_senders = 32
max_slot_wal_keep_size = 4GB
wal_sender_timeout = 30s
${SRC_EXTRA_CONF:-}"
    [ -n "${SRC_HBA:-}" ] && [ ! -f "$SRC/.hba-applied" ] && { cat "$SRC_HBA" >> "$SRC/pg_hba.conf"; touch "$SRC/.hba-applied"; "$PG_BIN/pg_ctl" -D "$SRC" reload >/dev/null; }
    start_one "$TGT" "$TGT_PORT" "${TGT_EXTRA_CONF:-}"
    [ -n "${TGT_HBA:-}" ] && [ ! -f "$TGT/.hba-applied" ] && { cat "$TGT_HBA" >> "$TGT/pg_hba.conf"; touch "$TGT/.hba-applied"; "$PG_BIN/pg_ctl" -D "$TGT" reload >/dev/null; }
    for p in "$SRC_PORT" "$TGT_PORT"; do
      "$PG_BIN/psql" -h localhost -p "$p" -U postgres -Atqc "SELECT 1 FROM pg_database WHERE datname = 'app'" | grep -q 1 \
        || "$PG_BIN/psql" -h localhost -p "$p" -U postgres -qc "CREATE DATABASE app"
    done
    "$PG_BIN/psql" -h localhost -p "$SRC_PORT" -U postgres -d app -Atqc "SELECT 1 FROM pg_roles WHERE rolname = 'pgsync'" | grep -q 1 || {
      for f in "$HERE"/docker/source-init/*.sql; do "$PG_BIN/psql" -h localhost -p "$SRC_PORT" -U postgres -d app -q -f "$f"; done
    }
    echo "source: postgres://pgsync:pgsync@localhost:$SRC_PORT/app  (admin: postgres://postgres@localhost:$SRC_PORT/app)"
    echo "target: postgres://postgres@localhost:$TGT_PORT/app"
    ;;
  stop)
    for d in "$SRC" "$TGT"; do [ -f "$d/postmaster.pid" ] && "$PG_BIN/pg_ctl" -D "$d" -m fast -w stop >/dev/null || true; done
    ;;
  *) echo "usage: $0 start|stop [datadir]"; exit 2;;
esac
