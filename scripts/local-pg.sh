#!/usr/bin/env bash
# Start (or stop) two host-local PostgreSQL clusters equivalent to docker-compose.yml, for
# benchmarks on machines where the Docker VM disk is too small.
#
#   PG_BIN=/opt/homebrew/opt/postgresql@18/bin ./scripts/local-pg.sh start [datadir]
#   ./scripts/local-pg.sh stop [datadir]
#
# Source: port 5533, target: port 5534. Same roles, demo schema and publication as Docker.
set -euo pipefail
CMD=${1:-start}
BASE=${2:-${PGSYNC_LOCAL_DIR:-$PWD/target/local-pg}}
PG_BIN=${PG_BIN:-$(dirname "$(command -v postgres)")}
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
    start_one "$SRC" 5533 "wal_level = logical
max_replication_slots = 32
max_wal_senders = 32
max_slot_wal_keep_size = 4GB
wal_sender_timeout = 30s"
    start_one "$TGT" 5534 ""
    for p in 5533 5534; do
      psql -h localhost -p $p -U postgres -Atqc "SELECT 1 FROM pg_database WHERE datname = 'app'" | grep -q 1 \
        || psql -h localhost -p $p -U postgres -qc "CREATE DATABASE app"
    done
    psql -h localhost -p 5533 -U postgres -d app -Atqc "SELECT 1 FROM pg_roles WHERE rolname = 'pgsync'" | grep -q 1 || {
      for f in "$HERE"/docker/source-init/*.sql; do psql -h localhost -p 5533 -U postgres -d app -q -f "$f"; done
    }
    echo "source: postgres://pgsync:pgsync@localhost:5533/app  (admin: postgres://postgres@localhost:5533/app)"
    echo "target: postgres://postgres@localhost:5534/app"
    ;;
  stop)
    for d in "$SRC" "$TGT"; do [ -f "$d/postmaster.pid" ] && "$PG_BIN/pg_ctl" -D "$d" -m fast -w stop >/dev/null || true; done
    ;;
  *) echo "usage: $0 start|stop [datadir]"; exit 2;;
esac
