#!/usr/bin/env bash
# TLS test lab: a private CA, a server certificate for a NON-loopback address of this
# machine, and a host-local PostgreSQL pair with ssl=on, hostssl-only pg_hba entries and
# SCRAM authentication. pgsync treats the LAN address as remote, so TLS with verification
# is mandatory on these URLs, exactly as it would be against a real server.
#
#   PG_BIN=/opt/homebrew/opt/postgresql@18/bin ./scripts/tls-lab.sh start [datadir]
#   ./scripts/tls-lab.sh stop [datadir]
#   ./scripts/tls-lab.sh env       # prints the URLs and CA path for the e2e suite
#
# Nothing here touches system trust stores or /etc/hosts.
set -euo pipefail
CMD=${1:-start}
BASE=${2:-${PGSYNC_TLS_DIR:-$PWD/target/tls-lab}}
PG_BIN=${PG_BIN:-$(dirname "$(command -v postgres)")}
SRC_PORT=${SRC_PORT:-5833}; TGT_PORT=${TGT_PORT:-5834}
# First non-loopback IPv4 of this host (the certificate carries it as an IP SAN).
LAN_IP=${LAN_IP:-$(ifconfig 2>/dev/null | awk '/inet / && $2 != "127.0.0.1" {print $2; exit}')}
HERE=$(cd "$(dirname "$0")/.." && pwd)
CERTS=$BASE/certs

make_certs() {
  mkdir -p "$CERTS"
  [ -f "$CERTS/ca.crt" ] && return
  openssl req -x509 -newkey rsa:2048 -nodes -days 30 -sha256 \
    -keyout "$CERTS/ca.key" -out "$CERTS/ca.crt" -subj "/CN=pgsync test CA" >/dev/null 2>&1
  cat > "$CERTS/san.cnf" <<CNF
[req]
distinguished_name = dn
req_extensions = ext
prompt = no
[dn]
CN = pgsync-tls-lab
[ext]
subjectAltName = IP:$LAN_IP, DNS:pgsync-tls-lab.invalid
CNF
  openssl req -new -newkey rsa:2048 -nodes -keyout "$CERTS/server.key" -out "$CERTS/server.csr" \
    -config "$CERTS/san.cnf" >/dev/null 2>&1
  openssl x509 -req -in "$CERTS/server.csr" -CA "$CERTS/ca.crt" -CAkey "$CERTS/ca.key" \
    -CAcreateserial -out "$CERTS/server.crt" -days 30 -sha256 \
    -extfile "$CERTS/san.cnf" -extensions ext >/dev/null 2>&1
  # A second certificate from the same CA for a *different* name: hostname mismatch case.
  sed "s/IP:$LAN_IP, DNS:pgsync-tls-lab.invalid/DNS:other-host.invalid/" "$CERTS/san.cnf" > "$CERTS/san-other.cnf"
  openssl req -new -newkey rsa:2048 -nodes -keyout "$CERTS/other.key" -out "$CERTS/other.csr" \
    -config "$CERTS/san-other.cnf" >/dev/null 2>&1
  openssl x509 -req -in "$CERTS/other.csr" -CA "$CERTS/ca.crt" -CAkey "$CERTS/ca.key" \
    -CAcreateserial -out "$CERTS/other.crt" -days 30 -sha256 \
    -extfile "$CERTS/san-other.cnf" -extensions ext >/dev/null 2>&1
  # An unrelated CA, to test "untrusted issuer".
  openssl req -x509 -newkey rsa:2048 -nodes -days 30 -sha256 \
    -keyout "$CERTS/otherca.key" -out "$CERTS/otherca.crt" -subj "/CN=some other CA" >/dev/null 2>&1
  chmod 600 "$CERTS"/*.key
}

install_server_cert() { # datadir [cert key]
  cp "${2:-$CERTS/server.crt}" "$1/server.crt"; cp "${3:-$CERTS/server.key}" "$1/server.key"; chmod 600 "$1/server.key"
}

case "$CMD" in
  start)
    [ -n "$LAN_IP" ] || { echo "no non-loopback IPv4 address found; set LAN_IP" >&2; exit 2; }
    make_certs
    # hostssl only for the LAN address; loopback stays trust so the lab can be administered.
    HBA=$BASE/hba.conf
    cat > "$HBA" <<HBA
hostssl all        all   $LAN_IP/32  scram-sha-256
hostssl replication all  $LAN_IP/32  scram-sha-256
host    all        all   $LAN_IP/32  reject
HBA
    first=0; [ -f "$BASE/source/PG_VERSION" ] || first=1
    # Plain start first (initdb, roles, demo schema), then switch both servers to TLS.
    PG_BIN=$PG_BIN SRC_PORT=$SRC_PORT TGT_PORT=$TGT_PORT \
      SRC_EXTRA_CONF="password_encryption = scram-sha-256" TGT_EXTRA_CONF="password_encryption = scram-sha-256" \
      "$HERE/scripts/local-pg.sh" start "$BASE" >/dev/null
    for d in "$BASE/source" "$BASE/target"; do
      install_server_cert "$d"
      if ! grep -q '^ssl = on' "$d/postgresql.conf"; then
        cat >> "$d/postgresql.conf" <<CONF
listen_addresses = '*'
ssl = on
ssl_cert_file = 'server.crt'
ssl_key_file = 'server.key'
CONF
        cat "$HBA" >> "$d/pg_hba.conf"
      fi
      "$PG_BIN/pg_ctl" -D "$d" -m fast -w restart -l "$d/server.log" >/dev/null
    done
    if [ "$first" = 1 ]; then
      "$PG_BIN/psql" -h localhost -p "$SRC_PORT" -U postgres -qc "ALTER ROLE pgsync PASSWORD 'pgsync'; ALTER ROLE postgres PASSWORD 'postgres'"
      "$PG_BIN/psql" -h localhost -p "$TGT_PORT" -U postgres -qc "ALTER ROLE postgres PASSWORD 'postgres'"
    fi
    "$0" env "$BASE"
    ;;
  env)
    echo "export PGSYNC_TLS_CA=$CERTS/ca.crt"
    echo "export PGSYNC_TLS_LAN_IP=$LAN_IP"
    echo "export PGSYNC_E2E_SOURCE_ADMIN_URL='postgres://postgres:postgres@$LAN_IP:$SRC_PORT/app?sslmode=verify-full&sslrootcert=$CERTS/ca.crt'"
    echo "export PGSYNC_E2E_SOURCE_URL='postgres://pgsync:pgsync@$LAN_IP:$SRC_PORT/app?sslmode=require'"
    echo "export PGSYNC_E2E_TARGET_URL='postgres://postgres:postgres@$LAN_IP:$TGT_PORT/app?sslmode=require'"
    ;;
  stop)
    "$HERE/scripts/local-pg.sh" stop "$BASE"
    ;;
  *) echo "usage: $0 start|stop|env [datadir]"; exit 2;;
esac
