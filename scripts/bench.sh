#!/usr/bin/env bash
# Resource and throughput benchmark for PGSync against docker compose.
#
#   SOURCE_URL=... TARGET_URL=... ADMIN_URL=... ./scripts/bench.sh [rows]
#
# Generates a synthetic dataset of `rows` rows (default 400000 ≈ 100 MB), measures initial
# copy throughput, then a CDC phase under a sustained writer, sampling RSS and CPU of the
# pgsync process every second. Results are printed as JSON and appended to bench-results/.
set -euo pipefail

ROWS=${1:-400000}
SOURCE_URL=${SOURCE_URL:-postgres://pgsync:pgsync@localhost:5433/app}
TARGET_URL=${TARGET_URL:-postgres://postgres:postgres@localhost:5434/app}
ADMIN_URL=${ADMIN_URL:-postgres://postgres:postgres@localhost:5433/app}
CDC_SECONDS=${CDC_SECONDS:-60}
WRITERS=${WRITERS:-4}
BIN=${BIN:-./target/release/pgsync}
OUT=bench-results
mkdir -p "$OUT"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
CFG=$OUT/bench-$STAMP.yaml
LOG=$OUT/bench-$STAMP.log
SAMPLES=$OUT/bench-$STAMP.samples

cat > "$CFG" <<YAML
job:
  id: bench
source:
  url_env: SOURCE_URL
  publication: pub_bench
  slot: slot_bench
target:
  url_env: TARGET_URL
replication:
  tables: [bench.items, bench.ledger]
resources:
  max_parallel_copies: ${COPIES:-2}
  memory_budget_mb: ${BUDGET_MB:-128}
safety:
  monitor_interval: 10s
YAML

echo "== preparing $ROWS rows on the source" >&2
psql -q "$ADMIN_URL" <<SQL
DROP PUBLICATION IF EXISTS pub_bench;
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'slot_bench' AND NOT active;
DROP SCHEMA IF EXISTS bench CASCADE;
CREATE SCHEMA bench;
CREATE TABLE bench.items (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  sku text NOT NULL, name text NOT NULL, price numeric(12,2) NOT NULL, qty int NOT NULL,
  attrs jsonb NOT NULL, tags text[] NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), blob bytea
);
CREATE TABLE bench.ledger (
  id bigserial PRIMARY KEY, item_id bigint NOT NULL, delta int NOT NULL, note text, at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO bench.items (sku, name, price, qty, attrs, tags, blob)
SELECT 'SKU-' || g, 'Item number ' || g || ' ' || md5(g::text), (g % 10000)::numeric / 100, g % 500,
       jsonb_build_object('color', (ARRAY['red','green','blue'])[g % 3 + 1], 'w', g % 97, 'desc', md5(g::text || 'x')),
       ARRAY['t' || (g % 7), 'cat' || (g % 13)], decode(md5(g::text), 'hex')
FROM generate_series(1, $ROWS) g;
INSERT INTO bench.ledger (item_id, delta, note)
SELECT (g % $ROWS) + 1, g % 11 - 5, CASE WHEN g % 3 = 0 THEN NULL ELSE 'ledger entry ' || g END
FROM generate_series(1, $ROWS) g;
GRANT USAGE ON SCHEMA bench TO pgsync; GRANT SELECT ON ALL TABLES IN SCHEMA bench TO pgsync;
CREATE PUBLICATION pub_bench FOR TABLE bench.items, bench.ledger;
VACUUM ANALYZE bench.items; VACUUM ANALYZE bench.ledger;
SQL
psql -q "$TARGET_URL" -c "DROP SCHEMA IF EXISTS bench CASCADE; DELETE FROM pgsync.job WHERE job_id = 'bench';" 2>/dev/null || true
SRC_BYTES=$(psql -Atq "$ADMIN_URL" -c "SELECT pg_total_relation_size('bench.items') + pg_total_relation_size('bench.ledger')")
echo "== dataset: $ROWS rows per table, $SRC_BYTES bytes on disk" >&2

sample() { # pid file
  while kill -0 "$1" 2>/dev/null; do
    ps -o rss=,%cpu= -p "$1" | awk -v t="$(date +%s)" -v ph="$3" '{print t, $1*1024, $2, ph}' >> "$2" || true
    sleep 1
  done
}

export SOURCE_URL TARGET_URL
WAL_AT_START=$(psql -Atq "$ADMIN_URL" -c "SELECT pg_current_wal_lsn()")
T0=$(date +%s.%N)
"$BIN" --config "$CFG" --json bootstrap > "$LOG" 2>&1 &
PID=$!
sample $PID "$SAMPLES" snapshot &
SAMPLER=$!
# Optional writers during the snapshot (WRITERS_DURING_SNAPSHOT=n): the catch-up phase
# between copy completion and STREAMING then has real work to do.
SNAP_WRITERS=()
for w in $(seq 1 "${WRITERS_DURING_SNAPSHOT:-0}"); do
  ( while ! psql -Atq "$TARGET_URL" -c "SELECT state FROM pgsync.job WHERE job_id='bench'" 2>/dev/null | grep -q 'CATCHING_UP\|STREAMING'; do
      psql -q "$ADMIN_URL" -c "UPDATE bench.items SET qty = qty + 1 WHERE id IN (SELECT ((random()*$ROWS)::int % $ROWS) + 1 FROM generate_series(1,20)); INSERT INTO bench.ledger (item_id, delta, note) SELECT ((random()*$ROWS)::int % $ROWS) + 1, 1, 'snap$w' FROM generate_series(1,30);" >/dev/null 2>&1 || true
    done ) &
  SNAP_WRITERS+=($!)
done
T_COPY=
RETAINED_PEAK=0
until psql -Atq "$TARGET_URL" -c "SELECT state FROM pgsync.job WHERE job_id='bench'" 2>/dev/null | grep -q STREAMING; do
  if ! kill -0 $PID 2>/dev/null; then echo "pgsync exited early; see $LOG" >&2; exit 1; fi
  if [ -z "$T_COPY" ] && psql -Atq "$TARGET_URL" -c "SELECT state FROM pgsync.job WHERE job_id='bench'" 2>/dev/null | grep -q CATCHING_UP; then T_COPY=$(date +%s.%N); fi
  R=$(psql -Atq "$ADMIN_URL" -c "SELECT coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn),0)::bigint FROM pg_replication_slots WHERE slot_name='slot_bench'" 2>/dev/null || echo 0)
  [ "${R:-0}" -gt "$RETAINED_PEAK" ] 2>/dev/null && RETAINED_PEAK=$R
  sleep 0.5
done
T1=$(date +%s.%N)
[ -z "$T_COPY" ] && T_COPY=$T1
WAL_AT_STREAMING=$(psql -Atq "$ADMIN_URL" -c "SELECT pg_current_wal_lsn()")
WAL_DURING_SNAPSHOT=$(psql -Atq "$ADMIN_URL" -c "SELECT pg_wal_lsn_diff('$WAL_AT_STREAMING', '$WAL_AT_START')::bigint")
wait "${SNAP_WRITERS[@]}" 2>/dev/null || true
kill $SAMPLER 2>/dev/null || true; wait $SAMPLER 2>/dev/null || true
SNAP_SECS=$(echo "$T1 - $T0" | bc)
COPY_SECS=$(echo "$T_COPY - $T0" | bc)
CATCHUP_SECS=$(echo "$T1 - $T_COPY" | bc)
COPIED=$(psql -Atq "$TARGET_URL" -c "SELECT sum(bytes_copied) FROM pgsync.table_sync WHERE job_id='bench'")
echo "== snapshot: copy $COPY_SECS s, catch-up $CATCHUP_SECS s, total $SNAP_SECS s, $COPIED COPY bytes, $WAL_DURING_SNAPSHOT WAL bytes generated, retained peak $RETAINED_PEAK" >&2

echo "== CDC phase: $WRITERS writers for $CDC_SECONDS s" >&2
sample $PID "$SAMPLES" cdc &
SAMPLER=$!
for w in $(seq 1 $WRITERS); do
  (
    END=$(( $(date +%s) + CDC_SECONDS ))
    i=0
    while [ "$(date +%s)" -lt $END ]; do
      i=$((i+1))
      psql -q "$ADMIN_URL" -c "BEGIN; UPDATE bench.items SET qty = qty + 1, attrs = attrs || '{\"touched\": true}' WHERE id IN (SELECT ((random()*$ROWS)::int % $ROWS) + 1 FROM generate_series(1,20)); INSERT INTO bench.ledger (item_id, delta, note) SELECT ((random()*$ROWS)::int % $ROWS) + 1, 1, 'w$w-' || g FROM generate_series(1,30) g; DELETE FROM bench.ledger WHERE id IN (SELECT id FROM bench.ledger ORDER BY id LIMIT 5); COMMIT;" >/dev/null 2>&1 || true
    done
  ) &
done
wait $(jobs -p | grep -v "^$SAMPLER$" | grep -v "^$PID$") 2>/dev/null || true
T2=$(date +%s.%N)
echo "== writers done; waiting for convergence" >&2
until [ "$(psql -Atq "$ADMIN_URL" -c "SELECT pg_current_wal_lsn()")" = "$(psql -Atq "$TARGET_URL" -c "SELECT acked_lsn FROM pgsync.job WHERE job_id='bench'")" ]; do sleep 1; done
T3=$(date +%s.%N)
kill $SAMPLER 2>/dev/null || true; wait $SAMPLER 2>/dev/null || true
kill -TERM $PID; wait $PID || true

SRC_HASH=$(psql -Atq "$ADMIN_URL" -c "SELECT md5(string_agg(t::text,'|' ORDER BY id)) FROM bench.items t")
TGT_HASH=$(psql -Atq "$TARGET_URL" -c "SELECT md5(string_agg(t::text,'|' ORDER BY id)) FROM bench.items t")
SRC_HASH2=$(psql -Atq "$ADMIN_URL" -c "SELECT md5(string_agg(t::text,'|' ORDER BY id)) FROM bench.ledger t")
TGT_HASH2=$(psql -Atq "$TARGET_URL" -c "SELECT md5(string_agg(t::text,'|' ORDER BY id)) FROM bench.ledger t")
TXNS=$(psql -Atq "$TARGET_URL" -c "SELECT txns_applied FROM pgsync.job WHERE job_id='bench'")
ROWS_APPLIED=$(grep -o '"rows_applied":[0-9]*' "$LOG" | tail -1 | cut -d: -f2 || true)
CDC_SECS=$(echo "$T3 - $T1" | bc)
LAG_SECS=$(echo "$T3 - $T2" | bc)

python3 - "$SAMPLES" <<PY > "$OUT/bench-$STAMP.json"
import sys, json, statistics
rows=[l.split() for l in open(sys.argv[1])]
def stats(ph):
    r=[x for x in rows if x[3]==ph]
    rss=[int(x[1]) for x in r]; cpu=[float(x[2]) for x in r]
    return {"samples":len(r),"rss_avg_mb":round(statistics.mean(rss)/1048576,1) if rss else None,
            "rss_peak_mb":round(max(rss)/1048576,1) if rss else None,"cpu_avg_pct":round(statistics.mean(cpu),1) if cpu else None}
print(json.dumps({
 "rows_per_table":$ROWS,"source_bytes_on_disk":$SRC_BYTES,"copy_bytes":$COPIED,
 "snapshot_seconds":$SNAP_SECS,"snapshot_mb_per_s":round($COPIED/1e6/$SNAP_SECS,1),
 "copy_seconds":$COPY_SECS,"copy_mb_per_s":round($COPIED/1e6/max($COPY_SECS,0.001),1),"catchup_seconds_after_copy":$CATCHUP_SECS,
 "writers_during_snapshot":${WRITERS_DURING_SNAPSHOT:-0},"wal_bytes_generated_during_snapshot":$WAL_DURING_SNAPSHOT,"retained_wal_peak_bytes":$RETAINED_PEAK,
 "cdc_seconds":$CDC_SECS,"cdc_txns_applied":$TXNS,"cdc_rows_applied":${ROWS_APPLIED:-0},"cdc_txns_per_s":round($TXNS/$CDC_SECS,1),
 "drain_lag_seconds_after_writers_stopped":$LAG_SECS,
 "snapshot":stats("snapshot"),"cdc":stats("cdc"),
 "consistent": "$SRC_HASH"=="$TGT_HASH" and "$SRC_HASH2"=="$TGT_HASH2",
 "machine": "$(uname -m) $(sysctl -n machdep.cpu.brand_string 2>/dev/null || grep -m1 'model name' /proc/cpuinfo | cut -d: -f2)", "ram_bytes": $(sysctl -n hw.memsize 2>/dev/null || grep MemTotal /proc/meminfo | awk '{print $2*1024}'),
 "cores": $(sysctl -n hw.ncpu 2>/dev/null || nproc), "kernel": "$(uname -sr)", "disk_free_bytes": $(df -k . | awk 'NR==2 {print $4*1024}'),
 "pgsync_commit": "$(git rev-parse --short HEAD)", "source_pg": "$(psql -Atq "$ADMIN_URL" -c 'SHOW server_version')", "timestamp": "$STAMP"
}, indent=2))
PY
cat "$OUT/bench-$STAMP.json"
