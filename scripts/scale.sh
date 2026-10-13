#!/usr/bin/env bash
# Run N independent PGSync jobs (one process each) against docker compose and measure
# aggregate and per-process RSS while a writer loads every job.
#
#   SOURCE_URL=... TARGET_URL=... ADMIN_URL=... ./scripts/scale.sh <workers> [rows] [seconds]
set -euo pipefail
N=${1:-5}
ROWS=${2:-20000}
SECS=${3:-60}
SOURCE_URL=${SOURCE_URL:-postgres://pgsync:pgsync@localhost:5433/app}
TARGET_URL=${TARGET_URL:-postgres://postgres:postgres@localhost:5434/app}
ADMIN_URL=${ADMIN_URL:-postgres://postgres:postgres@localhost:5433/app}
BIN=${BIN:-./target/release/pgsync}
OUT=bench-results/scale-$N-$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$OUT"
export SOURCE_URL TARGET_URL

for i in $(seq 1 "$N"); do
  psql -q "$ADMIN_URL" <<SQL
DROP PUBLICATION IF EXISTS pub_scale$i;
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'slot_scale$i' AND NOT active;
DROP SCHEMA IF EXISTS scale$i CASCADE;
CREATE SCHEMA scale$i;
CREATE TABLE scale$i.t (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, v text NOT NULL, n int NOT NULL, j jsonb);
INSERT INTO scale$i.t (v, n, j) SELECT md5(g::text), g, jsonb_build_object('g', g) FROM generate_series(1, $ROWS) g;
GRANT USAGE ON SCHEMA scale$i TO pgsync; GRANT SELECT ON ALL TABLES IN SCHEMA scale$i TO pgsync;
CREATE PUBLICATION pub_scale$i FOR TABLE scale$i.t;
SQL
  psql -q "$TARGET_URL" -c "DROP SCHEMA IF EXISTS scale$i CASCADE; DELETE FROM pgsync.job WHERE job_id = 'scale$i';" >/dev/null 2>&1 || true
  cat > "$OUT/job$i.yaml" <<YAML
job: {id: scale$i}
source: {url_env: SOURCE_URL, publication: pub_scale$i, slot: slot_scale$i}
target: {url_env: TARGET_URL}
replication: {tables: [scale$i.t]}
safety: {monitor_interval: 30s}
YAML
done

PIDS=()
for i in $(seq 1 "$N"); do
  "$BIN" --config "$OUT/job$i.yaml" --json bootstrap > "$OUT/job$i.log" 2>&1 &
  PIDS+=($!)
done
echo "== started $N workers: ${PIDS[*]}" >&2
until [ "$(psql -Atq "$TARGET_URL" -c "SELECT count(*) FROM pgsync.job WHERE job_id LIKE 'scale%' AND state = 'STREAMING'")" = "$N" ]; do sleep 1; done
echo "== all streaming" >&2

( END=$(( $(date +%s) + SECS )); while [ "$(date +%s)" -lt $END ]; do
    for i in $(seq 1 "$N"); do
      psql -q "$ADMIN_URL" -c "UPDATE scale$i.t SET n = n + 1 WHERE id IN (SELECT (random()*$ROWS)::int % $ROWS + 1 FROM generate_series(1,10)); INSERT INTO scale$i.t (v, n) VALUES (md5(random()::text), 0);" >/dev/null 2>&1 || true
    done
  done ) &
WRITER=$!
: > "$OUT/samples"
while kill -0 $WRITER 2>/dev/null; do
  ps -o pid=,rss=,%cpu= -p "$(IFS=,; echo "${PIDS[*]}")" | awk -v t="$(date +%s)" '{print t, $1, $2*1024, $3}' >> "$OUT/samples"
  sleep 1
done
sleep 3
for p in "${PIDS[@]}"; do kill -TERM "$p" 2>/dev/null || true; done
wait || true
OK=0
for i in $(seq 1 "$N"); do
  a=$(psql -Atq "$ADMIN_URL" -c "SELECT md5(string_agg(t::text,'|' ORDER BY id)) FROM scale$i.t t")
  b=$(psql -Atq "$TARGET_URL" -c "SELECT md5(string_agg(t::text,'|' ORDER BY id)) FROM scale$i.t t")
  [ "$a" = "$b" ] && OK=$((OK+1))
done
python3 - "$OUT/samples" "$N" "$OK" <<'PY' | tee "$OUT/summary.json"
import sys, json, collections, statistics
rows=[l.split() for l in open(sys.argv[1])]
by_t=collections.defaultdict(list)
per=collections.defaultdict(list)
for t,pid,rss,cpu in rows:
    by_t[t].append(int(rss)); per[pid].append(int(rss))
tot=[sum(v) for v in by_t.values()]
print(json.dumps({"workers":int(sys.argv[2]),"consistent_jobs":int(sys.argv[3]),
 "aggregate_rss_avg_mb":round(statistics.mean(tot)/1048576,1),"aggregate_rss_peak_mb":round(max(tot)/1048576,1),
 "per_process_rss_avg_mb":round(statistics.mean([statistics.mean(v) for v in per.values()])/1048576,1),
 "per_process_rss_peak_mb":round(max(max(v) for v in per.values())/1048576,1)}, indent=2))
PY
