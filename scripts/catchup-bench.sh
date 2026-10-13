#!/usr/bin/env bash
# Catch-up throughput: with the bench job bootstrapped and stopped, generate a backlog of
# committed transactions on the source, then start `pgsync run` and time how long it takes
# to acknowledge the current WAL position. Measures apply throughput without a writer
# bottleneck. Expects scripts/bench.sh to have run first (schema bench.*, job 'bench').
set -euo pipefail
SOURCE_URL=${SOURCE_URL:?}; TARGET_URL=${TARGET_URL:?}; ADMIN_URL=${ADMIN_URL:?}
BIN=${BIN:-./target/release/pgsync}
TXNS=${TXNS:-5000}; ROWS_PER_TXN=${ROWS_PER_TXN:-100}
OUT=bench-results; STAMP=$(date -u +%Y%m%dT%H%M%SZ)
CFG=$(ls -t $OUT/bench-*.yaml | head -1)
export SOURCE_URL TARGET_URL

echo "== generating backlog: $TXNS transactions x $ROWS_PER_TXN rows (inserts + updates + deletes)" >&2
psql -q "$ADMIN_URL" <<SQL
DO \$\$
DECLARE i int; n bigint;
BEGIN
  SELECT count(*) INTO n FROM bench.items;
  FOR i IN 1..$TXNS LOOP
    INSERT INTO bench.ledger (item_id, delta, note) SELECT (random()*n)::bigint % n + 1, 1, 'catchup-' || i || '-' || g FROM generate_series(1, $ROWS_PER_TXN) g;
    UPDATE bench.items SET qty = qty + 1 WHERE id IN (SELECT (random()*n)::bigint % n + 1 FROM generate_series(1, 10));
    DELETE FROM bench.ledger WHERE id IN (SELECT id FROM bench.ledger ORDER BY id DESC LIMIT 5);
    COMMIT;
  END LOOP;
END \$\$;
SQL
TARGET_LSN=$(psql -Atq "$ADMIN_URL" -c "SELECT pg_current_wal_lsn()")
START_LSN=$(psql -Atq "$TARGET_URL" -c "SELECT acked_lsn FROM pgsync.job WHERE job_id='bench'")
BACKLOG=$(psql -Atq "$ADMIN_URL" -c "SELECT pg_wal_lsn_diff('$TARGET_LSN', '$START_LSN')")
echo "== backlog: $BACKLOG WAL bytes" >&2

SAMPLES=$OUT/catchup-$STAMP.samples; : > "$SAMPLES"
T0=$(date +%s.%N)
"$BIN" --config "$CFG" --json run > "$OUT/catchup-$STAMP.log" 2>&1 &
PID=$!
( while kill -0 $PID 2>/dev/null; do ps -o rss=,%cpu= -p $PID | awk -v t="$(date +%s)" '{print t, $1*1024, $2}' >> "$SAMPLES"; sleep 1; done ) &
until [ "$(psql -Atq "$TARGET_URL" -c "SELECT acked_lsn >= '$TARGET_LSN'::pg_lsn FROM pgsync.job WHERE job_id='bench'")" = "t" ]; do
  kill -0 $PID 2>/dev/null || { echo "pgsync exited; see $OUT/catchup-$STAMP.log" >&2; exit 1; }
  sleep 0.5
done
T1=$(date +%s.%N)
kill -TERM $PID; wait $PID || true
SECS=$(echo "$T1 - $T0" | bc)
A=$(psql -Atq "$ADMIN_URL" -c "SELECT md5(string_agg(h, ',' ORDER BY b)) || ':' || sum(n) FROM (SELECT id / 100000 AS b, md5(string_agg(t::text,'|' ORDER BY id)) AS h, count(*) AS n FROM bench.ledger t GROUP BY 1) s")
B=$(psql -Atq "$TARGET_URL" -c "SELECT md5(string_agg(h, ',' ORDER BY b)) || ':' || sum(n) FROM (SELECT id / 100000 AS b, md5(string_agg(t::text,'|' ORDER BY id)) AS h, count(*) AS n FROM bench.ledger t GROUP BY 1) s")
ROWS=$(grep -o '"rows_applied":[0-9]*' "$OUT/catchup-$STAMP.log" | tail -1 | cut -d: -f2 || true)
TX=$(psql -Atq "$TARGET_URL" -c "SELECT txns_applied FROM pgsync.job WHERE job_id='bench'")
python3 - "$SAMPLES" <<PY | tee "$OUT/catchup-$STAMP.json"
import sys, json, statistics
r=[l.split() for l in open(sys.argv[1])]
rss=[int(x[1]) for x in r]; cpu=[float(x[2]) for x in r]
print(json.dumps({"backlog_txns":$TXNS,"rows_per_txn":$ROWS_PER_TXN,"backlog_wal_bytes":$BACKLOG,"catchup_seconds":$SECS,
 "txns_per_s":round($TXNS/$SECS,1),"rows_applied_logged":${ROWS:-0},"rows_per_s":round(${ROWS:-0}/$SECS,1),"txns_applied_total":$TX,
 "wal_mb_per_s":round($BACKLOG/1e6/$SECS,1),"rss_avg_mb":round(statistics.mean(rss)/1048576,1),"rss_peak_mb":round(max(rss)/1048576,1),
 "cpu_avg_pct":round(statistics.mean(cpu),1),"consistent":"$A"=="$B","timestamp":"$STAMP"},indent=2))
PY
