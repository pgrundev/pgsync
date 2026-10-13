#!/usr/bin/env bash
# Long-running soak: mixed workload with bursts, idle intervals, large transactions and a
# reproducible interruption schedule, sampled every SAMPLE_SECS. Ends with a full-content
# hash comparison. Results (CSV samples, JSON summary, logs) go to bench-results/soak-<stamp>/.
#
#   SOURCE_URL=... TARGET_URL=... ADMIN_URL=... DURATION_SECS=14400 ./scripts/soak.sh
#
# Interruptions (minutes from the start of the CDC phase, fixed so runs are comparable):
#   SIGKILL pgsync   at  7 23 41 67 97 131 173 211 (then every 40 min)
#   SIGTERM pgsync   at 15 53 89 151 199           (then every 60 min)
#   kill walsender   at 31 113 197
# Workload phases repeat every 20 minutes: 0-12 steady (4 writers), 12-14 burst (bulk
# updates), 14-16 idle, 16-18 one large transaction (LARGE_ROWS rows), 18-20 steady.
# SCHEDULE_UNIT_SECS (default 60) is the length of a "minute" above; set it to 5 for a
# smoke test that exercises every path in a few minutes.
set -euo pipefail
UNIT=${SCHEDULE_UNIT_SECS:-60}

ROWS=${ROWS:-400000}
DURATION_SECS=${DURATION_SECS:-14400}
SAMPLE_SECS=${SAMPLE_SECS:-10}
LARGE_ROWS=${LARGE_ROWS:-200000}
SOURCE_URL=${SOURCE_URL:?}; TARGET_URL=${TARGET_URL:?}; ADMIN_URL=${ADMIN_URL:?}
BIN=${BIN:-./target/release/pgsync}
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
OUT=bench-results/soak-$STAMP
mkdir -p "$OUT"
CFG=$OUT/soak.yaml
JOB=soak
export SOURCE_URL TARGET_URL

cat > "$CFG" <<YAML
job:
  id: $JOB
source:
  url_env: SOURCE_URL
  publication: pub_soak
  slot: slot_soak
target:
  url_env: TARGET_URL
replication:
  tables: [soak.items, soak.ledger]
resources:
  retry_backoff: {initial: 500ms, max: 5s}
safety:
  monitor_interval: 10s
YAML

log() { echo "$(date -u +%H:%M:%S) $*" | tee -a "$OUT/events.log" >&2; }

log "preparing $ROWS rows per table"
psql -q "$ADMIN_URL" <<SQL
DROP PUBLICATION IF EXISTS pub_soak;
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'slot_soak' AND NOT active;
DROP SCHEMA IF EXISTS soak CASCADE;
CREATE SCHEMA soak;
CREATE TABLE soak.items (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  sku text NOT NULL, name text NOT NULL, price numeric(12,2) NOT NULL, qty int NOT NULL,
  attrs jsonb NOT NULL, tags text[] NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), blob bytea
);
CREATE TABLE soak.ledger (
  id bigserial PRIMARY KEY, item_id bigint NOT NULL, delta int NOT NULL, note text, at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO soak.items (sku, name, price, qty, attrs, tags, blob)
SELECT 'SKU-' || g, 'Item ' || g || ' ' || md5(g::text), (g % 10000)::numeric / 100, g % 500,
       jsonb_build_object('color', (ARRAY['red','green','blue'])[g % 3 + 1], 'w', g % 97),
       ARRAY['t' || (g % 7), 'cat' || (g % 13)], decode(md5(g::text), 'hex')
FROM generate_series(1, $ROWS) g;
INSERT INTO soak.ledger (item_id, delta, note)
SELECT (g % $ROWS) + 1, g % 11 - 5, CASE WHEN g % 3 = 0 THEN NULL ELSE 'ledger ' || g END
FROM generate_series(1, $ROWS) g;
GRANT USAGE ON SCHEMA soak TO pgsync; GRANT SELECT ON ALL TABLES IN SCHEMA soak TO pgsync;
CREATE PUBLICATION pub_soak FOR TABLE soak.items, soak.ledger;
VACUUM ANALYZE soak.items; VACUUM ANALYZE soak.ledger;
SQL
psql -q "$TARGET_URL" -c "DROP SCHEMA IF EXISTS soak CASCADE; DELETE FROM pgsync.job WHERE job_id = '$JOB';" 2>/dev/null || true

PID=
start_pgsync() { # command
  "$BIN" --config "$CFG" --json "$1" >> "$OUT/pgsync.log" 2>&1 &
  PID=$!
  log "pgsync $1 started pid=$PID"
}
start_pgsync bootstrap
until psql -Atq "$TARGET_URL" -c "SELECT state FROM pgsync.job WHERE job_id='$JOB'" 2>/dev/null | grep -q STREAMING; do
  kill -0 $PID 2>/dev/null || { log "pgsync exited during bootstrap; see $OUT/pgsync.log"; exit 1; }
  sleep 1
done
log "snapshot complete; CDC phase begins"
T0=$(date +%s)

# ---- workload --------------------------------------------------------------------------
STOP=$OUT/stop
phase_of() { # seconds since T0 -> steady|burst|idle|large
  local m=$(( ($1 / UNIT) % 20 ))
  if [ $m -lt 12 ]; then echo steady; elif [ $m -lt 14 ]; then echo burst; elif [ $m -lt 16 ]; then echo idle; elif [ $m -lt 18 ]; then echo large; else echo steady; fi
}
writer() { # id
  local w=$1 i=0 ph
  while [ ! -f "$STOP" ]; do
    ph=$(phase_of $(( $(date +%s) - T0 )))
    case $ph in
      idle) sleep 5; continue;;
      burst)
        psql -q "$ADMIN_URL" -c "UPDATE soak.items SET qty = qty + 1, attrs = attrs || '{\"burst\": true}' WHERE id % 50 = $w % 50 AND id < 100000;" >/dev/null 2>&1 || true
        sleep 2;;
      large)
        if [ "$w" = 1 ]; then
          psql -q "$ADMIN_URL" -c "BEGIN; INSERT INTO soak.ledger (item_id, delta, note) SELECT (g % $ROWS) + 1, 2, 'large-' || g FROM generate_series(1, $LARGE_ROWS) g; DELETE FROM soak.ledger WHERE note LIKE 'large-%' AND id % 2 = 0; COMMIT;" >/dev/null 2>&1 || true
          sleep $(( UNIT ))
        else
          sleep 5
        fi;;
      *)
        i=$((i+1))
        psql -q "$ADMIN_URL" -c "BEGIN; UPDATE soak.items SET qty = qty + 1, name = left(name || 'x', 120) WHERE id IN (SELECT ((random()*$ROWS)::int % $ROWS) + 1 FROM generate_series(1,20)); INSERT INTO soak.ledger (item_id, delta, note) SELECT ((random()*$ROWS)::int % $ROWS) + 1, 1, 'w$w-' || g FROM generate_series(1,30) g; DELETE FROM soak.ledger WHERE id IN (SELECT id FROM soak.ledger ORDER BY id LIMIT 5); COMMIT;" >/dev/null 2>&1 || true
        sleep 0.05;;
    esac
  done
}
for w in 1 2 3 4; do writer $w & done
log "4 writers started"

# ---- sampler ---------------------------------------------------------------------------
echo "t,elapsed,phase,state,rss_bytes,cpu_pct,acked_lsn,source_wal_lsn,apply_lag_bytes,retained_wal_bytes,confirmed_flush,txns_applied,reconnects,src_conns,tgt_conns,pgsync_alive" > "$OUT/samples.csv"
sample() {
  local now=$(date +%s) el ph st rss cpu row acked cur lag ret conf txns rec sc tc alive
  el=$((now - T0)); ph=$(phase_of $el)
  if kill -0 "$PID" 2>/dev/null; then alive=1; read -r rss cpu < <(ps -o rss=,%cpu= -p "$PID" | awk '{print $1*1024, $2}'); else alive=0; rss=0; cpu=0; fi
  row=$(psql -Atq "$TARGET_URL" -c "SELECT state, acked_lsn, txns_applied FROM pgsync.job WHERE job_id='$JOB'" 2>/dev/null || echo "?|0/0|0")
  st=${row%%|*}; acked=$(echo "$row" | cut -d'|' -f2); txns=$(echo "$row" | cut -d'|' -f3)
  row=$(psql -Atq "$ADMIN_URL" -c "SELECT pg_current_wal_lsn(), coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(), '$acked'),0)::bigint, coalesce((SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) FROM pg_replication_slots WHERE slot_name='slot_soak'),0)::bigint, coalesce((SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name='slot_soak'),'0/0'), (SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'pgsync%')" 2>/dev/null || echo "0/0|0|0|0/0|0")
  cur=$(echo "$row" | cut -d'|' -f1); lag=$(echo "$row" | cut -d'|' -f2); ret=$(echo "$row" | cut -d'|' -f3); conf=$(echo "$row" | cut -d'|' -f4); sc=$(echo "$row" | cut -d'|' -f5)
  tc=$(psql -Atq "$TARGET_URL" -c "SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'pgsync%'" 2>/dev/null || echo 0)
  rec=$(grep -c '"transient failure; reconnecting"' "$OUT/pgsync.log" 2>/dev/null || true)
  echo "$now,$el,$ph,$st,$rss,$cpu,$acked,$cur,$lag,$ret,$conf,$txns,${rec:-0},$sc,$tc,$alive" >> "$OUT/samples.csv"
}

# ---- interruption schedule ------------------------------------------------------------
fired=""
schedule() { # minute -> action or empty
  local m=$1
  case " 7 23 41 67 97 131 173 211 " in *" $m "*) echo kill; return;; esac
  case " 15 53 89 151 199 " in *" $m "*) echo term; return;; esac
  case " 31 113 197 " in *" $m "*) echo walsender; return;; esac
  if [ $m -gt 211 ]; then
    if [ $(( (m - 211) % 40 )) = 0 ]; then echo kill; return; fi
    if [ $(( (m - 199) % 60 )) = 0 ]; then echo term; return; fi
  fi
}

MIN_FREE_MB=${MIN_FREE_MB:-2048}
free_mb() { df -k . | awk 'NR==2 {print int($4/1024)}'; }
END=$((T0 + DURATION_SECS))
while [ "$(date +%s)" -lt $END ]; do
  sample
  if [ "$(free_mb)" -lt "$MIN_FREE_MB" ]; then
    log "free disk $(free_mb) MB below MIN_FREE_MB=$MIN_FREE_MB; ending the soak early (clean stop, then hashes)"
    break
  fi
  m=$(( ($(date +%s) - T0) / UNIT ))
  act=$(schedule $m)
  if [ -n "$act" ] && [[ " $fired " != *" $m "* ]]; then
    fired="$fired $m"
    case $act in
      kill) log "minute $m: SIGKILL pgsync"; kill -KILL $PID 2>/dev/null || true; wait $PID 2>/dev/null || true; sleep 2; start_pgsync run;;
      term) log "minute $m: SIGTERM pgsync"; kill -TERM $PID 2>/dev/null || true; wait $PID 2>/dev/null || true; sleep 2; start_pgsync run;;
      walsender) log "minute $m: terminating walsender"; psql -Atq "$ADMIN_URL" -c "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name='slot_soak' AND active_pid IS NOT NULL" >/dev/null || true;;
    esac
  fi
  if ! kill -0 $PID 2>/dev/null; then
    log "pgsync exited unexpectedly (state $(psql -Atq "$TARGET_URL" -c "SELECT state || ' ' || coalesce(last_error,'') FROM pgsync.job WHERE job_id='$JOB'")); restarting"
    start_pgsync run
  fi
  sleep "$SAMPLE_SECS"
done
touch "$STOP"
log "duration reached; stopping writers"
wait $(jobs -p | grep -v "^$PID$") 2>/dev/null || true
log "writers stopped; waiting for convergence"
T_WRITERS_DONE=$(date +%s)
until [ "$(psql -Atq "$ADMIN_URL" -c "SELECT pg_current_wal_lsn()")" = "$(psql -Atq "$TARGET_URL" -c "SELECT acked_lsn FROM pgsync.job WHERE job_id='$JOB'")" ]; do
  sample; sleep 2
  [ $(( $(date +%s) - T_WRITERS_DONE )) -gt 600 ] && { log "no convergence after 10 minutes"; break; }
done
DRAIN=$(( $(date +%s) - T_WRITERS_DONE ))
kill -TERM $PID 2>/dev/null || true; wait $PID 2>/dev/null || true
log "pgsync stopped; comparing contents"
h() { psql -Atq "$1" -c "SELECT md5(string_agg(t::text,'|' ORDER BY id)) || ':' || count(*) FROM soak.$2 t"; }
SI=$(h "$ADMIN_URL" items); TI=$(h "$TARGET_URL" items); SL=$(h "$ADMIN_URL" ledger); TL=$(h "$TARGET_URL" ledger)
log "items  source=$SI target=$TI"
log "ledger source=$SL target=$TL"

MACHINE="$(uname -m) $(sysctl -n machdep.cpu.brand_string 2>/dev/null || grep -m1 'model name' /proc/cpuinfo | cut -d: -f2) $(uname -sr)"
python3 - "$OUT" "$DRAIN" "$SI" "$TI" "$SL" "$TL" "$MACHINE" "$(git rev-parse --short HEAD)" <<'PY' | tee "$OUT/summary.json"
import sys, csv, json, statistics
out, drain, si, ti, sl, tl = sys.argv[1], int(sys.argv[2]), *sys.argv[3:7]
machine, commit = sys.argv[7], sys.argv[8]
rows = list(csv.DictReader(open(f"{out}/samples.csv")))
alive = [r for r in rows if r["pgsync_alive"] == "1" and int(r["rss_bytes"]) > 0]
rss = [int(r["rss_bytes"]) for r in alive]
cpu = [float(r["cpu_pct"]) for r in alive]
lag = [int(r["apply_lag_bytes"]) for r in rows]
ret = [int(r["retained_wal_bytes"]) for r in rows]
dur = int(rows[-1]["elapsed"]) if rows else 0
q = max(1, dur // 4)
quarters = []
for i in range(4):
    qs = [int(r["rss_bytes"]) for r in alive if i*q <= int(r["elapsed"]) < (i+1)*q]
    quarters.append({"rss_avg_mb": round(statistics.mean(qs)/1048576,1) if qs else None, "rss_peak_mb": round(max(qs)/1048576,1) if qs else None})
def pct(v, p):
    v = sorted(v); return v[min(len(v)-1, int(len(v)*p))] if v else None
# a stall: lag above 64 MB for more than 5 consecutive minutes while the job is alive
stall, longest, run = 0, 0, 0
for r in rows:
    if int(r["apply_lag_bytes"]) > 64*1048576: run += 1
    else: run = 0
    longest = max(longest, run)
events = open(f"{out}/events.log").read()
print(json.dumps({
  "duration_seconds": dur, "samples": len(rows),
  "rss_avg_mb": round(statistics.mean(rss)/1048576,1) if rss else None, "rss_peak_mb": round(max(rss)/1048576,1) if rss else None,
  "rss_quarters": quarters, "rss_first_10min_avg_mb": round(statistics.mean([int(r["rss_bytes"]) for r in alive if int(r["elapsed"]) < 600])/1048576,1) if alive else None,
  "rss_last_10min_avg_mb": round(statistics.mean([int(r["rss_bytes"]) for r in alive if int(r["elapsed"]) > dur-600])/1048576,1) if alive else None,
  "cpu_avg_pct": round(statistics.mean(cpu),1) if cpu else None,
  "apply_lag_p50_mb": round(pct(lag,0.5)/1048576,2), "apply_lag_p99_mb": round(pct(lag,0.99)/1048576,2), "apply_lag_max_mb": round(max(lag)/1048576,2),
  "retained_wal_max_mb": round(max(ret)/1048576,1), "longest_lag_over_64mb_samples": longest,
  "txns_applied": int(rows[-1]["txns_applied"]), "reconnects_logged": int(rows[-1]["reconnects"]),
  "sigkills": events.count("SIGKILL"), "sigterms": events.count("SIGTERM"), "walsender_kills": events.count("terminating walsender"),
  "unexpected_exits": events.count("exited unexpectedly"),
  "drain_seconds_after_writers_stopped": drain,
  "consistent": si == ti and sl == tl, "items": {"source": si, "target": ti}, "ledger": {"source": sl, "target": tl},
  "machine": machine, "pgsync_commit": commit, "stamp": out.split('-')[-1]}, indent=2))
PY
