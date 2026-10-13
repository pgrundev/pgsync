#!/usr/bin/env bash
# Run scripts/scale.sh for 1, 5, 10 and 20 jobs and derive the incremental memory per job:
# (aggregate RSS at N - aggregate RSS at M) / (N - M), idle and active. One summary JSON.
#
#   SOURCE_URL=... TARGET_URL=... ADMIN_URL=... ./scripts/scale-matrix.sh [rows] [seconds]
set -euo pipefail
ROWS=${1:-20000}; SECS=${2:-60}
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
ROOT=bench-results/scale-matrix-$STAMP
mkdir -p "$ROOT"
for n in ${SIZES:-1 5 10 20}; do
  OUT=$ROOT/n$n ./scripts/scale.sh "$n" "$ROWS" "$SECS" > "$ROOT/n$n.log" 2>&1
  echo "== $n jobs: $(python3 -c "import json; d=json.load(open('$ROOT/n$n/summary.json')); print('idle', d['idle'], 'active', d['active'], 'consistent', d['consistent_jobs'])")" >&2
done
python3 - "$ROOT" <<'PY' | tee "$ROOT/summary.json"
import sys, json, glob, os
root=sys.argv[1]
runs=sorted([json.load(open(f)) for f in glob.glob(f"{root}/n*/summary.json")], key=lambda d: d["workers"])
out={"runs":runs,"incremental_mb_per_job":[]}
for a,b in zip(runs,runs[1:]):
    dn=b["workers"]-a["workers"]
    out["incremental_mb_per_job"].append({"from":a["workers"],"to":b["workers"],
        "idle":round((b["idle"]["aggregate_rss_avg_mb"]-a["idle"]["aggregate_rss_avg_mb"])/dn,2),
        "active":round((b["active"]["aggregate_rss_avg_mb"]-a["active"]["aggregate_rss_avg_mb"])/dn,2),
        "active_peak":round((b["active"]["aggregate_rss_peak_mb"]-a["active"]["aggregate_rss_peak_mb"])/dn,2)})
print(json.dumps(out, indent=2))
PY
