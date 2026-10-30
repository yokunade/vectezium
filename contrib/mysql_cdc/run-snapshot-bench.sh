#!/usr/bin/env bash
# Snapshot benchmark for the mysql_cdc source.
#
# Usage: ./run-snapshot-bench.sh /path/to/vector ROWS_PER_TABLE [TABLES] [THREADS] [CHUNK_SIZE]
#        e.g. 100000000 5 8 1000000  (5 x 100M = 500M rows, 8 snapshot threads)
#
# Seeds tables on the MySQL server itself (INSERT ... SELECT, binlog off for the seed), takes one
# snapshot with Vector into local gzip NDJSON files, and verifies every table's row count,
# sum(id) and sum(val) against MySQL, plus Debezium's first/last snapshot markers.
# Requires: docker (with compose), jq, python3.
set -uo pipefail

VECTOR=${1:?usage: run-snapshot-bench.sh /path/to/vector ROWS_PER_TABLE [TABLES] [THREADS] [CHUNK]}
VECTOR=$(cd "$(dirname "$VECTOR")" && pwd)/$(basename "$VECTOR")
ROWS=${2:?rows per table}
TABLES=${3:-5}
THREADS=${4:-8}
CHUNK=${5:-1000000}
SEED_BATCH=1000000
cd "$(dirname "$0")"

for tool in docker jq python3; do
  command -v "$tool" >/dev/null || { echo "$tool is required"; exit 2; }
done

log() { echo "[$(date +%T)] $*"; }
TOTAL=$(( ROWS * TABLES ))

# ---------------------------------------------------------------- preflight
cores=$(nproc)
mem_gb=$(awk '/MemTotal/ {printf "%d", $2 / 1048576}' /proc/meminfo)
wsl_free_gb=$(df -BG --output=avail / | tail -1 | tr -dc '0-9')
win_free_gb=$(df -BG --output=avail /mnt/c 2>/dev/null | tail -1 | tr -dc '0-9')
free_gb=$wsl_free_gb
# WSL's virtual disk grows on the Windows drive, so the Windows free space is the real limit.
[ -n "$win_free_gb" ] && [ "$win_free_gb" -lt "$free_gb" ] && free_gb=$win_free_gb
# Measured: ~490 B/row in InnoDB (row, PK, page overhead) + ~70 B/row of gzip output; +20%.
need_gb=$(( TOTAL * 560 / 1000000000 * 12 / 10 + 5 ))
log "Snapshot benchmark: $TABLES tables x $ROWS rows = $TOTAL rows, $THREADS threads, chunk $CHUNK"
log "Host: $cores cores, ${mem_gb} GB RAM visible to WSL, ${free_gb} GB free disk (WSL ${wsl_free_gb} GB, Windows ${win_free_gb:-?} GB); estimated need ${need_gb} GB"
if [ "$free_gb" -lt "$need_gb" ]; then
  log "ERROR: not enough free disk for this run (need ~${need_gb} GB, have ${free_gb} GB)"
  exit 3
fi

# Give InnoDB a buffer pool proportional to the memory available (25%, 1-8 GB).
pool_gb=$(( mem_gb / 4 )); [ "$pool_gb" -lt 1 ] && pool_gb=1; [ "$pool_gb" -gt 8 ] && pool_gb=8

VPID=""
SAMPLER=""
cleanup() {
  [ -n "$SAMPLER" ] && kill "$SAMPLER" 2>/dev/null
  [ -n "$VPID" ] && kill -TERM "$VPID" 2>/dev/null
}
trap cleanup EXIT

mysql_root() { docker exec -i cdc-mysql mysql -uroot -prootpw "$@" 2>>out/load-bench-mysql.err; }

wait_for() {
  local timeout=$1 desc=$2; shift 2
  for _ in $(seq 1 "$timeout"); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  log "timed out waiting for: $desc"
  return 1
}

# ---------------------------------------------------------------- environment
rm -rf out/bench-* out/load-bench-* && mkdir -p out/bench-data out/bench-output
sed -e "s/@SNAP_THREADS@/$THREADS/" -e "s/@SNAP_CHUNK@/$CHUNK/" -e "s|@OUT_DIR@|$(pwd)/out/bench-output|" \
  vector-snapbench.yaml > out/bench-vector.yaml
if ! "$VECTOR" validate --no-environment out/bench-vector.yaml > out/load-bench-validate.log 2>&1; then
  log "ERROR: the generated Vector config is invalid:"; cat out/load-bench-validate.log
  exit 1
fi
docker compose down -v >/dev/null 2>&1
docker compose up -d mysql >/dev/null 2>&1 || { log "docker compose up failed"; exit 1; }
wait_for 180 "MySQL healthy" \
  bash -c '[ "$(docker inspect -f "{{.State.Health.Status}}" cdc-mysql)" = healthy ]' || exit 1
wait_for 60 "init.sql applied" bash -c "docker exec cdc-mysql mysql -uroot -prootpw -N -e 'SELECT 1 FROM inventory.customers LIMIT 1' 2>/dev/null | grep -q 1" || exit 1

mysql_root -e "
  SET GLOBAL innodb_buffer_pool_size = ${pool_gb} * 1024 * 1024 * 1024;
  SET GLOBAL innodb_redo_log_capacity = 4 * 1024 * 1024 * 1024;
  SET GLOBAL innodb_flush_log_at_trx_commit = 2;
  SET GLOBAL sync_binlog = 0;"

# ---------------------------------------------------------------- seed (server side)
log "Seeding (binlog off for the seed; ${pool_gb} GB buffer pool)"
seed_start=$(date +%s)
mysql_root -e "
  SET SESSION sql_log_bin = 0;
  SET SESSION cte_max_recursion_depth = $(( SEED_BATCH + 1 ));
  DROP DATABASE IF EXISTS bench;
  CREATE DATABASE bench;
  CREATE TABLE bench.seq (n INT PRIMARY KEY);
  INSERT INTO bench.seq WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < $SEED_BATCH) SELECT n FROM c;"
seed_table() { # one session per table, so tables load in parallel
  local t=$1
  {
    echo "SET SESSION sql_log_bin = 0;"
    echo "CREATE TABLE bench.t$t (id BIGINT PRIMARY KEY, val INT NOT NULL, note VARCHAR(64), amount DECIMAL(12,2), status ENUM('new','paid','shipped'), updated DATETIME(6));"
    local done_rows=0
    while [ "$done_rows" -lt "$ROWS" ]; do
      local n=$(( ROWS - done_rows )); [ "$n" -gt "$SEED_BATCH" ] && n=$SEED_BATCH
      echo "INSERT INTO bench.t$t SELECT $done_rows + n, (($done_rows + n) * 7919) % 1000003, CONCAT('row ', $done_rows + n), ($done_rows + n) / 100, ELT(1 + (($done_rows + n) % 3), 'new', 'paid', 'shipped'), '2024-01-01 00:00:00.000000' FROM bench.seq WHERE n <= $n;"
      done_rows=$(( done_rows + n ))
    done
  } | mysql_root
}
pids=()
for t in $(seq 1 "$TABLES"); do seed_table "$t" & pids+=($!); done
progress() {
  while true; do
    sleep 60
    local loaded
    loaded=$(mysql_root -N -e "SELECT COALESCE(SUM(TABLE_ROWS), 0) FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'bench' AND TABLE_NAME LIKE 't%'" | tr -d ' ')
    log "  seeding... ~$loaded rows so far"
  done
}
progress & PROGRESS=$!
for p in "${pids[@]}"; do wait "$p"; done
kill "$PROGRESS" 2>/dev/null
seed_seconds=$(( $(date +%s) - seed_start ))
log "Seeded in ${seed_seconds}s"

log "Computing expected counts and checksums"
: > out/load-bench-truth.tsv
for t in $(seq 1 "$TABLES"); do
  echo "t$t $(mysql_root -N -e "SELECT COUNT(*), SUM(id), SUM(val) FROM bench.t$t" | tr '\t' ' ')" >> out/load-bench-truth.tsv
done
cat out/load-bench-truth.tsv

# ---------------------------------------------------------------- snapshot
sample_resources() {
  local hz prev_ticks="" prev_t=""
  hz=$(getconf CLK_TCK)
  echo "t,rss_kb,cpu_pct" > out/load-bench-resources.csv
  while true; do
    if [ -r "/proc/$1/stat" ]; then
      local ticks rss now
      ticks=$(awk '{print $14 + $15}' "/proc/$1/stat")
      rss=$(awk '/VmRSS/ {print $2}' "/proc/$1/status")
      now=$(date +%s.%N)
      if [ -n "$prev_ticks" ]; then
        awk -v t="$now" -v pt="$prev_t" -v k="$ticks" -v pk="$prev_ticks" -v hz="$hz" -v rss="$rss" \
          'BEGIN { printf "%.1f,%d,%.1f\n", t, rss, (k - pk) / hz / (t - pt) * 100 }' >> out/load-bench-resources.csv
      fi
      prev_ticks=$ticks
      prev_t=$now
    fi
    sleep 2
  done
}

log "Snapshotting with Vector ($THREADS threads)"
vector_start=$(date +%s.%N)
"$VECTOR" --config out/bench-vector.yaml > out/load-vector.log 2>&1 &
VPID=$!
sample_resources "$VPID" & SAMPLER=$!
last_report=0
completed_at=""
while kill -0 "$VPID" 2>/dev/null; do
  sleep 5
  now=$(date +%s)
  # Vector exits by itself once the snapshot is delivered; don't wait forever if it doesn't.
  if [ -z "$completed_at" ] && grep -q "Snapshot completed" out/load-vector.log; then completed_at=$now; fi
  if [ -n "$completed_at" ] && [ $(( now - completed_at )) -gt 300 ]; then
    log "Vector still running 5 minutes after the snapshot completed; stopping it"
    kill -TERM "$VPID" 2>/dev/null
  fi
  if [ $(( now - last_report )) -ge 60 ]; then
    last_report=$now
    rows_done=$(grep -o 'Snapshot progress.*rows=[0-9]*' out/load-vector.log | tail -1 | grep -o 'rows=[0-9]*' | tr -dc '0-9')
    log "  snapshot... ${rows_done:-0} rows emitted, output $(du -sh out/bench-output | cut -f1)"
  fi
done
wait "$VPID"; vrc=$?
VPID=""
vector_seconds=$(awk -v s="$vector_start" -v e="$(date +%s.%N)" 'BEGIN { printf "%.1f", e - s }')
kill "$SAMPLER" 2>/dev/null; SAMPLER=""
log "Vector exited with $vrc after ${vector_seconds}s"

ts() { grep "$1" out/load-vector.log | head -1 | grep -oE '^[0-9TZ:.-]+'; }
started=$(ts "Snapshot started"); completed=$(ts "Snapshot completed")
snapshot_seconds=$(python3 -c "
from datetime import datetime
f = lambda s: datetime.fromisoformat(s.replace('Z', '+00:00'))
print(round((f('$completed') - f('$started')).total_seconds(), 1))" 2>/dev/null)
log "Snapshot: ${snapshot_seconds:-?}s (from 'Snapshot started' to 'Snapshot completed')"

python3 - "$THREADS" "$CHUNK" "$seed_seconds" "${snapshot_seconds:-0}" "$vector_seconds" \
  "$cores" "$mem_gb" > out/load-bench-metrics.json <<'PY'
import csv, json, statistics, sys
threads, chunk, seed, snap, vec, cores, mem = sys.argv[1:]
rows = [r for r in csv.DictReader(open("out/load-bench-resources.csv"))]
rss = [int(r["rss_kb"]) / 1024 for r in rows]
cpu = [float(r["cpu_pct"]) for r in rows]
print(json.dumps({
    "threads": int(threads), "chunk_size": int(chunk), "seed_seconds": int(seed),
    "snapshot_seconds": float(snap), "vector_seconds": float(vec),
    "rss_mb_max": round(max(rss), 1) if rss else None,
    "cpu_pct_avg": round(statistics.mean(cpu), 1) if cpu else None,
    "cpu_pct_max": round(max(cpu), 1) if cpu else None,
    "host": {"cores": int(cores), "mem_gb": int(mem)},
}))
PY

log "Verifying"
python3 verify_snapshot.py --output out/bench-output --truth out/load-bench-truth.tsv \
  --metrics out/load-bench-metrics.json --report out/load-bench-report.json
verified=$?
errors=$(grep -c " ERROR " out/load-vector.log)
log "Vector logged $errors error line(s)"
grep " ERROR " out/load-vector.log | cut -c1-200 | head -5
log "Disk used: MySQL $(docker exec cdc-mysql du -sh /var/lib/mysql 2>/dev/null | cut -f1), output $(du -sh out/bench-output | cut -f1)"
log "To free the space afterwards: docker compose down -v && rm -rf out/"
[ "$verified" -eq 0 ] && [ "$errors" -eq 0 ] && [ "$vrc" -eq 0 ]
