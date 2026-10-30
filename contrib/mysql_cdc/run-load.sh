#!/usr/bin/env bash
# Load test for the mysql_cdc source.
#
# Usage: ./run-load.sh /path/to/vector [RATE] [DURATION_SECONDS] [TABLES]
#        defaults: 500 changes/s for 300 s across 5 tables
#
# Seeds tables (snapshot input), generates a steady mix of inserts/updates/deletes, hard-kills
# Vector (SIGKILL) halfway through and restarts it, then verifies against MySQL that every change
# was delivered and that replaying the captured changes reproduces each table exactly.
# Requires: docker (with compose), jq, python3.
set -uo pipefail

VECTOR=${1:?usage: run-load.sh /path/to/vector [rate] [duration] [tables]}
VECTOR=$(cd "$(dirname "$VECTOR")" && pwd)/$(basename "$VECTOR")
# Rate 0 runs the snapshot benchmark instead: run-load.sh VECTOR 0 ROWS_PER_TABLE [TABLES] [THREADS] [CHUNK]
if [ "${2:-}" = "0" ]; then
  exec "$(dirname "$0")/run-snapshot-bench.sh" "$VECTOR" "${@:3}"
fi
# Rate 1 runs the production-readiness suite instead (run-readiness.sh); its report is copied to
# out/load-readiness-report.json so build tooling that collects out/load-* picks it up.
if [ "${2:-}" = "1" ]; then
  exec "$(dirname "$0")/run-readiness.sh" "$VECTOR"
fi
RATE=${2:-500}
DURATION=${3:-300}
TABLES=${4:-5}
SEED_ROWS=1000
cd "$(dirname "$0")"

for tool in docker jq python3; do
  command -v "$tool" >/dev/null || { echo "$tool is required"; exit 2; }
done

VPID=""
SAMPLER=""
log() { echo "[$(date +%T)] $*"; }

start_vector() {
  "$VECTOR" --config vector-load.yaml >>out/load-vector.log 2>&1 &
  VPID=$!
  echo "$VPID" > out/load-vector.pid
}

cleanup() {
  [ -n "$SAMPLER" ] && kill "$SAMPLER" 2>/dev/null
  [ -n "$VPID" ] && kill -TERM "$VPID" 2>/dev/null && wait "$VPID" 2>/dev/null
}
trap cleanup EXIT

# Samples Vector's resident memory and CPU use (per interval, not lifetime average) every 2 s.
sample_resources() {
  local hz prev_ticks="" prev_t=""
  hz=$(getconf CLK_TCK)
  echo "t,rss_kb,cpu_pct" > out/load-resources.csv
  while true; do
    local pid
    pid=$(cat out/load-vector.pid 2>/dev/null)
    if [ -n "$pid" ] && [ -r "/proc/$pid/stat" ]; then
      local ticks rss now
      ticks=$(awk '{print $14 + $15}' "/proc/$pid/stat")
      rss=$(awk '/VmRSS/ {print $2}' "/proc/$pid/status")
      now=$(date +%s.%N)
      if [ -n "$prev_ticks" ] && [ "$ticks" -ge "$prev_ticks" ]; then
        awk -v t="$now" -v pt="$prev_t" -v k="$ticks" -v pk="$prev_ticks" -v hz="$hz" -v rss="$rss" \
          'BEGIN { printf "%.1f,%d,%.1f\n", t, rss, (k - pk) / hz / (t - pt) * 100 }' >> out/load-resources.csv
      fi
      prev_ticks=$ticks
      prev_t=$now
    else
      prev_ticks=""
    fi
    sleep 2
  done
}

mysql_root() { docker exec -i cdc-mysql mysql -uroot -prootpw "$@" 2>>out/load-mysql.err; }

wait_for() { # wait_for <seconds> <description> <command...>
  local timeout=$1 desc=$2; shift 2
  for _ in $(seq 1 "$timeout"); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  log "timed out waiting for: $desc"
  return 1
}

log "Load test: $RATE changes/s for ${DURATION}s across $TABLES tables ($SEED_ROWS seed rows each)"
rm -rf out/load-* && mkdir -p out/load-data out/load-truth out/load-blobs
docker compose down -v >/dev/null 2>&1
docker compose up -d >/dev/null 2>&1 || { log "docker compose up failed"; exit 1; }
wait_for 120 "MySQL healthy" \
  bash -c '[ "$(docker inspect -f "{{.State.Health.Status}}" cdc-mysql)" = healthy ]' || exit 1
wait_for 60 "init.sql applied" bash -c "docker exec cdc-mysql mysql -uroot -prootpw -N -e 'SELECT 1 FROM inventory.customers LIMIT 1' 2>/dev/null | grep -q 1" || exit 1
# Public read access lets the verification fetch blobs with plain HTTP (local emulator only).
wait_for 60 "Azurite container" docker exec cdc-azcli az storage container create -n load --public-access container --only-show-errors || exit 1

log "Seeding tables"
python3 loadgen.py setup --tables "$TABLES" --seed-rows "$SEED_ROWS" | mysql_root

log "Starting Vector (snapshot)"
sample_resources &
SAMPLER=$!
start_vector
wait_for 120 "snapshot completed" grep -q "Snapshot completed" out/load-vector.log || exit 1

log "Generating load"
python3 -u loadgen.py run --tables "$TABLES" --rate "$RATE" --duration "$DURATION" \
  --seed-rows "$SEED_ROWS" --summary out/load-expected.json | mysql_root &
GEN=$!

sleep $(( DURATION / 2 ))
log "Hard-killing Vector (SIGKILL) mid-load"
kill -9 "$VPID" 2>/dev/null
wait "$VPID" 2>/dev/null
sleep 5
log "Restarting Vector"
start_vector

wait "$GEN"
log "Load finished: $(jq -c '{statements, rate_generated, duration_s}' out/load-expected.json)"

log "Waiting for the source to catch up with MySQL"
caught_up() {
  local executed stored
  executed=$(mysql_root -N -e "SELECT @@global.gtid_executed" | tr -d ' \n\r\\')
  stored=$(jq -r .gtid_set out/load-data/mysql/offsets.json 2>/dev/null | tr -d ' \n\r')
  [ -n "$executed" ] && [ "$executed" = "$stored" ]
}
if wait_for 300 "offsets to reach gtid_executed" caught_up; then
  log "Caught up"
else
  log "WARNING: source did not catch up within 300 s"
fi
log "Letting the sink drain its buffer"
sleep 30
kill -TERM "$VPID" 2>/dev/null
wait "$VPID" 2>/dev/null
VPID=""
kill "$SAMPLER" 2>/dev/null
SAMPLER=""

log "Dumping final table contents"
for i in $(seq 1 "$TABLES"); do
  mysql_root -N -e "SELECT id, val FROM loadtest.t$i ORDER BY id" > "out/load-truth/t$i.tsv"
done

log "Downloading blobs from Azurite"
# Fetched over plain HTTP (the container is public in the emulator), so the stored gzip bytes
# arrive unchanged; the Azure CLI's downloader decompresses and its progress bar can crash.
docker exec cdc-azcli az storage blob list -c load --num-results '*' --query '[].name' -o tsv \
  --only-show-errors 2>/dev/null | tr -d '\r' > out/load-blob-names.txt
listed=$(grep -c '^cdc/' out/load-blob-names.txt)
xargs -P 8 -I{} curl -sf --create-dirs -o "out/load-blobs/{}" "http://127.0.0.1:10000/devstoreaccount1/load/{}" \
  < out/load-blob-names.txt
downloaded=$(find out/load-blobs/cdc -type f 2>/dev/null | wc -l)
log "Data blobs: $downloaded downloaded of $listed in Azurite, $(du -sh out/load-blobs/cdc 2>/dev/null | cut -f1)"
if [ "$downloaded" != "$listed" ]; then
  log "ERROR: not every blob was downloaded; the verification below would be meaningless"
  exit 1
fi
find out/load-blobs/_schemas -type f -name '*.json' -exec cat {} + 2>/dev/null | jq -s . > out/load-schemas.json
log "Schema documents: $(jq length out/load-schemas.json) (copied to load-schemas.json)"

if [ -s out/load-mysql.err ] && grep -qv "Using a password" out/load-mysql.err; then
  log "MySQL client reported errors:"; grep -v "Using a password" out/load-mysql.err | head -5
fi

log "Verifying"
python3 verify_load.py --blobs out/load-blobs/cdc --expected out/load-expected.json \
  --truth out/load-truth --resources out/load-resources.csv --report out/load-report.json
verified=$?
errors=$(grep -c " ERROR " out/load-vector.log)
log "Vector logged $errors error line(s)"
grep " ERROR " out/load-vector.log | cut -c1-200 | head -5
[ "$verified" -eq 0 ] && [ "$errors" -eq 0 ]
