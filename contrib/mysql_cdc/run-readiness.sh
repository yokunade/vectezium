#!/usr/bin/env bash
# Production-readiness suite for the mysql_cdc source (Vectezium).
#
# Usage: ./run-readiness.sh /path/to/vector [--only 1.1,2.1] [--skip-load] [--soak-hours N]
#
# Starts a fresh MySQL + Azurite (docker compose), runs readiness.py (tests 1.1-7.3, see that
# file), then run-load.sh for 6.1 (SIGKILL under 500 changes/s) and optionally a soak run, and
# prints a scorecard. Results: out/readiness/report.json, per-test logs in out/readiness/<id>/.
# Requires: docker (with compose), python3, openssl, jq, curl.
set -uo pipefail

VECTOR=${1:?usage: run-readiness.sh /path/to/vector [--only ids] [--skip-load] [--soak-hours N]}
VECTOR=$(cd "$(dirname "$VECTOR")" && pwd)/$(basename "$VECTOR")
shift
ONLY=""
SKIP_LOAD=0
SOAK_HOURS=0
while [ $# -gt 0 ]; do
  case "$1" in
    --only) ONLY=$2; shift 2 ;;
    --skip-load) SKIP_LOAD=1; shift ;;
    --soak-hours) SOAK_HOURS=$2; shift 2 ;;
    *) echo "unknown option: $1"; exit 2 ;;
  esac
done
cd "$(dirname "$0")"

for tool in docker python3 openssl jq curl; do
  command -v "$tool" >/dev/null || { echo "$tool is required"; exit 2; }
done
log() { echo "[$(date +%T)] $*"; }

log "Starting a fresh MySQL + Azurite"
rm -rf out/readiness && mkdir -p out/readiness
docker compose down -v >/dev/null 2>&1
docker compose up -d >/dev/null 2>&1 || { log "docker compose up failed"; exit 1; }
for _ in $(seq 1 120); do
  [ "$(docker inspect -f '{{.State.Health.Status}}' cdc-mysql 2>/dev/null)" = healthy ] &&
    docker exec cdc-mysql mysql -uroot -prootpw -N -e 'SELECT 1 FROM inventory.customers LIMIT 1' 2>/dev/null | grep -q 1 &&
    break
  sleep 1
done

python3 -u readiness.py "$VECTOR" ${ONLY:+--only "$ONLY"}

load_args=()
if [ "$SKIP_LOAD" = 0 ] && [ -z "$ONLY" ]; then
  log "Test 6.1: SIGKILL under 500 changes/s (run-load.sh, 120 s)"
  ./run-load.sh "$VECTOR" 500 120 5 > out/readiness/6.1-load.log 2>&1
  rc=$?
  tail -15 out/readiness/6.1-load.log
  load_args+=(--load-rc "$rc")
fi
if [ "$SOAK_HOURS" -gt 0 ]; then
  log "Test 7.4: soak for ${SOAK_HOURS} h at 500 changes/s"
  ./run-load.sh "$VECTOR" 500 $(( SOAK_HOURS * 3600 )) 5 > out/readiness/7.4-soak.log 2>&1
  rc=$?
  tail -15 out/readiness/7.4-soak.log
  load_args+=(--soak-rc "$rc")
fi

python3 readiness.py --summary out/readiness/report.json "${load_args[@]}" | tee out/readiness/scorecard.txt
rc=${PIPESTATUS[0]}
cp -f out/readiness/report.json out/load-readiness-report.json
exit "$rc"
