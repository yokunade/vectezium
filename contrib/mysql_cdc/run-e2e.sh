#!/usr/bin/env bash
# End-to-end test for the mysql_cdc source.
#
# Usage: ./run-e2e.sh /path/to/vector
#
# Requires: docker (with compose), jq.
# Starts MySQL + Azurite, runs Vector with vector-e2e.yaml and checks snapshot, streaming,
# restart/resume, filtering and the blobs written to Azurite.
set -uo pipefail

VECTOR=${1:?usage: run-e2e.sh /path/to/vector}
VECTOR=$(cd "$(dirname "$VECTOR")" && pwd)/$(basename "$VECTOR")
cd "$(dirname "$0")"

command -v jq >/dev/null || { echo "jq is required"; exit 2; }

PASS=0
FAIL=0
VPID=""

ok()   { echo "  PASS  $*"; PASS=$((PASS + 1)); }
bad()  { echo "  FAIL  $*"; FAIL=$((FAIL + 1)); }
check() { # check "description" <command...>
  local desc=$1; shift
  if "$@" >/dev/null 2>&1; then ok "$desc"; else bad "$desc"; fi
}

sql() { docker exec -i cdc-mysql mysql --default-character-set=utf8mb4 -uroot -prootpw inventory -N -e "$1" 2>/dev/null; }

start_vector() {
  "$VECTOR" --config vector-e2e.yaml >>out/vector.log 2>&1 &
  VPID=$!
}

stop_vector() {
  if [ -n "$VPID" ]; then
    kill -TERM "$VPID" 2>/dev/null
    wait "$VPID" 2>/dev/null
    VPID=""
  fi
}

cleanup() { stop_vector; }
trap cleanup EXIT

# wait_for <seconds> <description> <command...>
wait_for() {
  local timeout=$1 desc=$2; shift 2
  for _ in $(seq 1 "$timeout"); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo "  timed out waiting for: $desc"
  return 1
}

raw() { cat out/raw.ndjson 2>/dev/null; }
count() { raw | jq -s "[.[] | select($1)] | length"; }
has_at_least() { [ "$(count "$1")" -ge "$2" ]; }

echo "==> Starting MySQL and Azurite"
rm -rf out && mkdir -p out/data
docker compose down -v >/dev/null 2>&1
docker compose up -d || exit 1
wait_for 120 "MySQL healthy" \
  bash -c '[ "$(docker inspect -f "{{.State.Health.Status}}" cdc-mysql)" = healthy ]' || exit 1
# The entrypoint restarts mysqld after running init.sql; make sure the data is there.
wait_for 60 "init.sql applied" bash -c "docker exec cdc-mysql mysql -uroot -prootpw -N -e 'SELECT COUNT(*) FROM inventory.customers' 2>/dev/null | grep -q 3" || exit 1
wait_for 60 "Azurite container created" \
  docker exec cdc-azcli az storage container create -n cdc --only-show-errors || exit 1

echo "==> Snapshot"
start_vector
wait_for 60 "3 snapshot events" has_at_least '.op == "r"' 3

check "3 snapshot read events"            test "$(count '.op == "r"')" -eq 3
check "first event marked first"          test "$(raw | head -1 | jq -r .source.snapshot)" = first
check "last event marked last"            test "$(count '.op == "r" and .source.snapshot == "last"')" -eq 1
check "excluded table not captured"       test "$(count '.source.table == "audit_log"')" -eq 0
check "excluded column removed"           test "$(count '.after.ssn != null')" -eq 0
A='select(.op == "r" and .after.id == 1) | .after'
check "decimal as string"                 test "$(raw | jq -r "$A | .balance")" = "1234.50"
check "float shortest form"               test "$(raw | jq -r "$A | .ratio")" = "0.1"
check "bigint unsigned wraps like Debezium" test "$(raw | jq -r "$A | .big")" = "-1"
check "bit(1) as boolean"                 test "$(raw | jq -r "$A | .active")" = "true"
check "enum as string"                    test "$(raw | jq -r "$A | .status")" = "active"
check "set as string"                     test "$(raw | jq -r "$A | .tags")" = "a,c"
json_is_gold() { raw | jq -r "$A | .profile" | jq -e '.tier == "gold"'; }
check "json as string"                    json_is_gold
check "date as epoch days"                test "$(raw | jq -r "$A | .born")" = "7441"
check "time(3) as microseconds"           test "$(raw | jq -r "$A | .wake_up")" = "27000250000"
check "datetime(6) as epoch micros"       test "$(raw | jq -r "$A | .created")" = "1704110400123456"
check "timestamp(3) as ISO string"        test "$(raw | jq -r "$A | .updated")" = "2024-01-01T12:00:00.500Z"
check "latin1 text decoded"               test "$(raw | jq -r "$A | .latin")" = "café"
check "varbinary as base64"               test "$(raw | jq -r "$A | .avatar")" = "3q2+7w=="
check "geometry srid"                     test "$(raw | jq -r "$A | .location.srid")" = "4326"
check "negative time"                     test "$(raw | jq -r 'select(.op == "r" and .after.id == 3) | .after.wake_up')" = "-43200000000"

echo "==> Streaming"
sql "INSERT INTO customers (id, name, balance) VALUES (4, 'Dave', 10.00);"
sql "UPDATE customers SET name = 'Alicia' WHERE id = 1;"
sql "UPDATE customers SET id = 20 WHERE id = 2;"
sql "DELETE FROM customers WHERE id = 3;"
sql "START TRANSACTION; INSERT INTO customers (id, name) VALUES (99, 'Ghost'); ROLLBACK;"
sql "START TRANSACTION; INSERT INTO customers (id, name) VALUES (6, 'Eve'); INSERT INTO customers (id, name) VALUES (7, 'Frank'); COMMIT;"
sql "INSERT INTO audit_log (note) VALUES ('still ignored');"
sql "UPDATE customers SET latin = 'über' WHERE id = 4;"
sql "ALTER TABLE customers ADD COLUMN nickname VARCHAR(20);"
sql "INSERT INTO customers (id, name, nickname) VALUES (8, 'Gina', 'gg');"
wait_for 30 "insert after DDL" has_at_least '.op == "c" and .after.id == 8' 1

check "insert captured"                   test "$(count '.op == "c" and .after.id == 4')" -eq 1
check "update has before and after"       test "$(count '.op == "u" and .before.name == "Alice" and .after.name == "Alicia"')" -eq 1
check "pk change -> delete old key"       test "$(count '.op == "d" and .before.id == 2')" -eq 1
check "pk change -> create new key"       test "$(count '.op == "c" and .after.id == 20')" -eq 1
check "delete captured"                   test "$(count '.op == "d" and .before.id == 3 and .after == null')" -eq 1
check "rolled-back insert not emitted"    test "$(count '.after.id == 99')" -eq 0
one_gtid() { [ "$(raw | jq -s '[.[] | select(.after.id == 6 or .after.id == 7) | .source.gtid] | unique | length')" -eq 1 ]; }
check "multi-row tx shares one GTID"      one_gtid
check "streamed events have gtid/file/pos" test "$(count '.op == "c" and .after.id == 4 and .source.gtid != null and .source.pos > 0')" -eq 1
check "source.query populated"            test "$(count '.after.id == 4 and (.source.query | test("INSERT"))')" -eq 1
check "latin1 text decoded (binlog)"      test "$(count '.op == "u" and .after.id == 4 and .after.latin == "über"')" -eq 1
check "new column after DDL"              test "$(count '.after.id == 8 and .after.nickname == "gg"')" -eq 1
check "filtered table still ignored"      test "$(count '.source.table == "audit_log"')" -eq 0
check "DDL emitted on the control output" grep -q 'ADD COLUMN nickname' out/ddl.ndjson
check "every row event has a seq"         test "$(count '.source.seq == null')" -eq 0

echo "==> Restart and resume"
sleep 3   # let the last acknowledgements land
stop_vector
check "offset file written"               test -s out/data/mysql/offsets.json
check "offset marks snapshot complete"    test "$(jq -r .snapshot_completed out/data/mysql/offsets.json)" = true
before_restart=$(raw | wc -l)
sql "INSERT INTO customers (id, name) VALUES (5, 'While-Down');"
start_vector
wait_for 60 "change made while stopped" has_at_least '.op == "c" and .after.id == 5' 1
check "change made while stopped is captured" test "$(count '.op == "c" and .after.id == 5')" -eq 1
check "no re-snapshot after restart"      test "$(count '.op == "r"')" -eq 3
dupes=$(raw | jq -s '[.[] | select(.op != "r") | "\(.source.gtid)/\(.source.pos)/\(.source.row)/\(.op)"] | group_by(.) | map(select(length > 1)) | length')
if [ "$dupes" -eq 0 ]; then ok "no duplicates across restart"; else echo "  NOTE  $dupes duplicated change(s) across restart (allowed: at-least-once)"; fi
echo "        events before restart: $before_restart, after: $(raw | wc -l)"

echo "==> ADLS (Azurite, metadata + record format)"
sleep 5   # sink batch timeout
blobs=$(docker exec cdc-azcli az storage blob list -c cdc --prefix mysql/inventory/customers/ --query "[].name" -o tsv --only-show-errors 2>/dev/null | tr -d '\r')
if [ -n "$blobs" ]; then ok "blobs written under mysql/inventory/customers/"; else bad "no blobs written to Azurite"; fi
echo "$blobs" | head -3 | sed 's/^/        /'
all_json_gz() { ! echo "$blobs" | grep -qv '\.json\.gz$'; }
check "blob_extension json.gz used"       all_json_gz

# Download every blob and concatenate the rows (the Azure CLI may transparently gunzip).
: > out/blobs.ndjson
for b in $blobs; do
  docker exec cdc-azcli az storage blob download -c cdc -n "$b" -f /tmp/b --overwrite --only-show-errors >/dev/null 2>&1
  docker exec cdc-azcli sh -c 'gzip -dcf /tmp/b' >> out/blobs.ndjson 2>/dev/null
done
blob() { jq -s "[.[] | select($1)] | length" out/blobs.ndjson; }
head -1 out/blobs.ndjson | cut -c1-240 | sed 's/^/        /'

only_envelope_keys() { [ "$(jq -s '[.[] | keys] | unique | length' out/blobs.ndjson)" -eq 1 ] && [ "$(head -1 out/blobs.ndjson | jq -c 'keys')" = '["metadata","record"]' ] && [ "$(head -1 out/blobs.ndjson | jq -c '.record | keys')" = '["after","before"]' ]; }
check "rows contain only metadata and record (before, after)" only_envelope_keys
check "snapshot row: op r, before null"   test "$(blob '.metadata.op == "r" and .record.after.id == 1 and .record.before == null')" -eq 1
check "update row has before and after"   test "$(blob '.metadata.op == "u" and .record.after.name == "Alicia" and .record.before.name == "Alice"')" -eq 1
check "delete row: deleted row, key in after"test "$(blob '.metadata.op == "d" and .metadata.deleted == true and .record.before.id == 3 and .record.after == {"id": 3}')" -eq 1
check "metadata carries key and position" test "$(blob '.metadata.op == "c" and .record.after.id == 4 and .metadata.key.id == 4 and .metadata.pos > 0 and .metadata.gtid != null')" -eq 1
check "excluded column absent from record" test "$(blob '(.record.after.ssn // .record.before.ssn) != null')" -eq 0
blob_rows=$(wc -l < out/blobs.ndjson | tr -d ' ')
raw_rows=$(count '.source.table == "customers"')
echo "        rows in blobs: $blob_rows, change events captured (debezium source): $raw_rows"
check "every change reached ADLS"         test "$blob_rows" -ge "$raw_rows"

echo "==> Table schemas"
schema_blobs=$(docker exec cdc-azcli az storage blob list -c cdc --prefix _schemas/inventory/customers/ --query "[].name" -o tsv --only-show-errors 2>/dev/null | tr -d '\r')
echo "$schema_blobs" | sed 's/^/        /'
: > out/schemas.ndjson
for b in $schema_blobs; do
  docker exec cdc-azcli az storage blob download -c cdc -n "$b" -f /tmp/s --overwrite --only-show-errors >/dev/null 2>&1
  docker exec cdc-azcli cat /tmp/s >> out/schemas.ndjson 2>/dev/null
done
n_versions=$(echo "$schema_blobs" | grep -c .)
# Two definitions of `customers`: before and after `ADD COLUMN nickname`.
check "two schema versions (before/after DDL)" test "$n_versions" -eq 2
names_are_versions() { echo "$schema_blobs" | grep -Eq '/[0-9a-f]{16}[.]json$'; }
check "schema blobs named <version>.json"  names_are_versions
versions_in_rows=$(jq -r '.metadata.schema_version' out/blobs.ndjson | sort -u | grep -c .)
check "rows reference exactly those versions" test "$versions_in_rows" -eq "$n_versions"
every_row_version_documented() {
  comm -23 <(jq -r '.metadata.schema_version' out/blobs.ndjson | sort -u) <(jq -r '.schema_version' out/schemas.ndjson | sort -u) | grep -q . && return 1 || return 0
}
check "every row's version has a document" every_row_version_documented
snap_v=$(jq -r 'select(.metadata.op == "r") | .metadata.schema_version' out/blobs.ndjson | sort -u)
pre_ddl_v=$(jq -r 'select(.metadata.op == "c" and .record.after.id == 4) | .metadata.schema_version' out/blobs.ndjson)
check "snapshot and binlog agree on version" test "$snap_v" = "$pre_ddl_v"
S='select(.columns | map(.name) | index("nickname") | not)'
col() { jq -r "$S | .columns[] | select(.name == \"$1\") | $2" out/schemas.ndjson; }
check "pk recorded"                       test "$(jq -c "$S | .primary_key" out/schemas.ndjson)" = '["id"]'
check "decimal precision/scale"           test "$(col balance '"\(.precision),\(.scale),\(.logical_type)"')" = "12,2,decimal"
check "datetime(6) -> timestamp_micros_local" test "$(col created .logical_type)" = "timestamp_micros_local"
check "timestamp -> iso8601 utc"          test "$(col updated .logical_type)" = "timestamp_iso8601_utc"
check "bigint unsigned flagged"           test "$(col big .unsigned)" = "true"
check "enum values listed"                test "$(col status '.values | join(",")')" = "new,active,closed"
check "varbinary -> bytes/base64"         test "$(col avatar '"\(.mysql_type),\(.logical_type),\(.encoding)"')" = "varbinary,bytes,base64"
check "not-null pk, nullable others"      test "$(col id .nullable),$(col email .nullable)" = "false,true"
check "excluded column not in schema"     test -z "$(col ssn .name)"

echo "==> Single-writer lock"
# A second instance with the same server_id, offsets and lock must wait while the first runs,
# take over when it stops, and resume from its offset; a crashed holder blocks only until its
# lease expires.
rawb() { cat out/raw-b.ndjson 2>/dev/null; }
countb() { rawb | jq -s "[.[] | select($1)] | length"; }
check "first instance holds the lock"     grep -q 'Acquired the single-writer lock' out/vector.log
"$VECTOR" --config vector-e2e-lock.yaml >>out/vector-b.log 2>&1 &
BPID=$!
wait_for 30 "second instance waits" grep -q 'Another instance holds the single-writer lock' out/vector-b.log
check "second instance waits for the lock" grep -q 'Another instance holds the single-writer lock' out/vector-b.log
sql "INSERT INTO customers (id, name) VALUES (30, 'While-Locked');"
wait_for 30 "first instance captures" has_at_least '.op == "c" and .after.id == 30' 1
sleep 3
check "second instance never connected"   bash -c "! grep -q 'Connected to MySQL' out/vector-b.log"
check "second instance emitted nothing"   test "$(rawb | wc -l)" -eq 0
sleep 2   # let the first instance store its offset
stop_vector
check "first instance released the lock"  grep -q 'Released the single-writer lock' out/vector.log
wait_for 20 "second instance takes over" grep -q 'Acquired the single-writer lock' out/vector-b.log
check "second instance takes over on release" grep -q 'Acquired the single-writer lock' out/vector-b.log
sql "INSERT INTO customers (id, name) VALUES (31, 'After-Takeover');"
wait_for 30 "second instance captures" bash -c "[ \"\$(cat out/raw-b.ndjson 2>/dev/null | jq -s '[.[] | select(.after.id == 31)] | length')\" -ge 1 ]"
check "second instance captures new changes" test "$(countb '.op == "c" and .after.id == 31')" -eq 1
check "second instance resumed (no snapshot, no replay of id 30)" test "$(countb '.op == "r" or .after.id == 30')" -eq 0
sleep 2
kill -KILL "$BPID" 2>/dev/null; wait "$BPID" 2>/dev/null
lock_wait_start=$(date +%s)
before_locks=$(grep -c 'Acquired the single-writer lock' out/vector.log)
start_vector
wait_for 40 "lease of the crashed instance expires" bash -c "[ \$(grep -c 'Acquired the single-writer lock' out/vector.log) -gt $before_locks ]"
waited=$(( $(date +%s) - lock_wait_start ))
echo "        took over from a crashed holder after ${waited}s (lease 15 s)"
check "takes over after a crashed holder's lease expires" test "$waited" -le 30
check "waited for the crashed holder's lease"   test "$waited" -ge 5

stop_vector
echo
echo "Result: $PASS passed, $FAIL failed (logs: out/vector.log)"
[ "$FAIL" -eq 0 ]
