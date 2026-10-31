#!/usr/bin/env python3
"""Verifies a mysql_cdc load test.

  verify_load.py --blobs DIR --expected load-expected.json --truth DIR --resources res.csv

* reads every blob written by the source (`event_format: record`, gzip or plain NDJSON);
* removes duplicates (at-least-once delivery) using the documented key;
* checks the number of unique changes per table and operation against what was generated;
* replays snapshot rows + changes in binlog order and compares the result with the tables'
  actual final contents (`truth` holds `<table>.tsv` dumps of `id<TAB>val`);
* reports capture lag (commit -> captured), throughput and Vector's resource use.
"""

import argparse
import gzip
import json
import os
import statistics
import sys


def read_rows(directory):
    for root, _, files in os.walk(directory):
        for name in sorted(files):
            path = os.path.join(root, name)
            with open(path, "rb") as f:
                data = f.read()
            if data[:2] == b"\x1f\x8b":
                data = gzip.decompress(data)
            for line in data.splitlines():
                if line.strip():
                    yield json.loads(line)


def pct(values, p):
    if not values:
        return None
    values = sorted(values)
    k = min(len(values) - 1, max(0, int(round(p / 100 * (len(values) - 1)))))
    return values[k]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--blobs", required=True)
    ap.add_argument("--expected", required=True)
    ap.add_argument("--truth", required=True)
    ap.add_argument("--resources")
    ap.add_argument("--report", default="load-report.json")
    args = ap.parse_args()

    expected = json.load(open(args.expected))
    failures = []

    raw = 0
    unique = {}
    ends = {}
    snapshot_ids = set()
    seq_of = {}
    seq_conflicts = 0
    for row in read_rows(args.blobs):
        if "type" in row:  # a control event
            if row["type"] == "snapshot_completed" and row.get("db") == "loadtest":
                ends.setdefault(row["table"], row)
            continue
        m = row["metadata"]
        if m.get("db") != "loadtest":
            continue
        # Replayed changes must carry the same seq as their first delivery. Snapshot reads need
        # not: a snapshot read again after a crash numbers its rows afresh (README, Duplicates).
        if m["op"] != "r":
            ident = (m["table"], m["file"], m["pos"], m["row"], m["op"])
            if seq_of.setdefault(ident, m.get("seq")) != m.get("seq"):
                seq_conflicts += 1
        if m["op"] == "r":
            snapshot_ids.add(m.get("snapshot_id"))
        raw += 1
        if m["op"] == "r":
            key = ("r", m["table"], row["record"]["id"])
        else:
            key = (m["table"], m["file"], m["pos"], m["row"], m["op"])
        unique.setdefault(key, row)

    rows = list(unique.values())
    duplicates = raw - len(rows)

    # --- counts per table and operation ---
    counts = {}
    for row in rows:
        m = row["metadata"]
        counts.setdefault(m["table"], {"r": 0, "c": 0, "u": 0, "d": 0})[m["op"]] += 1
    for table, exp in expected["counts"].items():
        got = counts.get(table, {"r": 0, "c": 0, "u": 0, "d": 0})
        for op in ("c", "u", "d"):
            if got[op] != exp[op]:
                failures.append(f"{table}: expected {exp[op]} '{op}' events, got {got[op]}")
        if got["r"] != expected["seed_rows"]:
            failures.append(f"{table}: expected {expected['seed_rows']} snapshot rows, got {got['r']}")

    # --- snapshot markers: one per table, row count and snapshot id agree with the rows ---
    for table in expected["counts"]:
        end = ends.get(table)
        if end is None:
            failures.append(f"{table}: no snapshot_completed event")
        elif end.get("rows") != expected["seed_rows"]:
            failures.append(f"{table}: snapshot_completed says {end.get('rows')} rows, expected {expected['seed_rows']}")
    if len(snapshot_ids) != 1 or None in snapshot_ids or {e.get("snapshot_id") for e in ends.values()} != snapshot_ids:
        failures.append(f"snapshot ids disagree: rows {snapshot_ids}, markers {[e.get('snapshot_id') for e in ends.values()]}")

    if seq_conflicts:
        failures.append(f"{seq_conflicts} replayed changes got a different seq than their first delivery")
    if any(r["metadata"].get("seq") is None for r in rows):
        failures.append("events without metadata.seq")

    # --- replay and compare with the real table contents ---
    def binlog_order(row):
        m = row["metadata"]
        return (m["file"], m["pos"], m["row"])

    state = {}
    for row in rows:
        if row["metadata"]["op"] == "r":
            state.setdefault(row["metadata"]["table"], {})[row["record"]["id"]] = row["record"]["val"]
    changes = sorted((r for r in rows if r["metadata"]["op"] != "r"), key=binlog_order)
    for row in changes:
        m = row["metadata"]
        table = state.setdefault(m["table"], {})
        if m["op"] == "d":
            table.pop(row["record"]["id"], None)
        else:
            table[row["record"]["id"]] = row["record"]["val"]

    for table in expected["counts"]:
        truth = {}
        with open(os.path.join(args.truth, f"{table}.tsv")) as f:
            for line in f:
                if line.strip():
                    i, v = line.split("\t")
                    truth[int(i)] = int(v)
        replayed = state.get(table, {})
        if replayed != truth:
            missing = len(truth.keys() - replayed.keys())
            extra = len(replayed.keys() - truth.keys())
            wrong = sum(1 for k in truth.keys() & replayed.keys() if truth[k] != replayed[k])
            failures.append(
                f"{table}: replayed state differs from MySQL (missing {missing}, extra {extra}, "
                f"wrong values {wrong})"
            )

    # --- lag and throughput ---
    lags = [
        r["metadata"]["captured_ts_ms"] - r["metadata"]["commit_ts_ms"]
        for r in changes
    ]
    captured = [r["metadata"]["captured_ts_ms"] for r in changes]
    span_s = (max(captured) - min(captured)) / 1000 if captured else 0

    resources = []
    if args.resources and os.path.exists(args.resources):
        for line in open(args.resources):
            parts = line.split(",")
            if len(parts) == 3 and parts[1].strip().isdigit():
                resources.append((float(parts[0]), int(parts[1]) / 1024, float(parts[2])))

    report = {
        "result": "PASS" if not failures else "FAIL",
        "failures": failures,
        "generated": {
            "statements": expected["statements"],
            "rate_target_per_s": expected["rate_target"],
            "rate_generated_per_s": expected["rate_generated"],
            "duration_s": expected["duration_s"],
            "tables": expected["tables"],
        },
        "delivered": {
            "rows_in_blobs": raw,
            "unique_changes": len(changes),
            "snapshot_rows": len(rows) - len(changes),
            "duplicates_after_crash": duplicates,
            "capture_rate_per_s": round(len(changes) / span_s, 1) if span_s else None,
        },
        "capture_lag_ms": {
            "p50": pct(lags, 50),
            "p95": pct(lags, 95),
            "p99": pct(lags, 99),
            "max": max(lags) if lags else None,
            "mean": round(statistics.mean(lags), 1) if lags else None,
        },
        "vector_resources": {
            "rss_mb_max": round(max(r[1] for r in resources), 1) if resources else None,
            "rss_mb_avg": round(statistics.mean(r[1] for r in resources), 1) if resources else None,
            "cpu_pct_avg": round(statistics.mean(r[2] for r in resources), 1) if resources else None,
            "cpu_pct_max": round(max(r[2] for r in resources), 1) if resources else None,
        },
    }
    with open(args.report, "w") as f:
        json.dump(report, f, indent=2)
    print(json.dumps(report, indent=2))
    sys.exit(0 if not failures else 1)


if __name__ == "__main__":
    main()
