#!/usr/bin/env python3
"""Verifies a snapshot benchmark: per table row count, sum(id), sum(val) and snapshot markers,
compared with MySQL. Streams the gzip NDJSON files in parallel (one process per file)."""

import argparse
import gzip
import json
import os
import sys
from multiprocessing import Pool


def scan(path):
    count = sum_id = sum_val = 0
    markers = {}
    with gzip.open(path, "rt") as f:
        for line in f:
            row = json.loads(line)
            record = row["record"]
            count += 1
            sum_id += record["id"]
            sum_val += record["val"]
            marker = row["metadata"]["snapshot"]
            if marker != "true":
                markers[marker] = markers.get(marker, 0) + 1
    return os.path.basename(path).split(".")[0], count, sum_id, sum_val, markers


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output", required=True)
    ap.add_argument("--truth", required=True, help="TSV: table count sum_id sum_val")
    ap.add_argument("--metrics", required=True, help="JSON with timings and resources")
    ap.add_argument("--report", required=True)
    args = ap.parse_args()

    files = sorted(
        os.path.join(args.output, f) for f in os.listdir(args.output) if f.endswith(".ndjson.gz")
    )
    with Pool(min(len(files), os.cpu_count() or 1) or 1) as pool:
        results = pool.map(scan, files)

    truth = {}
    for line in open(args.truth):
        if line.strip():
            table, count, sum_id, sum_val = line.split()
            truth[table] = (int(count), int(sum_id), int(sum_val))

    failures = []
    seen = {}
    markers = {}
    for table, count, sum_id, sum_val, m in results:
        seen[table] = (count, sum_id, sum_val)
        for k, v in m.items():
            markers[k] = markers.get(k, 0) + v
    for table, expected in truth.items():
        got = seen.get(table)
        if got != expected:
            failures.append(f"{table}: expected (rows, sum id, sum val) {expected}, got {got}")
    tables = len(truth)
    if markers.get("first", 0) != 1 or markers.get("last", 0) != 1:
        failures.append(f"expected exactly one 'first' and one 'last' marker, got {markers}")
    if markers.get("first", 0) + markers.get("first_in_data_collection", 0) != tables:
        failures.append(f"expected {tables} first-of-table markers, got {markers}")
    if markers.get("last", 0) + markers.get("last_in_data_collection", 0) != tables:
        failures.append(f"expected {tables} last-of-table markers, got {markers}")
    control_path = os.path.join(args.output, "_control.ndjson")
    completed = []
    if os.path.exists(control_path):
        completed = [json.loads(l) for l in open(control_path) if l.strip()]
        completed = [c for c in completed if c.get("type") == "snapshot_completed"]
    if len(completed) != tables:
        failures.append(f"expected {tables} snapshot_completed events, got {len(completed)}")
    elif sum(c["rows"] for c in completed) != sum(t[0] for t in truth.values()):
        failures.append(f"snapshot_completed row counts {sum(c['rows'] for c in completed)} != rows")
    elif any(c.get("started_seq") is None for c in completed):
        failures.append("snapshot_completed without started_seq")

    metrics = json.load(open(args.metrics))
    rows = sum(c for c, _, _ in truth.values())
    snap_s = metrics.get("snapshot_seconds") or 0
    report = {
        "result": "PASS" if not failures else "FAIL",
        "failures": failures,
        "rows": rows,
        "tables": tables,
        "threads": metrics.get("threads"),
        "chunk_size": metrics.get("chunk_size"),
        "seed_seconds": metrics.get("seed_seconds"),
        "snapshot_seconds": snap_s,
        "rows_per_second": round(rows / snap_s) if snap_s else None,
        "vector_total_seconds": metrics.get("vector_seconds"),
        "output_gzip_bytes": sum(os.path.getsize(f) for f in files),
        "vector_rss_mb_max": metrics.get("rss_mb_max"),
        "vector_cpu_pct_avg": metrics.get("cpu_pct_avg"),
        "vector_cpu_pct_max": metrics.get("cpu_pct_max"),
        "host": metrics.get("host"),
    }
    with open(args.report, "w") as f:
        json.dump(report, f, indent=2)
    print(json.dumps(report, indent=2))
    sys.exit(0 if not failures else 1)


if __name__ == "__main__":
    main()
