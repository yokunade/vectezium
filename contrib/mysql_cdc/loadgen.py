#!/usr/bin/env python3
"""Load generator for the mysql_cdc load test.

Writes SQL to stdout (pipe it into the `mysql` client) at a fixed rate of row changes.

  loadgen.py setup --tables 5 --seed-rows 1000      # schema + seed rows (snapshot input)
  loadgen.py run --tables 5 --rate 500 --duration 300 --summary out/load-expected.json

Every statement changes exactly one row, so the number of statements per table and operation
is exactly the number of change events the source must deliver.
"""

import argparse
import json
import random
import sys
import time

DB = "loadtest"


def table_name(i):
    return f"t{i + 1}"


def setup(args):
    out = sys.stdout
    out.write(f"DROP DATABASE IF EXISTS {DB};\nCREATE DATABASE {DB};\nUSE {DB};\n")
    for t in range(args.tables):
        out.write(
            f"CREATE TABLE {table_name(t)} ("
            "id BIGINT PRIMARY KEY, val INT NOT NULL, note VARCHAR(64), "
            "amount DECIMAL(12,2), status ENUM('new','paid','shipped'), updated DATETIME(6));\n"
        )
        rows = []
        for i in range(1, args.seed_rows + 1):
            rows.append(f"({i},{i},'seed {i}',{i}.25,'new',NOW(6))")
            if len(rows) == 500:
                out.write(f"INSERT INTO {table_name(t)} VALUES {','.join(rows)};\n")
                rows = []
        if rows:
            out.write(f"INSERT INTO {table_name(t)} VALUES {','.join(rows)};\n")


def run(args):
    rng = random.Random(args.seed)
    out = sys.stdout
    out.write(f"USE {DB};\n")
    live = [list(range(1, args.seed_rows + 1)) for _ in range(args.tables)]
    next_id = [args.seed_rows + 1] * args.tables
    counts = {table_name(t): {"c": 0, "u": 0, "d": 0} for t in range(args.tables)}
    statuses = ["new", "paid", "shipped"]

    tick = 0.05
    per_tick = args.rate * tick
    carry = 0.0
    start = time.monotonic()
    deadline = start + args.duration
    total = 0
    in_tx = 0  # statements left in the current explicit transaction

    while True:
        now = time.monotonic()
        if now >= deadline:
            break
        carry += per_tick
        n = int(carry)
        carry -= n
        lines = []
        for _ in range(n):
            t = rng.randrange(args.tables)
            name = table_name(t)
            if in_tx == 0 and rng.random() < 0.3:
                in_tx = rng.randint(2, 5)
                lines.append("BEGIN;")
            r = rng.random()
            if r < 0.5 or len(live[t]) < 100:
                i = next_id[t]
                next_id[t] += 1
                live[t].append(i)
                lines.append(
                    f"INSERT INTO {name} VALUES ({i},{rng.randrange(10**6)},'row {i}',"
                    f"{rng.randrange(10**6)}.{rng.randrange(100):02d},'{rng.choice(statuses)}',NOW(6));"
                )
                counts[name]["c"] += 1
            elif r < 0.85:
                i = live[t][rng.randrange(len(live[t]))]
                # val always changes, so every UPDATE produces exactly one row event.
                lines.append(
                    f"UPDATE {name} SET val = val + 1, status = '{rng.choice(statuses)}', "
                    f"updated = NOW(6) WHERE id = {i};"
                )
                counts[name]["u"] += 1
            else:
                k = rng.randrange(len(live[t]))
                i = live[t][k]
                live[t][k] = live[t][-1]
                live[t].pop()
                lines.append(f"DELETE FROM {name} WHERE id = {i};")
                counts[name]["d"] += 1
            total += 1
            if in_tx:
                in_tx -= 1
                if in_tx == 0:
                    lines.append("COMMIT;")
        if lines:
            out.write("\n".join(lines) + "\n")
            out.flush()
        sleep = tick - (time.monotonic() - now)
        if sleep > 0:
            time.sleep(sleep)

    if in_tx:
        out.write("COMMIT;\n")
    out.flush()
    elapsed = time.monotonic() - start
    with open(args.summary, "w") as f:
        json.dump(
            {
                "tables": args.tables,
                "rate_target": args.rate,
                "duration_s": round(elapsed, 1),
                "statements": total,
                "rate_generated": round(total / elapsed, 1),
                "seed_rows": args.seed_rows,
                "counts": counts,
            },
            f,
            indent=2,
        )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("mode", choices=["setup", "run"])
    p.add_argument("--tables", type=int, default=5)
    p.add_argument("--seed-rows", type=int, default=1000)
    p.add_argument("--rate", type=float, default=500)
    p.add_argument("--duration", type=float, default=300)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--summary", default="load-expected.json")
    args = p.parse_args()
    if args.mode == "setup":
        setup(args)
    else:
        run(args)


if __name__ == "__main__":
    main()
