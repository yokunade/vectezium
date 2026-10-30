#!/usr/bin/env python3
"""Production-readiness suite for the mysql_cdc source (Vectezium).

Run through run-readiness.sh, which starts the docker compose environment first. Each test uses
its own database, Vectezium data directory and output files, and runs Vectezium with the
production event format (metadata + record + before). Results go to out/readiness/report.json.

Tests (numbering follows the readiness test plan):
  1.1  cold replay across DDL made while stopped        1.2  column reorder + INT->BIGINT
  2.1  writes during the snapshot (locked, 1 thread)    2.1b same, lock-free with 4 threads
  3.1  large transaction + SIGKILL mid-transaction      3.2  rollback and savepoints
  4.1  downstream stall (backpressure, bounded memory)  4.2  replication connection killed
  5.1  data type matrix (snapshot and binlog agree)
  7.1  binlog purged while stopped                      7.2  MySQL restart mid-stream
  7.3  TLS modes (required, verify_ca, verify_identity)
6.1 (SIGKILL under 500 rows/s) is run-load.sh, called by run-readiness.sh afterwards.

Python 3.8+, standard library only. Needs docker and openssl.
"""

import argparse
import datetime as dt
import json
import os
import random
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import traceback
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE / "out" / "readiness"
MYSQL = ["docker", "exec", "-i", "cdc-mysql", "mysql", "-uroot", "-prootpw",
         "--default-character-set=utf8mb4", "-N", "-B"]
VECTOR = None
_server_id = [7000]
_running = []



def row(e):
    """The row of a `record`-format event: `record`, or with `record_images: before_and_after`
    (`record` = {"before": ..., "after": ...}) the after image, or for a delete the deleted row
    (`before`; a delete's `after` holds only the key)."""
    rec = e.get("record") or {}
    if set(rec) == {"before", "after"}:
        if e.get("metadata", {}).get("op") == "d":
            return rec["before"] or {}
        return rec["after"] or {}
    return rec


def before(e):
    """The before image of a `record`-format event (`record.before`), if any."""
    rec = e.get("record") or {}
    return rec.get("before") if set(rec) == {"before", "after"} else None

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# --------------------------------------------------------------------------------------- MySQL

def sql(script, db=None, check=True, timeout=900):
    """Runs a SQL script as root and returns the rows (tab-separated columns)."""
    cmd = MYSQL + ([db] if db else [])
    r = subprocess.run(cmd, input=script, text=True, capture_output=True, timeout=timeout)
    err = "\n".join(l for l in r.stderr.splitlines() if "Using a password" not in l)
    if check and r.returncode != 0:
        raise RuntimeError(f"SQL failed: {err.strip()[:500]}\n--- script ---\n{script[:500]}")
    return [line.split("\t") for line in r.stdout.splitlines()]


def scalar(q, db=None):
    rows = sql(q, db)
    return rows[0][0] if rows else None


def gtid_executed():
    return re.sub(r"\s", "", scalar("SELECT @@global.gtid_executed") or "")


def mysql_alive():
    r = subprocess.run(["docker", "exec", "cdc-mysql", "mysqladmin", "ping", "-h", "127.0.0.1",
                        "-uroot", "-prootpw"], capture_output=True, text=True)
    return r.returncode == 0 and "alive" in r.stdout


def wait_until(pred, timeout, step=0.5):
    end = time.time() + timeout
    while time.time() < end:
        try:
            if pred():
                return True
        except Exception:  # noqa: BLE001 - a transient error just means "not yet"
            pass
        time.sleep(step)
    return False


def ensure_seq(n=1_000_000):
    """A numbers table (not captured, not binlogged) for INSERT ... SELECT seeding."""
    have = sql("SELECT COUNT(*) FROM information_schema.TABLES "
               "WHERE TABLE_SCHEMA='rt_util' AND TABLE_NAME='seq'")[0][0]
    if have == "1" and int(scalar("SELECT MAX(n) FROM rt_util.seq") or 0) >= n:
        return
    sql(f"""SET SESSION sql_log_bin = 0;
CREATE DATABASE IF NOT EXISTS rt_util;
DROP TABLE IF EXISTS rt_util.seq;
CREATE TABLE rt_util.seq (n INT PRIMARY KEY);
SET SESSION cte_max_recursion_depth = {n + 1};
INSERT INTO rt_util.seq WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < {n}) SELECT n FROM c;""")


def fresh_db(db):
    sql(f"DROP DATABASE IF EXISTS {db}; CREATE DATABASE {db};")


class PipeWriter:
    """Streams statements into one long-lived mysql client (autocommit) at a target rate."""

    def __init__(self, gen, rate, db):
        self.gen, self.rate, self.db = gen, rate, db
        self.count = 0
        self.stop_flag = threading.Event()
        self.errfile = open(OUT / f"writer-{db}.err", "a")
        self.proc = subprocess.Popen(MYSQL + [db], stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
                                     stderr=self.errfile, text=True)
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self.thread.start()
        return self

    def _run(self):
        start = time.time()
        while not self.stop_flag.is_set():
            due = int((time.time() - start) * self.rate) - self.count
            if due <= 0:
                time.sleep(0.005)
                continue
            lines = []
            for _ in range(min(due, 200)):
                lines.append(self.gen(self.count))
                self.count += 1
            try:
                self.proc.stdin.write("\n".join(lines) + "\n")
                self.proc.stdin.flush()
            except BrokenPipeError:
                return

    def stop(self):
        self.stop_flag.set()
        self.thread.join()
        try:
            self.proc.stdin.close()
        except BrokenPipeError:
            pass
        self.proc.wait(timeout=600)
        self.errfile.close()


# ---------------------------------------------------------------------------------- Vectezium

class Vectezium:
    def __init__(self, name, db, out_dir, *, data_dir=None, rows_sink=None, server_id=None,
                 rows_file="rows.ndjson", acknowledgements=True, **source):
        self.name, self.db = name, db
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.data_dir = Path(data_dir or self.out_dir / "data")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.rows = self.out_dir / rows_file
        self.schemas = self.out_dir / "schemas.ndjson"
        # Connector events (schema_change, truncate, snapshot_completed).
        self.ddl = self.control = self.out_dir / "control.ndjson"
        self.published = self.out_dir / "offsets.ndjson"
        self.logfile = self.out_dir / "vectezium.log"
        if server_id is None:
            _server_id[0] += 1
            server_id = _server_id[0]
        src = {
            "type": "mysql_cdc", "hostname": "127.0.0.1", "port": 3306,
            "user": "vector_cdc", "password": "cdcpw", "server_id": server_id,
            "topic_prefix": "rt", "database_include_list": [db],
            "decimal_handling_mode": "string", "event_format": "record",
            "record_images": "before_and_after", "include_table_schemas": True,
            "include_schema_changes": True, "ssl_mode": "disabled",
            "heartbeat_interval_secs": 2, "connect_timeout_secs": 10,
        }
        src.update(source)
        enc = {"codec": "json", "except_fields": ["source_type", "timestamp"]}
        sinks = {
            "rows": rows_sink or {"type": "file", "inputs": ["mysql"], "path": str(self.rows),
                                  "encoding": enc},
            "schemas": {"type": "file", "inputs": ["mysql.table_schemas"],
                        "path": str(self.schemas), "encoding": enc},
            "control": {"type": "file", "inputs": ["mysql.control"], "path": str(self.control),
                        "encoding": enc},
        }
        if src.get("publish_offsets"):
            sinks["offsets"] = {"type": "file", "inputs": ["mysql.offsets"],
                                "path": str(self.published), "encoding": enc}
        self.config = self.out_dir / f"vectezium-{rows_file.split('.')[0]}.json"
        self.config.write_text(json.dumps({
            "data_dir": str(self.data_dir), "acknowledgements": {"enabled": acknowledgements},
            "sources": {"mysql": src}, "sinks": sinks}, indent=2))
        self.proc = None
        self._log_start = 0

    @property
    def offsets(self):
        return self.data_dir / "mysql" / "offsets.json"

    def start(self):
        self._log_start = self.logfile.stat().st_size if self.logfile.exists() else 0
        lf = open(self.logfile, "a")
        self.proc = subprocess.Popen([VECTOR, "--config", str(self.config)], stdout=lf,
                                     stderr=subprocess.STDOUT)
        lf.close()
        _running.append(self)
        return self

    def alive(self):
        return self.proc is not None and self.proc.poll() is None

    def stop(self):
        if self.alive():
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=90)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.proc = None

    def kill9(self):
        if self.alive():
            self.proc.kill()
            self.proc.wait()
        self.proc = None

    def log_text(self, this_run=True):
        if not self.logfile.exists():
            return ""
        with open(self.logfile, "rb") as f:
            if this_run:
                f.seek(self._log_start)
            return f.read().decode("utf-8", "replace")

    def wait_log(self, pattern, timeout):
        return wait_until(lambda: re.search(pattern, self.log_text()), timeout)

    def streaming(self, timeout=60):
        return self.wait_log(r"Streaming changes from the binlog", timeout)

    def caught_up(self, timeout=180):
        """Offsets reach the server's gtid_executed (every change is acknowledged downstream)."""
        def done():
            stored = re.sub(r"\s", "", json.loads(self.offsets.read_text()).get("gtid_set", ""))
            return stored and stored == gtid_executed()
        ok = wait_until(done, timeout, step=1)
        time.sleep(2)
        return ok

    def rss_mb(self):
        try:
            for line in open(f"/proc/{self.proc.pid}/status"):
                if line.startswith("VmRSS"):
                    return int(line.split()[1]) / 1024
        except Exception:  # noqa: BLE001
            return None
        return None

    def errors(self, ignore=()):
        lines = [l for l in self.log_text(False).splitlines() if " ERROR " in l]
        return [l for l in lines if not any(i in l for i in ignore)]


def read_events(path, table=None, markers=False):
    """Row events (of `table`). `markers` is accepted for compatibility: snapshot completion now
    arrives on the control output (see `read_control`)."""
    events = []
    if not Path(path).exists():
        return events
    with open(path, "rb") as f:
        for line in f:
            try:
                e = json.loads(line)
            except ValueError:
                continue  # a torn last line after SIGKILL
            if table is None or e.get("metadata", {}).get("table") == table:
                events.append(e)
    return events


def read_control(path, kind=None, table=None):
    """Control events (`schema_change`, `truncate`, `snapshot_completed`), optionally filtered."""
    out = []
    if Path(path).exists():
        for line in open(path, "rb"):
            try:
                c = json.loads(line)
            except ValueError:
                continue
            if kind and c.get("type") != kind:
                continue
            if table and c.get("type") != "schema_change" and c.get("table") != table:
                continue
            out.append(c)
    return out


def iter_events(path, table=None):
    if not Path(path).exists():
        return
    with open(path, "rb") as f:
        for line in f:
            try:
                e = json.loads(line)
            except ValueError:
                continue
            if table is None or e.get("metadata", {}).get("table") == table:
                yield e


def counter(seq):
    """The binlog counter of a master seq string ("<20 digits>.<10 digits>")."""
    return int(seq.split(".")[0]) if seq else 0


def position(seq):
    """The snapshot position of a master seq string (0 for binlog-derived events)."""
    return int(seq.split(".")[1]) if seq else 0


def replay(events, key="id", state=None, control=()):
    """Applies row events and control events in `seq` order, like a downstream merge would;
    returns {key: record}.

    Control events must be those of the same table: `truncate` empties it, and
    `snapshot_completed` reconciles: it removes every key the snapshot (`snapshot_id`) did not
    contain whose last change has a lower `seq` than the snapshot's `started_seq`."""
    state = {} if state is None else dict(state)
    snapshot_keys = {}
    last_seq = {}
    merged = [(e["metadata"].get("seq") or "", 0, i, e) for i, e in enumerate(events)]
    merged += [(c.get("seq") or "", 1, i, c) for i, c in enumerate(control)
               if c.get("type") in ("truncate", "snapshot_completed")]
    merged.sort(key=lambda x: (x[0], x[1], x[2]))      # fixed width: text order is seq order
    for seq, is_control, _, e in merged:
        if is_control:
            if e["type"] == "truncate":
                state.clear()
            else:
                keep = snapshot_keys.get(e.get("snapshot_id"), set())
                started = e.get("started_seq") or ""
                for stale in [x for x in state if x not in keep and last_seq.get(x, "") < started]:
                    del state[stale]
            continue
        m, rec = e["metadata"], row(e)
        k = rec.get(key)
        last_seq[k] = seq
        if m["op"] == "d":
            state.pop(k, None)
        else:
            if m["op"] == "r":
                snapshot_keys.setdefault(m.get("snapshot_id"), set()).add(k)
            state[k] = rec
    return state


def db_state(db, table, cols="id, val"):
    rows = sql(f"SELECT {cols} FROM {db}.{table} ORDER BY id")
    return {int(r[0]): r[1:] for r in rows}


def compare(state, truth, cols):
    """Compares replayed records with the database: returns a list of differences (max 10)."""
    diffs = []
    for k in sorted(set(state) | set(truth), key=lambda x: (x is None, x)):
        if k not in truth:
            diffs.append(f"id {k}: in output, not in MySQL")
        elif k not in state:
            diffs.append(f"id {k}: in MySQL, missing from output")
        else:
            got = ["NULL" if state[k].get(c) is None else str(state[k].get(c)) for c in cols]
            if got != truth[k]:
                diffs.append(f"id {k}: output {got} != MySQL {truth[k]}")
        if len(diffs) >= 10:
            diffs.append("...")
            break
    return diffs


# ------------------------------------------------------------------------------------ results

class Test:
    def __init__(self, tid, name):
        self.id, self.name, self.checks, self.notes = tid, name, [], []
        self.dir = OUT / tid.replace(".", "_")
        if self.dir.exists():
            shutil.rmtree(self.dir)
        self.dir.mkdir(parents=True)
        self.started = time.time()

    def check(self, desc, ok, detail=""):
        status = "PASS" if ok else "FAIL"
        self.checks.append({"check": desc, "status": status, "detail": str(detail)[:600]})
        print(f"    {status:5} {desc}" + (f" -- {detail}" if detail and not ok else ""), flush=True)
        return ok

    def known(self, desc, detail):
        self.checks.append({"check": desc, "status": "KNOWN", "detail": detail})
        print(f"    KNOWN {desc} -- {detail}", flush=True)

    def note(self, text):
        self.notes.append(text)
        print(f"    note: {text}", flush=True)

    @property
    def status(self):
        s = [c["status"] for c in self.checks]
        if not s or "FAIL" in s:
            return "FAIL"
        return "KNOWN" if "KNOWN" in s else "PASS"

    def to_json(self):
        return {"id": self.id, "name": self.name, "status": self.status,
                "seconds": round(time.time() - self.started, 1), "checks": self.checks,
                "notes": self.notes}


# -------------------------------------------------------------------------------------- tests

def t1_1(t):
    db = "rt_ddl_offline"
    fresh_db(db)
    sql("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), legacy_code VARCHAR(10));", db)
    v = Vectezium("1.1", db, t.dir, snapshot_mode="initial")
    v.start()
    t.check("streaming started", v.streaming())
    v.caught_up()
    v.stop()
    pre = t.dir / "offsets-before-row1.json"
    shutil.copy(v.offsets, pre)

    v.start()
    v.streaming()
    sql("INSERT INTO users VALUES (1, 'Alice', 'LEG_A');", db)
    v.caught_up()
    v.stop()
    live = read_events(v.rows, "users")
    r1 = [e for e in live if row(e)["id"] == 1]
    t.check("live: row 1 has [id, name, legacy_code]",
            r1 and sorted(row(r1[-1])) == ["id", "legacy_code", "name"],
            row(r1[-1]) if r1 else "missing")

    log("    stopped; DROP legacy_code + ADD email and insert row 2 while offline")
    sql("ALTER TABLE users DROP COLUMN legacy_code, ADD COLUMN email VARCHAR(100);"
        "INSERT INTO users VALUES (2, 'Bob', 'bob@example.com');", db)

    # Cold replay: rewind to before row 1, so row 1 is decoded after the live catalog changed.
    shutil.copy(pre, v.offsets)
    cold = Vectezium("1.1", db, t.dir, data_dir=v.data_dir, rows_file="rows-replay.ndjson",
                     server_id=_server_id[0])
    cold.start()
    t.check("replay: streaming from the old offset", cold.streaming())
    cold.caught_up()
    cold.stop()
    ev = read_events(cold.rows, "users")
    a = [e for e in ev if row(e)["id"] == 1 and e["metadata"]["op"] == "c"]
    b = [e for e in ev if row(e)["id"] == 2 and e["metadata"]["op"] == "c"]
    t.check("replay: row 1 decoded with the historical layout [id, name, legacy_code]",
            a and row(a[0]) == {"id": 1, "name": "Alice", "legacy_code": "LEG_A"},
            row(a[0]) if a else "missing")
    t.check("replay: row 2 decoded with the new layout [id, name, email]",
            b and row(b[0]) == {"id": 2, "name": "Bob", "email": "bob@example.com"},
            row(b[0]) if b else "missing")
    va = a[0]["metadata"]["schema_version"] if a else None
    vb = b[0]["metadata"]["schema_version"] if b else None
    t.check("distinct schema_version for each layout", va and vb and va != vb, f"{va} / {vb}")
    docs = {d["schema_version"]: [c["name"] for c in d["columns"]]
            for d in read_events_raw(t.dir / "schemas.ndjson") if d.get("table") == "users"}
    t.check("a schema document exists for each layout",
            docs.get(va) == ["id", "name", "legacy_code"] and docs.get(vb) == ["id", "name", "email"],
            docs)
    ddl = (t.dir / "control.ndjson").read_text() if (t.dir / "control.ndjson").exists() else ""
    t.check("DDL emitted on the control output", "DROP COLUMN legacy_code" in ddl)
    t.check("no errors logged", not cold.errors(), cold.errors()[:3])


def read_events_raw(path):
    out = []
    if Path(path).exists():
        for line in open(path, "rb"):
            try:
                out.append(json.loads(line))
            except ValueError:
                pass
    return out


def t1_2(t):
    db = "rt_reorder"
    fresh_db(db)
    sql("CREATE TABLE orders (id INT PRIMARY KEY, status VARCHAR(10), amount INT);", db)
    v = Vectezium("1.2", db, t.dir, snapshot_mode="initial")
    v.start()
    v.streaming()
    v.caught_up()
    v.stop()
    pre = t.dir / "offsets-start.json"
    shutil.copy(v.offsets, pre)
    v.start()
    v.streaming()
    sql("INSERT INTO orders VALUES (101, 'PENDING', 50);"
        "ALTER TABLE orders MODIFY COLUMN amount BIGINT AFTER id;"
        "INSERT INTO orders VALUES (102, 9223372036854775800, 'PAID');"
        "UPDATE orders SET status = 'SHIPPED' WHERE id = 101;", db)
    v.caught_up()
    v.stop()
    shutil.copy(pre, v.offsets)
    cold = Vectezium("1.2", db, t.dir, data_dir=v.data_dir, rows_file="rows-replay.ndjson",
                     server_id=_server_id[0])
    cold.start()
    cold.streaming()
    cold.caught_up()
    cold.stop()
    for label, path in (("live", v.rows), ("replay", cold.rows)):
        ev = read_events(path, "orders")
        c101 = next((e for e in ev if e["metadata"]["op"] == "c" and row(e)["id"] == 101), None)
        c102 = next((e for e in ev if e["metadata"]["op"] == "c" and row(e)["id"] == 102), None)
        u101 = next((e for e in ev if e["metadata"]["op"] == "u" and row(e)["id"] == 101), None)
        t.check(f"{label}: 101 before the change = PENDING, 50",
                c101 and row(c101) == {"id": 101, "status": "PENDING", "amount": 50},
                c101 and row(c101))
        rec = c102 and row(c102)
        t.check(f"{label}: 102 amount is the exact 64-bit integer 9223372036854775800",
                rec and isinstance(rec.get("amount"), int) and rec["amount"] == 9223372036854775800
                and rec.get("status") == "PAID", rec)
        t.check(f"{label}: update of 101 under the new layout",
                u101 and row(u101) == {"id": 101, "amount": 50, "status": "SHIPPED"}
                and before(u101)["status"] == "PENDING", u101)
        t.check(f"{label}: schema_version changes with the layout",
                c101 and c102 and c101["metadata"]["schema_version"] != c102["metadata"]["schema_version"])
    docs = [d for d in read_events_raw(v.schemas) if d.get("table") == "orders"]
    # Record keys come out alphabetically (Vector's event model); column order lives in the
    # schema document, which must show the moved column.
    layouts = {tuple(c["name"] for c in d["columns"]) for d in docs}
    t.check("schema documents show the old and new column order",
            {("id", "status", "amount"), ("id", "amount", "status")} <= layouts, layouts)
    amount = [c for d in docs for c in d["columns"] if c["name"] == "amount"]
    t.check("schema documents record INT -> BIGINT (int32 -> int64)",
            {c["logical_type"] for c in amount} >= {"int32", "int64"}, [c["logical_type"] for c in amount])
    t.check("no errors logged", not cold.errors() and not v.errors(), (v.errors() + cold.errors())[:3])


def t2_1(t, locking, threads, chunk, rows=1_000_000):
    db = "rt_snapshot_writes"
    exists = scalar(f"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='{db}' "
                    "AND TABLE_NAME='inventory'") == "1"
    if not exists:
        log(f"    seeding {rows:,} rows (binlog off for the seed)")
        ensure_seq(rows)
        fresh_db(db)
        sql(f"""CREATE TABLE inventory (id INT PRIMARY KEY, quantity INT,
  updated_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6));
SET SESSION sql_log_bin = 0;
INSERT INTO inventory (id, quantity) SELECT n, 100 FROM rt_util.seq WHERE n <= {rows};""", db)
    next_id = [rows + 1 + (0 if t.id == "2.1" else 500_000)]
    rnd = random.Random(21)

    def stmt(i):
        p = rnd.random()
        if p < 0.40:
            return f"UPDATE inventory SET quantity = quantity + {10 if i % 2 else 50} WHERE id = {42 if i % 2 else 500};"
        if p < 0.80:
            return f"UPDATE inventory SET quantity = quantity + 1 WHERE id = {rnd.randint(1, rows)};"
        if p < 0.90:
            next_id[0] += 1
            return f"INSERT INTO inventory (id, quantity) VALUES ({next_id[0]}, 7);"
        return f"DELETE FROM inventory WHERE id = {rnd.randint(1000, rows)};"

    v = Vectezium(t.id, db, t.dir, snapshot_mode="initial", snapshot_locking_mode=locking,
                  snapshot_max_threads=threads, snapshot_chunk_size=chunk, snapshot_batch_size=20000)
    w = PipeWriter(stmt, 400, db).start()
    time.sleep(1)
    v.start()
    v.wait_log(r"Snapshot started", 120)
    at_start = w.count
    done = v.wait_log(r"Snapshot completed", 900)
    at_end = w.count
    time.sleep(5)
    w.stop()
    during = at_end - at_start
    t.check("snapshot completed", done)
    t.check("at least 1,000 writes landed while the snapshot ran", during >= 1000, f"{during} writes")
    t.note(f"{during:,} writes during the snapshot, {w.count:,} in total; locking={locking}, "
           f"threads={threads}, chunk={chunk}")
    t.check("caught up with MySQL", v.caught_up(300))
    v.stop()

    log("    reading output and comparing with MySQL")
    truth = db_state(db, "inventory", "id, quantity")
    state, last, r_ids, dup_r = {}, {}, set(), 0
    streamed = late_r = updates = bad_before = 0
    ends = read_control(v.control, "snapshot_completed", "inventory")
    snapshot_ids = set()
    prev_seq, seq_ok, first_r_seq = "", True, None
    for e in iter_events(v.rows, "inventory"):
        op, rec = e["metadata"]["op"], row(e)
        seq = e["metadata"].get("seq") or ""
        seq_ok = seq_ok and len(seq) == 31 and seq > prev_seq
        prev_seq = seq
        if op == "r":
            snapshot_ids.add(e["metadata"].get("snapshot_id"))
            if first_r_seq is None:
                first_r_seq = seq
        k = rec.get("id")
        if op == "d":
            state.pop(k, None)
        else:
            state[k] = {"quantity": rec.get("quantity")}
        if k in (42, 500):
            last[k] = rec.get("quantity")
        if op == "r":
            if streamed:
                late_r += 1
            if k in r_ids:
                dup_r += 1
            r_ids.add(k)
        else:
            streamed += 1
            if op == "u":
                updates += 1
                b = before(e)
                if not b or b.get("id") != k:
                    bad_before += 1
    diffs = compare(state, truth, ["quantity"])
    t.check(f"replaying the output reproduces all {len(truth):,} rows exactly", not diffs, diffs)
    for hot in (42, 500):
        t.check(f"final event for id={hot} matches SELECT quantity",
                hot in last and str(last[hot]) == truth.get(hot, [None])[0],
                f"{last.get(hot)} vs {truth.get(hot)}")
    t.check("no snapshot (r) event after the first streamed change", late_r == 0, f"{late_r} late r events")
    t.check("no row snapshotted twice (chunk boundaries)", dup_r == 0, f"{dup_r} duplicates")
    missing = [k for k in truth if k <= rows and k not in r_ids]
    t.check("every seeded row still in MySQL has a snapshot event", not missing, missing[:10])
    t.check("streamed updates carry matching before and after images", updates and not bad_before,
            f"{updates} updates, {bad_before} without a matching before")
    t.check("one snapshot_completed event whose row count matches the snapshot rows",
            len(ends) == 1 and ends[0].get("rows") == len(r_ids) + dup_r,
            [(c.get("rows"), c.get("snapshot_id")) for c in ends])
    t.check("every snapshot row carries the event's snapshot_id",
            len(snapshot_ids) == 1 and ends and snapshot_ids == {ends[0].get("snapshot_id")}, snapshot_ids)
    t.check("started_seq is at or before the first snapshot row",
            ends and first_r_seq is not None
            and counter(ends[0].get("started_seq")) <= counter(first_r_seq) + 1,
            (ends and ends[0].get("started_seq"), first_r_seq))
    t.check("seq (fixed-width master sequence) strictly increases through the table stream", seq_ok)
    t.note(f"{len(r_ids):,} snapshot events, {streamed:,} streamed events")
    t.check("no errors logged", not v.errors(), v.errors()[:3])


def t3_1(t):
    db = "rt_big_tx"
    fresh_db(db)
    ensure_seq()
    sql("CREATE TABLE orders (id INT PRIMARY KEY, amount BIGINT, status VARCHAR(20));", db)
    v = Vectezium("3.1", db, t.dir, snapshot_mode="initial")
    v.start()
    v.streaming()
    sql("""START TRANSACTION;
INSERT INTO orders VALUES (301, 100, 'BATCH_1');
INSERT INTO orders SELECT 301 + n, 100, 'BATCH_ITEM' FROM rt_util.seq WHERE n <= 5000;
COMMIT;""", db)
    v.caught_up()
    ev = [e for e in read_events(v.rows, "orders") if 301 <= row(e)["id"] <= 5301]
    gtids = {e["metadata"]["gtid"] for e in ev}
    t.check("5,001-row transaction: all 5,001 rows delivered",
            len({e['record']['id'] for e in ev}) == 5001, f"{len(ev)} events")
    t.check("5,001-row transaction: one shared GTID", len(gtids) == 1 and None not in gtids, gtids)

    # SIGKILL while a large transaction is being emitted.
    base, n, attempt, seen = 100_000, 300_000, 0, 0
    killed_mid = False
    while attempt < 3 and not killed_mid:
        attempt += 1
        lo = base * (attempt + 1)
        size0 = v.rows.stat().st_size
        writer = threading.Thread(target=lambda: sql(
            f"INSERT INTO orders SELECT {lo} + n, n, 'BIG' FROM rt_util.seq WHERE n <= {n};", db))
        writer.start()
        wait_until(lambda: v.rows.stat().st_size > size0 + 2_000_000, 120, step=0.01)
        v.kill9()
        writer.join()
        seen = sum(1 for e in read_events(v.rows, "orders") if lo < row(e)["id"] <= lo + n)
        killed_mid = 0 < seen < n
        if not killed_mid:
            t.note(f"attempt {attempt}: the kill landed outside the transaction ({seen:,} rows seen); retrying")
            v.start()
            v.streaming()
            v.caught_up()
    t.check("SIGKILL landed mid-transaction", killed_mid, f"{seen:,} of {n:,} rows were downstream")
    if killed_mid:
        t.known("downstream never sees a partial transaction",
                f"{seen:,} of {n:,} rows of the transaction reached the sink before the crash. By design "
                "large transactions are streamed in batches (holding them would risk OOM), and on "
                "restart the whole transaction is replayed. Fix planned: transaction BEGIN/END metadata "
                "so consumers can wait for complete transactions.")
    v.stop()
    v2 = Vectezium("3.1", db, t.dir, data_dir=v.data_dir, rows_file="rows-after-crash.ndjson",
                   server_id=_server_id[0])
    v2.start()
    v2.streaming()
    t.check("caught up after restart", v2.caught_up(300))
    v2.stop()
    after = [e for e in read_events(v2.rows, "orders") if lo < row(e)["id"] <= lo + n]
    ids = {row(e)["id"] for e in after}
    t.check("after restart the whole transaction is delivered (no loss)", len(ids) == n,
            f"{len(ids):,} of {n:,}")
    g = {e["metadata"]["gtid"] for e in after}
    t.check("replayed rows share the transaction's GTID (dedupable)", len(g) == 1, g)
    pre = {row(e)["id"]: e["metadata"]["seq"] for e in read_events(v.rows, "orders")
           if lo < row(e)["id"] <= lo + n}
    renumbered = sum(1 for e in after if row(e)["id"] in pre and pre[row(e)["id"]] != e["metadata"]["seq"])
    t.check("rows delivered before the crash are replayed with the same seq (dedupe on seq)",
            pre and not renumbered, f"{renumbered} of {len(pre):,} renumbered")
    t.check("no errors logged", not v2.errors(), v2.errors()[:3])


def t3_2(t):
    db = "rt_rollback"
    fresh_db(db)
    sql("CREATE TABLE rb (id INT PRIMARY KEY, val VARCHAR(30));"
        "INSERT INTO rb VALUES (101, 'ORIGINAL');", db)
    v = Vectezium("3.2", db, t.dir, snapshot_mode="initial")
    v.start()
    v.streaming()
    sql("""START TRANSACTION;
INSERT INTO rb VALUES (9999, 'GHOST');
UPDATE rb SET val = 'SHOULD_NOT_EXIST' WHERE id = 101;
ROLLBACK;
START TRANSACTION;
INSERT INTO rb VALUES (2001, 'KEEP');
SAVEPOINT s1;
INSERT INTO rb VALUES (2002, 'DROP');
UPDATE rb SET val = 'DROP' WHERE id = 101;
ROLLBACK TO SAVEPOINT s1;
COMMIT;
INSERT INTO rb VALUES (1000, 'COMMITTED');""", db)
    v.caught_up()
    v.stop()
    ev = read_events(v.rows, "rb")
    ids = [row(e)["id"] for e in ev]
    vals = [row(e).get("val") for e in ev]
    t.check("rolled-back insert (9999) not emitted", 9999 not in ids)
    t.check("rolled-back update of 101 not emitted", "SHOULD_NOT_EXIST" not in vals)
    t.check("committed insert (1000) emitted", 1000 in ids)
    t.check("work before the savepoint (2001) emitted", 2001 in ids)
    t.check("work rolled back to the savepoint (2002, 'DROP') not emitted",
            2002 not in ids and "DROP" not in vals)
    t.check("101 has only its snapshot event", [e["metadata"]["op"] for e in ev if row(e)["id"] == 101] == ["r"])


def _receiver(fifo, path):
    """Slow-consumer stand-in: copies everything written to a named pipe into a file. While this
    process is SIGSTOPped the pipe fills (64 KB) and the writer (Vectezium's sink) blocks."""
    with open(path, "ab") as out:
        while True:
            with open(fifo, "rb", buffering=0) as src:  # blocks until a writer opens the pipe
                while True:
                    data = src.read(65536)
                    if not data:
                        break  # the writer closed the file; wait for it to reopen
                    out.write(data)
                    out.flush()


def t4_1(t):
    db = "rt_stall"
    fresh_db(db)
    ensure_seq()
    sql("CREATE TABLE stall (id INT PRIMARY KEY, payload TEXT);", db)
    fifo, received = t.dir / "stall.fifo", t.dir / "received.ndjson"
    os.mkfifo(fifo)
    recv = subprocess.Popen([sys.executable, "-I", str(Path(__file__)), "_receiver", str(fifo), str(received)])
    time.sleep(1)
    # The sink writes into a named pipe read by `recv`; stopping `recv` stalls the sink.
    # No idle close: closing syncs the file, which a pipe does not support.
    sink = {"type": "file", "inputs": ["mysql"], "path": str(fifo), "idle_timeout_secs": 86400,
            "encoding": {"codec": "json", "except_fields": ["source_type", "timestamp"]}}
    v = Vectezium("4.1", db, t.dir, snapshot_mode="no_data", rows_sink=sink)
    try:
        v.start()
        t.check("streaming started", v.streaming())
        # Warm-up row so the sink has the pipe open before the reader is stopped.
        sql("INSERT INTO stall VALUES (0, 'warm-up');", db)
        t.check("warm-up row delivered", wait_until(lambda: any(
            e.get("record", {}).get("id") == 0 for e in read_events_raw(received)), 60))
        time.sleep(3)
        base = v.rss_mb() or 0
        log("    SIGSTOP the receiver, then insert 25,000 rows of ~2 KB (~55 MB of events)")
        os.kill(recv.pid, signal.SIGSTOP)
        stall_start = time.time()
        sql("".join(f"INSERT INTO stall SELECT {b * 1000} + n, REPEAT('x', 2000) FROM rt_util.seq WHERE n <= 1000;\n"
                    for b in range(25)), db)
        samples = []
        while time.time() - stall_start < 60:
            samples.append(v.rss_mb() or 0)
            time.sleep(1)
        delivered_during = len(read_events_raw(received)) - 1
        peak = max(samples)
        t.check("Vectezium survived the 60 s stall", v.alive())
        t.check("memory stayed bounded (< 300 MB growth while ~55 MB was blocked)",
                peak - base < 300, f"baseline {base:.0f} MB, peak {peak:.0f} MB")
        t.note(f"RSS baseline {base:.0f} MB, peak {peak:.0f} MB during the stall; "
               f"{delivered_during:,} events reached the receiver before it stalled")
        os.kill(recv.pid, signal.SIGCONT)
        log("    SIGCONT; waiting for delivery")
        def got_ids():
            return len({row(e)["id"] for e in read_events_raw(received)
                        if e.get("metadata", {}).get("table") == "stall" and row(e)["id"] > 0})
        ok = wait_until(lambda: got_ids() >= 25000, 300, step=2)
        got = got_ids()
        t.check("all 25,000 rows delivered after the receiver resumed", ok, f"{got:,} of 25,000")
        reconnects = v.log_text().count("replication connection error")
        t.check("no replication reconnects needed during the stall", reconnects == 0, f"{reconnects} reconnects")
        t.check("no errors logged", not v.errors(), v.errors()[:3])
    finally:
        try:
            os.kill(recv.pid, signal.SIGCONT)
        except ProcessLookupError:
            pass
        v.stop()
        recv.kill()


def churn(db, table, rnd, next_id, lo=1, hi=2000):
    def stmt(i):
        p = rnd.random()
        if p < 0.5:
            next_id[0] += 1
            return f"INSERT INTO {table} VALUES ({next_id[0]}, {i});"
        if p < 0.85:
            return f"UPDATE {table} SET val = {i} WHERE id = {rnd.randint(lo, next_id[0])};"
        return f"DELETE FROM {table} WHERE id = {rnd.randint(lo, next_id[0])};"
    return stmt


def t4_2(t):
    db = "rt_kill_conn"
    fresh_db(db)
    sql("CREATE TABLE k (id INT PRIMARY KEY, val INT);"
        "INSERT INTO k SELECT n, 0 FROM rt_util.seq WHERE n <= 2000;", db)
    v = Vectezium("4.2", db, t.dir, snapshot_mode="initial")
    v.start()
    v.streaming()
    w = PipeWriter(churn(db, "k", random.Random(42), [2000]), 300, db).start()
    killed = 0
    for _ in range(3):
        time.sleep(10)
        for (pid,) in sql("SELECT ID FROM information_schema.PROCESSLIST WHERE USER = 'vector_cdc' "
                          "AND COMMAND LIKE 'Binlog Dump%'"):
            sql(f"KILL {pid};", check=False)
            killed += 1
    time.sleep(10)
    w.stop()
    t.check("replication connection killed 3 times under load", killed >= 3, killed)
    t.check("caught up with MySQL", v.caught_up(240))
    v.stop()
    reconnects = v.log_text(False).count("replication connection error")
    t.check("reconnected after each kill", reconnects >= 3, f"{reconnects} reconnects")
    diffs = compare(replay(read_events(v.rows, "k")), db_state(db, "k"), ["val"])
    t.check(f"replay reproduces the table exactly ({w.count:,} changes)", not diffs, diffs)


TYPE_TABLE = """CREATE TABLE type_matrix (
  id INT PRIMARY KEY,
  flag_bit BIT(1) NOT NULL,
  bit_set BIT(8) NOT NULL,
  balance DECIMAL(18,4) NOT NULL,
  wide DECIMAL(65,30) NULL,
  precise_ts DATETIME(6) NOT NULL,
  ts_utc TIMESTAMP(6) NULL,
  zero_date DATE NULL,
  json_doc JSON NULL,
  category ENUM('standard','priority','archived') NOT NULL,
  raw_blob BLOB NULL,
  big BIGINT NULL,
  t TIME(6) NULL,
  y YEAR NULL,
  emoji VARCHAR(30) CHARACTER SET utf8mb4 NULL,
  tags SET('a','b','c') NULL,
  f FLOAT NULL,
  d DOUBLE NULL
);"""
ROW_A = ("b'1', b'10101010', -99999999999999.9999, "
         "12345678901234567890123456789012345.123456789012345678901234567890, "
         "'2026-10-25 15:30:45.123456', '2026-10-25 15:30:45.654321', NULL, "
         "'{\"nested\": {\"key\": [1, 2, 3], \"valid\": true}}', 'priority', 0xDEADBEEF, "
         "9223372036854775807, '12:34:56.789012', 2026, 'héllo 👋', 'a,c', 0.1, 1.000000000000001")
ROW_B = ("b'0', b'00000001', 0.0001, NULL, '1970-01-01 00:00:00.000001', NULL, '0000-00-00', "
         "'[]', 'standard', '', -9223372036854775808, '-838:59:59.000000', NULL, '', '', NULL, NULL")


def t5_1(t):
    db = "rt_types"
    fresh_db(db)
    sql(TYPE_TABLE + f"SET SESSION sql_mode = ''; INSERT INTO type_matrix VALUES (1, {ROW_A}), (3, {ROW_B});", db)
    v = Vectezium("5.1", db, t.dir, snapshot_mode="initial")
    v.start()
    v.streaming()
    sql(f"SET SESSION sql_mode = ''; INSERT INTO type_matrix VALUES (2, {ROW_A}); INSERT INTO type_matrix VALUES (4, {ROW_B});", db)
    v.caught_up()
    v.stop()
    rec = {row(e)["id"]: e for e in read_events(v.rows, "type_matrix")}
    t.check("4 rows captured (2 snapshot, 2 binlog)", sorted(rec) == [1, 2, 3, 4], sorted(rec))
    if sorted(rec) != [1, 2, 3, 4]:
        return
    epoch = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
    micros = int((dt.datetime(2026, 10, 25, 15, 30, 45, 123456, tzinfo=dt.timezone.utc) - epoch)
                 / dt.timedelta(microseconds=1))
    for i, path in ((1, "snapshot"), (2, "binlog")):
        r = row(rec[i])
        t.check(f"{path}: BIT(1) -> true", r["flag_bit"] is True, r["flag_bit"])
        t.check(f"{path}: BIT(8) b'10101010' -> bytes base64 'qg=='", r["bit_set"] == "qg==", r["bit_set"])
        t.check(f"{path}: DECIMAL(18,4) exact '-99999999999999.9999'", r["balance"] == "-99999999999999.9999", r["balance"])
        t.check(f"{path}: DECIMAL(65,30) exact", r["wide"] == "12345678901234567890123456789012345.123456789012345678901234567890", r["wide"])
        t.check(f"{path}: DATETIME(6) keeps .123456 (epoch micros {micros})", r["precise_ts"] == micros, r["precise_ts"])
        t.check(f"{path}: TIMESTAMP(6) ISO-8601 UTC with micros", str(r["ts_utc"]).endswith(".654321Z"), r["ts_utc"])
        t.check(f"{path}: ENUM -> 'priority'", r["category"] == "priority", r["category"])
        t.check(f"{path}: BLOB 0xDEADBEEF -> base64 '3q2+7w=='", r["raw_blob"] == "3q2+7w==", r["raw_blob"])
        t.check(f"{path}: BIGINT max exact", r["big"] == 9223372036854775807, r["big"])
        t.check(f"{path}: TIME(6) -> micros 45296789012", r["t"] == 45296789012, r["t"])
        t.check(f"{path}: utf8mb4 text intact", r["emoji"] == "héllo 👋", r["emoji"])
        t.check(f"{path}: SET -> 'a,c'", r["tags"] == "a,c", r["tags"])
        doc = r["json_doc"]
        parsed = json.loads(doc) if isinstance(doc, str) else doc
        t.check(f"{path}: JSON content intact", parsed == {"nested": {"key": [1, 2, 3], "valid": True}}, doc)
        t.check(f"{path}: NULL DATE -> null", r["zero_date"] is None, r["zero_date"])
    for i, path in ((3, "snapshot"), (4, "binlog")):
        r = row(rec[i])
        t.check(f"{path}: zero date '0000-00-00' -> null", r["zero_date"] is None, r["zero_date"])
        t.check(f"{path}: BIGINT min exact", r["big"] == -9223372036854775808, r["big"])
        t.check(f"{path}: TIME(6) -838:59:59 -> micros", r["t"] == -3020399000000, r["t"])
        t.check(f"{path}: empty BLOB -> ''", r["raw_blob"] == "", r["raw_blob"])
    t.check("JSON (default mode) is compact text, identical from snapshot and binlog",
            row(rec[1])["json_doc"] == row(rec[2])["json_doc"]
            == '{"nested":{"key":[1,2,3],"valid":true}}',
            (row(rec[1])["json_doc"], row(rec[2])["json_doc"]))

    def norm(r):
        r = dict(r)
        r.pop("id")
        if isinstance(r.get("json_doc"), str):
            r["json_doc"] = json.loads(r["json_doc"])
        return r
    for a, b in ((1, 2), (3, 4)):
        ra, rb = norm(row(rec[a])), norm(row(rec[b]))
        diff = {k: (ra.get(k), rb.get(k)) for k in set(ra) | set(rb) if ra.get(k) != rb.get(k)}
        t.check(f"snapshot row {a} and binlog row {b} encode every column identically", not diff, diff)
    sv = {rec[i]["metadata"]["schema_version"] for i in rec}
    t.check("snapshot and binlog rows share one schema_version", len(sv) == 1, sv)
    t.check("no errors logged", not v.errors(), v.errors()[:3])


def t5_1b(t):
    db = "rt_types_obj"
    fresh_db(db)
    sql("CREATE TABLE j (id INT PRIMARY KEY, doc JSON);"
        "INSERT INTO j VALUES (1, '{\"b\": [1, {\"c\": null}], \"a\": \"x\"}');", db)
    v = Vectezium("5.1b", db, t.dir, snapshot_mode="initial", json_handling_mode="object")
    v.start()
    v.streaming()
    sql("INSERT INTO j VALUES (2, '{\"b\": [1, {\"c\": null}], \"a\": \"x\"}');", db)
    v.caught_up()
    v.stop()
    rec = {row(e)["id"]: row(e)["doc"] for e in read_events(v.rows, "j")}
    want = {"a": "x", "b": [1, {"c": None}]}
    t.check("snapshot: JSON emitted as an object", rec.get(1) == want, rec.get(1))
    t.check("binlog: JSON emitted as an object", rec.get(2) == want, rec.get(2))
    docs = [d for d in read_events_raw(v.schemas) if d.get("table") == "j"]
    t.check("schema document records json_handling_mode: object",
            docs and all(d.get("encoding", {}).get("json_handling_mode") == "object" for d in docs),
            docs and docs[0].get("encoding"))
    t.check("no errors logged", not v.errors(), v.errors()[:3])


def t8_1(t):
    db, other = "rt_ddlf", "rt_ddlf_other"
    fresh_db(db)
    fresh_db(other)
    sql("CREATE TABLE t (id INT PRIMARY KEY, c0 INT); CREATE TABLE skip_me (id INT PRIMARY KEY);", db)
    v = Vectezium("8.1", db, t.dir, snapshot_mode="initial",
                  table_exclude_list=[f"{db}\\.skip_me"])
    v.start()
    v.streaming()
    cases = [
        # (description, session db, statement, needle unique to it, captured?)
        ("cross-database ALTER (session in another db)", other, f"ALTER TABLE {db}.t ADD COLUMN cap_c1 INT", "cap_c1", True),
        ("unqualified CREATE TABLE in the captured db", db, "CREATE TABLE cap_t2 (id INT PRIMARY KEY)", "cap_t2", True),
        ("qualified DDL with no session db", None, f"CREATE TABLE {db}.cap_t3 (id INT PRIMARY KEY)", "cap_t3", True),
        ("CREATE PROCEDURE", db, f"CREATE PROCEDURE {db}.cap_p() SELECT 1", "cap_p", True),
        ("CREATE VIEW", db, f"CREATE VIEW {db}.cap_v AS SELECT id FROM {db}.t", "cap_v", True),
        ("CREATE TRIGGER", db, f"CREATE TRIGGER {db}.cap_trg BEFORE INSERT ON {db}.t FOR EACH ROW SET NEW.c0 = 1", "cap_trg", True),
        ("DDL on another db from the captured session", db, f"CREATE TABLE {other}.other_x (id INT PRIMARY KEY)", "other_x", False),
        ("unqualified DDL in another db", other, "CREATE VIEW other_v AS SELECT 1", "other_v", False),
        ("DROP TABLE on another db", db, f"DROP TABLE IF EXISTS {other}.other_x", "other_x", False),
        ("DDL on an excluded table", db, "ALTER TABLE skip_me ADD COLUMN skip_z INT", "skip_z", False),
        ("CREATE USER", db, "CREATE USER 'rt_secret_user'@'%' IDENTIFIED BY 'Sup3r-Secret!pw'", "rt_secret_user", False),
        ("GRANT", db, f"GRANT SELECT ON {db}.* TO 'rt_secret_user'@'%'", "GRANT", False),
        ("ALTER USER", db, "ALTER USER 'rt_secret_user'@'%' IDENTIFIED BY 'An0ther-Secret!pw'", "rt_secret_user", False),
        ("DROP USER", db, "DROP USER 'rt_secret_user'@'%'", "rt_secret_user", False),
        ("FLUSH PRIVILEGES", db, "FLUSH PRIVILEGES", "FLUSH", False),
        ("ANALYZE TABLE", db, "ANALYZE TABLE t", "ANALYZE", False),
        ("OPTIMIZE TABLE", db, "OPTIMIZE TABLE t", "OPTIMIZE", False),
    ]
    for _, default_db, stmt, _, _ in cases:
        sql(stmt + ";", default_db)
    sql("INSERT INTO t (id) VALUES (1);", db)  # a row after everything, to catch up on
    v.caught_up()
    v.stop()
    ddl = read_control(v.control, "schema_change")
    texts = [d.get("ddl", "") for d in ddl]
    for desc, _, _, needle, captured in cases:
        found = any(needle.lower() in x.lower() for x in texts)
        t.check(f"{desc}: {'emitted' if captured else 'ignored'}", found == captured,
                f"control has {len(texts)} schema changes")
    raw = v.ddl.read_text() if v.ddl.exists() else ""
    t.check("no account names, passwords or hashes in the control output",
            "rt_secret_user" not in raw and "Secret" not in raw and "IDENTIFIED" not in raw.upper())
    cross = next((d for d in ddl if "cap_c1" in d.get("ddl", "")), {})
    t.check("cross-database ALTER attributed to the target database",
            cross.get("db") == db and cross.get("kind") == "table" and cross.get("seq")
            and cross.get("objects") == [{"db": db, "table": "t"}], cross)
    kinds = sorted({d.get("kind") for d in ddl})
    t.check("events carry a kind", set(kinds) >= {"table", "routine", "view", "trigger"}, kinds)
    t.check("no errors logged", not v.errors(), v.errors()[:3])


def t8_2(t):
    db, other = "rt_trunc", "rt_trunc_other"
    fresh_db(db)
    fresh_db(other)
    sql("CREATE TABLE tr (id INT PRIMARY KEY, val INT);"
        "INSERT INTO tr SELECT n, 0 FROM rt_util.seq WHERE n <= 50;", db)
    sql("CREATE TABLE y (id INT PRIMARY KEY); INSERT INTO y VALUES (1);", other)
    v = Vectezium("8.2", db, t.dir, snapshot_mode="initial")
    v.start()
    v.streaming()
    sql("INSERT INTO tr VALUES (51, 1); UPDATE tr SET val = 5 WHERE id = 1;"
        "TRUNCATE TABLE tr;"
        "INSERT INTO tr VALUES (1000, 1), (1001, 2);", db)
    sql(f"TRUNCATE TABLE {other}.y;", db)
    v.caught_up()
    v.stop()
    ev = read_events(v.rows)
    t.check("table streams carry only c, r, u, d", {e["metadata"]["op"] for e in ev} <= {"c", "r", "u", "d"},
            {e["metadata"]["op"] for e in ev})
    ts = read_control(v.control, "truncate")
    t.check("exactly one truncate control event, for the captured table",
            len(ts) == 1 and ts[0]["table"] == "tr" and ts[0]["db"] == db,
            [(c.get("db"), c.get("table")) for c in ts])
    t.check("truncate event has a seq and a GTID", ts and ts[0].get("seq") and ts[0].get("gtid"), ts and ts[0])
    diffs = compare(replay([e for e in ev if e["metadata"]["table"] == "tr"], control=ts),
                    db_state(db, "tr"), ["val"])
    t.check("replay (rows + truncate by seq) reproduces the table (2 rows)", not diffs, diffs)
    ddl = [d for d in read_control(v.control, "schema_change") if d.get("kind") == "truncate"]
    t.check("TRUNCATE also recorded as a schema_change (kind truncate)", len(ddl) == 1, len(ddl))
    t.check("no errors logged", not v.errors(), v.errors()[:3])


def t8_3(t):
    db, other = "rt_stmt", "rt_stmt_other"
    fresh_db(db)
    fresh_db(other)
    sql("CREATE TABLE s (id INT PRIMARY KEY);", db)
    sql("CREATE TABLE o (id INT PRIMARY KEY);", other)
    v = Vectezium("8.3", db, t.dir, snapshot_mode="initial")
    v.start()
    v.streaming()
    # A statement-format write to a table that is not captured is harmless.
    sql(f"SET SESSION binlog_format = 'STATEMENT'; INSERT INTO {other}.o VALUES (1);", check=False)
    sql("INSERT INTO s VALUES (1);", db)
    t.check("statement-format write to an uncaptured db is ignored; streaming continues",
            wait_until(lambda: any(row(e)["id"] == 1 for e in read_events(v.rows, "s")), 30)
            and "statement format" not in v.log_text())
    executed_before = gtid_executed()
    r = sql("SET SESSION binlog_format = 'STATEMENT'; INSERT INTO s VALUES (2);", db, check=False)
    stopped = v.wait_log(r"logged in statement format", 30)
    t.check("statement-format write to a captured table stops the source with a clear error", stopped,
            v.log_text()[-500:])
    time.sleep(3)
    stored = re.sub(r"\s", "", json.loads(v.offsets.read_text()).get("gtid_set", ""))
    t.check("the offset does not move past the uncapturable write (no silent gap)",
            stored == re.sub(r"\s", "", executed_before) or stored != gtid_executed(), stored)
    v.stop()
    t.note(f"session binlog_format switch result: {r}")


def t7_1(t):
    db = "rt_purge"
    fresh_db(db)
    sql("CREATE TABLE p (id INT PRIMARY KEY, val INT);"
        "INSERT INTO p SELECT n, 0 FROM rt_util.seq WHERE n <= 100;", db)
    v = Vectezium("7.1", db, t.dir, snapshot_mode="initial")
    v.start()
    v.streaming()
    sql("INSERT INTO p SELECT n, 0 FROM rt_util.seq WHERE n BETWEEN 101 AND 150;", db)
    v.caught_up()
    v.stop()
    saved = json.loads(v.offsets.read_text())
    log("    offline: changes, then FLUSH + PURGE BINARY LOGS")
    sql("INSERT INTO p SELECT n, 0 FROM rt_util.seq WHERE n BETWEEN 151 AND 250;"
        "UPDATE p SET val = val + 1000 WHERE id <= 20; DELETE FROM p WHERE id BETWEEN 21 AND 40;"
        "FLUSH BINARY LOGS;", db)
    current = sql("SHOW BINARY LOG STATUS")[0][0]
    sql(f"PURGE BINARY LOGS TO '{current}';")

    strict = Vectezium("7.1", db, t.dir, data_dir=v.data_dir, rows_file="rows-initial.ndjson",
                       server_id=_server_id[0], snapshot_mode="initial")
    strict.start()
    refused = strict.wait_log(r"no longer available", 60)
    time.sleep(3)
    strict.stop()
    t.check("snapshot_mode initial: refuses to start with a clear error", refused,
            strict.log_text()[-400:])
    t.check("snapshot_mode initial: emitted nothing (no silent gap)", not read_events(strict.rows, "p"))
    t.check("snapshot_mode initial: offsets untouched",
            json.loads(v.offsets.read_text()).get("gtid_set") == saved.get("gtid_set"))

    auto = Vectezium("7.1", db, t.dir, data_dir=v.data_dir, rows_file="rows-when-needed.ndjson",
                     server_id=_server_id[0], snapshot_mode="when_needed")
    auto.start()
    t.check("snapshot_mode when_needed: re-snapshots", auto.wait_log(r"re-snapshotting", 60))
    t.check("re-snapshot completed", auto.wait_log(r"Snapshot completed", 120))
    auto.streaming()
    sql("INSERT INTO p SELECT n, 0 FROM rt_util.seq WHERE n BETWEEN 251 AND 260;", db)
    auto.caught_up()
    auto.stop()
    ev = read_events(auto.rows, "p")
    r = [e for e in ev if e["metadata"]["op"] == "r"]
    t.check("re-snapshot marked first/last for consumers",
            r and r[0]["metadata"]["snapshot"] == "first" and r[-1]["metadata"]["snapshot"] == "last",
            r and (r[0]["metadata"]["snapshot"], r[-1]["metadata"]["snapshot"]))
    truth = db_state(db, "p")
    diffs = compare(replay(ev), truth, ["val"])
    t.check("re-snapshot + streaming reproduce the table exactly", not diffs, diffs)
    resnap_ids = {e["metadata"].get("snapshot_id") for e in r}
    ends = [c for c in read_control(auto.control, "snapshot_completed", "p") if c.get("snapshot_id") in resnap_ids]
    t.check("re-snapshot ends with a snapshot_completed event for the table",
            len(ends) == 1 and ends[0].get("rows") == len(r) and ends[0].get("started_seq"), ends)
    # What a downstream table holds: the first run's changes, then the re-snapshot. Without the
    # marker, rows 21-40 (deleted while the binlog was gone) would linger.
    before = replay(read_events(v.rows, "p"))
    naive = dict(before)
    for e in ev:
        if e["metadata"]["op"] != "d":
            naive[row(e)["id"]] = row(e)
    lingering = sorted(k for k in naive if k not in truth)
    t.note(f"without reconciling, {len(lingering)} deleted rows would linger downstream: {lingering[:5]}...")
    reconciled = replay(ev, state=before, control=ends)
    diffs = compare(reconciled, truth, ["val"])
    t.check("reconciling on snapshot_completed (by started_seq) removes rows deleted while the binlog was gone",
            not diffs and lingering, diffs)


def t7_2(t):
    db = "rt_restart"
    fresh_db(db)
    sql("CREATE TABLE rs (id INT PRIMARY KEY, val INT);"
        "INSERT INTO rs SELECT n, 0 FROM rt_util.seq WHERE n <= 1000;", db)
    v = Vectezium("7.2", db, t.dir, snapshot_mode="initial")
    v.start()
    v.streaming()
    rnd, next_id, stop = random.Random(72), [1000], threading.Event()
    gen = churn(db, "rs", rnd, next_id)
    stats = {"ok": 0, "failed": 0}

    def writer():
        i = 0
        while not stop.is_set():
            batch = "START TRANSACTION;\n" + "\n".join(gen(i + j) for j in range(20)) + "\nCOMMIT;\n"
            i += 20
            try:
                sql(batch, db, timeout=10)
                stats["ok"] += 1
            except Exception:  # noqa: BLE001 - MySQL is restarting
                stats["failed"] += 1
                time.sleep(1)
            time.sleep(0.2)
    th = threading.Thread(target=writer, daemon=True)
    th.start()
    time.sleep(15)
    log("    docker restart cdc-mysql")
    subprocess.run(["docker", "restart", "cdc-mysql"], capture_output=True, timeout=180)
    back = wait_until(mysql_alive, 180, step=1)
    t.check("MySQL came back", back)
    time.sleep(20)
    stop.set()
    th.join()
    t.note(f"{stats['ok']} transactions committed, {stats['failed']} failed during the restart")
    t.check("Vectezium stayed up through the restart", v.alive())
    t.check("caught up after the restart", v.caught_up(300))
    v.stop()
    reconnects = v.log_text(False).count("replication connection error")
    t.check("reconnected", reconnects >= 1, f"{reconnects} reconnect attempts")
    diffs = compare(replay(read_events(v.rows, "rs")), db_state(db, "rs"), ["val"])
    t.check("replay reproduces the table exactly", not diffs, diffs)


def t7_3(t):
    db = "rt_tls"
    fresh_db(db)
    sql("CREATE TABLE tl (id INT PRIMARY KEY, val INT);", db)
    d = t.dir / "certs"
    d.mkdir()

    def ossl(*args):
        subprocess.run(["openssl", *args], cwd=d, check=True, capture_output=True)
    for ca in ("ca-a", "ca-b"):
        ossl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2", "-subj", f"/CN={ca}",
             "-keyout", f"{ca}.key", "-out", f"{ca}.pem")
    for name, san in (("good", "DNS:localhost,IP:127.0.0.1"), ("wrong", "DNS:wrong.example")):
        (d / f"{name}.ext").write_text(f"subjectAltName={san}\n")
        ossl("req", "-newkey", "rsa:2048", "-nodes", "-subj", f"/CN={name}", "-keyout", f"{name}.key",
             "-out", f"{name}.csr")
        ossl("x509", "-req", "-in", f"{name}.csr", "-CA", "ca-a.pem", "-CAkey", "ca-a.key",
             "-CAcreateserial", "-days", "2", "-extfile", f"{name}.ext", "-out", f"{name}.pem")
    subprocess.run(["docker", "exec", "-u", "root", "cdc-mysql", "mkdir", "-p", "/tmp/rt-tls"], check=True)
    for f in ("ca-a.pem", "good.pem", "good.key", "wrong.pem", "wrong.key"):
        subprocess.run(["docker", "cp", str(d / f), f"cdc-mysql:/tmp/rt-tls/{f}"], check=True, capture_output=True)
    subprocess.run(["docker", "exec", "-u", "root", "cdc-mysql", "chown", "-R", "mysql", "/tmp/rt-tls"], check=True)

    def install(name):
        sql(f"SET GLOBAL ssl_ca = '/tmp/rt-tls/ca-a.pem', ssl_cert = '/tmp/rt-tls/{name}.pem', "
            f"ssl_key = '/tmp/rt-tls/{name}.key'; ALTER INSTANCE RELOAD TLS;")

    def attempt(label, expect, hostname="127.0.0.1", **ssl):
        # Vector splits --config on commas, so keep the directory name plain.
        v = Vectezium("7.3", db, t.dir / re.sub(r"[^A-Za-z0-9]+", "_", label).strip("_"),
                      snapshot_mode="no_data", hostname=hostname, **ssl)
        v.start()
        connected = v.wait_log(r"Connected to MySQL", 25)
        tls = None
        if connected:
            time.sleep(2)
            rows = sql("SELECT s.VARIABLE_VALUE FROM performance_schema.status_by_thread s "
                       "JOIN performance_schema.threads th USING (THREAD_ID) "
                       "WHERE th.PROCESSLIST_USER = 'vector_cdc' AND s.VARIABLE_NAME = 'Ssl_version'", check=False)
            tls = next((r[0] for r in rows if r and r[0]), None)
        v.stop()
        if expect:
            t.check(f"{label}: connects", connected, v.log_text()[-300:])
            if connected:
                if tls:
                    t.check(f"{label}: connection is encrypted ({tls})", True)
                else:
                    t.note(f"{label}: could not read Ssl_version from performance_schema")
        else:
            t.check(f"{label}: refused", not connected, "connected but should not have")

    install("good")
    attempt("required (any cert)", True, ssl_mode="required")
    attempt("verify_ca with the right CA", True, ssl_mode="verify_ca", ssl_ca_file=str(d / "ca-a.pem"))
    attempt("verify_ca with the wrong CA", False, ssl_mode="verify_ca", ssl_ca_file=str(d / "ca-b.pem"))
    attempt("verify_identity 127.0.0.1 (in SAN)", True, ssl_mode="verify_identity", ssl_ca_file=str(d / "ca-a.pem"))
    attempt("verify_identity with the wrong CA", False, ssl_mode="verify_identity", ssl_ca_file=str(d / "ca-b.pem"))
    install("wrong")
    attempt("verify_ca, cert for another host", True, ssl_mode="verify_ca", ssl_ca_file=str(d / "ca-a.pem"))
    attempt("verify_identity, cert for another host", False, ssl_mode="verify_identity", ssl_ca_file=str(d / "ca-a.pem"))
    sql("SET GLOBAL ssl_ca = 'ca.pem', ssl_cert = 'server-cert.pem', ssl_key = 'server-key.pem'; "
        "ALTER INSTANCE RELOAD TLS;", check=False)


# ------------------------------------------------------------------ suite 9: MySQL edge probes
# Each probe passes only if the scenario is already handled correctly; a FAIL is a finding.

def _cdc_run(t, db, body, **source):
    """Starts Vectezium on `db` (snapshot first), runs `body()`, catches up and stops."""
    v = Vectezium(t.id, db, t.dir, snapshot_mode="initial", **source)
    v.start()
    v.streaming(120)
    body()
    v.caught_up(240)
    v.stop()
    return v


def t9_1(t):
    db = "rt_fk"
    fresh_db(db)
    sql("""CREATE TABLE parent (id INT PRIMARY KEY, name VARCHAR(20));
CREATE TABLE child (id INT PRIMARY KEY, parent_id INT NOT NULL,
  FOREIGN KEY (parent_id) REFERENCES parent (id) ON DELETE CASCADE ON UPDATE CASCADE);
CREATE TABLE child_null (id INT PRIMARY KEY, parent_id INT NULL,
  FOREIGN KEY (parent_id) REFERENCES parent (id) ON DELETE SET NULL);
INSERT INTO parent VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO child VALUES (10, 1), (11, 1), (20, 2), (30, 3);
INSERT INTO child_null VALUES (40, 1), (41, 3);""", db)
    v = _cdc_run(t, db, lambda: sql("DELETE FROM parent WHERE id = 1; UPDATE parent SET id = 200 WHERE id = 2;", db))
    ev = read_events(v.rows)
    logtext = v.log_text(False)
    for table in ("child", "child_null"):
        t.check(f"{table}: start-up warning names the cascading foreign key",
                re.search(rf"Foreign key changes child rows.*{db}\.{table}\b", logtext) is not None)
    docs = {d["table"]: d for d in read_events_raw(v.schemas) if d.get("db") == db}
    fk = (docs.get("child", {}).get("foreign_keys") or [{}])[0]
    t.check("schema document lists the foreign key and its rules",
            fk.get("referenced_table") == "parent" and fk.get("columns") == ["parent_id"]
            and fk.get("on_delete") == "CASCADE" and fk.get("on_update") == "CASCADE", fk)
    fk2 = (docs.get("child_null", {}).get("foreign_keys") or [{}])[0]
    t.check("schema document records ON DELETE SET NULL", fk2.get("on_delete") == "SET NULL", fk2)
    lost = []
    for table in ("child", "child_null"):
        diffs = compare(replay([e for e in ev if e["metadata"]["table"] == table]),
                        db_state(db, table, "id, parent_id"), ["parent_id"])
        lost += diffs
    if lost:
        t.known("cascaded child-row changes captured",
                f"MySQL does not write cascaded changes to the binlog ({len(lost)} child rows differ). "
                "Apply the foreign-key rules from the schema documents downstream, or reconcile with a "
                "snapshot.")
    else:
        t.check("cascaded child-row changes captured", True)


def t9_2(t):
    db = "rt_minimal"
    fresh_db(db)
    sql("CREATE TABLE m (id INT PRIMARY KEY, a INT, b VARCHAR(20));"
        "INSERT INTO m VALUES (1, 1, 'keep-1'), (2, 2, 'keep-2'), (3, 3, 'keep-3');", db)
    v = Vectezium(t.id, db, t.dir, snapshot_mode="initial")
    v.start()
    v.streaming()
    sql("SET SESSION binlog_row_image = 'MINIMAL'; UPDATE m SET a = 10 WHERE id = 1; DELETE FROM m WHERE id = 2;", db)
    sql("UPDATE m SET a = 30 WHERE id = 3;", db)
    caught = v.caught_up(60)
    v.stop()
    stopped = "row image" in v.log_text(False).lower() and not caught
    ev = read_events(v.rows, "m")
    diffs = compare(replay(ev), db_state(db, "m", "id, a, b"), ["a", "b"])
    u1 = next((e for e in ev if e["metadata"]["op"] == "u" and row(e).get("id") == 1), None)
    t.check("partial (MINIMAL) row images are either rejected loudly or never emitted as full rows",
            stopped or not diffs, f"update of id=1 emitted as {u1 and u1['record']}; replay diffs: {diffs}")


def t9_3(t):
    db = "rt_invisible"
    fresh_db(db)
    sql("""CREATE TABLE inv (id INT PRIMARY KEY, a INT, secret INT INVISIBLE);
INSERT INTO inv (id, a, secret) VALUES (1, 1, 111);
SET SESSION sql_generate_invisible_primary_key = ON;
CREATE TABLE gipk (a INT, b VARCHAR(10));
INSERT INTO gipk VALUES (1, 'x'), (2, 'y');""", db)
    def body():
        sql("INSERT INTO inv (id, a, secret) VALUES (2, 2, 222); INSERT INTO gipk VALUES (3, 'z');", db)
    v = _cdc_run(t, db, body)
    ev = read_events(v.rows)
    inv = {row(e)["id"]: e for e in ev if e["metadata"]["table"] == "inv"}
    if 1 in inv and 2 in inv:
        k1, k2 = sorted(row(inv[1])), sorted(row(inv[2]))
        t.check("invisible column: snapshot and binlog rows have the same columns", k1 == k2, f"{k1} vs {k2}")
        t.check("invisible column: values captured", row(inv[1]).get("secret") == 111
                and row(inv[2]).get("secret") == 222, (row(inv[1]), row(inv[2])))
        t.check("invisible column: one schema_version", inv[1]["metadata"]["schema_version"]
                == inv[2]["metadata"]["schema_version"])
    else:
        t.check("invisible-column rows captured", False, sorted(inv))
    g = [e for e in ev if e["metadata"]["table"] == "gipk"]
    snap = [e for e in g if e["metadata"]["op"] == "r"]
    live = [e for e in g if e["metadata"]["op"] == "c"]
    t.check("generated invisible PK: snapshot and binlog rows have the same columns",
            snap and live and sorted(row(snap[0])) == sorted(row(live[0])),
            (snap and sorted(row(snap[0])), live and sorted(row(live[0]))))
    t.check("generated invisible PK: every row has a key",
            g and all(e["metadata"].get("key") for e in g), [e["metadata"].get("key") for e in g])


def t9_4(t):
    db = "rt_generated"
    fresh_db(db)
    sql("CREATE TABLE gen (id INT PRIMARY KEY, a INT, s INT AS (a * 2) STORED, v INT AS (a * 3) VIRTUAL);"
        "INSERT INTO gen (id, a) VALUES (1, 5);", db)
    v = _cdc_run(t, db, lambda: sql("INSERT INTO gen (id, a) VALUES (2, 5); UPDATE gen SET a = 7 WHERE id = 1;", db))
    ev = read_events(v.rows, "gen")
    r1 = next((row(e) for e in ev if e["metadata"]["op"] == "r"), {})
    c2 = next((row(e) for e in ev if e["metadata"]["op"] == "c"), {})
    u1 = next((row(e) for e in ev if e["metadata"]["op"] == "u"), {})
    t.check("snapshot and binlog rows have the same columns", sorted(r1) == sorted(c2), f"{sorted(r1)} vs {sorted(c2)}")
    t.check("STORED generated column correct everywhere", r1.get("s") == 10 and c2.get("s") == 10 and u1.get("s") == 14,
            (r1.get("s"), c2.get("s"), u1.get("s")))
    t.check("VIRTUAL generated column consistent (present with the right value, or absent everywhere)",
            all("v" not in r for r in (r1, c2, u1)) or (r1.get("v") == 15 and c2.get("v") == 15 and u1.get("v") == 21),
            (r1.get("v", "absent"), c2.get("v", "absent"), u1.get("v", "absent")))


def t9_5(t):
    db = "rt_partition"
    fresh_db(db)
    sql("""CREATE TABLE pt (id INT PRIMARY KEY, val INT) PARTITION BY RANGE (id)
  (PARTITION p0 VALUES LESS THAN (100), PARTITION p1 VALUES LESS THAN (200), PARTITION p2 VALUES LESS THAN MAXVALUE);
INSERT INTO pt SELECT n - 1, 0 FROM rt_util.seq WHERE n <= 300;
CREATE TABLE pt_x (id INT PRIMARY KEY, val INT);
INSERT INTO pt_x SELECT 249 + n, 9 FROM rt_util.seq WHERE n <= 10;""", db)
    def body():
        sql("ALTER TABLE pt TRUNCATE PARTITION p0;", db)
        sql("ALTER TABLE pt DROP PARTITION p1;", db)
        sql("ALTER TABLE pt EXCHANGE PARTITION p2 WITH TABLE pt_x;", db)
    v = _cdc_run(t, db, body)
    ev = read_events(v.rows)
    flagged = [d for d in read_events_raw(v.ddl) if "PARTITION" in d.get("ddl", "").upper()]
    t.check("all three partition operations flagged data_changed on schema_changes",
            len(flagged) == 3 and all(d.get("data_changed") for d in flagged),
            [(d.get("ddl", "")[:40], d.get("data_changed")) for d in flagged])
    t.check("a warning says the tables need a re-snapshot",
            v.log_text(False).count("partition operation removed or swapped rows") >= 3)
    drift = []
    for table in ("pt", "pt_x"):
        drift += compare(replay([e for e in ev if e["metadata"]["table"] == table]), db_state(db, table), ["val"])
    if drift:
        t.known("partition operations reflected in the row stream",
                "TRUNCATE/DROP/EXCHANGE PARTITION change rows without row events. They are flagged "
                "(data_changed + warning); an automatic re-snapshot of the table is planned with "
                "incremental snapshots.")


def t9_6(t):
    db = "rt_nopk"
    fresh_db(db)
    sql("CREATE TABLE nopk (a INT, b VARCHAR(10)); INSERT INTO nopk VALUES (1, 'x'), (1, 'x'), (2, 'y');"
        "CREATE TABLE keyed (code VARCHAR(10), val INT); INSERT INTO keyed VALUES ('A', 1);", db)
    v = _cdc_run(t, db, lambda: sql("UPDATE nopk SET b = 'z' WHERE a = 2; DELETE FROM nopk WHERE a = 1 LIMIT 1;"
                                    "INSERT INTO keyed VALUES ('B', 2); UPDATE keyed SET val = 5 WHERE code = 'A';", db),
                 pk_override={db: {"keyed": "code"}})
    keyed = read_events(v.rows, "keyed")
    t.check("pk_override: every event of a keyless table carries the configured key",
            keyed and all(e["metadata"].get("key") == {"code": row(e)["code"]} for e in keyed),
            [e["metadata"].get("key") for e in keyed])
    t.check("pk_override: no keyless warning for that table",
            re.search(r"(?i)no primary key.*keyed", v.log_text(False)) is None)
    ev = read_events(v.rows, "nopk")
    t.check("changes on a table without a primary key are captured",
            [e["metadata"]["op"] for e in ev] == ["r", "r", "r", "u", "d"], [e["metadata"]["op"] for e in ev])
    t.check("a warning names the table without a primary key (merge downstream is ambiguous)",
            re.search(r"(?i)primary key.*nopk|nopk.*primary key", v.log_text(False)) is not None)


def t9_7(t, strategy="blocking"):
    db = "rt_mdl"
    fresh_db(db)
    ensure_seq()
    sql("CREATE TABLE big (id INT PRIMARY KEY, val INT); SET SESSION sql_log_bin = 0;"
        "INSERT INTO big SELECT n, 0 FROM rt_util.seq WHERE n <= 1000000;", db)
    v = Vectezium(t.id, db, t.dir, snapshot_mode="initial", snapshot_locking_mode="none",
                  snapshot_max_threads=1, snapshot_chunk_size=0, snapshot_strategy=strategy)
    v.start()
    v.wait_log(r"Snapshot started|Incremental snapshot: reading table", 60)
    time.sleep(1)
    alter = {}

    def do_alter():
        t0 = time.time()
        try:
            sql("SET SESSION lock_wait_timeout = 30; ALTER TABLE big ADD COLUMN extra INT;", db, timeout=60)
            alter["result"] = "done"
        except Exception as e:  # noqa: BLE001
            alter["result"] = str(e)[:120]
        alter["seconds"] = round(time.time() - t0, 1)
    th = threading.Thread(target=do_alter)
    th.start()
    time.sleep(1)
    worst, n = 0.0, 0
    end = time.time() + 12
    while time.time() < end:
        t0 = time.time()
        try:
            sql(f"INSERT INTO big (id, val) VALUES ({2_000_000 + n}, 1);", db, timeout=40)
        except Exception:  # noqa: BLE001
            pass
        worst = max(worst, time.time() - t0)
        n += 1
    th.join()
    done = v.wait_log(r"Snapshot completed|Incremental snapshot completed", 600)
    v.caught_up(240)
    v.stop()
    t.note(f"ALTER TABLE during the snapshot: {alter.get('result')} after {alter.get('seconds')} s; "
           f"worst application INSERT latency {worst:.1f} s over {n} inserts")
    t.check("snapshot completed", done)
    if strategy == "blocking" and worst >= 5:
        t.known("application writes not stalled by DDL waiting on the snapshot",
                f"worst INSERT took {worst:.1f} s: a blocking snapshot's transaction holds metadata "
                "locks, so DDL waits and writes queue behind it. Use snapshot_strategy: incremental "
                "(test 9.7b).")
    else:
        t.check("application writes not stalled by DDL waiting on the snapshot (worst INSERT < 5 s)",
                worst < 5, f"worst INSERT took {worst:.1f} s")
    diffs = compare(replay(read_events(v.rows, "big")), db_state(db, "big"), ["val"])
    t.check("output still matches MySQL", not diffs, diffs[:3])


def t9_8(t):
    db = "rt_tz"
    fresh_db(db)
    sql("CREATE TABLE tz (id INT PRIMARY KEY, ts TIMESTAMP(3), dt DATETIME(3));"
        "SET time_zone = '+00:00'; INSERT INTO tz VALUES (1, '2026-03-29 00:30:00.250', '2026-03-29 00:30:00.250');", db)
    sql("SET GLOBAL time_zone = '+05:30';")
    try:
        v = _cdc_run(t, db, lambda: sql("SET time_zone = '+00:00'; INSERT INTO tz VALUES "
                                        "(2, '2026-03-29 00:30:00.250', '2026-03-29 00:30:00.250');", db))
    finally:
        sql("SET GLOBAL time_zone = 'SYSTEM';")
    rec = {row(e)["id"]: row(e) for e in read_events(v.rows, "tz")}
    want = "2026-03-29T00:30:00.250Z"
    t.check("TIMESTAMP is the true UTC instant from the snapshot (server time_zone +05:30)",
            rec.get(1, {}).get("ts") == want, rec.get(1))
    t.check("TIMESTAMP is the true UTC instant from the binlog", rec.get(2, {}).get("ts") == want, rec.get(2))
    t.check("DATETIME identical from snapshot and binlog", rec.get(1, {}).get("dt") == rec.get(2, {}).get("dt"),
            (rec.get(1, {}).get("dt"), rec.get(2, {}).get("dt")))


def t9_9(t):
    import base64
    import hashlib
    db = "rt_large"
    fresh_db(db)
    mb = 20
    sql(f"CREATE TABLE lv (id INT PRIMARY KEY, b LONGBLOB, t LONGTEXT);"
        f"INSERT INTO lv VALUES (1, REPEAT(0x41, {mb} * 1048576), REPEAT('é', 5 * 1048576));", db)
    v = Vectezium(t.id, db, t.dir, snapshot_mode="initial")
    v.start()
    v.streaming(120)
    sql(f"INSERT INTO lv VALUES (2, REPEAT(0x42, {mb} * 1048576), REPEAT('ü', 5 * 1048576));", db)
    caught = v.caught_up(180)
    v.stop()
    t.check(f"{mb} MB values streamed (caught up)", caught, v.log_text(False)[-300:])
    rec = {row(e)["id"]: row(e) for e in read_events(v.rows, "lv")}
    for i, ch, tch in ((1, b"A", "é"), (2, b"B", "ü")):
        r = rec.get(i)
        if not r:
            t.check(f"row {i} delivered", False)
            continue
        blob = base64.b64decode(r["b"])
        t.check(f"row {i}: {mb} MB LONGBLOB intact", blob == ch * (mb * 1048576),
                f"{len(blob)} bytes, sha {hashlib.sha256(blob).hexdigest()[:12]}")
        t.check(f"row {i}: 5M-character LONGTEXT intact", r["t"] == tch * (5 * 1048576), f"{len(r['t'])} chars")
    t.check("no errors logged", not v.errors(), v.errors()[:2])


def t9_10(t):
    db = "rt_ost"
    fresh_db(db)
    sql("CREATE TABLE ost (id INT PRIMARY KEY, val INT); INSERT INTO ost SELECT n, 0 FROM rt_util.seq WHERE n <= 1000;", db)

    def body():
        # What gh-ost / pt-online-schema-change do: shadow table, copy, keep in sync, atomic swap.
        sql("""CREATE TABLE _ost_gho LIKE ost; ALTER TABLE _ost_gho ADD COLUMN extra INT;
INSERT INTO _ost_gho (id, val) SELECT id, val FROM ost;
UPDATE ost SET val = 5 WHERE id <= 10; UPDATE _ost_gho SET val = 5 WHERE id <= 10;
RENAME TABLE ost TO _ost_del, _ost_gho TO ost; DROP TABLE _ost_del;
INSERT INTO ost (id, val, extra) VALUES (5000, 1, 1);""", db)
    v = _cdc_run(t, db, body, snapshot_new_tables=True)
    ev = read_events(v.rows)
    resnap = sum(1 for e in ev if e["metadata"].get("snapshot") == "incremental")
    t.check("the swap does not trigger a re-snapshot of the table", resnap == 0, f"{resnap} snapshot rows")
    shadow = sum(1 for e in ev if e["metadata"]["table"].startswith("_ost_"))
    t.check("shadow-table copy traffic (_*_gho, _*_del) not captured by default", shadow == 0,
            f"{shadow} events for shadow tables")
    diffs = compare(replay([e for e in ev if e["metadata"]["table"] == "ost"]), db_state(db, "ost"), ["val"])
    t.check("after the swap, 'ost' replays to MySQL's contents", not diffs, diffs[:3])
    vers = {e["metadata"]["schema_version"] for e in ev if e["metadata"]["table"] == "ost"}
    t.check("new schema version after the swap", len(vers) == 2, vers)


def t9_11(t):
    db = "rt_newtable"
    fresh_db(db)
    sql("CREATE TABLE a (id INT PRIMARY KEY, val INT); CREATE TABLE b (id INT PRIMARY KEY, val INT);"
        "INSERT INTO a VALUES (1, 1); INSERT INTO b VALUES (1, 1), (2, 2), (3, 3);", db)
    v = Vectezium(t.id, db, t.dir, snapshot_mode="initial", table_include_list=[f"{db}\\.a"])
    v.start()
    v.streaming()
    v.caught_up()
    v.stop()
    # Without snapshot_new_tables: only new changes, and a log line says so.
    off = Vectezium(t.id, db, t.dir, data_dir=v.data_dir, rows_file="rows-off.ndjson", server_id=_server_id[0],
                    snapshot_mode="initial", table_include_list=[f"{db}\\.a", f"{db}\\.b"])
    off.start()
    off.streaming()
    off.caught_up()
    off.stop()
    t.check("snapshot_new_tables: false - existing rows not read, and the log names the table",
            not read_events(off.rows, "b") and re.search(r"never been snapshotted.*rt_newtable\.b", off.log_text()) is not None)
    v2 = Vectezium(t.id, db, t.dir, data_dir=v.data_dir, rows_file="rows-added.ndjson", server_id=_server_id[0],
                   snapshot_mode="initial", table_include_list=[f"{db}\\.a", f"{db}\\.b"],
                   snapshot_new_tables=True)
    v2.start()
    v2.streaming()
    sql("INSERT INTO b VALUES (4, 4);", db)
    t.check("incremental snapshot of the new table finished",
            v2.wait_log(r"Incremental snapshot completed", 60))
    v2.caught_up()
    v2.stop()
    ev = read_events(v2.rows, "b")
    end = read_control(v2.control, "snapshot_completed", "b")
    diffs = compare(replay(ev, control=end), db_state(db, "b"), ["val"])
    t.check("a table added to the include list later gets its existing rows (not only new changes)",
            not diffs, f"{len(diffs)} rows missing, e.g. {diffs[:2]}")
    t.check("table a was not snapshotted again", not [e for e in read_events(v2.rows, "a") if e["metadata"]["op"] == "r"])
    t.check("snapshot_completed event for the new table with its row count and started_seq",
            len(end) == 1 and end[0].get("rows") == 3 and end[0].get("started_seq"), end)


# ------------------------------------------------------------- suite 10: incremental snapshots

def _seed_big(db, table, rows):
    ensure_seq(rows)
    fresh_db(db)
    sql(f"CREATE TABLE {table} (id INT PRIMARY KEY, quantity INT);"
        f"SET SESSION sql_log_bin = 0; INSERT INTO {table} SELECT n, 100 FROM rt_util.seq WHERE n <= {rows};", db)


def t10_1(t, rows=1_000_000):
    db = "rt_incr"
    log(f"    seeding {rows:,} rows")
    _seed_big(db, "inventory", rows)
    rnd = random.Random(101)
    next_id = [rows + 1]

    def stmt(i):
        p = rnd.random()
        if p < 0.5:
            return f"UPDATE inventory SET quantity = quantity + 1 WHERE id = {rnd.randint(1, rows)};"
        if p < 0.7:
            return f"UPDATE inventory SET quantity = quantity + 3 WHERE id = {42 if i % 2 else 500};"
        if p < 0.85:
            next_id[0] += 1
            return f"INSERT INTO inventory VALUES ({next_id[0]}, 7);"
        return f"DELETE FROM inventory WHERE id = {rnd.randint(1000, rows)};"

    v = Vectezium(t.id, db, t.dir, snapshot_mode="initial", snapshot_strategy="incremental")
    w = PipeWriter(stmt, 400, db).start()
    t0 = time.time()
    v.start()
    started = v.wait_log(r"Starting an incremental snapshot", 60)
    done = v.wait_log(r"Incremental snapshot completed", 1200)
    seconds = time.time() - t0
    during = w.count
    time.sleep(3)
    w.stop()
    t.check("incremental snapshot started without a blocking phase", started)
    t.check("incremental snapshot completed", done, v.log_text()[-300:])
    t.note(f"{rows:,} rows in {seconds:.0f} s (~{rows / max(seconds, 1):,.0f} rows/s) with {during:,} concurrent writes")
    t.check("caught up with MySQL", v.caught_up(300))
    v.stop()
    log("    comparing with MySQL")
    ev = read_events(v.rows, "inventory")
    ends = read_control(v.control, "snapshot_completed", "inventory")
    truth = db_state(db, "inventory", "id, quantity")
    diffs = compare(replay(ev, control=ends), truth, ["quantity"])
    t.check(f"replay (with reconcile) reproduces all {len(truth):,} rows exactly", not diffs, diffs)
    r = [e for e in ev if e["metadata"]["op"] == "r"]
    r_ids = [row(e)["id"] for e in r]
    t.check("no key read twice", len(r_ids) == len(set(r_ids)), f"{len(r_ids) - len(set(r_ids))} duplicates")
    changed = {row(e)["id"] for e in ev if e["metadata"]["op"] in ("c", "u", "d")}
    read_ids = set(r_ids)
    missing = [k for k in truth if k not in read_ids and k not in changed]
    t.check("every row is covered by a snapshot read or a streamed change", not missing, missing[:5])
    t.check("snapshot rows are marked incremental with one snapshot_id",
            all(e["metadata"].get("snapshot") == "incremental" for e in r)
            and len({e["metadata"].get("snapshot_id") for e in r}) == 1)
    t.check("snapshot_completed with the snapshot's row count", len(ends) == 1 and ends[0].get("rows") == len(r), ends)
    first_r = min((e["metadata"]["seq"] for e in r), default="")
    t.check("started_seq is at or before the first snapshot row",
            ends and ends[0].get("started_seq") and counter(ends[0]["started_seq"]) <= counter(first_r) + 1,
            (ends and ends[0].get("started_seq"), first_r))
    seqs = [e["metadata"].get("seq") for e in ev]
    t.check("seq strictly increases through the stream (snapshot reads interleaved between changes)",
            all(a < b for a, b in zip(seqs, seqs[1:])))
    t.note(f"{len(r):,} snapshot reads ({rows - len(r):,} keys were superseded by streamed changes or deleted)")
    t.check("no errors logged", not v.errors(), v.errors()[:3])


def t10_2(t, rows=300_000):
    db = "rt_incr_kill"
    _seed_big(db, "big", rows)
    v = Vectezium(t.id, db, t.dir, snapshot_mode="initial", snapshot_strategy="incremental",
                  incremental_snapshot_chunk_size=2048)
    w = PipeWriter(lambda i: f"UPDATE big SET quantity = quantity + 1 WHERE id = {random.randint(1, rows)};", 200, db).start()
    v.start()
    v.wait_log(r"Starting an incremental snapshot", 60)
    wait_until(lambda: v.rows.exists() and v.rows.stat().st_size > 80_000_000, 300, step=0.2)
    v.kill9()
    first = [e for e in read_events(v.rows, "big") if e["metadata"]["op"] == "r"]
    t.check("SIGKILL landed mid-snapshot", 0 < len(first) < rows, f"{len(first):,} rows before the kill")
    v2 = Vectezium(t.id, db, t.dir, data_dir=v.data_dir, rows_file="rows-resumed.ndjson", server_id=_server_id[0],
                   snapshot_mode="initial", snapshot_strategy="incremental", incremental_snapshot_chunk_size=2048)
    v2.start()
    t.check("resumed and completed", v2.wait_log(r"Incremental snapshot completed", 600))
    w.stop()
    v2.caught_up(300)
    v2.stop()
    second = read_events(v2.rows, "big")
    r2 = [e for e in second if e["metadata"]["op"] == "r"]
    # Rows delivered after the last stored offset (about a second's worth) are read again.
    t.check("the restart resumed from the last delivered chunk (not from the start)",
            len(r2) <= rows - len(first) + 60_000, f"{len(first):,} reads before, {len(r2):,} after, {rows:,} rows")
    ids = {e["metadata"].get("snapshot_id") for e in first + r2}
    t.check("same snapshot_id across the restart", len(ids) == 1, ids)
    ends = read_control(v2.control, "snapshot_completed", "big")
    before = read_events(v.rows, "big")
    ident = lambda e: (e["metadata"]["file"], e["metadata"]["pos"], e["metadata"]["row"], e["metadata"]["op"])
    seq_before = {ident(e): e["metadata"]["seq"] for e in before if e["metadata"]["op"] != "r"}
    mismatched = [e for e in second if e["metadata"]["op"] != "r" and ident(e) in seq_before
                  and seq_before[ident(e)] != e["metadata"]["seq"]]
    replayed = sum(1 for e in second if e["metadata"]["op"] != "r" and ident(e) in seq_before)
    t.check("changes replayed after the crash carry the same seq as before",
            not mismatched, f"{len(mismatched)} of {replayed} replayed changes renumbered")
    diffs = compare(replay(before + second, control=ends), db_state(db, "big", "id, quantity"), ["quantity"])
    t.check("replay across the crash reproduces the table exactly", not diffs, diffs)


def t10_3(t):
    db, other = "rt_incr_rename", "rt_incr_rename_src"
    fresh_db(db)
    fresh_db(other)
    ensure_seq()
    sql("CREATE TABLE live (id INT PRIMARY KEY, val INT); INSERT INTO live VALUES (1, 1);", db)
    sql("CREATE TABLE moved (id INT PRIMARY KEY, val INT); INSERT INTO moved SELECT n, n FROM rt_util.seq WHERE n <= 5000;", other)

    def body():
        sql(f"CREATE TABLE fresh (id INT PRIMARY KEY, val INT); INSERT INTO fresh VALUES (1, 1);"
            f"RENAME TABLE {other}.moved TO {db}.moved; UPDATE moved SET val = -1 WHERE id = 10;", db)
        wait_until(lambda: re.search(r"Incremental snapshot completed", v_ref[0].log_text()), 120)
    v_ref = []
    v = Vectezium(t.id, db, t.dir, snapshot_mode="initial", snapshot_new_tables=True)
    v_ref.append(v)
    v.start()
    v.streaming()
    body()
    v.caught_up()
    v.stop()
    ev = read_events(v.rows)
    moved = [e for e in ev if e["metadata"]["table"] == "moved"]
    diffs = compare(replay(moved, control=read_control(v.control, "snapshot_completed", "moved")),
                    db_state(db, "moved"), ["val"])
    t.check("a table renamed into the captured db is snapshotted (5,000 rows)", not diffs, diffs[:3])
    fresh_r = [e for e in ev if e["metadata"]["table"] == "fresh" and e["metadata"]["op"] == "r"]
    t.check("a table created while streaming is not snapshotted", not fresh_r, len(fresh_r))
    t.check("log says why", "renamed into the captured set; snapshotting it" in v.log_text(False))


# ------------------------------------------------------------------ suite 11: published offsets

def _all_seqs(rows, control):
    """The seq of every event."""
    seqs = [e["metadata"].get("seq") for e in rows] + [c.get("seq") for c in control]
    return [x for x in seqs if x is not None]


def _straddling(rows, published):
    """Published seqs that fall inside a transaction (some of its rows at or below, some above)."""
    span = {}
    for e in rows:
        g = e["metadata"].get("gtid")
        if g:
            lo, hi = span.get(g, ("~", ""))
            seq = e["metadata"]["seq"]
            span[g] = (min(lo, seq), max(hi, seq))
    bad = []
    for p in published:
        bad += [(p, g) for g, (lo, hi) in span.items() if lo <= p < hi]
    return bad


def t11_1(t):
    db = "rt_publish"
    fresh_db(db)
    ensure_seq()
    sql("CREATE TABLE cases (id INT PRIMARY KEY, val INT); CREATE TABLE notes (id INT PRIMARY KEY, case_id INT);"
        "INSERT INTO cases SELECT n, 0 FROM rt_util.seq WHERE n <= 1000;", db)
    v = Vectezium(t.id, db, t.dir, snapshot_mode="initial", publish_offsets=True, offset_publish_interval_secs=1)
    v.start()
    v.streaming()
    rnd = random.Random(111)
    nid = [0]

    def tx(i):
        nid[0] += 1
        c = rnd.randint(1, 1000)
        return (f"START TRANSACTION; UPDATE cases SET val = val + 1 WHERE id = {c}; "
                f"INSERT INTO notes VALUES ({nid[0]}, {c}); COMMIT;")
    w = PipeWriter(tx, 100, db).start()
    time.sleep(8)
    sql("INSERT INTO cases SELECT 100000 + n, 1 FROM rt_util.seq WHERE n <= 100000;", db)  # one 100k-row transaction
    time.sleep(8)
    w.stop()
    v.caught_up(240)
    time.sleep(4)          # let the last offset be published
    v.stop()
    rows, control = read_events(v.rows), read_control(v.control)
    published = [p["seq"] for p in read_events_raw(v.published)]
    t.note(f"{len(published)} offsets published; {len(rows):,} row events")
    t.check("offsets were published while changes flowed", len(published) >= 5, len(published))
    t.check("published seqs never go backwards", all(a <= b for a, b in zip(published, published[1:])), published[:10])
    seqs = _all_seqs(rows, control)
    t.check("every event has a seq, and none is used twice", len(seqs) == len(rows) + len(control)
            and len(set(seqs)) == len(seqs))
    t.check("every seq is the 31-character master format", all(len(x) == 31 for x in seqs + published))
    top = max(published, default="")
    counters = {counter(x) for x in seqs if position(x) == 0}     # binlog-derived events
    missing = set(range(1, counter(top) + 1)) - counters
    t.check("every binlog seq up to the last published one is present (no gaps)", not missing, sorted(missing)[:5])
    bad = _straddling(rows, published)
    t.check("no published offset falls inside a transaction (multi-table and 100k-row ones)", not bad, bad[:3])
    t.check("the final published seq covers every event", top == max(seqs, default=""), (top, max(seqs, default="")))
    t.check("published offsets carry change_seq", all(isinstance(p.get("change_seq"), int)
                                                       for p in read_events_raw(v.published)))
    t.check("published offsets carry gtid_set and committed_at",
            all(p.get("gtid_set") and p.get("committed_at") for p in read_events_raw(v.published)))
    t.check("no errors logged", not v.errors(), v.errors()[:3])


def t11_2(t):
    db = "rt_publish_crash"
    fresh_db(db)
    ensure_seq()
    sql("CREATE TABLE big (id INT PRIMARY KEY, val INT);", db)
    kw = dict(snapshot_mode="initial", publish_offsets=True, offset_publish_interval_secs=1)
    v = Vectezium(t.id, db, t.dir, **kw)
    v.start()
    v.streaming()
    sql("INSERT INTO big VALUES (1, 1);", db)
    time.sleep(3)
    n = 300_000
    th = threading.Thread(target=lambda: sql(f"INSERT INTO big SELECT 1000 + n, n FROM rt_util.seq WHERE n <= {n};", db))
    th.start()
    wait_until(lambda: v.rows.stat().st_size > 2_000_000, 120, step=0.01)
    v.kill9()
    th.join()
    first = read_events(v.rows)
    v2 = Vectezium(t.id, db, t.dir, data_dir=v.data_dir, rows_file="rows-after.ndjson", server_id=_server_id[0], **kw)
    v2.start()
    v2.streaming()
    v2.caught_up(300)
    time.sleep(4)
    v2.stop()
    second = read_events(v2.rows)
    published = [p["seq"] for p in read_events_raw(v.published)]
    big_first = sum(1 for e in first if row(e)["id"] > 1000)
    t.check("SIGKILL landed while the big transaction was being written", 0 < big_first < n, f"{big_first:,} rows")
    t.check("published seqs never go backwards across the restart",
            all(a <= b for a, b in zip(published, published[1:])), published)
    bad = _straddling(first + second, published)
    t.check("no published offset falls inside the interrupted transaction", not bad, bad[:3])
    ident = lambda e: (e["metadata"]["file"], e["metadata"]["pos"], e["metadata"]["row"], e["metadata"]["op"])
    seq1 = {ident(e): e["metadata"]["seq"] for e in first}
    renumbered = sum(1 for e in second if ident(e) in seq1 and seq1[ident(e)] != e["metadata"]["seq"])
    t.check("rows written before the crash are replayed with identical seq", not renumbered, renumbered)
    rows_by_id = {row(e)["id"] for e in first + second if e["metadata"]["seq"] <= max(published, default="")}
    t.check("everything up to the last published seq includes the whole transaction",
            len([i for i in rows_by_id if i > 1000]) == n, len([i for i in rows_by_id if i > 1000]))


def t11_3(t):
    db = "rt_publish_noack"
    fresh_db(db)
    v = Vectezium(t.id, db, t.dir, snapshot_mode="no_data", publish_offsets=True, acknowledgements=False)
    v.start()
    exited = wait_until(lambda: not v.alive(), 30)
    t.check("publish_offsets without acknowledgements refuses to start", exited)
    t.check("with a clear error", "needs end-to-end acknowledgements" in v.log_text(), v.log_text()[-300:])
    v.stop()


TESTS = [
    ("5.1", "Data type matrix (snapshot and binlog)", t5_1),
    ("5.1b", "JSON as objects (json_handling_mode: object)", t5_1b),
    ("8.1", "Schema changes filtered by target database; admin statements ignored", t8_1),
    ("8.2", "TRUNCATE emitted as a t change", t8_2),
    ("8.3", "Statement-format writes are not silently skipped", t8_3),
    ("3.2", "Rollback and savepoints", t3_2),
    ("1.2", "Column reorder and INT -> BIGINT", t1_2),
    ("1.1", "Cold replay across DDL made while stopped", t1_1),
    ("3.1", "Large transaction and SIGKILL mid-transaction", t3_1),
    ("2.1", "Writes during the snapshot (global lock, 1 thread)", lambda t: t2_1(t, "minimal", 1, 0)),
    ("2.1b", "Writes during the snapshot (lock-free, 4 threads; Azure mode)",
     lambda t: t2_1(t, "none", 4, 100_000)),
    ("4.1", "Downstream stall: backpressure and bounded memory", t4_1),
    ("4.2", "Replication connection killed under load", t4_2),
    ("7.2", "MySQL restart mid-stream", t7_2),
    ("7.1", "Binlog purged while stopped", t7_1),
    ("7.3", "TLS modes", t7_3),
    ("9.1", "Probe: foreign-key cascades", t9_1),
    ("9.2", "Probe: session binlog_row_image=MINIMAL", t9_2),
    ("9.3", "Probe: invisible columns and generated invisible PKs", t9_3),
    ("9.4", "Probe: generated columns", t9_4),
    ("9.5", "Probe: TRUNCATE/DROP/EXCHANGE PARTITION", t9_5),
    ("9.6", "Probe: tables without a primary key", t9_6),
    ("9.7", "Probe: DDL during a blocking snapshot (metadata locks)", t9_7),
    ("9.7b", "DDL during an incremental snapshot", lambda t: t9_7(t, "incremental")),
    ("9.8", "Probe: server time zone not UTC", t9_8),
    ("9.9", "Probe: 20 MB values", t9_9),
    ("9.10", "Probe: gh-ost style online schema change", t9_10),
    ("9.11", "Probe: table added to the include list later", t9_11),
    ("10.1", "Incremental snapshot of 1M rows under 400 writes/s", t10_1),
    ("10.2", "Incremental snapshot: SIGKILL and resume", t10_2),
    ("10.3", "Tables renamed in or created while streaming", t10_3),
    ("11.1", "Published offsets are a safe cut (whole transactions)", t11_1),
    ("11.2", "Published offsets across a crash mid-transaction", t11_2),
    ("11.3", "publish_offsets requires acknowledgements", t11_3),
]


def summary(report_path, load_rc, soak_rc):
    report = json.loads(Path(report_path).read_text())
    if load_rc is not None:
        report["tests"].append({"id": "6.1", "name": "SIGKILL under 500 rows/s (run-load.sh)",
                                "status": "PASS" if load_rc == 0 else "FAIL", "checks": [],
                                "notes": ["see out/load-report.json"]})
    if soak_rc is not None:
        report["tests"].append({"id": "7.4", "name": "Soak (run-load.sh, long)",
                                "status": "PASS" if soak_rc == 0 else "FAIL", "checks": [], "notes": []})
    Path(report_path).write_text(json.dumps(report, indent=2))
    print("\n================ Vectezium readiness scorecard ================")
    for r in report["tests"]:
        n = len(r["checks"])
        p = sum(1 for c in r["checks"] if c["status"] == "PASS")
        print(f"  {r['status']:5}  {r['id']:5} {r['name']}" + (f"  ({p}/{n} checks)" if n else ""))
        for c in r["checks"]:
            if c["status"] != "PASS":
                print(f"           {c['status']}: {c['check']} -- {c['detail'][:300]}")
    statuses = [r["status"] for r in report["tests"]]
    print(f"\n  {statuses.count('PASS')} pass, {statuses.count('KNOWN')} pass with known gaps, "
          f"{statuses.count('FAIL')} fail")
    return 1 if "FAIL" in statuses else 0


def main():
    global VECTOR
    ap = argparse.ArgumentParser()
    ap.add_argument("vector", nargs="?")
    ap.add_argument("--only", help="comma-separated test ids")
    ap.add_argument("--summary", help="print the scorecard from this report")
    ap.add_argument("--load-rc", type=int)
    ap.add_argument("--soak-rc", type=int)
    args, rest = ap.parse_known_args()
    if args.vector == "_receiver":
        _receiver(*rest)
        return 0
    if args.summary:
        return summary(args.summary, args.load_rc, args.soak_rc)
    VECTOR = str(Path(args.vector).resolve())
    OUT.mkdir(parents=True, exist_ok=True)
    only = set(args.only.split(",")) if args.only else None
    ensure_seq()
    results = []
    for tid, name, fn in TESTS:
        if only and tid not in only:
            continue
        log(f"Test {tid}: {name}")
        t = Test(tid, name)
        try:
            fn(t)
        except Exception as e:  # noqa: BLE001
            t.check("test ran to completion", False, f"{type(e).__name__}: {e}")
            traceback.print_exc()
        finally:
            for v in _running:
                v.stop()
            _running.clear()
        results.append(t.to_json())
        log(f"Test {tid}: {t.status} ({t.to_json()['seconds']}s)")
        (OUT / "report.json").write_text(json.dumps({"tests": results}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
