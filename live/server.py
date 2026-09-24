"""Live page: pushes the changes of views to the browser, from one of 2 sources.

- fluss (default): the tables that the Flink job writes (k8s/flink-live.sql), read by fluss_source.py.
- risingwave: RisingWave subscriptions (CREATE SUBSCRIPTION in k8s/init.sql). One thread reads each
  subscription. Each change is a row with an `op` column.

Each open page chooses a source and an asset, and gets those changes as Server-Sent Events.
"""

import json
import os
import queue
import re
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import psycopg

from fluss_source import FlussSource

DSN = os.environ.get("RW_DSN", "host=risingwave port=4566 user=root dbname=dev")
INIT_SQL = Path(os.environ.get("INIT_SQL", "/sql/init.sql"))
FLINK_SQL = Path(os.environ.get("FLINK_SQL", "/flink-sql/live.sql"))
SOURCES = ("fluss", "risingwave")
STATIC = Path(__file__).with_name("static")
CONTENT_TYPES = {".html": "text/html", ".css": "text/css", ".js": "text/javascript"}
SUBSCRIPTIONS = {"trade": "ticks_sub", "latest": "latest_sub", "gap": "price_gap_sub"}
SHOWN_VIEWS = ("latest", "price_gap")

# Event queue of each open page -> (source, asset) that page shows.
clients: dict[queue.Queue, tuple[str, str]] = {}
clients_lock = threading.Lock()


def to_json(obj: dict) -> str:
    return json.dumps(obj, default=lambda o: o.isoformat() if isinstance(o, datetime) else str(o))


def query(sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    with psycopg.connect(DSN, autocommit=True) as conn:
        cur = conn.execute(sql, params)
        cols = [c.name for c in cur.description]
        return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def event(kind: str, row: dict[str, Any]) -> dict[str, Any]:
    row.pop("rw_timestamp", None)
    if kind == "trade":
        row["asset"] = row["product_id"].split("-")[0]
    return {"kind": kind, **row, "source": "risingwave"}


def publish(ev: dict[str, Any]) -> None:
    data = to_json(ev)
    with clients_lock:
        targets = [
            q
            for q, (source, asset) in clients.items()
            if ev["source"] == source and (ev["kind"] == "gap" or ev["asset"] == asset)
        ]
    for q in targets:
        try:
            q.put_nowait(data)
        except queue.Full:
            pass  # a slow page misses events; it must not hold up the others


def follow(kind: str, subscription: str) -> None:
    """Read the changes of one subscription for ever and publish them. Reconnect on error."""
    while True:
        try:
            with psycopg.connect(DSN, autocommit=True) as conn:
                conn.execute(f"DECLARE cur SUBSCRIPTION CURSOR FOR {subscription}")
                while True:
                    cur = conn.execute("FETCH 1000 FROM cur WITH (timeout = '1s')")
                    cols = [c.name for c in cur.description]
                    for values in cur.fetchall():
                        row = dict(zip(cols, values, strict=True))
                        op = row.pop("op")
                        # An update is UpdateDelete (old row) + UpdateInsert (new row): the new row is enough.
                        # A Delete on ticks is a trade that leaves the 15-minute window: the tape does not show it.
                        if op in ("Insert", "UpdateInsert") or (op == "Delete" and kind == "gap"):
                            publish(event(kind, row) | {"op": op})
        except psycopg.Error as e:
            print(f"{subscription}: {e!r}; reconnect in 2 s", flush=True)
            time.sleep(2)


def snapshot(source: str, asset: str) -> list[dict[str, Any]]:
    """The current rows, so a new page does not start empty."""
    if source == "fluss":
        return fluss_source.snapshot(asset)
    # More than the 30 tape rows: the page joins fills of one order and hides small orders.
    trades = query("SELECT * FROM ticks WHERE product_id LIKE %s ORDER BY ts DESC LIMIT 500", (asset + "-%",))
    latest = query("SELECT * FROM latest WHERE asset = %s", (asset,))
    gaps = query("SELECT * FROM price_gap")
    return (
        [event("trade", r) for r in reversed(trades)]
        + [event("latest", r) for r in latest]
        + [event("gap", r) for r in gaps]
    )


def view_sql(source: str, name: str) -> str:
    """The SQL that computes a view: RisingWave's CREATE statement, or the Flink INSERT for the Fluss table."""
    if source == "fluss":
        m = re.search(rf"INSERT INTO fluss\.crypto\.{name} .*?;", FLINK_SQL.read_text(), re.S)
    else:
        m = re.search(rf"CREATE MATERIALIZED VIEW IF NOT EXISTS {name} AS\n.*?;", INIT_SQL.read_text(), re.S)
    return m[0] if m else ""


def assets(source: str) -> list[str]:
    if source == "fluss":
        return fluss_source.assets()
    rows = query(
        "SELECT asset FROM latest GROUP BY asset "
        "ORDER BY count(*) DESC, sum(volume_24h * price) DESC NULLS LAST LIMIT 200"
    )
    return [r["asset"] for r in rows]


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        url = urlparse(self.path)
        params = parse_qs(url.query)
        source = params.get("source", ["fluss"])[0]
        if source not in SOURCES:
            self.send_error(400, "source must be fluss or risingwave")
            return
        if url.path == "/":
            self.static("index.html")
        elif url.path.startswith("/static/"):
            self.static(url.path.removeprefix("/static/"))
        elif url.path == "/assets":
            self.reply(json.dumps(assets(source)).encode(), "application/json")
        elif url.path == "/sql":
            self.reply(json.dumps({v: view_sql(source, v) for v in SHOWN_VIEWS}).encode(), "application/json")
        elif url.path == "/events":
            self.stream(source, params.get("asset", ["BTC"])[0])
        else:
            self.send_error(404)

    def static(self, name: str) -> None:
        path = (STATIC / name).resolve()
        # Only files inside static/, and only the types the page uses.
        if not path.is_relative_to(STATIC.resolve()) or path.suffix not in CONTENT_TYPES or not path.is_file():
            self.send_error(404)
            return
        self.reply(path.read_bytes(), CONTENT_TYPES[path.suffix] + "; charset=utf-8")

    def reply(self, body: bytes, content_type: str) -> None:
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def stream(self, source: str, asset: str) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        q: queue.Queue = queue.Queue(maxsize=10000)
        # Register before the snapshot, so no change falls between the snapshot and the stream.
        with clients_lock:
            clients[q] = (source, asset)
        try:
            for ev in snapshot(source, asset):
                self.send_event(to_json(ev | {"snapshot": True}))
            while True:
                try:
                    self.send_event(q.get(timeout=15))
                except queue.Empty:
                    self.wfile.write(b": keep-alive\n\n")
                    self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass  # the page was closed
        finally:
            with clients_lock:
                clients.pop(q, None)

    def send_event(self, data: str) -> None:
        self.wfile.write(f"data: {data}\n\n".encode())
        self.wfile.flush()

    def log_message(self, format: str, *args: Any) -> None:
        pass  # no line for each request


fluss_source: FlussSource  # set at startup

if __name__ == "__main__":
    fluss_source = FlussSource(publish)
    for kind, subscription in SUBSCRIPTIONS.items():
        threading.Thread(target=follow, args=(kind, subscription), daemon=True).start()
    server = ThreadingHTTPServer(("", 8000), Handler)
    server.daemon_threads = True
    print("live page on :8000", flush=True)
    server.serve_forever()
