"""Live page: pushes the changes of RisingWave views to the browser.

RisingWave subscriptions (CREATE SUBSCRIPTION in k8s/init.sql) give each change of a view as a row with an
`op` column. One thread reads each subscription and passes the changes to the open pages as Server-Sent Events.
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

DSN = os.environ.get("RW_DSN", "host=risingwave port=4566 user=root dbname=dev")
INIT_SQL = Path(os.environ.get("INIT_SQL", "/sql/init.sql"))
STATIC = Path(__file__).with_name("static")
CONTENT_TYPES = {".html": "text/html", ".css": "text/css", ".js": "text/javascript"}
SUBSCRIPTIONS = {"trade": "ticks_sub", "latest": "latest_sub", "gap": "price_gap_sub"}
SHOWN_VIEWS = ("latest", "price_gap")

# Event queue of each open page -> the asset that page shows.
clients: dict[queue.Queue, str] = {}
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
    return {"kind": kind, **row}


def publish(ev: dict[str, Any]) -> None:
    data = to_json(ev)
    with clients_lock:
        targets = [q for q, asset in clients.items() if ev["kind"] == "gap" or ev["asset"] == asset]
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


def snapshot(asset: str) -> list[dict[str, Any]]:
    """The current rows, so a new page does not start empty."""
    # More than the 30 tape rows: the page joins fills of one order and hides small orders.
    trades = query("SELECT * FROM ticks WHERE product_id LIKE %s ORDER BY ts DESC LIMIT 500", (asset + "-%",))
    latest = query("SELECT * FROM latest WHERE asset = %s", (asset,))
    gaps = query("SELECT * FROM price_gap")
    return (
        [event("trade", r) for r in reversed(trades)]
        + [event("latest", r) for r in latest]
        + [event("gap", r) for r in gaps]
    )


def view_sql(name: str) -> str:
    """The CREATE statement of a view, as written in init.sql."""
    m = re.search(rf"CREATE MATERIALIZED VIEW IF NOT EXISTS {name} AS\n.*?;", INIT_SQL.read_text(), re.S)
    return m[0] if m else ""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        url = urlparse(self.path)
        if url.path == "/":
            self.static("index.html")
        elif url.path.startswith("/static/"):
            self.static(url.path.removeprefix("/static/"))
        elif url.path == "/assets":
            rows = query(
                "SELECT asset FROM latest GROUP BY asset "
                "ORDER BY count(*) DESC, sum(volume_24h * price) DESC NULLS LAST LIMIT 200"
            )
            self.reply(json.dumps([r["asset"] for r in rows]).encode(), "application/json")
        elif url.path == "/sql":
            self.reply(json.dumps({v: view_sql(v) for v in SHOWN_VIEWS}).encode(), "application/json")
        elif url.path == "/events":
            self.stream(parse_qs(url.query).get("asset", ["BTC"])[0])
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

    def stream(self, asset: str) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        q: queue.Queue = queue.Queue(maxsize=10000)
        # Register before the snapshot, so no change falls between the snapshot and the stream.
        with clients_lock:
            clients[q] = asset
        try:
            for ev in snapshot(asset):
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


if __name__ == "__main__":
    for kind, subscription in SUBSCRIPTIONS.items():
        threading.Thread(target=follow, args=(kind, subscription), daemon=True).start()
    server = ThreadingHTTPServer(("", 8000), Handler)
    server.daemon_threads = True
    print("live page on :8000", flush=True)
    server.serve_forever()
