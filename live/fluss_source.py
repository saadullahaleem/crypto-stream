"""The live page's Fluss source: follows the tables that the Flink job writes (k8s/flink-live.sql).

The Fluss Python client is asynchronous, so this module runs its own event loop in a background thread.
Each change goes to the same publish() function as RisingWave's changes, tagged with source "fluss".
"""

import asyncio
import os
import threading
from collections.abc import Callable
from typing import Any

import fluss

BOOTSTRAP = os.environ.get("FLUSS_BOOTSTRAP", "fluss.crypto.svc.cluster.local:9123")
DATABASE = "crypto"
TAPE_TAIL = 1500  # the newest trades read from each bucket of the trades table for a new page

Event = dict[str, Any]


class FlussSource:
    def __init__(self, publish: Callable[[Event], None]):
        self._publish = publish
        self._loop = asyncio.new_event_loop()
        self._conn = None
        threading.Thread(target=self._loop.run_forever, daemon=True).start()
        for table in ("trades", "latest", "price_gap"):
            asyncio.run_coroutine_threadsafe(self._follow(table), self._loop)

    def snapshot(self, asset: str) -> list[Event]:
        """The current rows for a new page. Called from a request thread."""
        return asyncio.run_coroutine_threadsafe(self._snapshot(asset), self._loop).result(timeout=30)

    def assets(self) -> list[str]:
        """Coins on the most exchanges first, then by 24-hour volume."""
        rows = asyncio.run_coroutine_threadsafe(self._current_rows("latest"), self._loop).result(timeout=30)
        stats: dict[str, list[float]] = {}
        for r in rows:
            s = stats.setdefault(r["asset"], [0, 0.0])
            s[0] += 1
            s[1] += (r["volume_24h"] or 0) * (r["price"] or 0)
        return sorted(stats, key=lambda a: (-stats[a][0], -stats[a][1]))[:200]

    async def _connection(self):
        if self._conn is None:
            self._conn = await fluss.FlussConnection.create(fluss.Config({"bootstrap.servers": BOOTSTRAP}))
        return self._conn

    async def _table(self, name: str):
        conn = await self._connection()
        path = fluss.TablePath(DATABASE, name)
        admin = conn.get_admin()
        info = await admin.get_table_info(path)
        return await conn.get_table(path), path, admin, info.num_buckets

    async def _follow(self, name: str) -> None:
        """Read the table's log (trades) or change log (latest, price_gap) from its end, for ever."""
        kind = {"trades": "trade", "latest": "latest", "price_gap": "gap"}[name]
        while True:
            try:
                table, path, admin, buckets = await self._table(name)
                offsets = await admin.list_offsets(path, list(range(buckets)), fluss.OffsetSpec.latest())
                scanner = await table.new_scan().create_log_scanner()
                scanner.subscribe_buckets(offsets)
                while True:
                    for record in await scanner.poll(1000):
                        change = record.change_type.short_string()
                        # +A: a row appended to a log table (trades). +I and +U carry the new row of a primary-key
                        # table; -U is the old row of an update. -D matters only for price_gap, where a coin leaves.
                        if change in ("+A", "+I", "+U"):
                            self._publish(_event(kind, record.row, "Insert"))
                        elif change == "-D" and kind == "gap":
                            self._publish(_event(kind, record.row, "Delete"))
            except Exception as e:  # noqa: BLE001 - the table may not exist yet, or Fluss restarts: try again
                print(f"fluss {name}: {e!r}; again in 5 s", flush=True)
                self._conn = None
                await asyncio.sleep(5)

    async def _current_rows(self, name: str) -> list[dict]:
        """All rows of a primary-key table, from a limit scan of each bucket."""
        table, _, _, buckets = await self._table(name)
        table_id = table.get_table_info().table_id
        rows = []
        for b in range(buckets):
            scanner = table.new_scan().limit(100_000).create_bucket_batch_scanner(fluss.TableBucket(table_id, b))
            rows += (await scanner.to_arrow()).to_pylist()
        return rows

    async def _recent_trades(self, asset: str) -> list[dict]:
        table, path, admin, buckets = await self._table("trades")
        ends = await admin.list_offsets(path, list(range(buckets)), fluss.OffsetSpec.latest())
        scanner = await table.new_scan().create_log_scanner()
        scanner.subscribe_buckets({b: max(0, end - TAPE_TAIL) for b, end in ends.items()})
        trades, remaining = [], sum(min(end, TAPE_TAIL) for end in ends.values())
        while remaining > 0:
            batch = list(await scanner.poll(1000))
            if not batch:
                break
            remaining -= len(batch)
            trades += [r.row for r in batch if r.row["asset"] == asset]
        return sorted(trades, key=lambda r: r["ts"])[-500:]

    async def _snapshot(self, asset: str) -> list[Event]:
        latest = [r for r in await self._current_rows("latest") if r["asset"] == asset]
        return (
            [_event("trade", r, "Insert") for r in await self._recent_trades(asset)]
            + [_event("latest", r, "Insert") for r in latest]
            + [_event("gap", r, "Insert") for r in await self._current_rows("price_gap")]
        )


def _event(kind: str, row: dict, op: str) -> Event:
    return {"kind": kind, **row, "op": op, "source": "fluss"}
