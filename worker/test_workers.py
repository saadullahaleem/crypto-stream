"""Run: python test_workers.py"""

import json
import time

import binance
import coinbase
import kraken
import okx
from utils import Quote, Trade, process


def feed(module, msg: dict, book: dict[str, Quote]) -> list[Trade]:
    """Process one raw message of an exchange, as the worker does."""
    return process(module.__name__, module.parse, json.dumps(msg), book)


# Coinbase: one ticker message is a quote and a trade together.
book = {}
cb = {
    "type": "ticker",
    "product_id": "BTC-USD",
    "time": "2026-01-01T00:00:00Z",
    "side": "buy",
    "price": "100",
    "last_size": "0.5",
    "best_bid": "99",
    "best_ask": "101",
    "volume_24h": "10",
}
[t] = feed(coinbase, cb, book)
assert (t.spread, t.price, t.ts.year) == (2.0, 100.0, 2026)
assert feed(coinbase, {"type": "subscriptions"}, book) == []

# Kraken: the quote comes first on its own channel, then the trade gets it.
book = {}
kraken_ticker = {"channel": "ticker", "data": [{"symbol": "BTC/USD", "bid": 99.5, "ask": 100.5, "volume": 7}]}
assert feed(kraken, kraken_ticker, book) == []
kraken_trade = {
    "channel": "trade",
    "data": [{"symbol": "BTC/USD", "side": "sell", "price": 100, "qty": 1, "timestamp": "2026-01-01T00:00:00Z"}],
}
[t] = feed(kraken, kraken_trade, book)
assert (t.product_id, t.side, t.spread) == ("BTC-USD", "sell", 1.0)
assert feed(kraken, {"channel": "heartbeat"}, book) == []

# Binance: a trade before any quote has no spread, and the field is left out of the JSON.
book = {}
binance_trade = {"data": {"e": "trade", "s": "ETHUSDT", "p": "2", "q": "3", "T": 1790188652202, "m": True}}
[t] = feed(binance, binance_trade, book)
assert (t.product_id, t.side, t.ts.year, t.spread) == ("ETH-USDT", "sell", 2026, None)
assert "spread" not in t.model_dump_json(exclude_none=True)

# OKX: millisecond timestamps as strings.
book = {}
okx_ticker = {
    "arg": {"channel": "tickers"},
    "data": [{"instId": "SOL-USDT", "bidPx": "1", "askPx": "1.5", "vol24h": "9"}],
}
feed(okx, okx_ticker, book)
okx_trade = {
    "arg": {"channel": "trades"},
    "data": [{"instId": "SOL-USDT", "px": "1.2", "sz": "4", "side": "buy", "ts": "1790188652202"}],
}
[t] = feed(okx, okx_trade, book)
assert (t.price, t.spread, t.volume_24h, t.ts.year) == (1.2, 0.5, 9.0, 2026)

# OKX sends an empty bid or ask for a market with no orders on one side: not an error, no quote.
okx_empty = {"arg": {"channel": "tickers"}, "data": [{"instId": "X-USDT", "bidPx": "", "askPx": "", "vol24h": "0"}]}
assert feed(okx, okx_empty, {}) == []

# A message that breaks the schema must raise ValueError, so the worker skips it.
okx_bad = {"arg": {"channel": "trades"}, "data": [{"instId": "X", "px": "abc", "sz": "1", "side": "buy", "ts": "1"}]}
try:
    feed(okx, okx_bad, {})
    raise AssertionError("bad price passed")
except ValueError:
    pass

# Throughput of one worker, to size replicas against the feed rates.
msg = json.dumps({"data": {"e": "trade", "s": "BTCUSDT", "p": "84000.1", "q": "0.01", "T": 1790188652202, "m": False}})
n, start = 20000, time.perf_counter()
for _ in range(n):
    process("binance", binance.parse, msg, book)
print(f"ok; one worker parses about {n / (time.perf_counter() - start):,.0f} msg/s")
