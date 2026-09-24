# Exchange feeds

Coinbase (`ticker`), Kraken (websocket v2 `trade` and `ticker`), Binance (`@trade`, `@ticker`) and OKX
(`trades`, `tickers`). Checked on 2026-09-23.

## 1. Trade IDs count up by 1 on Coinbase, Kraken and Binance

**What we measured:** in about 55,000 trades over 2 minutes, each product's trade IDs went up by exactly 1,
with no missing ID, once the order in Connect was fixed ([redpanda-connect.md](redpanda-connect.md), lessons 1 and 2).

**Why it matters:** one field does 3 jobs.
- Gap detection: a missing ID is a lost trade.
- Deduplication: the same ID twice is the same trade.
- Backfill: the REST APIs can fetch trades from a given trade ID.

## 2. OKX `trades` leaves out trades

**What we measured:** about 20% of OKX trade IDs never arrived (5,577 missing IDs, 19,617 received).

**Cause:** the `trades` channel sends one message for each taker order, even when the order filled several
resting orders. The IDs of the other fills are skipped.

**Fix (planned):** the `trades-all` channel, which sends every trade. Check it when we build gap detection.

## 3. Binance rejects a subscribe message over 4,096 bytes

**What happened:** 2 of 3 Binance connections got `Invalid JSON: EOF while parsing a string at line 1 column 4096`.

**Cause:** 200 symbols × 2 streams make a subscribe message of about 9,000 bytes. Binance cuts it at 4,096.

**Fix:** `MAX_CHUNK = 80` in [worker/binance.py](../../worker/binance.py) (about 3,000 bytes). Binance also allows at
most 1,024 streams on one connection.

## 4. One taker order becomes many trades

**What happened:** the trade tape showed 16 to 80 Binance rows with the same time, side and price.

**Cause:** an exchange sends one trade for each resting order that a taker order fills.

**Fix:** the live page joins trades with the same exchange, product, side and time into one row, with a
`fills` count ([live/static/js/tape.js](../../live/static/js/tape.js)).

## 5. Smaller points

| Exchange | Point |
|---|---|
| Kraken | The REST API uses the old asset names `XBT` and `XDG`. Websocket v2 uses `BTC` and `DOGE`. |
| Kraken | `ticker` updates only on trades by default (`event_trigger: trades`). A bid or ask change without a trade is not sent. |
| Binance | `@ticker` sends the best bid and ask once each second, so the Binance spread can be 1 second old. `@bookTicker` is real time. |
| Binance | Binance.com worked from this network. Some regions get HTTP 451 and need Binance.US. |
| OKX | `tickers` sends `bidPx: ""` for a market with no orders on one side. That is not an error: there is no top of book. |
| OKX | The REST API returns 403 without a `User-Agent` header. Coinbase also needs one. |
| All | Symbol counts: Coinbase 402 USD pairs, Kraken 622, Binance 496 USDT pairs, OKX 406. |
| All | The same ticker can be 2 different tokens on 2 exchanges (for example old and new LUNA). The `price_gap` view leaves out gaps over 5%. |

## 6. Delay from the exchange

| Exchange | p50, exchange → worker |
|---|---|
| Coinbase | 27 ms |
| Kraken | 74 ms |
| OKX | 78 ms |
| Binance | 90 ms |

Most of this time is before Connect: at the exchange and on the network. Light in fiber needs about 5 ms for each
1,000 km, and Asia is about 11,000 km of cable away. Our own part (Connect, Redpanda, worker) was about 17 ms,
and it is now about 30 ms because of the message-order fix.
