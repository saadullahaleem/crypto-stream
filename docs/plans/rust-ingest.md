# Plan: Rust ingest and Rust workers

Status: proposed, 2026-09-24. Not started.

## Goal

Replace Redpanda Connect and the Python workers with 2 Rust services, so the pipeline can take L2 order-book data
(estimate: 4–25 MB/s and 4,000–80,000 messages/s, against 0.5 MB/s and 1,600 messages/s now).
The Python worker code stays in the repo. After the cutover, Kubernetes no longer runs it.

After this work: gap detection, data fidelity, and completeness reports that the user can see.

## Decisions

| Topic | Decision | Source |
|---|---|---|
| Who writes the code | Claude writes all of it, including reconnect and gap detection | user, 2026-09-24 |
| Repo | A Cargo workspace in `crypto-stream/rust/`, started from `market-ingest` commit `2885761` | user |
| Output format | Avro with the schema registry, from the start | user |
| Redundancy | 1 connection per symbol chunk. Gap detection and REST backfill fill the holes later | user |
| market-ingest design | Keep the envelope, the traits, `Draft::stamp` and the error classes. Do not connect to the exchange in the worker | user |

## The 2 services

```
exchange websockets
      │
      ▼
ingest (Rust, 1 pod per exchange)     holds the sockets, adds receive time, no full parse
      │  raw frame + headers
      ▼
<exchange>-raw  (Redpanda, 12 partitions, key = product)
      │
      ▼
worker (Rust, 1–12 pods per exchange, KEDA)   parse, validate, stamp the envelope, Avro
      │
      ▼
trades  (Redpanda, Avro, key = exchange:product)
      │
      ├──▶ RisingWave (Grafana)
      └──▶ Flink + Fluss (live page)
```

### Ingest

- **Stack:** tokio, tokio-tungstenite with rustls, rdkafka (librdkafka).
- **Order without the 24 ms wait:** an idempotent producer (`enable.idempotence=true`) keeps the order in each
  partition with up to 5 requests in flight. Connect needed `max_in_flight: 1` for that. Target `linger.ms` 5.
- **Kafka key:** the product. The ingest reads only that one field from the frame (serde with a borrowed struct).
  It does not parse the rest.
- **Headers on each message:**
  - `receive_time_ms`: NTP-corrected (a Rust copy of `worker/clock.py`)
  - `conn_id`
  - `conn_seq`: the message number on that connection. A hole in `conn_seq` means a loss inside our pipeline, not at
    the exchange.
  - `traceparent`: on 1% of messages.
- **Connections:**
  - One tokio task for each connection of up to `CHUNK` symbols.
  - Reconnect with exponential backoff and jitter.
  - A connection with no message for N seconds is closed and opened again.
  - Pings as each exchange requires: OKX needs a text `ping` every 30 s or less.
- **Planned reconnects without a gap:** Binance closes each connection after 24 h. The ingest opens the new
  connection first, then closes the old one. The overlap makes duplicates, and the `event_id` removes them
  downstream.
- **Symbol list:** from the exchange's REST API at start, and again every 10 minutes.
- **Which symbol goes on which connection:**
  - At start, the ingest sorts the symbols by 24 h trade count, from the REST API.
  - It puts each symbol on the connection with the lowest total trade count that still has space.
  - Then each connection carries about the same load. Now (Connect), the chunks are alphabetical, and a chunk can
    get many busy symbols.
  - After the start, a symbol stays on its connection. A new symbol goes to the connection with the lowest load. A
    delisted symbol is unsubscribed. No other connection changes.
- **Connection events** (open, close, the reason, the time) go to the topic `ingest-events`. Gap detection uses
  them: a closed connection is a time range where gaps are possible.
- **Channel changes for gap detection:**
  - Coinbase: `matches` (every trade, with `trade_id` and `sequence`) plus `ticker` (for the quote). The
    `market-ingest` adapter already reads `matches`.
  - OKX: `trades-all` on the business endpoint. `trades` skips trade IDs.

### Worker

- rdkafka consumer, `cooperative-sticky`, the same KEDA settings as now.
- **Parsing:** one `SourceAdapter` for each exchange (the `market-ingest` contract). `Draft::stamp` adds the
  worker's fields.
- **At least once, with commits off the hot path:**
  - Settings: `enable.auto.commit=true` and `enable.auto.offset.store=false`.
  - The produce delivery callback stores the input offset in memory (`store_offset`). This is a cheap call.
  - librdkafka's own background thread commits the stored offsets every second, so the poll loop never waits for
    a commit.
  - Output records can be confirmed out of order, because they go to different partitions. So for each input
    partition, the worker stores only the offset below the oldest record that is not yet confirmed. Otherwise a
    crash could commit an offset that was not written and lose data. Duplicates are the worst case.
  - After a crash, up to about 1 s of records come again. The `event_id` removes the copies downstream.
- **The last quote** of each product stays in memory and goes into each trade, the same as now. That keeps
  `spread` on the dashboards.

### The envelope on the wire: changes to the market-ingest design

1. **One topic for each record type, with a flat Avro record.** Not one topic with a `payload` union. Flink's
   Avro format cannot read a union of records. In Rust, the `Payload` enum stays; it selects the topic. L2 adds a
   `book` topic later.
2. **No `raw` bytes in the envelope.** The record carries `raw_partition` and `raw_offset` instead: the raw frame
   is in `<exchange>-raw`. With L2, a copy of the raw frame in each record would double the data.
3. **Prices and sizes:** Avro `decimal(38, 18)`. It holds 0.000000001234 (small coins) and 100,000 (BTC)
   exactly.
4. **`trace_id` only in the `traceparent` header**, not in the record.

The `trades` record (Avro):

- **Base:** `exchange`, `symbol`, `event_time` and `receive_time` (both `timestamp-millis`), `event_id`,
  `event_id_origin`, `sequence` (nullable), `raw_partition`, `raw_offset`.
- **Trade:** `price`, `size`, `side` (taker: `buy`, `sell` or `unknown`), `currency`.
- **Quote context (nullable):** `best_bid`, `best_ask`, `volume_24h`.

## Migration: side by side, then switch

The old path (Connect → `*-raw-feed` → Python → `processed-data`) runs until phase 3 ends. The new path uses new
topics, so both paths can be compared on the same live data.

| Phase | Work | Check before the next phase | Estimate (Claude's time) |
|---|---|---|---|
| 0 | Workspace in `rust/`, copied from market-ingest. One Docker image with the 2 binaries (cargo-chef for the build cache). `cargo test` and `clippy` pass | The image builds and imports into the cluster | 30 min |
| 1 | Ingest for the 4 exchanges → `<exchange>-raw`, `ingest-events` | 24 h beside Connect: per-product message counts within 0.1% of Connect's. Trade IDs in order in each partition. Ingest p50 time under 10 ms | 3–4 h, then 24 h of running |
| 2 | Worker for the 4 exchanges → `trades` (Avro), KEDA | Trades per exchange per minute equal to Python's `processed-data`. A replay test: messages/s per CPU core | 3–4 h |
| 3 | RisingWave source and Flink table read `trades` (Avro). The views keep their column names, so Grafana and the live page do not change | Both dashboards show data. Freshness p50 not worse than now (Fluss 393 ms, RisingWave 456 ms) | 2 h |
| 4 | Remove Connect and the Python workers from `kustomization.yaml`. Delete the old topics. Update `architecture.md`, the README, the artifact page, and the lessons | `kubectl apply -k k8s` gives a working system | 1 h |

## Throughput targets

- **Ingest:** 50,000 messages/s in 1 pod.
- **Worker:** 20,000 messages/s for each CPU core.

Measure both with a replay: a new consumer group reads 1 h of a raw topic from the start.

## Risks to check early

1. **RisingWave and Flink read Avro `decimal`:** test with one record in phase 0, before the schema is final.
2. **The Flink image needs the `avro-confluent-registry` format jar.**
3. **Building rdkafka on Windows:** it needs cmake. Build inside Docker only.
4. **Coinbase `matches` frames** have a different shape than `ticker`. Check that the quote from `ticker` still
   joins the trade.

## After this plan

1. **Gap detection** in the worker: a hole in a product's trade IDs, and holes in `conn_seq`. Output to a
   `data-gaps` topic.
2. **Backfill** by trade ID from each exchange's REST API.
3. **Deduplication** by `(exchange, symbol, event_id)` in RisingWave and Flink.
4. **A completeness panel** in Grafana and on the live page, so that the user can see the gaps.
