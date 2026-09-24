# Measuring and debugging

Claims I made on the first day that a measurement later proved wrong, and what the measurement showed.

| # | Claim | Measurement | Result |
|---|---|---|---|
| 1 | "The OKX delay came from the 4-minute pause test." | The trade-latency metric after the test | Wrong. Connect was 2 minutes behind, and that problem existed before the test. |
| 2 | "The Redpanda ack is slow, so a faster ack will help." | `redpanda_kafka_request_latency_seconds` | Wrong. Redpanda acks in 0.8 ms. The wait was the Kafka client's 10 ms linger. |
| 3 | "The Docker clock is about 1 second behind the exchanges." | Pod time compared with Binance's server time | Wrong. The difference was about 0.1 s or less. RisingWave's `now()` had misled me. |
| 4 | "`max_in_flight` reorders the messages." | Trade-ID order, with each setting changed alone | Half right. Pipeline threads also reorder, and each cause alone is enough. |
| 5 | "Connect needs only 10 ms for each message." | Trade-ID order at that setting | That speed came from writes out of order. With the order kept, it is about 24 ms. |

## What worked

- **Measure before you change something.** Claim 2 would have led to changes in Redpanda that could not help.
- **Change one setting at a time.** Claim 4 looked fixed only when both settings changed.
- **Check the data itself, not only the metrics.** The trade IDs in the raw topics showed the order problem.
  No dashboard showed it.
- **Check that a test measures the new code.** One order test ran on the old config, because my edit had not
  applied. The result looked like "the fix does not work".
- **Keep a small script for each check,** so the same check runs before and after a change: trade-ID order
  and holes, Connect span times, exchange-to-Kafka delay.

## Useful numbers

| What | Value |
|---|---|
| Raw messages, all 1,926 symbols | about 1,800 each second (OKX about 1,230) |
| Trades to RisingWave | about 400 each second |
| One Python worker, parse only | about 100,000 messages each second |
| One Python worker, in the cluster | about 14,000 messages each second |
| Worker parse time | p50 25 to 78 µs, p99 230 to 450 µs |
| Redpanda produce ack | 0.8 ms p50, 3.9 ms p99 |
| Connect, each message | about 24 ms (with order kept) |
| Exchange → worker, p50 | Coinbase 27 ms, Kraken 74 ms, OKX 78 ms, Binance 90 ms |
