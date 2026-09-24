# Redpanda Connect

Connect version 4.110. The config is made by [worker/connect_config.py](../../worker/connect_config.py).

## 1. Several pipeline threads put messages out of order

**What happened:** the trade IDs of one product arrived at Kafka out of order. On Binance, 25% to 35% of the
steps went backwards. Kafka keeps the order inside a partition, and all messages of one product go to one
partition, so the order was already wrong when Connect wrote them.

**Cause:** `pipeline.threads` is 1 for each CPU by default. Several threads run the processors at the same time,
and each message leaves when its thread is done.

**Fix:** `"pipeline": {"threads": 1}`. Our only processor is a small Bloblang mutation, so 1 thread is enough.

**How we found it:** we checked if each product's trade IDs count up by 1 (see [exchange-feeds.md](exchange-feeds.md)).
The first guess, `max_in_flight`, was only half of the cause (lesson 2).

## 2. More than 1 write in flight also puts messages out of order

**What happened:** with 1 pipeline thread and `max_in_flight: 256`, the backwards steps came back
(13,086 of 55,192 on Binance).

**Cause:** Connect sends the writes that are in flight at the same time independently, and they reach Redpanda
in any order.

**Fix:** `max_in_flight: 1`, and batches for throughput: `batching: {count: 2000, period: 5ms}`. One batch keeps
its messages in arrival order.

**Cost:** Connect needs about 24 ms for each message, compared with about 10 ms with 256 in flight. The worker
needs the order (it adds each product's last quote to its trades), and gap detection needs it (it looks for
missing trade IDs), so we pay that cost.

## 3. The default output settings cap throughput at about 860 messages each second

**What happened:** OKX trades reached Kafka 2 minutes late, and the delay kept growing. RisingWave then dropped
them (see [risingwave.md](risingwave.md), lesson 1).

**Cause:** each write waits about 11 ms (lesson 4), and by default Connect allows 10 writes in flight, one
message each. 10 ÷ 0.0116 s ≈ 860 messages each second. OKX sends 1,000 to 1,300. Connect does not drop
messages when it is behind; it reads the websocket more slowly, and the backlog waits in the network buffers.

**Fix:** batches (lesson 2). One write now carries all messages of the last 5 ms.

**How we found it:** the trade-latency metric on the Pipeline Health dashboard showed OKX at the top of its range
(30 s or more) the first time we looked.

## 4. The Kafka client waits 10 ms before each request, and we cannot change it

**What happened:** each Kafka write in Connect took about 11 ms, but Redpanda answers a produce request in
0.8 ms (p50, from `redpanda_kafka_request_latency_seconds`).

**Cause:** the `kafka_franz` output uses the franz-go client, whose default `ProducerLinger` is 10 ms: it waits
for more records before it sends a request. None of the outputs `kafka_franz`, `redpanda` or `kafka` has a
setting for this.

**Fix:** none yet. Options: a Kafka output without linger, or a Connect version that exposes the setting.

## 5. One websocket connection sends only one subscribe message

**What happened:** Kraken takes one channel in each subscribe message, and we need 2 channels (trades and quotes).

**Cause:** the `websocket` input has `open_message` (one message). It has no list of messages.

**Fix:** 2 connections for each group of Kraken symbols, one for each channel
([worker/kraken.py](../../worker/kraken.py)).

## 6. Multi-line Bloblang in a YAML flow sequence becomes one line

**What happened:** `processors: [{ mutation: 'meta a = 1` + a line break + `meta b = 2' }]` failed.

**Cause:** in a single-quoted YAML string, a line break becomes a space. Bloblang needs a new line between statements.

**Fix:** block style (`mutation: |`), or `\n` in the generated JSON config.

## 7. Smaller points

- `redpanda connect lint <file>` checks a config without running it. We run it on every generated config.
- A JSON file is valid YAML, so Python can write the config as JSON.
- `tracing_span().traceparent` in Bloblang gives the W3C trace ID of a message. With
  `metadata.include_patterns: ["^traceparent$"]` it goes into a Kafka header, and the worker continues the trace.
  Because we take it in the `mutation` step, Jaeger shows the worker span under `mutation`, not under the output span.
- The newer `redpanda` output has `inject_tracing_map`, which could fix that tree shape. Not tested.
- Connect serves Prometheus metrics on `:4195/metrics` (`output_sent_total`, `input_received_total`).
