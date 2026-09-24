# RisingWave

RisingWave 3.1, `single_node` mode. The SQL is in [k8s/init.sql](../../k8s/init.sql).

## 1. The watermark drops a late exchange because of the other exchanges

**What happened:** RisingWave had only 47 OKX trades from the last 2 minutes, and 27,332 Binance trades.

**Cause:** `WATERMARK FOR ts AS ts - INTERVAL '30 seconds'` drops each trade that is more than 30 seconds older
than the newest trade seen. Each `processed-data` partition has trades from all exchanges. Binance and Coinbase
were on time and moved the watermark forward. OKX was 2 minutes late (see
[redpanda-connect.md](redpanda-connect.md), lesson 3), so its trades were too old and were dropped.

**Fix:** we fixed the OKX delay. The watermark is still 30 s.

**Options:** a longer watermark (more open windows in memory), or one `processed-data` topic for each exchange,
so each exchange has its own watermark.

## 2. Put the time filter after the aggregation

**Cause:** `WHERE ts > now() - INTERVAL '1 day'` before a `GROUP BY` makes RisingWave remove each old trade from
its window again. To do that, it must store every trade.

**Fix:** filter the result: `SELECT * FROM (... GROUP BY ...) WHERE window_start > now() - INTERVAL '1 day'`.
The watermark lets RisingWave delete the state of closed windows.

## 3. `single_node` stops at startup with too little memory

**What happened:** exit code 139, with
`assertion failed: compactor_memory_limit_bytes > min_compactor_memory_limit_bytes as usize * 2`.

**Cause:** in `single_node`, the built-in compactor gets a part of the memory limit. With a 6 GiB limit, that
part was too small.

**Fix:** a memory limit of 8 GiB.

## 4. Metrics listen on 127.0.0.1 only

**What happened:** the collector could not scrape RisingWave (`connection refused`), but `curl localhost:1260`
inside the pod worked.

**Fix:** `RW_SINGLE_NODE_PROMETHEUS_LISTENER_ADDR=0.0.0.0:1250`.

## 5. `now()` follows the checkpoint time

**What happened:** `now() - max(ts)` was about −1 s, so I concluded that the Docker clock was 1 second behind.
That was wrong.

**Cause:** `now()` in RisingWave follows its checkpoint (epoch) time, which can be up to about 1 second behind
real time. A direct check against Binance's server time showed that the pod clock was within about 0.1 s.

## 6. RisingWave's consumer groups show a lag that means nothing

**What happened:** `rw-consumer-9` showed a lag of 143,000 that kept growing.

**Cause:** RisingWave stores its Kafka offsets in its own checkpoints and does not commit them to Redpanda, so
Redpanda's lag formula uses an old offset.

**Fix:** the lag panel shows only the `*-worker` groups. To see if RisingWave falls behind, use its rows read each
second and its barrier latency.

## 7. A restart deleted every source, view and subscription

**What happened:** after a restart of the computer, the Grafana market dashboard and the live page showed no data.
The Pipeline Health dashboard still worked, because it reads Prometheus.

**Cause:** `single_node` kept its metadata and data inside the container. The restart gave it a new, empty
container, so RisingWave had no source, no views and no subscriptions. The `rw-init` Job that makes them had
run once, on the first deploy, and Kubernetes had deleted it 2 minutes later (`ttlSecondsAfterFinished`), so
nothing made them again.

**Fix:** a 20 GiB PersistentVolumeClaim, and `RW_SINGLE_NODE_STORE_DIRECTORY=/data`
([k8s/infra.yaml](../../k8s/infra.yaml)). Tested: after a deleted pod, all 10 objects were still there, and the
candle rows kept growing (34,500 → 34,580).

**My mistake:** a comment and the architecture document said that a restart builds the views again from
`processed-data`. I wrote that without a test. A restart test on the first day would have shown the problem.

## 8. New rows become visible only at each checkpoint

**What we measured:** a trade reached a subscriber about 920 ms after it happened (p50, NTP-corrected), and
RisingWave's checkpoint came every 1,000 ms (`barrier_interval_ms`). Views and subscriptions show new rows only
after the next checkpoint, so that interval was the largest wait in the whole pipeline.

**Change:** `ALTER SYSTEM SET barrier_interval_ms = 250` (in [k8s/init.sql](../../k8s/init.sql)). Result: about
460 ms (p50). Cost: RisingWave CPU 1.24 → 2.95 cores. A checkpoint still takes 67 ms (p50) and 219 ms (p99), so
checkpoints do not queue up. Below about 250 ms, they would start to.

## 9. Smaller points

- `CREATE SOURCE` fails if the Kafka topic does not exist yet. The `rw-init` Job retries until the topics Job is done.
- Subscriptions push the changes of a view: `CREATE SUBSCRIPTION`, then `DECLARE ... SUBSCRIPTION CURSOR` and
  `FETCH n FROM cur WITH (timeout = '1s')`. Each row has an `op` column: `Insert`, `Delete`, `UpdateDelete`,
  `UpdateInsert`. The live page uses this.
- `first_value(x ORDER BY ts)` and `last_value(...)` work in streaming views (we use them for candles and `latest`).
