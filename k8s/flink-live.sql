-- The Flink job for the live page: processed-data (Kafka) -> 3 Fluss tables.
-- flink/submit.sh runs this file (mounted from a ConfigMap) whenever the job "crypto-live" is not running. Every statement can run again safely.

SET 'pipeline.name' = 'crypto-live';
SET 'parallelism.default' = '1';
-- Checkpoints keep the Kafka offsets and flush the Fluss writers. The state is small (about 2,000 rows),
-- so the JobManager keeps it in memory: no shared file system needed.
SET 'execution.checkpointing.interval' = '10s';
SET 'state.checkpoint-storage' = 'jobmanager';
-- A row with a null key column is dropped. Without this, one bad record would stop the whole job.
SET 'table.exec.sink.not-null-enforcer' = 'DROP';

CREATE CATALOG fluss WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'fluss.crypto.svc.cluster.local:9123'
);
CREATE DATABASE IF NOT EXISTS fluss.crypto;

-- Every trade, for the tape. A log table: append only. Kept for 1 hour.
CREATE TABLE IF NOT EXISTS fluss.crypto.trades (
  exchange STRING, product_id STRING, asset STRING, ts TIMESTAMP_LTZ(6), side STRING,
  price DOUBLE, last_size DOUBLE, best_bid DOUBLE, best_ask DOUBLE, spread DOUBLE, volume_24h DOUBLE
) WITH ('bucket.num' = '4', 'table.log.ttl' = '1h');

-- The last trade of each exchange and product. A primary-key table: each new trade replaces the old row.
CREATE TABLE IF NOT EXISTS fluss.crypto.latest (
  exchange STRING, product_id STRING, asset STRING,
  price DOUBLE, spread DOUBLE, volume_24h DOUBLE, ts TIMESTAMP_LTZ(6),
  PRIMARY KEY (exchange, product_id) NOT ENFORCED
) WITH ('bucket.num' = '4');

-- The price gap between exchanges for each coin on 3 or more exchanges. Same rules as RisingWave's price_gap.
CREATE TABLE IF NOT EXISTS fluss.crypto.price_gap (
  asset STRING, exchanges BIGINT, low DOUBLE, low_exchange STRING, high DOUBLE, high_exchange STRING, gap_bps DOUBLE,
  PRIMARY KEY (asset) NOT ENFORCED
) WITH ('bucket.num' = '1');

-- The source: the workers' Trade records.
CREATE TEMPORARY TABLE processed (
  exchange STRING, product_id STRING, ts TIMESTAMP_LTZ(6), side STRING,
  price DOUBLE, last_size DOUBLE, best_bid DOUBLE, best_ask DOUBLE, spread DOUBLE, volume_24h DOUBLE
) WITH (
  'connector' = 'kafka',
  'topic' = 'processed-data',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'flink-live',
  -- The live page shows only new data, so a restarted job starts at the end of the topic.
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'json.ignore-parse-errors' = 'true'
);

-- The last price of each exchange and product, kept in the job's own state (about 2,000 rows).
-- Not read back from the Fluss table latest: a Fluss client reads a table's snapshot files directly from Fluss's
-- remote storage, which is a folder on the Fluss pod here, so the TaskManager cannot read it.
CREATE TEMPORARY VIEW latest_prices AS
SELECT exchange, SPLIT_INDEX(product_id, '-', 0) AS asset, LAST_VALUE(price) AS price
FROM processed
WHERE price > 0
GROUP BY exchange, product_id;

-- Each INSERT sets the Fluss writer's wait with a hint (the catalog does not accept writer options): by default
-- the writer waits up to 100 ms to collect rows into one request; 5 ms keeps the tables fresh.
EXECUTE STATEMENT SET
BEGIN

INSERT INTO fluss.crypto.trades /*+ OPTIONS('client.writer.batch-timeout' = '5ms') */
SELECT exchange, product_id, SPLIT_INDEX(product_id, '-', 0), ts, side,
       price, last_size, best_bid, best_ask, spread, volume_24h
FROM processed;

INSERT INTO fluss.crypto.latest /*+ OPTIONS('client.writer.batch-timeout' = '5ms') */
SELECT exchange, product_id, SPLIT_INDEX(product_id, '-', 0) AS asset, price, spread, volume_24h, ts
FROM processed;

-- For each coin: the number of exchanges, and the exchange with the lowest and the highest price (a top-1 query
-- for each), from latest_prices above. When a coin no longer matches the WHERE clause, Flink sends a delete, and
-- Fluss removes the row.
INSERT INTO fluss.crypto.price_gap /*+ OPTIONS('client.writer.batch-timeout' = '5ms') */
SELECT n.asset, n.exchanges, lo.price, lo.exchange, hi.price, hi.exchange,
       (hi.price - lo.price) / lo.price * 10000 AS gap_bps
FROM (
  SELECT asset, COUNT(*) AS exchanges FROM latest_prices GROUP BY asset
) AS n
JOIN (
  SELECT asset, exchange, price FROM (
    SELECT asset, exchange, price, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY price ASC) AS rn
    FROM latest_prices
  ) WHERE rn = 1
) AS lo ON n.asset = lo.asset
JOIN (
  SELECT asset, exchange, price FROM (
    SELECT asset, exchange, price, ROW_NUMBER() OVER (PARTITION BY asset ORDER BY price DESC) AS rn
    FROM latest_prices
  ) WHERE rn = 1
) AS hi ON n.asset = hi.asset
WHERE n.exchanges >= 3 AND (hi.price - lo.price) / lo.price * 10000 < 500;

END;
