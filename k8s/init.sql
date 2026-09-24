-- A checkpoint every 250 ms (default 1,000 ms): new rows become visible in the views and subscriptions at each
-- checkpoint. Measured on 2026-09-24: trade age at a subscriber p50 about 920 -> 460 ms, for about 1.7 more CPU cores.
ALTER SYSTEM SET barrier_interval_ms = 250;

-- A source stores nothing. Each view below keeps only the time range the dashboard shows.
-- The watermark lets RisingWave drop the state of a window once no more trades can arrive for it;
-- a trade more than 30 seconds older than the newest one seen is dropped.
CREATE SOURCE IF NOT EXISTS trades (
  exchange VARCHAR, product_id VARCHAR, ts TIMESTAMPTZ, side VARCHAR,
  price DOUBLE PRECISION, last_size DOUBLE PRECISION,
  best_bid DOUBLE PRECISION, best_ask DOUBLE PRECISION,
  spread DOUBLE PRECISION, volume_24h DOUBLE PRECISION,
  WATERMARK FOR ts AS ts - INTERVAL '30 seconds'
) WITH (
  connector = 'kafka',
  topic = 'processed-data',
  properties.bootstrap.server = 'redpanda:9092',
  scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;

CREATE MATERIALIZED VIEW IF NOT EXISTS ticks AS
SELECT * FROM trades
WHERE ts > now() - INTERVAL '15 minutes';

CREATE MATERIALIZED VIEW IF NOT EXISTS candles_1m AS
SELECT * FROM (
SELECT exchange, product_id, window_start,
       first_value(price ORDER BY ts) AS open,
       max(price) AS high,
       min(price) AS low,
       last_value(price ORDER BY ts) AS close,
       sum(last_size) AS volume,
       count(*) AS trades
FROM TUMBLE(trades, ts, INTERVAL '1 minute')
GROUP BY exchange, product_id, window_start
) WHERE window_start > now() - INTERVAL '1 day';

-- One point per second per product: keeps the comparison charts light at a 1s refresh.
CREATE MATERIALIZED VIEW IF NOT EXISTS prices_1s AS
SELECT * FROM (
SELECT exchange, product_id, window_start,
       last_value(price ORDER BY ts) AS price,
       avg(spread) AS spread,
       count(*) AS trades
FROM TUMBLE(trades, ts, INTERVAL '1 second')
GROUP BY exchange, product_id, window_start
) WHERE window_start > now() - INTERVAL '15 minutes';

-- Trades per second per exchange, over all products.
CREATE MATERIALIZED VIEW IF NOT EXISTS exchange_rate_1s AS
SELECT * FROM (
SELECT exchange, window_start, count(*) AS trades
FROM TUMBLE(trades, ts, INTERVAL '1 second')
GROUP BY exchange, window_start
) WHERE window_start > now() - INTERVAL '15 minutes';

CREATE MATERIALIZED VIEW IF NOT EXISTS latest AS
SELECT exchange, product_id,
       split_part(product_id, '-', 1) AS asset,
       last_value(price ORDER BY ts) AS price,
       last_value(spread ORDER BY ts) AS spread,
       last_value(volume_24h ORDER BY ts) AS volume_24h,
       max(ts) AS ts
FROM trades
GROUP BY exchange, product_id;

-- Cross-exchange price gap of each asset that trades on 3 or more exchanges.
-- A gap over 5% (500 bps) is almost always 2 different tokens with the same ticker (for example LUNA), so it is left out.
-- Binance and OKX quote in USDT, the others in USD, so a small part of each gap is the USDT/USD rate.
CREATE MATERIALIZED VIEW IF NOT EXISTS price_gap AS
SELECT * FROM (
SELECT asset,
       count(*) AS exchanges,
       min(price) AS low,
       first_value(exchange ORDER BY price) AS low_exchange,
       max(price) AS high,
       last_value(exchange ORDER BY price) AS high_exchange,
       (max(price) - min(price)) / min(price) * 10000 AS gap_bps
FROM latest
WHERE price > 0
GROUP BY asset
HAVING count(*) >= 3
) WHERE gap_bps < 500;

-- Change streams for the live page (live/server.py): each change of the view is a row with an op column.
CREATE SUBSCRIPTION IF NOT EXISTS ticks_sub FROM ticks WITH (retention = '5m');
CREATE SUBSCRIPTION IF NOT EXISTS latest_sub FROM latest WITH (retention = '5m');
CREATE SUBSCRIPTION IF NOT EXISTS price_gap_sub FROM price_gap WITH (retention = '5m');
