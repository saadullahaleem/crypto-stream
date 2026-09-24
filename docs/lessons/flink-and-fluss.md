# Flink and Fluss

Flink 1.20.3 and Fluss 1.0.0 (released 2026-09-22), for the live page. Setup: [docs/architecture.md](../architecture.md), section 15.

## 1. A Fluss client reads snapshot files directly from Fluss's remote storage

**What happened:** the Flink job restarted again and again with
`FileNotFoundException: /data/remote/kv/crypto/latest-2/0/snap-0/...`.

**Cause:** the `price_gap` part read the Fluss key table `latest` as a source. To read a key table from the start,
a Fluss client gets the table's snapshot files **itself**, from `remote.data.dir`. Here that is a folder on the
Fluss pod's volume, and the Flink TaskManager runs in another pod.

**Fix:** the job keeps the last price of each product in its own state (`latest_prices`), and no longer reads a
Fluss table. Fluss is only the output.

**The other fix:** S3-compatible storage for `remote.data.dir`, reachable from every client. The Fluss quickstart
uses RustFS for that.

**Side effect of the restarts:** each restart continued from the last checkpoint, so some trades were written 2
times to `trades`, and the live page got fewer new trades.

## 2. The Fluss catalog accepts only 3 options

**What happened:** `Unsupported options found for 'fluss'. Unsupported options: client.writer.batch-timeout`.

**Cause:** `CREATE CATALOG ... WITH (...)` accepts only `bootstrap.servers`, `default-database` and
`property-version`.

**Fix:** a hint on each `INSERT`: `INSERT INTO fluss.crypto.trades /*+ OPTIONS('client.writer.batch-timeout' = '5ms') */`.

## 3. The Fluss writer waits up to 100 ms by default

`client.writer.batch-timeout` is 100 ms: the writer collects rows for up to 100 ms before it sends them. It is the
same pattern as the 10 ms linger in Redpanda Connect ([redpanda-connect.md](redpanda-connect.md), lesson 4). We set 5 ms.

## 4. A log table's records have the change type `+A`

**What happened:** the live page got no new trades from Fluss, but the table grew.

**Cause:** records in a log table (append only) have the change type `+A`. Key tables have `+I`, `-U`, `+U` and
`-D`. The reader accepted only `+I` and `+U`.

## 5. A Flink session cluster forgets its jobs

Without HA, a restarted JobManager has no jobs. The `submitter` container in the JobManager pod
([flink/submit.sh](../../flink/submit.sh)) checks `flink list -r` every 30 s and submits the SQL again. Tested:
after a JobManager restart, the job was back in about 1 minute.

## 6. Smaller points

- **Fluss needs 3 processes:** ZooKeeper, a coordinator and a tablet server. In one pod they share the network, so
  the tablet server needs its own port (9124). `bind.listeners` is where a server listens; `advertised.listeners`
  is the address it gives to clients.
- **The ZooKeeper image's start script** runs `chown` on its data folders, so they must exist. Mounting subfolders
  of the volume at its default paths (`/data`, `/datalog`) worked.
- **Flink's Prometheus reporter** is already in `plugins/metrics-prometheus` in the 1.20 image. The Fluss
  quickstart's own image has no Kafka connector, so we build our own image.
- **pyfluss is asynchronous.** The live server runs its own event loop in a thread. A record's `.row` is a dict,
  and `.timestamp` is the time Fluss stored it.
- **The Fluss REST gateway** (new in 1.0) cannot read records yet, so it is not a way to serve a page.
