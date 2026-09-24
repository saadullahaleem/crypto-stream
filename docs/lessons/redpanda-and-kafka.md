# Redpanda and Kafka

## 1. Reported lag goes up and down because of the commit interval

**What happened:** the processor showed a lag of about 220, and it looked like it was behind.

**Cause:** lag = newest offset − committed offset. The consumer commits every 5 s by default
(`auto.commit.interval.ms`), so at 80 messages each second the reported lag went from 0 to about 400 and back,
even though the consumer was up to date.

**Fix:** `auto.commit.interval.ms: 1000` in [worker/utils.py](../../worker/utils.py). The reported lag is now at most
about 1 second of messages. After a crash, up to 1 second of messages is processed again.

## 2. A stopped consumer holds its partitions until its session times out

**What happened:** after a restart of the workers, 4 partitions got no new reads for about 45 seconds, and the lag grew.

**Cause:** the old members stay in the group until their session times out (45 s by default).

**Fix:** `session.timeout.ms: 10000`, and on `SIGTERM` the worker closes its consumer, so it leaves the group at
once. `partition.assignment.strategy: cooperative-sticky` moves only the partitions that change owner.

## 3. Few keys spread badly over partitions

**What happened:** 3 Binance products on 3 partitions: the key hash put 2 products on one partition and none on
another, so 1 of 3 workers did nothing.

**Fix at that time:** a fixed partition for each product. With about 1,900 products on 12 partitions, the hash
spreads well, so that is no longer necessary.

## 4. Automatic topic creation makes topics with the wrong partition count

**Risk:** if Connect writes before the `topics` Job runs, Redpanda makes the topic with 1 partition.

**Fix:** `auto_create_topics_enabled: false`, set in `/etc/redpanda/.bootstrap.yaml` (a ConfigMap). Redpanda reads
that file only when the cluster forms the first time.

**Quirk:** in Kubernetes, `rpk redpanda start --set=redpanda.auto_create_topics_enabled=false` failed:
Redpanda stopped with `unrecognised option '--set=...'`.

## 5. `rpk` inside a Job needs the admin address too

**What happened:** `rpk cluster health` failed with `dial tcp 127.0.0.1:9644: connection refused`.

**Cause:** `RPK_BROKERS` sets the Kafka address only. The admin API has its own address.

**Fix:** `RPK_ADMIN_HOSTS=redpanda:9644` in [k8s/jobs.yaml](../../k8s/jobs.yaml).

## 6. Consumer lag is not a metric by default

**Fix:** `rpk cluster config set enable_consumer_group_metrics '["group","partition","consumer_lag"]'`. Then
Redpanda publishes `redpanda_kafka_consumer_group_lag_sum` and `_max`.

## 7. The same schema registered at the same moment makes several versions

**What happened:** 6 workers started at the same time, and the registry got 6 versions of one identical schema.

**Cause:** the registry does not merge identical schemas that arrive at the same moment. Registering again later
returns the existing version.

**Fix:** a one-time `schema` Job registers the schema. Each worker has an init container that only checks that
the schema is there.

## 8. Advertise a name that other namespaces can resolve

KEDA runs in the `keda` namespace and connects to the address that Redpanda advertises. `redpanda:9092` does not
resolve there, so Redpanda advertises `redpanda.crypto.svc.cluster.local:9092`.
