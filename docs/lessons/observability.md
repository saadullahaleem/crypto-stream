# Observability

OpenTelemetry Collector → Tempo (traces) and Prometheus (metrics) → Grafana. Details:
[docs/architecture.md](../architecture.md), section 13.

## 1. The dashboard found 2 real problems the first time we opened it

- The trade-latency panel showed OKX at 30 s or more: Connect was 2 minutes behind
  ([redpanda-connect.md](redpanda-connect.md), lesson 3).
- The "Worker messages read" panel showed about 0.6 skipped OKX messages each second: empty bid and ask prices
  ([exchange-feeds.md](exchange-feeds.md), lesson 5).

Both problems existed before the dashboard. The trade-latency metric (exchange time to worker time) was the most
useful single metric, because a delay in any part before RisingWave shows in it.

## 2. Jaeger 2.21 removed the API that Grafana uses

On 2026-09-26, Tempo replaced Jaeger. This lesson is history.

**What happened:** the Grafana Jaeger data source failed with `404 Not Found`.

**Cause:** Jaeger 2.21 serves only `/api/v3/*`. Grafana 13.2's Jaeger data source still calls `/api/services`
and `/api/traces`. Version 2.20.0 still has them.

**Fix:** `jaegertracing/jaeger:2.20.0` in [k8s/observability.yaml](../../k8s/observability.yaml).

**How we found it:** `/jaeger/api/services` answered 200, but that was only the UI's start page (the fallback
for unknown paths).

## 3. Each worker pod needs its own instance ID

**Cause:** 2 replicas with the same `service.name` and no instance ID write the same series in Prometheus.

**Fix:** `OTEL_RESOURCE_ATTRIBUTES=service.instance.id=$(POD_NAME),k8s.pod.name=$(POD_NAME)`.

## 4. Sample traces at the start of the pipeline

Connect traces 1% of messages. The worker uses the `parentbased_traceidratio` sampler, so it follows Connect's
decision: a trace is complete or absent, never half. Tracing all 1,400 messages each second would be too much.

## 5. KEDA did not scale, because the workers were too fast

**What happened:** OKX was paused for 4 minutes (196,212 messages of lag). After the release, 1 worker cleared
the lag in about 14 seconds, before the HPA's next check (every 15 s). KEDA scaled for the first time later,
during a burst of trades.

**Measured:** one Python worker processes about 14,000 messages each second in the cluster. The largest feed
(OKX) sends about 1,300.

**Quirk:** KEDA warns that `pollingInterval` and `cooldownPeriod` have no effect when `minReplicaCount` is 1.

## 6. Smaller points

- p50 and p99 come from histogram buckets and are estimates. A value at the top bucket (30 s for trade latency)
  means "that much or more".
- Grafana cannot animate a table: it draws the whole table again at each refresh. A trade tape that you can
  follow needed our own page ([live/](../../live)).
- Grafana did not load a changed dashboard file through a Windows bind mount (in the Compose setup). A restart did.
- RisingWave's consumer groups are left out of the lag panel ([risingwave.md](risingwave.md), lesson 6).
