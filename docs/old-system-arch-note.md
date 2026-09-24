# Market-Data Platform — System Architecture

*The whole-system reference. For the reasoning behind each choice see DECISIONS.md; for the Rust component internals see the Rust system design doc; for the engineering plan see the V2 spec. This document describes what the system **is** and how data **moves** through it (iteration 2: decoupled ingestion via Redpanda Connect).*

---

## 1. Overview

A multi-source, multi-region **real-time market-data streaming platform**. It ingests live feeds (crypto first; delayed equities and enrichment later), maps them into a canonical message, processes them statefully, serves them at sub-second freshness alongside deep history, and treats data **completeness** as a first-class, honest concern. It is engineered for correctness and fault tolerance under partial and correlated failure, and is designed to scale to many sources and high volume.

The defining architectural move: **Redpanda sits between ingestion and processing.** Ingestion (holding the socket) is Redpanda Connect's job; everything downstream is a stateless consumer of durable topics. This makes the reliability boundary the broker, not any single process — so components can crash and restart from a committed offset with no data loss, and the whole class of worker-HA problems (supervision, failover, split-brain) disappears for the processing path.

---

## 2. Design Principles
- **Decouple ingestion from processing; the broker is the reliability boundary.** Once an event is in a topic it is durable; downstream is stateless and offset-restartable.
- **Plan for scale; don't bolt it on.** Per-source strategy abstractions, source/region-agnostic model, isolation boundaries designed from the start.
- **Correctness and honesty over the happy path.** Point-in-time correctness, deterministic dedup, and — critically — data absence represented as first-class queryable fact (never silently empty).
- **Failures are correlated, not independent.** Backfill is defended with circuit breakers, jitter, and concurrency caps against retry storms.
- **Streaming all the way to serving; sub-second-capable, not microsecond.**
- **Cutting-edge where it earns its place; proven where it doesn't.** Every incubating component (Fluss, Lance) adopted for a stated reason.
- **Observability is first-class from day one.**

---

## 3. End-to-End Data Flow (happy path)

1. **Redpanda Connect** (YAML config, one per source) holds the source WebSocket and writes raw events to `raw-<source>-events`.
2. In parallel, **Connect-side gap detection** (Bloblang/YAML/JS, per source) watches the stream and writes any detected gap to the `gaps` topic.
3. A **Rust mapping worker** (stateless consumer) reads `raw-<source>-events`, maps each raw record into a canonical **MarketMessage** (Avro, validated against the schema registry), and produces to `market-messages`.
4. **Apache Flink** (DataStream/Java) consumes `market-messages` and does stateful stream processing: OHLCV windowing, dedup, enrichment/temporal joins, halt detection, VWAP, anomaly detection.
5. Flink writes results to **Apache Fluss** (hot tier) — sub-second fresh, days of retention, durable, primary-key merge engines.
6. Fluss **tiers** aged data (COW, ~4–5h cadence) to **Apache Iceberg** (cold tier) — full history, append-mostly, universal read ecosystem.
7. **Serving** (DuckDB) queries the **Fluss logical table**, which presents fresh (Fluss) + historical (tiered Iceberg) as one table via **union read**.

**Recovery flow (parallel):** a **Rust gap-filling worker** (per source) consumes `gaps`, applies that source's backfill strategy (snapshot / REST / sequence-replay) with retries + circuit-breaking, and produces recovered records into the pipeline (deduped at the seam by deterministic `event_id`). Unrecoverable gaps become **completeness records** (§7).

---

## 4. Component Inventory

| Component | Role | Tech |
|-----------|------|------|
| **Ingestion** | Hold each source WebSocket; write raw events | Redpanda Connect (YAML/Bloblang) |
| **Gap detection** | Per-source detect gaps → `gaps` topic | Redpanda Connect (Bloblang/YAML/JS) |
| **Broker** | Durable, replicated log; the reliability boundary | Redpanda |
| **Schema registry** | Avro envelope schema + backward-compat evolution | Redpanda integrated registry |
| **Mapping worker** | Stateless: raw → MarketMessage (Avro) | Rust (Kafka consumer) |
| **Gap-filling worker** | Per-source backfill via strategy; give-up | Rust (per source, separate failure domain) |
| **Stream processing** | Stateful: OHLCV, dedup, enrichment, anomaly | Apache Flink (DataStream / Java) |
| **Hot tier** | Sub-second fresh; PK merge engines; days retention; durable | Apache Fluss |
| **Cold tier** | Full history; append-mostly; COW; universal reads | Apache Iceberg |
| **AI/vector tier** *(deferred)* | Embeddings + canonical ID for similarity search | Apache Lance |
| **Serving** | Union-read queries (fresh + historical) | DuckDB (native Iceberg/Lance extensions) |
| **Catalog** | Table metadata / atomic commit coordination | Apache Polaris *(vs Gravitino, open)* |
| **Completeness store** | Queryable record of known data gaps | `data_gaps` topic/table |
| **Observability** | Metrics, traces, dashboards, alerts | OTel Collector → Prometheus + Jaeger → Grafana |

---

## 5. Topic Inventory

| Topic | Produced by | Consumed by | Contents |
|-------|-------------|-------------|----------|
| `raw-<source>-events` | Redpanda Connect (per source) | Mapping worker; (Connect gap detection reads the stream) | Untouched source events |
| `gaps` | Connect gap detection (per source) | Gap-filling workers | `{source, symbol, gap_spec, detected_at, detection_type}` |
| `market-messages` | Mapping workers; gap-filling workers (backfilled) | Flink | Canonical MarketMessage (Avro) |
| `data_gaps` (topic or table) | Gap-filling workers (terminal give-up) | Serving / dashboards / consumers | `{source, symbol, start, end, reason}` — permanent absence facts |
| `dead-letter-<source>` *(optional)* | Mapping worker | Ops/inspection | Poison records that fail parsing |

Ordering: `market-messages` (and raw topics) are keyed by symbol for per-symbol ordering within a partition. Global ordering is not assumed; within-millisecond order comes from sequence/trade-id, not the timestamp.

---

## 6. The Layers (age = tier)

- **Ingestion / transport:** Connect → raw topics → broker. Reliability boundary.
- **Mapping:** stateless Rust → canonical MarketMessage. Offset-restartable.
- **Processing:** Flink, stateful, event-time.
- **Hot (Fluss):** freshest slice, sub-second query, days retention, durable, mutable via merge engines.
- **Cold (Iceberg):** full history, append-mostly, COW, universal analytical reads.
- **AI (Lance, deferred):** vectors/embeddings, random-access retrieval, fed by a vectorization job (not Fluss tiering).
- **Serving:** DuckDB reads the Fluss logical table (union read across hot + tiered cold).

Faster tier = shallower retention. A query spanning ages is one union read; the tiering is invisible to the consumer.

---

## 7. Reliability: Gap Detection, Backfill, Completeness

The reliability story is three-staged and the platform's most distinctive part.

**Detect** (Connect, per source, → `gaps`): strictly-incrementing sequence IDs, missing heartbeats, or timestamp discontinuity — whichever the source affords.

**Fill** (Rust gap-filling worker, per source): apply the source's `BackfillStrategy` (snapshot / REST history / sequence replay / none). Backfilled records reuse the mapping and dedupe at the seam via deterministic `event_id` + Fluss dedup. Defended against **correlated failure** (a whole exchange dropping at once floods one rate-limited API): **circuit breaker per source**, **jittered backoff**, **global concurrency cap**; the durable `gaps` topic is the pressure-relief buffer so gaps drain at a sustainable rate.

**Represent absence honestly** (completeness subsystem): an unfillable gap is a permanent fact. Three states, kept distinct so "missing" is never mistaken for "empty":
- **no-data** — nothing happened (correct empty)
- **not-yet** — detected, backfill pending (empty, provisional)
- **never** — `UNRECOVERABLE` (empty, permanently missing + reason)

Unrecoverable gaps → a queryable `data_gaps` record + an engineer alert + a Grafana data-quality panel; and query results overlapping a known gap carry a completeness annotation. (Efficient query-time propagation is a consumer-API concern, deferred.)

---

## 8. Failure & Recovery Model

- **Mapping/processing consumer dies** → restart from committed offset; Redpanda replays; no loss. (Domain 1.)
- **Ingestion loses events at the source boundary** (source outage, reconnect-window loss, silent skip) → Connect-side gap detection → gap-filling worker backfill → completeness if unrecoverable. (Domain 2 — the hard one, handled by the gap subsystem.)
- **Correlated / whole-source outage** → circuit breaker + jitter + concurrency cap prevent a retry storm; gaps queue durably and drain when the source recovers.
- **Poison record** → skip-and-log / dead-letter; never fatal.
- **Backpressure** → a slow consumer lags its offset (Redpanda retains); lag is alerted; add consumers to scale.

---

## 9. Data Model (envelope)

- **Base fields:** source, venue, region, symbol, event-time (ms, UTC), receive-time, `event_id`, sequence, `trace_id`, raw.
- **`payload`:** an Avro **union per record type** → Rust enum, one variant per type (`Trade` / `Quote` / `Bar` / `Status`). The variant *is* the record type (no separate `record_type` field). Each variant routes to its own Fluss table with its own primary key.
- **`event_id`:** the venue's ID when present, else a **deterministic** hash of `{source, venue, symbol, event_time_ms, sequence, payload}` — never random. Dedup key = `(source, venue, symbol, event_id)`. Determinism is what makes backfilled duplicates merge away.
- **Timestamps:** milliseconds since epoch, UTC. Finer source precision is truncated; within-ms order comes from sequence/trade-id. Original precision survives in `raw`.
- **Schema evolution:** the Avro envelope is schema-first (Rust structs generated from it); evolves under a backward-compatibility policy enforced by the registry.

---

## 10. Observability

Every Rust component (mapping and gap-filling workers) and the pipeline services emit via **OTLP** to the **OTel Collector** (separate process, off the critical path) → **Prometheus** (metrics: throughput counters, produce-latency histograms, consumer lag, error counts by type, backfill/gap counts, restart counts) + **Jaeger** (traces). **Grafana** unifies both, plus a **data-quality panel** fed by `data_gaps`.

**Trace propagation (async):** the `trace_id` is generated at ingestion and carried **in the message** (Kafka header / envelope) so each stage (mapping → Flink → …) tags its span with the same id; Jaeger reconstructs the event's path across the decoupled stages. Metrics are raw counters/gauges/histograms; rates/percentiles are computed at query time (Prometheus, requested by Grafana).

---

## 11. Deployment Topology

- **Now (V0, local):** Docker Compose — Redpanda + Connect + schema registry, the Rust workers, Flink, Iceberg (local disk behind a configurable URI, no object store yet), the OTel/Prometheus/Jaeger/Grafana stack.
- **Growth:** containers per source (fault isolation + independent scaling); the isolation boundary moves outward without rewriting worker logic.
- **Scale-up (later):** cloud; Databento tick data; distributed-compute tuning; object storage backend chosen at cloud time. Cost controlled via burst compute (spot, auto-terminate).

---

## 12. Deferred / Open
- Cold-storage backend (decided at cloud time; not MinIO — archived).
- Catalog: Polaris vs Gravitino (multi-format, incl. Lance).
- Heartbeat-only gap detection: verify Connect surfaces disconnects, else a Rust WS worker for that source.
- Efficient query-time completeness propagation → consumer-API design.
- Lance AI/vector tier + vectorization job → when the ML layer is built.
- WASM Data Transform as an alternative to the Rust mapping worker (evaluated, declined; revisit if external-worker maintenance grows).
- `data_gaps` store: topic vs table.
- Product-market-fit exploration → after V0.

---

## 13. System Diagram (text)

```
 SOURCES (crypto now; delayed equities + enrichment later)
    │
    ▼
 REDPANDA CONNECT (per-source YAML)  ──gap detection (Bloblang)──►  gaps topic
    │  writes                                                          │
    ▼                                                                  │ consumed by
 raw-<source>-events topic                                             ▼
    │  consumed by                                        GAP-FILLING WORKER (Rust, per source)
    ▼                                                       │  BackfillStrategy + circuit breaker
 MAPPING WORKER (Rust, stateless)                           │  ├─ recovered ─► market-messages
    │  raw → MarketMessage (Avro, schema registry)          │  └─ give-up ─► data_gaps (completeness)
    ▼                                                        │
 market-messages topic  ◄─────────────────────────────────┘
    │  consumed by
    ▼
 APACHE FLINK (DataStream/Java, stateful)
    │  OHLCV · dedup · enrichment · anomaly
    ▼
 APACHE FLUSS (hot tier: sub-second, days, durable, PK merge engines)
    │  union read serves ─────────────────────►  DuckDB / SERVING  (fresh + historical)
    │  tiers (COW, ~4-5h)
    ▼
 APACHE ICEBERG (cold tier: full history, append-mostly)
    │
    └─(deferred) vectorization job ─► APACHE LANCE (AI/vector tier)

 CROSS-CUTTING:
   • Catalog: Polaris (metadata / atomic commits)
   • Observability: all components ─OTLP─► OTel Collector ─► Prometheus + Jaeger ─► Grafana
     (trace_id propagated in message headers; data-quality panel from data_gaps)
```