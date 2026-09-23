"""Shared worker logic: the record schema, quote and trade merging, the Kafka loop, and schema registration."""

import json
import signal
import sys
import time
import urllib.request
from collections.abc import Callable, Iterable
from datetime import datetime
from typing import Any, Literal

from confluent_kafka import Consumer, Producer
from opentelemetry import metrics, propagate, trace
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import SpanKind, Status, StatusCode
from pydantic import BaseModel

BROKERS = "redpanda:9092"
REGISTRY = "http://redpanda:8081"
OUT_TOPIC = "processed-data"


class Quote(BaseModel):
    """Top of book and 24h volume, as the exchange last reported it."""

    product_id: str
    best_bid: float
    best_ask: float
    volume_24h: float


class Fill(BaseModel):
    """One trade, as the exchange reported it. side is the taker side."""

    product_id: str
    ts: datetime
    side: Literal["buy", "sell"]
    price: float
    last_size: float


class Trade(BaseModel):
    """One record on processed-data. RisingWave's trades source reads these fields."""

    exchange: str
    product_id: str
    ts: datetime
    side: Literal["buy", "sell"]
    price: float
    last_size: float
    best_bid: float | None = None
    best_ask: float | None = None
    spread: float | None = None
    volume_24h: float | None = None


Parser = Callable[[dict], Iterable[Quote | Fill]]


def get_json(url: str) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": "crypto-stream"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def process(exchange: str, parse: Parser, raw: bytes | str, book: dict[str, Quote]) -> list[Trade]:
    """Raw message -> trades, each with the last quote of its product. Quotes only update the book."""
    trades = []
    for ev in parse(json.loads(raw)):
        if isinstance(ev, Quote):
            book[ev.product_id] = ev
            continue
        q = book.get(ev.product_id)
        extra = (q.model_dump(exclude={"product_id"}) | {"spread": q.best_ask - q.best_bid}) if q else {}
        trades.append(Trade(exchange=exchange, **ev.model_dump(), **extra))
    return trades


def schema_request(path: str) -> str:
    """POST the Trade JSON schema to the registry. HTTPError if the registry refuses it or does not know it."""
    body = json.dumps({"schemaType": "JSON", "schema": json.dumps(Trade.model_json_schema())}).encode()
    req = urllib.request.Request(
        f"{REGISTRY}/subjects/{OUT_TOPIC}-value{path}", body, {"Content-Type": "application/vnd.schemaregistry.v1+json"}
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return r.read().decode()


def telemetry() -> tuple[TracerProvider, MeterProvider]:
    """OTLP traces and metrics. The standard OTEL_* env vars set the endpoint, service name and sampler."""
    resource = Resource.create()
    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(tracer_provider)
    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[PeriodicExportingMetricReader(OTLPMetricExporter(), export_interval_millis=10000)],
    )
    metrics.set_meter_provider(meter_provider)
    return tracer_provider, meter_provider


def run(exchange: str, parse: Parser) -> None:
    providers = telemetry()
    tracer = trace.get_tracer("worker")
    meter = metrics.get_meter("worker")
    messages = meter.create_counter("worker.messages", "{message}", "Raw messages read, by result")
    trade_count = meter.create_counter("worker.trades", "{trade}", "Trades written to processed-data")
    duration = meter.create_histogram(
        "worker.process.duration",
        "ms",
        "Parse and validate time of one raw message",
        explicit_bucket_boundaries_advisory=[0.02, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
    )
    # Exchange clock to worker clock. The Docker VM clock can be about 1 s behind the exchanges,
    # so values can be negative.
    latency = meter.create_histogram(
        "trade.latency",
        "ms",
        "Trade time on the exchange to trade processed",
        explicit_bucket_boundaries_advisory=[0, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000],
    )
    attrs = {"exchange": exchange}

    consumer = Consumer(
        {
            "bootstrap.servers": BROKERS,
            "group.id": f"{exchange}-worker",
            "auto.offset.reset": "earliest",
            "auto.commit.interval.ms": 1000,
            # Autoscaling adds and removes members often: move only the partitions that change owner.
            "partition.assignment.strategy": "cooperative-sticky",
            "session.timeout.ms": 10000,
        }
    )
    producer = Producer({"bootstrap.servers": BROKERS, "linger.ms": 20, "compression.type": "lz4"})
    consumer.subscribe([f"{exchange}-raw-feed"])
    # Connect keys raw messages by product, so the quotes and trades of a product reach the same worker.
    # ponytail: the book is in memory, so after a rebalance spread is null until the next quote.
    book: dict[str, Quote] = {}
    # Kubernetes sends SIGTERM on scale-down: leave the group and flush at once, not after the session timeout.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    try:
        while True:
            m = consumer.poll(1.0)
            if m is None:
                continue
            if m.error():
                print("consumer:", m.error())
                continue
            # Continue the trace that Connect started for this message (traceparent header). Connect samples 1%.
            parent = propagate.extract({k: v.decode() for k, v in m.headers() or [] if v is not None})
            with tracer.start_as_current_span(
                f"process {exchange}",
                context=parent,
                kind=SpanKind.CONSUMER,
                attributes={
                    "messaging.system": "kafka",
                    "messaging.destination.name": m.topic(),
                    "messaging.kafka.partition": m.partition(),
                    "messaging.kafka.offset": m.offset(),
                },
            ) as span:
                start = time.perf_counter()
                try:
                    trades = process(exchange, parse, m.value(), book)
                # pydantic ValidationError is a ValueError. One bad message must not stop the feed.
                except (ValueError, KeyError) as e:
                    messages.add(1, attrs | {"result": "skipped"})
                    span.record_exception(e)
                    span.set_status(Status(StatusCode.ERROR))
                    print(f"skip: {e!r}: {m.value()[:200]!r}")
                    continue
                duration.record((time.perf_counter() - start) * 1000, attrs)
                messages.add(1, attrs | {"result": "ok"})
                span.set_attribute("trades", len(trades))
                headers = {}
                propagate.inject(headers)  # processed-data carries the trace on
                now = time.time()
                for t in trades:
                    latency.record((now - t.ts.timestamp()) * 1000, attrs)
                    producer.produce(
                        OUT_TOPIC,
                        key=f"{exchange}:{t.product_id}",
                        value=t.model_dump_json(exclude_none=True),
                        headers=list(headers.items()),
                    )
                trade_count.add(len(trades), attrs)
            producer.poll(0)
    finally:
        consumer.close()
        producer.flush(10)
        for p in providers:
            p.shutdown()


if __name__ == "__main__":
    # register: put the schema in the registry (the schema Job).
    # check: exit non-zero until the schema is there (the init container of each worker).
    print(schema_request({"register": "/versions", "check": ""}[sys.argv[1]]))
