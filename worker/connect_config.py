"""Print the Redpanda Connect config for one exchange: python connect_config.py <exchange>

Env:
  BASES        "*" for all symbols, or a list of base assets such as "BTC,ETH,SOL"
  CHUNK        symbols for each websocket connection (an exchange module can set a lower MAX_CHUNK)
  TRACE_RATIO  part of the messages that get a trace
The output is JSON, which Connect reads as YAML.
"""

import importlib
import json
import os
import sys

from utils import BROKERS

OTEL_GRPC = "otel-collector:4317"


def config(exchange: str, bases: str, chunk: int, trace_ratio: float) -> dict:
    ex = importlib.import_module(exchange)
    wanted = None if bases == "*" else set(bases.split(","))
    syms = sorted(s for s, base in ex.symbols().items() if wanted is None or base in wanted)
    if not syms:
        sys.exit(f"{exchange}: no symbols match BASES={bases}")
    chunk = min(chunk, getattr(ex, "MAX_CHUNK", chunk))
    inputs = [
        {"websocket": {**ws, "open_message_type": "text"}}
        for i in range(0, len(syms), chunk)
        for ws in ex.inputs(syms[i : i + chunk])
    ]
    print(f"{exchange}: {len(syms)} symbols on {len(inputs)} connections", file=sys.stderr)
    return {
        "input": {"broker": {"inputs": inputs}},
        # traceparent: the worker continues the trace of this message.
        "pipeline": {
            "processors": [{"mutation": f'meta key = {ex.KEY}.or("")\nmeta traceparent = tracing_span().traceparent'}]
        },
        # Keyed by product: the quotes and trades of one product stay in order on one partition.
        "output": {
            "kafka_franz": {
                "seed_brokers": [BROKERS],
                "topic": f"{exchange}-raw-feed",
                "key": '${! meta("key") }',
                "compression": "lz4",
                "metadata": {"include_patterns": ["^traceparent$"]},
                # Each write waits about 11 ms: the Kafka client (franz-go) lingers 10 ms to collect records
                # into one request, and Redpanda acks in about 1 ms. So the in-flight limit sets the capacity:
                # the default 10 gave about 860 msg/s (OKX fell minutes behind); 256 gives about 22,000.
                # No Connect batching: franz-go already combines records, and a batch period only adds wait.
                "max_in_flight": 256,
            }
        },
        # Metrics: Prometheus format on :4195/metrics (the default), scraped by the collector.
        "tracer": {
            "open_telemetry_collector": {
                "service": f"{exchange}-ingest",
                "grpc": [{"address": OTEL_GRPC}],
                "sampling": {"enabled": True, "ratio": trace_ratio},
            }
        },
    }


if __name__ == "__main__":
    bases = os.environ.get("BASES", "*")
    chunk = int(os.environ.get("CHUNK", "200"))
    trace_ratio = float(os.environ.get("TRACE_RATIO", "0.01"))
    print(json.dumps(config(sys.argv[1], bases, chunk, trace_ratio), indent=1))
