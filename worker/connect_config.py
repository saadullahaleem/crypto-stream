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
            # 1 thread: with several (the default is 1 per CPU), messages can leave the pipeline out of order.
            "threads": 1,
            "processors": [{"mutation": f'meta key = {ex.KEY}.or("")\nmeta traceparent = tracing_span().traceparent'}],
        },
        # Keyed by product: the quotes and trades of one product stay in order on one partition.
        "output": {
            "kafka_franz": {
                "seed_brokers": [BROKERS],
                "topic": f"{exchange}-raw-feed",
                "key": '${! meta("key") }',
                "compression": "lz4",
                "metadata": {"include_patterns": ["^traceparent$"]},
                # Order matters: the worker adds each product's last quote to its trades, and gap detection
                # needs each product's trade IDs in order. Connect sends writes that are in flight at the same
                # time in any order, so more than 1 in flight reorders a product's messages (measured: 13,086
                # backwards steps in 55,192 on Binance with 256). So: 1 write in flight, plus batches for
                # throughput. Each write waits about 11 ms (franz-go lingers 10 ms, Redpanda acks in about
                # 1 ms); one batch carries all messages of the last 5 ms, in arrival order. Measured: about
                # 27 ms in Connect for each message, and all trade IDs in order.
                "max_in_flight": 1,
                "batching": {"count": 2000, "period": "5ms"},
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
