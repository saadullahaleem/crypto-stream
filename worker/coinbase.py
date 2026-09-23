import json
from collections.abc import Iterator

from utils import Fill, Quote, get_json, run

KEY = "this.product_id"  # Bloblang path of the product in a raw message: the Kafka key


def symbols() -> dict[str, str]:
    """{symbol: base asset} of all USD pairs that trade now."""
    return {
        p["id"]: p["base_currency"]
        for p in get_json("https://api.exchange.coinbase.com/products")
        if p["quote_currency"] == "USD" and p["status"] == "online" and not p["trading_disabled"]
    }


def inputs(chunk: list[str]) -> list[dict]:
    msg = {"type": "subscribe", "product_ids": chunk, "channels": ["ticker"]}
    return [{"url": "wss://ws-feed.exchange.coinbase.com", "open_message": json.dumps(msg)}]


def parse(m: dict) -> Iterator[Quote | Fill]:
    if m.get("type") != "ticker":
        return
    p = m["product_id"]
    yield Quote(product_id=p, best_bid=m["best_bid"], best_ask=m["best_ask"], volume_24h=m["volume_24h"])
    yield Fill(product_id=p, ts=m["time"], side=m["side"], price=m["price"], last_size=m["last_size"])


if __name__ == "__main__":
    run("coinbase", parse)
