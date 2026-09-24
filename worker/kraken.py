import json
from collections.abc import Iterator

from utils import Fill, Quote, get_json, run

KEY = "this.data.0.symbol"
# No server_time(): Kraken's time API returns whole seconds only, too coarse for a clock offset.
# The REST API uses the old Kraken names; websocket v2 uses the ISO names.
RENAME = {"XBT": "BTC", "XDG": "DOGE"}


def symbols() -> dict[str, str]:
    out = {}
    for p in get_json("https://api.kraken.com/0/public/AssetPairs")["result"].values():
        if p.get("quote") in ("ZUSD", "USD") and p.get("status") == "online":
            base = p["wsname"].split("/")[0]
            base = RENAME.get(base, base)
            out[f"{base}/USD"] = base
    return out


def inputs(chunk: list[str]) -> list[dict]:
    # Kraken takes one channel per subscribe message, so trades and quotes use two connections.
    return [
        {
            "url": "wss://ws.kraken.com/v2",
            "open_message": json.dumps({"method": "subscribe", "params": {"channel": ch, "symbol": chunk}}),
        }
        for ch in ("trade", "ticker")
    ]


def parse(m: dict) -> Iterator[Quote | Fill]:
    ch = m.get("channel")
    if ch not in ("trade", "ticker"):
        return
    for d in m.get("data", []):
        p = d["symbol"].replace("/", "-")
        if ch == "ticker":
            yield Quote(product_id=p, best_bid=d["bid"], best_ask=d["ask"], volume_24h=d["volume"])
        else:
            yield Fill(product_id=p, ts=d["timestamp"], side=d["side"], price=d["price"], last_size=d["qty"])


if __name__ == "__main__":
    run("kraken", parse)
