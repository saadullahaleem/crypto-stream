import json
from collections.abc import Iterator

from utils import Fill, Quote, get_json, run

KEY = "this.arg.instId"


def symbols() -> dict[str, str]:
    return {
        i["instId"]: i["baseCcy"]
        for i in get_json("https://www.okx.com/api/v5/public/instruments?instType=SPOT")["data"]
        if i["quoteCcy"] == "USDT" and i["state"] == "live"
    }


def server_time() -> float:
    """OKX's clock, in seconds."""
    return int(get_json("https://www.okx.com/api/v5/public/time")["data"][0]["ts"]) / 1000


def inputs(chunk: list[str]) -> list[dict]:
    args = [{"channel": ch, "instId": s} for s in chunk for ch in ("trades", "tickers")]
    return [
        {"url": "wss://ws.okx.com:8443/ws/v5/public", "open_message": json.dumps({"op": "subscribe", "args": args})}
    ]


def parse(m: dict) -> Iterator[Quote | Fill]:
    ch = m.get("arg", {}).get("channel")
    for d in m.get("data", []):
        p = d["instId"]
        if ch == "tickers":
            if not d["bidPx"] or not d["askPx"]:  # a market with no orders on one side: no top of book
                continue
            yield Quote(product_id=p, best_bid=d["bidPx"], best_ask=d["askPx"], volume_24h=d["vol24h"])
        elif ch == "trades":
            yield Fill(product_id=p, ts=int(d["ts"]), side=d["side"], price=d["px"], last_size=d["sz"])


if __name__ == "__main__":
    run("okx", parse, server_time)
