import json
from collections.abc import Iterator

from utils import Fill, Quote, get_json, run

KEY = "this.data.s"
# Binance rejects a subscribe message over 4096 bytes: 80 symbols is about 3000.
MAX_CHUNK = 80


def symbols() -> dict[str, str]:
    info = get_json("https://api.binance.com/api/v3/exchangeInfo?permissions=SPOT&symbolStatus=TRADING")
    return {s["symbol"]: s["baseAsset"] for s in info["symbols"] if s["quoteAsset"] == "USDT"}


def server_time() -> float:
    """Binance's clock, in seconds."""
    return get_json("https://api.binance.com/api/v3/time")["serverTime"] / 1000


def inputs(chunk: list[str]) -> list[dict]:
    streams = [f"{s.lower()}@{kind}" for s in chunk for kind in ("trade", "ticker")]
    return [
        {
            "url": "wss://stream.binance.com:9443/stream",
            "open_message": json.dumps({"method": "SUBSCRIBE", "params": streams, "id": 1}),
        }
    ]


def parse(m: dict) -> Iterator[Quote | Fill]:
    d = m.get("data", {})
    p = d.get("s", "").removesuffix("USDT") + "-USDT"
    # ponytail: 24hrTicker gives bid/ask once a second, so spread can be up to 1s old; add @bookTicker if that matters
    if d.get("e") == "24hrTicker":
        yield Quote(product_id=p, best_bid=d["b"], best_ask=d["a"], volume_24h=d["v"])
    elif d.get("e") == "trade":
        # T is epoch ms. m means the buyer is the maker, so the taker sold.
        yield Fill(product_id=p, ts=d["T"], side="sell" if d["m"] else "buy", price=d["p"], last_size=d["q"])


if __name__ == "__main__":
    run("binance", parse, server_time)
