# Lessons learned

Problems we met while building crypto-stream, what caused them, and what we changed.
Each lesson has the same parts: **What happened**, **Cause**, **Fix**, and (where useful) **How we found it**.

| File | Area |
|---|---|
| [redpanda-connect.md](redpanda-connect.md) | Redpanda Connect: message order, write speed, config quirks |
| [exchange-feeds.md](exchange-feeds.md) | Coinbase, Kraken, Binance and OKX: limits, formats, trade IDs |
| [redpanda-and-kafka.md](redpanda-and-kafka.md) | Topics, consumer groups, lag, the schema registry |
| [risingwave.md](risingwave.md) | Watermarks, time filters, memory, metrics, subscriptions |
| [kubernetes-and-docker-desktop.md](kubernetes-and-docker-desktop.md) | The local cluster, images, disks, Windows tools |
| [observability.md](observability.md) | Metrics, traces, dashboards, KEDA |
| [measuring-and-debugging.md](measuring-and-debugging.md) | How we worked: claims we made that measurements proved wrong |

## How to add a lesson

1. Add it to the file of its area, or make a new file and add it to the table above.
2. Write down the numbers you measured, and the date.
3. Link the code or config that has the fix.

First written on 2026-09-23, after the first day of work.
