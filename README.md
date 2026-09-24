# crypto-stream

Exchange websockets → Redpanda Connect → `<exchange>-raw-feed` → Python workers → `processed-data` → RisingWave → Grafana.

Exchanges: Coinbase, Kraken, Binance, OKX. Each has one file in `worker/` (symbol list, websocket subscribe, parser)
and one file in `k8s/exchanges/` (ingest pod, worker Deployment, KEDA autoscaler).

Full description: [docs/architecture.md](docs/architecture.md).
Lessons learned: [docs/lessons/](docs/lessons/README.md). The design before this build: [docs/old-system-arch-note.md](docs/old-system-arch-note.md).

## Deploy (Docker Desktop Kubernetes, kind type)

1. Build the worker image and load it into the cluster node (do this again after each code change):
   ```bash
   docker build -t crypto-worker:dev worker
   docker save crypto-worker:dev | docker exec -i desktop-control-plane ctr -n k8s.io images import -
   ```
   The live page and Flink have their own images: the same 2 commands with `crypto-live:dev` and the `live` folder,
   and with `crypto-flink:dev` and the `flink` folder.
2. Install KEDA one time:
   ```bash
   kubectl apply --server-side -f https://github.com/kedacore/keda/releases/download/v2.21.0/keda-2.21.0.yaml
   ```
3. Deploy:
   ```bash
   kubectl apply -k k8s
   ```
4. After a code change, restart the pods that use the image:
   ```bash
   kubectl -n crypto get deploy -o name | grep -E 'ingest|worker' | xargs kubectl -n crypto rollout restart
   ```

| URL | What |
|---|---|
| http://localhost:3000 | Grafana dashboard |
| http://localhost:8080 | Redpanda Console (topics, consumer groups, schema) |
| `localhost:4566`, user `root`, database `dev` | RisingWave (Postgres protocol) |
| http://localhost:8000 | Live page: trade tape, and views with their SQL. Source: Flink + Fluss (default) or RisingWave |
| http://localhost:8081 | Flink web UI (the job `crypto-live`) |
| http://localhost:3000/d/pipeline-health | Pipeline Health: throughput, lag, latency, resources, traces |
| http://localhost:16686 | Jaeger (traces) |
| http://localhost:9090 | Prometheus (metrics) |

## Settings

- Symbols: `BASES` in `k8s/kustomization.yaml`. `*` = all USD (Coinbase, Kraken) and USDT (Binance, OKX) pairs.
- Worker scale: KEDA, 1 to 12 pods for each exchange, by consumer lag (`lagThreshold` in `k8s/exchanges/*.yaml`).
  12 = the partitions of each raw feed (`k8s/jobs.yaml`).
- Tests: `docker run --rm crypto-worker:dev python test_workers.py`
- Format and lint (settings in `worker/pyproject.toml`): `uvx ruff format worker && uvx ruff check worker`
- Dependencies: `worker/pyproject.toml` only. The Dockerfile installs them from there with uv.
