# What this project is, and where you'd actually use it

## The one-paragraph version

The Data Processing Engine is a **streaming analytics backend**: it ingests a
continuous flow of events (transactions, clicks, sensor readings, requests),
processes them in 200-millisecond micro-batches, keeps recent analytics in
memory so dashboards and APIs get answers in milliseconds, detects interesting
changes on its own (spikes, drops, trends, rate violations) and writes them out
as insights, and routes every incoming query to the cheapest storage layer that
can answer it accurately. The same pipeline is implemented twice — pure Python
(runs anywhere, demoable in seconds) and Apache Spark Structured Streaming
(production scale, Kafka → PostgreSQL) — with identical semantics.

The synthetic event generator stands in for any real producer: swap it for your
Kafka topic, webhook receiver, or CDC stream and nothing else changes.

## Use cases (the pattern behind the code)

| Domain | What streams in | What the engine does | What you'd see on the dashboard |
|---|---|---|---|
| **E-commerce / retail live ops** | orders, carts, refunds, page views | revenue per region per minute, refund-rate monitoring, demand spikes | "us-east revenue 4.2σ above baseline" during a flash sale; refund-rate alert when a payment provider breaks |
| **Payments / risk ops** | transactions, auth attempts | velocity and rate thresholds, anomaly detection on transaction volume | sudden drop in approval revenue (provider outage) surfaced as a critical anomaly within seconds |
| **SaaS product analytics** | clicks, feature usage, signups | usage trends per region/plan, adoption movers | "eu-central usage rising steadily +40% over 20 windows" — a growth signal nobody asked for, found automatically |
| **IoT / fleet telemetry** | sensor readings, GPS pings | per-region aggregates, threshold alerts, z-score anomaly detection | a region's telemetry dipping 5σ below baseline = failing device farm, flagged without a human writing a rule per metric |
| **Ad-tech campaign monitoring** | impressions, clicks, conversions | revenue-share movers between campaigns/regions, trend detection | "latam gained 8.2% of revenue share window-over-window" — budget-shift signal |
| **Ops / infra monitoring** | request logs, error events | rate thresholds, anomaly spikes per service/region | traffic surge detection that feeds autoscaling decisions |

The common shape — and why this project generalizes: a **high-volume event
stream + per-dimension aggregations + statistical alerting + cheap query
serving**. Change the enums in `src/events.py` (event types, regions, devices),
the enrichment tables, and the insight thresholds in `config/config.yaml`, and
the same engine runs any of the above.

## What each piece contributes

- **`src/ingest/`** — paced generator (demo), bulk generator (seeding), Kafka
  producer (production front-end for the Spark path).
- **`src/stream/engine.py`** — the sub-second loop: 200 ms micro-batches →
  validate/enrich → in-memory hot layer → incremental storage flushes.
- **`src/stream/memory.py`** — the hot layer: last hour of 1-minute window
  aggregates in RAM; serves recent-window queries with zero disk I/O.
- **`src/insights/engine.py`** — automated analytics: rolling z-score anomalies,
  normalized trend detection, window-over-window revenue-share movers,
  configurable rate thresholds, with per-entity cooldowns so the feed stays
  signal-dense.
- **`src/routing/router.py`** — intelligent query routing: TTL cache → hot layer
  → pre-aggregated hourly summary → indexed point lookup → raw scan fallback.
  Every decision is recorded with its rationale.
- **`src/parallel/executor.py`** — chunked process-pool fan-out for heavy
  batches; workers generate/transform locally so serialization never dominates.
- **`src/storage/`** — PostgreSQL (primary, see `db/schema.sql`: composite
  indexes, BRIN, materialized view) with a SQLite fallback so the demo runs
  anywhere.
- **`src/spark/`** — the production-scale twin: Structured Streaming job
  (Kafka → 1-minute windows → PostgreSQL upserts, 200 ms trigger) and a nightly
  batch job with parallel JDBC reads.
- **`src/api/`** — FastAPI service + live HTML dashboard.

## What it is not (honest scoping)

- Not a message broker — it *consumes* from Kafka; it doesn't replace it.
- Not a general observability stack (no traces/logs pipelines) — its analytics
  layer is metric/dimension-shaped.
- Not a real-time database — it's the processing + serving layer in front of one.
- In demo mode the "data" is synthetic; the pipeline, insights, routing and API
  are the real, tested system.

## Scale notes

- 50K records/day (~0.6 events/s average) is trivially handled by the pure-Python
  path; the demo streams 700–1,400 events/s sustained on a laptop to prove the
  pipeline's throughput envelope.
- The design headroom for millions/day is already in place: Kafka ingestion +
  the Spark streaming path, PostgreSQL table partitioning by time (noted in
  `db/schema.sql`), and pre-aggregated summaries that keep heavy queries
  independent of raw-data growth.
