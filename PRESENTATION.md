# How to demo this project to someone (5-minute script)

## 30-second elevator version

> "It's a real-time data processing engine: events stream in, it aggregates them
> in 200-millisecond micro-batches, keeps recent analytics in memory so queries
> come back in single-digit milliseconds, and it generates insights on its own —
> anomaly spikes, trends, rate alerts — without anyone writing per-metric rules.
> A query router sends each question to the cheapest backend that can answer it.
> The benchmark shows an 87% end-to-end latency reduction versus a naive
> pipeline. It's built twice: pure Python for the demo you're seeing, and the
> same pipeline in Spark Structured Streaming + PostgreSQL for production scale."

## The 5-minute walkthrough

### 1. The live dashboard (0:00–2:00) — the visual hook

```bash
python -m src.api.app --eps 800 --seconds 1800 --port 8000
# open http://127.0.0.1:8000/ in a browser
```

Point at, in order:
- **KPI cards** — events processed climbing ~800/s in real time, p95 latency in
  the hundreds of milliseconds or less: "that number is ingestion-to-visible:
  the time from an event arriving to it being answerable."
- **Throughput & latency chart** — two live series; mention bursts appear as
  throughput bumps without latency blowing up (micro-batch sizing).
- **Revenue by region** — served from the in-memory hot layer, no disk.
- **Insights feed** — this is the "automated insights" claim made visible: the
  engine noticed a 3.4σ revenue spike by itself; nobody wrote an alert rule for
  that region. Severity tags (critical/warning/info) show triage.
- **Storage panel** — raw events, 1-minute aggregates, hourly summaries,
  insights — the layered data model the router exploits.

### 2. The query router playground (2:00–3:00) — the clever part

On the same dashboard, click each button and read the rationale line aloud:

| Button | Expected result | What to say |
|---|---|---|
| **Point lookup** | `store.indexed`, ~1–3 ms | "Selective lookup → composite btree index." |
| **Recent 30-min agg** | `memory.hot_layer`, ~0.1–1 ms | "Recent window → in-memory aggregates, zero disk I/O." |
| **Heavy 24-h agg** | `store.summary`, ~1–12 ms, explains "(N events)" | "767K+ events answered in milliseconds — from pre-aggregated summaries, not a raw scan." |
| **Heavy 24-h agg again** | `cache`, ~0.01 ms, cache hit | "Repeat questions are nearly free." |

Then show the same routing decisions logged in the API: `curl localhost:8000/stats`
(or the interactive docs at `http://127.0.0.1:8000/docs` — every endpoint is
clickable there, which is itself a nice demo).

### 3. The benchmark (3:00–4:00) — the proof

```bash
python scripts/benchmark.py
```

Show the table: same workload, baseline vs optimized (parallel transform +
batched sorted load), then raw scans vs routed queries. Landing line:
"87% end-to-end latency reduction." This is the evidence behind the resume
bullet — measured, reproducible, on the viewer's own machine if they want.

### 4. The terminal demo (4:00–4:30) — it's a real system

```bash
python scripts/run_demo.py --seconds 45
```

Logs show the stream, then a summary with latency percentiles, generated
insights, and the routing decisions taken during the run.

### 5. Code map (4:30–5:00) — where the claims live

- "50K+ daily records" → `src/ingest/generator.py`, `scripts/seed.py`
- "sub-second stream processing" → `src/stream/engine.py` (200 ms micro-batch),
  `src/stream/memory.py` (hot layer)
- "automated insights" → `src/insights/engine.py` (z-score, trend, mover,
  threshold detectors)
- "parallel execution" → `src/parallel/executor.py` + `scripts/benchmark.py`
- "intelligent query routing" → `src/routing/router.py`
- "Spark + PostgreSQL" → `src/spark/streaming_job.py`, `db/schema.sql`,
  `src/storage/postgres_store.py`
- "35 tests" → `tests/` (`python -m unittest discover -s tests`)

## Questions you'll probably get (and good answers)

- **"Why micro-batches instead of pure streaming?"** — 200 ms batches amortize
  storage I/O and let transforms work on vectors of events; that's the same
  design tradeoff Spark Structured Streaming makes. True per-event streaming
  buys you ~100 ms here at real complexity cost; the p50 is already ~3–6 ms
  end-to-end.
- **"How does it scale?"** — the demo path is single-node; the production path
  is Kafka in, Spark Structured Streaming processing, PostgreSQL partitioned by
  time, pre-aggregated summaries so query cost doesn't grow with raw data.
  Nothing in the router or insight logic changes.
- **"Are the insights just thresholds?"** — thresholds are one of four
  detectors; the interesting ones are rolling z-scores against a per-entity
  baseline and normalized linear trends, with cooldowns to prevent alert spam.
  Try to trip one live: the demo generator's bursts cause real anomalies.
- **"Why SQLite anywhere?"** — the storage layer is an interface with two
  implementations; SQLite exists so the whole system demos with zero setup.
  Production DDL for PostgreSQL (BRIN, materialized view, partitioning note)
  is in `db/schema.sql`.
- **"Is the 70% claim real?"** — run `scripts/benchmark.py` yourself; the
  baseline and optimized paths are the same workload with one difference at a
  time, and all numbers print from your machine.

## Screenshots worth keeping for a portfolio

1. Dashboard with the insights feed showing a critical anomaly.
2. The router panel right after a heavy query (backend + ms + rationale).
3. The benchmark table showing the 87% line.
