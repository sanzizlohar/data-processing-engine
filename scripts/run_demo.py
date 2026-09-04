#!/usr/bin/env python
"""End-to-end demo: seed a day of traffic -> stream live events -> automated
insights -> intelligent query routing.

Run:  python scripts/run_demo.py --seconds 45 --eps 900 --time-scale 10
"""
import argparse
import asyncio
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config import Config  # noqa: E402
from src.insights.engine import InsightsEngine  # noqa: E402
from src.routing.router import QueryRouter, QuerySpec  # noqa: E402
from src.storage.base import create_store  # noqa: E402
from src.stream.engine import StreamingEngine  # noqa: E402
from src.stream.memory import MemoryAggregates  # noqa: E402
from src.tools.seed import seed_events  # noqa: E402
from src.utils.logging_setup import configure  # noqa: E402
from src.utils.metrics import Metrics  # noqa: E402


def print_insights(rows: list[dict]) -> None:
    print(f"\n--- latest insights ({len(rows)}) ---")
    for r in rows:
        print(f"  [{r['severity']:8s}] {r['kind']:13s} {r['entity']:12s} {r['message']}")


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run the full engine demo")
    parser.add_argument("--seconds", type=float, default=45)
    parser.add_argument("--eps", type=int, default=900)
    parser.add_argument("--time-scale", type=float, default=10.0,
                        help="event-time compression so several minute-windows elapse quickly")
    parser.add_argument("--skip-seed", action="store_true")
    parser.add_argument("--seed-n", type=int, default=50000)
    args = parser.parse_args()

    configure("INFO")
    cfg = Config.load()
    cfg.generator.time_scale = args.time_scale

    store = create_store(cfg)
    store.init_schema()
    counts = store.counts()
    print(f"storage: {store.name} | {counts}")

    if not args.skip_seed and counts["events"] < args.seed_n:
        print(f"\nseeding {args.seed_n} events (one simulated day of traffic)...")
        stats = seed_events(store, n=args.seed_n)
        print(f"seeded {stats['rows_inserted']} rows in {stats['total_s']}s "
              f"(parallel transform via {stats['parallel_backend']})")
        print(f"counts: {store.counts()}")

    memory = MemoryAggregates(hot_minutes=cfg.routing.hot_window_minutes)
    metrics = Metrics()
    insights = InsightsEngine(cfg.insights)
    engine = StreamingEngine(cfg, store, memory, insights, metrics)

    print(f"\nstreaming {args.eps} events/s for {args.seconds}s "
          f"(micro-batch {cfg.engine.micro_batch_ms} ms, time-scale x{args.time_scale})...")
    stats = await engine.run(eps=args.eps, duration=args.seconds)

    lat = stats["timings"].get("stream_latency_ms", {})
    counters = stats["counters"]
    print("\n--- engine stats ---")
    print(f"  events processed : {counters.get('events_processed', 0)}")
    print(f"  dropped          : {sum(v for k, v in counters.items() if k.startswith('dropped.'))}")
    print(f"  throughput       : {stats.get('throughput_eps', 0)} events/s")
    if lat:
        print(f"  stream latency   : p50={lat['p50_ms']:.1f}ms  p95={lat['p95_ms']:.1f}ms  "
              f"p99={lat['p99_ms']:.1f}ms  max={lat['max_ms']:.1f}ms  (ingestion -> analytics-visible)")
    print(f"  insights generated: {stats['insights_generated']}")

    print_insights(store.latest_insights(12))

    router = QueryRouter(store, memory, cfg.routing, metrics)
    now = datetime.now(timezone.utc)
    print("\n--- query routing demo ---")
    user = store.sample_user()
    if user:
        r = router.route(QuerySpec(kind="point", user_id=user))
        print(f"  point lookup        -> {r.backend:18s} {r.elapsed_ms:8.2f} ms  ({len(r.rows)} rows)  {r.rationale}")
    r = router.route(QuerySpec(kind="recent_agg", minutes=30))
    print(f"  recent 30-min agg   -> {r.backend:18s} {r.elapsed_ms:8.2f} ms  ({len(r.rows)} rows)  {r.rationale}")
    r = router.route(QuerySpec(kind="heavy_agg", group_by="region",
                               start_iso=(now - timedelta(hours=24)).isoformat(),
                               end_iso=now.isoformat()))
    print(f"  heavy 24h agg       -> {r.backend:18s} {r.elapsed_ms:8.2f} ms  ({len(r.rows)} rows)  {r.rationale}")
    r = router.route(QuerySpec(kind="heavy_agg", group_by="region",
                               start_iso=(now - timedelta(hours=24)).isoformat(),
                               end_iso=now.isoformat()))
    print(f"  same query again    -> {r.backend:18s} {r.elapsed_ms:8.2f} ms  (cache_hit={r.cache_hit})")

    print("\n--- routing decisions ---")
    for (kind, backend), n in sorted(router.decisions.items()):
        print(f"  {kind:10s} -> {backend:18s} x{n}")


if __name__ == "__main__":
    asyncio.run(main())
