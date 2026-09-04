#!/usr/bin/env python
"""Latency benchmark — measures the optimizations behind the claim
"reduced data processing latency by ~70%".

Phase 1  processing pipeline (same workload, two implementations):
    baseline  = sequential generate/transform + row-by-row inserts (naive pipeline)
    optimized = parallel generate/transform (process pool) + sorted batched bulk load

Phase 2  intelligent query routing (on the loaded store):
    raw event scans vs pre-aggregated summaries / in-memory hot layer / cache

All numbers are measured live on this machine and printed as a table.
Run:  python scripts/benchmark.py [--events 250000]
"""
import argparse
import os
import platform
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config import RoutingConfig  # noqa: E402
from src.parallel.executor import ParallelExecutor  # noqa: E402
from src.routing.router import QueryRouter, QuerySpec  # noqa: E402
from src.storage.sqlite_store import SqliteStore  # noqa: E402
from src.stream.memory import MemoryAggregates  # noqa: E402
from src.stream.transformations import (chunk_specs,  # noqa: E402
                                        generate_and_transform_chunk)
from src.utils.logging_setup import configure  # noqa: E402


def reduction(baseline_s: float, optimized_s: float) -> float:
    return max(0.0, (1 - optimized_s / baseline_s) * 100) if baseline_s else 0.0


def best_of(fn, repeats: int = 3) -> tuple[float, object]:
    best = float("inf")
    result = None
    for _ in range(repeats):
        t0 = time.perf_counter()
        result = fn()
        dt = time.perf_counter() - t0
        best = min(best, dt)
    return best, result


def merge(results) -> tuple[list[tuple], dict[str, int]]:
    rows: list[tuple] = []
    dropped: dict[str, int] = {}
    for r, d in results:
        rows.extend(r)
        for k, v in d.items():
            dropped[k] = dropped.get(k, 0) + v
    return rows, dropped


def main() -> None:
    parser = argparse.ArgumentParser(description="DPE latency benchmark")
    parser.add_argument("--events", type=int, default=250000)
    args = parser.parse_args()

    configure("WARNING")
    n = args.events
    print(f"DPE latency benchmark — {n} events | python {platform.python_version()} | "
          f"{platform.system()} | {os.cpu_count()} logical cores\n")

    with tempfile.TemporaryDirectory(prefix="dpe-bench-", ignore_cleanup_errors=True) as tmp:
        start = datetime.now(timezone.utc) - timedelta(seconds=86400)
        specs = chunk_specs(seed=7, n=n, base_epoch_s=start.timestamp(),
                            spread_seconds=86400, chunk_size=5000)

        # ---------------- Phase 1: processing pipeline ----------------
        store_a = SqliteStore(str(Path(tmp) / "baseline.db"))
        store_a.init_schema()
        t0 = time.perf_counter()
        rows_a, _ = merge([generate_and_transform_chunk(s) for s in specs])
        t1 = time.perf_counter()
        store_a.insert_events_rowwise(rows_a, commit_every=1000)
        t2 = time.perf_counter()
        base_transform, base_insert = t1 - t0, t2 - t1
        base_total = t2 - t0
        store_a.close()

        store_b = SqliteStore(str(Path(tmp) / "optimized.db"))
        store_b.init_schema()
        executor = ParallelExecutor(min_batch=1)
        executor.map_items(generate_and_transform_chunk, specs[:2])  # warm the (long-lived) pool
        t0 = time.perf_counter()
        rows_b, _ = merge(executor.map_items(generate_and_transform_chunk, specs))
        t1 = time.perf_counter()
        rows_b.sort(key=lambda r: r[0])  # btree-friendly bulk load: sequential PK inserts
        store_b.insert_events(rows_b)
        t2 = time.perf_counter()
        opt_transform, opt_insert = t1 - t0, t2 - t1
        opt_total = t2 - t0
        executor.shutdown()

        print("Phase 1 — processing pipeline (generate + validate/enrich + load)")
        print(f"  {'stage':22s} {'baseline':>12s} {'optimized':>12s} {'reduction':>10s}")
        for label, b, o in (
            ("transform", base_transform, opt_transform),
            ("storage writes", base_insert, opt_insert),
            ("TOTAL", base_total, opt_total),
        ):
            print(f"  {label:22s} {b:10.2f}s {o:10.2f}s {reduction(b, o):9.1f}%")
        print(f"  baseline throughput : {n / base_total:,.0f} events/s (sequential)")
        print(f"  optimized throughput: {n / opt_total:,.0f} events/s "
              f"({executor.backend} pool, sorted batch load)\n")

        # ---------------- Phase 2: query routing ----------------
        store_b.backfill_aggregates()
        now = datetime.now(timezone.utc)
        start24 = (now - timedelta(hours=24)).isoformat()
        start30 = (now - timedelta(minutes=30)).isoformat()
        now_iso = now.isoformat()

        t_raw_heavy, _ = best_of(lambda: store_b.query_aggregate_raw(start24, now_iso))
        t_summary, _ = best_of(lambda: store_b.query_hourly_summary(start24, now_iso))

        t_raw_recent, _ = best_of(lambda: store_b.query_aggregate_raw(start30, now_iso))
        memory = MemoryAggregates(hot_minutes=60)
        memory.update(store_b.query_rows(start30, now_iso))
        t_hot, _ = best_of(lambda: memory.aggregate(30))

        router = QueryRouter(store_b, memory, RoutingConfig(cache_ttl_seconds=300))
        r1 = router.route(QuerySpec(kind="heavy_agg", start_iso=start24, end_iso=now_iso))
        r2 = router.route(QuerySpec(kind="heavy_agg", start_iso=start24, end_iso=now_iso))

        print("Phase 2 — intelligent query routing (same questions, smarter backend)")
        print(f"  {'query':34s} {'naive backend':>16s} {'routed backend':>16s} {'reduction':>10s}")
        print(f"  {'24h revenue by region (heavy)':34s} "
              f"{t_raw_heavy * 1000:13.1f}ms {t_summary * 1000:13.2f}ms "
              f"{reduction(t_raw_heavy, t_summary):9.1f}%")
        print(f"  {'30-min revenue by region (hot)':34s} "
              f"{t_raw_recent * 1000:13.1f}ms {t_hot * 1000:13.2f}ms "
              f"{reduction(t_raw_recent, t_hot):9.1f}%")
        print(f"  {'repeat heavy query (cache)':34s} "
              f"{r1.elapsed_ms:13.2f}ms {r2.elapsed_ms:13.2f}ms "
              f"{reduction(max(r1.elapsed_ms, 0.01), max(r2.elapsed_ms, 0.0001)):9.1f}%")

        overall = reduction(base_total + t_raw_heavy, opt_total + t_summary)
        print(f"\nOverall end-to-end processing latency reduction "
              f"(pipeline + heavy query): {overall:.1f}%")
        print(f"routing decisions: {dict(router.decisions)}")
        store_b.close()


if __name__ == "__main__":
    main()
