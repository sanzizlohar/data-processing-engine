"""Shared seeding helper: bulk-generate a day of traffic in parallel worker
processes, load it, and backfill aggregate tables."""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone

from src.parallel.executor import ParallelExecutor
from src.stream.transformations import chunk_specs, generate_and_transform_chunk

log = logging.getLogger("tools.seed")


def seed_events(store, n: int = 60000, spread_hours: float = 24.0, seed: int = 42,
                workers: int = 0) -> dict:
    """Generate n events across the last `spread_hours`, load them, backfill aggregates.

    Events are generated + transformed inside worker processes (deterministic
    per-chunk seeding) — only storage rows cross the process boundary.
    """
    start = datetime.now(timezone.utc) - timedelta(hours=spread_hours)
    specs = chunk_specs(seed, n, start.timestamp(), int(spread_hours * 3600))

    executor = ParallelExecutor(workers=workers, min_batch=1)
    t0 = time.perf_counter()
    results = executor.map_items(generate_and_transform_chunk, specs)
    rows: list[tuple] = []
    dropped: dict[str, int] = {}
    for r, d in results:
        rows.extend(r)
        for k, v in d.items():
            dropped[k] = dropped.get(k, 0) + v
    t1 = time.perf_counter()

    store.insert_events(rows)
    t2 = time.perf_counter()
    store.backfill_aggregates()
    t3 = time.perf_counter()

    stats = {
        "events_generated": n,
        "rows_inserted": len(rows),
        "dropped": dropped,
        "transform_s": round(t1 - t0, 3),
        "load_s": round(t2 - t1, 3),
        "backfill_s": round(t3 - t2, 3),
        "total_s": round(t3 - t0, 3),
        "parallel_backend": executor.backend,
    }
    log.info("seeded %d events in %.2fs (transform %.2fs via %s pool, load %.2fs, backfill %.2fs)",
             len(rows), stats["total_s"], stats["transform_s"], executor.backend,
             stats["load_s"], stats["backfill_s"])
    return stats
