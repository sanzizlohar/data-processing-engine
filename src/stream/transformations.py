"""Validation, normalization and enrichment — the CPU-bound heart of the pipeline.

Two entry points:
  * transform_batch(events)          — pure function over a list of Events
  * generate_and_transform_chunk(spec) — worker-process entry point: generates a
    deterministic chunk locally (no Event objects cross the process boundary)
    and transforms it. Only compact storage rows come back.

The Spark job mirrors these semantics in DataFrame form.
"""
from __future__ import annotations

import random
from datetime import datetime, timezone

from src.events import (DEVICES, EVENT_TYPES, REGIONS, REGION_FX, TYPE_MARGIN,
                        Event, amount_band, random_event)

REVENUE_PRECISION = 4


def transform_batch(events: list[Event]) -> tuple[list[tuple], dict[str, int]]:
    """Validate -> enrich -> storage rows. Returns (rows, dropped_by_reason).

    Row layout matches EVENT_COLUMNS:
    (event_id, ts, user_id, session_id, event_type, region, device,
     amount, revenue, hour, day_of_week, amount_band)
    """
    rows: list[tuple] = []
    dropped: dict[str, int] = {}
    for e in events:
        reason = _validate(e)
        if reason:
            dropped[reason] = dropped.get(reason, 0) + 1
            continue
        amount = round(e.amount, 2) if e.amount is not None else None
        revenue = round((amount or 0.0) * TYPE_MARGIN[e.event_type] * REGION_FX[e.region],
                        REVENUE_PRECISION)
        rows.append((
            e.event_id,
            e.ts.isoformat(),
            e.user_id,
            e.session_id,
            e.event_type,
            e.region,
            e.device,
            amount,
            revenue,
            e.ts.hour,
            e.ts.weekday(),
            amount_band(amount),
        ))
    return rows, dropped


def _validate(e: Event) -> str | None:
    if e.event_type not in EVENT_TYPES:
        return "bad_event_type"
    if e.region not in REGIONS:
        return "bad_region"
    if e.device not in DEVICES:
        return "bad_device"
    if not e.user_id:
        return "missing_user"
    if e.amount is not None and e.amount < 0:
        return "negative_amount"
    if e.event_type in ("purchase", "refund") and not e.amount:
        return "missing_amount"
    return None


# ---------------------------------------------------------------------------
# Chunked generate+transform: the parallel bulk path used by seeding and the
# benchmark. Each chunk is generated deterministically inside the worker from
# (seed, offset), so no Event objects are pickled across process boundaries —
# a naive "send events to workers" design spends more time serializing than
# transforming.
# ---------------------------------------------------------------------------

def chunk_specs(seed: int, n: int, base_epoch_s: float, spread_seconds: int,
                chunk_size: int = 5000) -> list[tuple]:
    return [(seed, i, min(chunk_size, n - i), base_epoch_s, spread_seconds)
            for i in range(0, n, chunk_size)]


def generate_and_transform_chunk(spec: tuple) -> tuple[list[tuple], dict[str, int]]:
    seed, offset, count, base_epoch_s, spread_seconds = spec
    rng = random.Random((seed << 32) ^ offset)
    events = [
        random_event(rng, datetime.fromtimestamp(base_epoch_s + rng.random() * spread_seconds,
                                                 tz=timezone.utc))
        for _ in range(count)
    ]
    return transform_batch(events)
