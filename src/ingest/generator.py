"""Synthetic event generator.

Two modes:
  * bulk()   — deterministic seeded generation for seeding storage / benchmarks
               (default spread: one full day of traffic, i.e. 50K+ records/day)
  * stream() — paced async production into an asyncio queue with periodic
               traffic bursts; `ts_scale` compresses event-time for demos.
"""
from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime, timedelta, timezone

from src.events import Event, random_event

log = logging.getLogger("ingest.generator")


class EventGenerator:
    def __init__(self, seed: int | None = 42):
        self._seed = seed

    def bulk(self, n: int, start_ts: datetime | None = None,
             spread_seconds: int = 86400) -> list[Event]:
        rng = random.Random(self._seed)
        start = start_ts or (datetime.now(timezone.utc) - timedelta(seconds=spread_seconds))
        events = []
        for _ in range(n):
            ts = start + timedelta(seconds=rng.random() * spread_seconds)
            events.append(random_event(rng, ts))
        events.sort(key=lambda e: e.ts)
        return events

    async def stream(self, queue: asyncio.Queue, eps: float, duration: float | None = None,
                     burst_every: int = 15, burst_duration: int = 5, burst_multiplier: int = 4,
                     ts_scale: float = 1.0, stop: asyncio.Event | None = None,
                     metrics=None) -> None:
        """Produce ~eps events/second into `queue` until `duration` elapses or `stop` is set."""
        loop = asyncio.get_running_loop()
        start = loop.time()
        base_ts = datetime.now(timezone.utc)
        next_burst = start + burst_every
        tick = 0.1
        produced = 0
        rng = random.Random()  # wall-clock streaming need not be reproducible
        try:
            while True:
                now = loop.time()
                if duration is not None and now - start >= duration:
                    break
                if stop is not None and stop.is_set():
                    break
                in_burst = next_burst <= now < next_burst + burst_duration
                if burst_every and now >= next_burst + burst_duration:
                    next_burst = now + burst_every
                rate = eps * (burst_multiplier if in_burst else 1)
                n = max(1, int(rate * tick))
                event_ts = base_ts + timedelta(seconds=(now - start) * ts_scale)
                for _ in range(n):
                    ev = random_event(rng, event_ts)
                    try:
                        queue.put_nowait(ev)
                    except asyncio.QueueFull:
                        try:
                            queue.get_nowait()  # shed oldest: backpressure by drop-oldest
                        except asyncio.QueueFull:
                            pass
                        try:
                            queue.put_nowait(ev)
                        except asyncio.QueueFull:
                            pass
                        if metrics:
                            metrics.incr("events_dropped_queue")
                    produced += 1
                await asyncio.sleep(tick)
        finally:
            log.info("generator stopped after producing %d events", produced)
