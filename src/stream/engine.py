"""Micro-batch streaming engine (pure-Python path).

Sub-second pipeline:
  paced ingest -> 200 ms micro-batches -> validation/enrichment
  (inline, or process-pool fan-out under load) -> in-memory window aggregates
  (hot layer) -> incremental storage flushes -> automated insights every N batches.
"""
from __future__ import annotations

import asyncio
import logging
import time

from src.ingest.generator import EventGenerator
from src.insights.engine import InsightsEngine
from src.parallel.executor import ParallelExecutor
from src.stream.memory import MemoryAggregates
from src.stream.transformations import transform_batch
from src.utils.metrics import Metrics

log = logging.getLogger("stream.engine")


class StreamingEngine:
    def __init__(self, cfg, store, memory: MemoryAggregates, insights: InsightsEngine,
                 metrics: Metrics | None = None, generator: EventGenerator | None = None,
                 executor: ParallelExecutor | None = None):
        self.cfg = cfg
        self.store = store
        self.memory = memory
        self.insights = insights
        self.metrics = metrics or Metrics()
        self.generator = generator or EventGenerator()
        self.executor = executor or ParallelExecutor(
            workers=cfg.engine.parallel_workers,
            chunk_size=cfg.engine.chunk_size,
            min_batch=cfg.engine.parallel_min_batch,
        )
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=50000)
        self.stop_event = asyncio.Event()
        self.running = False

    # ------------------------------------------------------------------
    async def run(self, eps: float | None = None, duration: float | None = None) -> dict:
        eps = eps or self.cfg.generator.events_per_second
        self.running = True
        t_start = time.perf_counter()
        self._t_start = t_start
        produce = asyncio.create_task(self.generator.stream(
            self.queue, eps, duration=duration, stop=self.stop_event,
            metrics=self.metrics, ts_scale=self.cfg.generator.time_scale,
            burst_every=self.cfg.generator.burst_every_seconds,
            burst_duration=self.cfg.generator.burst_duration_seconds,
            burst_multiplier=self.cfg.generator.burst_multiplier,
        ))
        batches = 0
        last_flush = time.perf_counter()
        last_log = t_start
        pending_rows: list[tuple] = []
        try:
            while True:
                batch = await self._drain()
                if batch is None:
                    if produce.done() and self.queue.empty():
                        break
                    continue
                rows, dropped = await self._transform(batch)
                pending_rows.extend(rows)
                self.memory.update(rows)
                now_ns = time.time_ns()
                for e in batch:
                    # latency: ingestion -> analytics-visible (hot layer updated)
                    self.metrics.observe("stream_latency_ms", (now_ns - e.created_ns) / 1e6)
                self.metrics.incr("events_in", len(batch))
                self.metrics.incr("events_processed", len(rows))
                for reason, n in dropped.items():
                    self.metrics.incr(f"dropped.{reason}", n)
                batches += 1
                if batches % self.cfg.engine.insights_every_n_batches == 0:
                    self._run_insights()
                now = time.perf_counter()
                if now - last_flush >= self.cfg.engine.flush_interval_s:
                    pending_rows = await self._flush(pending_rows)
                    last_flush = now
                if now - last_log >= 10:
                    counters = self.metrics.snapshot()["counters"]
                    lat = self.metrics.snapshot()["timings"].get("stream_latency_ms", {})
                    log.info("throughput=%.0f eps queue=%d batches=%d p95=%.1fms insights=%d",
                             counters.get("events_processed", 0) / max(now - t_start, 1e-9),
                             self.queue.qsize(), batches,
                             lat.get("p95_ms", 0.0), self.insights.generated)
                    last_log = now
        finally:
            self.stop_event.set()
            produce.cancel()
            try:
                await produce
            except (asyncio.CancelledError, Exception):
                pass
            if pending_rows:
                await self._flush(pending_rows)
            self.executor.shutdown()
            self.running = False
        return self.stats(t_start)

    # ------------------------------------------------------------------
    async def _drain(self) -> list | None:
        """Collect one micro-batch: first event with a timeout, then drain greedily."""
        timeout = self.cfg.engine.micro_batch_ms / 1000
        try:
            first = await asyncio.wait_for(self.queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None
        batch = [first]
        while len(batch) < self.cfg.engine.max_batch_size:
            try:
                batch.append(self.queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        return batch

    async def _transform(self, batch) -> tuple[list[tuple], dict[str, int]]:
        loop = asyncio.get_running_loop()
        results = await self.executor.async_map(loop, transform_batch, batch)
        rows: list[tuple] = []
        dropped: dict[str, int] = {}
        for r, d in results:
            rows.extend(r)
            for k, v in d.items():
                dropped[k] = dropped.get(k, 0) + v
        return rows, dropped

    async def _flush(self, pending_rows: list[tuple]) -> list[tuple]:
        if pending_rows:
            await asyncio.to_thread(self.store.insert_events, pending_rows)
            pending_rows = []
        min_rows, hour_rows = self.memory.drain_dirty()
        if min_rows:
            await asyncio.to_thread(self.store.upsert_window_aggregates, min_rows)
        if hour_rows:
            await asyncio.to_thread(self.store.upsert_hourly_summary, hour_rows)
        return pending_rows

    def _run_insights(self) -> None:
        window_rows = self.memory.last_window_rows()
        if not window_rows:
            return
        produced = self.insights.process_windows(window_rows)
        if produced:
            self.metrics.incr("insights_generated", len(produced))
            try:
                self.store.insert_insights([i.to_row() for i in produced])
            except Exception:
                log.exception("failed to persist insights")

    # ------------------------------------------------------------------
    def stats(self, t_start: float | None = None) -> dict:
        if t_start is None:
            t_start = getattr(self, "_t_start", None)
        snap = self.metrics.snapshot()
        counters = snap["counters"]
        out = {
            "counters": counters,
            "timings": snap["timings"],
            "insights_generated": self.insights.generated,
        }
        if t_start is not None:
            elapsed = time.perf_counter() - t_start
            out["elapsed_s"] = round(elapsed, 2)
            out["throughput_eps"] = round(counters.get("events_processed", 0) / max(elapsed, 1e-9), 1)
        return out

    async def stop(self) -> None:
        self.stop_event.set()
