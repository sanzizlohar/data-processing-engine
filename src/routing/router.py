"""Intelligent query routing.

Sends each query to the cheapest backend that can answer it accurately
(first match wins):

  point lookup                -> storage indexed path (composite btree)
  recent aggregate (hot)      -> in-memory hot layer (no disk I/O)
  heavy historical aggregate  -> pre-aggregated hourly summary
                              -> raw storage scan (fallback when no coverage)
  anything repeated           -> TTL cache

Every decision is recorded (backend + rationale) for observability.
"""
from __future__ import annotations

import logging
import time
from collections import Counter, OrderedDict
from dataclasses import dataclass

from src.config import RoutingConfig

log = logging.getLogger("routing")


@dataclass(slots=True)
class QuerySpec:
    kind: str = "recent_agg"      # point | recent_agg | heavy_agg
    user_id: str | None = None
    minutes: int = 60             # for recent_agg
    start_iso: str | None = None  # for heavy_agg
    end_iso: str | None = None
    region: str | None = None
    event_type: str | None = None
    group_by: str = "region"


@dataclass(slots=True)
class RouteResult:
    rows: list
    backend: str
    elapsed_ms: float
    cache_hit: bool = False
    rationale: str = ""


class QueryRouter:
    def __init__(self, store, memory, cfg: RoutingConfig, metrics=None):
        self.store = store
        self.memory = memory
        self.cfg = cfg
        self.metrics = metrics
        self._cache: OrderedDict[tuple, tuple[float, list]] = OrderedDict()
        self.decisions: Counter = Counter()

    # ------------------------------------------------------------------
    def route(self, spec: QuerySpec) -> RouteResult:
        t0 = time.perf_counter()
        if spec.kind == "point":
            res = self._point(spec)
        elif spec.kind == "recent_agg":
            res = self._recent(spec)
        elif spec.kind == "heavy_agg":
            res = self._heavy(spec)
        else:
            raise ValueError(f"unknown query kind: {spec.kind!r}")
        res.elapsed_ms = round((time.perf_counter() - t0) * 1000, 3)
        self.decisions[(spec.kind, res.backend)] += 1
        if self.metrics:
            self.metrics.incr(f"route.{spec.kind}.{res.backend}")
            self.metrics.observe(f"route_latency.{res.backend}", res.elapsed_ms)
        return res

    # -- backends --------------------------------------------------------
    def _point(self, spec: QuerySpec) -> RouteResult:
        if not spec.user_id:
            raise ValueError("point queries require user_id")
        rows = self.store.query_user_events(spec.user_id, limit=50)
        return RouteResult(
            rows, "store.indexed", 0.0,
            rationale="selective point lookup served by the composite index (user_id, ts)",
        )

    def _recent(self, spec: QuerySpec) -> RouteResult:
        if spec.minutes <= self.cfg.hot_window_minutes:
            agg = self.memory.aggregate(spec.minutes, spec.region, spec.event_type,
                                        spec.group_by)
            return RouteResult(
                agg["rows"], "memory.hot_layer", 0.0,
                rationale=(f"{spec.minutes} min window is inside the hot layer "
                           f"({self.cfg.hot_window_minutes} min) — served from in-memory "
                           f"window aggregates with no disk I/O"),
            )
        from datetime import datetime, timedelta, timezone
        end = spec.end_iso or datetime.now(timezone.utc).isoformat()
        start = spec.start_iso or (datetime.now(timezone.utc)
                                   - timedelta(minutes=spec.minutes)).isoformat()
        rows = self.store.query_hourly_summary(start, end, spec.region, spec.event_type,
                                               spec.group_by)
        return RouteResult(
            rows, "store.summary", 0.0,
            rationale="beyond the hot layer — served from the pre-aggregated hourly summary",
        )

    def _heavy(self, spec: QuerySpec) -> RouteResult:
        if not spec.start_iso or not spec.end_iso:
            raise ValueError("heavy_agg requires start_iso and end_iso")
        key = ("heavy", spec.group_by, spec.region, spec.event_type, spec.start_iso, spec.end_iso)
        cached = self._cache_get(key)
        if cached is not None:
            return RouteResult(
                cached, "cache", 0.0, cache_hit=True,
                rationale=f"TTL cache hit (ttl={self.cfg.cache_ttl_seconds}s)",
            )
        rows = self.store.query_hourly_summary(spec.start_iso, spec.end_iso, spec.region,
                                               spec.event_type, spec.group_by)
        if rows:
            self._cache_put(key, rows)
            return RouteResult(
                rows, "store.summary", 0.0,
                rationale=("heavy historical aggregate served from the pre-aggregated "
                           "hourly summary instead of a raw event scan"),
            )
        if self.cfg.spark_enabled:
            # Reserved hook: the Spark batch job (src/spark/batch_job.py) can answer
            # heavy aggregates at cluster scale when wired in.
            log.info("spark backend enabled but no summary coverage; using raw scan fallback")
        rows = self.store.query_aggregate_raw(spec.start_iso, spec.end_iso, spec.region,
                                              spec.event_type, spec.group_by)
        return RouteResult(
            rows, "store.raw_scan", 0.0,
            rationale="no summary coverage for this range — fell back to a raw scan (cold path)",
        )

    # -- cache -------------------------------------------------------------
    def _cache_get(self, key: tuple):
        entry = self._cache.get(key)
        if entry is None:
            return None
        expires, rows = entry
        if time.monotonic() > expires:
            del self._cache[key]
            return None
        self._cache.move_to_end(key)
        return rows

    def _cache_put(self, key: tuple, rows: list) -> None:
        self._cache[key] = (time.monotonic() + self.cfg.cache_ttl_seconds, rows)
        self._cache.move_to_end(key)
        while len(self._cache) > self.cfg.cache_max_items:
            self._cache.popitem(last=False)
