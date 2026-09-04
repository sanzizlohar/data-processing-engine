"""In-memory hot layer: 1-minute window aggregates for sub-second analytics.

Serves recent-window queries without touching disk and tracks dirty deltas so
the engine can flush incremental upserts to storage.
"""
from __future__ import annotations

from collections import OrderedDict


class MemoryAggregates:
    def __init__(self, hot_minutes: int = 60):
        self.hot_minutes = hot_minutes
        # window_start(ISO minute) -> {(region, event_type): [events, revenue]}
        self._windows: OrderedDict[str, dict[tuple[str, str], list[float]]] = OrderedDict()
        self._dirty_min: dict[tuple[str, str, str], list[float]] = {}
        self._dirty_hour: dict[tuple[str, str, str], list[float]] = {}

    def update(self, rows: list[tuple]) -> int:
        """Fold transformed storage rows into the window aggregates."""
        for r in rows:
            ts = r[1]
            minute = ts[:16]
            hour = ts[:13]
            et = r[4]
            region = r[5]
            revenue = r[8] or 0.0
            win = self._windows.setdefault(minute, {})
            bucket = win.get((region, et))
            if bucket is None:
                win[(region, et)] = [1, revenue]
            else:
                bucket[0] += 1
                bucket[1] += revenue
            for key_store, key in ((self._dirty_min, (minute, region, et)),
                                   (self._dirty_hour, (hour, region, et))):
                d = key_store.get(key)
                if d is None:
                    key_store[key] = [1, revenue]
                else:
                    d[0] += 1
                    d[1] += revenue
        self._prune()
        return len(rows)

    def _prune(self) -> None:
        # keep only the newest `hot_minutes` active minute-windows
        while len(self._windows) > self.hot_minutes:
            self._windows.popitem(last=False)

    def aggregate(self, minutes: int = 60, region: str | None = None,
                  event_type: str | None = None, group_by: str = "region") -> dict:
        """Aggregate the most recent `minutes` of hot windows, grouped by one dimension."""
        agg: dict[str, list[float]] = {}
        total_events = 0
        total_revenue = 0.0
        windows = list(self._windows)[-minutes:] if minutes else list(self._windows)
        for ws in windows:
            for (r, et), (cnt, rev) in self._windows[ws].items():
                if region and r != region:
                    continue
                if event_type and et != event_type:
                    continue
                dim = {"region": r, "event_type": et}.get(group_by, r)
                a = agg.setdefault(dim, [0, 0.0])
                a[0] += cnt
                a[1] += rev
                total_events += cnt
                total_revenue += rev
        rows = [{"dim": k, "events": int(v[0]), "revenue": round(v[1], 4)}
                for k, v in agg.items()]
        rows.sort(key=lambda x: -x["revenue"])
        return {
            "rows": rows,
            "events": total_events,
            "revenue": round(total_revenue, 4),
            "windows": len(windows),
        }

    def last_window_rows(self) -> list[dict]:
        """The most recent minute window, as insight-engine input rows."""
        if not self._windows:
            return []
        ws = next(reversed(self._windows))
        return [
            {"window_start": ws, "region": r, "event_type": et,
             "events": cnt, "revenue": round(rev, 4)}
            for (r, et), (cnt, rev) in self._windows[ws].items()
        ]

    def drain_dirty(self) -> tuple[list[tuple], list[tuple]]:
        """Return (1-minute, hourly) delta rows for storage upserts, then clear them."""
        min_rows = [(ws, r, et, c, round(rev, 6))
                    for (ws, r, et), (c, rev) in self._dirty_min.items()]
        hour_rows = [(ws, r, et, c, round(rev, 6))
                     for (ws, r, et), (c, rev) in self._dirty_hour.items()]
        self._dirty_min.clear()
        self._dirty_hour.clear()
        return min_rows, hour_rows
