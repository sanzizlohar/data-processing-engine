"""Lightweight thread-safe metrics recorder: counters + latency histograms."""
from __future__ import annotations

import threading
import time
from collections import Counter, deque


def _percentile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, max(0, round(p / 100 * (len(sorted_vals) - 1))))
    return sorted_vals[idx]


class Metrics:
    def __init__(self, max_samples: int = 20000):
        self._lock = threading.Lock()
        self._counters: Counter = Counter()
        self._timings: dict[str, deque] = {}
        self._max_samples = max_samples

    def incr(self, name: str, n: int = 1) -> None:
        with self._lock:
            self._counters[name] += n

    def observe(self, name: str, value_ms: float) -> None:
        with self._lock:
            dq = self._timings.setdefault(name, deque(maxlen=self._max_samples))
            dq.append(value_ms)

    def timer(self, name: str):
        cm = self

        class _Timer:
            def __enter__(self):
                self.t0 = time.perf_counter()
                return self

            def __exit__(self, *exc):
                cm.observe(name, (time.perf_counter() - self.t0) * 1000)
                return False

        return _Timer()

    def snapshot(self) -> dict:
        with self._lock:
            counters = dict(self._counters)
            timings = {}
            for name, dq in self._timings.items():
                vals = sorted(dq)
                timings[name] = {
                    "count": len(vals),
                    "mean_ms": round(sum(vals) / len(vals), 3) if vals else 0.0,
                    "p50_ms": round(_percentile(vals, 50), 3),
                    "p95_ms": round(_percentile(vals, 95), 3),
                    "p99_ms": round(_percentile(vals, 99), 3),
                    "max_ms": round(vals[-1], 3) if vals else 0.0,
                }
        return {"counters": counters, "timings": timings}
