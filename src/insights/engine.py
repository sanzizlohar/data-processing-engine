"""Automated insights generation.

Consumes per-minute window aggregates from the hot layer and produces four
kinds of insight without human intervention:
  * anomaly_spike / anomaly_drop — rolling z-score vs a per-entity baseline
  * trend_up / trend_down        — normalized linear slope over the baseline
  * top_mover                    — window-over-window revenue share shifts
  * threshold                    — configurable rate alerts (refund rate)

A per-(kind, entity) cooldown keeps the insight stream signal-dense.
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone

import numpy as np

from src.config import InsightsConfig


@dataclass(slots=True)
class Insight:
    ts: str
    kind: str      # anomaly_spike | anomaly_drop | trend_up | trend_down | top_mover | threshold
    severity: str  # info | warning | critical
    entity: str
    metric: str
    value: float
    baseline: float
    message: str

    def to_row(self) -> tuple:
        return (self.ts, self.kind, self.severity, self.entity, self.metric,
                self.value, self.baseline, self.message)


class InsightsEngine:
    def __init__(self, cfg: InsightsConfig):
        self.cfg = cfg
        self._history: dict[tuple[str, str], deque] = {}
        self._cooldown: dict[tuple[str, str], float] = {}
        self._prev_shares: dict[str, float] | None = None
        self.generated = 0
        self.recent: deque[Insight] = deque(maxlen=200)

    def process_windows(self, window_rows: list[dict]) -> list[Insight]:
        """Run all detectors over one minute-window of aggregate rows."""
        insights: list[Insight] = []
        region_rev: dict[str, float] = {}
        type_counts: dict[str, int] = {}
        total_rev = 0.0
        for row in window_rows:
            region_rev[row["region"]] = region_rev.get(row["region"], 0.0) + (row["revenue"] or 0.0)
            type_counts[row["event_type"]] = type_counts.get(row["event_type"], 0) + row["events"]
            total_rev += row["revenue"] or 0.0
        now_iso = datetime.now(timezone.utc).isoformat()
        for region, rev in sorted(region_rev.items()):
            insights += self._check_metric("revenue", region, rev, now_iso)
        self._check_threshold(type_counts, now_iso, insights)
        self._check_top_movers(region_rev, total_rev, now_iso, insights)
        for ins in insights:
            self.generated += 1
            self.recent.append(ins)
        return insights

    # -- detectors ---------------------------------------------------------
    def _check_metric(self, metric: str, entity: str, value: float, now_iso: str) -> list[Insight]:
        key = (metric, entity)
        dq = self._history.setdefault(key, deque(maxlen=self.cfg.baseline_windows))
        out: list[Insight] = []
        baseline = list(dq)
        if len(baseline) >= 5:
            mean = float(np.mean(baseline))
            std = float(np.std(baseline))
            # floor std so a perfectly flat baseline still flags deviations
            # (1% of the mean, or a small epsilon when the mean is ~0)
            std = max(std, max(0.01 * abs(mean), 1e-6))
            z = (value - mean) / std
            if abs(z) >= self.cfg.zscore_threshold and self._cooldown_ok("anomaly", f"{metric}:{entity}"):
                sev = "critical" if abs(z) >= self.cfg.critical_zscore else "warning"
                kind = "anomaly_spike" if z > 0 else "anomaly_drop"
                out.append(Insight(
                    now_iso, kind, sev, entity, metric, round(value, 2), round(mean, 2),
                    f"{metric} for {entity} is {abs(z):.1f} sigma "
                    f"{'above' if z > 0 else 'below'} baseline "
                    f"({value:.2f} vs mean {mean:.2f})",
                ))
        dq.append(value)
        if len(dq) >= self.cfg.trend_min_points:
            vals = list(dq)
            slope = float(np.polyfit(np.arange(len(vals)), vals, 1)[0])
            mean2 = float(np.mean(vals)) or 1.0
            norm = slope * len(vals) / abs(mean2)
            if abs(norm) >= self.cfg.trend_sensitivity and self._cooldown_ok("trend", f"{metric}:{entity}"):
                kind = "trend_up" if norm > 0 else "trend_down"
                out.append(Insight(
                    now_iso, kind, "info", entity, metric, round(value, 2), round(mean2, 2),
                    f"{metric} for {entity} is {'rising' if norm > 0 else 'declining'} steadily "
                    f"({norm:+.0%} over the last {len(vals)} windows)",
                ))
        return out

    def _check_threshold(self, type_counts: dict[str, int], now_iso: str, out: list[Insight]) -> None:
        purchases = type_counts.get("purchase", 0)
        refunds = type_counts.get("refund", 0)
        if purchases >= self.cfg.min_purchases_for_rate:
            rate = refunds / purchases
            if rate > self.cfg.refund_rate_threshold and self._cooldown_ok("threshold", "refund_rate"):
                out.append(Insight(
                    now_iso, "threshold", "warning", "refund_rate", "refund_rate",
                    round(rate, 4), self.cfg.refund_rate_threshold,
                    f"refund rate {rate:.1%} exceeds threshold "
                    f"{self.cfg.refund_rate_threshold:.0%} ({refunds}/{purchases} purchases)",
                ))

    def _check_top_movers(self, region_rev: dict[str, float], total_rev: float,
                          now_iso: str, out: list[Insight]) -> None:
        if total_rev <= 0 or not region_rev:
            self._prev_shares = None
            return
        shares = {r: rev / total_rev for r, rev in region_rev.items()}
        prev = self._prev_shares
        if prev and self._cooldown_ok("top_mover", "region_share"):
            for r, share in shares.items():
                delta = share - prev.get(r, 0.0)
                if abs(delta) >= 0.05:
                    direction = "gained" if delta > 0 else "lost"
                    out.append(Insight(
                        now_iso, "top_mover", "info", r, "revenue_share",
                        round(share, 4), round(prev.get(r, 0.0), 4),
                        f"{r} {direction} {abs(delta):.1%} of revenue share "
                        f"window-over-window ({prev.get(r, 0.0):.1%} -> {share:.1%})",
                    ))
                    break  # one mover per window keeps the signal clean
        self._prev_shares = shares

    def _cooldown_ok(self, kind: str, entity: str) -> bool:
        key = (kind, entity)
        now = time.monotonic()
        last = self._cooldown.get(key)
        if last is not None and (now - last) < self.cfg.cooldown_seconds:
            return False
        self._cooldown[key] = now
        return True
