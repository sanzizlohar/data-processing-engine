import unittest
from datetime import datetime, timedelta, timezone

from src.config import InsightsConfig
from src.insights.engine import InsightsEngine


def window(rows: dict) -> list[dict]:
    """rows: {(region, event_type): (events, revenue)} -> window aggregate rows."""
    return [
        {"window_start": "2026-09-04T12:30", "region": r, "event_type": et,
         "events": c, "revenue": rev}
        for (r, et), (c, rev) in rows.items()
    ]


class TestInsights(unittest.TestCase):
    def make_engine(self, **cfg_overrides) -> InsightsEngine:
        cfg = InsightsConfig(cooldown_seconds=0, zscore_threshold=2.5, **cfg_overrides)
        return InsightsEngine(cfg)

    def feed_steady(self, engine: InsightsEngine, n: int = 10) -> None:
        for i in range(n):
            engine.process_windows(window({
                ("us-east", "purchase"): (100, 320.0),
                ("eu-central", "purchase"): (80, 276.0),
                ("us-east", "view"): (400, 0.0),
            }))

    def test_spike_triggers_anomaly(self):
        engine = self.make_engine()
        self.feed_steady(engine)
        insights = engine.process_windows(window({
            ("us-east", "purchase"): (100, 1500.0),  # 4.7x baseline revenue
            ("eu-central", "purchase"): (80, 276.0),
            ("us-east", "view"): (400, 0.0),
        }))
        kinds = [i.kind for i in insights]
        self.assertIn("anomaly_spike", kinds)

    def test_no_anomaly_on_steady_stream(self):
        engine = self.make_engine()
        self.feed_steady(engine)
        insights = engine.process_windows(window({
            ("us-east", "purchase"): (101, 323.0),
            ("eu-central", "purchase"): (80, 276.0),
            ("us-east", "view"): (400, 0.0),
        }))
        self.assertNotIn("anomaly_spike", [i.kind for i in insights])
        self.assertNotIn("anomaly_drop", [i.kind for i in insights])

    def test_cooldown_suppresses_duplicates(self):
        engine = InsightsEngine(InsightsConfig(cooldown_seconds=3600, zscore_threshold=2.5))
        spike = lambda rev: window({  # noqa: E731
            ("us-east", "purchase"): (100, rev),
            ("eu-central", "purchase"): (80, 276.0),
        })
        self.feed_steady(engine)
        first = engine.process_windows(spike(1500.0))
        self.feed_steady(engine, n=6)
        second = engine.process_windows(spike(1600.0))
        first_anoms = [i for i in first if i.kind == "anomaly_spike"]
        second_anoms = [i for i in second if i.kind == "anomaly_spike"]
        self.assertEqual(len(first_anoms), 1)
        self.assertEqual(len(second_anoms), 0)  # suppressed by cooldown

    def test_trend_detection(self):
        engine = self.make_engine(trend_min_points=8, trend_sensitivity=0.1)
        for i in range(9):
            engine.process_windows(window({
                ("us-east", "purchase"): (100 + i * 15, 320.0 + i * 48),
            }))
        insights = engine.process_windows(window({
            ("us-east", "purchase"): (100 + 9 * 15, 320.0 + 9 * 48),
        }))
        self.assertIn("trend_up", [i.kind for i in insights])

    def test_refund_rate_threshold(self):
        engine = self.make_engine()
        insights = engine.process_windows(window({
            ("us-east", "purchase"): (100, 320.0),
            ("us-east", "refund"): (12, -20.0),
        }))
        thresholds = [i for i in insights if i.kind == "threshold"]
        self.assertEqual(len(thresholds), 1)
        self.assertAlmostEqual(thresholds[0].value, 0.12, places=4)

    def test_top_mover(self):
        engine = self.make_engine()
        steady = window({
            ("us-east", "purchase"): (100, 500.0),
            ("latam", "purchase"): (100, 500.0),
        })
        engine.process_windows(steady)
        mover = window({
            ("us-east", "purchase"): (100, 950.0),
            ("latam", "purchase"): (100, 50.0),
        })
        insights = engine.process_windows(mover)
        movers = [i for i in insights if i.kind == "top_mover"]
        self.assertEqual(len(movers), 1)
        self.assertEqual(movers[0].entity, "us-east")


if __name__ == "__main__":
    unittest.main()
