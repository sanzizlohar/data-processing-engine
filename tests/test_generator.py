import unittest
from datetime import timedelta

from src.events import DEVICES, EVENT_TYPES, REGIONS
from src.ingest.generator import EventGenerator


class TestGenerator(unittest.TestCase):
    def test_bulk_is_deterministic_with_seed(self):
        a = EventGenerator(seed=42).bulk(500, spread_seconds=3600)
        b = EventGenerator(seed=42).bulk(500, spread_seconds=3600)
        self.assertEqual([e.event_type for e in a], [e.event_type for e in b])
        self.assertEqual([e.user_id for e in a], [e.user_id for e in b])

    def test_bulk_count_and_spread(self):
        events = EventGenerator(seed=1).bulk(1000, spread_seconds=86400)
        self.assertEqual(len(events), 1000)
        span = events[-1].ts - events[0].ts
        self.assertLessEqual(span, timedelta(seconds=86400))
        self.assertEqual(events, sorted(events, key=lambda e: e.ts))

    def test_event_fields_valid(self):
        events = EventGenerator(seed=2).bulk(500)
        for e in events:
            self.assertIn(e.event_type, EVENT_TYPES)
            self.assertIn(e.region, REGIONS)
            self.assertIn(e.device, DEVICES)
            self.assertTrue(e.user_id.startswith("u"))
            if e.event_type in ("purchase", "refund"):
                self.assertIsNotNone(e.amount)
                self.assertGreater(e.amount, 0)
            else:
                self.assertIsNone(e.amount)

    def test_volume_capability(self):
        """50K+ daily records is well within reach of a single generator pass."""
        import time
        t0 = time.perf_counter()
        events = EventGenerator(seed=5).bulk(50000)
        dt = time.perf_counter() - t0
        self.assertEqual(len(events), 50000)
        self.assertLess(dt, 30.0)  # generous: generation is ~10-20k events/s


if __name__ == "__main__":
    unittest.main()
