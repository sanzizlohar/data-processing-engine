import unittest

from src.config import RoutingConfig
from src.routing.router import QueryRouter, QuerySpec
from src.stream.memory import MemoryAggregates


class FakeStore:
    name = "fake.indexed"

    def query_user_events(self, user_id, limit=50):
        return [{"event_id": "e1", "user_id": user_id}]

    def query_hourly_summary(self, start, end, region=None, event_type=None, group_by="region"):
        return [{"dim": "us-east", "events": 100, "revenue": 32.0}]

    def query_aggregate_raw(self, start, end, region=None, event_type=None, group_by="region"):
        return [{"dim": "us-east", "events": 100, "revenue": 32.0}]


def hot_rows() -> list[tuple]:
    base = ("2026-09-04T12:3", )
    rows = []
    for i in range(5):
        rows.append(("e%d" % i, f"{base[0]}{i}:00+00:00", "u1", "s1", "purchase",
                     "us-east", "mobile", 10.0, 3.2, 12, 4, "low"))
    return rows


class TestRouter(unittest.TestCase):
    def setUp(self):
        self.memory = MemoryAggregates(hot_minutes=60)
        self.memory.update(hot_rows())
        self.router = QueryRouter(FakeStore(), self.memory,
                                  RoutingConfig(hot_window_minutes=60, cache_ttl_seconds=60))

    def test_point_goes_to_indexed_store(self):
        res = self.router.route(QuerySpec(kind="point", user_id="u1"))
        self.assertEqual(res.backend, "store.indexed")
        self.assertEqual(res.rows[0]["user_id"], "u1")

    def test_recent_agg_uses_hot_layer(self):
        res = self.router.route(QuerySpec(kind="recent_agg", minutes=30))
        self.assertEqual(res.backend, "memory.hot_layer")
        self.assertEqual(len(res.rows), 1)
        self.assertEqual(res.rows[0]["dim"], "us-east")
        self.assertEqual(res.rows[0]["events"], 5)

    def test_old_recent_agg_uses_summary(self):
        res = self.router.route(QuerySpec(kind="recent_agg", minutes=500))
        self.assertEqual(res.backend, "store.summary")

    def test_heavy_agg_uses_summary_then_cache(self):
        spec = QuerySpec(kind="heavy_agg", start_iso="2026-09-03T00:00:00+00:00",
                         end_iso="2026-09-04T00:00:00+00:00")
        first = self.router.route(spec)
        self.assertEqual(first.backend, "store.summary")
        self.assertFalse(first.cache_hit)
        second = self.router.route(spec)
        self.assertEqual(second.backend, "cache")
        self.assertTrue(second.cache_hit)

    def test_unknown_kind_raises(self):
        with self.assertRaises(ValueError):
            self.router.route(QuerySpec(kind="wild"))

    def test_point_requires_user(self):
        with self.assertRaises(ValueError):
            self.router.route(QuerySpec(kind="point"))

    def test_decisions_are_recorded(self):
        self.router.route(QuerySpec(kind="point", user_id="u1"))
        self.router.route(QuerySpec(kind="recent_agg", minutes=30))
        kinds = {k for k, _ in self.router.decisions}
        self.assertIn("point", kinds)
        self.assertIn("recent_agg", kinds)


if __name__ == "__main__":
    unittest.main()
