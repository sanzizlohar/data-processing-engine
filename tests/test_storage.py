import tempfile
import unittest
from pathlib import Path

from src.storage.sqlite_store import SqliteStore


ROW = ("e1", "2026-09-04T12:30:15.000000+00:00", "u00001", "s1", "purchase",
       "us-east", "mobile", 100.0, 32.0, 12, 4, "mid")


class TestSqliteStore(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = SqliteStore(str(Path(self.tmp.name) / "test.db"))
        self.store.init_schema()

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def test_insert_and_count(self):
        self.store.insert_events([ROW, ROW])  # idempotent on event_id
        self.assertEqual(self.store.counts()["events"], 1)

    def test_aggregate_upsert_accumulates(self):
        row = ("2026-09-04T12:30", "us-east", "purchase", 2, 64.0)
        self.store.upsert_window_aggregates([row])
        self.store.upsert_window_aggregates([row])
        counts = self.store.counts()
        self.assertEqual(counts["aggregates_1m"], 1)

    def test_backfill_from_events(self):
        row2 = ("e2", "2026-09-04T12:31:20.000000+00:00", "u00002", "s2", "view",
                "eu-central", "desktop", None, 0.0, 12, 4, "none")
        self.store.insert_events([ROW, row2])
        self.store.backfill_aggregates()
        agg = self.store.query_aggregate_raw("2026-09-04T00:00:00+00:00",
                                             "2026-09-05T00:00:00+00:00")
        by_dim = {r["dim"]: r for r in agg}
        self.assertEqual(by_dim["us-east"]["events"], 1)
        self.assertEqual(by_dim["eu-central"]["events"], 1)
        summary = self.store.query_hourly_summary("2026-09-04T00:00:00+00:00",
                                                  "2026-09-05T00:00:00+00:00")
        self.assertEqual(len(summary), 2)

    def test_point_query(self):
        self.store.insert_events([ROW])
        rows = self.store.query_user_events("u00001")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["event_id"], "e1")
        self.assertEqual(self.store.sample_user(), "u00001")

    def test_insights_roundtrip(self):
        self.store.insert_insights([
            ("2026-09-04T12:30:00+00:00", "anomaly_spike", "warning", "us-east",
             "revenue", 1500.0, 320.0, "revenue spiked 4.7x"),
        ])
        latest = self.store.latest_insights(10)
        self.assertEqual(len(latest), 1)
        self.assertEqual(latest[0]["kind"], "anomaly_spike")

    def test_query_rows(self):
        self.store.insert_events([ROW])
        rows = self.store.query_rows("2026-09-04T00:00:00+00:00", "2026-09-05T00:00:00+00:00")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "e1")


if __name__ == "__main__":
    unittest.main()
