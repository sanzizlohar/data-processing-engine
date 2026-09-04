import unittest
from datetime import datetime, timezone

from src.events import Event
from src.stream.transformations import transform_batch


def make_event(**overrides) -> Event:
    defaults = dict(
        event_id="e1",
        ts=datetime(2026, 9, 4, 12, 30, tzinfo=timezone.utc),
        user_id="u00001",
        session_id="s123",
        event_type="purchase",
        region="us-east",
        device="mobile",
        amount=100.00,
        created_ns=0,
    )
    defaults.update(overrides)
    return Event(**defaults)


class TestTransformations(unittest.TestCase):
    def test_valid_purchase_enrichment(self):
        rows, dropped = transform_batch([make_event()])
        self.assertEqual(dropped, {})
        self.assertEqual(len(rows), 1)
        r = rows[0]
        # revenue = amount * purchase margin * region fx = 100 * 0.32 * 1.00
        self.assertAlmostEqual(r[8], 32.0, places=4)
        self.assertEqual(r[9], 12)                       # hour
        self.assertEqual(r[10], make_event().ts.weekday())
        self.assertEqual(r[11], "high")                  # 100 -> high band (>= 100 < 1000)

    def test_invalid_event_type_dropped(self):
        rows, dropped = transform_batch([make_event(event_type="hack")])
        self.assertEqual(rows, [])
        self.assertEqual(dropped.get("bad_event_type"), 1)

    def test_negative_amount_dropped(self):
        rows, dropped = transform_batch([make_event(amount=-5.0)])
        self.assertEqual(rows, [])
        self.assertEqual(dropped.get("negative_amount"), 1)

    def test_purchase_without_amount_dropped(self):
        rows, dropped = transform_batch([make_event(amount=None)])
        self.assertEqual(rows, [])
        self.assertEqual(dropped.get("missing_amount"), 1)

    def test_view_without_amount_is_valid(self):
        rows, dropped = transform_batch([make_event(event_type="view", amount=None)])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][8], 0.0)  # revenue 0 for views
        self.assertEqual(rows[0][11], "none")

    def test_refund_revenue_is_negative(self):
        rows, _ = transform_batch([make_event(event_type="refund", amount=50.0,
                                              region="eu-central")])
        # 50 * -0.35 * 1.08
        self.assertAlmostEqual(rows[0][8], -18.9, places=4)

    def test_batch_totals(self):
        events = [make_event(event_id=f"e{i}") for i in range(50)]
        events.append(make_event(event_id="bad", region="mars"))
        rows, dropped = transform_batch(events)
        self.assertEqual(len(rows), 50)
        self.assertEqual(dropped.get("bad_region"), 1)


if __name__ == "__main__":
    unittest.main()
