import unittest

from src.ingest.generator import EventGenerator
from src.parallel.executor import ParallelExecutor
from src.stream.transformations import transform_batch


class TestParallelExecutor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.events = EventGenerator(seed=3).bulk(3000, spread_seconds=3600)

    def test_order_preserved(self):
        ex = ParallelExecutor(workers=4, backend="thread", chunk_size=500, min_batch=1000)
        parts = ex.map_chunks(lambda items: list(items), list(range(3000)))
        merged = [x for part in parts for x in part]
        self.assertEqual(merged, list(range(3000)))

    def test_transform_matches_sequential(self):
        ex = ParallelExecutor(workers=4, backend="thread", chunk_size=500, min_batch=1000)
        parts = ex.map_chunks(transform_batch, self.events)
        rows_par = [r for part in parts for r in part[0]]
        rows_seq, _ = transform_batch(self.events)
        self.assertEqual(rows_par, rows_seq)

    def test_small_batch_runs_inline(self):
        ex = ParallelExecutor(workers=4, backend="process", chunk_size=500, min_batch=100000)
        parts = ex.map_chunks(lambda items: ["ran-inline"], [1, 2, 3])
        self.assertEqual(parts, [["ran-inline"]])

    def test_dropped_counts_merge(self):
        events = self.events + [self.events[0].__class__(
            event_id="x", ts=self.events[0].ts, user_id="u", session_id="s",
            event_type="nope", region="us-east", device="mobile", amount=None, created_ns=0)]
        ex = ParallelExecutor(workers=2, backend="thread", chunk_size=1000, min_batch=1000)
        parts = ex.map_chunks(transform_batch, events)
        dropped = {}
        for _r, d in parts:
            for k, v in d.items():
                dropped[k] = dropped.get(k, 0) + v
        self.assertEqual(dropped.get("bad_event_type"), 1)


if __name__ == "__main__":
    unittest.main()
