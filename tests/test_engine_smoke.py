import tempfile
import unittest
from pathlib import Path

from src.config import Config
from src.insights.engine import InsightsEngine
from src.ingest.generator import EventGenerator
from src.parallel.executor import ParallelExecutor
from src.storage.sqlite_store import SqliteStore
from src.stream.engine import StreamingEngine
from src.stream.memory import MemoryAggregates


class EngineSmokeTest(unittest.IsolatedAsyncioTestCase):
    async def test_engine_processes_stream_end_to_end(self):
        cfg = Config.load(path="/dev/null" if Path("/dev/null").exists() else None)
        cfg.storage.backend = "sqlite"
        cfg.engine.insights_every_n_batches = 10
        cfg.generator.time_scale = 20.0  # compress event-time so windows rotate

        tmp = tempfile.TemporaryDirectory()
        try:
            store = SqliteStore(str(Path(tmp.name) / "engine.db"))
            store.init_schema()
            memory = MemoryAggregates(hot_minutes=60)
            insights = InsightsEngine(cfg.insights)
            engine = StreamingEngine(
                cfg, store, memory, insights,
                generator=EventGenerator(seed=11),
                executor=ParallelExecutor(backend="inline"),
            )
            stats = await engine.run(eps=1000, duration=3.0)

            counters = stats["counters"]
            self.assertGreater(counters.get("events_processed", 0), 500)
            self.assertIn("stream_latency_ms", stats["timings"])
            lat = stats["timings"]["stream_latency_ms"]
            self.assertGreater(lat["count"], 500)
            # sub-second stream: p95 ingestion->visible must stay under 1s
            self.assertLess(lat["p95_ms"], 1000.0)
            self.assertGreater(store.counts()["events"], 500)
            self.assertGreater(store.counts()["aggregates_1m"], 0)
            store.close()
        finally:
            tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
