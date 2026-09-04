"""Parallel execution optimization.

Chunked fan-out of pure batch functions across a worker pool:
  * backend="process" — true multi-core CPU parallelism for transform work
  * backend="thread"  — for tests / IO-flavoured work
  * backend="inline"  — sequential, below the parallel threshold

The pool is created lazily and reused across calls (workers are long-lived in
production, so pool warm-up is amortized). Small batches run inline to avoid
serialization overhead on the hot path.
"""
from __future__ import annotations

import asyncio
import os
from concurrent.futures import Executor


class ParallelExecutor:
    def __init__(self, workers: int = 0, backend: str = "auto",
                 chunk_size: int = 5000, min_batch: int = 5000):
        self.workers = workers if workers > 0 else max(1, (os.cpu_count() or 2) - 1)
        self.backend = backend if backend != "auto" else ("process" if (os.cpu_count() or 1) > 1 else "inline")
        self.chunk_size = chunk_size
        self.min_batch = min_batch
        self._executor: Executor | None = None

    def _ensure(self) -> Executor:
        if self._executor is None:
            if self.backend == "process":
                from concurrent.futures import ProcessPoolExecutor
                self._executor = ProcessPoolExecutor(max_workers=self.workers)
            else:
                from concurrent.futures import ThreadPoolExecutor
                self._executor = ThreadPoolExecutor(max_workers=self.workers)
        return self._executor

    def _should_fan_out(self, n_items: int) -> bool:
        return not (self.backend == "inline" or n_items < self.min_batch or self.workers == 1)

    def chunks(self, items: list) -> list[list]:
        if len(items) <= self.chunk_size:
            return [items]
        return [items[i:i + self.chunk_size] for i in range(0, len(items), self.chunk_size)]

    def map_chunks(self, fn, items: list) -> list:
        """Blocking fan-out: fn(chunk) -> list per chunk, concatenated in order."""
        if not self._should_fan_out(len(items)):
            return [fn(items)]
        pool = self._ensure()
        futures = [pool.submit(fn, chunk) for chunk in self.chunks(items)]
        return [f.result() for f in futures]

    def map_items(self, fn, items: list) -> list:
        """Fan-out where fn is applied per item (one task per item), results in order.

        Use when each item is already a self-contained unit of work
        (e.g. a (seed, offset, count) generation spec).
        """
        if self.backend == "inline" or self.workers == 1:
            return [fn(it) for it in items]
        pool = self._ensure()
        futures = [pool.submit(fn, it) for it in items]
        return [f.result() for f in futures]

    async def async_map(self, loop, fn, items: list) -> list:
        """Async fan-out for use inside the streaming engine's event loop."""
        if not self._should_fan_out(len(items)):
            return [fn(items)]
        pool = self._ensure()
        futures = [loop.run_in_executor(pool, fn, chunk) for chunk in self.chunks(items)]
        return await asyncio.gather(*futures)

    def shutdown(self) -> None:
        if self._executor is not None:
            self._executor.shutdown(wait=False)
            self._executor = None
