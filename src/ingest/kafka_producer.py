"""Optional Kafka producer — publishes generated events as JSON to a topic.

Used as the ingestion front-end for the Spark Structured Streaming path
(src/spark/streaming_job.py). Requires `pip install kafka-python`.
"""
from __future__ import annotations

import asyncio
import logging
import random

from src.events import random_event

log = logging.getLogger("ingest.kafka")


class KafkaEventProducer:
    def __init__(self, bootstrap: str = "localhost:9092", topic: str = "events"):
        try:
            from kafka import KafkaProducer
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "kafka-python is not installed: pip install -r requirements-extras.txt"
            ) from exc
        self.topic = topic
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            acks=1,
            linger_ms=5,
            value_serializer=lambda v: v.encode("utf-8"),
        )
        log.info("connected to kafka %s (topic=%s)", bootstrap, topic)

    def send(self, event) -> None:
        self._producer.send(self.topic, value=event.to_json())

    async def stream(self, eps: float = 500, duration: float | None = None) -> int:
        """Paced production loop mirroring EventGenerator.stream()."""
        loop = asyncio.get_running_loop()
        start = loop.time()
        tick = 0.1
        sent = 0
        rng = random.Random()
        while duration is None or loop.time() - start < duration:
            for _ in range(max(1, int(eps * tick))):
                self.send(random_event(rng))
                sent += 1
            await asyncio.sleep(tick)
        self.close()
        log.info("produced %d events to topic %s", sent, self.topic)
        return sent

    def close(self) -> None:
        self._producer.flush(5)
        self._producer.close(5)
