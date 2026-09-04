"""Event model and shared domain constants.

The same enrichment tables (REGION_FX / TYPE_MARGIN) are mirrored in the Spark
job so both pipeline paths produce identical semantics.
"""
from __future__ import annotations

import json
import random
import time
import uuid
from dataclasses import dataclass
from datetime import datetime

EVENT_TYPES = ("view", "click", "add_to_cart", "purchase", "refund")
REGIONS = ("us-east", "us-west", "eu-central", "ap-south", "latam")
DEVICES = ("mobile", "desktop", "tablet")

REGION_FX = {"us-east": 1.00, "us-west": 1.00, "eu-central": 1.08, "ap-south": 0.93, "latam": 1.12}
TYPE_MARGIN = {"view": 0.0, "click": 0.01, "add_to_cart": 0.02, "purchase": 0.32, "refund": -0.35}

EVENT_COLUMNS = (
    "event_id", "ts", "user_id", "session_id", "event_type",
    "region", "device", "amount", "revenue", "hour", "day_of_week", "amount_band",
)

_TYPE_WEIGHTS = (0.45, 0.25, 0.12, 0.15, 0.03)


@dataclass(slots=True)
class Event:
    event_id: str
    ts: datetime
    user_id: str
    session_id: str
    event_type: str
    region: str
    device: str
    amount: float | None
    created_ns: int  # wall-clock ingestion timestamp, drives stream latency metrics

    def to_json(self) -> str:
        return json.dumps({
            "event_id": self.event_id,
            "ts": self.ts.isoformat(),
            "user_id": self.user_id,
            "session_id": self.session_id,
            "event_type": self.event_type,
            "region": self.region,
            "device": self.device,
            "amount": self.amount,
            "created_ns": self.created_ns,
        })


def amount_band(amount: float | None) -> str:
    if amount is None:
        return "none"
    if amount < 10:
        return "low"
    if amount < 100:
        return "mid"
    if amount < 1000:
        return "high"
    return "premium"


def random_event(rng: random.Random, ts: datetime | None = None) -> Event:
    """Generate one realistic event. `rng` may be a seeded Random or the random module."""
    if ts is None:
        ts = datetime.now()
    et = rng.choices(EVENT_TYPES, weights=_TYPE_WEIGHTS, k=1)[0]
    amount = None
    if et in ("purchase", "refund"):
        amount = round(rng.lognormvariate(3.6, 0.9), 2)  # median spend ~36
    return Event(
        event_id=uuid.uuid4().hex,
        ts=ts,
        user_id=f"u{rng.randrange(5000):05d}",
        session_id=uuid.uuid4().hex[:8],
        event_type=et,
        region=rng.choice(REGIONS),
        device=rng.choice(DEVICES),
        amount=amount,
        created_ns=time.time_ns(),
    )
