#!/usr/bin/env python
"""Seed storage with a day of events (default 60K) + aggregate backfill.

Demonstrates the 50K+ daily-records capacity of the engine.
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config import Config  # noqa: E402
from src.storage.base import create_store  # noqa: E402
from src.tools.seed import seed_events  # noqa: E402
from src.utils.logging_setup import configure  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed a day of events into storage")
    parser.add_argument("--n", type=int, default=60000)
    parser.add_argument("--spread-hours", type=float, default=24.0)
    parser.add_argument("--fresh", action="store_true", help="delete the sqlite file first")
    args = parser.parse_args()

    configure("INFO")
    cfg = Config.load()
    if args.fresh and cfg.storage.backend in ("auto", "sqlite"):
        p = Path(cfg.storage.sqlite_path)
        if p.exists():
            p.unlink()
            print(f"removed {p}")
    store = create_store(cfg)
    stats = seed_events(store, n=args.n, spread_hours=args.spread_hours)
    print(f"seed stats: {stats}")
    print(f"counts: {store.counts()}")


if __name__ == "__main__":
    main()
