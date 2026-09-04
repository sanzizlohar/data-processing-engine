#!/usr/bin/env python
"""Initialize storage schema (PostgreSQL when reachable, SQLite fallback)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config import Config  # noqa: E402
from src.storage.base import create_store  # noqa: E402
from src.utils.logging_setup import configure  # noqa: E402


def main() -> None:
    configure("INFO")
    cfg = Config.load()
    store = create_store(cfg)
    store.init_schema()
    print(f"storage ready: backend={store.name} counts={store.counts()}")


if __name__ == "__main__":
    main()
