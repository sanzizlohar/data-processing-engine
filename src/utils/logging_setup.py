"""Console logging setup shared by all entry points."""
from __future__ import annotations

import logging
import sys


def configure(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(name)-20s %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
        force=True,
    )
