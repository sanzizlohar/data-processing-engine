"""Storage abstraction + backend factory.

PostgreSQL is the primary production backend; SQLite is a drop-in fallback so
the whole engine runs with zero infrastructure. Both implement the same
interface, so the streaming engine, insights engine and query router are
backend-agnostic.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod

log = logging.getLogger("storage")

# Columns the aggregate queries may GROUP BY (injection-safe whitelist).
GROUPABLE = ("region", "event_type", "device", "hour")


class StorageBackend(ABC):
    name = "base"

    @abstractmethod
    def init_schema(self) -> None: ...

    @abstractmethod
    def ping(self) -> bool: ...

    @abstractmethod
    def insert_events(self, rows) -> None: ...

    @abstractmethod
    def upsert_window_aggregates(self, rows) -> None: ...

    @abstractmethod
    def upsert_hourly_summary(self, rows) -> None: ...

    @abstractmethod
    def insert_insights(self, rows) -> None: ...

    @abstractmethod
    def query_user_events(self, user_id: str, limit: int = 50) -> list[dict]: ...

    @abstractmethod
    def query_aggregate_raw(self, start_iso: str, end_iso: str, region=None,
                            event_type=None, group_by: str = "region") -> list[dict]: ...

    @abstractmethod
    def query_hourly_summary(self, start_iso: str, end_iso: str, region=None,
                             event_type=None, group_by: str = "region") -> list[dict]: ...

    @abstractmethod
    def backfill_aggregates(self) -> None: ...

    @abstractmethod
    def latest_insights(self, limit: int = 20) -> list[dict]: ...

    @abstractmethod
    def counts(self) -> dict: ...

    # Optional helpers (used by benchmark / demos)
    def insert_events_rowwise(self, rows, commit_every: int = 1000) -> None:
        raise NotImplementedError

    def query_rows(self, start_iso: str, end_iso: str) -> list[tuple]:
        raise NotImplementedError

    def sample_user(self) -> str | None:
        raise NotImplementedError

    def close(self) -> None:
        pass


def create_store(cfg):
    """Create the configured backend. backend=auto prefers PostgreSQL when reachable."""
    from src.storage.postgres_store import PostgresStore
    from src.storage.sqlite_store import SqliteStore

    backend = cfg.storage.backend
    if backend == "sqlite":
        store = SqliteStore(cfg.storage.sqlite_path)
        store.init_schema()
        log.info("storage backend: sqlite (%s)", cfg.storage.sqlite_path)
        return store
    if backend == "postgres":
        store = PostgresStore(cfg.storage.pg_dsn)
        store.init_schema()
        log.info("storage backend: postgresql")
        return store
    # auto: prefer postgres when reachable, else fall back to sqlite
    try:
        store = PostgresStore(cfg.storage.pg_dsn)
        store.init_schema()
        log.info("storage backend: postgresql")
        return store
    except Exception as exc:
        log.info("postgres not reachable (%s); falling back to sqlite", exc)
        store = SqliteStore(cfg.storage.sqlite_path)
        store.init_schema()
        log.info("storage backend: sqlite (%s)", cfg.storage.sqlite_path)
        return store
