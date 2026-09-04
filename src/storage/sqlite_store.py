"""SQLite backend — zero-dependency fallback implementing the storage interface.

Uses WAL mode, composite indexes and batched upserts; serves as the indexed
point-query and pre-aggregated summary backend in demo environments.
"""
from __future__ import annotations

import logging
import sqlite3
import threading
from pathlib import Path

from src.storage.base import StorageBackend

log = logging.getLogger("storage.sqlite")

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_id    TEXT PRIMARY KEY,
    ts          TEXT NOT NULL,
    user_id     TEXT NOT NULL,
    session_id  TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    region      TEXT NOT NULL,
    device      TEXT NOT NULL,
    amount      REAL,
    revenue     REAL,
    hour        INTEGER,
    day_of_week INTEGER,
    amount_band TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_ts        ON events (ts);
CREATE INDEX IF NOT EXISTS idx_events_user_ts   ON events (user_id, ts);
CREATE INDEX IF NOT EXISTS idx_events_region_ts ON events (region, ts);
CREATE INDEX IF NOT EXISTS idx_events_type_ts   ON events (event_type, ts);

CREATE TABLE IF NOT EXISTS aggregates_1m (
    window_start TEXT NOT NULL,
    region       TEXT NOT NULL,
    event_type   TEXT NOT NULL,
    events       INTEGER NOT NULL DEFAULT 0,
    revenue      REAL NOT NULL DEFAULT 0,
    PRIMARY KEY (window_start, region, event_type)
);

CREATE TABLE IF NOT EXISTS summary_hourly (
    window_start TEXT NOT NULL,
    region       TEXT NOT NULL,
    event_type   TEXT NOT NULL,
    events       INTEGER NOT NULL DEFAULT 0,
    revenue      REAL NOT NULL DEFAULT 0,
    PRIMARY KEY (window_start, region, event_type)
);

CREATE TABLE IF NOT EXISTS insights (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    ts        TEXT NOT NULL,
    kind      TEXT NOT NULL,
    severity  TEXT NOT NULL,
    entity    TEXT NOT NULL,
    metric    TEXT NOT NULL,
    value     REAL,
    baseline  REAL,
    message   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_insights_ts ON insights (ts DESC);
"""

_EVENT_SQL = "INSERT OR IGNORE INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"
_UPSERT_SQL = """
INSERT INTO {table} (window_start, region, event_type, events, revenue)
VALUES (?,?,?,?,?)
ON CONFLICT(window_start, region, event_type) DO UPDATE
SET events = events + excluded.events,
    revenue = revenue + excluded.revenue
"""

_RAW_AGG_COLS = {"region": "region", "event_type": "event_type", "device": "device", "hour": "hour"}
_SUMMARY_AGG_COLS = {"region": "region", "event_type": "event_type"}


class SqliteStore(StorageBackend):
    name = "sqlite.indexed"

    def __init__(self, path: str):
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        self.path = str(p)
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA cache_size=-131072")  # 128 MB page cache for index builds
        self._conn.execute("PRAGMA temp_store=MEMORY")
        self._conn.execute("PRAGMA mmap_size=268435456")

    # -- setup ------------------------------------------------------------
    def init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._conn.commit()

    def ping(self) -> bool:
        try:
            self._conn.execute("SELECT 1")
            return True
        except sqlite3.Error:
            return False

    # -- writes -----------------------------------------------------------
    def insert_events(self, rows) -> None:
        with self._lock:
            self._conn.executemany(_EVENT_SQL, rows)
            self._conn.commit()

    def insert_events_rowwise(self, rows, commit_every: int = 1000) -> None:
        """Naive write path kept for the benchmark baseline."""
        with self._lock:
            for i, row in enumerate(rows, 1):
                self._conn.execute(_EVENT_SQL, row)
                if i % commit_every == 0:
                    self._conn.commit()
            self._conn.commit()

    def upsert_window_aggregates(self, rows) -> None:
        self._upsert("aggregates_1m", rows)

    def upsert_hourly_summary(self, rows) -> None:
        self._upsert("summary_hourly", rows)

    def _upsert(self, table: str, rows) -> None:
        with self._lock:
            self._conn.executemany(_UPSERT_SQL.format(table=table), rows)
            self._conn.commit()

    def insert_insights(self, rows) -> None:
        sql = ("INSERT INTO insights (ts, kind, severity, entity, metric, value, baseline, message)"
               " VALUES (?,?,?,?,?,?,?,?)")
        with self._lock:
            self._conn.executemany(sql, rows)
            self._conn.commit()

    # -- queries ----------------------------------------------------------
    def query_user_events(self, user_id: str, limit: int = 50) -> list[dict]:
        cur = self._conn.execute(
            "SELECT * FROM events WHERE user_id = ? ORDER BY ts DESC LIMIT ?",
            (user_id, limit),
        )
        return [dict(r) for r in cur.fetchall()]

    def query_aggregate_raw(self, start_iso: str, end_iso: str, region=None,
                            event_type=None, group_by: str = "region") -> list[dict]:
        col = _RAW_AGG_COLS[group_by]
        sql = (f"SELECT {col} AS dim, COUNT(*) AS events, COALESCE(SUM(revenue),0) AS revenue "
               f"FROM events WHERE ts BETWEEN ? AND ?")
        params: list = [start_iso, end_iso]
        if region:
            sql += " AND region = ?"
            params.append(region)
        if event_type:
            sql += " AND event_type = ?"
            params.append(event_type)
        sql += f" GROUP BY {col}"
        return [dict(r) for r in self._conn.execute(sql, params).fetchall()]

    def query_hourly_summary(self, start_iso: str, end_iso: str, region=None,
                             event_type=None, group_by: str = "region") -> list[dict]:
        col = _SUMMARY_AGG_COLS[group_by]
        sql = (f"SELECT {col} AS dim, SUM(events) AS events, SUM(revenue) AS revenue "
               f"FROM summary_hourly WHERE window_start BETWEEN ? AND ?")
        params: list = [start_iso, end_iso]
        if region:
            sql += " AND region = ?"
            params.append(region)
        if event_type:
            sql += " AND event_type = ?"
            params.append(event_type)
        sql += f" GROUP BY {col}"
        return [dict(r) for r in self._conn.execute(sql, params).fetchall()]

    def query_rows(self, start_iso: str, end_iso: str) -> list[tuple]:
        cur = self._conn.execute("SELECT * FROM events WHERE ts BETWEEN ? AND ?", (start_iso, end_iso))
        return cur.fetchall()

    def sample_user(self) -> str | None:
        row = self._conn.execute("SELECT user_id FROM events LIMIT 1").fetchone()
        return row["user_id"] if row else None

    def latest_insights(self, limit: int = 20) -> list[dict]:
        cur = self._conn.execute("SELECT * FROM insights ORDER BY id DESC LIMIT ?", (limit,))
        return [dict(r) for r in cur.fetchall()]

    # -- maintenance --------------------------------------------------------
    def backfill_aggregates(self) -> None:
        with self._lock:
            self._conn.executescript("""
                DELETE FROM aggregates_1m;
                INSERT INTO aggregates_1m (window_start, region, event_type, events, revenue)
                SELECT substr(ts,1,16), region, event_type, COUNT(*), COALESCE(SUM(revenue),0)
                FROM events GROUP BY 1,2,3;
                DELETE FROM summary_hourly;
                INSERT INTO summary_hourly (window_start, region, event_type, events, revenue)
                SELECT substr(ts,1,13), region, event_type, COUNT(*), COALESCE(SUM(revenue),0)
                FROM events GROUP BY 1,2,3;
            """)
            self._conn.commit()

    def counts(self) -> dict:
        out = {}
        for table in ("events", "aggregates_1m", "summary_hourly", "insights"):
            row = self._conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()
            out[table] = row["c"]
        return out

    def close(self) -> None:
        self._conn.close()
