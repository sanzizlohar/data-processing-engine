"""PostgreSQL backend (primary production store).

Connection is created lazily on import of this module's class (psycopg2 is an
optional dependency) so demo mode never requires it. Bulk loads use
execute_values; aggregate tables use ON CONFLICT upserts.
"""
from __future__ import annotations

import logging
from pathlib import Path

from src.storage.base import StorageBackend

log = logging.getLogger("storage.postgres")

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "db" / "schema.sql"

_EVENT_SQL = ("INSERT INTO events (event_id, ts, user_id, session_id, event_type, region, device,"
              " amount, revenue, hour, day_of_week, amount_band) VALUES %s"
              " ON CONFLICT (event_id) DO NOTHING")
_UPSERT_SQL = """
INSERT INTO {table} (window_start, region, event_type, events, revenue) VALUES %s
ON CONFLICT (window_start, region, event_type) DO UPDATE
SET events = {table}.events + EXCLUDED.events,
    revenue = {table}.revenue + EXCLUDED.revenue
"""
_INSIGHT_SQL = ("INSERT INTO insights (ts, kind, severity, entity, metric, value, baseline, message)"
                " VALUES %s")

_RAW_AGG_COLS = {"region": "region", "event_type": "event_type", "device": "device", "hour": "hour"}
_SUMMARY_AGG_COLS = {"region": "region", "event_type": "event_type"}


class PostgresStore(StorageBackend):
    name = "postgres.indexed"

    def __init__(self, dsn: str):
        import psycopg2
        from psycopg2.extras import RealDictCursor, execute_values

        self._RealDictCursor = RealDictCursor
        self._execute_values = execute_values
        self._conn = psycopg2.connect(dsn, connect_timeout=3)

    # -- setup ------------------------------------------------------------
    def init_schema(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute(SCHEMA_PATH.read_text(encoding="utf-8"))
        self._conn.commit()

    def ping(self) -> bool:
        try:
            with self._conn.cursor() as cur:
                cur.execute("SELECT 1")
            return True
        except Exception:
            return False

    # -- writes -----------------------------------------------------------
    def insert_events(self, rows) -> None:
        with self._conn.cursor() as cur:
            self._execute_values(cur, _EVENT_SQL, rows, page_size=1000)
        self._conn.commit()

    def insert_events_rowwise(self, rows, commit_every: int = 1000) -> None:
        single = _EVENT_SQL.replace("%s", "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
        with self._conn.cursor() as cur:
            for i, row in enumerate(rows, 1):
                cur.execute(single, row)
                if i % commit_every == 0:
                    self._conn.commit()
        self._conn.commit()

    def upsert_window_aggregates(self, rows) -> None:
        self._upsert("aggregates_1m", rows)

    def upsert_hourly_summary(self, rows) -> None:
        self._upsert("summary_hourly", rows)

    def _upsert(self, table: str, rows) -> None:
        with self._conn.cursor() as cur:
            self._execute_values(cur, _UPSERT_SQL.format(table=table), rows, page_size=1000)
        self._conn.commit()

    def insert_insights(self, rows) -> None:
        with self._conn.cursor() as cur:
            self._execute_values(cur, _INSIGHT_SQL, rows, page_size=500)
        self._conn.commit()

    # -- queries ----------------------------------------------------------
    def _fetch(self, cur) -> list[dict]:
        return [dict(r) for r in cur.fetchall()]

    def query_user_events(self, user_id: str, limit: int = 50) -> list[dict]:
        with self._conn.cursor(cursor_factory=self._RealDictCursor) as cur:
            cur.execute("SELECT * FROM events WHERE user_id = %s ORDER BY ts DESC LIMIT %s",
                        (user_id, limit))
            return self._fetch(cur)

    def query_aggregate_raw(self, start_iso: str, end_iso: str, region=None,
                            event_type=None, group_by: str = "region") -> list[dict]:
        col = _RAW_AGG_COLS[group_by]
        sql = (f"SELECT {col} AS dim, COUNT(*) AS events, COALESCE(SUM(revenue),0) AS revenue"
               f" FROM events WHERE ts BETWEEN %s AND %s")
        params: list = [start_iso, end_iso]
        if region:
            sql += " AND region = %s"
            params.append(region)
        if event_type:
            sql += " AND event_type = %s"
            params.append(event_type)
        sql += f" GROUP BY {col}"
        with self._conn.cursor(cursor_factory=self._RealDictCursor) as cur:
            cur.execute(sql, params)
            return self._fetch(cur)

    def query_hourly_summary(self, start_iso: str, end_iso: str, region=None,
                             event_type=None, group_by: str = "region") -> list[dict]:
        col = _SUMMARY_AGG_COLS[group_by]
        sql = (f"SELECT {col} AS dim, SUM(events) AS events, SUM(revenue) AS revenue"
               f" FROM summary_hourly WHERE window_start BETWEEN %s AND %s")
        params: list = [start_iso, end_iso]
        if region:
            sql += " AND region = %s"
            params.append(region)
        if event_type:
            sql += " AND event_type = %s"
            params.append(event_type)
        sql += f" GROUP BY {col}"
        with self._conn.cursor(cursor_factory=self._RealDictCursor) as cur:
            cur.execute(sql, params)
            return self._fetch(cur)

    def query_rows(self, start_iso: str, end_iso: str) -> list[tuple]:
        with self._conn.cursor() as cur:
            cur.execute("SELECT event_id, ts, user_id, session_id, event_type, region, device,"
                        " amount, revenue, hour, day_of_week, amount_band"
                        " FROM events WHERE ts BETWEEN %s AND %s", (start_iso, end_iso))
            return cur.fetchall()

    def sample_user(self) -> str | None:
        with self._conn.cursor() as cur:
            cur.execute("SELECT user_id FROM events LIMIT 1")
            row = cur.fetchone()
        return row[0] if row else None

    def latest_insights(self, limit: int = 20) -> list[dict]:
        with self._conn.cursor(cursor_factory=self._RealDictCursor) as cur:
            cur.execute("SELECT * FROM insights ORDER BY id DESC LIMIT %s", (limit,))
            return self._fetch(cur)

    # -- maintenance --------------------------------------------------------
    def backfill_aggregates(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute("DELETE FROM aggregates_1m")
            cur.execute("""
                INSERT INTO aggregates_1m (window_start, region, event_type, events, revenue)
                SELECT date_trunc('minute', ts), region, event_type, COUNT(*), COALESCE(SUM(revenue),0)
                FROM events GROUP BY 1,2,3
            """)
            cur.execute("DELETE FROM summary_hourly")
            cur.execute("""
                INSERT INTO summary_hourly (window_start, region, event_type, events, revenue)
                SELECT date_trunc('hour', ts), region, event_type, COUNT(*), COALESCE(SUM(revenue),0)
                FROM events GROUP BY 1,2,3
            """)
        self._conn.commit()

    def counts(self) -> dict:
        out = {}
        with self._conn.cursor() as cur:
            for table in ("events", "aggregates_1m", "summary_hourly", "insights"):
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                out[table] = cur.fetchone()[0]
        return out

    def close(self) -> None:
        self._conn.close()
