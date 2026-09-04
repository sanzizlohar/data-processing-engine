-- Data Processing Engine — PostgreSQL schema
-- Production note: for very large deployments partition `events` by RANGE (ts)
-- with daily partitions; indexes below are local-friendly either way.

CREATE TABLE IF NOT EXISTS events (
    event_id    TEXT PRIMARY KEY,
    ts          TIMESTAMPTZ NOT NULL,
    user_id     TEXT NOT NULL,
    session_id  TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    region      TEXT NOT NULL,
    device      TEXT NOT NULL,
    amount      NUMERIC(12,2),
    revenue     NUMERIC(14,4),
    hour        SMALLINT,
    day_of_week SMALLINT,
    amount_band TEXT
);

-- Point queries: composite index on the hot lookup path
CREATE INDEX IF NOT EXISTS idx_events_user_ts   ON events (user_id, ts DESC);
-- Window aggregates: time-first access
CREATE INDEX IF NOT EXISTS idx_events_ts        ON events (ts DESC);
CREATE INDEX IF NOT EXISTS idx_events_region_ts ON events (region, ts DESC);
CREATE INDEX IF NOT EXISTS idx_events_type_ts   ON events (event_type, ts DESC);
-- Cheap pruning for large time-range scans
CREATE INDEX IF NOT EXISTS idx_events_brin      ON events USING brin (ts);

CREATE TABLE IF NOT EXISTS aggregates_1m (
    window_start TIMESTAMPTZ NOT NULL,
    region       TEXT NOT NULL,
    event_type   TEXT NOT NULL,
    events       BIGINT NOT NULL DEFAULT 0,
    revenue      NUMERIC(16,4) NOT NULL DEFAULT 0,
    PRIMARY KEY (window_start, region, event_type)
);

CREATE TABLE IF NOT EXISTS summary_hourly (
    window_start TIMESTAMPTZ NOT NULL,
    region       TEXT NOT NULL,
    event_type   TEXT NOT NULL,
    events       BIGINT NOT NULL DEFAULT 0,
    revenue      NUMERIC(16,4) NOT NULL DEFAULT 0,
    PRIMARY KEY (window_start, region, event_type)
);

CREATE TABLE IF NOT EXISTS insights (
    id        BIGSERIAL PRIMARY KEY,
    ts        TIMESTAMPTZ NOT NULL,
    kind      TEXT NOT NULL,
    severity  TEXT NOT NULL,
    entity    TEXT NOT NULL,
    metric    TEXT NOT NULL,
    value     DOUBLE PRECISION,
    baseline  DOUBLE PRECISION,
    message   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_insights_ts ON insights (ts DESC);

-- Materialized view served by the query router for heavy historical aggregates.
-- Refresh periodically (e.g. from the Spark batch job):
--   REFRESH MATERIALIZED VIEW CONCURRENTLY mv_region_daily;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_region_daily AS
SELECT date_trunc('day', ts) AS day, region, event_type,
       COUNT(*) AS events, SUM(revenue) AS revenue
FROM events
GROUP BY 1, 2, 3
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_region_daily ON mv_region_daily (day, region, event_type);
