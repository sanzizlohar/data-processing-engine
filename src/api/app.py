"""FastAPI service — live engine stats, insights, routed queries, and a
real-time HTML dashboard at "/" for visual demos.

Runs the streaming engine and the API in one process/one event loop, so
/stats and /analytics/summary reflect live in-memory state.

Usage:
  python -m src.api.app --eps 800 --seconds 600 --port 8000
  # then open http://127.0.0.1:8000/ for the live dashboard
"""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from src.config import Config
from src.insights.engine import InsightsEngine
from src.routing.router import QueryRouter, QuerySpec
from src.storage.base import create_store
from src.stream.engine import StreamingEngine
from src.stream.memory import MemoryAggregates
from src.utils.logging_setup import configure
from src.utils.metrics import Metrics

log = logging.getLogger("api")

DASHBOARD_HTML = (Path(__file__).resolve().parent / "dashboard.html").read_text(encoding="utf-8")


class QueryBody(BaseModel):
    kind: str = "recent_agg"      # point | recent_agg | heavy_agg
    user_id: str | None = None
    minutes: int = 60
    start_iso: str | None = None
    end_iso: str | None = None
    region: str | None = None
    event_type: str | None = None
    group_by: str = "region"


def build_app(engine: StreamingEngine, router: QueryRouter, store) -> FastAPI:
    app = FastAPI(title="Data Processing Engine", version="1.0.0",
                  docs_url="/docs", redirect_slashes=True)

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    async def dashboard():
        return DASHBOARD_HTML

    @app.get("/health")
    async def health():
        return {"status": "ok", "storage": store.name, "engine_running": engine.running}

    @app.get("/stats")
    async def stats():
        return engine.stats()

    @app.get("/sample-user")
    async def sample_user():
        user = store.sample_user()
        if not user:
            raise HTTPException(status_code=404, detail="no events stored yet")
        return {"user_id": user}

    @app.get("/storage/counts")
    async def storage_counts():
        return store.counts()

    @app.get("/insights")
    async def insights(limit: int = 20):
        return {"insights": store.latest_insights(min(limit, 100))}

    @app.get("/analytics/summary")
    async def summary(minutes: int = 60, region: str | None = None,
                      event_type: str | None = None):
        res = router.route(QuerySpec(kind="recent_agg", minutes=minutes,
                                     region=region, event_type=event_type))
        return {"backend": res.backend, "elapsed_ms": res.elapsed_ms, "result": res.rows}

    @app.post("/route")
    async def route(body: QueryBody):
        try:
            res = router.route(QuerySpec(**body.model_dump()))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {"backend": res.backend, "elapsed_ms": res.elapsed_ms,
                "cache_hit": res.cache_hit, "rationale": res.rationale,
                "result": res.rows}

    return app


async def main() -> None:
    import argparse

    import uvicorn

    parser = argparse.ArgumentParser(description="Streaming engine + REST API")
    parser.add_argument("--eps", type=int, default=None)
    parser.add_argument("--seconds", type=int, default=600)
    parser.add_argument("--port", type=int, default=None)
    args = parser.parse_args()

    configure("INFO")
    cfg = Config.load()
    if args.eps:
        cfg.generator.events_per_second = args.eps
    if args.port:
        cfg.api.port = args.port

    store = create_store(cfg)
    memory = MemoryAggregates(hot_minutes=cfg.routing.hot_window_minutes)
    metrics = Metrics()
    insights = InsightsEngine(cfg.insights)
    router = QueryRouter(store, memory, cfg.routing, metrics)
    engine = StreamingEngine(cfg, store, memory, insights, metrics)
    app = build_app(engine, router, store)

    uvi = uvicorn.Server(uvicorn.Config(app, host=cfg.api.host, port=cfg.api.port,
                                        log_level="warning"))
    engine_task = asyncio.create_task(engine.run(duration=args.seconds))
    serve_task = asyncio.create_task(uvi.serve())
    log.info("API on http://%s:%d (engine runs for %ss)", cfg.api.host, cfg.api.port, args.seconds)
    await engine_task
    uvi.should_exit = True
    await serve_task

    snap = engine.stats()
    log.info("engine finished: %s events processed, %d insights generated",
             snap["counters"].get("events_processed", 0), snap["insights_generated"])
    store.close()


if __name__ == "__main__":
    asyncio.run(main())
