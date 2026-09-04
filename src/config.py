"""Configuration loading: defaults <- config/config.yaml <- environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "config.yaml"


@dataclass
class StorageConfig:
    backend: str = "auto"  # auto | postgres | sqlite
    sqlite_path: str = "data/dpe.db"
    pg_dsn: str = "postgresql://dpe:dpe@localhost:5432/dpe"


@dataclass
class GeneratorConfig:
    events_per_second: int = 500
    burst_every_seconds: int = 15
    burst_duration_seconds: int = 5
    burst_multiplier: int = 4
    time_scale: float = 1.0  # >1 compresses event-time (demos); 1.0 = real time


@dataclass
class EngineConfig:
    micro_batch_ms: int = 200
    max_batch_size: int = 5000
    flush_interval_s: float = 1.0
    insights_every_n_batches: int = 5
    parallel_workers: int = 0  # 0 = auto (cpu_count - 1)
    chunk_size: int = 5000
    parallel_min_batch: int = 5000


@dataclass
class InsightsConfig:
    zscore_threshold: float = 3.0
    critical_zscore: float = 5.0
    baseline_windows: int = 20
    cooldown_seconds: int = 30
    trend_min_points: int = 8
    trend_sensitivity: float = 0.15
    refund_rate_threshold: float = 0.05
    min_purchases_for_rate: int = 20


@dataclass
class RoutingConfig:
    hot_window_minutes: int = 60
    cache_ttl_seconds: int = 15
    cache_max_items: int = 256
    spark_enabled: bool = False


@dataclass
class APIConfig:
    host: str = "127.0.0.1"
    port: int = 8000


@dataclass
class Config:
    storage: StorageConfig = field(default_factory=StorageConfig)
    generator: GeneratorConfig = field(default_factory=GeneratorConfig)
    engine: EngineConfig = field(default_factory=EngineConfig)
    insights: InsightsConfig = field(default_factory=InsightsConfig)
    routing: RoutingConfig = field(default_factory=RoutingConfig)
    api: APIConfig = field(default_factory=APIConfig)

    @classmethod
    def load(cls, path: str | os.PathLike | None = None) -> "Config":
        cfg = cls()
        p = Path(path) if path else DEFAULT_CONFIG_PATH
        data: dict[str, Any] = {}
        if p.exists():
            with open(p, "r", encoding="utf-8") as fh:
                data = yaml.safe_load(fh) or {}
        for section_name in ("storage", "generator", "engine", "insights", "routing", "api"):
            section = getattr(cfg, section_name)
            for f in fields(section):
                section_data = data.get(section_name) or {}
                if f.name in section_data:
                    setattr(section, f.name, section_data[f.name])
        _apply_env(cfg)
        return cfg


def _apply_env(cfg: Config) -> None:
    env_map = {
        "DPE_STORAGE_BACKEND": ("storage", "backend", str),
        "DPE_SQLITE_PATH": ("storage", "sqlite_path", str),
        "DPE_PG_DSN": ("storage", "pg_dsn", str),
        "DPE_API_PORT": ("api", "port", int),
        "DPE_EPS": ("generator", "events_per_second", int),
    }
    for var, (section, name, cast) in env_map.items():
        raw = os.environ.get(var)
        if raw:
            setattr(getattr(cfg, section), name, cast(raw))
