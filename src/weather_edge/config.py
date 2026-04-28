from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel

_CONFIG_DIR = Path(__file__).parent.parent.parent / "config"


class StationConfig(BaseModel):
    icao: str
    name: str
    lat: float
    lon: float
    timezone: str
    unit: str
    market_slug_pattern: str
    resolution_field: str
    lock_time_utc: str


class ThresholdsConfig(BaseModel):
    min_edge: float
    max_spread: float
    min_liquidity: float
    max_raw_prob: float
    market_freshness_minutes: int
    max_kelly_fraction: float = 0.25
    kelly_multiplier: float = 1.0


@lru_cache(maxsize=1)
def load_stations() -> dict[str, StationConfig]:
    path = _CONFIG_DIR / "stations.yaml"
    with open(path) as f:
        raw: dict[str, Any] = yaml.safe_load(f)
    return {icao: StationConfig(icao=icao, **data) for icao, data in raw.items()}


@lru_cache(maxsize=1)
def load_thresholds() -> ThresholdsConfig:
    path = _CONFIG_DIR / "thresholds.yaml"
    with open(path) as f:
        raw: dict[str, Any] = yaml.safe_load(f)
    return ThresholdsConfig(**raw)


def get_station(icao: str) -> StationConfig:
    stations = load_stations()
    if icao not in stations:
        from weather_edge.exceptions import ConfigError
        raise ConfigError(f"Unknown station: {icao!r}. Known: {list(stations)}")
    return stations[icao]
