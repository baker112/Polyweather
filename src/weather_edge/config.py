from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

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
    # Per-station Kelly multiplier (#6). Conservative default of 0.5 lets young
    # stations accumulate evidence before sizing up. Auto-promoted to 1.0 by
    # edge_gate.effective_kelly_multiplier once a station has PROMOTE_MIN_BETS+
    # resolved bets with positive mean CLV; override here to clamp a station
    # that's earned promotion back down for risk reasons.
    kelly_multiplier: float = 0.5
    # Optional per-station liquidity floor that overrides ThresholdsConfig.min_liquidity.
    # Asian markets (RKSI/ZSPD/RCSS) are persistently thin, so a global floor of $100
    # rejects most of their otherwise-positive-edge opportunities.
    min_liquidity: float | None = None
    # Forecast aggregation mode:
    #   "bma"      — Bayesian model averaging across ECMWF / GEFS / ICON / WN2 (default).
    #   "wn2_only" — Use only WeatherNext 2's 64-member ensemble; ignore other models.
    #                μ/σ come from the WN2 members directly (with WN2-specific EMOS
    #                applied if cached params exist). Useful for benchmarking the ML
    #                model in isolation against the BMA blend.
    bma_mode: Literal["bma", "wn2_only"] = "bma"
    # Optional intraday lock — fire a SECOND lock during the day targeting the SAME
    # day (not D+1) using a short-lead WN2 forecast. Format "HH:MM" UTC. When set,
    # the scheduler registers an extra job at this time that:
    #   - picks the most recent published WN2 init (with min 4h publication lag),
    #   - forces bma_mode = "wn2_only" for the run,
    #   - targets the current UTC date (so the bet is on today's daily max).
    # Recommend ~1-2h before local-afternoon peak so the 6h-12h lead covers it.
    intraday_lock_time_utc: str | None = None


class ThresholdsConfig(BaseModel):
    min_edge: float
    max_spread: float
    min_liquidity: float
    max_raw_prob: float
    market_freshness_minutes: int
    max_kelly_fraction: float = 0.25
    kelly_multiplier: float = 1.0
    # Liquidity / spread filter (#4): skip or downsize when book depth is thin.
    min_top_size_usdc: float = 0.0  # skip bracket if depth at top of relevant side < this many USDC
    depth_safety_factor: float = 0.5  # cap stake at top_size * price * factor (only consume part of TOB)
    min_net_edge: float = 0.0  # require abs(edge) - spread >= this; default 0 = inactive


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
