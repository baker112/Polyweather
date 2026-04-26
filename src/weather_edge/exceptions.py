class WeatherEdgeError(Exception):
    """Base exception."""


class IngestError(WeatherEdgeError):
    """Raised when all forecast sources fail."""


class AlreadyLockedError(WeatherEdgeError):
    """Raised when lock_picks is called for an already-locked (date, station) pair."""


class ConfigError(WeatherEdgeError):
    """Raised for invalid or missing configuration."""


class MarketError(WeatherEdgeError):
    """Raised for malformed or missing Polymarket data."""


class EmosError(WeatherEdgeError):
    """Raised when EMOS fitting fails or no params are available."""
