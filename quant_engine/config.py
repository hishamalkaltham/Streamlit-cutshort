"""Centralized configuration — env-driven, immutable.

Loaded ONCE at module import. Use `get_settings()` everywhere instead of
re-reading os.environ to keep the engine reproducible.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except ImportError:
    pass


def _csv(raw: str | None, default: list[str]) -> list[str]:
    if not raw:
        return list(default)
    return [x.strip() for x in raw.split(",") if x.strip()]


def _bool(raw: str | None, default: bool) -> bool:
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def _float(raw: str | None, default: float) -> float:
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _int(raw: str | None, default: int) -> int:
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    """Immutable engine configuration."""

    # ── API keys ──
    polygon_key:  str = field(default_factory=lambda: os.getenv("POLYGON_API_KEY", ""))
    tiingo_key:   str = field(default_factory=lambda: os.getenv("TIINGO_API_KEY", ""))
    intrinio_key: str = field(default_factory=lambda: os.getenv("INTRINIO_API_KEY", ""))
    finnhub_key:  str = field(default_factory=lambda: os.getenv("FINNHUB_API_KEY", ""))

    # ── Provider priority (highest → lowest) ──
    provider_chain: list[str] = field(
        default_factory=lambda: _csv(
            os.getenv("PROVIDER_CHAIN"),
            ["polygon", "tiingo", "intrinio", "finnhub", "yahoo"],
        )
    )

    # ── Cache TTLs (seconds) ──
    ttl_realtime:    int = field(default_factory=lambda: _int(os.getenv("TTL_REALTIME"), 30))
    ttl_historical:  int = field(default_factory=lambda: _int(os.getenv("TTL_HISTORICAL"), 1800))
    ttl_fundamental: int = field(default_factory=lambda: _int(os.getenv("TTL_FUNDAMENTAL"), 21600))
    ttl_news:        int = field(default_factory=lambda: _int(os.getenv("TTL_NEWS"), 600))
    ttl_metadata:    int = field(default_factory=lambda: _int(os.getenv("TTL_METADATA"), 86400))
    cache_max_size:  int = field(default_factory=lambda: _int(os.getenv("CACHE_MAX_SIZE"), 5000))

    # ── HTTP ──
    request_timeout: float = field(default_factory=lambda: _float(os.getenv("REQUEST_TIMEOUT"), 8.0))
    max_retries:     int   = field(default_factory=lambda: _int(os.getenv("MAX_RETRIES"), 3))
    backoff_base:    float = field(default_factory=lambda: _float(os.getenv("BACKOFF_BASE"), 0.5))
    backoff_max:     float = field(default_factory=lambda: _float(os.getenv("BACKOFF_MAX"), 8.0))
    backoff_jitter:  float = field(default_factory=lambda: _float(os.getenv("BACKOFF_JITTER"), 0.3))

    # ── Health monitoring ──
    health_window_size:    int   = field(default_factory=lambda: _int(os.getenv("HEALTH_WINDOW_SIZE"), 50))
    health_degraded_below: float = field(default_factory=lambda: _float(os.getenv("HEALTH_DEGRADED"), 0.6))
    health_unhealthy_below:float = field(default_factory=lambda: _float(os.getenv("HEALTH_UNHEALTHY"), 0.3))
    health_recovery_factor:float = field(default_factory=lambda: _float(os.getenv("HEALTH_RECOVERY"), 0.05))

    # ── Translation ──
    translation_provider: str = field(default_factory=lambda: os.getenv("TRANSLATION_PROVIDER", "auto"))
    anthropic_key:        str = field(default_factory=lambda: os.getenv("ANTHROPIC_API_KEY", ""))
    openai_key:           str = field(default_factory=lambda: os.getenv("OPENAI_API_KEY", ""))
    translation_cache_max:int = field(default_factory=lambda: _int(os.getenv("TRANSLATION_CACHE_MAX"), 2000))

    # ── Logging ──
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    debug:     bool = field(default_factory=lambda: _bool(os.getenv("DEBUG"), False))


_SETTINGS: Settings | None = None


def get_settings() -> Settings:
    """Process-wide settings singleton."""
    global _SETTINGS
    if _SETTINGS is None:
        _SETTINGS = Settings()
        _setup_logging(_SETTINGS)
    return _SETTINGS


def _setup_logging(s: Settings) -> None:
    level = getattr(logging, str(s.log_level).upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )
    if not s.debug:
        # quiet noisy libraries
        for name in ("urllib3", "httpx", "requests", "asyncio"):
            logging.getLogger(name).setLevel(logging.WARNING)


def reset_settings() -> None:
    """Mainly for tests — re-reads the environment."""
    global _SETTINGS
    _SETTINGS = None


__all__ = ["Settings", "get_settings", "reset_settings"]
