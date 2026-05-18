"""Typed data models used across the package.

Pydantic v2 (preferred) with a graceful dataclass fallback for environments
that don't have pydantic installed — the package works either way.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

# ── Pydantic detection — keep optional so the engine still loads without it ─
try:
    from pydantic import BaseModel, Field  # type: ignore
    _HAS_PYDANTIC = True
except ImportError:  # pragma: no cover
    _HAS_PYDANTIC = False
    BaseModel = object  # type: ignore[assignment,misc]
    def Field(*a, **kw):  # type: ignore[misc]
        return None


# ─── Enums ───────────────────────────────────────────────────────────────────
class DataType(str, Enum):
    REALTIME = "realtime"            # quotes, last price (cache: short TTL)
    HISTORICAL = "historical"        # OHLCV bars (cache: medium)
    FUNDAMENTAL = "fundamental"      # financials, ratios (cache: long)
    NEWS = "news"
    METADATA = "metadata"            # static company info


class FusionStrategy(str, Enum):
    PRIORITY_FIRST   = "priority_first"     # use highest-priority provider
    WEIGHTED_AVERAGE = "weighted_average"   # weight by health × trust
    MEDIAN           = "median"             # robust against outliers
    CONFIDENCE_BEST  = "confidence_best"    # highest individual confidence


class HealthStatus(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


# ─── Core dataclasses (no pydantic dependency) ───────────────────────────────
@dataclass
class ProviderResponse:
    """Single provider's response for a given (symbol, field) request."""
    provider: str
    symbol: str
    field: str
    value: Any
    confidence: float = 0.0     # 0..1 — how confident *this* provider is
    latency_ms: float = 0.0
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    error: str | None = None
    raw: Any = None             # original response, for debugging

    def to_dict(self) -> dict:
        return asdict(self)

    @property
    def ok(self) -> bool:
        return self.error is None and self.value is not None


@dataclass
class FusionResult:
    """Output of the fusion engine — what the public API returns."""
    value: Any
    confidence: float
    primary_provider: str | None
    contributors: list[str]              # ordered by descending contribution
    all_values: dict[str, Any]           # provider → value
    strategy: str
    warnings: list[str] = field(default_factory=list)
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class DiagnosticEntry:
    """Single (provider, request) telemetry record."""
    provider: str
    symbol: str
    field: str
    success: bool
    latency_ms: float
    error_type: str | None = None
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    extra: dict = field(default_factory=dict)


@dataclass
class HealthSnapshot:
    """Provider health metrics snapshot."""
    provider: str
    status: str = HealthStatus.UNKNOWN.value
    score: float = 0.5                  # 0..1
    success_rate: float = 0.0
    avg_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    error_count: int = 0
    rate_limit_count: int = 0
    total_calls: int = 0
    last_check: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


# ─── Pydantic API models (FastAPI bodies) ────────────────────────────────────
if _HAS_PYDANTIC:
    class FieldResponse(BaseModel):
        symbol: str
        field: str
        value: Any
        confidence: float
        primary_provider: str | None
        contributors: list[str]
        warnings: list[str] = []
        timestamp: str

    class PriceResponse(BaseModel):
        symbol: str
        price: float | None
        currency: str | None = None
        confidence: float
        primary_provider: str | None
        timestamp: str

    class HealthResponse(BaseModel):
        status: str
        providers: list[dict]
        cache: dict
        engine_uptime_s: float

    class TranslationResponse(BaseModel):
        original: str
        translated: str
        confidence: float
        mode: str
        cached: bool = False

else:  # pragma: no cover — fall back so api.py imports don't crash
    FieldResponse = dict  # type: ignore[misc,assignment]
    PriceResponse = dict  # type: ignore[misc,assignment]
    HealthResponse = dict  # type: ignore[misc,assignment]
    TranslationResponse = dict  # type: ignore[misc,assignment]


__all__ = [
    "DataType", "FusionStrategy", "HealthStatus",
    "ProviderResponse", "FusionResult", "DiagnosticEntry", "HealthSnapshot",
    "FieldResponse", "PriceResponse", "HealthResponse", "TranslationResponse",
]
