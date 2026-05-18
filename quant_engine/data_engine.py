"""DataEngine — the orchestrator.

Pipeline (per public call):

    fetch ─→ retry ─→ cache ─→ diagnostics ─→ health ─→ fusion ─→ result

Public methods always return either a dict (price/fundamentals) or a
`FusionResult` — they NEVER raise. Every error path is logged and surfaces
through the diagnostics + health systems instead.
"""
from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from .cache import SmartCache, get_default_cache
from .config import get_settings
from .diagnostics import DiagnosticsCollector
from .fusion import fuse
from .health import ProviderHealthMonitor
from .models import (
    DataType, FusionResult, FusionStrategy, ProviderResponse,
)
from .providers import PROVIDER_REGISTRY, BaseProvider
from .retry import BackoffConfig, with_backoff

logger = logging.getLogger("quant_engine.engine")


# Default field → data_type classification for cache TTL selection
FIELD_TYPE_MAP: dict[str, DataType] = {
    "price":          DataType.REALTIME,
    "open":           DataType.REALTIME,
    "high":           DataType.REALTIME,
    "low":            DataType.REALTIME,
    "close":          DataType.REALTIME,
    "volume":         DataType.REALTIME,
    "vwap":           DataType.REALTIME,
    "previous_close": DataType.REALTIME,
    "pe_ratio":       DataType.FUNDAMENTAL,
    "eps":            DataType.FUNDAMENTAL,
    "dividend_yield": DataType.FUNDAMENTAL,
    "market_cap":     DataType.FUNDAMENTAL,
    "name":           DataType.METADATA,
    "exchange":       DataType.METADATA,
    "sector":         DataType.METADATA,
    "industry":       DataType.METADATA,
    "currency":       DataType.METADATA,
    "ceo":            DataType.METADATA,
    "employees":      DataType.METADATA,
    "description":    DataType.METADATA,
}


class DataEngine:
    """Single entry-point for cooperative multi-provider data fetching."""

    def __init__(
        self,
        *,
        provider_chain: list[str] | None = None,
        cache: SmartCache | None = None,
        diagnostics: DiagnosticsCollector | None = None,
        health: ProviderHealthMonitor | None = None,
        api_keys: dict[str, str] | None = None,
        max_parallel: int = 8,
    ):
        s = get_settings()
        keys = api_keys or {
            "polygon":  s.polygon_key,
            "tiingo":   s.tiingo_key,
            "intrinio": s.intrinio_key,
            "finnhub":  s.finnhub_key,
            "yahoo":    "",  # keyless
        }

        self.provider_chain = provider_chain or list(s.provider_chain)
        self.cache = cache or get_default_cache()
        self.diagnostics = diagnostics or DiagnosticsCollector()
        self.health = health or ProviderHealthMonitor()
        self.max_parallel = max_parallel
        self._started_at = time.time()

        # Instantiate provider instances by name (lazy — they don't open network)
        self._providers: dict[str, BaseProvider] = {}
        for name in self.provider_chain:
            cls = PROVIDER_REGISTRY.get(name)
            if cls is None:
                logger.warning("Unknown provider in chain: %s", name)
                continue
            self._providers[name] = cls(api_key=keys.get(name, ""))

    # ─── PUBLIC API ─────────────────────────────────────────────────────────
    def get_field(
        self,
        symbol: str,
        field: str,
        *,
        data_type: DataType | None = None,
        strategy: FusionStrategy = FusionStrategy.PRIORITY_FIRST,
    ) -> FusionResult:
        """Fetch a single field across the provider chain and fuse the result."""
        symbol = (symbol or "").strip().upper()
        data_type = data_type or FIELD_TYPE_MAP.get(field, DataType.REALTIME)

        responses = self._collect_field(symbol, field, data_type)
        # Health-aware chain (so PRIORITY_FIRST favours healthy providers)
        ordered_chain = self.health.reorder_chain(self.provider_chain)
        health_scores = {p: self.health.score(p) for p in self.provider_chain}

        return fuse(
            responses,
            strategy=strategy,
            chain=ordered_chain,
            health_scores=health_scores,
        )

    def get_price(self, symbol: str, *, strategy: FusionStrategy = FusionStrategy.WEIGHTED_AVERAGE) -> dict:
        """Convenience: fused last price + currency in one shot."""
        price = self.get_field(symbol, "price", strategy=strategy)
        currency = self.get_field(symbol, "currency",
                                    data_type=DataType.METADATA,
                                    strategy=FusionStrategy.PRIORITY_FIRST)
        return {
            "symbol": symbol.upper(),
            "price": price.value,
            "currency": currency.value,
            "confidence": price.confidence,
            "primary_provider": price.primary_provider,
            "contributors": price.contributors,
            "warnings": price.warnings,
            "timestamp": price.timestamp,
        }

    def get_fundamentals(self, symbol: str) -> dict[str, FusionResult]:
        """Fetch a canonical fundamentals bundle in parallel."""
        fields = ["pe_ratio", "eps", "dividend_yield", "market_cap"]
        return self._fetch_many_fields(symbol, fields,
                                         strategy=FusionStrategy.PRIORITY_FIRST)

    def get_metadata(self, symbol: str) -> dict[str, FusionResult]:
        fields = ["name", "exchange", "sector", "industry", "currency"]
        return self._fetch_many_fields(symbol, fields,
                                         strategy=FusionStrategy.PRIORITY_FIRST)

    def get_diagnostics(self) -> dict:
        summary = self.diagnostics.summary()
        return {
            **summary.__dict__,
            "uptime_s": round(self.diagnostics.uptime_s, 2),
        }

    def get_provider_health(self) -> list[dict]:
        return [s.__dict__ for s in self.health.all_snapshots()]

    def get_cache_stats(self) -> dict:
        return self.cache.stats().to_dict()

    def clear_cache(self, symbol: str | None = None) -> dict:
        if symbol:
            removed = self.cache.invalidate_symbol(symbol)
            return {"cleared_for_symbol": symbol.upper(), "removed": removed}
        self.cache.clear()
        return {"cleared_all": True}

    # ─── INTERNAL ───────────────────────────────────────────────────────────
    def _collect_field(
        self,
        symbol: str,
        field: str,
        data_type: DataType,
    ) -> list[ProviderResponse]:
        """Run every supported provider in parallel and return all responses."""
        tasks: dict[str, BaseProvider] = {}
        for name, prov in self._providers.items():
            if prov.supports(field):
                tasks[name] = prov

        if not tasks:
            return []

        responses: list[ProviderResponse] = []
        max_workers = min(len(tasks), self.max_parallel)

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {
                ex.submit(self._fetch_one, prov, symbol, field, data_type): name
                for name, prov in tasks.items()
            }
            for fut in as_completed(futures):
                try:
                    responses.append(fut.result(timeout=get_settings().request_timeout * 2))
                except Exception as exc:
                    logger.warning("Provider task crashed: %s", exc)
        return responses

    def _fetch_one(
        self,
        provider: BaseProvider,
        symbol: str,
        field: str,
        data_type: DataType,
    ) -> ProviderResponse:
        """Cache-aware single fetch with retry + diagnostics + health update."""
        cache_key = SmartCache.make_key(provider.NAME, symbol, field, data_type)
        cached = self.cache.get(cache_key)
        if cached is not None:
            # Cached responses contribute to fusion at full confidence
            cached.latency_ms = 0.0
            return cached

        # Retry transient failures
        try:
            response = with_backoff(
                provider.fetch, symbol, field, data_type,
                config=BackoffConfig(),
            )
        except Exception as exc:
            response = ProviderResponse(
                provider=provider.NAME, symbol=symbol, field=field,
                value=None, confidence=0.0, latency_ms=0.0,
                error=type(exc).__name__,
            )

        # Diagnostics
        self.diagnostics.record(
            provider=provider.NAME,
            symbol=symbol, field=field,
            success=response.ok,
            latency_ms=response.latency_ms,
            error_type=response.error,
        )
        # Health update
        self.health.record(
            provider.NAME,
            success=response.ok,
            latency_ms=response.latency_ms,
            error_type=response.error,
        )
        # Cache successful responses
        if response.ok:
            self.cache.set_typed(cache_key, response, data_type)
        return response

    def _fetch_many_fields(
        self,
        symbol: str,
        fields: list[str],
        *,
        strategy: FusionStrategy,
    ) -> dict[str, FusionResult]:
        """Parallel fan-out across many fields for the same symbol."""
        results: dict[str, FusionResult] = {}
        with ThreadPoolExecutor(max_workers=min(len(fields), self.max_parallel)) as ex:
            future_map = {
                ex.submit(self.get_field, symbol, f, strategy=strategy): f
                for f in fields
            }
            for fut in as_completed(future_map):
                f = future_map[fut]
                try:
                    results[f] = fut.result()
                except Exception as exc:
                    logger.warning("get_field(%s) failed: %s", f, exc)
        return results

    @property
    def uptime_s(self) -> float:
        return time.time() - self._started_at


__all__ = ["DataEngine", "FIELD_TYPE_MAP"]
