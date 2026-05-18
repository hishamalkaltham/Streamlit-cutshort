"""Abstract base for every concrete provider.

Concrete providers implement `_fetch_field()`. Everything else (timing,
diagnostics record, retry, error classification, normalization) is handled
by the base class so providers stay tiny and consistent.
"""
from __future__ import annotations

import abc
import logging
import time
from typing import Any, ClassVar

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ..config import get_settings
from ..errors import (
    ProviderBadResponse, ProviderError, ProviderRateLimited,
    ProviderTimeout, ProviderUnauthorized,
)
from ..models import DataType, ProviderResponse

logger = logging.getLogger("quant_engine.providers")

# Map of name → class — populated via @register
PROVIDER_REGISTRY: dict[str, type["BaseProvider"]] = {}


def register(name: str):
    """Class decorator that registers a provider in PROVIDER_REGISTRY."""
    def deco(cls: type["BaseProvider"]):
        PROVIDER_REGISTRY[name] = cls
        cls.NAME = name
        return cls
    return deco


# ─── shared HTTP session (pooled connections + tiny urllib3 retry) ──────────
_SESSION: requests.Session | None = None


def _shared_session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        s = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=20, pool_maxsize=40,
            max_retries=Retry(total=0, connect=0, read=0,
                                backoff_factor=0.0,
                                status_forcelist=[]),
        )
        s.mount("http://", adapter)
        s.mount("https://", adapter)
        s.headers.update({"User-Agent": "QuantEngine/1.0"})
        _SESSION = s
    return _SESSION


class BaseProvider(abc.ABC):
    """Common interface + plumbing for every data provider.

    Subclasses MUST:
      • set NAME (or use @register)
      • implement `_fetch_field(symbol, field, data_type)` returning a value
      • optionally override `default_confidence(field)` for finer scoring
    """

    NAME: ClassVar[str] = "base"
    BASE_URL: ClassVar[str] = ""
    SUPPORTS_FIELDS: ClassVar[set[str]] = set()
    REQUIRES_KEY: ClassVar[bool] = True

    def __init__(self, api_key: str | None = None, *, timeout: float | None = None):
        s = get_settings()
        self.api_key = api_key or ""
        self.timeout = timeout or s.request_timeout
        self.session = _shared_session()

    # ─── public API used by DataEngine ─────────────────────────────────────
    def fetch(
        self,
        symbol: str,
        field: str,
        data_type: DataType = DataType.REALTIME,
    ) -> ProviderResponse:
        """Single, instrumented fetch. Never raises — failures land in
        `ProviderResponse.error`."""
        symbol = (symbol or "").strip().upper()
        started = time.perf_counter()

        if self.REQUIRES_KEY and not self.api_key:
            return ProviderResponse(
                provider=self.NAME, symbol=symbol, field=field,
                value=None, confidence=0.0,
                latency_ms=0.0, error="no_api_key",
            )

        try:
            value = self._fetch_field(symbol, field, data_type)
            elapsed = (time.perf_counter() - started) * 1000.0
            confidence = self.default_confidence(field) if value is not None else 0.0
            return ProviderResponse(
                provider=self.NAME, symbol=symbol, field=field,
                value=value,
                confidence=confidence,
                latency_ms=round(elapsed, 2),
                raw=None,
            )
        except ProviderError as exc:
            elapsed = (time.perf_counter() - started) * 1000.0
            return ProviderResponse(
                provider=self.NAME, symbol=symbol, field=field,
                value=None, confidence=0.0,
                latency_ms=round(elapsed, 2),
                error=type(exc).__name__,
            )
        except Exception as exc:
            elapsed = (time.perf_counter() - started) * 1000.0
            logger.exception("Unexpected error in %s.fetch(%s, %s)", self.NAME, symbol, field)
            return ProviderResponse(
                provider=self.NAME, symbol=symbol, field=field,
                value=None, confidence=0.0,
                latency_ms=round(elapsed, 2),
                error=type(exc).__name__,
            )

    @abc.abstractmethod
    def _fetch_field(self, symbol: str, field: str, data_type: DataType) -> Any:
        """Return the raw value (or None). Raise ProviderError subclasses to signal a known failure mode."""

    # ─── helpers shared by all subclasses ──────────────────────────────────
    def _http_get(self, url: str, params: dict | None = None,
                  headers: dict | None = None) -> dict:
        """Convenience HTTP GET with consistent error mapping."""
        try:
            r = self.session.get(url, params=params, headers=headers,
                                  timeout=self.timeout)
        except requests.exceptions.Timeout as exc:
            raise ProviderTimeout(self.NAME, str(exc)) from exc
        except requests.exceptions.ConnectionError as exc:
            raise ProviderError(self.NAME, str(exc)) from exc

        if r.status_code == 429:
            raise ProviderRateLimited(self.NAME, "429 Too Many Requests", status=429)
        if r.status_code in (401, 402, 403):
            raise ProviderUnauthorized(self.NAME, f"HTTP {r.status_code}", status=r.status_code)
        if not r.ok:
            raise ProviderError(self.NAME, f"HTTP {r.status_code}", status=r.status_code)

        try:
            return r.json()
        except ValueError as exc:
            raise ProviderBadResponse(self.NAME, "non-JSON body") from exc

    def default_confidence(self, field: str) -> float:
        """Per-field confidence prior — concrete providers may override."""
        return 0.75

    @classmethod
    def supports(cls, field: str) -> bool:
        return not cls.SUPPORTS_FIELDS or field in cls.SUPPORTS_FIELDS


__all__ = ["BaseProvider", "PROVIDER_REGISTRY", "register"]
