"""Yahoo Finance provider — keyless fallback via yfinance.

This is the universal safety net: if every other provider rate-limits or
times out, Yahoo will (almost) always answer.
"""
from __future__ import annotations

from typing import Any

from ..errors import ProviderBadResponse, ProviderError
from ..models import DataType
from .base import BaseProvider, register


@register("yahoo")
class YahooProvider(BaseProvider):
    REQUIRES_KEY = False
    SUPPORTS_FIELDS = {"price", "open", "high", "low", "close", "volume",
                        "previous_close", "name", "market_cap", "currency",
                        "pe_ratio", "eps", "dividend_yield", "sector", "industry"}

    def default_confidence(self, field: str) -> float:
        # Yahoo is reliable but unofficial; treat as a tiebreaker, not source of truth
        return 0.55 if field == "price" else 0.50

    def _fast_info(self, symbol: str) -> dict:
        try:
            import yfinance as yf
        except ImportError as exc:
            raise ProviderError(self.NAME, "yfinance not installed") from exc
        try:
            t = yf.Ticker(symbol)
            return dict(getattr(t, "fast_info", {}) or {})
        except Exception as exc:
            raise ProviderBadResponse(self.NAME, f"fast_info failed: {exc}") from exc

    def _full_info(self, symbol: str) -> dict:
        try:
            import yfinance as yf
        except ImportError as exc:
            raise ProviderError(self.NAME, "yfinance not installed") from exc
        try:
            return yf.Ticker(symbol).info or {}
        except Exception as exc:
            raise ProviderBadResponse(self.NAME, f"info failed: {exc}") from exc

    def _fetch_field(self, symbol: str, field: str, data_type: DataType) -> Any:
        # Fast path — fast_info is much cheaper than .info
        if field in {"price", "open", "high", "low", "previous_close",
                      "market_cap", "currency"}:
            info = self._fast_info(symbol)
            return {
                "price":          info.get("last_price") or info.get("lastPrice"),
                "open":           info.get("open"),
                "high":           info.get("day_high") or info.get("dayHigh"),
                "low":            info.get("day_low")  or info.get("dayLow"),
                "previous_close": info.get("previous_close") or info.get("previousClose"),
                "market_cap":     info.get("market_cap")    or info.get("marketCap"),
                "currency":       info.get("currency"),
            }.get(field)

        if field == "volume":
            info = self._fast_info(symbol)
            return info.get("last_volume") or info.get("lastVolume")

        # Slow path — pulls .info; only call when necessary
        if field in {"name", "pe_ratio", "eps", "dividend_yield",
                      "sector", "industry"}:
            info = self._full_info(symbol)
            return {
                "name":           info.get("shortName") or info.get("longName"),
                "pe_ratio":       info.get("trailingPE") or info.get("forwardPE"),
                "eps":            info.get("trailingEps"),
                "dividend_yield": info.get("dividendYield"),
                "sector":         info.get("sector"),
                "industry":       info.get("industry"),
            }.get(field)

        return None
