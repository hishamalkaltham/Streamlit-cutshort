"""Finnhub provider — broad symbol coverage + cheap quotes."""
from __future__ import annotations

from typing import Any

from ..models import DataType
from .base import BaseProvider, register


@register("finnhub")
class FinnhubProvider(BaseProvider):
    BASE_URL = "https://finnhub.io/api/v1"
    SUPPORTS_FIELDS = {"price", "open", "high", "low", "previous_close",
                        "name", "market_cap", "exchange", "industry",
                        "pe_ratio", "eps"}

    def default_confidence(self, field: str) -> float:
        if field == "price":
            return 0.85
        return 0.72

    def _fetch_field(self, symbol: str, field: str, data_type: DataType) -> Any:
        if field in {"price", "open", "high", "low", "previous_close"}:
            data = self._http_get(f"{self.BASE_URL}/quote",
                                    params={"symbol": symbol, "token": self.api_key})
            return {
                "price":          data.get("c"),
                "open":           data.get("o"),
                "high":           data.get("h"),
                "low":            data.get("l"),
                "previous_close": data.get("pc"),
            }.get(field)

        if field in {"name", "market_cap", "exchange", "industry"}:
            data = self._http_get(f"{self.BASE_URL}/stock/profile2",
                                    params={"symbol": symbol, "token": self.api_key})
            return {
                "name":       data.get("name"),
                "market_cap": data.get("marketCapitalization"),
                "exchange":   data.get("exchange"),
                "industry":   data.get("finnhubIndustry"),
            }.get(field)

        if field in {"pe_ratio", "eps"}:
            data = self._http_get(f"{self.BASE_URL}/stock/metric",
                                    params={"symbol": symbol, "metric": "all",
                                              "token": self.api_key})
            metrics = (data or {}).get("metric") or {}
            return {
                "pe_ratio": metrics.get("peNormalizedAnnual") or metrics.get("peTTM"),
                "eps":      metrics.get("epsNormalizedAnnual") or metrics.get("epsTTM"),
            }.get(field)

        return None
