"""Polygon.io provider — gold-standard real-time + historical."""
from __future__ import annotations

from typing import Any

from ..models import DataType
from .base import BaseProvider, register


@register("polygon")
class PolygonProvider(BaseProvider):
    BASE_URL = "https://api.polygon.io"
    SUPPORTS_FIELDS = {"price", "open", "high", "low", "close", "volume",
                        "vwap", "previous_close", "name", "market_cap"}

    def default_confidence(self, field: str) -> float:
        # Polygon is consensus-grade for US equities
        if field in {"price", "vwap"}:
            return 0.92
        return 0.85

    def _fetch_field(self, symbol: str, field: str, data_type: DataType) -> Any:
        # Real-time/last-trade/snapshot quote
        if field in {"price", "open", "high", "low", "close", "volume", "vwap"}:
            url = f"{self.BASE_URL}/v2/aggs/ticker/{symbol}/prev"
            data = self._http_get(url, params={"adjusted": "true", "apiKey": self.api_key})
            results = (data or {}).get("results") or []
            if not results:
                return None
            row = results[0]
            return {
                "price": row.get("c"),       # close
                "open":  row.get("o"),
                "high":  row.get("h"),
                "low":   row.get("l"),
                "close": row.get("c"),
                "volume": row.get("v"),
                "vwap": row.get("vw"),
            }.get(field)

        # Previous close (faster endpoint)
        if field == "previous_close":
            url = f"{self.BASE_URL}/v2/aggs/ticker/{symbol}/prev"
            data = self._http_get(url, params={"apiKey": self.api_key})
            results = (data or {}).get("results") or []
            return results[0].get("c") if results else None

        # Company info
        if field in {"name", "market_cap"}:
            url = f"{self.BASE_URL}/v3/reference/tickers/{symbol}"
            data = self._http_get(url, params={"apiKey": self.api_key})
            results = (data or {}).get("results") or {}
            return {
                "name": results.get("name"),
                "market_cap": results.get("market_cap"),
            }.get(field)

        return None
