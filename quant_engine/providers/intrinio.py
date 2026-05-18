"""Intrinio provider — exceptional fundamentals coverage."""
from __future__ import annotations

from typing import Any

from ..models import DataType
from .base import BaseProvider, register


@register("intrinio")
class IntrinioProvider(BaseProvider):
    BASE_URL = "https://api-v2.intrinio.com"
    SUPPORTS_FIELDS = {"price", "previous_close", "name", "market_cap",
                        "pe_ratio", "eps", "dividend_yield", "sector",
                        "industry", "ceo", "employees"}

    def default_confidence(self, field: str) -> float:
        # Intrinio shines on fundamentals — boost their confidence there.
        if field in {"pe_ratio", "eps", "dividend_yield", "market_cap"}:
            return 0.92
        if field == "price":
            return 0.80
        return 0.78

    def _fetch_field(self, symbol: str, field: str, data_type: DataType) -> Any:
        if field in {"price", "previous_close"}:
            url = f"{self.BASE_URL}/securities/{symbol}/prices/realtime"
            data = self._http_get(url, params={"api_key": self.api_key})
            return {
                "price": data.get("last_price") or data.get("close_price"),
                "previous_close": data.get("close_price"),
            }.get(field)

        if field in {"name", "market_cap", "sector", "industry", "ceo", "employees"}:
            url = f"{self.BASE_URL}/companies/{symbol}"
            data = self._http_get(url, params={"api_key": self.api_key})
            return {
                "name":       data.get("name") or data.get("legal_name"),
                "market_cap": data.get("market_cap"),
                "sector":     data.get("sector"),
                "industry":   data.get("industry_category"),
                "ceo":        data.get("ceo"),
                "employees":  data.get("employees"),
            }.get(field)

        if field in {"pe_ratio", "eps", "dividend_yield"}:
            tag_map = {
                "pe_ratio": "pricetoearnings",
                "eps": "basiceps",
                "dividend_yield": "dividendyield",
            }
            tag = tag_map[field]
            url = f"{self.BASE_URL}/historical_data/{symbol}/{tag}"
            data = self._http_get(url, params={"api_key": self.api_key,
                                                  "page_size": 1})
            history = (data or {}).get("historical_data") or []
            return history[0].get("value") if history else None

        return None
