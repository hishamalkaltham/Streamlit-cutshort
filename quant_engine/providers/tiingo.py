"""Tiingo provider — clean EOD + IEX intraday."""
from __future__ import annotations

from typing import Any

from ..models import DataType
from .base import BaseProvider, register


@register("tiingo")
class TiingoProvider(BaseProvider):
    BASE_URL = "https://api.tiingo.com"
    SUPPORTS_FIELDS = {"price", "open", "high", "low", "close", "volume",
                        "previous_close", "name", "exchange", "description"}

    def default_confidence(self, field: str) -> float:
        if field in {"price", "close"}:
            return 0.88
        return 0.80

    def _auth_headers(self) -> dict:
        return {"Authorization": f"Token {self.api_key}",
                "Content-Type": "application/json"}

    def _fetch_field(self, symbol: str, field: str, data_type: DataType) -> Any:
        if field in {"price", "open", "high", "low", "close", "volume"}:
            # IEX endpoint — most intraday-friendly free tier path
            url = f"{self.BASE_URL}/iex/{symbol}"
            data = self._http_get(url, headers=self._auth_headers())
            if not isinstance(data, list) or not data:
                return None
            row = data[0]
            return {
                "price":  row.get("last") or row.get("tngoLast"),
                "open":   row.get("open"),
                "high":   row.get("high"),
                "low":    row.get("low"),
                "close":  row.get("last") or row.get("tngoLast"),
                "volume": row.get("volume"),
            }.get(field)

        if field == "previous_close":
            url = f"{self.BASE_URL}/iex/{symbol}"
            data = self._http_get(url, headers=self._auth_headers())
            if not isinstance(data, list) or not data:
                return None
            return data[0].get("prevClose")

        if field in {"name", "exchange", "description"}:
            url = f"{self.BASE_URL}/tiingo/daily/{symbol}"
            data = self._http_get(url, headers=self._auth_headers())
            if not isinstance(data, dict):
                return None
            return {
                "name":        data.get("name"),
                "exchange":    data.get("exchangeCode"),
                "description": data.get("description"),
            }.get(field)

        return None
