"""Provider implementations.

Importing this package eagerly registers every concrete provider in
`PROVIDER_REGISTRY`, so the DataEngine can look one up by name.
"""
from __future__ import annotations

from .base import BaseProvider, PROVIDER_REGISTRY, register
from .polygon import PolygonProvider
from .tiingo import TiingoProvider
from .intrinio import IntrinioProvider
from .finnhub import FinnhubProvider
from .yahoo import YahooProvider

__all__ = [
    "BaseProvider", "PROVIDER_REGISTRY", "register",
    "PolygonProvider", "TiingoProvider", "IntrinioProvider",
    "FinnhubProvider", "YahooProvider",
]
