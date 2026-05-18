"""Internal exception hierarchy for the IBKR engine.

These are caught at every public-API boundary and returned as structured
error dicts (`{"_ok": False, "error": ..., ...}`) so callers never need to
wrap calls in try/except.
"""
from __future__ import annotations


class IBKREngineError(Exception):
    """Root for every error raised inside the engine."""


# ---- transport / connection ----
class ConnectionFailed(IBKREngineError):
    """Could not establish a connection to a single transport."""


class AllConnectionsFailed(IBKREngineError):
    """Every transport in the priority chain failed."""


class UnsupportedTransport(IBKREngineError):
    """Active transport doesn't support the requested operation."""


# ---- session / auth ----
class AuthenticationRequired(IBKREngineError):
    """REST gateway is reachable but the user has not signed in."""


class SessionExpired(IBKREngineError):
    """Previously valid session is no longer accepted."""


# ---- orders / risk ----
class OrderBlocked(IBKREngineError):
    """Order rejected by the safety / risk layer (NOT a broker rejection)."""


class OrderValidationFailed(OrderBlocked):
    """Symbol / action / quantity / price failed validation."""


class LiveTradingBlocked(OrderBlocked):
    """Tried to send a live order while IBKR_ALLOW_LIVE_TRADING is false."""


class OrderPlacementDisabled(OrderBlocked):
    """IBKR_ENABLE_ORDER_PLACEMENT is false (default)."""


class MaxQuantityExceeded(OrderBlocked):
    pass


class MaxNotionalExceeded(OrderBlocked):
    pass


# ---- environment / runtime ----
class MissingDependency(IBKREngineError):
    """An optional library (ib_insync / httpx / nest_asyncio) is not installed."""


class EventLoopError(IBKREngineError):
    """asyncio bootstrap failed — usually a Python 3.14 + thread issue."""


# ---- market data ----
class MarketDataUnavailable(IBKREngineError):
    """Subscription missing, market closed, or bad symbol."""


__all__ = [
    "IBKREngineError",
    "ConnectionFailed",
    "AllConnectionsFailed",
    "UnsupportedTransport",
    "AuthenticationRequired",
    "SessionExpired",
    "OrderBlocked",
    "OrderValidationFailed",
    "LiveTradingBlocked",
    "OrderPlacementDisabled",
    "MaxQuantityExceeded",
    "MaxNotionalExceeded",
    "MissingDependency",
    "EventLoopError",
    "MarketDataUnavailable",
]
