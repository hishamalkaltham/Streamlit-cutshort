"""Typed dataclasses + the canonical response builders.

Every public engine method returns a dict matching this contract:

    {
        "_ok":    bool,
        "source": "ib_insync" | "client_portal" | "none",
        "mode":   "paper" | "live" | "unknown",
        "data":   ...,
        "error":  None | str,
        "details": dict,
    }

`ok()` and `err()` are the only blessed builders — never craft these by hand.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

# Source / mode literals (kept as str for serialisation simplicity)
SOURCE_SOCKET = "ib_insync"
SOURCE_REST = "client_portal"
SOURCE_NONE = "none"

MODE_PAPER = "paper"
MODE_LIVE = "live"
MODE_UNKNOWN = "unknown"


# ============================================================
# Dataclasses
# ============================================================
@dataclass
class ConnectionInfo:
    source: str = SOURCE_NONE
    mode: str = MODE_UNKNOWN
    host: str = "127.0.0.1"
    port: int | None = None
    base_url: str | None = None
    server_version: str | None = None
    client_id: int | None = None
    connected_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(timespec="seconds")
    )

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class AccountInfo:
    account_id: str
    currency: str | None = None
    net_liquidation: float | None = None
    available_funds: float | None = None
    buying_power: float | None = None
    excess_liquidity: float | None = None
    raw: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class Position:
    account: str
    symbol: str
    quantity: float
    avg_cost: float | None = None
    market_price: float | None = None
    market_value: float | None = None
    unrealized_pnl: float | None = None
    realized_pnl: float | None = None
    raw: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class OrderTicket:
    symbol: str
    action: str  # "BUY" | "SELL"
    quantity: float
    order_type: str = "MKT"  # MKT | LMT | STP | STP_LMT
    limit_price: float | None = None
    aux_price: float | None = None
    tif: str = "DAY"  # DAY | GTC | OPG | IOC
    exchange: str = "SMART"
    currency: str = "USD"
    outside_rth: bool = False

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class MarketDataSnapshot:
    symbol: str
    last: float | None = None
    bid: float | None = None
    ask: float | None = None
    volume: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(timespec="seconds")
    )
    raw: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


# ============================================================
# Canonical response builders
# ============================================================
def ok(source: str, mode: str, data: Any, **details: Any) -> dict:
    """Build a uniform success response."""
    return {
        "_ok": True,
        "source": source,
        "mode": mode,
        "data": data,
        "error": None,
        "details": details,
    }


def err(source: str, mode: str, message: str, **details: Any) -> dict:
    """Build a uniform error response. Never raises."""
    return {
        "_ok": False,
        "source": source,
        "mode": mode,
        "data": None,
        "error": str(message),
        "details": details,
    }


__all__ = [
    "SOURCE_SOCKET", "SOURCE_REST", "SOURCE_NONE",
    "MODE_PAPER", "MODE_LIVE", "MODE_UNKNOWN",
    "ConnectionInfo", "AccountInfo", "Position", "OrderTicket", "MarketDataSnapshot",
    "ok", "err",
]
