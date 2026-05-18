"""Configuration for the IBKR engine.

Reads from environment variables (optionally loaded via python-dotenv) so the
running process can be configured without code changes.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()  # No-op if .env doesn't exist
except ImportError:
    # python-dotenv is optional — env vars set in the OS shell still work.
    pass


def _csv_ints(raw: str | None, default: list[int]) -> list[int]:
    """Parse `4002,4001,7497` → [4002, 4001, 7497]."""
    if not raw:
        return list(default)
    out: list[int] = []
    for chunk in str(raw).split(","):
        chunk = chunk.strip()
        if chunk.isdigit():
            out.append(int(chunk))
    return out or list(default)


def _bool(raw: str | None, default: bool) -> bool:
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")


@dataclass(frozen=True)
class IBKRConfig:
    """Immutable runtime configuration."""

    host: str = field(default_factory=lambda: os.getenv("IBKR_HOST", "127.0.0.1"))
    socket_ports: list[int] = field(
        default_factory=lambda: _csv_ints(
            os.getenv("IBKR_SOCKET_PORTS"),
            [4002, 4001, 7497, 7496],  # Gateway paper → Gateway live → TWS paper → TWS live
        )
    )
    client_id: int = field(default_factory=lambda: int(os.getenv("IBKR_CLIENT_ID", "11")))
    client_portal_base: str = field(
        default_factory=lambda: os.getenv("IBKR_CLIENT_PORTAL_BASE", "https://localhost:5000/v1/api")
    )

    enable_order_placement: bool = field(
        default_factory=lambda: _bool(os.getenv("IBKR_ENABLE_ORDER_PLACEMENT"), False)
    )
    allow_live_trading: bool = field(
        default_factory=lambda: _bool(os.getenv("IBKR_ALLOW_LIVE_TRADING"), False)
    )
    max_order_qty: int = field(default_factory=lambda: int(os.getenv("IBKR_MAX_ORDER_QTY", "10")))
    max_notional_usd: float = field(default_factory=lambda: float(os.getenv("IBKR_MAX_NOTIONAL_USD", "1000")))

    timeout_seconds: float = field(default_factory=lambda: float(os.getenv("IBKR_TIMEOUT_SECONDS", "5")))
    log_level: str = field(default_factory=lambda: os.getenv("IBKR_LOG_LEVEL", "INFO"))

    # ---- helpers ----
    @staticmethod
    def port_mode(port: int | None) -> str:
        """Map a known port to its trading mode."""
        if port is None:
            return "unknown"
        return {
            4001: "live",   # IB Gateway live
            4002: "paper",  # IB Gateway paper
            7496: "live",   # TWS live
            7497: "paper",  # TWS paper
        }.get(int(port), "unknown")

    @staticmethod
    def port_label(port: int | None) -> str:
        if port is None:
            return "unknown"
        return {
            4001: "IB Gateway · live",
            4002: "IB Gateway · paper",
            7496: "TWS · live",
            7497: "TWS · paper",
        }.get(int(port), f"custom:{port}")
