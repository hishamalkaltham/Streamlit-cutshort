"""Cross-cutting helpers: event-loop bootstrap, validators, redaction."""
from __future__ import annotations

import asyncio
import logging
import re
import socket
from typing import Any

logger = logging.getLogger("ibkr_engine.utils")


# ============================================================
# AsyncIO bootstrap
# ============================================================
def ensure_event_loop() -> asyncio.AbstractEventLoop:
    """Guarantee that the current thread has a usable event loop.

    Handles every variant we've seen in production:
      - Python 3.14+ removed implicit loop creation in non-main threads
      - Streamlit's `ScriptRunner.scriptThread` has no loop on first hit
      - `eventkit/util.py` (a transitive ib_insync dep) calls
        `asyncio.get_event_loop()` at import time and crashes if none exists.

    Always returns a loop. Never raises.
    """
    # 1. If a loop is already running in this thread, return it
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        pass
    # 2. Try the policy's loop (may exist but not be running)
    try:
        return asyncio.get_event_loop()
    except RuntimeError:
        pass
    # 3. Create a fresh loop and install it
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop


def install_nest_asyncio() -> bool:
    """Apply nest_asyncio if available — lets ib_insync run inside a running loop.

    Returns True if applied, False if the package is missing.
    """
    try:
        import nest_asyncio  # type: ignore
    except ImportError:
        return False
    try:
        nest_asyncio.apply()
        return True
    except Exception as exc:
        logger.warning("nest_asyncio.apply() failed: %s", exc)
        return False


# ============================================================
# Network probe
# ============================================================
def is_port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    """Cheap TCP probe used before attempting an `ib_insync` handshake."""
    try:
        with socket.create_connection((host, int(port)), timeout=float(timeout)):
            return True
    except OSError:
        return False


# ============================================================
# Validators
# ============================================================
def validate_symbol(symbol: str) -> str:
    if not isinstance(symbol, str):
        raise ValueError("symbol must be a string")
    s = symbol.strip()
    if not s:
        raise ValueError("symbol must be a non-empty string")
    if len(s) > 32:
        raise ValueError("symbol too long")
    return s.upper()


def validate_action(action: str) -> str:
    if not isinstance(action, str):
        raise ValueError("action must be a string")
    a = action.strip().upper()
    if a not in {"BUY", "SELL"}:
        raise ValueError("action must be 'BUY' or 'SELL'")
    return a


def validate_quantity(qty: Any) -> float:
    try:
        q = float(qty)
    except (TypeError, ValueError):
        raise ValueError("quantity must be numeric")
    if q != q or q in (float("inf"), float("-inf")):
        raise ValueError("quantity is not a finite number")
    if q <= 0:
        raise ValueError("quantity must be > 0")
    return q


def validate_limit_price(price: Any) -> float | None:
    if price is None:
        return None
    try:
        p = float(price)
    except (TypeError, ValueError):
        raise ValueError("limit_price must be numeric")
    if p <= 0:
        raise ValueError("limit_price must be > 0")
    return p


def safe_float(value: Any) -> float | None:
    """Best-effort numeric cast; returns None for NaN / inf / bad input."""
    try:
        if value is None or value == "":
            return None
        f = float(value)
    except (TypeError, ValueError):
        return None
    if f != f or f in (float("inf"), float("-inf")):
        return None
    return f


# ============================================================
# Secret redaction (for logging)
# ============================================================
_REDACT_RULES: list[tuple[re.Pattern[str], str]] = [
    # key=value style
    (re.compile(
        r"(api[_-]?key|token|password|secret|cookie|authorization|bearer)\s*[:=]\s*[\"']?([\w\-./+=]+)",
        re.IGNORECASE,
    ), r"\1=***"),
    # IBKR account ids start with U/DU/F + 6+ digits
    (re.compile(r"\b(D?U|F)\d{6,}\b"), r"<account>"),
]


def redact(text: Any) -> str:
    """Strip obvious secrets / account ids from a message before logging."""
    s = str(text)
    for pat, repl in _REDACT_RULES:
        s = pat.sub(repl, s)
    return s


__all__ = [
    "ensure_event_loop", "install_nest_asyncio", "is_port_open",
    "validate_symbol", "validate_action", "validate_quantity", "validate_limit_price",
    "safe_float", "redact",
]
