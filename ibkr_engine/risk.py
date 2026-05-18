"""Pre-trade safety / risk validation.

`OrderRiskValidator.check()` runs BEFORE any transport is touched. It returns
a `RiskCheckResult` — never raises — so the caller can fold the rejection
straight into the standard error response.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from .config import IBKRConfig
from .utils import (
    redact,
    validate_action,
    validate_limit_price,
    validate_quantity,
    validate_symbol,
)

logger = logging.getLogger("ibkr_engine.risk")


@dataclass
class RiskCheckResult:
    ok: bool
    reason: str | None = None
    code: str | None = None
    details: dict[str, Any] = field(default_factory=dict)


class OrderRiskValidator:
    """Stateless validator — instantiated per `IBKR()` engine."""

    def __init__(self, config: IBKRConfig):
        self.config = config

    # ----- public -----
    def check(
        self,
        *,
        mode: str,
        symbol: str,
        action: str,
        quantity: float,
        limit_price: float | None,
    ) -> RiskCheckResult:
        # 1. Field-level validation
        try:
            symbol = validate_symbol(symbol)
            action = validate_action(action)
            quantity = validate_quantity(quantity)
            limit_price = validate_limit_price(limit_price)
        except ValueError as exc:
            return self._block("validation_failed", str(exc), field=str(exc))

        # 2. Master order-placement switch
        if not self.config.enable_order_placement:
            return self._block(
                "order_placement_disabled",
                "Order placement is disabled. Set IBKR_ENABLE_ORDER_PLACEMENT=true to enable.",
                flag="IBKR_ENABLE_ORDER_PLACEMENT",
            )

        # 3. Live-trading guard — defense in depth
        normalized_mode = (mode or "unknown").lower()
        if normalized_mode == "live" and not self.config.allow_live_trading:
            return self._block(
                "live_trading_blocked",
                "Live trading is blocked. Set IBKR_ALLOW_LIVE_TRADING=true to enable.",
                mode=normalized_mode,
                flag="IBKR_ALLOW_LIVE_TRADING",
            )

        # 4. Quantity cap
        if quantity > self.config.max_order_qty:
            return self._block(
                "max_quantity_exceeded",
                f"quantity {quantity:g} > max {self.config.max_order_qty}",
                quantity=quantity,
                max_quantity=self.config.max_order_qty,
            )

        # 5. Notional cap (only computable when we have a price)
        if limit_price is not None and self.config.max_notional_usd > 0:
            notional = limit_price * quantity
            if notional > self.config.max_notional_usd:
                return self._block(
                    "max_notional_exceeded",
                    f"notional ${notional:,.2f} > max ${self.config.max_notional_usd:,.2f}",
                    notional=notional,
                    max_notional=self.config.max_notional_usd,
                )

        # All checks passed
        logger.info(
            "Risk check OK · %s %s %s mode=%s",
            action, quantity, redact(symbol), normalized_mode,
        )
        return RiskCheckResult(ok=True)

    # ----- internal -----
    def _block(self, code: str, reason: str, **details: Any) -> RiskCheckResult:
        logger.warning("Risk check BLOCKED · %s · %s", code, reason)
        return RiskCheckResult(ok=False, reason=reason, code=code, details=details)


__all__ = ["RiskCheckResult", "OrderRiskValidator"]
