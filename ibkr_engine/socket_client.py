"""ib_insync transport for the IBKR socket API (TWS / Gateway).

Wraps `ib_insync.IB` in a façade that:
  * boots the asyncio loop before importing `ib_insync` (Python 3.14 + Streamlit safe)
  * probes ports with a TCP connect before attempting a full handshake
  * never raises across the API boundary — every method returns a `dict`
"""
from __future__ import annotations

import logging
from typing import Any

from .config import IBKRConfig
from .errors import EventLoopError, MissingDependency
from .models import (
    AccountInfo,
    ConnectionInfo,
    MarketDataSnapshot,
    OrderTicket,
    Position,
    err,
    ok,
)
from .utils import (
    ensure_event_loop,
    install_nest_asyncio,
    is_port_open,
    safe_float,
    validate_symbol,
)

logger = logging.getLogger("ibkr_engine.socket")


class IBSocketClient:
    """Manages a single ib_insync.IB() handle, picking the first reachable port."""

    def __init__(self, config: IBKRConfig):
        self.config = config
        self._ib: Any = None
        self._connected_port: int | None = None

    # ============================================================
    # Lifecycle
    # ============================================================
    def _import_ib_insync(self) -> Any:
        """Import ib_insync after bootstrapping the event loop."""
        ensure_event_loop()
        install_nest_asyncio()
        try:
            import ib_insync as ibi  # noqa: WPS433 (intentional inline import)
            return ibi
        except ImportError as exc:
            raise MissingDependency(
                "ib_insync is not installed — run `pip install ib_insync nest_asyncio`"
            ) from exc
        except RuntimeError as exc:
            raise EventLoopError(f"event-loop bootstrap insufficient: {exc}") from exc

    def connect_first_available(self, ports: list[int] | None = None) -> dict:
        """Try each port in order; stop on the first successful connection."""
        try:
            ibi = self._import_ib_insync()
        except (MissingDependency, EventLoopError) as exc:
            return err(
                "ib_insync", "unknown", str(exc),
                exception=type(exc).__name__,
            )

        ports = list(ports or self.config.socket_ports)
        attempts: list[dict[str, Any]] = []

        for port in ports:
            if not is_port_open(self.config.host, port, timeout=1.0):
                attempts.append({"port": port, "reachable": False, "error": "port closed"})
                continue
            try:
                ib = ibi.IB()
                ib.connect(
                    self.config.host,
                    int(port),
                    clientId=int(self.config.client_id),
                    timeout=float(self.config.timeout_seconds),
                    readonly=False,
                )
                if not ib.isConnected():
                    attempts.append({"port": port, "reachable": True, "error": "handshake_failed"})
                    continue
                self._ib = ib
                self._connected_port = int(port)
                info = ConnectionInfo(
                    source="ib_insync",
                    mode=self.config.port_mode(int(port)),
                    host=self.config.host,
                    port=int(port),
                    server_version=str(ib.client.serverVersion()),
                    client_id=int(self.config.client_id),
                )
                logger.info(
                    "Socket connected · %s:%s · mode=%s · server=%s",
                    self.config.host, port, info.mode, info.server_version,
                )
                return ok(
                    "ib_insync", info.mode, info.to_dict(),
                    attempts=attempts + [{"port": port, "ok": True}],
                    label=self.config.port_label(int(port)),
                )
            except Exception as exc:  # broad on purpose — never crash
                attempts.append({
                    "port": port, "reachable": True, "error": str(exc)[:160],
                })
                logger.warning("Socket connect failed · %s:%s · %s", self.config.host, port, exc)
                # Make sure we don't leak a half-open client
                try:
                    ib.disconnect()  # type: ignore[name-defined]
                except Exception:
                    pass

        return err("ib_insync", "unknown", "All socket ports failed", attempts=attempts)

    def disconnect(self) -> dict:
        if self._ib is None:
            return ok("ib_insync", "unknown", {"status": "already_disconnected"})
        try:
            self._ib.disconnect()
        except Exception as exc:
            return err("ib_insync", self.mode, f"disconnect failed: {exc}")
        finally:
            self._ib = None
            self._connected_port = None
        return ok("ib_insync", "unknown", {"status": "disconnected"})

    # ============================================================
    # State
    # ============================================================
    @property
    def ib(self) -> Any:
        return self._ib

    @property
    def is_connected(self) -> bool:
        return self._ib is not None and self._ib.isConnected()

    @property
    def mode(self) -> str:
        return self.config.port_mode(self._connected_port)

    def status(self) -> dict:
        if not self.is_connected:
            return ok("ib_insync", "unknown", {"connected": False})
        return ok("ib_insync", self.mode, {
            "connected": True,
            "host": self.config.host,
            "port": self._connected_port,
            "client_id": self.config.client_id,
            "server_version": str(self._ib.client.serverVersion()),
            "label": self.config.port_label(self._connected_port),
        })

    # ============================================================
    # Read-only queries
    # ============================================================
    def get_accounts(self) -> dict:
        if not self.is_connected:
            return err("ib_insync", "unknown", "not connected")
        try:
            return ok("ib_insync", self.mode, list(self._ib.managedAccounts() or []))
        except Exception as exc:
            return err("ib_insync", self.mode, str(exc))

    def get_account_summary(self) -> dict:
        if not self.is_connected:
            return err("ib_insync", "unknown", "not connected")
        try:
            rows = self._ib.accountSummary()
            grouped: dict[str, dict[str, Any]] = {}
            for row in rows:
                acc_fields = grouped.setdefault(row.account, {"_account": row.account})
                acc_fields[row.tag] = {"value": row.value, "currency": row.currency}

            accounts: list[dict[str, Any]] = []
            for account_id, fields in grouped.items():
                info = AccountInfo(
                    account_id=account_id,
                    currency=(fields.get("NetLiquidation") or {}).get("currency"),
                    net_liquidation=safe_float((fields.get("NetLiquidation") or {}).get("value")),
                    available_funds=safe_float((fields.get("AvailableFunds") or {}).get("value")),
                    buying_power=safe_float((fields.get("BuyingPower") or {}).get("value")),
                    excess_liquidity=safe_float((fields.get("ExcessLiquidity") or {}).get("value")),
                    raw=fields,
                )
                accounts.append(info.to_dict())
            return ok("ib_insync", self.mode, accounts)
        except Exception as exc:
            return err("ib_insync", self.mode, str(exc))

    def get_positions(self) -> dict:
        if not self.is_connected:
            return err("ib_insync", "unknown", "not connected")
        try:
            poss = self._ib.positions()
            out: list[dict[str, Any]] = []
            for p in poss:
                pos = Position(
                    account=p.account,
                    symbol=getattr(p.contract, "symbol", "?"),
                    quantity=float(p.position),
                    avg_cost=safe_float(p.avgCost),
                    raw={
                        "secType": getattr(p.contract, "secType", None),
                        "exchange": getattr(p.contract, "exchange", None),
                        "currency": getattr(p.contract, "currency", None),
                        "conId": getattr(p.contract, "conId", None),
                    },
                )
                out.append(pos.to_dict())
            return ok("ib_insync", self.mode, out)
        except Exception as exc:
            return err("ib_insync", self.mode, str(exc))

    def get_open_orders(self) -> dict:
        if not self.is_connected:
            return err("ib_insync", "unknown", "not connected")
        try:
            orders = self._ib.openOrders()
            out = [{
                "orderId": o.orderId,
                "permId": getattr(o, "permId", None),
                "action": o.action,
                "totalQuantity": float(o.totalQuantity or 0),
                "orderType": o.orderType,
                "lmtPrice": safe_float(getattr(o, "lmtPrice", None)),
                "auxPrice": safe_float(getattr(o, "auxPrice", None)),
                "tif": o.tif,
                "outsideRth": getattr(o, "outsideRth", False),
            } for o in orders]
            return ok("ib_insync", self.mode, out)
        except Exception as exc:
            return err("ib_insync", self.mode, str(exc))

    # ============================================================
    # Contracts + market data
    # ============================================================
    def qualify_contract(self, symbol: str, exchange: str = "SMART", currency: str = "USD") -> Any:
        ibi = self._import_ib_insync()
        contract = ibi.Stock(validate_symbol(symbol), exchange, currency)
        qualified = self._ib.qualifyContracts(contract)
        if not qualified:
            raise ValueError(f"Could not qualify contract for {symbol}")
        return qualified[0]

    def get_market_data(self, symbol: str) -> dict:
        if not self.is_connected:
            return err("ib_insync", "unknown", "not connected")
        try:
            contract = self.qualify_contract(symbol)
            ticker = self._ib.reqMktData(contract, "", snapshot=False, regulatorySnapshot=False)
            self._ib.sleep(1.5)  # let the snapshot populate
            snap = MarketDataSnapshot(
                symbol=validate_symbol(symbol),
                last=safe_float(getattr(ticker, "last", None)),
                bid=safe_float(getattr(ticker, "bid", None)),
                ask=safe_float(getattr(ticker, "ask", None)),
                volume=safe_float(getattr(ticker, "volume", None)),
                high=safe_float(getattr(ticker, "high", None)),
                low=safe_float(getattr(ticker, "low", None)),
                close=safe_float(getattr(ticker, "close", None)),
                raw={
                    "marketDataType": getattr(ticker, "marketDataType", None),
                    "contract": {
                        "symbol": contract.symbol,
                        "secType": contract.secType,
                        "exchange": contract.exchange,
                        "currency": contract.currency,
                        "conId": contract.conId,
                    },
                },
            )
            try:
                self._ib.cancelMktData(contract)
            except Exception:
                pass
            return ok("ib_insync", self.mode, snap.to_dict())
        except Exception as exc:
            return err("ib_insync", self.mode, str(exc))

    # ============================================================
    # Orders
    # ============================================================
    def place_order(self, ticket: OrderTicket) -> dict:
        if not self.is_connected:
            return err("ib_insync", "unknown", "not connected")
        try:
            ibi = self._import_ib_insync()
            contract = self.qualify_contract(ticket.symbol, ticket.exchange, ticket.currency)

            order_type = ticket.order_type.upper()
            if order_type == "MKT":
                order = ibi.MarketOrder(ticket.action, ticket.quantity)
            elif order_type == "LMT":
                if ticket.limit_price is None:
                    return err("ib_insync", self.mode, "limit_price required for LMT order")
                order = ibi.LimitOrder(ticket.action, ticket.quantity, ticket.limit_price)
            elif order_type == "STP":
                if ticket.aux_price is None:
                    return err("ib_insync", self.mode, "aux_price required for STP order")
                order = ibi.StopOrder(ticket.action, ticket.quantity, ticket.aux_price)
            elif order_type == "STP_LMT":
                if ticket.limit_price is None or ticket.aux_price is None:
                    return err("ib_insync", self.mode,
                               "limit_price and aux_price required for STP_LMT order")
                order = ibi.StopLimitOrder(
                    ticket.action, ticket.quantity,
                    ticket.limit_price, ticket.aux_price,
                )
            else:
                return err("ib_insync", self.mode, f"unsupported order_type: {ticket.order_type}")

            order.tif = ticket.tif
            order.outsideRth = bool(ticket.outside_rth)

            trade = self._ib.placeOrder(contract, order)
            return ok("ib_insync", self.mode, {
                "orderId": trade.order.orderId,
                "permId": getattr(trade.order, "permId", None),
                "status": trade.orderStatus.status,
                "filled": float(trade.orderStatus.filled or 0),
                "remaining": float(trade.orderStatus.remaining or 0),
                "avgFillPrice": safe_float(trade.orderStatus.avgFillPrice),
                "submitted": True,
            })
        except Exception as exc:
            return err("ib_insync", self.mode, str(exc))

    def cancel_order(self, order_id: int) -> dict:
        if not self.is_connected:
            return err("ib_insync", "unknown", "not connected")
        try:
            target_id = int(order_id)
            for trade in self._ib.openTrades():
                if trade.order.orderId == target_id:
                    self._ib.cancelOrder(trade.order)
                    return ok("ib_insync", self.mode, {
                        "orderId": target_id,
                        "status": "cancel_sent",
                    })
            return err("ib_insync", self.mode, f"orderId {target_id} not found among open trades")
        except Exception as exc:
            return err("ib_insync", self.mode, str(exc))


__all__ = ["IBSocketClient"]
