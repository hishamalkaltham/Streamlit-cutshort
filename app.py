"""
Stock Research Terminal

pip requirements:
- streamlit>=1.42
- pandas>=2.2
- plotly>=5.24
- requests>=2.32
- yfinance>=0.2.55
- websocket-client>=1.8  # optional for short Binance WebSocket probes

Run:
    streamlit run app.py

Environment variables examples:
    export FMP_API_KEY="your_fmp_key"
    export TWELVE_DATA_API_KEY="your_twelve_data_key"
    export POLYGON_API_KEY="your_polygon_key"
    export ALPHA_VANTAGE_API_KEY="your_alpha_vantage_key"
    export FINNHUB_API_KEY="your_finnhub_key"
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import re
import socket
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from html import escape
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlparse


# ============================================================
# AsyncIO bootstrap (MUST run before importing eventkit/ib_insync)
# ------------------------------------------------------------
# Python 3.14 no longer auto-creates an event loop in non-main threads
# (e.g. Streamlit's `ScriptRunner.scriptThread`). `eventkit/util.py`
# (a transitive dep of ib_insync) calls `get_event_loop()` AT IMPORT TIME,
# so the import would crash with:
#   RuntimeError: There is no current event loop in thread 'ScriptRunner.scriptThread'.
# We pre-create one here so any later `import ib_insync` succeeds. On Python
# 3.14 we intentionally avoid nest_asyncio because it breaks asyncio.timeout.
# ============================================================
def _bootstrap_event_loop() -> None:
    try:
        asyncio.get_running_loop()
        return  # already inside a running loop — nothing to do
    except RuntimeError:
        pass
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    # nest_asyncio is optional, but it is not safe with Python 3.14's
    # asyncio.timeout/current_task semantics used by ib_insync.connectAsync.
    if sys.version_info < (3, 14):
        try:
            import nest_asyncio  # type: ignore
            nest_asyncio.apply()
        except Exception:
            pass


_bootstrap_event_loop()

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st
from plotly.subplots import make_subplots
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry



try:
    import yfinance as yf
except Exception:  # noqa: BLE001
    yf = None

try:
    import websocket as websocket_client
except Exception:  # noqa: BLE001
    websocket_client = None

HTTP_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
}
TTL_SECONDS = 900
TTL_NEWS_SECONDS = 300
MAX_COMPARE_SYMBOLS = 6
YAHOO_COOLDOWN_SECONDS = 600

ENV_KEY_NAMES = {
    "fmp": "FMP_API_KEY",
    "twelve_data": "TWELVE_DATA_API_KEY",
    "alpha_vantage": "ALPHA_VANTAGE_API_KEY",
    "polygon": "POLYGON_API_KEY",
    "finnhub": "FINNHUB_API_KEY",
    "tiingo": "TIINGO_API_KEY",
    "intrinio": "INTRINIO_API_KEY",
}

NEWS_PROVIDER_LABELS: dict[str, str] = {
    "finnhub":    "Finnhub",
    "yahoo":      "Yahoo Finance",
    "fmp":        "FMP",
    "twelve_data":"Twelve Data",
    "marketaux":  "Marketaux",
    "benzinga":   "Benzinga",
    "sec_api":    "SEC Filings",
    "newsapi_ai": "NewsAPI.ai",
}
NEWS_PROVIDER_ORDER = ["benzinga", "marketaux", "newsapi_ai", "sec_api", "finnhub", "fmp", "yahoo", "twelve_data"]
TRADINGVIEW_SUPPORTED_RESOLUTIONS = ["1", "5", "15", "30", "60", "D", "W", "M"]
TRADINGVIEW_EXCHANGES = ["NASDAQ", "NYSE"]
TRADINGVIEW_SYMBOL_TYPES = ["stock", "crypto"]

PROVIDER_LABELS = {
    "fmp": "Financial Modeling Prep",
    "yahoo": "Yahoo Finance",
    "alpha_vantage": "Alpha Vantage",
    "twelve_data": "Twelve Data",
    "polygon": "Polygon / Massive",
    "finnhub": "Finnhub",
    "tiingo": "Tiingo",
    "intrinio": "Intrinio",
    "calculated": "Calculated",
}

DEFAULT_PROVIDER_ORDER = ["fmp", "finnhub", "twelve_data", "polygon", "tiingo", "intrinio", "yahoo", "alpha_vantage"]
MARKET_PROVIDER_ORDER = ["fmp", "finnhub", "twelve_data", "yahoo", "polygon", "tiingo", "alpha_vantage"]
PROVIDER_THROTTLE_SECONDS = {
    "fmp": 0.0,
    "twelve_data": 0.2,
    "polygon": 0.15,
    "finnhub": 0.35,
    "alpha_vantage": 1.2,
    "yahoo": 0.5,
    "tiingo": 0.25,
    "intrinio": 0.25,
    "marketaux":  0.5,
    "benzinga":   0.5,
    "sec_api":    0.3,
    "newsapi_ai": 0.5,
}
PROVIDER_TIMEOUTS = {
    "fmp": 25,
    "twelve_data": 25,
    "polygon": 25,
    "finnhub": 25,
    "alpha_vantage": 30,
    "yahoo": 30,
    "tiingo": 20,
    "intrinio": 20,
    "marketaux":  20,
    "benzinga":   20,
    "sec_api":    25,
    "newsapi_ai": 20,
}
PROVIDER_DEFAULT_WORKERS = {
    "fmp": 6,
    "twelve_data": 3,
    "polygon": 2,
    "finnhub": 2,
    "alpha_vantage": 1,
    "yahoo": 1,
    "tiingo": 2,
    "intrinio": 2,
}
CORE_DIAGNOSTIC_FIELDS = ["price", "market_cap", "pe_ratio", "revenue", "free_cash_flow", "roe", "revenue_growth", "beta"]
FIELD_PROVIDER_ORDER = {
    "price": ["fmp", "finnhub", "twelve_data", "yahoo", "polygon", "tiingo", "alpha_vantage"],
    "previous_close": ["fmp", "finnhub", "twelve_data", "yahoo", "polygon", "tiingo", "alpha_vantage"],
    "change": ["fmp", "finnhub", "twelve_data", "yahoo", "polygon", "tiingo", "alpha_vantage"],
    "change_pct": ["fmp", "finnhub", "twelve_data", "yahoo", "polygon", "tiingo", "alpha_vantage"],
    "market_cap": ["fmp", "finnhub", "twelve_data", "yahoo", "polygon", "intrinio", "alpha_vantage"],
    "shares_outstanding": ["fmp", "finnhub", "twelve_data", "yahoo", "polygon", "intrinio", "alpha_vantage"],
    "revenue": ["fmp", "intrinio", "finnhub", "twelve_data", "polygon", "yahoo", "alpha_vantage"],
    "net_income": ["fmp", "intrinio", "finnhub", "twelve_data", "polygon", "yahoo", "alpha_vantage"],
    "operating_cash_flow": ["fmp", "intrinio", "finnhub", "twelve_data", "polygon", "yahoo", "alpha_vantage"],
    "free_cash_flow": ["fmp", "intrinio", "finnhub", "twelve_data", "polygon", "yahoo", "alpha_vantage"],
    "pe_ratio": ["fmp", "finnhub", "twelve_data", "yahoo", "alpha_vantage", "intrinio", "polygon"],
    "forward_pe": ["fmp", "finnhub", "twelve_data", "yahoo", "alpha_vantage", "intrinio", "polygon"],
    "pb_ratio": ["fmp", "finnhub", "twelve_data", "yahoo", "alpha_vantage", "intrinio", "polygon"],
    "ps_ratio": ["fmp", "finnhub", "twelve_data", "yahoo", "alpha_vantage", "intrinio", "polygon"],
    "ev_ebitda": ["fmp", "finnhub", "twelve_data", "yahoo", "alpha_vantage", "intrinio", "polygon"],
    "beta": ["finnhub", "fmp", "twelve_data", "yahoo", "alpha_vantage", "polygon"],
}
PROVIDER_LOCK = threading.Lock()
PROVIDER_LAST_CALL: dict[str, float] = {}
YAHOO_STATE = {"cooldown_until": 0.0, "reason": ""}
TEXT_FIELDS = [
    "company_name",
    "sector",
    "industry",
    "exchange",
    "country",
    "currency",
    "website",
    "description",
    "logo_url",
]
MARKET_FIELDS = {"price", "previous_close", "change", "change_pct", "market_cap", "shares_outstanding"}


@dataclass(frozen=True)
class MetricDefinition:
    key: str
    label: str
    category: str
    kind: str


@dataclass(frozen=True)
class RuntimeOptions:
    safe_mode: bool
    light_mode: bool
    debug_mode: bool


@dataclass(frozen=True)
class BinanceControls:
    symbols: list[str]
    primary_symbol: str
    interval: str
    depth_limit: int
    book_levels: int
    speed_ms: int
    refresh_seconds: int
    microseconds: bool
    market_data_only: bool
    enable_ws_probe: bool


METRIC_DEFINITIONS = [
    MetricDefinition("price", "Price", "Market", "money"),
    MetricDefinition("change", "Change", "Market", "money_signed"),
    MetricDefinition("change_pct", "Change %", "Market", "percent_signed"),
    MetricDefinition("market_cap", "Market Cap", "Market", "large_money"),
    MetricDefinition("enterprise_value", "Enterprise Value", "Market", "large_money"),
    MetricDefinition("beta", "Beta", "Market", "multiple"),
    MetricDefinition("volume", "Volume", "Market", "large_number"),
    MetricDefinition("avg_volume_20", "Avg Volume 20D", "Market", "large_number"),
    MetricDefinition("dollar_volume", "Dollar Volume", "Market", "large_money"),
    MetricDefinition("pe_ratio", "P/E", "Valuation", "multiple"),
    MetricDefinition("forward_pe", "Forward P/E", "Valuation", "multiple"),
    MetricDefinition("pb_ratio", "P/B", "Valuation", "multiple"),
    MetricDefinition("ps_ratio", "P/S", "Valuation", "multiple"),
    MetricDefinition("ev_ebitda", "EV / EBITDA", "Valuation", "multiple"),
    MetricDefinition("dividend_yield", "Dividend Yield", "Valuation", "percent"),
    MetricDefinition("analyst_target", "Analyst Target", "Valuation", "money"),
    MetricDefinition("intrinsic_value", "Intrinsic Value", "DCF", "money"),
    MetricDefinition("fair_value_after_mos", "Fair Value After Margin", "DCF", "money"),
    MetricDefinition("dcf_upside", "DCF Upside", "DCF", "percent_signed"),
    MetricDefinition("revenue", "Revenue", "Financials", "large_money"),
    MetricDefinition("net_income", "Net Income", "Financials", "large_money"),
    MetricDefinition("operating_cash_flow", "Operating Cash Flow", "Financials", "large_money"),
    MetricDefinition("free_cash_flow", "Free Cash Flow", "Financials", "large_money"),
    MetricDefinition("book_value_per_share", "Book Value / Share", "Financials", "money"),
    MetricDefinition("revenue_growth", "Revenue Growth", "Growth", "percent_signed"),
    MetricDefinition("earnings_growth", "Earnings Growth", "Growth", "percent_signed"),
    MetricDefinition("ema_20", "EMA 20", "Trend", "money"),
    MetricDefinition("ema_50", "EMA 50", "Trend", "money"),
    MetricDefinition("sma_200", "SMA 200", "Trend", "money"),
    MetricDefinition("price_vs_sma_200", "Price vs SMA 200", "Trend", "percent_signed"),
    MetricDefinition("trend_score", "Trend Score", "Trend", "score"),
    MetricDefinition("trend_state", "Trend State", "Trend", "text"),
    MetricDefinition("crossover_signal", "EMA Cross Signal", "Trend", "text"),
    MetricDefinition("rsi_14", "RSI 14", "Momentum", "number"),
    MetricDefinition("macd_line", "MACD", "Momentum", "number"),
    MetricDefinition("macd_signal", "MACD Signal", "Momentum", "number"),
    MetricDefinition("macd_hist", "MACD Histogram", "Momentum", "number"),
    MetricDefinition("technical_momentum_score", "Technical Momentum", "Momentum", "score"),
    MetricDefinition("momentum_6m", "6M Momentum", "Momentum", "percent_signed"),
    MetricDefinition("momentum_1y", "1Y Momentum", "Momentum", "percent_signed"),
    MetricDefinition("max_drawdown", "Max Drawdown", "Momentum", "percent_signed"),
    MetricDefinition("volatility_1y", "Volatility 1Y", "Momentum", "percent"),
    MetricDefinition("volume_ratio", "Volume / Avg", "Volume", "multiple"),
    MetricDefinition("vwap_60", "VWAP 60D", "Volume", "money"),
    MetricDefinition("volume_score", "Volume Score", "Volume", "score"),
    MetricDefinition("volume_spike", "Volume Spike", "Volume", "text"),
    MetricDefinition("liquidity_state", "Liquidity State", "Volume", "text"),
    MetricDefinition("support_level", "Support", "Structure", "money"),
    MetricDefinition("resistance_level", "Resistance", "Structure", "money"),
    MetricDefinition("distance_to_support", "Distance to Support", "Structure", "percent_signed"),
    MetricDefinition("distance_to_resistance", "Distance to Resistance", "Structure", "percent_signed"),
    MetricDefinition("breakout_signal", "Breakout / Retest", "Structure", "text"),
    MetricDefinition("range_state", "Range State", "Structure", "text"),
    MetricDefinition("atr_14", "ATR 14", "Risk", "money"),
    MetricDefinition("atr_pct", "ATR %", "Risk", "percent"),
    MetricDefinition("stop_loss", "Suggested Stop", "Risk", "money"),
    MetricDefinition("stop_loss_distance", "Stop Distance", "Risk", "percent"),
    MetricDefinition("risk_reward_to_resistance", "Reward / Risk", "Risk", "multiple"),
    MetricDefinition("position_size", "Position Size", "Risk", "integer"),
    MetricDefinition("risk_score", "Risk Score", "Risk", "score"),
    MetricDefinition("relative_strength_6m", "RS 6M vs Benchmark", "Relative Strength", "percent_signed"),
    MetricDefinition("relative_strength_1y", "RS 1Y vs Benchmark", "Relative Strength", "percent_signed"),
    MetricDefinition("relative_strength_rank", "RS Rank", "Relative Strength", "integer"),
    MetricDefinition("relative_strength_score", "Relative Strength Score", "Relative Strength", "score"),
    MetricDefinition("gross_margin", "Gross Margin", "Quality", "percent"),
    MetricDefinition("operating_margin", "Operating Margin", "Quality", "percent"),
    MetricDefinition("profit_margin", "Profit Margin", "Quality", "percent"),
    MetricDefinition("roe", "ROE", "Quality", "percent"),
    MetricDefinition("roa", "ROA", "Quality", "percent"),
    MetricDefinition("roic", "ROIC", "Quality", "percent"),
    MetricDefinition("current_ratio", "Current Ratio", "Quality", "multiple"),
    MetricDefinition("quick_ratio", "Quick Ratio", "Quality", "multiple"),
    MetricDefinition("debt_to_equity", "Debt / Equity", "Quality", "multiple"),
    MetricDefinition("interest_coverage", "Interest Coverage", "Quality", "multiple"),
    MetricDefinition("value_score", "Value Score", "Summary", "score"),
    MetricDefinition("quality_score", "Quality Score", "Summary", "score"),
    MetricDefinition("growth_score", "Growth Score", "Summary", "score"),
    MetricDefinition("fundamental_score", "Fundamental Score", "Summary", "score"),
    MetricDefinition("momentum_score", "Momentum Score", "Summary", "score"),
    MetricDefinition("overall_score", "Overall Score", "Summary", "score"),
    MetricDefinition("final_ai_score", "Final AI Score", "Summary", "score"),
    MetricDefinition("decision_label", "Decision Label", "Summary", "text"),
    MetricDefinition("alert_count", "Active Alerts", "Summary", "integer"),
    MetricDefinition("overall_verdict", "Overall Verdict", "Summary", "text"),
]
METRIC_LOOKUP = {item.key: item for item in METRIC_DEFINITIONS}
CATEGORY_ORDER = [
    "Market",
    "Trend",
    "Momentum",
    "Volume",
    "Structure",
    "Risk",
    "Relative Strength",
    "Valuation",
    "DCF",
    "Financials",
    "Growth",
    "Quality",
    "Summary",
]
TEXT_METRIC_KEYS = {
    "overall_verdict",
    "trend_state",
    "crossover_signal",
    "volume_spike",
    "liquidity_state",
    "breakout_signal",
    "range_state",
    "decision_label",
}
LOWER_IS_BETTER_METRICS = {
    "pe_ratio",
    "forward_pe",
    "pb_ratio",
    "ps_ratio",
    "ev_ebitda",
    "debt_to_equity",
    "volatility_1y",
    "atr_pct",
    "stop_loss_distance",
}
BENCHMARK_OPTIONS = ["SPY", "QQQ", "^GSPC", "^IXIC"]

BINANCE_WS_BASE_9443 = "wss://stream.binance.com:9443"
BINANCE_WS_BASE_443 = "wss://stream.binance.com:443"
BINANCE_WS_MARKET_DATA_ONLY = "wss://data-stream.binance.vision"
BINANCE_REST_BASE = "https://api.binance.com"
BINANCE_DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
BINANCE_INTERVALS = ["1s", "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"]
BINANCE_DEPTH_LIMITS = [100, 500, 1000, 5000]
BINANCE_BOOK_LEVELS = [5, 10, 20]
BINANCE_SPEED_OPTIONS = [100, 1000]

BINANCE_WARNINGS = [
    "Raw Stream: /ws/<streamName>",
    "Combined Stream: /stream?streams=stream1/stream2/stream3",
    "WebSocket symbols must be lowercase, e.g. btcusdt.",
    "A single connection is automatically disconnected after 24 hours.",
    "Binance sends ping frames every 20 seconds and expects pong within one minute.",
    "One connection supports up to 1024 streams.",
    "Inbound connection messages are limited to 5 messages per second.",
    "Connection attempts are capped at 300 attempts per 5 minutes per IP.",
    "Use data-stream.binance.vision for market data only.",
]

BINANCE_REQUIRED_STREAMS_FOR_BOOKMAP = {
    "depth": "Order Book / Liquidity / Market Depth",
    "trade": "Executed Trades / Time & Sales",
    "kline": "Candlestick Chart",
    "bookTicker": "Best Bid / Best Ask",
}

LOCAL_ORDER_BOOK_STEPS = [
    "Open <symbol>@depth or <symbol>@depth@100ms and buffer events.",
    "Fetch REST snapshot: /api/v3/depth?symbol=SYMBOL&limit=5000.",
    "Drop events where u <= snapshot lastUpdateId.",
    "The first applied event must contain lastUpdateId inside [U, u].",
    "Apply bids/asks: quantity 0 removes a price level, otherwise update/insert.",
    "If U is greater than local update_id + 1, rebuild from snapshot.",
]

BOOKMAP_DASHBOARD_PLAN = {
    "depth": ["Builds liquidity levels", "Feeds bid/ask volume", "Powers liquidity heatmap"],
    "trade": ["Shows executed trades", "Feeds time & sales", "Calculates buy/sell pressure"],
    "kline": ["Draws candles", "Feeds RSI / MACD / EMA", "Gives price context"],
    "bookTicker": ["Best bid/ask", "Spread monitoring", "Fast microstructure signal"],
}

TRADE_FIELDS = {
    "e": "event type",
    "E": "event time",
    "s": "symbol",
    "t": "trade id",
    "p": "price",
    "q": "quantity",
    "T": "trade time",
    "m": "buyer is market maker",
}

KLINE_FIELDS = {
    "e": "event type",
    "E": "event time",
    "s": "symbol",
    "k": "kline object",
    "o": "open",
    "c": "close",
    "h": "high",
    "l": "low",
    "v": "base volume",
    "n": "number of trades",
    "x": "closed candle",
    "q": "quote volume",
}

DEPTH_FIELDS = {
    "e": "event type",
    "E": "event time",
    "s": "symbol",
    "U": "first update id",
    "u": "final update id",
    "b": "bid updates [price, quantity]",
    "a": "ask updates [price, quantity]",
}

BOOK_TICKER_FIELDS = {
    "u": "order book update id",
    "s": "symbol",
    "b": "best bid price",
    "B": "best bid quantity",
    "a": "best ask price",
    "A": "best ask quantity",
}


def provider_label(name: str | None) -> str:
    if not name:
        return "Unknown"
    if str(name).startswith("calculated vs "):
        return str(name).replace("calculated", "Calculated", 1)
    return PROVIDER_LABELS.get(name, str(name).replace("_", " ").title())


def is_value_present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (int, float)):
        try:
            return not math.isnan(float(value))
        except (TypeError, ValueError):
            return False
    return True


def unwrap_scalar(value: Any) -> Any:
    if isinstance(value, dict):
        if "raw" in value and not isinstance(value["raw"], (dict, list)):
            return value["raw"]
        if "value" in value and not isinstance(value["value"], (dict, list)):
            return value["value"]
        if "reportedValue" in value and isinstance(value["reportedValue"], dict):
            inner = value["reportedValue"].get("raw") or value["reportedValue"].get("amount")
            if inner is not None:
                return inner
        if "fmt" in value and not isinstance(value["fmt"], (dict, list)):
            return value["fmt"]
        if "amount" in value and not isinstance(value["amount"], (dict, list)):
            return value["amount"]
    return value


def to_num(value: Any, default: float | None = None) -> float | None:
    value = unwrap_scalar(value)
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        number = float(value)
        if math.isnan(number) or math.isinf(number):
            return default
        return number
    text = str(value).strip()
    if not text or text.lower() in {"none", "null", "nan", "n/a", "-", "na"}:
        return default
    negative = False
    if text.startswith("(") and text.endswith(")"):
        negative = True
        text = text[1:-1]
    is_percent = text.endswith("%")
    text = re.sub(r"[^0-9.\-]", "", text)
    if text in {"", "-", ".", "-."}:
        return default
    try:
        number = float(text)
    except ValueError:
        return default
    if negative:
        number *= -1
    if is_percent:
        number /= 100.0
    return number


def is_num(value: Any) -> bool:
    return value is not None and isinstance(value, (int, float)) and not math.isnan(float(value)) and not math.isinf(float(value))


def percent_field(value: Any) -> float | None:
    number = to_num(value)
    if not is_num(number):
        return None
    if abs(number) > 1.5:
        return number / 100.0
    return number


def safe_div(a: Any, b: Any) -> float | None:
    a_num = to_num(a)
    b_num = to_num(b)
    if not is_num(a_num) or not is_num(b_num) or b_num == 0:
        return None
    return a_num / b_num


def avg(*values: Any) -> float | None:
    nums = [to_num(v) for v in values if is_num(to_num(v))]
    if not nums:
        return None
    return sum(nums) / len(nums)


def sum_numbers(*values: Any) -> float | None:
    nums = [to_num(v) for v in values if is_num(to_num(v))]
    if not nums:
        return None
    return sum(nums)


def coalesce_num(*values: Any) -> float | None:
    for value in values:
        number = to_num(value)
        if is_num(number):
            return number
    return None


def abs_num(value: Any) -> float | None:
    number = to_num(value)
    if not is_num(number):
        return None
    return abs(number)


def clamp(value: Any, min_value: float, max_value: float) -> float | None:
    number = to_num(value)
    if not is_num(number):
        return None
    return max(min_value, min(max_value, number))


def normalize_key(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(text or "").lower())


def flatten_map(obj: Any, out: dict[str, list[Any]]) -> None:
    if obj is None:
        return
    if isinstance(obj, list):
        for item in obj[:12]:
            flatten_map(item, out)
        return
    if isinstance(obj, dict):
        for key, value in obj.items():
            normalized = normalize_key(key)
            scalar = unwrap_scalar(value)
            out.setdefault(normalized, [])
            if not isinstance(scalar, (dict, list)) and scalar not in (None, ""):
                out[normalized].append(scalar)
            flatten_map(value, out)


def get_field_from_object(obj: Any, keys: list[str], number_mode: bool = False) -> Any:
    if not isinstance(obj, dict):
        return None
    mapping: dict[str, list[Any]] = {}
    flatten_map(obj, mapping)
    for key in keys:
        normalized = normalize_key(key)
        if mapping.get(normalized):
            value = mapping[normalized][0]
            return to_num(value) if number_mode else value
    existing_keys = list(mapping.keys())
    for key in keys:
        normalized = normalize_key(key)
        for existing in existing_keys:
            if (normalized in existing or existing in normalized) and mapping.get(existing):
                value = mapping[existing][0]
                return to_num(value) if number_mode else value
    return None


def get_num_from_many(objects: list[Any], keys: list[str]) -> float | None:
    for obj in objects:
        value = get_field_from_object(obj, keys, number_mode=True)
        if is_num(value):
            return value
    return None


def get_str_from_many(objects: list[Any], keys: list[str]) -> str:
    for obj in objects:
        value = get_field_from_object(obj, keys, number_mode=False)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if value not in (None, ""):
            return str(value)
    return ""


def first_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, list) and value and isinstance(value[0], dict):
        return value[0]
    if isinstance(value, dict):
        return value
    return {}


def normalize_envelope(payload: Any) -> Any:
    if isinstance(payload, dict):
        data = payload.get("data")
        result = payload.get("result")
        if isinstance(data, dict):
            return data
        if isinstance(result, dict):
            return result
    return payload or {}


def extract_records(payload: Any) -> list[dict[str, Any]]:
    if payload is None:
        return []
    if isinstance(payload, list):
        records = [item for item in payload if isinstance(item, dict)]
    elif isinstance(payload, dict):
        source = payload.get("data") or payload.get("result") or payload
        if isinstance(source, list):
            records = [item for item in source if isinstance(item, dict)]
        else:
            records = []
            for key in [
                "income_statement",
                "balance_sheet",
                "cash_flow",
                "annualReports",
                "quarterlyReports",
                "values",
                "historical",
                "results",
                "incomeStatementHistory",
                "balanceSheetHistory",
                "cashflowStatementHistory",
            ]:
                candidate = source.get(key) if isinstance(source, dict) else None
                if isinstance(candidate, list):
                    records = [item for item in candidate if isinstance(item, dict)]
                    break
    else:
        records = []
    return sorted(records, key=date_score, reverse=True)


def get_date_from_record(record: dict[str, Any]) -> datetime | None:
    for key in [
        "fiscalDateEnding",
        "fiscal_date",
        "end_date",
        "start_date",
        "date",
        "datetime",
        "period_ending",
        "calendarDate",
        "filing_date",
    ]:
        value = get_field_from_object(record, [key], number_mode=False)
        if not value:
            continue
        text = str(value).replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            try:
                return datetime.strptime(text[:10], "%Y-%m-%d")
            except ValueError:
                continue
    return None


def date_score(record: dict[str, Any]) -> float:
    found = get_date_from_record(record)
    if not found:
        return 0
    return found.timestamp()


def record_year(record: dict[str, Any]) -> str:
    found = get_date_from_record(record)
    if found:
        return str(found.year)
    for key in ["fiscalYear", "year", "calendarYear"]:
        value = get_field_from_object(record, [key], number_mode=False)
        if value:
            return str(value)[:4]
    return "—"


def extract_history_rows(payload: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if isinstance(payload, list):
        source = payload
    elif isinstance(payload, dict):
        source = payload.get("values") or payload.get("results") or payload.get("data", {}).get("values") or []
    else:
        source = []
    if not isinstance(source, list):
        return rows
    for item in source:
        if not isinstance(item, dict):
            continue
        date_value = item.get("datetime") or item.get("date") or item.get("formatted_date")
        close_value = to_num(item.get("close") or item.get("c"))
        if date_value and is_num(close_value):
            row = {"date": str(date_value)[:10], "close": close_value}
            high_value = to_num(item.get("high") or item.get("h"))
            low_value = to_num(item.get("low") or item.get("l"))
            volume_value = to_num(item.get("volume") or item.get("v"))
            if is_num(high_value):
                row["high"] = high_value
            if is_num(low_value):
                row["low"] = low_value
            if is_num(volume_value):
                row["volume"] = volume_value
            rows.append(row)
    rows.sort(key=lambda row: row["date"])
    return rows


def sum_dividends_last_year(payload: Any) -> float | None:
    rows = extract_records(payload)
    if not rows:
        return None
    cutoff = datetime.now() - timedelta(days=365)
    total = 0.0
    found = False
    for row in rows:
        row_date = get_date_from_record(row)
        amount = get_num_from_many([row], ["amount", "dividend", "cashAmount", "value"])
        if row_date and row_date >= cutoff and is_num(amount):
            total += amount
            found = True
    return total if found else None


def merge_annual_series(income_records: list[dict[str, Any]], cash_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    years: dict[str, dict[str, Any]] = {}
    for row in income_records[:5]:
        year = record_year(row)
        item = years.setdefault(year, {"year": year})
        item["revenue"] = coalesce_num(get_num_from_many([row], ["revenue", "totalRevenue", "sales"]))
        item["gross_profit"] = coalesce_num(get_num_from_many([row], ["grossProfit", "gross_profit"]))
        item["operating_income"] = coalesce_num(get_num_from_many([row], ["operatingIncome", "operating_income", "ebit"]))
        item["net_income"] = coalesce_num(get_num_from_many([row], ["netIncome", "net_income", "netIncomeLoss"]))
        item["ebitda"] = coalesce_num(get_num_from_many([row], ["ebitda"]))
    for row in cash_records[:5]:
        year = record_year(row)
        item = years.setdefault(year, {"year": year})
        ocf = coalesce_num(get_num_from_many([row], ["operatingCashFlow", "operatingCashflow", "cash_flow_from_operations"]))
        capex = abs_num(get_num_from_many([row], ["capitalExpenditure", "capitalExpenditures", "capital_expenditure"]))
        fcf = coalesce_num(get_num_from_many([row], ["freeCashFlow", "free_cash_flow", "levered_free_cash_flow_ttm"]))
        if not is_num(fcf) and is_num(ocf) and is_num(capex):
            fcf = ocf - capex
        item["operating_cash_flow"] = ocf
        item["capital_expenditure"] = capex
        item["free_cash_flow"] = fcf
    output = list(years.values())
    output.sort(key=lambda row: row["year"])
    return output


def merge_polygon_annual_series(financial_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in financial_records[:5]:
        if not isinstance(record, dict):
            continue
        financials = record.get("financials", {})
        year = record.get("fiscal_year") or record_year(record)
        income = financials.get("income_statement", {}) if isinstance(financials, dict) else {}
        cash = financials.get("cash_flow_statement", {}) if isinstance(financials, dict) else {}
        rows.append(
            {
                "year": str(year),
                "revenue": get_num_from_many([income], ["revenues", "revenue", "sales"]),
                "gross_profit": get_num_from_many([income], ["gross_profit"]),
                "operating_income": get_num_from_many([income], ["operating_income_loss", "operating_income"]),
                "net_income": get_num_from_many([income], ["net_income_loss", "netincomeloss"]),
                "operating_cash_flow": get_num_from_many([cash], ["net_cash_flow_from_operating_activities", "operatingcashflow"]),
                "capital_expenditure": abs_num(get_num_from_many([cash], ["capital_expenditures", "capitalexpenditure"])),
                "free_cash_flow": None,
            }
        )
        if is_num(rows[-1]["operating_cash_flow"]) and is_num(rows[-1]["capital_expenditure"]):
            rows[-1]["free_cash_flow"] = rows[-1]["operating_cash_flow"] - rows[-1]["capital_expenditure"]
    rows.sort(key=lambda row: row["year"])
    return rows


def parse_symbols(text: str) -> list[str]:
    raw = re.split(r"[,;\s]+", text.upper().strip())
    cleaned: list[str] = []
    for item in raw:
        if not item:
            continue
        item = re.sub(r"[^A-Z0-9.\-_=^/]", "", item)
        if item and item not in cleaned:
            cleaned.append(item)
    return cleaned[:MAX_COMPARE_SYMBOLS]


def parse_binance_symbols(text: str) -> list[str]:
    raw = re.split(r"[,;\s]+", text.upper().strip())
    cleaned: list[str] = []
    for item in raw:
        symbol = normalize_symbol_rest(item)
        if symbol and symbol not in cleaned:
            cleaned.append(symbol)
    return cleaned[:MAX_COMPARE_SYMBOLS]


def normalize_symbol(symbol: str) -> str:
    if not symbol:
        raise ValueError("symbol is required")
    return symbol.replace("/", "").replace("-", "").replace("_", "").lower().strip()


def normalize_symbol_rest(symbol: str) -> str:
    if not symbol:
        return ""
    return symbol.replace("/", "").replace("-", "").replace("_", "").upper().strip()


def raw_stream_url(stream_name: str, base: str = BINANCE_WS_BASE_9443) -> str:
    return f"{base}/ws/{stream_name}"


def combined_stream_url(streams: list[str], base: str = BINANCE_WS_BASE_9443, microseconds: bool = False) -> str:
    if not streams:
        raise ValueError("streams list is empty")
    url = f"{base}/stream?streams={'/'.join(streams)}"
    if microseconds:
        url += "&timeUnit=MICROSECOND"
    return url


def make_binance_rest_url(path: str, params: dict[str, Any] | None = None) -> str:
    if params:
        return f"{BINANCE_REST_BASE}{path}?{urlencode(params)}"
    return f"{BINANCE_REST_BASE}{path}"


def trade_stream(symbol: str) -> str:
    return f"{normalize_symbol(symbol)}@trade"


def aggregate_trade_stream(symbol: str) -> str:
    return f"{normalize_symbol(symbol)}@aggTrade"


def kline_stream(symbol: str, interval: str = "1m") -> str:
    return f"{normalize_symbol(symbol)}@kline_{interval}"


def kline_stream_utc8(symbol: str, interval: str = "1m") -> str:
    return f"{normalize_symbol(symbol)}@kline_{interval}@+08:00"


def mini_ticker_stream(symbol: str) -> str:
    return f"{normalize_symbol(symbol)}@miniTicker"


def all_market_mini_tickers_stream() -> str:
    return "!miniTicker@arr"


def ticker_stream(symbol: str) -> str:
    return f"{normalize_symbol(symbol)}@ticker"


def all_market_tickers_stream() -> str:
    return "!ticker@arr"


def rolling_ticker_stream(symbol: str, window_size: str = "1h") -> str:
    return f"{normalize_symbol(symbol)}@ticker_{window_size}"


def all_market_rolling_ticker_stream(window_size: str = "1h") -> str:
    return f"!ticker_{window_size}@arr"


def book_ticker_stream(symbol: str) -> str:
    return f"{normalize_symbol(symbol)}@bookTicker"


def avg_price_stream(symbol: str) -> str:
    return f"{normalize_symbol(symbol)}@avgPrice"


def partial_book_depth_stream(symbol: str, levels: int = 20, speed_ms: int = 1000) -> str:
    if levels not in [5, 10, 20]:
        raise ValueError("levels must be 5, 10, or 20")
    if speed_ms not in [100, 1000]:
        raise ValueError("speed_ms must be 100 or 1000")
    suffix = "@100ms" if speed_ms == 100 else ""
    return f"{normalize_symbol(symbol)}@depth{levels}{suffix}"


def diff_depth_stream(symbol: str, speed_ms: int = 1000) -> str:
    if speed_ms not in [100, 1000]:
        raise ValueError("speed_ms must be 100 or 1000")
    suffix = "@100ms" if speed_ms == 100 else ""
    return f"{normalize_symbol(symbol)}@depth{suffix}"


def bookmap_like_streams(symbol: str, interval: str = "1m", speed_ms: int = 100) -> list[str]:
    return [
        diff_depth_stream(symbol, speed_ms=speed_ms),
        trade_stream(symbol),
        kline_stream(symbol, interval),
        book_ticker_stream(symbol),
    ]


def bookmap_like_combined_url(symbol: str = "BTCUSDT", interval: str = "1m", speed_ms: int = 100, microseconds: bool = False) -> str:
    return combined_stream_url(bookmap_like_streams(symbol, interval, speed_ms), microseconds=microseconds)


def multi_symbol_bookmap_streams(symbols: list[str], interval: str = "1m", speed_ms: int = 100) -> list[str]:
    streams: list[str] = []
    for symbol in symbols:
        streams.extend(bookmap_like_streams(symbol, interval, speed_ms))
    return streams


def multi_symbol_combined_url(symbols: list[str], interval: str = "1m", speed_ms: int = 100, microseconds: bool = False) -> str:
    return combined_stream_url(multi_symbol_bookmap_streams(symbols, interval, speed_ms), microseconds=microseconds)


def simple_chart_streams(symbol: str, interval: str = "1m") -> list[str]:
    return [kline_stream(symbol, interval), ticker_stream(symbol), book_ticker_stream(symbol)]


def simple_chart_combined_url(symbol: str, interval: str = "1m") -> str:
    return combined_stream_url(simple_chart_streams(symbol, interval))


def subscribe_message(streams: list[str], request_id: int = 1) -> dict[str, Any]:
    return {"method": "SUBSCRIBE", "params": streams, "id": request_id}


def unsubscribe_message(streams: list[str], request_id: int = 2) -> dict[str, Any]:
    return {"method": "UNSUBSCRIBE", "params": streams, "id": request_id}


def list_subscriptions_message(request_id: int = 3) -> dict[str, Any]:
    return {"method": "LIST_SUBSCRIPTIONS", "id": request_id}


def set_combined_property_message(enabled: bool = True, request_id: int = 4) -> dict[str, Any]:
    return {"method": "SET_PROPERTY", "params": ["combined", enabled], "id": request_id}


def get_combined_property_message(request_id: int = 5) -> dict[str, Any]:
    return {"method": "GET_PROPERTY", "params": ["combined"], "id": request_id}


def ping_rest_url() -> str:
    return make_binance_rest_url("/api/v3/ping")


def server_time_rest_url() -> str:
    return make_binance_rest_url("/api/v3/time")


def exchange_info_rest_url(symbol: str | None = None) -> str:
    params = {"symbol": normalize_symbol_rest(symbol)} if symbol else None
    return make_binance_rest_url("/api/v3/exchangeInfo", params)


def depth_snapshot_rest_url(symbol: str, limit: int = 5000) -> str:
    return make_binance_rest_url("/api/v3/depth", {"symbol": normalize_symbol_rest(symbol), "limit": limit})


def recent_trades_rest_url(symbol: str, limit: int = 500) -> str:
    return make_binance_rest_url("/api/v3/trades", {"symbol": normalize_symbol_rest(symbol), "limit": limit})


def agg_trades_rest_url(symbol: str, limit: int = 500) -> str:
    return make_binance_rest_url("/api/v3/aggTrades", {"symbol": normalize_symbol_rest(symbol), "limit": limit})


def klines_rest_url(symbol: str, interval: str = "1m", limit: int = 500) -> str:
    return make_binance_rest_url("/api/v3/klines", {"symbol": normalize_symbol_rest(symbol), "interval": interval, "limit": limit})


def avg_price_rest_url(symbol: str) -> str:
    return make_binance_rest_url("/api/v3/avgPrice", {"symbol": normalize_symbol_rest(symbol)})


def ticker_24hr_rest_url(symbol: str | None = None) -> str:
    params = {"symbol": normalize_symbol_rest(symbol)} if symbol else None
    return make_binance_rest_url("/api/v3/ticker/24hr", params)


def ticker_price_rest_url(symbol: str | None = None) -> str:
    params = {"symbol": normalize_symbol_rest(symbol)} if symbol else None
    return make_binance_rest_url("/api/v3/ticker/price", params)


def book_ticker_rest_url(symbol: str | None = None) -> str:
    params = {"symbol": normalize_symbol_rest(symbol)} if symbol else None
    return make_binance_rest_url("/api/v3/ticker/bookTicker", params)


def safe_subtract(a: Any, b: Any) -> float | None:
    a_num = to_num(a)
    b_num = to_num(b)
    if not is_num(a_num) or not is_num(b_num):
        return None
    return a_num - b_num


def infer_provider_kind_error(message: str, provider: str, symbol: str) -> str:
    text = str(message or "").strip()
    lowered = text.lower()
    if not text:
        return "Unknown error"
    if "api credits" in lowered or "current minute" in lowered or "quota" in lowered:
        return f"{provider_label(provider)} hit a credit or quota limit for {symbol}."
    if "429" in lowered or "rate limit" in lowered or "too many" in lowered:
        return f"{provider_label(provider)} is rate-limiting requests for {symbol}."
    if "missing api key" in lowered:
        return f"{provider_label(provider)} API key is missing."
    if "not entitled" in lowered or "not authorized" in lowered or "premium" in lowered or "plan" in lowered or "active subscription" in lowered:
        return f"{provider_label(provider)} denied this endpoint on the current plan."
    if "invalid api key" in lowered or "forbidden" in lowered or "unauthorized" in lowered:
        return f"{provider_label(provider)} rejected the API key or account permissions."
    if "symbol" in lowered and ("not found" in lowered or "invalid" in lowered or "unsupported" in lowered):
        return f"{provider_label(provider)} does not support {symbol} or returned no data for it."
    if "non-json" in lowered:
        return f"{provider_label(provider)} returned an unexpected response format."
    return text


def summarize_error(errors: dict[str, str], provider: str, symbol: str) -> str:
    if not errors:
        return "—"
    first = next(iter(errors.values()))
    return infer_provider_kind_error(first, provider, symbol)


def likely_non_us_symbol(symbol: str) -> bool:
    return "." in symbol and not symbol.endswith((".A", ".B"))


def latest_history_date(history: list[dict[str, Any]]) -> str:
    if not history:
        return "—"
    return str(history[-1].get("date") or "—")


def latest_annual_label(annuals: list[dict[str, Any]]) -> str:
    if not annuals:
        return "—"
    return str(annuals[-1].get("year") or "—")


def datetime_from_unix(value: Any) -> str:
    stamp = to_num(value)
    if not is_num(stamp):
        return ""
    if stamp > 1e12:
        stamp = stamp / 1000.0
    try:
        return datetime.fromtimestamp(stamp, tz=timezone.utc).date().isoformat()
    except (OverflowError, OSError, ValueError):
        return ""


@st.cache_resource(show_spinner=False)
def get_http_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        read=3,
        connect=3,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(HTTP_HEADERS)
    return session


def throttle_provider(provider: str, extra_delay: float = 0.0) -> None:
    wait_seconds = max(PROVIDER_THROTTLE_SECONDS.get(provider, 0.0), extra_delay)
    if wait_seconds <= 0:
        return
    with PROVIDER_LOCK:
        now = time.monotonic()
        previous = PROVIDER_LAST_CALL.get(provider, 0.0)
        sleep_for = wait_seconds - (now - previous)
        if sleep_for > 0:
            time.sleep(sleep_for)
        PROVIDER_LAST_CALL[provider] = time.monotonic()


def extract_api_error(payload: Any, status_code: int, provider: str, symbol: str) -> str | None:
    text = ""
    if isinstance(payload, (dict, list)):
        try:
            text = json.dumps(payload)[:600].lower()
        except Exception:  # noqa: BLE001
            text = str(payload).lower()
    if isinstance(payload, dict):
        if provider == "yahoo":
            chart_error = ((payload.get("chart") or {}).get("error")) if isinstance(payload.get("chart"), dict) else None
            summary_error = ((payload.get("quoteSummary") or {}).get("error")) if isinstance(payload.get("quoteSummary"), dict) else None
            if chart_error:
                return str(chart_error.get("description") or chart_error.get("code") or "Yahoo error")
            if summary_error:
                return str(summary_error.get("description") or summary_error.get("code") or "Yahoo error")
        if provider == "polygon":
            status_text = str(payload.get("status", "")).strip().lower()
            if status_text in {"not_authorized", "error"}:
                return str(payload.get("message") or payload.get("status"))
        if provider == "twelve_data":
            code_value = to_num(payload.get("code"))
            if code_value and code_value >= 400:
                return str(payload.get("message") or payload.get("status") or payload.get("code"))
        if payload.get("Error Message"):
            return str(payload["Error Message"])
        if payload.get("error"):
            return str(payload["error"])
        if payload.get("human"):
            return str(payload.get("human"))
        if payload.get("message"):
            return str(payload.get("message"))
        if payload.get("Note"):
            return str(payload["Note"])
        if payload.get("Information"):
            return str(payload["Information"])
        status_text = str(payload.get("status", "")).strip().lower()
        if status_text in {"error", "failed", "not_authorized", "not authorized"}:
            return str(payload.get("message") or payload.get("status"))
        if status_code >= 400:
            return str(payload.get("message") or payload.get("status") or f"HTTP {status_code}")
        if "code" in payload and payload.get("message") and to_num(payload.get("code")) and to_num(payload.get("code")) >= 400:
            return str(payload["message"])
        if provider == "finnhub" and payload == {}:
            return f"Finnhub returned an empty payload for {symbol}"
    if status_code >= 400:
        if provider == "yahoo" and status_code == 429:
            return "Yahoo Finance rate limit (HTTP 429)"
        return f"HTTP {status_code}"
    if provider == "alpha_vantage" and "thank you for using alpha vantage" in text:
        return "Alpha Vantage rate limit or daily quota reached"
    if provider == "polygon" and "not entitled" in text:
        return "Polygon plan restriction for this endpoint"
    return None


@st.cache_data(ttl=TTL_SECONDS, show_spinner=False)
def cached_get_json(provider: str, symbol: str, url: str, params_items: tuple[tuple[str, str], ...], refresh_token: int, timeout: int) -> Any:
    del refresh_token
    throttle_provider(provider)
    params = {key: value for key, value in params_items}
    response = get_http_session().get(url, params=params, timeout=timeout)
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Non-JSON response from {url}") from exc
    error_message = extract_api_error(payload, response.status_code, provider, symbol)
    if error_message:
        raise RuntimeError(error_message)
    return payload


def fetch_json(provider: str, symbol: str, url: str, params: dict[str, Any], refresh_token: int, timeout: int | None = None) -> Any:
    serialized = tuple(sorted((str(key), str(value)) for key, value in params.items() if value not in (None, "")))
    return cached_get_json(provider, symbol, url, serialized, refresh_token, timeout or PROVIDER_TIMEOUTS.get(provider, 25))


def binance_refresh_bucket(refresh_seconds: int) -> int:
    seconds = max(1, int(refresh_seconds or 1))
    return int(time.time() // seconds)


def binance_controls_cache_key(controls: BinanceControls) -> tuple[Any, ...]:
    return (
        tuple(controls.symbols),
        controls.interval,
        controls.depth_limit,
        controls.book_levels,
        controls.speed_ms,
        controls.refresh_seconds,
        controls.microseconds,
        controls.market_data_only,
        controls.enable_ws_probe,
    )


def request_binance_rest(path: str, params: dict[str, Any] | None = None, timeout: int = 8) -> Any:
    response = get_http_session().get(f"{BINANCE_REST_BASE}{path}", params=params, timeout=timeout)
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Binance returned non-JSON data for {path}") from exc
    if response.status_code >= 400:
        message = payload.get("msg") if isinstance(payload, dict) else str(payload)
        raise RuntimeError(f"Binance HTTP {response.status_code}: {message}")
    if isinstance(payload, dict) and payload.get("code") and payload.get("msg"):
        raise RuntimeError(f"Binance API error {payload.get('code')}: {payload.get('msg')}")
    return payload


@st.cache_data(ttl=1, show_spinner=False)
def fetch_depth(symbol: str, depth_limit: int, refresh_bucket: int, cache_salt: tuple[Any, ...]) -> Any:
    del refresh_bucket, cache_salt
    return request_binance_rest("/api/v3/depth", {"symbol": normalize_symbol_rest(symbol), "limit": int(depth_limit)}, timeout=5)


@st.cache_data(ttl=1, show_spinner=False)
def fetch_trades(symbol: str, refresh_bucket: int, cache_salt: tuple[Any, ...]) -> Any:
    del refresh_bucket, cache_salt
    return request_binance_rest("/api/v3/trades", {"symbol": normalize_symbol_rest(symbol), "limit": 500}, timeout=5)


@st.cache_data(ttl=1, show_spinner=False)
def fetch_agg_trades(symbol: str, refresh_bucket: int, cache_salt: tuple[Any, ...]) -> Any:
    del refresh_bucket, cache_salt
    return request_binance_rest("/api/v3/aggTrades", {"symbol": normalize_symbol_rest(symbol), "limit": 500}, timeout=5)


@st.cache_data(ttl=1, show_spinner=False)
def fetch_ticker(symbol: str, refresh_bucket: int, cache_salt: tuple[Any, ...]) -> Any:
    del refresh_bucket, cache_salt
    normalized = normalize_symbol_rest(symbol)
    return {
        "ticker_24hr": request_binance_rest("/api/v3/ticker/24hr", {"symbol": normalized}, timeout=5),
        "price": request_binance_rest("/api/v3/ticker/price", {"symbol": normalized}, timeout=5),
        "avg_price": request_binance_rest("/api/v3/avgPrice", {"symbol": normalized}, timeout=5),
    }


@st.cache_data(ttl=1, show_spinner=False)
def fetch_book_ticker(symbol: str, refresh_bucket: int, cache_salt: tuple[Any, ...]) -> Any:
    del refresh_bucket, cache_salt
    return request_binance_rest("/api/v3/ticker/bookTicker", {"symbol": normalize_symbol_rest(symbol)}, timeout=5)


@st.cache_data(ttl=3, show_spinner=False)
def fetch_klines(symbol: str, interval: str, refresh_bucket: int, cache_salt: tuple[Any, ...]) -> Any:
    del refresh_bucket, cache_salt
    return request_binance_rest("/api/v3/klines", {"symbol": normalize_symbol_rest(symbol), "interval": interval, "limit": 500}, timeout=6)


@st.cache_data(ttl=300, show_spinner=False)
def fetch_exchange_info(symbol: str, cache_salt: tuple[Any, ...]) -> Any:
    del cache_salt
    return request_binance_rest("/api/v3/exchangeInfo", {"symbol": normalize_symbol_rest(symbol)}, timeout=8)


def run_request_batch(
    provider: str,
    symbol: str,
    batch: list[tuple[str, str, dict[str, Any], int | None]],
    refresh_token: int,
    max_workers: int | None = None,
    sequential: bool = False,
) -> tuple[dict[str, Any], dict[str, str]]:
    data: dict[str, Any] = {}
    errors: dict[str, str] = {}
    if not batch:
        return data, errors
    if sequential or (max_workers or 1) == 1:
        for name, url, params, timeout in batch:
            try:
                data[name] = fetch_json(provider, symbol, url, params, refresh_token, timeout)
            except Exception as exc:  # noqa: BLE001
                errors[name] = str(exc)
        return data, errors
    workers = min(max_workers or PROVIDER_DEFAULT_WORKERS.get(provider, 2), len(batch))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(fetch_json, provider, symbol, url, params, refresh_token, timeout): name
            for name, url, params, timeout in batch
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                data[name] = future.result()
            except Exception as exc:  # noqa: BLE001
                errors[name] = str(exc)
    return data, errors


def make_source(
    provider: str,
    symbol: str,
    raw: dict[str, Any],
    errors: dict[str, str],
    metrics: dict[str, Any],
    texts: dict[str, Any],
    history: list[dict[str, Any]],
    annuals: list[dict[str, Any]],
    news: list[dict[str, Any]] | None = None,
    recommendations: list[dict[str, Any]] | None = None,
    notes: list[str] | None = None,
) -> dict[str, Any]:
    coverage = sum(1 for value in metrics.values() if is_value_present(value))
    has_partial_content = bool(texts or history or annuals or news or recommendations)
    if coverage == 0 and errors and not has_partial_content:
        status = "error"
    elif errors:
        status = "partial"
    else:
        status = "ok"
    return {
        "provider": provider,
        "raw": raw,
        "errors": errors,
        "metrics": metrics,
        "texts": texts,
        "history": history,
        "annuals": annuals,
        "news": news or [],
        "recommendations": recommendations or [],
        "notes": notes or [],
        "status": status,
        "coverage": coverage,
        "error_count": len(errors),
        "freshness": latest_history_date(history) if history else latest_annual_label(annuals),
        "summary_error": summarize_error(errors, provider, symbol),
    }


def disabled_source(provider: str) -> dict[str, Any]:
    return {
        "provider": provider,
        "raw": {},
        "errors": {},
        "metrics": {},
        "texts": {},
        "history": [],
        "annuals": [],
        "news": [],
        "recommendations": [],
        "notes": [],
        "status": "disabled",
        "coverage": 0,
        "error_count": 0,
        "freshness": "—",
        "summary_error": "Disabled by user",
    }


def fetch_fmp_source(symbol: str, keys: dict[str, str], refresh_token: int, options: RuntimeOptions | None = None) -> dict[str, Any]:
    options = options or RuntimeOptions(False, False, False)
    api_key = keys.get("fmp", "").strip()
    if not api_key:
        return make_source("fmp", symbol, {}, {"api": "Missing API key"}, {}, {}, [], [])
    base = "https://financialmodelingprep.com/stable"
    batch = [
        ("quote", f"{base}/quote", {"symbol": symbol, "apikey": api_key}, None),
        ("profile", f"{base}/profile", {"symbol": symbol, "apikey": api_key}, None),
        ("key_metrics_ttm", f"{base}/key-metrics-ttm", {"symbol": symbol, "apikey": api_key}, None),
        ("ratios_ttm", f"{base}/ratios-ttm", {"symbol": symbol, "apikey": api_key}, None),
        ("history", f"{base}/historical-price-eod/full", {"symbol": symbol, "apikey": api_key}, None),
        ("dcf", f"{base}/discounted-cash-flow", {"symbol": symbol, "apikey": api_key}, None),
        ("ratings", f"{base}/ratings-snapshot", {"symbol": symbol, "apikey": api_key}, None),
    ]
    if not options.light_mode:
        batch.extend(
            [
                ("growth", f"{base}/financial-growth", {"symbol": symbol, "limit": 4, "apikey": api_key}, None),
                ("income", f"{base}/income-statement", {"symbol": symbol, "period": "annual", "limit": 5, "apikey": api_key}, None),
                ("cash", f"{base}/cash-flow-statement", {"symbol": symbol, "period": "annual", "limit": 5, "apikey": api_key}, None),
                ("balance", f"{base}/balance-sheet-statement", {"symbol": symbol, "period": "annual", "limit": 5, "apikey": api_key}, None),
                ("analyst_estimates", f"{base}/analyst-estimates", {"symbol": symbol, "period": "annual", "limit": 2, "apikey": api_key}, None),
            ]
        )
    raw, errors = run_request_batch("fmp", symbol, batch, refresh_token, max_workers=2 if options.safe_mode else 6, sequential=options.safe_mode)

    quote = first_dict(raw.get("quote"))
    profile = first_dict(raw.get("profile"))
    key_metrics = first_dict(raw.get("key_metrics_ttm"))
    ratios = first_dict(raw.get("ratios_ttm"))
    growth = first_dict(raw.get("growth"))
    income_records = extract_records(raw.get("income"))
    cash_records = extract_records(raw.get("cash"))
    balance_records = extract_records(raw.get("balance"))
    dcf_obj = first_dict(raw.get("dcf"))
    rating_obj = first_dict(raw.get("ratings"))
    analyst = first_dict(raw.get("analyst_estimates"))
    income0 = income_records[0] if income_records else {}
    income1 = income_records[1] if len(income_records) > 1 else {}
    cash0 = cash_records[0] if cash_records else {}
    balance0 = balance_records[0] if balance_records else {}
    history_raw = raw.get("history") if isinstance(raw.get("history"), list) else []
    history = []
    history_raw = sorted([row for row in history_raw if isinstance(row, dict)], key=lambda row: str(row.get("date", "")))
    for row in history_raw[-260:]:
        close_value = to_num(row.get("close"))
        if row.get("date") and is_num(close_value):
            history_row = {"date": str(row["date"])[:10], "close": close_value}
            high_value = to_num(row.get("high"))
            low_value = to_num(row.get("low"))
            volume_value = to_num(row.get("volume"))
            if is_num(high_value):
                history_row["high"] = high_value
            if is_num(low_value):
                history_row["low"] = low_value
            if is_num(volume_value):
                history_row["volume"] = volume_value
            history.append(history_row)
    history.sort(key=lambda row: row["date"])
    annuals = merge_annual_series(income_records, cash_records)

    price = coalesce_num(quote.get("price"), profile.get("price"))
    previous_close = coalesce_num(quote.get("previousClose"), price - to_num(quote.get("change")) if is_num(price) else None)
    shares = coalesce_num(
        profile.get("sharesOutstanding"),
        quote.get("sharesOutstanding"),
        get_num_from_many([key_metrics, income0], ["weightedAverageShsOutDil", "weightedAverageShsOut", "sharesOutstanding"]),
    )
    revenue = coalesce_num(income0.get("revenue"), income0.get("totalRevenue"))
    gross_profit = coalesce_num(income0.get("grossProfit"))
    operating_income = coalesce_num(income0.get("operatingIncome"), income0.get("ebit"))
    net_income = coalesce_num(income0.get("netIncome"), income0.get("bottomLineNetIncome"))
    ebitda = coalesce_num(income0.get("ebitda"))
    total_equity = coalesce_num(balance0.get("totalStockholdersEquity"), balance0.get("totalEquity"))
    current_assets = coalesce_num(balance0.get("currentAssets"), balance0.get("totalCurrentAssets"))
    inventory = coalesce_num(balance0.get("inventory"))
    current_liabilities = coalesce_num(balance0.get("currentLiabilities"), balance0.get("totalCurrentLiabilities"))
    total_debt = coalesce_num(balance0.get("totalDebt"), sum_numbers(balance0.get("shortTermDebt"), balance0.get("longTermDebt")))
    cash_equivalents = coalesce_num(balance0.get("cashAndCashEquivalents"), balance0.get("cashAndShortTermInvestments"))
    operating_cash_flow = coalesce_num(cash0.get("operatingCashFlow"), cash0.get("netCashProvidedByOperatingActivities"))
    capital_expenditure = abs_num(coalesce_num(cash0.get("capitalExpenditure"), cash0.get("investmentsInPropertyPlantAndEquipment")))
    free_cash_flow = coalesce_num(cash0.get("freeCashFlow"), operating_cash_flow - capital_expenditure if is_num(operating_cash_flow) and is_num(capital_expenditure) else None)

    metrics = {
        "price": price,
        "previous_close": previous_close,
        "change": coalesce_num(quote.get("change"), price - previous_close if is_num(price) and is_num(previous_close) else None),
        "change_pct": coalesce_num(percent_field(quote.get("changePercentage")), (price / previous_close - 1) if is_num(price) and is_num(previous_close) and previous_close else None),
        "market_cap": coalesce_num(quote.get("marketCap"), profile.get("marketCap"), price * shares if is_num(price) and is_num(shares) else None),
        "shares_outstanding": shares,
        "enterprise_value": coalesce_num(get_num_from_many([key_metrics], ["enterpriseValueTTM", "enterpriseValue"])),
        "pe_ratio": coalesce_num(get_num_from_many([ratios], ["priceToEarningsRatioTTM", "peRatio"])),
        "forward_pe": coalesce_num(get_num_from_many([analyst], ["forwardPE", "forwardPe"])),
        "pb_ratio": coalesce_num(get_num_from_many([ratios], ["priceToBookRatioTTM", "priceToBookRatio"])),
        "ps_ratio": coalesce_num(get_num_from_many([ratios], ["priceToSalesRatioTTM", "priceToSalesRatio"])),
        "ev_ebitda": coalesce_num(get_num_from_many([key_metrics], ["evToEBITDATTM"]), get_num_from_many([ratios], ["enterpriseValueMultipleTTM", "evToEbitda"])),
        "dividend_yield": coalesce_num(percent_field(profile.get("lastDividend")) / price if is_num(to_num(profile.get("lastDividend"))) and is_num(price) and price else None, percent_field(get_num_from_many([ratios], ["dividendYieldTTM"]))),
        "analyst_target": coalesce_num(get_num_from_many([analyst], ["targetMeanPrice", "priceTarget"])),
        "intrinsic_value": coalesce_num(dcf_obj.get("dcf")),
        "revenue": revenue,
        "gross_profit": gross_profit,
        "operating_income": operating_income,
        "net_income": net_income,
        "operating_cash_flow": operating_cash_flow,
        "free_cash_flow": free_cash_flow,
        "book_value_per_share": coalesce_num(get_num_from_many([key_metrics], ["bookValuePerShareTTM", "bookValuePerShare"])),
        "revenue_growth": coalesce_num(growth.get("revenueGrowth")),
        "earnings_growth": coalesce_num(growth.get("netIncomeGrowth"), growth.get("epsdilutedGrowth")),
        "gross_margin": coalesce_num(get_num_from_many([ratios], ["grossProfitMarginTTM"])),
        "operating_margin": coalesce_num(get_num_from_many([ratios], ["operatingProfitMarginTTM"])),
        "profit_margin": coalesce_num(get_num_from_many([ratios], ["netProfitMarginTTM"])),
        "roe": coalesce_num(get_num_from_many([key_metrics, ratios], ["returnOnEquityTTM", "returnOnEquity"])),
        "roa": coalesce_num(get_num_from_many([key_metrics, ratios], ["returnOnAssetsTTM", "returnOnAssets"])),
        "roic": coalesce_num(get_num_from_many([key_metrics], ["returnOnInvestedCapitalTTM", "returnOnCapitalEmployedTTM"])),
        "current_ratio": coalesce_num(get_num_from_many([ratios], ["currentRatioTTM", "currentRatio"])),
        "quick_ratio": coalesce_num(get_num_from_many([ratios], ["quickRatioTTM", "quickRatio"])),
        "debt_to_equity": coalesce_num(get_num_from_many([ratios], ["debtToEquityRatioTTM", "debtToEquityRatio"])),
        "interest_coverage": coalesce_num(get_num_from_many([ratios], ["interestCoverageTTM", "interestCoverage"])),
        "beta": coalesce_num(profile.get("beta")),
        "annual_dividend": coalesce_num(profile.get("lastDividend")),
        "total_assets": coalesce_num(balance0.get("totalAssets")),
        "total_equity": total_equity,
        "current_assets": current_assets,
        "current_liabilities": current_liabilities,
        "total_debt": total_debt,
        "cash_and_equivalents": cash_equivalents,
        "rating_score": coalesce_num(rating_obj.get("overallScore")),
    }
    texts = {
        "company_name": profile.get("companyName") or quote.get("name") or symbol,
        "sector": profile.get("sector") or "",
        "industry": profile.get("industry") or "",
        "exchange": profile.get("exchangeShortName") or quote.get("exchange") or "",
        "country": profile.get("country") or "",
        "currency": quote.get("currency") or profile.get("currency") or "USD",
        "website": profile.get("website") or "",
        "description": profile.get("description") or "",
        "logo_url": profile.get("image") or "",
    }
    if not options.light_mode and not is_num(metrics["analyst_target"]):
        errors.setdefault("analyst_estimates", "Analyst target unavailable on current plan or response.")
    if income1:
        metrics["revenue_prev"] = coalesce_num(income1.get("revenue"), income1.get("totalRevenue"))
        metrics["net_income_prev"] = coalesce_num(income1.get("netIncome"))
    return make_source("fmp", symbol, raw, errors, metrics, texts, history, annuals)


def fetch_twelve_source(symbol: str, keys: dict[str, str], refresh_token: int, options: RuntimeOptions | None = None) -> dict[str, Any]:
    options = options or RuntimeOptions(False, False, False)
    api_key = keys.get("twelve_data", "").strip()
    if not api_key:
        return make_source("twelve_data", symbol, {}, {"api": "Missing API key"}, {}, {}, [], [])
    base = "https://api.twelvedata.com"
    batch = [
        ("quote", f"{base}/quote", {"symbol": symbol, "apikey": api_key}, None),
        ("profile", f"{base}/profile", {"symbol": symbol, "apikey": api_key}, None),
        ("statistics", f"{base}/statistics", {"symbol": symbol, "apikey": api_key}, None),
        ("history", f"{base}/time_series", {"symbol": symbol, "interval": "1day", "outputsize": 260, "order": "ASC", "apikey": api_key}, None),
    ]
    if not options.light_mode:
        batch.extend(
            [
                ("income", f"{base}/income_statement", {"symbol": symbol, "interval": "annual", "apikey": api_key}, None),
                ("balance", f"{base}/balance_sheet", {"symbol": symbol, "interval": "annual", "apikey": api_key}, None),
                ("cash", f"{base}/cash_flow", {"symbol": symbol, "interval": "annual", "apikey": api_key}, None),
            ]
        )
    raw, errors = run_request_batch("twelve_data", symbol, batch, refresh_token, max_workers=1 if options.safe_mode else 3, sequential=options.safe_mode)

    quote_obj = normalize_envelope(raw.get("quote"))
    profile_obj = normalize_envelope(raw.get("profile"))
    statistics_raw = raw.get("statistics", {}) if isinstance(raw.get("statistics"), dict) else {}
    stats_obj = statistics_raw.get("statistics", statistics_raw)
    stats_meta = statistics_raw.get("meta", {})
    income_records = extract_records(raw.get("income"))
    balance_records = extract_records(raw.get("balance"))
    cash_records = extract_records(raw.get("cash"))
    income0 = income_records[0] if income_records else {}
    income1 = income_records[1] if len(income_records) > 1 else {}
    balance0 = balance_records[0] if balance_records else {}
    history = extract_history_rows(raw.get("history"))
    annuals = merge_annual_series(income_records, cash_records)

    price = coalesce_num(get_num_from_many([quote_obj], ["close", "price", "last_price", "last"]))
    previous_close = coalesce_num(get_num_from_many([quote_obj], ["previous_close", "prev_close"]))
    shares = coalesce_num(get_num_from_many([stats_obj], ["shares_outstanding"]), get_num_from_many([income0], ["diluted_shares_outstanding", "basic_shares_outstanding"]))
    revenue = get_num_from_many([stats_obj, income0], ["revenue_ttm", "revenue", "sales", "total_revenue"])
    net_income = get_num_from_many([stats_obj, income0], ["net_income_to_common_ttm", "net_income", "netincome"])
    operating_cash_flow = get_num_from_many([stats_obj, cash_records[0] if cash_records else {}], ["operating_cash_flow_ttm", "operating_cash_flow"])
    capex = abs_num(get_num_from_many([cash_records[0] if cash_records else {}], ["capital_expenditure", "capital_expenditures"]))
    free_cash_flow = coalesce_num(
        get_num_from_many([stats_obj, cash_records[0] if cash_records else {}], ["levered_free_cash_flow_ttm", "free_cash_flow"]),
        operating_cash_flow - capex if is_num(operating_cash_flow) and is_num(capex) else None,
    )

    metrics = {
        "price": price,
        "previous_close": previous_close,
        "change": coalesce_num(get_num_from_many([quote_obj], ["change"]), price - previous_close if is_num(price) and is_num(previous_close) else None),
        "change_pct": coalesce_num(get_num_from_many([quote_obj], ["percent_change", "change_percent"]), (price / previous_close - 1) if is_num(price) and is_num(previous_close) and previous_close else None),
        "market_cap": coalesce_num(get_num_from_many([stats_obj], ["market_capitalization", "market_cap"]), price * shares if is_num(price) and is_num(shares) else None),
        "shares_outstanding": shares,
        "enterprise_value": coalesce_num(get_num_from_many([stats_obj], ["enterprise_value"])),
        "pe_ratio": coalesce_num(get_num_from_many([stats_obj], ["trailing_pe", "price_to_earnings_ratio"])),
        "forward_pe": coalesce_num(get_num_from_many([stats_obj], ["forward_pe"])),
        "pb_ratio": coalesce_num(get_num_from_many([stats_obj], ["price_to_book_mrq", "price_to_book_ratio"])),
        "ps_ratio": coalesce_num(get_num_from_many([stats_obj], ["price_to_sales_ttm", "price_to_sales_ratio"])),
        "ev_ebitda": coalesce_num(get_num_from_many([stats_obj], ["enterprise_to_ebitda", "ev_ebitda"])),
        "dividend_yield": coalesce_num(get_num_from_many([stats_obj], ["forward_annual_dividend_yield", "dividend_yield"])),
        "analyst_target": coalesce_num(get_num_from_many([stats_obj], ["target_mean_price", "analyst_target_price"])),
        "revenue": revenue,
        "gross_profit": get_num_from_many([stats_obj, income0], ["gross_profit_ttm", "gross_profit"]),
        "operating_income": get_num_from_many([income0], ["operating_income"]),
        "net_income": net_income,
        "operating_cash_flow": operating_cash_flow,
        "free_cash_flow": free_cash_flow,
        "book_value_per_share": coalesce_num(get_num_from_many([stats_obj], ["book_value_per_share_mrq", "book_value_per_share"])),
        "revenue_growth": coalesce_num(get_num_from_many([stats_obj], ["quarterly_revenue_growth"])),
        "earnings_growth": coalesce_num(get_num_from_many([stats_obj], ["quarterly_earnings_growth_yoy"])),
        "gross_margin": coalesce_num(get_num_from_many([stats_obj], ["gross_margin"])),
        "operating_margin": coalesce_num(get_num_from_many([stats_obj], ["operating_margin"])),
        "profit_margin": coalesce_num(get_num_from_many([stats_obj], ["profit_margin"])),
        "roe": coalesce_num(get_num_from_many([stats_obj], ["return_on_equity_ttm"])),
        "roa": coalesce_num(get_num_from_many([stats_obj], ["return_on_assets_ttm"])),
        "roic": coalesce_num(get_num_from_many([stats_obj], ["roic", "return_on_invested_capital"])),
        "current_ratio": coalesce_num(get_num_from_many([stats_obj], ["current_ratio_mrq", "current_ratio"])),
        "quick_ratio": coalesce_num(get_num_from_many([balance0], ["quick_ratio"])),
        "debt_to_equity": coalesce_num(get_num_from_many([stats_obj], ["total_debt_to_equity_mrq", "debt_to_equity"])),
        "interest_coverage": coalesce_num(get_num_from_many([income0], ["interest_coverage"])),
        "beta": coalesce_num(get_num_from_many([stats_obj], ["beta"])),
        "annual_dividend": coalesce_num(get_num_from_many([stats_obj], ["dividend_rate", "forward_annual_dividend_rate", "trailing_annual_dividend_rate"])),
        "total_assets": get_num_from_many([balance0], ["total_assets", "assets"]),
        "total_equity": get_num_from_many([balance0], ["total_stockholder_equity", "stockholders_equity", "shareholders_equity"]),
        "current_assets": get_num_from_many([balance0], ["current_assets", "total_current_assets"]),
        "current_liabilities": get_num_from_many([balance0], ["current_liabilities", "total_current_liabilities"]),
        "total_debt": get_num_from_many([balance0, stats_obj], ["total_debt_mrq", "total_debt"]),
        "cash_and_equivalents": get_num_from_many([balance0, stats_obj], ["cash_and_cash_equivalents", "total_cash_mrq", "cash"]),
    }
    if income1:
        metrics["revenue_prev"] = get_num_from_many([income1], ["sales", "revenue", "total_revenue"])
        metrics["net_income_prev"] = get_num_from_many([income1], ["net_income", "netincome"])

    texts = {
        "company_name": get_str_from_many([profile_obj, quote_obj, stats_meta], ["name"]) or symbol,
        "sector": get_str_from_many([profile_obj], ["sector"]),
        "industry": get_str_from_many([profile_obj], ["industry"]),
        "exchange": get_str_from_many([quote_obj, profile_obj, stats_meta], ["exchange", "mic_code"]),
        "country": get_str_from_many([profile_obj], ["country"]),
        "currency": get_str_from_many([quote_obj, stats_meta], ["currency"]) or "USD",
        "website": get_str_from_many([profile_obj], ["website"]),
        "description": get_str_from_many([profile_obj], ["description", "business_summary"]),
        "logo_url": "",
    }
    if likely_non_us_symbol(symbol) and not history:
        errors.setdefault("history", "Twelve Data returned no history for this non-US symbol on the current plan.")
    return make_source("twelve_data", symbol, raw, errors, metrics, texts, history, annuals)


def fetch_tiingo_source(symbol: str, keys: dict[str, str], refresh_token: int, options: RuntimeOptions | None = None) -> dict[str, Any]:
    """Tiingo normalized provider for price/history fallback."""
    options = options or RuntimeOptions(False, False, False)
    api_key = keys.get("tiingo", "").strip()
    if not api_key:
        return make_source("tiingo", symbol, {}, {"api": "Missing API key"}, {}, {}, [], [])
    base = "https://api.tiingo.com/tiingo/daily"
    start = (date.today() - timedelta(days=730)).isoformat()
    batch = [
        ("meta", f"{base}/{symbol.lower()}", {"token": api_key}, None),
        ("prices", f"{base}/{symbol.lower()}/prices", {"startDate": start, "resampleFreq": "daily", "token": api_key}, None),
    ]
    raw, errors = run_request_batch("tiingo", symbol, batch, refresh_token, max_workers=1 if options.safe_mode else 2, sequential=options.safe_mode)
    meta = raw.get("meta") if isinstance(raw.get("meta"), dict) else {}
    rows = raw.get("prices") if isinstance(raw.get("prices"), list) else []
    history: list[dict[str, Any]] = []
    for row in rows[-520:]:
        if not isinstance(row, dict):
            continue
        close_value = coalesce_num(row.get("adjClose"), row.get("close"))
        if not row.get("date") or not is_num(close_value):
            continue
        history_row = {"date": str(row["date"])[:10], "close": close_value}
        if is_num(row.get("high")):
            history_row["high"] = float(row["high"])
        if is_num(row.get("low")):
            history_row["low"] = float(row["low"])
        if is_num(row.get("volume")):
            history_row["volume"] = float(row["volume"])
        history.append(history_row)
    history.sort(key=lambda item: item["date"])
    price = history[-1]["close"] if history else None
    previous_close = history[-2]["close"] if len(history) > 1 else None
    metrics = {
        "price": price,
        "previous_close": previous_close,
        "change": price - previous_close if is_num(price) and is_num(previous_close) else None,
        "change_pct": (price / previous_close - 1) if is_num(price) and is_num(previous_close) and previous_close else None,
    }
    texts = {
        "company_name": meta.get("name") or symbol,
        "exchange": meta.get("exchangeCode") or "",
        "currency": "USD",
        "description": meta.get("description") or "",
    }
    return make_source("tiingo", symbol, raw, errors, metrics, texts, history, [])


def fetch_intrinio_source(symbol: str, keys: dict[str, str], refresh_token: int, options: RuntimeOptions | None = None) -> dict[str, Any]:
    """Intrinio normalized provider for company metadata and realtime price."""
    options = options or RuntimeOptions(False, False, False)
    api_key = keys.get("intrinio", "").strip()
    if not api_key:
        return make_source("intrinio", symbol, {}, {"api": "Missing API key"}, {}, {}, [], [])
    base = "https://api-v2.intrinio.com"
    start = (date.today() - timedelta(days=730)).isoformat()
    batch = [
        ("company", f"{base}/companies/{symbol}", {"api_key": api_key}, None),
        ("quote", f"{base}/securities/{symbol}/prices/realtime", {"api_key": api_key}, None),
        ("history", f"{base}/securities/{symbol}/prices", {"start_date": start, "page_size": 260, "api_key": api_key}, None),
    ]
    raw, errors = run_request_batch("intrinio", symbol, batch, refresh_token, max_workers=1 if options.safe_mode else 2, sequential=options.safe_mode)
    company = raw.get("company") if isinstance(raw.get("company"), dict) else {}
    quote = raw.get("quote") if isinstance(raw.get("quote"), dict) else {}
    price_rows = []
    history_payload = raw.get("history")
    if isinstance(history_payload, dict):
        price_rows = history_payload.get("stock_prices") or history_payload.get("prices") or []
    elif isinstance(history_payload, list):
        price_rows = history_payload
    history: list[dict[str, Any]] = []
    for row in price_rows:
        if not isinstance(row, dict):
            continue
        close_value = coalesce_num(row.get("adj_close"), row.get("close"), row.get("last_price"))
        row_date = row.get("date") or row.get("time")
        if not row_date or not is_num(close_value):
            continue
        history_row = {"date": str(row_date)[:10], "close": close_value}
        if is_num(row.get("high")):
            history_row["high"] = float(row["high"])
        if is_num(row.get("low")):
            history_row["low"] = float(row["low"])
        if is_num(row.get("volume")):
            history_row["volume"] = float(row["volume"])
        history.append(history_row)
    history.sort(key=lambda item: item["date"])
    price = coalesce_num(quote.get("last_price"), quote.get("last"), history[-1]["close"] if history else None)
    previous_close = coalesce_num(quote.get("prev_close"), history[-2]["close"] if len(history) > 1 else None)
    metrics = {
        "price": price,
        "previous_close": previous_close,
        "change": price - previous_close if is_num(price) and is_num(previous_close) else None,
        "change_pct": (price / previous_close - 1) if is_num(price) and is_num(previous_close) and previous_close else None,
        "market_cap": coalesce_num(company.get("market_cap")),
        "shares_outstanding": coalesce_num(company.get("shares_outstanding")),
    }
    texts = {
        "company_name": company.get("name") or symbol,
        "sector": company.get("sector") or "",
        "industry": company.get("industry_category") or company.get("industry_group") or "",
        "exchange": company.get("exchange") or "",
        "country": company.get("country") or "",
        "currency": "USD",
        "website": company.get("website") or "",
        "description": company.get("short_description") or company.get("long_description") or "",
    }
    return make_source("intrinio", symbol, raw, errors, metrics, texts, history, [])


def fetch_polygon_source(symbol: str, keys: dict[str, str], refresh_token: int, options: RuntimeOptions | None = None) -> dict[str, Any]:
    options = options or RuntimeOptions(False, False, False)
    api_key = keys.get("polygon", "").strip()
    if not api_key:
        return make_source("polygon", symbol, {}, {"api": "Missing API key"}, {}, {}, [], [])
    base = "https://api.polygon.io"
    to_day = date.today()
    from_day = to_day - timedelta(days=730)
    batch = [
        ("reference", f"{base}/v3/reference/tickers/{symbol}", {"apiKey": api_key}, None),
        ("history", f"{base}/v2/aggs/ticker/{symbol}/range/1/day/{from_day.isoformat()}/{to_day.isoformat()}", {"adjusted": "true", "sort": "asc", "limit": 500, "apiKey": api_key}, None),
    ]
    if not options.light_mode:
        batch.append(("financials", f"{base}/vX/reference/financials", {"ticker": symbol, "timeframe": "annual", "limit": 5, "sort": "filing_date", "order": "desc", "apiKey": api_key}, None))
    raw, errors = run_request_batch("polygon", symbol, batch, refresh_token, max_workers=1 if options.safe_mode else 2, sequential=options.safe_mode)

    reference = raw.get("reference", {}).get("results", {}) if isinstance(raw.get("reference"), dict) else {}
    financial_rows = raw.get("financials", {}).get("results", []) if isinstance(raw.get("financials"), dict) else []
    financial_records = extract_records(financial_rows)
    fin0 = financial_records[0].get("financials", {}) if financial_records and isinstance(financial_records[0], dict) else {}
    fin1 = financial_records[1].get("financials", {}) if len(financial_records) > 1 and isinstance(financial_records[1], dict) else {}
    history_rows = raw.get("history", {}).get("results", []) if isinstance(raw.get("history"), dict) else []
    history = []
    for row in history_rows:
        if not isinstance(row, dict):
            continue
        close_value = to_num(row.get("c"))
        stamp = to_num(row.get("t"))
        if not is_num(close_value) or not is_num(stamp):
            continue
        history_row = {"date": datetime.fromtimestamp(stamp / 1000, tz=timezone.utc).date().isoformat(), "close": close_value}
        high_value = to_num(row.get("h"))
        low_value = to_num(row.get("l"))
        volume_value = to_num(row.get("v"))
        if is_num(high_value):
            history_row["high"] = high_value
        if is_num(low_value):
            history_row["low"] = low_value
        if is_num(volume_value):
            history_row["volume"] = volume_value
        history.append(history_row)
    history.sort(key=lambda row: row["date"])
    annuals = merge_polygon_annual_series(financial_rows)

    price = history[-1]["close"] if history else None
    previous_close = history[-2]["close"] if len(history) > 1 else None
    revenue = get_num_from_many([fin0], ["revenues", "revenue", "sales"])
    net_income = get_num_from_many([fin0], ["net_income_loss", "netincomeloss"])
    operating_cash_flow = get_num_from_many([fin0], ["net_cash_flow_from_operating_activities", "netcashprovidedbyusedinoperatingactivities", "operatingcashflow"])
    capex = abs_num(get_num_from_many([fin0], ["capital_expenditures", "capitalexpenditure"]))
    free_cash_flow = operating_cash_flow - capex if is_num(operating_cash_flow) and is_num(capex) else None
    total_equity = get_num_from_many([fin0], ["equity", "stockholders_equity", "total_equity"])
    total_assets = get_num_from_many([fin0], ["assets", "total_assets"])
    current_assets = get_num_from_many([fin0], ["current_assets"])
    current_liabilities = get_num_from_many([fin0], ["current_liabilities"])
    total_debt = get_num_from_many([fin0], ["debt", "total_debt", "long_term_debt"])
    cash_equivalents = get_num_from_many([fin0], ["cash_and_cash_equivalents", "cash"])
    shares = coalesce_num(reference.get("share_class_shares_outstanding"), reference.get("weighted_shares_outstanding"))

    metrics = {
        "price": price,
        "previous_close": previous_close,
        "change": price - previous_close if is_num(price) and is_num(previous_close) else None,
        "change_pct": (price / previous_close - 1) if is_num(price) and is_num(previous_close) and previous_close else None,
        "market_cap": coalesce_num(reference.get("market_cap")),
        "shares_outstanding": shares,
        "revenue": revenue,
        "gross_profit": get_num_from_many([fin0], ["gross_profit"]),
        "operating_income": get_num_from_many([fin0], ["operating_income_loss", "operating_income"]),
        "net_income": net_income,
        "operating_cash_flow": operating_cash_flow,
        "free_cash_flow": free_cash_flow,
        "revenue_prev": get_num_from_many([fin1], ["revenues", "revenue", "sales"]),
        "net_income_prev": get_num_from_many([fin1], ["net_income_loss", "netincomeloss"]),
        "gross_margin": safe_div(get_num_from_many([fin0], ["gross_profit"]), revenue),
        "operating_margin": safe_div(get_num_from_many([fin0], ["operating_income_loss", "operating_income"]), revenue),
        "profit_margin": safe_div(net_income, revenue),
        "roe": safe_div(net_income, total_equity),
        "roa": safe_div(net_income, total_assets),
        "current_ratio": safe_div(current_assets, current_liabilities),
        "debt_to_equity": safe_div(total_debt, total_equity),
        "book_value_per_share": safe_div(total_equity, shares),
        "enterprise_value": sum_numbers(coalesce_num(reference.get("market_cap")), total_debt, -1 * (cash_equivalents or 0)),
        "total_assets": total_assets,
        "total_equity": total_equity,
        "current_assets": current_assets,
        "current_liabilities": current_liabilities,
        "total_debt": total_debt,
        "cash_and_equivalents": cash_equivalents,
    }
    texts = {
        "company_name": reference.get("name") or symbol,
        "sector": get_str_from_many([reference], ["sic_description", "sector"]),
        "industry": get_str_from_many([reference], ["industry_description", "type"]),
        "exchange": reference.get("primary_exchange") or "",
        "country": reference.get("locale") or "",
        "currency": reference.get("currency_name") or "USD",
        "website": reference.get("homepage_url") or "",
        "description": reference.get("description") or "",
        "logo_url": "",
    }
    notes: list[str] = []
    if likely_non_us_symbol(symbol):
        notes.append("Polygon stock coverage is primarily strongest for US-listed equities.")
    if reference and not financial_rows:
        notes.append("Reference metadata loaded but Polygon financial statements were unavailable on the current plan or symbol.")
    if not history:
        errors.setdefault("history", "Polygon returned no aggregate history for this symbol.")
    return make_source("polygon", symbol, raw, errors, metrics, texts, history, annuals, notes=notes)


def fetch_alpha_source(symbol: str, keys: dict[str, str], refresh_token: int, options: RuntimeOptions | None = None) -> dict[str, Any]:
    options = options or RuntimeOptions(False, False, False)
    api_key = keys.get("alpha_vantage", "").strip()
    if not api_key:
        return make_source("alpha_vantage", symbol, {}, {"api": "Missing API key"}, {}, {}, [], [])
    base = "https://www.alphavantage.co/query"
    raw: dict[str, Any] = {}
    errors: dict[str, str] = {}
    requests_list = [
        ("overview", {"function": "OVERVIEW", "symbol": symbol, "apikey": api_key}),
        ("global_quote", {"function": "GLOBAL_QUOTE", "symbol": symbol, "apikey": api_key}),
    ]
    if not options.light_mode:
        requests_list.append(("history", {"function": "TIME_SERIES_DAILY_ADJUSTED", "symbol": symbol, "outputsize": "compact", "apikey": api_key}))
        requests_list.append(("dividends", {"function": "DIVIDENDS", "symbol": symbol, "apikey": api_key}))
    for index, (name, params) in enumerate(requests_list):
        if index:
            time.sleep(max(1.2, PROVIDER_THROTTLE_SECONDS["alpha_vantage"]))
        try:
            raw[name] = fetch_json("alpha_vantage", symbol, base, params, refresh_token)
        except Exception as exc:  # noqa: BLE001
            errors[name] = str(exc)

    overview = raw.get("overview", {}) if isinstance(raw.get("overview"), dict) else {}
    quote = raw.get("global_quote", {}).get("Global Quote", {}) if isinstance(raw.get("global_quote"), dict) else {}
    daily_series = raw.get("history", {}).get("Time Series (Daily)", {}) if isinstance(raw.get("history"), dict) else {}
    history: list[dict[str, Any]] = []
    if isinstance(daily_series, dict):
        for day, values in daily_series.items():
            close_value = to_num(values.get("5. adjusted close") if isinstance(values, dict) else None)
            if is_num(close_value):
                history_row = {"date": day[:10], "close": close_value}
                high_value = to_num(values.get("2. high") if isinstance(values, dict) else None)
                low_value = to_num(values.get("3. low") if isinstance(values, dict) else None)
                volume_value = to_num(values.get("6. volume") if isinstance(values, dict) else None)
                if is_num(high_value):
                    history_row["high"] = high_value
                if is_num(low_value):
                    history_row["low"] = low_value
                if is_num(volume_value):
                    history_row["volume"] = volume_value
                history.append(history_row)
    history.sort(key=lambda row: row["date"])
    dividends_total = None
    if isinstance(raw.get("dividends"), dict) and isinstance(raw["dividends"].get("data"), list):
        dividends_total = sum_dividends_last_year(raw["dividends"])

    price = coalesce_num(quote.get("05. price"))
    previous_close = coalesce_num(quote.get("08. previous close"))
    shares = coalesce_num(overview.get("SharesOutstanding"))
    metrics = {
        "price": price,
        "previous_close": previous_close,
        "change": coalesce_num(quote.get("09. change"), safe_subtract(price, previous_close)),
        "change_pct": coalesce_num(to_num(quote.get("10. change percent")), (price / previous_close - 1) if is_num(price) and is_num(previous_close) and previous_close else None),
        "market_cap": coalesce_num(overview.get("MarketCapitalization"), price * shares if is_num(price) and is_num(shares) else None),
        "shares_outstanding": shares,
        "pe_ratio": coalesce_num(overview.get("PERatio")),
        "forward_pe": coalesce_num(overview.get("ForwardPE")),
        "pb_ratio": coalesce_num(overview.get("PriceToBookRatio")),
        "ps_ratio": coalesce_num(overview.get("PriceToSalesRatioTTM")),
        "ev_ebitda": coalesce_num(overview.get("EVToEBITDA")),
        "dividend_yield": coalesce_num(to_num(overview.get("DividendYield"))),
        "analyst_target": coalesce_num(overview.get("AnalystTargetPrice")),
        "revenue": coalesce_num(overview.get("RevenueTTM")),
        "book_value_per_share": coalesce_num(overview.get("BookValue")),
        "revenue_growth": coalesce_num(to_num(overview.get("QuarterlyRevenueGrowthYOY"))),
        "earnings_growth": coalesce_num(to_num(overview.get("QuarterlyEarningsGrowthYOY"))),
        "operating_margin": coalesce_num(to_num(overview.get("OperatingMarginTTM"))),
        "profit_margin": coalesce_num(to_num(overview.get("ProfitMargin"))),
        "roe": coalesce_num(to_num(overview.get("ReturnOnEquityTTM"))),
        "roa": coalesce_num(to_num(overview.get("ReturnOnAssetsTTM"))),
        "beta": coalesce_num(overview.get("Beta")),
        "annual_dividend": coalesce_num(overview.get("DividendPerShare"), dividends_total),
    }
    texts = {
        "company_name": overview.get("Name") or symbol,
        "sector": overview.get("Sector") or "",
        "industry": overview.get("Industry") or "",
        "exchange": overview.get("Exchange") or "",
        "country": overview.get("Country") or "",
        "currency": overview.get("Currency") or "USD",
        "website": overview.get("OfficialSite") or "",
        "description": overview.get("Description") or "",
        "logo_url": "",
    }
    return make_source("alpha_vantage", symbol, raw, errors, metrics, texts, history, [])


def fetch_finnhub_source(symbol: str, keys: dict[str, str], refresh_token: int, options: RuntimeOptions | None = None) -> dict[str, Any]:
    options = options or RuntimeOptions(False, False, False)
    api_key = keys.get("finnhub", "").strip()
    if not api_key:
        return make_source("finnhub", symbol, {}, {"api": "Missing API key"}, {}, {}, [], [])
    base = "https://finnhub.io/api/v1"
    batch = [
        ("quote", f"{base}/quote", {"symbol": symbol, "token": api_key}, None),
        ("profile", f"{base}/stock/profile2", {"symbol": symbol, "token": api_key}, None),
        ("metric", f"{base}/stock/metric", {"symbol": symbol, "metric": "all", "token": api_key}, None),
        ("recommendation", f"{base}/stock/recommendation", {"symbol": symbol, "token": api_key}, None),
    ]
    if not options.light_mode:
        end_day = date.today()
        start_day = end_day - timedelta(days=30)
        batch.append(("news", f"{base}/company-news", {"symbol": symbol, "from": start_day.isoformat(), "to": end_day.isoformat(), "token": api_key}, None))
    raw, errors = run_request_batch("finnhub", symbol, batch, refresh_token, max_workers=1 if options.safe_mode else 2, sequential=options.safe_mode)

    quote = raw.get("quote", {}) if isinstance(raw.get("quote"), dict) else {}
    profile = raw.get("profile", {}) if isinstance(raw.get("profile"), dict) else {}
    metric_root = raw.get("metric", {}) if isinstance(raw.get("metric"), dict) else {}
    metrics_obj = metric_root.get("metric", metric_root if isinstance(metric_root, dict) else {})
    recommendations = raw.get("recommendation", []) if isinstance(raw.get("recommendation"), list) else []
    news_rows = raw.get("news", []) if isinstance(raw.get("news"), list) else []

    recommendation_score = None
    if recommendations and isinstance(recommendations[0], dict):
        rec0 = recommendations[0]
        bullish = coalesce_num(rec0.get("strongBuy"), 0) + coalesce_num(rec0.get("buy"), 0)
        bearish = coalesce_num(rec0.get("sell"), 0) + coalesce_num(rec0.get("strongSell"), 0)
        total = bullish + bearish + coalesce_num(rec0.get("hold"), 0)
        if is_num(total) and total > 0:
            recommendation_score = (bullish - bearish) / total

    market_cap = coalesce_num(profile.get("marketCapitalization"))
    if is_num(market_cap):
        market_cap *= 1_000_000.0
    shares = coalesce_num(profile.get("shareOutstanding"))
    if is_num(shares):
        shares *= 1_000_000.0
    enterprise_value = coalesce_num(get_num_from_many([metrics_obj], ["enterpriseValue"]))
    if is_num(enterprise_value):
        enterprise_value *= 1_000_000.0
    revenue_per_share = coalesce_num(get_num_from_many([metrics_obj], ["revenuePerShareTTM"]))
    cash_flow_per_share = coalesce_num(get_num_from_many([metrics_obj], ["cashFlowPerShareTTM"]))
    eps_ttm = coalesce_num(get_num_from_many([metrics_obj], ["epsTTM"]))
    ev_fcf_multiple = coalesce_num(get_num_from_many([metrics_obj], ["currentEv/freeCashFlowTTM"]))

    metrics = {
        "price": coalesce_num(quote.get("c")),
        "previous_close": coalesce_num(quote.get("pc")),
        "change": coalesce_num(quote.get("d")),
        "change_pct": percent_field(quote.get("dp")),
        "market_cap": market_cap,
        "shares_outstanding": shares,
        "enterprise_value": enterprise_value,
        "pe_ratio": coalesce_num(get_num_from_many([metrics_obj], ["peTTM", "peNormalizedAnnual", "peBasicExclExtraTTM"])),
        "forward_pe": coalesce_num(get_num_from_many([metrics_obj], ["peExiTTM"])),
        "pb_ratio": coalesce_num(get_num_from_many([metrics_obj], ["pbAnnual", "pbQuarterly"])),
        "ps_ratio": coalesce_num(get_num_from_many([metrics_obj], ["psTTM"])),
        "dividend_yield": coalesce_num(percent_field(get_num_from_many([metrics_obj], ["dividendYieldIndicatedAnnual", "currentDividendYieldTTM"]))),
        "analyst_target": coalesce_num(get_num_from_many([metrics_obj], ["targetPriceMedian", "targetPriceAvg"])),
        "revenue": revenue_per_share * shares if is_num(revenue_per_share) and is_num(shares) else None,
        "net_income": eps_ttm * shares if is_num(eps_ttm) and is_num(shares) else None,
        "operating_cash_flow": cash_flow_per_share * shares if is_num(cash_flow_per_share) and is_num(shares) else None,
        "free_cash_flow": enterprise_value / ev_fcf_multiple if is_num(enterprise_value) and is_num(ev_fcf_multiple) and ev_fcf_multiple else None,
        "book_value_per_share": coalesce_num(get_num_from_many([metrics_obj], ["bookValuePerShareQuarterly", "bookValuePerShareAnnual"])),
        "revenue_growth": percent_field(get_num_from_many([metrics_obj], ["revenueGrowthTTMYoy", "revenueGrowth5Y"])),
        "earnings_growth": percent_field(get_num_from_many([metrics_obj], ["epsGrowthTTMYoy", "epsGrowth5Y"])),
        "gross_margin": percent_field(get_num_from_many([metrics_obj], ["grossMarginTTM"])),
        "operating_margin": percent_field(get_num_from_many([metrics_obj], ["operatingMarginTTM"])),
        "profit_margin": percent_field(get_num_from_many([metrics_obj], ["netMarginTTM"])),
        "roe": percent_field(get_num_from_many([metrics_obj], ["roeTTM", "roeRfy"])),
        "roa": percent_field(get_num_from_many([metrics_obj], ["roaTTM", "roaRfy"])),
        "roic": percent_field(get_num_from_many([metrics_obj], ["roicTTM"])),
        "current_ratio": coalesce_num(get_num_from_many([metrics_obj], ["currentRatioQuarterly", "currentRatioAnnual"])),
        "debt_to_equity": coalesce_num(get_num_from_many([metrics_obj], ["totalDebt/totalEquityQuarterly", "totalDebt/totalEquityAnnual", "totalDebtToEquityQuarterly"])),
        "beta": coalesce_num(get_num_from_many([metrics_obj], ["beta"])),
        "annual_dividend": coalesce_num(get_num_from_many([metrics_obj], ["dividendIndicatedAnnual", "dividendPerShareTTM"])),
        "recommendation_score": recommendation_score,
    }
    texts = {
        "company_name": profile.get("name") or symbol,
        "sector": profile.get("finnhubIndustry") or "",
        "industry": profile.get("finnhubIndustry") or "",
        "exchange": profile.get("exchange") or "",
        "country": profile.get("country") or "",
        "currency": profile.get("currency") or "USD",
        "website": profile.get("weburl") or "",
        "description": "",
        "logo_url": profile.get("logo") or "",
    }
    notes = []
    if not news_rows and not options.light_mode:
        notes.append("Finnhub news returned no rows for the selected window.")
    return make_source("finnhub", symbol, raw, errors, metrics, texts, [], [], news=news_rows[:20], recommendations=recommendations[:12], notes=notes)


def fetch_yahoo_source(symbol: str, refresh_token: int, options: RuntimeOptions | None = None) -> dict[str, Any]:
    del refresh_token
    options = options or RuntimeOptions(False, False, False)
    if yf is None:
        return make_source("yahoo", symbol, {}, {"library": "yfinance is not installed"}, {}, {}, [], [])
    now = time.time()
    if YAHOO_STATE["cooldown_until"] > now:
        return make_source("yahoo", symbol, {}, {"cooldown": YAHOO_STATE["reason"] or "Yahoo temporarily cooled down after rate limit"}, {}, {}, [], [])
    try:
        throttle_provider("yahoo")
        ticker = yf.Ticker(symbol)
        history_df = ticker.history(period="2y", interval="1d", auto_adjust=False)
        fast_info = dict(ticker.fast_info or {})
        info = dict(ticker.info or {})
        income_df = pd.DataFrame()
        cash_df = pd.DataFrame()
        balance_df = pd.DataFrame()
        if not options.light_mode:
            try:
                income_df = ticker.financials if isinstance(ticker.financials, pd.DataFrame) else pd.DataFrame()
            except Exception:  # noqa: BLE001
                income_df = pd.DataFrame()
            try:
                cash_df = ticker.cashflow if isinstance(ticker.cashflow, pd.DataFrame) else pd.DataFrame()
            except Exception:  # noqa: BLE001
                cash_df = pd.DataFrame()
            try:
                balance_df = ticker.balance_sheet if isinstance(ticker.balance_sheet, pd.DataFrame) else pd.DataFrame()
            except Exception:  # noqa: BLE001
                balance_df = pd.DataFrame()
    except Exception as exc:  # noqa: BLE001
        message = str(exc)
        if "429" in message or "Too Many Requests" in message:
            YAHOO_STATE["cooldown_until"] = time.time() + YAHOO_COOLDOWN_SECONDS
            YAHOO_STATE["reason"] = "Yahoo Finance returned 429 and was deprioritized temporarily."
        return make_source("yahoo", symbol, {}, {"fetch": message}, {}, {}, [], [])

    raw = {
        "fast_info": fast_info,
        "info": info,
        "history_rows": len(history_df),
    }
    history: list[dict[str, Any]] = []
    if not history_df.empty:
        for idx, row in history_df.reset_index().iterrows():
            close_value = to_num(row.get("Close"))
            date_value = row.get("Date")
            if not is_num(close_value) or pd.isna(date_value):
                continue
            history_row = {"date": pd.Timestamp(date_value).date().isoformat(), "close": close_value}
            high_value = to_num(row.get("High"))
            low_value = to_num(row.get("Low"))
            volume_value = to_num(row.get("Volume"))
            if is_num(high_value):
                history_row["high"] = high_value
            if is_num(low_value):
                history_row["low"] = low_value
            if is_num(volume_value):
                history_row["volume"] = volume_value
            history.append(history_row)
    annuals: list[dict[str, Any]] = []
    if not options.light_mode and not income_df.empty:
        years = [str(pd.Timestamp(col).year)[:4] for col in income_df.columns]
        for idx, year in enumerate(years[:5]):
            column = income_df.columns[idx]
            cash_col = cash_df.columns[idx] if idx < len(cash_df.columns) else None
            annuals.append(
                {
                    "year": year,
                    "revenue": to_num(income_df.at["Total Revenue", column]) if "Total Revenue" in income_df.index else None,
                    "gross_profit": to_num(income_df.at["Gross Profit", column]) if "Gross Profit" in income_df.index else None,
                    "operating_income": to_num(income_df.at["Operating Income", column]) if "Operating Income" in income_df.index else None,
                    "net_income": to_num(income_df.at["Net Income", column]) if "Net Income" in income_df.index else None,
                    "operating_cash_flow": to_num(cash_df.at["Operating Cash Flow", cash_col]) if cash_col is not None and "Operating Cash Flow" in cash_df.index else None,
                    "capital_expenditure": abs_num(cash_df.at["Capital Expenditure", cash_col]) if cash_col is not None and "Capital Expenditure" in cash_df.index else None,
                    "free_cash_flow": None,
                }
            )
            if is_num(annuals[-1]["operating_cash_flow"]) and is_num(annuals[-1]["capital_expenditure"]):
                annuals[-1]["free_cash_flow"] = annuals[-1]["operating_cash_flow"] - annuals[-1]["capital_expenditure"]
        annuals.sort(key=lambda row: row["year"])

    shares = coalesce_num(info.get("sharesOutstanding"), fast_info.get("shares"))
    price = coalesce_num(info.get("currentPrice"), fast_info.get("lastPrice"))
    previous_close = coalesce_num(info.get("previousClose"), fast_info.get("previousClose"), fast_info.get("regularMarketPreviousClose"))
    total_equity = None
    if not balance_df.empty and "Stockholders Equity" in balance_df.index and len(balance_df.columns):
        total_equity = to_num(balance_df.iloc[balance_df.index.get_loc("Stockholders Equity"), 0])
    metrics = {
        "price": price,
        "previous_close": previous_close,
        "change": safe_subtract(price, previous_close),
        "change_pct": (price / previous_close - 1) if is_num(price) and is_num(previous_close) and previous_close else None,
        "market_cap": coalesce_num(info.get("marketCap"), fast_info.get("marketCap")),
        "shares_outstanding": shares,
        "enterprise_value": coalesce_num(info.get("enterpriseValue")),
        "pe_ratio": coalesce_num(info.get("trailingPE")),
        "forward_pe": coalesce_num(info.get("forwardPE")),
        "pb_ratio": coalesce_num(info.get("priceToBook")),
        "ps_ratio": coalesce_num(info.get("priceToSalesTrailing12Months")),
        "ev_ebitda": coalesce_num(info.get("enterpriseToEbitda")),
        "dividend_yield": coalesce_num(info.get("dividendYield")),
        "analyst_target": coalesce_num(info.get("targetMeanPrice")),
        "revenue": coalesce_num(info.get("totalRevenue"), annuals[-1]["revenue"] if annuals else None),
        "gross_profit": annuals[-1]["gross_profit"] if annuals else None,
        "operating_income": annuals[-1]["operating_income"] if annuals else None,
        "net_income": coalesce_num(info.get("netIncomeToCommon"), annuals[-1]["net_income"] if annuals else None),
        "operating_cash_flow": coalesce_num(info.get("operatingCashflow"), annuals[-1]["operating_cash_flow"] if annuals else None),
        "free_cash_flow": coalesce_num(info.get("freeCashflow"), annuals[-1]["free_cash_flow"] if annuals else None),
        "book_value_per_share": coalesce_num(info.get("bookValue")),
        "revenue_growth": coalesce_num(info.get("revenueGrowth")),
        "earnings_growth": coalesce_num(info.get("earningsGrowth")),
        "gross_margin": coalesce_num(info.get("grossMargins")),
        "operating_margin": coalesce_num(info.get("operatingMargins")),
        "profit_margin": coalesce_num(info.get("profitMargins")),
        "roe": coalesce_num(info.get("returnOnEquity")),
        "roa": coalesce_num(info.get("returnOnAssets")),
        "current_ratio": coalesce_num(info.get("currentRatio")),
        "beta": coalesce_num(info.get("beta"), fast_info.get("beta")),
        "annual_dividend": coalesce_num(info.get("dividendRate")),
        "total_equity": total_equity,
    }
    if len(annuals) > 1:
        metrics["revenue_prev"] = annuals[-2]["revenue"]
        metrics["net_income_prev"] = annuals[-2]["net_income"]

    texts = {
        "company_name": str(info.get("longName") or info.get("shortName") or symbol),
        "sector": str(info.get("sector") or ""),
        "industry": str(info.get("industry") or ""),
        "exchange": str(info.get("exchange") or fast_info.get("exchange") or ""),
        "country": str(info.get("country") or ""),
        "currency": str(info.get("currency") or fast_info.get("currency") or "USD"),
        "website": str(info.get("website") or ""),
        "description": str(info.get("longBusinessSummary") or ""),
        "logo_url": str(info.get("logo_url") or ""),
    }
    return make_source("yahoo", symbol, raw, {}, metrics, texts, history, annuals)


def fetch_all_sources(
    symbol: str,
    keys: dict[str, str],
    refresh_token: int,
    enabled_providers: list[str],
    options: RuntimeOptions,
) -> dict[str, dict[str, Any]]:
    jobs: dict[Any, str] = {}
    output: dict[str, dict[str, Any]] = {provider: disabled_source(provider) for provider in PROVIDER_LABELS if provider != "calculated"}
    providers_to_run = enabled_providers[:]
    if options.safe_mode:
        for provider in providers_to_run:
            try:
                if provider == "fmp":
                    output[provider] = fetch_fmp_source(symbol, keys, refresh_token, options)
                elif provider == "twelve_data":
                    output[provider] = fetch_twelve_source(symbol, keys, refresh_token, options)
                elif provider == "polygon":
                    output[provider] = fetch_polygon_source(symbol, keys, refresh_token, options)
                elif provider == "tiingo":
                    output[provider] = fetch_tiingo_source(symbol, keys, refresh_token, options)
                elif provider == "intrinio":
                    output[provider] = fetch_intrinio_source(symbol, keys, refresh_token, options)
                elif provider == "finnhub":
                    output[provider] = fetch_finnhub_source(symbol, keys, refresh_token, options)
                elif provider == "yahoo":
                    output[provider] = fetch_yahoo_source(symbol, refresh_token, options)
                elif provider == "alpha_vantage":
                    output[provider] = fetch_alpha_source(symbol, keys, refresh_token, options)
            except Exception as exc:  # noqa: BLE001
                output[provider] = make_source(provider, symbol, {}, {"fatal": str(exc)}, {}, {}, [], [])
        return output
    with ThreadPoolExecutor(max_workers=max(1, min(len(enabled_providers), 4))) as executor:
        if "fmp" in enabled_providers:
            jobs[executor.submit(fetch_fmp_source, symbol, keys, refresh_token, options)] = "fmp"
        if "finnhub" in enabled_providers:
            jobs[executor.submit(fetch_finnhub_source, symbol, keys, refresh_token, options)] = "finnhub"
        if "twelve_data" in enabled_providers:
            jobs[executor.submit(fetch_twelve_source, symbol, keys, refresh_token, options)] = "twelve_data"
        if "polygon" in enabled_providers:
            jobs[executor.submit(fetch_polygon_source, symbol, keys, refresh_token, options)] = "polygon"
        if "tiingo" in enabled_providers:
            jobs[executor.submit(fetch_tiingo_source, symbol, keys, refresh_token, options)] = "tiingo"
        if "intrinio" in enabled_providers:
            jobs[executor.submit(fetch_intrinio_source, symbol, keys, refresh_token, options)] = "intrinio"
        if "yahoo" in enabled_providers:
            jobs[executor.submit(fetch_yahoo_source, symbol, refresh_token, options)] = "yahoo"
        if "alpha_vantage" in enabled_providers:
            jobs[executor.submit(fetch_alpha_source, symbol, keys, refresh_token, options)] = "alpha_vantage"
        for future in as_completed(jobs):
            provider = jobs[future]
            try:
                output[provider] = future.result()
            except Exception as exc:  # noqa: BLE001
                output[provider] = make_source(provider, symbol, {}, {"fatal": str(exc)}, {}, {}, [], [])
    # ─── جلب الأخبار الإضافية الأربعة بالتوازي ───
    extra_news = {"marketaux": [], "benzinga": [], "sec_api": [], "newsapi_ai": []}
    if options.safe_mode:
        for _np, _fn, _k in [
            ("marketaux",  fetch_marketaux_news,  "marketaux"),
            ("benzinga",   fetch_benzinga_news,   "benzinga"),
            ("sec_api",    fetch_secapi_filings,  "sec_api"),
            ("newsapi_ai", fetch_newsapi_ai_news, "newsapi_ai"),
        ]:
            try:
                extra_news[_np] = _fn(symbol, keys.get(_k, ""), refresh_token)
            except Exception:
                pass
    else:
        _news_jobs = {}
        with ThreadPoolExecutor(max_workers=4) as _ne:
            for _np, _fn, _k in [
                ("marketaux",  fetch_marketaux_news,  "marketaux"),
                ("benzinga",   fetch_benzinga_news,   "benzinga"),
                ("sec_api",    fetch_secapi_filings,  "sec_api"),
                ("newsapi_ai", fetch_newsapi_ai_news, "newsapi_ai"),
            ]:
                if keys.get(_k):
                    _news_jobs[_ne.submit(_fn, symbol, keys[_k], refresh_token)] = _np
            for _fut in as_completed(_news_jobs):
                _pv = _news_jobs[_fut]
                try:
                    extra_news[_pv] = _fut.result() or []
                except Exception:
                    pass
    output["_news_extra"] = extra_news
    return output


def normalize_news_item(
    headline,
    url,
    datetime_unix,
    source,
    category,
    sentiment,
    summary=None,
    provider=None,
):
    """Schema موحد لكل خبر من أي مصدر."""
    def _is_n(v):
        try:
            return v is not None and float(v) == float(v)
        except (TypeError, ValueError):
            return False
    return {
        "headline":     str(headline or "").strip() or None,
        "url":          str(url or "").strip() or None,
        "datetime":     int(datetime_unix) if _is_n(datetime_unix) else None,
        "source":       str(source or "").strip() or None,
        "category":     str(category or "").strip() or None,
        "sentiment":    float(sentiment) if _is_n(sentiment) else None,
        "summary":      str(summary or "").strip()[:400] or None,
        "provider_tag": str(provider or ""),
    }


def parse_news_datetime(value: Any) -> int | None:
    if is_num(value):
        return int(float(value))
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def coerce_news_item(provider: str, item: dict[str, Any]) -> dict[str, Any]:
    return normalize_news_item(
        headline=item.get("headline") or item.get("title") or item.get("name"),
        url=item.get("url") or item.get("link") or item.get("article_url"),
        datetime_unix=parse_news_datetime(
            item.get("datetime")
            or item.get("published_at")
            or item.get("publishedAt")
            or item.get("dateTime")
            or item.get("created")
        ),
        source=item.get("source") or item.get("site") or NEWS_PROVIDER_LABELS.get(provider, provider),
        category=item.get("category") or item.get("formType") or item.get("type") or "General",
        sentiment=item.get("sentiment") or item.get("sentiment_score"),
        summary=item.get("summary") or item.get("description") or item.get("teaser") or item.get("body"),
        provider=item.get("provider_tag") or provider,
    )


def news_merge_key(item: dict[str, Any]) -> str:
    url = str(item.get("url") or "").strip().lower()
    if url:
        return f"url:{re.sub(r'[#?].*$', '', url)}"
    headline = str(item.get("headline") or "").lower()
    headline = re.sub(r"[^a-z0-9]+", " ", headline).strip()
    if headline:
        return f"title:{headline[:120]}"
    return f"empty:{id(item)}"


def news_field_score(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, str):
        return len(value.strip())
    return 1


def merge_news_records(base: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    providers = list(merged.get("provider_tags") or [merged.get("provider_tag") or ""])
    incoming_provider = incoming.get("provider_tag") or ""
    if incoming_provider and incoming_provider not in providers:
        providers.append(incoming_provider)
    merged["provider_tags"] = [provider for provider in providers if provider]
    merged["provider_tag"] = merged["provider_tags"][0] if merged["provider_tags"] else incoming_provider
    merged["source_count"] = len(merged["provider_tags"])

    for field in ["headline", "url", "source", "category", "summary"]:
        if news_field_score(incoming.get(field)) > news_field_score(merged.get(field)):
            merged[field] = incoming.get(field)

    merged["datetime"] = max(
        to_num(merged.get("datetime"), 0) or 0,
        to_num(incoming.get("datetime"), 0) or 0,
    ) or None

    sentiment_values = [
        value
        for value in [merged.get("sentiment"), incoming.get("sentiment")]
        if isinstance(value, (int, float))
    ]
    merged["sentiment"] = sum(sentiment_values) / len(sentiment_values) if sentiment_values else None
    return merged


@st.cache_data(ttl=TTL_NEWS_SECONDS, show_spinner=False)
def fetch_marketaux_news(symbol, api_key, refresh_token):
    del refresh_token
    if not api_key:
        return []
    try:
        throttle_provider("marketaux")
        params = {
            "symbols": symbol,
            "filter_entities": "true",
            "language": "en",
            "api_token": api_key,
            "limit": 20,
        }
        resp = get_http_session().get(
            "https://api.marketaux.com/v1/news/all",
            params=params,
            timeout=PROVIDER_TIMEOUTS["marketaux"],
        )
        resp.raise_for_status()
        payload = resp.json()
        rows = payload.get("data") if isinstance(payload.get("data"), list) else []
        result = []
        for item in rows:
            if not isinstance(item, dict):
                continue
            entities = item.get("entities") or []
            sentiment = None
            for ent in entities:
                if isinstance(ent, dict):
                    s = to_num(ent.get("sentiment_score"))
                    if is_num(s):
                        sentiment = float(s)
                        break
            pub = item.get("published_at") or ""
            dt_unix = None
            if pub:
                try:
                    from datetime import datetime as _dt
                    dt_unix = int(_dt.fromisoformat(pub.replace("Z", "+00:00")).timestamp())
                except (ValueError, OverflowError):
                    pass
            src_obj = item.get("source")
            source_name = src_obj.get("name") if isinstance(src_obj, dict) else str(src_obj or "")
            result.append(normalize_news_item(
                headline=item.get("title"),
                url=item.get("url"),
                datetime_unix=dt_unix,
                source=source_name or "Marketaux",
                category="General",
                sentiment=sentiment,
                summary=item.get("description"),
                provider="marketaux",
            ))
        return result
    except Exception:
        return []


@st.cache_data(ttl=TTL_NEWS_SECONDS, show_spinner=False)
def fetch_benzinga_news(symbol, api_key, refresh_token):
    del refresh_token
    if not api_key:
        return []
    try:
        throttle_provider("benzinga")
        params = {
            "token": api_key,
            "tickers": symbol,
            "pageSize": 20,
            "displayOutput": "full",
        }
        resp = get_http_session().get(
            "https://api.benzinga.com/api/v2/news",
            params=params,
            timeout=PROVIDER_TIMEOUTS["benzinga"],
        )
        resp.raise_for_status()
        payload = resp.json()
        rows = payload if isinstance(payload, list) else (payload.get("data") or [])
        result = []
        for item in rows:
            if not isinstance(item, dict):
                continue
            created = item.get("created") or ""
            dt_unix = None
            if created:
                try:
                    from datetime import datetime as _dt
                    dt_unix = int(_dt.fromisoformat(created.replace("Z", "+00:00")).timestamp())
                except (ValueError, OverflowError):
                    pass
            channels = item.get("channels") or []
            cat = channels[0].get("name") if channels and isinstance(channels[0], dict) else "General"
            result.append(normalize_news_item(
                headline=item.get("title"),
                url=item.get("url"),
                datetime_unix=dt_unix,
                source="Benzinga",
                category=cat,
                sentiment=None,
                summary=item.get("teaser"),
                provider="benzinga",
            ))
        return result
    except Exception:
        return []


@st.cache_data(ttl=TTL_NEWS_SECONDS, show_spinner=False)
def fetch_secapi_filings(symbol, api_key, refresh_token):
    del refresh_token
    if not api_key:
        return []
    try:
        throttle_provider("sec_api")
        from datetime import date as _date, timedelta as _td, timezone as _tz
        end_day = _date.today()
        start_day = end_day - _td(days=90)
        params = {
            "query": f"ticker:{symbol}",
            "dateRange": "custom",
            "startdt": start_day.isoformat(),
            "enddt": end_day.isoformat(),
            "forms": "8-K,10-K,10-Q",
        }
        req_headers = {"Authorization": api_key}
        resp = get_http_session().get(
            "https://api.sec-api.io",
            params=params,
            headers=req_headers,
            timeout=PROVIDER_TIMEOUTS["sec_api"],
        )
        resp.raise_for_status()
        payload = resp.json()
        filings = payload.get("filings") or payload.get("hits", {}).get("hits") or []
        result = []
        for item in filings:
            if not isinstance(item, dict):
                continue
            src = item.get("_source", item)
            filed_at = src.get("filedAt") or ""
            dt_unix = None
            if filed_at:
                try:
                    from datetime import datetime as _dt
                    dt_unix = int(_dt.fromisoformat(filed_at[:19]).replace(tzinfo=_tz.utc).timestamp())
                except (ValueError, OverflowError):
                    pass
            form_type = src.get("formType") or "SEC Filing"
            company = src.get("companyName") or symbol
            result.append(normalize_news_item(
                headline=f"{form_type} — {company}",
                url=src.get("linkToFilingDetails"),
                datetime_unix=dt_unix,
                source="SEC EDGAR",
                category=form_type,
                sentiment=None,
                summary=f"Form {form_type} filed on {filed_at[:10]}",
                provider="sec_api",
            ))
        return result[:20]
    except Exception:
        return []


@st.cache_data(ttl=TTL_NEWS_SECONDS, show_spinner=False)
def fetch_newsapi_ai_news(symbol, api_key, refresh_token):
    del refresh_token
    if not api_key:
        return []
    try:
        throttle_provider("newsapi_ai")
        req_body = {
            "action": "getArticles",
            "keyword": symbol,
            "apiKey": api_key,
            "lang": "eng",
            "count": 20,
            "articlesSortBy": "date",
            "articlesSortByAsc": False,
            "includeArticleSentiment": True,
        }
        resp = get_http_session().post(
            "https://eventregistry.org/api/v1/article/getArticles",
            json=req_body,
            timeout=PROVIDER_TIMEOUTS["newsapi_ai"],
        )
        resp.raise_for_status()
        payload = resp.json()
        articles_obj = payload.get("articles") or {}
        rows = articles_obj.get("results") if isinstance(articles_obj, dict) else []
        rows = rows or []
        result = []
        for item in rows:
            if not isinstance(item, dict):
                continue
            date_time = item.get("dateTime") or ""
            dt_unix = None
            if date_time:
                try:
                    from datetime import datetime as _dt
                    dt_unix = int(_dt.fromisoformat(date_time.replace("Z", "+00:00")).timestamp())
                except (ValueError, OverflowError):
                    pass
            src = item.get("source") or {}
            source_name = src.get("title") if isinstance(src, dict) else str(src or "")
            sentiment_raw = to_num(item.get("sentiment"))
            cats = item.get("categories") or []
            cat = cats[0] if cats else "General"
            body_text = item.get("body") or ""
            result.append(normalize_news_item(
                headline=item.get("title"),
                url=item.get("url"),
                datetime_unix=dt_unix,
                source=source_name or "NewsAPI.ai",
                category=cat,
                sentiment=float(sentiment_raw) if is_num(sentiment_raw) else None,
                summary=body_text[:300] if body_text else None,
                provider="newsapi_ai",
            ))
        return result
    except Exception:
        return []


def timestamp_ms_to_iso(value: Any) -> str:
    stamp = to_num(value)
    if not is_num(stamp):
        return ""
    try:
        return datetime.fromtimestamp(stamp / 1000.0, tz=timezone.utc).isoformat()
    except (OSError, OverflowError, ValueError):
        return ""


def parse_binance_klines(payload: Any) -> pd.DataFrame:
    rows = []
    if not isinstance(payload, list):
        return pd.DataFrame(rows)
    for item in payload:
        if not isinstance(item, list) or len(item) < 11:
            continue
        open_time = to_num(item[0])
        rows.append(
            {
                "open_time": pd.to_datetime(open_time, unit="ms", utc=True) if is_num(open_time) else pd.NaT,
                "open": to_num(item[1]),
                "high": to_num(item[2]),
                "low": to_num(item[3]),
                "close": to_num(item[4]),
                "volume": to_num(item[5]),
                "close_time": timestamp_ms_to_iso(item[6]),
                "quote_volume": to_num(item[7]),
                "trades": to_num(item[8]),
                "taker_buy_base": to_num(item[9]),
                "taker_buy_quote": to_num(item[10]),
            }
        )
    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame = frame.dropna(subset=["open_time", "close"]).sort_values("open_time").reset_index(drop=True)
    return frame


def parse_binance_trades(payload: Any) -> pd.DataFrame:
    rows = []
    if not isinstance(payload, list):
        return pd.DataFrame(rows)
    for item in payload:
        if not isinstance(item, dict):
            continue
        price = to_num(item.get("price") or item.get("p"))
        qty = to_num(item.get("qty") or item.get("q"))
        quote_qty = coalesce_num(item.get("quoteQty"), price * qty if is_num(price) and is_num(qty) else None)
        timestamp = coalesce_num(item.get("time"), item.get("T"))
        is_buyer_maker = bool(item.get("isBuyerMaker", item.get("m", False)))
        rows.append(
            {
                "time": pd.to_datetime(timestamp, unit="ms", utc=True) if is_num(timestamp) else pd.NaT,
                "price": price,
                "quantity": qty,
                "quote_quantity": quote_qty,
                "side": "Sell Taker" if is_buyer_maker else "Buy Taker",
                "trade_id": item.get("id") or item.get("t") or item.get("a"),
            }
        )
    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame = frame.dropna(subset=["time", "price", "quantity"]).sort_values("time").reset_index(drop=True)
    return frame


def parse_binance_depth(payload: Any, levels: int) -> pd.DataFrame:
    rows = []
    if not isinstance(payload, dict):
        return pd.DataFrame(rows)
    for side, key in [("Bid", "bids"), ("Ask", "asks")]:
        for index, item in enumerate((payload.get(key) or [])[:levels], start=1):
            if not isinstance(item, list) or len(item) < 2:
                continue
            price = to_num(item[0])
            qty = to_num(item[1])
            if not is_num(price) or not is_num(qty):
                continue
            rows.append(
                {
                    "side": side,
                    "level": index,
                    "price": price,
                    "quantity": qty,
                    "notional": price * qty,
                }
            )
    return pd.DataFrame(rows)


def klines_to_history_rows(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    rows = []
    for _, row in frame.iterrows():
        if pd.isna(row.get("open_time")):
            continue
        rows.append(
            {
                "date": pd.Timestamp(row["open_time"]).date().isoformat(),
                "close": row.get("close"),
                "high": row.get("high"),
                "low": row.get("low"),
                "volume": row.get("volume"),
            }
        )
    return rows


def get_symbol_filter(exchange_info: dict[str, Any], filter_type: str) -> dict[str, Any]:
    symbols = exchange_info.get("symbols") if isinstance(exchange_info, dict) else []
    symbol_info = symbols[0] if isinstance(symbols, list) and symbols and isinstance(symbols[0], dict) else {}
    filters = symbol_info.get("filters") if isinstance(symbol_info, dict) else []
    if not isinstance(filters, list):
        return {}
    for item in filters:
        if isinstance(item, dict) and item.get("filterType") == filter_type:
            return item
    return {}


def build_binance_metrics(
    symbol: str,
    raw: dict[str, Any],
    klines: pd.DataFrame,
    trades: pd.DataFrame,
    depth: pd.DataFrame,
    depth_levels: int,
) -> dict[str, Any]:
    ticker = raw.get("ticker_24hr") if isinstance(raw.get("ticker_24hr"), dict) else {}
    price_payload = raw.get("price") if isinstance(raw.get("price"), dict) else {}
    book = raw.get("book_ticker") if isinstance(raw.get("book_ticker"), dict) else {}
    avg_price_payload = raw.get("avg_price") if isinstance(raw.get("avg_price"), dict) else {}
    exchange_info = raw.get("exchange_info") if isinstance(raw.get("exchange_info"), dict) else {}

    price = coalesce_num(ticker.get("lastPrice"), price_payload.get("price"), klines["close"].iloc[-1] if not klines.empty else None)
    weighted_avg = coalesce_num(ticker.get("weightedAvgPrice"), avg_price_payload.get("price"))
    bid = coalesce_num(book.get("bidPrice"))
    ask = coalesce_num(book.get("askPrice"))
    bid_qty = coalesce_num(book.get("bidQty"))
    ask_qty = coalesce_num(book.get("askQty"))
    mid = avg(bid, ask)
    spread = ask - bid if is_num(ask) and is_num(bid) else None
    spread_pct = safe_div(spread, mid)

    bid_depth = depth[depth["side"] == "Bid"] if not depth.empty else pd.DataFrame()
    ask_depth = depth[depth["side"] == "Ask"] if not depth.empty else pd.DataFrame()
    bid_notional = float(bid_depth["notional"].sum()) if not bid_depth.empty else None
    ask_notional = float(ask_depth["notional"].sum()) if not ask_depth.empty else None
    bid_quantity = float(bid_depth["quantity"].sum()) if not bid_depth.empty else None
    ask_quantity = float(ask_depth["quantity"].sum()) if not ask_depth.empty else None
    imbalance = safe_div((bid_notional or 0) - (ask_notional or 0), (bid_notional or 0) + (ask_notional or 0))

    buy_quote = float(trades.loc[trades["side"] == "Buy Taker", "quote_quantity"].sum()) if not trades.empty else None
    sell_quote = float(trades.loc[trades["side"] == "Sell Taker", "quote_quantity"].sum()) if not trades.empty else None
    pressure_total = (buy_quote or 0) + (sell_quote or 0)
    buy_pressure = buy_quote / pressure_total if pressure_total else None
    trade_vwap = safe_div((trades["price"] * trades["quantity"]).sum(), trades["quantity"].sum()) if not trades.empty else None
    avg_trade_size = float(trades["quote_quantity"].mean()) if not trades.empty else None

    history_stats = calculate_history_insights(klines_to_history_rows(klines))
    lot_filter = get_symbol_filter(exchange_info, "LOT_SIZE")
    price_filter = get_symbol_filter(exchange_info, "PRICE_FILTER")

    liquidity_score = average_percent(
        [
            score_range_value(bid_notional, 2_000_000, 150_000),
            score_range_value(ask_notional, 2_000_000, 150_000),
            score_range_value(spread_pct, 0.0005, 0.003, lower_is_better=True),
            score_range_value(abs(imbalance) if is_num(imbalance) else None, 0.15, 0.45, lower_is_better=True),
        ]
    )
    flow_score = average_percent(
        [
            clamp((buy_pressure or 0.5) * 100, 0, 100) if is_num(buy_pressure) else None,
            score_range_value(trade_vwap / weighted_avg - 1 if is_num(trade_vwap) and is_num(weighted_avg) and weighted_avg else None, 0.002, -0.002),
            score_range_value(to_num(ticker.get("count")), 100_000, 10_000),
        ]
    )
    bookmap_score = weighted_average_percent(
        [
            (liquidity_score, 0.35),
            (flow_score, 0.25),
            (history_stats.get("trend_score"), 0.20),
            (history_stats.get("technical_momentum_score"), 0.20),
        ]
    )

    return {
        "symbol": symbol,
        "price": price,
        "weighted_avg_price": weighted_avg,
        "bid": bid,
        "ask": ask,
        "bid_qty": bid_qty,
        "ask_qty": ask_qty,
        "mid": mid,
        "spread": spread,
        "spread_pct": spread_pct,
        "price_change": coalesce_num(ticker.get("priceChange")),
        "price_change_pct": percent_field(ticker.get("priceChangePercent")),
        "high_24h": coalesce_num(ticker.get("highPrice")),
        "low_24h": coalesce_num(ticker.get("lowPrice")),
        "base_volume_24h": coalesce_num(ticker.get("volume")),
        "quote_volume_24h": coalesce_num(ticker.get("quoteVolume")),
        "trade_count_24h": coalesce_num(ticker.get("count")),
        "depth_levels": depth_levels,
        "bid_depth_quantity": bid_quantity,
        "ask_depth_quantity": ask_quantity,
        "bid_depth_notional": bid_notional,
        "ask_depth_notional": ask_notional,
        "book_imbalance": imbalance,
        "buy_quote": buy_quote,
        "sell_quote": sell_quote,
        "buy_pressure": buy_pressure,
        "trade_vwap": trade_vwap,
        "avg_trade_size": avg_trade_size,
        "last_update_id": raw.get("depth", {}).get("lastUpdateId") if isinstance(raw.get("depth"), dict) else None,
        "min_qty": coalesce_num(lot_filter.get("minQty")),
        "step_size": coalesce_num(lot_filter.get("stepSize")),
        "tick_size": coalesce_num(price_filter.get("tickSize")),
        "liquidity_score": liquidity_score,
        "flow_score": flow_score,
        "bookmap_score": bookmap_score,
        **{f"tech_{key}": value for key, value in history_stats.items()},
    }


def fetch_binance_symbol_bundle(symbol: str, controls: BinanceControls, refresh_bucket: int, controls_key: tuple[Any, ...]) -> dict[str, Any]:
    symbol = normalize_symbol_rest(symbol)
    slow_refresh_bucket = refresh_bucket // 3
    fetch_jobs = {
        "depth": (fetch_depth, (symbol, controls.depth_limit, refresh_bucket, controls_key)),
        "trades": (fetch_trades, (symbol, refresh_bucket, controls_key)),
        "agg_trades": (fetch_agg_trades, (symbol, refresh_bucket, controls_key)),
        "ticker": (fetch_ticker, (symbol, refresh_bucket, controls_key)),
        "book_ticker": (fetch_book_ticker, (symbol, refresh_bucket, controls_key)),
        "klines": (fetch_klines, (symbol, controls.interval, slow_refresh_bucket, controls_key)),
        "exchange_info": (fetch_exchange_info, (symbol, controls_key)),
    }
    raw: dict[str, Any] = {}
    errors: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=min(7, len(fetch_jobs))) as executor:
        futures = {executor.submit(func, *args): name for name, (func, args) in fetch_jobs.items()}
        for future in as_completed(futures):
            name = futures[future]
            try:
                payload = future.result()
                if name == "ticker" and isinstance(payload, dict):
                    raw.update(payload)
                else:
                    raw[name] = payload
            except Exception as exc:  # noqa: BLE001
                errors[name] = str(exc)
    klines = parse_binance_klines(raw.get("klines"))
    trades = parse_binance_trades(raw.get("trades"))
    agg_trades = parse_binance_trades(raw.get("agg_trades"))
    depth = parse_binance_depth(raw.get("depth"), controls.book_levels)
    metrics = build_binance_metrics(symbol, raw, klines, trades, depth, controls.book_levels)
    ws_base = BINANCE_WS_MARKET_DATA_ONLY if controls.market_data_only else BINANCE_WS_BASE_9443
    return {
        "symbol": symbol,
        "raw": raw,
        "errors": errors,
        "metrics": metrics,
        "klines": klines,
        "trades": trades,
        "agg_trades": agg_trades,
        "depth": depth,
        "urls": {
            "bookmap_ws": combined_stream_url(bookmap_like_streams(symbol, controls.interval, controls.speed_ms), base=ws_base, microseconds=controls.microseconds),
            "simple_chart_ws": combined_stream_url(simple_chart_streams(symbol, controls.interval), base=ws_base),
            "depth_ws": raw_stream_url(diff_depth_stream(symbol, controls.speed_ms), base=ws_base),
            "depth_snapshot_rest": depth_snapshot_rest_url(symbol, controls.depth_limit),
            "recent_trades_rest": recent_trades_rest_url(symbol),
            "agg_trades_rest": agg_trades_rest_url(symbol),
            "klines_rest": klines_rest_url(symbol, controls.interval),
            "avg_price_rest": avg_price_rest_url(symbol),
            "ticker_24hr_rest": ticker_24hr_rest_url(symbol),
            "ticker_price_rest": ticker_price_rest_url(symbol),
            "book_ticker_rest": book_ticker_rest_url(symbol),
            "exchange_info_rest": exchange_info_rest_url(symbol),
            "server_time_rest": server_time_rest_url(),
            "ping_rest": ping_rest_url(),
        },
        "streams": {
            "bookmap": bookmap_like_streams(symbol, controls.interval, controls.speed_ms),
            "simple_chart": simple_chart_streams(symbol, controls.interval),
            "partial_book": partial_book_depth_stream(symbol, controls.book_levels, controls.speed_ms),
            "agg_trade": aggregate_trade_stream(symbol),
            "mini_ticker": mini_ticker_stream(symbol),
            "avg_price": avg_price_stream(symbol),
        },
    }


def fetch_binance_dashboard(controls: BinanceControls) -> dict[str, dict[str, Any]]:
    refresh_bucket = binance_refresh_bucket(controls.refresh_seconds)
    controls_key = binance_controls_cache_key(controls)
    bundles: dict[str, dict[str, Any]] = {}
    for symbol in controls.symbols:
        bundles[symbol] = fetch_binance_symbol_bundle(symbol, controls, refresh_bucket, controls_key)
    return bundles


def collect_binance_ws_probe(symbol: str, controls: BinanceControls, max_messages: int = 3) -> dict[str, Any]:
    if websocket_client is None:
        return {"ok": False, "error": "websocket-client is not installed", "messages": []}
    ws_base = BINANCE_WS_MARKET_DATA_ONLY if controls.market_data_only else BINANCE_WS_BASE_9443
    url = combined_stream_url(bookmap_like_streams(symbol, controls.interval, controls.speed_ms), base=ws_base, microseconds=controls.microseconds)
    messages = []
    try:
        ws = websocket_client.create_connection(url, timeout=1)
        ws.settimeout(1)
        for _ in range(max_messages):
            message = ws.recv()
            if not message:
                continue
            parsed = json.loads(message)
            data = parsed.get("data", parsed) if isinstance(parsed, dict) else parsed
            event_type = data.get("e") if isinstance(data, dict) else None
            stream_name = parsed.get("stream") if isinstance(parsed, dict) else None
            messages.append({"stream": stream_name, "event": event_type, "payload": data})
        ws.close()
        return {"ok": True, "url": url, "messages": messages, "error": ""}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "url": url, "messages": messages, "error": str(exc)}


def choose_text(sources: dict[str, dict[str, Any]], field: str) -> tuple[Any, str | None]:
    candidates: list[tuple[int, int, str, Any]] = []
    order = {name: index for index, name in enumerate(DEFAULT_PROVIDER_ORDER)}
    for provider, payload in sources.items():
        value = payload.get("texts", {}).get(field)
        if is_value_present(value):
            candidates.append((order.get(provider, 99), -payload.get("coverage", 0), provider, value))
    if candidates:
        candidates.sort()
        _, _, provider, value = candidates[0]
        return value, provider
    return None, None


def choose_metric(sources: dict[str, dict[str, Any]], field: str) -> tuple[float | None, str | None]:
    order_list = FIELD_PROVIDER_ORDER.get(field, MARKET_PROVIDER_ORDER if field in MARKET_FIELDS else DEFAULT_PROVIDER_ORDER)
    order = {name: index for index, name in enumerate(order_list)}
    candidates: list[tuple[float, int, int, str, float]] = []
    for provider, payload in sources.items():
        value = payload.get("metrics", {}).get(field)
        if is_num(value):
            status_penalty = 0 if payload.get("status") == "ok" else 1 if payload.get("status") == "partial" else 2
            health_state = LIVE_PROVIDER_HEALTH.get(provider) if "LIVE_PROVIDER_HEALTH" in globals() else None
            health_penalty = 0.0
            if isinstance(health_state, dict):
                health_penalty = max(0.0, 1.0 - float(health_state.get("health", 1.0))) * 4.0
            candidates.append((order.get(provider, 99) + health_penalty, status_penalty, -payload.get("coverage", 0), provider, float(value)))
    if candidates:
        candidates.sort()
        _, _, _, provider, value = candidates[0]
        return value, provider
    return None, None


def choose_history(sources: dict[str, dict[str, Any]]) -> tuple[list[dict[str, Any]], str | None]:
    ranked: list[tuple[int, int, int, str, list[dict[str, Any]]]] = []
    provider_rank = {name: index for index, name in enumerate(MARKET_PROVIDER_ORDER)}
    for provider, payload in sources.items():
        rows = payload.get("history", []) or []
        if rows:
            status_bonus = 1 if payload.get("status") == "ok" else 0
            ranked.append((len(rows), status_bonus, -provider_rank.get(provider, 99), provider, rows))
    if not ranked:
        return [], None
    ranked.sort(reverse=True)
    _, _, _, provider, rows = ranked[0]
    return rows, provider


def choose_annuals(sources: dict[str, dict[str, Any]]) -> tuple[list[dict[str, Any]], str | None]:
    ranked: list[tuple[int, int, int, str, list[dict[str, Any]]]] = []
    provider_rank = {name: index for index, name in enumerate(DEFAULT_PROVIDER_ORDER)}
    for provider, payload in sources.items():
        rows = payload.get("annuals", []) or []
        if rows:
            status_bonus = 1 if payload.get("status") == "ok" else 0
            ranked.append((len(rows), status_bonus, -provider_rank.get(provider, 99), provider, rows))
    if not ranked:
        return [], None
    ranked.sort(reverse=True)
    _, _, _, provider, rows = ranked[0]
    return rows, provider


def merge_all_news(sources):
    """Merge every news API into one enriched feed; providers fill missing fields for each other."""
    merged_by_key: dict[str, dict[str, Any]] = {}
    providers_used = []
    extra = sources.get("_news_extra") or {}
    if not isinstance(extra, dict):
        extra = {}

    provider_rows: list[tuple[str, list[dict[str, Any]]]] = []
    for provider in NEWS_PROVIDER_ORDER:
        if provider in extra:
            rows = extra.get(provider) or []
        else:
            rows = sources.get(provider, {}).get("news", []) or []
        if rows:
            providers_used.append(provider)
            provider_rows.append((provider, rows))

    for provider, rows in provider_rows:
        for raw_item in rows:
            if not isinstance(raw_item, dict):
                continue
            item = coerce_news_item(provider, raw_item)
            key = news_merge_key(item)
            if key in merged_by_key:
                merged_by_key[key] = merge_news_records(merged_by_key[key], item)
            else:
                item["provider_tags"] = [item["provider_tag"]] if item.get("provider_tag") else []
                item["source_count"] = len(item["provider_tags"])
                merged_by_key[key] = item

    all_items = list(merged_by_key.values())
    all_items.sort(
        key=lambda item: (
            to_num(item.get("datetime"), 0) or 0,
            item.get("source_count", 0),
            news_field_score(item.get("summary")),
        ),
        reverse=True,
    )
    return all_items, providers_used


def choose_news(sources):
    """Legacy wrapper — يُعيد أول مصدر متاح (للتوافقية)."""
    rows, providers = merge_all_news(sources)
    provider_label = providers[0] if providers else None
    return rows, provider_label


def choose_recommendations(sources: dict[str, dict[str, Any]]) -> tuple[list[dict[str, Any]], str | None]:
    for provider in ["finnhub", "fmp"]:
        rows = sources.get(provider, {}).get("recommendations", []) or []
        if rows:
            return rows, provider
    return [], None


def score_metric(value: float | None, thresholds: tuple[float, float], lower_is_better: bool = True, upside_mode: bool = False) -> float | None:
    if not is_num(value):
        return None
    if upside_mode:
        if value >= thresholds[0]:
            return 1.0
        if value <= thresholds[1]:
            return -1.0
        return 0.0
    if lower_is_better:
        if value <= thresholds[0]:
            return 1.0
        if value <= thresholds[1]:
            return 0.0
        return -1.0
    if value >= thresholds[0]:
        return 1.0
    if value >= thresholds[1]:
        return 0.0
    return -1.0


def score_to_percent(score: float | None) -> float | None:
    if not is_num(score):
        return None
    return (score + 1.0) * 50.0


def average_percent(values: list[Any]) -> float | None:
    valid = [to_num(value) for value in values if is_num(to_num(value))]
    if not valid:
        return None
    return sum(valid) / len(valid)


def weighted_average_percent(items: list[tuple[Any, float]]) -> float | None:
    total = 0.0
    weight_total = 0.0
    for value, weight in items:
        number = to_num(value)
        if is_num(number) and weight > 0:
            total += number * weight
            weight_total += weight
    if weight_total <= 0:
        return None
    return total / weight_total


def verdict_from_score(score: float | None) -> str:
    if not is_num(score):
        return "Neutral / Incomplete"
    if score >= 80:
        return "Strong Buy setup"
    if score >= 60:
        return "Buy setup"
    if score >= 40:
        return "Neutral / Watch"
    if score >= 25:
        return "Defensive / Caution"
    return "Avoid / Wait"


def decision_label_from_score(score: float | None) -> str:
    if not is_num(score):
        return "Insufficient data"
    if score >= 80:
        return "Strong Buy"
    if score >= 60:
        return "Buy"
    if score >= 40:
        return "Neutral"
    if score >= 25:
        return "Caution"
    return "Avoid"


def score_rsi_value(value: Any) -> float | None:
    rsi = to_num(value)
    if not is_num(rsi):
        return None
    if 45 <= rsi <= 68:
        return 100.0
    if 35 <= rsi < 45 or 68 < rsi <= 75:
        return 72.0
    if 30 <= rsi < 35 or 75 < rsi <= 82:
        return 48.0
    return 28.0


def score_range_value(value: Any, strong: float, weak: float, lower_is_better: bool = False) -> float | None:
    number = to_num(value)
    if not is_num(number):
        return None
    if lower_is_better:
        if number <= strong:
            return 100.0
        if number <= weak:
            return 65.0
        return 30.0
    if number >= strong:
        return 100.0
    if number >= weak:
        return 65.0
    return 30.0


def count_active_alerts(metrics: dict[str, Any]) -> int:
    checks = [
        is_num(metrics.get("rsi_14")) and (metrics["rsi_14"] < 30 or metrics["rsi_14"] > 70),
        is_num(metrics.get("volume_ratio")) and metrics["volume_ratio"] >= 1.8,
        str(metrics.get("breakout_signal") or "").lower() in {"breakout", "breakdown", "testing resistance", "retesting support"},
        "downtrend" in str(metrics.get("trend_state") or "").lower(),
        is_num(metrics.get("macd_hist")) and metrics["macd_hist"] < 0 and is_num(metrics.get("trend_score")) and metrics["trend_score"] < 50,
    ]
    return sum(1 for item in checks if item)


def update_composite_scores(metrics: dict[str, Any], source_map: dict[str, str]) -> None:
    metrics["fundamental_score"] = weighted_average_percent(
        [
            (metrics.get("value_score"), 0.34),
            (metrics.get("quality_score"), 0.38),
            (metrics.get("growth_score"), 0.28),
        ]
    )
    metrics["final_ai_score"] = weighted_average_percent(
        [
            (metrics.get("fundamental_score"), 0.30),
            (metrics.get("trend_score"), 0.18),
            (metrics.get("momentum_score"), 0.18),
            (metrics.get("volume_score"), 0.10),
            (metrics.get("relative_strength_score"), 0.14),
            (metrics.get("risk_score"), 0.10),
        ]
    )
    metrics["overall_score"] = metrics.get("final_ai_score") or metrics.get("overall_score")
    metrics["overall_verdict"] = verdict_from_score(metrics.get("overall_score"))
    metrics["decision_label"] = decision_label_from_score(metrics.get("final_ai_score"))
    metrics["alert_count"] = count_active_alerts(metrics)
    for key in [
        "fundamental_score",
        "final_ai_score",
        "overall_score",
        "overall_verdict",
        "decision_label",
        "alert_count",
    ]:
        if is_value_present(metrics.get(key)):
            source_map[key] = "calculated"


def calculate_smart_dcf(
    free_cash_flow: float | None,
    shares: float | None,
    assumptions: dict[str, float],
    current_price: float | None,
    revenue_growth: float | None,
    earnings_growth: float | None,
) -> dict[str, float | None]:
    if not is_num(free_cash_flow) or not is_num(shares) or shares <= 0:
        return {"intrinsic_value": None, "fair_value_after_mos": None, "dcf_upside": None, "used_growth": None}
    growth_candidates = [assumptions["growth"]]
    if is_num(revenue_growth):
        growth_candidates.append(clamp(revenue_growth, -0.04, 0.18) or revenue_growth)
    if is_num(earnings_growth):
        growth_candidates.append(clamp(earnings_growth, -0.04, 0.18) or earnings_growth)
    normalized_growth = avg(*growth_candidates)
    normalized_growth = clamp(normalized_growth, -0.04, 0.18) if is_num(normalized_growth) else assumptions["growth"]
    discount = assumptions["discount"]
    terminal = assumptions["terminal"]
    if discount <= terminal:
        return {"intrinsic_value": None, "fair_value_after_mos": None, "dcf_upside": None, "used_growth": normalized_growth}
    projected_fcf = free_cash_flow
    present_value = 0.0
    years = int(assumptions["years"])
    for year in range(1, years + 1):
        projected_fcf *= 1 + normalized_growth
        present_value += projected_fcf / ((1 + discount) ** year)
    terminal_value = (projected_fcf * (1 + terminal)) / (discount - terminal)
    present_value += terminal_value / ((1 + discount) ** years)
    intrinsic_value = present_value / shares
    fair_value_after_mos = intrinsic_value * (1 - assumptions["mos"])
    upside = (fair_value_after_mos / current_price - 1) if is_num(current_price) and current_price else None
    return {
        "intrinsic_value": intrinsic_value,
        "fair_value_after_mos": fair_value_after_mos,
        "dcf_upside": upside,
        "used_growth": normalized_growth,
    }


def calculate_history_insights(history: list[dict[str, Any]]) -> dict[str, Any]:
    defaults: dict[str, Any] = {
        "momentum_6m": None,
        "momentum_1y": None,
        "max_drawdown": None,
        "volatility_1y": None,
        "ema_20": None,
        "ema_50": None,
        "sma_200": None,
        "price_vs_sma_200": None,
        "trend_score": None,
        "trend_state": "No data",
        "crossover_signal": "No data",
        "rsi_14": None,
        "macd_line": None,
        "macd_signal": None,
        "macd_hist": None,
        "technical_momentum_score": None,
        "volume": None,
        "avg_volume_20": None,
        "avg_volume_50": None,
        "volume_ratio": None,
        "dollar_volume": None,
        "vwap_60": None,
        "volume_score": None,
        "volume_spike": "No data",
        "liquidity_state": "No data",
        "support_level": None,
        "resistance_level": None,
        "distance_to_support": None,
        "distance_to_resistance": None,
        "breakout_signal": "No data",
        "range_state": "No data",
        "atr_14": None,
        "atr_pct": None,
        "stop_loss": None,
        "stop_loss_distance": None,
        "risk_reward_to_resistance": None,
        "risk_score": None,
    }
    frame = pd.DataFrame(history)
    if frame.empty or len(frame) < 10:
        return defaults
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["close"] = frame["close"].apply(to_num)
    for column in ["high", "low", "volume"]:
        if column in frame.columns:
            frame[column] = frame[column].apply(to_num)
    frame = frame.dropna(subset=["date", "close"])
    if frame.empty or len(frame) < 10:
        return defaults
    frame = frame.sort_values("date").reset_index(drop=True)
    if "high" not in frame or frame["high"].isna().all():
        frame["high"] = frame["close"]
    else:
        frame["high"] = frame["high"].fillna(frame["close"])
    if "low" not in frame or frame["low"].isna().all():
        frame["low"] = frame["close"]
    else:
        frame["low"] = frame["low"].fillna(frame["close"])
    if "volume" not in frame:
        frame["volume"] = None
    diffs = frame["date"].diff().dt.days.dropna()
    median_gap = float(diffs.median()) if not diffs.empty else 1.0
    periods_per_year = 52 if median_gap >= 5 else 252
    returns = frame["close"].pct_change().dropna()
    frame["drawdown"] = frame["close"] / frame["close"].cummax() - 1
    half_year_index = periods_per_year // 2
    latest_close = float(frame["close"].iloc[-1])
    momentum_6m = frame["close"].iloc[-1] / frame["close"].iloc[-half_year_index - 1] - 1 if len(frame) > half_year_index else None
    momentum_1y = frame["close"].iloc[-1] / frame["close"].iloc[-periods_per_year - 1] - 1 if len(frame) > periods_per_year else None
    volatility_1y = float(returns.std() * math.sqrt(periods_per_year)) if not returns.empty else None
    max_drawdown = float(frame["drawdown"].min()) if not frame["drawdown"].empty else None

    frame["ema_12"] = frame["close"].ewm(span=12, adjust=False).mean()
    frame["ema_20"] = frame["close"].ewm(span=20, adjust=False).mean()
    frame["ema_26"] = frame["close"].ewm(span=26, adjust=False).mean()
    frame["ema_50"] = frame["close"].ewm(span=50, adjust=False).mean()
    frame["sma_200"] = frame["close"].rolling(200).mean()
    ema_20 = float(frame["ema_20"].iloc[-1]) if len(frame) >= 20 else None
    ema_50 = float(frame["ema_50"].iloc[-1]) if len(frame) >= 50 else None
    sma_200 = float(frame["sma_200"].iloc[-1]) if frame["sma_200"].notna().any() else None
    price_vs_sma_200 = latest_close / sma_200 - 1 if is_num(sma_200) and sma_200 else None

    trend_parts = []
    if is_num(ema_20):
        trend_parts.append(100.0 if latest_close > ema_20 else 35.0)
    if is_num(ema_50):
        trend_parts.append(100.0 if latest_close > ema_50 else 30.0)
    if is_num(ema_20) and is_num(ema_50):
        trend_parts.append(100.0 if ema_20 > ema_50 else 30.0)
    if is_num(sma_200):
        trend_parts.append(100.0 if latest_close > sma_200 else 20.0)
    if is_num(ema_50) and is_num(sma_200):
        trend_parts.append(100.0 if ema_50 > sma_200 else 25.0)
    if is_num(price_vs_sma_200):
        trend_parts.append(clamp(50 + price_vs_sma_200 * 250, 0, 100))
    trend_score = average_percent(trend_parts)

    if is_num(sma_200) and latest_close > sma_200 and is_num(ema_50) and ema_50 > sma_200 and is_num(ema_20) and ema_20 > ema_50:
        trend_state = "Confirmed uptrend"
    elif is_num(sma_200) and latest_close > sma_200:
        trend_state = "Uptrend"
    elif is_num(sma_200) and latest_close < sma_200 and is_num(ema_50) and ema_50 < sma_200:
        trend_state = "Confirmed downtrend"
    elif is_num(ema_50) and latest_close > ema_50:
        trend_state = "Constructive"
    elif is_num(ema_50) and latest_close < ema_50:
        trend_state = "Weak trend"
    else:
        trend_state = "No data"

    crossover_signal = "No 200D baseline"
    if len(frame) >= 201 and frame["sma_200"].notna().iloc[-1] and frame["sma_200"].notna().iloc[-2]:
        prev_ema_50 = float(frame["ema_50"].iloc[-2])
        prev_sma_200 = float(frame["sma_200"].iloc[-2])
        if prev_ema_50 <= prev_sma_200 and is_num(ema_50) and is_num(sma_200) and ema_50 > sma_200:
            crossover_signal = "Fresh golden cross"
        elif prev_ema_50 >= prev_sma_200 and is_num(ema_50) and is_num(sma_200) and ema_50 < sma_200:
            crossover_signal = "Fresh death cross"
        elif is_num(ema_50) and is_num(sma_200) and ema_50 > sma_200:
            crossover_signal = "Golden cross active"
        elif is_num(ema_50) and is_num(sma_200) and ema_50 < sma_200:
            crossover_signal = "Death cross active"
    elif is_num(ema_20) and is_num(ema_50):
        crossover_signal = "EMA20 over EMA50" if ema_20 > ema_50 else "EMA20 under EMA50"

    delta = frame["close"].diff()
    gains = delta.clip(lower=0).ewm(alpha=1 / 14, adjust=False).mean()
    losses = (-delta.clip(upper=0)).ewm(alpha=1 / 14, adjust=False).mean()
    rs = gains / losses.replace(0, pd.NA)
    rsi = 100 - (100 / (1 + rs))
    rsi_14 = float(rsi.iloc[-1]) if len(rsi.dropna()) else None
    frame["macd_line"] = frame["ema_12"] - frame["ema_26"]
    frame["macd_signal"] = frame["macd_line"].ewm(span=9, adjust=False).mean()
    frame["macd_hist"] = frame["macd_line"] - frame["macd_signal"]
    macd_line = float(frame["macd_line"].iloc[-1]) if len(frame) >= 26 else None
    macd_signal = float(frame["macd_signal"].iloc[-1]) if len(frame) >= 35 else None
    macd_hist = float(frame["macd_hist"].iloc[-1]) if len(frame) >= 35 else None
    technical_momentum_score = average_percent(
        [
            score_rsi_value(rsi_14),
            100.0 if is_num(macd_hist) and macd_hist > 0 else 35.0 if is_num(macd_hist) else None,
            score_range_value(momentum_6m, 0.12, 0.02),
            score_range_value(momentum_1y, 0.20, 0.05),
        ]
    )

    volume_series = frame["volume"].dropna()
    volume = float(volume_series.iloc[-1]) if not volume_series.empty and is_num(volume_series.iloc[-1]) else None
    avg_volume_20 = float(volume_series.tail(20).mean()) if len(volume_series) >= 5 else None
    avg_volume_50 = float(volume_series.tail(50).mean()) if len(volume_series) >= 10 else None
    volume_ratio = safe_div(volume, avg_volume_20)
    dollar_volume = volume * latest_close if is_num(volume) else None
    vwap_60 = None
    if is_num(volume_series.sum()) and volume_series.sum() > 0:
        vwap_frame = frame.dropna(subset=["volume"]).tail(60).copy()
        if not vwap_frame.empty and vwap_frame["volume"].sum() > 0:
            typical_price = (vwap_frame["high"] + vwap_frame["low"] + vwap_frame["close"]) / 3
            vwap_60 = float((typical_price * vwap_frame["volume"]).sum() / vwap_frame["volume"].sum())
    volume_score = average_percent(
        [
            score_range_value(volume_ratio, 1.5, 0.8),
            score_range_value(dollar_volume, 50_000_000, 5_000_000),
            80.0 if is_num(vwap_60) and latest_close > vwap_60 else 45.0 if is_num(vwap_60) else None,
        ]
    )
    if is_num(volume_ratio) and volume_ratio >= 2:
        volume_spike = "Major spike"
    elif is_num(volume_ratio) and volume_ratio >= 1.5:
        volume_spike = "Volume expansion"
    elif is_num(volume_ratio):
        volume_spike = "Normal"
    else:
        volume_spike = "No data"
    if is_num(dollar_volume) and dollar_volume >= 50_000_000:
        liquidity_state = "Institutional"
    elif is_num(dollar_volume) and dollar_volume >= 5_000_000:
        liquidity_state = "Tradable"
    elif is_num(dollar_volume):
        liquidity_state = "Thin"
    else:
        liquidity_state = "No data"

    structure_lookback = min(len(frame), 64)
    structure_frame = frame.tail(structure_lookback)
    baseline_frame = structure_frame.iloc[:-1] if len(structure_frame) > 20 else structure_frame
    support_level = float(baseline_frame["low"].min()) if not baseline_frame.empty else None
    resistance_level = float(baseline_frame["high"].max()) if not baseline_frame.empty else None
    distance_to_support = latest_close / support_level - 1 if is_num(support_level) and support_level else None
    distance_to_resistance = latest_close / resistance_level - 1 if is_num(resistance_level) and resistance_level else None
    if is_num(resistance_level) and latest_close > resistance_level * 1.005:
        breakout_signal = "Breakout"
    elif is_num(support_level) and latest_close < support_level * 0.995:
        breakout_signal = "Breakdown"
    elif is_num(distance_to_resistance) and abs(distance_to_resistance) <= 0.015:
        breakout_signal = "Testing resistance"
    elif is_num(distance_to_support) and abs(distance_to_support) <= 0.015:
        breakout_signal = "Retesting support"
    else:
        breakout_signal = "Inside range"
    range_width = safe_div(resistance_level - support_level if is_num(resistance_level) and is_num(support_level) else None, support_level)
    if is_num(trend_score) and trend_score >= 70 and is_num(range_width) and range_width >= 0.08:
        range_state = "Trend"
    elif is_num(range_width) and range_width <= 0.08:
        range_state = "Tight range"
    elif is_num(range_width):
        range_state = "Range"
    else:
        range_state = "No data"

    previous_close = frame["close"].shift(1)
    true_range = pd.concat(
        [
            frame["high"] - frame["low"],
            (frame["high"] - previous_close).abs(),
            (frame["low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = true_range.rolling(14).mean()
    atr_14 = float(atr.iloc[-1]) if atr.notna().any() else None
    atr_pct = atr_14 / latest_close if is_num(atr_14) and latest_close else None
    stop_candidates = []
    if is_num(atr_14):
        stop_candidates.append(latest_close - 2 * atr_14)
    if is_num(support_level):
        stop_candidates.append(support_level * 0.98)
    valid_stops = [value for value in stop_candidates if is_num(value) and value < latest_close]
    stop_loss = max(valid_stops) if valid_stops else None
    stop_loss_distance = (latest_close - stop_loss) / latest_close if is_num(stop_loss) and latest_close else None
    risk_reward_to_resistance = safe_div(resistance_level - latest_close if is_num(resistance_level) else None, latest_close - stop_loss if is_num(stop_loss) else None)
    risk_score = average_percent(
        [
            score_range_value(volatility_1y, 0.25, 0.45, lower_is_better=True),
            score_range_value(abs(max_drawdown) if is_num(max_drawdown) else None, 0.15, 0.35, lower_is_better=True),
            score_range_value(atr_pct, 0.025, 0.055, lower_is_better=True),
            score_range_value(stop_loss_distance, 0.06, 0.14, lower_is_better=True),
        ]
    )

    return {
        **defaults,
        "momentum_6m": momentum_6m,
        "momentum_1y": momentum_1y,
        "max_drawdown": max_drawdown,
        "volatility_1y": volatility_1y,
        "ema_20": ema_20,
        "ema_50": ema_50,
        "sma_200": sma_200,
        "price_vs_sma_200": price_vs_sma_200,
        "trend_score": trend_score,
        "trend_state": trend_state,
        "crossover_signal": crossover_signal,
        "rsi_14": rsi_14,
        "macd_line": macd_line,
        "macd_signal": macd_signal,
        "macd_hist": macd_hist,
        "technical_momentum_score": technical_momentum_score,
        "volume": volume,
        "avg_volume_20": avg_volume_20,
        "avg_volume_50": avg_volume_50,
        "volume_ratio": volume_ratio,
        "dollar_volume": dollar_volume,
        "vwap_60": vwap_60,
        "volume_score": volume_score,
        "volume_spike": volume_spike,
        "liquidity_state": liquidity_state,
        "support_level": support_level,
        "resistance_level": resistance_level,
        "distance_to_support": distance_to_support,
        "distance_to_resistance": distance_to_resistance,
        "breakout_signal": breakout_signal,
        "range_state": range_state,
        "atr_14": atr_14,
        "atr_pct": atr_pct,
        "stop_loss": stop_loss,
        "stop_loss_distance": stop_loss_distance,
        "risk_reward_to_resistance": risk_reward_to_resistance,
        "risk_score": risk_score,
    }


def merge_provider_data(symbol: str, sources: dict[str, dict[str, Any]], assumptions: dict[str, Any]) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    texts: dict[str, Any] = {}
    source_map: dict[str, str] = {}

    for field in TEXT_FIELDS:
        value, provider = choose_text(sources, field)
        texts[field] = value or ""
        if provider:
            source_map[field] = provider

    raw_fields = [
        "price",
        "previous_close",
        "change",
        "change_pct",
        "market_cap",
        "shares_outstanding",
        "enterprise_value",
        "pe_ratio",
        "forward_pe",
        "pb_ratio",
        "ps_ratio",
        "ev_ebitda",
        "dividend_yield",
        "analyst_target",
        "intrinsic_value",
        "revenue",
        "revenue_prev",
        "gross_profit",
        "operating_income",
        "net_income",
        "net_income_prev",
        "operating_cash_flow",
        "free_cash_flow",
        "book_value_per_share",
        "revenue_growth",
        "earnings_growth",
        "gross_margin",
        "operating_margin",
        "profit_margin",
        "roe",
        "roa",
        "roic",
        "current_ratio",
        "quick_ratio",
        "debt_to_equity",
        "interest_coverage",
        "beta",
        "total_assets",
        "total_equity",
        "current_assets",
        "current_liabilities",
        "total_debt",
        "cash_and_equivalents",
        "annual_dividend",
        "rating_score",
    ]
    for field in raw_fields:
        value, provider = choose_metric(sources, field)
        metrics[field] = value
        if provider:
            source_map[field] = provider

    history_rows, history_provider = choose_history(sources)
    annual_rows, annual_provider = choose_annuals(sources)
    news_rows, _news_providers = merge_all_news(sources)
    news_provider = ", ".join(
        NEWS_PROVIDER_LABELS.get(p, p) for p in _news_providers[:3]
    ) or None
    recommendation_rows, recommendation_provider = choose_recommendations(sources)

    def fill_calculated(key: str, value: Any) -> None:
        if not is_num(metrics.get(key)) and is_num(value):
            metrics[key] = float(value)
            source_map[key] = "calculated"

    fill_calculated("change", metrics.get("price") - metrics.get("previous_close") if is_num(metrics.get("price")) and is_num(metrics.get("previous_close")) else None)
    fill_calculated("change_pct", (metrics["price"] / metrics["previous_close"] - 1) if is_num(metrics.get("price")) and is_num(metrics.get("previous_close")) and metrics.get("previous_close") else None)
    fill_calculated("market_cap", metrics.get("price") * metrics.get("shares_outstanding") if is_num(metrics.get("price")) and is_num(metrics.get("shares_outstanding")) else None)
    fill_calculated("shares_outstanding", safe_div(metrics.get("market_cap"), metrics.get("price")))
    fill_calculated("enterprise_value", sum_numbers(metrics.get("market_cap"), metrics.get("total_debt"), -1 * (metrics.get("cash_and_equivalents") or 0)))
    fill_calculated("book_value_per_share", safe_div(metrics.get("total_equity"), metrics.get("shares_outstanding")))
    fill_calculated("pe_ratio", safe_div(metrics.get("price"), safe_div(metrics.get("net_income"), metrics.get("shares_outstanding"))))
    fill_calculated("pb_ratio", safe_div(metrics.get("price"), metrics.get("book_value_per_share")))
    fill_calculated("gross_margin", safe_div(metrics.get("gross_profit"), metrics.get("revenue")))
    fill_calculated("profit_margin", safe_div(metrics.get("net_income"), metrics.get("revenue")))
    fill_calculated("operating_margin", safe_div(metrics.get("operating_income"), metrics.get("revenue")))
    fill_calculated("roe", safe_div(metrics.get("net_income"), metrics.get("total_equity")))
    fill_calculated("roa", safe_div(metrics.get("net_income"), metrics.get("total_assets")))
    fill_calculated("current_ratio", safe_div(metrics.get("current_assets"), metrics.get("current_liabilities")))
    fill_calculated("debt_to_equity", safe_div(metrics.get("total_debt"), metrics.get("total_equity")))
    fill_calculated("dividend_yield", safe_div(metrics.get("annual_dividend"), metrics.get("price")))
    fill_calculated("revenue_growth", safe_div(metrics.get("revenue") - metrics.get("revenue_prev") if is_num(metrics.get("revenue")) and is_num(metrics.get("revenue_prev")) else None, abs(metrics.get("revenue_prev")) if is_num(metrics.get("revenue_prev")) else None))
    fill_calculated("earnings_growth", safe_div(metrics.get("net_income") - metrics.get("net_income_prev") if is_num(metrics.get("net_income")) and is_num(metrics.get("net_income_prev")) else None, abs(metrics.get("net_income_prev")) if is_num(metrics.get("net_income_prev")) else None))

    history_stats = calculate_history_insights(history_rows)
    for key, value in history_stats.items():
        metrics[key] = value
        if is_value_present(value):
            source_map[key] = history_provider or "calculated"

    dcf = calculate_smart_dcf(
        metrics.get("free_cash_flow"),
        metrics.get("shares_outstanding"),
        assumptions,
        metrics.get("price"),
        metrics.get("revenue_growth"),
        metrics.get("earnings_growth"),
    )
    if is_num(dcf.get("intrinsic_value")):
        metrics["intrinsic_value"] = dcf["intrinsic_value"]
        source_map["intrinsic_value"] = source_map.get("intrinsic_value", "calculated")
    metrics["fair_value_after_mos"] = dcf["fair_value_after_mos"]
    metrics["dcf_upside"] = dcf["dcf_upside"]
    metrics["dcf_growth_used"] = dcf["used_growth"]
    if is_num(metrics.get("fair_value_after_mos")):
        source_map["fair_value_after_mos"] = "calculated"
    if is_num(metrics.get("dcf_upside")):
        source_map["dcf_upside"] = "calculated"

    account_size = to_num(assumptions.get("account_size"), 10_000.0)
    risk_per_trade = to_num(assumptions.get("risk_per_trade"), 0.01)
    stop_loss = metrics.get("stop_loss")
    price = metrics.get("price")
    if is_num(account_size) and is_num(risk_per_trade) and is_num(price) and is_num(stop_loss) and stop_loss < price:
        risk_budget = account_size * risk_per_trade
        risk_per_share = price - stop_loss
        shares_by_risk = math.floor(risk_budget / risk_per_share) if risk_per_share > 0 else None
        shares_by_cash = math.floor(account_size / price) if price > 0 else None
        if is_num(shares_by_risk) and is_num(shares_by_cash):
            metrics["position_size"] = max(0, min(int(shares_by_risk), int(shares_by_cash)))
            source_map["position_size"] = "calculated"

    value_signals = [
        score_metric(metrics.get("pe_ratio"), (15, 24), lower_is_better=True),
        score_metric(metrics.get("forward_pe"), (14, 22), lower_is_better=True),
        score_metric(metrics.get("pb_ratio"), (2, 5), lower_is_better=True),
        score_metric(metrics.get("ps_ratio"), (3, 7), lower_is_better=True),
        score_metric(metrics.get("ev_ebitda"), (10, 18), lower_is_better=True),
        score_metric(metrics.get("dcf_upside"), (0.15, -0.15), lower_is_better=False, upside_mode=True),
    ]
    quality_signals = [
        score_metric(metrics.get("gross_margin"), (0.5, 0.3), lower_is_better=False),
        score_metric(metrics.get("profit_margin"), (0.2, 0.08), lower_is_better=False),
        score_metric(metrics.get("roe"), (0.18, 0.1), lower_is_better=False),
        score_metric(metrics.get("current_ratio"), (1.5, 1.0), lower_is_better=False),
        score_metric(metrics.get("debt_to_equity"), (0.5, 1.2), lower_is_better=True),
    ]
    free_cash_flow_margin = safe_div(metrics.get("free_cash_flow"), metrics.get("revenue"))
    growth_signals = [
        score_metric(metrics.get("revenue_growth"), (0.12, 0.03), lower_is_better=False),
        score_metric(metrics.get("earnings_growth"), (0.15, 0.04), lower_is_better=False),
        score_metric(free_cash_flow_margin, (0.15, 0.05), lower_is_better=False),
    ]
    momentum_signals = [
        score_metric(metrics.get("momentum_6m"), (0.12, 0.02), lower_is_better=False),
        score_metric(metrics.get("momentum_1y"), (0.2, 0.05), lower_is_better=False),
        score_metric(metrics.get("max_drawdown"), (-0.15, -0.35), lower_is_better=False),
        score_metric(-1 * metrics.get("volatility_1y") if is_num(metrics.get("volatility_1y")) else None, (-0.25, -0.45), lower_is_better=False),
        (score_rsi_value(metrics.get("rsi_14")) - 50) / 50 if is_num(score_rsi_value(metrics.get("rsi_14"))) else None,
        1.0 if is_num(metrics.get("macd_hist")) and metrics.get("macd_hist") > 0 else -0.4 if is_num(metrics.get("macd_hist")) else None,
    ]

    def average_score(values: list[float | None]) -> float | None:
        valid = [value for value in values if is_num(value)]
        if not valid:
            return None
        return sum(valid) / len(valid)

    value_score_raw = average_score(value_signals)
    quality_score_raw = average_score(quality_signals)
    growth_score_raw = average_score(growth_signals)
    momentum_score_raw = average_score(momentum_signals)
    overall_raw = average_score([value_score_raw, quality_score_raw, growth_score_raw, momentum_score_raw])

    metrics["value_score"] = score_to_percent(value_score_raw)
    metrics["quality_score"] = score_to_percent(quality_score_raw)
    metrics["growth_score"] = score_to_percent(growth_score_raw)
    metrics["momentum_score"] = weighted_average_percent(
        [
            (score_to_percent(momentum_score_raw), 0.55),
            (metrics.get("technical_momentum_score"), 0.45),
        ]
    )
    metrics["overall_score"] = score_to_percent(overall_raw)
    source_map["value_score"] = "calculated"
    source_map["quality_score"] = "calculated"
    source_map["growth_score"] = "calculated"
    source_map["momentum_score"] = "calculated"
    update_composite_scores(metrics, source_map)

    return {
        "symbol": symbol,
        **texts,
        "metrics": metrics,
        "sources": source_map,
        "providers": sources,
        "history": history_rows,
        "history_provider": history_provider,
        "annuals": annual_rows,
        "annuals_provider": annual_provider,
        "news": news_rows,
        "news_provider": news_provider,
        "news_providers": _news_providers,
        "recommendations": recommendation_rows,
        "recommendations_provider": recommendation_provider,
        "notes": [note for payload in sources.values() for note in payload.get("notes", [])],
        "assumptions": assumptions,
        "available_providers": [name for name, payload in sources.items() if payload.get("status") not in {"error", "disabled"}],
    }


def status_text(metric: str, value: Any) -> str:
    number = to_num(value)
    if metric in TEXT_METRIC_KEYS:
        return str(value or "")
    if not is_num(number):
        return "No data"
    if metric in {
        "value_score",
        "quality_score",
        "growth_score",
        "fundamental_score",
        "momentum_score",
        "trend_score",
        "technical_momentum_score",
        "volume_score",
        "risk_score",
        "relative_strength_score",
        "overall_score",
        "final_ai_score",
    }:
        if number >= 75:
            return "Excellent"
        if number >= 60:
            return "Strong"
        if number >= 45:
            return "Balanced"
        return "Weak"
    if metric == "rsi_14":
        if number < 30:
            return "Oversold"
        if number > 70:
            return "Overbought"
        return "Normal"
    if metric == "volume_ratio":
        if number >= 2:
            return "Major spike"
        if number >= 1.5:
            return "Expansion"
        return "Normal"
    if metric == "atr_pct":
        if number <= 0.025:
            return "Calm"
        if number <= 0.055:
            return "Normal risk"
        return "High risk"
    if metric == "risk_reward_to_resistance":
        if number >= 2:
            return "Strong asymmetry"
        if number >= 1:
            return "Acceptable"
        return "Thin reward"
    if metric == "dcf_upside":
        if number >= 0.15:
            return "Discount"
        if number >= -0.1:
            return "Fair"
        return "Expensive"
    if metric in {"pe_ratio", "forward_pe", "pb_ratio", "ps_ratio", "ev_ebitda"}:
        if metric in {"pe_ratio", "forward_pe"}:
            if number <= (15 if metric == "pe_ratio" else 14):
                return "Attractive"
            if number <= (24 if metric == "pe_ratio" else 22):
                return "Fair"
            return "Rich"
        if metric == "pb_ratio":
            if number <= 2:
                return "Attractive"
            if number <= 5:
                return "Fair"
            return "Rich"
        if metric == "ps_ratio":
            if number <= 3:
                return "Attractive"
            if number <= 7:
                return "Fair"
            return "Rich"
        if metric == "ev_ebitda":
            if number <= 10:
                return "Attractive"
            if number <= 18:
                return "Fair"
            return "Rich"
    if metric in {"gross_margin", "operating_margin", "profit_margin", "roe", "roa", "roic"}:
        if number >= 0.15:
            return "Strong"
        if number >= 0.08:
            return "Healthy"
        return "Weak"
    if metric == "debt_to_equity":
        if number <= 0.5:
            return "Low debt"
        if number <= 1.2:
            return "Manageable"
        return "Heavy debt"
    if metric in {"momentum_6m", "momentum_1y"}:
        if number >= 0.15:
            return "Strong trend"
        if number >= 0.03:
            return "Positive"
        return "Soft"
    if metric == "max_drawdown":
        if number >= -0.15:
            return "Contained"
        if number >= -0.3:
            return "Normal"
        return "Deep"
    return "Available"


def format_money(value: Any, currency: str = "") -> str:
    number = to_num(value)
    if not is_num(number):
        return "—"
    prefix = f"{currency} " if currency else ""
    return f"{prefix}{number:,.2f}"


def format_large(value: Any, currency: str = "") -> str:
    number = to_num(value)
    if not is_num(number):
        return "—"
    absolute = abs(number)
    scaled = number
    suffix = ""
    if absolute >= 1e12:
        scaled = number / 1e12
        suffix = "T"
    elif absolute >= 1e9:
        scaled = number / 1e9
        suffix = "B"
    elif absolute >= 1e6:
        scaled = number / 1e6
        suffix = "M"
    elif absolute >= 1e3:
        scaled = number / 1e3
        suffix = "K"
    prefix = f"{currency} " if currency else ""
    return f"{prefix}{scaled:,.2f}{suffix}"


def format_pct(value: Any, signed: bool = False) -> str:
    number = to_num(value)
    if not is_num(number):
        return "—"
    sign = "+" if signed and number > 0 else ""
    return f"{sign}{number * 100:,.1f}%"


def format_multiple(value: Any) -> str:
    number = to_num(value)
    if not is_num(number):
        return "—"
    return f"{number:,.2f}x"


def format_number(value: Any, decimals: int = 2) -> str:
    number = to_num(value)
    if not is_num(number):
        return "—"
    return f"{number:,.{decimals}f}"


def format_large_number(value: Any) -> str:
    number = to_num(value)
    if not is_num(number):
        return "—"
    absolute = abs(number)
    scaled = number
    suffix = ""
    if absolute >= 1e12:
        scaled = number / 1e12
        suffix = "T"
    elif absolute >= 1e9:
        scaled = number / 1e9
        suffix = "B"
    elif absolute >= 1e6:
        scaled = number / 1e6
        suffix = "M"
    elif absolute >= 1e3:
        scaled = number / 1e3
        suffix = "K"
    return f"{scaled:,.2f}{suffix}"


def format_score(value: Any) -> str:
    number = to_num(value)
    if not is_num(number):
        return "—"
    return f"{number:,.0f}/100"


def format_by_type(value: Any, kind: str, currency: str = "") -> str:
    if kind == "money":
        return format_money(value, currency)
    if kind == "money_signed":
        number = to_num(value)
        if not is_num(number):
            return "—"
        return f"{'+' if number > 0 else ''}{format_money(number, currency)}"
    if kind == "large_money":
        return format_large(value, currency)
    if kind == "percent":
        return format_pct(value)
    if kind == "percent_signed":
        return format_pct(value, signed=True)
    if kind == "multiple":
        return format_multiple(value)
    if kind == "number":
        return format_number(value)
    if kind == "integer":
        number = to_num(value)
        if not is_num(number):
            return "—"
        return f"{number:,.0f}"
    if kind == "large_number":
        return format_large_number(value)
    if kind == "score":
        return format_score(value)
    if kind == "text":
        return str(value) if is_value_present(value) else "—"
    return str(value) if is_value_present(value) else "—"


def metric_frame(bundle: dict[str, Any], categories: list[str]) -> pd.DataFrame:
    currency = bundle.get("currency", "")
    rows = []
    for definition in METRIC_DEFINITIONS:
        if definition.category not in categories:
            continue
        value = bundle["metrics"].get(definition.key)
        rows.append(
            {
                "Metric": definition.label,
                "Value": format_by_type(value, definition.kind, currency),
                "Assessment": status_text(definition.key, value),
                "Source": provider_label(bundle["sources"].get(definition.key)),
            }
        )
    return pd.DataFrame(rows)


def provider_health_frame(bundle: dict[str, Any]) -> pd.DataFrame:
    rows = []
    for provider, payload in bundle.get("providers", {}).items():
        rows.append(
            {
                "Provider": provider_label(provider),
                "Status": str(payload.get("status", "unknown")).title(),
                "Coverage": payload.get("coverage", 0),
                "Errors": payload.get("error_count", 0),
                "Freshness": payload.get("freshness", "—"),
                "Summary": payload.get("summary_error", "—"),
            }
        )
    return pd.DataFrame(rows)


def provider_coverage_heatmap(bundle: dict[str, Any]) -> go.Figure:
    providers = list(bundle.get("providers", {}).keys())
    z_values: list[list[int]] = []
    for provider in providers:
        metrics = bundle["providers"].get(provider, {}).get("metrics", {})
        z_values.append([1 if is_num(metrics.get(field)) else 0 for field in CORE_DIAGNOSTIC_FIELDS])
    fig = go.Figure(
        data=go.Heatmap(
            z=z_values,
            x=CORE_DIAGNOSTIC_FIELDS,
            y=[provider_label(name) for name in providers],
            colorscale=[[0, "#f1f1f1"], [1, "#0a6c71"]],
            showscale=False,
        )
    )
    fig.update_layout(
        template="plotly_white",
        height=360,
        margin={"l": 10, "r": 10, "t": 40, "b": 10},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        title="Provider coverage heatmap",
    )
    return fig


def provider_coverage_bar(bundle: dict[str, Any]) -> go.Figure:
    frame = provider_health_frame(bundle)
    fig = px.bar(frame, x="Provider", y="Coverage", color="Status", height=360)
    fig.update_layout(
        template="plotly_white",
        margin={"l": 10, "r": 10, "t": 40, "b": 10},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        title="Provider metric coverage",
    )
    return fig


def recommendation_trend_chart(bundle: dict[str, Any]) -> go.Figure:
    rows = bundle.get("recommendations", []) or []
    fig = go.Figure()
    if not rows:
        fig.update_layout(template="plotly_white", height=360, title="Recommendation trend unavailable")
        return fig
    frame = pd.DataFrame(rows)
    if "period" not in frame.columns:
        fig.update_layout(template="plotly_white", height=360, title="Recommendation trend unavailable")
        return fig
    frame["period"] = pd.to_datetime(frame["period"])
    frame = frame.sort_values("period")
    for field, color in [("strongBuy", "#0a6c71"), ("buy", "#1f8f5f"), ("hold", "#bc7a12"), ("sell", "#d26b3f"), ("strongSell", "#b83f54")]:
        if field in frame.columns:
            fig.add_trace(go.Bar(x=frame["period"], y=frame[field], name=field, marker_color=color))
    fig.update_layout(
        template="plotly_white",
        barmode="stack",
        height=360,
        margin={"l": 10, "r": 10, "t": 40, "b": 10},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        title=f"Analyst recommendation trend · {provider_label(bundle.get('recommendations_provider'))}",
    )
    return fig


def news_frame(bundle: dict[str, Any]) -> pd.DataFrame:
    rows = []
    for item in bundle.get("news", [])[:15]:
        if not isinstance(item, dict):
            continue
        stamp = item.get("datetime")
        date_label = datetime_from_unix(stamp) if stamp else ""
        rows.append(
            {
                "Date": date_label or "—",
                "Providers": " + ".join(NEWS_PROVIDER_LABELS.get(tag, tag) for tag in item.get("provider_tags", []) or [item.get("provider_tag", "")]) or "—",
                "Source": item.get("source") or "—",
                "Headline": item.get("headline") or "—",
                "Category": item.get("category") or "—",
                "Fused Sources": item.get("source_count") or 1,
                "URL": item.get("url") or "—",
            }
        )
    return pd.DataFrame(rows)


def tradingview_configuration_data() -> dict[str, Any]:
    return {
        "supports_search": True,
        "supports_group_request": False,
        "supports_marks": True,
        "supports_timescale_marks": True,
        "supports_time": True,
        "exchanges": [{"value": "", "name": "All Exchanges", "desc": ""}]
        + [{"value": name, "name": name, "desc": f"{name} Exchange"} for name in TRADINGVIEW_EXCHANGES],
        "symbols_types": [{"name": "All types", "value": ""}]
        + [{"name": name.title(), "value": name} for name in TRADINGVIEW_SYMBOL_TYPES],
        "supported_resolutions": TRADINGVIEW_SUPPORTED_RESOLUTIONS,
    }


def tradingview_symbol_info(bundle: dict[str, Any]) -> dict[str, Any]:
    symbol = bundle.get("symbol") or ""
    exchange = bundle.get("exchange") or ""
    return {
        "ticker": symbol,
        "name": symbol,
        "description": bundle.get("company_name") or symbol,
        "type": "stock",
        "session": "0930-1600",
        "timezone": "America/New_York",
        "exchange": exchange,
        "minmov": 1,
        "pricescale": 100,
        "has_intraday": True,
        "supported_resolutions": TRADINGVIEW_SUPPORTED_RESOLUTIONS,
        "volume_precision": 2,
        "data_status": "streaming",
    }


def tradingview_bars_frame(bundle: dict[str, Any]) -> pd.DataFrame:
    rows = []
    for item in bundle.get("history", []) or []:
        if not isinstance(item, dict):
            continue
        close = coalesce_num(item.get("close"), item.get("adjClose"), item.get("price"))
        if not is_num(close):
            continue
        stamp = get_date_from_record(item)
        if stamp is None:
            continue
        open_price = coalesce_num(item.get("open"), close)
        high = coalesce_num(item.get("high"), close)
        low = coalesce_num(item.get("low"), close)
        volume = coalesce_num(item.get("volume"), 0)
        rows.append(
            {
                "time": int(stamp.replace(tzinfo=timezone.utc).timestamp() * 1000),
                "open": float(open_price),
                "high": float(high),
                "low": float(low),
                "close": float(close),
                "volume": float(volume or 0),
            }
        )
    return pd.DataFrame(rows).sort_values("time").reset_index(drop=True) if rows else pd.DataFrame(rows)


def comparison_summary_frame(bundles: dict[str, dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for symbol, bundle in bundles.items():
        metrics = bundle["metrics"]
        rows.append(
            {
                "Symbol": symbol,
                "Company": bundle.get("company_name") or symbol,
                "Price": format_money(metrics.get("price"), bundle.get("currency", "")),
                "Market Cap": format_large(metrics.get("market_cap"), bundle.get("currency", "")),
                "P/E": format_multiple(metrics.get("pe_ratio")),
                "Revenue Growth": format_pct(metrics.get("revenue_growth"), signed=True),
                "ROE": format_pct(metrics.get("roe")),
                "FCF": format_large(metrics.get("free_cash_flow"), bundle.get("currency", "")),
                "DCF Upside": format_pct(metrics.get("dcf_upside"), signed=True),
                "Trend": format_score(metrics.get("trend_score")),
                "Momentum": format_score(metrics.get("momentum_score")),
                "Volume": format_score(metrics.get("volume_score")),
                "Final AI": format_score(metrics.get("final_ai_score")),
                "Decision": metrics.get("decision_label") or "—",
            }
        )
    return pd.DataFrame(rows)


def metric_leader(bundles: dict[str, dict[str, Any]], key: str) -> str:
    values = []
    for symbol, bundle in bundles.items():
        value = to_num(bundle["metrics"].get(key))
        if is_num(value):
            values.append((float(value), symbol))
    if not values:
        return "—"
    if key in LOWER_IS_BETTER_METRICS:
        value, symbol = min(values)
    else:
        value, symbol = max(values)
    return f"{symbol} ({format_number(value, 2)})"


def comparison_xy_frame(bundles: dict[str, dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for definition in METRIC_DEFINITIONS:
        row = {
            "Category": definition.category,
            "X Metric": definition.label,
            "Leader": metric_leader(bundles, definition.key),
        }
        for symbol, bundle in bundles.items():
            row[symbol] = format_by_type(bundle["metrics"].get(definition.key), definition.kind, bundle.get("currency", ""))
        rows.append(row)
    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame["Category"] = pd.Categorical(frame["Category"], categories=CATEGORY_ORDER, ordered=True)
        frame = frame.sort_values(["Category", "X Metric"]).reset_index(drop=True)
        frame["Category"] = frame["Category"].astype(str)
    return frame


def comparison_leaderboard_frame(bundles: dict[str, dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for symbol, bundle in bundles.items():
        metrics = bundle["metrics"]
        rows.append(
            {
                "Rank": to_num(metrics.get("relative_strength_rank")),
                "Symbol": symbol,
                "Decision": metrics.get("decision_label") or "—",
                "Final AI": format_score(metrics.get("final_ai_score")),
                "Trend": format_score(metrics.get("trend_score")),
                "Momentum": format_score(metrics.get("momentum_score")),
                "Volume": format_score(metrics.get("volume_score")),
                "Fundamental": format_score(metrics.get("fundamental_score")),
                "Risk": format_score(metrics.get("risk_score")),
                "RS": format_score(metrics.get("relative_strength_score")),
                "Alerts": format_by_type(metrics.get("alert_count"), "integer"),
            }
        )
    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame["_sort"] = [to_num(bundle["metrics"].get("final_ai_score"), -1) for bundle in bundles.values()]
        frame = frame.sort_values("_sort", ascending=False).drop(columns=["_sort"]).reset_index(drop=True)
        frame["Rank"] = range(1, len(frame) + 1)
    return frame


COMPARISON_METRIC_OPTIONS = {
    "Price": ("price", "money"),
    "P/E Ratio": ("pe_ratio", "multiple"),
    "Revenue": ("revenue", "large_money"),
    "Net Income": ("net_income", "large_money"),
    "ROE": ("roe", "percent"),
    "ROA": ("roa", "percent"),
    "Debt-to-Equity": ("debt_to_equity", "multiple"),
    "Dividend Yield": ("dividend_yield", "percent"),
    "Revenue Growth": ("revenue_growth", "percent_signed"),
    "Earnings Growth": ("earnings_growth", "percent_signed"),
    "Free Cash Flow": ("free_cash_flow", "large_money"),
    "Market Cap": ("market_cap", "large_money"),
}


def unavailable_chart(title: str, message: str = "Not enough provider data for this chart.") -> go.Figure:
    fig = go.Figure()
    fig.add_annotation(text=message, x=0.5, y=0.5, showarrow=False, font={"size": 14})
    fig.update_layout(
        template="plotly_white",
        height=420,
        title=title,
        xaxis={"visible": False},
        yaxis={"visible": False},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin={"l": 10, "r": 10, "t": 50, "b": 10},
    )
    return fig


def comparison_numeric_frame(bundles: dict[str, dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for symbol, bundle in bundles.items():
        metrics = bundle["metrics"]
        row = {"Company": bundle.get("company_name") or symbol, "Symbol": symbol}
        for label, (key, _) in COMPARISON_METRIC_OPTIONS.items():
            row[label] = to_num(metrics.get(key))
        row["Payout Ratio"] = calculate_payout_ratio(bundle)
        rows.append(row)
    return pd.DataFrame(rows)


def calculate_payout_ratio(bundle: dict[str, Any]) -> float | None:
    metrics = bundle.get("metrics", {})
    annual_dividend = to_num(metrics.get("annual_dividend"))
    shares = to_num(metrics.get("shares_outstanding"))
    net_income = to_num(metrics.get("net_income"))
    if is_num(annual_dividend) and is_num(shares) and is_num(net_income) and net_income:
        return (annual_dividend * shares) / net_income
    dividend_yield = to_num(metrics.get("dividend_yield"))
    pe_ratio = to_num(metrics.get("pe_ratio"))
    if is_num(dividend_yield) and is_num(pe_ratio):
        return dividend_yield * pe_ratio
    return None


def bundle_annual_frame(symbol: str, bundle: dict[str, Any]) -> pd.DataFrame:
    frame = pd.DataFrame(bundle.get("annuals", []) or [])
    if frame.empty:
        return pd.DataFrame()
    frame = frame.copy()
    frame["Symbol"] = symbol
    frame["Company"] = bundle.get("company_name") or symbol
    if "year" not in frame.columns:
        frame["year"] = range(1, len(frame) + 1)
    frame["year"] = frame["year"].astype(str)
    for column in ["revenue", "gross_profit", "operating_income", "net_income", "operating_cash_flow", "capital_expenditure", "free_cash_flow"]:
        if column not in frame.columns:
            frame[column] = None
        frame[column] = frame[column].apply(to_num)
    return frame.sort_values("year")


def comparison_annual_frame(bundles: dict[str, dict[str, Any]]) -> pd.DataFrame:
    frames = [bundle_annual_frame(symbol, bundle) for symbol, bundle in bundles.items()]
    frames = [frame for frame in frames if not frame.empty]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def latest_annual_metric(bundle: dict[str, Any], key: str) -> float | None:
    frame = bundle_annual_frame(str(bundle.get("symbol") or ""), bundle)
    if not frame.empty and key in frame.columns:
        values = [value for value in frame[key].dropna().tolist() if is_num(value)]
        if values:
            return float(values[-1])
    return to_num(bundle.get("metrics", {}).get(key))


def xy_custom_scatter_chart(bundles: dict[str, dict[str, Any]], x_axis: str, y_axis: str) -> go.Figure:
    frame = comparison_numeric_frame(bundles)
    if frame.empty or x_axis not in frame or y_axis not in frame:
        return unavailable_chart("Custom X vs Y comparison")
    frame = frame.dropna(subset=[x_axis, y_axis], how="any")
    if frame.empty:
        return unavailable_chart(f"{x_axis} vs {y_axis}")
    size_col = None
    if "Market Cap" in frame and frame["Market Cap"].notna().any():
        frame["Bubble Size"] = frame["Market Cap"].where(frame["Market Cap"].gt(0))
        fallback_size = frame["Bubble Size"].dropna().median()
        frame["Bubble Size"] = frame["Bubble Size"].fillna(fallback_size if is_num(fallback_size) and fallback_size > 0 else 1)
        size_col = "Bubble Size"
    fig = px.scatter(
        frame,
        x=x_axis,
        y=y_axis,
        color="Symbol",
        text="Symbol",
        size=size_col,
        size_max=60,
        hover_name="Company",
        title=f"{x_axis} vs {y_axis}",
        height=440,
    )
    fig.update_traces(textposition="top center")
    fig.update_layout(template="plotly_white", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    return fig


def normalized_price_performance_chart(bundles: dict[str, dict[str, Any]], lookback_days: int = 365) -> go.Figure:
    fig = go.Figure()
    cutoff = pd.Timestamp.now(tz="UTC").tz_localize(None) - pd.Timedelta(days=int(lookback_days))
    for symbol, bundle in bundles.items():
        frame = pd.DataFrame(bundle.get("history", []) or [])
        if frame.empty or "close" not in frame:
            continue
        frame["date"] = pd.to_datetime(frame.get("date"), errors="coerce").dt.tz_localize(None)
        frame["close"] = frame["close"].apply(to_num)
        frame = frame.dropna(subset=["date", "close"]).sort_values("date")
        frame = frame[frame["date"] >= cutoff] if lookback_days else frame
        if frame.empty or not is_num(frame["close"].iloc[0]) or not frame["close"].iloc[0]:
            continue
        frame["normalized"] = frame["close"] / frame["close"].iloc[0] * 100
        fig.add_trace(go.Scatter(x=frame["date"], y=frame["normalized"], mode="lines", name=symbol, line={"width": 3}))
    if not fig.data:
        return unavailable_chart("Normalized Price Performance")
    fig.update_layout(template="plotly_white", height=460, title="Normalized Price Performance (Base 100)", yaxis_title="Base 100", hovermode="x unified", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    return fig


def pe_ratio_comparison_chart(bundles: dict[str, dict[str, Any]]) -> go.Figure:
    fig = go.Figure()
    for symbol, bundle in bundles.items():
        annual = bundle_annual_frame(symbol, bundle)
        market_cap = to_num(bundle["metrics"].get("market_cap"))
        if not annual.empty and is_num(market_cap):
            annual["pe_ratio"] = annual["net_income"].apply(lambda value: safe_div(market_cap, value))
            annual = annual.dropna(subset=["pe_ratio"])
            if not annual.empty:
                fig.add_trace(go.Scatter(x=annual["year"], y=annual["pe_ratio"], mode="lines+markers", name=symbol))
                continue
        pe_ratio = to_num(bundle["metrics"].get("pe_ratio"))
        if is_num(pe_ratio):
            fig.add_trace(go.Scatter(x=["Current"], y=[pe_ratio], mode="markers+text", text=[symbol], name=symbol))
    if not fig.data:
        return unavailable_chart("P/E Ratio Comparison")
    fig.update_layout(template="plotly_white", height=420, title="P/E Ratio Comparison", yaxis_title="P/E", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    return fig


def side_by_side_financials_chart(bundles: dict[str, dict[str, Any]]) -> go.Figure:
    rows = []
    for symbol, bundle in bundles.items():
        rows.append({"Symbol": symbol, "Metric": "Revenue", "Value": latest_annual_metric(bundle, "revenue")})
        rows.append({"Symbol": symbol, "Metric": "Net Income", "Value": latest_annual_metric(bundle, "net_income")})
    frame = pd.DataFrame(rows).dropna(subset=["Value"])
    if frame.empty:
        return unavailable_chart("Revenue & Net Income")
    frame["Value"] = frame["Value"] / 1e9
    fig = px.bar(frame, x="Symbol", y="Value", color="Metric", barmode="group", title="Side-by-Side Financials", height=420)
    fig.update_layout(template="plotly_white", yaxis_title="USD Billions", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    return fig


def profitability_radar_chart(bundles: dict[str, dict[str, Any]]) -> go.Figure:
    fig = go.Figure()
    labels = ["Gross Margin", "Operating Margin", "Net Margin"]
    keys = ["gross_margin", "operating_margin", "profit_margin"]
    for symbol, bundle in bundles.items():
        values = [to_num(bundle["metrics"].get(key), 0) * 100 for key in keys]
        fig.add_trace(go.Scatterpolar(r=values + [values[0]], theta=labels + [labels[0]], fill="toself", name=symbol, opacity=0.55))
    if not fig.data:
        return unavailable_chart("Profitability Radar Chart")
    fig.update_layout(template="plotly_white", height=430, title="Profitability Radar Chart", polar={"radialaxis": {"visible": True}}, paper_bgcolor="rgba(0,0,0,0)")
    return fig


def roe_roa_comparison_chart(bundles: dict[str, dict[str, Any]]) -> go.Figure:
    rows = []
    for symbol, bundle in bundles.items():
        rows.append({"Symbol": symbol, "Metric": "ROE", "Value": to_num(bundle["metrics"].get("roe"))})
        rows.append({"Symbol": symbol, "Metric": "ROA", "Value": to_num(bundle["metrics"].get("roa"))})
    frame = pd.DataFrame(rows).dropna(subset=["Value"])
    if frame.empty:
        return unavailable_chart("ROE & ROA Comparison")
    fig = px.bar(frame, x="Symbol", y="Value", color="Metric", barmode="group", title="ROE & ROA Comparison", height=420)
    fig.update_layout(template="plotly_white", yaxis_tickformat=".0%", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    return fig


def free_cash_flow_trend_chart(bundles: dict[str, dict[str, Any]]) -> go.Figure:
    fig = go.Figure()
    for symbol, bundle in bundles.items():
        annual = bundle_annual_frame(symbol, bundle)
        if annual.empty or "free_cash_flow" not in annual:
            continue
        annual = annual.dropna(subset=["free_cash_flow"])
        if annual.empty:
            continue
        fig.add_trace(go.Scatter(x=annual["year"], y=annual["free_cash_flow"] / 1e9, mode="lines+markers", fill="tozeroy", name=symbol))
    if not fig.data:
        return unavailable_chart("Free Cash Flow Trend")
    fig.update_layout(template="plotly_white", height=420, title="Free Cash Flow Trend", yaxis_title="USD Billions", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    return fig


def debt_equity_stacked_chart(bundles: dict[str, dict[str, Any]]) -> go.Figure:
    rows = []
    for symbol, bundle in bundles.items():
        metrics = bundle["metrics"]
        debt = to_num(metrics.get("total_debt"))
        equity = to_num(metrics.get("total_equity"))
        if is_num(debt):
            rows.append({"Symbol": symbol, "Component": "Debt", "Value": debt / 1e9})
        if is_num(equity):
            rows.append({"Symbol": symbol, "Component": "Equity", "Value": equity / 1e9})
    frame = pd.DataFrame(rows)
    if frame.empty:
        return unavailable_chart("Debt-to-Equity Stacked Bar")
    fig = px.bar(frame, x="Symbol", y="Value", color="Component", barmode="stack", title="Debt-to-Equity Stacked Bar", height=420)
    fig.update_layout(template="plotly_white", yaxis_title="USD Billions", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    return fig


def growth_matrix_chart(bundles: dict[str, dict[str, Any]]) -> go.Figure:
    frame = comparison_numeric_frame(bundles).rename(columns={"Revenue Growth": "Revenue Growth", "Earnings Growth": "EPS/Earnings Growth"})
    frame = frame.dropna(subset=["Revenue Growth", "EPS/Earnings Growth"], how="any")
    if frame.empty:
        return unavailable_chart("Growth Matrix")
    frame["Bubble Size"] = frame["Market Cap"].where(frame["Market Cap"].gt(0))
    fallback_size = frame["Bubble Size"].dropna().median()
    frame["Bubble Size"] = frame["Bubble Size"].fillna(fallback_size if is_num(fallback_size) and fallback_size > 0 else 1)
    fig = px.scatter(frame, x="Revenue Growth", y="EPS/Earnings Growth", color="Symbol", text="Symbol", size="Bubble Size", size_max=65, hover_name="Company", title="Growth Matrix", height=430)
    fig.update_traces(textposition="top center")
    fig.update_layout(template="plotly_white", xaxis_tickformat=".0%", yaxis_tickformat=".0%", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    return fig


def dividend_comparison_chart(bundles: dict[str, dict[str, Any]]) -> go.Figure:
    rows = []
    for symbol, bundle in bundles.items():
        rows.append({"Symbol": symbol, "Dividend Yield": to_num(bundle["metrics"].get("dividend_yield")), "Payout Ratio": calculate_payout_ratio(bundle)})
    frame = pd.DataFrame(rows)
    if frame[["Dividend Yield", "Payout Ratio"]].dropna(how="all").empty:
        return unavailable_chart("Dividend Yield & Payout Ratio")
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(x=frame["Symbol"], y=frame["Dividend Yield"], name="Dividend Yield", marker_color="#0a6c71"), secondary_y=False)
    fig.add_trace(go.Scatter(x=frame["Symbol"], y=frame["Payout Ratio"], name="Payout Ratio", mode="lines+markers", line={"color": "#f47f4b", "width": 3}), secondary_y=True)
    fig.update_layout(template="plotly_white", height=420, title="Dividend Yield & Payout Ratio", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    fig.update_yaxes(tickformat=".0%", title_text="Dividend Yield", secondary_y=False)
    fig.update_yaxes(tickformat=".0%", title_text="Payout Ratio", secondary_y=True)
    return fig


def market_cap_bubble_chart(bundles: dict[str, dict[str, Any]]) -> go.Figure:
    frame = comparison_numeric_frame(bundles).dropna(subset=["Market Cap"])
    if frame.empty:
        return unavailable_chart("Market Cap Bubble Chart")
    frame["Bubble Size"] = frame["Market Cap"].where(frame["Market Cap"].gt(0)).fillna(1)
    fig = px.scatter(frame, x="Symbol", y="Market Cap", size="Bubble Size", color="Symbol", text="Symbol", hover_name="Company", hover_data={"Revenue": ":,.0f"}, size_max=80, title="Market Cap Bubble Chart", height=430)
    fig.update_traces(textposition="top center")
    fig.update_layout(template="plotly_white", xaxis_title="Company", yaxis_title="Market Cap", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    return fig


def technical_signal_frame(bundle: dict[str, Any]) -> pd.DataFrame:
    currency = bundle.get("currency", "")
    metrics = bundle["metrics"]
    rows = [
        ("Trend", "Trend State", metrics.get("trend_state"), "text"),
        ("Trend", "EMA 20", metrics.get("ema_20"), "money"),
        ("Trend", "EMA 50", metrics.get("ema_50"), "money"),
        ("Trend", "SMA 200", metrics.get("sma_200"), "money"),
        ("Trend", "Cross", metrics.get("crossover_signal"), "text"),
        ("Momentum", "RSI 14", metrics.get("rsi_14"), "number"),
        ("Momentum", "MACD Histogram", metrics.get("macd_hist"), "number"),
        ("Volume", "Volume Spike", metrics.get("volume_spike"), "text"),
        ("Volume", "Volume / Avg", metrics.get("volume_ratio"), "multiple"),
        ("Structure", "Support", metrics.get("support_level"), "money"),
        ("Structure", "Resistance", metrics.get("resistance_level"), "money"),
        ("Structure", "Breakout / Retest", metrics.get("breakout_signal"), "text"),
        ("Risk", "ATR %", metrics.get("atr_pct"), "percent"),
        ("Risk", "Suggested Stop", metrics.get("stop_loss"), "money"),
        ("Risk", "Position Size", metrics.get("position_size"), "integer"),
    ]
    return pd.DataFrame(
        [
            {
                "Group": group,
                "Signal": label,
                "Value": format_by_type(value, kind, currency),
                "Assessment": status_text(next((definition.key for definition in METRIC_DEFINITIONS if definition.label == label), ""), value),
            }
            for group, label, value, kind in rows
        ]
    )


def alert_frame(bundle: dict[str, Any]) -> pd.DataFrame:
    metrics = bundle["metrics"]
    rows = []
    if is_num(metrics.get("rsi_14")) and metrics["rsi_14"] < 30:
        rows.append({"Severity": "Opportunity", "Alert": "RSI oversold", "Signal": format_number(metrics["rsi_14"]), "Action": "Watch reversal confirmation"})
    if is_num(metrics.get("rsi_14")) and metrics["rsi_14"] > 70:
        rows.append({"Severity": "Risk", "Alert": "RSI overbought", "Signal": format_number(metrics["rsi_14"]), "Action": "Avoid chasing extended moves"})
    if is_num(metrics.get("macd_hist")) and metrics["macd_hist"] > 0:
        rows.append({"Severity": "Bullish", "Alert": "MACD momentum positive", "Signal": format_number(metrics["macd_hist"]), "Action": "Confirm with trend and volume"})
    if is_num(metrics.get("macd_hist")) and metrics["macd_hist"] < 0:
        rows.append({"Severity": "Watch", "Alert": "MACD momentum negative", "Signal": format_number(metrics["macd_hist"]), "Action": "Wait for improving histogram"})
    if is_num(metrics.get("volume_ratio")) and metrics["volume_ratio"] >= 1.8:
        rows.append({"Severity": "Event", "Alert": "Volume spike", "Signal": format_multiple(metrics["volume_ratio"]), "Action": "Validate breakout or news catalyst"})
    if str(metrics.get("breakout_signal") or "") in {"Breakout", "Breakdown", "Testing resistance", "Retesting support"}:
        rows.append({"Severity": "Price", "Alert": str(metrics.get("breakout_signal")), "Signal": format_money(metrics.get("price"), bundle.get("currency", "")), "Action": "Check close and retest behavior"})
    if "downtrend" in str(metrics.get("trend_state") or "").lower():
        rows.append({"Severity": "Risk", "Alert": "Trend is negative", "Signal": str(metrics.get("trend_state")), "Action": "Reduce size or demand stronger confirmation"})
    if not rows:
        rows.append({"Severity": "OK", "Alert": "No major technical alert", "Signal": "—", "Action": "Use factor table for ranking"})
    return pd.DataFrame(rows)


def decision_heatmap_chart(bundles: dict[str, dict[str, Any]]) -> go.Figure:
    factors = [
        ("Final AI", "final_ai_score"),
        ("Fundamental", "fundamental_score"),
        ("Trend", "trend_score"),
        ("Momentum", "momentum_score"),
        ("Volume", "volume_score"),
        ("Relative Strength", "relative_strength_score"),
        ("Risk", "risk_score"),
    ]
    z_values = []
    symbols = []
    for symbol, bundle in bundles.items():
        symbols.append(symbol)
        z_values.append([to_num(bundle["metrics"].get(key), 0) or 0 for _, key in factors])
    fig = go.Figure(
        data=go.Heatmap(
            z=z_values,
            x=[label for label, _ in factors],
            y=symbols,
            zmin=0,
            zmax=100,
            colorscale=[[0, "#b83f54"], [0.5, "#f4c15d"], [1, "#0a6c71"]],
            colorbar_title="Score",
            text=[[format_score(value) for value in row] for row in z_values],
            texttemplate="%{text}",
        )
    )
    fig.update_layout(
        template="plotly_white",
        height=max(320, 72 + len(symbols) * 46),
        margin={"l": 10, "r": 10, "t": 45, "b": 10},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        title="Decision factor heatmap",
    )
    return fig


def format_crypto_price(value: Any) -> str:
    number = to_num(value)
    if not is_num(number):
        return "—"
    if abs(number) >= 100:
        return f"{number:,.2f}"
    if abs(number) >= 1:
        return f"{number:,.4f}"
    return f"{number:,.8f}"


def format_crypto_quote(value: Any) -> str:
    return format_large(value, "USDT")


def binance_metric_frame(bundle: dict[str, Any]) -> pd.DataFrame:
    metrics = bundle["metrics"]
    rows = [
        ("Price", format_crypto_price(metrics.get("price")), "Last traded price"),
        ("24H Change", format_pct(metrics.get("price_change_pct"), signed=True), "Ticker 24hr"),
        ("Best Bid", format_crypto_price(metrics.get("bid")), "BookTicker"),
        ("Best Ask", format_crypto_price(metrics.get("ask")), "BookTicker"),
        ("Spread", format_pct(metrics.get("spread_pct")), "Ask minus bid / mid"),
        ("24H Quote Volume", format_crypto_quote(metrics.get("quote_volume_24h")), "Ticker 24hr"),
        ("Depth Bid Notional", format_crypto_quote(metrics.get("bid_depth_notional")), f"Top {metrics.get('depth_levels')} levels"),
        ("Depth Ask Notional", format_crypto_quote(metrics.get("ask_depth_notional")), f"Top {metrics.get('depth_levels')} levels"),
        ("Book Imbalance", format_pct(metrics.get("book_imbalance"), signed=True), "Bid/ask notional imbalance"),
        ("Buy Pressure", format_pct(metrics.get("buy_pressure")), "Recent trades"),
        ("Liquidity Score", format_score(metrics.get("liquidity_score")), "Depth + spread"),
        ("Bookmap Score", format_score(metrics.get("bookmap_score")), "Liquidity + flow + trend"),
    ]
    return pd.DataFrame(rows, columns=["Metric", "Value", "Source / Meaning"])


def binance_comparison_frame(bundles: dict[str, dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for symbol, bundle in bundles.items():
        metrics = bundle["metrics"]
        rows.append(
            {
                "Symbol": symbol,
                "Price": format_crypto_price(metrics.get("price")),
                "24H Change": format_pct(metrics.get("price_change_pct"), signed=True),
                "Quote Volume": format_crypto_quote(metrics.get("quote_volume_24h")),
                "Spread": format_pct(metrics.get("spread_pct")),
                "Imbalance": format_pct(metrics.get("book_imbalance"), signed=True),
                "Buy Pressure": format_pct(metrics.get("buy_pressure")),
                "Liquidity": format_score(metrics.get("liquidity_score")),
                "Flow": format_score(metrics.get("flow_score")),
                "Trend": format_score(metrics.get("tech_trend_score")),
                "Bookmap Score": format_score(metrics.get("bookmap_score")),
            }
        )
    return pd.DataFrame(rows)


def binance_url_frame(bundle: dict[str, Any]) -> pd.DataFrame:
    rows = []
    for label, value in bundle.get("urls", {}).items():
        rows.append({"API": label.replace("_", " ").title(), "URL": value})
    return pd.DataFrame(rows)


def binance_stream_frame(bundle: dict[str, Any]) -> pd.DataFrame:
    rows = []
    for label, value in bundle.get("streams", {}).items():
        rows.append({"Group": label.replace("_", " ").title(), "Stream": json.dumps(value, ensure_ascii=False) if isinstance(value, list) else value})
    rows.append({"Group": "Subscribe Message", "Stream": json.dumps(subscribe_message(bundle["streams"]["bookmap"]), ensure_ascii=False)})
    rows.append({"Group": "Unsubscribe Message", "Stream": json.dumps(unsubscribe_message(bundle["streams"]["bookmap"]), ensure_ascii=False)})
    rows.append({"Group": "List Subscriptions", "Stream": json.dumps(list_subscriptions_message(), ensure_ascii=False)})
    rows.append({"Group": "Set Combined", "Stream": json.dumps(set_combined_property_message(True), ensure_ascii=False)})
    rows.append({"Group": "Get Combined", "Stream": json.dumps(get_combined_property_message(), ensure_ascii=False)})
    return pd.DataFrame(rows)


def binance_field_map_frame() -> pd.DataFrame:
    rows = []
    for group, mapping in [("Trade", TRADE_FIELDS), ("Kline", KLINE_FIELDS), ("Depth", DEPTH_FIELDS), ("BookTicker", BOOK_TICKER_FIELDS)]:
        for key, value in mapping.items():
            rows.append({"Payload": group, "Field": key, "Meaning": value})
    return pd.DataFrame(rows)


def binance_order_book_chart(bundle: dict[str, Any]) -> go.Figure:
    depth = bundle.get("depth", pd.DataFrame())
    fig = go.Figure()
    if depth.empty:
        fig.update_layout(template="plotly_white", height=440, title="Order book unavailable")
        return fig
    frame = depth.copy()
    frame["signed_notional"] = frame.apply(lambda row: row["notional"] if row["side"] == "Bid" else -row["notional"], axis=1)
    fig.add_trace(
        go.Bar(
            y=frame["price"],
            x=frame["signed_notional"],
            orientation="h",
            marker_color=frame["side"].map({"Bid": "#0a6c71", "Ask": "#b83f54"}),
            text=frame["quantity"].map(lambda value: format_large_number(value)),
            hovertemplate="Side=%{customdata[0]}<br>Price=%{y}<br>Notional=%{x:$,.0f}<extra></extra>",
            customdata=frame[["side"]],
        )
    )
    fig.update_layout(
        template="plotly_white",
        height=440,
        margin={"l": 10, "r": 10, "t": 45, "b": 10},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        title=f"{bundle['symbol']} depth ladder",
        xaxis_title="Bid notional (+) / Ask notional (-)",
        yaxis_title="Price",
    )
    return fig


def binance_candlestick_chart(bundle: dict[str, Any]) -> go.Figure:
    klines = bundle.get("klines", pd.DataFrame())
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.72, 0.28], vertical_spacing=0.03)
    if klines.empty:
        fig.update_layout(template="plotly_white", height=520, title="Klines unavailable")
        return fig
    frame = klines.copy()
    fig.add_trace(
        go.Candlestick(
            x=frame["open_time"],
            open=frame["open"],
            high=frame["high"],
            low=frame["low"],
            close=frame["close"],
            name="Candles",
            increasing_line_color="#0a6c71",
            decreasing_line_color="#b83f54",
        ),
        row=1,
        col=1,
    )
    frame["ema_20"] = frame["close"].ewm(span=20, adjust=False).mean()
    frame["ema_50"] = frame["close"].ewm(span=50, adjust=False).mean()
    fig.add_trace(go.Scatter(x=frame["open_time"], y=frame["ema_20"], mode="lines", name="EMA 20", line={"color": "#f47f4b", "width": 2}), row=1, col=1)
    fig.add_trace(go.Scatter(x=frame["open_time"], y=frame["ema_50"], mode="lines", name="EMA 50", line={"color": "#1d5f68", "width": 2}), row=1, col=1)
    fig.add_trace(go.Bar(x=frame["open_time"], y=frame["quote_volume"], name="Quote Volume", marker_color="rgba(29,95,104,0.45)"), row=2, col=1)
    fig.update_layout(
        template="plotly_white",
        height=520,
        margin={"l": 10, "r": 10, "t": 45, "b": 10},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        title=f"{bundle['symbol']} candles + volume",
        xaxis_rangeslider_visible=False,
        hovermode="x unified",
        legend={"orientation": "h", "y": 1.04, "x": 1, "xanchor": "right"},
    )
    fig.update_yaxes(gridcolor="rgba(16,32,40,0.08)")
    return fig


def binance_trades_chart(bundle: dict[str, Any]) -> go.Figure:
    trades = bundle.get("trades", pd.DataFrame())
    fig = go.Figure()
    if trades.empty:
        fig.update_layout(template="plotly_white", height=420, title="Trades unavailable")
        return fig
    frame = trades.tail(220).copy()
    fig.add_trace(
        go.Scatter(
            x=frame["time"],
            y=frame["price"],
            mode="markers",
            marker={
                "size": frame["quote_quantity"].apply(lambda value: max(6, min(28, math.log10(max(float(value or 1), 1)) * 3))),
                "color": frame["side"].map({"Buy Taker": "#0a6c71", "Sell Taker": "#b83f54"}),
                "opacity": 0.76,
                "line": {"width": 0},
            },
            customdata=frame[["side", "quantity", "quote_quantity"]],
            hovertemplate="%{customdata[0]}<br>Price=%{y}<br>Qty=%{customdata[1]:,.6f}<br>Quote=%{customdata[2]:,.2f}<extra></extra>",
            name="Trades",
        )
    )
    fig.update_layout(
        template="plotly_white",
        height=420,
        margin={"l": 10, "r": 10, "t": 45, "b": 10},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        title=f"{bundle['symbol']} time & sales",
        yaxis_title="Price",
    )
    return fig


def binance_flow_chart(bundle: dict[str, Any]) -> go.Figure:
    metrics = bundle["metrics"]
    values = [metrics.get("buy_quote") or 0, metrics.get("sell_quote") or 0]
    fig = go.Figure(
        data=[
            go.Pie(
                labels=["Buy Taker Quote", "Sell Taker Quote"],
                values=values,
                hole=0.58,
                marker={"colors": ["#0a6c71", "#b83f54"]},
            )
        ]
    )
    fig.update_layout(
        template="plotly_white",
        height=360,
        margin={"l": 10, "r": 10, "t": 45, "b": 10},
        paper_bgcolor="rgba(0,0,0,0)",
        title="Recent order-flow pressure",
    )
    return fig


def binance_score_radar_chart(bundle: dict[str, Any]) -> go.Figure:
    metrics = bundle["metrics"]
    labels = ["Bookmap", "Liquidity", "Flow", "Trend", "Momentum", "Risk"]
    values = [
        metrics.get("bookmap_score") or 0,
        metrics.get("liquidity_score") or 0,
        metrics.get("flow_score") or 0,
        metrics.get("tech_trend_score") or 0,
        metrics.get("tech_technical_momentum_score") or 0,
        metrics.get("tech_risk_score") or 0,
    ]
    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(r=values + [values[0]], theta=labels + [labels[0]], fill="toself", line={"color": "#f47f4b", "width": 3}, name=bundle["symbol"]))
    fig.update_layout(
        template="plotly_white",
        height=360,
        margin={"l": 10, "r": 10, "t": 45, "b": 10},
        paper_bgcolor="rgba(0,0,0,0)",
        polar={"radialaxis": {"visible": True, "range": [0, 100]}},
        title="Bookmap factor radar",
        showlegend=False,
    )
    return fig


def binance_multi_score_chart(bundles: dict[str, dict[str, Any]]) -> go.Figure:
    frame = pd.DataFrame(
        [
            {
                "Symbol": symbol,
                "Bookmap": bundle["metrics"].get("bookmap_score"),
                "Liquidity": bundle["metrics"].get("liquidity_score"),
                "Flow": bundle["metrics"].get("flow_score"),
                "Trend": bundle["metrics"].get("tech_trend_score"),
            }
            for symbol, bundle in bundles.items()
        ]
    )
    frame = frame.melt(id_vars=["Symbol"], var_name="Factor", value_name="Score").dropna(subset=["Score"])
    if frame.empty:
        fig = go.Figure()
        fig.update_layout(template="plotly_white", height=420, title="Score comparison unavailable")
        return fig
    fig = px.bar(frame, x="Symbol", y="Score", color="Factor", barmode="group", height=420, color_discrete_sequence=["#0a6c71", "#f47f4b", "#b83f54", "#1d5f68"])
    fig.update_layout(
        template="plotly_white",
        margin={"l": 10, "r": 10, "t": 45, "b": 10},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        title="Crypto score comparison",
        yaxis_range=[0, 100],
    )
    return fig


def metrics_export_frame(bundle: dict[str, Any]) -> pd.DataFrame:
    rows = []
    currency = bundle.get("currency", "")
    for definition in METRIC_DEFINITIONS:
        raw_value = bundle["metrics"].get(definition.key)
        rows.append(
            {
                "key": definition.key,
                "label": definition.label,
                "category": definition.category,
                "raw_value": "" if raw_value is None else str(raw_value),
                "display_value": format_by_type(raw_value, definition.kind, currency),
                "source": provider_label(bundle["sources"].get(definition.key)),
                "assessment": status_text(definition.key, raw_value),
            }
        )
    return pd.DataFrame(rows)


# ============================================================
# Theme presets — user-selectable + customizable
# Each preset declares its own surface + base mode so the override is consistent.
# ============================================================
THEME_PRESETS: dict = {
    "Atlas Light":    {"accent": "#d35a1f", "ink": "#0b1220", "bg": "#f4f6fb", "surface": "#ffffff", "is_dark": False},
    "Atlas Dark":     {"accent": "#ff9a5c", "ink": "#f1f5f9", "bg": "#050a13", "surface": "#11202f", "is_dark": True},
    "Bloomberg":      {"accent": "#ff9c33", "ink": "#000000", "bg": "#fff7ed", "surface": "#fffbeb", "is_dark": False},
    "Bloomberg Dark": {"accent": "#ffaa00", "ink": "#fefce8", "bg": "#000000", "surface": "#1a1410", "is_dark": True},
    "Terminal Green": {"accent": "#22c55e", "ink": "#bbf7d0", "bg": "#022c22", "surface": "#064e3b", "is_dark": True},
    "Ocean":          {"accent": "#0ea5e9", "ink": "#0f172a", "bg": "#ecfeff", "surface": "#ffffff", "is_dark": False},
    "Ocean Dark":     {"accent": "#38bdf8", "ink": "#e0f2fe", "bg": "#0c1320", "surface": "#1e293b", "is_dark": True},
    "Sunset":         {"accent": "#f97316", "ink": "#1c1917", "bg": "#fff7ed", "surface": "#ffffff", "is_dark": False},
    "Solarized":      {"accent": "#b58900", "ink": "#073642", "bg": "#fdf6e3", "surface": "#eee8d5", "is_dark": False},
    "Solarized Dark": {"accent": "#cb4b16", "ink": "#93a1a1", "bg": "#002b36", "surface": "#073642", "is_dark": True},
    "Custom":         {"accent": "#d35a1f", "ink": "#0b1220", "bg": "#f4f6fb", "surface": "#ffffff", "is_dark": False},
}


def _derive_theme_overrides(preset: dict, accent: str, ink: str, bg: str, surface: str) -> dict:
    """Build the complete CSS-variable override dict from the active palette.

    Derives muted/border/shadow values appropriate to light vs dark surfaces so
    the dashboard never ends up with mismatched contrast.
    """
    is_dark = bool(preset.get("is_dark"))

    # Compute muted text (60% opacity over background)
    muted = "rgba(241,245,249,0.65)" if is_dark else "rgba(15,23,42,0.55)"
    muted_2 = "rgba(241,245,249,0.45)" if is_dark else "rgba(15,23,42,0.40)"
    border = "rgba(203,218,230,0.20)" if is_dark else "rgba(15,23,42,0.10)"
    border_strong = "rgba(203,218,230,0.40)" if is_dark else "rgba(15,23,42,0.22)"
    shadow_base = "rgba(0,0,0,0.40)" if is_dark else "rgba(11,18,32,0.08)"

    # Accent-soft = accent at ~16% alpha for backgrounds
    accent_soft = _hex_to_rgba(accent, 0.16 if is_dark else 0.10)
    accent_2 = _adjust_brightness(accent, 0.10 if is_dark else -0.05)

    return {
        "accent": accent,
        "accent-2": accent_2,
        "accent-ink": accent if is_dark else _adjust_brightness(accent, -0.20),
        "accent-soft": accent_soft,
        "ink": ink,
        "ink-strong": ink,
        "muted": muted,
        "muted-2": muted_2,
        "bg-0": bg,
        "bg-1": bg,
        "surface": surface,
        "surface-strong": surface,
        "surface-elev": surface,
        "input-bg": surface,
        "tab-bg": surface,
        "sidebar-bg": _adjust_brightness(bg, -0.04 if not is_dark else 0.04),
        "border": border,
        "border-strong": border_strong,
        "shadow-sm": f"0 1px 2px {shadow_base}",
        "shadow-md": f"0 6px 14px {shadow_base}, 0 12px 28px {shadow_base}",
        "shadow-lg": f"0 16px 36px {shadow_base}, 0 28px 60px {shadow_base}",
    }


def _hex_to_rgba(hex_color: str, alpha: float) -> str:
    """Convert #rrggbb to rgba(r,g,b,alpha)."""
    h = hex_color.lstrip("#")
    if len(h) == 3:
        h = "".join(c * 2 for c in h)
    if len(h) != 6:
        return hex_color
    try:
        r = int(h[0:2], 16)
        g = int(h[2:4], 16)
        b = int(h[4:6], 16)
        return f"rgba({r},{g},{b},{alpha:.2f})"
    except ValueError:
        return hex_color


def _adjust_brightness(hex_color: str, factor: float) -> str:
    """Lighten (positive factor) / darken (negative factor) a hex color."""
    h = hex_color.lstrip("#")
    if len(h) == 3:
        h = "".join(c * 2 for c in h)
    if len(h) != 6:
        return hex_color
    try:
        r = int(h[0:2], 16)
        g = int(h[2:4], 16)
        b = int(h[4:6], 16)
        if factor > 0:
            r = int(r + (255 - r) * factor)
            g = int(g + (255 - g) * factor)
            b = int(b + (255 - b) * factor)
        else:
            r = int(r * (1 + factor))
            g = int(g * (1 + factor))
            b = int(b * (1 + factor))
        return f"#{max(0, min(255, r)):02x}{max(0, min(255, g)):02x}{max(0, min(255, b)):02x}"
    except ValueError:
        return hex_color

THEME_FONT_OPTIONS: dict = {
    "Inter (default)":     "'Inter'",
    "IBM Plex Sans":       "'IBM Plex Sans'",
    "Roboto":              "'Roboto'",
    "JetBrains Mono":      "'JetBrains Mono'",
    "Source Sans 3":       "'Source Sans 3'",
    "System UI":           "system-ui",
}


def _build_theme_override(overrides: dict | None) -> str:
    """Convert a flat dict of CSS variables into a `:root` override block."""
    if not overrides:
        return ""
    rules = []
    for key, value in overrides.items():
        if value:
            rules.append(f"            --{key}: {value};")
    if not rules:
        return ""
    return "\n        :root {\n" + "\n".join(rules) + "\n        }\n"


def inject_styles(theme_mode: str = "System", overrides: dict | None = None) -> None:
    """Inject the Atlas Terminal stylesheet (light + dark)."""
    google_font = (
        "@import url('https://fonts.googleapis.com/css2?"
        "family=Inter:wght@400;500;600;700;800&"
        "family=JetBrains+Mono:wght@400;500;600&display=swap');"
    )

    light_vars = """
        :root {
            --bg-0: #f4f6fb;
            --bg-1: #e7ecf4;
            --page-bg: radial-gradient(1200px 600px at 10% -10%, #fff5ec 0%, transparent 60%),
                        radial-gradient(900px 500px at 100% 0%, #e9f3ff 0%, transparent 55%),
                        linear-gradient(180deg, var(--bg-0) 0%, var(--bg-1) 100%);
            --surface: rgba(255,255,255,0.94);
            --surface-strong: #ffffff;
            --surface-elev: #ffffff;
            --sidebar-bg: #eef2f8;
            --input-bg: #ffffff;
            --tab-bg: rgba(255,255,255,0.86);
            --button-bg: linear-gradient(180deg, #ffffff 0%, #fff1e8 100%);
            --border: rgba(15,23,42,0.10);
            --border-strong: rgba(15,23,42,0.18);
            --ink: #0b1220;
            --ink-strong: #02060f;
            --muted: #475569;
            --muted-2: #64748b;
            --hero-a: #0b1220;
            --hero-b: #144a78;
            --hero-c: #1c6e8c;
            --accent: #d35a1f;
            --accent-2: #ea7a3c;
            --accent-ink: #9c3f12;
            --accent-soft: #fff1e6;
            --teal: #0a8f95;
            --teal-soft: #d8f1ee;
            --teal-ink: #075f65;
            --indigo: #4f46e5;
            --indigo-soft: #eef0ff;
            --green: #047857;
            --green-soft: #d6f3e3;
            --amber: #b45309;
            --amber-soft: #fff1d6;
            --red: #be123c;
            --red-soft: #fde6ec;
            --shadow-sm: 0 1px 2px rgba(11,18,32,0.04), 0 1px 3px rgba(11,18,32,0.06);
            --shadow-md: 0 6px 14px rgba(11,18,32,0.06), 0 12px 28px rgba(11,18,32,0.08);
            --shadow-lg: 0 16px 36px rgba(11,18,32,0.10), 0 28px 60px rgba(11,18,32,0.10);
        }
    """
    dark_vars = """
        :root {
            --bg-0: #050a13;
            --bg-1: #0a1422;
            --page-bg: radial-gradient(1200px 600px at 8% -10%, rgba(255,154,92,0.12) 0%, transparent 55%),
                        radial-gradient(1000px 600px at 100% 0%, rgba(80,190,210,0.10) 0%, transparent 60%),
                        linear-gradient(180deg, var(--bg-0) 0%, var(--bg-1) 100%);
            --surface: rgba(18,28,42,0.92);
            --surface-strong: #11202f;
            --surface-elev: #16283a;
            --sidebar-bg: #0a1622;
            --input-bg: #0f1d2c;
            --tab-bg: rgba(18,28,42,0.94);
            --button-bg: linear-gradient(180deg, #1a3046 0%, #0f1f2e 100%);
            --border: rgba(203,218,230,0.16);
            --border-strong: rgba(203,218,230,0.30);
            --ink: #f1f5f9;
            --ink-strong: #ffffff;
            --muted: #cbd5e1;
            --muted-2: #94a3b8;
            --hero-a: #050d18;
            --hero-b: #0f3956;
            --hero-c: #1d6a82;
            --accent: #ff9a5c;
            --accent-2: #ffb37a;
            --accent-ink: #ffd2b1;
            --accent-soft: rgba(255,154,92,0.16);
            --teal: #4ec9cf;
            --teal-soft: rgba(78,201,207,0.18);
            --teal-ink: #9beff3;
            --indigo: #8b8fff;
            --indigo-soft: rgba(139,143,255,0.18);
            --green: #5bd58e;
            --green-soft: rgba(91,213,142,0.18);
            --amber: #f4bd58;
            --amber-soft: rgba(244,189,88,0.18);
            --red: #ff7188;
            --red-soft: rgba(255,113,136,0.18);
            --shadow-sm: 0 1px 2px rgba(0,0,0,0.40);
            --shadow-md: 0 8px 24px rgba(0,0,0,0.44);
            --shadow-lg: 0 18px 48px rgba(0,0,0,0.55);
        }
    """
    normalized_theme = str(theme_mode or "System").strip().lower()
    if normalized_theme == "dark":
        theme_vars = dark_vars
    elif normalized_theme == "light":
        theme_vars = light_vars
    else:
        theme_vars = light_vars + "\n@media (prefers-color-scheme: dark) {\n" + dark_vars.replace(":root", ":root") + "\n}"

    # Apply user overrides (color pickers / preset values) as a final :root block
    override_css = _build_theme_override(overrides)
    if override_css:
        theme_vars = theme_vars + override_css

    common_css = """
        * { box-sizing: border-box; }
        html, body, [class*="css"] {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI",
                         Roboto, Helvetica, Arial, sans-serif !important;
            -webkit-font-smoothing: antialiased;
            -moz-osx-font-smoothing: grayscale;
        }
        code, pre, kbd, samp, .stCodeBlock {
            font-family: 'JetBrains Mono', ui-monospace, SFMono-Regular, Menlo,
                         Monaco, Consolas, "Liberation Mono", "Courier New", monospace !important;
        }
        .stApp {
            background: var(--page-bg);
            color: var(--ink);
        }
        [data-testid="stSidebar"] {
            background: var(--sidebar-bg);
            border-right: 1px solid var(--border);
        }
        [data-testid="stSidebar"] .stMarkdown,
        [data-testid="stSidebar"] label,
        [data-testid="stSidebar"] .stRadio label,
        [data-testid="stSidebar"] .stCheckbox label {
            color: var(--ink) !important;
        }
        .stApp p, .stApp li, .stApp label,
        .stApp h1, .stApp h2, .stApp h3, .stApp h4, .stApp h5, .stApp h6 {
            color: var(--ink);
        }
        .stApp h1 { font-weight: 800; letter-spacing: -0.02em; }
        .stApp h2 { font-weight: 760; letter-spacing: -0.01em; }
        .stApp h3 { font-weight: 700; letter-spacing: -0.005em; }
        .stCaption, [data-testid="stCaptionContainer"], .stMarkdown small {
            color: var(--muted) !important;
        }
        .stTextInput input, .stNumberInput input, .stTextArea textarea {
            background: var(--input-bg) !important;
            color: var(--ink) !important;
            border: 1px solid var(--border) !important;
            border-radius: 10px !important;
            transition: border-color .15s ease, box-shadow .15s ease;
        }
        .stTextInput input:focus, .stNumberInput input:focus, .stTextArea textarea:focus {
            border-color: var(--accent) !important;
            box-shadow: 0 0 0 3px var(--accent-soft) !important;
            outline: none !important;
        }
        [data-baseweb="select"] > div, [data-baseweb="radio"] {
            background: var(--input-bg);
            color: var(--ink);
            border-color: var(--border);
            border-radius: 10px;
        }
        .block-container {
            max-width: 1500px;
            padding-top: 1.1rem;
            padding-bottom: 2.4rem;
        }
        /* === HERO === */
        .hero {
            position: relative;
            overflow: hidden;
            border-radius: 18px;
            padding: 30px 32px;
            color: #f8fafc;
            background: linear-gradient(135deg, var(--hero-a) 0%, var(--hero-b) 55%, var(--hero-c) 100%);
            border: 1px solid rgba(255,255,255,0.10);
            box-shadow: var(--shadow-lg);
        }
        .hero::before {
            content: "";
            position: absolute;
            top: -20%; right: -10%;
            width: 480px; height: 480px;
            background: radial-gradient(closest-side, rgba(255,154,92,0.35), transparent 70%);
            pointer-events: none;
        }
        .hero::after {
            content: "";
            position: absolute;
            bottom: -30%; left: -5%;
            width: 420px; height: 420px;
            background: radial-gradient(closest-side, rgba(78,201,207,0.22), transparent 70%);
            pointer-events: none;
        }
        .hero-kicker {
            text-transform: uppercase;
            letter-spacing: 0.18em;
            font-size: 0.74rem;
            font-weight: 700;
            opacity: 0.78;
        }
        .hero-title {
            margin: 0.45rem 0 0;
            font-size: 2.5rem;
            font-weight: 800;
            line-height: 1.04;
            letter-spacing: -0.02em;
        }
        .hero-symbol {
            display: inline-block;
            margin-left: 10px;
            font-family: 'JetBrains Mono', monospace;
            font-size: 1.55rem;
            font-weight: 600;
            padding: 4px 12px;
            border-radius: 8px;
            background: rgba(255,255,255,0.10);
            border: 1px solid rgba(255,255,255,0.16);
            vertical-align: middle;
        }
        .hero-subtitle {
            margin-top: 0.85rem;
            color: rgba(248,250,252,0.82);
            font-size: 0.98rem;
            line-height: 1.5;
            max-width: 920px;
        }
        .hero-strip {
            position: relative;
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
            margin-top: 1.15rem;
            z-index: 1;
        }
        .hero-pill {
            padding: 8px 14px;
            border-radius: 999px;
            font-size: 0.84rem;
            font-weight: 500;
            background: rgba(255,255,255,0.08);
            border: 1px solid rgba(255,255,255,0.16);
            backdrop-filter: blur(10px);
            transition: background .15s ease, transform .15s ease;
        }
        .hero-pill:hover {
            background: rgba(255,255,255,0.14);
            transform: translateY(-1px);
        }
        .hero-pill b {
            color: #ffd2b1;
            margin-right: 4px;
            font-weight: 700;
        }
        /* === METRIC CARD === */
        .metric-card {
            background: var(--surface-strong);
            border: 1px solid var(--border);
            border-radius: 14px;
            padding: 18px 20px;
            min-height: 130px;
            box-shadow: var(--shadow-sm);
            transition: transform .15s ease, box-shadow .15s ease, border-color .15s ease;
        }
        .metric-card:hover {
            transform: translateY(-2px);
            box-shadow: var(--shadow-md);
            border-color: var(--border-strong);
        }
        .metric-label {
            color: var(--muted);
            text-transform: uppercase;
            letter-spacing: 0.10em;
            font-size: 0.72rem;
            font-weight: 600;
        }
        .metric-value {
            margin-top: 0.55rem;
            font-size: 1.7rem;
            font-weight: 760;
            color: var(--ink-strong);
            line-height: 1.1;
            font-feature-settings: "tnum";
        }
        .metric-meta {
            margin-top: 0.5rem;
            color: var(--muted);
            font-size: 0.85rem;
        }
        .panel {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 14px;
            padding: 20px;
            box-shadow: var(--shadow-sm);
        }
        .panel-title {
            font-size: 0.74rem;
            letter-spacing: 0.14em;
            text-transform: uppercase;
            color: var(--muted);
            font-weight: 600;
        }
        .panel-big {
            margin-top: 0.55rem;
            font-size: 2rem;
            font-weight: 760;
            color: var(--ink-strong);
        }
        /* === SOURCE CHIPS === */
        .chip-row {
            display: flex;
            gap: 8px;
            flex-wrap: wrap;
            margin: 0.5rem 0 0.2rem;
        }
        .source-chip {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            border-radius: 999px;
            padding: 6px 12px;
            border: 1px solid var(--border);
            font-size: 0.80rem;
            font-weight: 500;
            background: var(--surface-strong);
            color: var(--ink);
            transition: transform .15s ease;
        }
        .source-chip:hover { transform: translateY(-1px); }
        .source-chip::before {
            content: "";
            width: 7px; height: 7px;
            border-radius: 50%;
            background: var(--muted);
        }
        .source-chip.ok { background: var(--green-soft); color: var(--green); border-color: rgba(4,120,87,0.25); }
        .source-chip.ok::before { background: var(--green); }
        .source-chip.partial { background: var(--amber-soft); color: var(--amber); border-color: rgba(180,83,9,0.25); }
        .source-chip.partial::before { background: var(--amber); }
        .source-chip.error { background: var(--red-soft); color: var(--red); border-color: rgba(190,18,60,0.25); }
        .source-chip.error::before { background: var(--red); }
        .source-chip.disabled { background: var(--tab-bg); color: var(--muted-2); }
        /* === TABS === */
        .stTabs [data-baseweb="tab-list"] {
            gap: 0.4rem;
            margin-bottom: 0.5rem;
            border-bottom: 1px solid var(--border);
            padding-bottom: 4px;
        }
        .stTabs [data-baseweb="tab"] {
            border-radius: 10px;
            background: var(--tab-bg);
            border: 1px solid var(--border);
            padding: 0.5rem 1rem;
            color: var(--ink);
            font-weight: 540;
            transition: all .15s ease;
        }
        .stTabs [data-baseweb="tab"]:hover {
            background: var(--surface-strong);
            border-color: var(--border-strong);
        }
        .stTabs [aria-selected="true"] {
            background: linear-gradient(180deg, var(--accent-soft), transparent 130%) !important;
            color: var(--accent) !important;
            border-color: var(--accent) !important;
            font-weight: 650;
        }
        /* === BUTTONS === */
        .stButton > button, .stDownloadButton > button {
            border-radius: 10px;
            border: 1px solid var(--border-strong);
            background: var(--button-bg);
            color: var(--ink);
            font-weight: 560;
            transition: all .15s ease;
        }
        .stButton > button:hover, .stDownloadButton > button:hover {
            border-color: var(--accent);
            color: var(--accent);
            transform: translateY(-1px);
            box-shadow: var(--shadow-sm);
        }
        .stButton > button[kind="primary"] {
            background: linear-gradient(180deg, var(--accent-2), var(--accent)) !important;
            color: #ffffff !important;
            border-color: var(--accent) !important;
        }
        /* === DATAFRAMES === */
        [data-testid="stDataFrame"] {
            border-radius: 12px;
            overflow: hidden;
            border: 1px solid var(--border);
        }
        /* === METRICS (st.metric) === */
        [data-testid="stMetric"] {
            background: var(--surface-strong);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 14px 16px;
            box-shadow: var(--shadow-sm);
        }
        [data-testid="stMetricLabel"] {
            color: var(--muted);
            font-size: 0.74rem;
            text-transform: uppercase;
            letter-spacing: 0.10em;
            font-weight: 600;
        }
        [data-testid="stMetricValue"] {
            color: var(--ink-strong);
            font-weight: 760;
            font-feature-settings: "tnum";
        }
        /* === ALERTS === */
        .stAlert {
            border-radius: 12px;
            border: 1px solid var(--border);
        }
        /* === EXPANDERS === */
        [data-testid="stExpander"] {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 12px;
            box-shadow: var(--shadow-sm);
        }
        /* === NEWS CARDS === */
        .news-card {
            position: relative;
            background: var(--surface-strong);
            border: 1px solid var(--border);
            border-radius: 14px;
            padding: 16px 18px;
            margin-bottom: 12px;
            box-shadow: var(--shadow-sm);
            transition: transform .15s ease, box-shadow .15s ease, border-color .15s ease;
            overflow: hidden;
        }
        .news-card:hover {
            transform: translateY(-2px);
            box-shadow: var(--shadow-md);
            border-color: var(--border-strong);
        }
        .news-card-header {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            gap: 12px;
            margin-bottom: 8px;
        }
        .news-card-headline {
            font-size: 0.99rem;
            font-weight: 660;
            color: var(--ink-strong);
            line-height: 1.38;
            flex: 1;
            letter-spacing: -0.005em;
        }
        .news-card-headline a {
            color: var(--ink-strong);
            text-decoration: none;
            background-image: linear-gradient(var(--accent), var(--accent));
            background-size: 0 1.5px;
            background-repeat: no-repeat;
            background-position: 0 100%;
            transition: background-size .25s ease, color .15s ease;
        }
        .news-card-headline a:hover {
            color: var(--accent);
            background-size: 100% 1.5px;
        }
        .news-card-meta {
            font-size: 0.76rem;
            color: var(--muted-2);
            white-space: nowrap;
            font-feature-settings: "tnum";
        }
        .news-card-source {
            display: flex;
            align-items: center;
            gap: 6px;
            font-size: 0.78rem;
            font-weight: 600;
            color: var(--muted);
            margin-bottom: 4px;
        }
        .news-card-summary {
            font-size: 0.86rem;
            color: var(--muted);
            line-height: 1.5;
            margin-top: 6px;
            display: -webkit-box;
            -webkit-line-clamp: 3;
            -webkit-box-orient: vertical;
            overflow: hidden;
        }
        .news-sentiment-bar {
            height: 4px;
            border-radius: 2px;
            margin-top: 10px;
            opacity: 0.85;
        }
        .sentiment-positive { background: linear-gradient(90deg, var(--green) 0%, var(--teal) 100%); }
        .sentiment-negative { background: linear-gradient(90deg, var(--red) 0%, #f97316 100%); }
        .sentiment-neutral  { background: linear-gradient(90deg, var(--amber) 0%, #94a3b8 100%); }
        .news-provider-pill {
            display: inline-block;
            padding: 3px 9px;
            border-radius: 999px;
            font-size: 0.71rem;
            font-weight: 600;
            background: var(--accent-soft);
            color: var(--accent);
            border: 1px solid rgba(211,90,31,0.20);
            margin-right: 6px;
            letter-spacing: 0.02em;
        }
        .news-fuse-pill {
            display: inline-block;
            padding: 3px 9px;
            border-radius: 999px;
            font-size: 0.71rem;
            font-weight: 650;
            background: var(--indigo-soft);
            color: var(--indigo);
            border: 1px solid rgba(79,70,229,0.18);
            margin-right: 6px;
        }
        .news-cat-pill {
            display: inline-block;
            padding: 3px 9px;
            border-radius: 999px;
            font-size: 0.70rem;
            font-weight: 540;
            background: var(--surface-elev);
            color: var(--muted);
            border: 1px solid var(--border);
            margin-right: 6px;
        }
        .news-sentiment-pill {
            display: inline-block;
            padding: 3px 9px;
            border-radius: 999px;
            font-size: 0.70rem;
            font-weight: 600;
            margin-right: 6px;
        }
        .news-sentiment-pill.pos { background: var(--green-soft); color: var(--green); border: 1px solid rgba(4,120,87,0.22); }
        .news-sentiment-pill.neg { background: var(--red-soft); color: var(--red); border: 1px solid rgba(190,18,60,0.22); }
        .news-sentiment-pill.neu { background: var(--amber-soft); color: var(--amber); border: 1px solid rgba(180,83,9,0.22); }
        .sec-filing-card {
            border-left: 4px solid var(--teal);
            background: linear-gradient(90deg, var(--teal-soft) 0%, var(--surface-strong) 60%);
        }
        .sec-filing-card .news-card-source { color: var(--teal-ink); }
        /* === SECTION HEADER === */
        .section-header {
            display: flex;
            align-items: center;
            gap: 10px;
            margin: 1.4rem 0 0.6rem;
            padding-bottom: 0.4rem;
            border-bottom: 1px solid var(--border);
        }
        .section-header h3 {
            margin: 0;
            font-size: 1.08rem;
            font-weight: 700;
            color: var(--ink-strong);
        }
        .section-header .section-count {
            font-size: 0.78rem;
            color: var(--muted);
            background: var(--surface-elev);
            padding: 2px 9px;
            border-radius: 999px;
            border: 1px solid var(--border);
        }
        /* === SCROLLBAR === */
        ::-webkit-scrollbar { width: 10px; height: 10px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: var(--border-strong); border-radius: 999px; }
        ::-webkit-scrollbar-thumb:hover { background: var(--muted-2); }
    """

    st.markdown(
        "<style>\n"
        + google_font + "\n"
        + theme_vars + "\n"
        + common_css
        + "\n</style>",
        unsafe_allow_html=True,
    )


def render_hero(bundle: dict[str, Any], compare_count: int) -> None:
    metrics = bundle["metrics"]
    subtitle = bundle.get("description") or (
        "Multi-source stock intelligence blending fundamentals, valuation, momentum, "
        "DCF, news, and provider resilience into one serious research cockpit."
    )
    subtitle = str(subtitle).strip()
    if len(subtitle) > 260:
        subtitle = subtitle[:257].rstrip() + "..."

    co_name = bundle.get("company_name") or bundle.get("symbol") or "—"
    sym = bundle.get("symbol") or ""
    decision = (bundle["metrics"].get("decision_label") or "Insufficient data")
    trend = bundle["metrics"].get("trend_state") or "No data"
    providers_n = len(bundle.get("available_providers", []))
    news_n = len(bundle.get("news") or [])

    pills = [
        ("Price", format_money(metrics.get("price"), bundle.get("currency", ""))),
        (
            "Move",
            f"{format_by_type(metrics.get('change'), 'money_signed', bundle.get('currency', ''))} · "
            f"{format_by_type(metrics.get('change_pct'), 'percent_signed')}",
        ),
        ("AI Score", format_score(metrics.get("final_ai_score"))),
        ("Decision", decision),
        ("Trend", trend),
        ("Alerts", format_by_type(metrics.get("alert_count"), "integer")),
        ("Providers", str(providers_n)),
        ("News", str(news_n)),
        ("Compare", str(compare_count)),
    ]
    pills_html = "".join(
        f'<div class="hero-pill"><b>{escape(label)}</b>{escape(str(value))}</div>'
        for label, value in pills
    )
    st.markdown(
        f"""
        <div class="hero">
            <div class="hero-kicker">Atlas · Stock Intelligence Terminal</div>
            <div class="hero-title">{escape(str(co_name))}<span class="hero-symbol">{escape(str(sym))}</span></div>
            <div class="hero-subtitle">{escape(subtitle)}</div>
            <div class="hero-strip">{pills_html}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_source_chips(bundle: dict[str, Any]) -> None:
    chips: list[str] = []
    for provider, payload in bundle.get("providers", {}).items():
        status = str(payload.get("status", "partial")).lower()
        coverage = payload.get("coverage", 0)
        chips.append(
            f'<div class="source-chip {escape(status)}" title="{escape(provider_label(provider))} — {escape(status)} — {coverage} metrics">'
            f'{escape(provider_label(provider))} · {coverage}'
            f'</div>'
        )
    if not chips:
        chips.append('<div class="source-chip disabled">No providers</div>')
    st.markdown('<div class="chip-row">' + "".join(chips) + "</div>", unsafe_allow_html=True)


def render_metric_card(bundle: dict[str, Any], key: str) -> None:
    definition = METRIC_LOOKUP[key]
    value = bundle["metrics"].get(key)
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-label">{definition.label}</div>
            <div class="metric-value">{format_by_type(value, definition.kind, bundle.get('currency', ''))}</div>
            <div class="metric-meta">{status_text(key, value)} · {provider_label(bundle['sources'].get(key))}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def history_chart(bundle: dict[str, Any]) -> go.Figure:
    frame = pd.DataFrame(bundle.get("history", []))
    fig = go.Figure()
    if frame.empty:
        fig.update_layout(template="plotly_white", height=460, title="No price history available")
        return fig
    frame["date"] = pd.to_datetime(frame["date"])
    frame = frame.sort_values("date")
    frame["ema_20"] = frame["close"].ewm(span=20, adjust=False).mean()
    frame["ema_50"] = frame["close"].ewm(span=50, adjust=False).mean()
    frame["sma_200"] = frame["close"].rolling(200).mean()
    fig.add_trace(
        go.Scatter(
            x=frame["date"],
            y=frame["close"],
            mode="lines",
            name=bundle.get("symbol"),
            line={"color": "#f47f4b", "width": 3},
            fill="tozeroy",
            fillcolor="rgba(244,127,75,0.08)",
        )
    )
    if frame["ema_20"].notna().any():
        fig.add_trace(go.Scatter(x=frame["date"], y=frame["ema_20"], mode="lines", name="EMA 20", line={"color": "#0a6c71", "width": 2}))
    if frame["ema_50"].notna().any():
        fig.add_trace(go.Scatter(x=frame["date"], y=frame["ema_50"], mode="lines", name="EMA 50", line={"color": "#1f8f5f", "width": 2, "dash": "dash"}))
    if frame["sma_200"].notna().any():
        fig.add_trace(go.Scatter(x=frame["date"], y=frame["sma_200"], mode="lines", name="SMA 200", line={"color": "#4d5a63", "width": 2, "dash": "dot"}))
    metrics = bundle.get("metrics", {})
    if is_num(metrics.get("support_level")):
        fig.add_hline(y=metrics["support_level"], line_dash="dot", line_color="#1d8c58", annotation_text="Support")
    if is_num(metrics.get("resistance_level")):
        fig.add_hline(y=metrics["resistance_level"], line_dash="dot", line_color="#b83f54", annotation_text="Resistance")
    fig.update_layout(
        template="plotly_white",
        height=460,
        margin={"l": 10, "r": 10, "t": 50, "b": 10},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        title=f"{bundle.get('symbol')} price trend",
        yaxis_title=bundle.get("currency") or "Price",
        hovermode="x unified",
        legend={"orientation": "h", "y": 1.05, "x": 1, "xanchor": "right"},
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="rgba(16,32,40,0.08)")
    return fig


def annuals_chart(bundle: dict[str, Any]) -> go.Figure:
    frame = pd.DataFrame(bundle.get("annuals", []))
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    if frame.empty:
        fig.update_layout(template="plotly_white", height=420, title="No annual fundamentals available")
        return fig
    frame = frame.sort_values("year")
    for column in ["revenue", "net_income", "free_cash_flow"]:
        if column in frame:
            frame[column] = frame[column].apply(lambda value: to_num(value) / 1e9 if is_num(to_num(value)) else None)
    fig.add_trace(go.Bar(x=frame["year"], y=frame["revenue"], name="Revenue (B)", marker_color="#0a6c71"), secondary_y=False)
    fig.add_trace(go.Bar(x=frame["year"], y=frame["free_cash_flow"], name="FCF (B)", marker_color="#f47f4b"), secondary_y=False)
    fig.add_trace(go.Scatter(x=frame["year"], y=frame["net_income"], name="Net Income (B)", mode="lines+markers", line={"color": "#102028", "width": 3}), secondary_y=True)
    fig.update_layout(
        template="plotly_white",
        height=420,
        margin={"l": 10, "r": 10, "t": 40, "b": 10},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        barmode="group",
        title=f"Annual fundamentals · {provider_label(bundle.get('annuals_provider'))}",
        legend={"orientation": "h", "y": 1.05, "x": 1, "xanchor": "right"},
    )
    fig.update_yaxes(title_text="Revenue / FCF (Billions)", secondary_y=False)
    fig.update_yaxes(title_text="Net Income (Billions)", secondary_y=True)
    return fig


def dcf_sensitivity_chart(bundle: dict[str, Any]) -> go.Figure:
    metrics = bundle["metrics"]
    if not is_num(metrics.get("free_cash_flow")) or not is_num(metrics.get("shares_outstanding")):
        fig = go.Figure()
        fig.update_layout(template="plotly_white", height=420, title="DCF sensitivity unavailable")
        return fig
    growth_base = bundle["assumptions"]["growth"]
    discount_base = bundle["assumptions"]["discount"]
    growths = [round(max(-0.02, growth_base - 0.04 + i * 0.02), 3) for i in range(5)]
    discounts = [round(max(0.06, discount_base - 0.03 + i * 0.02), 3) for i in range(5)]
    z_values: list[list[float | None]] = []
    for discount in discounts:
        row = []
        for growth in growths:
            result = calculate_smart_dcf(
                metrics.get("free_cash_flow"),
                metrics.get("shares_outstanding"),
                {
                    **bundle["assumptions"],
                    "growth": growth,
                    "discount": discount,
                },
                metrics.get("price"),
                metrics.get("revenue_growth"),
                metrics.get("earnings_growth"),
            )
            row.append(result["fair_value_after_mos"])
        z_values.append(row)
    fig = go.Figure(
        data=go.Heatmap(
            z=z_values,
            x=[f"{value*100:.1f}%" for value in growths],
            y=[f"{value*100:.1f}%" for value in discounts],
            colorscale="Tealrose",
            colorbar_title=f"Fair Value ({bundle.get('currency') or ''})",
        )
    )
    fig.update_layout(
        template="plotly_white",
        height=420,
        margin={"l": 10, "r": 10, "t": 40, "b": 10},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        title="DCF sensitivity matrix",
        xaxis_title="Growth rate",
        yaxis_title="Discount rate",
    )
    return fig


def score_radar_chart(bundle: dict[str, Any]) -> go.Figure:
    labels = ["Fundamental", "Trend", "Momentum", "Volume", "Risk", "Final"]
    values = [
        bundle["metrics"].get("fundamental_score"),
        bundle["metrics"].get("trend_score"),
        bundle["metrics"].get("momentum_score"),
        bundle["metrics"].get("volume_score"),
        bundle["metrics"].get("risk_score"),
        bundle["metrics"].get("final_ai_score"),
    ]
    values = [to_num(value) or 0 for value in values]
    values.append(values[0])
    labels.append(labels[0])
    fig = go.Figure()
    fig.add_trace(
        go.Scatterpolar(
            r=values,
            theta=labels,
            fill="toself",
            line={"color": "#f47f4b", "width": 3},
            fillcolor="rgba(244,127,75,0.20)",
            name=bundle.get("symbol"),
        )
    )
    fig.update_layout(
        template="plotly_white",
        height=420,
        margin={"l": 10, "r": 10, "t": 40, "b": 10},
        polar={"radialaxis": {"visible": True, "range": [0, 100]}},
        showlegend=False,
        title="Factor radar",
    )
    return fig


def comparison_performance_chart(bundles: dict[str, dict[str, Any]]) -> go.Figure:
    fig = go.Figure()
    for symbol, bundle in bundles.items():
        frame = pd.DataFrame(bundle.get("history", []))
        if frame.empty:
            continue
        frame["date"] = pd.to_datetime(frame["date"])
        frame = frame.sort_values("date").tail(260)
        base = frame["close"].iloc[0]
        if not base:
            continue
        frame["normalized"] = frame["close"] / base * 100
        fig.add_trace(go.Scatter(x=frame["date"], y=frame["normalized"], mode="lines", name=symbol, line={"width": 3}))
    fig.update_layout(
        template="plotly_white",
        height=460,
        margin={"l": 10, "r": 10, "t": 45, "b": 10},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        title="Relative performance (base 100)",
        hovermode="x unified",
        legend={"orientation": "h", "y": 1.05, "x": 1, "xanchor": "right"},
        yaxis_title="Base 100",
    )
    return fig


def comparison_radar_chart(bundles: dict[str, dict[str, Any]]) -> go.Figure:
    fig = go.Figure()
    labels = ["Fundamental", "Trend", "Momentum", "Volume", "Risk", "Final"]
    for symbol, bundle in bundles.items():
        values = [
            bundle["metrics"].get("fundamental_score") or 0,
            bundle["metrics"].get("trend_score") or 0,
            bundle["metrics"].get("momentum_score") or 0,
            bundle["metrics"].get("volume_score") or 0,
            bundle["metrics"].get("risk_score") or 0,
            bundle["metrics"].get("final_ai_score") or 0,
        ]
        fig.add_trace(go.Scatterpolar(r=values + [values[0]], theta=labels + [labels[0]], fill="toself", name=symbol, opacity=0.45))
    fig.update_layout(
        template="plotly_white",
        height=460,
        margin={"l": 10, "r": 10, "t": 45, "b": 10},
        polar={"radialaxis": {"visible": True, "range": [0, 100]}},
        title="Multi-stock factor radar",
    )
    return fig


def comparison_bubble_chart(bundles: dict[str, dict[str, Any]]) -> go.Figure:
    rows = []
    for symbol, bundle in bundles.items():
        metrics = bundle["metrics"]
        rows.append(
            {
                "Symbol": symbol,
                "Revenue Growth": metrics.get("revenue_growth"),
                "ROE": metrics.get("roe"),
                "Market Cap": metrics.get("market_cap"),
                "Final AI Score": metrics.get("final_ai_score"),
            }
        )
    frame = pd.DataFrame(rows)
    frame = frame.dropna(subset=["Revenue Growth", "ROE", "Market Cap", "Final AI Score"], how="any")
    if frame.empty:
        fig = go.Figure()
        fig.update_layout(template="plotly_white", height=420, title="Bubble comparison unavailable")
        return fig
    fig = px.scatter(
        frame,
        x="Revenue Growth",
        y="ROE",
        size="Market Cap",
        color="Final AI Score",
        text="Symbol",
        color_continuous_scale="Tealgrn",
        height=420,
    )
    fig.update_traces(textposition="top center")
    fig.update_layout(
        template="plotly_white",
        margin={"l": 10, "r": 10, "t": 40, "b": 10},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        title="Growth vs quality map",
    )
    fig.update_xaxes(tickformat=".0%")
    fig.update_yaxes(tickformat=".0%")
    return fig


def comparison_valuation_chart(bundles: dict[str, dict[str, Any]]) -> go.Figure:
    rows = []
    for symbol, bundle in bundles.items():
        metrics = bundle["metrics"]
        rows.extend(
            [
                {"Symbol": symbol, "Metric": "P/E", "Value": metrics.get("pe_ratio")},
                {"Symbol": symbol, "Metric": "P/B", "Value": metrics.get("pb_ratio")},
                {"Symbol": symbol, "Metric": "EV/EBITDA", "Value": metrics.get("ev_ebitda")},
            ]
        )
    frame = pd.DataFrame(rows).dropna(subset=["Value"], how="any")
    if frame.empty:
        fig = go.Figure()
        fig.update_layout(template="plotly_white", height=420, title="Valuation comparison unavailable")
        return fig
    fig = px.bar(frame, x="Symbol", y="Value", color="Metric", barmode="group", height=420)
    fig.update_layout(
        template="plotly_white",
        margin={"l": 10, "r": 10, "t": 40, "b": 10},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        title="Valuation stack",
    )
    return fig


def load_key(provider: str) -> str:
    return os.getenv(ENV_KEY_NAMES[provider], "")


def sidebar_controls() -> tuple[list[str], str, dict[str, Any], dict[str, str], list[str], RuntimeOptions]:
    st.sidebar.markdown("## Research Control Room")
    symbol_text = st.sidebar.text_input(
        "Symbols",
        value=st.session_state.get("symbols_text", "AAPL,MSFT,NVDA"),
        help="Separate symbols with commas. Examples: AAPL, MSFT, NVDA, 2222.SR",
    )
    st.session_state["symbols_text"] = symbol_text
    symbols = parse_symbols(symbol_text)
    primary_symbol = symbols[0] if symbols else ""
    if symbols:
        primary_symbol = st.sidebar.selectbox("Primary focus", symbols, index=0)

    if "refresh_token" not in st.session_state:
        st.session_state["refresh_token"] = 0
    col_a, col_b = st.sidebar.columns(2)
    refresh_clicked = col_a.button("Refresh", width="stretch")
    clear_clicked = col_b.button("Clear Cache", width="stretch")
    if clear_clicked:
        st.cache_data.clear()
        st.session_state["refresh_token"] += 1
    if refresh_clicked:
        st.session_state["refresh_token"] += 1

    with st.sidebar.expander("DCF Assumptions", expanded=True):
        years = st.slider("Forecast years", min_value=3, max_value=10, value=5, step=1)
        growth = st.slider("Growth rate", min_value=-0.04, max_value=0.20, value=0.08, step=0.005, format="%.3f")
        discount = st.slider("Discount rate", min_value=0.06, max_value=0.20, value=0.10, step=0.005, format="%.3f")
        terminal = st.slider("Terminal growth", min_value=0.00, max_value=0.05, value=0.03, step=0.0025, format="%.4f")
        mos = st.slider("Margin of safety", min_value=0.00, max_value=0.35, value=0.15, step=0.01, format="%.2f")

    with st.sidebar.expander("Risk & Benchmark", expanded=True):
        benchmark_symbol = st.selectbox("Benchmark", BENCHMARK_OPTIONS, index=0)
        account_size = st.number_input("Account size", min_value=1000.0, max_value=10_000_000.0, value=25_000.0, step=1000.0)
        risk_per_trade = st.slider("Risk per trade", min_value=0.0025, max_value=0.05, value=0.01, step=0.0025, format="%.4f")

    provider_options = ["fmp", "finnhub", "twelve_data", "polygon", "tiingo", "intrinio", "yahoo", "alpha_vantage"]
    default_providers = ["fmp", "finnhub", "twelve_data", "polygon", "tiingo", "yahoo"]
    enabled_providers = st.sidebar.multiselect(
        "Enabled providers",
        provider_options,
        default=default_providers,
        format_func=provider_label,
        help="Alpha Vantage is intentionally optional because the free tier is aggressively rate-limited.",
    )
    if len(symbols) > 1 and "alpha_vantage" in enabled_providers:
        st.sidebar.warning("Alpha Vantage is slow and rate-limited on free plans. It will be skipped automatically in multi-symbol mode.")

    with st.sidebar.expander("Runtime Modes", expanded=False):
        safe_mode = st.checkbox("Safe mode", value=False, help="Reduce concurrency and favor sequential provider access.")
        light_mode = st.checkbox("Light mode", value=False, help="Fetch market data first and skip heavy financial statement endpoints where possible.")
        debug_mode = st.checkbox("Debug mode", value=False, help="Expose richer diagnostics and raw provider state.")

    with st.sidebar.expander("API Keys", expanded=False):
        fmp = st.text_input("FMP", value=load_key("fmp"), type="password")
        finnhub = st.text_input("Finnhub", value=load_key("finnhub"), type="password")
        twelve = st.text_input("Twelve Data", value=load_key("twelve_data"), type="password")
        alpha = st.text_input("Alpha Vantage", value=load_key("alpha_vantage"), type="password")
        polygon = st.text_input("Polygon / Massive", value=load_key("polygon"), type="password")
        tiingo_key = st.text_input("Tiingo", value=load_key("tiingo"), type="password")
        intrinio_key = st.text_input(
            "Intrinio API Key",
            value=os.getenv("INTRINIO_API_KEY", INTRINIO_DEFAULT_API_KEY),
            type="password",
            help="intrinio.com — requires an active subscription for most company, realtime price, and historical price endpoints.",
        )
        st.caption("Yahoo Finance works without a key via the Yahoo stack. Keep heavily rate-limited providers optional in wide comparison mode.")
        st.divider()
        st.caption("📰 News APIs (optional — news-only sources)")
        marketaux_key = st.text_input(
            "Marketaux API Key",
            value=os.getenv("MARKETAUX_API_KEY", "kDVhuKDZly0yHX3RmyS7k8X8KnhoVpmvOsybGcIQ"),
            type="password",
            help="marketaux.com — entity-level sentiment",
        )
        benzinga_key = st.text_input(
            "Benzinga API Key",
            value=os.getenv("BENZINGA_API_KEY", "bz.V3TML3YXQKOPEPPEORV7NC52RN5RA7CS"),
            type="password",
            help="benzinga.com — financial news",
        )
        sec_api_key = st.text_input(
            "SEC-API.io Key",
            value=os.getenv("SEC_API_KEY", "567e98537638b01f9555ed6a5a2f1ab053d2b2262b472ecfa378879ba1b5035d"),
            type="password",
            help="sec-api.io — SEC filings 8-K/10-K/10-Q",
        )
        newsapi_ai_key = st.text_input(
            "NewsAPI.ai Key",
            value=os.getenv("NEWSAPI_AI_KEY", "7396136f-ff03-40b0-968e-d29c813c6b23"),
            type="password",
            help="newsapi.ai (EventRegistry)",
        )
        st.divider()
        st.caption("🇸🇦 Saudi market & broker (optional)")
        sahmk_key = st.text_input(
            "Sahm.sa API Key",
            value=os.getenv("SAHMK_API_KEY", SAHMK_DEFAULT_API_KEY),
            type="password",
            help="sahmk.sa — Saudi stocks (TASI / NOMU). Used by the Sahm.sa section in the IBKR tab.",
        )
        sahmk_base_url = st.text_input(
            "Sahm.sa Base URL",
            value=os.getenv("SAHMK_BASE_URL", SAHMK_DEFAULT_BASE_URL),
            help="Override only if the vendor changes the API host.",
        )
        st.caption("Interactive Brokers — TWS / Gateway (ib_insync transport)")
        ib_host = st.text_input(
            "IB host",
            value=os.getenv("IB_HOST", IB_INSYNC_DEFAULT_HOST),
            help="127.0.0.1 for the local Gateway/TWS",
        )
        ib_port = st.number_input(
            "IB port",
            min_value=1, max_value=65535,
            value=int(os.getenv("IB_PORT", IB_INSYNC_DEFAULT_PAPER_PORT)),
            help="4002 = Gateway paper · 4001 = Gateway live · 7497 = TWS paper · 7496 = TWS live",
        )
        ib_client_id = st.number_input(
            "IB clientId",
            min_value=1, max_value=999,
            value=int(os.getenv("IB_CLIENT_ID", IB_INSYNC_DEFAULT_CLIENT_ID)),
        )

    assumptions = {
        "years": float(years),
        "growth": float(growth),
        "discount": float(discount),
        "terminal": float(terminal),
        "mos": float(mos),
        "benchmark_symbol": benchmark_symbol,
        "account_size": float(account_size),
        "risk_per_trade": float(risk_per_trade),
    }
    keys = {
        "fmp":        fmp.strip(),
        "finnhub":    finnhub.strip(),
        "twelve_data":twelve.strip(),
        "alpha_vantage": alpha.strip(),
        "polygon":    polygon.strip(),
        "tiingo":     tiingo_key.strip(),
        "marketaux":  marketaux_key.strip(),
        "benzinga":   benzinga_key.strip(),
        "sec_api":    sec_api_key.strip(),
        "newsapi_ai": newsapi_ai_key.strip(),
        "intrinio":   intrinio_key.strip(),
        "sahmk":      sahmk_key.strip(),
        "sahmk_base_url": sahmk_base_url.strip(),
        "ib_host":    ib_host.strip(),
        "ib_port":    int(ib_port),
        "ib_client_id": int(ib_client_id),
    }
    options = RuntimeOptions(safe_mode=safe_mode, light_mode=light_mode, debug_mode=debug_mode)
    return symbols, primary_symbol, assumptions, keys, enabled_providers, options


def binance_sidebar_controls() -> BinanceControls:
    st.sidebar.markdown("## Binance Control Room")
    symbol_text = st.sidebar.text_input(
        "Crypto symbols",
        value=st.session_state.get("binance_symbols_text", ",".join(BINANCE_DEFAULT_SYMBOLS)),
        help="Separate symbols with commas. Examples: BTCUSDT, ETHUSDT, SOLUSDT",
    )
    st.session_state["binance_symbols_text"] = symbol_text
    symbols = parse_binance_symbols(symbol_text) or BINANCE_DEFAULT_SYMBOLS
    primary_symbol = st.sidebar.selectbox("Primary crypto focus", symbols, index=0)

    with st.sidebar.expander("Market Data", expanded=True):
        interval = st.selectbox("Kline interval", BINANCE_INTERVALS, index=BINANCE_INTERVALS.index("1m"), key="binance_interval")
        depth_limit = st.selectbox("REST depth snapshot limit", BINANCE_DEPTH_LIMITS, index=1, key="binance_depth_limit")
        book_levels = st.selectbox("Visible book levels", BINANCE_BOOK_LEVELS, index=2, key="binance_book_levels")
        speed_ms = st.selectbox("WebSocket depth speed", BINANCE_SPEED_OPTIONS, index=0, format_func=lambda value: f"{value} ms", key="binance_speed_ms")
        refresh_seconds = st.slider("REST refresh bucket", min_value=1, max_value=10, value=1, step=1, key="binance_refresh_seconds")

    with st.sidebar.expander("WebSocket Options", expanded=False):
        microseconds = st.checkbox("Use microsecond timeUnit", value=False, key="binance_microseconds")
        market_data_only = st.checkbox("Use market-data-only endpoint reference", value=False, key="binance_market_data_only")
        enable_ws_probe = st.checkbox(
            "Run short WebSocket probe",
            value=False,
            help="Collects three live messages with a one-second socket timeout. Keep off if your network blocks WebSockets.",
            key="binance_enable_ws_probe",
        )

    st.sidebar.caption("Binance public REST is cached by refresh bucket. WebSocket URLs are generated from the official stream naming model.")
    return BinanceControls(
        symbols=symbols,
        primary_symbol=primary_symbol,
        interval=interval,
        depth_limit=int(depth_limit),
        book_levels=int(book_levels),
        speed_ms=int(speed_ms),
        refresh_seconds=int(refresh_seconds),
        microseconds=bool(microseconds),
        market_data_only=bool(market_data_only),
        enable_ws_probe=bool(enable_ws_probe),
    )


@st.cache_data(ttl=TTL_SECONDS, show_spinner=False)
def fetch_benchmark_history(symbol: str, refresh_token: int) -> list[dict[str, Any]]:
    del refresh_token
    if yf is None or not symbol:
        return []
    try:
        throttle_provider("yahoo", extra_delay=0.2)
        history_df = yf.Ticker(symbol).history(period="2y", interval="1d", auto_adjust=False)
    except Exception:
        return []
    rows: list[dict[str, Any]] = []
    if history_df.empty:
        return rows
    for _, row in history_df.reset_index().iterrows():
        close_value = to_num(row.get("Close"))
        date_value = row.get("Date")
        if not is_num(close_value) or pd.isna(date_value):
            continue
        item = {"date": pd.Timestamp(date_value).date().isoformat(), "close": close_value}
        high_value = to_num(row.get("High"))
        low_value = to_num(row.get("Low"))
        volume_value = to_num(row.get("Volume"))
        if is_num(high_value):
            item["high"] = high_value
        if is_num(low_value):
            item["low"] = low_value
        if is_num(volume_value):
            item["volume"] = volume_value
        rows.append(item)
    return rows


def apply_cross_sectional_intelligence(bundles: dict[str, dict[str, Any]], benchmark_history: list[dict[str, Any]], benchmark_symbol: str) -> None:
    benchmark_stats = calculate_history_insights(benchmark_history) if benchmark_history else {}
    benchmark_6m = benchmark_stats.get("momentum_6m")
    benchmark_1y = benchmark_stats.get("momentum_1y")
    has_benchmark = is_num(benchmark_6m) or is_num(benchmark_1y)

    for bundle in bundles.values():
        metrics = bundle["metrics"]
        sources = bundle["sources"]
        if is_num(metrics.get("momentum_6m")) and is_num(benchmark_6m):
            metrics["relative_strength_6m"] = metrics["momentum_6m"] - benchmark_6m
            sources["relative_strength_6m"] = f"calculated vs {benchmark_symbol}"
        if is_num(metrics.get("momentum_1y")) and is_num(benchmark_1y):
            metrics["relative_strength_1y"] = metrics["momentum_1y"] - benchmark_1y
            sources["relative_strength_1y"] = f"calculated vs {benchmark_symbol}"

    if len(bundles) < 2 and not has_benchmark:
        for bundle in bundles.values():
            update_composite_scores(bundle["metrics"], bundle["sources"])
        return

    ranked: list[tuple[float, str]] = []
    for symbol, bundle in bundles.items():
        metrics = bundle["metrics"]
        rank_value = coalesce_num(metrics.get("relative_strength_6m"), metrics.get("momentum_6m"), metrics.get("final_ai_score"))
        if is_num(rank_value):
            ranked.append((float(rank_value), symbol))
    ranked.sort(reverse=True)
    total = len(ranked)
    for index, (_, symbol) in enumerate(ranked, start=1):
        metrics = bundles[symbol]["metrics"]
        sources = bundles[symbol]["sources"]
        percentile = None if total == 1 else (total - index) / (total - 1) * 100
        benchmark_component = clamp(50 + (metrics.get("relative_strength_6m") or 0) * 220, 0, 100) if is_num(metrics.get("relative_strength_6m")) else None
        metrics["relative_strength_rank"] = index
        metrics["relative_strength_score"] = weighted_average_percent(
            [
                (percentile, 0.45),
                (benchmark_component, 0.55),
            ]
        )
        sources["relative_strength_rank"] = "calculated"
        sources["relative_strength_score"] = "calculated"
        update_composite_scores(metrics, sources)


def load_dashboard(
    symbols: list[str],
    assumptions: dict[str, Any],
    keys: dict[str, str],
    enabled_providers: list[str],
    options: RuntimeOptions,
) -> dict[str, dict[str, Any]]:
    bundles: dict[str, dict[str, Any]] = {}
    effective_providers = enabled_providers[:]
    effective_options = RuntimeOptions(
        safe_mode=options.safe_mode,
        light_mode=options.light_mode or len(symbols) > 2,
        debug_mode=options.debug_mode,
    )
    if len(symbols) > 1 and "alpha_vantage" in effective_providers:
        effective_providers = [name for name in effective_providers if name != "alpha_vantage"]
    if len(symbols) > 2 and "yahoo" in effective_providers and not options.safe_mode:
        effective_providers = [name for name in effective_providers if name != "yahoo"] + ["yahoo"]
    refresh_token = int(st.session_state.get("refresh_token", 0))
    for symbol in symbols:
        sources = fetch_all_sources(symbol, keys, refresh_token, effective_providers, effective_options)
        bundles[symbol] = merge_provider_data(symbol, sources, assumptions)
    benchmark_symbol = str(assumptions.get("benchmark_symbol") or "").strip()
    benchmark_history = fetch_benchmark_history(benchmark_symbol, refresh_token) if benchmark_symbol else []
    apply_cross_sectional_intelligence(bundles, benchmark_history, benchmark_symbol)
    return bundles


def render_equity_decision_dashboard(bundle: dict, bundles: dict) -> None:
    """Bloomberg-style 9-category × ~28-section equity research dashboard.

    Reads from the existing `bundle` (already aggregated from FMP / Finnhub /
    Twelve / Alpha / Polygon / Yahoo / Sahm.sa) so no extra API calls.
    Sections without data show a polite placeholder.
    """
    metrics = bundle.get("metrics") or {}
    financials = bundle.get("financials") or {}
    sym = bundle.get("symbol") or "—"
    company = bundle.get("company_name") or sym

    # Prominent symbol badge at the top of the dashboard
    st.markdown(
        f'<div style="display:flex;align-items:center;justify-content:space-between;'
        f'gap:12px;flex-wrap:wrap;margin-bottom:0.8rem;">'
        f'<div class="section-header" style="margin:0;border:none;padding:0;">'
        f'<h3>📊 Equity Decision Engine · Pro Dashboard</h3>'
        f'<span class="section-count">28 sections · multi-source</span></div>'
        f'{symbol_badge(sym, exchange=bundle.get("exchange"), size="lg")}'
        f'</div>'
        f'<div style="color:var(--muted);font-size:0.86rem;margin-bottom:0.6rem;">'
        f'<b>{escape(str(company))}</b> · '
        f'price <b>{escape(fmt_money(metrics.get("price"), bundle.get("currency", "$")))}</b> · '
        f'AI score <b>{escape(str(metrics.get("final_ai_score") or "—"))}</b>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ===== 🟢 1. Core Financials =====
    with st.expander("🟢 Core Financials  (Income · Balance · Cash Flow · Annual · Quarterly)", expanded=True):
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Income Statement**")
            df = _df_safe(financials.get("income_statement") or financials.get("income"))
            if df is not None:
                st.dataframe(df, width="stretch", hide_index=True, height=260)
            else:
                st.caption("Income statement not available in current bundle.")
        with c2:
            st.markdown("**Balance Sheet & Leverage**")
            df = _df_safe(financials.get("balance_sheet") or financials.get("balance"))
            if df is not None:
                st.dataframe(df, width="stretch", hide_index=True, height=260)
            else:
                st.caption("Balance sheet not available.")
        c3, c4 = st.columns(2)
        with c3:
            st.markdown("**Cash Flow**")
            df = _df_safe(financials.get("cash_flow") or financials.get("cashflow"))
            if df is not None:
                st.dataframe(df, width="stretch", hide_index=True, height=260)
            else:
                st.caption("Cash flow not available.")
        with c4:
            st.markdown("**Annual + Quarterly snapshots**")
            ann = _df_safe(financials.get("annual"))
            qtr = _df_safe(financials.get("quarterly"))
            if ann is not None:
                st.markdown("*Annual*")
                st.dataframe(ann, width="stretch", hide_index=True, height=120)
            if qtr is not None:
                st.markdown("*Quarterly*")
                st.dataframe(qtr, width="stretch", hide_index=True, height=120)
            if ann is None and qtr is None:
                st.caption("No annual / quarterly data.")

    # ===== 🟡 2. Quality & Analysis =====
    with st.expander("🟡 Quality & Analysis  (Statement Quality · Ratios · DuPont · Health Scores)"):
        q1, q2 = st.columns(2)
        with q1:
            st.markdown("**Statement Quality (earnings vs cash)**")
            quality_metrics = ["earnings_quality", "accruals_ratio", "cash_conversion"]
            df = metric_frame(bundle, ["Quality"]) if "Quality" in (metrics or {}) else None
            if df is not None and not df.empty:
                st.dataframe(df, width="stretch", hide_index=True, height=240)
            else:
                st.caption("Run a deeper financials provider (FMP) to populate quality scores.")
            st.markdown("**Key Financial Metrics**")
            df = metric_frame(bundle, ["Summary", "Trend", "Momentum", "Volume", "Risk", "Relative Strength"])
            st.dataframe(df, width="stretch", hide_index=True, height=240)
        with q2:
            st.markdown("**DuPont · Financial Health (Piotroski / Altman)**")
            health = {
                "Net margin":        metrics.get("net_margin"),
                "Asset turnover":    metrics.get("asset_turnover"),
                "Equity multiplier": metrics.get("equity_multiplier"),
                "ROE":               metrics.get("roe"),
                "ROA":               metrics.get("roa"),
                "Piotroski F-score": metrics.get("piotroski"),
                "Altman Z-score":    metrics.get("altman_z"),
                "Beneish M-score":   metrics.get("beneish_m"),
            }
            health = {k: v for k, v in health.items() if v is not None}
            if health:
                st.dataframe(pd.DataFrame([health]).T.rename(columns={0: "value"}),
                              width="stretch", height=320)
            else:
                st.caption("Health scores need FMP / Finnhub financials.")

    # ===== 🔵 3. Valuation & Investment =====
    with st.expander("🔵 Valuation & Investment  (Multiples · Dividends · Ownership · Insider)"):
        v1, v2 = st.columns(2)
        with v1:
            st.markdown("**Valuation & Multiples**")
            mult = {
                "P/E":        metrics.get("pe"),
                "P/B":        metrics.get("pb"),
                "P/S":        metrics.get("ps"),
                "EV/EBITDA":  metrics.get("ev_ebitda"),
                "PEG":        metrics.get("peg"),
                "FCF yield":  metrics.get("fcf_yield"),
                "Earnings yield": metrics.get("earnings_yield"),
            }
            mult = {k: v for k, v in mult.items() if v is not None}
            if mult:
                st.dataframe(pd.DataFrame([mult]).T.rename(columns={0: "value"}), width="stretch", height=260)
            else:
                st.caption("Multiples unavailable.")
            st.markdown("**Dividends & Yield**")
            div = {
                "Dividend yield": metrics.get("dividend_yield"),
                "Payout ratio":   metrics.get("payout_ratio"),
                "Last dividend":  metrics.get("last_dividend"),
                "5y dividend CAGR": metrics.get("dividend_cagr_5y"),
            }
            div = {k: v for k, v in div.items() if v is not None}
            if div:
                st.dataframe(pd.DataFrame([div]).T.rename(columns={0: "value"}), width="stretch", height=200)
            else:
                st.caption("No dividend data.")
        with v2:
            st.markdown("**Ownership · Institutional · Insider**")
            own = {
                "Insider ownership":     metrics.get("insider_ownership"),
                "Institutional ownership": metrics.get("institutional_ownership"),
                "Float":                 metrics.get("float_shares"),
                "Short interest":        metrics.get("short_interest"),
                "Days to cover":         metrics.get("days_to_cover"),
            }
            own = {k: v for k, v in own.items() if v is not None}
            if own:
                st.dataframe(pd.DataFrame([own]).T.rename(columns={0: "value"}), width="stretch", height=240)
            else:
                st.caption("Ownership data needs Finnhub or FMP.")
            st.markdown("**Insider Trading (latest 10)**")
            df = _df_safe(bundle.get("insider_trades"))
            if df is not None:
                st.dataframe(df.head(10), width="stretch", hide_index=True, height=200)
            else:
                st.caption("No insider trades in current bundle.")

    # ===== 🟣 4. Advanced Insights =====
    with st.expander("🟣 Advanced Insights  (Segments · Estimates · ESG · Debt)"):
        a1, a2 = st.columns(2)
        with a1:
            st.markdown("**Segment Analysis**")
            df = _df_safe(financials.get("segments"))
            if df is not None:
                st.dataframe(df, width="stretch", hide_index=True, height=200)
            else:
                st.caption("Segment breakdown needs FMP advanced.")
            st.markdown("**Consensus Estimates**")
            df = _df_safe(bundle.get("estimates"))
            if df is not None:
                st.dataframe(df, width="stretch", hide_index=True, height=200)
            else:
                st.caption("Analyst estimates not in bundle.")
        with a2:
            st.markdown("**ESG**")
            esg = bundle.get("esg") or {}
            if esg:
                st.dataframe(pd.DataFrame([esg]).T.rename(columns={0: "value"}), width="stretch", height=200)
            else:
                st.caption("ESG data not in bundle.")
            st.markdown("**Debt & Credit**")
            credit = {
                "Total debt":    metrics.get("total_debt"),
                "Net debt":      metrics.get("net_debt"),
                "Debt/Equity":   metrics.get("debt_equity"),
                "Interest coverage": metrics.get("interest_coverage"),
                "Credit rating": metrics.get("credit_rating"),
            }
            credit = {k: v for k, v in credit.items() if v is not None}
            if credit:
                st.dataframe(pd.DataFrame([credit]).T.rename(columns={0: "value"}), width="stretch", height=200)
            else:
                st.caption("Debt analytics not loaded.")

    # ===== 🟠 5. Market & Trading =====
    with st.expander("🟠 Market & Trading  (Options · Peers · Sentiment · Earnings Calls)"):
        m1, m2 = st.columns(2)
        with m1:
            st.markdown("**Options Chain (preview)**")
            df = _df_safe(bundle.get("options_chain"))
            if df is not None:
                st.dataframe(df.head(20), width="stretch", hide_index=True, height=240)
            else:
                st.caption("Options data not in bundle (use Polygon / Yahoo options).")
            st.markdown("**Peer Comparison**")
            if len(bundles) > 1:
                st.dataframe(comparison_leaderboard_frame(bundles), width="stretch", hide_index=True, height=240)
            else:
                st.caption("Add comparison symbols in the sidebar to enable peer comparison.")
        with m2:
            st.markdown("**Sentiment Analysis**")
            news = bundle.get("news") or []
            sentiments = [n.get("sentiment") for n in news if isinstance(n.get("sentiment"), (int, float))]
            if sentiments:
                avg = sum(sentiments) / len(sentiments)
                pos = sum(1 for s in sentiments if s > 0.1)
                neg = sum(1 for s in sentiments if s < -0.1)
                neu = len(sentiments) - pos - neg
                sm1, sm2, sm3, sm4 = st.columns(4)
                sm1.metric("Avg sentiment", f"{avg:+.2f}")
                sm2.metric("Bullish", pos)
                sm3.metric("Bearish", neg)
                sm4.metric("Neutral", neu)
            else:
                st.caption("No scored news in bundle.")
            st.markdown("**Earnings Call (latest)**")
            ec = bundle.get("earnings_call") or bundle.get("transcript")
            if ec:
                st.text_area("Transcript snippet", value=str(ec)[:1200], height=180, disabled=True,
                              key="dash_ec_snip")
            else:
                st.caption("Earnings call transcript not in bundle.")

    # ===== 🔴 6. Macro =====
    with st.expander("🔴 Macro  (Indicators · Yield curves)"):
        m1, m2 = st.columns(2)
        with m1:
            st.markdown("**Macroeconomic Indicators**")
            macro = bundle.get("macro") or {}
            if macro:
                st.dataframe(pd.DataFrame([macro]).T.rename(columns={0: "value"}),
                              width="stretch", height=240)
            else:
                st.caption("Macro indicators (CPI / GDP / Fed) need a macro provider.")
        with m2:
            st.markdown("**Fixed Income & Yield Curves**")
            curve = bundle.get("yield_curve") or {}
            if curve:
                df = pd.DataFrame(list(curve.items()), columns=["maturity", "yield"])
                try:
                    import plotly.express as _px
                    fig = _px.line(df, x="maturity", y="yield", markers=True, title="US Treasury yield curve")
                    fig.update_layout(margin=dict(l=10, r=10, t=40, b=10), height=260)
                    st.plotly_chart(fig, width="stretch", key="dash_yield_curve")
                except Exception:
                    st.dataframe(df, width="stretch", hide_index=True)
            else:
                st.caption("No yield curve in bundle.")

    # ===== 🟤 7. Strategy & Quant =====
    with st.expander("🟤 Strategy & Quant  (Backtesting · ETF holdings)"):
        s1, s2 = st.columns(2)
        with s1:
            st.markdown("**Backtesting & Strategy Lab**")
            st.caption("⚠️ Backtest engine is a separate module — see Decision Engine ▸ Decision Signals "
                       "for technical signal scores that feed into the strategy lab.")
            df = technical_signal_frame(bundle)
            if df is not None and not df.empty:
                st.dataframe(df.head(12), width="stretch", hide_index=True, height=240)
        with s2:
            st.markdown("**ETF & Funds Holdings**")
            df = _df_safe(bundle.get("etf_holdings"))
            if df is not None:
                st.dataframe(df.head(20), width="stretch", hide_index=True, height=240)
            else:
                st.caption("ETF holdings need a fund-data provider.")

    # ===== ⚫ 8. Corporate Actions =====
    with st.expander("⚫ Corporate Actions  (Splits · Dividends · M&A)"):
        df = _df_safe(bundle.get("corporate_actions"))
        if df is not None:
            st.dataframe(df, width="stretch", hide_index=True, height=240)
        else:
            actions = []
            if metrics.get("last_dividend"):
                actions.append({"event": "Dividend", "value": metrics["last_dividend"]})
            if metrics.get("last_split"):
                actions.append({"event": "Split", "value": metrics["last_split"]})
            if actions:
                st.dataframe(pd.DataFrame(actions), width="stretch", hide_index=True, height=160)
            else:
                st.caption("No corporate actions in bundle.")

    # ===== ⚪ 9. Raw Data Layer =====
    with st.expander("⚪ Provider Raw Slice  (Debug data layer)"):
        providers = bundle.get("providers") or {}
        if providers:
            for prov_name, prov_payload in list(providers.items())[:8]:
                st.markdown(f"**{provider_label(prov_name)}** · status `{prov_payload.get('status', '?')}`")
                with st.expander(f"raw payload — {prov_name}", expanded=False):
                    st.json(prov_payload)
        else:
            st.caption("No provider raw data — re-run with debug mode to capture per-provider responses.")


def _df_safe(value) -> "pd.DataFrame | None":
    """Best-effort coerce any container into a DataFrame, or return None."""
    if value is None:
        return None
    if isinstance(value, pd.DataFrame):
        return value if not value.empty else None
    if isinstance(value, dict):
        try:
            df = pd.DataFrame(value)
            return df if not df.empty else None
        except Exception:
            try:
                return pd.DataFrame([value])
            except Exception:
                return None
    if isinstance(value, list):
        if not value:
            return None
        try:
            return pd.DataFrame(value)
        except Exception:
            return None
    return None


# ============================================================
# Live Multi-Provider Data Fetcher  (Bloomberg-style dashboard)
# ============================================================
# Each method tries multiple providers in parallel, merges results,
# and never raises. Returns {"primary": <best>, "by_provider": {...},
# "coverage": {provider: "ok"|"partial"|"error"}}.
#
# Heavy `@st.cache_data` keeps round-trips minimal (TTL = 10 minutes).

# ============================================================
# Live dashboard timing config
# ------------------------------------------------------------
# Different resources change at different cadences — caching them with
# matching TTLs cuts redundant API calls dramatically.
# ============================================================
LIVE_TTL_QUOTE      = 60       # 1 minute  — prices, sentiment buzz
LIVE_TTL_FUNDAMENT  = 6 * 3600 # 6 hours   — financial statements (re-released quarterly)
LIVE_TTL_OWNERSHIP  = 24 * 3600 # 24 hours — institutional/insider data
LIVE_TTL_MACRO      = 12 * 3600 # 12 hours — CPI/GDP/yield curves
LIVE_TTL_DEFAULT    = 600       # 10 min   — generic
LIVE_DASHBOARD_TTL  = LIVE_TTL_DEFAULT
LIVE_DASHBOARD_TIMEOUT = 8.0

# Shared session — reuse TCP connections across requests (much faster than
# spinning up a fresh socket every call).
_LIVE_HTTP_SESSION: requests.Session | None = None
LIVE_DIAGNOSTICS: list[dict[str, Any]] = []
LIVE_PROVIDER_HEALTH: dict[str, dict[str, Any]] = {}
LIVE_CACHE_STATS: dict[str, int] = {"requests": 0, "success": 0, "failure": 0, "rate_limited": 0}
LIVE_DIAGNOSTICS_MAX = 800


def _record_live_diagnostic(
    provider: str,
    symbol: str,
    ok: bool,
    latency_ms: float,
    error_type: str | None = None,
    status_code: int | None = None,
) -> None:
    """Record request diagnostics and update health score.

    Kept in process memory on purpose: it is fast, avoids external state, and
    gives the Streamlit UI immediate observability without crashing if a vendor
    has a bad day.
    """
    provider = provider or "unknown"
    event = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "provider": provider,
        "symbol": (symbol or "").upper(),
        "success": bool(ok),
        "latency_ms": round(float(latency_ms or 0.0), 2),
        "error_type": error_type or None,
        "status_code": status_code,
    }
    LIVE_DIAGNOSTICS.append(event)
    if len(LIVE_DIAGNOSTICS) > LIVE_DIAGNOSTICS_MAX:
        del LIVE_DIAGNOSTICS[: len(LIVE_DIAGNOSTICS) - LIVE_DIAGNOSTICS_MAX]

    state = LIVE_PROVIDER_HEALTH.setdefault(
        provider,
        {"requests": 0, "success": 0, "failure": 0, "rate_limited": 0, "latency_ms": 0.0, "health": 1.0},
    )
    state["requests"] += 1
    if ok:
        state["success"] += 1
    else:
        state["failure"] += 1
    if status_code == 429 or str(error_type or "").lower() in {"rate_limit", "429"}:
        state["rate_limited"] += 1
    prev_latency = float(state.get("latency_ms") or 0.0)
    state["latency_ms"] = round((prev_latency * 0.75) + (float(latency_ms or 0.0) * 0.25), 2)
    total = max(int(state["requests"]), 1)
    success_rate = float(state["success"]) / total
    latency_penalty = min(float(state["latency_ms"]) / 4000.0, 0.35)
    rate_penalty = min(float(state["rate_limited"]) / total, 0.25)
    state["health"] = round(max(0.0, min(1.0, success_rate - latency_penalty - rate_penalty)), 3)


def get_live_diagnostics_frame() -> pd.DataFrame:
    return pd.DataFrame(LIVE_DIAGNOSTICS[-200:])


def get_live_provider_health_frame() -> pd.DataFrame:
    rows = []
    for provider, state in LIVE_PROVIDER_HEALTH.items():
        rows.append({
            "Provider": provider_label(provider),
            "Health": state.get("health", 0.0),
            "Requests": state.get("requests", 0),
            "Success": state.get("success", 0),
            "Failures": state.get("failure", 0),
            "429s": state.get("rate_limited", 0),
            "Latency ms": state.get("latency_ms", 0.0),
        })
    if not rows:
        return pd.DataFrame(columns=["Provider", "Health", "Requests", "Success", "Failures", "429s", "Latency ms"])
    return pd.DataFrame(rows).sort_values(["Health", "Latency ms"], ascending=[False, True])


def _live_session() -> requests.Session:
    """Return a process-wide pooled HTTP session (10× faster than per-call sockets)."""
    global _LIVE_HTTP_SESSION
    if _LIVE_HTTP_SESSION is None:
        s = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=20,
            pool_maxsize=40,
            max_retries=Retry(total=1, connect=1, read=0,
                              backoff_factor=0.3,
                              status_forcelist=[502, 503, 504]),
        )
        s.mount("http://", adapter)
        s.mount("https://", adapter)
        s.headers.update({"User-Agent": "Atlas-Terminal/1.0"})
        _LIVE_HTTP_SESSION = s
    return _LIVE_HTTP_SESSION


def _live_safe_request(method: str, url: str, provider: str = "unknown", symbol: str = "", **kwargs) -> dict:
    """Safe HTTP wrapper with exponential backoff, jitter, diagnostics, and health.

    It never raises into Streamlit. 429/5xx/network timeouts are retried with
    small exponential delays, while diagnostics are recorded for the provider
    health monitor.
    """
    timeout = kwargs.pop("timeout", LIVE_DASHBOARD_TIMEOUT)
    max_attempts = int(kwargs.pop("max_attempts", 3))
    params = kwargs.get("params") or {}
    symbol_hint = symbol or str(params.get("symbol") or params.get("ticker") or params.get("tickers") or "")
    LIVE_CACHE_STATS["requests"] += 1
    last_error = ""
    last_status = 0
    start_all = time.perf_counter()
    for attempt in range(max(1, max_attempts)):
        start = time.perf_counter()
        try:
            r = _live_session().request(method, url, timeout=timeout, **kwargs)
            latency_ms = (time.perf_counter() - start) * 1000.0
            last_status = int(r.status_code)
            try:
                payload = r.json()
            except Exception:
                payload = {"raw": r.text[:1500]}
            if r.status_code in {429, 500, 502, 503, 504} and attempt < max_attempts - 1:
                last_error = f"HTTP {r.status_code}"
                _record_live_diagnostic(provider, symbol_hint, False, latency_ms, last_error, r.status_code)
                time.sleep(min(2.0, 0.35 * (2 ** attempt)) + random.uniform(0.0, 0.2))
                continue
            ok = bool(r.ok)
            if ok:
                LIVE_CACHE_STATS["success"] += 1
            else:
                LIVE_CACHE_STATS["failure"] += 1
                if r.status_code == 429:
                    LIVE_CACHE_STATS["rate_limited"] += 1
            _record_live_diagnostic(provider, symbol_hint, ok, latency_ms, None if ok else f"HTTP {r.status_code}", r.status_code)
            return {"_ok": ok, "_status": r.status_code, "data": payload, "_latency_ms": round(latency_ms, 2)}
        except requests.exceptions.RequestException as exc:
            latency_ms = (time.perf_counter() - start) * 1000.0
            last_error = type(exc).__name__
            _record_live_diagnostic(provider, symbol_hint, False, latency_ms, last_error, 0)
            if attempt < max_attempts - 1:
                time.sleep(min(2.0, 0.35 * (2 ** attempt)) + random.uniform(0.0, 0.2))
                continue
            LIVE_CACHE_STATS["failure"] += 1
            return {
                "_ok": False,
                "_status": 0,
                "_error": last_error,
                "_error_detail": str(exc)[:240],
                "_latency_ms": round((time.perf_counter() - start_all) * 1000.0, 2),
                "data": None,
            }
    LIVE_CACHE_STATS["failure"] += 1
    return {"_ok": False, "_status": last_status, "_error": last_error or "request_failed", "data": None}


def _detect_provider_anomaly(provider: str, payload: dict) -> dict:
    """Each vendor has its own way of returning HTTP-200-but-actually-an-error.

    This helper inspects the JSON body and reclassifies known patterns so the
    dashboard's `coverage` badges show 🔴 for rate-limit / premium / bad-symbol
    responses instead of the misleading 🟢.
    """
    if not payload.get("_ok"):
        return payload
    data = payload.get("data")
    if not isinstance(data, dict):
        return payload  # arrays / strings cannot match the patterns below

    # ---- Alpha Vantage ----
    if provider == "alpha":
        if "Error Message" in data:
            return {**payload, "_ok": False, "_error": "av_bad_request",
                    "_error_detail": str(data["Error Message"])[:240]}
        if "Note" in data:  # rate-limited
            return {**payload, "_ok": False, "_error": "av_rate_limit",
                    "_error_detail": str(data["Note"])[:240]}
        if "Information" in data and len(data) <= 2:
            # Premium-only or invalid key — AV puts the explanation here
            return {**payload, "_ok": False, "_error": "av_premium_or_invalid",
                    "_error_detail": str(data["Information"])[:240]}

    # ---- Twelve Data ----
    elif provider == "twelve":
        # Twelve Data returns {"code": 401, "status": "error", "message": "..."}
        if data.get("status") == "error" or (data.get("code") and data["code"] >= 400):
            return {**payload, "_ok": False, "_error": "twelve_error",
                    "_error_detail": str(data.get("message", ""))[:240]}

    # ---- Finnhub ----
    elif provider == "finnhub":
        # Finnhub typically signals via HTTP 401/429; if HTTP-200 but body has
        # `error` field, surface it.
        if isinstance(data, dict) and data.get("error"):
            return {**payload, "_ok": False, "_error": "finnhub_error",
                    "_error_detail": str(data["error"])[:240]}

    # ---- Polygon ----
    elif provider == "polygon":
        # Polygon: {"status":"ERROR","error":"...","results":null}
        if data.get("status") in {"ERROR", "NOT_AUTHORIZED"}:
            return {**payload, "_ok": False, "_error": "polygon_error",
                    "_error_detail": str(data.get("error") or data.get("message", ""))[:240]}

    # ---- FMP ----
    elif provider == "fmp":
        # FMP returns {"Error Message": "..."} for bad symbol/key
        if data.get("Error Message"):
            return {**payload, "_ok": False, "_error": "fmp_error",
                    "_error_detail": str(data["Error Message"])[:240]}

    # ---- Intrinio ----
    elif provider == "intrinio":
        # Intrinio: {"error": "...", "message": "..."} OR HTTP 401 / 402 / 429
        if data.get("error") or data.get("error_message") or data.get("human") or data.get("message"):
            return {**payload, "_ok": False, "_error": "intrinio_error",
                    "_error_detail": str(data.get("human") or data.get("message") or data.get("error") or "")[:240]}

    # ---- Tiingo ----
    elif provider == "tiingo":
        if data.get("detail") or data.get("message") or data.get("error"):
            return {**payload, "_ok": False, "_error": "tiingo_error",
                    "_error_detail": str(data.get("detail") or data.get("message") or data.get("error") or "")[:240]}

    return payload


# Intrinio requires an active subscription for most endpoints. Do not ship a
# bundled fallback key: a stale/no-subscription key makes the app look broken
# by returning 401s instead of a clear "Missing API key" diagnostic.
INTRINIO_DEFAULT_API_KEY = ""
INTRINIO_BASE_URL = "https://api-v2.intrinio.com"


# ─── Provider-specific cached helpers ────────────────────────────────────────
@st.cache_data(ttl=LIVE_DASHBOARD_TTL, show_spinner=False)
def _live_fmp(path: str, api_key: str, **params) -> dict:
    if not api_key:
        return {"_ok": False, "_error": "no_key"}
    url = f"https://financialmodelingprep.com/api/v3/{path.lstrip('/')}"
    res = _live_safe_request("GET", url, provider="fmp", symbol=str(params.get("symbol", "")), params={**params, "apikey": api_key})
    return _detect_provider_anomaly("fmp", res)


@st.cache_data(ttl=LIVE_DASHBOARD_TTL, show_spinner=False)
def _live_finnhub(path: str, api_key: str, **params) -> dict:
    if not api_key:
        return {"_ok": False, "_error": "no_key"}
    url = f"https://finnhub.io/api/v1/{path.lstrip('/')}"
    res = _live_safe_request("GET", url, provider="finnhub", symbol=str(params.get("symbol", "")), params={**params, "token": api_key})
    return _detect_provider_anomaly("finnhub", res)


@st.cache_data(ttl=LIVE_DASHBOARD_TTL, show_spinner=False)
def _live_twelve(path: str, api_key: str, **params) -> dict:
    if not api_key:
        return {"_ok": False, "_error": "no_key"}
    url = f"https://api.twelvedata.com/{path.lstrip('/')}"
    res = _live_safe_request("GET", url, provider="twelve_data", symbol=str(params.get("symbol", "")), params={**params, "apikey": api_key})
    return _detect_provider_anomaly("twelve", res)


@st.cache_data(ttl=LIVE_DASHBOARD_TTL, show_spinner=False)
def _live_alpha(function: str, api_key: str, **params) -> dict:
    """Alpha Vantage — call /query with `function`. Detects rate-limit / premium
    responses (AV returns HTTP 200 with a `Note` / `Information` field).

    Validated against the official endpoint catalog (alphavantage.co/documentation):
      Fundamental:  INCOME_STATEMENT · BALANCE_SHEET · CASH_FLOW · EARNINGS · OVERVIEW
      News & Sent.: NEWS_SENTIMENT  (premium / pay-as-you-go on most plans)
      Transcripts:  EARNINGS_CALL_TRANSCRIPT  (premium)
      Macro:        CPI · FEDERAL_FUNDS_RATE · TREASURY_YIELD · GDP · UNEMPLOYMENT
    """
    if not api_key:
        return {"_ok": False, "_error": "no_key"}
    res = _live_safe_request(
        "GET", "https://www.alphavantage.co/query", provider="alpha_vantage", symbol=str(params.get("symbol", "")),
        params={"function": function, "apikey": api_key, **params},
    )
    return _detect_provider_anomaly("alpha", res)


@st.cache_data(ttl=LIVE_DASHBOARD_TTL, show_spinner=False)
def _live_polygon(path: str, api_key: str, **params) -> dict:
    if not api_key:
        return {"_ok": False, "_error": "no_key"}
    url = f"https://api.polygon.io/{path.lstrip('/')}"
    res = _live_safe_request("GET", url, provider="polygon", symbol=str(params.get("ticker", params.get("tickers", ""))), params={**params, "apiKey": api_key})
    return _detect_provider_anomaly("polygon", res)


@st.cache_data(ttl=LIVE_DASHBOARD_TTL, show_spinner=False)
def _live_intrinio(path: str, api_key: str, **params) -> dict:
    """Intrinio API v2 — uses Basic Auth via api_key as the username.

    Intrinio's keys are pre-encoded base64 strings, so we pass them directly in
    the `api_key` query param (their canonical method) rather than via Basic
    Auth headers. The free tier returns 402 for premium endpoints.

    Endpoint families used by the Data Engine:
      /companies/{ticker}                       (overview)
      /companies/{ticker}/fundamentals/standardized  (financial statements)
      /securities/{ticker}/prices/realtime      (quote)
      /securities/{ticker}/news                 (news feed)
      /companies/{ticker}/dividends             (dividend history)
      /companies/{ticker}/insider_transactions  (insider activity)
    """
    if not api_key:
        return {"_ok": False, "_error": "no_key"}
    url = f"{INTRINIO_BASE_URL}/{path.lstrip('/')}"
    res = _live_safe_request("GET", url, provider="intrinio", symbol=path.split("/")[1] if "/" in path else "", params={**params, "api_key": api_key})
    return _detect_provider_anomaly("intrinio", res)


TIINGO_BASE_URL = "https://api.tiingo.com"


@st.cache_data(ttl=LIVE_DASHBOARD_TTL, show_spinner=False)
def _live_tiingo(path: str, api_key: str, **params) -> dict:
    """Tiingo REST wrapper used as a resilient price/history provider."""
    if not api_key:
        return {"_ok": False, "_error": "no_key"}
    url = f"{TIINGO_BASE_URL}/{path.lstrip('/')}"
    headers = {"Authorization": f"Token {api_key}"}
    res = _live_safe_request("GET", url, provider="tiingo", symbol=str(params.get("tickers", params.get("symbol", ""))), headers=headers, params=params)
    return _detect_provider_anomaly("tiingo", res)


# ─── yfinance — heaviest provider ────────────────────────────────────────────
# Cache the Ticker handle itself; recreating it parses Yahoo's HTML each time.
@st.cache_resource(show_spinner=False)
def _yahoo_ticker(symbol: str):
    try:
        import yfinance as yf
        return yf.Ticker(symbol)
    except Exception:
        return None


@st.cache_data(ttl=LIVE_DASHBOARD_TTL, show_spinner=False)
def _live_yahoo(symbol: str, what: str = "info") -> dict:
    """Yahoo via yfinance — no key needed.

    Each `what` accessor is a property that hits Yahoo's HTML/JSON internals,
    so we wrap the whole call in a generic try/except.
    """
    t = _yahoo_ticker(symbol)
    if t is None:
        return {"_ok": False, "_error": "yfinance_unavailable",
                "_error_detail": "Install yfinance: pip install yfinance"}
    try:
        if what == "info":
            return {"_ok": True, "data": dict(getattr(t, "fast_info", {}) or {})}
        if what == "history":
            df = t.history(period="1y")
            return {"_ok": True, "data": df.tail(40).to_dict(orient="records") if df is not None and not df.empty else []}
        if what == "income":
            df = t.income_stmt
            return {"_ok": True, "data": df.to_dict() if df is not None and not df.empty else {}}
        if what == "balance":
            df = t.balance_sheet
            return {"_ok": True, "data": df.to_dict() if df is not None and not df.empty else {}}
        if what == "cashflow":
            df = t.cashflow
            return {"_ok": True, "data": df.to_dict() if df is not None and not df.empty else {}}
        if what == "dividends":
            ser = t.dividends
            return {"_ok": True, "data": ser.tail(20).to_dict() if ser is not None and not ser.empty else {}}
        if what == "splits":
            ser = t.splits
            return {"_ok": True, "data": ser.tail(20).to_dict() if ser is not None and not ser.empty else {}}
        if what == "options":
            return {"_ok": True, "data": list(t.options[:8]) if t.options else []}
        if what == "institutional":
            df = t.institutional_holders
            return {"_ok": True, "data": df.head(15).to_dict(orient="records") if df is not None and not df.empty else []}
        if what == "insider":
            df = t.insider_transactions
            return {"_ok": True, "data": df.head(20).to_dict(orient="records") if df is not None and not df.empty else []}
        if what == "recommendations":
            df = t.recommendations
            return {"_ok": True, "data": df.tail(10).to_dict(orient="records") if df is not None and not df.empty else []}
        if what == "calendar":
            cal = t.calendar
            return {"_ok": True, "data": cal.to_dict() if hasattr(cal, "to_dict") else (dict(cal) if cal else {})}
        if what == "esg":
            sus = t.sustainability
            return {"_ok": True, "data": sus.to_dict() if sus is not None and not sus.empty else {}}
        return {"_ok": False, "_error": "unknown_resource"}
    except Exception as exc:
        return {"_ok": False, "_error": type(exc).__name__, "_error_detail": str(exc)[:160]}


class MultiProviderFetcher:
    """Cooperative fetcher across all configured equity APIs."""

    def __init__(self, keys: dict):
        self.keys = keys or {}
        self.fmp = (keys or {}).get("fmp", "")
        self.finnhub = (keys or {}).get("finnhub", "")
        self.twelve = (keys or {}).get("twelve_data", "")
        self.alpha = (keys or {}).get("alpha_vantage", "")
        self.polygon = (keys or {}).get("polygon", "")
        self.tiingo = (keys or {}).get("tiingo", "")
        self.sahmk = (keys or {}).get("sahmk", "")
        self.sahmk_base = (keys or {}).get("sahmk_base_url", SAHMK_DEFAULT_BASE_URL)
        self.intrinio = (keys or {}).get("intrinio", "")

    # ---------- helpers ----------
    @staticmethod
    def _coverage_dict(by_provider: dict) -> dict:
        cov = {}
        for prov, payload in by_provider.items():
            if not isinstance(payload, dict):
                cov[prov] = "error"
            elif payload.get("_ok") and payload.get("data"):
                cov[prov] = "ok"
            elif payload.get("_ok"):
                cov[prov] = "partial"
            elif payload.get("_error") == "no_key":
                cov[prov] = "no_key"
            else:
                cov[prov] = "error"
        return cov

    def _bundle_response(self, by_provider: dict, primary: Any = None) -> dict:
        coverage = self._coverage_dict(by_provider)
        if primary is None:
            for prov, payload in by_provider.items():
                if isinstance(payload, dict) and payload.get("_ok") and payload.get("data"):
                    primary = payload.get("data")
                    break
        return {"primary": primary, "by_provider": by_provider, "coverage": coverage}

    # ---------- parallel runner ----------
    def _run_parallel(self, tasks: dict) -> dict:
        """Execute provider tasks concurrently.

        `tasks` is a dict of provider_name → (callable, has_key_flag).
        Tasks with `has_key_flag=False` are short-circuited (no thread, no
        request) and immediately marked as `no_key` — saves both a thread spin
        and a network round-trip on the cache-miss path.

        Concurrent fetching turns "wait for the slowest of N providers" into
        the slowest single response time (vs sequential N × response_time).
        """
        out: dict[str, Any] = {}
        live_tasks: dict[str, Any] = {}
        for name, (fn, has_key) in tasks.items():
            if not has_key:
                out[name] = {"_ok": False, "_error": "no_key"}
            else:
                live_tasks[name] = fn

        if live_tasks:
            n = min(len(live_tasks), 8)
            with ThreadPoolExecutor(max_workers=n) as ex:
                future_map = {ex.submit(fn): name for name, fn in live_tasks.items()}
                for fut in as_completed(future_map):
                    name = future_map[fut]
                    try:
                        out[name] = fut.result(timeout=LIVE_DASHBOARD_TIMEOUT * 2)
                    except Exception as exc:
                        out[name] = {"_ok": False, "_error": "exec_error",
                                      "_error_detail": str(exc)[:200]}
        return out

    # ---------- 1. Income Statement ----------
    def price_snapshot(self, symbol: str) -> dict:
        """Fast price snapshot across quote-oriented providers."""
        start = str(date.today() - timedelta(days=10))
        tasks = {
            "fmp":      (lambda: _live_fmp(f"quote/{symbol}", self.fmp), bool(self.fmp)),
            "finnhub":  (lambda: _live_finnhub("quote", self.finnhub, symbol=symbol), bool(self.finnhub)),
            "twelve":   (lambda: _live_twelve("quote", self.twelve, symbol=symbol), bool(self.twelve)),
            "polygon":  (lambda: _live_polygon(f"v2/aggs/ticker/{symbol}/prev", self.polygon, adjusted="true"), bool(self.polygon)),
            "tiingo":   (lambda: _live_tiingo(f"tiingo/daily/{symbol.lower()}/prices", self.tiingo, symbol=symbol, startDate=start, resampleFreq="daily"), bool(self.tiingo)),
            "intrinio": (lambda: _live_intrinio(f"securities/{symbol}/prices/realtime", self.intrinio), bool(self.intrinio)),
            "yahoo":    (lambda: _live_yahoo(symbol, "info"), True),
        }
        return self._bundle_response(self._run_parallel(tasks))

    # ---------- 1. Income Statement ----------
    def income_statement(self, symbol: str) -> dict:
        tasks = {
            "fmp":     (lambda: _live_fmp(f"income-statement/{symbol}", self.fmp, limit=4), bool(self.fmp)),
            "finnhub": (lambda: _live_finnhub("stock/financials-reported", self.finnhub, symbol=symbol, freq="annual"), bool(self.finnhub)),
            "twelve":  (lambda: _live_twelve("income_statement", self.twelve, symbol=symbol), bool(self.twelve)),
            "alpha":   (lambda: _live_alpha("INCOME_STATEMENT", self.alpha, symbol=symbol), bool(self.alpha)),
            "intrinio": (lambda: _live_intrinio(f"companies/{symbol}/fundamentals", self.intrinio,
                                                  statement_code="income_statement", type="QTR"), bool(self.intrinio)),
            "yahoo":   (lambda: _live_yahoo(symbol, "income"), True),
        }
        return self._bundle_response(self._run_parallel(tasks))

    # ---------- 2. Balance Sheet ----------
    def balance_sheet(self, symbol: str) -> dict:
        tasks = {
            "fmp":    (lambda: _live_fmp(f"balance-sheet-statement/{symbol}", self.fmp, limit=4), bool(self.fmp)),
            "twelve": (lambda: _live_twelve("balance_sheet", self.twelve, symbol=symbol), bool(self.twelve)),
            "alpha":  (lambda: _live_alpha("BALANCE_SHEET", self.alpha, symbol=symbol), bool(self.alpha)),
            "intrinio": (lambda: _live_intrinio(f"companies/{symbol}/fundamentals", self.intrinio,
                                                  statement_code="balance_sheet_statement", type="QTR"), bool(self.intrinio)),
            "yahoo":  (lambda: _live_yahoo(symbol, "balance"), True),
        }
        return self._bundle_response(self._run_parallel(tasks))

    # ---------- 3. Cash Flow ----------
    def cash_flow(self, symbol: str) -> dict:
        tasks = {
            "fmp":    (lambda: _live_fmp(f"cash-flow-statement/{symbol}", self.fmp, limit=4), bool(self.fmp)),
            "twelve": (lambda: _live_twelve("cash_flow", self.twelve, symbol=symbol), bool(self.twelve)),
            "alpha":  (lambda: _live_alpha("CASH_FLOW", self.alpha, symbol=symbol), bool(self.alpha)),
            "intrinio": (lambda: _live_intrinio(f"companies/{symbol}/fundamentals", self.intrinio,
                                                  statement_code="cash_flow_statement", type="QTR"), bool(self.intrinio)),
            "yahoo":  (lambda: _live_yahoo(symbol, "cashflow"), True),
        }
        return self._bundle_response(self._run_parallel(tasks))

    # ---------- 4-5. Earnings (annual + quarterly snapshot) ----------
    def earnings(self, symbol: str) -> dict:
        tasks = {
            "fmp":     (lambda: _live_fmp(f"earnings/{symbol}", self.fmp, limit=12), bool(self.fmp)),
            "finnhub": (lambda: _live_finnhub("stock/earnings", self.finnhub, symbol=symbol), bool(self.finnhub)),
            "alpha":   (lambda: _live_alpha("EARNINGS", self.alpha, symbol=symbol), bool(self.alpha)),
        }
        return self._bundle_response(self._run_parallel(tasks))

    # ---------- 7. Key Metrics ----------
    def key_metrics(self, symbol: str) -> dict:
        tasks = {
            "fmp":      (lambda: _live_fmp(f"key-metrics/{symbol}", self.fmp, limit=4), bool(self.fmp)),
            "finnhub":  (lambda: _live_finnhub("stock/metric", self.finnhub, symbol=symbol, metric="all"), bool(self.finnhub)),
            "twelve":   (lambda: _live_twelve("statistics", self.twelve, symbol=symbol), bool(self.twelve)),
            "alpha":    (lambda: _live_alpha("OVERVIEW", self.alpha, symbol=symbol), bool(self.alpha)),
            "tiingo":   (lambda: _live_tiingo(f"tiingo/daily/{symbol.lower()}", self.tiingo, symbol=symbol), bool(self.tiingo)),
            "intrinio": (lambda: _live_intrinio(f"companies/{symbol}", self.intrinio), bool(self.intrinio)),
        }
        return self._bundle_response(self._run_parallel(tasks))

    # ---------- 11. Dividends ----------
    def dividends(self, symbol: str) -> dict:
        tasks = {
            "fmp":     (lambda: _live_fmp(f"historical-price-full/stock_dividend/{symbol}", self.fmp), bool(self.fmp)),
            "finnhub": (lambda: _live_finnhub("stock/dividend", self.finnhub, symbol=symbol,
                                                **{"from": "2020-01-01", "to": str(date.today())}), bool(self.finnhub)),
            "twelve":  (lambda: _live_twelve("dividends", self.twelve, symbol=symbol), bool(self.twelve)),
            "polygon": (lambda: _live_polygon("v3/reference/dividends", self.polygon, ticker=symbol, limit=20), bool(self.polygon)),
            "yahoo":   (lambda: _live_yahoo(symbol, "dividends"), True),
        }
        return self._bundle_response(self._run_parallel(tasks))

    # ---------- 12-13. Ownership ----------
    def institutional(self, symbol: str) -> dict:
        tasks = {
            "fmp":     (lambda: _live_fmp(f"institutional-holder/{symbol}", self.fmp), bool(self.fmp)),
            "finnhub": (lambda: _live_finnhub("stock/institutional-ownership", self.finnhub, symbol=symbol, limit=15), bool(self.finnhub)),
            "yahoo":   (lambda: _live_yahoo(symbol, "institutional"), True),
        }
        return self._bundle_response(self._run_parallel(tasks))

    # ---------- 14. Insider trading ----------
    def insider_trading(self, symbol: str) -> dict:
        tasks = {
            "fmp":      (lambda: _live_fmp("insider-trading", self.fmp, symbol=symbol, limit=20), bool(self.fmp)),
            "finnhub":  (lambda: _live_finnhub("stock/insider-transactions", self.finnhub, symbol=symbol), bool(self.finnhub)),
            "intrinio": (lambda: _live_intrinio(f"companies/{symbol}/insider_transaction_filings", self.intrinio,
                                                  page_size=20), bool(self.intrinio)),
            "yahoo":    (lambda: _live_yahoo(symbol, "insider"), True),
        }
        return self._bundle_response(self._run_parallel(tasks))

    # ---------- 15. Segments ----------
    def segments(self, symbol: str) -> dict:
        tasks = {
            "fmp_product": (lambda: _live_fmp("revenue-product-segmentation", self.fmp, symbol=symbol), bool(self.fmp)),
            "fmp_geo":     (lambda: _live_fmp("revenue-geographic-segmentation", self.fmp, symbol=symbol), bool(self.fmp)),
        }
        return self._bundle_response(self._run_parallel(tasks))

    # ---------- 16. Estimates ----------
    def estimates(self, symbol: str) -> dict:
        tasks = {
            "fmp":     (lambda: _live_fmp(f"analyst-estimates/{symbol}", self.fmp, limit=8), bool(self.fmp)),
            "finnhub": (lambda: _live_finnhub("stock/recommendation", self.finnhub, symbol=symbol), bool(self.finnhub)),
            "twelve":  (lambda: _live_twelve("analyst_ratings", self.twelve, symbol=symbol), bool(self.twelve)),
            "yahoo":   (lambda: _live_yahoo(symbol, "recommendations"), True),
        }
        return self._bundle_response(self._run_parallel(tasks))

    # ---------- 17. ESG ----------
    def esg(self, symbol: str) -> dict:
        tasks = {
            "fmp":   (lambda: _live_fmp("esg-environmental-social-governance-data", self.fmp, symbol=symbol), bool(self.fmp)),
            "yahoo": (lambda: _live_yahoo(symbol, "esg"), True),
        }
        return self._bundle_response(self._run_parallel(tasks))

    # ---------- 19. Options ----------
    def options(self, symbol: str) -> dict:
        tasks = {
            "polygon": (lambda: _live_polygon("v3/reference/options/contracts", self.polygon,
                                                underlying_ticker=symbol, limit=50), bool(self.polygon)),
            "yahoo":   (lambda: _live_yahoo(symbol, "options"), True),
        }
        return self._bundle_response(self._run_parallel(tasks))

    # ---------- 20. Peers ----------
    def peers(self, symbol: str) -> dict:
        tasks = {
            "fmp":     (lambda: _live_fmp("stock_peers", self.fmp, symbol=symbol), bool(self.fmp)),
            "finnhub": (lambda: _live_finnhub("stock/peers", self.finnhub, symbol=symbol), bool(self.finnhub)),
        }
        return self._bundle_response(self._run_parallel(tasks))

    # ---------- 21. Sentiment ----------
    def sentiment(self, symbol: str) -> dict:
        tasks = {
            "finnhub": (lambda: _live_finnhub("news-sentiment", self.finnhub, symbol=symbol), bool(self.finnhub)),
            "alpha":   (lambda: _live_alpha("NEWS_SENTIMENT", self.alpha, tickers=symbol, limit=20), bool(self.alpha)),
        }
        return self._bundle_response(self._run_parallel(tasks))

    # ---------- 22. Earnings Call Transcript ----------
    def earnings_transcript(self, symbol: str) -> dict:
        tasks = {
            "fmp":   (lambda: _live_fmp(f"earning_call_transcript/{symbol}", self.fmp,
                                          year=date.today().year, quarter=1), bool(self.fmp)),
            "alpha": (lambda: _live_alpha("EARNINGS_CALL_TRANSCRIPT", self.alpha, symbol=symbol), bool(self.alpha)),
        }
        return self._bundle_response(self._run_parallel(tasks))

    # ---------- 23. Macro ----------
    def macro(self) -> dict:
        tasks = {
            "fmp_inflation": (lambda: _live_fmp("economic", self.fmp, name="CPI"), bool(self.fmp)),
            "fmp_gdp":       (lambda: _live_fmp("economic", self.fmp, name="GDP"), bool(self.fmp)),
            "fmp_unemploy":  (lambda: _live_fmp("economic", self.fmp, name="unemploymentRate"), bool(self.fmp)),
            "alpha_cpi":     (lambda: _live_alpha("CPI", self.alpha, interval="monthly"), bool(self.alpha)),
            "alpha_fedrate": (lambda: _live_alpha("FEDERAL_FUNDS_RATE", self.alpha, interval="monthly"), bool(self.alpha)),
        }
        return self._bundle_response(self._run_parallel(tasks))

    # ---------- 24. Yield curve ----------
    def yield_curve(self) -> dict:
        tasks = {
            "fmp":   (lambda: _live_fmp("treasury", self.fmp,
                                          **{"from": "2024-01-01", "to": str(date.today())}), bool(self.fmp)),
            "alpha": (lambda: _live_alpha("TREASURY_YIELD", self.alpha,
                                            interval="daily", maturity="10year"), bool(self.alpha)),
        }
        return self._bundle_response(self._run_parallel(tasks))

    # ---------- 26. ETF holdings ----------
    def etf_holdings(self, symbol: str) -> dict:
        tasks = {
            "fmp":     (lambda: _live_fmp(f"etf-holder/{symbol}", self.fmp), bool(self.fmp)),
            "finnhub": (lambda: _live_finnhub("etf/holdings", self.finnhub, symbol=symbol), bool(self.finnhub)),
        }
        return self._bundle_response(self._run_parallel(tasks))

    # ---------- 27. Corporate actions (splits + dividends) ----------
    def corporate_actions(self, symbol: str) -> dict:
        tasks = {
            "fmp_splits":     (lambda: _live_fmp(f"historical-price-full/stock_split/{symbol}", self.fmp), bool(self.fmp)),
            "polygon_splits": (lambda: _live_polygon("v3/reference/splits", self.polygon, ticker=symbol, limit=20), bool(self.polygon)),
            "yahoo_splits":   (lambda: _live_yahoo(symbol, "splits"), True),
        }
        return self._bundle_response(self._run_parallel(tasks))

    # ---------- Sahm.sa (Saudi market) — already cooperative ----------
    def saudi(self, symbol: str) -> dict:
        if not self.sahmk:
            return self._bundle_response({"sahmk": {"_ok": False, "_error": "no_key"}})
        client = get_sahmk_client(api_key=self.sahmk, base_url=self.sahmk_base)
        # Sahm.sa hits the same backend; no benefit from threading it
        out = {
            "sahmk_quote":      client.quote(symbol),
            "sahmk_company":    client.company(symbol),
            "sahmk_financials": client.financials(symbol),
            "sahmk_dividends":  client.dividends(symbol),
            "sahmk_news":       client.news(symbol),
        }
        return self._bundle_response(out)


@st.cache_resource
def get_multi_provider_fetcher(_keys_signature: str, keys: dict | None = None) -> MultiProviderFetcher:
    """Cached fetcher — `_keys_signature` is a hash of the keys to invalidate cache."""
    return MultiProviderFetcher(keys or {})


# ============================================================
# Data Engine — the heart of the system
# ------------------------------------------------------------
# Takes the raw `by_provider` dict from MultiProviderFetcher, scores each
# response by quality, picks the BEST primary, and produces a deeply-merged
# result that fills missing fields from secondary providers.
#
# This is what separates Atlas from a wrapper script — every API has gaps,
# rate limits, and bad days; the Data Engine cooperates them.
# ============================================================
# Provider trust priors — used to break ties when quality scores match.
# Weights are tuned per resource family based on typical data quality:
PROVIDER_TRUST: dict = {
    "fundamentals": {  # income, balance, cash flow, key metrics
        "fmp": 1.0, "intrinio": 0.95, "alpha": 0.85, "twelve": 0.75,
        "finnhub": 0.70, "tiingo": 0.62, "yahoo": 0.55,
    },
    "ownership": {  # insider, institutional
        "finnhub": 0.95, "fmp": 0.90, "intrinio": 0.90, "yahoo": 0.65,
    },
    "dividends": {
        "fmp": 0.95, "polygon": 0.92, "twelve": 0.85, "intrinio": 0.85,
        "finnhub": 0.80, "yahoo": 0.75,
    },
    "estimates": {
        "fmp": 0.90, "finnhub": 0.88, "twelve": 0.80, "yahoo": 0.65,
    },
    "macro": {  # CPI/GDP/yield
        "fmp": 0.85, "alpha": 0.92,
    },
    "default": {
        "intrinio": 0.95, "fmp": 0.90, "alpha": 0.85, "finnhub": 0.85,
        "twelve": 0.80, "polygon": 0.85, "tiingo": 0.82, "yahoo": 0.65, "sahmk": 0.85,
    },
}


def _score_response(payload: dict) -> tuple[float, dict]:
    """Score a single provider response on quality.

    Returns (score, details). Score is 0..1 where 1 is best.
    Components:
      • completeness: % of fields populated (non-empty / non-None)
      • record count:  log-scaled count of rows
      • freshness:     bonus if a recent timestamp is present
      • shape penalty: array > scalar > empty
    """
    if not payload or not payload.get("_ok"):
        return 0.0, {"reason": payload.get("_error") if payload else "no_response"}

    data = payload.get("data")
    if data is None:
        return 0.0, {"reason": "null_data"}

    # Completeness + record count
    if isinstance(data, list):
        record_count = len(data)
        if record_count == 0:
            return 0.05, {"shape": "empty_list"}
        sample = data[0] if isinstance(data[0], dict) else {}
        if sample:
            non_empty = sum(1 for v in sample.values() if v not in (None, "", [], {}))
            completeness = non_empty / max(len(sample), 1)
        else:
            completeness = 0.5  # array of scalars
    elif isinstance(data, dict):
        record_count = 1
        non_empty = sum(1 for v in data.values() if v not in (None, "", [], {}))
        completeness = non_empty / max(len(data), 1)
    else:
        return 0.30, {"shape": "scalar"}

    # Record-count bonus (log-scaled, capped)
    import math
    count_score = min(math.log10(max(record_count, 1) + 1) / 1.5, 1.0)

    # Freshness — look for any ISO-ish date field
    fresh_bonus = 0.0
    for key in ("date", "fiscalDateEnding", "filingDate", "datetime",
                "lastUpdated", "lastTradeTime"):
        if isinstance(data, dict) and data.get(key):
            fresh_bonus = 0.10
            break
        if isinstance(data, list) and data and isinstance(data[0], dict) and data[0].get(key):
            fresh_bonus = 0.10
            break

    # Weighted score
    score = (completeness * 0.55 + count_score * 0.30 + fresh_bonus + 0.05)
    score = min(max(score, 0.0), 1.0)
    return score, {
        "completeness": round(completeness, 3),
        "count_score": round(count_score, 3),
        "record_count": record_count,
        "fresh_bonus": fresh_bonus,
    }


def _trust_weight(provider: str, family: str) -> float:
    bag = PROVIDER_TRUST.get(family) or PROVIDER_TRUST["default"]
    # Look up by exact key, then by prefix (handles fmp_product, alpha_cpi, …)
    if provider in bag:
        return bag[provider]
    for k, w in bag.items():
        if provider.startswith(k):
            return w * 0.9  # slight discount for sub-variants
    return 0.5


def _provider_health_score(provider: str) -> float:
    """Current adaptive provider health score, defaulting to healthy."""
    state = LIVE_PROVIDER_HEALTH.get(provider) if "LIVE_PROVIDER_HEALTH" in globals() else None
    if isinstance(state, dict) and is_num(state.get("health")):
        return max(0.0, min(1.0, float(state["health"])))
    return 1.0


def _numeric_median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def fuse_candidate_values(candidates: list[dict[str, Any]], strategy: str = "confidence_best") -> dict[str, Any]:
    """Fuse field-level values from multiple providers.

    Output contract:
      value, confidence, provider_attribution, all_provider_values, warnings.

    Strategies:
      weighted_average   numeric only, weighted by confidence
      priority_first     first non-empty provider in the supplied order
      median             numeric median, robust to outliers
      confidence_best    highest confidence provider
    """
    valid = [c for c in candidates if is_value_present(c.get("value"))]
    warnings: list[str] = []
    if not valid:
        return {
            "value": None,
            "confidence": 0.0,
            "provider_attribution": None,
            "all_provider_values": {},
            "warnings": ["No provider supplied a usable value."],
            "strategy": strategy,
        }

    for c in valid:
        c["confidence"] = round(max(0.0, min(1.0, float(c.get("confidence", 0.5)))), 3)

    all_values = {
        c["provider"]: {"value": c.get("value"), "confidence": c.get("confidence")}
        for c in valid
    }
    numeric_candidates = [c for c in valid if is_num(c.get("value"))]

    if strategy == "weighted_average":
        if not numeric_candidates:
            warnings.append("Weighted average requires numeric values; used confidence_best instead.")
            strategy = "confidence_best"
        else:
            weight_sum = sum(max(float(c.get("confidence", 0.0)), 0.01) for c in numeric_candidates)
            value = sum(float(c["value"]) * max(float(c.get("confidence", 0.0)), 0.01) for c in numeric_candidates) / max(weight_sum, 0.01)
            confidence = min(1.0, sum(float(c.get("confidence", 0.0)) for c in numeric_candidates) / max(len(numeric_candidates), 1) + 0.04 * (len(numeric_candidates) - 1))
            return {
                "value": value,
                "confidence": round(confidence, 3),
                "provider_attribution": " + ".join(c["provider"] for c in numeric_candidates),
                "all_provider_values": all_values,
                "warnings": warnings,
                "strategy": "weighted_average",
            }

    if strategy == "median":
        if not numeric_candidates:
            warnings.append("Median requires numeric values; used confidence_best instead.")
            strategy = "confidence_best"
        else:
            value = _numeric_median([float(c["value"]) for c in numeric_candidates])
            best_near = max(numeric_candidates, key=lambda c: float(c.get("confidence", 0.0)))
            dispersion = 0.0
            if len(numeric_candidates) > 1 and is_num(value) and value:
                dispersion = max(abs(float(c["value"]) - float(value)) / abs(float(value)) for c in numeric_candidates)
            confidence = max(0.0, min(1.0, float(best_near.get("confidence", 0.5)) - min(dispersion, 0.4)))
            return {
                "value": value,
                "confidence": round(confidence, 3),
                "provider_attribution": "median(" + ", ".join(c["provider"] for c in numeric_candidates) + ")",
                "all_provider_values": all_values,
                "warnings": warnings,
                "strategy": "median",
            }

    if strategy == "priority_first":
        chosen = valid[0]
    else:
        chosen = max(valid, key=lambda c: float(c.get("confidence", 0.0)))

    return {
        "value": chosen.get("value"),
        "confidence": chosen.get("confidence", 0.0),
        "provider_attribution": chosen.get("provider"),
        "all_provider_values": all_values,
        "warnings": warnings,
        "strategy": strategy,
    }


def fuse_bundle_metric(bundle: dict[str, Any], field: str, strategy: str = "confidence_best") -> dict[str, Any]:
    """Field-level fusion over the already-normalized provider payloads."""
    providers = bundle.get("providers", {}) if isinstance(bundle, dict) else {}
    order_list = FIELD_PROVIDER_ORDER.get(field, MARKET_PROVIDER_ORDER if field in MARKET_FIELDS else DEFAULT_PROVIDER_ORDER)
    ordered = order_list + [p for p in providers.keys() if p not in order_list]
    candidates: list[dict[str, Any]] = []
    for provider in ordered:
        payload = providers.get(provider) or {}
        metrics = payload.get("metrics") or {}
        value = metrics.get(field)
        if not is_value_present(value):
            continue
        status = payload.get("status", "unknown")
        coverage = float(payload.get("coverage", 0) or 0)
        status_score = 1.0 if status == "ok" else 0.78 if status == "partial" else 0.45
        rank_score = 1.0 - (min(ordered.index(provider), 12) * 0.045)
        confidence = status_score * (0.58 + min(coverage, 100.0) / 100.0 * 0.22) * _provider_health_score(provider) * rank_score
        candidates.append({
            "provider": provider,
            "value": value,
            "confidence": confidence,
            "status": status,
            "coverage": coverage,
        })

    fused = fuse_candidate_values(candidates, strategy=strategy)
    fused["field"] = field
    fused["symbol"] = bundle.get("symbol")
    return fused


def _extract_price_from_live_payload(provider: str, payload: dict) -> float | None:
    """Best-effort live quote normalizer for DataEngine.get_price()."""
    if not isinstance(payload, dict) or not payload.get("_ok"):
        return None
    data = payload.get("data")
    if isinstance(data, list) and data:
        data = data[-1] if provider == "tiingo" else data[0]
    if not isinstance(data, dict):
        return None
    key_sets = {
        "finnhub": ["c"],
        "twelve": ["close", "price"],
        "polygon": ["c", "close"],
        "tiingo": ["close", "adjClose"],
        "intrinio": ["last_price", "last"],
        "yahoo": ["last_price", "lastPrice", "regularMarketPrice"],
        "fmp": ["price"],
    }
    keys = key_sets.get(provider, []) + ["price", "close", "last", "c"]
    return get_num_from_many([data], keys)


def _deep_merge(primary: Any, secondary: Any) -> Any:
    """Fill missing keys in `primary` from `secondary`. Non-destructive."""
    if primary is None:
        return secondary
    if secondary is None:
        return primary
    if isinstance(primary, dict) and isinstance(secondary, dict):
        out = dict(primary)
        for k, v in secondary.items():
            if k not in out or out[k] in (None, "", [], {}):
                out[k] = v
            elif isinstance(out[k], (dict, list)) and isinstance(v, (dict, list)):
                out[k] = _deep_merge(out[k], v)
        return out
    if isinstance(primary, list) and isinstance(secondary, list):
        # Lists: prefer the longer one, no element-level merging
        return primary if len(primary) >= len(secondary) else secondary
    return primary


class DataEngine:
    """Cooperative multi-provider data engine.

    Pipeline per resource:
      1. MultiProviderFetcher fetches all providers in parallel
      2. _score_response assigns a quality score to each
      3. trust_weight modifier breaks ties by provider reputation
      4. Best-scoring response becomes primary
      5. Other OK responses fill primary's missing fields (deep merge)
      6. Returns a result with primary, merged, scores, and provenance
    """

    def __init__(self, fetcher: MultiProviderFetcher):
        self.fetcher = fetcher

    @staticmethod
    def fuse(by_provider: dict, family: str = "default") -> dict:
        """Score every provider response and produce the unified result."""
        scored: list[tuple[float, str, dict]] = []
        for prov, payload in by_provider.items():
            base, detail = _score_response(payload)
            trust = _trust_weight(prov, family)
            final = base * (0.6 + 0.4 * trust)  # 60% data quality + 40% trust
            scored.append((final, prov, {
                "score": round(final, 3),
                "raw_score": round(base, 3),
                "trust": round(trust, 2),
                **detail,
            }))
        scored.sort(key=lambda x: x[0], reverse=True)

        if not scored or scored[0][0] == 0.0:
            return {
                "primary": None,
                "merged": None,
                "by_provider": by_provider,
                "primary_provider": None,
                "fusion_chain": [s[1] for s in scored],
                "quality_scores": {p: d for _, p, d in scored},
                "coverage": {p: ("ok" if (by_provider.get(p) or {}).get("_ok") and (by_provider.get(p) or {}).get("data")
                                  else "no_key" if (by_provider.get(p) or {}).get("_error") == "no_key"
                                  else "error") for p in by_provider},
            }

        # Build the merged payload starting from the best provider
        primary_provider = scored[0][1]
        primary_data = (by_provider[primary_provider] or {}).get("data")
        merged = primary_data
        for _, prov, _ in scored[1:]:
            if (by_provider.get(prov) or {}).get("_ok"):
                secondary = (by_provider[prov] or {}).get("data")
                merged = _deep_merge(merged, secondary)

        coverage = {}
        for prov, payload in by_provider.items():
            if not isinstance(payload, dict):
                coverage[prov] = "error"
            elif payload.get("_ok") and payload.get("data"):
                coverage[prov] = "ok"
            elif payload.get("_ok"):
                coverage[prov] = "partial"
            elif payload.get("_error") == "no_key":
                coverage[prov] = "no_key"
            else:
                coverage[prov] = "error"

        return {
            "primary": primary_data,
            "merged": merged,
            "primary_provider": primary_provider,
            "fusion_chain": [s[1] for s in scored if s[0] > 0],
            "quality_scores": {p: d for _, p, d in scored},
            "by_provider": by_provider,
            "coverage": coverage,
        }

    # Resource-family wrappers — apply the right trust bag to each
    def income_statement(self, symbol: str) -> dict:
        return self.fuse(self.fetcher.income_statement(symbol)["by_provider"], "fundamentals")

    def balance_sheet(self, symbol: str) -> dict:
        return self.fuse(self.fetcher.balance_sheet(symbol)["by_provider"], "fundamentals")

    def cash_flow(self, symbol: str) -> dict:
        return self.fuse(self.fetcher.cash_flow(symbol)["by_provider"], "fundamentals")

    def key_metrics(self, symbol: str) -> dict:
        return self.fuse(self.fetcher.key_metrics(symbol)["by_provider"], "fundamentals")

    def dividends(self, symbol: str) -> dict:
        return self.fuse(self.fetcher.dividends(symbol)["by_provider"], "dividends")

    def institutional(self, symbol: str) -> dict:
        return self.fuse(self.fetcher.institutional(symbol)["by_provider"], "ownership")

    def insider_trading(self, symbol: str) -> dict:
        return self.fuse(self.fetcher.insider_trading(symbol)["by_provider"], "ownership")

    def estimates(self, symbol: str) -> dict:
        return self.fuse(self.fetcher.estimates(symbol)["by_provider"], "estimates")

    def macro(self) -> dict:
        return self.fuse(self.fetcher.macro()["by_provider"], "macro")

    def yield_curve(self) -> dict:
        return self.fuse(self.fetcher.yield_curve()["by_provider"], "macro")

    # Unified API-style methods used by the Streamlit UI and the optional
    # in-file FastAPI factory. They return structured safe payloads.
    def get_price(self, symbol: str) -> dict:
        try:
            raw = self.fetcher.price_snapshot(symbol)["by_provider"]
            candidates = []
            for provider, payload in raw.items():
                value = _extract_price_from_live_payload(provider, payload)
                if is_num(value):
                    quality, _ = _score_response(payload if isinstance(payload, dict) else {})
                    confidence = (0.55 * quality + 0.45 * _trust_weight(provider, "default")) * _provider_health_score(provider)
                    candidates.append({"provider": provider, "value": float(value), "confidence": confidence})
            fused = fuse_candidate_values(candidates, "confidence_best")
            return {"_ok": fused.get("value") is not None, "source": fused.get("provider_attribution"), "data": fused, "error": None if fused.get("value") is not None else "No price available"}
        except Exception as exc:
            return {"_ok": False, "source": "none", "data": None, "error": str(exc)[:240]}

    def get_field(self, symbol: str, field: str) -> dict:
        try:
            if field in MARKET_FIELDS or field == "price":
                price = self.get_price(symbol)
                if field == "price":
                    return price
            fused_bundle = self.fuse(self.fetcher.key_metrics(symbol)["by_provider"], "fundamentals")
            return {"_ok": True, "source": fused_bundle.get("primary_provider"), "data": fused_bundle, "error": None, "field": field}
        except Exception as exc:
            return {"_ok": False, "source": "none", "data": None, "error": str(exc)[:240], "field": field}

    def get_fundamentals(self, symbol: str) -> dict:
        try:
            data = {
                "income_statement": self.income_statement(symbol),
                "balance_sheet": self.balance_sheet(symbol),
                "cash_flow": self.cash_flow(symbol),
                "key_metrics": self.key_metrics(symbol),
            }
            return {"_ok": True, "source": "data_engine", "data": data, "error": None}
        except Exception as exc:
            return {"_ok": False, "source": "none", "data": None, "error": str(exc)[:240]}

    @staticmethod
    def get_diagnostics() -> dict:
        return {"_ok": True, "source": "diagnostics", "data": LIVE_DIAGNOSTICS[-200:], "error": None}

    @staticmethod
    def get_provider_health() -> dict:
        return {"_ok": True, "source": "health", "data": LIVE_PROVIDER_HEALTH, "error": None}

    @staticmethod
    def clear_cache() -> dict:
        try:
            st.cache_data.clear()
            LIVE_DIAGNOSTICS.clear()
            LIVE_PROVIDER_HEALTH.clear()
            LIVE_CACHE_STATS.update({"requests": 0, "success": 0, "failure": 0, "rate_limited": 0})
            return {"_ok": True, "source": "cache", "data": {"cleared": True}, "error": None}
        except Exception as exc:
            return {"_ok": False, "source": "cache", "data": None, "error": str(exc)[:240]}


def create_fastapi_backend(keys: dict[str, str] | None = None):
    """Optional in-file FastAPI backend factory.

    The user requested the architecture but also requested `app.py` only. This
    factory keeps the backend available without starting another server from
    Streamlit. Run it externally with an ASGI server if needed:
      uvicorn app:create_fastapi_backend --factory
    """
    try:
        import importlib
        fastapi_module = importlib.import_module("fastapi")
        cors_module = importlib.import_module("fastapi.middleware.cors")
        pydantic_module = importlib.import_module("pydantic")
        fastapi_constructor = getattr(fastapi_module, "FastAPI")
        cors_middleware = getattr(cors_module, "CORSMiddleware")
        base_model = getattr(pydantic_module, "BaseModel")
        field_constructor = getattr(pydantic_module, "Field")
        try:
            config_dict_constructor = getattr(pydantic_module, "ConfigDict")
        except Exception:
            config_dict_constructor = None
    except Exception as exc:
        return {"_ok": False, "error": f"FastAPI dependencies unavailable: {exc}"}

    class SafeResponse(base_model):
        ok: bool = field_constructor(alias="_ok")
        source: str = "none"
        data: Any = None
        error: str | None = None

        if config_dict_constructor is not None:
            model_config = config_dict_constructor(populate_by_name=True)
        else:
            class Config:
                allow_population_by_field_name = True

    fastapi_app = fastapi_constructor(title="Atlas Stock Intelligence API", version="1.0")
    fastapi_app.add_middleware(
        cors_middleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    engine = DataEngine(MultiProviderFetcher(keys or {}))

    @fastapi_app.get("/health", response_model=SafeResponse)
    def api_health():
        return {"_ok": True, "source": "api", "data": {"status": "ok"}, "error": None}

    @fastapi_app.get("/stock/{symbol}/price")
    def api_stock_price(symbol: str):
        return engine.get_price(symbol.upper())

    @fastapi_app.get("/stock/{symbol}/field/{field}")
    def api_stock_field(symbol: str, field: str):
        return engine.get_field(symbol.upper(), field)

    @fastapi_app.get("/diagnostics")
    def api_diagnostics():
        return DataEngine.get_diagnostics()

    @fastapi_app.get("/providers/health")
    def api_provider_health():
        return DataEngine.get_provider_health()

    @fastapi_app.post("/cache/clear")
    def api_cache_clear():
        return DataEngine.clear_cache()

    return fastapi_app


# ============================================================
# Live dashboard — 9 categories × 28 sections × multi-provider
# ============================================================
def _coverage_badges(coverage: dict) -> str:
    """Build inline HTML badges for provider coverage."""
    parts = []
    for prov, status in coverage.items():
        emoji = {"ok": "🟢", "partial": "🟡", "no_key": "⚪", "error": "🔴"}.get(status, "⚫")
        label = {
            "fmp": "FMP", "finnhub": "Finnhub", "twelve": "Twelve Data",
            "alpha": "Alpha Vantage", "polygon": "Polygon", "yahoo": "Yahoo",
            "sahmk": "Sahm.sa", "intrinio": "Intrinio", "tiingo": "Tiingo",
            "fmp_product": "FMP·product", "fmp_geo": "FMP·geo",
            "fmp_inflation": "CPI", "fmp_gdp": "GDP", "fmp_unemploy": "Unemp",
            "alpha_cpi": "CPI·AV", "alpha_fedrate": "Fed",
            "fmp_splits": "Splits·FMP", "polygon_splits": "Splits·Polygon",
            "yahoo_splits": "Splits·Yahoo",
            "sahmk_quote": "Sahm·Q", "sahmk_company": "Sahm·Co",
            "sahmk_financials": "Sahm·Fin", "sahmk_dividends": "Sahm·Div",
            "sahmk_news": "Sahm·News",
        }.get(prov, prov)
        parts.append(f"<span class='news-cat-pill'>{emoji} {escape(label)}</span>")
    return "".join(parts)


def _provider_data_to_df(by_provider: dict, prefer: list[str] | None = None,
                          symbol: str | None = None) -> "pd.DataFrame | None":
    """Pick the first provider with usable data; coerce into a DataFrame.

    If `symbol` is supplied, prepend a `Symbol` column for table clarity.
    """
    order = (prefer or []) + [k for k in by_provider.keys() if k not in (prefer or [])]
    for prov in order:
        payload = by_provider.get(prov) or {}
        if not payload.get("_ok"):
            continue
        data = payload.get("data")
        if data is None:
            continue
        df = _df_safe(data)
        if df is not None and not df.empty:
            df.attrs["_source_provider"] = prov
            if symbol:
                df = add_symbol_to_df(df, symbol) or df
            return df
    return None


def render_live_data_dashboard(bundle: dict, keys: dict) -> None:
    """Bloomberg-style live multi-provider dashboard.

    Renders 28 sections grouped into 9 expanders. Each section pulls REAL data
    from Finnhub / Twelve Data / Alpha Vantage / Polygon / Sahm.sa / Yahoo / FMP.
    """
    symbol = (bundle.get("symbol") or "").upper()
    if not symbol:
        st.warning("No symbol available for the live dashboard.")
        return

    # Build the fetcher (cached on a stable key signature)
    sig = "|".join(f"{k}={'1' if keys.get(k) else '0'}" for k in
                   ("fmp", "finnhub", "twelve_data", "alpha_vantage", "polygon", "tiingo", "intrinio", "sahmk"))
    fetcher = MultiProviderFetcher(keys)

    st.markdown(
        f'<div style="display:flex;align-items:center;justify-content:space-between;'
        f'gap:12px;flex-wrap:wrap;margin-bottom:0.8rem;">'
        f'<div class="section-header" style="margin:0;border:none;padding:0;">'
        f'<h3>🌐 Live Data Dashboard</h3>'
        f'<span class="section-count">9 categories · 28 sections · Polygon · Tiingo · Intrinio · Finnhub · Yahoo</span></div>'
        f'{symbol_badge(symbol, exchange=bundle.get("exchange"), size="lg")}'
        f'</div>',
        unsafe_allow_html=True,
    )

    # Top control bar
    cb1, cb2, cb3 = st.columns([2, 1, 1])
    cb1.caption(
        "🟢 OK · 🟡 partial · ⚪ no API key · 🔴 error.  "
        "Each section is cached for 10 minutes — click *Force refresh* to bust the cache."
    )
    if cb2.button("🔄 Force refresh", key="live_force_refresh"):
        st.cache_data.clear()
        st.success("Cache cleared. Re-open expanders to refetch.")
    auto = cb3.checkbox("Auto-fetch", value=False, key="live_auto",
                        help="Fetch all sections immediately. OFF saves API calls.")

    with st.expander("📡 Provider Health · Diagnostics · Cache", expanded=False):
        h1, h2, h3, h4 = st.columns(4)
        h1.metric("HTTP requests", LIVE_CACHE_STATS.get("requests", 0))
        h2.metric("Success", LIVE_CACHE_STATS.get("success", 0))
        h3.metric("Failures", LIVE_CACHE_STATS.get("failure", 0))
        h4.metric("Rate limited", LIVE_CACHE_STATS.get("rate_limited", 0))
        health_df = get_live_provider_health_frame()
        diag_df = get_live_diagnostics_frame()
        ht, dt = st.tabs(["Provider health", "Request diagnostics"])
        with ht:
            if health_df.empty:
                st.info("No provider requests recorded yet. Fetch a section to populate health scores.")
            else:
                st.dataframe(health_df, width="stretch", hide_index=True, height=240)
        with dt:
            if diag_df.empty:
                st.info("Diagnostics will appear after the first live provider call.")
            else:
                st.dataframe(diag_df.sort_values("timestamp", ascending=False), width="stretch", hide_index=True, height=260)
        if st.button("Clear live diagnostics", key="live_clear_diagnostics"):
            LIVE_DIAGNOSTICS.clear()
            LIVE_PROVIDER_HEALTH.clear()
            LIVE_CACHE_STATS.update({"requests": 0, "success": 0, "failure": 0, "rate_limited": 0})
            st.success("Live diagnostics cleared.")

    # ------------------------------------------------------------------
    # 🧠 Data Engine — fused, scored, best-pick across providers
    # ------------------------------------------------------------------
    engine = DataEngine(fetcher)
    with st.expander("🧠 Data Engine  (cooperate · score · merge across providers)",
                      expanded=auto):
        st.caption(
            "Fetches every provider in parallel → scores each by completeness · "
            "freshness · trust → picks the best primary → fills missing fields "
            "from the rest. The fused result is what powers the rest of the dashboard."
        )
        engine_pick = st.selectbox(
            "Resource",
            ["Income Statement", "Balance Sheet", "Cash Flow", "Key Metrics",
             "Dividends", "Institutional", "Insider Trading",
             "Estimates", "Macro", "Yield Curve"],
            key="data_engine_resource",
        )
        if st.button("🚀 Run Data Engine", key="data_engine_run") or auto:
            method_map = {
                "Income Statement":  ("income_statement", (symbol,)),
                "Balance Sheet":     ("balance_sheet", (symbol,)),
                "Cash Flow":         ("cash_flow", (symbol,)),
                "Key Metrics":       ("key_metrics", (symbol,)),
                "Dividends":         ("dividends", (symbol,)),
                "Institutional":     ("institutional", (symbol,)),
                "Insider Trading":   ("insider_trading", (symbol,)),
                "Estimates":         ("estimates", (symbol,)),
                "Macro":             ("macro", ()),
                "Yield Curve":       ("yield_curve", ()),
            }
            method_name, args = method_map[engine_pick]
            with st.spinner(f"Fusing {engine_pick} from all providers…"):
                fused = getattr(engine, method_name)(*args)

            primary_provider = fused.get("primary_provider") or "—"
            chain = fused.get("fusion_chain") or []
            scores = fused.get("quality_scores") or {}

            # Header summary
            sm1, sm2, sm3 = st.columns(3)
            sm1.metric("🥇 Primary source", primary_provider.upper())
            sm2.metric("Fusion chain", " → ".join(chain[:5]) or "—")
            best_score = scores.get(primary_provider, {}).get("score", 0) if primary_provider in scores else 0
            sm3.metric("Best score", f"{best_score:.2f}/1.00")

            st.markdown(_coverage_badges(fused.get("coverage") or {}), unsafe_allow_html=True)

            # Quality scores table
            st.markdown("**Quality scores per provider**")
            score_rows = [
                {
                    "Provider": prov.upper(),
                    "Score": d.get("score"),
                    "Completeness": d.get("completeness"),
                    "Records": d.get("record_count"),
                    "Trust prior": d.get("trust"),
                }
                for prov, d in scores.items()
            ]
            if score_rows:
                st.dataframe(
                    pd.DataFrame(score_rows).sort_values("Score", ascending=False),
                    width="stretch", hide_index=True, height=240,
                )

            # The merged (fused) primary
            st.markdown(f"**🧬 Merged result** (primary={primary_provider} + filled from " +
                         ", ".join(chain[1:5]) + ")")
            merged = fused.get("merged")
            if merged is not None:
                df = _df_safe(merged)
                if df is not None and not df.empty:
                    df = add_symbol_to_df(df, symbol) or df
                    st.dataframe(df.head(40), width="stretch", height=320)
                else:
                    st.json(merged)
            else:
                st.info(t("status.no_data"))

            with st.expander("Raw responses by provider", expanded=False):
                st.json(fused.get("by_provider"))

    # ------------------------------------------------------------------
    # 🟢 1. Core Financials
    # ------------------------------------------------------------------
    with st.expander("🟢 Core Financials  (Income · Balance · Cash Flow · Annual · Quarterly)",
                      expanded=auto):
        fetch_btn = st.button("📡 Fetch", key="live_core_btn") or auto
        if fetch_btn:
            with st.spinner("Fetching financial statements…"):
                inc = fetcher.income_statement(symbol)
                bal = fetcher.balance_sheet(symbol)
                cf = fetcher.cash_flow(symbol)
                earn = fetcher.earnings(symbol)
            tabs = st.tabs(["Income Statement", "Balance Sheet", "Cash Flow", "Annual / Quarterly"])
            with tabs[0]:
                st.markdown(_coverage_badges(inc["coverage"]), unsafe_allow_html=True)
                df = _provider_data_to_df(inc["by_provider"], prefer=["fmp", "yahoo", "alpha", "twelve"], symbol=symbol)
                if df is not None:
                    st.dataframe(df, width="stretch", height=320)
                    st.caption(f"Primary source: **{df.attrs.get('_source_provider', 'unknown')}**")
                else:
                    st.info("No income statement data returned by any provider.")
                with st.expander("Raw responses", expanded=False):
                    st.json(inc["by_provider"])
            with tabs[1]:
                st.markdown(_coverage_badges(bal["coverage"]), unsafe_allow_html=True)
                df = _provider_data_to_df(bal["by_provider"], prefer=["fmp", "yahoo", "alpha"], symbol=symbol)
                if df is not None:
                    st.dataframe(df, width="stretch", height=320)
                    st.caption(f"Primary: **{df.attrs.get('_source_provider', '?')}**")
                else:
                    st.info("No balance sheet returned.")
            with tabs[2]:
                st.markdown(_coverage_badges(cf["coverage"]), unsafe_allow_html=True)
                df = _provider_data_to_df(cf["by_provider"], prefer=["fmp", "yahoo", "alpha"], symbol=symbol)
                if df is not None:
                    st.dataframe(df, width="stretch", height=320)
                else:
                    st.info("No cash-flow data returned.")
            with tabs[3]:
                st.markdown(_coverage_badges(earn["coverage"]), unsafe_allow_html=True)
                df = _provider_data_to_df(earn["by_provider"], prefer=["fmp", "finnhub", "alpha"], symbol=symbol)
                if df is not None:
                    st.dataframe(df, width="stretch", height=320)
                else:
                    st.info("No earnings data.")

    # ------------------------------------------------------------------
    # 🟡 2. Quality & Analysis
    # ------------------------------------------------------------------
    with st.expander("🟡 Quality & Analysis  (Statement Quality · Key Metrics · DuPont · Health Scores)",
                      expanded=auto):
        if st.button("📡 Fetch", key="live_quality_btn") or auto:
            with st.spinner("Fetching key metrics…"):
                km = fetcher.key_metrics(symbol)
            st.markdown(_coverage_badges(km["coverage"]), unsafe_allow_html=True)
            df = _provider_data_to_df(km["by_provider"], prefer=["fmp", "intrinio", "finnhub", "twelve", "tiingo", "alpha"], symbol=symbol)
            if df is not None:
                st.dataframe(df, width="stretch", height=300)
                # Highlight DuPont + health rows if FMP shape
                if "fmp" in km["by_provider"] and km["by_provider"]["fmp"].get("_ok"):
                    raw = km["by_provider"]["fmp"].get("data") or []
                    if isinstance(raw, list) and raw:
                        m = raw[0]
                        d1, d2, d3, d4, d5 = st.columns(5)
                        d1.metric("ROE", f"{(m.get('roe') or 0)*100:.2f}%" if m.get("roe") else "—")
                        d2.metric("ROA", f"{(m.get('returnOnTangibleAssets') or m.get('roa') or 0)*100:.2f}%"
                                          if m.get("returnOnTangibleAssets") or m.get("roa") else "—")
                        d3.metric("Net margin", f"{(m.get('netIncomePerShare') or 0):.2f}")
                        d4.metric("D/E", f"{(m.get('debtToEquity') or 0):.2f}")
                        d5.metric("Current ratio", f"{(m.get('currentRatio') or 0):.2f}")
            else:
                st.info("No key metrics returned.")
            with st.expander("Raw responses", expanded=False):
                st.json(km["by_provider"])

    # ------------------------------------------------------------------
    # 🔵 3. Valuation & Investment
    # ------------------------------------------------------------------
    with st.expander("🔵 Valuation & Investment  (Multiples · Dividends · Ownership · Insider)",
                      expanded=auto):
        if st.button("📡 Fetch", key="live_val_btn") or auto:
            with st.spinner("Fetching valuation, dividends & ownership…"):
                divs = fetcher.dividends(symbol)
                inst = fetcher.institutional(symbol)
                ins = fetcher.insider_trading(symbol)
            tabs = st.tabs(["Dividends", "Institutional", "Insider trades"])
            with tabs[0]:
                st.markdown(_coverage_badges(divs["coverage"]), unsafe_allow_html=True)
                df = _provider_data_to_df(divs["by_provider"], prefer=["fmp", "polygon", "yahoo", "twelve"], symbol=symbol)
                if df is not None:
                    st.dataframe(df.head(30), width="stretch", height=300)
                else:
                    st.info("No dividend history returned.")
            with tabs[1]:
                st.markdown(_coverage_badges(inst["coverage"]), unsafe_allow_html=True)
                df = _provider_data_to_df(inst["by_provider"], prefer=["fmp", "yahoo", "finnhub"], symbol=symbol)
                if df is not None:
                    st.dataframe(df.head(15), width="stretch", height=300)
                else:
                    st.info("No institutional data.")
            with tabs[2]:
                st.markdown(_coverage_badges(ins["coverage"]), unsafe_allow_html=True)
                df = _provider_data_to_df(ins["by_provider"], prefer=["finnhub", "fmp", "yahoo"], symbol=symbol)
                if df is not None:
                    st.dataframe(df.head(20), width="stretch", height=300)
                else:
                    st.info("No insider trades.")

    # ------------------------------------------------------------------
    # 🟣 4. Advanced Insights
    # ------------------------------------------------------------------
    with st.expander("🟣 Advanced Insights  (Segments · Estimates · ESG · Debt)", expanded=auto):
        if st.button("📡 Fetch", key="live_adv_btn") or auto:
            with st.spinner("Fetching segments / estimates / ESG…"):
                seg = fetcher.segments(symbol)
                est = fetcher.estimates(symbol)
                esg = fetcher.esg(symbol)
            tabs = st.tabs(["Segments", "Estimates", "ESG"])
            with tabs[0]:
                st.markdown(_coverage_badges(seg["coverage"]), unsafe_allow_html=True)
                for prov_key, label in [("fmp_product", "By product"), ("fmp_geo", "By geography")]:
                    payload = seg["by_provider"].get(prov_key, {})
                    if payload.get("_ok") and payload.get("data"):
                        st.markdown(f"**{label}**")
                        df = _df_safe(payload["data"])
                        if df is not None:
                            st.dataframe(df.head(8), width="stretch", height=200)
            with tabs[1]:
                st.markdown(_coverage_badges(est["coverage"]), unsafe_allow_html=True)
                df = _provider_data_to_df(est["by_provider"], prefer=["fmp", "finnhub", "yahoo", "twelve"], symbol=symbol)
                if df is not None:
                    st.dataframe(df.head(15), width="stretch", height=300)
                else:
                    st.info("No analyst estimates.")
            with tabs[2]:
                st.markdown(_coverage_badges(esg["coverage"]), unsafe_allow_html=True)
                df = _provider_data_to_df(esg["by_provider"], prefer=["fmp", "yahoo"], symbol=symbol)
                if df is not None:
                    st.dataframe(df, width="stretch", height=240)
                else:
                    st.info("No ESG data.")

    # ------------------------------------------------------------------
    # 🟠 5. Market & Trading
    # ------------------------------------------------------------------
    with st.expander("🟠 Market & Trading  (Options · Peers · Sentiment · Earnings Call)", expanded=auto):
        if st.button("📡 Fetch", key="live_mkt_btn") or auto:
            with st.spinner("Fetching options / peers / sentiment / transcripts…"):
                opt = fetcher.options(symbol)
                peers = fetcher.peers(symbol)
                sent = fetcher.sentiment(symbol)
                trans = fetcher.earnings_transcript(symbol)
            tabs = st.tabs(["Options", "Peers", "Sentiment", "Earnings call"])
            with tabs[0]:
                st.markdown(_coverage_badges(opt["coverage"]), unsafe_allow_html=True)
                df = _provider_data_to_df(opt["by_provider"], prefer=["polygon", "yahoo"], symbol=symbol)
                if df is not None:
                    st.dataframe(df.head(30), width="stretch", height=320)
                else:
                    st.info("No options data — needs Polygon Stocks Options or Yahoo.")
            with tabs[1]:
                st.markdown(_coverage_badges(peers["coverage"]), unsafe_allow_html=True)
                # peers are usually a flat list of strings
                payload = (peers["by_provider"].get("fmp", {}).get("data")
                           or peers["by_provider"].get("finnhub", {}).get("data") or [])
                if payload:
                    if isinstance(payload, list) and payload and isinstance(payload[0], str):
                        st.markdown(" · ".join(f"`{p}`" for p in payload[:30]))
                    else:
                        df = _df_safe(payload)
                        if df is not None:
                            st.dataframe(df, width="stretch", height=240)
                else:
                    st.info("No peers returned.")
            with tabs[2]:
                st.markdown(_coverage_badges(sent["coverage"]), unsafe_allow_html=True)
                # Finnhub returns dict, Alpha Vantage returns dict with 'feed'
                fh = sent["by_provider"].get("finnhub", {}).get("data") or {}
                av = sent["by_provider"].get("alpha", {}).get("data") or {}
                if fh:
                    sm1, sm2, sm3, sm4 = st.columns(4)
                    if "buzz" in fh:
                        sm1.metric("Articles in week", fh.get("buzz", {}).get("articlesInLastWeek", "—"))
                    sm2.metric("Bullish %", f"{(fh.get('sentiment', {}).get('bullishPercent', 0))*100:.0f}%"
                                              if fh.get("sentiment") else "—")
                    sm3.metric("Bearish %", f"{(fh.get('sentiment', {}).get('bearishPercent', 0))*100:.0f}%"
                                              if fh.get("sentiment") else "—")
                    sm4.metric("Sector avg", f"{(fh.get('sectorAverageBullishPercent') or 0)*100:.0f}%")
                if av and av.get("feed"):
                    st.markdown("**Top recent articles (Alpha Vantage)**")
                    feed_df = pd.DataFrame(av["feed"][:10])[["title", "source", "overall_sentiment_label",
                                                              "overall_sentiment_score"]]
                    st.dataframe(feed_df, width="stretch", height=240)
            with tabs[3]:
                st.markdown(_coverage_badges(trans["coverage"]), unsafe_allow_html=True)
                payload = (trans["by_provider"].get("fmp", {}).get("data")
                           or trans["by_provider"].get("alpha", {}).get("data") or "")
                if payload:
                    text = payload[0].get("content", "") if isinstance(payload, list) and payload else str(payload)
                    st.text_area("Latest transcript", value=text[:5000], height=240,
                                  disabled=True, key="live_trans_text")
                else:
                    st.info("No earnings-call transcript.")

    # ------------------------------------------------------------------
    # 🔴 6. Macro
    # ------------------------------------------------------------------
    with st.expander("🔴 Macro  (Indicators · Yield Curves)", expanded=False):
        if st.button("📡 Fetch", key="live_macro_btn") or auto:
            with st.spinner("Fetching macro indicators & yield curve…"):
                macro = fetcher.macro()
                yc = fetcher.yield_curve()
            tabs = st.tabs(["Indicators", "Yield curve"])
            with tabs[0]:
                st.markdown(_coverage_badges(macro["coverage"]), unsafe_allow_html=True)
                for k, label in [("fmp_inflation", "CPI · FMP"), ("fmp_gdp", "GDP · FMP"),
                                  ("fmp_unemploy", "Unemployment"), ("alpha_cpi", "CPI · AlphaVantage"),
                                  ("alpha_fedrate", "Fed Funds Rate")]:
                    payload = macro["by_provider"].get(k, {})
                    if payload.get("_ok") and payload.get("data"):
                        df = _df_safe(payload["data"])
                        if df is not None and not df.empty:
                            st.markdown(f"**{label}**")
                            st.dataframe(df.head(8), width="stretch", height=180)
            with tabs[1]:
                st.markdown(_coverage_badges(yc["coverage"]), unsafe_allow_html=True)
                fmp_y = yc["by_provider"].get("fmp", {}).get("data") or []
                if fmp_y:
                    df = _df_safe(fmp_y)
                    if df is not None:
                        st.dataframe(df.head(15), width="stretch", height=240)
                        try:
                            import plotly.graph_objects as _go
                            cols = [c for c in df.columns if c.startswith("year")]
                            if cols and not df.empty:
                                row = df.iloc[0]
                                fig = _go.Figure(_go.Scatter(x=cols, y=[row.get(c) for c in cols], mode="lines+markers"))
                                fig.update_layout(title="US Treasury yield curve · latest",
                                                   margin=dict(l=10, r=10, t=40, b=10), height=260)
                                st.plotly_chart(fig, width="stretch", key="live_yc_chart")
                        except Exception:
                            pass

    # ------------------------------------------------------------------
    # 🟤 7. Strategy & Quant — ETF holdings (backtest is in main Decision tab)
    # ------------------------------------------------------------------
    with st.expander("🟤 Strategy & Quant  (ETF holdings · pointers to Strategy Lab)", expanded=False):
        if st.button("📡 Fetch", key="live_strat_btn") or auto:
            with st.spinner("Fetching ETF holdings…"):
                etf = fetcher.etf_holdings(symbol)
            st.markdown(_coverage_badges(etf["coverage"]), unsafe_allow_html=True)
            df = _provider_data_to_df(etf["by_provider"], prefer=["fmp", "finnhub"], symbol=symbol)
            if df is not None:
                st.dataframe(df.head(30), width="stretch", height=320)
            else:
                st.info("No ETF holdings — `{symbol}` may not be an ETF.".format(symbol=symbol))
            st.caption("⚠️ Backtest engine is in **Decision Engine ▸ Decision Signals** "
                        "(uses your local feature pipeline — no external API call).")

    # ------------------------------------------------------------------
    # ⚫ 8. Corporate Actions
    # ------------------------------------------------------------------
    with st.expander("⚫ Corporate Actions  (Splits · Dividends · M&A)", expanded=False):
        if st.button("📡 Fetch", key="live_ca_btn") or auto:
            with st.spinner("Fetching splits & corporate actions…"):
                ca = fetcher.corporate_actions(symbol)
            st.markdown(_coverage_badges(ca["coverage"]), unsafe_allow_html=True)
            for k, label in [("fmp_splits", "Splits · FMP"),
                              ("polygon_splits", "Splits · Polygon"),
                              ("yahoo_splits", "Splits · Yahoo")]:
                payload = ca["by_provider"].get(k, {})
                if payload.get("_ok") and payload.get("data"):
                    df = _df_safe(payload["data"])
                    if df is not None and not df.empty:
                        st.markdown(f"**{label}**")
                        st.dataframe(df.head(15), width="stretch", height=200)

    # ------------------------------------------------------------------
    # 🇸🇦 Sahm.sa Saudi market panel
    # ------------------------------------------------------------------
    with st.expander("🇸🇦 Sahm.sa  (Saudi market — TASI / Tadawul)", expanded=False):
        if st.button("📡 Fetch", key="live_sa_btn") or auto:
            with st.spinner("Fetching from Sahm.sa…"):
                sa = fetcher.saudi(symbol)
            st.markdown(_coverage_badges(sa["coverage"]), unsafe_allow_html=True)
            for sk, label in [("sahmk_quote", "Quote"), ("sahmk_company", "Company"),
                              ("sahmk_financials", "Financials"), ("sahmk_dividends", "Dividends"),
                              ("sahmk_news", "News")]:
                payload = sa["by_provider"].get(sk, {})
                if payload.get("_ok"):
                    st.markdown(f"**{label}**")
                    df = _df_safe(payload.get("data"))
                    if df is not None and not df.empty:
                        st.dataframe(df.head(10), width="stretch", height=200)
                    else:
                        st.json(payload.get("data"))

    # ------------------------------------------------------------------
    # ⚪ 9. Provider Raw Slice
    # ------------------------------------------------------------------
    with st.expander("⚪ Provider Raw Slice  (debug · all responses)", expanded=False):
        st.caption("Inspect every cached provider response from the fetches above.")
        debug_target = st.selectbox("Section", [
            "Income Statement", "Balance Sheet", "Cash Flow", "Earnings",
            "Key Metrics", "Dividends", "Institutional", "Insider",
            "Segments", "Estimates", "ESG", "Options", "Peers",
            "Sentiment", "Earnings Transcript", "Macro", "Yield Curve",
            "ETF Holdings", "Corporate Actions", "Saudi (Sahm.sa)",
        ], key="live_debug_select")
        if st.button("Fetch + show raw", key="live_debug_btn"):
            method_map = {
                "Income Statement": ("income_statement", (symbol,)),
                "Balance Sheet": ("balance_sheet", (symbol,)),
                "Cash Flow": ("cash_flow", (symbol,)),
                "Earnings": ("earnings", (symbol,)),
                "Key Metrics": ("key_metrics", (symbol,)),
                "Dividends": ("dividends", (symbol,)),
                "Institutional": ("institutional", (symbol,)),
                "Insider": ("insider_trading", (symbol,)),
                "Segments": ("segments", (symbol,)),
                "Estimates": ("estimates", (symbol,)),
                "ESG": ("esg", (symbol,)),
                "Options": ("options", (symbol,)),
                "Peers": ("peers", (symbol,)),
                "Sentiment": ("sentiment", (symbol,)),
                "Earnings Transcript": ("earnings_transcript", (symbol,)),
                "Macro": ("macro", ()),
                "Yield Curve": ("yield_curve", ()),
                "ETF Holdings": ("etf_holdings", (symbol,)),
                "Corporate Actions": ("corporate_actions", (symbol,)),
                "Saudi (Sahm.sa)": ("saudi", (symbol,)),
            }
            method_name, args = method_map[debug_target]
            with st.spinner(f"Fetching {debug_target}…"):
                payload = getattr(fetcher, method_name)(*args)
            st.json(payload)


def render_data_fusion_panel(bundle: dict[str, Any], key_prefix: str = "fusion") -> None:
    """Interactive field-level fusion lab for normalized provider metrics."""
    st.markdown("### Intelligent Data Fusion")
    c1, c2, c3 = st.columns([1.4, 1.1, 1])
    available_fields = sorted({
        field
        for payload in (bundle.get("providers", {}) if isinstance(bundle, dict) else {}).values()
        if isinstance(payload, dict)
        for field, value in (payload.get("metrics") or {}).items()
        if is_value_present(value)
    } | set(CORE_DIAGNOSTIC_FIELDS))
    default_field = "price" if "price" in available_fields else (available_fields[0] if available_fields else "price")
    field = c1.selectbox("Field", available_fields or ["price"], index=(available_fields or ["price"]).index(default_field), key=f"{key_prefix}_field")
    strategy = c2.selectbox(
        "Fusion strategy",
        ["confidence_best", "priority_first", "weighted_average", "median"],
        index=0,
        key=f"{key_prefix}_strategy",
    )
    fused = fuse_bundle_metric(bundle, field, strategy)
    raw_value = fused.get("value")
    source = fused.get("provider_attribution") or "—"
    confidence = float(fused.get("confidence") or 0.0)
    c3.metric("Confidence", f"{confidence:.0%}", source)

    f1, f2 = st.columns([1, 1.35])
    with f1:
        st.metric("Fused value", format_by_type(raw_value, "percent" if field.endswith(("pct", "margin", "yield")) or field in {"roe", "roa", "roic"} else "large_money" if field in {"market_cap", "revenue", "net_income", "free_cash_flow"} else "number"))
        if fused.get("warnings"):
            for warning in fused["warnings"]:
                st.caption(f"⚠️ {warning}")
    with f2:
        rows = [
            {
                "Provider": provider_label(provider),
                "Value": info.get("value"),
                "Confidence": info.get("confidence"),
            }
            for provider, info in (fused.get("all_provider_values") or {}).items()
        ]
        if rows:
            st.dataframe(pd.DataFrame(rows).sort_values("Confidence", ascending=False), width="stretch", hide_index=True, height=210)
        else:
            st.info("No provider value exists for this field.")


def _workspace_tab_label(tab: dict[str, Any]) -> str:
    return f"{str(tab.get('symbol') or '').upper()} · {tab.get('view', 'Overview')}"


def render_multi_symbol_workspace(bundles: dict[str, dict[str, Any]]) -> None:
    """Trading-style multi-symbol workspace.

    It renders only the active tab to keep the Streamlit run light. Every
    interactive widget is keyed by its tab id to prevent duplicate IDs and
    cross-tab state bleed.
    """
    if not bundles:
        return
    if "quant_workspace_tabs" not in st.session_state:
        first_symbol = next(iter(bundles.keys()))
        st.session_state["quant_workspace_tabs"] = [{
            "id": f"tab_{int(time.time() * 1000)}_0",
            "symbol": first_symbol,
            "view": "Overview",
        }]
        st.session_state["quant_workspace_active"] = st.session_state["quant_workspace_tabs"][0]["id"]

    tabs_state: list[dict[str, Any]] = st.session_state.get("quant_workspace_tabs", [])
    if not tabs_state:
        first_symbol = next(iter(bundles.keys()))
        tabs_state.append({"id": f"tab_{int(time.time() * 1000)}_0", "symbol": first_symbol, "view": "Overview"})
        st.session_state["quant_workspace_tabs"] = tabs_state
        st.session_state["quant_workspace_active"] = tabs_state[0]["id"]

    st.markdown("### Multi-Tab Workspace")
    top_a, top_b, top_c = st.columns([1.5, 0.5, 0.5])
    new_symbol = top_a.text_input("Open symbol", value="", placeholder="AAPL, MSFT, 2222.SR", key="workspace_new_symbol")
    if top_b.button("+", key="workspace_add_tab_btn", help="Add symbol workspace"):
        parsed_new = parse_symbols(new_symbol or next(iter(bundles.keys())))
        sym = parsed_new[0] if parsed_new else ""
        if sym:
            existing = {str(t.get("symbol", "")).upper() for t in tabs_state}
            if sym not in existing:
                tabs_state.append({"id": f"tab_{int(time.time() * 1000)}_{len(tabs_state)}", "symbol": sym, "view": "Overview"})
            st.session_state["quant_workspace_active"] = next(t["id"] for t in tabs_state if t["symbol"] == sym)
            if sym not in bundles:
                current = parse_symbols(st.session_state.get("symbols_text", ""))
                if sym not in current:
                    st.session_state["symbols_text"] = ",".join(current + [sym])
            st.rerun()
    if top_c.button("x", key="workspace_close_tab_btn", help="Close active workspace") and len(tabs_state) > 1:
        active_id = st.session_state.get("quant_workspace_active")
        st.session_state["quant_workspace_tabs"] = [t for t in tabs_state if t.get("id") != active_id]
        st.session_state["quant_workspace_active"] = st.session_state["quant_workspace_tabs"][0]["id"]
        st.rerun()

    ids = [str(t["id"]) for t in st.session_state["quant_workspace_tabs"]]
    active = st.session_state.get("quant_workspace_active")
    if active not in ids:
        active = ids[0]
    selected = st.radio(
        "Open workspaces",
        ids,
        horizontal=True,
        format_func=lambda tab_id: _workspace_tab_label(next((t for t in st.session_state["quant_workspace_tabs"] if t["id"] == tab_id), {})),
        key="workspace_active_radio",
        label_visibility="collapsed",
        index=ids.index(active),
    )
    st.session_state["quant_workspace_active"] = selected
    active_tab = next((t for t in st.session_state["quant_workspace_tabs"] if t["id"] == selected), st.session_state["quant_workspace_tabs"][0])
    tab_id = str(active_tab["id"])

    c1, c2 = st.columns([1, 1])
    parsed_symbol = parse_symbols(c1.text_input("Symbol", value=str(active_tab.get("symbol") or ""), key=f"{tab_id}_component_symbol"))
    symbol = parsed_symbol[0] if parsed_symbol else str(active_tab.get("symbol") or "").upper()
    view = c2.selectbox("View", ["Overview", "Chart", "Fundamentals", "Fusion", "Providers"], index=["Overview", "Chart", "Fundamentals", "Fusion", "Providers"].index(active_tab.get("view", "Overview")), key=f"{tab_id}_component_view")
    active_tab["symbol"] = symbol
    active_tab["view"] = view

    symbol_bundle = bundles.get(symbol)
    if not symbol_bundle:
        st.info(f"{symbol} is not loaded yet. It was added to the sidebar symbol list; the next rerun will fetch it.")
        return

    if view == "Overview":
        cols = st.columns(4)
        for i, metric_key in enumerate(["price", "final_ai_score", "trend_score", "risk_score"]):
            with cols[i]:
                render_metric_card(symbol_bundle, metric_key)
    elif view == "Chart":
        st.plotly_chart(history_chart(symbol_bundle), width="stretch", key=f"{tab_id}_component_chart")
    elif view == "Fundamentals":
        st.dataframe(metric_frame(symbol_bundle, ["Financials", "Valuation", "Quality"]).head(30), width="stretch", hide_index=True, height=360)
    elif view == "Fusion":
        render_data_fusion_panel(symbol_bundle, key_prefix=f"{tab_id}_component_fusion")
    elif view == "Providers":
        st.dataframe(provider_health_frame(symbol_bundle), width="stretch", hide_index=True, height=300)


# ============================================================
# Decision Intelligence — business / financial analysis only
# ------------------------------------------------------------
# This tab intentionally avoids technical indicators. It focuses on financial
# health, earnings quality, valuation, risks, provenance, and a cautious
# non-financial-advice decision summary.
# ============================================================
DECISION_INTEL_REQUIRED_FIELDS = [
    "revenue",
    "revenue_growth",
    "net_income",
    "operating_cash_flow",
    "free_cash_flow",
    "total_debt",
    "cash_and_equivalents",
    "gross_margin",
    "operating_margin",
    "profit_margin",
    "pe_ratio",
    "ps_ratio",
    "pb_ratio",
    "ev_ebitda",
]


def _di_metric(bundle: dict[str, Any], key: str) -> Any:
    return (bundle.get("metrics") or {}).get(key)


def _di_field_source(bundle: dict[str, Any], key: str) -> str:
    return provider_label((bundle.get("sources") or {}).get(key))


def _di_pct(value: Any, signed: bool = False) -> str:
    return format_pct(value, signed=signed)


def _di_required_data_quality(bundle: dict[str, Any]) -> dict[str, Any]:
    """Measure field coverage and provider health for the tab diagnostics."""
    metrics = bundle.get("metrics") or {}
    present = [field for field in DECISION_INTEL_REQUIRED_FIELDS if is_value_present(metrics.get(field))]
    missing = [field for field in DECISION_INTEL_REQUIRED_FIELDS if field not in present]
    providers = bundle.get("providers", {}) if isinstance(bundle, dict) else {}
    active_providers = []
    error_rows = []
    for provider, payload in providers.items():
        if not isinstance(payload, dict):
            continue
        if payload.get("status") in {"ok", "partial"}:
            active_providers.append(provider)
        if payload.get("summary_error") and payload.get("status") not in {"ok", "disabled"}:
            error_rows.append({
                "Provider": provider_label(provider),
                "Status": str(payload.get("status", "unknown")).title(),
                "Issue": payload.get("summary_error"),
            })
    confidence = len(present) / max(len(DECISION_INTEL_REQUIRED_FIELDS), 1)
    if active_providers:
        confidence = min(1.0, confidence + min(len(active_providers), 4) * 0.025)
    if error_rows:
        confidence = max(0.0, confidence - min(len(error_rows), 5) * 0.025)
    return {
        "confidence": round(confidence, 3),
        "present": present,
        "missing": missing,
        "active_providers": active_providers,
        "error_rows": error_rows,
    }


def _di_series_from_annuals(bundle: dict[str, Any], field: str) -> list[tuple[str, float]]:
    annuals = bundle.get("annuals") or []
    rows: list[tuple[str, float]] = []
    for row in annuals:
        if not isinstance(row, dict):
            continue
        year = str(row.get("year") or row.get("date") or row.get("fiscal_year") or "")
        value = to_num(row.get(field))
        if year and is_num(value):
            rows.append((year[:10], float(value)))
    rows.sort(key=lambda item: item[0])
    return rows


def _di_growth_from_series(rows: list[tuple[str, float]]) -> float | None:
    if len(rows) < 2:
        return None
    prev = rows[-2][1]
    latest = rows[-1][1]
    return safe_div(latest - prev, abs(prev))


def _di_metric_or_series_growth(bundle: dict[str, Any], field: str, growth_key: str | None = None) -> float | None:
    if growth_key and is_num(_di_metric(bundle, growth_key)):
        return float(_di_metric(bundle, growth_key))
    return _di_growth_from_series(_di_series_from_annuals(bundle, field))


def _di_margin_series(bundle: dict[str, Any]) -> pd.DataFrame:
    annuals = bundle.get("annuals") or []
    rows = []
    for row in annuals:
        if not isinstance(row, dict):
            continue
        year = str(row.get("year") or row.get("date") or row.get("fiscal_year") or "")
        revenue = to_num(row.get("revenue"))
        if not year or not is_num(revenue) or not revenue:
            continue
        rows.append({
            "Year": year[:10],
            "Gross Margin": safe_div(row.get("gross_profit"), revenue),
            "Operating Margin": safe_div(row.get("operating_income"), revenue),
            "Net Margin": safe_div(row.get("net_income"), revenue),
            "FCF Margin": safe_div(row.get("free_cash_flow"), revenue),
        })
    frame = pd.DataFrame(rows).sort_values("Year") if rows else pd.DataFrame()
    return frame


def _di_level(score: float | None) -> str:
    if not is_num(score):
        return "Unknown"
    if score >= 80:
        return "Low"
    if score >= 55:
        return "Medium"
    return "High"


def _di_level_color(level: str) -> str:
    return {
        "Low": "#16a34a",
        "Medium": "#d97706",
        "High": "#dc2626",
        "Unknown": "#64748b",
    }.get(level, "#64748b")


def _di_score_category(score: float | None) -> str:
    if not is_num(score):
        return "Neutral"
    if score >= 85:
        return "Excellent"
    if score >= 70:
        return "Good"
    if score >= 50:
        return "Neutral"
    if score >= 35:
        return "Weak"
    return "High Risk"


def _di_status_from_score(score: float | None, risk_count: int) -> str:
    if not is_num(score):
        return "Incomplete data"
    if score >= 75 and risk_count == 0:
        return "Stable / improving"
    if score >= 65:
        return "Generally stable"
    if score >= 50:
        return "Mixed picture"
    if risk_count >= 3:
        return "Risky / weakening"
    return "Needs monitoring"


def _di_business_strength(bundle: dict[str, Any]) -> dict[str, Any]:
    """Compute a 0-100 business strength score from fundamental inputs only."""
    metrics = bundle.get("metrics") or {}
    data_quality = _di_required_data_quality(bundle)
    revenue_growth = _di_metric_or_series_growth(bundle, "revenue", "revenue_growth")
    fcf_margin = safe_div(metrics.get("free_cash_flow"), metrics.get("revenue"))
    ocf_to_income = safe_div(metrics.get("operating_cash_flow"), abs(metrics.get("net_income")) if is_num(metrics.get("net_income")) else None)
    debt_to_equity = metrics.get("debt_to_equity")
    debt_to_cash = safe_div(metrics.get("total_debt"), metrics.get("cash_and_equivalents"))

    profitability = average_percent([
        score_range_value(metrics.get("gross_margin"), 0.40, 0.20),
        score_range_value(metrics.get("operating_margin"), 0.18, 0.06),
        score_range_value(metrics.get("profit_margin"), 0.12, 0.03),
        score_range_value(metrics.get("roe"), 0.16, 0.06),
    ])
    cash_flow_strength = average_percent([
        100.0 if is_num(metrics.get("free_cash_flow")) and metrics.get("free_cash_flow") > 0 else 30.0 if is_num(metrics.get("free_cash_flow")) else None,
        score_range_value(fcf_margin, 0.10, 0.03),
        score_range_value(ocf_to_income, 1.10, 0.75),
    ])
    debt_strength = average_percent([
        score_range_value(debt_to_equity, 0.60, 1.75, lower_is_better=True),
        score_range_value(debt_to_cash, 1.50, 4.00, lower_is_better=True),
    ])
    valuation_reasonableness = average_percent([
        _di_valuation_score(metrics.get("pe_ratio"), "pe"),
        _di_valuation_score(metrics.get("ps_ratio"), "ps"),
        _di_valuation_score(metrics.get("pb_ratio"), "pb"),
        _di_valuation_score(metrics.get("ev_ebitda"), "ev_ebitda"),
    ])
    margin_frame = _di_margin_series(bundle)
    margin_stability = None
    if not margin_frame.empty and "Operating Margin" in margin_frame and margin_frame["Operating Margin"].notna().sum() >= 2:
        latest = to_num(margin_frame["Operating Margin"].dropna().iloc[-1])
        prior = to_num(margin_frame["Operating Margin"].dropna().iloc[-2])
        margin_stability = score_range_value((latest - prior) if is_num(latest) and is_num(prior) else None, 0.00, -0.04)
    if not is_num(margin_stability):
        margin_stability = average_percent([
            score_range_value(metrics.get("operating_margin"), 0.18, 0.06),
            score_range_value(metrics.get("profit_margin"), 0.12, 0.03),
        ])

    score = weighted_average_percent([
        (score_range_value(revenue_growth, 0.12, 0.03), 1.15),
        (profitability, 1.30),
        (cash_flow_strength, 1.35),
        (debt_strength, 1.10),
        (margin_stability, 0.90),
        (valuation_reasonableness, 0.90),
        (data_quality["confidence"] * 100.0, 0.80),
    ])
    return {
        "score": round(score, 1) if is_num(score) else None,
        "category": _di_score_category(score),
        "components": {
            "Revenue growth": score_range_value(revenue_growth, 0.12, 0.03),
            "Profitability": profitability,
            "Cash flow strength": cash_flow_strength,
            "Debt risk control": debt_strength,
            "Margin stability": margin_stability,
            "Valuation reasonableness": valuation_reasonableness,
            "Data confidence": data_quality["confidence"] * 100.0,
        },
        "data_quality": data_quality,
    }


def _di_valuation_score(value: Any, kind: str) -> float | None:
    number = to_num(value)
    if not is_num(number):
        return None
    if number <= 0:
        return 35.0
    if kind == "pe":
        if 8 <= number <= 25:
            return 90.0
        if 25 < number <= 40 or 5 <= number < 8:
            return 60.0
        return 30.0
    if kind == "ps":
        if 0.5 <= number <= 6:
            return 85.0
        if 6 < number <= 10:
            return 55.0
        return 30.0
    if kind == "pb":
        if 0.7 <= number <= 5:
            return 85.0
        if 5 < number <= 9:
            return 55.0
        return 32.0
    if kind == "ev_ebitda":
        if 4 <= number <= 15:
            return 88.0
        if 15 < number <= 24:
            return 55.0
        return 32.0
    return None


def _di_risk_cards(bundle: dict[str, Any], confidence: float) -> list[dict[str, Any]]:
    metrics = bundle.get("metrics") or {}
    revenue_growth = _di_metric_or_series_growth(bundle, "revenue", "revenue_growth")
    fcf_margin = safe_div(metrics.get("free_cash_flow"), metrics.get("revenue"))
    ocf_to_income = safe_div(metrics.get("operating_cash_flow"), abs(metrics.get("net_income")) if is_num(metrics.get("net_income")) else None)
    debt_to_equity = metrics.get("debt_to_equity")
    debt_to_cash = safe_div(metrics.get("total_debt"), metrics.get("cash_and_equivalents"))
    margin_frame = _di_margin_series(bundle)
    margin_delta = None
    if not margin_frame.empty and margin_frame["Net Margin"].notna().sum() >= 2:
        vals = margin_frame["Net Margin"].dropna()
        margin_delta = vals.iloc[-1] - vals.iloc[-2]

    debt_score = average_percent([
        score_range_value(debt_to_equity, 0.60, 1.75, lower_is_better=True),
        score_range_value(debt_to_cash, 1.50, 4.00, lower_is_better=True),
    ])
    cash_score = average_percent([
        100.0 if is_num(metrics.get("free_cash_flow")) and metrics.get("free_cash_flow") > 0 else 25.0 if is_num(metrics.get("free_cash_flow")) else None,
        score_range_value(fcf_margin, 0.10, 0.03),
        score_range_value(ocf_to_income, 1.10, 0.75),
    ])
    margin_score = average_percent([
        score_range_value(metrics.get("operating_margin"), 0.18, 0.06),
        score_range_value(metrics.get("profit_margin"), 0.12, 0.03),
        score_range_value(margin_delta, 0.00, -0.04),
    ])
    revenue_score = score_range_value(revenue_growth, 0.12, 0.03)
    valuation_score = average_percent([
        _di_valuation_score(metrics.get("pe_ratio"), "pe"),
        _di_valuation_score(metrics.get("ps_ratio"), "ps"),
        _di_valuation_score(metrics.get("pb_ratio"), "pb"),
        _di_valuation_score(metrics.get("ev_ebitda"), "ev_ebitda"),
    ])
    data_score = confidence * 100.0
    return [
        {
            "title": "High debt risk",
            "level": _di_level(debt_score),
            "detail": f"Debt/equity: {format_multiple(debt_to_equity)} · Debt/cash: {format_multiple(debt_to_cash)}",
        },
        {
            "title": "Weak cash flow risk",
            "level": _di_level(cash_score),
            "detail": f"FCF margin: {_di_pct(fcf_margin)} · OCF / Net income: {format_multiple(ocf_to_income)}",
        },
        {
            "title": "Margin compression risk",
            "level": _di_level(margin_score),
            "detail": f"Operating margin: {_di_pct(metrics.get('operating_margin'))} · Net margin: {_di_pct(metrics.get('profit_margin'))}",
        },
        {
            "title": "Revenue slowdown risk",
            "level": _di_level(revenue_score),
            "detail": f"Revenue growth: {_di_pct(revenue_growth, signed=True)}",
        },
        {
            "title": "Valuation risk",
            "level": _di_level(valuation_score),
            "detail": f"P/E {format_multiple(metrics.get('pe_ratio'))} · P/S {format_multiple(metrics.get('ps_ratio'))} · P/B {format_multiple(metrics.get('pb_ratio'))}",
        },
        {
            "title": "Data quality risk",
            "level": _di_level(data_score),
            "detail": f"Fundamental coverage confidence: {data_score:.0f}%",
        },
    ]


def _di_stance(score: float | None, high_risks: int, confidence: float) -> str:
    if confidence < 0.45:
        return "Research More"
    if high_risks >= 4 or (is_num(score) and score < 35):
        return "Avoid"
    if is_num(score) and score >= 75 and high_risks <= 1:
        return "Hold"
    if is_num(score) and score >= 55:
        return "Watch"
    return "Attractive if fundamentals improve"


def _di_build_summary(bundle: dict[str, Any], strength: dict[str, Any], risk_cards: list[dict[str, Any]]) -> dict[str, Any]:
    metrics = bundle.get("metrics") or {}
    company = bundle.get("company_name") or bundle.get("symbol") or "The company"
    score = strength.get("score")
    high_risks = sum(1 for item in risk_cards if item["level"] == "High")
    confidence = strength["data_quality"]["confidence"]
    status = _di_status_from_score(score, high_risks)
    revenue_growth = _di_metric_or_series_growth(bundle, "revenue", "revenue_growth")
    fcf = metrics.get("free_cash_flow")
    profit_margin = metrics.get("profit_margin")
    debt_to_equity = metrics.get("debt_to_equity")

    positive = []
    negative = []
    monitor = []
    if is_num(revenue_growth) and revenue_growth > 0.03:
        positive.append(f"Revenue is growing at {_di_pct(revenue_growth, signed=True)}, which supports business momentum.")
    elif is_num(revenue_growth):
        negative.append(f"Revenue growth is weak or negative at {_di_pct(revenue_growth, signed=True)}.")
        monitor.append("Next revenue update and management guidance.")
    if is_num(fcf) and fcf > 0:
        positive.append("Free cash flow is positive, improving internal funding flexibility.")
    elif is_num(fcf):
        negative.append("Free cash flow is negative, which can increase reliance on cash reserves or external funding.")
        monitor.append("Operating cash flow conversion and capital spending.")
    if is_num(profit_margin) and profit_margin > 0.08:
        positive.append(f"Net margin is healthy at {_di_pct(profit_margin)}.")
    elif is_num(profit_margin):
        negative.append(f"Net margin is thin at {_di_pct(profit_margin)}.")
        monitor.append("Gross and operating margin stability.")
    if is_num(debt_to_equity) and debt_to_equity > 1.75:
        negative.append(f"Debt/equity is elevated at {format_multiple(debt_to_equity)}.")
        monitor.append("Debt maturities, interest costs, and liquidity position.")
    elif is_num(debt_to_equity):
        positive.append(f"Debt/equity is manageable at {format_multiple(debt_to_equity)}.")
    if confidence < 0.65:
        monitor.append("Missing provider fields and fallback data quality.")

    if not positive:
        positive.append("No major positive fundamental signal is confirmed by the available data.")
    if not negative:
        negative.append("No major fundamental red flag is confirmed by the available data.")
    if not monitor:
        monitor.append("Upcoming earnings, cash flow conversion, margins, and valuation multiples.")

    summary = (
        f"{company} currently screens as **{strength.get('category', 'Neutral')}** with a "
        f"Business Strength Score of **{format_score(score)}**. The business picture looks "
        f"**{status.lower()}** based on revenue quality, profitability, cash generation, "
        "balance-sheet risk, valuation reasonableness, and provider confidence."
    )
    return {
        "summary": summary,
        "status": status,
        "positive": positive[:4],
        "negative": negative[:4],
        "monitor": monitor[:5],
        "stance": _di_stance(score, high_risks, confidence),
    }


def _di_quality_of_earnings(bundle: dict[str, Any]) -> list[dict[str, str]]:
    metrics = bundle.get("metrics") or {}
    net_income = metrics.get("net_income")
    ocf = metrics.get("operating_cash_flow")
    revenue_growth = _di_metric_or_series_growth(bundle, "revenue", "revenue_growth")
    profit_margin = metrics.get("profit_margin")
    fcf = metrics.get("free_cash_flow")
    ocf_to_income = safe_div(ocf, abs(net_income) if is_num(net_income) else None)
    accrual_gap = safe_div((net_income - ocf) if is_num(net_income) and is_num(ocf) else None, abs(net_income) if is_num(net_income) else None)
    return [
        {
            "Area": "Net income vs operating cash flow",
            "Assessment": "Strong" if is_num(ocf_to_income) and ocf_to_income >= 1 else "Watch" if is_num(ocf_to_income) else "Missing",
            "Evidence": f"OCF / net income: {format_multiple(ocf_to_income)}",
        },
        {
            "Area": "Accrual risk",
            "Assessment": "Low" if is_num(accrual_gap) and accrual_gap <= 0.15 else "Elevated" if is_num(accrual_gap) else "Missing",
            "Evidence": f"Accrual gap: {_di_pct(accrual_gap)}",
        },
        {
            "Area": "Revenue growth quality",
            "Assessment": "Improving" if is_num(revenue_growth) and revenue_growth > 0.05 and is_num(fcf) and fcf > 0 else "Mixed" if is_num(revenue_growth) else "Missing",
            "Evidence": f"Revenue growth: {_di_pct(revenue_growth, signed=True)} · FCF: {format_large(fcf, bundle.get('currency', ''))}",
        },
        {
            "Area": "Profitability consistency",
            "Assessment": "Healthy" if is_num(profit_margin) and profit_margin >= 0.08 else "Thin" if is_num(profit_margin) else "Missing",
            "Evidence": f"Net margin: {_di_pct(profit_margin)}",
        },
    ]


def _di_fundamental_chart(bundle: dict[str, Any]) -> go.Figure:
    frame = pd.DataFrame(bundle.get("annuals", []) or [])
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    if frame.empty or "year" not in frame:
        fig.update_layout(template="plotly_dark", height=360, title="Fundamental trend unavailable")
        return fig
    frame = frame.sort_values("year").tail(6).copy()
    for col in ["revenue", "net_income", "operating_cash_flow", "free_cash_flow"]:
        if col in frame:
            frame[col] = frame[col].apply(lambda value: to_num(value) / 1e9 if is_num(to_num(value)) else None)
    fig.add_trace(go.Bar(x=frame["year"], y=frame.get("revenue"), name="Revenue (B)", marker_color="#38bdf8"), secondary_y=False)
    fig.add_trace(go.Bar(x=frame["year"], y=frame.get("free_cash_flow"), name="Free Cash Flow (B)", marker_color="#22c55e"), secondary_y=False)
    fig.add_trace(go.Scatter(x=frame["year"], y=frame.get("net_income"), name="Net Income (B)", mode="lines+markers", line={"color": "#facc15", "width": 3}), secondary_y=True)
    fig.update_layout(
        template="plotly_dark",
        height=360,
        margin={"l": 10, "r": 10, "t": 42, "b": 10},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(2,6,23,0.65)",
        title="Revenue, earnings, and free cash flow",
        legend={"orientation": "h", "y": 1.08, "x": 1, "xanchor": "right"},
    )
    fig.update_yaxes(title_text="Revenue / FCF (B)", secondary_y=False)
    fig.update_yaxes(title_text="Net income (B)", secondary_y=True)
    return fig


def _di_margin_chart(bundle: dict[str, Any]) -> go.Figure:
    frame = _di_margin_series(bundle).tail(6)
    fig = go.Figure()
    if frame.empty:
        fig.update_layout(template="plotly_dark", height=320, title="Margin trend unavailable")
        return fig
    for name, color in [
        ("Gross Margin", "#38bdf8"),
        ("Operating Margin", "#a78bfa"),
        ("Net Margin", "#facc15"),
        ("FCF Margin", "#22c55e"),
    ]:
        if name in frame:
            fig.add_trace(go.Scatter(x=frame["Year"], y=frame[name], mode="lines+markers", name=name, line={"color": color, "width": 3}))
    fig.update_layout(
        template="plotly_dark",
        height=320,
        margin={"l": 10, "r": 10, "t": 42, "b": 10},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(2,6,23,0.65)",
        title="Margin quality trend",
        yaxis_tickformat=".0%",
        legend={"orientation": "h", "y": 1.10, "x": 1, "xanchor": "right"},
    )
    return fig


def _di_debt_cash_chart(bundle: dict[str, Any]) -> go.Figure:
    metrics = bundle.get("metrics") or {}
    currency = bundle.get("currency") or ""
    labels = ["Total debt", "Cash & equivalents", "Operating cash flow", "Free cash flow"]
    values = [
        to_num(metrics.get("total_debt")),
        to_num(metrics.get("cash_and_equivalents")),
        to_num(metrics.get("operating_cash_flow")),
        to_num(metrics.get("free_cash_flow")),
    ]
    fig = go.Figure(go.Bar(
        x=labels,
        y=[v / 1e9 if is_num(v) else None for v in values],
        marker_color=["#ef4444", "#22c55e", "#38bdf8", "#facc15"],
    ))
    fig.update_layout(
        template="plotly_dark",
        height=320,
        margin={"l": 10, "r": 10, "t": 42, "b": 10},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(2,6,23,0.65)",
        title=f"Debt, cash, and cash flow ({currency or 'reported currency'} B)",
    )
    return fig


def _di_card_html(title: str, value: str, subtitle: str = "", color: str = "#38bdf8") -> str:
    return (
        "<div style='background:linear-gradient(145deg,#020617,#0f172a);"
        "border:1px solid rgba(148,163,184,.22);border-radius:12px;"
        "padding:1rem;min-height:118px;box-shadow:0 14px 28px rgba(2,6,23,.28);'>"
        f"<div style='font-size:.78rem;color:#94a3b8;text-transform:uppercase;letter-spacing:.04em;'>{escape(title)}</div>"
        f"<div style='font-size:1.55rem;font-weight:800;color:{color};margin-top:.35rem;'>{escape(value)}</div>"
        f"<div style='font-size:.9rem;color:#cbd5e1;margin-top:.35rem;line-height:1.45;'>{escape(subtitle)}</div>"
        "</div>"
    )


def render_decision_intelligence_tab(bundle: dict[str, Any], bundles: dict[str, dict[str, Any]]) -> None:
    """Independent AI/fundamental decision tab.

    No technical indicators are rendered here. All outputs are based on
    business fundamentals, valuation, risk, data quality, and provider
    provenance.
    """
    del bundles  # reserved for future peer-level fundamental comparison
    metrics = bundle.get("metrics") or {}
    currency = bundle.get("currency", "")
    symbol = str(bundle.get("symbol") or "").upper()
    company = bundle.get("company_name") or symbol or "Selected company"
    strength = _di_business_strength(bundle)
    risk_cards = _di_risk_cards(bundle, strength["data_quality"]["confidence"])
    insight = _di_build_summary(bundle, strength, risk_cards)
    high_risk_count = sum(1 for item in risk_cards if item["level"] == "High")

    st.markdown(
        """
        <style>
        .di-section-title {font-size:1.05rem;font-weight:800;margin:1.2rem 0 .6rem;color:#e2e8f0;}
        .di-note {color:#94a3b8;font-size:.9rem;line-height:1.55;}
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(
        f"""
        <div style="background:radial-gradient(circle at top left,rgba(56,189,248,.22),transparent 32%),
                    linear-gradient(135deg,#020617,#111827 58%,#0f172a);
                    border:1px solid rgba(148,163,184,.24);border-radius:16px;padding:1.25rem 1.35rem;
                    box-shadow:0 18px 44px rgba(2,6,23,.35);">
            <div style="display:flex;justify-content:space-between;gap:1rem;align-items:flex-start;flex-wrap:wrap;">
                <div>
                    <div style="color:#94a3b8;font-size:.82rem;text-transform:uppercase;letter-spacing:.08em;">Decision Intelligence</div>
                    <div style="font-size:1.85rem;font-weight:900;color:#f8fafc;margin-top:.2rem;">{escape(symbol)} · {escape(str(company))}</div>
                    <div style="color:#cbd5e1;max-width:900px;margin-top:.55rem;line-height:1.55;">{insight['summary']}</div>
                </div>
                <div style="min-width:180px;text-align:center;background:rgba(15,23,42,.82);border:1px solid rgba(148,163,184,.25);
                            border-radius:14px;padding:1rem;">
                    <div style="color:#94a3b8;font-size:.75rem;text-transform:uppercase;">Business Strength</div>
                    <div style="font-size:2.35rem;font-weight:900;color:#38bdf8;">{format_score(strength.get('score'))}</div>
                    <div style="font-weight:800;color:#e2e8f0;">{escape(strength.get('category') or 'Neutral')}</div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    top_cards = st.columns(4)
    top_cards[0].markdown(_di_card_html("Overall status", insight["status"], f"High-risk flags: {high_risk_count}", "#38bdf8"), unsafe_allow_html=True)
    top_cards[1].markdown(_di_card_html("Suggested stance", insight["stance"], "Not financial advice; use for research framing.", "#facc15"), unsafe_allow_html=True)
    top_cards[2].markdown(_di_card_html("Data confidence", f"{strength['data_quality']['confidence']:.0%}", f"{len(strength['data_quality']['present'])}/{len(DECISION_INTEL_REQUIRED_FIELDS)} fields available", "#22c55e"), unsafe_allow_html=True)
    top_cards[3].markdown(_di_card_html("Valuation snapshot", format_multiple(metrics.get("pe_ratio")), f"P/S {format_multiple(metrics.get('ps_ratio'))} · P/B {format_multiple(metrics.get('pb_ratio'))}", "#a78bfa"), unsafe_allow_html=True)

    st.markdown('<div class="di-section-title">Financial Health Analysis</div>', unsafe_allow_html=True)
    health_cols = st.columns(4)
    health_cards = [
        ("Revenue", format_large(metrics.get("revenue"), currency), f"Growth {_di_pct(_di_metric_or_series_growth(bundle, 'revenue', 'revenue_growth'), signed=True)}", "#38bdf8"),
        ("Net income", format_large(metrics.get("net_income"), currency), f"Margin {_di_pct(metrics.get('profit_margin'))}", "#facc15"),
        ("Operating cash flow", format_large(metrics.get("operating_cash_flow"), currency), f"Source: {_di_field_source(bundle, 'operating_cash_flow')}", "#22c55e"),
        ("Free cash flow", format_large(metrics.get("free_cash_flow"), currency), f"FCF margin {_di_pct(safe_div(metrics.get('free_cash_flow'), metrics.get('revenue')))}", "#22c55e"),
        ("Total debt", format_large(metrics.get("total_debt"), currency), f"D/E {format_multiple(metrics.get('debt_to_equity'))}", "#fb7185"),
        ("Cash position", format_large(metrics.get("cash_and_equivalents"), currency), f"Debt/cash {format_multiple(safe_div(metrics.get('total_debt'), metrics.get('cash_and_equivalents')))}", "#38bdf8"),
        ("Operating margin", _di_pct(metrics.get("operating_margin")), f"Gross {_di_pct(metrics.get('gross_margin'))}", "#a78bfa"),
        ("Net margin", _di_pct(metrics.get("profit_margin")), f"Source: {_di_field_source(bundle, 'profit_margin')}", "#facc15"),
    ]
    for idx, card in enumerate(health_cards):
        with health_cols[idx % 4]:
            st.markdown(_di_card_html(*card), unsafe_allow_html=True)

    chart_left, chart_right = st.columns([1.25, 1])
    with chart_left:
        st.plotly_chart(_di_fundamental_chart(bundle), width="stretch", key=f"di_fundamental_chart_{symbol}")
    with chart_right:
        st.plotly_chart(_di_margin_chart(bundle), width="stretch", key=f"di_margin_chart_{symbol}")
    st.plotly_chart(_di_debt_cash_chart(bundle), width="stretch", key=f"di_debt_cash_chart_{symbol}")

    q_col, score_col = st.columns([1.15, 1])
    with q_col:
        st.markdown('<div class="di-section-title">Quality of Earnings</div>', unsafe_allow_html=True)
        st.dataframe(pd.DataFrame(_di_quality_of_earnings(bundle)), width="stretch", hide_index=True, height=220)
    with score_col:
        st.markdown('<div class="di-section-title">Business Strength Components</div>', unsafe_allow_html=True)
        component_rows = [
            {"Component": name, "Score": round(value, 1) if is_num(value) else None}
            for name, value in (strength.get("components") or {}).items()
        ]
        comp_df = pd.DataFrame(component_rows)
        if not comp_df.empty:
            st.dataframe(comp_df, width="stretch", hide_index=True, height=220)
        else:
            st.info("Score components are unavailable because the selected stock has limited fundamental data.")

    st.markdown('<div class="di-section-title">Risk Dashboard</div>', unsafe_allow_html=True)
    risk_cols = st.columns(3)
    for idx, risk in enumerate(risk_cards):
        color = _di_level_color(risk["level"])
        with risk_cols[idx % 3]:
            st.markdown(
                _di_card_html(risk["title"], risk["level"], risk["detail"], color),
                unsafe_allow_html=True,
            )

    st.markdown('<div class="di-section-title">Valuation Overview</div>', unsafe_allow_html=True)
    valuation_rows = [
        {"Metric": "P/E ratio", "Value": format_multiple(metrics.get("pe_ratio")), "Source": _di_field_source(bundle, "pe_ratio"), "Assessment": status_text("pe_ratio", metrics.get("pe_ratio"))},
        {"Metric": "P/S ratio", "Value": format_multiple(metrics.get("ps_ratio")), "Source": _di_field_source(bundle, "ps_ratio"), "Assessment": status_text("ps_ratio", metrics.get("ps_ratio"))},
        {"Metric": "P/B ratio", "Value": format_multiple(metrics.get("pb_ratio")), "Source": _di_field_source(bundle, "pb_ratio"), "Assessment": status_text("pb_ratio", metrics.get("pb_ratio"))},
        {"Metric": "EV/EBITDA", "Value": format_multiple(metrics.get("ev_ebitda")), "Source": _di_field_source(bundle, "ev_ebitda"), "Assessment": "Reasonable" if is_num(_di_valuation_score(metrics.get("ev_ebitda"), "ev_ebitda")) and _di_valuation_score(metrics.get("ev_ebitda"), "ev_ebitda") >= 55 else "Elevated / missing"},
    ]
    st.dataframe(pd.DataFrame(valuation_rows), width="stretch", hide_index=True, height=190)
    st.caption("Historical valuation comparison is shown only when provider data exposes comparable historical multiples; otherwise the table uses current normalized provider values.")

    st.markdown('<div class="di-section-title">AI Recommendation Summary</div>', unsafe_allow_html=True)
    ai_cols = st.columns(4)
    blocks = [
        ("Positive points", insight["positive"]),
        ("Negative points", insight["negative"]),
        ("Key risks", [f"{item['title']}: {item['level']}" for item in risk_cards if item["level"] in {"High", "Medium"}][:5] or ["No major risk flag confirmed by available data."]),
        ("What to monitor next", insight["monitor"]),
    ]
    for idx, (title, items) in enumerate(blocks):
        with ai_cols[idx]:
            st.markdown(
                "<div style='background:#020617;border:1px solid rgba(148,163,184,.22);"
                "border-radius:12px;padding:1rem;min-height:260px;'>"
                f"<div style='font-weight:850;color:#e2e8f0;margin-bottom:.55rem;'>{escape(title)}</div>"
                + "".join(f"<div style='color:#cbd5e1;margin:.45rem 0;line-height:1.45;'>• {escape(str(item))}</div>" for item in items)
                + "</div>",
                unsafe_allow_html=True,
            )
    st.info(f"Suggested stance: **{insight['stance']}**. This is a research-oriented view, not direct buy/sell financial advice.")

    st.markdown('<div class="di-section-title">Data Diagnostics</div>', unsafe_allow_html=True)
    diag = strength["data_quality"]
    d1, d2 = st.columns([1, 1])
    with d1:
        diagnostics_rows = [
            {"Item": "Providers used", "Value": ", ".join(provider_label(p) for p in diag["active_providers"]) or "No active provider"},
            {"Item": "Missing fields", "Value": ", ".join(diag["missing"]) if diag["missing"] else "None"},
            {"Item": "Confidence score", "Value": f"{diag['confidence']:.0%}"},
            {"Item": "Last update time", "Value": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")},
            {"Item": "Annuals provider", "Value": provider_label(bundle.get("annuals_provider"))},
            {"Item": "History provider", "Value": provider_label(bundle.get("history_provider"))},
        ]
        st.dataframe(pd.DataFrame(diagnostics_rows), width="stretch", hide_index=True, height=250)
    with d2:
        if diag["error_rows"]:
            st.dataframe(pd.DataFrame(diag["error_rows"]), width="stretch", hide_index=True, height=250)
        else:
            st.success("No provider fallback errors are currently reported for the fields used by this tab.")


def render_decision_tab(bundle: dict[str, Any], bundles: dict[str, dict[str, Any]]) -> None:
    left, right = st.columns([1.25, 1])
    with left:
        st.markdown("### Decision Signals")
        st.dataframe(technical_signal_frame(bundle), width="stretch", hide_index=True, height=560)
    with right:
        st.markdown("### Smart Alerts")
        st.dataframe(alert_frame(bundle), width="stretch", hide_index=True, height=260)
        st.markdown("### Factor Scores")
        st.dataframe(metric_frame(bundle, ["Summary", "Trend", "Momentum", "Volume", "Risk", "Relative Strength"]), width="stretch", hide_index=True, height=260)
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.plotly_chart(decision_heatmap_chart(bundles), width="stretch", key="decision_heatmap")
    st.markdown("</div>", unsafe_allow_html=True)
    st.markdown("---")
    render_multi_symbol_workspace(bundles)
    st.markdown("---")
    render_data_fusion_panel(bundle, key_prefix="decision_fusion")
    render_financial_translation_lab("decision_translation")
    if len(bundles) > 1:
        st.markdown("### Screening Leaderboard")
        st.dataframe(comparison_leaderboard_frame(bundles), width="stretch", hide_index=True)
    # Bloomberg-style equity research dashboard (28-section)
    st.markdown("---")
    render_equity_decision_dashboard(bundle, bundles)
    # Live multi-provider dashboard (real API calls)
    st.markdown("---")
    render_live_data_dashboard(bundle, st.session_state.get("ibkr_keys_snapshot") or {})


def render_xy_matrix_tab(bundles: dict[str, dict[str, Any]]) -> None:
    st.markdown("### Custom X vs Y Comparison")
    metric_labels = list(COMPARISON_METRIC_OPTIONS.keys())
    control_a, control_b, control_c = st.columns([1, 1, 1])
    with control_a:
        x_axis = st.selectbox("X Axis Metric", metric_labels, index=metric_labels.index("P/E Ratio"), key="xy_custom_x_axis")
    with control_b:
        y_axis = st.selectbox("Y Axis Metric", metric_labels, index=metric_labels.index("ROE"), key="xy_custom_y_axis")
    with control_c:
        lookback_days = st.selectbox("Performance Window", [90, 180, 365, 730, 1095], index=2, format_func=lambda value: f"{value} days", key="xy_perf_window")

    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.plotly_chart(xy_custom_scatter_chart(bundles, x_axis, y_axis), width="stretch", key="xy_custom_scatter")
    st.markdown("</div>", unsafe_allow_html=True)
    st.caption("The scatter uses live provider metrics. Market cap controls bubble size when available.")

    st.markdown("### Core Comparison Charts")
    chart_pairs = [
        (
            normalized_price_performance_chart(bundles, int(lookback_days)),
            pe_ratio_comparison_chart(bundles),
            "xy_normalized_perf",
            "xy_pe_ratio",
        ),
        (
            side_by_side_financials_chart(bundles),
            profitability_radar_chart(bundles),
            "xy_financials_side_by_side",
            "xy_profitability_radar",
        ),
        (
            roe_roa_comparison_chart(bundles),
            free_cash_flow_trend_chart(bundles),
            "xy_roe_roa",
            "xy_fcf_trend",
        ),
        (
            debt_equity_stacked_chart(bundles),
            growth_matrix_chart(bundles),
            "xy_debt_equity",
            "xy_growth_matrix",
        ),
        (
            dividend_comparison_chart(bundles),
            market_cap_bubble_chart(bundles),
            "xy_dividend",
            "xy_market_cap_bubble",
        ),
    ]
    for left_fig, right_fig, left_key, right_key in chart_pairs:
        left, right = st.columns(2)
        with left:
            st.markdown('<div class="panel">', unsafe_allow_html=True)
            st.plotly_chart(left_fig, width="stretch", key=left_key)
            st.markdown("</div>", unsafe_allow_html=True)
        with right:
            st.markdown('<div class="panel">', unsafe_allow_html=True)
            st.plotly_chart(right_fig, width="stretch", key=right_key)
            st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("### Comparison Data Grid")
    st.dataframe(comparison_xy_frame(bundles), width="stretch", hide_index=True, height=520)
    st.markdown("### Factor Leaderboard")
    st.dataframe(comparison_leaderboard_frame(bundles), width="stretch", hide_index=True)


def render_binance_hero(bundle: dict[str, Any], controls: BinanceControls) -> None:
    metrics = bundle["metrics"]
    st.markdown(
        f"""
        <div class="hero">
            <div class="hero-kicker">Binance Spot Microstructure Lab</div>
            <div class="hero-title">{bundle['symbol']} · Bookmap-like Dashboard</div>
            <div class="hero-subtitle">
                REST snapshots, trade flow, depth ladder, kline context, and generated WebSocket streams in one Streamlit control room.
            </div>
            <div class="hero-strip">
                <div class="hero-pill">Price: {format_crypto_price(metrics.get('price'))}</div>
                <div class="hero-pill">24H: {format_pct(metrics.get('price_change_pct'), signed=True)}</div>
                <div class="hero-pill">Spread: {format_pct(metrics.get('spread_pct'))}</div>
                <div class="hero-pill">Buy Pressure: {format_pct(metrics.get('buy_pressure'))}</div>
                <div class="hero-pill">Bookmap Score: {format_score(metrics.get('bookmap_score'))}</div>
                <div class="hero-pill">Refresh: {controls.refresh_seconds}s</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_binance_kpis(bundle: dict[str, Any]) -> None:
    metrics = bundle["metrics"]
    top = st.columns(4)
    top[0].metric("Last Price", format_crypto_price(metrics.get("price")), format_pct(metrics.get("price_change_pct"), signed=True))
    top[1].metric("Best Bid / Ask", f"{format_crypto_price(metrics.get('bid'))} / {format_crypto_price(metrics.get('ask'))}", format_pct(metrics.get("spread_pct")))
    top[2].metric("24H Quote Volume", format_crypto_quote(metrics.get("quote_volume_24h")), f"{format_by_type(metrics.get('trade_count_24h'), 'integer')} trades")
    top[3].metric("Bookmap Score", format_score(metrics.get("bookmap_score")), f"Liquidity {format_score(metrics.get('liquidity_score'))}")

    lower = st.columns(4)
    lower[0].metric("Depth Bid Notional", format_crypto_quote(metrics.get("bid_depth_notional")), f"Imb {format_pct(metrics.get('book_imbalance'), signed=True)}")
    lower[1].metric("Depth Ask Notional", format_crypto_quote(metrics.get("ask_depth_notional")), f"Levels {format_by_type(metrics.get('depth_levels'), 'integer')}")
    lower[2].metric("Buy Pressure", format_pct(metrics.get("buy_pressure")), f"VWAP {format_crypto_price(metrics.get('trade_vwap'))}")
    lower[3].metric("ATR %", format_pct(metrics.get("tech_atr_pct")), f"Risk {format_score(metrics.get('tech_risk_score'))}")


def render_binance_overview_tab(bundle: dict[str, Any], bundles: dict[str, dict[str, Any]]) -> None:
    left, right = st.columns([1.45, 1])
    with left:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.plotly_chart(binance_candlestick_chart(bundle), width="stretch", key=f"binance_candles_{bundle['symbol']}")
        st.markdown("</div>", unsafe_allow_html=True)
    with right:
        st.markdown("### Market Microstructure")
        st.dataframe(binance_metric_frame(bundle), width="stretch", hide_index=True, height=430)
    if len(bundles) > 1:
        st.markdown("### Multi-Symbol Comparison")
        st.dataframe(binance_comparison_frame(bundles), width="stretch", hide_index=True)
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.plotly_chart(binance_multi_score_chart(bundles), width="stretch", key="binance_multi_score")
        st.markdown("</div>", unsafe_allow_html=True)


def render_binance_orderbook_tab(bundle: dict[str, Any]) -> None:
    left, right = st.columns([1.35, 1])
    with left:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.plotly_chart(binance_order_book_chart(bundle), width="stretch", key=f"binance_depth_{bundle['symbol']}")
        st.markdown("</div>", unsafe_allow_html=True)
    with right:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.plotly_chart(binance_score_radar_chart(bundle), width="stretch", key=f"binance_radar_{bundle['symbol']}")
        st.markdown("</div>", unsafe_allow_html=True)
    st.markdown("### Local Order Book Procedure")
    st.dataframe(pd.DataFrame({"Step": LOCAL_ORDER_BOOK_STEPS}), width="stretch", hide_index=True)
    if not bundle.get("depth", pd.DataFrame()).empty:
        st.markdown("### Visible Depth Levels")
        st.dataframe(bundle["depth"], width="stretch", hide_index=True, height=360)


def render_binance_trades_tab(bundle: dict[str, Any]) -> None:
    left, right = st.columns([1.45, 1])
    with left:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.plotly_chart(binance_trades_chart(bundle), width="stretch", key=f"binance_trades_{bundle['symbol']}")
        st.markdown("</div>", unsafe_allow_html=True)
    with right:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.plotly_chart(binance_flow_chart(bundle), width="stretch", key=f"binance_flow_{bundle['symbol']}")
        st.markdown("</div>", unsafe_allow_html=True)
    if not bundle.get("trades", pd.DataFrame()).empty:
        st.markdown("### Recent Time & Sales")
        trades = bundle["trades"].tail(200).copy()
        trades["time"] = trades["time"].astype(str)
        st.dataframe(trades, width="stretch", hide_index=True, height=420)


def render_binance_api_tab(bundle: dict[str, Any], controls: BinanceControls) -> None:
    st.markdown("### Generated WebSocket Streams")
    st.dataframe(binance_stream_frame(bundle), width="stretch", hide_index=True, height=280)
    st.markdown("### REST Endpoints")
    st.dataframe(binance_url_frame(bundle), width="stretch", hide_index=True, height=360)
    left, right = st.columns(2)
    with left:
        st.markdown("### Required Streams")
        st.dataframe(pd.DataFrame(BINANCE_REQUIRED_STREAMS_FOR_BOOKMAP.items(), columns=["Stream", "Purpose"]), width="stretch", hide_index=True)
        st.markdown("### Binance Warnings")
        st.dataframe(pd.DataFrame({"Warning": BINANCE_WARNINGS}), width="stretch", hide_index=True, height=310)
    with right:
        st.markdown("### Payload Field Maps")
        st.dataframe(binance_field_map_frame(), width="stretch", hide_index=True, height=520)

    if controls.enable_ws_probe:
        with st.spinner("Opening a short Binance WebSocket probe..."):
            probe = collect_binance_ws_probe(bundle["symbol"], controls)
        st.markdown("### Live WebSocket Probe")
        if probe.get("ok"):
            probe_rows = [
                {
                    "Stream": item.get("stream") or "—",
                    "Event": item.get("event") or "—",
                    "Preview": json.dumps(item.get("payload"), ensure_ascii=False, default=str)[:500],
                }
                for item in probe.get("messages", [])
            ]
            st.dataframe(pd.DataFrame(probe_rows), width="stretch", hide_index=True, height=260)
        else:
            st.warning(probe.get("error") or "WebSocket probe failed.")


def render_binance_plan_tab(bundle: dict[str, Any]) -> None:
    rows = []
    for stream_name, values in BOOKMAP_DASHBOARD_PLAN.items():
        for item in values:
            rows.append({"Stream": stream_name, "Dashboard Role": item})
    st.markdown("### Bookmap Dashboard Plan")
    st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
    st.markdown("### Raw Diagnostics")
    if bundle.get("errors"):
        st.dataframe(pd.DataFrame(bundle["errors"].items(), columns=["Endpoint", "Error"]), width="stretch", hide_index=True)
    else:
        st.success("All requested Binance REST endpoints returned successfully.")
    with st.expander("Raw Binance Payloads", expanded=False):
        st.json(bundle.get("raw", {}))


def render_binance_export_tab(bundle: dict[str, Any], bundles: dict[str, dict[str, Any]]) -> None:
    metrics_df = binance_metric_frame(bundle)
    comparison_df = binance_comparison_frame(bundles)
    depth_df = bundle.get("depth", pd.DataFrame())
    trades_df = bundle.get("trades", pd.DataFrame())
    raw_json = json.dumps({"primary_symbol": bundle["symbol"], "bundles": {key: {"metrics": value["metrics"], "urls": value["urls"], "streams": value["streams"]} for key, value in bundles.items()}}, ensure_ascii=False, indent=2, default=str)
    col_a, col_b, col_c, col_d = st.columns(4)
    col_a.download_button("Download metrics CSV", metrics_df.to_csv(index=False).encode("utf-8"), file_name=f"{bundle['symbol'].lower()}_binance_metrics.csv", mime="text/csv", width="stretch")
    col_b.download_button("Download comparison CSV", comparison_df.to_csv(index=False).encode("utf-8"), file_name="binance_comparison.csv", mime="text/csv", width="stretch")
    col_c.download_button("Download depth CSV", depth_df.to_csv(index=False).encode("utf-8"), file_name=f"{bundle['symbol'].lower()}_depth.csv", mime="text/csv", width="stretch")
    col_d.download_button("Download raw JSON", raw_json.encode("utf-8"), file_name="binance_dashboard_raw.json", mime="application/json", width="stretch")
    st.markdown("### Export Preview")
    st.dataframe(metrics_df, width="stretch", hide_index=True)
    if not trades_df.empty:
        st.markdown("### Trades Export Preview")
        st.dataframe(trades_df.tail(100), width="stretch", hide_index=True)


def render_binance_dashboard_app(controls: BinanceControls) -> None:
    if not controls.symbols:
        st.warning("Enter at least one Binance spot symbol.")
        st.stop()
    with st.spinner("Loading Binance REST snapshots, depth, trades, klines, and stream references..."):
        bundles = fetch_binance_dashboard(controls)
    primary_bundle = bundles.get(controls.primary_symbol) or next(iter(bundles.values()))
    render_binance_hero(primary_bundle, controls)
    render_binance_kpis(primary_bundle)
    if primary_bundle.get("errors"):
        st.warning("Some Binance endpoints returned errors. Open API / Diagnostics for details.")
    tabs = st.tabs(["Overview", "Order Book", "Trades", "API / WebSocket", "Plan & Diagnostics", "Export"])
    with tabs[0]:
        render_binance_overview_tab(primary_bundle, bundles)
    with tabs[1]:
        render_binance_orderbook_tab(primary_bundle)
    with tabs[2]:
        render_binance_trades_tab(primary_bundle)
    with tabs[3]:
        render_binance_api_tab(primary_bundle, controls)
    with tabs[4]:
        render_binance_plan_tab(primary_bundle)
    with tabs[5]:
        render_binance_export_tab(primary_bundle, bundles)


def sentiment_badge_html(sentiment: float | None) -> str:
    """Colored sentiment pill + bar."""
    if sentiment is None or not isinstance(sentiment, (int, float)):
        return ""
    s = float(sentiment)
    if s > 0.1:
        klass, label, bar_klass = "pos", f"+{s:.2f}", "sentiment-positive"
    elif s < -0.1:
        klass, label, bar_klass = "neg", f"{s:.2f}", "sentiment-negative"
    else:
        klass, label, bar_klass = "neu", f"{s:+.2f}", "sentiment-neutral"
    return (
        f'<span class="news-sentiment-pill {klass}">{label}</span>'
        f'<div class="news-sentiment-bar {bar_klass}"></div>'
    )


def render_news_card_html(item: dict) -> str:
    """Render one normalized news item as a polished HTML card."""
    headline = escape(str(item.get("headline") or "Untitled"))
    url = item.get("url") or "#"
    source = escape(str(item.get("source") or "Unknown"))
    category = item.get("category")
    summary = item.get("summary")
    provider_tag = item.get("provider_tag") or ""
    is_sec = provider_tag == "sec_api"
    fuse_n = int(item.get("source_count") or 0)
    sentiment = item.get("sentiment")

    ts = item.get("datetime")
    if isinstance(ts, (int, float)) and ts > 0:
        try:
            dt_str = datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%b %d, %Y · %H:%M")
        except Exception:
            dt_str = "—"
    else:
        dt_str = "—"

    # bar of pills
    pills: list[str] = []
    pills.append(
        f'<span class="news-provider-pill">{escape(NEWS_PROVIDER_LABELS.get(provider_tag, provider_tag or "News"))}</span>'
    )
    if fuse_n > 1:
        pills.append(f'<span class="news-fuse-pill">⚡ Fused × {fuse_n}</span>')
    if category:
        pills.append(f'<span class="news-cat-pill">{escape(str(category))}</span>')

    sent_html = ""
    if isinstance(sentiment, (int, float)):
        sent_html = sentiment_badge_html(sentiment)

    summary_html = ""
    if summary:
        summary_html = f'<div class="news-card-summary">{escape(str(summary))}</div>'

    card_class = "news-card" + (" sec-filing-card" if is_sec else "")
    return (
        f'<div class="{card_class}">'
        f'<div class="news-card-header">'
        f'<div class="news-card-headline"><a href="{escape(url)}" target="_blank" rel="noopener">{headline}</a></div>'
        f'<div class="news-card-meta">{dt_str}</div>'
        f'</div>'
        f'<div class="news-card-source">{source}</div>'
        f'<div>{"".join(pills)}</div>'
        f'{summary_html}'
        f'{sent_html}'
        f'</div>'
    )


# ============================================================
# Interactive Brokers (IBKR) Client Portal API — all-in-one
# ============================================================
# Requires IBKR Client Portal Gateway running on https://localhost:5000
# Sign-in once via the gateway UI before using the endpoints below.
# Rate limits (per IBKR docs): ~10 req/sec global; /iserver/orders &
# /iserver/trades are once per 5 seconds.

# ============================================================
# Sahmk.sa — Saudi Stocks Data API
# ============================================================
SAHMK_DEFAULT_BASE_URL = "https://api.sahmk.sa/v1"
SAHMK_DEFAULT_API_KEY = "shmk_live_966701843fba7a680e84c57387a8dbb4d3cd7bed8549faaa"

class SahmkClient:
    """Client for the Saudi market data service (sahmk.sa).

    Endpoints follow the common REST pattern used by Saudi data vendors.
    All methods return a uniform `{"_ok": bool, "_status": int, "data": ...}`
    dict so failures never crash the Streamlit page.
    """

    def __init__(
        self,
        api_key: str = SAHMK_DEFAULT_API_KEY,
        base_url: str = SAHMK_DEFAULT_BASE_URL,
        timeout: float = 8.0,
    ):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        # sahmk.sa accepts either a Bearer token or an X-API-Key header
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "X-API-Key": api_key,
            "Accept": "application/json",
            "User-Agent": "Atlas-Terminal/1.0",
        })

    def _safe(self, method: str, path: str, **kwargs) -> dict:
        try:
            r = self.session.request(
                method,
                f"{self.base_url}{path}",
                timeout=self.timeout,
                **kwargs,
            )
            try:
                data = r.json()
            except Exception:
                data = {"raw": r.text[:2000]}
            return {"_ok": r.ok, "_status": r.status_code, "data": data}
        except requests.exceptions.RequestException as exc:
            return {
                "_ok": False,
                "_status": 0,
                "_error": "connection",
                "_error_detail": str(exc),
                "data": None,
            }

    def quote(self, symbol: str) -> dict:
        """Latest quote for a Saudi stock (e.g. '2222' = Aramco)."""
        return self._safe("GET", f"/quote/{symbol}")

    def history(self, symbol: str, period: str = "1y", interval: str = "1d") -> dict:
        return self._safe("GET", f"/history/{symbol}", params={"period": period, "interval": interval})

    def search(self, query: str) -> dict:
        return self._safe("GET", "/search", params={"q": query})

    def companies(self) -> dict:
        return self._safe("GET", "/companies")

    def company(self, symbol: str) -> dict:
        return self._safe("GET", f"/companies/{symbol}")

    def markets(self) -> dict:
        return self._safe("GET", "/markets")

    def news(self, symbol: str | None = None, limit: int = 20) -> dict:
        path = f"/news/{symbol}" if symbol else "/news"
        return self._safe("GET", path, params={"limit": limit})

    def financials(self, symbol: str) -> dict:
        return self._safe("GET", f"/financials/{symbol}")

    def dividends(self, symbol: str) -> dict:
        return self._safe("GET", f"/dividends/{symbol}")

    def index_quote(self, index: str = "TASI") -> dict:
        """TASI / NOMU / etc."""
        return self._safe("GET", f"/index/{index}")


@st.cache_resource
def get_sahmk_client(api_key: str = SAHMK_DEFAULT_API_KEY,
                     base_url: str = SAHMK_DEFAULT_BASE_URL) -> SahmkClient:
    return SahmkClient(api_key=api_key, base_url=base_url)


# Add Sahmk to the news provider labels so it integrates with the news tab
try:
    NEWS_PROVIDER_LABELS["sahmk"] = "Sahm.sa"
except Exception:
    pass


# ============================================================
# ib_insync — TWS / IB Gateway transport (port 4002 paper, 4001 live)
# ============================================================
# This is an ALTERNATIVE to the Client Portal REST API (5000):
#   • TWS / Gateway use a binary socket protocol on port 4001/4002
#   • ib_insync wraps it with an asyncio-friendly API
# Loaded lazily so the app keeps running even if ib_insync isn't installed.

IB_INSYNC_DEFAULT_HOST = "127.0.0.1"
IB_INSYNC_DEFAULT_PAPER_PORT = 4002
IB_INSYNC_DEFAULT_LIVE_PORT = 4001
IB_INSYNC_DEFAULT_CLIENT_ID = 1


def ib_insync_available() -> tuple[bool, str]:
    """Return (installed, message). Doesn't connect — just imports."""
    try:
        import ib_insync  # noqa: F401
        return True, getattr(ib_insync, "__version__", "?")
    except ImportError as exc:
        return False, f"ib_insync not installed — `pip install ib_insync`. ({exc})"
    except Exception as exc:
        return False, f"ib_insync import failed: {exc}"


def is_tcp_port_listening(host: str, port: int, timeout: float = 0.35) -> bool:
    """Fast local TCP probe used before expensive IBKR connection attempts."""
    try:
        with socket.create_connection((host, int(port)), timeout=float(timeout)):
            return True
    except OSError:
        return False


def restore_asyncio_native_tasks_for_py314() -> None:
    """Undo the part of nest_asyncio that breaks Python 3.14 timeouts.

    nest_asyncio swaps asyncio's C Task/Future with pure-Python versions. On
    Python 3.14 that can make `asyncio.current_task()` return None while a task
    is running, which then breaks `asyncio.wait_for` inside ib_insync with:
    `RuntimeError("Timeout should be used inside a task")`.
    """
    if sys.version_info < (3, 14):
        return
    try:
        import _asyncio  # type: ignore
        asyncio.Task = asyncio.tasks.Task = asyncio.tasks._CTask = _asyncio.Task
        asyncio.Future = asyncio.futures.Future = asyncio.futures._CFuture = _asyncio.Future
    except Exception as exc:
        IBKR_LOG.warning("Could not restore native asyncio Task/Future: %s", exc)


def ib_connect_safe(
    host: str = "127.0.0.1",
    port: int = 4002,
    client_id: int = 11,
    timeout: float = 10.0,
    readonly: bool = True,
):
    """Connect ib_insync safely inside Streamlit/Python 3.14.

    Do not use `IB.connect(...)` in Streamlit here. It can trigger:
    `RuntimeError("Timeout should be used inside a task")` and leave
    `Connection.connectAsync` un-awaited. We explicitly create a fresh event
    loop, schedule `connectAsync` as a task, and keep the loop attached to the
    IB instance for later synchronous ib_insync calls.
    """
    _bootstrap_event_loop()
    restore_asyncio_native_tasks_for_py314()
    from ib_insync import IB  # type: ignore

    holder: dict[str, Any] = {}

    async def _connect():
        ib = IB()
        holder["ib"] = ib
        await ib.connectAsync(
            host,
            int(port),
            clientId=int(client_id),
            timeout=float(timeout),
            readonly=bool(readonly),
        )
        return ib

    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        task = loop.create_task(_connect())
        ib = loop.run_until_complete(task)
        # Keep the loop alive; ib_insync sync methods use the current loop.
        setattr(ib, "_streamlit_safe_loop", loop)
        return ib
    except Exception:
        ib = holder.get("ib")
        try:
            if ib is not None:
                ib.disconnect()
        except Exception:
            pass
        raise


@st.cache_resource
def get_ib_insync():
    """Return a connected `IB()` instance, or None on failure.

    The connection is cached for the Streamlit session so we don't reconnect
    on every rerun. Call `ib.disconnect()` to drop it.
    """
    return None  # placeholder; real connect happens in `ib_insync_connect`


def ib_insync_connect(
    host: str = IB_INSYNC_DEFAULT_HOST,
    port: int = IB_INSYNC_DEFAULT_PAPER_PORT,
    client_id: int = IB_INSYNC_DEFAULT_CLIENT_ID,
    timeout: float = 10.0,
) -> dict:
    """Connect to TWS/Gateway. Returns a status dict; never raises.

    The asyncio event loop is bootstrapped at module load (see
    `_bootstrap_event_loop` at the top of this file) so this call is safe
    even when Streamlit runs the script in `ScriptRunner.scriptThread`.
    """
    # Defensive: re-bootstrap in case the loop was closed by another lib
    _bootstrap_event_loop()

    try:
        import ib_insync as ibi
    except ImportError as exc:
        return {
            "_ok": False,
            "_error": "missing",
            "_message": "ib_insync is not installed.",
            "_hint": "Install it: `pip install ib_insync nest_asyncio`",
            "_detail": str(exc),
        }
    except RuntimeError as exc:
        # Should not happen after the bootstrap, but provide a clear message
        return {
            "_ok": False,
            "_error": "event_loop",
            "_message": "Failed to import ib_insync due to event-loop setup.",
            "_hint": "Install nest_asyncio: `pip install nest_asyncio` and restart Streamlit.",
            "_detail": str(exc),
        }

    # `patchAsyncio()` is a no-op when nest_asyncio is already applied;
    # we still call it so plain installs (without nest_asyncio) work too.
    try:
        ibi.util.patchAsyncio()
    except Exception:
        pass

    try:
        ib = st.session_state.get("_ib_insync_obj")
        if ib is None or not ib.isConnected():
            ib = ib_connect_safe(
                host,
                int(port),
                client_id=int(client_id),
                timeout=float(timeout),
                readonly=False,
            )
            st.session_state["_ib_insync_obj"] = ib
        return {
            "_ok": True,
            "_status": "connected",
            "host": host,
            "port": int(port),
            "client_id": int(client_id),
            "server_version": ib.client.serverVersion() if ib.isConnected() else None,
            "transport": (
                "Gateway live" if int(port) == 4001
                else "Gateway paper" if int(port) == 4002
                else "TWS live" if int(port) == 7496
                else "TWS paper" if int(port) == 7497
                else "Custom"
            ),
        }
    except Exception as exc:
        port_label = {4001: "Gateway live", 4002: "Gateway paper",
                      7496: "TWS live", 7497: "TWS paper"}.get(int(port), "custom")
        return {
            "_ok": False,
            "_error": "connection",
            "_message": str(exc),
            "_hint": (
                f"Make sure TWS or IB Gateway is running on port {port} ({port_label}), "
                "the API is enabled (Edit ▸ Global Configuration ▸ API ▸ Settings), "
                "and 'Allow connections from localhost' is checked."
            ),
        }


def ib_insync_disconnect() -> dict:
    ib = st.session_state.get("_ib_insync_obj")
    if ib is None:
        return {"_ok": True, "_status": "already_disconnected"}
    try:
        ib.disconnect()
        st.session_state["_ib_insync_obj"] = None
        return {"_ok": True, "_status": "disconnected"}
    except Exception as exc:
        return {"_ok": False, "_error": str(exc)}


def ib_insync_status() -> dict:
    ib = st.session_state.get("_ib_insync_obj")
    if ib is None:
        return {"connected": False, "message": "No client created yet"}
    return {
        "connected": bool(ib.isConnected()),
        "client_id": getattr(ib.client, "clientId", None),
        "server_version": ib.client.serverVersion() if ib.isConnected() else None,
        "host": getattr(ib.client, "host", None),
        "port": getattr(ib.client, "port", None),
    }


def ib_insync_call(method: str, *args, **kwargs) -> dict:
    """Call any method on the cached `IB()` instance and return a wrapped result."""
    ib = st.session_state.get("_ib_insync_obj")
    if ib is None or not ib.isConnected():
        return {"_ok": False, "_error": "not_connected", "_hint": "Connect first via the button above."}
    try:
        result = getattr(ib, method)(*args, **kwargs)
        # ib_insync returns dataclasses → make them JSON-friendly
        try:
            from ib_insync import util as ibutil
            return {"_ok": True, "data": ibutil.tree(result)}
        except Exception:
            return {"_ok": True, "data": str(result)}
    except Exception as exc:
        return {"_ok": False, "_error": str(exc)}


# ============================================================
# Launcher: open IB Gateway / Client Portal Gateway from the dashboard
# ============================================================
@lru_cache(maxsize=1)
def _platform_system() -> str:
    """Cached platform.system() — avoids repeated system calls per rerun."""
    import platform
    return platform.system()


def _ibkr_app_candidates() -> list[Path]:
    """Likely paths of the IB Gateway / TWS desktop binary, by OS."""
    system = _platform_system()
    home = Path.home()
    if system == "Darwin":
        return [
            Path("/Applications/IB Gateway.app"),
            Path("/Applications/Trader Workstation.app"),
            Path("/Applications/TWS.app"),
            home / "Applications" / "IB Gateway.app",
        ]
    if system == "Windows":
        return [
            Path(r"C:\Jts\ibgateway\latest\ibgateway.exe"),
            Path(r"C:\Jts\tws.exe"),
            Path(r"C:\Program Files\IB Gateway\ibgateway.exe"),
            home / "Jts" / "ibgateway" / "latest" / "ibgateway.exe",
        ]
    return [
        Path("/opt/ibgateway/ibgateway"),
        Path("/opt/Jts/ibgateway/latest/ibgateway"),
        home / "Jts" / "ibgateway" / "latest" / "ibgateway",
        home / "ibgateway" / "ibgateway",
    ]


def _clientportal_candidates() -> list[Path]:
    """Likely paths to the Client Portal Gateway run script."""
    system = _platform_system()
    home = Path.home()
    script = "run.bat" if system == "Windows" else "run.sh"
    paths = [
        home / "clientportal.gw" / "bin" / script,
        home / "Downloads" / "clientportal.gw" / "bin" / script,
        home / "Desktop" / "clientportal.gw" / "bin" / script,
        Path("/opt/clientportal.gw/bin") / script,
    ]
    if system == "Windows":
        paths.append(Path(f"C:/clientportal.gw/bin/{script}"))
    return paths


def launch_ib_gateway() -> dict:
    """Open IB Gateway / TWS via the OS-native launcher. Never raises."""
    import subprocess
    system = _platform_system()
    try:
        if system == "Darwin":
            # `open -a` searches Spotlight — works even if the .app is in a non-standard location.
            for name in ("IB Gateway", "Trader Workstation", "TWS"):
                rc = subprocess.run(["open", "-a", name], capture_output=True, text=True)
                if rc.returncode == 0:
                    return {"_ok": True, "_message": f"Launched '{name}'"}
            return {
                "_ok": False,
                "_message": (
                    "Could not find IB Gateway / TWS in /Applications. "
                    "Install from interactivebrokers.com → IB Gateway."
                ),
            }
        # Windows / Linux / generic Unix → spawn the binary directly
        for path in _ibkr_app_candidates():
            if path.exists():
                kwargs: dict = {"close_fds": True}
                if system != "Windows":
                    kwargs["start_new_session"] = True
                subprocess.Popen([str(path)], **kwargs)
                return {"_ok": True, "_message": f"Launched {path}"}
        return {
            "_ok": False,
            "_message": (
                "IB Gateway binary not found in default install paths. "
                "Tried: " + ", ".join(str(p) for p in _ibkr_app_candidates()[:3]) + " …"
            ),
        }
    except Exception as exc:
        return {"_ok": False, "_message": f"Launcher failed: {exc}"}


def launch_clientportal_gateway() -> dict:
    """Spawn the Client Portal Gateway (port :5000 REST). Never raises."""
    import subprocess
    system = _platform_system()
    conf = "root/conf.yaml"
    for path in _clientportal_candidates():
        if not path.exists():
            continue
        try:
            cwd = str(path.parent.parent)  # gateway expects cwd = clientportal.gw root
            if system == "Windows":
                subprocess.Popen([str(path), conf], cwd=cwd, close_fds=True)
            else:
                subprocess.Popen(
                    ["bash", str(path), conf],
                    cwd=cwd,
                    close_fds=True,
                    start_new_session=True,
                )
            return {
                "_ok": True,
                "_message": f"Started {path}",
                "url": "https://localhost:5000",
            }
        except Exception as exc:
            return {"_ok": False, "_message": f"Found {path} but failed to start: {exc}"}
    return {
        "_ok": False,
        "_message": (
            "Client Portal Gateway not found. Download from "
            "https://www.interactivebrokers.com/en/trading/ib-api.php#client-portal-api "
            "and unzip to ~/clientportal.gw (or ~/Downloads/clientportal.gw)."
        ),
    }


IBKR_DEFAULT_BASE_URL = "https://localhost:5000/v1/api"
IBKR_DEFAULT_WS_URL = "wss://localhost:5000/v1/api/ws"


@dataclass
class IBKROrderTicket:
    """One leg of an IBKR order."""
    conid: int
    side: str  # "BUY" | "SELL"
    quantity: float
    order_type: str = "MKT"  # MKT | LMT | STP | STP_LMT | MIDPRICE | TRAIL
    tif: str = "DAY"  # DAY | GTC | OPG | IOC
    price: float | None = None
    aux_price: float | None = None
    outside_rth: bool = False
    use_adaptive: bool = False

    def to_payload(self) -> dict:
        ticket: dict = {
            "conid": int(self.conid),
            "orderType": self.order_type.upper(),
            "side": self.side.upper(),
            "quantity": float(self.quantity),
            "tif": self.tif.upper(),
        }
        if self.order_type.upper() in {"LMT", "STP_LMT"} and self.price is not None:
            ticket["price"] = float(self.price)
        if self.order_type.upper() in {"STP", "STP_LMT", "TRAIL"} and self.aux_price is not None:
            ticket["auxPrice"] = float(self.aux_price)
        if self.outside_rth:
            ticket["outsideRTH"] = True
        if self.use_adaptive:
            ticket["useAdaptive"] = True
        return ticket


class IBKRClient:
    """Thin synchronous client around the IBKR Client Portal REST API.

    Notes
    -----
    * `/iserver/account/{accountId}/orders` accepts a JSON array of tickets
      (NOT `{"orders": [...]}` — the previous draft was wrong).
    * Many state-changing endpoints can return a `messageId` that must be
      confirmed via `/iserver/reply/{messageId}` with `{"confirmed": true}`.
    * Local gateway uses self-signed SSL — disable verification.
    """

    # Per-path throttle in seconds (IBKR specifically rate-limits these)
    _PATH_THROTTLE: dict[str, float] = {
        "/iserver/orders": 5.0,
        "/iserver/trades": 5.0,
    }
    # Global cap (~10 req/sec → keep 0.10s between back-to-back calls)
    _GLOBAL_THROTTLE: float = 0.10

    def __init__(
        self,
        base_url: str = IBKR_DEFAULT_BASE_URL,
        ws_url: str = IBKR_DEFAULT_WS_URL,
        verify_ssl: bool = False,
        timeout: float = 1.5,
    ):
        self.base_url = base_url.rstrip("/")
        self.ws_url = ws_url
        self.timeout = timeout
        self.session = requests.Session()
        self.session.verify = verify_ssl
        # Mount adapter with no retries — fail fast for a local gateway
        try:
            adapter = HTTPAdapter(max_retries=Retry(total=0, connect=0, read=0))
            self.session.mount("http://", adapter)
            self.session.mount("https://", adapter)
        except Exception:
            pass
        self._lock = threading.Lock()
        self._last_call_global: float = 0.0
        self._last_call_path: dict[str, float] = {}
        # Quiet the SSL warning when verify_ssl=False
        if not verify_ssl:
            try:
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            except Exception:
                pass

    # ---------- internals ----------
    def _throttle(self, path: str) -> None:
        """Enforce IBKR's documented rate limits before issuing a request."""
        now = time.time()
        with self._lock:
            # Global cap (~10 rps)
            wait = self._GLOBAL_THROTTLE - (now - self._last_call_global)
            if wait > 0:
                time.sleep(wait)
                now = time.time()
            # Per-path cap (e.g. /iserver/orders: once per 5s)
            for prefix, gap in self._PATH_THROTTLE.items():
                if path.startswith(prefix):
                    last = self._last_call_path.get(prefix, 0.0)
                    wait = gap - (now - last)
                    if wait > 0:
                        time.sleep(wait)
                        now = time.time()
                    self._last_call_path[prefix] = now
                    break
            self._last_call_global = now

    @staticmethod
    def _classify_exception(exc: Exception) -> tuple[str, str]:
        """Return (short_kind, friendly_hint) for a requests exception."""
        from requests.exceptions import (
            ConnectTimeout,
            ConnectionError as ReqConnError,
            ReadTimeout,
            SSLError,
        )
        if isinstance(exc, (ConnectTimeout, ReadTimeout)):
            return (
                "timeout",
                (
                    "Client Portal Gateway لم يرد بسرعة كافية. افتح https://localhost:5000 في المتصفح، "
                    "تأكد أن تسجيل الدخول مكتمل، ثم جرّب Auth status أو Fetch accounts مرة أخرى."
                ),
            )
        if isinstance(exc, SSLError):
            return (
                "ssl",
                "SSL handshake failed. Uncheck *Verify SSL* (the gateway uses a self-signed certificate by default).",
            )
        if isinstance(exc, ReqConnError):
            return (
                "connection",
                "Connection refused. Start the IBKR Client Portal Gateway (it listens on https://localhost:5000) "
                "and sign in once via the browser.",
            )
        return ("error", str(exc))

    def _safe_request(self, method: str, path: str, **kwargs) -> dict:
        """Wrap a request so timeouts/connection errors return a clean dict
        instead of crashing the Streamlit page."""
        self._throttle(path)
        try:
            url = f"{self.base_url}{path}"
            r = self.session.request(method, url, timeout=self.timeout, **kwargs)
            return self._unwrap(r)
        except requests.exceptions.RequestException as exc:
            kind, hint = self._classify_exception(exc)
            return {
                "_status": 0,
                "_ok": False,
                "_error": kind,
                "_error_hint": hint,
                "_error_detail": "" if kind in {"timeout", "connection", "ssl"} else str(exc),
                "data": None,
            }

    def _get(self, path: str, **kwargs) -> Any:
        return self._safe_request("GET", path, **kwargs)

    def _post(self, path: str, **kwargs) -> Any:
        return self._safe_request("POST", path, **kwargs)

    def _put(self, path: str, **kwargs) -> Any:
        return self._safe_request("PUT", path, **kwargs)

    def _delete(self, path: str, **kwargs) -> Any:
        return self._safe_request("DELETE", path, **kwargs)

    @staticmethod
    def _unwrap(r: requests.Response) -> Any:
        try:
            data = r.json()
        except Exception:
            data = {"raw": r.text}
        return {"_status": r.status_code, "_ok": r.ok, "data": data}

    # ---------- session / auth ----------
    def tickle(self) -> Any:
        """Keep the session alive and return the session token / status."""
        return self._get("/tickle")

    def auth_status(self) -> Any:
        return self._get("/iserver/auth/status")

    def reauthenticate(self) -> Any:
        return self._post("/iserver/reauthenticate")

    def sso_validate(self) -> Any:
        return self._get("/sso/validate")

    def logout(self) -> Any:
        return self._get("/logout")

    # ---------- accounts / portfolio ----------
    def portfolio_accounts(self) -> Any:
        return self._get("/portfolio/accounts")

    def portfolio_subaccounts(self) -> Any:
        return self._get("/portfolio/subaccounts")

    def portfolio_subaccounts2(self) -> Any:
        return self._get("/portfolio/subaccounts2")

    def iserver_accounts(self) -> Any:
        return self._get("/iserver/accounts")

    def account_summary(self, account_id: str) -> Any:
        return self._get(f"/portfolio/{account_id}/summary")

    def account_ledger(self, account_id: str) -> Any:
        return self._get(f"/portfolio/{account_id}/ledger")

    def positions(self, account_id: str, page: int = 0) -> Any:
        return self._get(f"/portfolio/{account_id}/positions/{page}")

    def pnl_partitioned(self) -> Any:
        return self._get("/iserver/account/pnl/partitioned")

    # ---------- market data ----------
    def snapshot(self, conids: list[int] | str, fields: list[str] | None = None) -> Any:
        if isinstance(conids, list):
            conids = ",".join(str(c) for c in conids)
        params = {"conids": str(conids)}
        if fields:
            params["fields"] = ",".join(fields)
        return self._get("/iserver/marketdata/snapshot", params=params)

    def history(
        self,
        conid: int,
        period: str = "1d",
        bar: str = "5min",
        outside_rth: bool = False,
    ) -> Any:
        params = {"conid": int(conid), "period": period, "bar": bar, "outsideRth": str(outside_rth).lower()}
        return self._get("/iserver/marketdata/history", params=params)

    def unsubscribe(self, conid: int) -> Any:
        return self._post("/iserver/marketdata/unsubscribe", json={"conid": int(conid)})

    def unsubscribe_all(self) -> Any:
        return self._get("/iserver/marketdata/unsubscribeall")

    def secdef(self, conids: list[int]) -> Any:
        # /trsv/secdef accepts up to 200 conids per call
        return self._post("/trsv/secdef", json={"conids": [int(c) for c in conids[:200]]})

    # ---------- scanner ----------
    def scanner_params(self) -> Any:
        return self._get("/iserver/scanner/params")

    def scanner_run(self, payload: dict) -> Any:
        return self._post("/iserver/scanner/run", json=payload)

    # ---------- trading ----------
    def place_order(self, account_id: str, ticket: IBKROrderTicket | dict) -> Any:
        """Submit a single order. NOTE: payload is a JSON array, not {"orders":[...]}.

        IBKR returns either an order acknowledgement OR a list of
        `messageId` warnings that must be confirmed via `confirm_reply`.
        """
        if isinstance(ticket, IBKROrderTicket):
            payload = [ticket.to_payload()]
        elif isinstance(ticket, dict):
            payload = [ticket]
        elif isinstance(ticket, list):
            payload = ticket
        else:
            raise TypeError("ticket must be IBKROrderTicket | dict | list[dict]")
        return self._post(f"/iserver/account/{account_id}/orders", json=payload)

    def place_orders_bulk(self, account_id: str, tickets: list[IBKROrderTicket | dict]) -> Any:
        payload = []
        for t in tickets:
            payload.append(t.to_payload() if isinstance(t, IBKROrderTicket) else t)
        return self._post(f"/iserver/account/{account_id}/orders", json=payload)

    def confirm_reply(self, message_id: str, confirmed: bool = True) -> Any:
        return self._post(f"/iserver/reply/{message_id}", json={"confirmed": bool(confirmed)})

    def modify_order(self, account_id: str, order_id: str, ticket: IBKROrderTicket | dict) -> Any:
        payload = ticket.to_payload() if isinstance(ticket, IBKROrderTicket) else ticket
        return self._post(f"/iserver/account/{account_id}/order/{order_id}", json=payload)

    def cancel_order(self, account_id: str, order_id: str) -> Any:
        # Note: cancel does NOT accept a JSON body
        return self._delete(f"/iserver/account/{account_id}/order/{order_id}")

    def suppress_questions(self, message_ids: list[str]) -> Any:
        return self._post("/iserver/questions/suppress", json={"messageIds": list(message_ids)})

    def suppress_questions_reset(self) -> Any:
        return self._post("/iserver/questions/suppress/reset")

    # ---------- orders / trades ----------
    def orders(self, filters: str | None = None, force: bool = True, account_id: str | None = None) -> Any:
        params: dict[str, Any] = {"force": str(force).lower()}
        if filters:
            params["filters"] = filters
        if account_id:
            params["accountId"] = account_id
        return self._get("/iserver/account/orders", params=params)

    def live_orders(self) -> Any:
        return self._get("/iserver/orders")

    def trades(self) -> Any:
        return self._get("/iserver/trades")

    # ---------- Portfolio Analyst (PA) ----------
    def pa_performance(self, account_ids: list[str], freq: str = "D") -> Any:
        """Performance report. freq = D (daily) | M (monthly) | Y (yearly)."""
        return self._post("/pa/performance", json={"acctIds": list(account_ids), "freq": freq})

    def pa_summary(self, account_ids: list[str]) -> Any:
        """Summary report for the given accounts."""
        return self._post("/pa/summary", json={"acctIds": list(account_ids)})

    def pa_transactions(
        self,
        account_ids: list[str],
        conids: list[int] | None = None,
        currency: str = "USD",
        days: int = 90,
    ) -> Any:
        """Transactions report (last N days)."""
        payload: dict = {
            "acctIds": list(account_ids),
            "currency": currency,
            "days": int(days),
        }
        if conids:
            payload["conids"] = [int(c) for c in conids]
        return self._post("/pa/transactions", json=payload)

    # ---------- FYI notifications ----------
    def fyi_unread(self) -> Any:
        return self._get("/fyi/unreadnumber")

    def fyi_notifications(self) -> Any:
        return self._get("/fyi/notifications")

    def fyi_notifications_more(self) -> Any:
        return self._get("/fyi/notifications/more")

    def fyi_mark_read(self, notification_id: str) -> Any:
        return self._put(f"/fyi/notifications/{notification_id}")

    def fyi_settings(self) -> Any:
        return self._get("/fyi/settings")

    def fyi_settings_update(self, typecode: str, enabled: bool) -> Any:
        return self._post(f"/fyi/settings/{typecode}", json={"enabled": bool(enabled)})

    def fyi_disclaimer_get(self, typecode: str) -> Any:
        return self._get(f"/fyi/disclaimer/{typecode}")

    def fyi_disclaimer_accept(self, typecode: str) -> Any:
        return self._put(f"/fyi/disclaimer/{typecode}")

    def fyi_delivery_options(self) -> Any:
        return self._get("/fyi/deliveryoptions")

    def fyi_delivery_email(self, enabled: bool) -> Any:
        return self._put("/fyi/deliveryoptions/email", json={"enabled": bool(enabled)})

    def fyi_delivery_device_add(self, device_token: str, device_name: str = "Atlas") -> Any:
        return self._post("/fyi/deliveryoptions/device", json={"deviceToken": device_token, "deviceName": device_name})

    def fyi_delivery_device_remove(self, device_id: str) -> Any:
        return self._delete(f"/fyi/deliveryoptions/{device_id}")

# ============================================================
# IBKR Engine — production-safe abstraction inside app.py
# ============================================================
# The user asked to keep this implementation in app.py only. The classes below
# mirror a small package layout (`config`, `connection_manager`, `socket_client`,
# `client_portal`, `orders`, `risk`, `errors`, `streamlit_helpers`) without
# creating extra files. Public methods never raise into Streamlit; they always
# return a structured envelope required by the dashboard.

IBKR_ENGINE_SOURCES = {"ib_insync", "client_portal", "none"}
IBKR_SOCKET_PRIORITY = (4002, 4001, 7497, 7496)


def ensure_event_loop() -> asyncio.AbstractEventLoop:
    """Return a usable event loop in Streamlit worker threads.

    Streamlit often runs code inside `ScriptRunner.scriptThread`, where Python
    3.14+ may not auto-create an event loop. ib_insync/eventkit needs one even
    at import time, so this helper is intentionally defensive.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
    if sys.version_info < (3, 14):
        try:
            import nest_asyncio  # type: ignore
            nest_asyncio.apply(loop)
        except Exception:
            pass
    return loop


def _ibkr_bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _ibkr_float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return float(default)


def _ibkr_int_env(name: str, default: int) -> int:
    try:
        return int(float(os.getenv(name, str(default))))
    except Exception:
        return int(default)


def _ibkr_parse_ports(raw: str | None) -> tuple[int, ...]:
    ports: list[int] = []
    for item in (raw or "").split(","):
        item = item.strip()
        if not item:
            continue
        try:
            port = int(item)
        except ValueError:
            continue
        if port not in ports:
            ports.append(port)
    return tuple(ports) or IBKR_SOCKET_PRIORITY


def _ibkr_mode_for_port(port: int) -> str:
    if int(port) in {4002, 7497}:
        return "paper"
    if int(port) in {4001, 7496}:
        return "live"
    return "unknown"


def _ibkr_transport_label(port: int) -> str:
    return {
        4002: "IB Gateway Paper Socket",
        4001: "IB Gateway Live Socket",
        7497: "TWS Paper Socket",
        7496: "TWS Live Socket",
    }.get(int(port), f"Custom Socket {port}")


def _ibkr_is_local_url(url: str) -> bool:
    normalized = (url or "").lower()
    return (
        "localhost" in normalized
        or "127.0.0.1" in normalized
        or "0.0.0.0" in normalized
        or "[::1]" in normalized
    )


def _ibkr_mask_account_id(account_id: str | None) -> str | None:
    if not account_id:
        return None
    text = str(account_id)
    if len(text) <= 4:
        return "****"
    return f"{text[:2]}***{text[-2:]}"


def ibkr_result(
    ok: bool,
    source: str = "none",
    mode: str = "unknown",
    data: Any = None,
    error: str | None = None,
    details: dict | None = None,
) -> dict:
    """Create the single public result envelope used by all IBKR methods."""
    safe_source = source if source in IBKR_ENGINE_SOURCES else "none"
    safe_mode = mode if mode in {"paper", "live", "unknown"} else "unknown"
    return {
        "_ok": bool(ok),
        "source": safe_source,
        "mode": safe_mode,
        "data": data,
        "error": None if ok else (error or "Unknown IBKR error."),
        "details": details or {},
    }


@dataclass(frozen=True)
class IBKREngineConfig:
    host: str = "127.0.0.1"
    socket_ports: tuple[int, ...] = IBKR_SOCKET_PRIORITY
    client_id: int = 11
    client_portal_base: str = IBKR_DEFAULT_BASE_URL
    enable_order_placement: bool = False
    allow_live_trading: bool = False
    max_order_qty: float = 10.0
    max_notional_usd: float = 1000.0
    timeout_seconds: float = 5.0
    log_level: str = "INFO"


def load_ibkr_config() -> IBKREngineConfig:
    """Load IBKR settings from .env/environment without failing the app."""
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except Exception:
        pass
    cfg = IBKREngineConfig(
        host=os.getenv("IBKR_HOST", "127.0.0.1"),
        socket_ports=_ibkr_parse_ports(os.getenv("IBKR_SOCKET_PORTS")),
        client_id=_ibkr_int_env("IBKR_CLIENT_ID", 11),
        client_portal_base=os.getenv("IBKR_CLIENT_PORTAL_BASE", IBKR_DEFAULT_BASE_URL),
        enable_order_placement=_ibkr_bool_env("IBKR_ENABLE_ORDER_PLACEMENT", False),
        allow_live_trading=_ibkr_bool_env("IBKR_ALLOW_LIVE_TRADING", False),
        max_order_qty=_ibkr_float_env("IBKR_MAX_ORDER_QTY", 10.0),
        max_notional_usd=_ibkr_float_env("IBKR_MAX_NOTIONAL_USD", 1000.0),
        timeout_seconds=_ibkr_float_env("IBKR_TIMEOUT_SECONDS", 5.0),
        log_level=os.getenv("IBKR_LOG_LEVEL", "INFO").upper(),
    )
    logging.getLogger("ibkr_engine").setLevel(getattr(logging, cfg.log_level, logging.INFO))
    return cfg


logging.basicConfig(
    level=getattr(logging, os.getenv("IBKR_LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
IBKR_LOG = logging.getLogger("ibkr_engine")


class IBKREngineError(Exception):
    """Base internal IBKR engine exception."""


class IBKROrderBlocked(IBKREngineError):
    """Raised internally when the risk layer blocks an order."""


class IBKRRiskManager:
    """Read-only-by-default risk gate for every order path."""

    def __init__(self, config: IBKREngineConfig):
        self.config = config

    def validate_order(
        self,
        symbol: str,
        action: str,
        quantity: float,
        mode: str,
        order_type: str = "MKT",
        limit_price: float | None = None,
    ) -> dict:
        symbol = (symbol or "").strip().upper()
        action = (action or "").strip().upper()
        order_type = (order_type or "MKT").strip().upper()
        try:
            qty = float(quantity)
        except Exception:
            return ibkr_result(False, error="Quantity must be numeric.", details={"code": "invalid_quantity"})

        if not symbol:
            return ibkr_result(False, error="Symbol is required.", details={"code": "invalid_symbol"})
        if action not in {"BUY", "SELL"}:
            return ibkr_result(False, error="Action must be BUY or SELL.", details={"code": "invalid_action"})
        if qty <= 0:
            return ibkr_result(False, error="Quantity must be greater than zero.", details={"code": "invalid_quantity"})
        if qty > float(self.config.max_order_qty):
            return ibkr_result(
                False,
                error=f"Order quantity exceeds IBKR_MAX_ORDER_QTY={self.config.max_order_qty:g}.",
                details={"code": "max_quantity_exceeded", "max_order_qty": self.config.max_order_qty},
            )
        if order_type == "LMT":
            if limit_price is None or float(limit_price) <= 0:
                return ibkr_result(False, error="Limit price must be greater than zero.", details={"code": "invalid_limit_price"})
            notional = qty * float(limit_price)
            if self.config.max_notional_usd > 0 and notional > float(self.config.max_notional_usd):
                return ibkr_result(
                    False,
                    error=f"Order notional exceeds IBKR_MAX_NOTIONAL_USD={self.config.max_notional_usd:g}.",
                    details={"code": "max_notional_exceeded", "estimated_notional": notional},
                )
        if mode == "live" and not self.config.allow_live_trading:
            return ibkr_result(
                False,
                error="Live trading is blocked. Set IBKR_ALLOW_LIVE_TRADING=true to enable it explicitly.",
                details={"code": "live_trading_blocked"},
            )
        if not self.config.enable_order_placement:
            return ibkr_result(
                False,
                error="Order placement is disabled. Set IBKR_ENABLE_ORDER_PLACEMENT=true to enable paper/live submissions.",
                details={"code": "order_placement_disabled", "safe_mode": "read_only"},
            )
        return ibkr_result(True, data={"symbol": symbol, "action": action, "quantity": qty, "order_type": order_type})


class IBKRSocketClient:
    """ib_insync transport for TWS / IB Gateway socket API."""

    def __init__(self, config: IBKREngineConfig, port: int, client_id: int):
        self.config = config
        self.port = int(port)
        self.client_id = int(client_id)
        self.mode = _ibkr_mode_for_port(self.port)
        self.ib = None

    @property
    def source(self) -> str:
        return "ib_insync"

    def _serialize(self, value: Any) -> Any:
        try:
            from ib_insync import util as ibutil  # type: ignore
            return ibutil.tree(value)
        except Exception:
            try:
                return json.loads(json.dumps(value, default=str))
            except Exception:
                return str(value)

    def _require_ib(self):
        if self.ib is None or not self.ib.isConnected():
            raise ConnectionError("ib_insync is not connected.")
        return self.ib

    def connect(self) -> dict:
        ensure_event_loop()
        try:
            import ib_insync as ibi  # type: ignore
            try:
                ibi.util.patchAsyncio()
            except Exception:
                pass
        except ImportError as exc:
            return ibkr_result(
                False,
                source="ib_insync",
                mode=self.mode,
                error="Missing package: ib_insync. Install `ib_insync nest_asyncio`.",
                details={"code": "missing_package", "package": "ib_insync", "detail": str(exc)},
            )
        except RuntimeError as exc:
            return ibkr_result(
                False,
                source="ib_insync",
                mode=self.mode,
                error="ib_insync event-loop initialization failed.",
                details={"code": "event_loop", "detail": str(exc)},
            )

        try:
            if self.ib is None or not self.ib.isConnected():
                IBKR_LOG.info("Attempting %s on %s:%s", _ibkr_transport_label(self.port), self.config.host, self.port)
                if not is_tcp_port_listening(self.config.host, self.port):
                    IBKR_LOG.info("IBKR socket port closed: %s:%s", self.config.host, self.port)
                    return ibkr_result(
                        False,
                        source=self.source,
                        mode=self.mode,
                        error=f"{_ibkr_transport_label(self.port)} is not listening on {self.config.host}:{self.port}.",
                        details={"code": "socket_port_closed", "host": self.config.host, "port": self.port},
                    )
                IBKR_LOG.info("IBKR socket port is listening: %s:%s", self.config.host, self.port)
                self.ib = ib_connect_safe(
                    self.config.host,
                    self.port,
                    client_id=self.client_id,
                    timeout=max(10.0, float(self.config.timeout_seconds)),
                    readonly=not self.config.enable_order_placement,
                )
            server_version = self.ib.client.serverVersion() if self.ib.isConnected() else None
            IBKR_LOG.info("Selected ib_insync transport %s mode=%s", _ibkr_transport_label(self.port), self.mode)
            return ibkr_result(
                True,
                source=self.source,
                mode=self.mode,
                data={"connected": True, "server_version": server_version},
                details={
                    "host": self.config.host,
                    "port": self.port,
                    "client_id": self.client_id,
                    "transport": _ibkr_transport_label(self.port),
                    "readonly": not self.config.enable_order_placement,
                },
            )
        except Exception as exc:
            try:
                if self.ib is not None:
                    self.ib.disconnect()
            except Exception:
                pass
            return ibkr_result(
                False,
                source=self.source,
                mode=self.mode,
                error=str(exc),
                details={
                    "code": "socket_connection_failed",
                    "host": self.config.host,
                    "port": self.port,
                    "transport": _ibkr_transport_label(self.port),
                    "hint": "Run TWS/IB Gateway, enable API, allow localhost, and use the matching paper/live port.",
                },
            )

    def disconnect(self) -> dict:
        try:
            if self.ib is not None and self.ib.isConnected():
                self.ib.disconnect()
            return ibkr_result(True, source=self.source, mode=self.mode, data={"connected": False})
        except Exception as exc:
            return ibkr_result(False, source=self.source, mode=self.mode, error=str(exc), details={"code": "disconnect_failed"})

    def status(self) -> dict:
        try:
            connected = bool(self.ib is not None and self.ib.isConnected())
            data = {"connected": connected}
            if connected:
                data["server_version"] = self.ib.client.serverVersion()
            return ibkr_result(
                connected,
                source=self.source,
                mode=self.mode,
                data=data,
                error=None if connected else "Socket transport is disconnected.",
                details={"host": self.config.host, "port": self.port, "client_id": self.client_id},
            )
        except Exception as exc:
            return ibkr_result(False, source=self.source, mode=self.mode, error=str(exc), details={"code": "status_failed"})

    def get_accounts(self) -> dict:
        try:
            ib = self._require_ib()
            return ibkr_result(True, source=self.source, mode=self.mode, data=list(ib.managedAccounts()))
        except Exception as exc:
            return ibkr_result(False, source=self.source, mode=self.mode, error=str(exc), details={"code": "accounts_failed"})

    def get_account_summary(self, account_id: str | None = None) -> dict:
        try:
            ib = self._require_ib()
            rows = ib.accountSummary(account=account_id or "")
            return ibkr_result(True, source=self.source, mode=self.mode, data=self._serialize(rows))
        except Exception as exc:
            return ibkr_result(False, source=self.source, mode=self.mode, error=str(exc), details={"code": "account_summary_failed"})

    def get_positions(self, account_id: str | None = None) -> dict:
        try:
            ib = self._require_ib()
            rows = ib.positions()
            if account_id:
                rows = [p for p in rows if getattr(p, "account", None) == account_id]
            return ibkr_result(True, source=self.source, mode=self.mode, data=self._serialize(rows))
        except Exception as exc:
            return ibkr_result(False, source=self.source, mode=self.mode, error=str(exc), details={"code": "positions_failed"})

    def get_open_orders(self) -> dict:
        try:
            ib = self._require_ib()
            rows = ib.openTrades()
            return ibkr_result(True, source=self.source, mode=self.mode, data=self._serialize(rows))
        except Exception as exc:
            return ibkr_result(False, source=self.source, mode=self.mode, error=str(exc), details={"code": "open_orders_failed"})

    def qualify_contract(self, symbol: str, exchange: str = "SMART", currency: str = "USD") -> dict:
        try:
            symbol = (symbol or "").strip().upper()
            if not symbol:
                return ibkr_result(False, source=self.source, mode=self.mode, error="Symbol is required.", details={"code": "invalid_symbol"})
            ib = self._require_ib()
            from ib_insync import Stock  # type: ignore
            contract = Stock(symbol, exchange, currency)
            qualified = ib.qualifyContracts(contract)
            if not qualified:
                return ibkr_result(False, source=self.source, mode=self.mode, error=f"Unsupported or unknown symbol: {symbol}", details={"code": "unsupported_asset"})
            return ibkr_result(True, source=self.source, mode=self.mode, data=self._serialize(qualified[0]))
        except Exception as exc:
            return ibkr_result(False, source=self.source, mode=self.mode, error=str(exc), details={"code": "qualify_failed"})

    def get_market_data(self, symbol: str) -> dict:
        try:
            symbol = (symbol or "").strip().upper()
            if not symbol:
                return ibkr_result(False, source=self.source, mode=self.mode, error="Symbol is required.", details={"code": "invalid_symbol"})
            ib = self._require_ib()
            from ib_insync import Stock  # type: ignore
            contract = Stock(symbol, "SMART", "USD")
            qualified = ib.qualifyContracts(contract)
            if not qualified:
                return ibkr_result(False, source=self.source, mode=self.mode, error=f"Unsupported or unknown symbol: {symbol}", details={"code": "unsupported_asset"})
            ticker = ib.reqMktData(qualified[0], "", False, False)
            try:
                ib.sleep(min(1.0, max(0.25, float(self.config.timeout_seconds) / 5.0)))
            except Exception:
                pass
            data = {
                "symbol": symbol,
                "bid": getattr(ticker, "bid", None),
                "ask": getattr(ticker, "ask", None),
                "last": getattr(ticker, "last", None),
                "close": getattr(ticker, "close", None),
                "market_price": ticker.marketPrice() if hasattr(ticker, "marketPrice") else None,
                "time": str(getattr(ticker, "time", "")) if getattr(ticker, "time", None) else None,
            }
            try:
                ib.cancelMktData(qualified[0])
            except Exception:
                pass
            return ibkr_result(True, source=self.source, mode=self.mode, data=data, details={"contract": self._serialize(qualified[0])})
        except Exception as exc:
            return ibkr_result(False, source=self.source, mode=self.mode, error=str(exc), details={"code": "market_data_failed"})

    def place_market_order(self, symbol: str, action: str, quantity: float) -> dict:
        return self._place_order(symbol, action, quantity, order_type="MKT", limit_price=None)

    def place_limit_order(self, symbol: str, action: str, quantity: float, limit_price: float) -> dict:
        return self._place_order(symbol, action, quantity, order_type="LMT", limit_price=limit_price)

    def _place_order(self, symbol: str, action: str, quantity: float, order_type: str, limit_price: float | None) -> dict:
        try:
            ib = self._require_ib()
            from ib_insync import LimitOrder, MarketOrder, Stock  # type: ignore
            contract = Stock(symbol.strip().upper(), "SMART", "USD")
            qualified = ib.qualifyContracts(contract)
            if not qualified:
                return ibkr_result(False, source=self.source, mode=self.mode, error=f"Unsupported or unknown symbol: {symbol}", details={"code": "unsupported_asset"})
            if order_type == "LMT":
                order = LimitOrder(action.upper(), float(quantity), float(limit_price))
            else:
                order = MarketOrder(action.upper(), float(quantity))
            trade = ib.placeOrder(qualified[0], order)
            return ibkr_result(True, source=self.source, mode=self.mode, data=self._serialize(trade), details={"order_type": order_type})
        except Exception as exc:
            return ibkr_result(False, source=self.source, mode=self.mode, error=str(exc), details={"code": "order_submit_failed"})

    def cancel_order(self, order_id: int | str) -> dict:
        try:
            ib = self._require_ib()
            target = int(order_id)
            for trade in ib.openTrades():
                if int(getattr(trade.order, "orderId", -1)) == target:
                    ib.cancelOrder(trade.order)
                    return ibkr_result(True, source=self.source, mode=self.mode, data={"cancelled_order_id": target})
            return ibkr_result(False, source=self.source, mode=self.mode, error=f"Open order {target} was not found.", details={"code": "order_not_found"})
        except Exception as exc:
            return ibkr_result(False, source=self.source, mode=self.mode, error=str(exc), details={"code": "cancel_failed"})


class IBKRClientPortalTransport:
    """Client Portal REST transport using httpx when available."""

    def __init__(self, config: IBKREngineConfig):
        self.config = config
        self.base_url = config.client_portal_base.rstrip("/")
        self.mode = "unknown"

    @property
    def source(self) -> str:
        return "client_portal"

    def _httpx_request(self, method: str, path: str, **kwargs) -> dict:
        url = f"{self.base_url}{path}"
        verify = not _ibkr_is_local_url(self.base_url)
        timeout = min(float(self.config.timeout_seconds), 1.5)
        try:
            import httpx  # type: ignore
            with httpx.Client(verify=verify, timeout=timeout, trust_env=False) as client:
                response = client.request(method, url, **kwargs)
            try:
                data = response.json()
            except Exception:
                data = {"raw": response.text}
            ok = 200 <= int(response.status_code) < 300
            return ibkr_result(
                ok,
                source=self.source,
                mode=self.mode,
                data=data,
                error=None if ok else f"HTTP {response.status_code}",
                details={"status_code": response.status_code, "path": path},
            )
        except ImportError:
            # Keep Streamlit alive even if httpx is absent; requests is already
            # used by the legacy Client Portal section in this app.
            try:
                response = requests.request(method, url, timeout=timeout, verify=verify, **kwargs)
                try:
                    data = response.json()
                except Exception:
                    data = {"raw": response.text}
                ok = bool(response.ok)
                return ibkr_result(
                    ok,
                    source=self.source,
                    mode=self.mode,
                    data=data,
                    error=None if ok else f"HTTP {response.status_code}",
                    details={"status_code": response.status_code, "path": path, "fallback": "requests"},
                )
            except requests.exceptions.RequestException as exc:
                return self._request_error(exc, path)
        except Exception as exc:
            return self._request_error(exc, path)

    def _request_error(self, exc: Exception, path: str) -> dict:
        name = exc.__class__.__name__.lower()
        detail = str(exc)
        if "connect" in name or "connection" in name or "refused" in detail.lower():
            error = (
                "Connection refused. Start IB Gateway/TWS for socket mode, or start Client Portal Gateway "
                "on https://localhost:5000 and sign in once in the browser."
            )
            code = "connection_refused"
        elif "ssl" in name or "certificate" in detail.lower():
            error = "SSL certificate issue. Local Client Portal uses a self-signed certificate; localhost verification is disabled by the engine."
            code = "ssl_certificate"
        elif "timeout" in name:
            error = "Client Portal Gateway timed out."
            code = "timeout"
        else:
            error = detail or "Client Portal request failed."
            code = "client_portal_error"
        safe_detail = "" if code in {"connection_refused", "ssl_certificate", "timeout"} else detail
        return ibkr_result(False, source=self.source, mode=self.mode, error=error, details={"code": code, "path": path, "detail": safe_detail})

    def _get(self, path: str, params: dict | None = None) -> dict:
        return self._httpx_request("GET", path, params=params)

    def _post(self, path: str, payload: Any | None = None, params: dict | None = None) -> dict:
        return self._httpx_request("POST", path, json=payload, params=params)

    def connect(self) -> dict:
        parsed = urlparse(self.base_url)
        host = parsed.hostname or "localhost"
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        if _ibkr_is_local_url(self.base_url) and not is_tcp_port_listening(host, port, timeout=0.25):
            IBKR_LOG.info("Client Portal ignored; not listening on %s:%s", host, port)
            return ibkr_result(
                False,
                source=self.source,
                mode=self.mode,
                error="Client Portal Gateway is unavailable; continuing with IB Gateway/TWS socket only.",
                details={"code": "client_portal_not_listening", "base_url": self.base_url},
            )
        status = self.auth_status()
        if not status.get("_ok"):
            IBKR_LOG.info("Client Portal unavailable: %s", status.get("error"))
            return status
        data = status.get("data") or {}
        authenticated = bool(data.get("authenticated") or data.get("connected"))
        if authenticated:
            IBKR_LOG.info("Selected Client Portal REST transport")
            return ibkr_result(True, source=self.source, mode=self.mode, data=data, details={"base_url": self.base_url, "authenticated": True})
        return ibkr_result(
            False,
            source=self.source,
            mode=self.mode,
            error="Client Portal is reachable but login is required or the session expired.",
            details={"code": "login_required", "base_url": self.base_url, "auth_status": data},
        )

    def status(self) -> dict:
        return self.auth_status()

    def auth_status(self) -> dict:
        return self._get("/iserver/auth/status")

    def get_accounts(self) -> dict:
        primary = self._get("/portfolio/accounts")
        if primary.get("_ok") and primary.get("data"):
            return primary
        fallback = self._get("/iserver/accounts")
        if fallback.get("_ok"):
            fallback.setdefault("details", {})["fallback_from"] = "/portfolio/accounts"
            return fallback
        return primary if not fallback.get("_ok") else fallback

    def _first_account_id(self) -> str | None:
        accounts = self.get_accounts()
        return _extract_ibkr_account_ids(accounts)[0] if _extract_ibkr_account_ids(accounts) else None

    def get_account_summary(self, account_id: str | None = None) -> dict:
        acct = account_id or self._first_account_id()
        if not acct:
            return ibkr_result(False, source=self.source, mode=self.mode, error="No account id available.", details={"code": "missing_account"})
        res = self._get(f"/portfolio/{acct}/summary")
        res["source"] = self.source
        res["mode"] = self.mode
        res.setdefault("details", {})["account_id_masked"] = _ibkr_mask_account_id(acct)
        return res

    def get_positions(self, account_id: str | None = None) -> dict:
        acct = account_id or self._first_account_id()
        if not acct:
            return ibkr_result(False, source=self.source, mode=self.mode, error="No account id available.", details={"code": "missing_account"})
        res = self._get(f"/portfolio/{acct}/positions/0")
        res["source"] = self.source
        res["mode"] = self.mode
        res.setdefault("details", {})["account_id_masked"] = _ibkr_mask_account_id(acct)
        return res

    def get_open_orders(self) -> dict:
        return self._get("/iserver/account/orders", params={"force": "true"})

    def _secdef_search(self, symbol: str) -> dict:
        return self._get("/iserver/secdef/search", params={"symbol": symbol.upper(), "name": "true", "secType": "STK"})

    def get_market_data(self, symbol: str) -> dict:
        symbol = (symbol or "").strip().upper()
        if not symbol:
            return ibkr_result(False, source=self.source, mode=self.mode, error="Symbol is required.", details={"code": "invalid_symbol"})
        search = self._secdef_search(symbol)
        if not search.get("_ok"):
            return search
        rows = search.get("data") or []
        if not isinstance(rows, list) or not rows:
            return ibkr_result(False, source=self.source, mode=self.mode, error=f"Unsupported or unknown symbol: {symbol}", details={"code": "unsupported_asset"})
        conid = rows[0].get("conid") if isinstance(rows[0], dict) else None
        if not conid:
            return ibkr_result(False, source=self.source, mode=self.mode, error=f"No conid found for {symbol}.", details={"code": "missing_conid", "search": rows[:3]})
        snap = self._get("/iserver/marketdata/snapshot", params={"conids": str(conid), "fields": "31,84,86,88,7059"})
        snap.setdefault("details", {})["symbol"] = symbol
        snap.setdefault("details", {})["conid"] = conid
        return snap

    def place_order(
        self,
        symbol: str,
        action: str,
        quantity: float,
        order_type: str = "MKT",
        limit_price: float | None = None,
    ) -> dict:
        acct = self._first_account_id()
        if not acct:
            return ibkr_result(False, source=self.source, mode=self.mode, error="No account id available.", details={"code": "missing_account"})
        search = self._secdef_search(symbol)
        if not search.get("_ok"):
            return search
        rows = search.get("data") or []
        conid = rows[0].get("conid") if isinstance(rows, list) and rows and isinstance(rows[0], dict) else None
        if not conid:
            return ibkr_result(False, source=self.source, mode=self.mode, error=f"No conid found for {symbol}.", details={"code": "missing_conid"})
        ticket = IBKROrderTicket(
            conid=int(conid),
            side=action.upper(),
            quantity=float(quantity),
            order_type=order_type.upper(),
            price=float(limit_price) if limit_price is not None else None,
        ).to_payload()
        res = self._post(f"/iserver/account/{acct}/orders", payload=[ticket])
        res.setdefault("details", {})["confirmation_note"] = (
            "Client Portal may return messageId/id warnings that require POST /iserver/reply/{messageId} "
            "with {'confirmed': true}. This engine does not auto-confirm risk prompts."
        )
        res.setdefault("details", {})["account_id_masked"] = _ibkr_mask_account_id(acct)
        return res

    def cancel_order(self, order_id: int | str) -> dict:
        acct = self._first_account_id()
        if not acct:
            return ibkr_result(False, source=self.source, mode=self.mode, error="No account id available.", details={"code": "missing_account"})
        url = f"{self.base_url}/iserver/account/{acct}/order/{order_id}"
        verify = not _ibkr_is_local_url(self.base_url)
        try:
            import httpx  # type: ignore
            with httpx.Client(verify=verify, timeout=float(self.config.timeout_seconds), trust_env=False) as client:
                response = client.delete(url)
            try:
                data = response.json()
            except Exception:
                data = {"raw": response.text}
            return ibkr_result(response.is_success, source=self.source, mode=self.mode, data=data, error=None if response.is_success else f"HTTP {response.status_code}")
        except Exception as exc:
            return self._request_error(exc, f"/iserver/account/{acct}/order/{order_id}")


def _extract_ibkr_account_ids(payload: dict | list | None) -> list[str]:
    """Extract account ids from either unified or legacy IBKR responses."""
    if payload is None:
        return []
    data: Any = payload.get("data") if isinstance(payload, dict) else payload
    ids: list[str] = []

    def add(value: Any) -> None:
        if value is None:
            return
        text = str(value).strip()
        if text and text not in ids:
            ids.append(text)

    if isinstance(data, list):
        for item in data:
            if isinstance(item, str):
                add(item)
            elif isinstance(item, dict):
                add(item.get("accountId") or item.get("account_id") or item.get("id") or item.get("account"))
    elif isinstance(data, dict):
        for key in ("accountId", "account_id", "selectedAccount", "account", "id"):
            add(data.get(key))
        for key in ("accounts", "acctIds", "accountIds"):
            nested = data.get(key)
            if isinstance(nested, list):
                for item in nested:
                    if isinstance(item, str):
                        add(item)
                    elif isinstance(item, dict):
                        add(item.get("accountId") or item.get("id") or item.get("account"))
    return ids


class IBKRConnectionManager:
    """Auto-detect and select the best available IBKR transport."""

    def __init__(self, config: IBKREngineConfig):
        self.config = config
        self.selected: IBKRSocketClient | IBKRClientPortalTransport | None = None
        self.attempts: list[dict] = []

    def connect(self) -> dict:
        if self.selected is not None:
            status = self.selected.status()
            if status.get("_ok"):
                return status
        self.attempts = []
        for idx, port in enumerate(self.config.socket_ports):
            client = IBKRSocketClient(self.config, port=int(port), client_id=self.config.client_id + idx)
            res = client.connect()
            self.attempts.append({
                "source": "ib_insync",
                "mode": _ibkr_mode_for_port(int(port)),
                "transport": _ibkr_transport_label(int(port)),
                "port": int(port),
                "_ok": bool(res.get("_ok")),
                "error": res.get("error"),
            })
            if res.get("_ok"):
                self.selected = client
                res.setdefault("details", {})["attempts"] = self.attempts
                return res

        cp = IBKRClientPortalTransport(self.config)
        res = cp.connect()
        self.attempts.append({
            "source": "client_portal",
            "mode": "unknown",
            "transport": "Client Portal Gateway REST",
            "base_url": self.config.client_portal_base,
            "_ok": bool(res.get("_ok")),
            "error": res.get("error"),
        })
        if res.get("_ok"):
            self.selected = cp
            res.setdefault("details", {})["attempts"] = self.attempts
            return res

        browser_session_note = (
            "Browser-authenticated Client Portal sessions are detected through /iserver/auth/status. "
            "Open https://localhost:5000 and sign in if Client Portal is running."
        )
        return ibkr_result(
            False,
            source="none",
            mode="unknown",
            error="لا يوجد اتصال IBKR نشط الآن. لم يستجب أي منفذ Socket أو جلسة Client Portal مصادقة.",
            details={
                "attempts": self.attempts,
                "priority": [
                    "127.0.0.1:4002 Gateway paper",
                    "127.0.0.1:4001 Gateway live",
                    "127.0.0.1:7497 TWS paper",
                    "127.0.0.1:7496 TWS live",
                    self.config.client_portal_base,
                ],
                "next_steps": [
                    "Socket: شغّل TWS أو IB Gateway وفعل API من Global Configuration > API > Settings.",
                    "Client Portal: افتح https://localhost:5000 وسجّل الدخول، ثم انتظر حتى تصبح auth status = authenticated.",
                    "إذا كان Client Portal مفتوحًا لكنه بطيء، أعد تشغيله ثم جرّب Fetch accounts.",
                ],
                "browser_session_detection": browser_session_note,
            },
        )

    def disconnect(self) -> dict:
        if self.selected is None:
            return ibkr_result(True, source="none", mode="unknown", data={"connected": False, "status": "already_disconnected"})
        res = self.selected.disconnect() if hasattr(self.selected, "disconnect") else ibkr_result(True, source=self.selected.source, mode=self.selected.mode, data={"connected": False})
        self.selected = None
        return res

    def status(self) -> dict:
        if self.selected is None:
            return ibkr_result(
                False,
                source="none",
                mode="unknown",
                error="Not connected.",
                details={"attempts": self.attempts, "socket_ports": list(self.config.socket_ports), "client_portal_base": self.config.client_portal_base},
            )
        return self.selected.status()

    def client_or_connect(self) -> tuple[IBKRSocketClient | IBKRClientPortalTransport | None, dict]:
        if self.selected is not None:
            status = self.selected.status()
            if status.get("_ok"):
                return self.selected, status
        connected = self.connect()
        return (self.selected, connected) if connected.get("_ok") else (None, connected)


class IBKR:
    """Unified, Streamlit-safe public IBKR API."""

    def __init__(self, config: IBKREngineConfig | None = None):
        self.config = config or load_ibkr_config()
        self.risk = IBKRRiskManager(self.config)
        self.connection_manager = IBKRConnectionManager(self.config)

    def connect(self) -> dict:
        try:
            return self.connection_manager.connect()
        except Exception as exc:
            IBKR_LOG.exception("IBKR connect failed")
            return ibkr_result(False, error=str(exc), details={"code": "connect_unhandled"})

    def disconnect(self) -> dict:
        try:
            return self.connection_manager.disconnect()
        except Exception as exc:
            return ibkr_result(False, error=str(exc), details={"code": "disconnect_unhandled"})

    def status(self) -> dict:
        try:
            return self.connection_manager.status()
        except Exception as exc:
            return ibkr_result(False, error=str(exc), details={"code": "status_unhandled"})

    def _client(self) -> tuple[IBKRSocketClient | IBKRClientPortalTransport | None, dict]:
        return self.connection_manager.client_or_connect()

    def get_account(self) -> dict:
        try:
            client, state = self._client()
            if client is None:
                return state
            return client.get_accounts()
        except Exception as exc:
            return ibkr_result(False, error=str(exc), details={"code": "account_unhandled"})

    def get_accounts(self) -> dict:
        return self.get_account()

    def get_account_summary(self, account_id: str | None = None) -> dict:
        try:
            client, state = self._client()
            if client is None:
                return state
            return client.get_account_summary(account_id)
        except Exception as exc:
            return ibkr_result(False, error=str(exc), details={"code": "summary_unhandled"})

    def get_positions(self, account_id: str | None = None) -> dict:
        try:
            client, state = self._client()
            if client is None:
                return state
            return client.get_positions(account_id)
        except Exception as exc:
            return ibkr_result(False, error=str(exc), details={"code": "positions_unhandled"})

    def get_open_orders(self) -> dict:
        try:
            client, state = self._client()
            if client is None:
                return state
            return client.get_open_orders()
        except Exception as exc:
            return ibkr_result(False, error=str(exc), details={"code": "open_orders_unhandled"})

    def get_market_data(self, symbol: str) -> dict:
        try:
            if not (symbol or "").strip():
                return ibkr_result(False, error="Symbol is required.", details={"code": "invalid_symbol"})
            client, state = self._client()
            if client is None:
                return state
            return client.get_market_data(symbol)
        except Exception as exc:
            return ibkr_result(False, error=str(exc), details={"code": "market_data_unhandled"})

    def qualify_contract(self, symbol: str, exchange: str = "SMART", currency: str = "USD") -> dict:
        try:
            client, state = self._client()
            if client is None:
                return state
            if isinstance(client, IBKRSocketClient):
                return client.qualify_contract(symbol, exchange=exchange, currency=currency)
            search = client._secdef_search((symbol or "").strip().upper())
            return search
        except Exception as exc:
            return ibkr_result(False, error=str(exc), details={"code": "qualify_unhandled"})

    def place_order(
        self,
        symbol: str,
        action: str,
        quantity: float,
        order_type: str = "MKT",
        limit_price: float | None = None,
    ) -> dict:
        try:
            mode = self.connection_manager.selected.mode if self.connection_manager.selected else "unknown"
            validation = self.risk.validate_order(symbol, action, quantity, mode=mode, order_type=order_type, limit_price=limit_price)
            if not validation.get("_ok"):
                IBKR_LOG.info("Order blocked: %s", validation.get("details", {}).get("code"))
                return validation
            client, state = self._client()
            if client is None:
                return state
            validation = self.risk.validate_order(symbol, action, quantity, mode=client.mode, order_type=order_type, limit_price=limit_price)
            if not validation.get("_ok"):
                IBKR_LOG.info("Order blocked after transport selection: %s", validation.get("details", {}).get("code"))
                return validation
            if isinstance(client, IBKRSocketClient):
                if order_type.upper() == "LMT":
                    return client.place_limit_order(symbol, action, quantity, float(limit_price))
                return client.place_market_order(symbol, action, quantity)
            return client.place_order(symbol, action, quantity, order_type=order_type, limit_price=limit_price)
        except Exception as exc:
            IBKR_LOG.exception("IBKR order failed")
            return ibkr_result(False, error=str(exc), details={"code": "order_unhandled"})

    def place_market_order(self, symbol: str, action: str, quantity: float) -> dict:
        return self.place_order(symbol, action, quantity, order_type="MKT")

    def place_limit_order(self, symbol: str, action: str, quantity: float, limit_price: float) -> dict:
        return self.place_order(symbol, action, quantity, order_type="LMT", limit_price=limit_price)

    def cancel_order(self, order_id: int | str) -> dict:
        try:
            client, state = self._client()
            if client is None:
                return state
            return client.cancel_order(order_id)
        except Exception as exc:
            return ibkr_result(False, error=str(exc), details={"code": "cancel_unhandled"})


@st.cache_resource
def get_ibkr_engine(
    client_portal_base: str | None = None,
    host: str | None = None,
    socket_ports_raw: str | None = None,
    client_id: int | None = None,
) -> IBKR:
    cfg = load_ibkr_config()
    if client_portal_base or host or socket_ports_raw or client_id is not None:
        cfg = IBKREngineConfig(
            host=host or cfg.host,
            socket_ports=_ibkr_parse_ports(socket_ports_raw) if socket_ports_raw else cfg.socket_ports,
            client_id=int(client_id if client_id is not None else cfg.client_id),
            client_portal_base=(client_portal_base or cfg.client_portal_base).rstrip("/"),
            enable_order_placement=cfg.enable_order_placement,
            allow_live_trading=cfg.allow_live_trading,
            max_order_qty=cfg.max_order_qty,
            max_notional_usd=cfg.max_notional_usd,
            timeout_seconds=cfg.timeout_seconds,
            log_level=cfg.log_level,
        )
    return IBKR(cfg)


# ---- Streamlit-cached helpers (so the IBKR client survives reruns) ----
@st.cache_resource
def get_ibkr_client(
    base_url: str = IBKR_DEFAULT_BASE_URL,
    ws_url: str = IBKR_DEFAULT_WS_URL,
    verify_ssl: bool = False,
) -> IBKRClient:
    return IBKRClient(base_url=base_url, ws_url=ws_url, verify_ssl=verify_ssl)


def render_ibkr_tab() -> None:
    """Interactive Brokers — auth, accounts, quick quote, Buy/Sell, P&L."""
    st.markdown(
        '<div class="section-header"><h3>🏦 Interactive Brokers · Live Trading</h3>'
        '<span class="section-count">Client Portal REST · TWS / Gateway socket</span></div>',
        unsafe_allow_html=True,
    )

    # ---- Quick launchers ----
    lc1, lc2, lc3, lc4 = st.columns(4)
    if lc1.button("🚀 Start IB Gateway / TWS", key="ibkr_launch_tws"):
        res = launch_ib_gateway()
        (st.success if res.get("_ok") else st.error)(res.get("_message", "—"))
    if lc2.button("🌐 Start Client Portal", key="ibkr_launch_cp"):
        res = launch_clientportal_gateway()
        (st.success if res.get("_ok") else st.error)(res.get("_message", "—"))
    if lc3.button("📡 Probe :5000", key="ibkr_probe"):
        probe = get_ibkr_client().tickle()
        if probe.get("_ok"):
            st.success("🟢 Client Portal gateway responding on :5000")
        else:
            st.warning(f"🔴 {probe.get('_error_hint') or 'No response'}")
    if lc4.button("🔌 Probe :4002 (TWS API)", key="ibkr_probe_tws"):
        ok_ibi, _ = ib_insync_available()
        if not ok_ibi:
            st.error("ib_insync not installed — `pip install ib_insync nest_asyncio`")
        else:
            res = ib_insync_connect(port=IB_INSYNC_DEFAULT_PAPER_PORT)
            (st.success if res.get("_ok") else st.warning)(
                f"🟢 TWS API connected · server v{res.get('server_version')}" if res.get("_ok")
                else f"🔴 {res.get('_message', '')}"
            )

    with st.expander("Connection settings", expanded=False):
        cc1, cc2, cc3 = st.columns([2, 2, 1])
        base_url = cc1.text_input("Base URL", value=os.getenv("IBKR_BASE_URL", IBKR_DEFAULT_BASE_URL), key="ibkr_base_url")
        ws_url = cc2.text_input("WebSocket URL", value=os.getenv("IBKR_WS_URL", IBKR_DEFAULT_WS_URL), key="ibkr_ws_url")
        verify_ssl = cc3.checkbox("Verify SSL", value=False, key="ibkr_verify_ssl")
        st.caption(
            "Run the Client Portal Gateway on `https://localhost:5000` and sign in once via the browser. "
            "**Rate limits:** ~10 req/s global · `/iserver/orders` & `/iserver/trades` = 1 req per 5s "
            "(enforced automatically)."
        )

    client = get_ibkr_client(base_url=base_url, ws_url=ws_url, verify_ssl=verify_ssl)
    engine = get_ibkr_engine(
        client_portal_base=base_url,
        host=os.getenv("IBKR_HOST", "127.0.0.1"),
        socket_ports_raw=os.getenv("IBKR_SOCKET_PORTS", ""),
        client_id=_ibkr_int_env("IBKR_CLIENT_ID", 11),
    )

    # ===== Session / Auth =====
    a1, a2, a3, a4, a5 = st.columns(5)
    if a1.button("🟢 Connect", key="ibkr_action_connect", type="primary"):
        st.session_state["ibkr_action_last"] = ("connect", engine.connect())
    if a2.button("🔄 Tickle", key="ibkr_tickle"):
        st.session_state["ibkr_last_call"] = ("tickle", client.tickle())
    if a3.button("✅ Auth status", key="ibkr_auth"):
        st.session_state["ibkr_last_call"] = ("auth_status", client.auth_status())
    if a4.button("🔁 Re-auth", key="ibkr_reauth"):
        st.session_state["ibkr_last_call"] = ("reauthenticate", client.reauthenticate())
    if a5.button("🚪 Logout", key="ibkr_logout"):
        st.session_state["ibkr_last_call"] = ("logout", client.logout())

    last = st.session_state.get("ibkr_last_call")
    if last:
        op, payload = last
        payload = payload or {}
        ok = bool(payload.get("_ok"))
        status = payload.get("_status", "?")
        if ok:
            badge, label = "🟢", f"HTTP {status}"
        elif status == 0:
            badge = "🔴"
            label = {"timeout": "Client Portal timeout", "connection": "Client Portal offline",
                      "ssl": "SSL check failed"}.get(payload.get("_error", ""),
                                                      f"Unavailable · {payload.get('_error', 'error')}")
        else:
            badge, label = "🟠", f"HTTP {status}"
        st.markdown(
            f'<div class="panel"><div class="panel-title">Last call · {escape(str(op))}</div>'
            f'<div class="panel-big">{badge} {escape(label)}</div></div>',
            unsafe_allow_html=True,
        )
        if payload.get("_error_hint"):
            st.warning(payload["_error_hint"])
        with st.expander("Response JSON", expanded=False):
            st.json(payload)

    st.markdown("---")

    # ============================================================
    # 📒 Accounts & Portfolio  — with custom account ID support
    # ============================================================
    st.markdown("#### 📒 Accounts & Portfolio")
    if st.button("Fetch accounts", key="ibkr_fetch_accounts"):
        accs = engine.get_account()
        st.session_state["ibkr_accounts"] = accs
        if not accs.get("_ok"):
            st.warning(f"⚠️ {accs.get('error') or 'No IBKR transport available.'}")

    accs = st.session_state.get("ibkr_accounts")
    fetched_ids: list[str] = _extract_ibkr_account_ids(accs) if (accs and accs.get("_ok")) else []

    # Account selector (with manual override box for IDs like U22163499)
    sa1, sa2 = st.columns([2, 2])
    options = (fetched_ids or []) + ["+ Custom…"]
    default_idx = 0
    if "ibkr_account_id" in st.session_state and st.session_state["ibkr_account_id"] in options:
        default_idx = options.index(st.session_state["ibkr_account_id"])
    selected = sa1.selectbox("Active account", options, index=default_idx, key="ibkr_account_select",
                              help="Pick a fetched account or type a custom ID (e.g. U22163499)")
    if selected == "+ Custom…":
        custom_id = sa2.text_input("Custom account ID", value=st.session_state.get("ibkr_account_custom", ""),
                                    placeholder="e.g. U22163499", key="ibkr_account_custom")
        account_id = custom_id.strip() or None
    else:
        sa2.markdown(f"<div style='padding-top:1.85rem;'><code>{escape(selected or '—')}</code></div>",
                     unsafe_allow_html=True)
        account_id = selected
    if account_id:
        st.session_state["ibkr_account_id"] = account_id

    # Account quick-actions
    if account_id:
        ac1, ac2, ac3, ac4 = st.columns(4)
        if ac1.button("Summary", key="ibkr_acc_summary"):
            st.json(engine.get_account_summary(account_id))
        if ac2.button("Ledger", key="ibkr_acc_ledger"):
            st.json(client.account_ledger(account_id))
        if ac3.button("Positions", key="ibkr_acc_positions"):
            st.session_state["ibkr_positions_cache"] = engine.get_positions(account_id)
        if ac4.button("PnL partitioned", key="ibkr_acc_pnl"):
            st.session_state["ibkr_pnl_cache"] = client.pnl_partitioned()

    if accs and not accs.get("_ok"):
        with st.expander("Connection diagnostics", expanded=False):
            st.json(accs)

    st.markdown("---")

    # ============================================================
    # 📊 P&L: Daily / Monthly / YTD / Annual + Allocation
    # ============================================================
    st.markdown("#### 📊 Performance & Allocation")
    pnl_cache = st.session_state.get("ibkr_pnl_cache")
    pos_cache = st.session_state.get("ibkr_positions_cache")
    pn1, pn2, pn3, pn4 = st.columns(4)
    if pn1.button("Refresh P&L", key="ibkr_pnl_refresh"):
        st.session_state["ibkr_pnl_cache"] = client.pnl_partitioned()
        pnl_cache = st.session_state["ibkr_pnl_cache"]
    if pn2.button("Daily", key="ibkr_perf_d") and account_id:
        st.session_state["ibkr_perf_cache"] = client.pa_performance([account_id], freq="D")
    if pn3.button("Monthly", key="ibkr_perf_m") and account_id:
        st.session_state["ibkr_perf_cache"] = client.pa_performance([account_id], freq="M")
    if pn4.button("Yearly", key="ibkr_perf_y") and account_id:
        st.session_state["ibkr_perf_cache"] = client.pa_performance([account_id], freq="Y")

    # Render P&L metrics
    pnl_metrics = _ibkr_extract_pnl_metrics(pnl_cache)
    if pnl_metrics:
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Daily P&L", _fmt_money(pnl_metrics.get("dpl")), _fmt_pct(pnl_metrics.get("dpl_pct")))
        m2.metric("Unrealized", _fmt_money(pnl_metrics.get("upl")), _fmt_pct(pnl_metrics.get("upl_pct")))
        m3.metric("Realized", _fmt_money(pnl_metrics.get("rpl")), _fmt_pct(pnl_metrics.get("rpl_pct")))
        m4.metric("Net Liq", _fmt_money(pnl_metrics.get("nl")))
        m5.metric("Excess Liq", _fmt_money(pnl_metrics.get("el")))
    else:
        st.caption("Press *Refresh P&L* after connecting to see daily / unrealized / realized P&L metrics.")

    # Performance series chart (D/M/Y)
    perf_cache = st.session_state.get("ibkr_perf_cache")
    if perf_cache and perf_cache.get("_ok"):
        with st.expander("Performance series", expanded=False):
            st.json(perf_cache)

    # Allocation table
    pos_rows = _ibkr_positions_to_rows(pos_cache)
    if pos_rows:
        try:
            df_pos = pd.DataFrame(pos_rows)
            total_value = float(df_pos["market_value"].sum())
            if total_value > 0:
                df_pos["allocation_%"] = (df_pos["market_value"] / total_value * 100.0).round(2)
            df_pos = df_pos.sort_values("market_value", ascending=False, na_position="last")
            st.markdown("##### Allocation by position")
            st.dataframe(df_pos, width="stretch", hide_index=True, height=320)
            # Pie chart
            try:
                import plotly.express as _px
                fig = _px.pie(df_pos.head(15), values="market_value", names="symbol",
                              title=f"Top 15 holdings · total ${total_value:,.0f}")
                fig.update_layout(margin=dict(l=10, r=10, t=40, b=10), height=380)
                st.plotly_chart(fig, width="stretch", key="ibkr_alloc_pie")
            except Exception:
                pass
        except Exception as exc:
            st.caption(f"Allocation calc failed: {exc}")

    st.markdown("---")

    # ============================================================
    # 💱 Buy/Sell  (renamed from "Place Order")
    # ============================================================
    st.markdown("#### 💱 Buy / Sell")

    # ---- 2. Live price quote on symbol input ----
    qc1, qc2 = st.columns([2, 1])
    o_symbol_raw = qc1.text_input("Symbol", value=st.session_state.get("ibkr_o_symbol", "AAPL"),
                                   key="ibkr_o_symbol",
                                   help="Type a US stock symbol — quote auto-fetches when you click Get quote")
    o_symbol = (o_symbol_raw or "").strip().upper()
    if qc2.button("📈 Get quote", key="ibkr_quote_btn"):
        with st.spinner(f"Fetching quote for {o_symbol}…"):
            md = engine.get_market_data(o_symbol) if o_symbol else None
        st.session_state["ibkr_quote_cache"] = (o_symbol, md)

    quote_cache = st.session_state.get("ibkr_quote_cache")
    if quote_cache and quote_cache[0] == o_symbol:
        _, md = quote_cache
        snap = (md or {}).get("data") or {}
        if md and md.get("_ok") and snap:
            qm1, qm2, qm3, qm4, qm5 = st.columns(5)
            qm1.metric("Last", _fmt_money(snap.get("last") or snap.get("close")))
            qm2.metric("Bid", _fmt_money(snap.get("bid")))
            qm3.metric("Ask", _fmt_money(snap.get("ask")))
            qm4.metric("Volume", _fmt_int(snap.get("volume")))
            qm5.metric("Day High/Low", f"{_fmt_money(snap.get('high'))} / {_fmt_money(snap.get('low'))}")
        elif md:
            st.caption(f"Quote unavailable: {md.get('error') or 'no data'}")

    # ---- 3. Expanded order types ----
    o1, o2, o3, o4 = st.columns([1, 1, 1, 2])
    o_side = o1.selectbox("Side", ["BUY", "SELL"], key="ibkr_o_side")
    o_qty = o2.number_input("Qty", min_value=1, value=1, step=1, key="ibkr_o_qty")
    order_types = ["MKT", "LMT", "STP", "STP_LMT", "MIDPRICE", "TRAIL", "TRAIL_LIMIT", "MOC", "LOC", "MOO", "LOO"]
    o_type = o3.selectbox("Type", order_types, key="ibkr_o_type",
                          help="MKT/LMT = market/limit · STP = stop · STP_LMT = stop-limit · "
                               "MIDPRICE = midpoint peg · TRAIL = trailing · MOC/LOC = market/limit on close · "
                               "MOO/LOO = market/limit on open")
    o_tif = o4.selectbox("Time-in-force", ["DAY", "GTC", "IOC", "OPG", "FOK"], key="ibkr_o_tif",
                         help="DAY = today · GTC = good till cancelled · IOC = immediate or cancel · "
                              "OPG = at the opening · FOK = fill or kill")

    # Conditional price inputs
    o_price = None
    o_aux = None
    needs_lmt = o_type in ("LMT", "STP_LMT", "TRAIL_LIMIT", "LOC", "LOO")
    needs_aux = o_type in ("STP", "STP_LMT", "TRAIL", "TRAIL_LIMIT")
    if needs_lmt or needs_aux:
        oc1, oc2 = st.columns(2)
        if needs_lmt:
            o_price = oc1.number_input("Limit price",
                                        min_value=0.0, value=float(snap.get("last") or 0.0) if quote_cache else 0.0,
                                        step=0.01, key="ibkr_o_price")
        if needs_aux:
            label = "Trailing amount" if o_type in ("TRAIL", "TRAIL_LIMIT") else "Stop price"
            o_aux = oc2.number_input(label, min_value=0.0, value=0.0, step=0.01, key="ibkr_o_aux")

    o_outside_rth = st.checkbox("Allow outside regular trading hours", value=False, key="ibkr_o_orth")

    # Submit / Confirm / Cancel
    op1, op2, op3 = st.columns([2, 1, 1])
    if op1.button(f"🚀 {o_side} {o_qty} {o_symbol}", key="ibkr_o_submit", type="primary",
                   disabled=not (o_symbol and account_id)):
        try:
            res = engine.place_order(
                o_symbol, o_side, float(o_qty),
                order_type=o_type,
                limit_price=float(o_price) if o_price else None,
            )
            st.session_state["ibkr_last_order"] = res
            if res.get("_ok"):
                st.success(f"✅ Order routed · {res.get('source', 'engine')}")
            else:
                st.warning(res.get("error") or "Order blocked by safety layer.")
            st.json(res)
        except Exception as exc:
            st.error(f"Order failed: {exc}")
    if op2.button("Confirm last reply", key="ibkr_confirm_btn"):
        last_order = st.session_state.get("ibkr_last_order") or {}
        data = last_order.get("data") if isinstance(last_order, dict) else None
        msg_id = None
        if isinstance(data, list) and data:
            msg_id = (data[0] or {}).get("id") or (data[0] or {}).get("messageId")
        if not msg_id:
            st.warning("No pending messageId on the last order.")
        else:
            st.json(client.confirm_reply(msg_id))
    cancel_id = op3.text_input("Cancel orderId", key="ibkr_cancel_id", placeholder="orderId")
    if cancel_id and account_id and st.button("Cancel", key="ibkr_cancel_btn"):
        st.json(client.cancel_order(account_id, cancel_id))

    st.markdown("---")

    # ============================================================
    # 📜 Orders & Trades
    # ============================================================
    st.markdown("#### 📜 Orders & Trades")
    t1, t2, t3 = st.columns(3)
    if t1.button("Live orders", key="ibkr_live_orders"):
        st.json(client.live_orders())
    if t2.button("Filled (force)", key="ibkr_orders_filled"):
        st.json(client.orders(filters="filled", force=True, account_id=account_id))
    if t3.button("Recent trades", key="ibkr_trades"):
        st.json(client.trades())

    st.markdown("---")

    # ============================================================
    # 🔔 FYI Notifications
    # ============================================================
    st.markdown("#### 🔔 FYI Notifications")
    f1, f2, f3, f4 = st.columns(4)
    if f1.button("Unread count", key="ibkr_fyi_unread"):
        st.json(client.fyi_unread())
    if f2.button("Recent", key="ibkr_fyi_list"):
        st.json(client.fyi_notifications())
    if f3.button("More (older)", key="ibkr_fyi_more"):
        st.json(client.fyi_notifications_more())
    if f4.button("Settings", key="ibkr_fyi_settings"):
        st.json(client.fyi_settings())

    st.markdown("---")

    # ============================================================
    # 🔌 ib_insync transport
    # ============================================================
    st.markdown(
        '<div class="section-header"><h3>🔌 ib_insync · TWS / Gateway</h3>'
        '<span class="section-count">Native socket — Gateway live (4001) · paper (4002) · TWS (7496/7497)</span></div>',
        unsafe_allow_html=True,
    )
    ok_ibi, ibi_msg = ib_insync_available()
    if not ok_ibi:
        st.warning(f"⚠️ {ibi_msg}")
        st.code("pip install ib_insync nest_asyncio", language="bash")
    else:
        st.caption(f"✅ ib_insync v{ibi_msg} · event loop ready")

    pp1, pp2, pp3, pp4 = st.columns(4)
    if pp1.button("🟢 Gateway live (4001)", key="ibi_preset_glive"):
        st.session_state["ibi_port"] = 4001
    if pp2.button("🧪 Gateway paper (4002)", key="ibi_preset_gpaper"):
        st.session_state["ibi_port"] = 4002
    if pp3.button("💻 TWS live (7496)", key="ibi_preset_twslive"):
        st.session_state["ibi_port"] = 7496
    if pp4.button("📘 TWS paper (7497)", key="ibi_preset_twspaper"):
        st.session_state["ibi_port"] = 7497

    ic1, ic2, ic3, ic4 = st.columns([2, 1, 1, 1])
    ibi_host = ic1.text_input("Host", value=IB_INSYNC_DEFAULT_HOST, key="ibi_host")
    port_options = [IB_INSYNC_DEFAULT_LIVE_PORT, IB_INSYNC_DEFAULT_PAPER_PORT, 7496, 7497]
    port_labels = {
        4001: "4001 · Gateway live ⚠️ REAL", 4002: "4002 · Gateway paper",
        7496: "7496 · TWS live ⚠️ REAL",     7497: "7497 · TWS paper",
    }
    default_port = st.session_state.get("ibi_port", IB_INSYNC_DEFAULT_PAPER_PORT)
    if default_port not in port_options:
        port_options.insert(0, default_port)
        port_labels[default_port] = f"{default_port} · custom"
    ibi_port = ic2.selectbox("Port", port_options, index=port_options.index(default_port),
                              format_func=lambda p: port_labels.get(p, str(p)), key="ibi_port")
    ibi_cid = ic3.number_input("Client ID", min_value=1, max_value=999,
                                value=IB_INSYNC_DEFAULT_CLIENT_ID, key="ibi_cid")
    if ic4.button("🔌 Connect", key="ibi_connect_btn", type="primary"):
        if int(ibi_port) in (4001, 7496):
            st.warning("⚠️ Connecting to a **LIVE** trading port — orders will use real money!")
        with st.spinner(f"Connecting to {ibi_host}:{int(ibi_port)} …"):
            res = ib_insync_connect(host=ibi_host, port=int(ibi_port), client_id=int(ibi_cid))
        if res.get("_ok"):
            st.success(f"✅ Connected to {res.get('transport', '?')} · server v{res.get('server_version')}")
        else:
            st.error(f"❌ {res.get('_message', 'Connection failed')}")

    status = ib_insync_status()
    if status.get("connected"):
        st.markdown(
            f'<div class="panel"><div class="panel-title">ib_insync · status</div>'
            f'<div class="panel-big">🟢 Connected</div>'
            f'<div class="metric-meta">{status.get("host")}:{status.get("port")} · clientId={status.get("client_id")} · '
            f'server v{status.get("server_version")}</div></div>',
            unsafe_allow_html=True,
        )
        ib1, ib2, ib3, ib4 = st.columns(4)
        if ib1.button("Account summary", key="ibi_acc_sum"):
            st.json(ib_insync_call("accountSummary"))
        if ib2.button("Positions", key="ibi_positions"):
            st.json(ib_insync_call("positions"))
        if ib3.button("Open orders", key="ibi_open_orders"):
            st.json(ib_insync_call("openOrders"))
        if ib4.button("Disconnect", key="ibi_disconnect"):
            st.json(ib_insync_disconnect())
    else:
        st.caption("⚠️ Run TWS or IB Gateway, enable API, then click Connect.")

    st.markdown("---")

    # ============================================================
    # 🇸🇦 Sahm.sa — cooperative with FMP / Finnhub / Twelve / Alpha / Polygon / Yahoo
    # ============================================================
    st.markdown(
        '<div class="section-header"><h3>🇸🇦 Sahm.sa · Saudi & Global Quotes</h3>'
        '<span class="section-count">Cooperative: Sahm.sa + FMP + Finnhub + Twelve + Alpha + Polygon + Yahoo</span></div>',
        unsafe_allow_html=True,
    )
    sk1, sk2 = st.columns([3, 1])
    sahmk_key = sk1.text_input("Sahm.sa API key", value=os.getenv("SAHMK_API_KEY", SAHMK_DEFAULT_API_KEY),
                                type="password", key="sahmk_key")
    sahmk_base = sk2.text_input("Base URL", value=SAHMK_DEFAULT_BASE_URL, key="sahmk_base")
    sahmk_client = get_sahmk_client(api_key=sahmk_key, base_url=sahmk_base)

    sm1, sm2, sm3 = st.columns([2, 1, 1])
    saudi_symbol = sm1.text_input("Saudi symbol", value="2222", key="sahmk_symbol",
                                   help="2222 = Aramco · 1120 = Al Rajhi · 7010 = STC · 1180 = SNB")
    saudi_period = sm2.selectbox("Period", ["1mo", "3mo", "6mo", "1y", "5y"], index=3, key="sahmk_period")
    if sm3.button("📡 Cooperative fetch", key="sahmk_coop_btn"):
        with st.spinner("Fetching from Sahm.sa + global APIs…"):
            st.session_state["sahmk_coop"] = _sahmk_cooperative_fetch(
                saudi_symbol, sahmk_client,
                fmp_key=st.session_state.get("ibkr_fmp_key", ""),  # passed through bundle keys
            )

    coop = st.session_state.get("sahmk_coop")
    if coop:
        coverage = coop.get("coverage", {})
        cv_cols = st.columns(len(coverage) or 1)
        for i, (provider, status) in enumerate(coverage.items()):
            badge = "🟢" if status == "ok" else "🟠" if status == "partial" else "🔴"
            cv_cols[i].markdown(f"{badge} **{NEWS_PROVIDER_LABELS.get(provider, provider)}**<br><small>{status}</small>",
                                  unsafe_allow_html=True)
        with st.expander("Cooperative response", expanded=False):
            st.json(coop)

    sb1, sb2, sb3, sb4, sb5 = st.columns(5)
    if sb1.button("Quote", key="sahmk_q"):
        st.json(sahmk_client.quote(saudi_symbol))
    if sb2.button("History", key="sahmk_h"):
        st.json(sahmk_client.history(saudi_symbol, period=saudi_period))
    if sb3.button("Company", key="sahmk_co"):
        st.json(sahmk_client.company(saudi_symbol))
    if sb4.button("Financials", key="sahmk_fin"):
        st.json(sahmk_client.financials(saudi_symbol))
    if sb5.button("News", key="sahmk_news"):
        st.json(sahmk_client.news(saudi_symbol))

    si1, si2, si3 = st.columns(3)
    if si1.button("TASI Index", key="sahmk_tasi"):
        st.json(sahmk_client.index_quote("TASI"))
    if si2.button("All markets", key="sahmk_markets"):
        st.json(sahmk_client.markets())
    if si3.button("All companies", key="sahmk_list"):
        st.json(sahmk_client.companies())


# ============================================================
# Number formatters — driven by sidebar Number Format panel
# ============================================================
NUMBER_FORMAT_DEFAULTS: dict = {
    "currency_symbol": "$",
    "currency_position": "prefix",      # prefix | suffix
    "currency_style": "compact",        # compact | full | detailed
    "decimal_places": 2,
    "thousands_separator": ",",
    "decimal_separator": ".",
    "negative_style": "minus",          # minus | parentheses
    "compact_threshold": 10000.0,       # values >= this → use K/M/B
    "percent_decimals": 2,
    "percent_signed": True,
    "multiplier_decimals": 2,
}


def get_number_format() -> dict:
    """Read current number-format settings from session_state, with safe defaults."""
    settings = dict(NUMBER_FORMAT_DEFAULTS)
    user = st.session_state.get("number_format") or {}
    if isinstance(user, dict):
        settings.update({k: v for k, v in user.items() if k in NUMBER_FORMAT_DEFAULTS})
    return settings


def _wrap_negative(formatted: str, is_negative: bool, style: str) -> str:
    """Apply negative-number styling: minus prefix or parentheses."""
    if not is_negative:
        return formatted
    if style == "parentheses":
        # Strip any leading '-' first
        clean = formatted.lstrip("-")
        return f"({clean})"
    return formatted  # minus already in the formatted string


def _format_number_compact(value: float, decimals: int = 2) -> str:
    """Format 1,234,567 → 1.23M / 9,876 → 9.88K."""
    abs_v = abs(value)
    if abs_v >= 1e12:
        return f"{value/1e12:.{decimals}f}T"
    if abs_v >= 1e9:
        return f"{value/1e9:.{decimals}f}B"
    if abs_v >= 1e6:
        return f"{value/1e6:.{decimals}f}M"
    if abs_v >= 1e3:
        return f"{value/1e3:.{decimals}f}K"
    return f"{value:.{decimals}f}"


def fmt_money(value, currency: str | None = None, *, force_full: bool = False) -> str:
    """Format a monetary value using the active number-format settings.

    Hot path — called thousands of times per render. The fast cases (compact $
    with default separators) skip dict lookups and string scans entirely.

    Examples (depending on settings):
      compact:  $1.23M · $1.5B SAR · 5.4M $
      full:     $1,234,567.89 · 5,423,100 SAR
      detailed: $1,234,567.89 (always full + 2 decimals)
    """
    # Fast bail-out for the most common "no data" case
    if value is None or value == "":
        return "—"
    try:
        f = float(value)
    except (TypeError, ValueError):
        return "—"
    # NaN / inf guard
    if f != f or f == float("inf") or f == float("-inf"):
        return "—"

    s = get_number_format()
    sym = currency if currency is not None else s["currency_symbol"]
    decimals = s["decimal_places"]
    is_neg = f < 0
    abs_v = -f if is_neg else f
    style = s["currency_style"]

    # Choose representation
    if force_full or style == "detailed" or style == "full":
        body = f"{abs_v:,.{decimals}f}"
    elif abs_v >= s["compact_threshold"]:
        body = _format_number_compact(abs_v, decimals)
    else:
        body = f"{abs_v:,.{decimals}f}"

    # Custom thousands / decimal separators (only swap if non-default — saves
    # two string scans on the hot path)
    thou_sep = s["thousands_separator"]
    dec_sep = s["decimal_separator"]
    if thou_sep != "," or dec_sep != ".":
        # Single-pass swap via translate (faster than two .replace calls)
        body = body.translate(str.maketrans({",": "\x00", ".": "\x01"})) \
                   .translate(str.maketrans({"\x00": thou_sep, "\x01": dec_sep}))

    # Negative sign / parentheses
    neg_style = s["negative_style"]
    if is_neg and neg_style != "parentheses":
        body = "-" + body

    # Currency placement
    if s["currency_position"] == "suffix":
        out = body + " " + sym
    else:
        out = sym + body

    if is_neg and neg_style == "parentheses":
        return "(" + out.lstrip("-") + ")"
    return out


def fmt_percent(value, *, signed: bool | None = None) -> str:
    """Format a percent. Input is treated as already-percent (e.g. 12.5 → '12.5%').

    Pass `signed=False` to suppress the leading + sign on positives.
    """
    if value is None or value == "":
        return "—"
    try:
        f = float(value)
    except (TypeError, ValueError):
        return "—"
    if f != f or f in (float("inf"), float("-inf")):
        return "—"

    s = get_number_format()
    decimals = int(s["percent_decimals"])
    use_sign = bool(s["percent_signed"]) if signed is None else bool(signed)
    is_neg = f < 0

    if s["negative_style"] == "parentheses" and is_neg:
        body = f"({abs(f):.{decimals}f}%)"
        return body
    if use_sign:
        return f"{f:+.{decimals}f}%"
    return f"{f:.{decimals}f}%"


def fmt_multiplier(value) -> str:
    """Format a multiplier like P/E ratios → '26.09x'."""
    if value is None or value == "":
        return "—"
    try:
        f = float(value)
    except (TypeError, ValueError):
        return "—"
    if f != f or f in (float("inf"), float("-inf")):
        return "—"
    s = get_number_format()
    decimals = int(s["multiplier_decimals"])
    is_neg = f < 0
    body = f"{abs(f):.{decimals}f}x"
    if is_neg:
        return f"({body})" if s["negative_style"] == "parentheses" else f"-{body}"
    return body


def fmt_int_compact(value) -> str:
    """Format an integer using K/M/B units when above the compact threshold."""
    if value is None or value == "":
        return "—"
    try:
        f = float(value)
    except (TypeError, ValueError):
        return "—"
    s = get_number_format()
    abs_v = abs(f)
    is_neg = f < 0

    if s["currency_style"] == "compact" and abs_v >= float(s["compact_threshold"]):
        body = _format_number_compact(abs_v, decimals=int(s["multiplier_decimals"]))
    else:
        body = f"{int(abs_v):,}"

    if is_neg:
        return f"({body})" if s["negative_style"] == "parentheses" else f"-{body}"
    return body


# ─── Simple Number-format presets ────────────────────────────────────────────
NUMBER_FORMAT_PRESETS: dict = {
    # Compact USD       → $45.50M · -$1.50M · 12.50%
    "Compact ($)": {
        "currency_symbol": "$", "currency_position": "prefix",
        "currency_style": "compact", "decimal_places": 2,
        "thousands_separator": ",", "decimal_separator": ".",
        "negative_style": "minus", "compact_threshold": 10000.0,
        "percent_decimals": 2, "percent_signed": True, "multiplier_decimals": 2,
    },
    # Full USD          → $45,500,000.00 · -$1,500,000.00 · +12.50%
    "Full ($)": {
        "currency_symbol": "$", "currency_position": "prefix",
        "currency_style": "full", "decimal_places": 2,
        "thousands_separator": ",", "decimal_separator": ".",
        "negative_style": "minus", "compact_threshold": 10000.0,
        "percent_decimals": 2, "percent_signed": True, "multiplier_decimals": 2,
    },
    # Detailed USD      → $5,423,100.00 (always full precision)
    "Detailed ($)": {
        "currency_symbol": "$", "currency_position": "prefix",
        "currency_style": "detailed", "decimal_places": 2,
        "thousands_separator": ",", "decimal_separator": ".",
        "negative_style": "minus", "compact_threshold": 1e12,
        "percent_decimals": 2, "percent_signed": True, "multiplier_decimals": 2,
    },
    # Accounting USD    → ($1,500,000) parentheses for losses
    "Accounting ($)": {
        "currency_symbol": "$", "currency_position": "prefix",
        "currency_style": "full", "decimal_places": 2,
        "thousands_separator": ",", "decimal_separator": ".",
        "negative_style": "parentheses", "compact_threshold": 10000.0,
        "percent_decimals": 2, "percent_signed": False, "multiplier_decimals": 2,
    },
    # Compact SAR       → 2.10B SAR
    "Compact (SAR)": {
        "currency_symbol": "SAR", "currency_position": "suffix",
        "currency_style": "compact", "decimal_places": 2,
        "thousands_separator": ",", "decimal_separator": ".",
        "negative_style": "minus", "compact_threshold": 10000.0,
        "percent_decimals": 2, "percent_signed": True, "multiplier_decimals": 2,
    },
    # Full SAR          → 5,423,100.00 SAR
    "Full (SAR)": {
        "currency_symbol": "SAR", "currency_position": "suffix",
        "currency_style": "full", "decimal_places": 2,
        "thousands_separator": ",", "decimal_separator": ".",
        "negative_style": "minus", "compact_threshold": 10000.0,
        "percent_decimals": 2, "percent_signed": True, "multiplier_decimals": 2,
    },
    # Compact EUR       → €45.50M (suffix variant: 45.50M €)
    "Compact (€)": {
        "currency_symbol": "€", "currency_position": "prefix",
        "currency_style": "compact", "decimal_places": 2,
        "thousands_separator": ".", "decimal_separator": ",",
        "negative_style": "minus", "compact_threshold": 10000.0,
        "percent_decimals": 2, "percent_signed": True, "multiplier_decimals": 2,
    },
}


def _on_number_format_change() -> None:
    """Apply the chosen preset to st.session_state['number_format']."""
    name = st.session_state.get("numfmt_preset", "Compact ($)")
    preset = NUMBER_FORMAT_PRESETS.get(name) or NUMBER_FORMAT_PRESETS["Compact ($)"]
    st.session_state["number_format"] = dict(preset)


def render_number_format_panel() -> None:
    """Sidebar Number-format options — preset radio + tiny preview."""
    # Initialize on first render
    if "numfmt_preset" not in st.session_state:
        st.session_state["numfmt_preset"] = "Compact ($)"
        _on_number_format_change()

    with st.sidebar.expander("🔢 Number format", expanded=False):
        st.radio(
            "Style",
            list(NUMBER_FORMAT_PRESETS.keys()),
            key="numfmt_preset",
            on_change=_on_number_format_change,
            help=(
                "**Compact** → $45.50M · 12.5%  ·  "
                "**Full** → $45,500,000.00  ·  "
                "**Detailed** → exact values, no shorthand  ·  "
                "**Accounting** → losses in parentheses (1,500) like balance sheets  ·  "
                "**SAR / €** → other currencies"
            ),
        )

        # Live preview reflecting the active preset
        st.caption("**Preview**")
        rows = [
            ("Multiplier (P/E)", fmt_multiplier(26.094)),
            ("Big number",       fmt_money(45_500_000)),
            ("Detailed value",   fmt_money(5_423_100, force_full=True)),
            ("Percent (gain)",   fmt_percent(12.5)),
            ("Percent (loss)",   fmt_percent(-3.4)),
            ("Loss amount",      fmt_money(-1_500_000)),
            ("Units",            fmt_int_compact(150_000)),
        ]
        st.dataframe(
            pd.DataFrame(rows, columns=["Type", "Output"]),
            width="stretch", hide_index=True, height=260,
        )


def symbol_badge(symbol: str | None, *, exchange: str | None = None,
                  size: str = "md") -> str:
    """Return HTML for a stock-symbol badge to embed in section headers / tables."""
    if not symbol:
        return ""
    sym = escape(str(symbol).upper())
    exch = escape(str(exchange or "")) if exchange else ""
    font_size = {"sm": "0.78rem", "md": "0.92rem", "lg": "1.1rem"}.get(size, "0.92rem")
    pad = {"sm": "2px 8px", "md": "4px 12px", "lg": "6px 16px"}.get(size, "4px 12px")
    extra = f" · <span style='opacity:0.7'>{exch}</span>" if exch else ""
    return (
        f"<span class='stock-symbol-badge' style='display:inline-flex;align-items:center;"
        f"gap:6px;padding:{pad};border-radius:8px;background:var(--accent-soft);"
        f"color:var(--accent);font-family:\"JetBrains Mono\",monospace;"
        f"font-weight:700;font-size:{font_size};border:1px solid var(--accent);'>"
        f"📈 {sym}{extra}</span>"
    )


def add_symbol_to_df(df: "pd.DataFrame | None", symbol: str | None,
                      column: str = "Symbol") -> "pd.DataFrame | None":
    """Insert a Symbol column at position 0 of a DataFrame for clarity."""
    if df is None or symbol is None:
        return df
    try:
        if column in df.columns:
            return df
        out = df.copy()
        out.insert(0, column, str(symbol).upper())
        return out
    except Exception:
        return df


# ============================================================
# IBKR helpers used by the tab
# ============================================================
def _fmt_money(value, currency: str = "$") -> str:
    """Backward-compatible wrapper around fmt_money()."""
    return fmt_money(value, currency=currency)


def _fmt_pct(value) -> str | None:
    """Backward-compatible wrapper around fmt_percent()."""
    if value is None:
        return None
    return fmt_percent(value)


def _fmt_int(value) -> str:
    """Backward-compatible wrapper around fmt_int_compact()."""
    return fmt_int_compact(value)


def _ibkr_extract_pnl_metrics(pnl_payload) -> dict | None:
    """Pull dpl/upl/rpl/nl/el out of /iserver/account/pnl/partitioned response."""
    if not pnl_payload or not isinstance(pnl_payload, dict):
        return None
    if not pnl_payload.get("_ok"):
        return None
    data = pnl_payload.get("data") or {}
    upnl = data.get("upnl") or data
    if not isinstance(upnl, dict):
        return None
    # Find first account block
    account_block = None
    for value in upnl.values():
        if isinstance(value, dict) and ("dpl" in value or "upl" in value or "nl" in value):
            account_block = value
            break
    if account_block is None:
        return None
    nl = float(account_block.get("nl") or 0)
    dpl = float(account_block.get("dpl") or 0)
    upl = float(account_block.get("upl") or 0)
    rpl = float(account_block.get("rpl") or 0)
    out = {"dpl": dpl, "upl": upl, "rpl": rpl, "nl": nl, "el": account_block.get("el")}
    if nl:
        out["dpl_pct"] = (dpl / nl) * 100.0
        out["upl_pct"] = (upl / nl) * 100.0
        out["rpl_pct"] = (rpl / nl) * 100.0
    return out


def _ibkr_positions_to_rows(pos_payload) -> list[dict]:
    """Normalize /portfolio/{id}/positions/0 OR ib_insync positions into a rowlist."""
    if not pos_payload or not isinstance(pos_payload, dict):
        return []
    if not pos_payload.get("_ok"):
        return []
    data = pos_payload.get("data") or []
    rows: list[dict] = []
    if isinstance(data, list):
        for p in data:
            if not isinstance(p, dict):
                continue
            sym = p.get("contractDesc") or p.get("ticker") or p.get("symbol") or "?"
            qty = p.get("position") or p.get("quantity") or 0
            mp = p.get("mktPrice") or p.get("market_price")
            mv = p.get("mktValue") or p.get("market_value")
            avg = p.get("avgCost") or p.get("avg_cost")
            upnl = p.get("unrealizedPnl") or p.get("unrealized_pnl")
            rpnl = p.get("realizedPnl") or p.get("realized_pnl")
            try:
                qty_f = float(qty) if qty is not None else 0.0
                mp_f = float(mp) if mp is not None else 0.0
                mv_f = float(mv) if mv is not None else (qty_f * mp_f)
            except (TypeError, ValueError):
                qty_f, mp_f, mv_f = 0.0, 0.0, 0.0
            rows.append({
                "symbol": sym,
                "quantity": qty_f,
                "avg_cost": _to_float(avg),
                "market_price": mp_f,
                "market_value": mv_f,
                "unrealized_pnl": _to_float(upnl),
                "realized_pnl": _to_float(rpnl),
            })
    return rows


def _to_float(v):
    try:
        return float(v) if v is not None and v != "" else None
    except (TypeError, ValueError):
        return None


def _sahmk_cooperative_fetch(symbol: str, sahmk_client, fmp_key: str = "") -> dict:
    """Fetch the same symbol from Sahm.sa + global APIs in parallel.

    Returns: {"primary": <best snapshot>, "by_provider": {...}, "coverage": {...}}
    """
    out = {"by_provider": {}, "coverage": {}}
    sym_upper = (symbol or "").strip().upper()

    # 1. Sahm.sa
    sah_q = sahmk_client.quote(symbol)
    out["by_provider"]["sahmk"] = sah_q
    out["coverage"]["sahmk"] = "ok" if sah_q.get("_ok") else "error"

    # 2. Yahoo (no key) — works for global symbols
    try:
        import yfinance as _yf
        ticker = _yf.Ticker(sym_upper if "." in sym_upper else f"{sym_upper}.SR")
        info = getattr(ticker, "fast_info", None) or {}
        if info:
            out["by_provider"]["yahoo"] = {"_ok": True, "data": dict(info)}
            out["coverage"]["yahoo"] = "ok"
        else:
            out["coverage"]["yahoo"] = "partial"
    except Exception as exc:
        out["coverage"]["yahoo"] = "error"
        out["by_provider"]["yahoo"] = {"_ok": False, "error": str(exc)[:160]}

    # 3. FMP — needs key, only run if provided
    if fmp_key:
        try:
            r = requests.get(f"https://financialmodelingprep.com/api/v3/quote/{sym_upper}",
                             params={"apikey": fmp_key}, timeout=6)
            out["by_provider"]["fmp"] = {"_ok": r.ok, "data": r.json() if r.ok else r.text[:200]}
            out["coverage"]["fmp"] = "ok" if r.ok else "error"
        except Exception as exc:
            out["coverage"]["fmp"] = "error"
            out["by_provider"]["fmp"] = {"_ok": False, "error": str(exc)[:160]}

    # Pick primary snapshot — Sahm.sa first if it returned data, else Yahoo
    if sah_q.get("_ok"):
        out["primary"] = sah_q.get("data")
    elif out["by_provider"].get("yahoo", {}).get("_ok"):
        out["primary"] = out["by_provider"]["yahoo"]["data"]
    return out


def render_news_tab(bundle: dict) -> None:
    """News & Events tab — multi-source aggregator with filters and card layout."""
    all_news = bundle.get("news") or []
    providers_used = bundle.get("news_providers") or []
    sec_items_all = [x for x in all_news if x.get("provider_tag") == "sec_api"]

    # ─── Stats ───
    sentiments = [x["sentiment"] for x in all_news if isinstance(x.get("sentiment"), (int, float))]
    avg_sent = sum(sentiments) / len(sentiments) if sentiments else None
    fused_count = sum(1 for x in all_news if int(x.get("source_count") or 1) > 1)
    pos_count = sum(1 for s in sentiments if s > 0.1)
    neg_count = sum(1 for s in sentiments if s < -0.1)

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Total Stories", len(all_news))
    c2.metric("News Sources", len(providers_used))
    c3.metric("SEC Filings", len(sec_items_all))
    c4.metric("Avg Sentiment", f"{avg_sent:+.2f}" if avg_sent is not None else "—")
    c5.metric("Bullish", pos_count, delta=None)
    c6.metric("Bearish", neg_count, delta=None)

    if not all_news:
        st.info(
            "📰 No news available yet. Add Marketaux, Benzinga, NewsAPI.ai, or SEC-API keys "
            "in the sidebar ▸ API Keys to fetch more sources."
        )
        return

    # ─── Filters ───
    st.markdown(
        '<div class="section-header"><h3>🔍 Filters</h3>'
        f'<span class="section-count">{len(all_news)} stories</span></div>',
        unsafe_allow_html=True,
    )

    sym = bundle.get("symbol", "x")
    fc1, fc2, fc3, fc4 = st.columns([1.5, 1.5, 1, 2])

    all_prov_tags = sorted({x.get("provider_tag") or "" for x in all_news if x.get("provider_tag")})
    prov_options = ["All"] + [NEWS_PROVIDER_LABELS.get(p, p) for p in all_prov_tags]
    sel_prov_lbl = fc1.selectbox("Source", prov_options, key=f"nf_prov_{sym}")
    sel_prov = (
        next((p for p in all_prov_tags if NEWS_PROVIDER_LABELS.get(p, p) == sel_prov_lbl), None)
        if sel_prov_lbl != "All"
        else None
    )

    all_cats = sorted({x.get("category") or "" for x in all_news if x.get("category")})
    sel_cat = fc2.selectbox("Category", ["All"] + all_cats, key=f"nf_cat_{sym}")

    sel_sent = fc3.selectbox("Sentiment", ["All", "Positive", "Negative", "Neutral"], key=f"nf_sent_{sym}")

    search_q = (
        fc4.text_input("Search headlines", placeholder="e.g. earnings, guidance…", key=f"nf_srch_{sym}")
        .strip()
        .lower()
    )

    # ─── Apply filters ───
    filtered = all_news[:]
    if sel_prov:
        filtered = [x for x in filtered if x.get("provider_tag") == sel_prov]
    if sel_cat != "All":
        filtered = [x for x in filtered if x.get("category") == sel_cat]
    if sel_sent == "Positive":
        filtered = [x for x in filtered if isinstance(x.get("sentiment"), (int, float)) and x["sentiment"] > 0.1]
    elif sel_sent == "Negative":
        filtered = [x for x in filtered if isinstance(x.get("sentiment"), (int, float)) and x["sentiment"] < -0.1]
    elif sel_sent == "Neutral":
        filtered = [
            x for x in filtered
            if isinstance(x.get("sentiment"), (int, float)) and -0.1 <= x["sentiment"] <= 0.1
        ]
    if search_q:
        filtered = [
            x for x in filtered
            if search_q in (x.get("headline") or "").lower()
            or search_q in (x.get("summary") or "").lower()
        ]

    st.caption(f"Showing **{len(filtered)}** of {len(all_news)} stories · " + (
        ", ".join(NEWS_PROVIDER_LABELS.get(p, p) for p in providers_used[:6]) or "—"
    ))

    if not filtered:
        st.info("No stories match the current filters.")
        return

    # ─── General news (2 columns) ───
    news_gen = [x for x in filtered if x.get("provider_tag") != "sec_api"]
    sec_filtered = [x for x in filtered if x.get("provider_tag") == "sec_api"]

    if news_gen:
        st.markdown(
            f'<div class="section-header"><h3>📰 Market News &amp; Analysis</h3>'
            f'<span class="section-count">{len(news_gen)} stories</span></div>',
            unsafe_allow_html=True,
        )
        left_col, right_col = st.columns(2)
        for idx, item in enumerate(news_gen[:60]):
            col = left_col if idx % 2 == 0 else right_col
            with col:
                st.markdown(render_news_card_html(item), unsafe_allow_html=True)

    # ─── SEC Filings ───
    if sec_filtered:
        st.markdown(
            f'<div class="section-header"><h3>🏛 SEC Regulatory Filings</h3>'
            f'<span class="section-count">{len(sec_filtered)} filings</span></div>',
            unsafe_allow_html=True,
        )
        st.caption("8-K, 10-K, 10-Q — last 90 days · Source: SEC-API.io")
        for item in sec_filtered[:20]:
            st.markdown(render_news_card_html(item), unsafe_allow_html=True)


def render_datafeed_tab(bundle: dict[str, Any]) -> None:
    config = tradingview_configuration_data()
    symbol_info = tradingview_symbol_info(bundle)
    bars = tradingview_bars_frame(bundle)
    cfg_col, symbol_col = st.columns([1, 1])
    with cfg_col:
        st.markdown("### TradingView Configuration")
        st.json(config)
    with symbol_col:
        st.markdown("### Resolved Symbol")
        st.json(symbol_info)
    st.markdown("### Historical Bars")
    if bars.empty:
        st.info("No historical bars are available for the selected symbol.")
    else:
        st.dataframe(bars.tail(300), width="stretch", hide_index=True, height=420)
        st.download_button(
            "Download bars JSON",
            data=bars.to_json(orient="records").encode("utf-8"),
            file_name=f"{str(bundle.get('symbol') or 'symbol').lower()}_tradingview_bars.json",
            mime="application/json",
            width="stretch",
        )


def render_overview_tab(bundle: dict[str, Any], bundles: dict[str, dict[str, Any]]) -> None:
    left, right = st.columns([1.35, 1])
    with left:
        st.markdown("### Company Snapshot")
        profile_rows = [
            ["Company", bundle.get("company_name") or "—"],
            ["Sector", bundle.get("sector") or "—"],
            ["Industry", bundle.get("industry") or "—"],
            ["Exchange", bundle.get("exchange") or "—"],
            ["Country", bundle.get("country") or "—"],
            ["Currency", bundle.get("currency") or "—"],
            ["Website", bundle.get("website") or "—"],
            ["History Source", provider_label(bundle.get("history_provider"))],
            ["Annuals Source", provider_label(bundle.get("annuals_provider"))],
        ]
        st.dataframe(pd.DataFrame(profile_rows, columns=["Field", "Value"]), width="stretch", hide_index=True)
        if bundle.get("description"):
            st.markdown("### Business Summary")
            st.write(bundle["description"])
        if bundle.get("notes"):
            st.markdown("### Runtime Notes")
            for note in bundle["notes"]:
                st.caption(note)
    with right:
        st.markdown("### Executive Scorecard")
        st.dataframe(metric_frame(bundle, ["Summary", "Valuation", "Growth"]).head(12), width="stretch", hide_index=True)
    chart_col, side_col = st.columns([1.8, 1])
    with chart_col:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.plotly_chart(history_chart(bundle), width="stretch", key=f"overview_history_{bundle.get('symbol','x')}")
        st.markdown("</div>", unsafe_allow_html=True)
    with side_col:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.plotly_chart(score_radar_chart(bundle), width="stretch", key=f"overview_radar_{bundle.get('symbol','x')}")
        st.markdown("</div>", unsafe_allow_html=True)
    if len(bundles) > 1:
        st.markdown("### Comparison Snapshot")
        st.dataframe(comparison_summary_frame(bundles), width="stretch", hide_index=True)
    if bundle.get("news"):
        _prov_disp = bundle.get("news_provider") or "Multiple sources"
        st.markdown(f"### Latest Headlines · {_prov_disp}")
        _preview_cards = "".join(render_news_card_html(i) for i in bundle["news"][:6])
        st.markdown(_preview_cards, unsafe_allow_html=True)
        st.caption(
            f"{len(bundle['news'])} total stories — open the 📰 News & Events tab for full view, filters & SEC filings."
        )


def render_financials_tab(bundle: dict[str, Any]) -> None:
    st.markdown("### Financial Statements")
    metrics = bundle["metrics"]
    currency = bundle.get("currency", "")
    kpi_cols = st.columns(5)
    kpi_cols[0].metric("Revenue", format_large(metrics.get("revenue"), currency), format_pct(metrics.get("revenue_growth"), signed=True))
    kpi_cols[1].metric("Net Income", format_large(metrics.get("net_income"), currency), format_pct(metrics.get("earnings_growth"), signed=True))
    kpi_cols[2].metric("Free Cash Flow", format_large(metrics.get("free_cash_flow"), currency))
    kpi_cols[3].metric("Gross Margin", format_pct(metrics.get("gross_margin")))
    kpi_cols[4].metric("Debt / Equity", format_multiple(metrics.get("debt_to_equity")))

    left, right = st.columns([1.55, 1])
    with left:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.plotly_chart(annuals_chart(bundle), width="stretch", key=f"financials_annuals_{bundle.get('symbol','x')}")
        st.markdown("</div>", unsafe_allow_html=True)
    with right:
        st.markdown("### Statement Quality")
        st.dataframe(metric_frame(bundle, ["Financials", "Growth", "Quality"]).head(24), width="stretch", hide_index=True, height=420)

    statement_tabs = st.tabs(["Income Statement", "Cash Flow", "Balance Sheet & Leverage", "Annual Statements", "Provider Raw Slice"])
    income_rows = [
        ("Revenue", metrics.get("revenue"), "large_money", "revenue"),
        ("Gross Profit", metrics.get("gross_profit"), "large_money", "gross_profit"),
        ("Operating Income", metrics.get("operating_income"), "large_money", "operating_income"),
        ("Net Income", metrics.get("net_income"), "large_money", "net_income"),
        ("Gross Margin", metrics.get("gross_margin"), "percent", "gross_margin"),
        ("Operating Margin", metrics.get("operating_margin"), "percent", "operating_margin"),
        ("Net Profit Margin", metrics.get("profit_margin"), "percent", "profit_margin"),
    ]
    cash_rows = [
        ("Operating Cash Flow", metrics.get("operating_cash_flow"), "large_money", "operating_cash_flow"),
        ("Free Cash Flow", metrics.get("free_cash_flow"), "large_money", "free_cash_flow"),
        ("FCF Margin", safe_div(metrics.get("free_cash_flow"), metrics.get("revenue")), "percent", "free_cash_flow"),
        ("Annual Dividend / Share", metrics.get("annual_dividend"), "money", "annual_dividend"),
        ("Dividend Yield", metrics.get("dividend_yield"), "percent", "dividend_yield"),
        ("Payout Ratio", calculate_payout_ratio(bundle), "percent", "calculated"),
    ]
    balance_rows = [
        ("Total Assets", metrics.get("total_assets"), "large_money", "total_assets"),
        ("Current Assets", metrics.get("current_assets"), "large_money", "current_assets"),
        ("Current Liabilities", metrics.get("current_liabilities"), "large_money", "current_liabilities"),
        ("Total Debt", metrics.get("total_debt"), "large_money", "total_debt"),
        ("Cash & Equivalents", metrics.get("cash_and_equivalents"), "large_money", "cash_and_equivalents"),
        ("Total Equity", metrics.get("total_equity"), "large_money", "total_equity"),
        ("Current Ratio", metrics.get("current_ratio"), "multiple", "current_ratio"),
        ("Debt / Equity", metrics.get("debt_to_equity"), "multiple", "debt_to_equity"),
        ("ROE", metrics.get("roe"), "percent", "roe"),
        ("ROA", metrics.get("roa"), "percent", "roa"),
    ]

    def statement_frame(rows: list[tuple[str, Any, str, str]]) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "Line Item": label,
                    "Value": format_by_type(value, kind, currency),
                    "Source": provider_label(bundle["sources"].get(source_key, source_key)),
                }
                for label, value, kind, source_key in rows
            ]
        )

    with statement_tabs[0]:
        st.dataframe(statement_frame(income_rows), width="stretch", hide_index=True)
    with statement_tabs[1]:
        st.dataframe(statement_frame(cash_rows), width="stretch", hide_index=True)
    with statement_tabs[2]:
        st.dataframe(statement_frame(balance_rows), width="stretch", hide_index=True)
    with statement_tabs[3]:
        annual_frame = pd.DataFrame(bundle.get("annuals", []) or [])
        if annual_frame.empty:
            st.info("No annual statement rows are available from the enabled providers.")
        else:
            st.dataframe(annual_frame, width="stretch", hide_index=True, height=460)
    with statement_tabs[4]:
        st.caption(f"Annuals provider: {provider_label(bundle.get('annuals_provider'))}")
        st.json({"annuals": bundle.get("annuals", []), "metrics_sources": bundle.get("sources", {})})


def render_valuation_tab(bundle: dict[str, Any], bundles: dict[str, dict[str, Any]]) -> None:
    left, right = st.columns([1.5, 1])
    with left:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.plotly_chart(dcf_sensitivity_chart(bundle), width="stretch", key=f"valuation_dcf_{bundle.get('symbol','x')}")
        st.markdown("</div>", unsafe_allow_html=True)
    with right:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.plotly_chart(recommendation_trend_chart(bundle), width="stretch", key=f"valuation_recommendation_{bundle.get('symbol','x')}")
        st.markdown("</div>", unsafe_allow_html=True)
    st.markdown("### Valuation Table")
    st.dataframe(metric_frame(bundle, ["Valuation", "DCF", "Summary"]), width="stretch", hide_index=True)
    if len(bundles) > 1:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.plotly_chart(comparison_valuation_chart(bundles), width="stretch", key="comparison_valuation")
        st.markdown("</div>", unsafe_allow_html=True)


def render_quality_tab(bundle: dict[str, Any], bundles: dict[str, dict[str, Any]]) -> None:
    left, right = st.columns([1.2, 1])
    with left:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.plotly_chart(score_radar_chart(bundle), width="stretch", key=f"quality_radar_{bundle.get('symbol','x')}")
        st.markdown("</div>", unsafe_allow_html=True)
    with right:
        st.markdown("### Quality Table")
        st.dataframe(metric_frame(bundle, ["Quality", "Summary"]), width="stretch", hide_index=True)
    if len(bundles) > 1:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.plotly_chart(comparison_bubble_chart(bundles), width="stretch", key="comparison_bubble")
        st.markdown("</div>", unsafe_allow_html=True)


def render_momentum_tab(bundle: dict[str, Any], bundles: dict[str, dict[str, Any]]) -> None:
    left, right = st.columns([1.6, 1])
    with left:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.plotly_chart(history_chart(bundle), width="stretch", key=f"momentum_history_{bundle.get('symbol','x')}")
        st.markdown("</div>", unsafe_allow_html=True)
    with right:
        st.markdown("### Momentum Table")
        st.dataframe(metric_frame(bundle, ["Trend", "Momentum", "Volume", "Structure", "Risk"]).head(24), width="stretch", hide_index=True)
    if len(bundles) > 1:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.plotly_chart(comparison_performance_chart(bundles), width="stretch", key="comparison_performance")
        st.markdown("</div>", unsafe_allow_html=True)


def render_provider_tab(bundle: dict[str, Any]) -> None:
    st.markdown("### Provider Diagnostics")
    st.dataframe(provider_health_frame(bundle), width="stretch", hide_index=True)
    left, right = st.columns(2)
    with left:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.plotly_chart(provider_coverage_bar(bundle), width="stretch", key=f"provider_bar_{bundle.get('symbol','x')}")
        st.markdown("</div>", unsafe_allow_html=True)
    with right:
        st.markdown('<div class="panel">', unsafe_allow_html=True)
        st.plotly_chart(provider_coverage_heatmap(bundle), width="stretch", key=f"provider_heatmap_{bundle.get('symbol','x')}")
        st.markdown("</div>", unsafe_allow_html=True)
    rows = []
    for definition in METRIC_DEFINITIONS:
        rows.append(
            {
                "Metric": definition.label,
                "Value": format_by_type(bundle["metrics"].get(definition.key), definition.kind, bundle.get("currency", "")),
                "Source": provider_label(bundle["sources"].get(definition.key)),
            }
        )
    st.markdown("### Metric Source Map")
    st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
    st.markdown("### Provider Error Console")
    providers = bundle.get("providers", {}) if isinstance(bundle, dict) else {}
    if not isinstance(providers, dict) or not providers:
        st.info("No provider diagnostics are available for this symbol.")
        return
    for provider, payload in providers.items():
        payload = payload if isinstance(payload, dict) else {"raw": payload, "status": "unknown"}
        label = f"{provider_label(provider)} · {payload.get('status', 'unknown').title()}"
        with st.expander(label, expanded=False):
            st.write(payload.get("summary_error", "—"))
            if payload.get("notes"):
                for note in payload["notes"]:
                    st.caption(note)
            if payload.get("errors"):
                for key, value in payload["errors"].items():
                    st.code(f"{key}: {value}")


def render_export_tab(bundle: dict[str, Any], bundles: dict[str, dict[str, Any]], debug_mode: bool) -> None:
    export_df = metrics_export_frame(bundle)
    compare_df = comparison_summary_frame(bundles)
    xy_df = comparison_xy_frame(bundles)
    raw_json = json.dumps(
        {
            "primary_symbol": bundle["symbol"],
            "bundles": bundles,
        },
        ensure_ascii=False,
        indent=2,
        default=str,
    )
    col_a, col_b, col_c, col_d = st.columns(4)
    with col_a:
        st.download_button(
            "Download primary metrics CSV",
            data=export_df.to_csv(index=False).encode("utf-8"),
            file_name=f"{bundle['symbol'].lower()}_metrics.csv",
            mime="text/csv",
            width="stretch",
        )
    with col_b:
        st.download_button(
            "Download comparison CSV",
            data=compare_df.to_csv(index=False).encode("utf-8"),
            file_name="comparison_summary.csv",
            mime="text/csv",
            width="stretch",
        )
    with col_c:
        st.download_button(
            "Download X+Y matrix CSV",
            data=xy_df.to_csv(index=False).encode("utf-8"),
            file_name="xy_comparison_matrix.csv",
            mime="text/csv",
            width="stretch",
        )
    with col_d:
        st.download_button(
            "Download raw JSON",
            data=raw_json.encode("utf-8"),
            file_name="dashboard_raw.json",
            mime="application/json",
            width="stretch",
        )
    st.markdown("### Primary Export View")
    st.dataframe(export_df, width="stretch", hide_index=True)
    if debug_mode:
        st.markdown("### Raw Provider Payloads")
        providers = bundle.get("providers", {}) if isinstance(bundle, dict) else {}
        if not isinstance(providers, dict) or not providers:
            st.info("No provider payloads are available for this symbol.")
            return
        for provider, payload in providers.items():
            with st.expander(provider_label(provider), expanded=False):
                if isinstance(payload, dict):
                    raw_payload = payload.get("raw", payload)
                else:
                    raw_payload = payload
                if raw_payload is None:
                    raw_payload = {"warning": "Provider payload is empty or missing raw data."}
                st.json(raw_payload)


class SimpleHTMLSummaryParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.title_parts: list[str] = []
        self.h1_parts: list[str] = []
        self.description = ""
        self._capture: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        attributes = {key.lower(): value or "" for key, value in attrs if key}
        if tag == "meta" and attributes.get("name", "").lower() == "description":
            self.description = attributes.get("content", "").strip()
        elif tag == "title":
            self._capture = "title"
        elif tag == "h1" and not self.h1_parts:
            self._capture = "h1"

    def handle_data(self, data: str) -> None:
        clean = data.strip()
        if not clean:
            return
        if self._capture == "title":
            self.title_parts.append(clean)
        elif self._capture == "h1":
            self.h1_parts.append(clean)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag == self._capture:
            self._capture = None

    def snapshot(self) -> dict[str, str]:
        return {
            "title": " ".join(self.title_parts).strip() or "No title found",
            "description": self.description or "No description found",
            "h1": " ".join(self.h1_parts).strip() or "No H1 found",
        }


def collect_url_snapshot(url: str) -> dict[str, str]:
    url = str(url or "").strip()
    if not url:
        return {"status": "error", "message": "Enter a URL first."}
    if not url.startswith(("http://", "https://")):
        url = f"https://{url}"

    try:
        response = requests.get(url, headers=HTTP_HEADERS, timeout=12)
        response.raise_for_status()
        parser = SimpleHTMLSummaryParser()
        parser.feed(response.text[:500_000])
        return {"status": "ok", "url": url, **parser.snapshot()}
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": str(exc)}


def render_collect_data_tab() -> None:
    st.markdown("### Collect Data")
    url = st.text_input(
        "URL",
        key="collect_data_url",
        placeholder="https://example.com",
    )
    if st.button("Collect Data", key="collect_data_button"):
        result = collect_url_snapshot(url)
        if result.get("status") == "ok":
            rows = [
                ("URL", result.get("url", "")),
                ("Title", result.get("title", "")),
                ("Description", result.get("description", "")),
                ("H1", result.get("h1", "")),
            ]
            st.success("Data collected.")
            st.dataframe(pd.DataFrame(rows, columns=["Field", "Value"]), width="stretch", hide_index=True)
        else:
            st.error(result.get("message", "Unknown error"))



def _on_theme_preset_change() -> None:
    """Callback fired when the user picks a new preset.

    The callback runs at the START of the next rerun, BEFORE other widgets
    re-instantiate, so it's safe to mutate the session_state keys those widgets
    bind to. (Streamlit only blocks the mutation when the widget has already
    been rendered in the same script run.)
    """
    name = st.session_state.get("theme_preset", "Atlas Light")
    preset = THEME_PRESETS.get(name) or THEME_PRESETS["Atlas Light"]
    st.session_state["theme_accent"] = preset["accent"]
    st.session_state["theme_ink"] = preset["ink"]
    st.session_state["theme_bg"] = preset["bg"]
    st.session_state["theme_surface"] = preset["surface"]
    st.session_state["visual_theme_mode"] = "Dark" if preset["is_dark"] else "Light"


def _on_theme_reset() -> None:
    """Reset button — re-applies the current preset and zeros out tweaks."""
    _on_theme_preset_change()
    st.session_state["theme_radius"] = 12
    st.session_state["theme_density"] = "Comfortable"


# ============================================================
# i18n — Arabic / English translation
# ============================================================
I18N_LANGUAGES = {"English": "en", "العربية": "ar"}

I18N_STRINGS: dict[str, dict[str, str]] = {
    # Navigation / sidebar
    "sidebar.api_keys":           {"en": "API Keys", "ar": "مفاتيح API"},
    "sidebar.theme":              {"en": "🎨 Theme & appearance",         "ar": "🎨 المظهر والثيم"},
    "sidebar.number_format":      {"en": "🔢 Number format",              "ar": "🔢 تنسيق الأرقام"},
    "sidebar.language":           {"en": "🌐 Language",                   "ar": "🌐 اللغة"},
    "sidebar.symbols":            {"en": "📌 Pinned symbols",             "ar": "📌 الأسهم المثبّتة"},
    "sidebar.dashboard_mode":     {"en": "Dashboard",                     "ar": "لوحة القيادة"},

    # Top tabs
    "tab.decision":               {"en": "Decision Engine",               "ar": "محرّك القرار"},
    "tab.overview":               {"en": "Overview",                      "ar": "نظرة عامة"},
    "tab.financials":             {"en": "Financial Statements",          "ar": "القوائم المالية"},
    "tab.valuation":              {"en": "Valuation",                     "ar": "التقييم"},
    "tab.quality":                {"en": "Quality",                       "ar": "الجودة"},
    "tab.momentum":               {"en": "Momentum",                      "ar": "الزخم"},
    "tab.news":                   {"en": "News & Events",                 "ar": "الأخبار والأحداث"},
    "tab.chart":                  {"en": "Chart Datafeed",                "ar": "الرسم البياني"},
    "tab.ibkr":                   {"en": "IBKR Trading",                  "ar": "تداول IBKR"},
    "tab.providers":              {"en": "Providers",                     "ar": "المزوّدون"},
    "tab.raw":                    {"en": "Raw Data / Debug",              "ar": "بيانات خام / تشخيص"},
    "tab.collect":                {"en": "Collect Data",                  "ar": "جمع البيانات"},

    # Dashboard sections
    "dash.live":                  {"en": "🌐 Live Data Dashboard",        "ar": "🌐 لوحة البيانات الحيّة"},
    "dash.data_engine":           {"en": "🧠 Data Engine",                "ar": "🧠 محرّك البيانات"},
    "dash.core_financials":       {"en": "Core Financials",               "ar": "القوائم المالية الأساسية"},
    "dash.income":                {"en": "Income Statement",              "ar": "قائمة الدخل"},
    "dash.balance":               {"en": "Balance Sheet",                 "ar": "الميزانية العمومية"},
    "dash.cash_flow":             {"en": "Cash Flow",                     "ar": "التدفقات النقدية"},
    "dash.dividends":             {"en": "Dividends",                     "ar": "التوزيعات"},
    "dash.institutional":         {"en": "Institutional Holdings",        "ar": "الملكية المؤسسية"},
    "dash.insider":               {"en": "Insider Trading",               "ar": "تداولات المطلعين"},
    "dash.estimates":             {"en": "Analyst Estimates",             "ar": "تقديرات المحللين"},
    "dash.options":               {"en": "Options Chain",                 "ar": "سلسلة الخيارات"},
    "dash.peers":                 {"en": "Peers",                         "ar": "الشركات المنافسة"},
    "dash.sentiment":             {"en": "Sentiment",                     "ar": "تحليل المشاعر"},
    "dash.macro":                 {"en": "Macro Indicators",              "ar": "المؤشرات الكلية"},
    "dash.yield_curve":           {"en": "Yield Curve",                   "ar": "منحنى العائد"},
    "dash.etf":                   {"en": "ETF Holdings",                  "ar": "حيازات الصناديق"},
    "dash.corporate":             {"en": "Corporate Actions",             "ar": "الأحداث المؤسسية"},
    "dash.saudi":                 {"en": "Sahm.sa · Saudi Market",        "ar": "Sahm.sa · السوق السعودي"},

    # Buttons / actions
    "btn.fetch":                  {"en": "📡 Fetch",                      "ar": "📡 جلب"},
    "btn.refresh":                {"en": "🔄 Force refresh",              "ar": "🔄 تحديث قسري"},
    "btn.auto_fetch":             {"en": "Auto-fetch",                    "ar": "جلب تلقائي"},
    "btn.compare":                {"en": "Compare",                       "ar": "مقارنة"},
    "btn.open_new_tab":           {"en": "Open in new tab",               "ar": "فتح في تبويب جديد"},
    "btn.add_symbol":             {"en": "Add symbol",                    "ar": "إضافة سهم"},
    "btn.clear":                  {"en": "Clear",                         "ar": "مسح"},

    # Status messages
    "status.no_data":             {"en": "No data returned by any provider.", "ar": "لا توجد بيانات من أي مزوّد."},
    "status.primary_source":      {"en": "Primary source",                "ar": "المصدر الأساسي"},
    "status.merged_from":         {"en": "Merged from",                   "ar": "مدمج من"},
    "status.quality_score":       {"en": "Quality score",                 "ar": "درجة الجودة"},

    # Metrics
    "metric.price":               {"en": "Price",                         "ar": "السعر"},
    "metric.change":              {"en": "Change",                        "ar": "التغيير"},
    "metric.volume":              {"en": "Volume",                        "ar": "الحجم"},
    "metric.market_cap":          {"en": "Market Cap",                    "ar": "القيمة السوقية"},
    "metric.pe":                  {"en": "P/E Ratio",                     "ar": "مكرر الربحية"},
    "metric.eps":                 {"en": "EPS",                           "ar": "ربحية السهم"},

    # Coverage labels
    "coverage.ok":                {"en": "OK",                            "ar": "متاح"},
    "coverage.partial":           {"en": "partial",                       "ar": "جزئي"},
    "coverage.no_key":            {"en": "no API key",                    "ar": "لا يوجد مفتاح"},
    "coverage.error":             {"en": "error",                         "ar": "خطأ"},
}


def t(key: str, **fmt) -> str:
    """Translate a key to the active language. Falls back to English then key."""
    lang = st.session_state.get("ui_language", "en")
    entry = I18N_STRINGS.get(key)
    if entry is None:
        return key  # missing key → show as-is for visibility
    text = entry.get(lang) or entry.get("en") or key
    if fmt:
        try:
            text = text.format(**fmt)
        except Exception:
            pass
    return text


FINANCIAL_GLOSSARY_AR: dict[str, str] = {
    "Revenue": "الإيرادات",
    "Net Income": "صافي الدخل",
    "Operating Income": "الدخل التشغيلي",
    "Free Cash Flow": "التدفق النقدي الحر",
    "Cash Flow": "التدفقات النقدية",
    "Balance Sheet": "الميزانية العمومية",
    "Income Statement": "قائمة الدخل",
    "Market Cap": "القيمة السوقية",
    "Enterprise Value": "قيمة المنشأة",
    "P/E Ratio": "مكرر الربحية",
    "EPS": "ربحية السهم",
    "ROE": "العائد على حقوق الملكية",
    "ROA": "العائد على الأصول",
    "Bullish": "اتجاه صاعد",
    "Bearish": "اتجاه هابط",
    "Neutral": "محايد",
    "Buy": "شراء",
    "Sell": "بيع",
    "Strong Buy": "شراء قوي",
    "Strong Sell": "بيع قوي",
    "Dividend": "توزيعات الأرباح",
    "Guidance": "التوجيهات المستقبلية",
    "Earnings": "الأرباح",
    "Analyst Estimates": "تقديرات المحللين",
    "sentiment": "المعنويات",
    "grew": "نمت",
    "growth": "النمو",
    "Gross Margin": "هامش الربح الإجمالي",
    "Operating Margin": "هامش التشغيل",
    "Profit Margin": "هامش صافي الربح",
    "Debt": "الدين",
    "Equity": "حقوق الملكية",
    "Liquidity": "السيولة",
    "Volatility": "التذبذب",
    "Momentum": "الزخم",
    "Trend": "الاتجاه",
    "Support": "الدعم",
    "Resistance": "المقاومة",
}


def _translate_financial_numbers_ar(text: str) -> str:
    def repl(match: re.Match[str]) -> str:
        num = match.group(1)
        suffix = match.group(2).upper()
        ar_suffix = {"B": "مليار", "M": "مليون", "K": "ألف", "T": "تريليون"}.get(suffix, suffix)
        return f"{num} {ar_suffix}"

    return re.sub(r"\b(\d+(?:\.\d+)?)\s*([BMKT])\b", repl, text, flags=re.IGNORECASE)


@st.cache_data(ttl=24 * 3600, show_spinner=False)
def translate_financial_ar(text: str, mode: str = "financial") -> dict[str, Any]:
    """Financial-aware Arabic translation helper.

    This is a local hybrid pipeline: glossary → phrase cleanup → number
    normalization → RTL-ready Arabic. It avoids duplicate work through
    Streamlit cache and never calls an external model unless you later wire one.
    """
    original = str(text or "").strip()
    if not original:
        return {"original": "", "translated": "", "confidence": 0.0, "mode": mode}
    translated = original
    replacements = 0
    for en, ar in sorted(FINANCIAL_GLOSSARY_AR.items(), key=lambda item: len(item[0]), reverse=True):
        pattern = re.compile(rf"\b{re.escape(en)}\b", re.IGNORECASE)
        translated, count = pattern.subn(ar, translated)
        replacements += count
    translated = _translate_financial_numbers_ar(translated)
    cleanup = {
        " YoY": " سنوياً",
        " QoQ": " ربعياً",
        " margin": " هامش",
        " beat estimates": " تجاوز التوقعات",
        " missed estimates": " جاء دون التوقعات",
        "الإيرادات نمت": "نمت الإيرادات",
        "المعنويات is": "المعنويات تشير إلى",
        " and ": " و",
    }
    for en, ar in cleanup.items():
        translated = translated.replace(en, ar)
    if mode == "ui":
        translated = translated.strip()
    elif mode == "report":
        translated = translated.strip()
        if translated and not translated.endswith((".", "۔", "؟", "!")):
            translated += "."
    confidence = 0.72 + min(replacements, 8) * 0.03
    return {
        "original": original,
        "translated": translated,
        "confidence": round(min(confidence, 0.96), 3),
        "mode": mode,
    }


def render_financial_translation_lab(key_prefix: str = "translation") -> None:
    with st.expander("🇸🇦 AI Arabic Financial Translation", expanded=False):
        c1, c2 = st.columns([2, 1])
        text = c1.text_area(
            "Financial text",
            value="Revenue grew 12% YoY while Free Cash Flow reached 1.5B. Analyst sentiment is Bullish.",
            key=f"{key_prefix}_text",
            height=90,
        )
        mode = c2.selectbox("Mode", ["financial", "ui", "report"], index=0, key=f"{key_prefix}_mode")
        result = translate_financial_ar(text, mode)
        st.markdown(
            f"<div dir='rtl' style='font-size:1.05rem;line-height:1.8;padding:0.85rem 1rem;"
            f"border:1px solid var(--border);border-radius:10px;background:var(--surface);'>"
            f"{escape(result.get('translated') or '')}</div>",
            unsafe_allow_html=True,
        )
        st.caption(f"Confidence: {float(result.get('confidence') or 0):.0%} · Cached translation")
        with st.expander("Structured translation payload", expanded=False):
            st.json(result)


def render_language_panel() -> None:
    """Sidebar language switcher with RTL CSS for Arabic."""
    if "ui_language" not in st.session_state:
        st.session_state["ui_language"] = "en"
    with st.sidebar.expander(t("sidebar.language"), expanded=False):
        lang_label = st.radio(
            "Language",
            list(I18N_LANGUAGES.keys()),
            index=0 if st.session_state["ui_language"] == "en" else 1,
            horizontal=True,
            key="ui_language_radio",
            label_visibility="collapsed",
        )
        st.session_state["ui_language"] = I18N_LANGUAGES[lang_label]
    if st.session_state["ui_language"] == "ar":
        st.markdown("""
        <style>
        html, body, .stApp, [data-testid="stSidebar"] { direction: rtl; }
        .stMarkdown, .stCaption, [data-testid="stCaptionContainer"],
        .stTextInput label, .stSelectbox label, .stNumberInput label,
        .stRadio label, .stCheckbox label { text-align: right; }
        .news-card-headline, .news-card-source { text-align: right; }
        .stTabs [data-baseweb="tab-list"] { direction: ltr; }  /* keep tab order LTR */
        [data-testid="stMetric"] { text-align: right; }
        </style>
        """, unsafe_allow_html=True)


# ============================================================
# Multi-symbol manager — pinned symbols + open-in-new-tab + side-by-side
# ============================================================
def render_symbol_manager() -> list[str]:
    """Sidebar widget: manage a list of pinned symbols with quick switch +
    'open in new tab' deep links.
    """
    if "pinned_symbols" not in st.session_state:
        st.session_state["pinned_symbols"] = ["AAPL"]
    pinned: list[str] = st.session_state["pinned_symbols"]

    with st.sidebar.expander(t("sidebar.symbols"), expanded=True):
        col1, col2 = st.columns([3, 1])
        new_sym = col1.text_input(
            t("btn.add_symbol"),
            value="",
            key="pin_new_symbol",
            placeholder="AAPL, TSLA, 2222.SR …",
            label_visibility="collapsed",
        )
        if col2.button("➕", key="pin_add_btn", use_container_width=True):
            for s in (new_sym or "").upper().replace(" ", "").split(","):
                s = s.strip()
                if s and s not in pinned:
                    pinned.append(s)
            st.session_state["pin_new_symbol"] = ""
            st.rerun()

        if pinned:
            st.caption(f"{len(pinned)} pinned · click to switch · 🪟 to open in new tab")
            for sym in list(pinned):
                row = st.columns([4, 1, 1])
                if row[0].button(f"📈 **{sym}**", key=f"pin_sw_{sym}",
                                  use_container_width=True,
                                  help=f"Switch primary view to {sym}"):
                    st.query_params["symbol"] = sym
                    st.session_state["pinned_active"] = sym
                    st.rerun()
                # Open in new browser tab via JS link with current query string
                row[1].markdown(
                    f'<a href="?symbol={sym}" target="_blank" '
                    f'style="display:inline-block;padding:4px 8px;border-radius:6px;'
                    f'background:var(--accent-soft);color:var(--accent);'
                    f'text-decoration:none;font-size:0.78rem;text-align:center;'
                    f'border:1px solid var(--accent);">🪟</a>',
                    unsafe_allow_html=True,
                )
                if row[2].button("✕", key=f"pin_rm_{sym}",
                                  help=f"Unpin {sym}",
                                  use_container_width=True):
                    pinned.remove(sym)
                    st.rerun()

            cb1, cb2 = st.columns(2)
            if cb1.button("🗑 " + t("btn.clear"), key="pin_clear_btn",
                           use_container_width=True):
                st.session_state["pinned_symbols"] = []
                st.rerun()
            if cb2.button("📋 Copy all", key="pin_copy_btn",
                           use_container_width=True,
                           help="Copy comma-separated list"):
                st.code(",".join(pinned), language="text")
        else:
            st.caption("Add a symbol above to start pinning.")

    return pinned


def render_theme_customizer() -> tuple[str, dict]:
    """Sidebar theme picker.

    Returns (theme_mode, overrides_dict). The override dict always contains a
    complete, internally-consistent palette so theme switching never produces
    mismatched colors.
    """
    preset_names = list(THEME_PRESETS.keys())

    # First-render initialization (BEFORE any widget instantiation)
    if "theme_preset" not in st.session_state:
        st.session_state["theme_preset"] = "Atlas Light"
    initial = THEME_PRESETS[st.session_state["theme_preset"]]
    for key, val in (
        ("theme_accent", initial["accent"]),
        ("theme_ink", initial["ink"]),
        ("theme_bg", initial["bg"]),
        ("theme_surface", initial["surface"]),
        ("visual_theme_mode", "Dark" if initial["is_dark"] else "Light"),
        ("theme_font", "Inter (default)"),
        ("theme_radius", 12),
        ("theme_density", "Comfortable"),
    ):
        if key not in st.session_state:
            st.session_state[key] = val

    with st.sidebar.expander("🎨 Theme & appearance", expanded=False):
        # Preset selector — change fires the callback BEFORE any other widget
        # re-instantiates, so the cascading session_state updates are valid.
        st.selectbox(
            "Preset",
            preset_names,
            key="theme_preset",
            on_change=_on_theme_preset_change,
            help="Pick a curated palette. Base theme + colors + surface auto-update.",
        )
        preset_name = st.session_state["theme_preset"]
        preset = THEME_PRESETS[preset_name]

        st.radio(
            "Base theme",
            ["System", "Light", "Dark"],
            horizontal=True,
            key="visual_theme_mode",
            help="Auto-set by the preset; override here to mix and match.",
        )

        st.caption("**Color overrides** — fine-tune the preset.")
        c1, c2 = st.columns(2)
        c1.color_picker("Accent", key="theme_accent", help="Buttons · links · highlights")
        c2.color_picker("Text", key="theme_ink", help="Primary text color")
        c3, c4 = st.columns(2)
        c3.color_picker("Background", key="theme_bg")
        c4.color_picker("Card surface", key="theme_surface")

        st.caption("**Typography & layout**")
        st.selectbox("Font family", list(THEME_FONT_OPTIONS.keys()), key="theme_font")
        st.slider("Corner radius (px)", min_value=0, max_value=24, key="theme_radius")
        st.select_slider(
            "Density",
            options=["Compact", "Comfortable", "Spacious"],
            key="theme_density",
        )

        cb1, cb2 = st.columns(2)
        cb1.button("🔄 Reset to preset", key="theme_reset_btn", on_click=_on_theme_reset)
        if cb2.button("💾 Save as default", key="theme_save_btn"):
            st.session_state["_saved_theme"] = {
                "preset": preset_name,
                "accent": st.session_state["theme_accent"],
                "ink": st.session_state["theme_ink"],
                "bg": st.session_state["theme_bg"],
                "surface": st.session_state["theme_surface"],
                "font": st.session_state["theme_font"],
                "radius": st.session_state["theme_radius"],
                "density": st.session_state["theme_density"],
            }
            st.toast("✅ Theme saved for this session.", icon="🎨")

    # Read final state and assemble the complete override block
    accent = st.session_state["theme_accent"]
    ink = st.session_state["theme_ink"]
    bg = st.session_state["theme_bg"]
    surface = st.session_state["theme_surface"]
    radius = int(st.session_state.get("theme_radius", 12))
    density = st.session_state.get("theme_density", "Comfortable")
    font_label = st.session_state.get("theme_font", "Inter (default)")
    theme_mode = st.session_state.get("visual_theme_mode", "Light")

    overrides = _derive_theme_overrides(preset, accent, ink, bg, surface)

    pad_map = {"Compact": "0.6rem", "Comfortable": "1rem", "Spacious": "1.4rem"}
    st.markdown(
        f"<style>:root {{ --radius-base: {radius}px; --pad-base: {pad_map[density]}; "
        f"--font-family: {THEME_FONT_OPTIONS.get(font_label, 'Inter')}, system-ui, sans-serif; }}"
        ".news-card, .metric-card, .panel { border-radius: var(--radius-base) !important; }"
        ".panel { padding: var(--pad-base) !important; }"
        "html, body, [class*='css'] { font-family: var(--font-family) !important; }"
        "</style>",
        unsafe_allow_html=True,
    )
    return theme_mode, overrides


def main() -> None:
    st.set_page_config(page_title="Atlas Stock Intelligence", page_icon="📊", layout="wide")
    render_language_panel()  # must run BEFORE any t() calls below
    theme_mode, theme_overrides = render_theme_customizer()
    inject_styles(theme_mode, overrides=theme_overrides)
    render_number_format_panel()
    pinned_symbols = render_symbol_manager()

    mode_query = str(st.query_params.get("mode", "")).lower()
    default_mode_index = 1 if mode_query in {"binance", "crypto", "bookmap"} else 0
    dashboard_mode = st.sidebar.radio(
        "Dashboard",
        ["Equity Decision Engine", "Binance Bookmap Lab"],
        index=default_mode_index,
        key="dashboard_mode",
        help="Switch between the equity multi-provider research engine and the Binance spot market microstructure dashboard.",
    )
    if dashboard_mode == "Binance Bookmap Lab":
        controls = binance_sidebar_controls()
        render_binance_dashboard_app(controls)
        return

    symbols, primary_symbol, assumptions, keys, enabled_providers, options = sidebar_controls()
    # Snapshot keys for the live multi-provider dashboard
    st.session_state["ibkr_keys_snapshot"] = dict(keys or {})
    if not symbols:
        st.warning("Enter at least one stock symbol to start.")
        st.stop()
    if len(symbols) >= MAX_COMPARE_SYMBOLS:
        st.info(f"Comparison is capped at {MAX_COMPARE_SYMBOLS} symbols to keep the dashboard fast and readable.")

    with st.spinner("Blending live market data, fundamentals, valuation layers, and comparison intelligence..."):
        bundles = load_dashboard(symbols, assumptions, keys, enabled_providers, options)

    primary_bundle = bundles.get(primary_symbol) or bundles[symbols[0]]
    render_hero(primary_bundle, len(symbols))
    render_source_chips(primary_bundle)
    st.write("")

    top_keys = [
        "price",
        "final_ai_score",
        "trend_score",
        "momentum_score",
        "volume_score",
        "relative_strength_score",
        "risk_score",
        "dcf_upside",
    ]
    upper = st.columns(4)
    for index, key in enumerate(top_keys[:4]):
        with upper[index]:
            render_metric_card(primary_bundle, key)
    lower = st.columns(4)
    for index, key in enumerate(top_keys[4:]):
        with lower[index]:
            render_metric_card(primary_bundle, key)

    st.write("")
    summary_col, verdict_col = st.columns([2.1, 1])
    with summary_col:
        st.markdown(
            f"""
            <div class="panel">
                <div class="panel-title">Decision Pulse</div>
                <div class="panel-big">{primary_bundle['metrics'].get('decision_label') or 'Insufficient data'} · {format_score(primary_bundle['metrics'].get('final_ai_score'))}</div>
                <p style="color:var(--muted); margin-top:0.65rem;">
                    Trend: {primary_bundle['metrics'].get('trend_state') or 'No data'} ·
                    Structure: {primary_bundle['metrics'].get('breakout_signal') or 'No data'} ·
                    Volume: {primary_bundle['metrics'].get('volume_spike') or 'No data'} ·
                    Risk: {format_score(primary_bundle['metrics'].get('risk_score'))}
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with verdict_col:
        st.markdown(
            f"""
            <div class="panel">
                <div class="panel-title">Risk Plan</div>
                <div class="panel-big">{format_money(primary_bundle['metrics'].get('stop_loss'), primary_bundle.get('currency', ''))}</div>
                <p style="color:var(--muted); margin-top:0.65rem;">
                    Position size: {format_by_type(primary_bundle['metrics'].get('position_size'), 'integer')}<br/>
                    Reward/risk: {format_multiple(primary_bundle['metrics'].get('risk_reward_to_resistance'))}<br/>
                    Fair value after margin: {format_money(primary_bundle['metrics'].get('fair_value_after_mos'), primary_bundle.get('currency', ''))}
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )
    if options.light_mode:
        st.caption("Light mode is active. Heavy statement endpoints were reduced to maximize speed and resilience.")
    elif len(symbols) > 2:
        st.caption("Wide comparison optimization is active. Heavy endpoints were reduced automatically for reliability.")
    if options.safe_mode:
        st.caption("Safe mode is active. Provider calls were throttled and made more sequential to reduce bans and partial failures.")

    tabs = st.tabs(["Decision Engine", "Decision Intelligence", "Overview", "X+Y Matrix", "Financial Statements", "Valuation", "Quality", "Momentum", "News & Events", "Chart Datafeed", "IBKR Trading", "Providers", "Raw Data / Debug", "Collect Data"])
    with tabs[0]:
        render_decision_tab(primary_bundle, bundles)
    with tabs[1]:
        render_decision_intelligence_tab(primary_bundle, bundles)
    with tabs[2]:
        render_overview_tab(primary_bundle, bundles)
    with tabs[3]:
        render_xy_matrix_tab(bundles)
    with tabs[4]:
        render_financials_tab(primary_bundle)
    with tabs[5]:
        render_valuation_tab(primary_bundle, bundles)
    with tabs[6]:
        render_quality_tab(primary_bundle, bundles)
    with tabs[7]:
        render_momentum_tab(primary_bundle, bundles)
    with tabs[8]:
        render_news_tab(primary_bundle)
    with tabs[9]:
        render_datafeed_tab(primary_bundle)
    with tabs[10]:
        render_ibkr_tab()
    with tabs[11]:
        render_provider_tab(primary_bundle)
    with tabs[12]:
        render_export_tab(primary_bundle, bundles, options.debug_mode)
    with tabs[13]:
        render_collect_data_tab()

if __name__ == "__main__":
    main()
