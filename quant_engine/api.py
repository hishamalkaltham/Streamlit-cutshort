"""FastAPI backend exposing the DataEngine over HTTP.

Run:
    uvicorn quant_engine.api:app --reload --port 8001

All endpoints return structured JSON; failures NEVER raise — they surface as
HTTP 200 with `{"status": "error", "detail": "..."}` so consumers don't have
to handle 500s for routine cache misses.
"""
from __future__ import annotations

import logging
import time
from typing import Any

try:
    from fastapi import FastAPI, HTTPException, Query  # type: ignore
    from fastapi.middleware.cors import CORSMiddleware  # type: ignore
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "FastAPI is required for the api module: pip install fastapi uvicorn"
    ) from exc

from .config import get_settings
from .data_engine import DataEngine
from .models import FusionStrategy
from .translation_engine import TranslationEngine

logger = logging.getLogger("quant_engine.api")


# ─── App + middleware ──────────────────────────────────────────────────────
app = FastAPI(
    title="Quant Engine API",
    description="Multi-provider stock data with intelligent fusion, "
                "diagnostics, health monitoring, and Arabic translation.",
    version="1.0.0",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Singletons ────────────────────────────────────────────────────────────
_engine: DataEngine | None = None
_translator: TranslationEngine | None = None
_started_at = time.time()


def get_engine() -> DataEngine:
    global _engine
    if _engine is None:
        _engine = DataEngine()
    return _engine


def get_translator() -> TranslationEngine:
    global _translator
    if _translator is None:
        _translator = TranslationEngine()
    return _translator


# ─── Endpoints ─────────────────────────────────────────────────────────────
@app.get("/health")
def health() -> dict:
    """Process health + cache + provider summary."""
    eng = get_engine()
    return {
        "status": "ok",
        "uptime_s": round(time.time() - _started_at, 2),
        "providers": eng.get_provider_health(),
        "cache": eng.get_cache_stats(),
        "engine_uptime_s": round(eng.uptime_s, 2),
    }


@app.get("/stock/{symbol}/price")
def stock_price(
    symbol: str,
    strategy: str = Query("weighted_average",
                            description="priority_first | weighted_average | median | confidence_best"),
) -> dict:
    """Fused last-price for a symbol."""
    try:
        strat = FusionStrategy(strategy)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Unknown strategy: {strategy}")
    eng = get_engine()
    return {"status": "ok", "data": eng.get_price(symbol, strategy=strat)}


@app.get("/stock/{symbol}/field/{field}")
def stock_field(
    symbol: str,
    field: str,
    strategy: str = Query("priority_first"),
) -> dict:
    """Fused single-field value (e.g. pe_ratio, market_cap, sector)."""
    try:
        strat = FusionStrategy(strategy)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Unknown strategy: {strategy}")
    eng = get_engine()
    result = eng.get_field(symbol, field, strategy=strat)
    return {
        "status": "ok",
        "data": {
            "symbol": symbol.upper(),
            "field": field,
            "value": result.value,
            "confidence": result.confidence,
            "primary_provider": result.primary_provider,
            "contributors": result.contributors,
            "all_values": result.all_values,
            "warnings": result.warnings,
            "timestamp": result.timestamp,
        },
    }


@app.get("/stock/{symbol}/fundamentals")
def stock_fundamentals(symbol: str) -> dict:
    eng = get_engine()
    bundle = eng.get_fundamentals(symbol)
    return {
        "status": "ok",
        "data": {k: v.to_dict() for k, v in bundle.items()},
    }


@app.get("/stock/{symbol}/metadata")
def stock_metadata(symbol: str) -> dict:
    eng = get_engine()
    bundle = eng.get_metadata(symbol)
    return {
        "status": "ok",
        "data": {k: v.to_dict() for k, v in bundle.items()},
    }


@app.get("/diagnostics")
def diagnostics() -> dict:
    eng = get_engine()
    return {"status": "ok", "data": eng.get_diagnostics()}


@app.get("/providers/health")
def providers_health() -> dict:
    eng = get_engine()
    return {"status": "ok", "data": eng.get_provider_health()}


@app.get("/cache/stats")
def cache_stats() -> dict:
    eng = get_engine()
    return {"status": "ok", "data": eng.get_cache_stats()}


@app.post("/cache/clear")
def cache_clear(symbol: str | None = Query(None)) -> dict:
    eng = get_engine()
    return {"status": "ok", "data": eng.clear_cache(symbol)}


# ─── Translation endpoint ──────────────────────────────────────────────────
@app.post("/translate")
def translate(
    text: str = Query(..., description="Text to translate"),
    mode: str = Query("financial",
                        description="financial | ui | report"),
    target: str = Query("ar"),
) -> dict:
    tr = get_translator()
    return {"status": "ok", "data": tr.translate(text, mode=mode, target=target)}


@app.get("/translate/stats")
def translate_stats() -> dict:
    return {"status": "ok", "data": get_translator().stats()}


# ─── Settings (debug-only) ─────────────────────────────────────────────────
@app.get("/settings")
def settings() -> dict:
    """Returns non-secret settings for debugging."""
    s = get_settings()
    safe = {
        "provider_chain": s.provider_chain,
        "request_timeout": s.request_timeout,
        "max_retries": s.max_retries,
        "ttl_realtime": s.ttl_realtime,
        "ttl_historical": s.ttl_historical,
        "ttl_fundamental": s.ttl_fundamental,
        "log_level": s.log_level,
        "debug": s.debug,
        "providers_with_keys": {
            "polygon":  bool(s.polygon_key),
            "tiingo":   bool(s.tiingo_key),
            "intrinio": bool(s.intrinio_key),
            "finnhub":  bool(s.finnhub_key),
            "yahoo":    True,  # keyless
        },
    }
    return {"status": "ok", "data": safe}


__all__ = ["app", "get_engine", "get_translator"]
