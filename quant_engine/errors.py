"""Exception hierarchy for the quant_engine package.

Public methods catch every internal exception and return structured error
dicts — these classes exist for **internal** flow control only.
"""
from __future__ import annotations


class QuantEngineError(Exception):
    """Root for every error raised inside the engine."""


# ─── Provider / transport ────────────────────────────────────────────────────
class ProviderError(QuantEngineError):
    """Base class for any provider-side failure."""

    def __init__(self, provider: str, message: str = "", *, status: int | None = None):
        self.provider = provider
        self.status = status
        super().__init__(f"[{provider}] {message}")


class ProviderTimeout(ProviderError):
    """Provider did not answer within the configured timeout."""


class ProviderRateLimited(ProviderError):
    """Provider returned 429 or its equivalent — caller should back off."""


class ProviderUnauthorized(ProviderError):
    """API key missing, invalid, or premium-only endpoint."""


class ProviderBadResponse(ProviderError):
    """Provider returned an unparseable / malformed body."""


class AllProvidersFailed(QuantEngineError):
    """No provider in the chain produced a usable response."""

    def __init__(self, symbol: str, attempts: list[dict]):
        self.symbol = symbol
        self.attempts = attempts
        super().__init__(f"All providers failed for {symbol}: {len(attempts)} attempts")


# ─── Data quality ────────────────────────────────────────────────────────────
class FusionError(QuantEngineError):
    """Could not fuse provider responses (e.g. all empty)."""


class CacheError(QuantEngineError):
    """Cache backend unavailable or corrupt."""


# ─── Translation ─────────────────────────────────────────────────────────────
class TranslationError(QuantEngineError):
    """AI translation pipeline failure (caller should fall back to glossary)."""


# ─── Configuration ───────────────────────────────────────────────────────────
class ConfigurationError(QuantEngineError):
    """Required environment variable / setting is missing or invalid."""


__all__ = [
    "QuantEngineError",
    "ProviderError", "ProviderTimeout", "ProviderRateLimited",
    "ProviderUnauthorized", "ProviderBadResponse", "AllProvidersFailed",
    "FusionError", "CacheError",
    "TranslationError", "ConfigurationError",
]
