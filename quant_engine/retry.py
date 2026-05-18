"""Exponential backoff with jitter — sync + async aware.

Wraps any callable that may raise transient errors. The decision of *what's
transient* is delegated to a predicate, so the same engine handles HTTP 429,
ProviderTimeout, ConnectionError, etc. uniformly.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, TypeVar

from .config import get_settings
from .errors import ProviderRateLimited, ProviderTimeout

logger = logging.getLogger("quant_engine.retry")

T = TypeVar("T")


# ─── Default predicate — what counts as "retry me" ──────────────────────────
def default_is_transient(exc: Exception) -> bool:
    """Standard transient-error classifier. Override per provider if needed."""
    if isinstance(exc, (ProviderTimeout, ProviderRateLimited)):
        return True
    name = type(exc).__name__
    if name in {"ConnectTimeout", "ReadTimeout", "ConnectionError",
                 "RemoteProtocolError", "ChunkedEncodingError",
                 "Timeout", "RequestException"}:
        return True
    msg = str(exc).lower()
    return any(needle in msg for needle in (
        "timed out", "timeout", "rate limit", "too many requests",
        "temporarily unavailable", "service unavailable", "bad gateway",
        "gateway timeout", "connection reset",
    ))


@dataclass
class BackoffConfig:
    """Per-call retry parameters (overrides settings on a one-off basis)."""
    max_retries: int | None = None
    base_delay: float | None = None
    max_delay: float | None = None
    jitter: float | None = None
    multiplier: float = 2.0

    def resolve(self) -> "BackoffConfig":
        s = get_settings()
        return BackoffConfig(
            max_retries=self.max_retries if self.max_retries is not None else s.max_retries,
            base_delay=self.base_delay if self.base_delay is not None else s.backoff_base,
            max_delay=self.max_delay  if self.max_delay  is not None else s.backoff_max,
            jitter=self.jitter        if self.jitter     is not None else s.backoff_jitter,
            multiplier=self.multiplier,
        )

    def delay_for(self, attempt: int) -> float:
        """Compute the delay for attempt N (0-indexed)."""
        delay = min(self.base_delay * (self.multiplier ** attempt), self.max_delay)  # type: ignore[operator]
        if self.jitter:
            spread = delay * float(self.jitter)
            delay = max(0.0, delay + random.uniform(-spread, spread))
        return delay


def with_backoff(
    fn: Callable[..., T],
    *args: Any,
    is_transient: Callable[[Exception], bool] = default_is_transient,
    config: BackoffConfig | None = None,
    on_retry: Callable[[int, Exception, float], None] | None = None,
    **kwargs: Any,
) -> T:
    """Synchronous retry wrapper. Re-raises on the final attempt."""
    cfg = (config or BackoffConfig()).resolve()
    last_exc: Exception | None = None
    for attempt in range(cfg.max_retries + 1):  # type: ignore[operator]
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            if attempt >= cfg.max_retries or not is_transient(exc):  # type: ignore[operator]
                raise
            delay = cfg.delay_for(attempt)
            if on_retry:
                try:
                    on_retry(attempt, exc, delay)
                except Exception:
                    pass
            logger.debug("Retry %d/%s after %.2fs — %s",
                          attempt + 1, cfg.max_retries, delay, exc)
            time.sleep(delay)
    assert last_exc is not None  # pragma: no cover
    raise last_exc


async def with_backoff_async(
    fn: Callable[..., Awaitable[T]],
    *args: Any,
    is_transient: Callable[[Exception], bool] = default_is_transient,
    config: BackoffConfig | None = None,
    on_retry: Callable[[int, Exception, float], None] | None = None,
    **kwargs: Any,
) -> T:
    """Async equivalent of `with_backoff`."""
    cfg = (config or BackoffConfig()).resolve()
    last_exc: Exception | None = None
    for attempt in range(cfg.max_retries + 1):  # type: ignore[operator]
        try:
            return await fn(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            if attempt >= cfg.max_retries or not is_transient(exc):  # type: ignore[operator]
                raise
            delay = cfg.delay_for(attempt)
            if on_retry:
                try:
                    on_retry(attempt, exc, delay)
                except Exception:
                    pass
            logger.debug("Async retry %d/%s after %.2fs — %s",
                          attempt + 1, cfg.max_retries, delay, exc)
            await asyncio.sleep(delay)
    assert last_exc is not None  # pragma: no cover
    raise last_exc


__all__ = [
    "BackoffConfig", "default_is_transient",
    "with_backoff", "with_backoff_async",
]
