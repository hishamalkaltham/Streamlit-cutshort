"""Smart in-memory cache with per-data-type TTL and LRU eviction.

Design:
  • LRU eviction (OrderedDict — O(1) move-to-end)
  • Per-key TTL — different data types expire at different rates
  • Thread-safe
  • Hit/miss/eviction counters for observability

Cache key format: f"{provider}:{symbol}:{field}:{data_type}"
"""
from __future__ import annotations

import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any

from .config import get_settings
from .models import DataType


@dataclass
class CacheStats:
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    expirations: int = 0
    sets: int = 0
    size: int = 0
    capacity: int = 0

    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return self.hits / total if total else 0.0

    def to_dict(self) -> dict:
        return {
            "hits": self.hits, "misses": self.misses,
            "evictions": self.evictions, "expirations": self.expirations,
            "sets": self.sets, "size": self.size, "capacity": self.capacity,
            "hit_rate": round(self.hit_rate, 4),
        }


@dataclass
class _Entry:
    value: Any
    expires_at: float


class SmartCache:
    """Thread-safe TTL+LRU cache with per-data-type expiration."""

    def __init__(self, max_size: int | None = None):
        s = get_settings()
        self._max_size = max_size or s.cache_max_size
        self._store: "OrderedDict[str, _Entry]" = OrderedDict()
        self._lock = threading.RLock()
        self._stats = CacheStats(capacity=self._max_size)
        self._ttls: dict[DataType, int] = {
            DataType.REALTIME:    s.ttl_realtime,
            DataType.HISTORICAL:  s.ttl_historical,
            DataType.FUNDAMENTAL: s.ttl_fundamental,
            DataType.NEWS:        s.ttl_news,
            DataType.METADATA:    s.ttl_metadata,
        }

    # ─── key construction ───────────────────────────────────────────────────
    @staticmethod
    def make_key(provider: str, symbol: str, field: str, data_type: DataType | str) -> str:
        dt = data_type.value if isinstance(data_type, DataType) else str(data_type)
        return f"{provider}:{symbol.upper()}:{field}:{dt}"

    def ttl_for(self, data_type: DataType) -> int:
        return self._ttls.get(data_type, get_settings().ttl_realtime)

    # ─── public API ─────────────────────────────────────────────────────────
    def get(self, key: str) -> Any:
        """Return cached value or None on miss / expired."""
        now = time.time()
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                self._stats.misses += 1
                return None
            if entry.expires_at < now:
                # Expired — drop it
                self._store.pop(key, None)
                self._stats.expirations += 1
                self._stats.misses += 1
                self._stats.size = len(self._store)
                return None
            # Bump LRU position
            self._store.move_to_end(key)
            self._stats.hits += 1
            return entry.value

    def set(self, key: str, value: Any, ttl: int) -> None:
        """Insert or update an entry. Evicts oldest if capacity exceeded."""
        with self._lock:
            self._store[key] = _Entry(value=value, expires_at=time.time() + max(1, int(ttl)))
            self._store.move_to_end(key)
            self._stats.sets += 1
            while len(self._store) > self._max_size:
                self._store.popitem(last=False)  # evict LRU
                self._stats.evictions += 1
            self._stats.size = len(self._store)

    def set_typed(self, key: str, value: Any, data_type: DataType) -> None:
        self.set(key, value, self.ttl_for(data_type))

    def invalidate(self, key: str) -> bool:
        with self._lock:
            removed = self._store.pop(key, None) is not None
            self._stats.size = len(self._store)
            return removed

    def invalidate_symbol(self, symbol: str) -> int:
        """Drop every entry for `symbol` regardless of provider/field."""
        symbol_up = symbol.upper()
        with self._lock:
            keys = [k for k in self._store if f":{symbol_up}:" in k]
            for k in keys:
                self._store.pop(k, None)
            self._stats.size = len(self._store)
            return len(keys)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()
            self._stats.size = 0

    def stats(self) -> CacheStats:
        with self._lock:
            self._stats.size = len(self._store)
            return CacheStats(**self._stats.__dict__)

    def __len__(self) -> int:
        with self._lock:
            return len(self._store)

    def __contains__(self, key: str) -> bool:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return False
            return entry.expires_at >= time.time()


# ─── module-level singleton (optional convenience) ──────────────────────────
_default_cache: SmartCache | None = None


def get_default_cache() -> SmartCache:
    global _default_cache
    if _default_cache is None:
        _default_cache = SmartCache()
    return _default_cache


__all__ = ["SmartCache", "CacheStats", "get_default_cache"]
