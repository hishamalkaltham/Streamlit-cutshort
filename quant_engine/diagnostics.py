"""Per-request telemetry collector.

Every provider call goes through `record()`. Aggregations (last-N, by-provider,
percentiles) are computed on demand so writes stay O(1).
"""
from __future__ import annotations

import statistics
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from .models import DiagnosticEntry


@dataclass
class DiagnosticsSummary:
    total_calls: int
    success_count: int
    error_count: int
    success_rate: float
    avg_latency_ms: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    by_provider: dict
    by_error_type: dict
    window: int


class DiagnosticsCollector:
    """Bounded in-memory ring buffer of provider call records."""

    def __init__(self, max_entries: int = 5000):
        self._max_entries = max_entries
        self._entries: deque[DiagnosticEntry] = deque(maxlen=max_entries)
        self._lock = threading.RLock()
        self._started_at = time.time()

    # ─── write path ─────────────────────────────────────────────────────────
    def record(
        self,
        *,
        provider: str,
        symbol: str,
        field: str,
        success: bool,
        latency_ms: float,
        error_type: str | None = None,
        **extra: Any,
    ) -> None:
        entry = DiagnosticEntry(
            provider=provider, symbol=symbol, field=field,
            success=bool(success), latency_ms=float(latency_ms),
            error_type=error_type,
            extra=extra,
        )
        with self._lock:
            self._entries.append(entry)

    # ─── read path ──────────────────────────────────────────────────────────
    def all_entries(self) -> list[DiagnosticEntry]:
        with self._lock:
            return list(self._entries)

    def recent(self, n: int = 100) -> list[DiagnosticEntry]:
        with self._lock:
            if n >= len(self._entries):
                return list(self._entries)
            return list(self._entries)[-n:]

    def by_provider(self, provider: str) -> list[DiagnosticEntry]:
        with self._lock:
            return [e for e in self._entries if e.provider == provider]

    def by_symbol(self, symbol: str) -> list[DiagnosticEntry]:
        sym = symbol.upper()
        with self._lock:
            return [e for e in self._entries if e.symbol.upper() == sym]

    # ─── aggregations ───────────────────────────────────────────────────────
    def summary(self) -> DiagnosticsSummary:
        entries = self.all_entries()
        total = len(entries)
        if total == 0:
            return DiagnosticsSummary(0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, {}, {}, 0)

        successes = [e for e in entries if e.success]
        success_count = len(successes)
        latencies = sorted(e.latency_ms for e in entries)
        avg = statistics.fmean(latencies)
        p50 = _quantile(latencies, 0.50)
        p95 = _quantile(latencies, 0.95)
        p99 = _quantile(latencies, 0.99)

        # Per-provider breakdown
        per_prov: dict[str, dict] = {}
        for e in entries:
            slot = per_prov.setdefault(e.provider, {
                "calls": 0, "success": 0, "errors": 0,
                "latencies_ms": [],
            })
            slot["calls"] += 1
            if e.success:
                slot["success"] += 1
            else:
                slot["errors"] += 1
            slot["latencies_ms"].append(e.latency_ms)
        for prov, slot in per_prov.items():
            ls = sorted(slot["latencies_ms"])
            slot["avg_latency_ms"] = round(statistics.fmean(ls), 2) if ls else 0.0
            slot["p95_latency_ms"] = round(_quantile(ls, 0.95), 2)
            slot["success_rate"] = round(slot["success"] / slot["calls"], 4) if slot["calls"] else 0.0
            del slot["latencies_ms"]  # keep response small

        # Error-type breakdown
        per_err: dict[str, int] = {}
        for e in entries:
            if not e.success and e.error_type:
                per_err[e.error_type] = per_err.get(e.error_type, 0) + 1

        return DiagnosticsSummary(
            total_calls=total,
            success_count=success_count,
            error_count=total - success_count,
            success_rate=round(success_count / total, 4),
            avg_latency_ms=round(avg, 2),
            p50_latency_ms=round(p50, 2),
            p95_latency_ms=round(p95, 2),
            p99_latency_ms=round(p99, 2),
            by_provider=per_prov,
            by_error_type=per_err,
            window=self._max_entries,
        )

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()
            self._started_at = time.time()

    @property
    def uptime_s(self) -> float:
        return time.time() - self._started_at


def _quantile(sorted_values: list[float], q: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    idx = max(0, min(int(round(q * (len(sorted_values) - 1))), len(sorted_values) - 1))
    return sorted_values[idx]


__all__ = ["DiagnosticsCollector", "DiagnosticsSummary"]
