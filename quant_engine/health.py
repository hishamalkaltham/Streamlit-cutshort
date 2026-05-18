"""Provider health monitor.

Maintains a rolling window of (success, latency) per provider and computes a
health score in [0, 1]. The DataEngine queries this monitor before issuing a
request and **dynamically reorders** its provider chain to favour healthy
providers.

Recovery: with every successful call, the score creeps back up by
`recovery_factor` so a provider that had a bad spell can earn its way back
to the top of the chain.
"""
from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Iterable

from .config import get_settings
from .models import HealthSnapshot, HealthStatus


@dataclass
class _Window:
    """Rolling per-provider window."""
    successes: deque[bool] = field(default_factory=deque)
    latencies: deque[float] = field(default_factory=deque)
    rate_limits: int = 0
    errors: int = 0
    total: int = 0
    score: float = 0.7   # neutral-warm start
    last_check: float = field(default_factory=time.time)


class ProviderHealthMonitor:
    """Health-track every provider and expose a deterministic score."""

    def __init__(self, window_size: int | None = None):
        s = get_settings()
        self._window_size = window_size or s.health_window_size
        self._degraded_below = s.health_degraded_below
        self._unhealthy_below = s.health_unhealthy_below
        self._recovery_factor = s.health_recovery_factor
        self._windows: dict[str, _Window] = {}
        self._lock = threading.RLock()

    # ─── update path ────────────────────────────────────────────────────────
    def record(
        self,
        provider: str,
        *,
        success: bool,
        latency_ms: float,
        error_type: str | None = None,
    ) -> None:
        with self._lock:
            w = self._windows.setdefault(provider, _Window(
                successes=deque(maxlen=self._window_size),
                latencies=deque(maxlen=self._window_size),
            ))
            w.successes.append(bool(success))
            w.latencies.append(float(latency_ms))
            w.total += 1
            w.last_check = time.time()
            if not success:
                w.errors += 1
                if error_type and "rate" in error_type.lower():
                    w.rate_limits += 1

            # Recompute score (recency-weighted in the window)
            w.score = self._compute_score(w)

    def boost(self, provider: str) -> None:
        """Manually nudge a provider's score up after a clean recovery period."""
        with self._lock:
            w = self._windows.get(provider)
            if w:
                w.score = min(1.0, w.score + self._recovery_factor)

    # ─── read path ──────────────────────────────────────────────────────────
    def score(self, provider: str) -> float:
        with self._lock:
            w = self._windows.get(provider)
            return w.score if w else 0.7  # neutral default for new providers

    def status(self, provider: str) -> str:
        s = self.score(provider)
        if s >= self._degraded_below:
            return HealthStatus.HEALTHY.value
        if s >= self._unhealthy_below:
            return HealthStatus.DEGRADED.value
        return HealthStatus.UNHEALTHY.value

    def snapshot(self, provider: str) -> HealthSnapshot:
        with self._lock:
            w = self._windows.get(provider)
            if w is None or w.total == 0:
                return HealthSnapshot(provider=provider)
            successes = sum(1 for x in w.successes if x)
            success_rate = successes / len(w.successes) if w.successes else 0.0
            avg_lat = sum(w.latencies) / len(w.latencies) if w.latencies else 0.0
            sorted_lat = sorted(w.latencies)
            p95 = sorted_lat[int(0.95 * (len(sorted_lat) - 1))] if sorted_lat else 0.0
            return HealthSnapshot(
                provider=provider,
                status=self.status(provider),
                score=round(w.score, 4),
                success_rate=round(success_rate, 4),
                avg_latency_ms=round(avg_lat, 2),
                p95_latency_ms=round(p95, 2),
                error_count=w.errors,
                rate_limit_count=w.rate_limits,
                total_calls=w.total,
            )

    def all_snapshots(self) -> list[HealthSnapshot]:
        with self._lock:
            providers = list(self._windows.keys())
        return [self.snapshot(p) for p in providers]

    # ─── ordering ───────────────────────────────────────────────────────────
    def reorder_chain(self, chain: Iterable[str]) -> list[str]:
        """Return `chain` re-sorted so healthier providers come first.

        Preserves stable ordering inside the same health bucket so the static
        priority from settings still matters when scores are equal.
        """
        chain = list(chain)
        # Higher score first; preserve original index as tiebreaker
        return sorted(chain, key=lambda p: (-self.score(p), chain.index(p)))

    def reset(self, provider: str | None = None) -> None:
        with self._lock:
            if provider is None:
                self._windows.clear()
            else:
                self._windows.pop(provider, None)

    # ─── scoring formula ────────────────────────────────────────────────────
    def _compute_score(self, w: _Window) -> float:
        """Weighted score:
            0.55 × success_rate +
            0.30 × latency_score +
            0.15 × rate_limit_penalty.
        """
        if not w.successes:
            return w.score
        success_rate = sum(1 for x in w.successes if x) / len(w.successes)

        # Latency score: 1.0 if avg ≤ 200ms, decaying linearly to 0 at 5000ms
        avg_lat = sum(w.latencies) / len(w.latencies) if w.latencies else 0.0
        if avg_lat <= 200:
            latency_score = 1.0
        elif avg_lat >= 5000:
            latency_score = 0.0
        else:
            latency_score = 1.0 - (avg_lat - 200) / 4800

        # Rate-limit penalty: each rate-limit in the window costs 5%
        recent_rate_limits = sum(
            1 for s, l in zip(w.successes, w.latencies)
            if not s and l > 0
        )
        rate_limit_score = max(0.0, 1.0 - 0.05 * recent_rate_limits)

        return round(
            0.55 * success_rate +
            0.30 * latency_score +
            0.15 * rate_limit_score,
            4,
        )


__all__ = ["ProviderHealthMonitor"]
