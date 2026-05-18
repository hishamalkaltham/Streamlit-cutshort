"""Data fusion strategies — merge multiple provider responses into one answer.

Strategies:
  • PRIORITY_FIRST   — first non-null wins (canonical source)
  • WEIGHTED_AVERAGE — weight = health × per-provider trust × confidence
  • MEDIAN           — robust to outliers
  • CONFIDENCE_BEST  — highest individual confidence wins

Every strategy returns a `FusionResult` with:
  • value
  • confidence  (0..1)
  • primary_provider
  • contributors (ordered)
  • all_values
  • warnings
"""
from __future__ import annotations

import logging
import statistics
from typing import Iterable

from .errors import FusionError
from .models import FusionResult, FusionStrategy, ProviderResponse

logger = logging.getLogger("quant_engine.fusion")


# Per-resource trust priors — used as a base weight when no health monitor
# is plugged in. Same shape as `quant_engine.models.PROVIDER_TRUST` would
# carry; kept here so fusion has a self-contained default.
DEFAULT_TRUST: dict[str, float] = {
    "polygon":  1.00,
    "intrinio": 0.95,
    "tiingo":   0.92,
    "finnhub":  0.85,
    "yahoo":    0.65,
}


def _coerce_numeric(v) -> float | None:
    if v is None:
        return None
    if isinstance(v, bool):  # bool is an int — exclude
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f != f or f in (float("inf"), float("-inf")):
        return None
    return f


def _ok_responses(responses: Iterable[ProviderResponse]) -> list[ProviderResponse]:
    return [r for r in responses if r.ok]


# ─── Strategies ─────────────────────────────────────────────────────────────
def fuse_priority_first(
    responses: list[ProviderResponse],
    chain: list[str] | None = None,
) -> FusionResult:
    """Use the first OK response that matches the priority chain order."""
    by_provider = {r.provider: r for r in responses}
    all_values = {r.provider: r.value for r in responses}
    warnings: list[str] = []

    chain = chain or [r.provider for r in responses]
    primary: ProviderResponse | None = None
    for prov in chain:
        r = by_provider.get(prov)
        if r is not None and r.ok:
            primary = r
            break

    if primary is None:
        # last-ditch: any OK response
        ok_set = _ok_responses(responses)
        if not ok_set:
            warnings.append("no_provider_returned_data")
            return FusionResult(
                value=None, confidence=0.0, primary_provider=None,
                contributors=[], all_values=all_values,
                strategy=FusionStrategy.PRIORITY_FIRST.value, warnings=warnings,
            )
        primary = ok_set[0]
        warnings.append(f"chain_exhausted_fell_back_to_{primary.provider}")

    contributors = [primary.provider] + [
        r.provider for r in _ok_responses(responses)
        if r.provider != primary.provider
    ]
    return FusionResult(
        value=primary.value,
        confidence=max(0.5, primary.confidence),
        primary_provider=primary.provider,
        contributors=contributors,
        all_values=all_values,
        strategy=FusionStrategy.PRIORITY_FIRST.value,
        warnings=warnings,
    )


def fuse_weighted_average(
    responses: list[ProviderResponse],
    *,
    health_scores: dict[str, float] | None = None,
    trust_overrides: dict[str, float] | None = None,
) -> FusionResult:
    """Weight = health × trust × confidence.

    Only valid for numeric values. Falls back to PRIORITY_FIRST otherwise.
    """
    health_scores = health_scores or {}
    trust = {**DEFAULT_TRUST, **(trust_overrides or {})}
    all_values = {r.provider: r.value for r in responses}
    warnings: list[str] = []

    pairs: list[tuple[ProviderResponse, float, float]] = []  # (resp, value, weight)
    for r in _ok_responses(responses):
        v = _coerce_numeric(r.value)
        if v is None:
            warnings.append(f"{r.provider}_non_numeric_skipped")
            continue
        h = health_scores.get(r.provider, 0.7)
        t = trust.get(r.provider, 0.5)
        c = max(0.05, float(r.confidence or 0.5))
        w = h * t * c
        pairs.append((r, v, w))

    if not pairs:
        warnings.append("no_numeric_values_available")
        return fuse_priority_first(responses)  # graceful fallback

    total_w = sum(w for _, _, w in pairs)
    avg = sum(v * w for _, v, w in pairs) / total_w
    contributors = [r.provider for r, _, _ in sorted(pairs, key=lambda x: -x[2])]
    primary = contributors[0] if contributors else None
    # Confidence: how concentrated is the contribution among top providers?
    top_w = max(w for _, _, w in pairs)
    confidence = round(min(1.0, 0.4 + 0.6 * (top_w / total_w)), 4)

    return FusionResult(
        value=round(avg, 6),
        confidence=confidence,
        primary_provider=primary,
        contributors=contributors,
        all_values=all_values,
        strategy=FusionStrategy.WEIGHTED_AVERAGE.value,
        warnings=warnings,
    )


def fuse_median(responses: list[ProviderResponse]) -> FusionResult:
    """Median value — robust against single-provider outliers."""
    all_values = {r.provider: r.value for r in responses}
    numeric: list[tuple[ProviderResponse, float]] = []
    warnings: list[str] = []
    for r in _ok_responses(responses):
        v = _coerce_numeric(r.value)
        if v is None:
            warnings.append(f"{r.provider}_non_numeric_skipped")
            continue
        numeric.append((r, v))
    if not numeric:
        return fuse_priority_first(responses)

    numeric.sort(key=lambda x: x[1])
    values = [v for _, v in numeric]
    med = statistics.median(values)

    # Pick the provider closest to the median as "primary"
    primary_resp = min(numeric, key=lambda x: abs(x[1] - med))[0]
    contributors = [r.provider for r, _ in numeric]

    # Confidence drops when spread is wide
    spread = (max(values) - min(values)) / (med if med else 1.0) if med else 0.0
    confidence = round(max(0.1, min(1.0, 1.0 - min(spread, 1.0))), 4)

    return FusionResult(
        value=round(med, 6),
        confidence=confidence,
        primary_provider=primary_resp.provider,
        contributors=contributors,
        all_values=all_values,
        strategy=FusionStrategy.MEDIAN.value,
        warnings=warnings,
    )


def fuse_confidence_best(responses: list[ProviderResponse]) -> FusionResult:
    """Pick the response with the highest (provider-reported) confidence."""
    ok = _ok_responses(responses)
    all_values = {r.provider: r.value for r in responses}
    warnings: list[str] = []
    if not ok:
        warnings.append("no_ok_responses")
        return FusionResult(
            value=None, confidence=0.0, primary_provider=None,
            contributors=[], all_values=all_values,
            strategy=FusionStrategy.CONFIDENCE_BEST.value, warnings=warnings,
        )
    ok.sort(key=lambda r: -float(r.confidence or 0.0))
    primary = ok[0]
    return FusionResult(
        value=primary.value,
        confidence=max(0.5, float(primary.confidence)),
        primary_provider=primary.provider,
        contributors=[r.provider for r in ok],
        all_values=all_values,
        strategy=FusionStrategy.CONFIDENCE_BEST.value,
        warnings=warnings,
    )


# ─── Dispatcher ─────────────────────────────────────────────────────────────
def fuse(
    responses: list[ProviderResponse],
    strategy: FusionStrategy | str = FusionStrategy.PRIORITY_FIRST,
    *,
    chain: list[str] | None = None,
    health_scores: dict[str, float] | None = None,
    trust_overrides: dict[str, float] | None = None,
) -> FusionResult:
    """Single entry-point for the fusion engine."""
    if not responses:
        raise FusionError("fuse() called with empty responses")
    strat = FusionStrategy(strategy) if not isinstance(strategy, FusionStrategy) else strategy
    if strat is FusionStrategy.PRIORITY_FIRST:
        return fuse_priority_first(responses, chain=chain)
    if strat is FusionStrategy.WEIGHTED_AVERAGE:
        return fuse_weighted_average(responses, health_scores=health_scores,
                                       trust_overrides=trust_overrides)
    if strat is FusionStrategy.MEDIAN:
        return fuse_median(responses)
    if strat is FusionStrategy.CONFIDENCE_BEST:
        return fuse_confidence_best(responses)
    raise FusionError(f"Unknown fusion strategy: {strategy}")


__all__ = [
    "fuse", "fuse_priority_first", "fuse_weighted_average",
    "fuse_median", "fuse_confidence_best", "DEFAULT_TRUST",
]
