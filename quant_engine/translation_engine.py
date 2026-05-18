"""Financial-grade Arabic translation engine.

Pipeline: text → glossary (deterministic financial term map) → AI (Anthropic
or OpenAI) → number-format post-processing → Arabic output.

Why hybrid?
  • Glossary FIRST  → guarantees correct financial terminology
                       ("Revenue → الإيرادات", not "العائد")
  • AI SECOND       → handles natural phrasing & context the glossary can't
  • Post-process    → keeps Latin tickers ($AAPL), normalizes numbers
                       ("1.5B" → "1.5 مليار")

The glossary path alone runs offline and covers ~80% of common UI strings,
so the engine works even without an AI key.
"""
from __future__ import annotations

import hashlib
import logging
import os
import re
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

from .config import get_settings
from .errors import TranslationError

logger = logging.getLogger("quant_engine.translation")


# ─── Financial glossary (the deterministic core) ─────────────────────────────
# Order matters — longer phrases first so we don't translate a substring twice.
FINANCIAL_GLOSSARY: list[tuple[str, str]] = [
    # Multi-word phrases
    ("Net Income",                    "صافي الدخل"),
    ("Net Loss",                      "صافي الخسارة"),
    ("Operating Income",              "الدخل التشغيلي"),
    ("Operating Cash Flow",           "التدفق النقدي التشغيلي"),
    ("Free Cash Flow",                "التدفق النقدي الحر"),
    ("Cash Flow",                     "التدفق النقدي"),
    ("Cash and Equivalents",          "النقد وما في حكمه"),
    ("Income Statement",              "قائمة الدخل"),
    ("Balance Sheet",                 "الميزانية العمومية"),
    ("Cash Flow Statement",           "قائمة التدفقات النقدية"),
    ("Earnings Per Share",            "ربحية السهم"),
    ("Price to Earnings",             "مكرر الربحية"),
    ("Market Capitalization",         "القيمة السوقية"),
    ("Market Cap",                    "القيمة السوقية"),
    ("Dividend Yield",                "عائد التوزيعات"),
    ("Total Assets",                  "إجمالي الأصول"),
    ("Total Liabilities",             "إجمالي الخصوم"),
    ("Shareholders Equity",           "حقوق المساهمين"),
    ("Gross Profit",                  "إجمالي الربح"),
    ("Operating Expenses",            "المصروفات التشغيلية"),
    ("Cost of Goods Sold",            "تكلفة البضاعة المباعة"),
    ("Cost of Revenue",               "تكلفة الإيرادات"),
    ("Insider Trading",               "تداولات المطلعين"),
    ("Institutional Holdings",        "الملكية المؤسسية"),
    ("Analyst Estimates",             "تقديرات المحللين"),
    ("Consensus Estimates",           "تقديرات إجماع المحللين"),
    ("Bullish",                       "اتجاه صاعد"),
    ("Bearish",                       "اتجاه هابط"),
    ("Buy Recommendation",            "توصية بالشراء"),
    ("Sell Recommendation",           "توصية بالبيع"),
    ("Hold",                          "احتفاظ"),
    ("Strong Buy",                    "شراء قوي"),
    ("Strong Sell",                   "بيع قوي"),

    # Single words
    ("Revenue",        "الإيرادات"),
    ("Earnings",       "الأرباح"),
    ("Profit",         "الربح"),
    ("Loss",           "الخسارة"),
    ("Assets",         "الأصول"),
    ("Liabilities",    "الخصوم"),
    ("Equity",         "حقوق الملكية"),
    ("Debt",           "الديون"),
    ("Dividend",       "التوزيع النقدي"),
    ("Dividends",      "التوزيعات النقدية"),
    ("Shareholders",   "المساهمون"),
    ("Stock",          "السهم"),
    ("Stocks",         "الأسهم"),
    ("Shares",         "الأسهم"),
    ("Volume",         "الحجم"),
    ("Price",          "السعر"),
    ("Open",           "الافتتاح"),
    ("Close",          "الإغلاق"),
    ("High",           "الأعلى"),
    ("Low",            "الأدنى"),
    ("Bid",            "العرض"),
    ("Ask",            "الطلب"),
    ("Spread",         "الفارق"),
    ("Volatility",     "التقلب"),
    ("Momentum",       "الزخم"),
    ("Trend",          "الاتجاه"),
    ("Support",        "الدعم"),
    ("Resistance",     "المقاومة"),
    ("Sector",         "القطاع"),
    ("Industry",       "الصناعة"),
    ("Index",          "المؤشر"),
    ("Indices",        "المؤشرات"),
    ("Market",         "السوق"),
    ("Markets",        "الأسواق"),
    ("Quarter",        "الربع"),
    ("Quarterly",      "ربع سنوي"),
    ("Annual",         "سنوي"),
    ("Annually",       "سنوياً"),
    ("Forecast",       "التوقعات"),
    ("Outlook",        "النظرة المستقبلية"),
    ("Guidance",       "التوجيه"),
    ("Risk",           "المخاطر"),
    ("Return",         "العائد"),
    ("Yield",          "العائد"),
    ("Performance",    "الأداء"),
    ("Valuation",      "التقييم"),
    ("Fundamentals",   "الأساسيات"),
    ("Insider",        "المطّلع"),
    ("Investor",       "المستثمر"),
    ("Investors",      "المستثمرون"),
    ("Trader",         "المتداول"),
    ("Trading",        "التداول"),
    ("Buy",            "شراء"),
    ("Sell",           "بيع"),
]


# ─── Number normalization ───────────────────────────────────────────────────
_NUM_PATTERN = re.compile(
    r"(?P<sign>-?)\$?(?P<value>\d+(?:[.,]\d+)?)\s*(?P<unit>[KMBTkmbt])\b"
)
_UNIT_AR = {
    "K": "ألف", "M": "مليون", "B": "مليار", "T": "تريليون",
    "k": "ألف", "m": "مليون", "b": "مليار", "t": "تريليون",
}


def _normalize_numbers_ar(text: str) -> str:
    """Translate '$1.5B' → '1.5 مليار', '$2.3M' → '2.3 مليون'."""
    def repl(m: re.Match) -> str:
        sign = m.group("sign") or ""
        value = m.group("value").replace(",", "")
        unit_ar = _UNIT_AR.get(m.group("unit"), "")
        return f"{sign}{value} {unit_ar}".strip()
    return _NUM_PATTERN.sub(repl, text)


# ─── LRU translation cache ──────────────────────────────────────────────────
@dataclass
class TranslationStats:
    cache_hits: int = 0
    cache_misses: int = 0
    glossary_only: int = 0
    ai_calls: int = 0
    ai_failures: int = 0

    def to_dict(self) -> dict:
        total = self.cache_hits + self.cache_misses
        return {**self.__dict__,
                "hit_rate": round(self.cache_hits / total, 4) if total else 0.0}


class TranslationEngine:
    """Hybrid Arabic translation pipeline."""

    def __init__(
        self,
        *,
        provider: str | None = None,
        cache_size: int | None = None,
    ):
        s = get_settings()
        self.provider_pref = (provider or s.translation_provider).lower()
        self._cache: OrderedDict[str, str] = OrderedDict()
        self._cache_size = cache_size or s.translation_cache_max
        self._stats = TranslationStats()

        # Precompile glossary substitutions for speed
        self._glossary_pairs = sorted(FINANCIAL_GLOSSARY,
                                        key=lambda x: -len(x[0]))

    # ─── public API ────────────────────────────────────────────────────────
    def translate(
        self,
        text: str,
        *,
        mode: str = "financial",
        target: str = "ar",
    ) -> dict[str, Any]:
        """Translate `text` to Arabic. Returns {original, translated, confidence, mode}."""
        if not text or not isinstance(text, str):
            return {"original": text, "translated": text or "",
                    "confidence": 1.0, "mode": mode, "cached": False}

        if target != "ar":
            return {"original": text, "translated": text,
                    "confidence": 1.0, "mode": mode, "cached": False}

        cache_key = self._cache_key(text, mode)
        cached = self._cache_get(cache_key)
        if cached is not None:
            self._stats.cache_hits += 1
            return {"original": text, "translated": cached,
                    "confidence": 0.95, "mode": mode, "cached": True}

        self._stats.cache_misses += 1

        # Stage 1 — glossary substitution
        intermediate, hits = self._apply_glossary(text)

        # Stage 2 — only call AI if there's still untranslated English content
        ai_used = False
        if mode == "ui" or self._is_fully_translated(intermediate):
            translated = intermediate
            self._stats.glossary_only += 1
            confidence = 0.95 if hits > 0 else 0.5
        else:
            ai_translated = self._call_ai(intermediate, mode)
            if ai_translated is None:
                # AI failed — fall back to glossary output
                translated = intermediate
                confidence = 0.7 if hits > 0 else 0.4
            else:
                translated = ai_translated
                ai_used = True
                confidence = 0.92

        # Stage 3 — number normalization
        translated = _normalize_numbers_ar(translated)

        # Cache the final result
        self._cache_put(cache_key, translated)

        return {
            "original": text,
            "translated": translated,
            "confidence": round(confidence, 3),
            "mode": mode,
            "cached": False,
            "ai_used": ai_used,
            "glossary_hits": hits,
        }

    def translate_batch(
        self,
        texts: list[str],
        *,
        mode: str = "financial",
    ) -> list[dict[str, Any]]:
        """Translate many strings at once (sequential — most callers want order)."""
        return [self.translate(t, mode=mode) for t in texts]

    def stats(self) -> dict:
        return self._stats.to_dict()

    def clear_cache(self) -> None:
        self._cache.clear()

    # ─── pipeline stages ───────────────────────────────────────────────────
    def _apply_glossary(self, text: str) -> tuple[str, int]:
        """Word-boundary-safe substitution of every glossary pair."""
        out = text
        hits = 0
        for en, ar in self._glossary_pairs:
            pattern = re.compile(rf"\b{re.escape(en)}\b", re.IGNORECASE)
            new_out, n = pattern.subn(ar, out)
            if n:
                out = new_out
                hits += n
        return out, hits

    @staticmethod
    def _is_fully_translated(text: str) -> bool:
        """Heuristic — treat as fully translated when ≥60% chars are Arabic."""
        if not text:
            return True
        arabic = sum(1 for c in text if "؀" <= c <= "ۿ")
        latin  = sum(1 for c in text if "a" <= c.lower() <= "z")
        if arabic + latin == 0:
            return True
        return arabic / (arabic + latin) >= 0.6

    def _call_ai(self, text: str, mode: str) -> str | None:
        """Call the configured AI provider. Returns None on any failure."""
        s = get_settings()
        prefer = self.provider_pref
        if prefer in ("auto", "anthropic") and s.anthropic_key:
            try:
                self._stats.ai_calls += 1
                return self._call_anthropic(text, mode, s.anthropic_key)
            except Exception as exc:
                logger.warning("Anthropic translation failed: %s", exc)
                self._stats.ai_failures += 1
        if prefer in ("auto", "openai") and s.openai_key:
            try:
                self._stats.ai_calls += 1
                return self._call_openai(text, mode, s.openai_key)
            except Exception as exc:
                logger.warning("OpenAI translation failed: %s", exc)
                self._stats.ai_failures += 1
        return None

    def _system_prompt(self, mode: str) -> str:
        prefix = (
            "You are a financial-grade Arabic translator. Translate the user's "
            "text into Modern Standard Arabic (MSA). Preserve numbers, "
            "currencies, ticker symbols (e.g. $AAPL, AAPL.US, 2222.SR), "
            "URLs, and units verbatim. Use natural, professional phrasing — "
            "not literal/robotic translation."
        )
        if mode == "ui":
            return prefix + " Keep translations short and consistent — these strings appear in user interface buttons, labels, and tooltips."
        if mode == "report":
            return prefix + " Maintain a formal report tone suitable for institutional investors."
        return prefix + " Use precise financial terminology."

    def _call_anthropic(self, text: str, mode: str, api_key: str) -> str:
        try:
            import anthropic  # type: ignore
        except ImportError as exc:
            raise TranslationError("anthropic SDK not installed") from exc
        client = anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model=os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-latest"),
            max_tokens=1024,
            system=self._system_prompt(mode),
            messages=[{"role": "user",
                        "content": f"Translate to Arabic:\n\n{text}"}],
        )
        # Extract text from the first text block
        for block in msg.content:
            if hasattr(block, "text"):
                return block.text.strip()
        raise TranslationError("Anthropic returned no text block")

    def _call_openai(self, text: str, mode: str, api_key: str) -> str:
        try:
            from openai import OpenAI  # type: ignore
        except ImportError as exc:
            raise TranslationError("openai SDK not installed") from exc
        client = OpenAI(api_key=api_key)
        resp = client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
            messages=[
                {"role": "system", "content": self._system_prompt(mode)},
                {"role": "user", "content": f"Translate to Arabic:\n\n{text}"},
            ],
            temperature=0.1,
            max_tokens=1024,
        )
        return (resp.choices[0].message.content or "").strip()

    # ─── cache plumbing ────────────────────────────────────────────────────
    @staticmethod
    def _cache_key(text: str, mode: str) -> str:
        h = hashlib.blake2b(text.encode("utf-8"), digest_size=12).hexdigest()
        return f"{mode}:{h}"

    def _cache_get(self, key: str) -> str | None:
        if key in self._cache:
            self._cache.move_to_end(key)
            return self._cache[key]
        return None

    def _cache_put(self, key: str, value: str) -> None:
        self._cache[key] = value
        self._cache.move_to_end(key)
        while len(self._cache) > self._cache_size:
            self._cache.popitem(last=False)


__all__ = ["TranslationEngine", "FINANCIAL_GLOSSARY", "TranslationStats"]
