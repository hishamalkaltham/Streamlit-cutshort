"""Multi-tab Streamlit workspace built on top of DataEngine.

Why "multi-tab" matters
-----------------------
Streamlit's default model — single script, single state — makes it hard to
maintain N independent charts simultaneously. We solve that by:

  • giving every tab a UUID-style `tab_id`
  • prefixing EVERY widget key with that id (`f"{tab_id}_input"`)
  • storing the active tab in `st.query_params["tab"]` so URLs persist
  • lazy-rendering: only the active tab's body runs each script pass

That alone eliminates StreamlitDuplicateElementId errors and lets the user
keep AAPL, TSLA, and 2222.SR open side-by-side without state collisions.

Run:
    streamlit run -m quant_engine.dashboard

…or import `render()` and embed the workspace inside another app.
"""
from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

try:
    import streamlit as st  # type: ignore
except ImportError as exc:  # pragma: no cover
    raise SystemExit("streamlit is required for the dashboard module") from exc

from .config import get_settings
from .data_engine import DataEngine
from .models import FusionStrategy
from .translation_engine import TranslationEngine

logger = logging.getLogger("quant_engine.dashboard")


# ─── Workspace model ────────────────────────────────────────────────────────
@dataclass
class TabState:
    tab_id: str
    title: str = "New tab"
    symbol: str = "AAPL"
    view: str = "overview"          # overview · fundamentals · diagnostics · health
    strategy: str = "weighted_average"
    created_at: float = field(default_factory=time.time)


def _new_tab_id() -> str:
    return uuid.uuid4().hex[:8]


# ─── Engine + translation singletons (cached so workspace switches are instant)─
@st.cache_resource(show_spinner=False)
def _engine() -> DataEngine:
    return DataEngine()


@st.cache_resource(show_spinner=False)
def _translator() -> TranslationEngine:
    return TranslationEngine()


# ─── Workspace state helpers ────────────────────────────────────────────────
def _ensure_workspace() -> dict:
    """Initialise workspace state (tabs + active tab)."""
    if "qe_tabs" not in st.session_state:
        first = TabState(tab_id=_new_tab_id(), title="AAPL", symbol="AAPL")
        st.session_state["qe_tabs"] = {first.tab_id: first}
        st.session_state["qe_active_tab"] = first.tab_id
    return st.session_state["qe_tabs"]


def _active_tab() -> TabState:
    tabs = _ensure_workspace()
    active_id = st.session_state.get("qe_active_tab")
    if active_id not in tabs:
        active_id = next(iter(tabs))
        st.session_state["qe_active_tab"] = active_id
    return tabs[active_id]


def _add_tab(symbol: str = "AAPL") -> str:
    tabs = _ensure_workspace()
    tab = TabState(tab_id=_new_tab_id(), title=symbol.upper(),
                    symbol=symbol.upper())
    tabs[tab.tab_id] = tab
    st.session_state["qe_active_tab"] = tab.tab_id
    return tab.tab_id


def _close_tab(tab_id: str) -> None:
    tabs = _ensure_workspace()
    if tab_id not in tabs:
        return
    if len(tabs) == 1:
        # Don't allow closing the last tab — reset it instead
        tabs[tab_id] = TabState(tab_id=tab_id, title="AAPL", symbol="AAPL")
        return
    del tabs[tab_id]
    if st.session_state.get("qe_active_tab") == tab_id:
        st.session_state["qe_active_tab"] = next(iter(tabs))


# ─── Translation helper ─────────────────────────────────────────────────────
def _t(text: str) -> str:
    """Translate `text` if Arabic mode is on, else return as-is."""
    if st.session_state.get("qe_lang", "en") != "ar":
        return text
    out = _translator().translate(text, mode="ui")
    return out.get("translated") or text


# ─── UI components ──────────────────────────────────────────────────────────
def _render_sidebar() -> None:
    st.sidebar.title("⚛️ Quant Engine")
    lang = st.sidebar.radio(
        "Language / اللغة",
        ["English", "العربية"],
        index=0 if st.session_state.get("qe_lang", "en") == "en" else 1,
        horizontal=True, key="qe_lang_radio",
    )
    st.session_state["qe_lang"] = "en" if lang == "English" else "ar"

    if st.session_state["qe_lang"] == "ar":
        st.markdown(
            "<style>html, body, .stApp, [data-testid='stSidebar']"
            "{ direction: rtl; }</style>",
            unsafe_allow_html=True,
        )

    with st.sidebar.expander(_t("Workspace controls"), expanded=True):
        new_sym = st.text_input(_t("New tab symbol"), value="",
                                  placeholder="MSFT", key="qe_new_sym_input")
        if st.button(_t("➕ Open in new tab"), key="qe_new_tab_btn",
                       use_container_width=True):
            sym = (new_sym or "AAPL").strip().upper()
            if sym:
                _add_tab(sym)
                st.rerun()
        if st.button(_t("🗑 Close current tab"), key="qe_close_btn",
                       use_container_width=True):
            _close_tab(_active_tab().tab_id)
            st.rerun()

    with st.sidebar.expander(_t("Engine status"), expanded=False):
        eng = _engine()
        diag = eng.get_diagnostics()
        st.metric(_t("Total calls"), diag.get("total_calls", 0))
        st.metric(_t("Success rate"),
                    f"{diag.get('success_rate', 0)*100:.1f}%")
        st.metric(_t("Avg latency"),
                    f"{diag.get('avg_latency_ms', 0):.0f} ms")
        if st.button(_t("🧹 Clear cache"), key="qe_clear_cache",
                       use_container_width=True):
            eng.clear_cache()
            st.success(_t("Cache cleared"))


def _render_tab_bar() -> None:
    """Top tab strip — shows every open tab as a button."""
    tabs = _ensure_workspace()
    cols = st.columns([1] * len(tabs) + [0.5])
    for i, (tid, tab) in enumerate(tabs.items()):
        is_active = tid == st.session_state.get("qe_active_tab")
        label = ("🟢 " if is_active else "⚪ ") + tab.title
        if cols[i].button(label, key=f"tabbar_{tid}",
                            use_container_width=True):
            st.session_state["qe_active_tab"] = tid
            st.rerun()
    if cols[-1].button("➕", key="tabbar_add", use_container_width=True,
                         help=_t("New tab")):
        _add_tab()
        st.rerun()


def _render_view_picker(tab: TabState) -> None:
    views = {
        "overview":     _t("Overview"),
        "fundamentals": _t("Fundamentals"),
        "diagnostics":  _t("Diagnostics"),
        "health":       _t("Provider health"),
    }
    keys = list(views.keys())
    chosen = st.radio(
        _t("View"),
        keys,
        format_func=lambda k: views[k],
        index=keys.index(tab.view) if tab.view in keys else 0,
        horizontal=True,
        key=f"{tab.tab_id}_view_picker",
    )
    tab.view = chosen


def _render_overview(tab: TabState) -> None:
    eng = _engine()
    c1, c2, c3 = st.columns([2, 1, 1])
    sym = c1.text_input(_t("Symbol"), value=tab.symbol,
                          key=f"{tab.tab_id}_symbol_input").strip().upper()
    if sym != tab.symbol:
        tab.symbol = sym
        tab.title = sym
        st.rerun()

    strategy = c2.selectbox(
        _t("Fusion strategy"),
        [s.value for s in FusionStrategy],
        index=list(FusionStrategy).index(FusionStrategy(tab.strategy)),
        key=f"{tab.tab_id}_strategy_input",
    )
    tab.strategy = strategy

    if c3.button(_t("Refresh"), key=f"{tab.tab_id}_refresh_btn",
                   use_container_width=True):
        eng.clear_cache(symbol=tab.symbol)
        st.rerun()

    with st.spinner(_t("Fetching from all providers…")):
        price = eng.get_price(tab.symbol, strategy=FusionStrategy(strategy))

    m1, m2, m3, m4 = st.columns(4)
    m1.metric(_t("Price"),
                f"{price.get('price', '—')} {price.get('currency') or ''}")
    m2.metric(_t("Confidence"), f"{price.get('confidence', 0)*100:.1f}%")
    m3.metric(_t("Primary"),    price.get("primary_provider") or "—")
    m4.metric(_t("Sources"),    len(price.get("contributors") or []))

    st.subheader(_t("Per-provider raw values"))
    field_result = eng.get_field(tab.symbol, "price",
                                   strategy=FusionStrategy(strategy))
    rows = [{"provider": p, "value": v}
            for p, v in (field_result.all_values or {}).items()]
    st.dataframe(rows, use_container_width=True, hide_index=True)

    if field_result.warnings:
        st.warning(", ".join(field_result.warnings))


def _render_fundamentals(tab: TabState) -> None:
    eng = _engine()
    with st.spinner(_t("Loading fundamentals…")):
        funds = eng.get_fundamentals(tab.symbol)
        meta = eng.get_metadata(tab.symbol)

    cols = st.columns(4)
    fields = ["pe_ratio", "eps", "dividend_yield", "market_cap"]
    for col, f in zip(cols, fields):
        result = funds.get(f)
        col.metric(
            _t({"pe_ratio": "P/E ratio", "eps": "EPS",
                "dividend_yield": "Dividend yield",
                "market_cap": "Market cap"}[f]),
            value=f"{result.value if result else '—'}",
            help=f"primary={result.primary_provider}" if result else "",
        )

    st.subheader(_t("Company metadata"))
    rows = []
    for k, r in meta.items():
        rows.append({
            "field": k,
            "value": r.value,
            "primary_provider": r.primary_provider,
            "confidence": f"{r.confidence:.2f}",
        })
    st.dataframe(rows, use_container_width=True, hide_index=True)


def _render_diagnostics(_tab: TabState) -> None:
    eng = _engine()
    diag = eng.get_diagnostics()
    st.subheader(_t("Engine diagnostics"))

    c1, c2, c3, c4 = st.columns(4)
    c1.metric(_t("Total calls"),  diag.get("total_calls", 0))
    c2.metric(_t("Success rate"), f"{diag.get('success_rate', 0)*100:.2f}%")
    c3.metric(_t("Avg latency"),  f"{diag.get('avg_latency_ms', 0):.0f} ms")
    c4.metric(_t("p95 latency"),  f"{diag.get('p95_latency_ms', 0):.0f} ms")

    st.markdown("**" + _t("By provider") + "**")
    rows = []
    for prov, slot in (diag.get("by_provider") or {}).items():
        rows.append({"provider": prov, **slot})
    st.dataframe(rows, use_container_width=True, hide_index=True)

    st.markdown("**" + _t("By error type") + "**")
    err_rows = [{"error_type": k, "count": v}
                for k, v in (diag.get("by_error_type") or {}).items()]
    st.dataframe(err_rows, use_container_width=True, hide_index=True)


def _render_health(_tab: TabState) -> None:
    eng = _engine()
    snapshots = eng.get_provider_health()
    st.subheader(_t("Provider health"))
    for snap in snapshots:
        with st.expander(f"{snap['provider'].upper()} · "
                          f"{snap['status']} · score {snap['score']}",
                          expanded=False):
            st.json(snap)


# ─── Public entry-point ─────────────────────────────────────────────────────
def render() -> None:
    """Main render — call this from any Streamlit script to embed the workspace."""
    st.set_page_config(page_title="Quant Engine Workspace",
                        page_icon="⚛️", layout="wide")
    _render_sidebar()
    _render_tab_bar()
    tab = _active_tab()
    _render_view_picker(tab)
    st.divider()

    if tab.view == "overview":
        _render_overview(tab)
    elif tab.view == "fundamentals":
        _render_fundamentals(tab)
    elif tab.view == "diagnostics":
        _render_diagnostics(tab)
    elif tab.view == "health":
        _render_health(tab)


if __name__ == "__main__":  # pragma: no cover
    render()


__all__ = ["render"]
