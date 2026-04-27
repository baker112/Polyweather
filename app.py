"""Weather Edge — Streamlit dashboard.

Run: streamlit run app.py
"""
from __future__ import annotations

import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).parent / "src"))

st.set_page_config(page_title="Weather Edge", layout="wide")
st.title("Weather Edge")

from weather_edge.store import parquet as store

STATIONS = ["EGLC"]

# ─── Sidebar ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Settings")
    station = st.selectbox("Station", STATIONS)
    today = date.today()


# ─── Tabs ─────────────────────────────────────────────────────────────────────

tab_today, tab_history, tab_data, tab_model = st.tabs(["Today", "History", "Data", "Model"])


# ══════════════════════════════════════════════════════════════════════════════
# TODAY TAB
# ══════════════════════════════════════════════════════════════════════════════

with tab_today:
    target_date = st.date_input("Target date", value=today + timedelta(days=1))

    picks_data = store.read_picks(station, target_date)

    if picks_data is None:
        st.warning(f"No lock found for {station} on {target_date}. Run `we lock` first.")
    else:
        import plotly.graph_objects as go

        mu = picks_data.get("mu", 0)
        sigma = picks_data.get("sigma", 1)
        mode = picks_data.get("provenance", {}).get("mode", "?")
        locked_at = picks_data.get("locked_at", "?")
        ensemble_size = picks_data.get("provenance", {}).get("ensemble_size", "?")

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Predicted mu", f"{mu:.2f} C")
        col2.metric("Sigma", f"{sigma:.2f} C")
        col3.metric("Mode", mode)
        col4.metric("Ensemble", str(ensemble_size))

        # Load most recent market snapshot for this date
        snap_dir = Path("data/market_snapshots") / f"station={station}" / f"date={target_date}"
        snap_data = None
        if snap_dir.exists():
            snaps = sorted(snap_dir.glob("*.json"))
            if snaps:
                with open(snaps[-1]) as f:
                    snap_data = json.load(f)

        if snap_data:
            outcomes = snap_data.get("outcomes", [])
            labels = [o["label"] for o in outcomes]
            market_probs = [o["mid"] for o in outcomes]

            # Compute model probs from picks or prediction
            model_prob_map: dict[str, float] = {}
            pred_path = Path("data/predictions") / f"station={station}" / f"date={target_date}" / "prediction.json"
            if pred_path.exists():
                with open(pred_path) as f:
                    pred = json.load(f)
                _mu = pred.get("mu", mu)
                _sigma = pred.get("sigma", sigma)
                from scipy.stats import norm
                for o in outcomes:
                    low = o.get("low")
                    high = o.get("high")
                    p_lo = 0.0 if low is None else float(norm.cdf(low, _mu, _sigma))
                    p_hi = 1.0 if high is None else float(norm.cdf(high, _mu, _sigma))
                    model_prob_map[o["label"]] = round(p_hi - p_lo, 4)

            picks = picks_data.get("picks", [])
            pick_labels = {p["bracket_label"]: p["side"] for p in picks}

            model_probs = [model_prob_map.get(l, 0) for l in labels]
            bar_colors = ["gold" if l in pick_labels else "steelblue" for l in labels]

            fig = go.Figure()
            fig.add_bar(name="Model %", x=labels, y=[p * 100 for p in model_probs],
                        marker_color=bar_colors, opacity=0.85)
            fig.add_bar(name="Market %", x=labels, y=[p * 100 for p in market_probs],
                        marker_color="lightcoral", opacity=0.85)
            fig.update_layout(
                barmode="group",
                title=f"Model vs Market — {station} {target_date}",
                xaxis_title="Bracket",
                yaxis_title="Probability (%)",
                height=400,
                legend=dict(orientation="h"),
            )
            st.plotly_chart(fig, use_container_width=True)

        # Picks table
        picks = picks_data.get("picks", [])
        if picks:
            st.subheader(f"Locked Picks ({len(picks)})")
            import polars as pl
            rows = []
            for p in picks:
                rows.append({
                    "Bracket": p["bracket_label"],
                    "Side": p["side"],
                    "Model %": f"{p['model_prob']*100:.1f}",
                    "Market %": f"{p['market_prob']*100:.1f}",
                    "Edge pp": f"{p['edge']*100:+.1f}",
                    "Spread": f"{p['spread']:.3f}",
                    "Liquidity $": f"{p['liquidity']:.0f}",
                })
            st.dataframe(rows, use_container_width=True)
        else:
            reason = picks_data.get("no_edge_reason") or "no qualifying brackets"
            st.info(f"No picks: {reason}")


# ══════════════════════════════════════════════════════════════════════════════
# HISTORY TAB
# ══════════════════════════════════════════════════════════════════════════════

with tab_history:
    st.subheader("P&L History")

    resolutions = store.read_all_resolutions(station)
    resolved = [r for r in resolutions if r.get("resolved")]

    if not resolved:
        st.info("No resolved picks yet. Run `we resolve` after markets close.")
    else:
        import plotly.graph_objects as go

        dates_r = [r["date"] for r in resolved]
        daily_pnl = [r.get("total_pnl_per_unit", 0) for r in resolved]
        cumulative = []
        running = 0.0
        for p in daily_pnl:
            running += p
            cumulative.append(running)

        fig = go.Figure()
        fig.add_bar(x=dates_r, y=daily_pnl, name="Daily P&L",
                    marker_color=["green" if p >= 0 else "red" for p in daily_pnl])
        fig.add_scatter(x=dates_r, y=cumulative, name="Cumulative", mode="lines+markers",
                        line=dict(color="white", width=2))
        fig.update_layout(title="P&L per unit staked", height=350, barmode="relative")
        st.plotly_chart(fig, use_container_width=True)

        # Per-pick table
        rows = []
        for r in resolved:
            for p in r.get("picks_pnl", []):
                rows.append({
                    "Date": r["date"],
                    "Resolved": r["resolved_label"],
                    "Bracket": p["bracket_label"],
                    "Side": p["side"],
                    "Entry": f"{p['entry_price']:.3f}",
                    "Correct": "Yes" if p["correct"] else "No",
                    "P&L": f"{p['pnl_per_unit']:+.4f}",
                })
        if rows:
            st.dataframe(rows, use_container_width=True)

    st.divider()
    st.subheader("All Picks (including unresolved)")
    all_picks = store.read_all_picks(station)
    if all_picks:
        rows = []
        for pk in all_picks:
            for p in pk.get("picks", []):
                res = store.read_resolution(station, date.fromisoformat(pk["date"]))
                outcome = (res or {}).get("resolved_label", "pending")
                rows.append({
                    "Date": pk["date"],
                    "Bracket": p["bracket_label"],
                    "Side": p["side"],
                    "Model %": f"{p['model_prob']*100:.1f}",
                    "Market %": f"{p['market_prob']*100:.1f}",
                    "Edge pp": f"{p['edge']*100:+.1f}",
                    "Resolved": outcome,
                    "Mode": pk.get("provenance", {}).get("mode", "?"),
                })
        st.dataframe(rows, use_container_width=True)
    else:
        st.info("No picks locked yet.")


# ══════════════════════════════════════════════════════════════════════════════
# DATA TAB
# ══════════════════════════════════════════════════════════════════════════════

with tab_data:
    st.subheader("Observation Coverage")
    obs = store.read_observations(station, date(2026, 1, 1), today)
    if obs.is_empty():
        st.warning("No observations loaded.")
    else:
        import polars as pl
        import plotly.graph_objects as go

        st.metric("Days of obs", len(obs))
        col1, col2 = st.columns(2)
        col1.metric("First date", str(obs["date"].min()))
        col2.metric("Last date", str(obs["date"].max()))

        fig = go.Figure(go.Scatter(
            x=obs["date"].to_list(),
            y=obs["daily_max_c"].to_list(),
            mode="lines+markers",
            name="Daily max C",
            line=dict(color="orange"),
        ))
        fig.update_layout(title="EGLC Daily Max Temperature", height=300,
                          xaxis_title="Date", yaxis_title="Temp (C)")
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Forecast Cache")
    fc_base = Path("data/forecasts")
    if fc_base.exists():
        gefs_dates = sorted([p.name.replace("init_date=", "") for p in (fc_base / "model=gefs").glob("init_date=*")])
        ecmwf_dates = sorted([p.name.replace("init_date=", "") for p in (fc_base / "model=ecmwf").glob("init_date=*")])
        col1, col2 = st.columns(2)
        col1.metric("GEFS init dates cached", len(gefs_dates))
        col2.metric("ECMWF init dates cached", len(ecmwf_dates))
        if gefs_dates:
            st.caption(f"GEFS: {gefs_dates[0]} to {gefs_dates[-1]}")


# ══════════════════════════════════════════════════════════════════════════════
# MODEL TAB
# ══════════════════════════════════════════════════════════════════════════════

with tab_model:
    st.subheader("EMOS Parameters")

    emos_base = Path("data/emos_params") / f"station={station}"
    if not emos_base.exists():
        st.info("No EMOS params found.")
    else:
        import plotly.graph_objects as go

        for lead in (24, 48, 72):
            sub = emos_base / f"lead_hours={lead}" / "pooled"
            if not sub.exists():
                continue
            params_list = []
            for p in sorted(sub.glob("*.json")):
                with open(p) as f:
                    d = json.load(f)
                    d["ts"] = p.stem
                    params_list.append(d)

            if not params_list:
                continue

            st.markdown(f"**Lead = {lead}h** ({len(params_list)} fits)")
            latest = params_list[-1]
            cols = st.columns(5)
            cols[0].metric("a", f"{latest['a']:.4f}")
            cols[1].metric("b", f"{latest['b']:.4f}")
            cols[2].metric("c", f"{latest['c']:.4f}")
            cols[3].metric("d", f"{latest['d']:.4f}")
            cols[4].metric("CRPS", f"{latest['train_crps']:.4f}")

    st.subheader("QRF")
    qrf_base = Path("data/qrf_params") / f"station={station}" / "lead_hours=24"
    if qrf_base.exists():
        qrf_files = sorted(qrf_base.glob("*.json"))
        if qrf_files:
            with open(qrf_files[-1]) as f:
                qrf_meta = json.load(f)
            col1, col2, col3 = st.columns(3)
            col1.metric("QRF samples", qrf_meta.get("n_samples", "?"))
            col2.metric("Trees", qrf_meta.get("n_estimators", "?"))
            col3.metric("Min leaf", qrf_meta.get("min_samples_leaf", "?"))
    else:
        st.info("No QRF params found.")
