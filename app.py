"""Weather Edge — Streamlit dashboard."""
from __future__ import annotations

import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).parent / "src"))

st.set_page_config(
    page_title="Weather Edge",
    page_icon="⛅",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Global CSS ───────────────────────────────────────────────────────────────

st.markdown("""
<style>
/* Hide default streamlit decoration */
#MainMenu, footer, header {visibility: hidden;}

/* Pick card */
.pick-card {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 10px;
    padding: 16px 20px;
    margin-bottom: 12px;
}
.pick-card.yes  { border-left: 4px solid #00d4aa; }
.pick-card.no   { border-left: 4px solid #ff6b6b; }

.pick-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; }
.pick-bracket { font-size: 1.15rem; font-weight: 700; color: #e6edf3; }
.pick-side-yes { background: #00d4aa22; color: #00d4aa; border-radius: 4px; padding: 2px 10px; font-weight: 700; font-size: 0.85rem; }
.pick-side-no  { background: #ff6b6b22; color: #ff6b6b; border-radius: 4px; padding: 2px 10px; font-weight: 700; font-size: 0.85rem; }

.pick-stats { display: flex; gap: 24px; flex-wrap: wrap; }
.stat { display: flex; flex-direction: column; }
.stat-label { font-size: 0.7rem; color: #8b949e; text-transform: uppercase; letter-spacing: 0.05em; }
.stat-value { font-size: 1rem; font-weight: 600; color: #e6edf3; }
.stat-value.edge-pos { color: #00d4aa; }
.stat-value.edge-neg { color: #ff6b6b; }

/* Section headers */
.section-header {
    font-size: 0.75rem;
    font-weight: 600;
    color: #8b949e;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin: 20px 0 10px 0;
    border-bottom: 1px solid #30363d;
    padding-bottom: 6px;
}

/* Status badge */
.badge { border-radius: 4px; padding: 2px 8px; font-size: 0.75rem; font-weight: 600; }
.badge-qrf   { background: #1f6feb33; color: #58a6ff; }
.badge-bma   { background: #3fb95033; color: #3fb950; }
.badge-emos  { background: #d2992533; color: #e3b341; }

/* Override metric label color */
[data-testid="stMetricLabel"] { color: #8b949e !important; font-size: 0.75rem !important; }
[data-testid="stMetricValue"] { font-size: 1.4rem !important; }
</style>
""", unsafe_allow_html=True)

from weather_edge.store import parquet as store

STATIONS = ["EGLC", "EGLL", "EHAM", "EDDF", "LFPB", "KJFK", "KLAX", "KORD", "KMIA"]

# ─── Sidebar ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## ⛅ Weather Edge")
    st.caption("Ensemble forecast → Polymarket edge")
    st.divider()

    station = st.selectbox("Station", STATIONS, label_visibility="collapsed",
                           format_func=lambda s: f"📍 {s}")
    today = date.today()

    st.divider()
    auto_refresh = st.toggle("Auto-refresh (60s)", value=False)
    if auto_refresh:
        import time
        st.caption(f"Last refresh: {datetime.now().strftime('%H:%M:%S')}")
        time.sleep(60)
        st.rerun()

    st.divider()
    st.caption(f"Today: **{today}**")


# ─── Page header ──────────────────────────────────────────────────────────────

col_h1, col_h2 = st.columns([3, 1])
with col_h1:
    st.markdown(f"# {station}")
with col_h2:
    st.markdown("<br>", unsafe_allow_html=True)

# ─── Tabs ─────────────────────────────────────────────────────────────────────

tab_today, tab_history, tab_data, tab_model, tab_calibration = st.tabs(
    ["🎯 Today", "📈 History", "🗄 Data", "🔬 Model", "📐 Calibration"]
)


# ══════════════════════════════════════════════════════════════════════════════
# TODAY TAB
# ══════════════════════════════════════════════════════════════════════════════

with tab_today:
    target_date = st.date_input(
        "Target date", value=today + timedelta(days=1),
        label_visibility="collapsed",
    )

    picks_data = store.read_picks(station, target_date)

    if picks_data is None:
        st.markdown("""
        <div style="background:#161b22;border:1px dashed #30363d;border-radius:10px;
                    padding:32px;text-align:center;color:#8b949e;margin:20px 0;">
            <div style="font-size:2rem;margin-bottom:8px;">🔒</div>
            <div style="font-weight:600;">No lock found</div>
            <div style="font-size:0.85rem;margin-top:4px;">Run <code>we lock --station {station} --date {date}</code></div>
        </div>
        """.replace("{station}", station).replace("{date}", str(target_date)),
        unsafe_allow_html=True)
    else:
        import plotly.graph_objects as go
        import numpy as np
        from scipy.stats import norm as _norm

        mu = picks_data.get("mu", 0.0)
        sigma = picks_data.get("sigma", 1.0)
        mode = picks_data.get("provenance", {}).get("mode", "pooled")
        locked_at_raw = picks_data.get("locked_at", "")
        ensemble_size = picks_data.get("provenance", {}).get("ensemble_size", "?")
        picks = picks_data.get("picks", [])

        # Mode badge
        badge_class = {"qrf": "badge-qrf", "bma": "badge-bma"}.get(mode, "badge-emos")
        locked_str = locked_at_raw[:16].replace("T", " ") if locked_at_raw else "?"

        st.markdown(
            f'<div style="display:flex;align-items:center;gap:12px;margin-bottom:16px;">'
            f'<span style="color:#8b949e;font-size:0.85rem;">Locked {locked_str}z</span>'
            f'<span class="badge {badge_class}">{mode.upper()}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

        # Metrics row
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Forecast μ", f"{mu:.2f}°C")
        c2.metric("Forecast σ", f"{sigma:.2f}°C")
        c3.metric("Ensemble size", str(ensemble_size))
        c4.metric("Picks found", str(len(picks)))

        # Pick cards
        if picks:
            st.markdown('<div class="section-header">Locked Picks</div>', unsafe_allow_html=True)
            for p in picks:
                side = p["side"]
                edge = p["edge"] * 100
                side_class = "yes" if side == "YES" else "no"
                side_badge = f'<span class="pick-side-yes">YES</span>' if side == "YES" else f'<span class="pick-side-no">NO</span>'
                edge_class = "edge-pos" if edge > 0 else "edge-neg"
                label = p["bracket_label"].replace("°", "°")

                st.markdown(f"""
                <div class="pick-card {side_class}">
                  <div class="pick-header">
                    <span class="pick-bracket">{label}</span>
                    {side_badge}
                  </div>
                  <div class="pick-stats">
                    <div class="stat"><span class="stat-label">Model</span><span class="stat-value">{p['model_prob']*100:.1f}%</span></div>
                    <div class="stat"><span class="stat-label">Market</span><span class="stat-value">{p['market_prob']*100:.1f}%</span></div>
                    <div class="stat"><span class="stat-label">Edge</span><span class="stat-value {edge_class}">{edge:+.1f}pp</span></div>
                    <div class="stat"><span class="stat-label">Kelly</span><span class="stat-value">{p.get('kelly_fraction', 0)*100:.1f}%</span></div>
                    <div class="stat"><span class="stat-label">Spread</span><span class="stat-value">{p['spread']:.3f}</span></div>
                    <div class="stat"><span class="stat-label">Liquidity</span><span class="stat-value">${p['liquidity']:,.0f}</span></div>
                  </div>
                </div>
                """, unsafe_allow_html=True)
        else:
            reason = picks_data.get("no_edge_reason") or "no qualifying brackets"
            st.markdown(f"""
            <div style="background:#161b22;border:1px solid #30363d;border-radius:8px;
                        padding:16px 20px;color:#8b949e;font-size:0.9rem;">
                ⚠️  No edge — {reason}
            </div>
            """, unsafe_allow_html=True)

        # Probability chart
        snap_dir = Path("data/market_snapshots") / f"station={station}" / f"date={target_date}"
        snap_data = None
        if snap_dir.exists():
            snaps = sorted(snap_dir.glob("*.json"))
            if snaps:
                with open(snaps[-1]) as f:
                    snap_data = json.load(f)

        if snap_data:
            st.markdown('<div class="section-header">Probability Distribution</div>', unsafe_allow_html=True)
            outcomes = snap_data.get("outcomes", [])
            labels = [o["label"] for o in outcomes]
            market_probs = [o["mid"] for o in outcomes]

            model_prob_map: dict[str, float] = {}
            pred_path = Path("data/predictions") / f"station={station}" / f"date={target_date}" / "prediction.json"
            if pred_path.exists():
                with open(pred_path) as f:
                    pred = json.load(f)
                _mu = pred.get("mu", mu)
                _sigma = pred.get("sigma", sigma)
                for o in outcomes:
                    low = o.get("low")
                    high = o.get("high")
                    p_lo = 0.0 if low is None else float(_norm.cdf(low, _mu, _sigma))
                    p_hi = 1.0 if high is None else float(_norm.cdf(high, _mu, _sigma))
                    model_prob_map[o["label"]] = round(p_hi - p_lo, 4)

            pick_labels = {p["bracket_label"]: p["side"] for p in picks}
            model_probs = [model_prob_map.get(l, 0) for l in labels]

            bar_colors_model = [
                "#00d4aa" if (pick_labels.get(l) == "YES") else
                "#ff6b6b" if (pick_labels.get(l) == "NO") else
                "#58a6ff"
                for l in labels
            ]

            fig = go.Figure()
            fig.add_bar(
                name="Model", x=labels, y=[p * 100 for p in model_probs],
                marker_color=bar_colors_model, opacity=0.9,
            )
            fig.add_bar(
                name="Market", x=labels, y=[p * 100 for p in market_probs],
                marker_color="#e3b341", opacity=0.6,
            )

            # Gaussian overlay
            xs = np.linspace(mu - 4 * sigma, mu + 4 * sigma, 300)
            ys = _norm.pdf(xs, mu, sigma)
            max_prob = max(model_probs) if model_probs else 0.1
            ys_scaled = ys / ys.max() * max_prob * 100
            fig.add_scatter(
                x=xs.tolist(), y=ys_scaled.tolist(),
                mode="lines", name="EMOS PDF",
                line=dict(color="#00d4aa", width=2, dash="dot"),
            )

            fig.update_layout(
                barmode="group",
                plot_bgcolor="#0d1117",
                paper_bgcolor="#0d1117",
                font=dict(color="#e6edf3"),
                xaxis=dict(gridcolor="#21262d", title="Bracket"),
                yaxis=dict(gridcolor="#21262d", title="Probability (%)"),
                legend=dict(orientation="h", bgcolor="rgba(0,0,0,0)"),
                height=380,
                margin=dict(l=0, r=0, t=10, b=0),
            )
            st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# HISTORY TAB
# ══════════════════════════════════════════════════════════════════════════════

with tab_history:
    resolutions = store.read_all_resolutions(station)
    resolved = [r for r in resolutions if r.get("resolved")]

    if not resolved:
        st.markdown("""
        <div style="background:#161b22;border:1px dashed #30363d;border-radius:10px;
                    padding:32px;text-align:center;color:#8b949e;margin:20px 0;">
            <div style="font-size:2rem;margin-bottom:8px;">📭</div>
            <div style="font-weight:600;">No resolved picks yet</div>
            <div style="font-size:0.85rem;margin-top:4px;">Run <code>we resolve --station {station} --all</code> after markets close</div>
        </div>
        """.replace("{station}", station), unsafe_allow_html=True)
    else:
        import plotly.graph_objects as go

        total_pnl = sum(r.get("total_pnl_per_unit", 0) for r in resolved)
        wins = sum(1 for r in resolved if r.get("total_pnl_per_unit", 0) > 0)
        win_rate = wins / len(resolved) * 100 if resolved else 0

        c1, c2, c3 = st.columns(3)
        pnl_color = "#00d4aa" if total_pnl >= 0 else "#ff6b6b"
        c1.metric("Total P&L", f"{total_pnl:+.4f}", delta_color="off")
        c2.metric("Win rate", f"{win_rate:.0f}%")
        c3.metric("Resolved dates", str(len(resolved)))

        dates_r = [r["date"] for r in resolved]
        daily_pnl = [r.get("total_pnl_per_unit", 0) for r in resolved]
        cumulative = []
        running = 0.0
        for p in daily_pnl:
            running += p
            cumulative.append(running)

        fig = go.Figure()
        fig.add_bar(
            x=dates_r, y=daily_pnl, name="Daily P&L",
            marker_color=["#00d4aa" if p >= 0 else "#ff6b6b" for p in daily_pnl],
            opacity=0.85,
        )
        fig.add_scatter(
            x=dates_r, y=cumulative, name="Cumulative",
            mode="lines+markers",
            line=dict(color="#e3b341", width=2),
            marker=dict(size=6),
        )
        fig.update_layout(
            plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
            font=dict(color="#e6edf3"),
            xaxis=dict(gridcolor="#21262d"),
            yaxis=dict(gridcolor="#21262d", title="P&L per unit"),
            legend=dict(orientation="h", bgcolor="rgba(0,0,0,0)"),
            height=320, margin=dict(l=0, r=0, t=10, b=0),
        )
        st.plotly_chart(fig, use_container_width=True)

        st.markdown('<div class="section-header">Pick History</div>', unsafe_allow_html=True)
        rows = []
        for r in resolved:
            for p in r.get("picks_pnl", []):
                rows.append({
                    "Date": r["date"],
                    "Resolved as": r["resolved_label"],
                    "Bracket": p["bracket_label"],
                    "Side": p["side"],
                    "Entry": f"{p['entry_price']:.3f}",
                    "Correct": "✓" if p["correct"] else "✗",
                    "P&L": f"{p['pnl_per_unit']:+.4f}",
                })
        if rows:
            st.dataframe(rows, use_container_width=True, hide_index=True)

    st.markdown('<div class="section-header">All Locked Picks</div>', unsafe_allow_html=True)
    all_picks = store.read_all_picks(station)
    if all_picks:
        rows = []
        for pk in all_picks:
            for p in pk.get("picks", []):
                res = store.read_resolution(station, date.fromisoformat(pk["date"]))
                outcome = (res or {}).get("resolved_label", "—")
                rows.append({
                    "Date": pk["date"],
                    "Bracket": p["bracket_label"],
                    "Side": p["side"],
                    "Model %": f"{p['model_prob']*100:.1f}",
                    "Market %": f"{p['market_prob']*100:.1f}",
                    "Edge": f"{p['edge']*100:+.1f}pp",
                    "Kelly": f"{p.get('kelly_fraction',0)*100:.1f}%",
                    "Resolved": outcome,
                    "Mode": pk.get("provenance", {}).get("mode", "?"),
                })
        st.dataframe(rows, use_container_width=True, hide_index=True)
    else:
        st.info("No picks locked yet.")


# ══════════════════════════════════════════════════════════════════════════════
# DATA TAB
# ══════════════════════════════════════════════════════════════════════════════

with tab_data:
    import plotly.graph_objects as go

    obs = store.read_observations(station, date(2025, 1, 1), today)

    st.markdown('<div class="section-header">Observations</div>', unsafe_allow_html=True)
    if obs.is_empty():
        st.warning("No observations loaded.")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Days of observations", len(obs))
        c2.metric("First date", str(obs["date"].min()))
        c3.metric("Last date", str(obs["date"].max()))

        fig = go.Figure(go.Scatter(
            x=obs["date"].to_list(),
            y=obs["daily_max_c"].to_list(),
            mode="lines",
            fill="tozeroy",
            fillcolor="rgba(0,212,170,0.1)",
            line=dict(color="#00d4aa", width=1.5),
            name="Daily max °C",
        ))
        fig.update_layout(
            plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
            font=dict(color="#e6edf3"),
            xaxis=dict(gridcolor="#21262d"),
            yaxis=dict(gridcolor="#21262d", title="Temp (°C)"),
            height=260, margin=dict(l=0, r=0, t=10, b=0),
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown('<div class="section-header">Forecast Cache</div>', unsafe_allow_html=True)
    fc_base = Path("data/forecasts")
    if fc_base.exists():
        gefs_path = fc_base / "model=gefs"
        ecmwf_path = fc_base / "model=ecmwf"
        gefs_dates = sorted([p.name.replace("init_date=", "") for p in gefs_path.glob("init_date=*")]) if gefs_path.exists() else []
        ecmwf_dates = sorted([p.name.replace("init_date=", "") for p in ecmwf_path.glob("init_date=*")]) if ecmwf_path.exists() else []

        c1, c2 = st.columns(2)
        c1.metric("GEFS init dates", len(gefs_dates),
                  delta=f"{gefs_dates[0]} → {gefs_dates[-1]}" if gefs_dates else None,
                  delta_color="off")
        c2.metric("ECMWF init dates", len(ecmwf_dates),
                  delta=f"{ecmwf_dates[0]} → {ecmwf_dates[-1]}" if ecmwf_dates else None,
                  delta_color="off")


# ══════════════════════════════════════════════════════════════════════════════
# MODEL TAB
# ══════════════════════════════════════════════════════════════════════════════

with tab_model:
    import plotly.graph_objects as go

    st.markdown('<div class="section-header">EMOS Parameters</div>', unsafe_allow_html=True)
    emos_base = Path("data/emos_params") / f"station={station}"

    if not emos_base.exists():
        st.info("No EMOS params found.")
    else:
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

            latest = params_list[-1]
            with st.expander(f"Lead = {lead}h  ·  n={latest.get('n_samples','?')}  ·  CRPS={latest.get('train_crps',0):.4f}", expanded=(lead==24)):
                cols = st.columns(4)
                cols[0].metric("a (bias)", f"{latest['a']:.4f}")
                cols[1].metric("b (ens scale)", f"{latest['b']:.4f}")
                cols[2].metric("c (base var)", f"{latest['c']:.4f}")
                cols[3].metric("d (ens var)", f"{latest['d']:.4f}")
                st.caption(f"Fitted {latest.get('ts','?')} · window={latest.get('training_window_days','?')}d")

    st.markdown('<div class="section-header">QRF</div>', unsafe_allow_html=True)
    qrf_base = Path("data/qrf_params") / f"station={station}" / "lead_hours=24"
    if qrf_base.exists():
        qrf_files = sorted(qrf_base.glob("*.json"))
        if qrf_files:
            with open(qrf_files[-1]) as f:
                qrf_meta = json.load(f)
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Training samples", qrf_meta.get("n_samples", "?"))
            c2.metric("Trees", qrf_meta.get("n_estimators", "?"))
            c3.metric("Min leaf", qrf_meta.get("min_samples_leaf", "?"))
            c4.metric("Fitted", str(qrf_files[-1].stem)[:10])
    else:
        st.info("No QRF params found. Run `we fit-qrf --station " + station + "`")


# ══════════════════════════════════════════════════════════════════════════════
# CALIBRATION TAB
# ══════════════════════════════════════════════════════════════════════════════

with tab_calibration:
    import plotly.graph_objects as go
    import numpy as np

    resolutions = store.read_all_resolutions(station)
    all_picks = store.read_all_picks(station)
    res_map = {r["date"]: r for r in resolutions if r.get("resolved")}

    obs_df = store.read_observations(station, date(2025, 1, 1), today)
    obs_by_date: dict[str, float] = {}
    if not obs_df.is_empty():
        for row in obs_df.iter_rows(named=True):
            obs_by_date[str(row["date"])] = row["daily_max_c"]

    calibration_rows = []
    pit_values = []

    for pk in all_picks:
        d = pk["date"]
        if d not in res_map:
            continue
        res = res_map[d]
        resolved_label = res.get("resolved_label")
        for pick in pk.get("picks", []):
            outcome = 1 if pick["bracket_label"] == resolved_label else 0
            calibration_rows.append({"model_prob": pick["model_prob"], "outcome": outcome})

        obs_temp = obs_by_date.get(d)
        if obs_temp is not None and pk.get("mu") and pk.get("sigma"):
            from scipy.stats import norm as _norm
            pit_values.append(float(_norm.cdf(obs_temp, pk["mu"], pk["sigma"])))

    st.markdown('<div class="section-header">Reliability Diagram</div>', unsafe_allow_html=True)

    if len(calibration_rows) >= 5:
        probs = [r["model_prob"] for r in calibration_rows]
        outcomes = [r["outcome"] for r in calibration_rows]
        bins = np.linspace(0, 1, 11)
        bin_centers = (bins[:-1] + bins[1:]) / 2
        obs_freq = []
        for lo, hi in zip(bins[:-1], bins[1:]):
            mask = [lo <= p < hi for p in probs]
            n = sum(mask)
            obs_freq.append(sum(o for o, m in zip(outcomes, mask) if m) / n if n > 0 else None)

        fig = go.Figure()
        fig.add_scatter(x=[0, 1], y=[0, 1], mode="lines", name="Perfect",
                        line=dict(color="#30363d", dash="dash", width=1))
        fig.add_scatter(x=bin_centers.tolist(), y=obs_freq, mode="markers+lines",
                        name="Observed", marker=dict(size=8, color="#00d4aa"),
                        line=dict(color="#00d4aa", width=2))
        fig.update_layout(
            plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
            font=dict(color="#e6edf3"),
            xaxis=dict(gridcolor="#21262d", range=[0, 1], title="Forecast probability"),
            yaxis=dict(gridcolor="#21262d", range=[0, 1], title="Observed frequency"),
            height=300, margin=dict(l=0, r=0, t=10, b=0),
            legend=dict(bgcolor="rgba(0,0,0,0)"),
        )
        st.plotly_chart(fig, use_container_width=True)
        st.caption(f"{len(calibration_rows)} bracket-outcome pairs")
    else:
        st.info(f"Need ≥5 resolved picks for reliability diagram — have {len(calibration_rows)}.")

    st.markdown('<div class="section-header">PIT Histogram</div>', unsafe_allow_html=True)

    if len(pit_values) >= 5:
        fig = go.Figure(go.Histogram(
            x=pit_values, nbinsx=10,
            marker_color="#58a6ff", opacity=0.8,
        ))
        expected = len(pit_values) / 10
        fig.add_hline(y=expected, line_dash="dash", line_color="#30363d",
                      annotation_text="Uniform", annotation_font_color="#8b949e")
        fig.update_layout(
            plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
            font=dict(color="#e6edf3"),
            xaxis=dict(gridcolor="#21262d", title="PIT value"),
            yaxis=dict(gridcolor="#21262d", title="Count"),
            height=260, margin=dict(l=0, r=0, t=10, b=0),
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)
        st.caption(f"{len(pit_values)} resolved lock dates")
    else:
        st.info(f"Need ≥5 resolved dates for PIT histogram — have {len(pit_values)}.")
