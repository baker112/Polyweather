"""Telegram message templates for the three-mode pipeline.

Keep all message formatting here so scheduler.py doesn't grow huge string
soup. Each function takes data-only inputs and returns a string ready to
pass to weather_edge.telegram.send().

Conventions (from THREE_MODE_REFACTOR §3.2):
  Mode icons:    📊 bma   ⚡ intraday   🎯 peak
  Status icons:  🟢 YES   🔴 NO   ⏸ no edge   ❌ failed
  Numbers:       1dp °C, 0dp %, $0.00

Telegram is rendered HTML (parse_mode=HTML); `&` must be `&amp;` and `<` `&lt;`
inside string literals. We use bold via <b>…</b>.
"""
from __future__ import annotations

from datetime import date
from typing import Any, Iterable

MODE_ICON = {"bma": "📊", "intraday": "⚡", "peak": "🎯"}
MODE_LABEL = {"bma": "BMA", "intraday": "Intraday", "peak": "Peak"}
SIDE_ICON = {"YES": "🟢", "NO": "🔴"}
LIVE_ICON = "💼"
DRY_ICON = "📒"


def _amp(s: str) -> str:
    """Escape `&` for Telegram HTML so the parser doesn't mangle 'P&L'."""
    return s.replace("&", "&amp;")


# ─── Lock digest ──────────────────────────────────────────────────────────────


def format_lock_digest(
    mode: str,
    target_date: date,
    station_rows: list[dict[str, Any]],
    bankroll: dict[str, Any] | None = None,
) -> str:
    """Render one digest message for a (mode, target_date) lock window.

    station_rows: each row is a dict with keys:
        sid (str), mu (float|None), picks (list[dict]), no_edge (bool),
        failed (bool), failure (str|None).
    bankroll: optional mode's dry bankroll dict (current_usdc, reserved_usdc).
    """
    icon = MODE_ICON.get(mode, "·")
    label = MODE_LABEL.get(mode, mode)
    lines: list[str] = [
        f"{icon} <b>{label} locks</b> · {target_date.isoformat()}"
    ]

    # Sort stations for stable output
    for row in sorted(station_rows, key=lambda r: r["sid"]):
        sid = row["sid"]
        if row.get("failed"):
            lines.append(f"   {sid}  ❌ {row.get('failure', 'failed')[:60]}")
            continue
        mu = row.get("mu")
        mu_str = f"{mu:.1f}°C" if isinstance(mu, (int, float)) else "—"
        picks = row.get("picks") or []
        if not picks:
            lines.append(f"   {sid}  μ={mu_str}   ⏸ no edge")
            continue
        # One line per pick (usually 1, occasionally 2)
        for p in picks:
            side = p.get("side", "?")
            bracket = p.get("bracket_label", "?")
            stake = p.get("usdc_stake")
            stake_str = f"  ${stake:.2f}" if isinstance(stake, (int, float)) else ""
            lines.append(
                f"   {sid}  μ={mu_str}   {SIDE_ICON.get(side, '·')} {side} {bracket}{stake_str}"
            )

    if bankroll is not None:
        cur = bankroll.get("current_usdc", 0.0)
        res = bankroll.get("reserved_usdc", 0.0)
        lines.append(f"   {DRY_ICON} bankroll: ${cur:.2f} (${res:.2f} reserved)")

    return _amp("\n".join(lines))


# ─── Resolution digest ────────────────────────────────────────────────────────


def format_resolution_digest(
    target_date: date,
    station_blocks: list[dict[str, Any]],
    day_totals: dict[str, float],
) -> str:
    """One Telegram message summarising all (station, mode) resolutions.

    station_blocks: list of dicts with keys:
        sid (str), resolved_label (str|None), modes (list[dict]).
        Each mode dict: {mode, picks: list with side/bracket/pnl/win}.
    day_totals: {mode: float} for the day's P&L totals.
    """
    lines = [f"📋 <b>Resolution</b> · {target_date.isoformat()}"]
    for block in sorted(station_blocks, key=lambda b: b["sid"]):
        sid = block["sid"]
        rlabel = block.get("resolved_label") or "—"
        lines.append(f"   <b>{sid}</b>  resolved {rlabel}")
        for m in block.get("modes", []):
            mode = m.get("mode", "?")
            picks = m.get("picks") or []
            icon = MODE_ICON.get(mode, "·")
            label = MODE_LABEL.get(mode, mode)
            if not picks:
                lines.append(f"     {icon} {label:<9} no bet")
                continue
            for p in picks:
                side = p.get("side", "?")
                bracket = p.get("bracket_label", "?")
                pnl = p.get("pnl", 0.0)
                win_icon = SIDE_ICON.get(side, "·")
                lines.append(
                    f"     {icon} {label:<9} {win_icon} {side} {bracket}    "
                    f"${pnl:+.2f}"
                )

    if day_totals:
        parts = []
        for m in ("bma", "intraday", "peak"):
            if m in day_totals:
                parts.append(f"{MODE_ICON[m]} ${day_totals[m]:+.2f}")
        if parts:
            lines.append("   <b>Day totals:</b>  " + "   ".join(parts))
    return _amp("\n".join(lines))


# ─── Bankroll ─────────────────────────────────────────────────────────────────


def format_bankroll(
    bankrolls_by_mode: dict[str, dict[str, Any]],
    live: dict[str, Any] | None,
) -> str:
    """Multi-line bankroll status: three dry + live."""
    lines = ["💰 <b>Bankrolls</b>"]
    for mode in ("bma", "intraday", "peak"):
        b = bankrolls_by_mode.get(mode)
        if b is None:
            lines.append(f"   {MODE_ICON[mode]} {MODE_LABEL[mode]:<9} —")
            continue
        cur = b.get("current_usdc", 0.0)
        reserved = b.get("reserved_usdc", 0.0)
        pnl = b.get("total_pnl", 0.0)
        trades = b.get("n_trades", 0)
        lines.append(
            f"   {MODE_ICON[mode]} {MODE_LABEL[mode]:<9} ${cur:.2f}  "
            f"(${reserved:.2f} reserved, ${pnl:+.2f} P&L, {trades} trades)"
        )

    if live is None:
        lines.append(f"   {LIVE_ICON} Live      $0.00    (not initialised)")
    else:
        cur = live.get("current_usdc", 0.0)
        reserved = live.get("reserved_usdc", 0.0)
        pnl = live.get("total_pnl", 0.0)
        trades = live.get("n_trades", 0)
        lines.append(
            f"   {LIVE_ICON} Live      ${cur:.2f}  "
            f"(${reserved:.2f} reserved, ${pnl:+.2f} P&L, {trades} trades)"
        )
    return _amp("\n".join(lines))


# ─── P&L ──────────────────────────────────────────────────────────────────────


def format_pnl(days: int, per_mode_stats: dict[str, dict[str, Any]]) -> str:
    """Per-mode trade summary over the last N days."""
    lines = [f"📈 <b>P&L last {days} days</b>"]
    for mode in ("bma", "intraday", "peak"):
        s = per_mode_stats.get(mode) or {}
        trades = s.get("n", 0)
        wins = s.get("wins", 0)
        staked = s.get("staked", 0.0)
        pnl = s.get("pnl", 0.0)
        roi = (pnl / staked * 100) if staked > 0 else 0.0
        icon = MODE_ICON[mode]
        label = MODE_LABEL[mode]
        if trades == 0:
            lines.append(f"   {icon} {label:<9} no resolved trades")
        else:
            lines.append(
                f"   {icon} {label:<9} trades={trades}  win={wins}  "
                f"P&L=${pnl:+.2f}  ROI={roi:+.1f}%"
            )
    return _amp("\n".join(lines))


# ─── Daily summary ────────────────────────────────────────────────────────────


def format_daily_summary(
    today: date,
    yesterday: date,
    tomorrow: date,
    mode_status: str,
    per_mode_yesterday: dict[str, dict[str, Any]],
    bankrolls_by_mode: dict[str, dict[str, Any]],
    bma_tomorrow_picks: list[dict[str, Any]],
) -> str:
    """Combined yesterday-P&L + bankroll + tomorrow-picks summary."""
    lines = [
        f"📊 <b>Polyweather digest</b> · {today.isoformat()}  mode={mode_status}",
        "",
        f"<b>Yesterday ({yesterday.isoformat()})</b>",
    ]
    for mode in ("bma", "intraday", "peak"):
        s = per_mode_yesterday.get(mode) or {}
        resolved = s.get("resolved", 0)
        total = s.get("total", 0)
        pnl = s.get("pnl", 0.0)
        staked = s.get("staked", 0.0)
        roi = (pnl / staked * 100) if staked > 0 else 0.0
        lines.append(
            f"  {MODE_ICON[mode]} {MODE_LABEL[mode]:<9} {resolved}/{total} resolved   "
            f"${pnl:+.2f}   ROI {roi:+.1f}%"
        )

    lines.append("")
    lines.append("<b>Bankrolls</b>")
    parts = []
    for mode in ("bma", "intraday", "peak"):
        b = bankrolls_by_mode.get(mode) or {}
        cur = b.get("current_usdc", 0.0)
        parts.append(f"{MODE_ICON[mode]} ${cur:.2f}")
    lines.append("  " + "   ".join(parts))

    lines.append("")
    lines.append(
        f"<b>Tomorrow ({tomorrow.isoformat()}) BMA picks</b>  "
        f"(intraday/peak fire same-day)"
    )
    if not bma_tomorrow_picks:
        lines.append("  No picks yet (locks fire later today)")
    else:
        for row in sorted(bma_tomorrow_picks, key=lambda r: r["sid"]):
            sid = row["sid"]
            mu = row.get("mu")
            mu_str = f"{mu:.1f}°C" if isinstance(mu, (int, float)) else "—"
            picks = row.get("picks") or []
            if not picks:
                lines.append(f"  {sid}  μ={mu_str}   ⏸ no edge")
                continue
            for p in picks:
                side = p.get("side", "?")
                bracket = p.get("bracket_label", "?")
                lines.append(
                    f"  {sid}  μ={mu_str}   {SIDE_ICON.get(side, '·')} {side} {bracket}"
                )

    return _amp("\n".join(lines))


# ─── /pick drill-down ─────────────────────────────────────────────────────────


def format_pick_detail(
    sid: str,
    mode: str,
    target_date: date,
    locked: dict[str, Any] | None,
) -> str:
    """Verbose single-station single-mode breakdown for `/pick STATION MODE`."""
    icon = MODE_ICON.get(mode, "·")
    label = MODE_LABEL.get(mode, mode)
    if locked is None:
        return _amp(
            f"{icon} <b>{label} {sid}</b> · {target_date.isoformat()}  no picks file"
        )
    mu = locked.get("mu")
    sigma = locked.get("sigma")
    prov = locked.get("provenance") or {}
    init_dt = prov.get("init_dt")
    lead = prov.get("lead_hours") or prov.get("emos_lead") or "?"

    head = f"{icon} <b>{label} {sid}</b> · {target_date.isoformat()}"
    sub = []
    if mu is not None and sigma is not None:
        sub.append(f"μ={mu:.1f}°C  σ={sigma:.2f}")
    if init_dt:
        sub.append(f"init={str(init_dt)[:13]}z")
    sub.append(f"lead≈{lead}h")

    lines = [head, "   " + "  ".join(sub)]
    picks = locked.get("picks") or []
    if not picks:
        reason = locked.get("no_edge_reason") or "all gates failed"
        lines.append(f"   ⏸ no edge — {reason[:160]}")
        return _amp("\n".join(lines))

    for p in picks:
        side = p.get("side", "?")
        bracket = p.get("bracket_label", "?")
        side_icon = SIDE_ICON.get(side, "·")
        model_p = p.get("model_prob", 0.0)
        mkt_p = p.get("market_prob", 0.0)
        edge = p.get("edge", 0.0)
        kelly = p.get("kelly_fraction", 0.0)
        cap = p.get("max_stake_usdc")
        cap_str = f"  cap=${cap:.2f}" if isinstance(cap, (int, float)) else ""
        lines.append(f"   {side_icon} {side} {bracket}")
        lines.append(
            f"     model {model_p:.0%} vs mkt {mkt_p:.0%}  edge {edge:+.1%}"
        )
        lines.append(
            f"     kelly {kelly:.2%} of bankroll{cap_str}"
        )

    return _amp("\n".join(lines))


# ─── Lock-failure single-station message ──────────────────────────────────────


def format_lock_failure(mode: str, sid: str, target_date: date, reason: str) -> str:
    icon = MODE_ICON.get(mode, "·")
    label = MODE_LABEL.get(mode, mode)
    return _amp(
        f"❌ <b>{label} FAILED:</b> {sid} · {target_date.isoformat()}\n"
        f"   {reason[:200]}"
    )
