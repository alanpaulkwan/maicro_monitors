#!/usr/bin/env python3
"""
Daily email: diagnose missing positions for the latest live run.

Starting from the same target/actual alignment as `targets_vs_actuals_daily`:
  - Targets:  maicro_logs.positions_jianan_v6 (earliest per (date, symbol))
  - Actuals:  maicro_logs.live_positions      (kind='current', last per (target_date, symbol))

This script focuses ONLY on symbols where:
  - target exists, but
  - actual position weight is effectively zero (i.e., we are "MISSING" the position).

For each such symbol it:
  - estimates target notional using an equity proxy from live positions,
  - looks up Hyperliquid metadata (min_usd, etc.) from maicro_logs.hl_meta,
  - pulls recent orders from maicro_monitors.orders around the latest run timestamp,
  - classifies a coarse reason for the missing position:
      * no_meta            → symbol missing from hl_meta
      * below_min_usd      → target notional below min_usd
      * reduce_only_only   → only reduceOnly orders were placed
      * orders_canceled    → all recent orders canceled
      * filled_but_no_pos  → filled orders seen but no live position
      * open_order_no_pos  → open orders but no live position
      * no_order_unknown   → no recent orders and not obviously below_min or no_meta
  - includes a short recent order summary (side, sz, px, status, orderType, reduceOnly).

Environment:
  - RESEND_API_KEY                    (required to send email)
  - ALERT_EMAIL                       (recipient, defaults to alanpaulkwan@gmail.com)
  - ALERT_FROM_EMAIL                  (optional, default 'Maicro Monitors <alerts@resend.dev>')
  - MISSING_POS_ORDERS_LOOKBACK_HOURS (optional, default '36')
"""

import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import requests

# Make repo modules importable
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if REPO_ROOT not in sys.path:
    sys.path.append(REPO_ROOT)

from modules.clickhouse_client import query_df  # type: ignore  # noqa: E402
from scheduled_processes.emails.daily.targets_vs_actuals_daily import (  # type: ignore  # noqa: E402
    _load_latest_run_context,
    _load_targets_for_date,
    _load_actuals_for_date,
    build_comparison,
)


RESEND_API_KEY = os.getenv("RESEND_API_KEY")
TO_EMAIL = os.getenv("ALERT_EMAIL", "alanpaulkwan@gmail.com")
FROM_EMAIL = os.getenv("ALERT_FROM_EMAIL", "Maicro Monitors <alerts@resend.dev>")
ORDERS_LOOKBACK_HOURS = int(os.getenv("MISSING_POS_ORDERS_LOOKBACK_HOURS", "36"))


@dataclass
class MissingPositionDiagnosis:
    symbol: str
    target_weight_pct: float
    est_notional_usd: float
    last_order_px: Optional[float]
    bn_spot_symbol: Optional[str]
    bn_spot_px: Optional[float]  # latest 1h close
    px_ratio_order_over_bn: Optional[float]  # vs latest
    bn_spot_px_at_order: Optional[float]
    px_ratio_order_over_bn_at_order: Optional[float]
    reason: str
    reason_detail: str
    orders_count: int
    last_order_ts: Optional[datetime]
    last_order_side: Optional[str]
    last_order_sz: Optional[float]
    last_order_status: Optional[str]
    last_order_type: Optional[str]
    last_order_reduce_only: Optional[bool]


def _estimate_equity_usd(actuals: pd.DataFrame) -> float:
    """
    Use median equity_usd from live_positions for the current run as a rough equity proxy.
    Fallback to 50k if unavailable.
    """
    if actuals.empty or "equity_usd" not in actuals.columns:
        return 50_000.0
    valid = actuals["equity_usd"].astype(float)
    valid = valid[valid > 0]
    if valid.empty:
        return 50_000.0
    return float(valid.median())


def _to_binance_symbol(symbol: str) -> Optional[str]:
    """
    Map an HL symbol (e.g. BTC, ETH) to a Binance spot symbol.
    For now we assume coins are quoted vs USDT on spot: BTC → BTCUSDT, etc.
    """
    sym = symbol.upper()
    if not sym:
        return None
    # Some special cases can be added here if needed later.
    return f"{sym}USDT"


def _load_latest_binance_prices(symbols: List[str]) -> Dict[str, float]:
    """
    Load the latest Binance spot close price for each mapped symbol from binance.bn_spot_klines.

    Returns a dict keyed by HL symbol (e.g. BTC) with the latest close price from
    the corresponding Binance symbol (e.g. BTCUSDT) using the most recent timestamp.
    """
    if not symbols:
        return {}

    mapping: Dict[str, str] = {}
    for s in symbols:
        bn_sym = _to_binance_symbol(s)
        if bn_sym:
            mapping[s] = bn_sym

    if not mapping:
        return {}

    bn_symbols = ", ".join(f"'{v}'" for v in sorted(set(mapping.values())))
    sql = f"""
        SELECT symbol, close
        FROM (
            SELECT
                symbol,
                close,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) AS rn
            FROM binance.bn_spot_klines
            WHERE symbol IN ({bn_symbols})
              AND interval = '1h'
        )
        WHERE rn = 1
    """
    df = query_df(sql)
    if df.empty:
        return {}

    df["symbol"] = df["symbol"].astype(str)
    df["close"] = df["close"].astype(float)
    latest_map = {row["symbol"]: row["close"] for _, row in df.iterrows()}

    hl_to_px: Dict[str, float] = {}
    for hl_sym, bn_sym in mapping.items():
        px = latest_map.get(bn_sym)
        if px is not None:
            hl_to_px[hl_sym] = float(px)
    return hl_to_px


def _load_binance_price_at_time(symbol: str, ts: datetime) -> Optional[float]:
    """
    Load the Binance spot close price (1h) for a given HL symbol at or before a timestamp.
    """
    bn_sym = _to_binance_symbol(symbol)
    if not bn_sym:
        return None
    sql = """
        SELECT close
        FROM binance.bn_spot_klines
        WHERE symbol = %(sym)s
          AND interval = '1h'
          AND timestamp <= %(ts)s
        ORDER BY timestamp DESC
        LIMIT 1
    """
    df = query_df(sql, params={"sym": bn_sym, "ts": ts})
    if df.empty:
        return None
    return float(df.iloc[0]["close"])


def _load_meta_for_symbols(symbols: List[str]) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame(columns=["symbol", "min_usd", "min_units", "size_step", "tick_size"])
    sym_list = ", ".join(f"'{s}'" for s in symbols)
    sql = f"""
        SELECT symbol, min_usd, min_units, size_step, tick_size
        FROM maicro_logs.hl_meta
        WHERE symbol IN ({sym_list})
    """
    df = query_df(sql)
    if df.empty:
        return pd.DataFrame(columns=["symbol", "min_usd", "min_units", "size_step", "tick_size"])
    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    df = df.drop_duplicates(subset=["symbol"], keep="first")
    return df.set_index("symbol")


def _load_recent_orders(symbols: List[str], run_ts: datetime) -> pd.DataFrame:
    """
    Load recent orders for the given symbols from maicro_monitors.orders
    within [run_ts - ORDERS_LOOKBACK_HOURS, run_ts].
    """
    if not symbols:
        return pd.DataFrame(
            columns=["symbol", "side", "limitPx", "sz", "oid", "timestamp", "status", "orderType", "reduceOnly"]
        )
    sym_list = ", ".join(f"'{s}'" for s in symbols)
    start_ts = run_ts - timedelta(hours=ORDERS_LOOKBACK_HOURS)
    sql = f"""
        SELECT
            coin AS symbol,
            side,
            limitPx,
            sz,
            oid,
            timestamp,
            status,
            orderType,
            reduceOnly
        FROM maicro_monitors.orders
        WHERE coin IN ({sym_list})
          AND timestamp >= %(start_ts)s
          AND timestamp <= %(end_ts)s
        ORDER BY timestamp DESC
    """
    df = query_df(sql, params={"start_ts": start_ts, "end_ts": run_ts})
    if df.empty:
        return df
    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def _classify_missing_position_row(
    row: pd.Series,
    meta_row: Optional[pd.Series],
    symbol_orders: pd.DataFrame,
    equity_used: float,
) -> MissingPositionDiagnosis:
    symbol = str(row["symbol"])
    target_weight_pct = float(row["target_weight_pct"])
    last_order_px: Optional[float] = None

    # Use raw (unnormalized) weight from positions_jianan_v6 if available
    raw_weight = float(row.get("target_raw_weight", 0.0))
    abs_weight = abs(raw_weight) if raw_weight != 0.0 else abs(target_weight_pct)
    est_notional = float(abs_weight * equity_used)

    # Default outputs
    reason = "no_order_unknown"
    reason_detail = "No recent orders and not clearly below min_usd or missing metadata."

    if meta_row is None or meta_row.isna().all():
        reason = "no_meta"
        reason_detail = "Symbol not found in maicro_logs.hl_meta; cannot compute min size."
    else:
        min_usd_raw = meta_row.get("min_usd")
        min_usd = float(min_usd_raw) if min_usd_raw is not None else 0.0
        if est_notional < min_usd:
            reason = "below_min_usd"
            reason_detail = (
                f"Estimated notional {est_notional:,.0f} USD is below min_usd={min_usd:,.0f} "
                "from maicro_logs.hl_meta."
            )

    # Orders-based refinement
    orders_count = int(len(symbol_orders))
    last_order_ts = None
    last_order_side = None
    last_order_sz = None
    last_order_px = None
    last_order_status = None
    last_order_type = None
    last_order_reduce_only: Optional[bool] = None

    if orders_count > 0:
        latest = symbol_orders.iloc[0]
        last_order_ts = pd.to_datetime(latest["timestamp"]).to_pydatetime()
        last_order_side = str(latest.get("side", "") or "")
        last_order_sz = float(latest.get("sz", 0.0) or 0.0)
        last_order_px_raw = latest.get("limitPx", None)
        if last_order_px_raw is not None:
            try:
                last_order_px = float(last_order_px_raw)
            except Exception:
                last_order_px = None
        last_order_status = str(latest.get("status", "") or "")
        last_order_type = str(latest.get("orderType", "") or "")
        last_order_reduce_only = bool(latest.get("reduceOnly", False))

        statuses = symbol_orders["status"].astype(str).str.lower()
        has_open = (statuses == "open").any()
        has_filled = (statuses == "filled").any()
        has_canceled = (statuses == "canceled").any()
        reduce_only_all = bool(symbol_orders.get("reduceOnly", False).all())

        # Desired side based on target sign
        sign = np.sign(target_weight_pct if raw_weight == 0.0 else raw_weight)
        desired_side = "B" if sign > 0 else ("A" if sign < 0 else None)
        sides_set = set(symbol_orders["side"].astype(str)) if "side" in symbol_orders.columns else set()
        has_desired_side = desired_side in sides_set if desired_side else False

        if reduce_only_all:
            reason = "reduce_only_only"
            reason_detail = (
                "Only reduceOnly orders seen for this symbol in the lookback window; "
                "pipeline was trying to shrink/exit, not open a new position."
            )
        elif has_canceled and not has_open and not has_filled:
            reason = "orders_canceled"
            if has_desired_side:
                reason_detail = (
                    "Recent orders in the target direction exist but all were canceled; "
                    "possible limit price / liquidity issue."
                )
            else:
                reason_detail = (
                    "Recent orders exist but none in the target direction; "
                    "possible timing or offset mismatch."
                )
        elif has_open and not has_filled:
            reason = "open_order_no_pos"
            reason_detail = (
                "Open orders present but no corresponding live position yet; "
                "position may still be building or waiting for fill."
            )
        elif has_filled:
            reason = "filled_but_no_pos"
            reason_detail = (
                "Filled orders found in the lookback window but no live position; "
                "possible fast round-trip or logging/settlement issue."
            )
        else:
            # If we already classified as below_min_usd/no_meta, keep that.
            if reason == "no_order_unknown":
                reason_detail = (
                    "Recent orders found but pattern does not fit a simple bucket; "
                    "inspect detailed order history for this symbol."
                )

    return MissingPositionDiagnosis(
        symbol=symbol,
        target_weight_pct=target_weight_pct,
        est_notional_usd=est_notional,
        last_order_px=last_order_px,
        bn_spot_symbol=None,
        bn_spot_px=None,
        px_ratio_order_over_bn=None,
        bn_spot_px_at_order=None,
        px_ratio_order_over_bn_at_order=None,
        reason=reason,
        reason_detail=reason_detail,
        orders_count=orders_count,
        last_order_ts=last_order_ts,
        last_order_side=last_order_side,
        last_order_sz=last_order_sz,
        last_order_status=last_order_status,
        last_order_type=last_order_type,
        last_order_reduce_only=last_order_reduce_only,
    )


def diagnose_missing_positions(
    target_date: pd.Timestamp,
    run_ts: pd.Timestamp,
) -> List[MissingPositionDiagnosis]:
    """
    Build comparison, filter to missing positions, and classify each one.
    """
    targets = _load_targets_for_date(target_date)
    if targets.empty:
        return []

    actuals = _load_actuals_for_date(target_date)
    if actuals.empty:
        # No actuals at all: treat as empty frame with proper columns so build_comparison works.
        actuals = pd.DataFrame(columns=["symbol", "weight_norm", "usd", "equity_usd"])

    comp = build_comparison(targets, actuals)

    # Merge raw target weight into comparison so we can estimate notionals
    raw_targets = targets[["symbol", "weight"]].rename(columns={"weight": "target_raw_weight"})
    comp = comp.merge(raw_targets, on="symbol", how="left")

    missing = comp[(comp["has_target"]) & (~comp["has_actual"])].copy()
    if missing.empty:
        return []

    symbols = sorted(missing["symbol"].astype(str).str.upper().unique().tolist())
    equity_used = _estimate_equity_usd(actuals)
    meta = _load_meta_for_symbols(symbols)
    orders = _load_recent_orders(symbols, run_ts)
    bn_prices = _load_latest_binance_prices(symbols)

    diagnoses: List[MissingPositionDiagnosis] = []
    for _, row in missing.iterrows():
        sym = str(row["symbol"]).upper()
        meta_row = meta.loc[sym] if sym in meta.index else None
        sym_orders = orders[orders["symbol"] == sym] if not orders.empty else pd.DataFrame()
        diag = _classify_missing_position_row(row, meta_row, sym_orders, equity_used)
        bn_sym = _to_binance_symbol(sym)
        bn_px_latest = bn_prices.get(sym)
        diag.bn_spot_symbol = bn_sym
        diag.bn_spot_px = bn_px_latest
        if diag.last_order_px and bn_px_latest and bn_px_latest > 0:
            diag.px_ratio_order_over_bn = diag.last_order_px / bn_px_latest

        # Price at order time (1h close at or before last_order_ts)
        if diag.last_order_ts is not None:
            bn_px_at_order = _load_binance_price_at_time(sym, diag.last_order_ts)
            diag.bn_spot_px_at_order = bn_px_at_order
            if diag.last_order_px and bn_px_at_order and bn_px_at_order > 0:
                diag.px_ratio_order_over_bn_at_order = diag.last_order_px / bn_px_at_order
        diagnoses.append(diag)

    # Sort by descending |target_weight_pct|
    diagnoses.sort(key=lambda d: abs(d.target_weight_pct), reverse=True)
    return diagnoses


def format_email_text(
    target_date: pd.Timestamp,
    run_ts: pd.Timestamp,
    diagnoses: List[MissingPositionDiagnosis],
    offset_days: int,
) -> str:
    lines: List[str] = []
    lines.append("MAICRO: Missing Positions Diagnosis")
    lines.append("==================================")
    lines.append(f"Target (signal) date [positions_jianan_v6.date / live_positions.target_date]: {target_date.date()}")
    lines.append(
        f"Run timestamp [live_positions.ts, kind='current']: {run_ts} "
        f"(run_ts.date - target_date = {offset_days:+d} days)"
    )
    lines.append("")
    lines.append("Explanation:")
    lines.append("- Model targets are keyed by positions_jianan_v6.date = D.")
    lines.append("- When the live run executes later, rows are stored in maicro_logs.live_positions")
    lines.append("  with target_date = D and ts = actual run time.")
    lines.append("- This report looks at the latest ts (kind='current') and, for symbols where")
    lines.append("  target weight exists but actual weight is effectively zero, attempts to")
    lines.append("  explain why (metadata, min_usd, and recent orders in maicro_monitors.orders).")
    lines.append("")

    if not diagnoses:
        lines.append("No missing positions for this run (all targets have non-zero actuals).")
        return "\n".join(lines)

    # Out-of-market percentages (normalized target space: longs sum to 1, shorts sum to 1)
    long_missing = sum(max(d.target_weight_pct, 0.0) for d in diagnoses)
    short_missing = sum(abs(min(d.target_weight_pct, 0.0)) for d in diagnoses)
    net_missing = long_missing - short_missing

    lines.append(f"Missing positions: {len(diagnoses)} symbol(s)")
    lines.append("")
    lines.append("Out-of-market exposure (normalized target weights):")
    lines.append(f"  Long side missing : {100*long_missing:5.2f}% of target longs")
    lines.append(f"  Short side missing: {100*short_missing:5.2f}% of target shorts")
    lines.append(f"  Net missing (long - short): {100*net_missing:5.2f}%")
    lines.append("")
    header = (
        f"{'Symbol':<10} {'Target%':>8} {'Notional$':>10} "
        f"{'Reason':<18} {'Orders':>6} {'LastOrder':<19} {'Side':<4} {'Sz':>8} "
        f"{'OrdPx':>10} {'Bn@Ord':>10} {'Ord/Bn@Ord':>11} {'Bn@Now':>10} {'Ord/Bn@Now':>11} {'Status':<10}"
    )
    lines.append(header)
    lines.append("-" * len(header))

    for d in diagnoses:
        last_ts_str = d.last_order_ts.strftime("%m-%d %H:%M") if d.last_order_ts else "-"
        sz_str = f"{d.last_order_sz:8.3f}" if d.last_order_sz is not None else " " * 8
        ord_px_str = f"{d.last_order_px:10.3f}" if d.last_order_px not in (None, 0.0) else " " * 10
        bn_ord_str = (
            f"{(d.bn_spot_px_at_order or 0):10.3f}"
            if d.bn_spot_px_at_order not in (None, 0.0)
            else " " * 10
        )
        ratio_ord_str = (
            f"{d.px_ratio_order_over_bn_at_order:11.4f}"
            if d.px_ratio_order_over_bn_at_order not in (None, 0.0)
            else " " * 11
        )
        bn_now_str = (
            f"{(d.bn_spot_px or 0):10.3f}"
            if d.bn_spot_px not in (None, 0.0)
            else " " * 10
        )
        ratio_now_str = (
            f"{d.px_ratio_order_over_bn:11.4f}"
            if d.px_ratio_order_over_bn not in (None, 0.0)
            else " " * 11
        )
        lines.append(
            f"{d.symbol:<10} "
            f"{100*d.target_weight_pct:8.2f} "
            f"{d.est_notional_usd:10.0f} "
            f"{d.reason:<18} "
            f"{d.orders_count:6d} "
            f"{last_ts_str:<19} "
            f"{(d.last_order_side or '-'):4} "
            f"{sz_str} "
            f"{ord_px_str} "
            f"{bn_ord_str} "
            f"{ratio_ord_str} "
            f"{bn_now_str} "
            f"{ratio_now_str} "
            f"{(d.last_order_status or '-'):10}"
        )

    lines.append("")
    lines.append("Reason legend (coarse buckets):")
    lines.append("  - no_meta           : symbol missing from maicro_logs.hl_meta.")
    lines.append("  - below_min_usd     : target notional below min_usd; planner prunes to zero.")
    lines.append("  - reduce_only_only  : only reduceOnly orders seen (trying to shrink/exit).")
    lines.append("  - orders_canceled   : orders in target direction all canceled (e.g., stale limit).")
    lines.append("  - open_order_no_pos : open orders but no live position yet.")
    lines.append("  - filled_but_no_pos : fills seen but no live position (round-trip / logging gap).")
    lines.append("  - no_order_unknown  : no recent orders and not clearly explained by metadata.")
    lines.append("")
    lines.append(
        f"Order lookback window: last {ORDERS_LOOKBACK_HOURS}h before run_ts in maicro_monitors.orders."
    )
    lines.append("")
    lines.append("Computation sketch (Python-ish):")
    lines.append("  # Out-of-market exposure (normalized target space)")
    lines.append("  long_missing  = sum(max(w, 0.0) for w in missing_target_weights)")
    lines.append("  short_missing = sum(abs(min(w, 0.0)) for w in missing_target_weights)")
    lines.append("  net_missing   = long_missing - short_missing")
    lines.append("")
    lines.append("  # Per-symbol diagnosis inputs")
    lines.append("  raw_weight   = positions_jianan_v6.weight[date == D, symbol]")
    lines.append("  equity_used  = median(live_positions.equity_usd[date == D])")
    lines.append("  est_notional = abs(raw_weight) * equity_used")
    lines.append("  meta         = hl_meta[symbol]  # min_usd, min_units, size_step, tick_size")
    lines.append("  bn_symbol   = symbol + 'USDT'  # simple spot mapping")
    lines.append("  bn_px       = latest_close(binance.bn_spot_klines[bn_symbol, interval='1h'])")
    lines.append("  ord_px      = last_order.limitPx")
    lines.append("  ord_over_bn = ord_px / bn_px if bn_px else None")
    lines.append("  orders       = maicro_monitors.orders[coin == symbol")
    lines.append("                                        & timestamp in (run_ts - lookback, run_ts)]")
    lines.append("")
    lines.append("  # Reason buckets (simplified):")
    lines.append("  if symbol not in hl_meta: reason = 'no_meta'")
    lines.append("  elif est_notional < meta.min_usd: reason = 'below_min_usd'")
    lines.append("  elif only_reduce_only_orders(orders): reason = 'reduce_only_only'")
    lines.append("  elif all_canceled(orders): reason = 'orders_canceled'")
    lines.append("  elif any_open(orders): reason = 'open_order_no_pos'")
    lines.append("  elif any_filled(orders): reason = 'filled_but_no_pos'")
    lines.append("  else: reason = 'no_order_unknown'")
    return "\n".join(lines)


def _reason_color(reason: str) -> str:
    """
    Map reason → background color for the Reason cell.
    """
    if reason in ("no_meta", "filled_but_no_pos"):
        return "#fee2e2"  # red-ish
    if reason in ("orders_canceled", "open_order_no_pos", "reduce_only_only"):
        return "#fed7aa"  # orange-ish
    if reason == "below_min_usd":
        return "#fef9c3"  # yellow-ish
    if reason == "no_order_unknown":
        return "#e5e7eb"  # gray-ish
    return "#ffffff"


def _ord_bn_cell_style(ratio: Optional[float]) -> str:
    """
    Style for Ord/Bn cell:
      - |ratio - 1| < 0.10%  → neutral
      - 0.10%–0.30%         → light green (very close)
      - 0.30%–1.00%         → yellow (mildly off)
      - 1.00%–3.00%         → orange (noticeably off)
      - ≥3.00%              → red (really out of market)
    """
    base = "padding:4px 8px; text-align:right;"
    if ratio is None or ratio == 0.0:
        return base
    diff = abs(ratio - 1.0)
    if diff < 0.001:  # < 10 bps → neutral
        return base
    if diff < 0.003:  # 10–30 bps
        return base + " background-color:#dcfce7;"  # light green
    if diff < 0.01:  # 30–100 bps
        return base + " background-color:#fef9c3;"  # yellow
    if diff < 0.03:  # 100–300 bps
        return base + " background-color:#fed7aa;"  # orange
    # ≥ 3% off
    return base + " background-color:#fecaca;"  # red


def format_email_html(
    target_date: pd.Timestamp,
    run_ts: pd.Timestamp,
    diagnoses: List[MissingPositionDiagnosis],
    offset_days: int,
) -> str:
    # Out-of-market percentages (normalized target space)
    if diagnoses:
        long_missing = sum(max(d.target_weight_pct, 0.0) for d in diagnoses)
        short_missing = sum(abs(min(d.target_weight_pct, 0.0)) for d in diagnoses)
        net_missing = long_missing - short_missing
        coverage_html = f"""
    <p style="margin-top:4px; color:#6b7280;">
      Out-of-market exposure (normalized target weights):<br>
      • Long side missing: <b>{100*long_missing:0.2f}%</b> of target longs<br>
      • Short side missing: <b>{100*short_missing:0.2f}%</b> of target shorts<br>
      • Net missing (long − short): <b>{100*net_missing:0.2f}%</b>
    </p>
    """
    else:
        coverage_html = """
    <p style="margin-top:4px; color:#6b7280;">
      No missing positions for this run (all targets have non-zero actuals).
    </p>
    """

    if not diagnoses:
        empty_body = """
        <p>No missing positions for this run (all targets have non-zero actuals).</p>
        """
        diagnoses_rows_html = empty_body
    else:
        rows: List[str] = []
        for d in diagnoses:
            reason_bg = _reason_color(d.reason)
            last_ts_str = d.last_order_ts.strftime("%Y-%m-%d %H:%M") if d.last_order_ts else "-"
            sz_str = f"{d.last_order_sz:.4f}" if d.last_order_sz is not None else "-"
            px_str = f"{d.last_order_px:.4f}" if d.last_order_px is not None and d.last_order_px != 0.0 else "-"
            bn_px_ord_str = (
                f"{d.bn_spot_px_at_order:.4f}"
                if d.bn_spot_px_at_order is not None and d.bn_spot_px_at_order != 0.0
                else "-"
            )
            ratio_ord_str = (
                f"{d.px_ratio_order_over_bn_at_order:.4f}"
                if d.px_ratio_order_over_bn_at_order is not None and d.px_ratio_order_over_bn_at_order != 0.0
                else "-"
            )
            bn_px_now_str = (
                f"{d.bn_spot_px:.4f}"
                if d.bn_spot_px is not None and d.bn_spot_px != 0.0
                else "-"
            )
            ratio_now_str = (
                f"{d.px_ratio_order_over_bn:.4f}"
                if d.px_ratio_order_over_bn is not None and d.px_ratio_order_over_bn != 0.0
                else "-"
            )
            reduce_only_label = "true" if d.last_order_reduce_only else "false" if d.last_order_reduce_only is not None else "-"

            rows.append(
                "<tr>"
                f"<td style='padding:4px 8px;'>{d.symbol}</td>"
                f"<td style='padding:4px 8px; text-align:right;'>{100*d.target_weight_pct:0.2f}</td>"
                f"<td style='padding:4px 8px; text-align:right;'>{d.est_notional_usd:,.0f}</td>"
                f"<td style='padding:4px 8px; background-color:{reason_bg};'>{d.reason}</td>"
                f"<td style='padding:4px 8px; text-align:right;'>{d.orders_count}</td>"
                f"<td style='padding:4px 8px;'>{last_ts_str}</td>"
                f"<td style='padding:4px 8px; text-align:center;'>{d.last_order_side or '-'}</td>"
                f"<td style='padding:4px 8px; text-align:right;'>{sz_str}</td>"
                f"<td style='padding:4px 8px; text-align:right;'>{px_str}</td>"
                f"<td style='padding:4px 8px; text-align:right;'>{bn_px_ord_str}</td>"
                f"<td style='padding:4px 8px; text-align:right;'>{ratio_ord_str}</td>"
                f"<td style='padding:4px 8px; text-align:right;'>{bn_px_now_str}</td>"
                f"<td style='{_ord_bn_cell_style(d.px_ratio_order_over_bn)}'>{ratio_now_str}</td>"
                f"<td style='padding:4px 8px; text-align:left;'>{d.last_order_status or '-'}</td>"
                f"<td style='padding:4px 8px; text-align:left;'>{d.last_order_type or '-'}</td>"
                f"<td style='padding:4px 8px; text-align:center;'>{reduce_only_label}</td>"
                "</tr>"
            )
        diagnoses_rows_html = "".join(rows)

    explanation_html = f"""
    <p style="margin-top:0; color:#6b7280;">
      Target (signal) date <code>D</code> is taken from <code>positions_jianan_v6.date</code> and
      <code>live_positions.target_date</code>. When the live run executes later, rows are stored in
      <code>maicro_logs.live_positions</code> with <code>target_date = D</code> and <code>ts</code> equal to the
      actual run time. This report uses the latest <code>ts</code> where <code>kind = 'current'</code> and focuses
      on symbols where a target weight exists but the actual live position weight is effectively zero.
    </p>
    <p style="margin-top:4px; color:#6b7280;">
      For each missing symbol we:
      (1) estimate notional using a median equity proxy from <code>live_positions</code>,
      (2) check <code>maicro_logs.hl_meta</code> for <code>min_usd</code>,
      (3) scan recent orders for that symbol in <code>maicro_monitors.orders</code> within the last
      {ORDERS_LOOKBACK_HOURS}h, and
      (4) assign a coarse reason bucket (no_meta, below_min_usd, reduce_only_only, orders_canceled, open_order_no_pos,
      filled_but_no_pos, no_order_unknown).
    </p>
    """
    code_snippet_html = """
    <h3 style="margin-top:12px; margin-bottom:4px;">Appendix: computation sketch</h3>
    <pre style="background-color:#111827; color:#e5e7eb; padding:8px 10px; border-radius:4px; font-size:12px; overflow-x:auto;">
<code># Out-of-market exposure (normalized target space)
long_missing  = sum(max(w, 0.0) for w in missing_target_weights)
short_missing = sum(abs(min(w, 0.0)) for w in missing_target_weights)
net_missing   = long_missing - short_missing

# Per-symbol diagnosis inputs
raw_weight   = positions_jianan_v6.weight[date == D, symbol]
equity_used  = median(live_positions.equity_usd[date == D])
est_notional = abs(raw_weight) * equity_used
meta         = hl_meta[symbol]  # min_usd, min_units, size_step, tick_size
orders       = maicro_monitors.orders[
    (coin == symbol)
    & (timestamp between run_ts - lookback and run_ts)
]

bn_symbol   = symbol + "USDT"  # simple spot mapping
bn_px       = latest_close(binance.bn_spot_klines[bn_symbol, interval="1h"])
ord_px      = last_order.limitPx
ord_over_bn = ord_px / bn_px if bn_px else None

# Reason buckets (simplified)
if symbol not in hl_meta:
    reason = "no_meta"
elif est_notional &lt; meta.min_usd:
    reason = "below_min_usd"
elif only_reduce_only_orders(orders):
    reason = "reduce_only_only"
elif all_canceled(orders):
    reason = "orders_canceled"
elif any_open(orders):
    reason = "open_order_no_pos"
elif any_filled(orders):
    reason = "filled_but_no_pos"
else:
    reason = "no_order_unknown"</code></pre>
    """

    html = f"""<html>
  <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; font-size: 14px; color: #111827;">
    <h2 style="margin-bottom:4px;">MAICRO: Missing Positions Diagnosis</h2>
    <p style="margin-top:0; color:#6b7280;">
      Target (signal) date: <b>{target_date.date()}</b> ·
      Run timestamp: <b>{run_ts}</b> ·
      Offset (run_ts.date - target_date): <b>{offset_days:+d}d</b>
    </p>
    {coverage_html}
    {explanation_html}
    <h3 style="margin-bottom:4px;">Missing positions for latest run</h3>
    <table cellspacing="0" cellpadding="0" style="border-collapse:collapse; border:1px solid #e5e7eb; margin-top:4px;">
      <thead>
        <tr style="background-color:#f3f4f6;">
          <th style="padding:4px 8px; text-align:left;">Symbol</th>
          <th style="padding:4px 8px; text-align:right;">Target %</th>
          <th style="padding:4px 8px; text-align:right;">Est Notional (USD)</th>
          <th style="padding:4px 8px; text-align:left;">Reason</th>
          <th style="padding:4px 8px; text-align:right;"># Orders</th>
          <th style="padding:4px 8px; text-align:left;">Last Order TS</th>
          <th style="padding:4px 8px; text-align:center;">Side</th>
          <th style="padding:4px 8px; text-align:right;">Sz</th>
          <th style="padding:4px 8px; text-align:right;">Ord Px</th>
          <th style="padding:4px 8px; text-align:right;">Bn Px @ Ord</th>
          <th style="padding:4px 8px; text-align:right;">Ord/Bn @ Ord</th>
          <th style="padding:4px 8px; text-align:right;">Bn Px @ Now</th>
          <th style="padding:4px 8px; text-align:right;">Ord/Bn @ Now</th>
          <th style="padding:4px 8px; text-align:left;">Status</th>
          <th style="padding:4px 8px; text-align:left;">Type</th>
          <th style="padding:4px 8px; text-align:center;">reduceOnly</th>
        </tr>
      </thead>
      <tbody>
        {diagnoses_rows_html}
      </tbody>
    </table>
    <p style="margin-top:12px; color:#6b7280; font-size:12px;">
      Reason colors:<br>
      <span style="background-color:#fee2e2; padding:2px 4px; border-radius:3px;">no_meta / filled_but_no_pos</span>
      (red, structural / serious),<br>
      <span style="background-color:#fed7aa; padding:2px 4px; border-radius:3px;">orders_canceled / open_order_no_pos / reduce_only_only</span>
      (orange, execution or intent),<br>
      <span style="background-color:#fef9c3; padding:2px 4px; border-radius:3px;">below_min_usd</span>
      (yellow, size constraint),<br>
      <span style="background-color:#e5e7eb; padding:2px 4px; border-radius:3px;">no_order_unknown</span>
      (gray, unexplained; check raw orders and logs).
    </p>
    {code_snippet_html}
  </body>
</html>"""
    return html


def send_email(subject: str, text_body: str, html_body: str) -> None:
    if not RESEND_API_KEY:
        print("RESEND_API_KEY not set; skipping email send.")
        return

    print(f"Sending missing-positions diagnosis email to {TO_EMAIL}...")
    url = "https://api.resend.com/emails"
    headers = {
        "Authorization": f"Bearer {RESEND_API_KEY}",
        "Content-Type": "application/json",
    }
    data: Dict[str, object] = {
        "from": FROM_EMAIL,
        "to": [TO_EMAIL],
        "subject": subject,
        "text": text_body,
        "html": html_body,
    }

    try:
        resp = requests.post(url, json=data, headers=headers, timeout=10)
        resp.raise_for_status()
        print("Email sent successfully.")
    except Exception as e:
        print(f"Failed to send email: {e}")
        if hasattr(e, "response") and e.response is not None:
            print(e.response.text)


def main() -> None:
    print("[missing_positions_diagnosis_daily] Starting...")

    ctx = _load_latest_run_context()
    if not ctx:
        print("No live_positions.current runs found; exiting.")
        return

    target_date, run_ts = ctx
    offset_days = (run_ts.date() - target_date.date()).days
    print(f"Using target_date={target_date.date()}, run_ts={run_ts}, offset_days={offset_days}")

    diagnoses = diagnose_missing_positions(target_date, run_ts)

    text_body = format_email_text(target_date, run_ts, diagnoses, offset_days)
    html_body = format_email_html(target_date, run_ts, diagnoses, offset_days)

    subject = (
        f"[MAICRO DAILY] Missing Positions Diagnosis - "
        f"D={target_date.date()} (run={run_ts.date()}, offset={offset_days:+d}d)"
    )

    print("----- EMAIL BODY BEGIN -----")
    print(text_body)
    print("----- EMAIL BODY END -----")

    send_email(subject, text_body, html_body)
    print("[missing_positions_diagnosis_daily] Done.")


if __name__ == "__main__":
    main()
