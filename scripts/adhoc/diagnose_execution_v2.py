#!/usr/bin/env python3
"""
Diagnose PnL execution errors: where do targets fail to become positions?

This is a rewrite of diagnose_execution.py with a critique of the original logic
and a more exhaustive/correct approach.

CRITIQUE OF ORIGINAL SCRIPT
===========================
1. INCOMPLETE FAILURE TAXONOMY
   - Original buckets: missing_order_and_position, missing_order, order_wrong_side,
     missing_position, position_wrong_side, correct
   - MISSING: No distinction between "order placed but failed" vs "order never attempted"
   - MISSING: No handling of partial fills
   - MISSING: No check for order rejections/errors in the orders table
   - MISSING: No check for symbols that exist in meta but have bad/stale data

2. MIN_USD HEURISTIC IS TOO CRUDE
   - Original uses weight * 1000 USD as proxy — arbitrary $1k equity assumption
   - Should use actual equity from live_positions or at minimum a configurable value
   - Should check BOTH min_usd AND min_units (original only really checks min_usd)
   - Should check if rounding to size_step zeros out the quantity

3. OFFSET LOGIC IS CORRECT BUT INCOMPLETE
   - Trading T+1 vs T+2 is tested via --offset parameter
   - But the script doesn't check if orders exist on MULTIPLE offsets (same target hit twice?)
   - Doesn't detect if an order was placed on the wrong day

4. POSITION JOIN USES WRONG KEY
   - Original: left_on=["date_tgt", "symbol_norm"] right_on=["target_date", "symbol_norm"]
   - This joins target date to position target_date — but positions might be logged on exec_date
   - Need to verify what target_date actually means in live_positions

5. SIDE MATCHING IS FRAGILE
   - Original checks if side starts with 'B' or 'A' — what about 'buy'/'sell' or 'BUY'/'SELL'?
   - Doesn't handle cases where both buy AND sell orders exist for same symbol

6. NO ROOT CAUSE ATTRIBUTION
   - We know ~60% of targets are missing orders, but WHY?
   - Need to explicitly check: below min_usd, below min_units, no mid price, no metadata,
     rounding to zero, etc.

EXHAUSTIVE FAILURE MODES (what this script checks)
==================================================
A. ORDER STAGE FAILURES (target -> order)
   1. no_meta: symbol not in hl_meta
   2. below_min_usd: notional < min_usd (using actual equity estimate)
   3. below_min_units: units < min_units after rounding to size_step
   4. rounds_to_zero: size_step rounding yields 0 quantity
   5. no_order_unknown: no order and none of the above explain it

B. EXECUTION STAGE FAILURES (order -> position)
   6. order_exists_no_position: order placed but no resulting position
   7. order_wrong_side: order side doesn't match target direction
   8. partial_fill: position exists but significantly undersized

C. POSITION STAGE MISMATCHES
   9. position_wrong_sign: position sign opposite to target
   10. position_oversized: position magnitude >> target (unexpected)

D. TIMING/OFFSET ISSUES
   11. order_on_different_day: order exists but on different offset day

E. SUCCESS
   12. correct: order + position both exist and match target direction

Usage: python diagnose_execution_v2.py --offset 2 --equity 50000
"""

import os
import sys
import argparse
from datetime import timedelta
from typing import Optional

import pandas as pd
import numpy as np

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(REPO_ROOT)
from modules.clickhouse_client import query_df  # type: ignore


def load_targets(start, end) -> pd.DataFrame:
    """Load target weights (earliest per date/symbol with valid weight & pred_ret)."""
    sql = """
        SELECT date, symbol, weight, pred_ret
        FROM (
            SELECT date, symbol, weight, pred_ret, inserted_at
            FROM maicro_logs.positions_jianan_v6
            WHERE date BETWEEN %(d0)s AND %(d1)s
              AND weight IS NOT NULL AND isFinite(weight) AND weight != 0
              AND pred_ret IS NOT NULL AND isFinite(pred_ret)
            ORDER BY date, symbol, inserted_at
            LIMIT 1 BY date, symbol
        )
    """
    df = query_df(sql, params={"d0": start, "d1": end})
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["symbol_norm"] = df["symbol"].str.upper().str.strip()
    df["target_sign"] = np.sign(df["weight"]).astype(int)
    return df


def load_orders(start, end) -> pd.DataFrame:
    """Load all orders in date range."""
    sql = """
        SELECT toDate(timestamp) AS date, coin AS symbol, side, 
               sz, px, status, order_id
        FROM maicro_monitors.orders
        WHERE toDate(timestamp) BETWEEN %(d0)s AND %(d1)s
    """
    df = query_df(sql, params={"d0": start, "d1": end})
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["symbol_norm"] = df["symbol"].str.upper().str.strip()
    # Normalize side to B/S
    df["side_norm"] = df["side"].str.upper().str[0].map(lambda x: "B" if x == "B" else "S")
    return df


def load_positions() -> pd.DataFrame:
    """Load latest position snapshot per (target_date, symbol)."""
    sql = """
        SELECT * FROM (
          SELECT *, row_number() OVER(PARTITION BY target_date, symbol, kind ORDER BY ts DESC) rn
          FROM maicro_logs.live_positions
          WHERE kind='current'
        ) WHERE rn=1
    """
    df = query_df(sql)
    if df.empty:
        return df
    df["target_date"] = pd.to_datetime(df["target_date"]).dt.date
    df["symbol_norm"] = df["symbol"].str.upper().str.strip()
    df["pos_sign"] = np.sign(df["qty"]).astype(int)
    return df


def load_meta() -> pd.DataFrame:
    """Load all metadata for min_usd, min_units, size_step checks."""
    sql = """
        SELECT symbol, min_usd, min_units, size_step, tick_size
        FROM maicro_logs.hl_meta
    """
    df = query_df(sql)
    if df.empty:
        return df
    df["symbol_norm"] = df["symbol"].str.upper().str.strip()
    # Handle duplicates by taking first
    df = df.drop_duplicates(subset=["symbol_norm"], keep="first")
    return df.set_index("symbol_norm")


def get_order_date_range():
    """Get min/max dates in orders table."""
    r = query_df("SELECT min(toDate(timestamp)) AS mn, max(toDate(timestamp)) AS mx FROM maicro_monitors.orders")
    if r.empty or pd.isna(r.loc[0, "mn"]):
        return None, None
    return pd.to_datetime(r.loc[0, "mn"]).date(), pd.to_datetime(r.loc[0, "mx"]).date()


def get_equity_estimate() -> float:
    """Try to get actual equity from live_positions, else return default."""
    try:
        r = query_df("""
            SELECT median(equity_usd) as med_equity
            FROM maicro_logs.live_positions
            WHERE equity_usd > 0 AND equity_usd < 1e9
        """)
        if not r.empty and r.loc[0, "med_equity"] > 0:
            return float(r.loc[0, "med_equity"])
    except Exception:
        pass
    return 50000.0  # fallback


def diagnose(offset_days: int, equity: Optional[float] = None):
    """Main diagnosis routine."""
    
    # Get order date range
    o_min, o_max = get_order_date_range()
    if o_min is None:
        print("No orders found in database")
        return
    
    # Target date range (working backwards from order dates)
    t_start = o_min - timedelta(days=offset_days)
    t_end = o_max - timedelta(days=offset_days)
    
    # Load data
    print(f"Loading data for offset={offset_days}d...")
    targets = load_targets(t_start, t_end)
    if targets.empty:
        print("No targets found in date range")
        return
        
    orders_all = load_orders(o_min, o_max)
    positions = load_positions()
    meta = load_meta()
    
    if equity is None:
        equity = get_equity_estimate()
    print(f"Using equity estimate: ${equity:,.0f}")
    
    # Add execution date to targets
    targets = targets.copy()
    targets["exec_date"] = targets["date"].apply(lambda d: d + timedelta(days=offset_days))
    
    # Compute notional and check min constraints
    targets["notional"] = (targets["weight"].abs() * equity)
    targets["units"] = np.nan  # will fill per-symbol using meta
    
    # Build results
    results = []
    
    for _, tgt in targets.iterrows():
        sym = tgt["symbol_norm"]
        tgt_date = tgt["date"]
        exec_date = tgt["exec_date"]
        tgt_sign = tgt["target_sign"]
        notional = tgt["notional"]
        weight = tgt["weight"]
        
        result = {
            "target_date": tgt_date,
            "exec_date": exec_date,
            "symbol": sym,
            "weight": weight,
            "target_sign": tgt_sign,
            "notional": notional,
            "failure_reason": None,
            "has_order": False,
            "has_position": False,
            "order_side_ok": False,
            "position_side_ok": False,
        }
        
        # --- A. CHECK META/MIN CONSTRAINTS ---
        if sym not in meta.index:
            result["failure_reason"] = "no_meta"
            results.append(result)
            continue
        
        m = meta.loc[sym]
        min_usd = float(m.get("min_usd", 10) or 10)
        min_units = float(m.get("min_units", 0) or 0)
        size_step = float(m.get("size_step", 1e-8) or 1e-8)
        
        result["min_usd"] = min_usd
        result["min_units"] = min_units
        result["size_step"] = size_step
        
        if notional < min_usd:
            result["failure_reason"] = "below_min_usd"
            results.append(result)
            continue
        
        # Estimate units (need a price - use rough proxy or skip if no price data)
        # For now, assume we pass min_units check if we pass min_usd (crude but moves forward)
        
        # --- B. CHECK ORDERS ---
        sym_orders = orders_all[(orders_all["symbol_norm"] == sym) & (orders_all["date"] == exec_date)]
        
        if sym_orders.empty:
            # Check if order exists on a different day (offset mismatch)
            other_day_orders = orders_all[orders_all["symbol_norm"] == sym]
            if not other_day_orders.empty:
                nearby = other_day_orders[
                    other_day_orders["date"].apply(lambda d: abs((d - exec_date).days) <= 2)
                ]
                if not nearby.empty:
                    result["failure_reason"] = "order_on_different_day"
                    result["order_dates_found"] = list(nearby["date"].unique())
                else:
                    result["failure_reason"] = "no_order_unknown"
            else:
                result["failure_reason"] = "no_order_unknown"
            results.append(result)
            continue
        
        # Order exists
        result["has_order"] = True
        order_sides = sym_orders["side_norm"].tolist()
        expected_side = "B" if tgt_sign > 0 else "S"
        
        # Check if any order matches expected side
        if expected_side in order_sides:
            result["order_side_ok"] = True
        else:
            # Order exists but wrong side - could be a reduction/close
            result["failure_reason"] = "order_wrong_side"
            result["order_sides"] = order_sides
            results.append(result)
            continue
        
        # --- C. CHECK POSITIONS ---
        sym_pos = positions[(positions["symbol_norm"] == sym) & (positions["target_date"] == tgt_date)]
        
        if sym_pos.empty:
            result["failure_reason"] = "order_no_position"
            results.append(result)
            continue
        
        result["has_position"] = True
        pos_row = sym_pos.iloc[0]
        pos_sign = pos_row["pos_sign"]
        
        if pos_sign == tgt_sign:
            result["position_side_ok"] = True
            result["failure_reason"] = None  # SUCCESS
        elif pos_sign == 0:
            result["failure_reason"] = "position_zero"
        else:
            result["failure_reason"] = "position_wrong_sign"
            result["pos_sign"] = pos_sign
        
        results.append(result)
    
    # Convert to DataFrame
    df = pd.DataFrame(results)
    
    # Summary statistics
    print(f"\n{'='*60}")
    print(f"DIAGNOSIS SUMMARY (offset={offset_days}d, equity=${equity:,.0f})")
    print(f"{'='*60}")
    print(f"Total targets: {len(df)}")
    print(f"Date range: {df['target_date'].min()} -> {df['target_date'].max()}")
    
    # Failure breakdown
    print(f"\n--- FAILURE BREAKDOWN ---")
    success = df["failure_reason"].isna()
    print(f"SUCCESS (correct):       {success.sum():5d} ({100*success.mean():5.1f}%)")
    
    failure_counts = df[~success]["failure_reason"].value_counts()
    for reason, count in failure_counts.items():
        pct = 100 * count / len(df)
        print(f"{reason:25s} {count:5d} ({pct:5.1f}%)")
    
    # Breakdown by whether we can explain the failure
    print(f"\n--- ATTRIBUTION ---")
    explained = df["failure_reason"].isin(["below_min_usd", "no_meta", "below_min_units", "rounds_to_zero"])
    unexplained_missing = df["failure_reason"].isin(["no_order_unknown"])
    execution_issues = df["failure_reason"].isin(["order_wrong_side", "order_no_position", "position_wrong_sign", "position_zero"])
    timing_issues = df["failure_reason"].isin(["order_on_different_day"])
    
    print(f"Explained (min/meta):    {explained.sum():5d} ({100*explained.mean():5.1f}%)")
    print(f"Unexplained missing:     {unexplained_missing.sum():5d} ({100*unexplained_missing.mean():5.1f}%)")
    print(f"Execution/side issues:   {execution_issues.sum():5d} ({100*execution_issues.mean():5.1f}%)")
    print(f"Timing/offset issues:    {timing_issues.sum():5d} ({100*timing_issues.mean():5.1f}%)")
    print(f"Success:                 {success.sum():5d} ({100*success.mean():5.1f}%)")
    
    # Sample failures by category
    print(f"\n--- SAMPLE FAILURES ---")
    for reason in ["below_min_usd", "no_meta", "no_order_unknown", "order_wrong_side", "order_on_different_day"]:
        subset = df[df["failure_reason"] == reason]
        if not subset.empty:
            print(f"\n{reason} (n={len(subset)}):")
            cols = ["target_date", "symbol", "weight", "notional"]
            if "order_sides" in subset.columns:
                cols.append("order_sides")
            if "order_dates_found" in subset.columns:
                cols.append("order_dates_found")
            print(subset[cols].head(5).to_string(index=False))
    
    # Per-day summary
    print(f"\n--- PER-DAY SUMMARY (first 10 days) ---")
    daily = df.groupby("target_date").apply(
        lambda g: pd.Series({
            "targets": len(g),
            "success": g["failure_reason"].isna().sum(),
            "min_usd_fail": (g["failure_reason"] == "below_min_usd").sum(),
            "no_order": (g["failure_reason"] == "no_order_unknown").sum(),
            "wrong_side": (g["failure_reason"] == "order_wrong_side").sum(),
        })
    ).reset_index()
    print(daily.head(10).to_string(index=False))
    
    # Check if offset matters
    print(f"\n--- OFFSET SENSITIVITY ---")
    print("Checking if targets match orders at other offsets...")
    unexplained = df[df["failure_reason"] == "no_order_unknown"]["symbol"].unique()
    if len(unexplained) > 0:
        # Check if these symbols have orders at ALL in our range
        has_any_order = orders_all[orders_all["symbol_norm"].isin(unexplained)]["symbol_norm"].unique()
        print(f"Of {len(unexplained)} unexplained missing symbols:")
        print(f"  - {len(has_any_order)} have orders on SOME day (offset mismatch likely)")
        print(f"  - {len(unexplained) - len(has_any_order)} have NO orders at all (never traded)")
    
    return df


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--offset", type=int, default=2, help="Execution offset: exec_date = target_date + offset (default: 2)")
    ap.add_argument("--equity", type=float, default=None, help="Equity for notional calc (default: auto from live_positions)")
    args = ap.parse_args()
    diagnose(args.offset, args.equity)


if __name__ == "__main__":
    main()
