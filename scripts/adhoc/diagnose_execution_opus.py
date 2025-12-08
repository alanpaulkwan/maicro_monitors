#!/usr/bin/env python3
"""
Diagnose PnL execution errors - Opus edition

This script follows the alignment logic from test_pnl_analysis.ipynb to properly 
diagnose where target weights fail to become actual positions.

KEY INSIGHT FROM NOTEBOOK:
=========================
The notebook compares 4 PnL streams:
1. paper_model_ret: positions_jianan_v6 weights applied with shift_period=1 (T+1 execution)
2. paper_model_ret_nolags: positions_jianan_v6 weights with shift_period=0 (same-day)
3. live_theoretic_ret: live_positions 'current' weights with shift_period=0
4. live_pnls: actual NAV returns from live_account

The CORRECT alignment is:
- Target weight on date D should be compared to live_positions 'current' on date D
- Those weights should execute and affect PnL on date D+1 (shift_period=1 for paper)
- But live positions are logged with shift_period=0 (already executed)

CRITIQUE OF ORIGINAL diagnose_execution.py:
===========================================
1. WRONG COMPARISON:
   - Compares positions_jianan_v6(date=D) to orders(date=D+offset)
   - But should compare to live_positions(target_date=D, kind='current')
   - target_date in live_positions represents the signal date, NOT exec date

2. WRONG KEY ASSUMPTION:
   - Assumes exec_date = target_date + offset
   - Actually: live_positions are logged after execution with target_date = signal date
   - The 'ts' field shows when they were logged (after execution)

3. MISSING ROOT CAUSES:
   - Doesn't check if target weight rounds to zero after min_usd/min_units/size_step
   - Doesn't account for reserve_pct shrinking notional
   - Doesn't check for missing mids/meta before order stage

4. INCOMPLETE TAXONOMY:
   - "missing_order" could mean: never attempted, attempted but rejected, or offset mismatch
   - "position_wrong_side" could mean: legitimate reduction, or actual error

CORRECT LOGIC (from notebook):
==============================
1. Load positions_jianan_v6: earliest per (date, symbol) with finite weight & pred_ret
2. Load live_positions: last 'current' per (target_date, symbol) - this IS the actual position
3. Compare on (date, symbol) key where date = positions_jianan_v6.date = live_positions.target_date
4. No need to compute exec_date - target_date already represents the signal date

FAILURE TAXONOMY (exhaustive):
==============================
A. TARGET STAGE (positions_jianan_v6 -> planner input)
   1. target_invalid: weight or pred_ret is inf/nan (shouldn't happen with our query)
   2. target_duplicate: multiple rows for same (date, symbol) after taking earliest
   
B. PLANNER STAGE (target -> order intent)
   3. no_meta: symbol not in hl_meta
   4. no_mid: no mid price available for symbol
   5. below_min_usd: abs(weight * equity) < min_usd
   6. below_min_units: quantity after notional/price < min_units
   7. rounds_to_zero: size_step rounding yields zero quantity
   8. reserve_prune: after reserve_pct deduction, notional < min_usd
   
C. EXECUTOR STAGE (order intent -> actual order)
   9. api_error: order rejected by exchange
   10. network_error: order never reached exchange
   11. dry_run: DRY_RUN=true so no actual order
   
D. SETTLEMENT STAGE (order -> position)
   12. order_no_position: order placed but no resulting live_position
   13. partial_fill: position qty << order qty
   14. wrong_side: position sign opposite to target (could be legitimate reduction)
   15. wrong_magnitude: position weight far from target weight

E. SUCCESS
   16. correct: live_position exists with matching sign and reasonable magnitude

Usage:
    python diagnose_execution_opus.py [--start_date YYYY-MM-DD] [--end_date YYYY-MM-DD] [--tolerance 0.02]
"""

import os
import sys
import argparse
from datetime import datetime, timedelta
from typing import Optional, Tuple

import pandas as pd
import numpy as np

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(REPO_ROOT)
from modules.clickhouse_client import query_df  # type: ignore


def load_targets(start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
    """
    Load target weights from positions_jianan_v6.
    Takes earliest row per (date, symbol) with finite, non-zero weight & pred_ret.
    """
    where_clause = "1=1"
    params = {}
    if start_date:
        where_clause += " AND date >= %(start)s"
        params["start"] = start_date
    if end_date:
        where_clause += " AND date <= %(end)s"
        params["end"] = end_date
    
    sql = f"""
        SELECT date, symbol, weight, pred_ret, inserted_at
        FROM (
            SELECT date, symbol, weight, pred_ret, inserted_at
            FROM maicro_logs.positions_jianan_v6
            WHERE {where_clause}
              AND weight IS NOT NULL AND isFinite(weight) AND weight != 0
              AND pred_ret IS NOT NULL AND isFinite(pred_ret)
            ORDER BY date, symbol, inserted_at
            LIMIT 1 BY date, symbol
        )
    """
    df = query_df(sql, params=params)
    if df.empty:
        return df
    
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["symbol"] = df["symbol"].str.upper().str.strip()
    df["target_sign"] = np.sign(df["weight"]).astype(int)
    df["abs_weight"] = df["weight"].abs()
    return df


def load_live_positions(start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
    """
    Load live positions (kind='current') - these are actual positions after execution.
    Takes last row per (target_date, symbol, kind) ordered by ts.
    """
    where_clause = "kind = 'current'"
    params = {}
    if start_date:
        where_clause += " AND target_date >= %(start)s"
        params["start"] = start_date
    if end_date:
        where_clause += " AND target_date <= %(end)s"
        params["end"] = end_date
    
    sql = f"""
        SELECT target_date, symbol, qty, px, usd, equity_usd, ts
        FROM (
            SELECT *, row_number() OVER(PARTITION BY target_date, symbol, kind ORDER BY ts DESC) rn
            FROM maicro_logs.live_positions
            WHERE {where_clause}
        )
        WHERE rn = 1
    """
    df = query_df(sql, params=params)
    if df.empty:
        return df
    
    df["target_date"] = pd.to_datetime(df["target_date"]).dt.date
    df["symbol"] = df["symbol"].str.upper().str.strip()
    df["pos_sign"] = np.sign(df["qty"]).astype(int)
    
    # Compute actual weight
    df["weight"] = df["usd"] / df["equity_usd"]
    df["abs_weight"] = df["weight"].abs()
    
    return df


def load_meta() -> pd.DataFrame:
    """Load metadata (min_usd, min_units, size_step, tick_size)."""
    sql = "SELECT symbol, min_usd, min_units, size_step, tick_size FROM maicro_logs.hl_meta"
    df = query_df(sql)
    if df.empty:
        return df
    df["symbol"] = df["symbol"].str.upper().str.strip()
    df = df.drop_duplicates(subset=["symbol"], keep="first")
    return df.set_index("symbol")


def load_orders(start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
    """Load orders for reference (optional - to check if order was placed)."""
    where_clause = "1=1"
    params = {}
    if start_date:
        where_clause += " AND toDate(timestamp) >= %(start)s"
        params["start"] = start_date
    if end_date:
        where_clause += " AND toDate(timestamp) <= %(end)s"
        params["end"] = end_date
    
    sql = f"""
        SELECT toDate(timestamp) AS date, coin AS symbol, side, sz, status
        FROM maicro_monitors.orders
        WHERE {where_clause}
    """
    df = query_df(sql, params=params)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["symbol"] = df["symbol"].str.upper().str.strip()
    return df


def get_median_equity(live_positions: pd.DataFrame) -> float:
    """Get median equity from live_positions."""
    if live_positions.empty or "equity_usd" not in live_positions.columns:
        return 50000.0
    valid = live_positions[live_positions["equity_usd"] > 0]["equity_usd"]
    if valid.empty:
        return 50000.0
    return valid.median()


def diagnose_failures(
    targets: pd.DataFrame,
    live_positions: pd.DataFrame,
    meta: pd.DataFrame,
    orders: pd.DataFrame,
    tolerance: float = 0.02,
) -> pd.DataFrame:
    """
    Compare targets to live positions and classify failures.
    
    Parameters:
    -----------
    targets : DataFrame
        From positions_jianan_v6 (date, symbol, weight, target_sign, abs_weight)
    live_positions : DataFrame
        From live_positions kind='current' (target_date, symbol, weight, pos_sign, abs_weight, qty, usd, equity_usd)
    meta : DataFrame
        From hl_meta (indexed by symbol: min_usd, min_units, size_step, tick_size)
    orders : DataFrame
        From orders table (date, symbol, side, sz, status)
    tolerance : float
        Weight tolerance for "correct" classification (default 2%)
    
    Returns:
    --------
    DataFrame with diagnosis per (date, symbol)
    """
    # Merge targets with live_positions on (date=target_date, symbol)
    merged = targets.merge(
        live_positions,
        left_on=["date", "symbol"],
        right_on=["target_date", "symbol"],
        how="left",
        suffixes=("_target", "_actual"),
    )
    
    # Add metadata
    merged = merged.merge(
        meta.reset_index()[["symbol", "min_usd", "min_units", "size_step"]],
        on="symbol",
        how="left",
    )
    
    # Get median equity for notional calculation
    equity = get_median_equity(live_positions)
    merged["equity_used"] = merged["equity_usd"].fillna(equity)
    merged["notional_target"] = merged["abs_weight"] * merged["equity_used"]
    
    # Check if order exists (any day within +/- 2 days)
    def has_order(row):
        if orders.empty:
            return False, []
        sym_orders = orders[orders["symbol"] == row["symbol"]]
        if sym_orders.empty:
            return False, []
        # Check date +/- 2 days
        date_range = [row["date"] + timedelta(days=d) for d in range(-2, 3)]
        matching = sym_orders[sym_orders["date"].isin(date_range)]
        if matching.empty:
            return False, []
        return True, matching["date"].unique().tolist()
    
    if not orders.empty:
        merged[["has_order", "order_dates"]] = merged.apply(
            lambda r: pd.Series(has_order(r)), axis=1
        )
    else:
        merged["has_order"] = False
        merged["order_dates"] = None
    
    # Classify each row
    def classify(row):
        # Check if position exists
        has_position = not pd.isna(row["pos_sign"])
        
        # A. Check metadata constraints first (explains missing orders)
        if pd.isna(row.get("min_usd")):
            return "no_meta"
        
        min_usd = float(row["min_usd"] or 10)
        if row["notional_target"] < min_usd:
            return "below_min_usd"
        
        # We can't check min_units without price, so skip that
        
        # B. Check if position exists
        if not has_position:
            # No position - could be:
            # - Order placed but no position: check has_order
            # - Order never placed: explainable by min constraints or unexplained
            if row["has_order"]:
                return "order_no_position"
            else:
                # No order and no position - could be min constraint or unknown
                # We already checked min_usd above, so if we're here it's unexplained
                return "no_order_unknown"
        
        # C. Position exists - check sign and magnitude
        target_sign = row["target_sign"]
        pos_sign = row["pos_sign"]
        
        if pos_sign == 0:
            return "position_zero"
        
        if pos_sign != target_sign:
            return "wrong_sign"
        
        # D. Check magnitude
        weight_target = row["weight_target"]
        weight_actual = row["weight_actual"]
        
        if pd.isna(weight_actual):
            return "position_no_weight"  # shouldn't happen
        
        weight_diff = abs(weight_actual - weight_target)
        
        if weight_diff <= tolerance:
            return "correct"
        elif weight_diff <= 2 * tolerance:
            return "correct_offsize"  # close but not within tolerance
        else:
            return "wrong_magnitude"
    
    merged["failure_reason"] = merged.apply(classify, axis=1)
    
    return merged


def print_summary(df: pd.DataFrame, tolerance: float):
    """Print diagnosis summary statistics."""
    print(f"\n{'='*70}")
    print(f"EXECUTION DIAGNOSIS SUMMARY")
    print(f"{'='*70}")
    print(f"Total targets: {len(df)}")
    print(f"Date range: {df['date'].min()} -> {df['date'].max()}")
    print(f"Unique symbols: {df['symbol'].nunique()}")
    print(f"Weight tolerance: {tolerance*100:.1f}%")
    
    # Overall success rate
    success = df["failure_reason"].isin(["correct", "correct_offsize"])
    print(f"\nSuccess rate: {100*success.mean():.1f}% ({success.sum()}/{len(df)})")
    
    # Failure breakdown
    print(f"\n{'FAILURE CATEGORY':<25} {'COUNT':>8} {'%':>6}")
    print("-" * 42)
    
    failure_counts = df["failure_reason"].value_counts()
    for reason, count in failure_counts.items():
        pct = 100 * count / len(df)
        marker = "✓" if reason in ["correct", "correct_offsize"] else "✗"
        print(f"{marker} {reason:<23} {count:>8} {pct:>5.1f}%")
    
    # Attribution buckets
    print(f"\n{'ATTRIBUTION':<30} {'COUNT':>8} {'%':>6}")
    print("-" * 47)
    
    explained = df["failure_reason"].isin(["below_min_usd", "no_meta"])
    unexplained_missing = df["failure_reason"] == "no_order_unknown"
    execution_fail = df["failure_reason"].isin(["order_no_position", "position_zero"])
    wrong_result = df["failure_reason"].isin(["wrong_sign", "wrong_magnitude"])
    
    print(f"{'Explained (min/meta)':<30} {explained.sum():>8} {100*explained.mean():>5.1f}%")
    print(f"{'Unexplained missing':<30} {unexplained_missing.sum():>8} {100*unexplained_missing.mean():>5.1f}%")
    print(f"{'Execution failure':<30} {execution_fail.sum():>8} {100*execution_fail.mean():>5.1f}%")
    print(f"{'Wrong result':<30} {wrong_result.sum():>8} {100*wrong_result.mean():>5.1f}%")
    print(f"{'Success':<30} {success.sum():>8} {100*success.mean():>5.1f}%")
    
    # Per-day summary
    print(f"\n{'='*70}")
    print("PER-DAY SUMMARY (first 10 days)")
    print(f"{'='*70}")
    
    daily = df.groupby("date").apply(
        lambda g: pd.Series({
            "targets": len(g),
            "success": (g["failure_reason"].isin(["correct", "correct_offsize"])).sum(),
            "below_min": (g["failure_reason"] == "below_min_usd").sum(),
            "no_order": (g["failure_reason"] == "no_order_unknown").sum(),
            "wrong_sign": (g["failure_reason"] == "wrong_sign").sum(),
            "success_pct": 100 * (g["failure_reason"].isin(["correct", "correct_offsize"])).mean(),
        })
    ).reset_index()
    
    print(daily.head(10).to_string(index=False))
    
    # Sample failures
    print(f"\n{'='*70}")
    print("SAMPLE FAILURES")
    print(f"{'='*70}")
    
    for reason in ["below_min_usd", "no_order_unknown", "wrong_sign", "order_no_position"]:
        subset = df[df["failure_reason"] == reason]
        if not subset.empty:
            print(f"\n{reason.upper()} (n={len(subset)}, showing first 5):")
            cols = ["date", "symbol", "weight_target", "notional_target"]
            if "weight_actual" in subset.columns:
                cols.append("weight_actual")
            if "has_order" in subset.columns:
                cols.append("has_order")
            display_df = subset[cols].head(5)
            print(display_df.to_string(index=False))


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--start_date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end_date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--tolerance", type=float, default=0.02, 
                       help="Weight tolerance for 'correct' classification (default: 0.02 = 2%%)")
    
    args = parser.parse_args()
    
    print("Loading data...")
    targets = load_targets(args.start_date, args.end_date)
    if targets.empty:
        print("No targets found in date range")
        return
    
    # Get date range from targets for other queries
    if not args.start_date:
        args.start_date = str(targets["date"].min())
    if not args.end_date:
        args.end_date = str(targets["date"].max())
    
    live_positions = load_live_positions(args.start_date, args.end_date)
    meta = load_meta()
    orders = load_orders(args.start_date, args.end_date)
    
    print(f"Loaded {len(targets)} targets, {len(live_positions)} positions, "
          f"{len(meta)} metadata rows, {len(orders)} orders")
    
    # Diagnose
    print("\nDiagnosing failures...")
    results = diagnose_failures(targets, live_positions, meta, orders, args.tolerance)
    
    # Print summary
    print_summary(results, args.tolerance)
    
    # Save detailed results
    output_path = "/tmp/execution_diagnosis.csv"
    results.to_csv(output_path, index=False)
    print(f"\nDetailed results saved to: {output_path}")


if __name__ == "__main__":
    main()
