#!/usr/bin/env python3
"""
Align target weights (positions_jianan_v6) with actual allocations (live_positions.current)

Creates a time series full outer joined by (date, symbol) with configurable offset.

The offset parameter tests different execution assumptions:
- offset=0: target on date D should match actual on date D (same-day execution)
- offset=1: target on date D should match actual on date D+1 (Lawrence's assumption)
- offset=2: target on date D should match actual on date D+2 (correct T+2 execution)

Output columns:
- date: the target signal date (from positions_jianan_v6)
- symbol: asset symbol
- target_weight_raw: raw weight from positions_jianan_v6 (can be missing/NaN)
- target_weight_pct: normalized weight (sum to 1 on long side, 1 on short side)
- actual_weight_raw: raw weight from live_positions (usd/equity)
- actual_weight_pct: normalized actual weight (sum to 1 on long/short side)
- actual_date: the date in live_positions.target_date (should be date + offset)
- has_target: boolean, True if in positions_jianan_v6
- has_actual: boolean, True if in live_positions.current
- weight_diff: actual_weight_pct - target_weight_pct (for matched positions)

Usage:
    python align_target_actual.py --offset 2 --start_date 2025-09-01 --end_date 2025-12-07
    python align_target_actual.py --offset 1 --output aligned_weights_t1.csv
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


def load_target_weights(start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
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
        SELECT date, symbol, weight
        FROM (
            SELECT date, symbol, weight, inserted_at
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
    return df


def load_actual_positions(start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
    """
    Load actual positions from live_positions (kind='current').
    Takes last row per (target_date, symbol) ordered by ts.
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
        SELECT target_date, symbol, qty, usd, equity_usd
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
    
    # Compute raw weight (can be positive or negative)
    df["weight"] = df["usd"] / df["equity_usd"]
    
    return df


def normalize_weights(df: pd.DataFrame, weight_col: str = "weight") -> pd.Series:
    """
    Normalize weights so longs sum to 1 and shorts sum to 1 (in absolute value).
    
    Returns:
        Series of normalized weights (same index as df)
    """
    if df.empty:
        return pd.Series(dtype=float)
    
    # Separate long and short
    long_mask = df[weight_col] > 0
    short_mask = df[weight_col] < 0
    
    # Calculate normalization factors
    long_sum = df.loc[long_mask, weight_col].sum()
    short_sum = df.loc[short_mask, weight_col].abs().sum()
    
    # Normalize
    normalized = pd.Series(index=df.index, dtype=float)
    
    if long_sum > 0:
        normalized.loc[long_mask] = df.loc[long_mask, weight_col] / long_sum
    
    if short_sum > 0:
        normalized.loc[short_mask] = df.loc[short_mask, weight_col] / short_sum
    
    return normalized


def align_weights(targets: pd.DataFrame, actuals: pd.DataFrame, offset: int = 2) -> pd.DataFrame:
    """
    Align target weights with actual positions using specified offset.
    
    Parameters:
    -----------
    targets : DataFrame
        From positions_jianan_v6 with columns: date, symbol, weight
    actuals : DataFrame
        From live_positions with columns: target_date, symbol, weight
    offset : int
        Number of days to shift: actual_date = target_date + offset
    
    Returns:
    --------
    DataFrame with full outer join on (date, symbol)
    """
    if targets.empty and actuals.empty:
        return pd.DataFrame()
    
    # Add offset to target dates to get expected actual date
    targets = targets.copy()
    targets["actual_date"] = targets["date"].apply(lambda d: d + timedelta(days=offset))
    
    # Prepare target data
    targets_prep = targets[["date", "symbol", "weight", "actual_date"]].rename(
        columns={"weight": "target_weight_raw"}
    )
    
    # Prepare actual data
    actuals_prep = actuals[["target_date", "symbol", "weight"]].rename(
        columns={"target_date": "actual_date", "weight": "actual_weight_raw"}
    )
    
    # Full outer join on (actual_date, symbol)
    merged = targets_prep.merge(
        actuals_prep,
        on=["actual_date", "symbol"],
        how="outer",
        indicator=True
    )
    
    # For targets that didn't match, we need to backfill the date
    # If actual_date exists but date is missing, calculate date = actual_date - offset
    merged["date"] = merged["date"].fillna(
        merged["actual_date"].apply(lambda d: d - timedelta(days=offset) if pd.notna(d) else pd.NaT)
    )
    
    # Create flags
    merged["has_target"] = merged["_merge"].isin(["both", "left_only"])
    merged["has_actual"] = merged["_merge"].isin(["both", "right_only"])
    
    # Fill missing weights with 0 for actual (but keep NaN for target to distinguish "not in model")
    merged["actual_weight_raw"] = merged["actual_weight_raw"].fillna(0.0)
    
    # Normalize weights per date
    # For targets: normalize within each target date
    # For actuals: normalize within each actual date
    
    normalized_targets = []
    normalized_actuals = []
    
    for date in merged["date"].dropna().unique():
        date_mask = merged["date"] == date
        date_data = merged[date_mask].copy()
        
        # Normalize target weights for this date
        target_subset = date_data[date_data["target_weight_raw"].notna()].copy()
        if not target_subset.empty:
            target_norm = normalize_weights(target_subset, "target_weight_raw")
            normalized_targets.append(pd.DataFrame({
                "date": date,
                "symbol": target_subset["symbol"],
                "target_weight_pct": target_norm
            }))
    
    for actual_date in merged["actual_date"].dropna().unique():
        actual_mask = merged["actual_date"] == actual_date
        actual_data = merged[actual_mask].copy()
        
        # Normalize actual weights for this date
        actual_subset = actual_data[actual_data["actual_weight_raw"] != 0].copy()
        if not actual_subset.empty:
            actual_norm = normalize_weights(actual_subset, "actual_weight_raw")
            normalized_actuals.append(pd.DataFrame({
                "actual_date": actual_date,
                "symbol": actual_subset["symbol"],
                "actual_weight_pct": actual_norm
            }))
    
    # Merge normalized weights back
    if normalized_targets:
        target_norm_df = pd.concat(normalized_targets, ignore_index=True)
        merged = merged.merge(target_norm_df, on=["date", "symbol"], how="left")
    else:
        merged["target_weight_pct"] = np.nan
    
    if normalized_actuals:
        actual_norm_df = pd.concat(normalized_actuals, ignore_index=True)
        merged = merged.merge(actual_norm_df, on=["actual_date", "symbol"], how="left")
    else:
        merged["actual_weight_pct"] = np.nan
    
    # Calculate difference (for positions that exist in both)
    merged["weight_diff"] = merged["actual_weight_pct"] - merged["target_weight_pct"]
    
    # Clean up
    merged = merged.drop(columns=["_merge"])
    
    # Sort by date, symbol
    merged = merged.sort_values(["date", "symbol"]).reset_index(drop=True)
    
    return merged


def print_summary(df: pd.DataFrame, offset: int):
    """Print summary statistics of the alignment."""
    print(f"\n{'='*80}")
    print(f"ALIGNMENT SUMMARY (offset={offset})")
    print(f"{'='*80}")
    
    if df.empty:
        print("No data to analyze")
        return
    
    print(f"\nTotal rows: {len(df)}")
    print(f"Date range: {df['date'].min()} → {df['date'].max()}")
    print(f"Unique symbols: {df['symbol'].nunique()}")
    
    # Count by category
    both = df["has_target"] & df["has_actual"]
    target_only = df["has_target"] & ~df["has_actual"]
    actual_only = ~df["has_target"] & df["has_actual"]
    
    print(f"\n{'Category':<30} {'Count':>10} {'%':>8}")
    print("-"*50)
    print(f"{'Both target & actual':<30} {both.sum():>10} {100*both.mean():>7.1f}%")
    print(f"{'Target only (not executed)':<30} {target_only.sum():>10} {100*target_only.mean():>7.1f}%")
    print(f"{'Actual only (not in model)':<30} {actual_only.sum():>10} {100*actual_only.mean():>7.1f}%")
    
    # Weight statistics for matched positions
    matched = df[both].copy()
    if not matched.empty:
        print(f"\n{'MATCHED POSITIONS STATISTICS'}")
        print("-"*50)
        print(f"Count: {len(matched)}")
        print(f"Mean absolute weight diff: {matched['weight_diff'].abs().mean():.4f}")
        print(f"Median absolute weight diff: {matched['weight_diff'].abs().median():.4f}")
        print(f"Max weight diff: {matched['weight_diff'].abs().max():.4f}")
        
        # Check sign agreement
        target_sign = np.sign(matched["target_weight_pct"])
        actual_sign = np.sign(matched["actual_weight_pct"])
        sign_match = (target_sign == actual_sign) | (matched["actual_weight_pct"] == 0)
        
        print(f"\nSign agreement: {100*sign_match.mean():.1f}% ({sign_match.sum()}/{len(matched)})")
        print(f"Wrong sign: {100*(~sign_match).mean():.1f}% ({(~sign_match).sum()}/{len(matched)})")
    
    # Per-day summary
    print(f"\n{'PER-DAY SUMMARY (first 10 days)'}")
    print("-"*50)
    
    daily = df.groupby("date").agg({
        "symbol": "count",
        "has_target": "sum",
        "has_actual": "sum",
    }).rename(columns={"symbol": "total", "has_target": "targets", "has_actual": "actuals"})
    daily["both"] = df[both].groupby("date").size()
    daily["both"] = daily["both"].fillna(0).astype(int)
    
    print(daily.head(10).to_string())
    
    # Check normalization
    print(f"\n{'NORMALIZATION CHECK'}")
    print("-"*50)
    
    # Target side sums per date
    target_check = df[df["target_weight_pct"].notna()].groupby("date").apply(
        lambda g: pd.Series({
            "long_sum": g[g["target_weight_pct"] > 0]["target_weight_pct"].sum(),
            "short_sum": g[g["target_weight_pct"] < 0]["target_weight_pct"].abs().sum(),
        })
    )
    
    if not target_check.empty:
        print(f"\nTarget weights (should sum to ~1.0):")
        print(f"  Long side:  mean={target_check['long_sum'].mean():.4f}, "
              f"std={target_check['long_sum'].std():.4f}, "
              f"range=[{target_check['long_sum'].min():.4f}, {target_check['long_sum'].max():.4f}]")
        print(f"  Short side: mean={target_check['short_sum'].mean():.4f}, "
              f"std={target_check['short_sum'].std():.4f}, "
              f"range=[{target_check['short_sum'].min():.4f}, {target_check['short_sum'].max():.4f}]")
    
    # Actual side sums per date
    actual_check = df[df["actual_weight_pct"].notna()].groupby("actual_date").apply(
        lambda g: pd.Series({
            "long_sum": g[g["actual_weight_pct"] > 0]["actual_weight_pct"].sum(),
            "short_sum": g[g["actual_weight_pct"] < 0]["actual_weight_pct"].abs().sum(),
        })
    )
    
    if not actual_check.empty:
        print(f"\nActual weights (should sum to ~1.0):")
        print(f"  Long side:  mean={actual_check['long_sum'].mean():.4f}, "
              f"std={actual_check['long_sum'].std():.4f}, "
              f"range=[{actual_check['long_sum'].min():.4f}, {actual_check['long_sum'].max():.4f}]")
        print(f"  Short side: mean={actual_check['short_sum'].mean():.4f}, "
              f"std={actual_check['short_sum'].std():.4f}, "
              f"range=[{actual_check['short_sum'].min():.4f}, {actual_check['short_sum'].max():.4f}]")


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--offset", type=int, default=2,
                       help="Execution offset: actual_date = target_date + offset (default: 2 for T+2)")
    parser.add_argument("--start_date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end_date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--output", default="/tmp/aligned_weights.csv",
                       help="Output CSV file path (default: /tmp/aligned_weights.csv)")
    
    args = parser.parse_args()
    
    print(f"Loading target weights from positions_jianan_v6...")
    targets = load_target_weights(args.start_date, args.end_date)
    
    if targets.empty:
        print("No target weights found")
        return
    
    # Adjust date range for actuals based on offset
    actual_start = None
    actual_end = None
    
    if args.start_date:
        actual_start = str(pd.to_datetime(args.start_date).date() + timedelta(days=args.offset))
    if args.end_date:
        actual_end = str(pd.to_datetime(args.end_date).date() + timedelta(days=args.offset))
    
    print(f"Loading actual positions from live_positions (with offset={args.offset})...")
    actuals = load_actual_positions(actual_start, actual_end)
    
    print(f"Loaded {len(targets)} target weights, {len(actuals)} actual positions")
    
    print(f"\nAligning with offset={args.offset}...")
    aligned = align_weights(targets, actuals, args.offset)
    
    # Save to CSV
    aligned.to_csv(args.output, index=False)
    print(f"\nSaved aligned weights to: {args.output}")
    
    # Print summary
    print_summary(aligned, args.offset)
    
    # Sample output
    print(f"\n{'='*80}")
    print("SAMPLE OUTPUT (first 20 rows)")
    print(f"{'='*80}")
    cols_to_show = ["date", "symbol", "target_weight_pct", "actual_weight_pct", 
                    "weight_diff", "has_target", "has_actual"]
    print(aligned[cols_to_show].head(20).to_string(index=False))


if __name__ == "__main__":
    main()
