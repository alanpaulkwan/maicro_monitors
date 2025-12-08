#!/usr/bin/env python3
"""
Lawrence Execution Diagnosis - Corrected Offset Analysis

KEY INSIGHT:
- ts = the day Lawrence executed/held positions
- Lawrence used: signal_date = ts - 1 (WRONG - T+1)
- Should have used: signal_date = ts - 2 (CORRECT - T+2)

This script:
1. Aligns positions_jianan_v6 to live_positions using ts (holdings date) as anchor
2. Computes tracking error for CORRECT alignment (ts - 2 = signal date)
3. Computes tracking error for WRONG alignment (ts - 1 = signal date)
4. Shows the cost of the offset bug

Analysis scope: Only days where live_positions has data

Thresholds:
- Direction: sign(target) vs sign(actual)
- Magnitude: >2% weight difference is "big gap"

Usage:
    python diagnose_offset_error.py
"""

import os
import sys
from datetime import timedelta

import pandas as pd
import numpy as np

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(REPO_ROOT)
from modules.clickhouse_client import query_df  # type: ignore

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

# Threshold for "big" weight difference
WEIGHT_THRESHOLD = 0.02  # 2% of portfolio


def load_jianan_signals() -> pd.DataFrame:
    """Load target weights from positions_jianan_v6."""
    sql = """
        SELECT date, symbol, weight
        FROM (
            SELECT date, symbol, weight, inserted_at
            FROM maicro_logs.positions_jianan_v6
            WHERE weight IS NOT NULL AND isFinite(weight) AND weight != 0
              AND pred_ret IS NOT NULL AND isFinite(pred_ret)
            ORDER BY date, symbol, inserted_at
            LIMIT 1 BY date, symbol
        )
    """
    df = query_df(sql)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["symbol"] = df["symbol"].str.upper().str.strip()
    return df


def load_live_positions() -> pd.DataFrame:
    """
    Load actual positions from live_positions (kind='current').
    Key: use ts (holdings date) as the anchor, not target_date.
    """
    sql = """
        SELECT 
            toDate(ts) as holdings_date,
            target_date,
            symbol, 
            qty, 
            usd, 
            equity_usd
        FROM (
            SELECT *, row_number() OVER(PARTITION BY toDate(ts), symbol, kind ORDER BY ts DESC) rn
            FROM maicro_logs.live_positions
            WHERE kind = 'current'
        )
        WHERE rn = 1
    """
    df = query_df(sql)
    df["holdings_date"] = pd.to_datetime(df["holdings_date"]).dt.date
    df["target_date"] = pd.to_datetime(df["target_date"]).dt.date
    df["symbol"] = df["symbol"].str.upper().str.strip()
    df["weight"] = df["usd"] / df["equity_usd"]
    return df


def align_and_diagnose(signals: pd.DataFrame, positions: pd.DataFrame, offset: int, label: str) -> pd.DataFrame:
    """
    Align signals to positions using: signal_date = holdings_date - offset
    
    Parameters:
    -----------
    signals : DataFrame with (date, symbol, weight) from jianan
    positions : DataFrame with (holdings_date, symbol, weight) from live_positions
    offset : int, number of days to subtract from holdings_date to get signal_date
    label : str, label for this alignment (e.g., "correct_t2" or "wrong_t1")
    
    Returns:
    --------
    DataFrame with aligned data and diagnosis
    """
    # Compute expected signal date for each position
    positions = positions.copy()
    positions["expected_signal_date"] = positions["holdings_date"].apply(
        lambda d: d - timedelta(days=offset)
    )
    
    # Join signals to positions
    merged = positions.merge(
        signals.rename(columns={"date": "signal_date", "weight": "target_weight"}),
        left_on=["expected_signal_date", "symbol"],
        right_on=["signal_date", "symbol"],
        how="outer",
        indicator=True
    )
    
    # Fill in missing dates
    merged["holdings_date"] = merged["holdings_date"].fillna(
        merged["signal_date"].apply(lambda d: d + timedelta(days=offset) if pd.notna(d) else None)
    )
    merged["expected_signal_date"] = merged["expected_signal_date"].fillna(merged["signal_date"])
    
    # Rename actual weight
    merged = merged.rename(columns={"weight": "actual_weight"})
    
    # Flags
    merged["has_target"] = merged["_merge"].isin(["both", "right_only"])
    merged["has_actual"] = merged["_merge"].isin(["both", "left_only"])
    merged["matched"] = merged["_merge"] == "both"
    
    # Fill missing weights
    merged["actual_weight"] = merged["actual_weight"].fillna(0.0)
    merged["target_weight"] = merged["target_weight"].fillna(0.0)
    
    # Direction analysis
    merged["target_sign"] = np.sign(merged["target_weight"])
    merged["actual_sign"] = np.sign(merged["actual_weight"])
    merged["sign_match"] = (merged["target_sign"] == merged["actual_sign"]) | (merged["target_weight"] == 0)
    
    # Magnitude analysis
    merged["weight_diff"] = merged["actual_weight"] - merged["target_weight"]
    merged["abs_weight_diff"] = merged["weight_diff"].abs()
    merged["big_gap"] = merged["abs_weight_diff"] > WEIGHT_THRESHOLD
    
    # Tracking error contribution (unsigned)
    merged["te_contribution"] = merged["abs_weight_diff"]
    
    # Label
    merged["alignment"] = label
    
    return merged.drop(columns=["_merge"])


def compute_daily_stats(df: pd.DataFrame) -> pd.DataFrame:
    """Compute per-day statistics."""
    # Group by holdings_date
    daily = df.groupby("holdings_date").agg({
        "symbol": "count",
        "has_target": "sum",
        "has_actual": "sum",
        "matched": "sum",
        "sign_match": lambda x: x[df.loc[x.index, "matched"]].sum() if df.loc[x.index, "matched"].any() else 0,
        "big_gap": "sum",
        "te_contribution": "sum",
        "abs_weight_diff": "mean",
    }).rename(columns={
        "symbol": "total_rows",
        "has_target": "n_targets",
        "has_actual": "n_actuals", 
        "matched": "n_matched",
        "sign_match": "n_sign_correct",
        "big_gap": "n_big_gaps",
        "te_contribution": "daily_te",
        "abs_weight_diff": "mean_abs_diff",
    })
    
    # Compute rates
    daily["match_rate"] = daily["n_matched"] / daily["n_targets"]
    daily["sign_correct_rate"] = daily["n_sign_correct"] / daily["n_matched"].replace(0, np.nan)
    
    return daily.reset_index()


def print_summary(correct_df: pd.DataFrame, wrong_df: pd.DataFrame):
    """Print comprehensive comparison of correct vs wrong alignment."""
    
    print("=" * 100)
    print("LAWRENCE OFFSET ERROR DIAGNOSIS")
    print("=" * 100)
    print(f"""
KEY FINDING:
- Lawrence used: signal_date = holdings_date - 1 (T+1, WRONG)
- Should have used: signal_date = holdings_date - 2 (T+2, CORRECT)

Analysis scope: Days with live_positions data only
Magnitude threshold: {WEIGHT_THRESHOLD*100:.0f}% weight difference = "big gap"
""")
    
    # Filter to matched rows for fair comparison
    correct_matched = correct_df[correct_df["matched"]]
    wrong_matched = wrong_df[wrong_df["matched"]]
    
    print("=" * 100)
    print("OVERALL COMPARISON")
    print("=" * 100)
    
    metrics = []
    for label, df, matched in [
        ("CORRECT (T+2)", correct_df, correct_matched),
        ("WRONG (T+1)", wrong_df, wrong_matched),
    ]:
        n_total = len(df[df["has_target"]])
        n_matched = len(matched)
        n_sign_correct = matched["sign_match"].sum()
        n_big_gaps = matched["big_gap"].sum()
        total_te = matched["abs_weight_diff"].sum()
        mean_te = matched["abs_weight_diff"].mean()
        
        metrics.append({
            "Alignment": label,
            "Targets": n_total,
            "Matched": n_matched,
            "Match%": f"{100*n_matched/n_total:.1f}%",
            "SignCorrect": n_sign_correct,
            "SignCorrect%": f"{100*n_sign_correct/n_matched:.1f}%" if n_matched > 0 else "N/A",
            "BigGaps": n_big_gaps,
            "BigGaps%": f"{100*n_big_gaps/n_matched:.1f}%" if n_matched > 0 else "N/A",
            "TotalTE": f"{total_te:.4f}",
            "MeanTE": f"{mean_te:.4f}",
        })
    
    metrics_df = pd.DataFrame(metrics)
    print(metrics_df.to_string(index=False))
    
    # Compute the COST of the offset error
    print("\n" + "=" * 100)
    print("COST OF OFFSET ERROR")
    print("=" * 100)
    
    correct_te = correct_matched["abs_weight_diff"].sum()
    wrong_te = wrong_matched["abs_weight_diff"].sum()
    
    correct_sign_rate = correct_matched["sign_match"].mean() if len(correct_matched) > 0 else 0
    wrong_sign_rate = wrong_matched["sign_match"].mean() if len(wrong_matched) > 0 else 0
    
    print(f"""
Tracking Error:
  - Correct (T+2): {correct_te:.4f} total, {correct_matched['abs_weight_diff'].mean():.4f} mean
  - Wrong (T+1):   {wrong_te:.4f} total, {wrong_matched['abs_weight_diff'].mean():.4f} mean
  - Difference:    {wrong_te - correct_te:.4f} extra TE from wrong offset

Sign Accuracy:
  - Correct (T+2): {100*correct_sign_rate:.1f}%
  - Wrong (T+1):   {100*wrong_sign_rate:.1f}%
  - Difference:    {100*(correct_sign_rate - wrong_sign_rate):.1f}% worse with wrong offset

Big Gaps (>{WEIGHT_THRESHOLD*100:.0f}% weight diff):
  - Correct (T+2): {correct_matched['big_gap'].sum()} positions
  - Wrong (T+1):   {wrong_matched['big_gap'].sum()} positions
""")
    
    # Direction breakdown
    print("=" * 100)
    print("DIRECTION ANALYSIS (matched positions only)")
    print("=" * 100)
    
    for label, matched in [("CORRECT (T+2)", correct_matched), ("WRONG (T+1)", wrong_matched)]:
        n = len(matched)
        sign_correct = matched["sign_match"].sum()
        sign_wrong = n - sign_correct
        
        print(f"\n{label}:")
        print(f"  Total matched: {n}")
        print(f"  Sign correct:  {sign_correct} ({100*sign_correct/n:.1f}%)")
        print(f"  Sign wrong:    {sign_wrong} ({100*sign_wrong/n:.1f}%)")
        
        if sign_wrong > 0:
            wrong_signs = matched[~matched["sign_match"]]
            print(f"\n  Symbols with most wrong signs:")
            print(wrong_signs["symbol"].value_counts().head(10).to_string())
    
    # Magnitude breakdown
    print("\n" + "=" * 100)
    print(f"MAGNITUDE ANALYSIS (>{WEIGHT_THRESHOLD*100:.0f}% = big gap)")
    print("=" * 100)
    
    for label, matched in [("CORRECT (T+2)", correct_matched), ("WRONG (T+1)", wrong_matched)]:
        n = len(matched)
        within_2pct = (matched["abs_weight_diff"] <= 0.02).sum()
        within_5pct = ((matched["abs_weight_diff"] > 0.02) & (matched["abs_weight_diff"] <= 0.05)).sum()
        within_10pct = ((matched["abs_weight_diff"] > 0.05) & (matched["abs_weight_diff"] <= 0.10)).sum()
        over_10pct = (matched["abs_weight_diff"] > 0.10).sum()
        
        print(f"\n{label}:")
        print(f"  ≤2% diff (good):   {within_2pct} ({100*within_2pct/n:.1f}%)")
        print(f"  2-5% diff:         {within_5pct} ({100*within_5pct/n:.1f}%)")
        print(f"  5-10% diff:        {within_10pct} ({100*within_10pct/n:.1f}%)")
        print(f"  >10% diff (bad):   {over_10pct} ({100*over_10pct/n:.1f}%)")
    
    # Per-day tracking error
    print("\n" + "=" * 100)
    print("DAILY TRACKING ERROR (first 15 days)")
    print("=" * 100)
    
    correct_daily = compute_daily_stats(correct_df)
    wrong_daily = compute_daily_stats(wrong_df)
    
    # Merge for comparison
    daily_compare = correct_daily[["holdings_date", "daily_te", "n_matched", "n_sign_correct"]].merge(
        wrong_daily[["holdings_date", "daily_te", "n_matched", "n_sign_correct"]],
        on="holdings_date",
        suffixes=("_correct", "_wrong")
    )
    daily_compare["te_diff"] = daily_compare["daily_te_wrong"] - daily_compare["daily_te_correct"]
    
    print(daily_compare.head(15).to_string(index=False))
    
    return correct_df, wrong_df, daily_compare


def main():
    print("Loading data...")
    signals = load_jianan_signals()
    positions = load_live_positions()
    
    print(f"Loaded {len(signals)} signals, {len(positions)} positions")
    print(f"Holdings date range: {positions['holdings_date'].min()} → {positions['holdings_date'].max()}")
    
    print("\nAligning with CORRECT offset (T+2: signal = holdings - 2)...")
    correct_df = align_and_diagnose(signals, positions, offset=2, label="correct_t2")
    
    print("Aligning with WRONG offset (T+1: signal = holdings - 1)...")
    wrong_df = align_and_diagnose(signals, positions, offset=1, label="wrong_t1")
    
    # Print summary
    correct_df, wrong_df, daily_compare = print_summary(correct_df, wrong_df)
    
    # Save outputs
    print("\n" + "=" * 100)
    print("SAVING OUTPUTS")
    print("=" * 100)
    
    correct_df.to_csv(os.path.join(OUTPUT_DIR, "alignment_correct_t2.csv"), index=False)
    wrong_df.to_csv(os.path.join(OUTPUT_DIR, "alignment_wrong_t1.csv"), index=False)
    daily_compare.to_csv(os.path.join(OUTPUT_DIR, "daily_te_comparison.csv"), index=False)
    
    print(f"Saved: alignment_correct_t2.csv")
    print(f"Saved: alignment_wrong_t1.csv")
    print(f"Saved: daily_te_comparison.csv")
    
    # Final summary
    print("\n" + "=" * 100)
    print("EXECUTIVE SUMMARY")
    print("=" * 100)
    
    correct_matched = correct_df[correct_df["matched"]]
    wrong_matched = wrong_df[wrong_df["matched"]]
    
    print(f"""
OFFSET ERROR IMPACT:
- Lawrence used T+1 (signal = yesterday) instead of T+2 (signal = 2 days ago)

TRACKING ERROR:
- Correct (T+2): {correct_matched['abs_weight_diff'].sum():.4f} total TE
- Wrong (T+1):   {wrong_matched['abs_weight_diff'].sum():.4f} total TE
- Extra TE from offset error: {wrong_matched['abs_weight_diff'].sum() - correct_matched['abs_weight_diff'].sum():.4f}

DIRECTION ACCURACY:
- Correct (T+2): {100*correct_matched['sign_match'].mean():.1f}% sign-correct
- Wrong (T+1):   {100*wrong_matched['sign_match'].mean():.1f}% sign-correct

MAGNITUDE (positions with >2% weight error):
- Correct (T+2): {correct_matched['big_gap'].sum()} positions ({100*correct_matched['big_gap'].mean():.1f}%)
- Wrong (T+1):   {wrong_matched['big_gap'].sum()} positions ({100*wrong_matched['big_gap'].mean():.1f}%)
""")


if __name__ == "__main__":
    main()
