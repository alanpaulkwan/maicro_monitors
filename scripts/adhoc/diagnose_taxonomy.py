#!/usr/bin/env python3
"""
Comprehensive Taxonomic Diagnosis of Lawrence's Execution Issues

This script analyzes the aligned_t0/t1/t2.csv files and produces a complete
taxonomic breakdown of WHERE and WHY PnL errors occur.

TAXONOMY OF FAILURES:
=====================

STAGE 1: DATA AVAILABILITY (pre-execution)
------------------------------------------
1.1 no_live_data: Target date has no live_positions data (logging gap)
1.2 live_data_available: Target date has live_positions data

STAGE 2: SYMBOL COVERAGE (for dates with live data)
---------------------------------------------------
2.1 both_matched: Symbol exists in both target and actual
2.2 target_only: Symbol in model but not executed (MISSING TRADE)
2.3 actual_only: Symbol executed but not in model (EXTRA TRADE)

STAGE 3: DIRECTION ANALYSIS (for matched symbols)
-------------------------------------------------
3.1 sign_correct: Target and actual have same sign
3.2 sign_wrong: Target and actual have opposite signs (WRONG DIRECTION)
3.3 actual_zero: Target exists but actual is zero (FLAT when should be positioned)

STAGE 4: MAGNITUDE ANALYSIS (for sign-correct matches)
------------------------------------------------------
4.1 within_5pct: Weight diff <= 5% (GOOD EXECUTION)
4.2 within_10pct: Weight diff 5-10% (ACCEPTABLE)
4.3 within_20pct: Weight diff 10-20% (OFF-SIZE)
4.4 over_20pct: Weight diff > 20% (BAD EXECUTION)

ROOT CAUSE CANDIDATES (for target_only failures):
-------------------------------------------------
- below_min_notional: Target notional < $10 min_usd
- no_metadata: Symbol not in hl_meta
- weekend_holiday: Date is weekend/holiday (no trading)
- logging_gap: No live_positions on this date
- unknown: None of the above explain it

Usage:
    python diagnose_taxonomy.py
"""

import os
import sys
from datetime import timedelta
from typing import Dict, List, Tuple

import pandas as pd
import numpy as np

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(REPO_ROOT)
from modules.clickhouse_client import query_df  # type: ignore

DIAGNOSIS_DIR = os.path.join(os.path.dirname(__file__), "diagnosis_lawrence_trades")


def load_aligned_data() -> Dict[int, pd.DataFrame]:
    """Load all aligned CSV files."""
    data = {}
    for offset in [0, 1, 2]:
        path = os.path.join(DIAGNOSIS_DIR, f"aligned_t{offset}.csv")
        if os.path.exists(path):
            df = pd.read_csv(path)
            df["date"] = pd.to_datetime(df["date"]).dt.date
            if "actual_date" in df.columns:
                df["actual_date"] = pd.to_datetime(df["actual_date"]).dt.date
            data[offset] = df
            print(f"Loaded offset={offset}: {len(df)} rows")
    return data


def load_live_dates() -> set:
    """Get all dates that have live_positions data."""
    df = query_df("""
        SELECT DISTINCT target_date
        FROM maicro_logs.live_positions
        WHERE kind = 'current'
    """)
    return set(pd.to_datetime(df["target_date"]).dt.date)


def load_metadata() -> pd.DataFrame:
    """Load hl_meta for min_usd checks."""
    df = query_df("""
        SELECT symbol, min_usd, min_units, size_step
        FROM maicro_logs.hl_meta
    """)
    df["symbol"] = df["symbol"].str.upper().str.strip()
    return df.drop_duplicates(subset=["symbol"], keep="first").set_index("symbol")


def diagnose_single_offset(df: pd.DataFrame, offset: int, live_dates: set, meta: pd.DataFrame) -> pd.DataFrame:
    """
    Add diagnosis columns to aligned dataframe.
    """
    df = df.copy()
    
    # Stage 1: Data availability
    df["actual_date_expected"] = df["date"].apply(lambda d: d + timedelta(days=offset) if pd.notna(d) else None)
    df["has_live_data"] = df["actual_date_expected"].apply(lambda d: d in live_dates if d else False)
    
    # Stage 2: Symbol coverage
    def get_coverage_status(row):
        if row["has_target"] and row["has_actual"]:
            return "both_matched"
        elif row["has_target"] and not row["has_actual"]:
            return "target_only"
        elif not row["has_target"] and row["has_actual"]:
            return "actual_only"
        else:
            return "neither"  # shouldn't happen
    
    df["coverage_status"] = df.apply(get_coverage_status, axis=1)
    
    # Stage 3: Direction analysis (for matched)
    def get_direction_status(row):
        if row["coverage_status"] != "both_matched":
            return None
        
        target_pct = row["target_weight_pct"]
        actual_pct = row["actual_weight_pct"]
        
        if pd.isna(target_pct) or pd.isna(actual_pct):
            return "missing_weight"
        
        target_sign = np.sign(target_pct)
        actual_sign = np.sign(actual_pct)
        
        if actual_sign == 0:
            return "actual_zero"
        elif target_sign == actual_sign:
            return "sign_correct"
        else:
            return "sign_wrong"
    
    df["direction_status"] = df.apply(get_direction_status, axis=1)
    
    # Stage 4: Magnitude analysis (for sign-correct)
    def get_magnitude_status(row):
        if row["direction_status"] != "sign_correct":
            return None
        
        diff = abs(row["weight_diff"]) if pd.notna(row["weight_diff"]) else None
        if diff is None:
            return "missing_diff"
        
        if diff <= 0.05:
            return "within_5pct"
        elif diff <= 0.10:
            return "within_10pct"
        elif diff <= 0.20:
            return "within_20pct"
        else:
            return "over_20pct"
    
    df["magnitude_status"] = df.apply(get_magnitude_status, axis=1)
    
    # Root cause for target_only
    def get_root_cause(row):
        if row["coverage_status"] != "target_only":
            return None
        
        # Check if it's a logging gap
        if not row["has_live_data"]:
            return "logging_gap"
        
        # Check weekend
        d = row["date"]
        if d and d.weekday() >= 5:  # Saturday=5, Sunday=6
            return "weekend"
        
        # Check min_usd (rough estimate with $50k equity)
        symbol = row["symbol"]
        if symbol in meta.index:
            min_usd = meta.loc[symbol, "min_usd"]
            if pd.notna(min_usd):
                target_raw = row["target_weight_raw"]
                if pd.notna(target_raw):
                    notional = abs(target_raw) * 50000  # assume $50k
                    if notional < min_usd:
                        return "below_min_notional"
        else:
            return "no_metadata"
        
        return "unknown"
    
    df["root_cause"] = df.apply(get_root_cause, axis=1)
    
    # Final taxonomic category
    def get_taxonomy(row):
        if not row["has_live_data"]:
            return "1_no_live_data"
        
        coverage = row["coverage_status"]
        if coverage == "target_only":
            cause = row["root_cause"] or "unknown"
            return f"2_target_only_{cause}"
        elif coverage == "actual_only":
            return "2_actual_only"
        elif coverage == "both_matched":
            direction = row["direction_status"]
            if direction == "sign_wrong":
                return "3_sign_wrong"
            elif direction == "actual_zero":
                return "3_actual_zero"
            elif direction == "sign_correct":
                magnitude = row["magnitude_status"]
                return f"4_{magnitude}" if magnitude else "4_unknown"
            else:
                return "3_unknown"
        else:
            return "0_error"
    
    df["taxonomy"] = df.apply(get_taxonomy, axis=1)
    
    return df


def print_taxonomy_summary(diagnosed: Dict[int, pd.DataFrame]):
    """Print comprehensive taxonomy breakdown for all offsets."""
    
    print("\n" + "="*100)
    print("COMPREHENSIVE TAXONOMIC BREAKDOWN")
    print("="*100)
    
    # Summary comparison across offsets
    print("\n" + "-"*100)
    print("OFFSET COMPARISON SUMMARY")
    print("-"*100)
    
    summary_rows = []
    for offset, df in diagnosed.items():
        # Only look at dates where live data exists
        live_df = df[df["has_live_data"]]
        
        total = len(live_df)
        both = (live_df["coverage_status"] == "both_matched").sum()
        target_only = (live_df["coverage_status"] == "target_only").sum()
        actual_only = (live_df["coverage_status"] == "actual_only").sum()
        
        sign_correct = (live_df["direction_status"] == "sign_correct").sum()
        sign_wrong = (live_df["direction_status"] == "sign_wrong").sum()
        
        within_5 = (live_df["magnitude_status"] == "within_5pct").sum()
        within_10 = (live_df["magnitude_status"] == "within_10pct").sum()
        
        summary_rows.append({
            "offset": offset,
            "total_with_live_data": total,
            "both_matched": both,
            "both_pct": 100*both/total if total > 0 else 0,
            "target_only": target_only,
            "target_only_pct": 100*target_only/total if total > 0 else 0,
            "sign_correct": sign_correct,
            "sign_correct_pct": 100*sign_correct/both if both > 0 else 0,
            "within_5pct": within_5,
            "good_exec_pct": 100*within_5/both if both > 0 else 0,
        })
    
    summary_df = pd.DataFrame(summary_rows)
    print(summary_df.to_string(index=False))
    
    # Detailed taxonomy for each offset
    for offset, df in diagnosed.items():
        print(f"\n{'='*100}")
        print(f"DETAILED TAXONOMY: OFFSET={offset} (target date + {offset} days = actual date)")
        print(f"{'='*100}")
        
        # Full dataset
        print(f"\n--- FULL DATASET ---")
        print(f"Total rows: {len(df)}")
        print(f"Unique dates: {df['date'].nunique()}")
        print(f"Date range: {df['date'].min()} → {df['date'].max()}")
        
        # Only dates with live data
        live_df = df[df["has_live_data"]]
        print(f"\n--- WITH LIVE DATA ONLY ---")
        print(f"Rows with live data: {len(live_df)}")
        print(f"Dates with live data: {live_df['date'].nunique()}")
        
        # Taxonomy breakdown
        print(f"\n--- TAXONOMY BREAKDOWN ---")
        tax_counts = df["taxonomy"].value_counts().sort_index()
        total = len(df)
        
        print(f"\n{'Category':<40} {'Count':>10} {'%':>8} {'Interpretation':<30}")
        print("-"*100)
        
        interpretations = {
            "1_no_live_data": "No live_positions logged (data gap)",
            "2_target_only_logging_gap": "Target exists, no actual (no live data)",
            "2_target_only_weekend": "Target exists, weekend (no trading)",
            "2_target_only_below_min_notional": "Target too small (< min_usd)",
            "2_target_only_no_metadata": "Symbol not in hl_meta",
            "2_target_only_unknown": "Missing actual, reason unknown",
            "2_actual_only": "Actual exists but not in model",
            "3_sign_wrong": "Both exist but OPPOSITE signs",
            "3_actual_zero": "Target exists, actual is zero",
            "4_within_5pct": "✓ Good execution (≤5% diff)",
            "4_within_10pct": "Acceptable (5-10% diff)",
            "4_within_20pct": "Off-size (10-20% diff)",
            "4_over_20pct": "Bad execution (>20% diff)",
        }
        
        for cat, count in tax_counts.items():
            pct = 100 * count / total
            interp = interpretations.get(cat, "")
            print(f"{cat:<40} {count:>10} {pct:>7.1f}% {interp:<30}")
        
        # Root cause breakdown for target_only
        target_only_df = df[df["coverage_status"] == "target_only"]
        if not target_only_df.empty:
            print(f"\n--- ROOT CAUSE: TARGET_ONLY ({len(target_only_df)} rows) ---")
            root_counts = target_only_df["root_cause"].value_counts()
            for cause, count in root_counts.items():
                pct = 100 * count / len(target_only_df)
                print(f"  {cause:<30} {count:>8} ({pct:>5.1f}%)")
        
        # Sign analysis for matched
        matched_df = df[df["coverage_status"] == "both_matched"]
        if not matched_df.empty:
            print(f"\n--- DIRECTION ANALYSIS: MATCHED ({len(matched_df)} rows) ---")
            dir_counts = matched_df["direction_status"].value_counts()
            for status, count in dir_counts.items():
                pct = 100 * count / len(matched_df)
                print(f"  {status:<30} {count:>8} ({pct:>5.1f}%)")
        
        # Magnitude analysis for sign-correct
        correct_df = matched_df[matched_df["direction_status"] == "sign_correct"]
        if not correct_df.empty:
            print(f"\n--- MAGNITUDE ANALYSIS: SIGN-CORRECT ({len(correct_df)} rows) ---")
            mag_counts = correct_df["magnitude_status"].value_counts()
            for status, count in mag_counts.items():
                pct = 100 * count / len(correct_df)
                print(f"  {status:<30} {count:>8} ({pct:>5.1f}%)")
    
    # Find best offset
    print("\n" + "="*100)
    print("BEST OFFSET DETERMINATION")
    print("="*100)
    
    best_offset = None
    best_score = -1
    
    for offset, df in diagnosed.items():
        live_df = df[df["has_live_data"]]
        matched = (live_df["coverage_status"] == "both_matched").sum()
        sign_correct = (live_df["direction_status"] == "sign_correct").sum()
        
        if matched > 0:
            score = sign_correct / matched
            print(f"Offset={offset}: {matched} matched, {sign_correct} sign-correct ({100*score:.1f}%)")
            if score > best_score:
                best_score = score
                best_offset = offset
    
    print(f"\n→ BEST OFFSET: {best_offset} (highest sign agreement: {100*best_score:.1f}%)")


def export_diagnosis(diagnosed: Dict[int, pd.DataFrame]):
    """Export diagnosed data to CSV files."""
    for offset, df in diagnosed.items():
        path = os.path.join(DIAGNOSIS_DIR, f"diagnosed_t{offset}.csv")
        df.to_csv(path, index=False)
        print(f"Exported: {path}")


def create_executive_summary(diagnosed: Dict[int, pd.DataFrame]) -> str:
    """Create executive summary of findings."""
    
    lines = [
        "="*80,
        "EXECUTIVE SUMMARY: LAWRENCE EXECUTION DIAGNOSIS",
        "="*80,
        "",
    ]
    
    # Use offset=0 as the baseline (appears to be what Lawrence implemented)
    df = diagnosed.get(0, diagnosed.get(1, list(diagnosed.values())[0]))
    live_df = df[df["has_live_data"]]
    
    total_targets = df["has_target"].sum()
    matched = (live_df["coverage_status"] == "both_matched").sum()
    sign_correct = (live_df["direction_status"] == "sign_correct").sum()
    good_exec = (live_df["magnitude_status"] == "within_5pct").sum()
    
    lines.extend([
        f"OVERALL METRICS (using all aligned data):",
        f"  Total targets (from positions_jianan_v6): {total_targets:,}",
        f"  Targets with live data available: {len(live_df):,}",
        f"  Matched (both target & actual): {matched:,} ({100*matched/len(live_df) if len(live_df) > 0 else 0:.1f}%)",
        f"  Sign correct: {sign_correct:,} ({100*sign_correct/matched if matched > 0 else 0:.1f}% of matched)",
        f"  Good execution (≤5% weight diff): {good_exec:,} ({100*good_exec/matched if matched > 0 else 0:.1f}% of matched)",
        "",
        "KEY FINDINGS:",
        "",
    ])
    
    # Find the dominant issues
    tax_counts = df["taxonomy"].value_counts()
    
    issues = []
    if "1_no_live_data" in tax_counts:
        issues.append(f"1. DATA GAPS: {tax_counts['1_no_live_data']:,} targets have no live_positions data (logging stopped)")
    
    target_only_unknown = tax_counts.get("2_target_only_unknown", 0)
    if target_only_unknown > 0:
        issues.append(f"2. UNEXPLAINED MISSING: {target_only_unknown:,} targets should have been traded but weren't")
    
    sign_wrong = tax_counts.get("3_sign_wrong", 0)
    if sign_wrong > 0:
        issues.append(f"3. WRONG DIRECTION: {sign_wrong:,} positions have opposite sign from target")
    
    over_20pct = tax_counts.get("4_over_20pct", 0)
    if over_20pct > 0:
        issues.append(f"4. BAD SIZING: {over_20pct:,} positions are >20% off from target weight")
    
    lines.extend(issues)
    
    lines.extend([
        "",
        "ROOT CAUSES OF MISSING TRADES:",
    ])
    
    target_only_df = df[df["coverage_status"] == "target_only"]
    if not target_only_df.empty:
        root_counts = target_only_df["root_cause"].value_counts()
        for cause, count in root_counts.head(5).items():
            pct = 100 * count / len(target_only_df)
            lines.append(f"  - {cause}: {count:,} ({pct:.1f}%)")
    
    lines.extend([
        "",
        "RECOMMENDATIONS:",
        "  1. Fix logging gaps (Nov 5-30 has no data)",
        "  2. Investigate 'unknown' missing trades",
        "  3. Review sign-wrong cases (might be T+1 vs T+2 offset issue)",
        "  4. Tighten execution to reduce >20% weight deviations",
        "",
        "="*80,
    ])
    
    return "\n".join(lines)


def main():
    print("Loading aligned data...")
    data = load_aligned_data()
    
    if not data:
        print("No aligned data found. Run align_target_actual.py first.")
        return
    
    print("\nLoading live_positions dates...")
    live_dates = load_live_dates()
    print(f"Found {len(live_dates)} dates with live_positions data")
    
    print("\nLoading metadata...")
    meta = load_metadata()
    print(f"Found {len(meta)} symbols in hl_meta")
    
    print("\nDiagnosing each offset...")
    diagnosed = {}
    for offset, df in data.items():
        print(f"  Diagnosing offset={offset}...")
        diagnosed[offset] = diagnose_single_offset(df, offset, live_dates, meta)
    
    # Print comprehensive summary
    print_taxonomy_summary(diagnosed)
    
    # Export diagnosed data
    print("\n" + "="*100)
    print("EXPORTING RESULTS")
    print("="*100)
    export_diagnosis(diagnosed)
    
    # Create executive summary
    summary = create_executive_summary(diagnosed)
    summary_path = os.path.join(DIAGNOSIS_DIR, "EXECUTIVE_SUMMARY.txt")
    with open(summary_path, "w") as f:
        f.write(summary)
    print(f"\nExecutive summary saved to: {summary_path}")
    print("\n" + summary)


if __name__ == "__main__":
    main()
