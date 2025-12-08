#!/usr/bin/env python3
"""
Daily Tracking Error & Email Report.
------------------------------------
Calculates tracking error for multiple lags (T0..T3) and sends a summary email.

Data Sources:
  - Actuals: `maicro_monitors.positions_snapshots` + `account_snapshots`
  - Targets: `maicro_logs.positions_jianan_v6`

Outputs:
  - Inserts into `maicro_monitors.tracking_error_multilag`
  - Sends email via Resend
"""

import os
import sys
from datetime import datetime, timedelta, date
from typing import Optional, Dict, List, Tuple
import pandas as pd
import numpy as np
import requests

# Make repo modules importable
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if REPO_ROOT not in sys.path:
    sys.path.append(REPO_ROOT)

from modules.clickhouse_client import query_df, execute
from config.settings import get_secret

RESEND_API_KEY = get_secret("RESEND_API_KEY")
TO_EMAIL = os.getenv("ALERT_EMAIL", "alanpaulkwan@gmail.com")
FROM_EMAIL = os.getenv("ALERT_FROM_EMAIL", "Maicro Monitors <alerts@resend.dev>")
STRATEGY_ID = "jianan_v6"

def _normalize_weights(df: pd.DataFrame, weight_col: str = "weight") -> pd.Series:
    """
    Standard normalization: assume raw weights are correct target allocations.
    If they are not (e.g. they sum to > 100 or are raw scores), this needs adjustment.
    For now, we trust the model output 'weight'.
    """
    if df.empty or weight_col not in df.columns:
        return pd.Series(index=df.index, dtype=float)
    return df[weight_col].astype(float).fillna(0.0)

def _load_actuals_snapshot(snapshot_date: date) -> Tuple[pd.DataFrame, float]:
    """
    Load the last snapshot of the given date from maicro_monitors.
    Returns (positions_df, equity_usd).
    Uses a single query with join/subquery to ensure timestamp alignment.
    """
    sql = """
    WITH latest_ts AS (
        SELECT max(timestamp) as ts
        FROM maicro_monitors.account_snapshots
        WHERE toDate(timestamp) = %(d)s
    )
    SELECT 
        p.coin as symbol, 
        p.positionValue, 
        p.szi,
        a.accountValue as equity
    FROM maicro_monitors.positions_snapshots p
    JOIN maicro_monitors.account_snapshots a ON p.timestamp = a.timestamp
    WHERE p.timestamp = (SELECT ts FROM latest_ts)
      AND a.timestamp = (SELECT ts FROM latest_ts)
    """
    df = query_df(sql, params={"d": snapshot_date})
    
    if df.empty:
        return pd.DataFrame(), 0.0
        
    equity = float(df.iloc[0]["equity"])
    if equity == 0:
        return pd.DataFrame(), 0.0

    df["symbol"] = df["symbol"].str.upper().str.strip()
    df["actual_weight"] = df["positionValue"] / equity
    
    return df[["symbol", "actual_weight", "szi", "positionValue"]], equity

def _load_targets(target_date: date) -> pd.DataFrame:
    """
    Load target weights from maicro_logs.positions_jianan_v6 for a specific date.
    """
    sql = """
        SELECT symbol, weight
        FROM (
            SELECT symbol, weight, inserted_at
            FROM maicro_logs.positions_jianan_v6
            WHERE date = %(d)s
              AND weight IS NOT NULL AND isFinite(weight) AND weight != 0
              AND pred_ret IS NOT NULL AND isFinite(pred_ret)
            ORDER BY symbol, inserted_at DESC
            LIMIT 1 BY symbol
        )
    """
    df = query_df(sql, params={"d": target_date})
    if df.empty:
        return pd.DataFrame(columns=["symbol", "target_weight"])
    
    df["symbol"] = df["symbol"].str.upper().str.strip()
    df["target_weight"] = df["weight"].astype(float)
    return df[["symbol", "target_weight"]]

def calculate_te(targets: pd.DataFrame, actuals: pd.DataFrame) -> Tuple[float, pd.DataFrame]:
    """
    Calculate Tracking Error (Sum of Absolute Differences) and return merged DF.
    TE = Sum(|Target - Actual|)
    """
    merged = pd.merge(targets, actuals, on="symbol", how="outer").fillna(0.0)
    merged["diff"] = merged["actual_weight"] - merged["target_weight"]
    merged["abs_diff"] = merged["diff"].abs()
    
    te = merged["abs_diff"].sum()
    return te, merged

def record_te(date_val: date, lag: int, te: float, target_date: date):
    """Insert TE record into ClickHouse."""
    sql = """
    INSERT INTO maicro_monitors.tracking_error_multilag
    (date, strategy_id, lag, te, target_date, timestamp)
    VALUES
    (%(date)s, %(strat)s, %(lag)s, %(te)s, %(tgt)s, now())
    """
    params = {
        "date": date_val,
        "strat": STRATEGY_ID,
        "lag": lag,
        "te": te,
        "tgt": target_date
    }
    try:
        execute(sql, params=params)
    except Exception as e:
        print(f"Error inserting TE for {date_val} lag {lag}: {e}")

def format_email_html(
    run_date: date,
    equity: float,
    te_results: List[Dict],
    detail_df: pd.DataFrame,
    ideal_lag: int
) -> str:
    # 1. Summary Table of Lags
    te_rows = ""
    for res in te_results:
        is_ideal = (res["lag"] == ideal_lag)
        style = "background-color:#dcfce7; font-weight:bold;" if is_ideal else ""
        te_rows += f"""
        <tr style="{style}">
            <td style="padding:4px 8px;">T-{res['lag']}</td>
            <td style="padding:4px 8px;">{res['target_date']}</td>
            <td style="padding:4px 8px; text-align:right;">{res['te']:.4f}</td>
            <td style="padding:4px 8px; text-align:right;">{res['te']*100:.2f}%</td>
        </tr>
        """
    
    # 2. Detailed Breakdown (for Ideal Lag)
    detail_df = detail_df.sort_values("abs_diff", ascending=False)
    pos_rows = ""
    for _, row in detail_df.iterrows():
        sym = row["symbol"]
        tgt = row["target_weight"]
        act = row["actual_weight"]
        diff = row["diff"]
        
        # Color coding
        status_color = ""
        if abs(diff) > 0.05: status_color = "background-color:#fee2e2;" # Red > 5%
        elif abs(diff) > 0.02: status_color = "background-color:#fef3c7;" # Yellow > 2%
        
        pos_rows += f"""
        <tr>
            <td style="padding:4px 8px;">{sym}</td>
            <td style="padding:4px 8px; text-align:right;">{tgt*100:.2f}%</td>
            <td style="padding:4px 8px; text-align:right;">{act*100:.2f}%</td>
            <td style="padding:4px 8px; text-align:right; {status_color}">{diff*100:+.2f}%</td>
        </tr>
        """

    html = f"""
    <html>
    <body style="font-family: sans-serif; color: #111827;">
        <h2>Maicro Daily Tracking Error Report</h2>
        <p><strong>Date:</strong> {run_date} <br>
           <strong>Equity:</strong> ${equity:,.2f}</p>
        
        <h3>Tracking Error by Lag</h3>
        <p>Comparison of actual holdings (T) vs signals from (T-Lag).</p>
        <table border="1" style="border-collapse: collapse; border-color: #e5e7eb;">
            <thead style="background-color: #f3f4f6;">
                <tr>
                    <th style="padding:4px 8px;">Lag</th>
                    <th style="padding:4px 8px;">Target Date</th>
                    <th style="padding:4px 8px;">TE (Sum Abs)</th>
                    <th style="padding:4px 8px;">TE %</th>
                </tr>
            </thead>
            <tbody>
                {te_rows}
            </tbody>
        </table>
        
        <h3>Detailed Breakdown (Lag {ideal_lag})</h3>
        <p>Target Date: {run_date - timedelta(days=ideal_lag)}</p>
        <table border="1" style="border-collapse: collapse; border-color: #e5e7eb; font-size: 12px;">
            <thead style="background-color: #f3f4f6;">
                <tr>
                    <th style="padding:4px 8px;">Symbol</th>
                    <th style="padding:4px 8px;">Target %</th>
                    <th style="padding:4px 8px;">Actual %</th>
                    <th style="padding:4px 8px;">Diff %</th>
                </tr>
            </thead>
            <tbody>
                {pos_rows}
            </tbody>
        </table>
    </body>
    </html>
    """
    return html

def send_email(subject: str, html_body: str):
    if not RESEND_API_KEY:
        print("RESEND_API_KEY not set; skipping email.")
        return
        
    print(f"Sending email to {TO_EMAIL}...")
    try:
        requests.post(
            "https://api.resend.com/emails",
            json={
                "from": FROM_EMAIL,
                "to": [TO_EMAIL],
                "subject": subject,
                "html": html_body
            },
            headers={
                "Authorization": f"Bearer {RESEND_API_KEY}",
                "Content-Type": "application/json"
            },
            timeout=10
        ).raise_for_status()
        print("Email sent.")
    except Exception as e:
        print(f"Email failed: {e}")

def main():
    print("[daily_te] Starting...")
    
    # Analyze "Yesterday" by default, as it's a daily morning report for the closed day
    # Or use "Today" if running late? Usually cron runs at 08:00 UTC, covering previous day?
    # Actually, cron says 08:00. This is usually for the *current* state if we are trading continuously.
    # However, snapshot-based calc is usually T vs T.
    # Let's use "Today" (current UTC date) - 0 days? Or Yesterday?
    # If we run at 08:00 UTC, we likely want to check the state *now* (or latest snapshot today)
    # vs targets.
    # The existing script used "latest run context".
    # I will use TODAY's date (UTC).
    run_date = datetime.utcnow().date()
    
    # 1. Load Actuals
    actuals_df, equity = _load_actuals_snapshot(run_date)
    if actuals_df.empty:
        # Fallback to yesterday if today has no snapshot yet (e.g. running at 00:01)
        run_date = run_date - timedelta(days=1)
        actuals_df, equity = _load_actuals_snapshot(run_date)
        
    if actuals_df.empty:
        print(f"No actual snapshots found for {run_date} or yesterday. Exiting.")
        return
        
    print(f"Loaded actuals for {run_date}. Equity: ${equity:,.2f}")
    
    te_results = []
    ideal_lag = 2
    ideal_detail_df = pd.DataFrame()
    
    # 2. Loop Lags
    for lag in range(4): # 0, 1, 2, 3
        target_date = run_date - timedelta(days=lag)
        targets_df = _load_targets(target_date)
        
        te, detail = calculate_te(targets_df, actuals_df)
        record_te(run_date, lag, te, target_date)
        
        te_results.append({
            "lag": lag,
            "target_date": target_date,
            "te": te
        })
        
        if lag == ideal_lag:
            ideal_detail_df = detail
            
    # 3. Send Email
    html = format_email_html(run_date, equity, te_results, ideal_detail_df, ideal_lag)
    subject = f"[MAICRO] Daily Tracking Error: {te_results[ideal_lag]['te']*100:.2f}% (T-{ideal_lag})"
    
    send_email(subject, html)
    print("[daily_te] Done.")

if __name__ == "__main__":
    main()
