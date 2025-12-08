#!/usr/bin/env python3
"""
Daily email: target vs actual positions (Jianan v6 vs live).

Targets:
  - `maicro_logs.positions_jianan_v6` (earliest row per (date, symbol) with finite, non-zero weight & pred_ret)

Actuals:
  - `maicro_logs.live_positions` (kind='current')
    • we take the latest available `target_date` and the last row per (target_date, symbol) by ts

Logic:
  - Pick latest target `date` from positions_jianan_v6.
  - Map to expected actual `target_date = date + OFFSET_DAYS` (default: 2).
  - Normalize weights separately on long/short sides.
  - Email summary: coverage stats + **all target positions**
    with color-coded differences (HTML table):
      - red  = actual < target (too little)
      - green = actual > target (too much)

Environment:
  - `RESEND_API_KEY`   (required to send email)
  - `ALERT_EMAIL`      (recipient, defaults to alanpaulkwan@gmail.com)
  - `ALERT_FROM_EMAIL` (optional, default 'Maicro Monitors <alerts@resend.dev>')
  - `TARGET_ACTUAL_OFFSET_DAYS` (optional, default '2')
"""

import os
import sys
from datetime import timedelta
from typing import Optional

import numpy as np
import pandas as pd
import requests

# Make repo modules importable
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if REPO_ROOT not in sys.path:
    sys.path.append(REPO_ROOT)

from modules.clickhouse_client import query_df  # type: ignore  # noqa: E402


RESEND_API_KEY = os.getenv("RESEND_API_KEY")
TO_EMAIL = os.getenv("ALERT_EMAIL", "alanpaulkwan@gmail.com")
FROM_EMAIL = os.getenv("ALERT_FROM_EMAIL", "Maicro Monitors <alerts@resend.dev>")
OFFSET_DAYS = int(os.getenv("TARGET_ACTUAL_OFFSET_DAYS", "2"))


def _normalize_weights(df: pd.DataFrame, weight_col: str = "weight") -> pd.Series:
    """
    Normalize weights so longs sum to 1 and shorts sum to 1 (abs).
    Returns a Series aligned to df.index.
    """
    if df.empty or weight_col not in df.columns:
        return pd.Series(index=df.index, dtype=float)

    weights = df[weight_col].astype(float)
    long_mask = weights > 0
    short_mask = weights < 0

    long_sum = weights[long_mask].sum()
    short_sum = weights[short_mask].abs().sum()

    normalized = pd.Series(index=df.index, dtype=float)
    if long_sum > 0:
        normalized[long_mask] = weights[long_mask] / long_sum
    if short_sum > 0:
        normalized[short_mask] = weights[short_mask] / short_sum
    normalized = normalized.fillna(0.0)
    return normalized


def _load_targets_for_date(target_date: pd.Timestamp) -> pd.DataFrame:
    """
    Earliest row per (date, symbol) with finite, non-zero weight & pred_ret.
    """
    sql = """
        SELECT date, symbol, weight
        FROM (
            SELECT date, symbol, weight, inserted_at
            FROM maicro_logs.positions_jianan_v6
            WHERE date = %(date)s
              AND weight IS NOT NULL AND isFinite(weight) AND weight != 0
              AND pred_ret IS NOT NULL AND isFinite(pred_ret)
            ORDER BY date, symbol, inserted_at
            LIMIT 1 BY date, symbol
        )
    """
    df = query_df(sql, params={"date": target_date.date()})
    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    df["weight"] = df["weight"].astype(float)
    df["weight_norm"] = _normalize_weights(df, "weight")
    return df


def _load_prev_targets_for_date(target_date: pd.Timestamp, steps: int = 1) -> Optional[pd.DataFrame]:
    """
    Load targets for the Nth latest date strictly before `target_date`.

    steps=1 → previous day (T-1 in signal space)
    steps=2 → two days back (T-2), etc.
    Returns None if there are not enough earlier dates.
    """
    cutoff = target_date.date()
    prev_date: Optional[pd.Timestamp] = None

    for _ in range(steps):
        sql = """
            SELECT max(date) AS max_date
            FROM maicro_logs.positions_jianan_v6
            WHERE date < %(cutoff)s
        """
        df = query_df(sql, params={"cutoff": cutoff})
        if df.empty or pd.isna(df.iloc[0]["max_date"]):
            return None
        prev_date = pd.to_datetime(df.iloc[0]["max_date"]).normalize()
        cutoff = prev_date.date()

    prev_targets = _load_targets_for_date(prev_date)
    if prev_targets.empty:
        return None

    prev_targets = prev_targets.copy()
    prev_targets["prev_date"] = prev_date.date()
    return prev_targets


def _load_next_targets_for_date(target_date: pd.Timestamp) -> Optional[pd.DataFrame]:
    """
    Load targets for the earliest date strictly after `target_date`.
    Returns None if no later date exists.
    """
    sql = """
        SELECT min(date) AS min_date
        FROM maicro_logs.positions_jianan_v6
        WHERE date > %(cutoff)s
    """
    df = query_df(sql, params={"cutoff": target_date.date()})
    if df.empty or pd.isna(df.iloc[0]["min_date"]):
        return None

    next_date = pd.to_datetime(df.iloc[0]["min_date"]).normalize()
    next_targets = _load_targets_for_date(next_date)
    if next_targets.empty:
        return None

    next_targets = next_targets.copy()
    next_targets["next_date"] = next_date.date()
    return next_targets
def _load_latest_run_context() -> Optional[tuple[pd.Timestamp, pd.Timestamp]]:
    """
    Latest live run context from `maicro_logs.live_positions` (kind='current').

    Returns:
        (target_date, run_ts)

    Where:
      - target_date is the model signal date D (same key as positions_jianan_v6.date)
      - run_ts is the actual run timestamp for that target_date.
    """
    sql = """
        SELECT target_date, max(ts) AS ts
        FROM maicro_logs.live_positions
        WHERE kind = 'current'
        GROUP BY target_date
        ORDER BY ts DESC
        LIMIT 1
    """
    df = query_df(sql)
    if df.empty or pd.isna(df.iloc[0]["target_date"]) or pd.isna(df.iloc[0]["ts"]):
        return None
    target_date = pd.to_datetime(df.iloc[0]["target_date"]).normalize()
    run_ts = pd.to_datetime(df.iloc[0]["ts"])
    return target_date, run_ts


def _load_actuals_for_date(actual_date: pd.Timestamp) -> pd.DataFrame:
    """
    Last row per (target_date, symbol, kind='current') ordered by ts.
    """
    sql = """
        SELECT target_date, symbol, qty, usd, equity_usd
        FROM (
            SELECT *, row_number() OVER(PARTITION BY target_date, symbol, kind ORDER BY ts DESC) AS rn
            FROM maicro_logs.live_positions
            WHERE kind = 'current' AND target_date = %(date)s
        )
        WHERE rn = 1
    """
    df = query_df(sql, params={"date": actual_date.date()})
    if df.empty:
        return df

    df["target_date"] = pd.to_datetime(df["target_date"]).dt.date
    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    df["usd"] = df["usd"].astype(float)
    df["equity_usd"] = df["equity_usd"].astype(float)
    # Avoid division by zero
    df["weight"] = df.apply(
        lambda row: row["usd"] / row["equity_usd"] if row["equity_usd"] not in (0, None, np.nan) else 0.0,
        axis=1,
    )
    df["weight_norm"] = _normalize_weights(df, "weight")
    return df


def build_comparison(targets: pd.DataFrame, actuals: pd.DataFrame) -> pd.DataFrame:
    """
    Join on symbol and compute normalized weight differences.
    """
    t = targets[["symbol", "weight_norm"]].rename(columns={"weight_norm": "target_weight_pct"})
    a = actuals[["symbol", "weight_norm"]].rename(columns={"weight_norm": "actual_weight_pct"})

    merged = t.merge(a, on="symbol", how="outer")
    merged["target_weight_pct"] = merged["target_weight_pct"].fillna(0.0)
    merged["actual_weight_pct"] = merged["actual_weight_pct"].fillna(0.0)

    merged["has_target"] = merged["target_weight_pct"] != 0.0
    merged["has_actual"] = merged["actual_weight_pct"] != 0.0
    merged["weight_diff"] = merged["actual_weight_pct"] - merged["target_weight_pct"]
    merged["abs_diff"] = merged["weight_diff"].abs()

    # Wrong sign: both non-zero, target and actual have opposite signs
    merged["wrong_sign"] = (
        merged["has_target"]
        & merged["has_actual"]
        & (merged["target_weight_pct"] != 0.0)
        & (merged["actual_weight_pct"] != 0.0)
        & (np.sign(merged["target_weight_pct"]) != np.sign(merged["actual_weight_pct"]))
    )

    merged = merged.sort_values("abs_diff", ascending=False).reset_index(drop=True)
    return merged


def format_email_text(
    target_date: pd.Timestamp,
    run_ts: pd.Timestamp,
    comp: pd.DataFrame,
    offset_days: int,
) -> str:
    """Plain-text fallback body (no colors)."""
    lines: list[str] = []
    lines.append("MAICRO: Targets vs Actual Positions")
    lines.append("===================================")
    lines.append(f"Target (signal) date [positions_jianan_v6.date / live_positions.target_date]: {target_date.date()}")
    lines.append(
        f"Run timestamp [live_positions.ts, kind='current']: {run_ts} "
        f"(run_ts.date - target_date = {offset_days:+d} days)"
    )
    lines.append("")
    lines.append("Explanation: model targets are keyed by positions_jianan_v6.date = D.")
    lines.append("When the live run executes later, rows are stored in maicro_logs.live_positions")
    lines.append("with target_date = D and ts = actual run time. This email always uses the")
    lines.append("latest ts (kind='current') and compares its weights to the targets for that D.")
    lines.append("")

    total_rows = len(comp)
    both = comp["has_target"] & comp["has_actual"]
    target_only = comp["has_target"] & ~comp["has_actual"]
    actual_only = ~comp["has_target"] & comp["has_actual"]

    lines.append("Coverage")
    lines.append("--------")
    lines.append(f"Total symbols in union : {total_rows}")
    lines.append(f"Targets only            : {int(target_only.sum())}")
    lines.append(f"Actuals only            : {int(actual_only.sum())}")
    lines.append(f"Both target & actual    : {int(both.sum())}")

    if both.any():
        matched = comp[both]
        mean_abs = matched["abs_diff"].mean()
        median_abs = matched["abs_diff"].median()
        max_abs = matched["abs_diff"].max()
        lines.append("")
        lines.append("Matched positions (weight diff stats)")
        lines.append("-------------------------------------")
        lines.append(f"Mean |diff|   : {100*mean_abs:5.2f}%")
        lines.append(f"Median |diff| : {100*median_abs:5.2f}%")
        lines.append(f"Max |diff|    : {100*max_abs:5.2f}%")

    # Detailed per-symbol view (all target positions, sorted by |diff|)
    view = comp[comp["has_target"]].copy()
    lines.append("")
    lines.append("All target symbols (sorted by |weight diff|)")
    lines.append("--------------------------------------------")
    lines.append(f"{'Symbol':<10} {'Target%':>9} {'Actual%':>9} {'Diff%':>9} {'Wrong?':>7} {'Prev1%':>8} {'Prev2%':>8} {'Next%':>8}")
    lines.append(f"{'-'*10} {'-'*9} {'-'*9} {'-'*9} {'-'*7} {'-'*8} {'-'*8} {'-'*8}")

    for _, row in view.iterrows():
        sym = row["symbol"]
        t_pct = 100 * row["target_weight_pct"]
        a_pct = 100 * row["actual_weight_pct"]
        d_pct = 100 * row["weight_diff"]
        prev1 = row.get("prev1_target_weight_pct", None)
        prev2 = row.get("prev2_target_weight_pct", None)
        nextv = row.get("next_target_weight_pct", None)
        prev1_pct_str = f"{100 * float(prev1):8.2f}" if prev1 is not None and not pd.isna(prev1) else " " * 8
        prev2_pct_str = f"{100 * float(prev2):8.2f}" if prev2 is not None and not pd.isna(prev2) else " " * 8
        next_pct_str = f"{100 * float(nextv):8.2f}" if nextv is not None and not pd.isna(nextv) else " " * 8
        wrong = row.get("wrong_sign", False)
        wrong_str = "WRONG" if wrong else ""
        lines.append(
            f"{sym:<10} {t_pct:9.2f} {a_pct:9.2f} {d_pct:9.2f} {wrong_str:>7} {prev1_pct_str} {prev2_pct_str} {next_pct_str}"
        )

    lines.append("")
    lines.append("Notes:")
    lines.append("- Target weights from maicro_logs.positions_jianan_v6 (earliest per date,symbol).")
    lines.append("- Actual weights from maicro_logs.live_positions (kind='current').")
    lines.append("- Long and short weights are normalized separately to sum to 1.")

    return "\n".join(lines)


def format_email_html(
    target_date: pd.Timestamp,
    run_ts: pd.Timestamp,
    comp: pd.DataFrame,
    offset_days: int,
) -> str:
    """HTML body with color-coded rows."""
    both = comp["has_target"] & comp["has_actual"]
    target_only = comp["has_target"] & ~comp["has_actual"]
    actual_only = ~comp["has_target"] & comp["has_actual"]

    # Only show target symbols in the detailed table
    view = comp[comp["has_target"]].copy()

    mean_abs = median_abs = max_abs = None
    if both.any():
        matched = comp[both]
        mean_abs = matched["abs_diff"].mean()
        median_abs = matched["abs_diff"].median()
        max_abs = matched["abs_diff"].max()

    def status_label(row: pd.Series) -> str:
        if not row.get("has_actual", False):
            return "MISSING"
        diff = float(row.get("weight_diff", 0.0))
        if diff < -1e-6:
            return "Underweight"
        if diff > 1e-6:
            return "Overweight"
        return "Matched"

    rows_html: list[str] = []

    def status_cell_style(status: str) -> str:
        """
        Color only the Status cell (not the whole row):
          - Underweight → orange
          - Overweight  → pink
          - MISSING     → yellow
          - Matched     → neutral
        """
        base = "padding:4px 8px;"
        if status == "Underweight":
            return base + " background-color:#fed7aa;"  # orange
        if status == "Overweight":
            return base + " background-color:#fecaca;"  # pink/red
        if status == "MISSING":
            return base + " background-color:#fef9c3;"  # yellow
        return base  # Matched / anything else
    for _, row in view.iterrows():
        sym = row["symbol"]
        t_pct = 100 * row["target_weight_pct"]
        a_pct = 100 * row["actual_weight_pct"]
        d_pct = 100 * row["weight_diff"]
        status = status_label(row)
        wrong = bool(row.get("wrong_sign", False))
        wrong_cell = (
            "<td style='padding:4px 8px; text-align:center; color:#b91c1c;'><b><i>WRONG</i></b></td>"
            if wrong
            else "<td style='padding:4px 8px; text-align:center;'>-</td>"
        )
        prev1_val = row.get("prev1_target_weight_pct", None)
        prev2_val = row.get("prev2_target_weight_pct", None)
        next_val = row.get("next_target_weight_pct", None)
        prev1_str = f"{100 * float(prev1_val):,.2f}%" if prev1_val is not None and not pd.isna(prev1_val) else "—"
        prev2_str = f"{100 * float(prev2_val):,.2f}%" if prev2_val is not None and not pd.isna(prev2_val) else "—"
        next_str = f"{100 * float(next_val):,.2f}%" if next_val is not None and not pd.isna(next_val) else "—"

        # Color for prev1 target cell based on sign vs current target
        prev1_cell_style = "padding:4px 8px; text-align:right;"
        if prev1_val is not None and not pd.isna(prev1_val):
            sign_prev1 = np.sign(float(prev1_val))
            sign_cur = np.sign(row["target_weight_pct"])
            if sign_prev1 != 0 and sign_cur != 0:
                if sign_prev1 == sign_cur:
                    # same sign: green
                    prev1_cell_style += " background-color:#dcfce7;"
                else:
                    # different sign: red
                    prev1_cell_style += " background-color:#fee2e2;"

        rows_html.append(
            "<tr>"
            f"<td style='padding:4px 8px;'>{sym}</td>"
            f"<td style='padding:4px 8px; text-align:right;'>{t_pct:,.2f}%</td>"
            f"<td style='padding:4px 8px; text-align:right;'>{a_pct:,.2f}%</td>"
            f"<td style='padding:4px 8px; text-align:right;'>{d_pct:,.2f}%</td>"
            f"<td style='{status_cell_style(status)}'>{status}</td>"
            f"{wrong_cell}"
            f"<td style='{prev1_cell_style}'>{prev1_str}</td>"
            f"<td style='padding:4px 8px; text-align:right;'>{prev2_str}</td>"
            f"<td style='padding:4px 8px; text-align:right;'>{next_str}</td>"
            "</tr>"
        )

    coverage_html = (
        "<ul>"
        f"<li>Total symbols in union: <b>{len(comp)}</b></li>"
        f"<li>Targets only: <b>{int(target_only.sum())}</b></li>"
        f"<li>Actuals only: <b>{int(actual_only.sum())}</b></li>"
        f"<li>Both target & actual: <b>{int(both.sum())}</b></li>"
        "</ul>"
    )

    stats_html = ""
    if mean_abs is not None:
        stats_html = (
            "<p><b>Matched positions (weight diff stats)</b><br>"
            f"Mean |diff|: {100*mean_abs:5.2f}% &nbsp; "
            f"Median |diff|: {100*median_abs:5.2f}% &nbsp; "
            f"Max |diff|: {100*max_abs:5.2f}%</p>"
        )

    html = f"""<html>
  <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; font-size: 14px; color: #111827;">
    <h2 style="margin-bottom:4px;">MAICRO: Targets vs Actual Positions</h2>
    <p style="margin-top:0; color:#6b7280;">
      Target (signal) date&nbsp;<code>positions_jianan_v6.date / live_positions.target_date</code>:
      <b>{target_date.date()}</b><br>
      Run timestamp&nbsp;<code>live_positions.ts</code> (kind='current'):
      <b>{run_ts}</b> (run_ts.date - target_date = {offset_days:+d} days)
    </p>
    <p style="margin-top:4px; color:#6b7280; font-size:12px;">
      Explanation: model targets are keyed by <code>positions_jianan_v6.date = D</code>.<br>
      When the live run executes later, rows are stored in <code>maicro_logs.live_positions</code>
      with <code>target_date = D</code> and <code>ts</code> equal to the actual run time.<br>
      This email always uses the latest <code>ts</code> (for <code>kind='current'</code>) and
      compares its weights to the targets for that same <code>D</code>.
    </p>
    <h3 style="margin-bottom:4px;">Coverage</h3>
    {coverage_html}
    {stats_html}
    <h3 style="margin-bottom:4px;">All target symbols (sorted by |weight diff|)</h3>
    <table cellspacing="0" cellpadding="0" style="border-collapse:collapse; border:1px solid #e5e7eb; margin-top:4px;">
      <thead>
        <tr style="background-color:#f3f4f6;">
          <th style="padding:4px 8px; text-align:left;">Symbol</th>
          <th style="padding:4px 8px; text-align:right;">Target %</th>
          <th style="padding:4px 8px; text-align:right;">Actual %</th>
          <th style="padding:4px 8px; text-align:right;">Diff %</th>
          <th style="padding:4px 8px; text-align:left;">Status</th>
          <th style="padding:4px 8px; text-align:center;">Wrong sign?</th>
          <th style="padding:4px 8px; text-align:right;">Prev1 Target %</th>
          <th style="padding:4px 8px; text-align:right;">Prev2 Target %</th>
          <th style="padding:4px 8px; text-align:right;">Next Target %</th>
        </tr>
      </thead>
      <tbody>
        {''.join(rows_html)}
      </tbody>
    </table>
    <p style="margin-top:12px; color:#6b7280; font-size:12px;">
      Notes:<br>
      - Target weights from <code>maicro_logs.positions_jianan_v6</code> (earliest per date,symbol).<br>
      - Actual weights from <code>maicro_logs.live_positions</code> (kind='current').<br>
      - Long and short weights are normalized separately on their respective sides to sum to 1.
    </p>
  </body>
</html>"""
    return html


def send_email(subject: str, text_body: str, html_body: str) -> None:
    if not RESEND_API_KEY:
        print("RESEND_API_KEY not set; skipping email send.")
        return

    print(f"Sending targets-vs-actuals email to {TO_EMAIL}...")
    url = "https://api.resend.com/emails"
    headers = {
        "Authorization": f"Bearer {RESEND_API_KEY}",
        "Content-Type": "application/json",
    }
    data = {
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
    print("[targets_vs_actuals_daily] Starting...")

    ctx = _load_latest_run_context()
    if not ctx:
        print("No live_positions.current runs found; exiting.")
        return

    target_date, run_ts = ctx
    offset_days = (run_ts.date() - target_date.date()).days
    print(f"Using target_date={target_date.date()}, run_ts={run_ts}, offset_days={offset_days}")

    targets = _load_targets_for_date(target_date)
    if targets.empty:
        print(f"No target rows found for date={target_date.date()}; exiting.")
        return

    actuals = _load_actuals_for_date(target_date)
    if actuals.empty:
        print(f"No actual positions found for target_date={actual_date.date()}; proceeding with targets only.")
        # Still build a comparison with empty actuals (all actual_weight_pct=0)
        actuals = pd.DataFrame(columns=["symbol", "weight_norm"])

    # Load previous/next targets (if available) and merge into comparison
    prev1_targets = _load_prev_targets_for_date(target_date, steps=1)
    prev2_targets = _load_prev_targets_for_date(target_date, steps=2)
    next_targets = _load_next_targets_for_date(target_date)

    comp = build_comparison(targets, actuals)
    if prev1_targets is not None and not prev1_targets.empty:
        prev1_view = prev1_targets[["symbol", "weight_norm"]].rename(
            columns={"weight_norm": "prev1_target_weight_pct"}
        )
        comp = comp.merge(prev1_view, on="symbol", how="left")

    if prev2_targets is not None and not prev2_targets.empty:
        prev2_view = prev2_targets[["symbol", "weight_norm"]].rename(
            columns={"weight_norm": "prev2_target_weight_pct"}
        )
        comp = comp.merge(prev2_view, on="symbol", how="left")

    if next_targets is not None and not next_targets.empty:
        next_view = next_targets[["symbol", "weight_norm"]].rename(
            columns={"weight_norm": "next_target_weight_pct"}
        )
        comp = comp.merge(next_view, on="symbol", how="left")

    text_body = format_email_text(target_date, run_ts, comp, offset_days)
    html_body = format_email_html(target_date, run_ts, comp, offset_days)

    subject = f"[MAICRO DAILY] Targets vs Actuals - D={target_date.date()} (run={run_ts.date()}, offset={offset_days:+d}d)"

    # Log text body to stdout for inspection
    print("----- EMAIL BODY BEGIN -----")
    print(text_body)
    print("----- EMAIL BODY END -----")

    send_email(subject, text_body, html_body)
    print("[targets_vs_actuals_daily] Done.")


if __name__ == "__main__":
    main()
