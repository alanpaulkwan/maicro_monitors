#!/usr/bin/env python3
"""
Tracking Error email: last 3 days (per day blocks).

For each of the last 3 UTC dates (D0=today, D-1, D-2):
  - For each Hyperliquid address:
      * Load end-of-day actual positions (maicro_monitors.account_snapshots + positions_snapshots).
      * For lags T0..T3, load targets from maicro_logs.positions_jianan_v6 and compute TE
        using the same helpers as `targets_vs_actuals_daily`.
      * Pick the "ideal" lag (minimum TE).
      * Classify symbol-level differences (intended vs actual) into:
          - missing  : target != 0, actual ~ 0
          - extra    : target ~ 0, actual != 0
          - sign_flip: target * actual < 0
          - mis_sized: |diff| > 2% and none of the above
          - aligned  : everything else

The email is structured similarly to the 3-day trades report:
  - One block per day (most recent first)
  - Within each day, one subsection per address with:
      * TE by lag summary
      * Counts / exposures for each diff category
      * Top symbols by |diff| for the ideal lag, with category labels

Environment:
  - RESEND_API_KEY   (required to send email; loaded via get_secret)
  - ALERT_EMAIL      (recipient, defaults to alanpaulkwan@gmail.com)
  - ALERT_FROM_EMAIL (optional, default 'Maicro Monitors <alerts@resend.dev>')
"""

import os
import sys
from datetime import datetime, timedelta, date
from typing import Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
import requests

# Make repo modules importable
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if REPO_ROOT not in sys.path:
    sys.path.append(REPO_ROOT)

from config.settings import get_secret, HYPERLIQUID_ADDRESSES  # type: ignore  # noqa: E402
from scheduled_processes.emails.daily.targets_vs_actuals_daily import (  # type: ignore  # noqa: E402
    _load_actuals_snapshot,
    _load_targets,
    calculate_te,
)

RESEND_API_KEY = get_secret("RESEND_API_KEY")
TO_EMAIL = os.getenv("ALERT_EMAIL", "alanpaulkwan@gmail.com")
FROM_EMAIL = os.getenv("ALERT_FROM_EMAIL", "Maicro Monitors <alerts@resend.dev>")


def _classify_diffs(detail: pd.DataFrame, missing_eps: float = 1e-3, mis_sized_eps: float = 0.02) -> pd.DataFrame:
    """
    Add a 'category' column to the TE detail DataFrame.

    detail columns expected:
      - symbol
      - target_weight
      - actual_weight
      - diff
      - abs_diff
    """
    if detail.empty:
        detail["category"] = pd.Series(dtype=str)
        return detail

    df = detail.copy()
    tw = df["target_weight"].astype(float)
    aw = df["actual_weight"].astype(float)

    target_nz = tw.abs() > missing_eps
    actual_nz = aw.abs() > missing_eps

    category = pd.Series("aligned", index=df.index, dtype=str)

    # Missing: intended weight but no actual position
    missing_mask = target_nz & (~actual_nz)
    category[missing_mask] = "missing"

    # Extra: actual position where no target
    extra_mask = (~target_nz) & actual_nz
    category[extra_mask] = "extra"

    # Sign flip: opposite direction
    flip_mask = (tw * aw < 0) & target_nz & actual_nz
    category[flip_mask] = "sign_flip"

    # Mis-sized: large diff but not in the above buckets
    mis_sized_mask = (df["abs_diff"] > mis_sized_eps) & ~(missing_mask | extra_mask | flip_mask)
    category[mis_sized_mask] = "mis_sized"

    df["category"] = category
    return df


def _summarize_categories(detail: pd.DataFrame) -> Dict[str, float]:
    """
    Summarize counts and exposures by category.

    Returns dict with:
      - count_{cat}
      - exp_{cat}_target  (sum |target_weight|)
      - exp_{cat}_actual  (sum |actual_weight|)
    """
    if detail.empty:
        return {}

    out: Dict[str, float] = {}
    for cat in ["missing", "extra", "sign_flip", "mis_sized"]:
        mask = detail["category"] == cat
        out[f"count_{cat}"] = int(mask.sum())
        if mask.any():
            sub = detail[mask]
            out[f"exp_{cat}_target"] = float(sub["target_weight"].abs().sum())
            out[f"exp_{cat}_actual"] = float(sub["actual_weight"].abs().sum())
        else:
            out[f"exp_{cat}_target"] = 0.0
            out[f"exp_{cat}_actual"] = 0.0
    return out


def _build_day_for_address(run_date: date, address: str) -> Optional[Dict]:
    """
    For a given date and address, compute TE by lag and detail for the best lag.
    """
    actuals_df, equity = _load_actuals_snapshot(run_date, address)
    if actuals_df.empty or equity <= 0:
        return None

    te_results: List[Dict] = []
    detail_by_lag: Dict[int, pd.DataFrame] = {}

    for lag in range(4):  # 0..3
        target_date = run_date - timedelta(days=lag)
        targets_df = _load_targets(target_date)
        te, detail = calculate_te(targets_df, actuals_df)
        te_results.append(
            {
                "lag": lag,
                "target_date": target_date,
                "te": te,
            }
        )
        detail_by_lag[lag] = detail

    if not te_results:
        return None

    # Choose lag with minimum TE
    ideal_entry = min(te_results, key=lambda x: x["te"])
    ideal_lag = int(ideal_entry["lag"])
    detail_df = detail_by_lag.get(ideal_lag, pd.DataFrame())

    if detail_df.empty:
        detail_df = pd.DataFrame(columns=["symbol", "target_weight", "actual_weight", "diff", "abs_diff"])

    detail_df = _classify_diffs(detail_df)
    cat_summary = _summarize_categories(detail_df)

    return {
        "equity": float(equity),
        "te_results": te_results,
        "ideal_lag": ideal_lag,
        "detail_df": detail_df,
        "category_summary": cat_summary,
    }


def _format_email_text(blocks: Dict[date, Dict[str, Dict]]) -> str:
    """
    Text body: last 3 days, one block per day, per-address TE + mismatch summary.
    """
    lines: List[str] = []
    if not blocks:
        lines.append("MAICRO: Tracking Error – Last 3 Days")
        lines.append("====================================")
        lines.append("No data available.")
        return "\n".join(lines)

    all_dates = sorted(blocks.keys(), reverse=True)
    d0 = all_dates[0]
    d_min = all_dates[-1]

    lines.append("MAICRO: Tracking Error – Last 3 Days")
    lines.append("====================================")
    lines.append(f"Dates (UTC): {d_min} → {d0}")
    lines.append("")

    for d in all_dates:
        day_data = blocks[d]
        lines.append(f"{d}:")
        if not day_data:
            lines.append("  (no data for any address)")
            lines.append("")
            continue

        for addr, info in day_data.items():
            equity = info["equity"]
            te_results = info["te_results"]
            ideal_lag = info["ideal_lag"]
            cat_summary = info["category_summary"]
            addr_short = addr[:8] + "..." if len(addr) > 8 else addr

            lines.append(f"  Address {addr_short} (equity ${equity:,.0f}):")

            # TE by lag
            parts = []
            for res in te_results:
                lag = res["lag"]
                te_val = res["te"]
                tag = f"T-{lag}"
                if lag == ideal_lag:
                    tag += "*"
                parts.append(f"{tag}={te_val*100:.2f}%")
            lines.append("    TE by lag: " + ", ".join(parts))

            # Category summary
            if cat_summary:
                missing_cnt = cat_summary.get("count_missing", 0)
                missing_exp = cat_summary.get("exp_missing_target", 0.0)
                extra_cnt = cat_summary.get("count_extra", 0)
                extra_exp = cat_summary.get("exp_extra_actual", 0.0)
                flip_cnt = cat_summary.get("count_sign_flip", 0)
                flip_exp = cat_summary.get("exp_sign_flip_target", 0.0)

                lines.append(
                    "    Mismatch summary: "
                    f"missing={missing_cnt} ({100*missing_exp:4.1f}% target), "
                    f"extra={extra_cnt} ({100*extra_exp:4.1f}% actual), "
                    f"sign_flip={flip_cnt} ({100*flip_exp:4.1f}% target)"
                )
            else:
                lines.append("    Mismatch summary: (none)")

            # Top diffs for ideal lag
            detail_df = info["detail_df"]
            if detail_df.empty:
                lines.append("    No symbol-level detail.")
                continue

            top = detail_df.sort_values("abs_diff", ascending=False).head(15)
            lines.append("    Top diffs (ideal lag):")
            for _, row in top.iterrows():
                sym = row["symbol"]
                tgt = float(row["target_weight"])
                act = float(row["actual_weight"])
                diff = float(row["diff"])
                cat = row.get("category", "")
                lines.append(
                    f"      - {sym}: target={tgt*100:6.2f}%, "
                    f"actual={act*100:6.2f}%, diff={diff*100:+6.2f}% [{cat}]"
                )
            lines.append("")

    return "\n".join(lines)


def _format_email_html(blocks: Dict[date, Dict[str, Dict]]) -> str:
    if not blocks:
        return """
        <html><body>
        <h2>MAICRO: Tracking Error – Last 3 Days</h2>
        <p>No data available.</p>
        </body></html>
        """

    all_dates = sorted(blocks.keys(), reverse=True)
    d0 = all_dates[0]
    d_min = all_dates[-1]

    day_sections: List[str] = []

    for d in all_dates:
        day_data = blocks[d]
        if not day_data:
            day_sections.append(f"<h3>{d}</h3><p>No data for any address.</p>")
            continue

        addr_sections: List[str] = []
        for addr, info in day_data.items():
            equity = info["equity"]
            te_results = info["te_results"]
            ideal_lag = info["ideal_lag"]
            cat_summary = info["category_summary"]
            detail_df = info["detail_df"]

            # TE table
            te_rows = ""
            for res in te_results:
                lag = res["lag"]
                target_date = res["target_date"]
                te_val = res["te"]
                is_ideal = lag == ideal_lag
                style = "font-weight:bold; background-color:#dcfce7;" if is_ideal else ""
                te_rows += f"""
                <tr style="{style}">
                  <td style="padding:4px 8px;">T-{lag}</td>
                  <td style="padding:4px 8px;">{target_date}</td>
                  <td style="padding:4px 8px; text-align:right;">{te_val:.4f}</td>
                  <td style="padding:4px 8px; text-align:right;">{te_val*100:.2f}%</td>
                </tr>
                """

            if cat_summary:
                missing_cnt = cat_summary.get("count_missing", 0)
                missing_exp = cat_summary.get("exp_missing_target", 0.0)
                extra_cnt = cat_summary.get("count_extra", 0)
                extra_exp = cat_summary.get("exp_extra_actual", 0.0)
                flip_cnt = cat_summary.get("count_sign_flip", 0)
                flip_exp = cat_summary.get("exp_sign_flip_target", 0.0)
                cat_text = (
                    f"Missing: {missing_cnt} symbols ({100*missing_exp:.1f}% of target); "
                    f"Extra: {extra_cnt} symbols ({100*extra_exp:.1f}% of actual); "
                    f"Sign flip: {flip_cnt} symbols ({100*flip_exp:.1f}% of target)."
                )
            else:
                cat_text = "No significant mismatches."

            # Detail table
            if detail_df.empty:
                detail_html = "<p>No symbol-level detail.</p>"
            else:
                top = detail_df.sort_values("abs_diff", ascending=False).head(15)
                rows = []
                for _, row in top.iterrows():
                    sym = row["symbol"]
                    tgt = float(row["target_weight"])
                    act = float(row["actual_weight"])
                    diff = float(row["diff"])
                    cat = row.get("category", "")
                    badge_color = {
                        "missing": "#fee2e2",
                        "extra": "#e0f2fe",
                        "sign_flip": "#fbb6ce",
                        "mis_sized": "#fef3c7",
                    }.get(cat, "#e5e7eb")
                    rows.append(
                        "<tr>"
                        f"<td style='padding:4px 8px;'>{sym}</td>"
                        f"<td style='padding:4px 8px; text-align:right;'>{tgt*100:.2f}%</td>"
                        f"<td style='padding:4px 8px; text-align:right;'>{act*100:.2f}%</td>"
                        f"<td style='padding:4px 8px; text-align:right;'>{diff*100:+.2f}%</td>"
                        f"<td style='padding:4px 8px; text-align:center; background-color:{badge_color};'>{cat}</td>"
                        "</tr>"
                    )
                detail_rows = "".join(rows)
                detail_html = f"""
                <table cellspacing="0" cellpadding="0" style="border-collapse:collapse; border:1px solid #e5e7eb; margin-top:4px; font-size:12px;">
                  <thead>
                    <tr style="background-color:#f3f4f6;">
                      <th style="padding:4px 8px; text-align:left;">Symbol</th>
                      <th style="padding:4px 8px; text-align:right;">Target %</th>
                      <th style="padding:4px 8px; text-align:right;">Actual %</th>
                      <th style="padding:4px 8px; text-align:right;">Diff %</th>
                      <th style="padding:4px 8px; text-align:center;">Category</th>
                    </tr>
                  </thead>
                  <tbody>
                    {detail_rows}
                  </tbody>
                </table>
                """

            addr_sections.append(
                f"""
            <div style="margin-bottom:20px; padding-bottom:12px; border-bottom:1px solid #e5e7eb;">
              <h4 style="margin:0 0 4px 0;">Address {addr}</h4>
              <p style="margin:0 0 6px 0; color:#6b7280;">Equity: ${equity:,.0f}</p>
              <h5 style="margin:4px 0;">Tracking Error by Lag</h5>
              <table cellspacing="0" cellpadding="0" style="border-collapse:collapse; border:1px solid #e5e7eb; font-size:12px;">
                <thead>
                  <tr style="background-color:#f3f4f6;">
                    <th style="padding:4px 8px; text-align:left;">Lag</th>
                    <th style="padding:4px 8px; text-align:left;">Target Date</th>
                    <th style="padding:4px 8px; text-align:right;">TE (avg abs)</th>
                    <th style="padding:4px 8px; text-align:right;">TE %</th>
                  </tr>
                </thead>
                <tbody>
                  {te_rows}
                </tbody>
              </table>
              <p style="margin:6px 0 4px 0; color:#4b5563;">{cat_text}</p>
              <h5 style="margin:4px 0;">Top diffs (ideal lag T-{ideal_lag})</h5>
              {detail_html}
            </div>
            """
            )

        day_sections.append(
            f"""
        <section style="margin-bottom:24px;">
          <h3 style="margin-bottom:4px;">{d}</h3>
          {''.join(addr_sections)}
        </section>
        """
        )

    html = f"""
    <html>
      <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; font-size: 14px; color: #111827;">
        <h2 style="margin-bottom:4px;">MAICRO: Tracking Error – Last 3 Days</h2>
        <p style="margin-top:0; color:#6b7280;">
          Dates (UTC): <b>{d_min}</b> → <b>{d0}</b>
        </p>
        {''.join(day_sections)}
      </body>
    </html>
    """
    return html


def _send_email(subject: str, text_body: str, html_body: str) -> None:
    if not RESEND_API_KEY:
        print("RESEND_API_KEY not set; skipping tracking-error email send.")
        return

    print(f"Sending tracking-error-3d email to {TO_EMAIL}...")
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
    print("[tracking_error_last3d_daily] Starting...")
    today = datetime.utcnow().date()

    # Last 3 days: today, yesterday, day-2
    dates = [today - timedelta(days=i) for i in range(3)]
    blocks: Dict[date, Dict[str, Dict]] = {}

    for d in dates:
        day_block: Dict[str, Dict] = {}
        for addr in HYPERLIQUID_ADDRESSES:
            print(f"  Processing date {d} address {addr}...")
            info = _build_day_for_address(d, addr)
            if info is not None:
                day_block[addr] = info
            else:
                print(f"    No usable data for {d} {addr}.")
        blocks[d] = day_block

    if not any(blocks.values()):
        print("No data for any of the last 3 days; exiting.")
        return

    text_body = _format_email_text(blocks)
    html_body = _format_email_html(blocks)

    print("----- EMAIL BODY (text) BEGIN -----")
    print(text_body)
    print("----- EMAIL BODY (text) END -----")

    subject = "[MAICRO DAILY] Tracking Error – Last 3 Days"
    _send_email(subject, text_body, html_body)
    print("[tracking_error_last3d_daily] Done.")


if __name__ == "__main__":
    main()

