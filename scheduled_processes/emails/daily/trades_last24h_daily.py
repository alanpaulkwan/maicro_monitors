#!/usr/bin/env python3
"""
Daily email: Trades in the last 3 days.

Summarizes Hyperliquid trades from `maicro_monitors.trades` over the
last 3 days (rolling window, UTC) and sends a concise email report.

Data source:
  - maicro_monitors.trades

Grouping:
  - Summary by address:
      * n_trades
      * n_symbols
      * gross_notional_usd (sum |sz * px|)
      * realized_pnl (sum closedPnl)
      * fees (sum fee)
  - Top trades by notional per day across all addresses (absolute sz * px), for each day in the 3-day window

Environment:
  - RESEND_API_KEY   (required to send email; loaded via get_secret)
  - ALERT_EMAIL      (recipient, defaults to alanpaulkwan@gmail.com)
  - ALERT_FROM_EMAIL (optional, default 'Maicro Monitors <alerts@resend.dev>')
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Tuple

import pandas as pd
import requests

# Make repo modules importable
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if REPO_ROOT not in sys.path:
    sys.path.append(REPO_ROOT)

from modules.clickhouse_client import query_df  # type: ignore  # noqa: E402
from config.settings import get_secret, HYPERLIQUID_ADDRESSES  # type: ignore  # noqa: E402

RESEND_API_KEY = get_secret("RESEND_API_KEY")
TO_EMAIL = os.getenv("ALERT_EMAIL", "alanpaulkwan@gmail.com")
FROM_EMAIL = os.getenv("ALERT_FROM_EMAIL", "Maicro Monitors <alerts@resend.dev>")


def _load_trades_last_24h() -> Tuple[pd.DataFrame, datetime, datetime]:
    """Load trades in the last 3 days from maicro_monitors.trades."""
    now = datetime.utcnow()
    since = now - timedelta(days=3)

    sql = """
        SELECT
            time,
            address,
            coin,
            side,
            px,
            sz,
            closedPnl,
            fee,
            sz * px AS notional
        FROM maicro_monitors.trades
        WHERE time >= %(since)s
          AND time <= %(now)s
    """

    df = query_df(sql, params={"since": since, "now": now})
    if df.empty:
        return df, since, now

    # Normalize types / columns
    df["time"] = pd.to_datetime(df["time"])
    df["address"] = df["address"].astype(str)
    df["coin"] = df["coin"].astype(str)
    df["side"] = df["side"].astype(str)
    df["px"] = df["px"].astype(float)
    df["sz"] = df["sz"].astype(float)
    df["closedPnl"] = df["closedPnl"].astype(float)
    df["fee"] = df["fee"].astype(float)
    df["notional"] = df["notional"].astype(float)
    df["notional_abs"] = df["notional"].abs()

    return df, since, now


def _format_dollar(value: float) -> str:
    return f"${value:,.2f}"


def _format_float(value: float) -> str:
    return f"{value:,.4f}"


def format_email_text(df: pd.DataFrame, since: datetime, now: datetime) -> str:
    lines = []
    lines.append("MAICRO: Trades in the Last 3 Days")
    lines.append("=================================")
    lines.append(f"Window (UTC): {since:%Y-%m-%d %H:%M:%S} → {now:%Y-%m-%d %H:%M:%S}")
    lines.append("")

    if df.empty:
        lines.append("No trades in the last 3 days.")
        return "\n".join(lines)

    # Summary by address
    summary = (
        df.groupby("address")
        .agg(
            n_trades=("coin", "size"),
            n_symbols=("coin", pd.Series.nunique),
            gross_notional_usd=("notional_abs", "sum"),
            realized_pnl=("closedPnl", "sum"),
            fees=("fee", "sum"),
        )
        .reset_index()
        .sort_values("gross_notional_usd", ascending=False)
    )

    lines.append("Summary by address:")
    for _, row in summary.iterrows():
        addr = row["address"]
        addr_short = addr[:8] + "..." if len(addr) > 8 else addr
        lines.append(
            f"- {addr_short}: "
            f"{int(row['n_trades'])} trades across {int(row['n_symbols'])} symbols | "
            f"Gross notional { _format_dollar(row['gross_notional_usd']) } | "
            f"Realized PnL { _format_dollar(row['realized_pnl']) } | "
            f"Fees { _format_dollar(row['fees']) }"
        )

    # Top trades by notional, split into blocks per day
    lines.append("")
    lines.append("Top trades by notional per day (all addresses):")

    # Group by UTC date, show most recent day first
    df = df.copy()
    df["trade_date"] = df["time"].dt.date
    unique_dates = sorted(df["trade_date"].unique(), reverse=True)

    for d in unique_dates:
        day_df = df[df["trade_date"] == d]
        if day_df.empty:
            continue
        lines.append(f"")
        lines.append(f"{d}:")
        top_trades = day_df.sort_values("notional_abs", ascending=False).head(20)
        for _, row in top_trades.iterrows():
            addr = row["address"]
            addr_short = addr[:8] + "..." if len(addr) > 8 else addr
            lines.append(
                f"- {row['time']:%Y-%m-%d %H:%M:%S} UTC | {addr_short} | "
                f"{row['coin']} {row['side']} sz={_format_float(row['sz'])} px={_format_dollar(row['px'])} | "
                f"notional={_format_dollar(row['notional'])} | "
                f"pnl={_format_dollar(row['closedPnl'])} | "
                f"fee={_format_dollar(row['fee'])}"
            )

    return "\n".join(lines)


def format_email_html(df: pd.DataFrame, since: datetime, now: datetime) -> str:
    if df.empty:
        body = "<p>No trades in the last 24 hours.</p>"
        summary_rows_html = ""
        top_trades_html = ""
    else:
        summary = (
            df.groupby("address")
            .agg(
                n_trades=("coin", "size"),
                n_symbols=("coin", pd.Series.nunique),
                gross_notional_usd=("notional_abs", "sum"),
                realized_pnl=("closedPnl", "sum"),
                fees=("fee", "sum"),
            )
            .reset_index()
            .sort_values("gross_notional_usd", ascending=False)
        )

        summary_rows = []
        for _, row in summary.iterrows():
            addr = row["address"]
            addr_short = addr[:10] + "..." if len(addr) > 10 else addr
            summary_rows.append(
                "<tr>"
                f"<td style='padding:4px 8px;'>{addr_short}</td>"
                f"<td style='padding:4px 8px; text-align:right;'>{int(row['n_trades'])}</td>"
                f"<td style='padding:4px 8px; text-align:right;'>{int(row['n_symbols'])}</td>"
                f"<td style='padding:4px 8px; text-align:right;'>{_format_dollar(row['gross_notional_usd'])}</td>"
                f"<td style='padding:4px 8px; text-align:right;'>{_format_dollar(row['realized_pnl'])}</td>"
                f"<td style='padding:4px 8px; text-align:right;'>{_format_dollar(row['fees'])}</td>"
                "</tr>"
            )
        summary_rows_html = "".join(summary_rows)

        # Top trades by notional: build 3 blocks, one per day in window
        df = df.copy()
        df["trade_date"] = df["time"].dt.date
        unique_dates = sorted(df["trade_date"].unique(), reverse=True)

        day_tables = []
        for d in unique_dates:
            day_df = df[df["trade_date"] == d]
            if day_df.empty:
                continue
            top_trades = day_df.sort_values("notional_abs", ascending=False).head(20)
            if top_trades.empty:
                continue

            rows = []
            for _, row in top_trades.iterrows():
                addr = row["address"]
                addr_short = addr[:10] + "..." if len(addr) > 10 else addr
                rows.append(
                    "<tr>"
                    f"<td style='padding:4px 8px;'>{row['time']:%Y-%m-%d %H:%M:%S}</td>"
                    f"<td style='padding:4px 8px;'>{addr_short}</td>"
                    f"<td style='padding:4px 8px;'>{row['coin']}</td>"
                    f"<td style='padding:4px 8px; text-align:center;'>{row['side']}</td>"
                    f"<td style='padding:4px 8px; text-align:right;'>{_format_float(row['sz'])}</td>"
                    f"<td style='padding:4px 8px; text-align:right;'>{_format_dollar(row['px'])}</td>"
                    f"<td style='padding:4px 8px; text-align:right;'>{_format_dollar(row['notional'])}</td>"
                    f"<td style='padding:4px 8px; text-align:right;'>{_format_dollar(row['closedPnl'])}</td>"
                    f"<td style='padding:4px 8px; text-align:right;'>{_format_dollar(row['fee'])}</td>"
                    "</tr>"
                )

            rows_html = "".join(rows)
            table_html = f"""
    <h4 style="margin-top:12px; margin-bottom:4px;">{d}</h4>
    <table cellspacing="0" cellpadding="0" style="border-collapse:collapse; border:1px solid #e5e7eb; margin-top:4px;">
      <thead>
        <tr style="background-color:#f9fafb;">
          <th style="padding:4px 8px; text-align:left;">Time (UTC)</th>
          <th style="padding:4px 8px; text-align:left;">Address</th>
          <th style="padding:4px 8px; text-align:left;">Symbol</th>
          <th style="padding:4px 8px; text-align:center;">Side</th>
          <th style="padding:4px 8px; text-align:right;">Size</th>
          <th style="padding:4px 8px; text-align:right;">Price</th>
          <th style="padding:4px 8px; text-align:right;">Notional</th>
          <th style="padding:4px 8px; text-align:right;">Realized PnL</th>
          <th style="padding:4px 8px; text-align:right;">Fee</th>
        </tr>
      </thead>
      <tbody>
        {rows_html}
      </tbody>
    </table>"""
            day_tables.append(table_html)

        top_trades_html = "".join(day_tables)

        body = ""

    # Build optional sections separately to avoid nested f-string issues
    summary_section = ""
    if summary_rows_html:
        summary_section = f"""
    <h3 style="margin-top:16px; margin-bottom:4px;">Summary by Address</h3>
    <table cellspacing="0" cellpadding="0" style="border-collapse:collapse; border:1px solid #e5e7eb; margin-top:4px;">
      <thead>
        <tr style="background-color:#f3f4f6;">
          <th style="padding:4px 8px; text-align:left;">Address</th>
          <th style="padding:4px 8px; text-align:right;">Trades</th>
          <th style="padding:4px 8px; text-align:right;">Symbols</th>
          <th style="padding:4px 8px; text-align:right;">Gross Notional</th>
          <th style="padding:4px 8px; text-align:right;">Realized PnL</th>
          <th style="padding:4px 8px; text-align:right;">Fees</th>
        </tr>
      </thead>
      <tbody>
        {summary_rows_html}
      </tbody>
    </table>"""

    top_trades_section = ""
    if top_trades_html:
        top_trades_section = f"""
    <h3 style="margin-top:16px; margin-bottom:4px;">Top Trades by Notional</h3>
    <table cellspacing="0" cellpadding="0" style="border-collapse:collapse; border:1px solid #e5e7eb; margin-top:4px;">
      <thead>
        <tr style="background-color:#f3f4f6;">
          <th style="padding:4px 8px; text-align:left;">Time (UTC)</th>
          <th style="padding:4px 8px; text-align:left;">Address</th>
          <th style="padding:4px 8px; text-align:left;">Symbol</th>
          <th style="padding:4px 8px; text-align:center;">Side</th>
          <th style="padding:4px 8px; text-align:right;">Size</th>
          <th style="padding:4px 8px; text-align:right;">Price</th>
          <th style="padding:4px 8px; text-align:right;">Notional</th>
          <th style="padding:4px 8px; text-align:right;">Realized PnL</th>
          <th style="padding:4px 8px; text-align:right;">Fee</th>
        </tr>
      </thead>
      <tbody>
        {top_trades_html}
      </tbody>
    </table>"""

    html = f"""<html>
  <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; font-size: 14px; color: #111827;">
    <h2 style="margin-bottom:4px;">MAICRO: Trades in the Last 3 Days</h2>
    <p style="margin-top:0; color:#6b7280;">
      Window (UTC): <b>{since:%Y-%m-%d %H:%M:%S}</b> → <b>{now:%Y-%m-%d %H:%M:%S}</b>
    </p>
    {body}
    {summary_section}
    {top_trades_section}
  </body>
</html>"""
    return html


def send_email(subject: str, text_body: str, html_body: str) -> None:
    if not RESEND_API_KEY:
        print("RESEND_API_KEY not set; skipping trades-last24h email send.")
        return

    print(f"Sending trades-last3d email to {TO_EMAIL}...")
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
    print("[trades_last24h_daily] Starting...")
    df, since, now = _load_trades_last_24h()

    print(
        f"Loaded {len(df)} trades from maicro_monitors.trades "
        f"for window {since:%Y-%m-%d %H:%M:%S} → {now:%Y-%m-%d %H:%M:%S} (UTC)."
    )

    text_body = format_email_text(df, since, now)
    html_body = format_email_html(df, since, now)

    subject = "[MAICRO DAILY] Trades in the Last 3 Days"

    print("----- EMAIL BODY (text) BEGIN -----")
    print(text_body)
    print("----- EMAIL BODY (text) END -----")

    send_email(subject, text_body, html_body)
    print("[trades_last24h_daily] Done.")


if __name__ == "__main__":
    main()
