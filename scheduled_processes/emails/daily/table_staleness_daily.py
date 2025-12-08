#!/usr/bin/env python3
"""
Daily email: Maicro tables staleness checker.

Produces an HTML + plain-text table summarizing, for key `maicro_*` tables:
  - last timestamp (UTC)
  - age vs now
  - threshold
  - status: OK / STALE / MISSING / ERROR

Tables covered (by default):
  - maicro_logs.live_account (ts)
  - maicro_logs.live_positions (ts)
  - maicro_logs.positions_jianan_v6 (inserted_at)
  - maicro_monitors.account_snapshots (timestamp)
  - maicro_monitors.positions_snapshots (timestamp)
  - maicro_monitors.trades (time)
  - maicro_monitors.orders (timestamp)
  - maicro_monitors.funding_payments (time)
  - maicro_monitors.ledger_updates (time)
  - maicro_monitors.candles (ts)
  - maicro_monitors.tracking_error (date)

Environment:
  - RESEND_API_KEY   (required to send email)
  - ALERT_EMAIL      (recipient, defaults to alanpaulkwan@gmail.com)
  - ALERT_FROM_EMAIL (optional, default 'Maicro Monitors <alerts@resend.dev>')
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

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


# (table_name, time_column, threshold_timedelta)
TABLES: List[Tuple[str, str, timedelta]] = [
    # Live logs
    ("maicro_logs.live_account", "ts", timedelta(minutes=15)),
    ("maicro_logs.live_positions", "ts", timedelta(minutes=15)),
    ("maicro_logs.positions_jianan_v6", "inserted_at", timedelta(hours=26)),
    # Monitors
    ("maicro_monitors.account_snapshots", "timestamp", timedelta(hours=6)),
    ("maicro_monitors.positions_snapshots", "timestamp", timedelta(hours=6)),
    ("maicro_monitors.trades", "time", timedelta(hours=6)),
    ("maicro_monitors.orders", "timestamp", timedelta(hours=6)),
    ("maicro_monitors.funding_payments", "time", timedelta(hours=12)),
    ("maicro_monitors.ledger_updates", "time", timedelta(hours=12)),
    ("maicro_monitors.candles", "ts", timedelta(hours=6)),
    ("maicro_monitors.tracking_error", "date", timedelta(days=2)),
    # Binance (synced every 6h)
    ("binance.bn_funding_rates", "fundingTime", timedelta(hours=12)),
    ("binance.bn_perp_klines", "timestamp", timedelta(hours=6)),
    ("binance.bn_spot_klines", "timestamp", timedelta(hours=6)),
    ("binance.bn_premium", "timestamp", timedelta(hours=6)),
    ("binance.bn_margin_interest_rates", "timestamp", timedelta(hours=26)),
]


def _format_timedelta(delta: timedelta) -> str:
    total_seconds = int(delta.total_seconds())
    if total_seconds < 0:
        return "0s"
    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes and not days:
        parts.append(f"{minutes}m")
    if not parts:
        parts.append(f"{seconds}s")
    return " ".join(parts)


def _coerce_time(raw: Any) -> Optional[datetime]:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    # Already datetime / date
    if hasattr(raw, "to_pydatetime"):
        try:
            return raw.to_pydatetime().replace(tzinfo=None)
        except Exception:
            pass
    if hasattr(raw, "strftime"):
        # date or datetime
        try:
            val = pd.to_datetime(raw)
            return val.to_pydatetime().replace(tzinfo=None)
        except Exception:
            return None
    # Numeric timestamp (sec or ms)
    try:
        val = float(raw)
    except Exception:
        try:
            dt = pd.to_datetime(raw)
            return dt.to_pydatetime().replace(tzinfo=None)
        except Exception:
            return None
    # Heuristic: ms if big, else seconds
    if val > 1e11:
        return datetime.utcfromtimestamp(val / 1000.0)
    return datetime.utcfromtimestamp(val)


def collect_staleness() -> List[Dict[str, Any]]:
    now = datetime.utcnow()
    rows: List[Dict[str, Any]] = []

    for table, time_col, threshold in TABLES:
        row: Dict[str, Any] = {
            "table": table,
            "time_column": time_col,
            "threshold": threshold,
            "last_time": None,
            "age": None,
            "status": "MISSING",
            "error": "",
        }
        try:
            sql = f"SELECT max({time_col}) AS last_time FROM {table}"
            df = query_df(sql)
            if df.empty or "last_time" not in df.columns or pd.isna(df.iloc[0]["last_time"]):
                row["status"] = "MISSING"
            else:
                last_raw = df.iloc[0]["last_time"]
                last_dt = _coerce_time(last_raw)
                if last_dt is None:
                    row["status"] = "ERROR"
                    row["error"] = f"Unparseable time: {last_raw!r}"
                else:
                    age = now - last_dt
                    row["last_time"] = last_dt
                    row["age"] = age
                    row["status"] = "OK" if age <= threshold else "STALE"
        except Exception as e:
            row["status"] = "ERROR"
            row["error"] = str(e)

        rows.append(row)

    return rows


def format_email_text(rows: List[Dict[str, Any]]) -> str:
    now = datetime.utcnow()
    lines: List[str] = []
    lines.append("MAICRO: Table Staleness Summary")
    lines.append("================================")
    lines.append(f"Reference time (UTC): {now:%Y-%m-%d %H:%M:%S}")
    lines.append("")
    lines.append("Legend: OK = age <= threshold; STALE = age > threshold;")
    lines.append("        MISSING = table empty/no time data; ERROR = query failed or bad time.")
    lines.append("")

    header = f"{'Table':<40} {'Last Time (UTC)':<20} {'Age':<10} {'Threshold':<10} {'Status':<8}"
    lines.append(header)
    lines.append("-" * len(header))

    for r in rows:
        table = r["table"]
        last_time = r["last_time"]
        age = r["age"]
        threshold = r["threshold"]
        status = r["status"]

        last_str = last_time.strftime("%Y-%m-%d %H:%M") if isinstance(last_time, datetime) else "-"
        age_str = _format_timedelta(age) if isinstance(age, timedelta) else "-"
        thr_str = _format_timedelta(threshold)

        lines.append(
            f"{table:<40} {last_str:<20} {age_str:<10} {thr_str:<10} {status:<8}"
        )

    # Append any errors at the bottom
    errors = [r for r in rows if r.get("error")]
    if errors:
        lines.append("")
        lines.append("Errors")
        lines.append("------")
        for r in errors:
            lines.append(f"{r['table']}: {r['error']}")

    return "\n".join(lines)


def format_email_html(rows: List[Dict[str, Any]]) -> str:
    now = datetime.utcnow()

    def status_color(status: str) -> str:
        if status == "OK":
            return "#dcfce7"  # green
        if status == "STALE":
            return "#fee2e2"  # red
        if status == "MISSING":
            return "#fef3c7"  # amber
        if status == "ERROR":
            return "#e5e7eb"  # gray
        return "#ffffff"

    rows_html: List[str] = []
    for r in rows:
        table = r["table"]
        last_time = r["last_time"]
        age = r["age"]
        threshold = r["threshold"]
        status = r["status"]

        last_str = last_time.strftime("%Y-%m-%d %H:%M") if isinstance(last_time, datetime) else "-"
        age_str = _format_timedelta(age) if isinstance(age, timedelta) else "-"
        thr_str = _format_timedelta(threshold)
        color = status_color(status)

        rows_html.append(
            f"<tr style='background-color:{color};'>"
            f"<td style='padding:4px 8px;'>{table}</td>"
            f"<td style='padding:4px 8px;'>{last_str}</td>"
            f"<td style='padding:4px 8px; text-align:right;'>{age_str}</td>"
            f"<td style='padding:4px 8px; text-align:right;'>{thr_str}</td>"
            f"<td style='padding:4px 8px;'>{status}</td>"
            "</tr>"
        )

    html = f"""<html>
  <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; font-size: 14px; color: #111827;">
    <h2 style="margin-bottom:4px;">MAICRO: Table Staleness Summary</h2>
    <p style="margin-top:0; color:#6b7280;">
      Reference time (UTC): <b>{now:%Y-%m-%d %H:%M:%S}</b>
    </p>
    <p style="margin-bottom:4px; color:#6b7280;">
      Legend: <span style="background-color:#dcfce7; padding:2px 4px; border-radius:3px;">OK</span> age ≤ threshold;
      <span style="background-color:#fee2e2; padding:2px 4px; border-radius:3px;">STALE</span> age &gt; threshold;
      <span style="background-color:#fef3c7; padding:2px 4px; border-radius:3px;">MISSING</span> empty / no time;
      <span style="background-color:#e5e7eb; padding:2px 4px; border-radius:3px;">ERROR</span> query failed.
    </p>
    <table cellspacing="0" cellpadding="0" style="border-collapse:collapse; border:1px solid #e5e7eb; margin-top:4px;">
      <thead>
        <tr style="background-color:#f3f4f6;">
          <th style="padding:4px 8px; text-align:left;">Table</th>
          <th style="padding:4px 8px; text-align:left;">Last Time (UTC)</th>
          <th style="padding:4px 8px; text-align:right;">Age</th>
          <th style="padding:4px 8px; text-align:right;">Threshold</th>
          <th style="padding:4px 8px; text-align:left;">Status</th>
        </tr>
      </thead>
      <tbody>
        {''.join(rows_html)}
      </tbody>
    </table>
  </body>
</html>"""
    return html


def send_email(subject: str, text_body: str, html_body: str) -> None:
    if not RESEND_API_KEY:
        print("RESEND_API_KEY not set; skipping email send.")
        return

    print(f"Sending table-staleness email to {TO_EMAIL}...")
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
    print("[table_staleness_daily] Starting...")
    rows = collect_staleness()
    text_body = format_email_text(rows)
    html_body = format_email_html(rows)

    subject = "[MAICRO DAILY] Maicro Table Staleness Summary"

    print("----- EMAIL BODY BEGIN -----")
    print(text_body)
    print("----- EMAIL BODY END -----")

    send_email(subject, text_body, html_body)
    print("[table_staleness_daily] Done.")


if __name__ == "__main__":
    main()
