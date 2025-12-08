#!/usr/bin/env python3
import sys
import os
import requests
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.clickhouse_client import query_df

# Resend API key must come from environment (no default in repo)
RESEND_API_KEY = os.getenv("RESEND_API_KEY")
FROM_EMAIL = "Gemini <gemini@resend.dev>" # Updated from address as per context
TO_EMAIL = "alanpaulkwan@gmail.com"

def generate_report_body():
    lines = []
    lines.append("DAILY TRADING SUMMARY")
    lines.append("=====================")
    
    # 1. Recent Trades
    lines.append("\n[Recent Trades (Last 24h)]")
    # Using existing query_df for simplicity for now.
    # This assumes closedPnl will be non-null when a trade closes a position.
    df_trades = query_df("""
        SELECT count(*) as count, sum(abs(sz * px)) as volume, sum(closedPnl) as pnl 
        FROM maicro_monitors.trades 
        WHERE time > now() - INTERVAL 24 HOUR
    """)
    if not df_trades.empty and df_trades.iloc[0]['count'] > 0:
        row = df_trades.iloc[0]
        lines.append(f"Count: {row['count']}")
        lines.append(f"Volume: ${row['volume']:,.2f}")
        lines.append(f"Realized PnL (from trades): ${row['pnl']:,.2f}")
    else:
        lines.append("No trades recorded in last 24h.")

    # 2. Open Orders
    lines.append("\n[Open Orders Snapshot]")
    df_orders = query_df("SELECT count(*) as count FROM maicro_monitors.orders WHERE status='open'")
    if not df_orders.empty:
        lines.append(f"Open Orders: {df_orders.iloc[0]['count']}")
    
    # 3. PnL Summary (from PnL Calculator)
    lines.append("\n[PnL Summary]")
    try:
import traceback
from 05_pnl_calculator.pnl_calculator import calculate_pnl, load_trades, load_funding_payments, load_positions, load_prices
        pnl_results = pnl_calculator.calculate_pnl(
            pnl_calculator.load_trades(),
            pnl_calculator.load_funding_payments(),
            pnl_calculator.load_positions(),
            pnl_calculator.load_prices()
        )
        for k, v in pnl_results.items():
            lines.append(f"{k}: ${v:,.2f}")
    except Exception as e:
        lines.append(f"Error calculating PnL: {e}")
        traceback.print_exc()

    # 4. Tracking Error (Latest from DB)
    lines.append("\n[Tracking Error (Latest)]")
    df_te = query_df("SELECT date, te_daily, te_rolling_7d FROM maicro_monitors.tracking_error ORDER BY date DESC LIMIT 1")
    if not df_te.empty:
        row = df_te.iloc[0]
        lines.append(f"Date: {row['date']}")
        lines.append(f"Daily TE: {row['te_daily']:.6f}")
        lines.append(f"7d Rolling TE: {row['te_rolling_7d']:.6f}")
    else:
        lines.append("No tracking error data available.")

    lines.append("\n=====================")
    lines.append("End of Report")
    
    return "\n".join(lines)

def send_email(subject, body):
    print(f"Sending email to {TO_EMAIL}...")
    url = "https://api.resend.com/emails"
    headers = {
        "Authorization": f"Bearer {RESEND_API_KEY}",
        "Content-Type": "application/json"
    }
    data = {
        "from": FROM_EMAIL,
        "to": [TO_EMAIL],
        "subject": subject,
        "text": body
    }
    
    try:
        response = requests.post(url, json=data, headers=headers)
        response.raise_for_status()
        print("Email sent successfully!")
        print(response.json())
    except Exception as e:
        print(f"Failed to send email: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(e.response.text)

def main():
    subject = f"Maicro Monitors Daily Summary - {datetime.now().date()}"
    body = generate_report_body()
    
    # Print to stdout for logging
    print(f"Subject: {subject}")
    print(body)
    print("-" * 20)
    
    # Send via Resend
    send_email(subject, body)

if __name__ == "__main__":
    main()
