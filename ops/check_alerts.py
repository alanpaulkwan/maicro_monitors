#!/usr/bin/env python3
"""
Operational Alerts Script
Checks for:
1. Stale data in critical tables.
2. High tracking error.
3. System health.

Intended to be run frequently (e.g., every 15-60 mins) via cron.
"""
import sys
import os
import requests
from datetime import datetime, timedelta
import pandas as pd

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.clickhouse_client import query_df
from config import settings

# --- Configuration ---

RESEND_API_KEY = os.getenv("RESEND_API_KEY", "re_MLXxTsvc_6JpYMDMB3QGgDgU97s8C8dxV")
FROM_EMAIL = "Gemini Alerts <gemini@resend.dev>"
TO_EMAIL = "alanpaulkwan@gmail.com"

# Thresholds
TRACKING_ERROR_THRESHOLD = 0.05  # 5%
STALE_THRESHOLD_MINUTES_LIVE = 15
STALE_THRESHOLD_MINUTES_MARKET = 30
STALE_THRESHOLD_HOURS_FUNDING = 9
STALE_THRESHOLD_HOURS_TARGETS = 26

# Tables to Monitor
# Format: (schema.table, time_column, stale_threshold_timedelta, optional_sql_filter)
MONITORED_TABLES = [
    # Live Data (High Frequency)
    ("maicro_logs.live_account", "ts", timedelta(minutes=STALE_THRESHOLD_MINUTES_LIVE), None),
    ("maicro_logs.live_positions", "ts", timedelta(minutes=STALE_THRESHOLD_MINUTES_LIVE), None),
    
    # Hyperliquid Data
    ("hyperliquid.asset_ctx", "time", timedelta(minutes=STALE_THRESHOLD_MINUTES_MARKET), None),
    ("hyperliquid.market_data", "time", timedelta(minutes=STALE_THRESHOLD_MINUTES_MARKET), None),
    
    # Binance Data
    ("binance.bn_spot_klines", "timestamp", timedelta(minutes=STALE_THRESHOLD_MINUTES_MARKET), "symbol='BTCUSDT' "), # Check BTC to ensure feed is alive
    ("binance.bn_perp_klines", "timestamp", timedelta(minutes=STALE_THRESHOLD_MINUTES_MARKET), "symbol='BTCUSDT'"),
    ("binance.bn_funding_rates", "fundingTime", timedelta(hours=STALE_THRESHOLD_HOURS_FUNDING), "symbol='BTCUSDT'"),

    # Targets (Daily)
    ("maicro_logs.positions_jianan_v6", "inserted_at", timedelta(hours=STALE_THRESHOLD_HOURS_TARGETS), None),
]

# --- Helper Functions ---

def send_alert_email(subject, body):
    print(f"Sending ALERT to {TO_EMAIL}...")
    url = "https://api.resend.com/emails"
    headers = {
        "Authorization": f"Bearer {RESEND_API_KEY}",
        "Content-Type": "application/json"
    }
    data = {
        "from": FROM_EMAIL,
        "to": [TO_EMAIL],
        "subject": f"[MAICRO ALERT] {subject}",
        "text": body
    }
    
    try:
        response = requests.post(url, json=data, headers=headers)
        response.raise_for_status()
        print("Alert sent successfully!")
    except Exception as e:
        print(f"Failed to send alert: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(e.response.text)

def check_stale_data():
    alerts = []
    print("Checking for stale data...")
    
    for table, time_col, threshold, filter_sql in MONITORED_TABLES:
        try:
            where_clause = f"WHERE {filter_sql}" if filter_sql else ""
            query = f"SELECT max({time_col}) as last_time FROM {table} {where_clause}"
            df = query_df(query)
            
            if df.empty or pd.isna(df.iloc[0]['last_time']):
                alerts.append(f"CRITICAL: Table {table} is empty or has no time data.")
                continue
            
            raw_time = df.iloc[0]['last_time']
            
            # Handle different time formats
            if isinstance(raw_time, (int, float)):
                # Assume milliseconds if > 1e11 (year 1973), else seconds
                if raw_time > 1e11:
                    last_time = datetime.utcfromtimestamp(raw_time / 1000.0)
                else:
                    last_time = datetime.utcfromtimestamp(raw_time)
            else:
                last_time = pd.to_datetime(raw_time).tz_localize(None) # Ensure naive for comparison
            
            now = datetime.utcnow()
            diff = now - last_time
            
            # Simple check: if diff is negative, it's not stale.
            # If diff > threshold, it's stale.
            
            if diff > threshold:
                alerts.append(f"STALE: {table} last update was {last_time} (UTC), which is {diff} ago (Threshold: {threshold}).")
            else:
                print(f"OK: {table} - {diff} ago")
                
        except Exception as e:
            alerts.append(f"ERROR: Failed to check {table}: {str(e)}")
            
    return alerts

def check_tracking_error():
    alerts = []
    print("Checking tracking error...")
    try:
        # Get latest tracking error
        df = query_df("SELECT * FROM maicro_monitors.tracking_error ORDER BY date DESC LIMIT 1")
        if df.empty:
            alerts.append("WARNING: No tracking error data found in maicro_monitors.tracking_error.")
            return alerts
            
        row = df.iloc[0]
        te_7d = row.get('te_rolling_7d', 0)
        te_daily = row.get('te_daily', 0)
        date = row['date']
        
        # Check staleness of TE calculation itself (should be yesterday or today)
        # Note: TE is daily.
        
        if te_7d > TRACKING_ERROR_THRESHOLD:
            alerts.append(f"HIGH RISK: 7-Day Tracking Error is {te_7d:.2%} (Threshold: {TRACKING_ERROR_THRESHOLD:.2%}). Date: {date}")
            
        if te_daily > TRACKING_ERROR_THRESHOLD * 2: # Higher tolerance for single day spike
             alerts.append(f"HIGH RISK: Daily Tracking Error is {te_daily:.2%}. Date: {date}")
             
    except Exception as e:
        alerts.append(f"ERROR: Failed to check tracking error: {str(e)}")
        
    return alerts

def main():
    all_alerts = []
    
    # Run Checks
    all_alerts.extend(check_stale_data())
    all_alerts.extend(check_tracking_error())
    
    if all_alerts:
        print("\n!!! ALERTS GENERATED !!!")
        for alert in all_alerts:
            print(alert)
            
        # Send Email
        subject = f"{len(all_alerts)} Operational Issues Detected"
        body = "\n".join(all_alerts)
        send_alert_email(subject, body)
    else:
        print("\nAll systems nominal.")

if __name__ == "__main__":
    main()
