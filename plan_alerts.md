# Alert System Improvement Plan

**Created:** 2025-12-08  
**Status:** Draft

---

## Current State Analysis

### What Exists
- `ops/check_alerts.py` — Stale data checks + tracking error alerts via email (Resend API)
- Monitors: account, positions, trades, orders, funding, ledger, candles
- Tracking error calculator comparing live vs paper portfolio

### Critical Issues to Fix

| Issue | Severity | Fix |
|-------|----------|-----|
| Hardcoded API key in source | 🔴 Critical | Remove default fallback, env-only |
| No alert deduplication | 🔴 Critical | State file to suppress repeat alerts |
| No ADL detection | 🔴 Critical | Check `dir` field in fills for "Adl" |
| Silent DB failures | 🟡 Medium | Separate connectivity alert |
| No severity levels | 🟡 Medium | Add INFO/WARN/CRITICAL |
| Single channel (email only) | 🟡 Medium | Add Slack webhook |
| Hardcoded thresholds | 🟢 Low | Move to settings.py |

---

## ADL (Auto-Deleverage) Detection

### How to Detect ADLs

The Hyperliquid `userFills` endpoint returns a `dir` field with values:
- `"Open Long"` / `"Open Short"` — Normal open
- `"Close Long"` / `"Close Short"` — Normal close  
- `"Adl"` — **Auto-deleveraging event** (forced position closure)

**Detection logic:**
```python
def detect_adl_events(fills_df):
    """Detect ADL events from fills data."""
    if fills_df.empty or 'dir' not in fills_df.columns:
        return pd.DataFrame()
    
    adl_fills = fills_df[fills_df['dir'].str.contains('Adl', case=False, na=False)]
    return adl_fills
```

### ADL Alert Content
When ADL detected, alert should include:
- Timestamp of ADL event
- Coin affected
- Position size closed
- Closed PnL (realized loss)
- Entry price vs exit price

---

## Email Strategy

### Proposed Email Types (3 emails)

#### 1. 🔴 **CRITICAL ALERT** (Immediate)
**Trigger:** ADL event, liquidation proximity (<5%), DB down, stale live data >1hr

**Subject:** `[MAICRO CRITICAL] {issue_count} Critical Issues Detected`

**Content:**
```
CRITICAL ALERT - Immediate Attention Required
=============================================
Timestamp: 2025-12-08 04:30:00 UTC

ISSUES DETECTED:

[ADL EVENT]
• Coin: ETH
• Time: 2025-12-08 04:28:15 UTC
• Size Closed: -2.5 ETH
• Entry Price: $3,850.00
• Exit Price: $3,720.00  
• Realized Loss: -$325.00

[LIQUIDATION PROXIMITY]
• Coin: SOL
• Current Price: $180.50
• Liquidation Price: $172.00
• Distance: 4.7%

ACTION REQUIRED: Review positions immediately.
```

#### 2. 🟡 **DAILY DIGEST** (Once daily, 08:00 UTC)
**Trigger:** Scheduled, regardless of issues

**Subject:** `[MAICRO DAILY] Portfolio Summary - {date}`

**Content:**
```
DAILY PORTFOLIO SUMMARY
=======================
Date: 2025-12-08
Account: 0x17f9...7206

PERFORMANCE
-----------
• Account Value: $125,430.50
• Daily PnL: +$1,250.30 (+1.01%)
• 7-Day PnL: +$3,420.00 (+2.80%)
• Tracking Error (7d): 2.3%

POSITIONS (5 active)
--------------------
| Coin | Size    | Entry   | Unrealized | Leverage |
|------|---------|---------|------------|----------|
| BTC  | 0.5     | $97,500 | +$450.00   | 3.2x     |
| ETH  | 5.0     | $3,800  | +$125.00   | 2.8x     |
| SOL  | 50.0    | $175.00 | -$75.00    | 4.1x     |
...

FUNDING (24h)
-------------
• Total Funding: -$45.30
• Top Payer: ETH (-$32.10)

ADL EVENTS (24h): None ✓

WARNINGS
--------
• Stale candle data: binance.bn_spot_klines (35 min ago)
• Tracking error elevated: 2.3% > 2.0% threshold

DATA FRESHNESS
--------------
✓ live_account: 2 min ago
✓ live_positions: 2 min ago
⚠ bn_spot_klines: 35 min ago
✓ targets: 4 hrs ago
```

#### 3. 🟢 **WEEKLY REPORT** (Once weekly, Monday 08:00 UTC)
**Trigger:** Scheduled

**Subject:** `[MAICRO WEEKLY] Week {week_num} Performance Report`

**Content:**
```
WEEKLY PERFORMANCE REPORT
=========================
Week: 2025-W49 (Dec 2-8)
Account: 0x17f9...7206

SUMMARY
-------
• Starting Value: $120,000.00
• Ending Value: $125,430.50
• Net PnL: +$5,430.50 (+4.53%)
• Sharpe (annualized): 2.1

PNL BREAKDOWN
-------------
• Realized PnL: +$3,200.00
• Unrealized PnL: +$500.50
• Funding Costs: -$270.00
• Trading Fees: -$45.00

TRADING ACTIVITY
----------------
• Total Trades: 47
• Buy Volume: $145,000
• Sell Volume: $142,000
• Avg Trade Size: $3,085

TRACKING ERROR
--------------
• Weekly TE: 1.8%
• Live Return: +4.53%
• Paper Return: +4.35%
• Difference: +0.18%

ADL EVENTS: 0 ✓
LIQUIDATIONS: 0 ✓

RISK METRICS
------------
• Max Drawdown: -1.2%
• Max Leverage Used: 4.5x
• Closest to Liquidation: SOL (8.2%)
```

---

## Slack Integration

### Setup
1. Create Slack App → Incoming Webhooks
2. Add webhook URL to environment: `SLACK_WEBHOOK_URL`

### Implementation
```python
import requests

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

def send_slack_alert(message: str, severity: str = "warning"):
    """Send alert to Slack channel."""
    if not SLACK_WEBHOOK_URL:
        print("SLACK_WEBHOOK_URL not configured, skipping Slack alert")
        return
    
    # Color coding
    colors = {
        "critical": "#FF0000",  # Red
        "warning": "#FFA500",   # Orange
        "info": "#36A64F"       # Green
    }
    
    payload = {
        "attachments": [{
            "color": colors.get(severity, "#808080"),
            "text": message,
            "footer": "Maicro Monitors",
            "ts": int(time.time())
        }]
    }
    
    try:
        response = requests.post(SLACK_WEBHOOK_URL, json=payload)
        response.raise_for_status()
    except Exception as e:
        print(f"Failed to send Slack alert: {e}")
```

### Slack Alert Format

**Critical (ADL/Liquidation):**
```
🚨 *CRITICAL: ADL Event Detected*
• Coin: ETH
• Size: -2.5 ETH
• Loss: -$325.00
• Time: 04:28 UTC
```

**Warning (Stale Data):**
```
⚠️ *WARNING: Stale Data*
• Table: binance.bn_spot_klines
• Last Update: 45 min ago
• Threshold: 30 min
```

---

## Alert Deduplication

### State File Approach
```python
import json
from pathlib import Path

ALERT_STATE_FILE = Path("data/alert_state.json")

def load_alert_state():
    if ALERT_STATE_FILE.exists():
        return json.loads(ALERT_STATE_FILE.read_text())
    return {"sent_alerts": {}}

def save_alert_state(state):
    ALERT_STATE_FILE.parent.mkdir(exist_ok=True)
    ALERT_STATE_FILE.write_text(json.dumps(state, indent=2))

def should_send_alert(alert_key: str, cooldown_minutes: int = 60):
    """Check if alert should be sent (not sent recently)."""
    state = load_alert_state()
    last_sent = state["sent_alerts"].get(alert_key)
    
    if last_sent:
        elapsed = (datetime.now() - datetime.fromisoformat(last_sent)).total_seconds() / 60
        if elapsed < cooldown_minutes:
            return False
    
    # Update state
    state["sent_alerts"][alert_key] = datetime.now().isoformat()
    save_alert_state(state)
    return True
```

### Dedup Keys
- Stale data: `stale:{table_name}` — 60 min cooldown
- High TE: `tracking_error:high` — 4 hour cooldown  
- ADL: `adl:{coin}:{timestamp}` — No cooldown (always send)
- Liquidation proximity: `liq_warning:{coin}` — 30 min cooldown

---

## New Alert Checks to Add

### 1. ADL Detection
```python
def check_adl_events():
    """Check for ADL events in recent fills."""
    alerts = []
    
    # Get fills from last 24h
    df = query_df("""
        SELECT * FROM maicro_monitors.trades 
        WHERE time >= now() - INTERVAL 24 HOUR
        AND dir LIKE '%Adl%'
    """)
    
    if not df.empty:
        for _, row in df.iterrows():
            alerts.append({
                "severity": "CRITICAL",
                "type": "ADL",
                "coin": row['coin'],
                "time": row['time'],
                "size": row['sz'],
                "price": row['px'],
                "closed_pnl": row['closedPnl']
            })
    
    return alerts
```

### 2. Liquidation Proximity
```python
def check_liquidation_proximity(threshold_pct=0.10):
    """Check if any position is within threshold of liquidation."""
    alerts = []
    
    # Get latest positions
    df = query_df("""
        SELECT coin, szi, entryPx, liquidationPx, positionValue
        FROM maicro_monitors.positions_snapshots
        WHERE timestamp = (SELECT max(timestamp) FROM maicro_monitors.positions_snapshots)
    """)
    
    # Get current prices
    prices = query_df("""
        SELECT coin, close as price
        FROM maicro_monitors.candles
        WHERE interval = '1h'
        AND ts = (SELECT max(ts) FROM maicro_monitors.candles WHERE interval = '1h')
    """)
    
    if df.empty or prices.empty:
        return alerts
    
    merged = df.merge(prices, on='coin')
    
    for _, row in merged.iterrows():
        if row['liquidationPx'] and row['liquidationPx'] > 0:
            distance = abs(row['price'] - row['liquidationPx']) / row['price']
            if distance < threshold_pct:
                alerts.append({
                    "severity": "CRITICAL",
                    "type": "LIQUIDATION_PROXIMITY",
                    "coin": row['coin'],
                    "current_price": row['price'],
                    "liquidation_price": row['liquidationPx'],
                    "distance_pct": distance * 100
                })
    
    return alerts
```

### 3. Drawdown Alert
```python
def check_drawdown(threshold_pct=0.10):
    """Check if account has drawn down more than threshold from peak."""
    alerts = []
    
    df = query_df("""
        SELECT timestamp, accountValue
        FROM maicro_monitors.account_snapshots
        WHERE timestamp >= now() - INTERVAL 7 DAY
        ORDER BY timestamp
    """)
    
    if df.empty:
        return alerts
    
    peak = df['accountValue'].cummax()
    drawdown = (df['accountValue'] - peak) / peak
    current_dd = drawdown.iloc[-1]
    
    if current_dd < -threshold_pct:
        alerts.append({
            "severity": "WARNING",
            "type": "DRAWDOWN",
            "current_value": df['accountValue'].iloc[-1],
            "peak_value": peak.iloc[-1],
            "drawdown_pct": abs(current_dd) * 100
        })
    
    return alerts
```

---

## Implementation Order

### Phase 1: Security & Stability (Priority)
- [ ] Remove hardcoded API keys from source
- [ ] Add alert deduplication with state file
- [ ] Add ADL detection check
- [ ] Add liquidation proximity check

### Phase 2: Multi-Channel & Severity
- [ ] Add Slack webhook integration
- [ ] Implement severity levels (CRITICAL/WARNING/INFO)
- [ ] Route CRITICAL to both Slack + Email
- [ ] Route WARNING to Slack only (optional email)

### Phase 3: Scheduled Reports
- [ ] Implement daily digest email (08:00 UTC)
- [ ] Implement weekly report email (Monday 08:00 UTC)
- [ ] Add cron entries for scheduled emails

### Phase 4: Enhanced Metrics
- [ ] Add drawdown monitoring
- [ ] Add position concentration alerts
- [ ] Add unusual activity detection (trade spike)
- [ ] Add funding rate alerts (unusually high/low)

---

## Configuration (settings.py additions)

```python
# Alert Configuration
ALERT_CONFIG = {
    # Thresholds
    "tracking_error_threshold": float(os.getenv("TE_THRESHOLD", "0.05")),
    "liquidation_proximity_threshold": float(os.getenv("LIQ_THRESHOLD", "0.10")),
    "drawdown_threshold": float(os.getenv("DD_THRESHOLD", "0.10")),
    
    # Stale data thresholds (minutes)
    "stale_live_data_minutes": int(os.getenv("STALE_LIVE_MIN", "15")),
    "stale_market_data_minutes": int(os.getenv("STALE_MARKET_MIN", "30")),
    "stale_targets_hours": int(os.getenv("STALE_TARGETS_HR", "26")),
    
    # Channels
    "slack_webhook_url": os.getenv("SLACK_WEBHOOK_URL"),
    "resend_api_key": os.getenv("RESEND_API_KEY"),  # No default!
    "alert_email": os.getenv("ALERT_EMAIL", "alanpaulkwan@gmail.com"),
    
    # Deduplication cooldowns (minutes)
    "cooldown_stale_data": 60,
    "cooldown_tracking_error": 240,
    "cooldown_liquidation_warning": 30,
}
```

---

## File Structure After Implementation

```
ops/
├── check_alerts.py          # Main alert runner (refactored)
├── alert_checks/
│   ├── __init__.py
│   ├── stale_data.py        # Stale data checks
│   ├── tracking_error.py    # TE checks
│   ├── adl_detection.py     # ADL event detection
│   ├── liquidation.py       # Liquidation proximity
│   └── drawdown.py          # Drawdown monitoring
├── alert_channels/
│   ├── __init__.py
│   ├── email.py             # Resend email integration
│   └── slack.py             # Slack webhook integration
├── alert_reports/
│   ├── __init__.py
│   ├── daily_digest.py      # Daily summary email
│   └── weekly_report.py     # Weekly performance report
└── alert_state.json         # Deduplication state (in data/)
```

---

## Questions to Resolve

1. **Email Recipients:** Single email or distribution list?
2. **Slack Channel:** Dedicated #maicro-alerts channel?
3. **On-Call Escalation:** Should critical alerts page someone (PagerDuty)?
4. **Timezone:** Reports in UTC or specific timezone?
5. **Historical ADL Data:** Backfill ADL detection for existing fills?
