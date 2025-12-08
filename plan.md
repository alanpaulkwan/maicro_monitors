# Maicro Monitors: Hyperliquid Trading Accountability System

**Purpose**: Track lead trader execution quality by comparing target weights vs actual positions, measure tracking error over N days, and provide comprehensive monitoring infrastructure.

---

## 🚀 Current Status & Handoff (Dec 8, 2025)

**Architecture Shift**: We have moved from individual scripts to a consolidated **Local-First** architecture.
1.  **Ingestion**: `scripts/orchestrate_monitors.py` runs locally on `chenlin04`. It collects data (Account, Positions, Trades, Orders, Funding, Ledger, OHLCV) and writes to the **Local ClickHouse** instance.
2.  **Synchronization**: `scripts/sync_to_remote.py` runs immediately after ingestion. It pushes new data from **Local ClickHouse** to **Remote Cloud ClickHouse** (Maicro Cloud).
3.  **Orchestration**: `scripts/run_monitors_and_sync.sh` wraps both steps and is scheduled via cron.

### ✅ Completed
- [x] **Local DB Setup**: User `maicrobot` created, schema initialized locally.
- [x] **Orchestrator**: Consolidated all monitors into `scripts/orchestrate_monitors.py`.
- [x] **Sync Script**: `scripts/sync_to_remote.py` implements incremental sync (Local -> Remote) for `maicro_monitors`, `maicro_logs`, `binance`, and `hyperliquid` tables.
- [x] **Cron Setup**: `scripts/generate_cron.py` generates the crontab entry for the combined workflow.

### 📋 Next Steps (The Plan)

1.  **Test Downward Sync**
    - Verify `scripts/sync_to_remote.py` is correctly pushing all tables, especially `maicro_logs.positions_jianan_v6` (recently fixed).
    - Ensure data integrity on the Remote Cloud ClickHouse.

2.  **PnL Calculator**
    - Develop a robust PnL calculation engine.
    - Needs to handle realized vs unrealized PnL, funding payments, and fee adjustments.
    - Should run on the Remote DB (or Local and synced) to power dashboards.

3.  **Tracking Error Calculator**
    - Implement `05_tracking_error` logic.
    - Compare `target_weights` (from strategy logs) vs `actual_weights` (from `positions_snapshots`).
    - Calculate daily and rolling tracking error.

4.  **Fix Summary Emails**
    - Decide on the number and content of emails (e.g., Daily PnL, Weekly Risk Report).
    - Integrate `resend.dev` for reliable delivery (Skill: `~/maestral/zz_agent_skills/email.md`).
    - Ensure analytics are built *first* so emails contain meaningful data.

5.  **Design Dashboard**
    - Build a comprehensive dashboard (Streamlit or similar).
    - **Tabs**:
        - **Overview**: High-level PnL, AUM, Exposure.
        - **Positions**: Real-time (synced) positions vs Targets.
        - **Trades**: Recent execution history.
        - **Tracking Error**: Visualizations of TE over time.
        - **System Health**: Sync latency, monitor heartbeat.

---

## 📂 Project Structure (Updated)

```
maicro_monitors/
├── plan.md                           # This document
├── config/
│   ├── settings.py                   # Dual Config: CLICKHOUSE_LOCAL_CONFIG & CLICKHOUSE_REMOTE_CONFIG
│   └── logging_config.py
├── scripts/
│   ├── orchestrate_monitors.py       # MAIN INGESTOR: Runs all monitors, writes to Local DB.
│   ├── sync_to_remote.py             # MAIN SYNC: Pushes Local DB -> Remote DB.
│   ├── run_monitors_and_sync.sh      # Wrapper for Cron.
│   ├── generate_cron.py              # Helper to setup cron.
│   └── init_db.py                    # Schema setup.
├── modules/
│   ├── hyperliquid_client.py
│   ├── clickhouse_client.py
│   └── buffer_manager.py             # Buffers data for batch insertion.
├── reports/
│   └── daily_summary_email.py        # (To be updated with new analytics)
├── 05_tracking_error/                # (To be implemented)
└── 06_dashboards/                    # (To be implemented)
```

---

## 🛑 Deprecated / Legacy (Reference Only)

*The following modules have been consolidated into `scripts/orchestrate_monitors.py` but kept for logic reference.*

- `01_trade_logger/`
- `02_order_monitor/`
- `03_ohlcv_puller/`
- `04_data_feed_latency/`

---

## Executive Summary (Original)

⚠️ Credentials: swap to non-admin ClickHouse user. The current defaults in `config/settings.py` include a privileged user; replace with a least-privilege account (read-only for dashboards, scoped write for ingesters) and override via env vars. Also consider primary/secondary endpoints for redundancy.

This system monitors Hyperliquid perpetual trading operations through:
1. Real-time trade logging to ClickHouse
2. Order state monitoring
3. Daily OHLCV price pulls
4. Data feed latency testing
5. **Tracking error estimation**: comparing `target_weights` (model signals) vs `actual_weights` (live positions)
