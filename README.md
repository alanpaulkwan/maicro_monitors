# Maicro Monitors

Hyperliquid trading monitoring and data ingestion system.

## 🚀 Current Architecture (Local-First)

We have consolidated individual monitors into a single orchestrator that runs locally and syncs to the cloud.

**See [plan.md](plan.md) for the detailed status and roadmap.**

### Key Scripts

- **`scripts/orchestrate_monitors.py`**: The main ingestion engine. Runs locally, collects all data (Trades, Orders, Account, OHLCV, etc.), and writes to the Local ClickHouse.
- **`scripts/sync_to_remote.py`**: Synchronizes data from Local ClickHouse to Remote Cloud ClickHouse.
- **`scripts/run_monitors_and_sync.sh`**: Wrapper script for cron jobs (runs Orchestrator + Sync).
- **`05_pnl_calculator/pnl_calculator.py`**: Computes realized PnL, funding, unrealized PnL, and NAV PnL from recent snapshots.
- **`05_tracking_error/tracking_error_calculator.py`**: Mirrors the ipynb methodology; compares live account returns vs model targets using `maicro_logs.live_account`, `maicro_logs.positions_jianan_v6`, and `maicro_monitors.candles` (forward returns). Defaults to 60-day lookback; override with `--lookback`.

### Setup

1.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Configure**:
    Ensure `config/settings.py` has the correct credentials for both Local and Remote ClickHouse.

3.  **Initialize DB**:
    ```bash
    python3 scripts/init_db.py
    ```

4.  **Run Manually**:
    ```bash
    ./scripts/run_monitors_and_sync.sh
    ```

5.  **Install Cron**:
    ```bash
    python3 scripts/generate_cron.py
    ```

### PnL & Tracking Error (ipynb-aligned)

- **Tracking Error** (default last 60 days):
  ```bash
  python3 05_tracking_error/tracking_error_calculator.py --lookback 60
  ```
  Uses the same tables as the notebook: live NAV from `maicro_logs.live_account`, model targets from `maicro_logs.positions_jianan_v6`, and forward returns from `maicro_monitors.candles` (`interval='1d'`). Results are stored in `maicro_monitors.tracking_error` (daily + 7d rolling).

- **PnL Calculator** (recent window adjustable via code param):
  ```bash
  python3 05_pnl_calculator/pnl_calculator.py
  ```
  Aggregates realized, unrealized, funding, and NAV PnL using recent trades, funding payments, positions snapshots, account snapshots, and candles.

## 📂 Legacy Modules

The numbered folders (`01_trade_logger`, etc.) contain the original logic but are now superseded by the orchestrator script. They are kept for reference.
