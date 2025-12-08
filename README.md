# Maicro Monitors

Hyperliquid trading monitoring and data ingestion system.

## 🚀 Current Architecture (Local-First)

We have consolidated individual monitors into a single orchestrator that runs locally and syncs to the cloud.

**See [plan.md](plan.md) for the detailed status and roadmap.**

### Key Scripts

- **`scripts/orchestrate_monitors.py`**: The main ingestion engine. Runs locally, collects all data (Trades, Orders, Account, OHLCV, etc.), and writes to the Local ClickHouse.
- **`scripts/sync_to_remote.py`**: Synchronizes data from Local ClickHouse to Remote Cloud ClickHouse.
- **`scripts/run_monitors_and_sync.sh`**: Wrapper script for cron jobs (runs Orchestrator + Sync).

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

## 📂 Legacy Modules

The numbered folders (`01_trade_logger`, etc.) contain the original logic but are now superseded by the orchestrator script. They are kept for reference.

