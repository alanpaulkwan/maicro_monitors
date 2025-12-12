"""Central configuration for dashboards and data access.
Override via environment variables where possible, and fall back to a local
JSON secrets file that is never committed to git.
"""
import os
import json
from pathlib import Path


def _load_local_secrets() -> dict:
    """Load optional local secrets from config/local_secrets.json (untracked).

    Shape is a simple key/value mapping, typically using the same keys as
    environment variables, e.g.:

        {
          "CLICKHOUSE_LOCAL_PASSWORD": "…",
          "CLICKHOUSE_REMOTE_PASSWORD": "…"
        }
    """
    path = Path(__file__).with_name("local_secrets.json")
    if not path.exists():
        return {}
    try:
        with path.open("r") as f:
            return json.load(f)
    except Exception:
        # Fail closed: if the file is malformed, ignore it rather than crash
        return {}


_LOCAL_SECRETS = _load_local_secrets()


def get_secret(name: str, default: str = "") -> str:
    """Return a secret from env or local_secrets.json.

    Priority:
      1. Environment variable `name`
      2. Entry in config/local_secrets.json using the same key
      3. Provided default
    """
    if name in os.environ:
        return os.environ[name]
    return _LOCAL_SECRETS.get(name, default)

# Hyperliquid endpoints
HYPERLIQUID_INFO_URL = os.getenv("HYPERLIQUID_INFO_URL", "https://api.hyperliquid.xyz/info")
HYPERLIQUID_WS_URL = os.getenv("HYPERLIQUID_WS_URL", "wss://api.hyperliquid.xyz/ws")

def _load_tracked_accounts() -> list:
    """Load tracked accounts from config/tracked_accounts.json"""
    path = Path(__file__).with_name("tracked_accounts.json")
    if not path.exists():
        # Fallback default
        return ["0x17f9d0098111D6Ae0915f980517264F082dB7206"]
    try:
        with path.open("r") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
            return []
    except Exception:
        return ["0x17f9d0098111D6Ae0915f980517264F082dB7206"]

HYPERLIQUID_ADDRESSES = _load_tracked_accounts()
HYPERLIQUID_ADDRESS = os.getenv("HYPERLIQUID_ADDRESS", HYPERLIQUID_ADDRESSES[0] if HYPERLIQUID_ADDRESSES else "0x17f9d0098111D6Ae0915f980517264F082dB7206")

# ClickHouse connection (Remote / Cloud)
# NOTE: passwords are expected to come from environment variables or
# config/local_secrets.json (never hardcoded in the repo).
CLICKHOUSE_REMOTE_CONFIG = {
    "host": os.getenv("CLICKHOUSE_REMOTE_HOST", "ym5ysl9yzf.eu-west-2.aws.clickhouse.cloud"),
    "port": int(os.getenv("CLICKHOUSE_REMOTE_PORT", "9440")),
    "user": os.getenv("CLICKHOUSE_REMOTE_USER", "default"),
    "password": get_secret("CLICKHOUSE_REMOTE_PASSWORD", ""),
    "secure": os.getenv("CLICKHOUSE_REMOTE_SECURE", "true").lower() == "true",
    "database": os.getenv("CLICKHOUSE_REMOTE_DATABASE", "default"),
}

# ClickHouse connection (Local)
# NOTE: passwords are expected to come from environment variables or
# config/local_secrets.json (never hardcoded in the repo).
CLICKHOUSE_LOCAL_CONFIG = {
    "host": os.getenv("CLICKHOUSE_LOCAL_HOST", "chenlin04.fbe.hku.hk"),
    "port": int(os.getenv("CLICKHOUSE_LOCAL_PORT", "9000")),
    "user": os.getenv("CLICKHOUSE_LOCAL_USER", "maicrobot"),
    "password": get_secret("CLICKHOUSE_LOCAL_PASSWORD", ""),
    "secure": os.getenv("CLICKHOUSE_LOCAL_SECURE", "false").lower() == "true",
    # No default database, or default to default
}

# Default to Local for monitors
CLICKHOUSE_CONFIG = CLICKHOUSE_LOCAL_CONFIG

# Default table candidates used by the dashboards (first existing table wins)
TABLE_CANDIDATES = {
    "prices": ["maicro_monitors.candles", "market_data.candles_1m", "market_data.candles_1h"],
    "trades": ["maicro_monitors.trades", "trading.user_fills", "user_fills"],
    "orders": ["maicro_monitors.orders", "trading.order_history"],
    "tracking_error": ["maicro_monitors.tracking_error", "risk.tracking_error_daily"],
    "account": ["maicro_monitors.account_snapshots", "maicro_logs.live_account"],
    "positions": ["maicro_monitors.positions_snapshots"],
}
