"""Central configuration for dashboards and data access.
Override via environment variables where possible to avoid hardcoding secrets.
"""
import os

# Hyperliquid endpoints
HYPERLIQUID_INFO_URL = os.getenv("HYPERLIQUID_INFO_URL", "https://api.hyperliquid.xyz/info")
HYPERLIQUID_WS_URL = os.getenv("HYPERLIQUID_WS_URL", "wss://api.hyperliquid.xyz/ws")
HYPERLIQUID_ADDRESS = os.getenv("HYPERLIQUID_ADDRESS", "0x17f9d0098111D6Ae0915f980517264F082dB7206")

# ClickHouse connection (Remote / Cloud)
# NOTE: passwords are expected to come **only** from environment variables.
CLICKHOUSE_REMOTE_CONFIG = {
    "host": os.getenv("CLICKHOUSE_REMOTE_HOST", "ym5ysl9yzf.eu-west-2.aws.clickhouse.cloud"),
    "port": int(os.getenv("CLICKHOUSE_REMOTE_PORT", "9440")),
    "user": os.getenv("CLICKHOUSE_REMOTE_USER", "default"),
    "password": os.getenv("CLICKHOUSE_REMOTE_PASSWORD", ""),
    "secure": os.getenv("CLICKHOUSE_REMOTE_SECURE", "true").lower() == "true",
    "database": os.getenv("CLICKHOUSE_REMOTE_DATABASE", "default"),
}

# ClickHouse connection (Local)
# NOTE: passwords are expected to come **only** from environment variables.
CLICKHOUSE_LOCAL_CONFIG = {
    "host": os.getenv("CLICKHOUSE_LOCAL_HOST", "chenlin04.fbe.hku.hk"),
    "port": int(os.getenv("CLICKHOUSE_LOCAL_PORT", "9000")),
    "user": os.getenv("CLICKHOUSE_LOCAL_USER", "maicrobot"),
    "password": os.getenv("CLICKHOUSE_LOCAL_PASSWORD", ""),
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
