"""Robust ClickHouse helper using clickhouse-driver (TCP).
Provides unified interface for query, insert, and DDL operations.
"""
import logging
import pandas as pd
from typing import Iterable, Optional, List, Dict, Any, Union
from clickhouse_driver import Client

from config.settings import CLICKHOUSE_CONFIG

logger = logging.getLogger(__name__)

_client_cache = None

def get_client() -> Client:
    """Get a cached ClickHouse client instance."""
    global _client_cache
    if _client_cache is None:
        _client_cache = Client(**CLICKHOUSE_CONFIG)
    return _client_cache

def query_df(sql: str, params: Optional[dict] = None) -> pd.DataFrame:
    """Execute a SELECT query and return a pandas DataFrame."""
    client = get_client()
    try:
        result, columns = client.execute(sql, params=params or {}, with_column_types=True)
        if not columns:
            return pd.DataFrame()
        col_names = [c[0] for c in columns]
        return pd.DataFrame(result, columns=col_names)
    except Exception as e:
        logger.error(f"Query failed: {e}")
        raise

def insert_df(table: str, df: pd.DataFrame):
    """Insert a pandas DataFrame into a table."""
    if df.empty:
        return
    
    client = get_client()
    data = df.to_dict('records')
    columns = ', '.join(df.columns)
    
    try:
        client.execute(f"INSERT INTO {table} ({columns}) VALUES", data)
    except Exception as e:
        logger.error(f"Insert failed for {table}: {e}")
        raise

def execute(sql: str, params: Optional[dict] = None):
    """Execute a DDL or DML statement (CREATE, DROP, ALTER, etc)."""
    client = get_client()
    return client.execute(sql, params=params or {})

def table_exists(full_table_name: str) -> bool:
    """Check if a table exists."""
    client = get_client()
    try:
        if "." in full_table_name:
            db, table = full_table_name.split(".", 1)
        else:
            db = CLICKHOUSE_CONFIG.get('database', 'default')
            table = full_table_name
            
        sql = "SELECT 1 FROM system.tables WHERE database = %(db)s AND name = %(table)s LIMIT 1"
        result = client.execute(sql, params={"db": db, "table": table})
        return bool(result)
    except Exception:
        return False

def first_existing(tables: Iterable[str]) -> Optional[str]:
    """Return the first table from the list that exists."""
    for t in tables:
        if table_exists(t):
            return t
    return None
