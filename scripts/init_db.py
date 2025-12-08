#!/usr/bin/env python3
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.clickhouse_client import execute

def init_db():
    with open("sql/init_db.sql", "r") as f:
        sql_script = f.read()
    
    statements = sql_script.split(";")
    for stmt in statements:
        if stmt.strip():
            print(f"Executing: {stmt[:50]}...")
            execute(stmt)
    print("Database initialization complete.")

if __name__ == "__main__":
    init_db()
