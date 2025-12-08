#!/usr/bin/env python3
"""
generate_cron.py
----------------

Prints recommended crontab entries for the `maicro_monitors` scheduled
processes. This is just a convenience wrapper around the contents of
`scheduled_processes/cron.md`.
"""

import os


def main() -> None:
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    print("# Maicro Monitors scheduled processes")
    print("# Add these lines to your crontab (crontab -e)")
    print(f"# REPO_ROOT = {repo_root}")
    print("")

    # 1. Hyperliquid ingest: hourly ping (buffer-only)
    print("# 1. Hyperliquid ingest (buffer-only ping, hourly)")
    print(f"0 * * * * cd {repo_root} && /usr/bin/python3 scheduled_processes/scheduled_ping_hyperliquid.py >> logs/hyperliquid_ping.log 2>&1")
    print("")

    # 1.2 Hyperliquid ingest: 3h flush to chenlin + Cloud
    print("# 1.2 Hyperliquid buffer flush (every 3 hours, dual-target)")
    print(f"0 */3 * * * cd {repo_root} && /usr/bin/python3 scheduled_processes/flush_hyperliquid_buffers.py >> logs/hyperliquid_flush.log 2>&1")
    print("")

    # 2. Daily emails (optional)
    print("# 2. Daily emails (optional)")
    print("# Target vs actual positions")
    print(f"0 8 * * * cd {repo_root} && /usr/bin/python3 scheduled_processes/emails/daily/targets_vs_actuals_daily.py >> logs/email_targets_vs_actuals.log 2>&1")
    print("# Table staleness summary")
    print(f"5 8 * * * cd {repo_root} && /usr/bin/python3 scheduled_processes/emails/daily/table_staleness_daily.py >> logs/email_table_staleness.log 2>&1")
    print("")

    # 3. Cloud → chenlin down-sync
    print("# 3. Cloud → chenlin down-sync (every 6 hours)")
    print(f"0 */6 * * * cd {repo_root} && /usr/bin/python3 scheduled_processes/pull_data_downward_from_cloud.py >> logs/pull_from_cloud.log 2>&1")
    print("")

    print("# Note: Ensure the log directory exists:")
    print(f"#   mkdir -p {os.path.join(repo_root, 'logs')}")


if __name__ == "__main__":
    main()
