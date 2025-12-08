"""
Email-related scheduled jobs.

This package is intended to hold small, single-purpose scripts that are
invoked via cron, for example:
  - alerts/system_health_alert.py
  - daily/targets_vs_actuals_daily.py
  - daily/table_staleness_daily.py

Each module should define a `main()` entrypoint and be runnable as a
standalone script, similar to other files in `scheduled_processes/`.
"""
