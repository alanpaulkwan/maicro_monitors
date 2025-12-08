#!/usr/bin/env bash
set -euo pipefail
# Run the dashboard with an optional enforced password.
# By default, the script requires MAICRO_DASH_PASSWORD to be set. Set FORCE_NO_PASSWORD=1 to run without a password (not recommended).

if [[ -z "${MAICRO_DASH_PASSWORD:-}" && -z "${FORCE_NO_PASSWORD:-}" ]]; then
  echo "MAICRO_DASH_PASSWORD is not set. Please set it to secure the dashboard, or set FORCE_NO_PASSWORD=1 to disable the password gate."
  echo "Example: MAICRO_DASH_PASSWORD=MyStrongPassword ./run_dashboard.sh"
  exit 1
fi

echo "Starting Streamlit dashboard (port 8501)..."
cd "$(dirname "$0")"
exec streamlit run streamlit_main.py --server.port 8501 --server.headless true
