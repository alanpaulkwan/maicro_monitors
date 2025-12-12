#!/usr/bin/env bash
set -euo pipefail
# Run the dashboard with an optional enforced password.
# By default, the script requires MAICRO_DASH_PASSWORD to be set. Set FORCE_NO_PASSWORD=1 to run without a password (not recommended).

if [[ -z "${MAICRO_DASH_PASSWORD:-}" && -z "${FORCE_NO_PASSWORD:-}" ]]; then
  echo "MAICRO_DASH_PASSWORD is not set. Please set it to secure the dashboard, or set FORCE_NO_PASSWORD=1 to disable the password gate."
  echo "Example: MAICRO_DASH_PASSWORD=MyStrongPassword ./run_dashboard.sh"
  exit 1
fi

# Ensure we run from the repo root so that `config` and other top-level
# packages are importable by Streamlit.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

echo "Starting Streamlit dashboard (port 8501)..."
exec streamlit run dashboard/streamlit_main.py --server.port 8501 --server.headless true
