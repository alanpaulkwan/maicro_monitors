#!/bin/bash

# Get the directory of the script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$(dirname "$DIR")"

# Navigate to root directory
cd "$ROOT_DIR"

echo "========================================================"
echo "Starting Monitor & Sync Run at $(date)"
echo "========================================================"

# 1. Run Orchestrator (Populate Local DB)
echo ">>> Running Orchestrator..."
python3 scripts/orchestrate_monitors.py
ORCH_EXIT_CODE=$?

if [ $ORCH_EXIT_CODE -ne 0 ]; then
    echo "!!! Orchestrator failed with exit code $ORCH_EXIT_CODE. Aborting sync."
    exit $ORCH_EXIT_CODE
fi

# 2. Run Sync (Push to Remote DB)
echo ">>> Running Sync to Remote..."
python3 scripts/sync_to_remote.py
SYNC_EXIT_CODE=$?

if [ $SYNC_EXIT_CODE -ne 0 ]; then
    echo "!!! Sync failed with exit code $SYNC_EXIT_CODE."
    exit $SYNC_EXIT_CODE
fi

echo "========================================================"
echo "Run Complete at $(date)"
echo "========================================================"
