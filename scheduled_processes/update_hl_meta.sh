#!/bin/bash
# Script to update Hyperliquid metadata in ClickHouse
#
# Usage:
#   ./update_hl_meta.sh
#
# This script:
# 1. Fetches all unique symbols from the weights table
# 2. Gets metadata (size_step, tick_size, min_units) from Hyperliquid API
# 3. Updates the hl_meta table in ClickHouse
#
# Run this periodically (e.g., weekly) to keep metadata current

echo "=========================================="
echo "Updating Hyperliquid Metadata"
echo "=========================================="
echo ""

python3 -m hl_order.update_hl_meta

echo ""
echo "=========================================="
echo "✅ Metadata Update Complete"
echo "=========================================="
echo ""
echo "The hl_meta table now contains:"
echo "  - size_step: Minimum size increment for each symbol"
echo "  - tick_size: Minimum price increment for each symbol"
echo "  - min_units: Minimum order size"
echo "  - min_usd: Minimum notional value"
echo ""
echo "This metadata is used for:"
echo "  - Rounding order sizes in diff_table"
echo "  - Rounding order prices in diff_table"
echo "  - Filtering orders below minimum size"
echo ""
echo "Run this script periodically to keep metadata current."

