import requests
import time
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class HyperliquidClient:
    BASE_URL = "https://api.hyperliquid.xyz"

    def __init__(self, address: str):
        self.address = address

    def _post(self, endpoint: str, payload: Dict[str, Any]) -> Any:
        url = f"{self.BASE_URL}{endpoint}"
        try:
            resp = requests.post(url, json=payload, headers={"Content-Type": "application/json"})
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Error calling {endpoint}: {e}")
            raise

    def get_user_fills(self) -> List[Dict[str, Any]]:
        """Get all user fills (trades)."""
        return self._post("/info", {"type": "userFills", "user": self.address})

    def get_open_orders(self) -> List[Dict[str, Any]]:
        """Get currently open orders."""
        return self._post("/info", {"type": "openOrders", "user": self.address})

    def get_historical_orders(self) -> List[Dict[str, Any]]:
        """Get historical orders."""
        return self._post("/info", {"type": "historicalOrders", "user": self.address})

    def get_user_state(self) -> Dict[str, Any]:
        """Get user state (account value, margin, positions)."""
        return self._post("/info", {"type": "clearinghouseState", "user": self.address})

    def get_meta_info(self) -> Dict[str, Any]:
        """Get global exchange metadata (universe, decimals, etc.)."""
        return self._post("/info", {"type": "meta"})

    def get_user_funding(self, start_time: int, end_time: int = None) -> List[Dict[str, Any]]:
        """Get user funding history."""
        # Note: userFunding endpoint takes startTime (and optional endTime?)
        # Official docs say: {"type": "userFunding", "user": "...", "startTime": 123}
        payload = {"type": "userFunding", "user": self.address, "startTime": start_time}
        if end_time:
            payload["endTime"] = end_time
        return self._post("/info", payload)

    def get_user_non_funding_ledger_updates(self, start_time: int, end_time: int = None) -> List[Dict[str, Any]]:
        """Get non-funding ledger updates (deposits, withdrawals, transfers)."""
        payload = {"type": "userNonFundingLedgerUpdates", "user": self.address, "startTime": start_time}
        if end_time:
            payload["endTime"] = end_time
        return self._post("/info", payload)

    def get_candles(self, coin: str, interval: str, start_time: int, end_time: int) -> List[Dict[str, Any]]:
        """Get OHLCV candles."""
        payload = {
            "type": "candleSnapshot",
            "req": {
                "coin": coin,
                "interval": interval,
                "startTime": start_time,
                "endTime": end_time
            }
        }
        return self._post("/info", payload)

    def fetch_account_state(self) -> Dict[str, Any]:
        """
        Fetch and parse account state into a standardized dictionary.
        Returns:
            {
                "equity_usd": float,
                "margin_used_usd": float | None,
                "positions": Dict[str, float],  # symbol -> size
                "meta": {"source": "sdk", "address": ...},
                "raw": ...
            }
        """
        data = self.get_user_state()

        # --- equity & margin ---
        equity = None
        margin_used = None
        ms = None
        if isinstance(data, dict):
            ms = data.get("marginSummary") or data.get("margin")
        if isinstance(ms, dict):
            for k in ("accountValue", "equity", "total"):
                if k in ms:
                    try:
                        equity = float(ms[k])
                        break
                    except Exception:
                        pass
            for k in ("totalMarginUsed", "initialMargin", "maintMarginUsed",
                      "marginUsed", "positionMargin"):
                if k in ms:
                    try:
                        margin_used = float(ms[k])
                        break
                    except Exception:
                        pass
        if equity is None and isinstance(data, dict):
            for k in ("accountValue", "equity", "total"):
                if k in data:
                    try:
                        equity = float(data[k])
                        break
                    except Exception:
                        pass
        if equity is None:
            equity = 0.0

        # --- positions ---
        positions: Dict[str, float] = {}
        arrays = []

        if isinstance(data, dict):
            for key in ("assetPositions", "positions"):
                arr = data.get(key)
                if isinstance(arr, list):
                    arrays.append(arr)
            if not arrays:
                acct = data.get("account")
                if isinstance(acct, dict):
                    for key in ("assetPositions", "positions"):
                        arr = acct.get(key)
                        if isinstance(arr, list):
                            arrays.append(arr)

        def _f(v):
            try:
                return float(v)
            except Exception:
                return 0.0

        for arr in arrays:
            for item in arr:
                if not isinstance(item, dict):
                    continue
                pos = item.get("position") if "position" in item else item
                if not isinstance(pos, dict):
                    continue
                sym = pos.get("coin") or pos.get("symbol")
                szi = None
                for k in ("szi", "size", "positionSize"):
                    if k in pos:
                        szi = pos[k]
                        break
                if sym is None or szi is None:
                    continue
                su = str(sym).strip().upper()
                positions[su] = positions.get(su, 0.0) + _f(szi)

        return {
            "equity_usd": float(equity),
            "margin_used_usd": float(margin_used) if margin_used is not None else None,
            "positions": positions,
            "meta": {"source": "sdk", "address": self.address},
            "raw": data,
        }
