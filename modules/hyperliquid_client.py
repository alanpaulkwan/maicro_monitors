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
