from __future__ import annotations

from typing import Any, Dict, Optional
import httpx


class SignalsHttpClient:
    def __init__(self, *, base_url: str, timeout_s: float = 10.0):
        self._base_url = str(base_url).rstrip("/")
        self._timeout = timeout_s

    async def candle_closed(
        self,
        *,
        indicator_set_id: str,
        ts: int,
        indicator_set: Optional[Dict[str, Any]] = None,
        indicator_snapshot: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "indicator_set_id": indicator_set_id,
            "ts": int(ts),
            "indicator_set": indicator_set,
            "indicator_snapshot": indicator_snapshot,
        }

        async with httpx.AsyncClient(timeout=self._timeout) as client:
            r = await client.post(f"{self._base_url}/triggers/candle-closed", json=payload)
            r.raise_for_status()
            return r.json()
