from __future__ import annotations

from typing import Any, Dict, Optional

import httpx


class TheGraphHttpClient:
    """
    Minimal The Graph Gateway client.

    Uses POST JSON:
      { "query": "...", "variables": {...} }

    Authorization:
      Bearer {api_key}
    """

    def __init__(self, *, endpoint: str, api_key: str, timeout_s: float = 20.0) -> None:
        self._endpoint = str(endpoint).strip()
        self._api_key = str(api_key).strip()
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(timeout_s, connect=5.0))

    async def aclose(self) -> None:
        await self._client.aclose()

    async def query(self, *, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._api_key}",
        }
        payload = {
            "query": query,
            "variables": variables or {},
        }
        r = await self._client.post(self._endpoint, headers=headers, json=payload)
        r.raise_for_status()
        data = r.json()
        return data
