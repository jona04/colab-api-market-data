from __future__ import annotations

from typing import Optional


class StreamKeyService:
    """
    Builds canonical stream keys for ingestion streams.

    Rules:
    - Base key: "{source}:{symbol}:{interval}" (all lowercased)
    - If pool_address is provided, it becomes part of the key to avoid collisions:
        "{source}:{symbol}:{interval}:{pool_address}"
    """

    @staticmethod
    def build(
        *,
        source: str,
        symbol: str,
        interval: str,
        pool_address: Optional[str] = None,
    ) -> str:
        src = (source or "").strip().lower()
        sym = (symbol or "").strip().lower()
        itv = (interval or "").strip().lower()

        if pool_address:
            pool = pool_address.strip().lower()
            return f"{src}:{sym}:{itv}:{pool}"

        return f"{src}:{sym}:{itv}"
