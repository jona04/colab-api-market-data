from __future__ import annotations

from typing import Any, Dict, Optional

from core.domain.entities.base_entity import MongoEntity


class IngestionStreamEntity(MongoEntity):
    """
    Represents a single ingestion stream definition stored in MongoDB.

    One stream = one source + one market + one interval (+ optional pool address).
    Multiple streams can run concurrently for the same pair across different sources.

    Example:
      - source_type="binance_ws", symbol="ETHUSDT", interval="1m"
      - source_type="thegraph_pancake_v3_base", symbol="WETH/USDC", interval="1m", pool_address="0x..."
    """

    enabled: bool = True

    source_type: str  # "binance_ws" | "thegraph_pancake_v3_base" | future
    source_name: str  # free label used in stream_key, e.g. "binance" or "thegraph_pancake_v3_base"

    symbol: str        # e.g. "ETHUSDT" or "WETH/USDC"
    interval: str      # e.g. "1m"

    # Optional for DEX/pool sources
    chain: Optional[str] = None
    dex: Optional[str] = None
    pool_address: Optional[str] = None

    # Runtime options
    enable_backfill_on_start: bool = True
    push_signals: bool = True

    # Source-specific configuration
    config: Optional[Dict[str, Any]] = None
