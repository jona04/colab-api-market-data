# core/domain/entities/candle_entity.py
from __future__ import annotations

from typing import Any, Dict, Optional

from core.domain.entities.base_entity import MongoEntity


class CandleEntity(MongoEntity):
    """
    Represents a closed OHLCV candle stored in MongoDB.

    stream_key is the canonical identifier for a stream (source + market + interval),
    e.g. "binance:BTCUSDT:1m" or "thegraph_pancake_v3_base:WETH/USDC:1m:0xPOOL".

    This entity supports both centralized exchange candles and synthetic candles derived
    from on-chain/dex sources (The Graph, RPC, etc.) via optional metadata fields.
    """

    stream_key: str
    source: str

    symbol: str
    interval: str

    open_time: int
    close_time: int

    open: float
    high: float
    low: float
    close: float

    volume: float
    trades: int
    is_closed: bool = True

    # Optional metadata for future sources
    raw_event_id: Optional[str] = None

    # DEX / on-chain enrichments (optional)
    chain: Optional[str] = None
    dex: Optional[str] = None
    pool_address: Optional[str] = None

    token0_symbol: Optional[str] = None
    token1_symbol: Optional[str] = None

    token0_price: Optional[float] = None
    token1_price: Optional[float] = None

    fee_tier: Optional[int] = None
    liquidity: Optional[str] = None  # keep as str to avoid int overflow / bson issues
    sqrt_price: Optional[str] = None
    tick: Optional[int] = None

    tvl_usd: Optional[float] = None
    volume_usd: Optional[float] = None

    extras: Optional[Dict[str, Any]] = None
