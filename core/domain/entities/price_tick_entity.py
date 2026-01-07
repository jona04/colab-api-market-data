from __future__ import annotations

from typing import Optional, Dict, Any
from core.domain.entities.base_entity import MongoEntity


class PriceTickEntity(MongoEntity):
    """
    Represents a high-frequency spot tick stored in MongoDB.

    This entity is designed to support candle construction.
    A tick belongs to a stream_key and a 1-minute bucket (minute_open_time).
    """

    stream_key: str
    source: str
    symbol: str
    interval: str  # logical candle interval (e.g. "1m")

    ts: int  # tick timestamp in ms
    minute_open_time: int  # floor(ts / 60000) * 60000

    price: float

    volume: float = 0.0
    trades: int = 0

    raw_event_id: Optional[str] = None
    extras: Optional[Dict[str, Any]] = None
