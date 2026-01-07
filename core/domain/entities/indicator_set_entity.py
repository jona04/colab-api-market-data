from __future__ import annotations

import hashlib
from typing import Optional

from core.domain.entities.base_entity import MongoEntity


class IndicatorSetEntity(MongoEntity):
    """
    Indicator-set configuration stored in MongoDB.

    This entity is keyed by cfg_hash, which is deterministically derived from:
    stream_key + ema_fast + ema_slow + atr_window
    """

    stream_key: str
    source: str
    symbol: str
    interval: str

    ema_fast: int
    ema_slow: int
    atr_window: int

    status: str = "ACTIVE"
    cfg_hash: Optional[str] = None

    pool_address: Optional[str] = None
    
    def normalize(self) -> "IndicatorSetEntity":
        """
        Normalize and compute cfg_hash deterministically.

        Returns:
            The same entity instance (mutated) for fluent usage.
        """
        self.source = str(self.source).lower().strip()
        self.symbol = str(self.symbol).upper().strip()
        self.interval = str(self.interval).strip()
        self.stream_key = str(self.stream_key).strip()
        self.ema_fast = int(self.ema_fast)
        self.ema_slow = int(self.ema_slow)
        self.atr_window = int(self.atr_window)
        self.status = str(self.status).upper().strip()

        raw = f"{self.stream_key}|{self.ema_fast}|{self.ema_slow}|{self.atr_window}"
        self.cfg_hash = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
        return self
