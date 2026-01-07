from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class IndicatorSetCreateDTO(BaseModel):
    """
    Request DTO for creating/upserting an indicator set.
    """
    symbol: str = Field(..., description="Trading pair symbol, e.g. BTCUSDT")
    ema_fast: int
    ema_slow: int
    atr_window: int
    source: Optional[str] = Field(default="binance", description="Ingestion source identifier")

    pool_address: Optional[str] = Field(
        default=None,
        description="Optional pool address to bind the indicator set to a specific pool stream.",
    )
    

class IndicatorSetOutDTO(BaseModel):
    """
    Response DTO for indicator sets.
    """
    id: Optional[str] = None

    stream_key: str
    source: str
    symbol: str
    interval: str

    ema_fast: int
    ema_slow: int
    atr_window: int

    status: str
    cfg_hash: str

    created_at: Optional[int] = None
    created_at_iso: Optional[str] = None
    updated_at: Optional[int] = None
    updated_at_iso: Optional[str] = None
