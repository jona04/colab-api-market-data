from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, field_validator


class IngestionStreamUpsertDTO(BaseModel):
    """
    DTO for creating or updating an ingestion stream definition.

    Identity fields:
      (source_type, source_name, symbol, interval, pool_address)

    Notes:
    - symbol for CEX: "ETHUSDT"
    - symbol for DEX: "WETH/USDC"
    """

    enabled: bool = Field(default=True)

    source_type: str = Field(..., description="e.g. binance_ws | thegraph_pancake_v3_base")
    source_name: str = Field(..., description="e.g. binance | thegraph_pancake_v3_base")

    symbol: str = Field(..., description='e.g. "ETHUSDT" or "WETH/USDC"')
    interval: str = Field(default="1m", description='Interval string, e.g. "1m"')

    chain: Optional[str] = Field(default=None, description='e.g. "base"')
    dex: Optional[str] = Field(default=None, description='e.g. "pancakeswap_v3"')
    pool_address: Optional[str] = Field(default=None, description="Required for pool-based sources")

    enable_backfill_on_start: bool = Field(default=True)
    push_signals: bool = Field(default=True)

    config: Optional[Dict[str, Any]] = Field(default=None, description="Source-specific config")

    @field_validator("source_type", "source_name", "symbol", "interval")
    @classmethod
    def _strip_required(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("field is required")
        return v

    @field_validator("pool_address")
    @classmethod
    def _normalize_pool(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        v = v.strip()
        return v.lower() if v else None


class IngestionStreamOutDTO(BaseModel):
    """
    DTO returned by API for ingestion streams.
    """

    enabled: bool
    source_type: str
    source_name: str
    symbol: str
    interval: str

    chain: Optional[str] = None
    dex: Optional[str] = None
    pool_address: Optional[str] = None

    enable_backfill_on_start: bool
    push_signals: bool

    config: Optional[Dict[str, Any]] = None
