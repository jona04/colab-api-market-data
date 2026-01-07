from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from adapters.external.database.candle_repository_mongodb import CandleRepositoryMongoDB
from adapters.external.database.indicator_repository_mongodb import IndicatorRepositoryMongoDB
from adapters.external.database.indicator_set_repository_mongodb import IndicatorSetRepositoryMongoDB
from core.domain.entities.indicator_set_entity import IndicatorSetEntity
from core.services.stream_key_service import StreamKeyService

from .deps import get_db
from .dtos.candle_dtos import CandleOutDTO
from .dtos.indicator_dtos import IndicatorSnapshotOutDTO
from .dtos.indicator_set_dtos import IndicatorSetCreateDTO, IndicatorSetOutDTO

router = APIRouter(prefix="/admin", tags=["admin"])


@router.post("/indicator-sets", response_model=IndicatorSetOutDTO)
async def create_indicator_set(
    dto: IndicatorSetCreateDTO,
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> IndicatorSetOutDTO:
    """
    Create (or upsert) an ACTIVE indicator set for a given stream_key.

    The stream_key is derived from the ingestion source + symbol + interval so that
    future data sources (e.g., TheGraph) can coexist without collisions.
    """
    repo = IndicatorSetRepositoryMongoDB(db)

    # For now, interval is fixed in indicator sets to "1m" to match collections.
    # If you later support multiple intervals, just add it to DTO and entity.
    interval = "1m"
    source = (dto.source or "binance").lower()
    stream_key = StreamKeyService.build(
        source=source,
        symbol=dto.symbol,
        interval=interval,
        pool_address=dto.pool_address,
    )

    ent = IndicatorSetEntity(
        stream_key=stream_key,
        source=source,
        symbol=dto.symbol.lower(),
        interval=interval,
        ema_fast=int(dto.ema_fast),
        ema_slow=int(dto.ema_slow),
        atr_window=int(dto.atr_window),
        status="ACTIVE",
    )
    
    if dto.pool_address:
        ent.pool_address = dto.pool_address.lower()
    
    stored = await repo.upsert_active(ent)
    return IndicatorSetOutDTO.model_validate(stored.model_dump())


@router.get("/indicator-sets", response_model=List[IndicatorSetOutDTO])
async def list_indicator_sets(
    stream_key: Optional[str] = Query(None, description="Filter by stream_key"),
    status: Optional[str] = Query("ACTIVE"),
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> List[IndicatorSetOutDTO]:
    """
    List indicator sets with optional filters.

    Uses repository methods to keep the code entity-first (no raw dicts).
    """
    repo = IndicatorSetRepositoryMongoDB(db)

    if stream_key and (status or "").upper() == "ACTIVE":
        items = await repo.get_active_by_stream(stream_key)
    else:
        items = await repo.filter(stream_key=stream_key, status=status)

    return [IndicatorSetOutDTO.model_validate(x.model_dump()) for x in items]


@router.get("/candles", response_model=List[CandleOutDTO])
async def list_candles(
    stream_key: str = Query(..., description="e.g. binance:BTCUSDT:1m"),
    limit: int = Query(500, ge=1, le=5000),
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> List[CandleOutDTO]:
    """
    List latest closed candles for a stream_key.
    """
    repo = CandleRepositoryMongoDB(db)
    candles = await repo.get_last_n_closed(stream_key=stream_key, n=int(limit))
    return [CandleOutDTO.model_validate(c.model_dump()) for c in candles]


@router.get("/indicators", response_model=List[IndicatorSnapshotOutDTO])
async def list_indicators(
    stream_key: str = Query(..., description="e.g. binance:BTCUSDT:1m"),
    cfg_hash: Optional[str] = Query(None, description="Filter by indicator-set cfg_hash"),
    limit: int = Query(500, ge=1, le=5000),
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> List[IndicatorSnapshotOutDTO]:
    """
    List latest indicator snapshots for a stream_key (optionally filtered by cfg_hash).
    """
    repo = IndicatorRepositoryMongoDB(db)
    snaps = await repo.list_last(stream_key=stream_key, cfg_hash=cfg_hash, limit=int(limit))
    return [IndicatorSnapshotOutDTO.model_validate(s.model_dump()) for s in snaps]
