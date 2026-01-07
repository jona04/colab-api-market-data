from __future__ import annotations

from typing import List

from fastapi import APIRouter, Depends, HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase

from adapters.external.database.ingestion_stream_repository_mongodb import IngestionStreamRepositoryMongoDB
from adapters.external.database.system_config_repository_mongodb import SystemConfigRepositoryMongoDB
from core.usecases.admin_config_use_case import AdminConfigUseCase

from .deps import get_db
from .dtos.ingestion_stream_dtos import IngestionStreamOutDTO, IngestionStreamUpsertDTO
from .dtos.system_config_dtos import SystemConfigOutDTO, SystemConfigUpdateDTO


router = APIRouter(prefix="/admin/config", tags=["admin-config"])


def _uc(db: AsyncIOMotorDatabase) -> AdminConfigUseCase:
    """
    Build AdminConfigUseCase with MongoDB repositories.
    """
    return AdminConfigUseCase(
        system_config_repo=SystemConfigRepositoryMongoDB(db),
        streams_repo=IngestionStreamRepositoryMongoDB(db),
    )


@router.get("/runtime", response_model=SystemConfigOutDTO)
async def get_runtime_config(db: AsyncIOMotorDatabase = Depends(get_db)) -> SystemConfigOutDTO:
    """
    Get the current runtime system configuration.
    """
    uc = _uc(db)
    cfg = await uc.get_runtime_config()
    if not cfg:
        raise HTTPException(status_code=404, detail="runtime config not found")
    return SystemConfigOutDTO.model_validate(cfg.model_dump())


@router.put("/runtime", response_model=SystemConfigOutDTO)
async def upsert_runtime_config(
    dto: SystemConfigUpdateDTO,
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> SystemConfigOutDTO:
    """
    Create or update the runtime system configuration (system_config.key == 'runtime').

    This replaces the need to run direct MongoDB updateOne commands.
    """
    uc = _uc(db)
    stored = await uc.upsert_runtime_config(
        signals_base_url=dto.signals_base_url,
        thegraph_api_key=dto.thegraph_api_key,
        extras=dto.extras,
    )
    return SystemConfigOutDTO.model_validate(stored.model_dump())


@router.post("/streams", response_model=IngestionStreamOutDTO)
async def upsert_ingestion_stream(
    dto: IngestionStreamUpsertDTO,
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> IngestionStreamOutDTO:
    """
    Create or update an ingestion stream definition.

    Identity:
      (source_type, source_name, symbol, interval, pool_address)
    """
    uc = _uc(db)
    stored = await uc.upsert_stream(dto.model_dump())
    return IngestionStreamOutDTO.model_validate(stored.model_dump())


@router.get("/streams", response_model=List[IngestionStreamOutDTO])
async def list_enabled_streams(db: AsyncIOMotorDatabase = Depends(get_db)) -> List[IngestionStreamOutDTO]:
    """
    List enabled ingestion streams.

    This is the operational view: only enabled streams are expected to run.
    """
    uc = _uc(db)
    items = await uc.list_streams(enabled=True)
    return [IngestionStreamOutDTO.model_validate(x.model_dump()) for x in items]
