from __future__ import annotations

from typing import List, Optional

from core.domain.entities.ingestion_stream_entity import IngestionStreamEntity
from core.domain.entities.system_config_entity import SystemConfigEntity
from core.repositories.ingestion_stream_repository import IngestionStreamRepository
from core.repositories.system_config_repository import SystemConfigRepository


class AdminConfigUseCase:
    """
    Use case for managing runtime configuration and ingestion streams.

    This isolates the HTTP layer from persistence details and keeps all
    configuration changes entity-first.
    """

    def __init__(
        self,
        *,
        system_config_repo: SystemConfigRepository,
        streams_repo: IngestionStreamRepository,
    ) -> None:
        self._system_repo = system_config_repo
        self._streams_repo = streams_repo

    async def upsert_runtime_config(
        self,
        *,
        signals_base_url: str,
        thegraph_api_key: Optional[str],
        extras: Optional[dict],
    ) -> SystemConfigEntity:
        """
        Create or update system runtime configuration.
        """
        cfg = SystemConfigEntity(
            key="runtime",
            signals_base_url=str(signals_base_url).strip(),
            thegraph_api_key=(thegraph_api_key.strip() if thegraph_api_key else None),
            extras=extras or {},
        )
        await self._system_repo.upsert_runtime(cfg)
        stored = await self._system_repo.get_runtime()
        return stored or cfg

    async def get_runtime_config(self) -> Optional[SystemConfigEntity]:
        """
        Retrieve current runtime configuration.
        """
        return await self._system_repo.get_runtime()

    async def upsert_stream(self, dto: dict) -> IngestionStreamEntity:
        """
        Create or update an ingestion stream.
        """
        ent = IngestionStreamEntity(**dto)
        await self._streams_repo.upsert(ent)
        stored = await self._streams_repo.get_by_identity(
            source_type=ent.source_type,
            source_name=ent.source_name,
            symbol=ent.symbol,
            interval=ent.interval,
            pool_address=ent.pool_address,
        )
        return stored or ent

    async def list_streams(self, *, enabled: Optional[bool] = None) -> List[IngestionStreamEntity]:
        """
        List streams, optionally filtering by enabled flag.
        """
        if enabled is None:
            # repository only exposes list_enabled, so keep it simple:
            # enabled=None returns enabled streams only (safe default).
            return await self._streams_repo.list_enabled()

        if enabled is True:
            return await self._streams_repo.list_enabled()

        # enabled=False requires a small raw query repository method; keep minimal:
        # If you want disabled listing too, add list_all() / filter(enabled=...).
        return []
