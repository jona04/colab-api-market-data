from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Optional

from core.domain.entities.ingestion_stream_entity import IngestionStreamEntity


class IngestionStreamRepository(ABC):
    """
    Abstraction for listing ingestion streams that should be running.
    """

    @abstractmethod
    async def list_enabled(self) -> List[IngestionStreamEntity]:
        """
        List enabled ingestion streams.
        """
        raise NotImplementedError

    @abstractmethod
    async def count_all(self) -> int:
        """
        Count all streams.
        """
        raise NotImplementedError

    @abstractmethod
    async def upsert(self, stream: IngestionStreamEntity) -> None:
        """
        Insert or update a stream definition.
        """
        raise NotImplementedError

    @abstractmethod
    async def get_by_identity(
        self,
        *,
        source_type: str,
        source_name: str,
        symbol: str,
        interval: str,
        pool_address: Optional[str] = None,
    ) -> Optional[IngestionStreamEntity]:
        """
        Retrieve a stream by its identity fields.
        """
        raise NotImplementedError
