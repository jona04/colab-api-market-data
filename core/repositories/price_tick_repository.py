from __future__ import annotations
from abc import ABC, abstractmethod
from typing import List
from core.domain.entities.price_tick_entity import PriceTickEntity


class PriceTickRepository(ABC):
    """Repository interface for tick persistence and retrieval."""

    @abstractmethod
    async def ensure_indexes(self) -> None: ...

    @abstractmethod
    async def insert_tick(self, tick: PriceTickEntity) -> None: ...

    @abstractmethod
    async def list_ticks_for_minute(self, stream_key: str, minute_open_time: int) -> List[PriceTickEntity]: ...

    @abstractmethod
    async def delete_ticks_for_minute(self, stream_key: str, minute_open_time: int) -> None: ...
