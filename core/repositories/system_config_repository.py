from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from core.domain.entities.system_config_entity import SystemConfigEntity


class SystemConfigRepository(ABC):
    """
    Abstraction for reading/writing global runtime config from a storage.
    """

    @abstractmethod
    async def get_runtime(self) -> Optional[SystemConfigEntity]:
        """
        Retrieve the runtime config, if present.
        """
        raise NotImplementedError

    @abstractmethod
    async def upsert_runtime(self, cfg: SystemConfigEntity) -> None:
        """
        Create or update the runtime config.
        """
        raise NotImplementedError
