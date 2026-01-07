from __future__ import annotations

from motor.motor_asyncio import AsyncIOMotorDatabase

from core.domain.entities.system_config_entity import SystemConfigEntity
from core.repositories.system_config_repository import SystemConfigRepository


class SystemConfigRepositoryMongoDB(SystemConfigRepository):
    """
    MongoDB repository for system runtime configuration.

    Uses a single document keyed by "key" == "runtime".
    """

    COLLECTION = "system_config"

    def __init__(self, db: AsyncIOMotorDatabase):
        self._db = db

    async def get_runtime(self) -> SystemConfigEntity | None:
        col = self._db[self.COLLECTION]
        doc = await col.find_one({"key": "runtime"})
        return SystemConfigEntity.from_mongo(doc) if doc else None

    async def upsert_runtime(self, cfg: SystemConfigEntity) -> None:
        col = self._db[self.COLLECTION]
        payload = cfg.to_mongo()
        payload["key"] = "runtime"
        await col.update_one({"key": "runtime"}, {"$set": payload}, upsert=True)
