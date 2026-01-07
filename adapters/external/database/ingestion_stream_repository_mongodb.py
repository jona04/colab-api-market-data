from __future__ import annotations

from typing import List, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase

from core.domain.entities.ingestion_stream_entity import IngestionStreamEntity
from core.repositories.ingestion_stream_repository import IngestionStreamRepository


class IngestionStreamRepositoryMongoDB(IngestionStreamRepository):
    """
    MongoDB repository for ingestion stream definitions.

    A stream identity is defined by:
      (source_type, source_name, symbol, interval, pool_address)
    """

    COLLECTION = "ingestion_streams"

    def __init__(self, db: AsyncIOMotorDatabase):
        self._db = db

    async def ensure_indexes(self) -> None:
        """
        Ensure uniqueness and efficient listing.
        """
        col = self._db[self.COLLECTION]
        await col.create_index(
            [("source_type", 1), ("source_name", 1), ("symbol", 1), ("interval", 1), ("pool_address", 1)],
            unique=True,
        )
        await col.create_index([("enabled", 1), ("source_type", 1)])

    async def list_enabled(self) -> List[IngestionStreamEntity]:
        col = self._db[self.COLLECTION]
        docs = await col.find({"enabled": True}).to_list(length=10_000)
        out = [IngestionStreamEntity.from_mongo(d) for d in docs]
        return [x for x in out if x is not None]

    async def count_all(self) -> int:
        col = self._db[self.COLLECTION]
        return int(await col.count_documents({}))

    async def upsert(self, stream: IngestionStreamEntity) -> None:
        col = self._db[self.COLLECTION]
        key = {
            "source_type": stream.source_type,
            "source_name": stream.source_name,
            "symbol": stream.symbol,
            "interval": stream.interval,
            "pool_address": stream.pool_address,
        }
        await col.update_one(key, {"$set": stream.to_mongo()}, upsert=True)

    async def get_by_identity(
        self,
        *,
        source_type: str,
        source_name: str,
        symbol: str,
        interval: str,
        pool_address: Optional[str] = None,
    ) -> IngestionStreamEntity | None:
        col = self._db[self.COLLECTION]
        doc = await col.find_one(
            {
                "source_type": str(source_type),
                "source_name": str(source_name),
                "symbol": str(symbol),
                "interval": str(interval),
                "pool_address": pool_address,
            }
        )
        return IngestionStreamEntity.from_mongo(doc) if doc else None
