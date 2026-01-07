from __future__ import annotations

from datetime import datetime, timezone
from typing import List

from motor.motor_asyncio import AsyncIOMotorDatabase

from core.domain.entities.candle_entity import CandleEntity
from core.repositories.candle_repository import CandleRepository


class CandleRepositoryMongoDB(CandleRepository):
    """
    MongoDB implementation for candle persistence.

    Uses a single collection for all sources/intervals, keyed by:
      (stream_key, open_time)

    stream_key already encodes interval, symbol and optional pool address when needed.
    """

    COLLECTION = "candles_1m"

    def __init__(self, db: AsyncIOMotorDatabase):
        """
        Args:
            db: Motor database handle.
        """
        self._db = db

    async def ensure_indexes(self) -> None:
        """
        Ensure uniqueness by (stream_key, open_time) and allow efficient recent queries.
        """
        col = self._db[self.COLLECTION]
        await col.create_index([("stream_key", 1), ("open_time", 1)], unique=True)
        await col.create_index([("stream_key", 1), ("open_time", -1)])
        await col.create_index([("stream_key", 1), ("is_closed", 1), ("open_time", -1)])
        await col.create_index([("source", 1), ("symbol", 1), ("interval", 1), ("open_time", -1)])

    async def upsert_closed_candle(self, candle: CandleEntity) -> None:
        """
        Upsert a closed candle by (stream_key, open_time).

        Adds timestamps:
        - created_at/created_at_iso on first insert
        - updated_at/updated_at_iso on every upsert
        """
        col = self._db[self.COLLECTION]

        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        now_iso = datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")

        key = {"stream_key": candle.stream_key, "open_time": int(candle.open_time)}
        payload = candle.to_mongo()

        payload["updated_at"] = now_ms
        payload["updated_at_iso"] = now_iso

        await col.update_one(
            key,
            {
                "$set": payload,
                "$setOnInsert": {
                    "created_at": now_ms,
                    "created_at_iso": now_iso,
                },
            },
            upsert=True,
        )

    async def get_last_n_closed(self, stream_key: str, n: int) -> List[CandleEntity]:
        """
        Fetch the last N closed candles for a stream_key in ascending order.
        """
        col = self._db[self.COLLECTION]
        cursor = (
            col.find({"stream_key": stream_key, "is_closed": True})
            .sort("open_time", -1)
            .limit(int(n))
        )

        docs = await cursor.to_list(length=int(n))
        entities = [CandleEntity.from_mongo(d) for d in docs]
        out = [e for e in entities if e is not None]
        out.reverse()
        return out
