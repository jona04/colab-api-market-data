# core/domain/entities/base_entity.py
from __future__ import annotations

from typing import Any, Dict, Optional, Type, TypeVar

from pydantic import BaseModel, ConfigDict

E = TypeVar("E", bound="MongoEntity")


class MongoEntity(BaseModel):
    """
   Base entity for Mongo-backed documents.

    - Maps Mongo's `_id` to `id` (string).
    - Carries common timestamps.
    - Accepts extra fields to avoid breaking on forward-compatible schema changes.
    """

    id: Optional[str] = None  # maps _id
    created_at: Optional[int] = None
    created_at_iso: Optional[str] = None
    updated_at: Optional[int] = None
    updated_at_iso: Optional[str] = None

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="allow",
        use_enum_values=True,
    )

    @classmethod
    def from_mongo(cls: Type[E], doc: Optional[dict[str, Any]]) -> Optional[E]:
        """
        Convert a MongoDB document into a strongly-typed entity.

        Args:
            doc: Raw MongoDB dict (may include `_id`).

        Returns:
            An entity instance or None if doc is falsy.
        """
        if not doc:
            return None
        data = dict(doc)
        if "_id" in data:
            data["id"] = str(data.pop("_id"))
        return cls.model_validate(data)

    def to_mongo(self) -> dict[str, Any]:
        """
        Convert this entity into a MongoDB document dict.

        Returns:
            Dict suitable for Mongo insert/update (`id` -> `_id`).
        """
        data = self.model_dump(mode="python", exclude_none=True)
        if "id" in data:
            data["_id"] = data.pop("id")
        return data

    def to_dict(self) -> Dict[str, Any]:
        """
        JSON-safe dict for HTTP payloads and logging.
        """
        # Pydantic v2
        try:
            return self.model_dump(mode="json", exclude_none=True)
        except Exception:
            # fallback p/ pydantic v1 ou algo custom
            return dict(self.__dict__)