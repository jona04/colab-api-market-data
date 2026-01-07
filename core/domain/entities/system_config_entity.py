from __future__ import annotations

from typing import Any, Dict, Optional

from core.domain.entities.base_entity import MongoEntity


class SystemConfigEntity(MongoEntity):
    """
    Represents global runtime configuration for the api-market-data service.

    This is stored in MongoDB and allows changing behavior without redeploying.
    """

    key: str  # e.g. "runtime"
    signals_base_url: str

    # Optional auth/keys for external sources
    thegraph_api_key: Optional[str] = None

    # Any extra settings you may want later
    extras: Optional[Dict[str, Any]] = None
