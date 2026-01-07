from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, field_validator


class SystemConfigUpdateDTO(BaseModel):
    """
    DTO for updating the runtime system configuration.

    This configuration is stored in MongoDB and used at startup to wire external clients.
    """

    signals_base_url: str = Field(..., description="Base URL for api-signals, e.g. http://host.docker.internal:8080")
    thegraph_api_key: Optional[str] = Field(None, description="The Graph Gateway API key (Bearer).")
    extras: Optional[Dict[str, Any]] = Field(default=None, description="Optional extra settings.")

    @field_validator("signals_base_url")
    @classmethod
    def _validate_signals_base_url(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("signals_base_url is required")
        return v


class SystemConfigOutDTO(BaseModel):
    """
    DTO returned by API for runtime system configuration.
    """

    key: str
    signals_base_url: str
    thegraph_api_key: Optional[str] = None
    extras: Optional[Dict[str, Any]] = None
