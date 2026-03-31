from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class APIModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class IndicatorRef(APIModel):
    id: UUID
    code: str


class Timestamped(APIModel):
    generated_at: datetime
    expires_at: datetime | None = None

