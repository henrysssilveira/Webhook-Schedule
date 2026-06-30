from typing import Any
from pydantic import BaseModel, Field

class DateConfig(BaseModel):
    day: int | None = Field(default=None, ge=1, le=31)
    month: int | None = Field(default=None, ge=1, le=12)
    year: int | None = Field(default=None, ge=2025)

class TimeConfig(BaseModel):
    hour: int = Field(ge=0, le=23)
    minutes: int = Field(ge=0, le=59)
    seconds: int | None = Field(default=None, ge=0, le=59)
    date: DateConfig

class Schedule(BaseModel):
    id: int | None = None
    name: str
    execute_at: TimeConfig
    webhook_url: str
    payload: dict[str, Any] | str | None = None
    daily: bool = False