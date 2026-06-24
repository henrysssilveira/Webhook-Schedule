from pydantic import BaseModel, Field

class Schedule(BaseModel):
    name: str
    execute_at: TimeConfig
    webhook_url: str
    payload: str | None = None
    daily: bool = False

class TimeConfig(BaseModel):
    hour: int = Field(ge=0, le=23)
    minutes: int = Field(ge=0, le=59)
    seconds: int | None = Field(default=None, ge=0, le=59)
    date: DateConfig

class DateConfig(BaseModel):
    day: int | None = Field(default=None, ge=1, le=31)
    month: int | None = Field(default=None, ge=1, le=12)
    year: int | None = Field(default=None, ge=2025)