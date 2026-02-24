from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class SchedulerStartRequest(BaseModel):
    poll_seconds: Optional[int] = Field(default=None, ge=1)
    max_jobs_per_cycle: Optional[int] = Field(default=None, ge=1)


class ForcedRunRequest(BaseModel):
    rows: Optional[int] = Field(default=None, ge=1)
    continue_on_error: bool = False
    skip_validation: bool = False


class JobUpsertRequest(BaseModel):
    name: str = Field(min_length=1)
    mode: str
    schedule_type: str
    interval_unit: Optional[str] = None
    interval_value: Optional[int] = Field(default=None, ge=1)
    daily_at: Optional[str] = None
    daily_repeat: bool = True
    rows: Optional[int] = Field(default=None, ge=1)
    continue_on_error: bool = False
    skip_validation: bool = False
    enabled: bool = True
    replace: bool = False
