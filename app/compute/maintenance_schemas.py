from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


class MaintenanceScope(BaseModel):
    template_codes: list[str] = Field(default_factory=list, max_length=50)
    project_codes: list[str] = Field(default_factory=list, max_length=50)
    class_codes: list[str] = Field(default_factory=list, max_length=50)

    @model_validator(mode="after")
    def require_dimension(self) -> "MaintenanceScope":
        values = self.template_codes + self.project_codes + self.class_codes
        if not any(str(item).strip() for item in values):
            raise ValueError("维护窗口至少需要按模板、课程或班级中的一个维度筛选")
        return self


class MaintenanceWindowCreate(BaseModel):
    code: str = Field(min_length=3, max_length=80, pattern=r"^[a-z0-9][a-z0-9._-]+$")
    reason: str = Field(min_length=2, max_length=500)
    scope: MaintenanceScope
    drain_at: datetime
    deadline_at: datetime
    deadline_policy: Literal["cancel", "requeue"] = "cancel"

    @model_validator(mode="after")
    def validate_times(self) -> "MaintenanceWindowCreate":
        if self.deadline_at <= self.drain_at:
            raise ValueError("截止时间必须晚于排空开始时间")
        return self


class MaintenanceRevoke(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=2, max_length=1000)
