from pydantic import BaseModel, Field, field_validator
from typing import List, Optional


class TaskSubmitRequest(BaseModel):
    id: str = Field(..., min_length=1)
    type: str = Field(..., min_length=1)
    duration_ms: int = Field(..., gt=0)
    dependencies: List[str] = Field(default_factory=list)

    @field_validator('id')
    @classmethod
    def validate_id(cls, v: str) -> str:
        if not v or v.strip() != v:
            raise ValueError("Task ID cannot be empty or have leading/trailing whitespace")
        return v


class TaskResponse(BaseModel):
    id: str
    type: str
    duration_ms: int
    dependencies: List[str]
    status: str
    created_at: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error_message: Optional[str] = None


class TaskListResponse(BaseModel):
    tasks: List[TaskResponse]
    total: int


class SubmitResponse(BaseModel):
    message: str
    task_id: str
