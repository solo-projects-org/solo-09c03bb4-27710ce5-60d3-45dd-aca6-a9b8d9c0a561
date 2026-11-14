"""Pydantic 请求与响应模型。"""

from __future__ import annotations

from pydantic import BaseModel, Field

from .models import BatchStatus, FileRole, TrimJobStatus


class BatchCreateRequest(BaseModel):
    sample_id: str = Field(min_length=1, max_length=128, description="样本编号")


class FileInfo(BaseModel):
    role: FileRole
    size: int
    uploaded_at: str


class BatchResponse(BaseModel):
    id: str
    sample_id: str
    status: BatchStatus
    byte_limit: int
    bytes_used: int
    error: str | None = None
    files: list[FileInfo] = Field(default_factory=list)
    created_at: str
    updated_at: str


class BatchListResponse(BaseModel):
    batches: list[BatchResponse]


class CancelResponse(BaseModel):
    id: str
    status: BatchStatus
    freed_bytes: int
    message: str


class TrimRequest(BaseModel):
    min_quality: int = Field(
        ge=0, le=93,
        description="Phred+33 最低质量值：两端尾部连续低于该值的碱基被裁去",
    )
    min_length: int = Field(
        ge=1, le=100000,
        description="最短读长：裁剪后任一端不足该长度则舍弃整对",
    )


class TrimJobInfo(BaseModel):
    id: str
    status: TrimJobStatus
    min_quality: int
    min_length: int
    pairs_in: int | None = None
    pairs_kept: int | None = None
    error: str | None = None
    created_at: str
    updated_at: str


class TrimAcceptedResponse(BaseModel):
    job: TrimJobInfo
    message: str


class TrimPackageInfo(BaseModel):
    job_id: str
    min_quality: int
    min_length: int
    pairs_in: int
    pairs_kept: int
    size: int
    published_at: str


class TrimStatusResponse(BaseModel):
    batch_id: str
    job: TrimJobInfo | None = None
    package: TrimPackageInfo | None = None
