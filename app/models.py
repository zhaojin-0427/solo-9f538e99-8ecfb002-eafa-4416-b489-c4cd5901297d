"""Pydantic 响应/请求模型与统一错误类型。"""
from __future__ import annotations

from typing import List, Optional

from fastapi import HTTPException
from pydantic import BaseModel, Field, field_validator


class ConflictError(HTTPException):
    """业务规则冲突（409）：请求本身合法，但违反数仓不变量。"""

    def __init__(self, detail: str) -> None:
        super().__init__(status_code=409, detail=detail)


class NotFound(HTTPException):
    """资源不存在（404）。"""

    def __init__(self, detail: str) -> None:
        super().__init__(status_code=404, detail=detail)


class IndicatorOut(BaseModel):
    indicator_id: int = Field(..., description="指标稳定身份 ID；改名不变，同名复用会得到新 ID")
    name: str = Field(..., description="当前指标名")
    active: bool


class SchemaOut(BaseModel):
    current_version: int = Field(..., description="当前字段版号")
    fixed_columns: List[str] = Field(..., description="固定字段")
    indicators: List[IndicatorOut] = Field(..., description="当前存续指标（按指标 ID 排序）")


class VersionIndicatorOut(BaseModel):
    indicator_id: int
    name: str
    active: bool


class VersionOut(BaseModel):
    version_id: int
    created_at: str
    change_note: str
    indicators: List[VersionIndicatorOut]


class RenameRequest(BaseModel):
    old_name: str = Field(..., min_length=1, description="当前指标名（例：浓度）")
    new_name: str = Field(..., min_length=1, description="新指标名（例：硝酸盐）")


class CreateIndicatorRequest(BaseModel):
    name: str = Field(..., min_length=1, description="新指标名；若与历史指标同名则视为全新指标，不继承旧值")


class IndicatorChangeResponse(BaseModel):
    indicator_id: int
    name: str
    active: bool
    new_version: int = Field(..., description="本次变更产生的字段版号")


class UploadResponse(BaseModel):
    shard_id: int
    schema_version: int = Field(..., description="本次上传指定并裁决所用的字段版")
    row_count: int
    stored_columns: List[str] = Field(..., description="分片实际落盘的指标身份列")
    file_path: str


class SnapshotMeta(BaseModel):
    snapshot_id: int
    created_at: str
    indicator_names: List[str]
    shard_ids: List[int]


class RowPage(BaseModel):
    snapshot_id: int
    snapshot_created: bool = Field(..., description="本次调用是否新建了快照（首次查询为 true）")
    page: int
    page_size: int
    total_rows: int
    total_pages: int
    fixed_columns: List[str]
    indicator_names: List[str] = Field(..., description="快照固化的当前指标名；旧分片没有的指标为 null")
    rows: List[dict]


# ---------------------------------------------------------------------------
# 质控隔离
# ---------------------------------------------------------------------------

class QcKeyIn(BaseModel):
    """一个质控键：站点 + UTC 时刻 + 存续指标 ID + 非负绝对容差。"""

    station: str = Field(..., min_length=1, description="站点", examples=["A"])
    sampled_at: str = Field(
        ...,
        description="UTC 采样时刻（ISO-8601；支持 Z/偏移量，无时区按 UTC）",
        examples=["2026-01-01T00:00:00Z"],
    )
    indicator_id: int = Field(..., ge=1, description="存续指标 ID（已删除或不存在会被拒绝）")
    tolerance: float = Field(..., ge=0, allow_inf_nan=False, description="非负绝对容差")

    @field_validator("station")
    @classmethod
    def _station_not_blank(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("station 不能为空白")
        return value


class QcIsolateRequest(BaseModel):
    keys: List[QcKeyIn] = Field(..., min_length=1, max_length=20, description="1~20 个质控键")


class QcReadingOut(BaseModel):
    shard_id: int
    source_file: str = Field(..., description="上传时的原始文件名")
    values: List[float] = Field(..., description="该分片内同键的全部非 NULL 读数（升序）")


class QcKeyResultOut(BaseModel):
    station: str
    sampled_at: str = Field(..., description="归一化后的 UTC 时刻")
    indicator_id: int
    tolerance: float
    min_value: float
    max_value: float
    spread: float = Field(..., description="隔离前全部在供读数的 最大值-最小值")
    within_tolerance: bool = Field(..., description="隔离前读数是否已在容差内")
    readings: List[QcReadingOut] = Field(..., description="冲突来源：每个在供分片的同键读数")


class IsolatedSourceOut(BaseModel):
    shard_id: int
    source_file: str


class QcIsolateResponse(BaseModel):
    event_id: Optional[int] = Field(..., description="隔离事件号；无需隔离时为 null")
    isolated_shard_ids: List[int] = Field(..., description="本次隔离（停供）的分片，升序")
    isolated_sources: List[IsolatedSourceOut] = Field(..., description="被停供的来源文件")
    key_results: List[QcKeyResultOut] = Field(..., description="逐键的冲突来源与读数分布")
    message: str


class IsolationRecordOut(BaseModel):
    shard_id: int
    source_file: str
    event_id: int
    isolated_at: str


class IsolationListOut(BaseModel):
    isolated_shard_ids: List[int]
    isolated: List[IsolationRecordOut]
