from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ProxyOptions(BaseModel):
    enabled: bool | None = Field(default=None)
    force_split: bool = False
    include_proxy_meta: bool = False
    max_pages_per_part: int | None = Field(default=None, gt=0)
    work_concurrency: int | None = Field(default=None, gt=0)
    poll_interval_sec: float | None = Field(default=None, gt=0)

    @model_validator(mode="before")
    @classmethod
    def apply_compatibility_aliases(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        normalized = dict(data)
        if "work_concurrency" not in normalized and "max_concurrency" in normalized:
            normalized["work_concurrency"] = normalized["max_concurrency"]
        return normalized


class TaskStatusResponse(BaseModel):
    task_id: str
    task_type: str = "convert"
    task_status: str
    task_position: int | None = None
    task_meta: dict[str, Any] | None = None
    error_message: str | None = None


class ExportDocumentResponse(BaseModel):
    md_content: str | None = None
    json_content: dict[str, Any] | None = None
    html_content: str | None = None
    text_content: str | None = None
    doctags_content: str | None = None
    yaml_content: str | None = None
    vtt_content: str | None = None


class ConvertDocumentResponse(BaseModel):
    document: ExportDocumentResponse
    status: str
    errors: list[dict[str, Any]] = Field(default_factory=list)
    processing_time: float = 0.0
    timings: dict[str, Any] = Field(default_factory=dict)
    proxy_meta: dict[str, Any] | None = None


class SourceTarget(BaseModel):
    model_config = ConfigDict(extra="allow")

    kind: str = "inbody"


class ProxySourceRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    options: dict[str, Any] = Field(default_factory=dict)
    sources: list[dict[str, Any]] | None = None
    http_sources: list[dict[str, Any]] | None = None
    file_sources: list[dict[str, Any]] | None = None
    target: SourceTarget = Field(default_factory=SourceTarget)
    proxy_options: ProxyOptions | None = None

    @model_validator(mode="after")
    def ensure_any_source(self) -> "ProxySourceRequest":
        if self.sources or self.http_sources or self.file_sources:
            return self
        raise ValueError("At least one source must be provided.")


class SplitDecision(BaseModel):
    should_split: bool
    reason: str
    total_pages: int | None = None
    parts: int | None = None


class ProxyPartInfo(BaseModel):
    part_index: int
    start_page: int
    end_page: int
    upstream_task_id: str | None = None
    task_status: str = "pending"
    error_message: str | None = None


class ProxyTaskMeta(BaseModel):
    total_parts: int = 0
    completed_parts: int = 0
    split: bool = False
    source_kind: Literal["file", "source"] = "file"
    filename: str | None = None
    parts: list[ProxyPartInfo] = Field(default_factory=list)
