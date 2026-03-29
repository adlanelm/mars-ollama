from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from docling_proxy.contracts import ProxyOptions, ProxyTaskMeta


@dataclass(slots=True)
class FilePayload:
    filename: str
    content: bytes
    content_type: str


@dataclass(slots=True)
class NormalizedSource:
    kind: str
    filename: str
    content: bytes
    original: dict[str, Any]


@dataclass(slots=True)
class ProxyJob:
    task_id: str
    status: str = "pending"
    source_kind: str = "file"
    filename: str | None = None
    error_message: str | None = None
    target_kind: str = "inbody"
    requested_formats: list[str] = field(default_factory=list)
    options: dict[str, Any] = field(default_factory=dict)
    proxy_options: ProxyOptions | None = None
    files: list[FilePayload] = field(default_factory=list)
    source_request: dict[str, Any] | None = None
    meta: ProxyTaskMeta = field(default_factory=ProxyTaskMeta)
    result_payload: dict[str, Any] | None = None
    result_zip: bytes | None = None
    passthrough_upstream_task_id: str | None = None
