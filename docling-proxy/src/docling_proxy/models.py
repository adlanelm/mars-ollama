from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from io import BytesIO
from pathlib import Path
from typing import Any

from docling_proxy.contracts import ProxyOptions, ProxyTaskMeta


@dataclass(slots=True)
class FilePayload:
    filename: str
    content: bytes | None
    content_type: str
    temp_path: Path | None = None
    cleanup_enabled: bool = True

    async def read_content(self) -> bytes:
        if self.content is not None:
            return self.content
        if self.temp_path is None:
            return b""
        self.content = await asyncio.to_thread(self.temp_path.read_bytes)
        return self.content

    async def peek_content(self, size: int = 8) -> bytes:
        if self.content is not None:
            return self.content[:size]
        if self.temp_path is None:
            return b""
        return await asyncio.to_thread(self._read_prefix, self.temp_path, size)

    def open_binary(self):
        if self.temp_path is not None:
            return self.temp_path.open("rb")
        return BytesIO(self.content or b"")

    def bind_persisted_path(self, path: Path) -> None:
        self.content = None
        self.temp_path = path
        self.cleanup_enabled = False

    async def cleanup(self) -> None:
        if self.temp_path is None or not self.cleanup_enabled:
            return
        temp_path = self.temp_path
        self.temp_path = None
        await asyncio.to_thread(temp_path.unlink, missing_ok=True)

    @staticmethod
    def _read_prefix(path: Path, size: int) -> bytes:
        with path.open("rb") as handle:
            return handle.read(size)


async def cleanup_file_payloads(files: list[FilePayload]) -> None:
    await asyncio.gather(*(file.cleanup() for file in files), return_exceptions=True)


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
    public: bool = True
    source_kind: str = "file"
    filename: str | None = None
    error_message: str | None = None
    target_kind: str = "inbody"
    requested_formats: list[str] = field(default_factory=list)
    options: dict[str, Any] = field(default_factory=dict)
    proxy_options: ProxyOptions | None = None
    files: list[FilePayload] = field(default_factory=list)
    source_request: dict[str, Any] | None = None
    auth_headers: dict[str, str] = field(default_factory=dict)
    task_dir: Path | None = None
    meta: ProxyTaskMeta = field(default_factory=ProxyTaskMeta)
    result_payload: dict[str, Any] | None = None
    result_zip: bytes | None = None
    result_payload_path: Path | None = None
    result_zip_path: Path | None = None
    archive_path: str | None = None
    passthrough_upstream_task_id: str | None = None
