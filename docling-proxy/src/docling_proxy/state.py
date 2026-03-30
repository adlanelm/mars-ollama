from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from docling_proxy.config import settings
from docling_proxy.contracts import ProxyOptions, ProxyTaskMeta
from docling_proxy.models import FilePayload, NormalizedSource, ProxyJob
from docling_proxy.upstream import forwarded_headers

INCOMPLETE_TASK_STATUSES = {"pending", "started"}


@dataclass(slots=True)
class PersistedInputFile:
    filename: str
    content_type: str
    path: Path


@dataclass(slots=True)
class PersistedOperation:
    task_id: str
    public: bool
    source_kind: str
    filename: str | None
    target_kind: str
    requested_formats: list[str]
    options: dict[str, Any]
    proxy_options: ProxyOptions | None
    status: str
    error_message: str | None
    auth_headers: dict[str, str]
    source_request: dict[str, Any] | None
    input_files: list[PersistedInputFile]
    normalized_source: NormalizedSource | None
    meta: ProxyTaskMeta
    result_payload_path: Path | None
    result_zip_path: Path | None
    archive_path: str | None
    task_dir: Path


class TaskStateStore:
    def __init__(self, state_dir: Path | None = None) -> None:
        self.state_dir = self._resolve_state_dir(state_dir)
        self.tasks_dir = self.state_dir / "tasks"
        self.tasks_dir.mkdir(parents=True, exist_ok=True)
        self._locks: dict[str, asyncio.Lock] = {}

    def _resolve_state_dir(self, explicit_state_dir: Path | None) -> Path:
        if explicit_state_dir is not None:
            return explicit_state_dir
        if settings.state_dir is not None:
            return settings.state_dir
        if settings.archive_dir is not None:
            return settings.archive_dir / ".proxy-state"
        return settings.temp_dir / "state"

    def _task_dir(self, task_id: str) -> Path:
        return self.tasks_dir / task_id

    def _manifest_path(self, task_id: str) -> Path:
        return self._task_dir(task_id) / "manifest.json"

    def _input_file_path(self, task_id: str, index: int, filename: str) -> Path:
        suffix = Path(filename).suffix
        safe_name = self._safe_component(Path(filename).stem or f"file-{index}")
        return self._task_dir(task_id) / "inputs" / f"{index:04d}_{safe_name}{suffix}"

    def _normalized_source_path(self, task_id: str, filename: str | None) -> Path:
        suffix = Path(filename or "document.pdf").suffix
        return self._task_dir(task_id) / "source" / f"original{suffix or '.bin'}"

    def part_payload_path(self, task_id: str, part_index: int) -> Path:
        return self._task_dir(task_id) / "parts" / f"{part_index:04d}.payload.json"

    def result_payload_path(self, task_id: str) -> Path:
        return self._task_dir(task_id) / "result" / "payload.json"

    def result_zip_path(self, task_id: str) -> Path:
        return self._task_dir(task_id) / "result" / "result.zip"

    def _lock(self, task_id: str) -> asyncio.Lock:
        lock = self._locks.get(task_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[task_id] = lock
        return lock

    async def exists(self, task_id: str) -> bool:
        manifest_path = self._manifest_path(task_id)
        return await asyncio.to_thread(manifest_path.exists)

    async def create_operation(
        self,
        task_id: str,
        *,
        public: bool,
        source_kind: str,
        filename: str | None,
        target_kind: str,
        requested_formats: list[str],
        options: dict[str, Any],
        proxy_options: ProxyOptions | None,
        headers: dict[str, str],
        files: list[FilePayload] | None = None,
        source_request: dict[str, Any] | None = None,
        normalized_source: NormalizedSource | None = None,
    ) -> None:
        async with self._lock(task_id):
            if await asyncio.to_thread(self._manifest_path(task_id).exists):
                return
            await asyncio.to_thread(
                self._create_operation_sync,
                task_id,
                public,
                source_kind,
                filename,
                target_kind,
                requested_formats,
                options,
                proxy_options.model_dump(exclude_none=True) if proxy_options is not None else None,
                forwarded_headers(headers),
                files or [],
                source_request,
                normalized_source,
            )

    async def sync_job(self, job: ProxyJob) -> None:
        if not await self.exists(job.task_id):
            return
        async with self._lock(job.task_id):
            manifest = await self._read_manifest(job.task_id)
            manifest["status"] = job.status
            manifest["error_message"] = job.error_message
            manifest["filename"] = job.filename
            manifest["target_kind"] = job.target_kind
            manifest["task_meta"] = job.meta.model_dump(exclude_none=True)
            manifest["archive_path"] = job.archive_path
            manifest["updated_at"] = self._timestamp()
            await self._write_manifest(job.task_id, manifest)

    async def ensure_split_plan(
        self,
        task_id: str,
        *,
        source_kind: str,
        filename: str,
        parts: list[tuple[int, int]],
    ) -> ProxyTaskMeta:
        async with self._lock(task_id):
            manifest = await self._read_manifest(task_id)
            current_meta = ProxyTaskMeta.model_validate(manifest.get("task_meta") or {})
            if current_meta.split and len(current_meta.parts) == len(parts):
                return current_meta
            current_meta.split = True
            current_meta.source_kind = source_kind  # type: ignore[assignment]
            current_meta.filename = filename
            current_meta.total_parts = len(parts)
            current_meta.completed_parts = 0
            from docling_proxy.contracts import ProxyPartInfo

            existing_parts = list(current_meta.parts)
            rebuilt_parts: list[ProxyPartInfo] = []
            for index, (start_page, end_page) in enumerate(parts):
                if index < len(existing_parts):
                    part = existing_parts[index]
                    part.part_index = index
                    part.start_page = start_page
                    part.end_page = end_page
                    rebuilt_parts.append(part)
                    continue
                rebuilt_parts.append(
                    ProxyPartInfo(
                        part_index=index,
                        start_page=start_page,
                        end_page=end_page,
                    )
                )
            current_meta.parts = rebuilt_parts
            manifest["task_meta"] = current_meta.model_dump(exclude_none=True)
            manifest["updated_at"] = self._timestamp()
            await self._write_manifest(task_id, manifest)
            return current_meta

    async def part_payload_exists(self, task_id: str, part_index: int) -> bool:
        part_path = self.part_payload_path(task_id, part_index)
        return await asyncio.to_thread(part_path.exists)

    async def persist_part_payload(self, task_id: str, part_index: int, payload: dict[str, Any]) -> Path:
        part_path = self.part_payload_path(task_id, part_index)
        await asyncio.to_thread(self._write_json_atomic, part_path, payload)
        return part_path

    async def load_part_payloads(self, task_id: str, total_parts: int) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._load_part_payloads_sync, task_id, total_parts)

    async def persist_final_payload(self, task_id: str, payload: dict[str, Any]) -> Path:
        result_path = self.result_payload_path(task_id)
        await asyncio.to_thread(self._write_json_atomic, result_path, payload)
        return result_path

    async def persist_final_zip(self, task_id: str, zip_bytes: bytes) -> Path:
        result_path = self.result_zip_path(task_id)
        await asyncio.to_thread(self._write_bytes_atomic, result_path, zip_bytes)
        return result_path

    async def load_result_payload(self, task_id: str) -> dict[str, Any] | None:
        result_path = self.result_payload_path(task_id)
        if not await asyncio.to_thread(result_path.exists):
            return None
        return await asyncio.to_thread(self._read_json_file, result_path)

    async def load_result_zip(self, task_id: str) -> bytes | None:
        result_path = self.result_zip_path(task_id)
        if not await asyncio.to_thread(result_path.exists):
            return None
        return await asyncio.to_thread(result_path.read_bytes)

    async def set_archive_path(self, task_id: str, archive_path: str) -> None:
        if not await self.exists(task_id):
            return
        async with self._lock(task_id):
            manifest = await self._read_manifest(task_id)
            manifest["archive_path"] = archive_path
            manifest["updated_at"] = self._timestamp()
            await self._write_manifest(task_id, manifest)

    async def load_job(self, task_id: str, *, include_private: bool = False) -> ProxyJob | None:
        operation = await self.load_operation(task_id)
        if operation is None or (not include_private and not operation.public):
            return None
        return self._operation_to_job(operation)

    async def load_operation(self, task_id: str) -> PersistedOperation | None:
        if not await self.exists(task_id):
            return None
        async with self._lock(task_id):
            manifest = await self._read_manifest(task_id)
        return self._operation_from_manifest(task_id, manifest)

    async def list_incomplete_operations(self) -> list[PersistedOperation]:
        manifests = await asyncio.to_thread(self._list_incomplete_manifests_sync)
        return [self._operation_from_manifest(task_id, manifest) for task_id, manifest in manifests]

    def _create_operation_sync(
        self,
        task_id: str,
        public: bool,
        source_kind: str,
        filename: str | None,
        target_kind: str,
        requested_formats: list[str],
        options: dict[str, Any],
        proxy_options: dict[str, Any] | None,
        auth_headers: dict[str, str],
        files: list[FilePayload],
        source_request: dict[str, Any] | None,
        normalized_source: NormalizedSource | None,
    ) -> None:
        task_dir = self._task_dir(task_id)
        task_dir.mkdir(parents=True, exist_ok=True)
        input_entries: list[dict[str, Any]] = []
        for index, file in enumerate(files):
            path = self._input_file_path(task_id, index, file.filename)
            self._copy_file_payload(file, path)
            input_entries.append(
                {
                    "filename": file.filename,
                    "content_type": file.content_type,
                    "path": str(path.relative_to(task_dir)),
                }
            )

        normalized_source_entry = None
        if normalized_source is not None:
            path = self._normalized_source_path(task_id, normalized_source.filename)
            self._write_bytes_atomic(path, normalized_source.content)
            normalized_source_entry = {
                "kind": normalized_source.kind,
                "filename": normalized_source.filename,
                "path": str(path.relative_to(task_dir)),
            }

        manifest = {
            "task_id": task_id,
            "public": public,
            "source_kind": source_kind,
            "filename": filename,
            "target_kind": target_kind,
            "requested_formats": list(requested_formats),
            "options": options,
            "proxy_options": proxy_options,
            "status": "pending",
            "error_message": None,
            "auth_headers": auth_headers,
            "source_request": source_request,
            "input_files": input_entries,
            "normalized_source": normalized_source_entry,
            "task_meta": ProxyTaskMeta(source_kind=source_kind, filename=filename).model_dump(exclude_none=True),
            "archive_path": None,
            "created_at": self._timestamp(),
            "updated_at": self._timestamp(),
        }
        self._write_json_atomic(self._manifest_path(task_id), manifest)

    def _operation_from_manifest(self, task_id: str, manifest: dict[str, Any]) -> PersistedOperation:
        task_dir = self._task_dir(task_id)
        input_files = [
            PersistedInputFile(
                filename=str(item["filename"]),
                content_type=str(item["content_type"]),
                path=task_dir / str(item["path"]),
            )
            for item in manifest.get("input_files") or []
        ]
        normalized_source = None
        normalized_source_entry = manifest.get("normalized_source")
        if normalized_source_entry is not None:
            source_path = task_dir / str(normalized_source_entry["path"])
            if source_path.exists():
                normalized_source = NormalizedSource(
                    kind=str(normalized_source_entry.get("kind") or "file"),
                    filename=str(normalized_source_entry.get("filename") or manifest.get("filename") or "document.pdf"),
                    content=source_path.read_bytes(),
                    original=manifest.get("source_request") or {},
                )
        result_payload_path = self.result_payload_path(task_id)
        result_zip_path = self.result_zip_path(task_id)
        return PersistedOperation(
            task_id=task_id,
            public=bool(manifest.get("public", True)),
            source_kind=str(manifest.get("source_kind") or "file"),
            filename=manifest.get("filename"),
            target_kind=str(manifest.get("target_kind") or "inbody"),
            requested_formats=[str(item) for item in manifest.get("requested_formats") or []],
            options=dict(manifest.get("options") or {}),
            proxy_options=ProxyOptions.model_validate(manifest["proxy_options"]) if manifest.get("proxy_options") else None,
            status=str(manifest.get("status") or "pending"),
            error_message=manifest.get("error_message"),
            auth_headers=dict(manifest.get("auth_headers") or {}),
            source_request=manifest.get("source_request"),
            input_files=input_files,
            normalized_source=normalized_source,
            meta=ProxyTaskMeta.model_validate(manifest.get("task_meta") or {}),
            result_payload_path=result_payload_path if result_payload_path.exists() else None,
            result_zip_path=result_zip_path if result_zip_path.exists() else None,
            archive_path=manifest.get("archive_path"),
            task_dir=task_dir,
        )

    def _operation_to_job(self, operation: PersistedOperation) -> ProxyJob:
        files = [
            FilePayload(
                filename=input_file.filename,
                content=None,
                content_type=input_file.content_type,
                temp_path=input_file.path,
                cleanup_enabled=False,
            )
            for input_file in operation.input_files
        ]
        return ProxyJob(
            task_id=operation.task_id,
            status=operation.status,
            public=operation.public,
            source_kind=operation.source_kind,
            filename=operation.filename,
            error_message=operation.error_message,
            target_kind=operation.target_kind,
            requested_formats=operation.requested_formats,
            options=operation.options,
            proxy_options=operation.proxy_options,
            files=files,
            source_request=operation.source_request,
            auth_headers=operation.auth_headers,
            task_dir=operation.task_dir,
            meta=operation.meta,
            result_payload_path=operation.result_payload_path,
            result_zip_path=operation.result_zip_path,
            archive_path=operation.archive_path,
        )

    def _list_incomplete_manifests_sync(self) -> list[tuple[str, dict[str, Any]]]:
        manifests: list[tuple[str, dict[str, Any]]] = []
        if not self.tasks_dir.exists():
            return manifests
        for task_dir in sorted(self.tasks_dir.iterdir()):
            manifest_path = task_dir / "manifest.json"
            if not manifest_path.exists():
                continue
            manifest = self._read_json_file(manifest_path)
            if str(manifest.get("status") or "pending") not in INCOMPLETE_TASK_STATUSES:
                continue
            manifests.append((task_dir.name, manifest))
        return manifests

    def _load_part_payloads_sync(self, task_id: str, total_parts: int) -> list[dict[str, Any]]:
        payloads: list[dict[str, Any]] = []
        for part_index in range(total_parts):
            part_path = self.part_payload_path(task_id, part_index)
            if not part_path.exists():
                raise FileNotFoundError(f"Missing persisted payload for chunk {part_index + 1}/{total_parts}: {part_path}")
            payloads.append(self._read_json_file(part_path))
        return payloads

    async def _read_manifest(self, task_id: str) -> dict[str, Any]:
        return await asyncio.to_thread(self._read_json_file, self._manifest_path(task_id))

    async def _write_manifest(self, task_id: str, manifest: dict[str, Any]) -> None:
        await asyncio.to_thread(self._write_json_atomic, self._manifest_path(task_id), manifest)

    def _copy_file_payload(self, file: FilePayload, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        if file.temp_path is not None:
            shutil.copy2(file.temp_path, destination)
            return
        self._write_bytes_atomic(destination, file.content or b"")

    def _read_json_file(self, path: Path) -> dict[str, Any]:
        return json.loads(path.read_text(encoding="utf-8"))

    def _write_json_atomic(self, path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self._tmp_path(path)
        tmp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        os.replace(tmp_path, path)

    def _write_bytes_atomic(self, path: Path, payload: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self._tmp_path(path)
        tmp_path.write_bytes(payload)
        os.replace(tmp_path, path)

    def _tmp_path(self, path: Path) -> Path:
        return path.with_name(f".{path.name}.tmp-{os.getpid()}-{time.time_ns()}")

    def _timestamp(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _safe_component(self, value: str) -> str:
        cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("._")
        return cleaned[:120] or "file"
