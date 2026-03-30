from __future__ import annotations

import asyncio
import logging
import re
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import HTTPException
from fastapi.responses import Response

from docling_proxy.archive import ArchiveStore
from docling_proxy.contracts import ConvertDocumentResponse, ProxyOptions, ProxyPartInfo, ProxyTaskMeta, TaskStatusResponse
from docling_proxy.local_docling import LocalDoclingManager
from docling_proxy.merge import build_batch_zip_response_bytes, build_zip_response_bytes, merge_results
from docling_proxy.models import FilePayload, NormalizedSource, ProxyJob, cleanup_file_payloads
from docling_proxy.parsing import decode_file_source, filename_from_url, normalize_requested_formats
from docling_proxy.pdf_tools import decide_split, is_pdf, split_pdf
from docling_proxy.state import TaskStateStore
from docling_proxy.store import job_store
from docling_proxy.upstream import UpstreamClient


logger = logging.getLogger(__name__)


def proxy_headers_subset(headers: dict[str, str]) -> dict[str, str]:
    return {k: v for k, v in headers.items()}


def file_target_kind(files: list[FilePayload], data: dict[str, Any]) -> str:
    if len(files) != 1:
        return "zip"
    return str(data.get("target_type") or "inbody")


def safe_batch_component(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("._")
    return cleaned[:120] or "converted"


@dataclass(slots=True)
class BatchFileResult:
    filename: str
    prefix: str
    processing_time: float
    zip_bytes: bytes | None = None
    payload: dict[str, Any] | None = None
    error: dict[str, Any] | None = None


@dataclass(slots=True)
class BatchConversionResult:
    zip_bytes: bytes
    status: str
    errors: list[dict[str, Any]]
    processing_time: float


class ProxyService:
    def __init__(
        self,
        upstream: UpstreamClient,
        local_docling: LocalDoclingManager,
        archive_store: ArchiveStore,
        state_store: TaskStateStore | None = None,
    ) -> None:
        self.upstream = upstream
        self.local_docling = local_docling
        self.archive_store = archive_store
        self.state_store = state_store

    async def resume_incomplete_operations(self) -> None:
        if self.state_store is None:
            return
        operations = await self.state_store.list_incomplete_operations()
        if not operations:
            return
        logger.info("recovering %s incomplete persisted operation(s)", len(operations))
        for operation in operations:
            job = await self.state_store.load_job(operation.task_id, include_private=True)
            if job is None:
                continue
            if operation.public:
                await job_store.put(job)
            logger.info(
                "[%s] resuming persisted %s operation: public=%s status=%s",
                operation.task_id,
                operation.source_kind,
                operation.public,
                operation.status,
            )
            if operation.source_kind == "file":
                asyncio.create_task(self._run_file_job(job, job.auth_headers))
            else:
                asyncio.create_task(self._run_source_job(job, job.auth_headers, operation.normalized_source))

    async def _ensure_operation_state(
        self,
        job: ProxyJob,
        headers: dict[str, str],
        normalized_source: NormalizedSource | None = None,
    ) -> None:
        if self.state_store is None:
            return
        await self.state_store.create_operation(
            job.task_id,
            public=job.public,
            source_kind=job.source_kind,
            filename=job.filename,
            target_kind=job.target_kind,
            requested_formats=job.requested_formats,
            options=job.options,
            proxy_options=job.proxy_options,
            headers=headers,
            files=job.files,
            source_request=job.source_request,
            normalized_source=normalized_source,
        )

    async def _sync_job_state(self, job: ProxyJob) -> None:
        if self.state_store is None:
            return
        await self.state_store.sync_job(job)

    async def _persist_final_job_state(self, job: ProxyJob) -> None:
        if self.state_store is None or not await self.state_store.exists(job.task_id):
            return
        if job.result_payload is not None:
            job.result_payload_path = await self.state_store.persist_final_payload(job.task_id, job.result_payload)
        if job.result_zip is not None:
            job.result_zip_path = await self.state_store.persist_final_zip(job.task_id, job.result_zip)
        await self._sync_job_state(job)

    async def _hydrate_job_result_from_state(self, job: ProxyJob) -> None:
        if self.state_store is None:
            return
        if job.result_zip is None:
            if job.result_zip_path is None:
                candidate = self.state_store.result_zip_path(job.task_id)
                if await asyncio.to_thread(candidate.exists):
                    job.result_zip_path = candidate
            if job.result_zip_path is not None:
                job.result_zip = await self.state_store.load_result_zip(job.task_id)
        if job.result_payload is None:
            if job.result_payload_path is None:
                candidate = self.state_store.result_payload_path(job.task_id)
                if await asyncio.to_thread(candidate.exists):
                    job.result_payload_path = candidate
            if job.result_payload_path is not None:
                job.result_payload = await self.state_store.load_result_payload(job.task_id)

    async def _restore_completed_job_if_needed(self, job: ProxyJob) -> bool:
        if job.status in {"success", "failure"}:
            return False
        await self._hydrate_job_result_from_state(job)
        if job.result_payload is None and job.result_zip is None:
            return False
        job.status = "success"
        if job.archive_path is None:
            await self._archive_job_result(job)
        await self._sync_job_state(job)
        return True

    async def _load_job(self, task_id: str) -> ProxyJob | None:
        job = await job_store.get(task_id)
        if job is not None:
            return job
        if self.state_store is None:
            return None
        job = await self.state_store.load_job(task_id)
        if job is not None and job.public:
            await job_store.put(job)
        return job

    def _build_sync_result_from_job(self, job: ProxyJob):
        if job.result_zip is not None:
            return self._zip_response(job.result_zip)
        if job.result_payload is None:
            raise RuntimeError("Split conversion completed without a result payload.")
        return ConvertDocumentResponse.model_validate(job.result_payload)

    async def process_file_sync(
        self,
        files: list[FilePayload],
        data: dict[str, Any],
        proxy_options: ProxyOptions | None,
        headers: dict[str, str],
    ):
        request_id = uuid.uuid4().hex[:8]
        logger.info("[%s] received sync file request: file_count=%s", request_id, len(files))
        archive_filename = files[0].filename if len(files) == 1 else f"batch-{len(files)}-files"
        target_kind = file_target_kind(files, data)
        try:
            if len(files) != 1:
                logger.info("[%s] isolated local batch processing: file_count=%s target=%s", request_id, len(files), target_kind)
                batch_result = await self._process_multi_file_request(request_id, files, data, headers)
                await self._archive_zip_bytes(
                    request_id,
                    "file",
                    archive_filename,
                    target_kind,
                    batch_result.zip_bytes,
                    status=batch_result.status,
                    errors=batch_result.errors,
                    processing_time=batch_result.processing_time,
                )
                return self._zip_response(batch_result.zip_bytes)

            file = files[0]
            file_content = await file.read_content()
            if not is_pdf(file.filename, file.content_type, file_content):
                logger.info("[%s] isolated local processing: non-pdf file=%s", request_id, file.filename)
                response = await self.local_docling.relay_file_sync(files, data, headers)
                await self._archive_httpx_response(request_id, "file", file.filename, target_kind, response)
                return response

            decision = decide_split(file_content, proxy_options)
            logger.info(
                "[%s] evaluated file=%s pages=%s should_split=%s reason=%s target=%s",
                request_id,
                file.filename,
                decision.total_pages,
                decision.should_split,
                decision.reason,
                target_kind,
            )
            if not decision.should_split:
                logger.info("[%s] isolated local processing: below split threshold file=%s", request_id, file.filename)
                response = await self.local_docling.relay_file_sync(files, data, headers)
                await self._archive_httpx_response(request_id, "file", file.filename, target_kind, response)
                return response
            split_job = ProxyJob(
                task_id=request_id,
                public=False,
                source_kind="file",
                filename=file.filename,
                target_kind=target_kind,
                requested_formats=normalize_requested_formats(data),
                options=data,
                proxy_options=proxy_options,
                files=files,
                auth_headers=proxy_headers_subset(headers),
                meta=ProxyTaskMeta(source_kind="file", filename=file.filename),
            )
            await self._ensure_operation_state(split_job, headers)
            await self._run_file_job(split_job, proxy_headers_subset(headers))
            if split_job.status == "failure":
                raise RuntimeError(split_job.error_message or f"Split conversion failed for {file.filename}.")
            return self._build_sync_result_from_job(split_job)
        finally:
            await cleanup_file_payloads(files)

    async def process_source_sync(
        self,
        payload: dict[str, Any],
        normalized_source: NormalizedSource | None,
        options: dict[str, Any],
        proxy_options: ProxyOptions | None,
        headers: dict[str, str],
        target_kind: str,
    ):
        request_id = uuid.uuid4().hex[:8]
        logger.info("[%s] received sync source request", request_id)
        archive_filename = normalized_source.filename if normalized_source else "source-request"
        if normalized_source is None:
            logger.info("[%s] isolated local processing: unsupported source shape", request_id)
            response = await self.local_docling.relay_source_sync(payload, headers)
            await self._archive_httpx_response(request_id, "source", archive_filename, target_kind, response)
            return response

        if not is_pdf(normalized_source.filename, None, normalized_source.content):
            logger.info("[%s] isolated local processing: non-pdf source=%s", request_id, normalized_source.filename)
            response = await self.local_docling.relay_source_sync(payload, headers)
            await self._archive_httpx_response(request_id, "source", normalized_source.filename, target_kind, response)
            return response

        decision = decide_split(normalized_source.content, proxy_options)
        logger.info(
            "[%s] evaluated source=%s pages=%s should_split=%s reason=%s target=%s",
            request_id,
            normalized_source.filename,
            decision.total_pages,
            decision.should_split,
            decision.reason,
            target_kind,
        )
        if not decision.should_split:
            logger.info("[%s] isolated local processing: below split threshold source=%s", request_id, normalized_source.filename)
            response = await self.local_docling.relay_source_sync(payload, headers)
            await self._archive_httpx_response(request_id, "source", normalized_source.filename, target_kind, response)
            return response
        split_job = ProxyJob(
            task_id=request_id,
            public=False,
            source_kind="source",
            filename=normalized_source.filename,
            target_kind=target_kind,
            requested_formats=normalize_requested_formats(options),
            options=options,
            proxy_options=proxy_options,
            source_request=payload,
            auth_headers=proxy_headers_subset(headers),
            meta=ProxyTaskMeta(source_kind="source", filename=normalized_source.filename),
        )
        await self._ensure_operation_state(split_job, headers, normalized_source)
        await self._run_source_job(split_job, proxy_headers_subset(headers), normalized_source)
        if split_job.status == "failure":
            raise RuntimeError(split_job.error_message or f"Split conversion failed for {normalized_source.filename}.")
        return self._build_sync_result_from_job(split_job)

    async def enqueue_file_job(
        self,
        files: list[FilePayload],
        data: dict[str, Any],
        proxy_options: ProxyOptions | None,
        headers: dict[str, str],
    ) -> TaskStatusResponse:
        task_id = uuid.uuid4().hex
        filename = files[0].filename if len(files) == 1 else f"batch-{len(files)}-files"
        job = ProxyJob(
            task_id=task_id,
            source_kind="file",
            filename=filename,
            target_kind=file_target_kind(files, data),
            requested_formats=normalize_requested_formats(data),
            options=data,
            proxy_options=proxy_options,
            files=files,
            auth_headers=proxy_headers_subset(headers),
            meta=ProxyTaskMeta(source_kind="file", filename=filename),
        )
        await self._ensure_operation_state(job, headers)
        await job_store.put(job)
        logger.info("[%s] queued async file job: file=%s target=%s", task_id, job.filename, job.target_kind)
        asyncio.create_task(self._run_file_job(job, proxy_headers_subset(headers)))
        return self._status_from_job(job)

    async def enqueue_source_job(
        self,
        payload: dict[str, Any],
        options: dict[str, Any],
        proxy_options: ProxyOptions | None,
        headers: dict[str, str],
        normalized_source: NormalizedSource | None,
        target_kind: str,
    ) -> TaskStatusResponse:
        task_id = uuid.uuid4().hex
        job = ProxyJob(
            task_id=task_id,
            source_kind="source",
            filename=normalized_source.filename if normalized_source else None,
            target_kind=target_kind,
            requested_formats=normalize_requested_formats(options),
            options=options,
            proxy_options=proxy_options,
            source_request=payload,
            auth_headers=proxy_headers_subset(headers),
            meta=ProxyTaskMeta(source_kind="source", filename=normalized_source.filename if normalized_source else None),
        )
        await self._ensure_operation_state(job, headers, normalized_source)
        await job_store.put(job)
        logger.info("[%s] queued async source job: file=%s target=%s", task_id, job.filename, job.target_kind)
        asyncio.create_task(self._run_source_job(job, proxy_headers_subset(headers), normalized_source))
        return self._status_from_job(job)

    async def get_status(self, task_id: str) -> TaskStatusResponse:
        job = await self._load_job(task_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Task not found.")
        await self._restore_completed_job_if_needed(job)
        return self._status_from_job(job)

    async def get_result(self, task_id: str) -> ProxyJob:
        job = await self._load_job(task_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Task not found.")
        await self._restore_completed_job_if_needed(job)
        await self._hydrate_job_result_from_state(job)
        if job.status not in {"success", "failure"}:
            raise HTTPException(status_code=404, detail="Task result not found. Please wait for a completion status.")
        return job

    async def _run_file_job(self, job: ProxyJob, headers: dict[str, str]) -> None:
        if await self._restore_completed_job_if_needed(job):
            logger.info("[%s] restored completed file job from persisted state", job.task_id)
            return
        job.status = "started"
        job.auth_headers = proxy_headers_subset(headers)
        await self._sync_job_state(job)
        logger.info("[%s] async file job started: file=%s", job.task_id, job.filename)
        try:
            if len(job.files) != 1:
                logger.info("[%s] async isolated local batch processing: file_count=%s target=%s", job.task_id, len(job.files), job.target_kind)
                batch_result = await self._process_multi_file_request(job.task_id, job.files, job.options, headers)
                await self._store_batch_result(job, batch_result)
                return

            file = job.files[0]
            file_content = await file.read_content()
            if not is_pdf(file.filename, file.content_type, file_content):
                logger.info("[%s] async isolated local processing: non-pdf file=%s", job.task_id, file.filename)
                response = await self.local_docling.relay_file_sync(job.files, job.options, headers)
                await self._store_local_response(job, response)
                return

            decision = decide_split(file_content, job.proxy_options)
            logger.info(
                "[%s] async evaluated file=%s pages=%s should_split=%s reason=%s",
                job.task_id,
                file.filename,
                decision.total_pages,
                decision.should_split,
                decision.reason,
            )
            if not decision.should_split:
                logger.info("[%s] async isolated local processing: below split threshold file=%s", job.task_id, file.filename)
                response = await self.local_docling.relay_file_sync(job.files, job.options, headers)
                await self._store_local_response(job, response)
                return

            result = await self._process_split_file(
                filename=file.filename,
                data=file_content,
                options=job.options,
                proxy_options=job.proxy_options,
                headers=headers,
                target_kind=job.target_kind,
                job=job,
            )
            await self._store_split_result(job, result, file.filename)
        except Exception as exc:  # noqa: BLE001
            job.status = "failure"
            job.error_message = str(exc)
            await self._sync_job_state(job)
            logger.exception("[%s] async file job failed: %s", job.task_id, exc)
        finally:
            await cleanup_file_payloads(job.files)

    async def _run_source_job(self, job: ProxyJob, headers: dict[str, str], normalized_source: NormalizedSource | None) -> None:
        if await self._restore_completed_job_if_needed(job):
            logger.info("[%s] restored completed source job from persisted state", job.task_id)
            return
        job.status = "started"
        job.auth_headers = proxy_headers_subset(headers)
        await self._sync_job_state(job)
        logger.info("[%s] async source job started: file=%s", job.task_id, job.filename)
        try:
            if normalized_source is None or not is_pdf(normalized_source.filename, None, normalized_source.content):
                logger.info("[%s] async isolated local processing: unsupported or non-pdf source", job.task_id)
                response = await self.local_docling.relay_source_sync(job.source_request or {}, headers)
                await self._store_local_response(job, response)
                return

            decision = decide_split(normalized_source.content, job.proxy_options)
            logger.info(
                "[%s] async evaluated source=%s pages=%s should_split=%s reason=%s",
                job.task_id,
                normalized_source.filename,
                decision.total_pages,
                decision.should_split,
                decision.reason,
            )
            if not decision.should_split:
                logger.info("[%s] async isolated local processing: below split threshold source=%s", job.task_id, normalized_source.filename)
                response = await self.local_docling.relay_source_sync(job.source_request or {}, headers)
                await self._store_local_response(job, response)
                return

            result = await self._process_split_file(
                filename=normalized_source.filename,
                data=normalized_source.content,
                options=job.options,
                proxy_options=job.proxy_options,
                headers=headers,
                target_kind=job.target_kind,
                job=job,
            )
            await self._store_split_result(job, result, normalized_source.filename)
        except Exception as exc:  # noqa: BLE001
            job.status = "failure"
            job.error_message = str(exc)
            await self._sync_job_state(job)
            logger.exception("[%s] async source job failed: %s", job.task_id, exc)

    async def _store_local_response(self, job: ProxyJob, response) -> None:
        response.raise_for_status()
        if response.headers.get("content-type", "").startswith("application/json"):
            job.result_payload = response.json()
            job.result_zip = None
        else:
            job.result_payload = None
            job.result_zip = response.content
            job.target_kind = "zip"
        await self._persist_final_job_state(job)
        await self._archive_job_result(job)
        job.status = "success"
        await self._sync_job_state(job)
        logger.info("[%s] local conversion ready: file=%s target=%s", job.task_id, job.filename, job.target_kind)

    async def _store_batch_result(self, job: ProxyJob, result: BatchConversionResult) -> None:
        job.result_zip = result.zip_bytes
        job.result_payload = {
            "status": result.status,
            "errors": result.errors,
            "processing_time": result.processing_time,
        }
        await self._persist_final_job_state(job)
        await self._archive_job_result(job)
        job.status = "success"
        await self._sync_job_state(job)
        logger.info("[%s] merged batch zip ready: file=%s target=%s status=%s", job.task_id, job.filename, job.target_kind, result.status)

    async def _store_split_result(self, job: ProxyJob, result: ConvertDocumentResponse, filename: str) -> None:
        if job.target_kind == "zip":
            job.result_zip = build_zip_response_bytes(result, filename)
        job.result_payload = result.model_dump(exclude_none=True)
        await self._persist_final_job_state(job)
        await self._archive_job_result(job)
        job.status = "success"
        await self._sync_job_state(job)
        logger.info("[%s] merged split result ready: file=%s target=%s", job.task_id, filename, job.target_kind)

    async def _archive_httpx_response(
        self,
        request_id: str,
        source_kind: str,
        filename: str | None,
        target_kind: str,
        response,
    ) -> None:
        if response.status_code >= 400:
            return
        content_type = response.headers.get("content-type", "")
        archive_path = None
        if content_type.startswith("application/json"):
            archive_path = await self.archive_store.persist_payload(
                request_id,
                source_kind,
                filename,
                target_kind,
                response.json(),
            )
        else:
            archive_path = await self._archive_zip_bytes(
                request_id,
                source_kind,
                filename,
                "zip",
                response.content,
            )
        if archive_path is not None:
            if self.state_store is not None and await self.state_store.exists(request_id):
                await self.state_store.set_archive_path(request_id, str(archive_path))
            logger.info("[%s] archived conversion outputs to %s", request_id, archive_path)

    async def _archive_convert_result(
        self,
        request_id: str,
        source_kind: str,
        filename: str | None,
        target_kind: str,
        result: ConvertDocumentResponse,
    ) -> None:
        archive_path = await self.archive_store.persist_payload(
            request_id,
            source_kind,
            filename,
            target_kind,
            result.model_dump(exclude_none=True),
        )
        if archive_path is not None:
            if self.state_store is not None and await self.state_store.exists(request_id):
                await self.state_store.set_archive_path(request_id, str(archive_path))
            logger.info("[%s] archived conversion outputs to %s", request_id, archive_path)

    async def _archive_zip_bytes(
        self,
        request_id: str,
        source_kind: str,
        filename: str | None,
        target_kind: str,
        zip_bytes: bytes,
        *,
        status: str = "success",
        errors: list[dict[str, Any]] | None = None,
        processing_time: float | None = None,
    ):
        return await self.archive_store.persist_zip(
            request_id,
            source_kind,
            filename,
            target_kind,
            zip_bytes,
            status=status,
            errors=errors,
            processing_time=processing_time,
        )

    async def _archive_job_result(self, job: ProxyJob) -> None:
        archive_filename = job.filename or "converted"
        archive_path = None
        if job.result_zip is not None:
            archive_path = await self._archive_zip_bytes(
                job.task_id,
                job.source_kind,
                archive_filename,
                job.target_kind,
                job.result_zip,
                status=(job.result_payload or {}).get("status", "success") if job.result_payload else "success",
                errors=(job.result_payload or {}).get("errors") if job.result_payload else None,
                processing_time=(job.result_payload or {}).get("processing_time") if job.result_payload else None,
            )
        elif job.result_payload is not None:
            archive_path = await self.archive_store.persist_payload(
                job.task_id,
                job.source_kind,
                archive_filename,
                job.target_kind,
                job.result_payload,
            )
        if archive_path is not None:
            job.archive_path = str(archive_path)
            await self._sync_job_state(job)
            logger.info("[%s] archived conversion outputs to %s", job.task_id, archive_path)

    def _zip_response(self, zip_bytes: bytes) -> Response:
        return Response(
            content=zip_bytes,
            media_type="application/zip",
            headers={"Content-Disposition": 'attachment; filename="converted_docs.zip"'},
        )

    def _batch_request_options(self, options: dict[str, Any]) -> dict[str, Any]:
        batch_options = dict(options)
        batch_options["target_type"] = "zip"
        return batch_options

    def _batch_output_prefixes(self, files: list[FilePayload]) -> list[str]:
        counts: dict[str, int] = {}
        prefixes: list[str] = []
        for index, file in enumerate(files, start=1):
            base_name = Path(file.filename or f"file-{index}").stem or f"file-{index}"
            safe_base = safe_batch_component(base_name)
            counts[safe_base] = counts.get(safe_base, 0) + 1
            suffix = counts[safe_base]
            prefixes.append(safe_base if suffix == 1 else f"{safe_base}_{suffix}")
        return prefixes

    async def _process_multi_file_request(
        self,
        operation_id: str,
        files: list[FilePayload],
        options: dict[str, Any],
        headers: dict[str, str],
    ) -> BatchConversionResult:
        total_files = len(files)
        completed_files = 0
        progress_lock = asyncio.Lock()
        batch_options = self._batch_request_options(options)
        prefixes = self._batch_output_prefixes(files)
        logger.info("[%s] batching %s file(s) into merged zip output", operation_id, total_files)

        async def convert_file(index: int, file: FilePayload, prefix: str) -> BatchFileResult:
            nonlocal completed_files
            started_at = time.perf_counter()
            logger.info("[%s] queued batch file %s/%s: file=%s", operation_id, index + 1, total_files, file.filename)

            def log_file_started(timing) -> None:
                logger.info(
                    "[%s] started batch file %s/%s: file=%s, wait=%.2fs, startup=%.2fs",
                    operation_id,
                    index + 1,
                    total_files,
                    file.filename,
                    timing.wait_duration_sec,
                    timing.startup_duration_sec,
                )

            try:
                execution = await self.local_docling.execute_file_sync(
                    [file],
                    batch_options,
                    headers,
                    on_request_start=log_file_started,
                )
                execution.response.raise_for_status()
                content_type = execution.response.headers.get("content-type", "")
                payload = execution.response.json() if content_type.startswith("application/json") else None
                result = BatchFileResult(
                    filename=file.filename,
                    prefix=prefix,
                    processing_time=float((payload or {}).get("processing_time") or execution.timing.processing_duration_sec),
                    zip_bytes=None if payload is not None else execution.response.content,
                    payload=payload,
                )
                logger.info(
                    "[%s] finished batch file %s/%s: file=%s, total=%.2fs, wait=%.2fs, startup=%.2fs, convert=%.2fs",
                    operation_id,
                    index + 1,
                    total_files,
                    file.filename,
                    execution.timing.processing_duration_sec,
                    execution.timing.wait_duration_sec,
                    execution.timing.startup_duration_sec,
                    execution.timing.request_duration_sec,
                )
            except Exception as exc:
                result = BatchFileResult(
                    filename=file.filename,
                    prefix=prefix,
                    processing_time=time.perf_counter() - started_at,
                    error={"filename": file.filename, "message": str(exc)},
                )
                logger.error(
                    "[%s] batch file %s/%s failed: file=%s, error=%s",
                    operation_id,
                    index + 1,
                    total_files,
                    file.filename,
                    exc,
                )
            async with progress_lock:
                completed_files += 1
                logger.info("[%s] completed batch files %s/%s", operation_id, completed_files, total_files)
            return result

        results = await asyncio.gather(
            *(convert_file(index, file, prefixes[index]) for index, file in enumerate(files))
        )

        zip_outputs: list[tuple[str, bytes]] = []
        payload_outputs: list[tuple[str, str, dict[str, Any]]] = []
        errors: list[dict[str, Any]] = []
        total_processing_time = 0.0
        succeeded = 0

        for result in results:
            total_processing_time += result.processing_time
            if result.error is not None:
                errors.append(result.error)
                continue
            if result.zip_bytes is not None:
                zip_outputs.append((result.prefix, result.zip_bytes))
                succeeded += 1
                continue
            if result.payload is None:
                errors.append({"filename": result.filename, "message": "Missing batch output payload."})
                continue

            payload_status = str(result.payload.get("status") or "success")
            if payload_status not in {"success", "partial_success"}:
                errors.append(
                    {
                        "filename": result.filename,
                        "message": f"Unexpected batch payload status: {payload_status}",
                        "errors": list(result.payload.get("errors") or []),
                    }
                )
                continue

            payload_outputs.append((result.prefix, result.filename, result.payload))
            succeeded += 1
            if payload_status == "partial_success":
                errors.append(
                    {
                        "filename": result.filename,
                        "status": payload_status,
                        "errors": list(result.payload.get("errors") or []),
                    }
                )

        if succeeded == 0:
            error_summary = "; ".join(
                f"{error.get('filename', 'unknown')}: {error.get('message', error.get('status', 'conversion failed'))}"
                for error in errors[:5]
            )
            raise RuntimeError(
                f"All files in batch failed to convert. {error_summary}" if error_summary else "All files in batch failed to convert."
            )

        status = "success" if not errors else "partial_success"
        zip_bytes = build_batch_zip_response_bytes(zip_outputs, payload_outputs, errors or None)
        logger.info(
            "[%s] batch processing completed: succeeded=%s failed=%s status=%s",
            operation_id,
            succeeded,
            total_files - succeeded,
            status,
        )
        return BatchConversionResult(
            zip_bytes=zip_bytes,
            status=status,
            errors=errors,
            processing_time=total_processing_time,
        )

    async def _process_split_file(
        self,
        filename: str,
        data: bytes,
        options: dict[str, Any],
        proxy_options: ProxyOptions | None,
        headers: dict[str, str],
        target_kind: str,
        job: ProxyJob | None = None,
        operation_id: str | None = None,
    ) -> ConvertDocumentResponse:
        op_id = operation_id or (job.task_id if job is not None else uuid.uuid4().hex[:8])
        requested_formats = normalize_requested_formats(options)
        part_options = dict(options)
        part_options["target_type"] = "inbody"
        if "json" not in requested_formats:
            part_options["to_formats"] = requested_formats + ["json"]

        parts = split_pdf(data, proxy_options)
        use_persistence = self.state_store is not None and await self.state_store.exists(op_id)
        if use_persistence and self.state_store is not None:
            split_meta = await self.state_store.ensure_split_plan(
                op_id,
                source_kind=job.source_kind if job is not None else "file",
                filename=filename,
                parts=[(start, end) for start, end, _ in parts],
            )
            if job is not None:
                job.meta = split_meta

        total_chunks = len(parts)
        total_pages = sum(end - start + 1 for start, end, _ in parts)
        completed_chunks = 0
        completed_pages = 0
        progress_lock = asyncio.Lock()
        logger.info(
            "[%s] splitting file=%s into %s part(s) with formats=%s target=%s",
            op_id,
            filename,
            total_chunks,
            requested_formats,
            target_kind,
        )
        if job is not None and not job.meta.split:
            job.meta.split = True
            job.meta.total_parts = len(parts)
            job.meta.completed_parts = 0
            job.meta.filename = filename
            job.meta.parts = [
                ProxyPartInfo(part_index=index, start_page=start, end_page=end)
                for index, (start, end, _) in enumerate(parts)
            ]
        if job is not None and use_persistence and self.state_store is not None:
            recovered_completed_parts = 0
            for index, part in enumerate(job.meta.parts):
                page_count = part.end_page - part.start_page + 1
                if part.task_status == "success" and await self.state_store.part_payload_exists(op_id, index):
                    completed_chunks += 1
                    completed_pages += page_count
                    recovered_completed_parts += 1
                    continue
                if part.task_status == "success":
                    part.task_status = "pending"
                if part.task_status == "failure":
                    part.task_status = "pending"
                    part.error_message = None
            job.meta.completed_parts = recovered_completed_parts
            await self._sync_job_state(job)
            if recovered_completed_parts:
                logger.info(
                    "[%s] resuming split file=%s with %s/%s completed chunk(s)",
                    op_id,
                    filename,
                    recovered_completed_parts,
                    total_chunks,
                )

        async def convert_part(index: int, start_page: int, end_page: int, part_bytes: bytes) -> dict[str, Any]:
            nonlocal completed_chunks, completed_pages
            page_count = end_page - start_page + 1
            logger.info(
                "[%s] queued chunk %s/%s for file=%s, pages %s-%s",
                op_id,
                index + 1,
                total_chunks,
                filename,
                start_page,
                end_page,
            )
            part_file = FilePayload(filename=f"{index + 1:04d}_{filename}", content=part_bytes, content_type="application/pdf")

            async def log_chunk_started(timing) -> None:
                logger.info(
                    "[%s] started chunk %s/%s for file=%s, pages %s-%s, wait=%.2fs, startup=%.2fs",
                    op_id,
                    index + 1,
                    total_chunks,
                    filename,
                    start_page,
                    end_page,
                    timing.wait_duration_sec,
                    timing.startup_duration_sec,
                )
                if job is not None:
                    async with progress_lock:
                        job.meta.parts[index].task_status = "started"
                        job.meta.parts[index].error_message = None
                        if use_persistence:
                            await self._sync_job_state(job)

            try:
                execution = await self.local_docling.execute_file_sync(
                    [part_file],
                    part_options,
                    headers,
                    on_request_start=log_chunk_started,
                )
                execution.response.raise_for_status()
                payload = execution.response.json()
            except Exception as exc:
                if job is not None:
                    async with progress_lock:
                        job.meta.parts[index].error_message = str(exc)
                        job.meta.parts[index].task_status = "failure"
                        if use_persistence:
                            await self._sync_job_state(job)
                logger.error(
                    "[%s] chunk %s/%s for file=%s failed, pages %s-%s, error=%s",
                    op_id,
                    index + 1,
                    total_chunks,
                    filename,
                    start_page,
                    end_page,
                    exc,
                )
                raise
            if use_persistence and self.state_store is not None:
                await self.state_store.persist_part_payload(op_id, index, payload)
            async with progress_lock:
                if job is not None:
                    job.meta.parts[index].task_status = "success"
                    job.meta.parts[index].error_message = None
                    job.meta.completed_parts = sum(1 for part in job.meta.parts if part.task_status == "success")
                    if use_persistence:
                        await self._sync_job_state(job)
                completed_chunks += 1
                completed_pages += page_count
                per_page_duration_sec = execution.timing.processing_duration_sec / page_count
                logger.info(
                    "[%s] finished chunk %s/%s for file=%s, pages %s-%s, total=%.2fs, per_page=%.2fs, wait=%.2fs, startup=%.2fs, convert=%.2fs",
                    op_id,
                    index + 1,
                    total_chunks,
                    filename,
                    start_page,
                    end_page,
                    execution.timing.processing_duration_sec,
                    per_page_duration_sec,
                    execution.timing.wait_duration_sec,
                    execution.timing.startup_duration_sec,
                    execution.timing.request_duration_sec,
                )
                logger.info(
                    "[%s] completed %s/%s for file=%s, %s/%s",
                    op_id,
                    completed_chunks,
                    total_chunks,
                    filename,
                    completed_pages,
                    total_pages,
                )
            return payload

        if use_persistence and self.state_store is not None:
            pending_parts = [
                (index, start, end, part_bytes)
                for index, (start, end, part_bytes) in enumerate(parts)
                if not (
                    job is not None
                    and index < len(job.meta.parts)
                    and job.meta.parts[index].task_status == "success"
                    and await self.state_store.part_payload_exists(op_id, index)
                )
            ]
            if pending_parts:
                await asyncio.gather(
                    *(convert_part(index, start, end, part_bytes) for index, start, end, part_bytes in pending_parts)
                )
            results = await self.state_store.load_part_payloads(op_id, total_chunks)
        else:
            results = await asyncio.gather(
                *(convert_part(index, start, end, part_bytes) for index, (start, end, part_bytes) in enumerate(parts))
            )
        logger.info("[%s] all chunks completed; merging %s part(s) for file=%s", op_id, len(parts), filename)
        proxy_meta = {
            "split": True,
            "parts": [part.model_dump() for part in job.meta.parts] if job is not None else [
                {"part_index": index, "start_page": start, "end_page": end}
                for index, (start, end, _) in enumerate(parts)
            ],
        }
        return merge_results(
            results,
            requested_formats=requested_formats,
            page_break_placeholder=options.get("md_page_break_placeholder"),
            include_proxy_meta=bool(proxy_options and proxy_options.include_proxy_meta),
            proxy_meta=proxy_meta,
        )

    def _status_from_job(self, job: ProxyJob) -> TaskStatusResponse:
        return TaskStatusResponse(
            task_id=job.task_id,
            task_status=job.status,
            task_meta=job.meta.model_dump(),
            error_message=job.error_message,
        )


async def source_to_normalized_source(upstream: UpstreamClient, sources: list[dict[str, Any]]) -> NormalizedSource | None:
    if len(sources) != 1:
        return None
    source = sources[0]
    kind = source.get("kind")
    if kind == "file":
        return decode_file_source(source)
    if kind == "http":
        url = source.get("url")
        if not url:
            raise ValueError("HTTP source is missing url.")
        response = await upstream.download(url, headers=source.get("headers"))
        return NormalizedSource(
            kind="http",
            filename=source.get("filename") or filename_from_url(url),
            content=response.content,
            original=source,
        )
    return None
