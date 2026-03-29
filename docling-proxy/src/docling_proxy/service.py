from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any

from fastapi import HTTPException
from fastapi.responses import Response

from docling_proxy.config import settings
from docling_proxy.contracts import ConvertDocumentResponse, ProxyOptions, ProxyPartInfo, ProxyTaskMeta, TaskStatusResponse
from docling_proxy.local_docling import LocalDoclingManager
from docling_proxy.merge import build_zip_response_bytes, merge_results
from docling_proxy.models import FilePayload, NormalizedSource, ProxyJob
from docling_proxy.parsing import decode_file_source, filename_from_url, normalize_requested_formats
from docling_proxy.pdf_tools import decide_split, is_pdf, split_pdf
from docling_proxy.store import job_store
from docling_proxy.upstream import UpstreamClient


logger = logging.getLogger(__name__)


def proxy_headers_subset(headers: dict[str, str]) -> dict[str, str]:
    return {k: v for k, v in headers.items()}


class ProxyService:
    def __init__(self, upstream: UpstreamClient, local_docling: LocalDoclingManager) -> None:
        self.upstream = upstream
        self.local_docling = local_docling

    async def process_file_sync(
        self,
        files: list[FilePayload],
        data: dict[str, Any],
        proxy_options: ProxyOptions | None,
        headers: dict[str, str],
    ):
        request_id = uuid.uuid4().hex[:8]
        logger.info("[%s] received sync file request: file_count=%s", request_id, len(files))
        if len(files) != 1:
            logger.info("[%s] isolated local processing: multi-file request", request_id)
            return await self.local_docling.relay_file_sync(files, data, headers)

        file = files[0]
        if not is_pdf(file.filename, file.content_type, file.content):
            logger.info("[%s] isolated local processing: non-pdf file=%s", request_id, file.filename)
            return await self.local_docling.relay_file_sync(files, data, headers)

        decision = decide_split(file.content, proxy_options)
        target_kind = str(data.get("target_type") or "inbody")
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
            return await self.local_docling.relay_file_sync(files, data, headers)
        result = await self._process_split_file(
            filename=file.filename,
            data=file.content,
            options=data,
            proxy_options=proxy_options,
            headers=headers,
            target_kind=target_kind,
            operation_id=request_id,
        )
        logger.info("[%s] split processing completed for file=%s", request_id, file.filename)
        if target_kind == "zip":
            return Response(
                content=build_zip_response_bytes(result, file.filename),
                media_type="application/zip",
                headers={"Content-Disposition": 'attachment; filename="converted_docs.zip"'},
            )
        return result

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
        if normalized_source is None:
            logger.info("[%s] isolated local processing: unsupported source shape", request_id)
            return await self.local_docling.relay_source_sync(payload, headers)

        if not is_pdf(normalized_source.filename, None, normalized_source.content):
            logger.info("[%s] isolated local processing: non-pdf source=%s", request_id, normalized_source.filename)
            return await self.local_docling.relay_source_sync(payload, headers)

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
            return await self.local_docling.relay_source_sync(payload, headers)
        result = await self._process_split_file(
            filename=normalized_source.filename,
            data=normalized_source.content,
            options=options,
            proxy_options=proxy_options,
            headers=headers,
            target_kind=target_kind,
            operation_id=request_id,
        )
        logger.info("[%s] split processing completed for source=%s", request_id, normalized_source.filename)
        if target_kind == "zip":
            return Response(
                content=build_zip_response_bytes(result, normalized_source.filename),
                media_type="application/zip",
                headers={"Content-Disposition": 'attachment; filename="converted_docs.zip"'},
            )
        return result

    async def enqueue_file_job(
        self,
        files: list[FilePayload],
        data: dict[str, Any],
        proxy_options: ProxyOptions | None,
        headers: dict[str, str],
    ) -> TaskStatusResponse:
        task_id = uuid.uuid4().hex
        job = ProxyJob(
            task_id=task_id,
            source_kind="file",
            filename=files[0].filename if len(files) == 1 else None,
            target_kind=str(data.get("target_type") or "inbody"),
            requested_formats=normalize_requested_formats(data),
            options=data,
            proxy_options=proxy_options,
            files=files,
            meta=ProxyTaskMeta(source_kind="file", filename=files[0].filename if len(files) == 1 else None),
        )
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
            meta=ProxyTaskMeta(source_kind="source", filename=normalized_source.filename if normalized_source else None),
        )
        await job_store.put(job)
        logger.info("[%s] queued async source job: file=%s target=%s", task_id, job.filename, job.target_kind)
        asyncio.create_task(self._run_source_job(job, proxy_headers_subset(headers), normalized_source))
        return self._status_from_job(job)

    async def get_status(self, task_id: str) -> TaskStatusResponse:
        job = await job_store.get(task_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Task not found.")
        return self._status_from_job(job)

    async def get_result(self, task_id: str) -> ProxyJob:
        job = await job_store.get(task_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Task not found.")
        if job.status not in {"success", "failure"}:
            raise HTTPException(status_code=404, detail="Task result not found. Please wait for a completion status.")
        return job

    async def _run_file_job(self, job: ProxyJob, headers: dict[str, str]) -> None:
        job.status = "started"
        logger.info("[%s] async file job started: file=%s", job.task_id, job.filename)
        try:
            if len(job.files) != 1:
                logger.info("[%s] async isolated local processing: multi-file request", job.task_id)
                response = await self.local_docling.relay_file_sync(job.files, job.options, headers)
                await self._store_local_response(job, response)
                return

            file = job.files[0]
            if not is_pdf(file.filename, file.content_type, file.content):
                logger.info("[%s] async isolated local processing: non-pdf file=%s", job.task_id, file.filename)
                response = await self.local_docling.relay_file_sync(job.files, job.options, headers)
                await self._store_local_response(job, response)
                return

            decision = decide_split(file.content, job.proxy_options)
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
                data=file.content,
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
            logger.exception("[%s] async file job failed: %s", job.task_id, exc)

    async def _run_source_job(self, job: ProxyJob, headers: dict[str, str], normalized_source: NormalizedSource | None) -> None:
        job.status = "started"
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
            logger.exception("[%s] async source job failed: %s", job.task_id, exc)

    async def _store_local_response(self, job: ProxyJob, response) -> None:
        response.raise_for_status()
        if response.headers.get("content-type", "").startswith("application/json"):
            job.result_payload = response.json()
            job.result_zip = None
        else:
            job.result_payload = None
            job.result_zip = response.content
        job.status = "success"
        logger.info("[%s] local conversion ready: file=%s target=%s", job.task_id, job.filename, job.target_kind)

    async def _store_split_result(self, job: ProxyJob, result: ConvertDocumentResponse, filename: str) -> None:
        if job.target_kind == "zip":
            job.result_zip = build_zip_response_bytes(result, filename)
        job.result_payload = result.model_dump(exclude_none=True)
        job.status = "success"
        logger.info("[%s] merged split result ready: file=%s target=%s", job.task_id, filename, job.target_kind)

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
        if job is not None:
            job.meta.split = True
            job.meta.total_parts = len(parts)
            job.meta.completed_parts = 0
            job.meta.filename = filename
            job.meta.parts = [
                ProxyPartInfo(part_index=index, start_page=start, end_page=end)
                for index, (start, end, _) in enumerate(parts)
            ]

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

            def log_chunk_started(timing) -> None:
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
                    job.meta.parts[index].task_status = "started"

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
                    job.meta.parts[index].error_message = str(exc)
                    job.meta.parts[index].task_status = "failure"
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
            if job is not None:
                job.meta.parts[index].task_status = "success"
                job.meta.completed_parts += 1
            async with progress_lock:
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
