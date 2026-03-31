import asyncio
from io import BytesIO
from pathlib import Path
import time
from zipfile import ZIP_DEFLATED, ZipFile

import logging

import pytest
from pypdf import PdfWriter

from docling_proxy.contracts import ConvertDocumentResponse, ExportDocumentResponse, ProxyOptions, ProxyTaskMeta
from docling_proxy.local_docling import LocalDoclingExecution, LocalDoclingTiming
from docling_proxy.models import FilePayload, ProxyJob
from docling_proxy.pdf_tools import split_pdf
from docling_proxy.state import TaskStateStore
from docling_proxy.service import ProxyService, settings as service_settings
from docling_proxy.store import job_store


def make_pdf(page_count: int) -> bytes:
    writer = PdfWriter()
    for _ in range(page_count):
        writer.add_blank_page(width=300, height=300)
    buffer = BytesIO()
    writer.write(buffer)
    return buffer.getvalue()


def make_zip_bytes(filename: str, content: str = "ok") -> bytes:
    buffer = BytesIO()
    with ZipFile(buffer, "w", compression=ZIP_DEFLATED) as zip_file:
        zip_file.writestr(f"{Path(filename).stem}.md", content)
    return buffer.getvalue()


def make_merged_result(results, requested_formats, page_break_placeholder=None, include_proxy_meta=False, proxy_meta=None):
    names = [result["document"]["json_content"]["name"] for result in results]
    return ConvertDocumentResponse(
        document=ExportDocumentResponse(
            md_content="\n".join(names) if "md" in requested_formats else None,
            json_content={"names": names} if "json" in requested_formats else None,
        ),
        status="success",
        errors=[],
        processing_time=float(len(results)),
        timings={},
        proxy_meta=proxy_meta if include_proxy_meta else None,
    )


class FakeResponse:
    def __init__(self, payload=None, content=b"", headers=None, status_code=200):
        self._payload = payload
        self.content = content
        self.headers = headers or {"content-type": "application/json"}
        self.status_code = status_code

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"status {self.status_code}")


class FakeUpstream:
    def __init__(self):
        self.sync_calls = 0
        self.async_submits = []
        self.status_calls = {}

    async def relay_file_sync(self, endpoint, files, data, headers):
        self.sync_calls += 1
        return FakeResponse(payload={"ok": True})

    async def submit_file_async(self, files, data, headers):
        task_id = f"task-{len(self.async_submits)}"
        self.async_submits.append((files, data))
        self.status_calls[task_id] = 0
        return {"task_id": task_id, "task_status": "pending"}

    async def status(self, task_id, headers):
        self.status_calls[task_id] += 1
        if self.status_calls[task_id] > 1:
            return {"task_id": task_id, "task_status": "success"}
        return {"task_id": task_id, "task_status": "started"}

    async def result(self, task_id, headers):
        index = int(task_id.split("-")[-1]) + 1
        return FakeResponse(
            payload={
                "document": {"json_content": {"name": f"chunk-{index}"}},
                "errors": [],
                "processing_time": 1.0,
            }
        )


class FakeLocalDocling:
    def __init__(self):
        self.calls = []
        self.relay_file_calls = []
        self.relay_source_calls = []

    async def relay_file_sync(self, files, data, headers):
        self.relay_file_calls.append((files, data, headers))
        return FakeResponse(
            payload={
                "document": {"md_content": "ok"},
                "status": "success",
                "errors": [],
                "processing_time": 1.0,
                "timings": {},
            }
        )

    async def relay_source_sync(self, payload, headers):
        self.relay_source_calls.append((payload, headers))
        return FakeResponse(
            payload={
                "document": {"md_content": "ok"},
                "status": "success",
                "errors": [],
                "processing_time": 1.0,
                "timings": {},
            }
        )

    async def convert_file(self, file, data, headers):
        self.calls.append((file, data, headers))
        index = len(self.calls)
        return {
            "document": {"json_content": {"name": f"chunk-{index}"}},
            "errors": [],
            "processing_time": 1.0,
        }

    async def execute_file_sync(self, files, data, headers, on_request_start=None):
        self.calls.append((files[0], data, headers))
        index = len(self.calls)
        timing = LocalDoclingTiming(
            wait_duration_sec=0.1,
            startup_duration_sec=0.2,
            request_duration_sec=0.3,
            processing_duration_sec=0.5,
        )
        if on_request_start is not None:
            maybe_result = on_request_start(timing)
            if maybe_result is not None:
                await maybe_result
        if data.get("target_type") == "zip":
            filename = files[0].filename
            return LocalDoclingExecution(
                response=FakeResponse(
                    content=make_zip_bytes(filename, content=f"zip-{Path(filename).stem}"),
                    headers={"content-type": "application/zip"},
                ),
                timing=timing,
            )
        return LocalDoclingExecution(
            response=FakeResponse(
                payload={
                    "document": {"json_content": {"name": f"chunk-{index}"}},
                    "errors": [],
                    "processing_time": 1.0,
                }
            ),
            timing=timing,
        )


class FakeArchiveStore:
    def __init__(self):
        self.payload_calls = []
        self.zip_calls = []

    async def persist_payload(self, request_id, source_kind, filename, target_kind, payload):
        self.payload_calls.append((request_id, source_kind, filename, target_kind, payload))
        return f"/archive/{request_id}"

    async def persist_zip(self, request_id, source_kind, filename, target_kind, zip_bytes, *, status="success", errors=None, processing_time=None):
        self.zip_calls.append((request_id, source_kind, filename, target_kind, zip_bytes, status, errors, processing_time))
        return f"/archive/{request_id}"


class ConcurrencyTrackingLocalDocling(FakeLocalDocling):
    def __init__(self):
        super().__init__()
        self.active_calls = 0
        self.max_active_calls = 0

    async def execute_file_sync(self, files, data, headers, on_request_start=None):
        self.calls.append((files[0], data, headers))
        self.active_calls += 1
        self.max_active_calls = max(self.max_active_calls, self.active_calls)
        timing = LocalDoclingTiming(
            wait_duration_sec=0.1,
            startup_duration_sec=0.2,
            request_duration_sec=0.3,
            processing_duration_sec=0.5,
        )
        if on_request_start is not None:
            maybe_result = on_request_start(timing)
            if maybe_result is not None:
                await maybe_result
        await asyncio.sleep(0.02)
        self.active_calls -= 1
        return LocalDoclingExecution(
            response=FakeResponse(
                payload={
                    "document": {"json_content": {"name": f"chunk-{len(self.calls)}"}},
                    "errors": [],
                    "processing_time": 1.0,
                }
            ),
            timing=timing,
        )


@pytest.mark.asyncio
async def test_small_pdf_passthrough(monkeypatch):
    upstream = FakeUpstream()
    local_docling = FakeLocalDocling()
    archive_store = FakeArchiveStore()
    service = ProxyService(upstream, local_docling, archive_store)
    file = FilePayload("small.pdf", make_pdf(1), "application/pdf")

    response = await service.process_file_sync([file], {"to_formats": ["md"]}, None, {})
    assert upstream.sync_calls == 0
    assert local_docling.calls == []
    assert len(local_docling.relay_file_calls) == 1
    assert len(archive_store.payload_calls) == 1
    assert response.json()["status"] == "success"


@pytest.mark.asyncio
async def test_split_pdf_merges_chunk_results(monkeypatch, caplog):
    upstream = FakeUpstream()
    local_docling = FakeLocalDocling()
    service = ProxyService(upstream, local_docling, FakeArchiveStore())
    caplog.set_level(logging.INFO)

    monkeypatch.setattr(
        "docling_proxy.service.merge_results",
        lambda results, requested_formats, page_break_placeholder=None, include_proxy_meta=False, proxy_meta=None: {
            "results": results,
            "requested_formats": requested_formats,
        },
    )

    result = await service._process_split_file(
        filename="large.pdf",
        pdf_source=make_pdf(5),
        options={"to_formats": ["md"]},
        proxy_options=ProxyOptions(force_split=True, max_pages_per_part=2, poll_interval_sec=0.001),
        headers={},
        target_kind="inbody",
    )

    assert len(local_docling.calls) == 3
    assert all(call[1]["target_type"] == "inbody" for call in local_docling.calls)
    assert all("json" in call[1]["to_formats"] for call in local_docling.calls)
    assert result["requested_formats"] == ["md"]
    assert len(result["results"]) == 3
    assert "queued chunk 1/3 for file=large.pdf, pages 1-2" in caplog.text
    assert "started chunk 1/3 for file=large.pdf, pages 1-2, wait=0.10s, startup=0.20s" in caplog.text
    assert "finished chunk 1/3 for file=large.pdf, pages 1-2, total=0.50s, per_page=0.25s, wait=0.10s, startup=0.20s, convert=0.30s" in caplog.text


@pytest.mark.asyncio
async def test_split_respects_request_work_concurrency_override(monkeypatch):
    monkeypatch.setattr(service_settings, "work_concurrency", 3)
    upstream = FakeUpstream()
    local_docling = ConcurrencyTrackingLocalDocling()
    service = ProxyService(upstream, local_docling, FakeArchiveStore())
    monkeypatch.setattr("docling_proxy.service.merge_results", make_merged_result)

    result = await service._process_split_file(
        filename="large.pdf",
        pdf_source=make_pdf(3),
        options={"to_formats": ["md"]},
        proxy_options=ProxyOptions(force_split=True, max_pages_per_part=1, work_concurrency=1),
        headers={},
        target_kind="inbody",
    )

    assert local_docling.max_active_calls == 1
    assert result.document.md_content == "chunk-1\nchunk-2\nchunk-3"


@pytest.mark.asyncio
async def test_split_processes_chunks_in_part_order(monkeypatch):
    monkeypatch.setattr(service_settings, "work_concurrency", 3)
    upstream = FakeUpstream()
    local_docling = FakeLocalDocling()
    service = ProxyService(upstream, local_docling, FakeArchiveStore())
    monkeypatch.setattr("docling_proxy.service.merge_results", make_merged_result)

    def delayed_materialize(source, chunk):
        if chunk.part_index == 0:
            time.sleep(0.05)
        return make_pdf(1)

    monkeypatch.setattr("docling_proxy.service.materialize_pdf_chunk", delayed_materialize)

    result = await service._process_split_file(
        filename="large.pdf",
        pdf_source=make_pdf(3),
        options={"to_formats": ["md"]},
        proxy_options=ProxyOptions(force_split=True, max_pages_per_part=1),
        headers={},
        target_kind="inbody",
    )

    assert [call[0].filename.split("_", 1)[0] for call in local_docling.calls] == ["0001", "0002", "0003"]
    assert result.document.md_content == "chunk-1\nchunk-2\nchunk-3"


@pytest.mark.asyncio
async def test_split_materialization_failure_marks_chunk_failure(monkeypatch, tmp_path):
    monkeypatch.setattr(service_settings, "work_concurrency", 1)
    upstream = FakeUpstream()
    local_docling = FakeLocalDocling()
    archive_store = FakeArchiveStore()
    state_store = TaskStateStore(tmp_path / "state")
    service = ProxyService(upstream, local_docling, archive_store, state_store)
    file_bytes = make_pdf(3)
    job = ProxyJob(
        task_id="job-materialize-fail",
        source_kind="file",
        filename="large.pdf",
        target_kind="inbody",
        requested_formats=["md"],
        options={"to_formats": ["md"]},
        proxy_options=ProxyOptions(force_split=True, max_pages_per_part=1, poll_interval_sec=0.001),
        files=[FilePayload("large.pdf", file_bytes, "application/pdf")],
        meta=ProxyTaskMeta(source_kind="file", filename="large.pdf"),
    )
    await service._ensure_operation_state(job, {})

    def fail_materialize(source, chunk):
        if chunk.part_index == 0:
            raise KeyError("/D")
        return make_pdf(1)

    monkeypatch.setattr("docling_proxy.service.materialize_pdf_chunk", fail_materialize)

    with pytest.raises(KeyError):
        await service._process_split_file(
            filename="large.pdf",
            pdf_source=file_bytes,
            options=job.options,
            proxy_options=job.proxy_options,
            headers={},
            target_kind=job.target_kind,
            job=job,
        )

    assert job.meta.parts[0].task_status == "failure"
    assert job.meta.parts[0].error_message == "'/D'"


@pytest.mark.asyncio
async def test_async_small_pdf_uses_local_isolated_processing():
    upstream = FakeUpstream()
    local_docling = FakeLocalDocling()
    archive_store = FakeArchiveStore()
    service = ProxyService(upstream, local_docling, archive_store)
    file = FilePayload("small.pdf", make_pdf(1), "application/pdf")
    job = ProxyJob(
        task_id="job-1",
        source_kind="file",
        filename=file.filename,
        target_kind="inbody",
        requested_formats=["md"],
        options={"to_formats": ["md"]},
        files=[file],
        meta=ProxyTaskMeta(source_kind="file", filename=file.filename),
    )

    await service._run_file_job(job, {})

    assert job.status == "success"
    assert len(local_docling.relay_file_calls) == 1
    assert job.result_payload["status"] == "success"
    assert len(archive_store.payload_calls) == 1


@pytest.mark.asyncio
async def test_ensure_operation_state_adopts_temp_file_into_task_state(tmp_path):
    upstream = FakeUpstream()
    archive_store = FakeArchiveStore()
    state_store = TaskStateStore(tmp_path / "state")
    service = ProxyService(upstream, FakeLocalDocling(), archive_store, state_store)
    upload_path = tmp_path / "incoming.pdf"
    upload_path.write_bytes(make_pdf(1))
    file = FilePayload("incoming.pdf", None, "application/pdf", temp_path=upload_path)
    job = ProxyJob(
        task_id="job-adopt",
        source_kind="file",
        filename=file.filename,
        target_kind="inbody",
        requested_formats=["md"],
        options={"to_formats": ["md"]},
        files=[file],
        meta=ProxyTaskMeta(source_kind="file", filename=file.filename),
    )

    await service._ensure_operation_state(job, {})

    assert upload_path.exists() is False
    assert file.temp_path is not None
    assert file.temp_path.exists()
    assert file.temp_path.parent.name == "inputs"
    assert file.cleanup_enabled is False


@pytest.mark.asyncio
async def test_split_temp_backed_upload_avoids_full_read(tmp_path, monkeypatch):
    upstream = FakeUpstream()
    local_docling = FakeLocalDocling()
    archive_store = FakeArchiveStore()
    service = ProxyService(upstream, local_docling, archive_store)
    pdf_path = tmp_path / "large.pdf"
    pdf_path.write_bytes(make_pdf(3))
    file = FilePayload("large.pdf", None, "application/pdf", temp_path=pdf_path)

    async def fail_read_content(self):
        raise AssertionError("read_content should not be used for temp-backed split uploads")

    monkeypatch.setattr(FilePayload, "read_content", fail_read_content)
    response = await service.process_file_sync([file], {"to_formats": ["md"], "target_type": "inbody"}, ProxyOptions(max_pages_per_part=1), {})

    assert len(local_docling.calls) == 3
    assert response.status == "success"


@pytest.mark.asyncio
async def test_split_job_persists_chunk_payloads_and_final_result_state(tmp_path, monkeypatch):
    upstream = FakeUpstream()
    local_docling = FakeLocalDocling()
    archive_store = FakeArchiveStore()
    state_store = TaskStateStore(tmp_path)
    service = ProxyService(upstream, local_docling, archive_store, state_store)
    monkeypatch.setattr("docling_proxy.service.merge_results", make_merged_result)

    file = FilePayload("large.pdf", make_pdf(5), "application/pdf")
    job = ProxyJob(
        task_id="job-split-state",
        source_kind="file",
        filename=file.filename,
        target_kind="inbody",
        requested_formats=["md"],
        options={"to_formats": ["md"]},
        proxy_options=ProxyOptions(force_split=True, max_pages_per_part=2, poll_interval_sec=0.001),
        files=[file],
        meta=ProxyTaskMeta(source_kind="file", filename=file.filename),
    )

    await service._ensure_operation_state(job, {})
    await service._run_file_job(job, {})

    assert job.status == "success"
    assert job.result_payload is not None
    assert job.result_payload_path is not None
    assert len(archive_store.payload_calls) == 1
    for part_index in range(3):
        assert state_store.part_payload_path(job.task_id, part_index).exists()

    await job_store.delete(job.task_id)
    recovered_service = ProxyService(FakeUpstream(), FakeLocalDocling(), archive_store, state_store)
    recovered_job = await recovered_service.get_result(job.task_id)

    assert recovered_job.status == "success"
    assert recovered_job.result_payload is not None
    assert recovered_job.result_payload["document"]["md_content"] == "chunk-1\nchunk-2\nchunk-3"


@pytest.mark.asyncio
async def test_split_recovery_skips_already_persisted_chunks(tmp_path, monkeypatch):
    upstream = FakeUpstream()
    local_docling = FakeLocalDocling()
    archive_store = FakeArchiveStore()
    state_store = TaskStateStore(tmp_path)
    service = ProxyService(upstream, local_docling, archive_store, state_store)
    monkeypatch.setattr("docling_proxy.service.merge_results", make_merged_result)

    file_bytes = make_pdf(5)
    file = FilePayload("large.pdf", file_bytes, "application/pdf")
    job = ProxyJob(
        task_id="job-split-resume",
        source_kind="file",
        filename=file.filename,
        target_kind="inbody",
        requested_formats=["md"],
        options={"to_formats": ["md"]},
        proxy_options=ProxyOptions(force_split=True, max_pages_per_part=2, poll_interval_sec=0.001),
        files=[file],
        meta=ProxyTaskMeta(source_kind="file", filename=file.filename),
    )

    await service._ensure_operation_state(job, {})
    split_parts = split_pdf(file_bytes, job.proxy_options)
    job.meta = await state_store.ensure_split_plan(
        job.task_id,
        source_kind="file",
        filename=file.filename,
        parts=[(start, end) for start, end, _ in split_parts],
    )
    await state_store.persist_part_payload(
        job.task_id,
        0,
        {
            "document": {"json_content": {"name": "chunk-1"}},
            "errors": [],
            "processing_time": 1.0,
        },
    )
    job.meta.parts[0].task_status = "success"
    job.meta.completed_parts = 1
    job.status = "started"
    await state_store.sync_job(job)

    recovered_local_docling = FakeLocalDocling()
    recovered_service = ProxyService(FakeUpstream(), recovered_local_docling, archive_store, state_store)
    recovered_job = await state_store.load_job(job.task_id, include_private=True)

    assert recovered_job is not None
    await recovered_service._run_file_job(recovered_job, {})

    assert recovered_job.status == "success"
    assert len(recovered_local_docling.calls) == 2
    assert recovered_job.result_payload is not None
    assert recovered_job.result_payload["document"]["md_content"] == "chunk-1\nchunk-1\nchunk-2"
    for part_index in range(3):
        assert state_store.part_payload_path(job.task_id, part_index).exists()


@pytest.mark.asyncio
async def test_sync_multi_file_request_builds_merged_zip_and_uses_per_file_isolation():
    upstream = FakeUpstream()
    local_docling = FakeLocalDocling()
    archive_store = FakeArchiveStore()
    service = ProxyService(upstream, local_docling, archive_store)
    files = [
        FilePayload("alpha.pdf", b"alpha", "application/pdf"),
        FilePayload("beta.pdf", b"beta", "application/pdf"),
    ]

    response = await service.process_file_sync(files, {"to_formats": ["md"]}, None, {})

    assert len(local_docling.calls) == 2
    assert len(local_docling.relay_file_calls) == 0
    assert len(archive_store.zip_calls) == 1
    assert archive_store.zip_calls[0][5] == "success"
    with ZipFile(BytesIO(response.body)) as zip_file:
        assert sorted(zip_file.namelist()) == ["alpha/alpha.md", "beta/beta.md"]


@pytest.mark.asyncio
async def test_async_multi_file_job_returns_zip_even_without_explicit_zip_target():
    upstream = FakeUpstream()
    local_docling = FakeLocalDocling()
    archive_store = FakeArchiveStore()
    service = ProxyService(upstream, local_docling, archive_store)
    files = [
        FilePayload("alpha.pdf", b"alpha", "application/pdf"),
        FilePayload("beta.pdf", b"beta", "application/pdf"),
    ]
    job = ProxyJob(
        task_id="job-batch",
        source_kind="file",
        filename="batch-2-files",
        target_kind="zip",
        requested_formats=["md"],
        options={"to_formats": ["md"]},
        files=files,
        meta=ProxyTaskMeta(source_kind="file", filename="batch-2-files"),
    )

    await service._run_file_job(job, {})

    assert job.status == "success"
    assert job.result_zip is not None
    assert job.result_payload["status"] == "success"
    assert len(local_docling.calls) == 2
    assert len(archive_store.zip_calls) == 1
    with ZipFile(BytesIO(job.result_zip)) as zip_file:
        assert sorted(zip_file.namelist()) == ["alpha/alpha.md", "beta/beta.md"]
