from io import BytesIO

import logging

import pytest
from pypdf import PdfWriter

from docling_proxy.contracts import ProxyOptions, ProxyTaskMeta
from docling_proxy.local_docling import LocalDoclingExecution, LocalDoclingTiming
from docling_proxy.models import FilePayload, ProxyJob
from docling_proxy.service import ProxyService


def make_pdf(page_count: int) -> bytes:
    writer = PdfWriter()
    for _ in range(page_count):
        writer.add_blank_page(width=300, height=300)
    buffer = BytesIO()
    writer.write(buffer)
    return buffer.getvalue()


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


@pytest.mark.asyncio
async def test_small_pdf_passthrough(monkeypatch):
    upstream = FakeUpstream()
    local_docling = FakeLocalDocling()
    service = ProxyService(upstream, local_docling)
    file = FilePayload("small.pdf", make_pdf(1), "application/pdf")

    response = await service.process_file_sync([file], {"to_formats": ["md"]}, None, {})
    assert upstream.sync_calls == 0
    assert local_docling.calls == []
    assert len(local_docling.relay_file_calls) == 1
    assert response.json()["status"] == "success"


@pytest.mark.asyncio
async def test_split_pdf_merges_chunk_results(monkeypatch, caplog):
    upstream = FakeUpstream()
    local_docling = FakeLocalDocling()
    service = ProxyService(upstream, local_docling)
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
        data=make_pdf(5),
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
async def test_async_small_pdf_uses_local_isolated_processing():
    upstream = FakeUpstream()
    local_docling = FakeLocalDocling()
    service = ProxyService(upstream, local_docling)
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
