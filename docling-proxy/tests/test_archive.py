import json
from pathlib import Path

import pytest

from docling_proxy.archive import ArchiveStore


@pytest.mark.asyncio
async def test_archive_store_persists_payload_outputs(tmp_path):
    store = ArchiveStore(tmp_path)

    archive_dir = await store.persist_payload(
        request_id="req-1",
        source_kind="file",
        filename="report.pdf",
        target_kind="inbody",
        payload={
            "status": "success",
            "processing_time": 1.25,
            "errors": [],
            "document": {
                "md_content": "hello",
                "json_content": {"x": 1},
            },
        },
    )

    assert archive_dir is not None
    assert (archive_dir / "report.md").read_text(encoding="utf-8") == "hello"
    assert json.loads((archive_dir / "report.json").read_text(encoding="utf-8")) == {"x": 1}
    meta = json.loads((archive_dir / "meta.json").read_text(encoding="utf-8"))
    assert meta["request_id"] == "req-1"
    assert meta["artifacts"] == ["report.md", "report.json"]


@pytest.mark.asyncio
async def test_archive_store_persists_zip_outputs(tmp_path):
    store = ArchiveStore(tmp_path)

    archive_dir = await store.persist_zip(
        request_id="req-2",
        source_kind="file",
        filename="bundle.pdf",
        target_kind="zip",
        zip_bytes=b"zip-data",
        status="success",
        errors=[],
        processing_time=2.5,
    )

    assert archive_dir is not None
    assert (archive_dir / "bundle.zip").read_bytes() == b"zip-data"
    meta = json.loads((archive_dir / "meta.json").read_text(encoding="utf-8"))
    assert meta["target_kind"] == "zip"
    assert meta["artifacts"] == ["bundle.zip"]


@pytest.mark.asyncio
async def test_archive_store_skips_failed_payloads(tmp_path):
    store = ArchiveStore(tmp_path)

    archive_dir = await store.persist_payload(
        request_id="req-3",
        source_kind="file",
        filename="failed.pdf",
        target_kind="inbody",
        payload={"status": "failure", "document": {}},
    )

    assert archive_dir is None
    assert list(Path(tmp_path).rglob("*")) == []
