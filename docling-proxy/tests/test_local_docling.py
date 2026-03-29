import asyncio
from types import SimpleNamespace
from pathlib import Path

import pytest

from docling_proxy.local_docling import LocalDoclingManager
from docling_proxy.models import FilePayload


@pytest.mark.asyncio
async def test_child_env_inherits_docling_settings_and_uses_unique_scratch(monkeypatch, tmp_path):
    monkeypatch.setenv("DOCLING_SERVE_LOG_LEVEL", "INFO")
    monkeypatch.setenv("DOCLING_SERVE_ENABLE_UI", "true")
    base_dir = tmp_path / "scratch-base"
    monkeypatch.setenv("DOCLING_SERVE_SCRATCH_PATH", str(base_dir))

    manager = LocalDoclingManager()
    scratch_dir = manager._create_child_scratch_dir()
    try:
        env = manager._environment(scratch_dir)
        child_scratch = Path(env["DOCLING_SERVE_SCRATCH_PATH"])

        assert env["DOCLING_SERVE_LOG_LEVEL"] == "INFO"
        assert env["DOCLING_SERVE_ENABLE_UI"] == "true"
        assert child_scratch.parent == base_dir
        assert child_scratch != base_dir
        assert child_scratch.exists()
    finally:
        await manager.close()


@pytest.mark.asyncio
async def test_global_session_semaphore_limits_concurrent_child_sessions(monkeypatch):
    manager = LocalDoclingManager()
    active_sessions = 0
    max_active_sessions = 0
    release_first = asyncio.Event()
    first_started = asyncio.Event()

    async def fake_start_session():
        nonlocal active_sessions, max_active_sessions
        active_sessions += 1
        max_active_sessions = max(max_active_sessions, active_sessions)
        if active_sessions == 1:
            first_started.set()
        return SimpleNamespace(base_url="http://127.0.0.1:9", process=SimpleNamespace(pid=active_sessions))

    async def fake_stop_session(session):
        nonlocal active_sessions
        if active_sessions == 1 and not release_first.is_set():
            await release_first.wait()
        active_sessions -= 1

    def fake_post(*args, **kwargs):
        return SimpleNamespace(status_code=200)

    monkeypatch.setattr(manager, "_start_session", fake_start_session)
    monkeypatch.setattr(manager, "_stop_session", fake_stop_session)
    monkeypatch.setattr(manager._request_client, "post", fake_post)

    file_payload = FilePayload("sample.pdf", b"pdf", "application/pdf")
    first_task = asyncio.create_task(manager.relay_file_sync([file_payload], {}, {}))
    await first_started.wait()
    second_task = asyncio.create_task(manager.relay_file_sync([file_payload], {}, {}))
    await asyncio.sleep(0.05)
    assert max_active_sessions == 1

    release_first.set()
    await asyncio.gather(first_task, second_task)
    assert max_active_sessions == 1
    await manager.close()
