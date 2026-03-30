import asyncio
import threading
from collections import deque
from types import SimpleNamespace
from pathlib import Path

import pytest

from docling_proxy.local_docling import LocalDoclingManager, settings as local_docling_settings
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


@pytest.mark.asyncio
async def test_warm_pool_prewarms_and_launches_replacement_before_request_finishes(monkeypatch):
    monkeypatch.setattr(local_docling_settings, "local_docling_pool_size", 1)
    monkeypatch.setattr(local_docling_settings, "local_docling_pool_retry_delay_sec", 0.01)
    monkeypatch.setattr(local_docling_settings, "local_docling_pool_idle_timeout_sec", 300.0)

    manager = LocalDoclingManager()
    start_calls = 0
    request_started = threading.Event()
    allow_finish = threading.Event()

    async def fake_start_session():
        nonlocal start_calls
        start_calls += 1
        return SimpleNamespace(
            base_url=f"http://127.0.0.1:{5000 + start_calls}",
            port=5000 + start_calls,
            process=SimpleNamespace(pid=start_calls, returncode=None),
            scratch_dir=Path(f"/tmp/fake-{start_calls}"),
            log_prefix=f"fake-{start_calls}",
            stderr_lines=deque(),
            stream_tasks=(),
        )

    async def fake_stop_session(session):
        return None

    def fake_post(*args, **kwargs):
        request_started.set()
        allow_finish.wait(timeout=1)
        return SimpleNamespace(status_code=200, headers={"content-type": "application/json"}, json=lambda: {})

    monkeypatch.setattr(manager, "_start_session", fake_start_session)
    monkeypatch.setattr(manager, "_stop_session", fake_stop_session)
    monkeypatch.setattr(manager._request_client, "post", fake_post)

    await manager.start()
    assert start_calls == 1

    file_payload = FilePayload("sample.pdf", b"pdf", "application/pdf")
    request_task = asyncio.create_task(manager.relay_file_sync([file_payload], {}, {}))

    while not request_started.is_set():
        await asyncio.sleep(0.01)
    await asyncio.sleep(0.05)

    assert start_calls == 2
    assert len(manager._ready_sessions) == 1

    allow_finish.set()
    await request_task
    await manager.close()


@pytest.mark.asyncio
async def test_warm_pool_drains_ready_instances_after_idle_timeout(monkeypatch):
    monkeypatch.setattr(local_docling_settings, "local_docling_pool_size", 1)
    monkeypatch.setattr(local_docling_settings, "local_docling_pool_retry_delay_sec", 0.01)
    monkeypatch.setattr(local_docling_settings, "local_docling_pool_idle_timeout_sec", 0.05)
    monkeypatch.setattr(local_docling_settings, "local_docling_pool_reap_interval_sec", 0.01)

    manager = LocalDoclingManager()
    start_calls = 0
    stop_calls: list[int] = []
    request_started = threading.Event()
    allow_finish = threading.Event()

    async def fake_start_session():
        nonlocal start_calls
        start_calls += 1
        return SimpleNamespace(
            base_url=f"http://127.0.0.1:{6000 + start_calls}",
            port=6000 + start_calls,
            process=SimpleNamespace(pid=start_calls, returncode=None),
            scratch_dir=Path(f"/tmp/fake-drain-{start_calls}"),
            log_prefix=f"fake-drain-{start_calls}",
            stderr_lines=deque(),
            stream_tasks=(),
        )

    async def fake_stop_session(session):
        stop_calls.append(session.process.pid)

    def fake_post(*args, **kwargs):
        request_started.set()
        allow_finish.wait(timeout=1)
        return SimpleNamespace(status_code=200, headers={"content-type": "application/json"}, json=lambda: {})

    monkeypatch.setattr(manager, "_start_session", fake_start_session)
    monkeypatch.setattr(manager, "_stop_session", fake_stop_session)
    monkeypatch.setattr(manager._request_client, "post", fake_post)

    await manager.start()
    try:
        file_payload = FilePayload("sample.pdf", b"pdf", "application/pdf")
        request_task = asyncio.create_task(manager.relay_file_sync([file_payload], {}, {}))

        while not request_started.is_set():
            await asyncio.sleep(0.01)
        for _ in range(50):
            if start_calls >= 2 and len(manager._ready_sessions) >= 1:
                break
            await asyncio.sleep(0.01)

        allow_finish.set()
        await request_task

        for _ in range(50):
            if manager._pool_suspended and not manager._ready_sessions and any(pid != 1 for pid in stop_calls):
                break
            await asyncio.sleep(0.01)

        assert start_calls >= 2
        assert manager._pool_suspended is True
        assert len(manager._ready_sessions) == 0
        assert stop_calls.count(1) == 1
        assert any(pid != 1 for pid in stop_calls)
    finally:
        await manager.close()
