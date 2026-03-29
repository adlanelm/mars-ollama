from __future__ import annotations

import asyncio
import logging
import os
import shlex
import shutil
import signal
import socket
import sys
import tempfile
import time
from collections.abc import Awaitable, Callable
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from docling_proxy.config import settings
from docling_proxy.models import FilePayload
from docling_proxy.upstream import form_data, forwarded_headers


@dataclass(slots=True)
class LocalDoclingSession:
    process: asyncio.subprocess.Process
    port: int
    base_url: str
    scratch_dir: Path
    log_prefix: str
    stderr_lines: deque[str]
    stream_tasks: tuple[asyncio.Task[None], ...]


@dataclass(slots=True)
class LocalDoclingTiming:
    wait_duration_sec: float
    startup_duration_sec: float
    request_duration_sec: float
    processing_duration_sec: float


@dataclass(slots=True)
class LocalDoclingExecution:
    response: httpx.Response
    timing: LocalDoclingTiming


logger = logging.getLogger(__name__)


class LocalDoclingManager:
    def __init__(self) -> None:
        self._active_sessions: dict[int, LocalDoclingSession] = {}
        self._lock = asyncio.Lock()
        self._session_semaphore = asyncio.Semaphore(settings.max_concurrency)
        self._ready_client = httpx.AsyncClient(timeout=5.0)
        self._request_client = httpx.Client(timeout=settings.local_docling_request_timeout_sec)
        self._scratch_root = settings.temp_dir / "child-docling"
        self._scratch_root.mkdir(parents=True, exist_ok=True)

    async def close(self) -> None:
        async with self._lock:
            sessions = list(self._active_sessions.values())
            self._active_sessions.clear()
        await asyncio.gather(*(self._terminate_session(session) for session in sessions), return_exceptions=True)
        await self._ready_client.aclose()
        await asyncio.to_thread(self._request_client.close)

    async def relay_file_sync(
        self,
        files: list[FilePayload],
        data: dict[str, Any],
        headers: dict[str, str],
        on_request_start: Callable[[LocalDoclingTiming], Awaitable[None] | None] | None = None,
    ) -> httpx.Response:
        execution = await self.execute_file_sync(files, data, headers, on_request_start=on_request_start)
        return execution.response

    async def execute_file_sync(
        self,
        files: list[FilePayload],
        data: dict[str, Any],
        headers: dict[str, str],
        on_request_start: Callable[[LocalDoclingTiming], Awaitable[None] | None] | None = None,
    ) -> LocalDoclingExecution:
        queued_at = time.perf_counter()
        async with self._session_semaphore:
            acquired_at = time.perf_counter()
            wait_duration_sec = acquired_at - queued_at
            startup_started_at = time.perf_counter()
            session = await self._start_session()
            try:
                startup_duration_sec = time.perf_counter() - startup_started_at
                request_started_at = time.perf_counter()
                processing_duration_sec = request_started_at - acquired_at
                timing = LocalDoclingTiming(
                    wait_duration_sec=wait_duration_sec,
                    startup_duration_sec=startup_duration_sec,
                    request_duration_sec=0.0,
                    processing_duration_sec=processing_duration_sec,
                )
                if on_request_start is not None:
                    maybe_result = on_request_start(timing)
                    if maybe_result is not None:
                        await maybe_result
                response = await asyncio.to_thread(
                    self._request_client.post,
                    f"{session.base_url}/v1/convert/file",
                    files=[("files", (file.filename, file.content, file.content_type)) for file in files],
                    data=form_data(data),
                    headers=forwarded_headers(headers),
                )
                request_duration_sec = time.perf_counter() - request_started_at
                return LocalDoclingExecution(
                    response=response,
                    timing=LocalDoclingTiming(
                        wait_duration_sec=wait_duration_sec,
                        startup_duration_sec=startup_duration_sec,
                        request_duration_sec=request_duration_sec,
                        processing_duration_sec=processing_duration_sec + request_duration_sec,
                    ),
                )
            finally:
                await self._stop_session(session)

    async def relay_source_sync(
        self,
        payload: dict[str, Any],
        headers: dict[str, str],
    ) -> httpx.Response:
        async with self._session_semaphore:
            session = await self._start_session()
            try:
                return await asyncio.to_thread(
                    self._request_client.post,
                    f"{session.base_url}/v1/convert/source",
                    json=payload,
                    headers=forwarded_headers(headers),
                )
            finally:
                await self._stop_session(session)

    async def convert_file(self, file: FilePayload, data: dict[str, Any], headers: dict[str, str]) -> dict[str, Any]:
        response = await self.relay_file_sync([file], data, headers)
        response.raise_for_status()
        content_type = response.headers.get("content-type", "")
        if not content_type.startswith("application/json"):
            raise RuntimeError(
                "Local docling-serve returned an unsupported response while processing a split chunk."
            )
        return response.json()

    async def _start_session(self) -> LocalDoclingSession:
        port = self._reserve_port()
        scratch_dir = self._create_child_scratch_dir()
        command = self._command(port)
        env = self._environment(scratch_dir)
        process = await asyncio.create_subprocess_exec(
            *command,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        log_prefix = f"docling-serve pid={process.pid} port={port}"
        stderr_lines: deque[str] = deque(maxlen=50)
        stream_tasks = (
            asyncio.create_task(self._capture_stream(process.stdout, log_prefix, "stdout")),
            asyncio.create_task(self._capture_stream(process.stderr, log_prefix, "stderr", stderr_lines)),
        )
        session = LocalDoclingSession(
            process=process,
            port=port,
            base_url=f"http://127.0.0.1:{port}",
            scratch_dir=scratch_dir,
            log_prefix=log_prefix,
            stderr_lines=stderr_lines,
            stream_tasks=stream_tasks,
        )
        async with self._lock:
            self._active_sessions[process.pid] = session
        try:
            await self._wait_until_ready(session)
        except Exception:
            await self._stop_session(session)
            raise
        return session

    async def _stop_session(self, session: LocalDoclingSession) -> None:
        async with self._lock:
            self._active_sessions.pop(session.process.pid, None)
        await self._terminate_session(session)

    async def _terminate_session(self, session: LocalDoclingSession) -> None:
        try:
            process = session.process
            if process.returncode is None:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    await process.wait()
                    return
                try:
                    await asyncio.wait_for(process.wait(), timeout=settings.local_docling_shutdown_timeout_sec)
                except asyncio.TimeoutError:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    await process.wait()
        finally:
            await self._wait_for_streams(session)
            shutil.rmtree(session.scratch_dir, ignore_errors=True)

    async def _wait_until_ready(self, session: LocalDoclingSession) -> None:
        deadline = time.monotonic() + settings.local_docling_startup_timeout_sec
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            if session.process.returncode is not None:
                await self._wait_for_streams(session)
                raise RuntimeError(
                    self._startup_error_message(
                        session,
                        f"Local docling-serve exited before becoming ready (exit code {session.process.returncode}).",
                    )
                )
            try:
                response = await self._ready_client.get(f"{session.base_url}/ready")
                if response.status_code == 200:
                    return
                last_error = RuntimeError(f"readiness returned {response.status_code}")
            except httpx.HTTPError as exc:
                last_error = exc
            await asyncio.sleep(settings.local_docling_ready_poll_interval_sec)
        detail = "Timed out waiting for local docling-serve readiness."
        if last_error is not None:
            detail = f"{detail} last error: {last_error}"
        raise RuntimeError(self._startup_error_message(session, detail))

    def _command(self, port: int) -> list[str]:
        if settings.local_docling_command:
            command = shlex.split(settings.local_docling_command)
        else:
            command = [sys.executable, "-m", "docling_serve", "run"]
        return [
            *command,
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--workers",
            str(settings.local_docling_workers),
        ]

    def _environment(self, scratch_dir: Path) -> dict[str, str]:
        env = dict(os.environ)
        env["DOCLING_SERVE_SCRATCH_PATH"] = str(scratch_dir)
        return env

    def _create_child_scratch_dir(self) -> Path:
        base_dir = self._scratch_base_dir()
        base_dir.mkdir(parents=True, exist_ok=True)
        return Path(tempfile.mkdtemp(prefix="docling-serve-", dir=base_dir))

    def _scratch_base_dir(self) -> Path:
        configured = os.environ.get("DOCLING_SERVE_SCRATCH_PATH")
        if configured and configured.strip():
            return Path(configured).expanduser()
        return self._scratch_root

    def _reserve_port(self) -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            return int(sock.getsockname()[1])

    async def _capture_stream(
        self,
        stream: asyncio.StreamReader | None,
        log_prefix: str,
        stream_name: str,
        stderr_lines: deque[str] | None = None,
    ) -> None:
        if stream is None:
            return
        while True:
            line = await stream.readline()
            if not line:
                return
            text = line.decode(errors="replace").rstrip()
            if text:
                if stderr_lines is not None:
                    stderr_lines.append(text)
                logger.info("[%s %s] %s", log_prefix, stream_name, text)

    async def _wait_for_streams(self, session: LocalDoclingSession) -> None:
        try:
            await asyncio.wait_for(asyncio.shield(asyncio.gather(*session.stream_tasks, return_exceptions=True)), timeout=1.0)
        except asyncio.TimeoutError:
            for task in session.stream_tasks:
                task.cancel()
            await asyncio.gather(*session.stream_tasks, return_exceptions=True)

    def _startup_error_message(self, session: LocalDoclingSession, message: str) -> str:
        stderr_tail = self._stderr_tail(session)
        if not stderr_tail:
            return message
        return f"{message} stderr tail:\n{stderr_tail}"

    def _stderr_tail(self, session: LocalDoclingSession) -> str:
        if not session.stderr_lines:
            return ""
        return "\n".join(session.stderr_lines)[-4000:]
