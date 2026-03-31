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
        self._ready_condition = asyncio.Condition(self._lock)
        self._ready_sessions: deque[LocalDoclingSession] = deque()
        self._warm_tasks: set[asyncio.Task[None]] = set()
        self._idle_reaper_task: asyncio.Task[None] | None = None
        self._active_child_semaphore = asyncio.Semaphore(settings.active_child_limit)
        self._launch_semaphore = asyncio.Semaphore(settings.child_launch_concurrency)
        self._ready_client = httpx.AsyncClient(timeout=5.0)
        self._request_client = httpx.Client(timeout=settings.child_request_timeout_sec)
        self._scratch_root = settings.temp_dir / "child-docling"
        self._scratch_root.mkdir(parents=True, exist_ok=True)
        self._started = False
        self._closed = False
        self._pool_suspended = False
        self._active_leases = 0
        self._last_pool_idle_at: float | None = None

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        if settings.warm_child_pool_size <= 0:
            return
        logger.info("prewarming %s local docling-serve instance(s)", settings.warm_child_pool_size)
        await self._ensure_pool_capacity()
        await self._wait_for_ready_sessions(settings.warm_child_pool_size)
        if settings.warm_child_idle_timeout_sec > 0:
            self._idle_reaper_task = asyncio.create_task(self._run_idle_reaper())
        logger.info("local docling-serve warm pool ready: idle=%s", settings.warm_child_pool_size)

    async def close(self) -> None:
        async with self._ready_condition:
            self._closed = True
            sessions = list(self._active_sessions.values())
            warm_tasks = list(self._warm_tasks)
            idle_reaper_task = self._idle_reaper_task
            self._active_sessions.clear()
            self._ready_sessions.clear()
            self._ready_condition.notify_all()
        if idle_reaper_task is not None:
            idle_reaper_task.cancel()
        await asyncio.gather(*(self._terminate_session(session) for session in sessions), return_exceptions=True)
        await asyncio.gather(*warm_tasks, return_exceptions=True)
        if idle_reaper_task is not None:
            await asyncio.gather(idle_reaper_task, return_exceptions=True)
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
        return await self._execute_request(
            lambda session: self._post_file_request(session, files, data, headers),
            on_request_start=on_request_start,
        )

    async def execute_source_sync(
        self,
        payload: dict[str, Any],
        headers: dict[str, str],
    ) -> LocalDoclingExecution:
        return await self._execute_request(
            lambda session: self._request_client.post(
                f"{session.base_url}/v1/convert/source",
                json=payload,
                headers=forwarded_headers(headers),
            )
        )

    async def _execute_request(
        self,
        request: Callable[[LocalDoclingSession], httpx.Response],
        on_request_start: Callable[[LocalDoclingTiming], Awaitable[None] | None] | None = None,
    ) -> LocalDoclingExecution:
        queued_at = time.perf_counter()
        async with self._active_child_semaphore:
            acquired_at = time.perf_counter()
            wait_duration_sec = acquired_at - queued_at
            startup_started_at = time.perf_counter()
            session = await self._acquire_session()
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
                response = await asyncio.to_thread(request, session)
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
                await self._release_session(session)

    async def relay_source_sync(
        self,
        payload: dict[str, Any],
        headers: dict[str, str],
    ) -> httpx.Response:
        execution = await self.execute_source_sync(payload, headers)
        return execution.response

    async def convert_file(self, file: FilePayload, data: dict[str, Any], headers: dict[str, str]) -> dict[str, Any]:
        response = await self.relay_file_sync([file], data, headers)
        response.raise_for_status()
        content_type = response.headers.get("content-type", "")
        if not content_type.startswith("application/json"):
            raise RuntimeError(
                "Local docling-serve returned an unsupported response while processing a split chunk."
            )
        return response.json()

    async def _acquire_session(self) -> LocalDoclingSession:
        if settings.warm_child_pool_size <= 0:
            return await self._start_session()
        async with self._ready_condition:
            if self._pool_suspended:
                self._pool_suspended = False
                self._last_pool_idle_at = None
                logger.info("request arrived after warm pool idle drain; resuming prewarm")
        await self._ensure_pool_capacity()
        while True:
            async with self._ready_condition:
                while not self._ready_sessions:
                    if self._closed:
                        raise RuntimeError("Local docling warm pool is shutting down.")
                    await self._ready_condition.wait()
                session = self._ready_sessions.popleft()
            if session.process.returncode is None:
                async with self._ready_condition:
                    self._active_leases += 1
                    self._last_pool_idle_at = None
                await self._ensure_pool_capacity()
                return session
            logger.warning(
                "discarding exited warm local docling session pid=%s port=%s returncode=%s",
                session.process.pid,
                session.port,
                session.process.returncode,
            )
            await self._stop_session(session)
            await self._ensure_pool_capacity()

    async def _release_session(self, session: LocalDoclingSession) -> None:
        should_track_idle = settings.warm_child_pool_size > 0
        try:
            await self._stop_session(session)
        finally:
            if should_track_idle:
                async with self._ready_condition:
                    if self._active_leases > 0:
                        self._active_leases -= 1
                    if self._active_leases == 0 and not self._closed and settings.warm_child_idle_timeout_sec > 0:
                        self._last_pool_idle_at = time.monotonic()
                        logger.info(
                            "warm pool idle timer started: timeout=%.2fs idle=%s/%s warming=%s",
                            settings.warm_child_idle_timeout_sec,
                            len(self._ready_sessions),
                            settings.warm_child_pool_size,
                            len(self._warm_tasks),
                        )
                        self._ready_condition.notify_all()

    async def _ensure_pool_capacity(self) -> None:
        if settings.warm_child_pool_size <= 0:
            return
        tasks_to_create = 0
        async with self._ready_condition:
            if self._closed or self._pool_suspended:
                return
            tasks_to_create = max(0, settings.warm_child_pool_size - len(self._ready_sessions) - len(self._warm_tasks))
            for _ in range(tasks_to_create):
                task = asyncio.create_task(self._warm_session())
                self._warm_tasks.add(task)
        if tasks_to_create:
            logger.info(
                "scheduling %s warm docling-serve instance(s); idle_target=%s",
                tasks_to_create,
                settings.warm_child_pool_size,
            )

    async def _wait_for_ready_sessions(self, target: int) -> None:
        async with self._ready_condition:
            while len(self._ready_sessions) < target:
                if self._closed:
                    raise RuntimeError("Local docling warm pool closed before becoming ready.")
                await self._ready_condition.wait()

    async def _warm_session(self) -> None:
        try:
            while True:
                async with self._ready_condition:
                    if self._closed:
                        return
                try:
                    session = await self._start_session()
                except Exception as exc:
                    async with self._ready_condition:
                        if self._closed:
                            return
                    logger.error("failed to prewarm local docling-serve instance: %s", exc)
                    await asyncio.sleep(settings.warm_child_pool_retry_delay_sec)
                    continue
                async with self._ready_condition:
                    if self._closed or self._pool_suspended:
                        discard = True
                    else:
                        self._ready_sessions.append(session)
                        ready_count = len(self._ready_sessions)
                        self._ready_condition.notify_all()
                        discard = False
                if discard:
                    await self._stop_session(session)
                else:
                    logger.info(
                        "warm local docling-serve instance ready: pid=%s port=%s idle=%s/%s",
                        session.process.pid,
                        session.port,
                        ready_count,
                        settings.warm_child_pool_size,
                    )
                return
        finally:
            current_task = asyncio.current_task()
            async with self._ready_condition:
                if current_task is not None:
                    self._warm_tasks.discard(current_task)
                self._ready_condition.notify_all()

    async def _run_idle_reaper(self) -> None:
        try:
            while True:
                await asyncio.sleep(settings.warm_child_reap_interval_sec)
                sessions_to_drain: list[LocalDoclingSession] = []
                idle_for_sec: float | None = None
                warming = 0
                async with self._ready_condition:
                    if self._closed:
                        return
                    if self._active_leases != 0 or self._last_pool_idle_at is None or self._pool_suspended:
                        continue
                    idle_for_sec = time.monotonic() - self._last_pool_idle_at
                    if idle_for_sec < settings.warm_child_idle_timeout_sec:
                        continue
                    self._pool_suspended = True
                    sessions_to_drain = list(self._ready_sessions)
                    self._ready_sessions.clear()
                    warming = len(self._warm_tasks)
                    self._ready_condition.notify_all()
                logger.info(
                    "warm pool idle timeout reached after %.2fs; draining %s ready instance(s) with %s warming",
                    idle_for_sec or 0.0,
                    len(sessions_to_drain),
                    warming,
                )
                await asyncio.gather(*(self._stop_session(session) for session in sessions_to_drain), return_exceptions=True)
                logger.info("warm pool drained to idle=0")
        except asyncio.CancelledError:
            raise

    async def _start_session(self) -> LocalDoclingSession:
        async with self._launch_semaphore:
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

    def _post_file_request(
        self,
        session: LocalDoclingSession,
        files: list[FilePayload],
        data: dict[str, Any],
        headers: dict[str, str],
    ) -> httpx.Response:
        opened_files = []
        multipart_files = []
        try:
            for file in files:
                handle = file.open_binary()
                opened_files.append(handle)
                multipart_files.append(("files", (file.filename, handle, file.content_type)))
            return self._request_client.post(
                f"{session.base_url}/v1/convert/file",
                files=multipart_files,
                data=form_data(data),
                headers=forwarded_headers(headers),
            )
        finally:
            for handle in opened_files:
                handle.close()

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
                    await asyncio.wait_for(process.wait(), timeout=settings.child_shutdown_timeout_sec)
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
        deadline = time.monotonic() + settings.child_startup_timeout_sec
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
            await asyncio.sleep(settings.child_ready_poll_interval_sec)
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
            "1",
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
