import asyncio
import json
import logging
import os
import shlex
import signal
import subprocess
import time
from contextlib import asynccontextmanager
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(case_sensitive=False)

    # Ollama
    ollama_base_url: str = "http://ollama:11434"
    ollama_busy_wait_timeout_seconds: float = 120.0
    ollama_ps_fallback_timeout_seconds: float = 180.0
    ollama_ps_poll_interval_seconds: float = 0.5

    # vLLM child process
    vllm_command: str
    vllm_base_url: str = "http://127.0.0.1:8000"
    vllm_api_key: str = ""
    vllm_start_timeout_seconds: float = 300.0
    vllm_stop_timeout_seconds: float = 20.0

    log_level: str = "INFO"


def load_settings() -> Settings:
    try:
        settings = Settings()
    except ValidationError as exc:
        missing_fields = {
            ".".join(str(part) for part in err["loc"])
            for err in exc.errors()
            if err.get("type") == "missing"
        }

        if "vllm_command" in missing_fields:
            raise RuntimeError(
                "Missing required configuration: VLLM_COMMAND.\n"
                "Set it to the full vLLM command line to run for reranking.\n"
                "Example:\n"
                "  VLLM_COMMAND='vllm serve tomaarsen/Qwen3-Reranker-0.6B-seq-cls "
                "--host 127.0.0.1 --port 8000 --runner pooling'\n"
                "Also ensure VLLM_BASE_URL matches that host and port, for example:\n"
                "  VLLM_BASE_URL='http://127.0.0.1:8000'"
            ) from exc

        raise RuntimeError(f"Invalid configuration: {exc}") from exc

    if not settings.vllm_command.strip():
        raise RuntimeError(
            "Invalid configuration: VLLM_COMMAND is set but empty.\n"
            "Provide the full vLLM command line, for example:\n"
            "  VLLM_COMMAND='vllm serve tomaarsen/Qwen3-Reranker-0.6B-seq-cls "
            "--host 127.0.0.1 --port 8000 --runner pooling'"
        )

    return settings


settings = load_settings()

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    force=True,
)
logger = logging.getLogger("arbiter")

HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}

OLLAMA_BUSY_PATHS = {
    "/api/chat",
    "/api/generate",
}


def parse_vllm_command(raw: str) -> list[str]:
    raw = raw.strip()
    if not raw:
        raise RuntimeError(
            "Invalid configuration: VLLM_COMMAND is empty. "
            "Set it to the full vLLM command line."
        )

    # Support either a shell string or a JSON array of argv parts.
    if raw.startswith("["):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                "Invalid configuration: VLLM_COMMAND looks like JSON but could not be parsed."
            ) from exc

        if not isinstance(parsed, list) or not parsed or not all(isinstance(x, str) and x for x in parsed):
            raise RuntimeError(
                "Invalid configuration: VLLM_COMMAND JSON form must be a non-empty array of strings."
            )

        return parsed

    argv = shlex.split(raw)
    if not argv:
        raise RuntimeError(
            "Invalid configuration: VLLM_COMMAND did not produce any executable arguments."
        )
    return argv


def filter_request_headers(headers: httpx.Headers) -> dict[str, str]:
    result: dict[str, str] = {}
    for key, value in headers.items():
        key_l = key.lower()
        if key_l in HOP_BY_HOP_HEADERS or key_l in {"host", "content-length"}:
            continue
        result[key] = value
    return result


def filter_response_headers(headers: httpx.Headers) -> dict[str, str]:
    result: dict[str, str] = {}
    for key, value in headers.items():
        key_l = key.lower()
        if key_l in HOP_BY_HOP_HEADERS or key_l == "content-length":
            continue
        result[key] = value
    return result


class EngineArbiter:
    def __init__(self, http: httpx.AsyncClient) -> None:
        self.http = http

        self._busy_lock = asyncio.Lock()
        self._ollama_busy_count = 0
        self._ollama_idle = asyncio.Event()
        self._ollama_idle.set()

        self._ollama_admission_open = asyncio.Event()
        self._ollama_admission_open.set()

        self._rerank_lock = asyncio.Lock()

        self._vllm_proc: Optional[subprocess.Popen] = None

    @property
    def ollama_busy_count(self) -> int:
        return self._ollama_busy_count

    async def begin_ollama_request(self) -> None:
        while True:
            await self._ollama_admission_open.wait()
            async with self._busy_lock:
                if self._ollama_admission_open.is_set():
                    self._ollama_busy_count += 1
                    self._ollama_idle.clear()
                    logger.debug("Ollama request started; busy_count=%s", self._ollama_busy_count)
                    return

    async def end_ollama_request(self) -> None:
        async with self._busy_lock:
            self._ollama_busy_count = max(0, self._ollama_busy_count - 1)
            logger.debug("Ollama request finished; busy_count=%s", self._ollama_busy_count)
            if self._ollama_busy_count == 0:
                self._ollama_idle.set()

    def close_ollama_admission(self) -> None:
        self._ollama_admission_open.clear()
        logger.debug("Closed Ollama admission gate")

    def open_ollama_admission(self) -> None:
        self._ollama_admission_open.set()
        logger.debug("Opened Ollama admission gate")

    async def wait_for_ollama_idle(self) -> None:
        try:
            await asyncio.wait_for(
                self._ollama_idle.wait(),
                timeout=settings.ollama_busy_wait_timeout_seconds,
            )
            logger.info("Ollama became idle via arbiter busy tracking")
            return
        except asyncio.TimeoutError:
            logger.warning(
                "Busy tracking did not reach idle within %ss; falling back to /api/ps",
                settings.ollama_busy_wait_timeout_seconds,
            )

        await self._wait_for_no_loaded_ollama_models()

    async def _wait_for_no_loaded_ollama_models(self) -> None:
        deadline = time.monotonic() + settings.ollama_ps_fallback_timeout_seconds

        while time.monotonic() < deadline:
            loaded = await self.list_loaded_ollama_models()
            if not loaded:
                logger.info("Fallback /api/ps says no Ollama models are loaded")
                async with self._busy_lock:
                    self._ollama_busy_count = 0
                    self._ollama_idle.set()
                return

            logger.debug("Fallback /api/ps still sees loaded models: %s", loaded)
            await asyncio.sleep(settings.ollama_ps_poll_interval_seconds)

        raise HTTPException(
            status_code=409,
            detail=(
                "Timed out waiting for Ollama to become idle. "
                "Busy tracking did not clear, and /api/ps still shows loaded models."
            ),
        )

    async def list_loaded_ollama_models(self) -> list[str]:
        resp = await self.http.get(f"{settings.ollama_base_url}/api/ps")
        resp.raise_for_status()

        models = resp.json().get("models", [])
        names: list[str] = []
        for item in models:
            name = item.get("model") or item.get("name")
            if name:
                names.append(name)
        return names

    async def unload_ollama_models(self) -> None:
        models = await self.list_loaded_ollama_models()
        if not models:
            logger.info("No Ollama models loaded")
            return

        logger.info("Unloading Ollama models: %s", models)
        for model_name in models:
            resp = await self.http.post(
                f"{settings.ollama_base_url}/api/generate",
                json={"model": model_name, "keep_alive": 0},
            )
            resp.raise_for_status()

        deadline = time.monotonic() + 60.0
        while time.monotonic() < deadline:
            remaining = await self.list_loaded_ollama_models()
            if not remaining:
                logger.info("All Ollama models unloaded")
                return
            await asyncio.sleep(0.25)

        raise HTTPException(
            status_code=503,
            detail="Timed out waiting for Ollama model unload to complete.",
        )

    def _build_vllm_command(self) -> list[str]:
        return parse_vllm_command(settings.vllm_command)

    async def ensure_vllm_started(self) -> None:
        if self._vllm_proc and self._vllm_proc.poll() is None:
            return

        cmd = self._build_vllm_command()
        logger.info("Starting vLLM: %s", " ".join(cmd))

        self._vllm_proc = subprocess.Popen(
            cmd,
            stdout=None,
            stderr=None,
            start_new_session=True,
            env=os.environ.copy(),
        )

        await self._wait_for_vllm_health()

    async def _wait_for_vllm_health(self) -> None:
        url = f"{settings.vllm_base_url.rstrip('/')}/health"
        deadline = time.monotonic() + settings.vllm_start_timeout_seconds

        while time.monotonic() < deadline:
            if self._vllm_proc and self._vllm_proc.poll() is not None:
                raise HTTPException(
                    status_code=503,
                    detail=f"vLLM exited early with code {self._vllm_proc.returncode}",
                )

            try:
                resp = await self.http.get(url)
                if resp.status_code == 200:
                    logger.info("vLLM is healthy")
                    return
            except httpx.HTTPError:
                pass

            await asyncio.sleep(1.0)

        raise HTTPException(status_code=503, detail="Timed out waiting for vLLM health.")

    async def stop_vllm(self) -> None:
        proc = self._vllm_proc
        self._vllm_proc = None

        if proc is None or proc.poll() is not None:
            return

        logger.info("Stopping vLLM pid=%s", proc.pid)
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            return

        deadline = time.monotonic() + settings.vllm_stop_timeout_seconds
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                logger.info("vLLM exited cleanly")
                return
            await asyncio.sleep(0.2)

        logger.warning("vLLM did not stop in time; sending SIGKILL")
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            return


@asynccontextmanager
async def lifespan(app: FastAPI):
    http = httpx.AsyncClient(timeout=None)
    arbiter = EngineArbiter(http=http)
    app.state.http = http
    app.state.arbiter = arbiter
    try:
        yield
    finally:
        await arbiter.stop_vllm()
        await http.aclose()


app = FastAPI(title="LLM Arbiter", version="1.3.0", lifespan=lifespan)


@app.get("/healthz")
async def healthz(request: Request):
    arbiter: EngineArbiter = request.app.state.arbiter
    vllm_running = bool(arbiter._vllm_proc and arbiter._vllm_proc.poll() is None)

    return {
        "ok": True,
        "ollama_busy_count": arbiter.ollama_busy_count,
        "ollama_admission_open": arbiter._ollama_admission_open.is_set(),
        "vllm_running": vllm_running,
        "vllm_base_url": settings.vllm_base_url,
    }


async def proxy_to_ollama(request: Request, path: str) -> Response:
    arbiter: EngineArbiter = request.app.state.arbiter
    http: httpx.AsyncClient = request.app.state.http

    upstream_url = f"{settings.ollama_base_url}/api/{path}"
    full_path = f"/api/{path}"
    is_busy_endpoint = full_path in OLLAMA_BUSY_PATHS

    body = await request.body()
    headers = filter_request_headers(request.headers)
    query_params = dict(request.query_params)

    if is_busy_endpoint:
        await arbiter.begin_ollama_request()

    try:
        upstream_request = http.build_request(
            request.method,
            upstream_url,
            params=query_params,
            content=body,
            headers=headers,
        )
        upstream_response = await http.send(upstream_request, stream=True)
    except Exception:
        if is_busy_endpoint:
            await arbiter.end_ollama_request()
        raise

    response_headers = filter_response_headers(upstream_response.headers)
    media_type = upstream_response.headers.get("content-type")

    async def body_iterator():
        try:
            async for chunk in upstream_response.aiter_raw():
                yield chunk
        finally:
            await upstream_response.aclose()
            if is_busy_endpoint:
                await arbiter.end_ollama_request()

    return StreamingResponse(
        body_iterator(),
        status_code=upstream_response.status_code,
        headers=response_headers,
        media_type=media_type,
    )


@app.api_route(
    "/api/{path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
)
async def ollama_proxy(path: str, request: Request):
    return await proxy_to_ollama(request, path)


async def proxy_rerank_to_vllm(request: Request) -> Response:
    arbiter: EngineArbiter = request.app.state.arbiter
    http: httpx.AsyncClient = request.app.state.http

    body = await request.body()
    headers = filter_request_headers(request.headers)
    if settings.vllm_api_key:
        headers["Authorization"] = f"Bearer {settings.vllm_api_key}"

    async with arbiter._rerank_lock:
        arbiter.close_ollama_admission()

        try:
            await arbiter.wait_for_ollama_idle()
            await arbiter.unload_ollama_models()
            await arbiter.ensure_vllm_started()

            resp = await http.post(
                f"{settings.vllm_base_url.rstrip('/')}/v1/rerank",
                content=body,
                headers=headers,
            )

            return Response(
                content=resp.content,
                status_code=resp.status_code,
                media_type=resp.headers.get("content-type", "application/json"),
                headers=filter_response_headers(resp.headers),
            )
        finally:
            await arbiter.stop_vllm()
            arbiter.open_ollama_admission()


@app.post("/v1/rerank")
async def rerank_v1(request: Request):
    return await proxy_rerank_to_vllm(request)


@app.post("/rerank")
async def rerank_alias(request: Request):
    return await proxy_rerank_to_vllm(request)
