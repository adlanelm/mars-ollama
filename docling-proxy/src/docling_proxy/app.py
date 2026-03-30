from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response

from docling_proxy.archive import ArchiveStore
from docling_proxy.config import settings
from docling_proxy.contracts import ProxySourceRequest
from docling_proxy.local_docling import LocalDoclingManager
from docling_proxy.parsing import normalize_source_request, parse_multipart_form
from docling_proxy.state import TaskStateStore
from docling_proxy.service import ProxyService, source_to_normalized_source
from docling_proxy.upstream import UpstreamClient


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logging.getLogger("httpx").setLevel(logging.WARNING)


@asynccontextmanager
async def lifespan(app: FastAPI):
    for message in settings.deprecated_environment_messages():
        logging.getLogger(__name__).warning(message)
    upstream = UpstreamClient()
    archive_store = ArchiveStore()
    state_store = TaskStateStore()
    local_docling = LocalDoclingManager()
    await local_docling.start()
    app.state.proxy_service = ProxyService(upstream, local_docling, archive_store, state_store)
    await app.state.proxy_service.resume_incomplete_operations()
    yield
    await local_docling.close()
    await upstream.close()


app = FastAPI(title=settings.app_name, lifespan=lifespan)


def service(request: Request) -> ProxyService:
    return request.app.state.proxy_service


def request_headers(request: Request) -> dict[str, str]:
    return dict(request.headers.items())


def relay_httpx_response(response) -> Response:
    content_type = response.headers.get("content-type", "application/json")
    return Response(
        content=response.content,
        status_code=response.status_code,
        media_type=content_type.split(";")[0],
        headers={
            key: value
            for key, value in response.headers.items()
            if key.lower() in {"content-disposition"}
        },
    )


@app.get("/health")
async def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/v1/convert/file")
async def convert_file(request: Request):
    form = await request.form()
    files, data, proxy_options = await parse_multipart_form(form)
    if not files:
        raise HTTPException(status_code=422, detail="At least one file is required.")
    result = await service(request).process_file_sync(files, data, proxy_options, request_headers(request))
    if isinstance(result, Response):
        return result
    if hasattr(result, "status_code"):
        return relay_httpx_response(result)
    return JSONResponse(content=result.model_dump(exclude_none=True))


@app.post("/v1/convert/file/async")
async def convert_file_async(request: Request):
    form = await request.form()
    files, data, proxy_options = await parse_multipart_form(form)
    if not files:
        raise HTTPException(status_code=422, detail="At least one file is required.")
    status = await service(request).enqueue_file_job(files, data, proxy_options, request_headers(request))
    return JSONResponse(content=status.model_dump(exclude_none=True))


@app.post("/v1/convert/source")
async def convert_source(payload: ProxySourceRequest, request: Request):
    sources, options, proxy_options, target_kind = normalize_source_request(payload)
    normalized_source = await source_to_normalized_source(service(request).upstream, sources)
    result = await service(request).process_source_sync(
        payload.model_dump(exclude_none=True),
        normalized_source,
        options,
        proxy_options,
        request_headers(request),
        target_kind,
    )
    if isinstance(result, Response):
        return result
    if hasattr(result, "status_code"):
        return relay_httpx_response(result)
    return JSONResponse(content=result.model_dump(exclude_none=True))


@app.post("/v1/convert/source/async")
async def convert_source_async(payload: ProxySourceRequest, request: Request):
    sources, options, proxy_options, target_kind = normalize_source_request(payload)
    normalized_source = await source_to_normalized_source(service(request).upstream, sources)
    status = await service(request).enqueue_source_job(
        payload.model_dump(exclude_none=True),
        options,
        proxy_options,
        request_headers(request),
        normalized_source,
        target_kind,
    )
    return JSONResponse(content=status.model_dump(exclude_none=True))


@app.get("/v1/status/poll/{task_id}")
async def poll_status(task_id: str, request: Request):
    status = await service(request).get_status(task_id)
    return JSONResponse(content=status.model_dump(exclude_none=True))


@app.get("/v1/result/{task_id}")
async def get_result(task_id: str, request: Request):
    job = await service(request).get_result(task_id)
    if job.result_zip is not None:
        return Response(
            content=job.result_zip,
            media_type="application/zip",
            headers={"Content-Disposition": 'attachment; filename="converted_docs.zip"'},
        )
    if job.result_payload is None:
        raise HTTPException(status_code=404, detail="Task result not found.")
    return JSONResponse(content=job.result_payload)
