#!/usr/bin/env python3
from __future__ import annotations

import json
import logging
import os
from typing import Any

import anyio
import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from starlette.datastructures import UploadFile

APP_NAME = "docling-openwebui-proxy"
APP_VERSION = "0.1.4"

DOCLING_BASE_URL = os.getenv("DOCLING_BASE_URL", "http://docling:5001").rstrip("/")
PROXY_HOST = os.getenv("PROXY_HOST", "0.0.0.0")
PROXY_PORT = int(os.getenv("PROXY_PORT", "8000"))

OLLAMA_CHAT_URL = os.getenv("OLLAMA_CHAT_URL", "http://ollama:11434/v1/chat/completions")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "").strip()
OLLAMA_TIMEOUT = int(os.getenv("OLLAMA_TIMEOUT", "180"))
OLLAMA_TEMPERATURE = float(os.getenv("OLLAMA_TEMPERATURE", "0.0"))
OLLAMA_MAX_TOKENS = int(os.getenv("OLLAMA_MAX_TOKENS", "8192"))

DOCLING_PIPELINE = os.getenv("DOCLING_PIPELINE", "vlm")
PROXY_STRIP_STANDARD_OCR = os.getenv("PROXY_STRIP_STANDARD_OCR", "true").lower() in {"1", "true", "yes", "on"}
PROXY_DEBUG = os.getenv("PROXY_DEBUG", "false").lower() in {"1", "true", "yes", "on"}

logging.basicConfig(
    level=logging.DEBUG if PROXY_DEBUG else logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger(APP_NAME)

app = FastAPI(title=APP_NAME, version=APP_VERSION)


def _build_vlm_pipeline_model_api() -> dict[str, Any]:
    if not OLLAMA_MODEL:
        raise RuntimeError("OLLAMA_MODEL is required")
    return {
        "url": OLLAMA_CHAT_URL,
        "params": {
            "model": OLLAMA_MODEL,
            "temperature": OLLAMA_TEMPERATURE,
            "max_tokens": OLLAMA_MAX_TOKENS,
        },
        "response_format": "doctags",
        "timeout": OLLAMA_TIMEOUT,
    }


def _inject_options(options: dict[str, Any]) -> dict[str, Any]:
    options = dict(options)

    for key in ("vlm_pipeline_model_api", "vlm_pipeline_preset", "vlm_pipeline_custom_config"):
        options.pop(key, None)

    if PROXY_STRIP_STANDARD_OCR:
        for key in ("do_ocr", "force_ocr", "ocr_engine", "ocr_lang"):
            options.pop(key, None)

    options["pipeline"] = DOCLING_PIPELINE
    options["vlm_pipeline_model_api"] = _build_vlm_pipeline_model_api()
    return options


def _copy_response(resp: httpx.Response) -> Response:
    excluded = {
        "content-encoding",
        "transfer-encoding",
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "upgrade",
    }
    headers = {k: v for k, v in resp.headers.items() if k.lower() not in excluded}
    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers=headers,
        media_type=resp.headers.get("content-type"),
    )


def _outgoing_headers(request: Request, *, has_body: bool = True) -> dict[str, str]:
    excluded = {"host", "content-length"}
    if has_body:
        excluded.add("content-type")
    return {k: v for k, v in request.headers.items() if k.lower() not in excluded}


def _add_form_field(form_data: dict[str, Any], name: str, value: str) -> None:
    if name in form_data:
        existing = form_data[name]
        if isinstance(existing, list):
            existing.append(value)
        else:
            form_data[name] = [existing, value]
    else:
        form_data[name] = value


def _send_multipart_sync(
    method: str,
    target_url: str,
    headers: dict[str, str],
    form_data: dict[str, Any],
    files: list[tuple[str, tuple[str, Any, str]]],
) -> httpx.Response:
    timeout = httpx.Timeout(connect=30.0, read=None, write=None, pool=None)
    with httpx.Client(timeout=timeout) as client:
        return client.request(
            method,
            target_url,
            headers=headers,
            data=form_data,
            files=files,
        )


@app.get("/")
async def root() -> dict[str, Any]:
    return {
        "service": APP_NAME,
        "version": APP_VERSION,
        "docling_base_url": DOCLING_BASE_URL,
        "pipeline": DOCLING_PIPELINE,
        "ollama_chat_url": OLLAMA_CHAT_URL,
        "ollama_model": OLLAMA_MODEL or None,
    }


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/__proxy/config")
async def show_proxy_config() -> dict[str, Any]:
    return {
        "docling_base_url": DOCLING_BASE_URL,
        "pipeline": DOCLING_PIPELINE,
        "ollama_chat_url": OLLAMA_CHAT_URL,
        "ollama_model": OLLAMA_MODEL or None,
        "ollama_timeout": OLLAMA_TIMEOUT,
        "ollama_temperature": OLLAMA_TEMPERATURE,
        "ollama_max_tokens": OLLAMA_MAX_TOKENS,
        "strip_standard_ocr": PROXY_STRIP_STANDARD_OCR,
    }


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"])
async def proxy(path: str, request: Request) -> Response:
    if not OLLAMA_MODEL:
        return JSONResponse(status_code=500, content={"error": "OLLAMA_MODEL is not configured"})

    target_url = f"{DOCLING_BASE_URL}/{path}"
    if request.url.query:
        target_url = f"{target_url}?{request.url.query}"

    content_type = request.headers.get("content-type", "")
    method = request.method.upper()

    if PROXY_DEBUG:
        log.debug("Incoming request: %s %s content-type=%s", method, target_url, content_type)

    try:
        if "multipart/form-data" in content_type:
            form = await request.form()

            form_data: dict[str, Any] = {}
            files: list[tuple[str, tuple[str, Any, str]]] = []

            for key, value in form.multi_items():
                if isinstance(value, UploadFile):
                    files.append(
                        (
                            key,
                            (
                                value.filename or "upload.bin",
                                value.file,
                                value.content_type or "application/octet-stream",
                            ),
                        )
                    )
                else:
                    if key in {"pipeline", "vlm_pipeline_model_api", "vlm_pipeline_preset", "vlm_pipeline_custom_config"}:
                        continue
                    if PROXY_STRIP_STANDARD_OCR and key in {"do_ocr", "force_ocr", "ocr_engine", "ocr_lang"}:
                        continue
                    _add_form_field(form_data, key, str(value))

            _add_form_field(form_data, "pipeline", DOCLING_PIPELINE)
            _add_form_field(
                form_data,
                "vlm_pipeline_model_api",
                json.dumps(_build_vlm_pipeline_model_api(), separators=(",", ":")),
            )

            if PROXY_DEBUG:
                debug_form_data = {
                    k: ("<json>" if k == "vlm_pipeline_model_api" else v)
                    for k, v in form_data.items()
                }
                log.debug(
                    "Forwarding multipart to %s data=%s files=%s",
                    target_url,
                    debug_form_data,
                    [f[0] for f in files],
                )

            resp = await anyio.to_thread.run_sync(
                _send_multipart_sync,
                method,
                target_url,
                _outgoing_headers(request),
                form_data,
                files,
            )
            return _copy_response(resp)

        timeout = httpx.Timeout(connect=30.0, read=None, write=None, pool=None)
        async with httpx.AsyncClient(timeout=timeout) as client:
            if "application/json" in content_type:
                payload = await request.json()
                if not isinstance(payload, dict):
                    raise HTTPException(status_code=400, detail="JSON body must be an object")

                if "options" in payload and isinstance(payload["options"], dict):
                    payload["options"] = _inject_options(payload["options"])
                else:
                    payload = _inject_options(payload)

                if PROXY_DEBUG:
                    debug_payload = json.loads(json.dumps(payload))
                    if "options" in debug_payload and "vlm_pipeline_model_api" in debug_payload["options"]:
                        debug_payload["options"]["vlm_pipeline_model_api"] = "<json>"
                    elif "vlm_pipeline_model_api" in debug_payload:
                        debug_payload["vlm_pipeline_model_api"] = "<json>"
                    log.debug("Forwarding JSON to %s payload=%s", target_url, debug_payload)

                resp = await client.request(
                    method,
                    target_url,
                    headers=_outgoing_headers(request),
                    json=payload,
                )
                return _copy_response(resp)

            body = await request.body()
            resp = await client.request(
                method,
                target_url,
                headers=_outgoing_headers(request, has_body=False),
                content=body,
            )
            return _copy_response(resp)

    except httpx.RequestError as exc:
        log.exception("Request to Docling failed")
        return JSONResponse(status_code=502, content={"error": "Failed to reach Docling", "detail": str(exc)})
    except Exception as exc:
        log.exception("Proxy failed")
        return JSONResponse(status_code=500, content={"error": "Proxy failed", "detail": str(exc)})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=PROXY_HOST, port=PROXY_PORT)
