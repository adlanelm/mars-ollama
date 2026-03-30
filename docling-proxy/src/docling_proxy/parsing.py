from __future__ import annotations

import asyncio
import base64
import os
import shutil
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from docling_proxy.config import settings
from starlette.datastructures import FormData, UploadFile

from docling_proxy.contracts import ProxyOptions, ProxySourceRequest
from docling_proxy.models import FilePayload, NormalizedSource, cleanup_file_payloads


def parse_bool(value: str | bool | None) -> bool | None:
    if value is None or isinstance(value, bool):
        return value
    lowered = value.lower()
    if lowered in {"1", "true", "yes", "on"}:
        return True
    if lowered in {"0", "false", "no", "off"}:
        return False
    return None


def as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


async def parse_multipart_form(form: FormData) -> tuple[list[FilePayload], dict[str, Any], ProxyOptions | None]:
    files: list[FilePayload] = []
    grouped: dict[str, list[Any]] = defaultdict(list)
    upload_dir = settings.temp_dir / "uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)

    try:
        for key, value in form.multi_items():
            if isinstance(value, UploadFile):
                if key == "files":
                    files.append(await _persist_upload_file(value, upload_dir))
                continue
            grouped[key].append(value)
    except Exception:
        await cleanup_file_payloads(files)
        raise

    proxy_options = extract_proxy_options(grouped)
    docling_data = collapse_form_values({k: v for k, v in grouped.items() if not k.startswith("proxy_")})
    return files, docling_data, proxy_options


async def _persist_upload_file(upload: UploadFile, upload_dir: Path) -> FilePayload:
    file_payload = await asyncio.to_thread(_copy_upload_to_temp, upload, upload_dir)
    await upload.close()
    return file_payload


def _copy_upload_to_temp(upload: UploadFile, upload_dir: Path) -> FilePayload:
    suffix = Path(upload.filename or "upload.bin").suffix
    fd, temp_name = tempfile.mkstemp(prefix="upload-", suffix=suffix, dir=upload_dir)
    os.close(fd)
    temp_path = Path(temp_name)
    upload.file.seek(0)
    try:
        with temp_path.open("wb") as destination:
            shutil.copyfileobj(upload.file, destination)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise
    finally:
        upload.file.seek(0)
    return FilePayload(
        filename=upload.filename or "upload.bin",
        content=None,
        content_type=upload.content_type or "application/octet-stream",
        temp_path=temp_path,
    )


def collapse_form_values(grouped: dict[str, list[Any]]) -> dict[str, Any]:
    collapsed: dict[str, Any] = {}
    for key, values in grouped.items():
        if len(values) == 1:
            collapsed[key] = values[0]
        else:
            collapsed[key] = values
    return collapsed


def extract_proxy_options(grouped: dict[str, list[Any]]) -> ProxyOptions | None:
    if not any(key.startswith("proxy_") for key in grouped):
        return None

    kwargs: dict[str, Any] = {}
    mapping = {
        "proxy_enabled": "enabled",
        "proxy_force_split": "force_split",
        "proxy_include_proxy_meta": "include_proxy_meta",
        "proxy_max_pages_per_part": "max_pages_per_part",
        "proxy_max_concurrency": "max_concurrency",
        "proxy_poll_interval_sec": "poll_interval_sec",
    }
    for form_key, model_key in mapping.items():
        values = grouped.get(form_key)
        if not values:
            continue
        raw = values[-1]
        if model_key in {"enabled", "force_split", "include_proxy_meta"}:
            kwargs[model_key] = parse_bool(raw)
        elif model_key in {"max_pages_per_part", "max_concurrency"}:
            kwargs[model_key] = int(raw)
        else:
            kwargs[model_key] = float(raw)
    return ProxyOptions(**kwargs)


def normalize_requested_formats(options: dict[str, Any]) -> list[str]:
    to_formats = options.get("to_formats")
    values = [str(v) for v in as_list(to_formats)]
    return values or ["md"]


def normalize_source_request(request: ProxySourceRequest) -> tuple[list[dict[str, Any]], dict[str, Any], ProxyOptions | None, str]:
    sources: list[dict[str, Any]] = []
    if request.sources:
        sources.extend(request.sources)
    if request.http_sources:
        sources.extend([{**item, "kind": item.get("kind", "http")} for item in request.http_sources])
    if request.file_sources:
        sources.extend([{**item, "kind": item.get("kind", "file")} for item in request.file_sources])
    target_kind = request.target.kind if request.target else "inbody"
    return sources, dict(request.options), request.proxy_options, target_kind


def decode_file_source(source: dict[str, Any]) -> NormalizedSource:
    b64_value = source.get("base64_string") or source.get("data")
    if not b64_value:
        raise ValueError("File source is missing base64_string.")
    filename = source.get("filename") or "document.pdf"
    content = base64.b64decode(b64_value)
    return NormalizedSource(kind="file", filename=filename, content=content, original=source)


def filename_from_url(url: str) -> str:
    path = Path(urlparse(url).path)
    return path.name or "document.pdf"
