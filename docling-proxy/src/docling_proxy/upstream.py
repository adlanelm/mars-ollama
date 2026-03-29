from __future__ import annotations

import asyncio
from typing import Any

import httpx

from docling_proxy.config import settings
from docling_proxy.models import FilePayload


AUTH_HEADER_NAMES = {"authorization", "x-api-key"}


def forwarded_headers(headers: dict[str, str]) -> dict[str, str]:
    return {key: value for key, value in headers.items() if key.lower() in AUTH_HEADER_NAMES}


def form_data(data: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in data.items():
        if value is None:
            continue
        if isinstance(value, list):
            normalized[key] = [str(item) for item in value]
        else:
            normalized[key] = str(value)
    return normalized


class UpstreamClient:
    def __init__(self) -> None:
        self._client = httpx.AsyncClient(timeout=settings.upstream_timeout_sec)
        self._sync_client = httpx.Client(timeout=settings.upstream_timeout_sec)

    async def close(self) -> None:
        await self._client.aclose()
        await asyncio.to_thread(self._sync_client.close)

    async def relay_file_sync(self, endpoint: str, files: list[FilePayload], data: dict[str, Any], headers: dict[str, str]) -> httpx.Response:
        upstream_files = [
            ("files", (file.filename, file.content, file.content_type)) for file in files
        ]
        return await asyncio.to_thread(
            self._sync_client.post,
            f"{settings.upstream_url}{endpoint}",
            files=upstream_files,
            data=form_data(data),
            headers=forwarded_headers(headers),
        )

    async def relay_source_sync(self, endpoint: str, payload: dict[str, Any], headers: dict[str, str]) -> httpx.Response:
        return await self._client.post(
            f"{settings.upstream_url}{endpoint}",
            json=payload,
            headers=forwarded_headers(headers),
        )

    async def submit_file_async(self, files: list[FilePayload], data: dict[str, Any], headers: dict[str, str]) -> dict[str, Any]:
        response = await self.relay_file_sync("/convert/file/async", files, data, headers)
        response.raise_for_status()
        return response.json()

    async def submit_source_async(self, payload: dict[str, Any], headers: dict[str, str]) -> dict[str, Any]:
        response = await self.relay_source_sync("/convert/source/async", payload, headers)
        response.raise_for_status()
        return response.json()

    async def status(self, task_id: str, headers: dict[str, str]) -> dict[str, Any]:
        response = await self._client.get(
            f"{settings.upstream_url}/status/poll/{task_id}",
            headers=forwarded_headers(headers),
        )
        response.raise_for_status()
        return response.json()

    async def result(self, task_id: str, headers: dict[str, str]) -> httpx.Response:
        response = await self._client.get(
            f"{settings.upstream_url}/result/{task_id}",
            headers=forwarded_headers(headers),
        )
        response.raise_for_status()
        return response

    async def download(self, url: str, headers: dict[str, str] | None = None) -> httpx.Response:
        response = await self._client.get(url, headers=headers)
        response.raise_for_status()
        return response
