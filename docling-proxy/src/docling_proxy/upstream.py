from __future__ import annotations

from typing import Any

import httpx

from docling_proxy.config import settings


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

    async def close(self) -> None:
        await self._client.aclose()

    async def download(self, url: str, headers: dict[str, str] | None = None) -> httpx.Response:
        response = await self._client.get(url, headers=headers)
        response.raise_for_status()
        return response
