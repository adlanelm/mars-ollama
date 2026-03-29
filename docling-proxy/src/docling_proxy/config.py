from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="DOCLING_PROXY_",
        extra="ignore",
    )

    app_name: str = "Docling Split Proxy"
    uvicorn_workers: int = 1
    upstream_url: str = "http://docling:5001/v1"
    max_pages_per_part: int = 25
    max_concurrency: int = 1
    poll_interval_sec: float = 2.0
    upstream_timeout_sec: float = 120.0
    local_docling_command: str | None = None
    local_docling_workers: int = 1
    local_docling_startup_timeout_sec: float = 60.0
    local_docling_ready_poll_interval_sec: float = 0.5
    local_docling_request_timeout_sec: float = 36000.0
    local_docling_shutdown_timeout_sec: float = 15.0
    total_job_timeout_sec: float = 36000.0
    temp_dir: Path = Field(default=Path("/tmp/docling-proxy"))
    cleanup_results: bool = False


settings = Settings()
