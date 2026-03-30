from __future__ import annotations

import os
from pathlib import Path

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="DOCLING_PROXY_",
        extra="ignore",
    )

    app_name: str = "Docling Split Proxy"
    archive_dir: Path | None = None
    state_dir: Path | None = None
    uvicorn_workers: int = 1
    upstream_url: str = "http://docling:5001/v1"
    max_pages_per_part: int = 25
    work_concurrency: int | None = None
    active_child_limit: int | None = None
    child_launch_concurrency: int = 1
    poll_interval_sec: float = 2.0
    upstream_timeout_sec: float = 120.0
    local_docling_command: str | None = None
    warm_child_pool_size: int | None = None
    warm_child_pool_retry_delay_sec: float | None = None
    warm_child_idle_timeout_sec: float | None = None
    warm_child_reap_interval_sec: float | None = None
    child_startup_timeout_sec: float | None = None
    child_ready_poll_interval_sec: float | None = None
    child_request_timeout_sec: float | None = None
    child_shutdown_timeout_sec: float | None = None
    total_job_timeout_sec: float = 36000.0
    temp_dir: Path = Field(default=Path("/tmp/docling-proxy"))
    cleanup_results: bool = False

    # Deprecated aliases kept for compatibility.
    max_concurrency: int | None = None
    local_docling_workers: int | None = None
    local_docling_pool_size: int | None = None
    local_docling_pool_retry_delay_sec: float | None = None
    local_docling_pool_idle_timeout_sec: float | None = None
    local_docling_pool_reap_interval_sec: float | None = None
    local_docling_startup_timeout_sec: float | None = None
    local_docling_ready_poll_interval_sec: float | None = None
    local_docling_request_timeout_sec: float | None = None
    local_docling_shutdown_timeout_sec: float | None = None

    @model_validator(mode="after")
    def apply_compatibility_aliases(self) -> Settings:
        env = os.environ

        if self.active_child_limit is None:
            if "DOCLING_PROXY_ACTIVE_CHILD_LIMIT" not in env and self.max_concurrency is not None:
                self.active_child_limit = self.max_concurrency
            else:
                self.active_child_limit = 1

        if self.work_concurrency is None:
            self.work_concurrency = self.active_child_limit

        if self.warm_child_pool_size is None:
            self.warm_child_pool_size = self.local_docling_pool_size if self.local_docling_pool_size is not None else 0
        if self.warm_child_pool_retry_delay_sec is None:
            self.warm_child_pool_retry_delay_sec = (
                self.local_docling_pool_retry_delay_sec if self.local_docling_pool_retry_delay_sec is not None else 5.0
            )
        if self.warm_child_idle_timeout_sec is None:
            self.warm_child_idle_timeout_sec = (
                self.local_docling_pool_idle_timeout_sec if self.local_docling_pool_idle_timeout_sec is not None else 300.0
            )
        if self.warm_child_reap_interval_sec is None:
            self.warm_child_reap_interval_sec = (
                self.local_docling_pool_reap_interval_sec if self.local_docling_pool_reap_interval_sec is not None else 5.0
            )
        if self.child_startup_timeout_sec is None:
            self.child_startup_timeout_sec = (
                self.local_docling_startup_timeout_sec if self.local_docling_startup_timeout_sec is not None else 60.0
            )
        if self.child_ready_poll_interval_sec is None:
            self.child_ready_poll_interval_sec = (
                self.local_docling_ready_poll_interval_sec if self.local_docling_ready_poll_interval_sec is not None else 0.5
            )
        if self.child_request_timeout_sec is None:
            self.child_request_timeout_sec = (
                self.local_docling_request_timeout_sec if self.local_docling_request_timeout_sec is not None else 36000.0
            )
        if self.child_shutdown_timeout_sec is None:
            self.child_shutdown_timeout_sec = (
                self.local_docling_shutdown_timeout_sec if self.local_docling_shutdown_timeout_sec is not None else 15.0
            )

        return self

    def deprecated_environment_messages(self) -> list[str]:
        env = os.environ
        messages: list[str] = []
        deprecated_map = {
            "DOCLING_PROXY_MAX_CONCURRENCY": "DOCLING_PROXY_ACTIVE_CHILD_LIMIT",
            "DOCLING_PROXY_LOCAL_DOCLING_POOL_SIZE": "DOCLING_PROXY_WARM_CHILD_POOL_SIZE",
            "DOCLING_PROXY_LOCAL_DOCLING_POOL_RETRY_DELAY_SEC": "DOCLING_PROXY_WARM_CHILD_POOL_RETRY_DELAY_SEC",
            "DOCLING_PROXY_LOCAL_DOCLING_POOL_IDLE_TIMEOUT_SEC": "DOCLING_PROXY_WARM_CHILD_IDLE_TIMEOUT_SEC",
            "DOCLING_PROXY_LOCAL_DOCLING_POOL_REAP_INTERVAL_SEC": "DOCLING_PROXY_WARM_CHILD_REAP_INTERVAL_SEC",
            "DOCLING_PROXY_LOCAL_DOCLING_STARTUP_TIMEOUT_SEC": "DOCLING_PROXY_CHILD_STARTUP_TIMEOUT_SEC",
            "DOCLING_PROXY_LOCAL_DOCLING_READY_POLL_INTERVAL_SEC": "DOCLING_PROXY_CHILD_READY_POLL_INTERVAL_SEC",
            "DOCLING_PROXY_LOCAL_DOCLING_REQUEST_TIMEOUT_SEC": "DOCLING_PROXY_CHILD_REQUEST_TIMEOUT_SEC",
            "DOCLING_PROXY_LOCAL_DOCLING_SHUTDOWN_TIMEOUT_SEC": "DOCLING_PROXY_CHILD_SHUTDOWN_TIMEOUT_SEC",
        }
        for old_name, new_name in deprecated_map.items():
            if old_name not in env:
                continue
            if new_name in env:
                messages.append(f"{old_name} is deprecated and ignored because {new_name} is set.")
            else:
                messages.append(f"{old_name} is deprecated; use {new_name} instead.")
        if "DOCLING_PROXY_LOCAL_DOCLING_WORKERS" in env:
            messages.append(
                "DOCLING_PROXY_LOCAL_DOCLING_WORKERS is deprecated and ignored; child docling-serve instances now always run with one HTTP worker."
            )
        return messages

    def resolved_state_dir(self) -> Path:
        if self.state_dir is not None:
            return self.state_dir
        if self.archive_dir is not None:
            return self.archive_dir / ".proxy-state"
        return self.temp_dir / "state"

    def upload_staging_dir(self) -> Path:
        return self.resolved_state_dir() / ".incoming"


settings = Settings()
