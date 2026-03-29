from __future__ import annotations

import asyncio

from docling_proxy.models import ProxyJob


class JobStore:
    def __init__(self) -> None:
        self._jobs: dict[str, ProxyJob] = {}
        self._lock = asyncio.Lock()

    async def put(self, job: ProxyJob) -> None:
        async with self._lock:
            self._jobs[job.task_id] = job

    async def get(self, task_id: str) -> ProxyJob | None:
        async with self._lock:
            return self._jobs.get(task_id)

    async def delete(self, task_id: str) -> None:
        async with self._lock:
            self._jobs.pop(task_id, None)


job_store = JobStore()
