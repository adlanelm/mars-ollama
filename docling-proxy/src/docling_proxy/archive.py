from __future__ import annotations

import asyncio
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from docling_proxy.config import settings

ARCHIVABLE_STATUSES = {"success", "partial_success"}
TEXT_OUTPUTS = {
    "md_content": ".md",
    "html_content": ".html",
    "text_content": ".txt",
    "doctags_content": ".doctags",
    "yaml_content": ".yaml",
    "vtt_content": ".vtt",
}


class ArchiveStore:
    def __init__(self, archive_dir: Path | None = None) -> None:
        self.archive_dir = archive_dir if archive_dir is not None else settings.archive_dir
        if self.archive_dir is not None:
            self.archive_dir.mkdir(parents=True, exist_ok=True)

    async def persist_payload(
        self,
        request_id: str,
        source_kind: str,
        filename: str | None,
        target_kind: str,
        payload: dict[str, Any],
    ) -> Path | None:
        if self.archive_dir is None:
            return None
        status = str(payload.get("status") or "")
        if status not in ARCHIVABLE_STATUSES:
            return None
        return await asyncio.to_thread(
            self._write_payload,
            request_id,
            source_kind,
            filename,
            target_kind,
            payload,
        )

    async def persist_zip(
        self,
        request_id: str,
        source_kind: str,
        filename: str | None,
        target_kind: str,
        zip_bytes: bytes,
        *,
        status: str = "success",
        errors: list[dict[str, Any]] | None = None,
        processing_time: float | None = None,
    ) -> Path | None:
        if self.archive_dir is None or status not in ARCHIVABLE_STATUSES:
            return None
        return await asyncio.to_thread(
            self._write_zip,
            request_id,
            source_kind,
            filename,
            target_kind,
            zip_bytes,
            status,
            errors or [],
            processing_time,
        )

    def _write_payload(
        self,
        request_id: str,
        source_kind: str,
        filename: str | None,
        target_kind: str,
        payload: dict[str, Any],
    ) -> Path:
        archive_dir = self._archive_run_dir(request_id, filename)
        archive_dir.mkdir(parents=True, exist_ok=False)

        document = payload.get("document") or {}
        artifacts: list[str] = []
        stem = self._safe_stem(filename)
        for key, ext in TEXT_OUTPUTS.items():
            content = document.get(key)
            if content is None:
                continue
            artifact_name = f"{stem}{ext}"
            (archive_dir / artifact_name).write_text(str(content), encoding="utf-8")
            artifacts.append(artifact_name)

        json_content = document.get("json_content")
        if json_content is not None:
            artifact_name = f"{stem}.json"
            (archive_dir / artifact_name).write_text(json.dumps(json_content, indent=2), encoding="utf-8")
            artifacts.append(artifact_name)

        self._write_meta(
            archive_dir,
            request_id=request_id,
            source_kind=source_kind,
            filename=filename,
            target_kind=target_kind,
            status=str(payload.get("status") or ""),
            processing_time=float(payload.get("processing_time") or 0.0),
            errors=list(payload.get("errors") or []),
            artifacts=artifacts,
        )
        return archive_dir

    def _write_zip(
        self,
        request_id: str,
        source_kind: str,
        filename: str | None,
        target_kind: str,
        zip_bytes: bytes,
        status: str,
        errors: list[dict[str, Any]],
        processing_time: float | None,
    ) -> Path:
        archive_dir = self._archive_run_dir(request_id, filename)
        archive_dir.mkdir(parents=True, exist_ok=False)
        zip_name = f"{self._safe_stem(filename)}.zip"
        (archive_dir / zip_name).write_bytes(zip_bytes)
        self._write_meta(
            archive_dir,
            request_id=request_id,
            source_kind=source_kind,
            filename=filename,
            target_kind=target_kind,
            status=status,
            processing_time=processing_time,
            errors=errors,
            artifacts=[zip_name],
        )
        return archive_dir

    def _write_meta(
        self,
        archive_dir: Path,
        *,
        request_id: str,
        source_kind: str,
        filename: str | None,
        target_kind: str,
        status: str,
        processing_time: float | None,
        errors: list[dict[str, Any]],
        artifacts: list[str],
    ) -> None:
        meta = {
            "request_id": request_id,
            "source_kind": source_kind,
            "filename": filename,
            "target_kind": target_kind,
            "status": status,
            "processing_time": processing_time,
            "errors": errors,
            "artifacts": artifacts,
            "archived_at": datetime.now(timezone.utc).isoformat(),
        }
        (archive_dir / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    def _archive_run_dir(self, request_id: str, filename: str | None) -> Path:
        assert self.archive_dir is not None
        now = datetime.now(timezone.utc)
        date_dir = self.archive_dir / now.strftime("%Y") / now.strftime("%m") / now.strftime("%d")
        run_name = f"{now.strftime('%Y%m%dT%H%M%S%fZ')}_{self._safe_component(request_id)}_{self._safe_stem(filename)}"
        return date_dir / run_name

    def _safe_stem(self, filename: str | None) -> str:
        if filename:
            stem = Path(filename).stem
        else:
            stem = "converted"
        return self._safe_component(stem)

    def _safe_component(self, value: str) -> str:
        cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("._")
        return cleaned[:120] or "converted"
