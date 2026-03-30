from __future__ import annotations

import json
from io import BytesIO
from pathlib import Path
from typing import Any
from zipfile import ZIP_DEFLATED, ZipFile

from docling_proxy.contracts import ConvertDocumentResponse, ExportDocumentResponse

SUPPORTED_SPLIT_FORMATS = {"md", "json", "html", "text"}
TEXT_OUTPUTS = {
    "md_content": ".md",
    "html_content": ".html",
    "text_content": ".txt",
    "doctags_content": ".doctags",
    "yaml_content": ".yaml",
    "vtt_content": ".vtt",
}


def ensure_supported_formats(requested_formats: list[str]) -> None:
    unsupported = sorted(set(requested_formats) - SUPPORTED_SPLIT_FORMATS)
    if unsupported:
        raise ValueError(
            "Split mode currently supports only md, json, html, and text outputs; "
            f"unsupported: {', '.join(unsupported)}"
        )


def merge_results(
    chunk_results: list[dict],
    requested_formats: list[str],
    page_break_placeholder: str | None = None,
    include_proxy_meta: bool = False,
    proxy_meta: dict | None = None,
) -> ConvertDocumentResponse:
    ensure_supported_formats(requested_formats)

    from docling_core.types.doc.document import DoclingDocument

    docs = []
    total_processing_time = 0.0
    errors = []
    for result in chunk_results:
        total_processing_time += float(result.get("processing_time") or 0.0)
        errors.extend(result.get("errors") or [])
        json_content = (result.get("document") or {}).get("json_content")
        if json_content is None:
            raise ValueError("Chunk result is missing document.json_content required for merging.")
        docs.append(DoclingDocument.model_validate(json_content))

    merged_doc = docs[0] if len(docs) == 1 else DoclingDocument.concatenate(docs=docs)

    document = ExportDocumentResponse(
        md_content=merged_doc.export_to_markdown(page_break_placeholder=page_break_placeholder)
        if "md" in requested_formats
        else None,
        json_content=merged_doc.export_to_dict() if "json" in requested_formats else None,
        html_content=merged_doc.export_to_html() if "html" in requested_formats else None,
        text_content=merged_doc.export_to_text(page_break_placeholder=page_break_placeholder)
        if "text" in requested_formats
        else None,
    )
    return ConvertDocumentResponse(
        document=document,
        status="success" if not errors else "partial_success",
        errors=errors,
        processing_time=total_processing_time,
        timings={},
        proxy_meta=proxy_meta if include_proxy_meta else None,
    )


def build_zip_response_bytes(result: ConvertDocumentResponse, filename: str) -> bytes:
    buffer = BytesIO()
    with ZipFile(buffer, "w", compression=ZIP_DEFLATED) as zip_file:
        for path, content in payload_to_zip_entries(result, filename).items():
            zip_file.writestr(path, content)
    return buffer.getvalue()


def payload_to_zip_entries(payload: ConvertDocumentResponse | dict[str, Any], filename: str) -> dict[str, bytes | str]:
    payload_dict = payload.model_dump(exclude_none=True) if isinstance(payload, ConvertDocumentResponse) else payload
    document = payload_dict.get("document") or {}
    stem = Path(filename).stem or "converted"
    outputs: dict[str, bytes | str] = {}
    for key, ext in TEXT_OUTPUTS.items():
        content = document.get(key)
        if content is None:
            continue
        outputs[f"{stem}{ext}"] = str(content)
    json_content = document.get("json_content")
    if json_content is not None:
        outputs[f"{stem}.json"] = json.dumps(json_content, indent=2)
    return outputs


def build_batch_zip_response_bytes(
    zip_outputs: list[tuple[str, bytes]],
    payload_outputs: list[tuple[str, str, dict[str, Any]]],
    errors: list[dict[str, Any]] | None = None,
) -> bytes:
    buffer = BytesIO()
    with ZipFile(buffer, "w", compression=ZIP_DEFLATED) as zip_file:
        for prefix, zip_bytes in zip_outputs:
            with ZipFile(BytesIO(zip_bytes)) as source_zip:
                for info in source_zip.infolist():
                    if info.is_dir():
                        continue
                    path = f"{prefix}/{info.filename}" if prefix else info.filename
                    zip_file.writestr(path, source_zip.read(info.filename))
        for prefix, filename, payload in payload_outputs:
            for path, content in payload_to_zip_entries(payload, filename).items():
                zip_file.writestr(f"{prefix}/{path}" if prefix else path, content)
        if errors:
            zip_file.writestr("errors.json", json.dumps(errors, indent=2))
    return buffer.getvalue()
