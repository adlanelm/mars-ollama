from __future__ import annotations

import json
from io import BytesIO
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from docling_proxy.contracts import ConvertDocumentResponse, ExportDocumentResponse

SUPPORTED_SPLIT_FORMATS = {"md", "json", "html", "text"}


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
    stem = Path(filename).stem or "converted"
    outputs = {
        "md": result.document.md_content,
        "json": json.dumps(result.document.json_content, indent=2) if result.document.json_content is not None else None,
        "html": result.document.html_content,
        "text": result.document.text_content,
    }

    buffer = BytesIO()
    with ZipFile(buffer, "w", compression=ZIP_DEFLATED) as zip_file:
        for fmt, content in outputs.items():
            if content is None:
                continue
            ext = "txt" if fmt == "text" else fmt
            zip_file.writestr(f"{stem}.{ext}", content)
    return buffer.getvalue()
