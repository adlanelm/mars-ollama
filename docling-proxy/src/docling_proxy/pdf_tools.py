from __future__ import annotations

from io import BytesIO
from math import ceil

from pypdf import PdfReader, PdfWriter

from docling_proxy.config import settings
from docling_proxy.contracts import ProxyOptions, SplitDecision


def is_pdf(filename: str | None, content_type: str | None, data: bytes) -> bool:
    if content_type == "application/pdf":
        return True
    if filename and filename.lower().endswith(".pdf"):
        return True
    return data.startswith(b"%PDF")


def count_pdf_pages(data: bytes) -> int:
    reader = PdfReader(BytesIO(data))
    return len(reader.pages)


def resolve_max_pages(proxy_options: ProxyOptions | None) -> int:
    return (
        proxy_options.max_pages_per_part
        if proxy_options and proxy_options.max_pages_per_part
        else settings.max_pages_per_part
    )


def decide_split(data: bytes, proxy_options: ProxyOptions | None = None) -> SplitDecision:
    max_pages = resolve_max_pages(proxy_options)
    total_pages = count_pdf_pages(data)

    if proxy_options and proxy_options.enabled is False:
        return SplitDecision(should_split=False, reason="proxy disabled", total_pages=total_pages, parts=1)

    if proxy_options and proxy_options.force_split:
        return SplitDecision(
            should_split=total_pages > 1,
            reason="forced split",
            total_pages=total_pages,
            parts=ceil(total_pages / max_pages),
        )

    if total_pages <= max_pages:
        return SplitDecision(should_split=False, reason="below page threshold", total_pages=total_pages, parts=1)

    return SplitDecision(
        should_split=True,
        reason="pdf exceeds page threshold",
        total_pages=total_pages,
        parts=ceil(total_pages / max_pages),
    )


def compute_pages_per_part(max_pages_per_part: int) -> int:
    return max(1, max_pages_per_part)


def split_pdf(data: bytes, proxy_options: ProxyOptions | None = None) -> list[tuple[int, int, bytes]]:
    max_pages = resolve_max_pages(proxy_options)
    total_pages = count_pdf_pages(data)
    pages_per_part = compute_pages_per_part(max_pages)

    reader = PdfReader(BytesIO(data))
    parts: list[tuple[int, int, bytes]] = []
    for start in range(0, total_pages, pages_per_part):
        end = min(start + pages_per_part, total_pages)
        writer = PdfWriter()
        for page_index in range(start, end):
            writer.add_page(reader.pages[page_index])
        buffer = BytesIO()
        writer.write(buffer)
        parts.append((start + 1, end, buffer.getvalue()))
    return parts
