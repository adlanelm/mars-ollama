from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from io import BytesIO
from math import ceil
from pathlib import Path
from typing import BinaryIO, Iterator

from pypdf import PdfReader, PdfWriter

from docling_proxy.config import settings
from docling_proxy.contracts import ProxyOptions, SplitDecision

PdfSource = bytes | Path


@dataclass(slots=True)
class PdfChunkPlan:
    part_index: int
    start_page: int
    end_page: int


@dataclass(slots=True)
class PdfSplitPlan:
    total_pages: int
    chunks: list[PdfChunkPlan]


def is_pdf(filename: str | None, content_type: str | None, data: bytes) -> bool:
    if content_type == "application/pdf":
        return True
    if filename and filename.lower().endswith(".pdf"):
        return True
    return data.startswith(b"%PDF")


def count_pdf_pages(data: bytes) -> int:
    return count_pdf_pages_from_source(data)


def count_pdf_pages_from_source(source: PdfSource) -> int:
    with open_pdf_source(source) as stream:
        reader = PdfReader(stream)
        return len(reader.pages)


def resolve_max_pages(proxy_options: ProxyOptions | None) -> int:
    return (
        proxy_options.max_pages_per_part
        if proxy_options and proxy_options.max_pages_per_part
        else settings.max_pages_per_part
    )


def build_split_plan(source: PdfSource, proxy_options: ProxyOptions | None = None) -> PdfSplitPlan:
    max_pages = resolve_max_pages(proxy_options)
    total_pages = count_pdf_pages_from_source(source)
    pages_per_part = compute_pages_per_part(max_pages)
    chunks = [
        PdfChunkPlan(
            part_index=index,
            start_page=start + 1,
            end_page=min(start + pages_per_part, total_pages),
        )
        for index, start in enumerate(range(0, total_pages, pages_per_part))
    ]
    return PdfSplitPlan(total_pages=total_pages, chunks=chunks)


def decide_split(data: bytes, proxy_options: ProxyOptions | None = None) -> SplitDecision:
    return decide_split_from_plan(build_split_plan(data, proxy_options), proxy_options)


def decide_split_from_plan(plan: PdfSplitPlan, proxy_options: ProxyOptions | None = None) -> SplitDecision:
    max_pages = resolve_max_pages(proxy_options)
    total_pages = plan.total_pages

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


def materialize_pdf_chunk(source: PdfSource, chunk: PdfChunkPlan) -> bytes:
    with open_pdf_source(source) as stream:
        reader = PdfReader(stream)
        writer = PdfWriter()
        for page_index in range(chunk.start_page - 1, chunk.end_page):
            writer.add_page(reader.pages[page_index])
        buffer = BytesIO()
        writer.write(buffer)
        return buffer.getvalue()


def split_pdf(data: bytes, proxy_options: ProxyOptions | None = None) -> list[tuple[int, int, bytes]]:
    plan = build_split_plan(data, proxy_options)
    return [
        (chunk.start_page, chunk.end_page, materialize_pdf_chunk(data, chunk))
        for chunk in plan.chunks
    ]


@contextmanager
def open_pdf_source(source: PdfSource) -> Iterator[BinaryIO]:
    if isinstance(source, Path):
        with source.open("rb") as stream:
            yield stream
        return
    yield BytesIO(source)
