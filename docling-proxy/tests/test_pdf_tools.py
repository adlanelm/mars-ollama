from io import BytesIO

from pypdf import PdfWriter

from docling_proxy.contracts import ProxyOptions
from docling_proxy.pdf_tools import build_split_plan, count_pdf_pages, decide_split, materialize_pdf_chunk, split_pdf


def make_pdf(page_count: int) -> bytes:
    writer = PdfWriter()
    for _ in range(page_count):
        writer.add_blank_page(width=300, height=300)
    buffer = BytesIO()
    writer.write(buffer)
    return buffer.getvalue()


def test_split_pdf_uses_page_threshold():
    pdf_bytes = make_pdf(7)
    parts = split_pdf(pdf_bytes, ProxyOptions(force_split=True, max_pages_per_part=3))
    assert [(start, end) for start, end, _ in parts] == [(1, 3), (4, 6), (7, 7)]
    assert [count_pdf_pages(chunk) for _, _, chunk in parts] == [3, 3, 1]


def test_build_split_plan_separates_ranges_from_chunk_materialization(tmp_path):
    pdf_path = tmp_path / "large.pdf"
    pdf_path.write_bytes(make_pdf(7))

    plan = build_split_plan(pdf_path, ProxyOptions(force_split=True, max_pages_per_part=3))

    assert [(chunk.start_page, chunk.end_page) for chunk in plan.chunks] == [(1, 3), (4, 6), (7, 7)]
    assert count_pdf_pages(materialize_pdf_chunk(pdf_path, plan.chunks[1])) == 3


def test_decide_split_respects_disable_flag():
    pdf_bytes = make_pdf(20)
    decision = decide_split(pdf_bytes, ProxyOptions(enabled=False, max_pages_per_part=2))
    assert decision.should_split is False
    assert decision.reason == "proxy disabled"


def test_decide_split_uses_only_page_threshold():
    pdf_bytes = make_pdf(4)
    decision = decide_split(pdf_bytes, ProxyOptions(max_pages_per_part=3))
    assert decision.should_split is True
    assert decision.reason == "pdf exceeds page threshold"
    assert decision.parts == 2
