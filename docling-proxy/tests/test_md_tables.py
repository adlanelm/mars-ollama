from docling_proxy.md_tables import rewrite_payload_tables, split_long_tables, truthy


def table(rows: int, header="| Item | Count |", sep="| - | - |") -> str:
    return "\n".join([header, sep] + [f"| I-{i} | {i * 7} |" for i in range(rows)])


def test_short_table_and_text_unchanged():
    md = "# Title\n\nSome text | with a pipe.\n\n" + table(3) + "\n\nAfter."
    assert split_long_tables(md, 200) == md


def test_disabled_when_max_chars_zero():
    md = table(500)
    assert split_long_tables(md, 0) == md


def test_long_table_split_with_header_repeated():
    md = "Before.\n\n" + table(40) + "\n\nAfter."
    out = split_long_tables(md, 120)
    blocks = [b for b in out.split("\n\n") if b.startswith("|")]
    assert len(blocks) > 1
    for block in blocks:
        lines = block.split("\n")
        assert lines[:2] == ["| Item | Count |", "| - | - |"]
        assert len(lines) >= 3
        assert len(block) <= 120
    rows = [line for block in blocks for line in block.split("\n")[2:]]
    assert rows == [f"| I-{i} | {i * 7} |" for i in range(40)]  # every row once, in order
    assert out.startswith("Before.\n\n") and out.endswith("\n\nAfter.")


def test_single_row_longer_than_limit_kept_whole():
    md = "| A | B |\n|---|---|\n| " + "x" * 300 + " | y |\n| short | row |"
    out = split_long_tables(md, 100)
    assert "x" * 300 in out
    assert out.count("| A | B |") == 2


def test_padded_separator_and_alignment_recognised():
    md = table(30, header="| Item   |   Count |", sep="|:-------|--------:|")
    out = split_long_tables(md, 150)
    assert out.count("|:-------|--------:|") > 1


def test_pipe_block_without_separator_unchanged():
    md = "\n".join(f"| not a table row {i} |" for i in range(50))
    assert split_long_tables(md, 100) == md


def test_tables_inside_code_fence_unchanged():
    md = "```\n" + table(40) + "\n```"
    assert split_long_tables(md, 100) == md


def test_page_break_placeholder_preserved():
    md = table(20) + "\n\n\f\n\n" + table(20)
    out = split_long_tables(md, 100)
    assert out.count("\f") == 1


def test_rewrite_payload_tables():
    payload = {"document": {"md_content": table(40), "json_content": {"k": 1}}, "status": "success"}
    assert rewrite_payload_tables(payload, 120) is True
    assert payload["document"]["md_content"].count("| Item | Count |") > 1
    assert payload["document"]["json_content"] == {"k": 1}
    assert rewrite_payload_tables(payload, 0) is False
    assert rewrite_payload_tables({"document": {"md_content": None}}, 120) is False
    assert rewrite_payload_tables({"status": "failure"}, 120) is False


def test_truthy():
    assert truthy("true") and truthy("True") and truthy(True) and truthy("1") and truthy(["false", "true"])
    assert not truthy(None) and not truthy("false") and not truthy("") and not truthy([])


def test_merge_results_compact_and_split():
    from docling_core.types.doc.document import DoclingDocument, TableCell, TableData

    from docling_proxy.merge import merge_results

    doc = DoclingDocument(name="synthetic")
    rows = [["Item", "Description"]] + [[f"I-{i}", "a deliberately long description cell"] for i in range(30)]
    cells = [
        TableCell(text=text, start_row_offset_idx=r, end_row_offset_idx=r + 1, start_col_offset_idx=c,
                  end_col_offset_idx=c + 1, column_header=r == 0)
        for r, row in enumerate(rows) for c, text in enumerate(row)
    ]
    doc.add_table(data=TableData(num_rows=len(rows), num_cols=2, table_cells=cells))
    chunk = {"document": {"json_content": doc.export_to_dict()}, "processing_time": 1.0}

    padded = merge_results([chunk], ["md"]).document.md_content
    compact = merge_results([chunk], ["md"], compact_tables=True).document.md_content
    split = merge_results([chunk], ["md"], compact_tables=True, table_max_chars=400).document.md_content
    assert len(compact) < len(padded)
    assert split.count("| Item | Description |") > 1
    assert all(len(block) <= 400 for block in split.split("\n\n") if block.startswith("|"))


class FakeHttpxResponse:
    def __init__(self, content: bytes, content_type="application/json", status_code=200):
        self.content, self.status_code = content, status_code
        self.headers = {"content-type": content_type}


def test_relay_rewrites_tables_and_keeps_utf8(monkeypatch):
    import json

    from docling_proxy import app as app_module

    monkeypatch.setattr(app_module.settings, "md_table_max_chars", 120)
    body = {"document": {"md_content": "## Sändningar\n\n" + table(40)}, "status": "success"}
    out = app_module.relay_httpx_response(FakeHttpxResponse(json.dumps(body, ensure_ascii=False).encode()))
    assert "Sändningar".encode() in out.body
    assert json.loads(out.body)["document"]["md_content"].count("| Item | Count |") > 1


def test_relay_passes_through_when_off_invalid_or_not_json(monkeypatch):
    from docling_proxy import app as app_module

    raw = b'{"document": {"md_content": "' + table(40).replace("\n", "\\n").encode() + b'"}}'
    monkeypatch.setattr(app_module.settings, "md_table_max_chars", 0)
    assert app_module.relay_httpx_response(FakeHttpxResponse(raw)).body == raw
    monkeypatch.setattr(app_module.settings, "md_table_max_chars", 120)
    assert app_module.relay_httpx_response(FakeHttpxResponse(b"not json{")).body == b"not json{"
    assert app_module.relay_httpx_response(FakeHttpxResponse(b"PK\x03\x04", "application/zip")).body == b"PK\x03\x04"
    assert app_module.relay_httpx_response(FakeHttpxResponse(raw, status_code=500)).body == raw
