from __future__ import annotations

import re
from typing import Any

# Markdown post-processing for RAG clients (Open WebUI) that chunk md_content with a character splitter.
# A long pipe table has no blank lines, so the splitter cuts it mid-row and every later chunk loses the header.
# split_long_tables() re-emits such a table as several tables of at most max_chars, each with the header and
# separator rows repeated, separated by blank lines, so a "\n\n"-first splitter cuts between whole sub-tables.

_SEPARATOR = re.compile(r"^\s*\|?\s*:?-+:?\s*(\|\s*:?-+:?\s*)*\|?\s*$")
_FENCE = re.compile(r"^\s*(```|~~~)")


def _is_table_line(line: str) -> bool:
    return line.lstrip().startswith("|")


def _split_table(lines: list[str], max_chars: int) -> list[str]:
    block = "\n".join(lines)
    if len(lines) < 3 or len(block) <= max_chars or not _SEPARATOR.match(lines[1]):
        return lines
    head = lines[:2]
    head_len = len(lines[0]) + len(lines[1]) + 2
    parts: list[list[str]] = []
    current: list[str] = []
    size = head_len
    for row in lines[2:]:
        if current and size + len(row) + 1 > max_chars:
            parts.append(current)
            current, size = [], head_len
        current.append(row)
        size += len(row) + 1
    parts.append(current)
    out: list[str] = []
    for i, rows in enumerate(parts):
        if i:
            out.append("")
        out.extend(head + rows)
    return out


def split_long_tables(md: str, max_chars: int) -> str:
    """Split every pipe table longer than max_chars at row boundaries (header repeated). max_chars <= 0: unchanged."""
    if max_chars <= 0 or "|" not in md:
        return md
    lines = md.split("\n")
    out: list[str] = []
    in_fence = False
    i = 0
    while i < len(lines):
        line = lines[i]
        if _FENCE.match(line):
            in_fence = not in_fence
        if in_fence or not _is_table_line(line):
            out.append(line)
            i += 1
            continue
        j = i
        while j < len(lines) and _is_table_line(lines[j]):
            j += 1
        out.extend(_split_table(lines[i:j], max_chars))
        i = j
    return "\n".join(out)


def rewrite_payload_tables(payload: Any, max_chars: int) -> bool:
    """Apply split_long_tables to payload["document"]["md_content"] in place. Returns True if it changed."""
    if max_chars <= 0 or not isinstance(payload, dict):
        return False
    document = payload.get("document")
    if not isinstance(document, dict) or not isinstance(document.get("md_content"), str):
        return False
    new = split_long_tables(document["md_content"], max_chars)
    if new == document["md_content"]:
        return False
    document["md_content"] = new
    return True


def truthy(value: Any) -> bool:
    """Form fields arrive as strings ("true", "True", "1"); JSON bodies as bools."""
    if isinstance(value, list):
        value = value[-1] if value else None
    return str(value).strip().lower() in {"1", "true", "yes", "on"}
