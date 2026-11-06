"""ONE SECTION-ID PARSER (T436, audit 2026-11-05 pipeline #18). `{blk}:{measure}` was split in the bundle's
`section_measure.stream_head` ((int, int)) and `continuation.line_of` ((str, int)) by two hand-written parsers that only
happened to agree. Both now go through `section_handles.split_section_id` (the key as written; `parse_section_id` is it
with the key as an int). A named key (`W:3`, the continuation tests' graphs) is a line to `line_of`, not a blue line."""
from __future__ import annotations

from pathlib import Path

from pipeline.common.section_handles import parse_section_id, split_section_id
from pipeline.deliver.bundle.continuation import line_of
from pipeline.deliver.bundle.section_measure import stream_head

ROOT = Path(__file__).resolve().parents[2]
IDS = ["380887781:0", "380887781:122095", "-12:40", "lake:356308001", "lake:", ":5", "380887781:x", "nocolon"]


def test_the_parser():
    assert parse_section_id("380887781:9942") == (380887781, 9942)
    assert parse_section_id("-12:40") == (-12, 40)           # a minted blue line
    assert [parse_section_id(i) for i in ("lake:356308001", "lake:", ":5", "380887781:x", "nocolon")] == [None] * 5


def test_both_bundle_helpers_answer_as_the_parser():
    for i in IDS:
        p = parse_section_id(i)
        assert stream_head(i) == p, i
        assert line_of(i) == (None if p is None else (i.rpartition(":")[0], p[1])) == split_section_id(i), i
    # a named line: continuation reads it, the numeric parser does not
    assert line_of("W:3") == split_section_id("W:3") == ("W", 3) and parse_section_id("W:3") is None


def test_no_second_parser_in_the_bundle_helpers():
    for f in ("pipeline/deliver/bundle/section_measure.py", "pipeline/deliver/bundle/continuation.py"):
        src = (ROOT / f).read_text(encoding="utf-8")
        assert "parse_section_id(" in src or "split_section_id(" in src, f
        assert 'rpartition(":")\n' not in src and "int(tail)" not in src, f"{f} parses a section id itself"
