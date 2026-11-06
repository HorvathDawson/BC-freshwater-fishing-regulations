"""ONE SECTION-ID PARSER (T436, audit 2026-11-05 pipeline #18). `{blk}:{measure}` was split in the bundle's
`section_measure.stream_head` ((int, int)) and `continuation.line_of` ((str, int)) by two hand-written parsers that only
happened to agree. Both now call `section_handles.parse_section_id`."""
from __future__ import annotations

from pathlib import Path

from pipeline.common.section_handles import parse_section_id
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
        assert line_of(i) == (None if p is None else (i.rpartition(":")[0], p[1])), i


def test_no_second_parser_in_the_bundle_helpers():
    for f in ("pipeline/deliver/bundle/section_measure.py", "pipeline/deliver/bundle/continuation.py"):
        src = (ROOT / f).read_text(encoding="utf-8")
        assert "parse_section_id(" in src, f
        assert 'rpartition(":")\n' not in src and "int(tail)" not in src, f"{f} parses a section id itself"
