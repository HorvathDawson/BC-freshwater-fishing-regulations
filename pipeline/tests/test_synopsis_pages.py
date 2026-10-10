"""The printed -> PDF page map: one map, read off the PDF's footers, and every link uses it.

An entry's `source_pages` (the bundle's and the export's `pages`) are PRINTED page numbers; a link
`fishing_synopsis.pdf#page=N` takes the PDF's page index, 2 to 6 ahead. The design mock linked the
printed number and opened pages early. These tests prove the map against the book itself — every
cited page, looked up in the map, is a PDF page whose text holds the entry's row — and that the
map is the one the export ships, the curation app serves and the mock carries.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from pipeline.common import synopsis_pages as SP

ROOT = Path(__file__).resolve().parents[2]
CATALOGUE = ROOT / "data" / "curated" / "regulations" / "entries" / "catalogue"
MOCK = ROOT / "app" / "design" / "regs-v3.html"

# The synopsis PDF is tracked in git: a missing PDF fails (`test_the_pdf_is_there`), never skips.


def test_the_pdf_is_there():
    assert SP.default_pdf().exists(), f"{SP.default_pdf()} is tracked in git and missing"


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


@pytest.fixture(scope="module")
def page_map() -> dict[int, int]:
    return SP.printed_to_pdf()


@pytest.fixture(scope="module")
def pdf_text() -> dict[int, str]:
    """1-based PDF page -> its text, letters and digits only (pdfium: the content stream's
    order, which keeps a column's sentence whole where a layout read interleaves columns)."""
    import pypdfium2 as pdfium
    doc = pdfium.PdfDocument(str(SP.default_pdf()))
    try:
        return {i + 1: _norm(doc[i].get_textpage().get_text_range()) for i in range(len(doc))}
    finally:
        doc.close()


@pytest.fixture(scope="module")
def entries() -> list[dict]:
    out = []
    for f in sorted(CATALOGUE.glob("region-*.json")):
        out += json.loads(f.read_text(encoding="utf-8"))["entries"]
    return out


def _row_marks(e: dict) -> list[str]:
    """What identifies the entry's row on a page: its printed name (whole, and without its
    parenthesis) and every clause of 14+ letters of its verbatim and its rules' verbatims."""
    marks = [n for n in (_norm(e.get("name")), _norm(re.sub(r"\(.*", "", e.get("name") or "")))
             if len(n) >= 6]
    texts = [e.get("regs_verbatim") or ""] + [r.get("verbatim") or "" for r in e.get("rules", [])]
    for t in texts:
        for c in re.split(r"[.;:\n()\[\]]|, | - | — ", t.replace("*", "")):
            n = _norm(c)
            if len(n) >= 14 and n not in marks:
                marks.append(n)
    return marks


def misplaced(entries: list[dict], page_map: dict[int, int], text: dict[int, str]) -> list:
    """(entry_id, printed page) for every cited page whose PDF page (by `page_map`) does not hold
    the entry's row."""
    bad = []
    for e in entries:
        marks = _row_marks(e)
        for p in e.get("source_pages") or []:
            t = text.get(page_map.get(p), "")
            if not any(m in t for m in marks):
                bad.append((e["entry_id"], p))
    return bad


def test_the_map_is_read_off_the_footers(page_map):
    # printed = PDF - 2 up to printed 40; four unnumbered centre pages (PDF 43-46); then PDF - 6
    assert (page_map[1], page_map[9], page_map[14], page_map[40]) == (3, 11, 16, 42)
    assert (page_map[41], page_map[42], page_map[58], page_map[80]) == (47, 48, 64, 86)
    assert list(page_map) == list(range(1, 81))
    assert set(page_map.values()).isdisjoint({43, 44, 45, 46})


def test_every_cited_page_is_a_pdf_page_holding_the_row(entries, page_map, pdf_text):
    cited = sum(len(e.get("source_pages") or []) for e in entries)
    assert cited > 1500                         # the whole catalogue, not a sample
    assert misplaced(entries, page_map, pdf_text) == []


@pytest.mark.parametrize("wrong", [
    lambda m: {k: k for k in m},                # the old bug: link the printed number
    lambda m: {k: v + 1 for k, v in m.items()},  # one page late
])
def test_a_wrong_map_is_caught(entries, page_map, pdf_text, wrong):
    """MUTATION PIN: the check above is not a tautology — linking the printed number (what the
    design mock did) or landing a page late puts hundreds of rows on the wrong page."""
    assert len(misplaced(entries, wrong(page_map), pdf_text)) > 500


def test_superior_closures_cites_only_the_page_its_text_is_on(entries, page_map, pdf_text):
    """zp:superior_closures: every word of it is on printed p.9; p.10 does not hold it."""
    from pipeline.regs.parsing.catalogue import CatalogueEntry
    e = next(x for x in entries if x["entry_id"] == "zp:superior_closures")
    assert e["source_pages"] == [9]
    # what the bundle build writes to `entry.pages` (bundle/rules.py: list(ce.source_pages))
    assert list(CatalogueEntry.model_validate(e).source_pages) == [9]
    assert misplaced([dict(e, source_pages=[9, 10])], page_map, pdf_text) == \
        [("zp:superior_closures", 10)]


def test_one_footer_reader():
    from pipeline.regs.extraction import extract_synopsis
    assert extract_synopsis.printed_page_number is SP.printed_page_number


def test_the_export_ships_the_map_and_refuses_an_unmapped_page(page_map):
    from pipeline.tools import export_ui_rules as X
    shipped = X.pdf_pages()
    assert shipped == {str(k): v for k, v in page_map.items()}
    doc = {"guide": {"pdf_pages": shipped},
           "entries": {"a": {"pages": [9]}, "b": {"pages": [81]}}}
    assert X.pdf_page_problems(doc) == [
        "entry b cites printed page 81, which guide.pdf_pages does not map"]
    assert X.pdf_page_problems({"guide": {}, "entries": {}})        # an empty map is refused


def test_the_design_mock_carries_the_shipped_map_and_links_through_it(page_map):
    html = MOCK.read_text(encoding="utf-8")
    got = re.search(r"var PDF_PAGES=(\{.*?\});", html)
    assert got, "regs-v3.html must carry PDF_PAGES"
    assert json.loads(got.group(1)) == {str(k): v for k, v in page_map.items()}
    # every #page= the mock builds takes the looked-up PDF page, never the printed one
    links = re.findall(r'"#page="\+(\w+)', html)
    assert links and set(links) == {"pdfPg"}
