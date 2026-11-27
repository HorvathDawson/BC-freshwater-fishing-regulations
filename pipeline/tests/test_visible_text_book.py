"""T1190: the visible-text method on the real book's hard pages (the repo copy, `needs_source`; about 10 s).

What each page hides is pinned as a count of pdfplumber characters (spaces and the footer's faux-bold doubles
included), measured 2026-11-26 when the method replaced the pixel heuristics with a whole-book check of 0
unexplained rows: PDF p.17 a stale table painted under the current one's row fills; pp.24-25 the region header drawn
twice (a white copy, then black on top); pp.65-67 a leftover Region 2 header under the blue banner. A count that moves
is a change of the method: re-run the whole-book gate (`extract_synopsis --check-against`) before re-pinning.
"""
import pdfplumber
import pytest

from pipeline.regs.extraction import visible as V
from pipeline.regs.extraction.baseline import page_rows
from pipeline.tests.conftest import need

HIDDEN = {17: 2581, 24: 27, 25: 27, 65: 25, 66: 25, 67: 25}


@pytest.fixture(scope="module")
def book(request):
    from pipeline.common.synopsis_pages import default_pdf
    need(request, "source", default_pdf(), "the repo copy of the synopsis PDF (runbook step 1)")
    return default_pdf()


@pytest.mark.needs_source
def test_hidden_characters_on_the_hard_pages(book):
    with pdfplumber.open(book) as pdf:
        got = {n: len(pdf.pages[n - 1].chars) - len(V.page_verdict(pdf.pages[n - 1]).visible) for n in HIDDEN}
    assert got == HIDDEN


@pytest.mark.needs_source
def test_the_stale_header_is_gone_and_the_real_one_reads(book):
    with pdfplumber.open(book) as pdf:
        page = pdf.pages[64]
        v = V.page_verdict(page)
        seen = "".join(c["text"] for i, c in enumerate(page.chars) if i in v.visible)
        hidden = "".join(c["text"] for i, c in enumerate(page.chars) if i not in v.visible)
    assert "REGION 7A - Omineca" in seen and "Lower Mainland" not in seen and "Lower Mainland" in hidden


@pytest.mark.needs_source
def test_nanaimo_rivers_struck_15_is_carried_marked_and_raw_regs_is_unchanged(book):
    (row,) = [r["row"] for r in page_rows((str(book), 19)) if r["row"]["water"] == "NANAIMO RIVER"]
    assert "the Hwy 19 bridge, Sept 1-Oct 15\n" in row["raw_regs"] and "~~" not in row["raw_regs"]
    assert row["raw_regs_marked"] == row["raw_regs"].replace("Sept 1-Oct 15\n", "Sept 1-Oct ~~15~~\n")
    others = [r["row"] for r in page_rows((str(book), 19)) if r["row"]["water"] != "NANAIMO RIVER"]
    assert others and not any("raw_regs_marked" in r for r in others)


@pytest.mark.needs_source
def test_the_one_glyph_the_two_readers_place_apart_is_named(book):
    """PDF p.61 (a map, no table): pdfminer puts a scale-bar '8' 47 pt left of where MuPDF and pdfium draw it."""
    with pdfplumber.open(book) as pdf:
        v = V.page_verdict(pdf.pages[60])
    assert [(t, a[0], b[0]) for _, t, a, b in v.disagreements] == [("8", 410.94, 457.52)]
    assert page_rows((str(book), 61)) == []                  # no rows: said on stderr, not refused
