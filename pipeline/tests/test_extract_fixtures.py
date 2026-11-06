"""T426 fast tier: hard synopsis rows replayed from their characters, with no PDF (every tier, seconds).

`fixtures/extract_rows.json.gz` holds 13 rows' two cells as pdfplumber reads them after cleaning (written by
`python -m pipeline.regs.extraction.baseline fixture`), one row per region at least, chosen hard: Heber River's tight
last line (T415), the Chilliwack/Vedder and Coquihalla rows (hand-fixed in the corpus), Shuswap Lake's long name,
the Columbia's 14 lines and 5 MUs, the Kootenay's name cleaning, the Atnarko's "[Includes Tributaries] EXCEPT" name,
the Dean and the Babine, a Classified + tributaries row, and bold "**No Fishing**" with a visual Stocked symbol in
7A, 7B and Region 8. Each row is replayed through `extract_text_with_bold` and `row_from_cells` (what
`extract_rows` does after it crops a row) and must give the committed row; the row's sha is the baseline's or an
accepted diff's, so these fixtures and the change gate (`test_extract_baseline.py`) state one reading.
The whole pages are re-read from the PDF in `test_extract_baseline.py::test_hard_pages_read_as_the_baseline`.
"""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from pipeline.regs.extraction import baseline as B
from pipeline.regs.extraction import extract_synopsis as X
from pipeline.regs.extraction.models import PageMetadata, PageResult, WaterbodyRow

FIX = B.read_fixture(Path(__file__).parent / "fixtures" / "extract_rows.json.gz")
ROWS = {r["image"]: r for r in FIX["rows"]}


def _replay(r, tmp_path):
    p = X.FishingSynopsisParser(output_dir=str(tmp_path))
    left = p.extract_text_with_bold(SimpleNamespace(chars=B.unpack(r["left"], r["fonts"]))) or ""
    right = p.extract_text_with_bold(SimpleNamespace(chars=B.unpack(r["right"], r["fonts"]))) or ""
    return p.row_from_cells(left, right, list(r["visual_symbols"]))


@pytest.mark.parametrize("image", sorted(ROWS))
def test_a_hard_row_replays_from_its_characters(image, tmp_path):
    r = ROWS[image]
    assert _replay(r, tmp_path) == r["expected"]


def test_the_fixtures_read_as_the_gate_does():
    """A fixture's row is the baseline's row, or the accepted diff's new reading of it — never a third reading."""
    manifest = json.loads(B.DEFAULT_MANIFEST.read_text(encoding="utf-8"))
    accepted = json.loads(B.DEFAULT_ACCEPTED.read_text(encoding="utf-8"))
    by_image = {v["image"]: v["sha"] for v in manifest["rows"].values()}
    now = {a["image"]: a["now"] for a in accepted["accepted"]}
    assert FIX["pdf_sha256"] == manifest["pdf_sha256"]
    for image, r in ROWS.items():
        assert r["sha"] == now.get(image, by_image[image]), image


def test_one_row_per_region_at_least():
    pages = {int(i.split("_")[1]) for i in ROWS}
    # PDF pages of each region's tables, 2025-27 (the baseline's `pages`)
    regions = {"1": range(16, 22), "2": range(24, 29), "3": range(31, 35), "4": range(37, 43), "5": range(49, 54),
               "6": range(56, 61), "7A": range(65, 68), "7B": range(71, 73), "8": range(75, 78)}
    assert {k for k, rng in regions.items() if pages & set(rng)} == set(regions)


def test_heber_reads_its_last_line_as_its_own(tmp_path):
    assert ROWS["page_018_row_021.png"]["expected"]["raw_regs"].endswith("confluence when\nopen")


def test_the_fast_tier_catches_the_t415_regression(tmp_path, monkeypatch):
    """Mutation: without T415's stacked-glyph rule, Heber garbles again and its fixture fails."""
    monkeypatch.setattr(X, "_stacked_under", lambda char, line: False)
    got = _replay(ROWS["page_018_row_021.png"], tmp_path)
    assert got != ROWS["page_018_row_021.png"]["expected"]
    assert "Folpye" in got["raw_regs"]
    # the other rows do not depend on it
    for image, r in ROWS.items():
        if image != "page_018_row_021.png":
            assert _replay(r, tmp_path) == r["expected"], image


def test_the_sample_table_page_is_skipped_loudly(capsys):
    """PDF p.6 of the 2025-27 book (printed p.4, ALICE / ALOUETTE LAKE): a table on a page with no REGION
    header is the how-to-read sample, never a regulation; the extraction and the gate share `keep_page`."""
    row = WaterbodyRow(water="ALICE LAKE", mu=["2-7"], raw_regs="x", symbols=[], page=4, image="page_006_row_000.png")
    assert X.keep_page(PageResult(metadata=PageMetadata(page_number=6, region=None), rows=[row]), 6) is None
    assert "[Skipped] PDF page 6" in capsys.readouterr().err
    kept = PageResult(metadata=PageMetadata(page_number=18, region="REGION 1 - Vancouver Island"), rows=[row])
    assert X.keep_page(kept, 18) is kept
    assert X.keep_page(PageResult(metadata=PageMetadata(page_number=43, region=None), rows=[]), 43) is None
