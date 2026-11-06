"""T426: the extractor change gate — `extract_synopsis --check-against` (`pipeline.regs.extraction.baseline`).

No PDF except `test_hard_pages_read_as_the_baseline` (`needs_source`): the comparison on synthetic rows, and the
committed baseline + accepted diffs agreeing with each other.
"""
import json

import pytest

from pipeline.regs.extraction import baseline as B
from pipeline.tests.conftest import need


def _row(image="page_018_row_000.png", water="A LAKE", regs="Bait ban", page=16, pdf_page=18, symbols=()):
    return {"water": water, "mu": ["1-1"], "raw_regs": regs, "symbols": list(symbols), "page": page,
            "image": image, "region": "REGION 1 - Vancouver Island", "pdf_page": pdf_page}


def _manifest(rows):
    out = {}
    for box, r in rows:
        out[B.row_key(r["pdf_page"], box)] = {"image": r["image"], "water": r["water"], "sha": B.row_sha(r),
                                              "fields": B.field_shas(r)}
    return {"rows": out}


def _got(rows):
    return [{"key": B.row_key(r["pdf_page"], box), "box": box, "row": r} for box, r in rows]


BOX_A, BOX_B = [36, 86.88, 540, 96.48], [36, 96.48, 540, 107.04]
A, Bw = _row(), _row(image="page_018_row_001.png", water="B RIVER", regs="**No Fishing**")


def test_the_same_reading_is_clean():
    res = B.compare(_manifest([(BOX_A, A), (BOX_B, Bw)]), _got([(BOX_A, A), (BOX_B, Bw)]), {"accepted": []})
    assert res["unexplained"] == res["explained"] == res["stale"] == [] and res["compared"] == 2


def test_symbol_order_is_not_a_change():
    m = _manifest([(BOX_A, _row(symbols=["Stocked", "Incl. Tribs"]))])
    assert B.compare(m, _got([(BOX_A, _row(symbols=["Incl. Tribs", "Stocked"]))]), {})["unexplained"] == []


def test_a_changed_row_is_named_with_its_field():
    m = _manifest([(BOX_A, A), (BOX_B, Bw)])
    res = B.compare(m, _got([(BOX_A, A), (BOX_B, dict(Bw, raw_regs="**No Fishing**\n"))]), {})
    (d,) = res["unexplained"]
    assert (d["key"], d["kind"], d["image"], d["water"], d["fields"]) == \
        ("018:36.00,96.48,540.00,107.04", "changed", "page_018_row_001.png", "B RIVER", ["raw_regs"])


def test_whitespace_is_a_change():
    m = _manifest([(BOX_A, _row(regs="when open"))])
    assert B.compare(m, _got([(BOX_A, _row(regs="when\nopen"))]), {})["unexplained"][0]["fields"] == ["raw_regs"]


def test_gone_and_new_rows_are_named():
    moved = [36, 96.50, 540, 107.04]                    # the row box moved: gone at one key, new at another
    res = B.compare(_manifest([(BOX_A, A), (BOX_B, Bw)]), _got([(BOX_A, A), (moved, Bw)]), {})
    assert [(d["kind"], d["water"]) for d in res["unexplained"]] == [("gone", "B RIVER"), ("new", "B RIVER")]


def test_an_accepted_diff_explains_exactly_its_move():
    m = _manifest([(BOX_A, A)])
    new = dict(A, raw_regs="Bait ban\nJan 1")
    acc = {"accepted": [{"key": B.row_key(18, BOX_A), "was": B.row_sha(A), "now": B.row_sha(new), "reason": "x"}]}
    res = B.compare(m, _got([(BOX_A, new)]), acc)
    assert res["unexplained"] == [] and res["explained"][0]["reason"] == "x"
    again = dict(A, raw_regs="Bait ban\nJan 2")          # the same row changing again is unexplained again
    res = B.compare(m, _got([(BOX_A, again)]), acc)
    assert len(res["unexplained"]) == 1 and len(res["stale"]) == 1


def test_an_accepted_diff_no_longer_seen_is_stale():
    m = _manifest([(BOX_A, A)])
    acc = {"accepted": [{"key": B.row_key(18, BOX_A), "was": B.row_sha(A), "now": "f" * 16, "reason": "x"}]}
    assert len(B.compare(m, _got([(BOX_A, A)]), acc)["stale"]) == 1
    # out of a single-page check's scope it is not judged
    assert B.compare(m, [], acc, pages=[19])["stale"] == [] and B.compare(m, [], acc, pages=[19])["compared"] == 0


def test_a_page_check_compares_only_its_pages():
    other = _row(image="page_019_row_000.png", pdf_page=19, page=17)
    res = B.compare(_manifest([(BOX_A, A), (BOX_A, other)]), _got([(BOX_A, A)]), {}, pages=[18])
    assert res["unexplained"] == [] and res["compared"] == 1


def test_the_committed_baseline_and_accepted_diffs_agree():
    m = json.loads(B.DEFAULT_MANIFEST.read_text(encoding="utf-8"))
    assert m["pdf_sha256"] == "08f58537261ffe51c513193013ba165754285719dec8d0332beaef1cbab8ecfa"
    assert m["rows_count"] == len(m["rows"]) == sum(m["pages"].values()) == 1393 and len(m["pages"]) == 39
    acc = B.load_accepted(B.DEFAULT_ACCEPTED, m)          # same baseline, every entry has a reason
    for a in acc["accepted"]:
        assert a["was"] is None or m["rows"][a["key"]]["sha"] == a["was"], a["key"]
        assert a["was"] is None or m["rows"][a["key"]]["image"] == a["image"], a["key"]
    # T415's Heber fix is the first change through the gate
    assert [a["water"] for a in acc["accepted"]][:1] == ["HEBER RIVER"]


def test_accepted_diffs_for_another_baseline_are_refused(tmp_path):
    m = json.loads(B.DEFAULT_MANIFEST.read_text(encoding="utf-8"))
    p = tmp_path / "a.json"
    p.write_text(json.dumps({"baseline_rows": "0" * 16, "accepted": []}))
    with pytest.raises(SystemExit, match="another baseline"):
        B.load_accepted(p, m)
    p.write_text(json.dumps({"baseline_rows": B.rows_digest(m), "accepted": [{"key": "k", "reason": " "}]}))
    with pytest.raises(SystemExit, match="no reason"):
        B.load_accepted(p, m)


#: the hard pages, re-read from the PDF: Heber (18), the Chilliwack/Coquihalla (24), Shuswap (34), Columbia (37),
#: the Dean (50), the Babine (56), 7A (65), 7B (71), Region 8 (75); the sample table (6, skipped) and the four
#: unnumbered centre pages (43-46, no rows) — about 6 s
HARD_PAGES = [6, 18, 24, 34, 37, 43, 44, 45, 46, 50, 56, 65, 71, 75]


@pytest.mark.needs_source
def test_hard_pages_read_as_the_baseline(request):
    from pipeline.common.synopsis_pages import default_pdf
    need(request, "source", default_pdf(), "the repo copy of the synopsis PDF (runbook step 1)")
    m = json.loads(B.DEFAULT_MANIFEST.read_text(encoding="utf-8"))
    pdf = B.book_pdf(m)                                   # the baseline's own book, by sha
    res = B.compare(m, B.extract(pdf, HARD_PAGES, workers=2), B.load_accepted(B.DEFAULT_ACCEPTED, m), HARD_PAGES)
    assert res["unexplained"] == [] and res["stale"] == [], res
    assert res["compared"] == sum(m["pages"].get(str(p), 0) for p in HARD_PAGES) == 331
