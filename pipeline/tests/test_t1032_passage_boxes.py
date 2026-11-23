"""T1032 — a region's or the province's rule opens its synopsis page with its passage highlighted: the words located in
the repo PDF (tracked in git, so these never skip), exactly once, on word bounds; anything else gets NO box, with why.
Pages: printed 8 / 9 / 10 (PDF 10 / 11 / 12, provincial) and 49 (PDF 55, Region 6 - Skeena)."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from pipeline.common.synopsis_pages import default_pdf
from pipeline.deliver.answers.display import decided as DEC
from pipeline.regs.extraction import passage_boxes as PB

SIZE = [576.0, 756.0]
BARBLESS = "Single barbless hook: must be used in all streams of Region 6, all year."
BAIT_BAN = "Bait ban: In the Skeena River, including tributaries, and Nass River, including tributaries, year-round."


@pytest.fixture(scope="module")
def book() -> PB.Book:
    return PB.Book(default_pdf())


def _box(pdf, *b):
    return {"pdf": pdf, "box": list(b), "size": SIZE}


@pytest.mark.parametrize("pdf, text, boxes", [
    # printed p.8, "It Is Unlawful To…": a passage across three lines of the middle column
    (10, "Snag (foul hook) fish (see definition, page 80).", [_box(10, 215.0, 250.75, 349.67, 275.25)]),
    # printed p.9, the quota definitions
    (11, "Quotas are not cumulative: you cannot possess a quota from each region that you fish.",
     [_box(11, 34.0, 188.81, 187.53, 224.31)]),
    # printed p.10, Keeping and Transporting Your Catch
    (12, "Leave the head, tail, and all fins on your catch until you get them to your permanent (ordinary) residence.",
     [_box(12, 43.0, 326.27, 178.18, 361.63)]),
    # printed p.49, Region 6 General Regulations
    (55, BARBLESS, [_box(55, 37.08, 451.91, 188.89, 476.41)]),
    # printed p.49: starting mid-line ("or slime. If you capture…"), the passage is two boxes, never the line's head
    (55, "If you capture a tagged fish, do not allow the tag to influence your decision to kill or release the fish.",
     [_box(55, 412.82, 292.85, 539.57, 306.35), _box(55, 380.76, 303.85, 531.75, 328.35)]),
])
def test_a_passage_is_boxed_where_it_is_printed(book, pdf, text, boxes):
    assert PB.locate(book.page(pdf), text) == ("ok", boxes)


@pytest.mark.parametrize("pdf, text, why", [
    (55, "Skeena River", PB.AMBIGUOUS),                                       # printed many times on p.49: which one?
    (55, "Trout", PB.SHORT),                                                  # under MIN_CHARS
    (55, BARBLESS[1:], PB.ABSENT),                                            # "ingle barbless…": starts inside a word
    (55, BARBLESS[:-3], PB.ABSENT),                                           # "…all ye": ends inside a word
    # the curated wording, not the book's ("Set lining: is only permitted…"): never a nearest match
    (55, "Set lining: only permitted in the lakes of Region 6.", PB.ABSENT),
    (10, BARBLESS, PB.ABSENT),                                                # Region 6's rule is not on printed p.8
])
def test_a_passage_not_printed_exactly_once_gets_no_box(book, pdf, text, why):
    assert PB.locate(book.page(pdf), text) == (why, [])


def test_the_rule_text_first_then_its_entrys(book):
    entry = BAIT_BAN + " " + "No fishing: in all rivers and streams for steelhead, May 15-June 15."
    by, boxes, why = PB.rule_boxes(book, [(49, 55)], BAIT_BAN, entry)
    assert (by, why) == (PB.BY_RULE, {}) and boxes == PB.locate(book.page(55), BAIT_BAN)[1]
    # the rule's own words paraphrase the book: its entry's printed words, which are the book's, place it
    by, boxes, why = PB.rule_boxes(book, [(49, 55)], "Bait ban: Skeena and Nass, year-round.", BAIT_BAN)
    assert (by, why) == (PB.BY_ENTRY, {49: PB.ABSENT}) and boxes == PB.locate(book.page(55), BAIT_BAN)[1]
    # neither: no box, and why
    assert PB.rule_boxes(book, [(8, 10), (49, 55)], "Set lining: only permitted", "Set lining: only permitted") == \
        (None, [], {8: PB.ABSENT, 49: PB.ABSENT})


def _doc(kind, verbatim, printed, pages=(49,)):
    return {"guide": {"pdf_pages": {"8": 10, "49": 55}},
            "entries": {"e": {"kind": kind, "full_name": "Region 6", "pages": list(pages), "printed": printed}},
            "rules": {}}


def test_book_boxes_boxes_a_zone_rule_and_leaves_a_water_to_its_row(monkeypatch):
    monkeypatch.setattr(DEC, "_row_boxes", lambda: {})
    monkeypatch.setattr(DEC, "_BOXES", {})
    x = {"entry_id": "e", "verbatim": BARBLESS}
    want = [_box(55, 37.08, 451.91, 188.89, 476.41)]
    for kind in ("zone", "province", "area"):
        assert DEC.book_boxes(x, _doc(kind, BARBLESS, BARBLESS)) == want
    assert DEC.book_boxes(x, _doc("water", BARBLESS, BARBLESS)) == []      # a water: its table row or nothing
    assert DEC.book_boxes(x, _doc("zone", BARBLESS, BARBLESS, pages=(8,))) == []   # not on its page: no box


def test_book_boxes_refuses_a_page_the_guide_cannot_open(monkeypatch):
    from pipeline.deliver.answers.common import AnswersError
    monkeypatch.setattr(DEC, "_row_boxes", lambda: {})
    monkeypatch.setattr(DEC, "_BOXES", {})
    with pytest.raises(AnswersError, match="not in the guide's pdf_pages"):
        DEC.book_boxes({"entry_id": "e", "verbatim": BARBLESS}, _doc("zone", BARBLESS, BARBLESS, pages=(50,)))


def test_the_report_counts_boxed_rules_per_page_and_lists_the_rest(book):
    doc = {"guide": {"pdf_pages": {"8": 10, "49": 55, "53": 59}},
           "entries": {"z": {"kind": "zone", "pages": [8, 49], "printed": BARBLESS},
                       "y": {"kind": "zone", "pages": [49], "printed": BAIT_BAN},
                       "p": {"kind": "province", "pages": [8], "printed": "Set lining: only permitted"},
                       "w": {"kind": "water", "pages": [53], "printed": "row"}},
           "rules": {"z::r1": {"entry_id": "z", "verbatim": BARBLESS},
                     "p::r1": {"entry_id": "p", "verbatim": "Set lining: only permitted"},
                     "w::r1": {"entry_id": "w", "verbatim": "row"},
                     "y::r1": {"entry_id": "y", "verbatim": "Bait ban: Skeena and Nass, year-round."},   # paraphrased
                     "z::r2": {"entry_id": "z", "verbatim": BARBLESS}},       # in no water's rule set: not counted
           "rulesets": {"1": {"sections": [1], "reach": ["z::r1", "p::r1", "w::r1", "y::r1"]}}}
    rep = PB.report(doc, book, lambda x, d: [_box(59, 1.0, 2.0, 3.0, 4.0)])
    assert rep["pages"] == {"8": {"rules": 2, "boxed": 0, "water_rules": 0, "water_boxed": 0},   # z is on p.49 only
                            "49": {"rules": 2, "boxed": 2, "water_rules": 0, "water_boxed": 0},
                            "53": {"rules": 0, "boxed": 0, "water_rules": 1, "water_boxed": 1}}
    assert rep["placed_by"] == {"rule": 1, "entry": 1, "row": 1}
    # placed by its entry's whole passage, not its own words: listed, so a whole-panel box is never silent
    assert [(u["rule"], u["boxes"], u["why"]) for u in rep["by_entry"]] == [("y::r1", 1, {"49": PB.ABSENT})]
    assert [(u["rule"], u["why"]) for u in rep["unlocated"]] == [("p::r1", {"8": PB.ABSENT})]


def test_the_cli_runs_bare_on_the_generated_export(monkeypatch):
    # #67 review: `--export-dir` defaulted to GENERATED.regs, a namespace, and the bare documented command crashed
    from pipeline.deliver.answers import common
    from pipeline.tests import under_test
    got = []

    class Stop(Exception):
        pass

    def load(d):
        got.append(d)
        raise Stop

    monkeypatch.setattr(common, "load_export", load)
    with pytest.raises(Stop):
        PB.main([])
    assert got == [under_test.generated().regs.extraction.parent] and isinstance(got[0], Path)
    assert (got[0] / "ui-rules-export.json").name == "ui-rules-export.json"


def test_the_gate_holds_the_not_located_rules_to_the_pinned_list(tmp_path, monkeypatch):
    pdf = tmp_path / "s.pdf"
    pdf.write_bytes(b"%PDF one")
    pin = tmp_path / "pin.json"
    monkeypatch.setattr(PB, "PIN", pin)
    PB.write_pin(["z::r2", "z::r1"], pdf, "a book")
    assert json.loads(pin.read_text())["books"] == {hashlib.sha256(b"%PDF one").hexdigest():
                                                     {"about": "a book", "unlocated": ["z::r1", "z::r2"]}}
    held = ["z::r1", "z::r2", "z::r3"]
    PB.hold_to_pin(["z::r1", "z::r2"], held, pdf)                            # exactly the pin: passes
    with pytest.raises(PB.PassageGateError, match=r"NOT LOCATED now \(no box\): z::r3") as e:
        PB.hold_to_pin(["z::r1", "z::r2", "z::r3"], held, pdf)                    # a rule lost its box
    assert "take them off" not in str(e.value)
    with pytest.raises(PB.PassageGateError, match="LOCATED now, take them off the pin: z::r2"):
        PB.hold_to_pin(["z::r1"], held, pdf)                                     # one is located now: the pin must follow
    # (#67 CI) an export that does not hold a pinned rule (a partial or hand-made one) is not asked about it
    PB.hold_to_pin(["z::r1"], ["z::r1", "z::r3"], pdf)
    PB.hold_to_pin([], [], pdf)
    pdf.write_bytes(b"%PDF two")                                             # another book: nothing pinned for it
    with pytest.raises(PB.PassageGateError, match="no pinned not-located list"):
        PB.hold_to_pin([], [], pdf)


def test_the_build_gates_every_zone_rule_given_no_box(monkeypatch, tmp_path):
    # W46: add_sources used to leave an unlocated zone rule without a box, silently
    from pipeline.deliver.answers.common import AnswersError
    monkeypatch.setattr(DEC, "_row_boxes", lambda: {})
    monkeypatch.setattr(DEC, "_BOXES", {})
    monkeypatch.setattr(DEC, "applies_here", lambda x, doc: ("A Region 6 rule.", None))
    pin = tmp_path / "pin.json"
    monkeypatch.setattr(PB, "PIN", pin)
    doc = {"guide": {"pdf_pages": {"49": 55}}, "rulesets": {},
           "entries": {"z": {"kind": "zone", "pages": [49], "printed": "Set lining: only permitted"}},
           "rules": {"z::r1": {"id": "z::r1", "entry_id": "z", "verbatim": BARBLESS},
                     "z::r2": {"id": "z::r2", "entry_id": "z", "verbatim": "Set lining: only permitted"}}}
    PB.write_pin([], default_pdf(), "nothing")
    facts = [{}, {}]
    with pytest.raises(AnswersError, match=r"NOT LOCATED now \(no box\): z::r2$|NOT LOCATED now \(no box\): z::r2 "):
        DEC.add_sources(facts, doc, ["z::r1", "z::r2"])
    assert DEC.unlocated_passages(doc) == ["z::r2"]
    PB.write_pin(["z::r2"], default_pdf(), "z::r2 pinned")
    facts = [{}, {}]
    DEC.add_sources(facts, doc, ["z::r1", "z::r2"])
    assert facts[0]["book_box"] == [_box(55, 37.08, 451.91, 188.89, 476.41)] and "book_box" not in facts[1]
