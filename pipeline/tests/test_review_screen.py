"""Curation review phase 3 (T134): what the review screen decides, each in one function.

    coverage     the strip: which words of the row no rule quotes — found by THE comparison
                 normaliser (`squash`), mapped back to the characters a reviewer reads
    tiers        PLAN §3's tiers (S0 P1 Z P2 P3 P4 sweep) and the S0 split test
    row boxes    the one-off helper's check that a box is the extraction's row (no credits)
    evidence     a zone passage found on its printed page by text; the Adam River row legible

Reads the curated catalogue and splits.json, and the repo PDF; no generated data except where a
test is marked. Whole values throughout; each key behaviour carries a mutation pin that names the
test the mutant fails.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from pipeline.common.curated import CURATED
from pipeline.regs.parsing.catalogue import clean_verbatim, squash
from pipeline.regs.review import coverage as COV
from pipeline.regs.review import tiers as T

REPO = Path(__file__).resolve().parents[2]


def _corpus() -> list[dict]:
    out = []
    for p in sorted(CURATED.regulations.entries.catalogue.glob("region-*.json")):
        out += json.loads(p.read_text(encoding="utf-8"))["entries"]
    return out


# --------------------------------------------------------------------------- coverage

def test_the_strip_marks_what_no_rule_quotes():
    got = COV.coverage("**No Fishing** upstream of the falls, Dec 1-May 31; bait ban\nTrout quota = 2",
                       [("a.r1", "No Fishing upstream of the falls, Dec 1–May 31"),
                        ("a.r2", "Trout quota = 2"),
                        ("a.r3", "fly fishing only")])
    assert got == {
        "segments": [{"text": "No Fishing upstream of the falls, Dec 1-May 31", "kind": "quoted"},
                     {"text": "; ", "kind": "gap"},
                     {"text": "bait ban", "kind": "unquoted"},
                     {"text": "\n", "kind": "gap"},
                     {"text": "Trout quota = 2", "kind": "quoted"}],
        "unquoted_chars": 7,
        "missing": ["a.r3"]}


def test_the_adam_river_row_is_quoted_whole():
    adam = next(e for e in _corpus() if e["entry_id"] == "r1:adam_river_except_eve_river@1-10")
    assert COV.entry_coverage(adam) == {
        "segments": [{"text": "Artificial fly only upstream of Eve River, to Hwy 19 bridge",
                      "kind": "quoted"}],
        "unquoted_chars": 0, "missing": []}


def test_the_fold_is_squash_over_every_row_of_the_corpus():
    """The strip finds a quote exactly where the gate does: `_folded` IS `squash`, character for
    character, over every row's text — so the two cannot drift."""
    texts = [e.get("regs_verbatim") or "" for e in _corpus()]
    assert len(texts) > 1000
    assert COV.folded_matches_squash(texts) == []


def test_the_fold_check_is_pinned_by_mutation(monkeypatch):
    """MUTATION: a fold that forgets squash's lower-casing disagrees with squash on the corpus —
    `test_the_fold_is_squash_over_every_row_of_the_corpus` fails on it."""
    real = COV._folded

    def keeps_case(text):
        folded, idx = real(text)
        return "".join(text[i] if text[i].isalpha() else c for c, i in zip(folded, idx)), idx

    monkeypatch.setattr(COV, "_folded", keeps_case)
    assert COV.folded_matches_squash([e.get("regs_verbatim") or "" for e in _corpus()]) != []


def test_the_fold_maps_each_character_back():
    text = clean_verbatim("**Bait** ban —  single   barbless\n> hook")
    folded, idx = COV._folded(text)
    assert folded == squash(text) == "bait ban - single barbless hook"
    # each folded character comes from the character a reviewer reads in that place: the same
    # letter, the dash it normalised, or (a space) a run of white space or a line's marker
    assert all(f == text[i].lower() or (f == "-" and text[i] == "—")
               or (f == " " and (text[i].isspace() or text[i] == ">"))
               for f, i in zip(folded, idx))
    assert sorted(idx) == idx and len(idx) == len(folded)


# --------------------------------------------------------------------------- tiers

def _e(eid, rules=None, matched=("gnis:1",), **kw):
    return {"entry_id": eid, "matched": list(matched), "rules": rules or [], **kw}


CLOSE = {"rule_id": "x.r1", "type": "retention_limit", "take": 0, "may_target": 0,
         "extents": [{"op": "whole"}]}
QUOTA = {"rule_id": "x.r2", "type": "retention_limit", "take": 2, "extents": [{"op": "whole"}]}
CUT = {"op": "between", "splits": ["a", "b"]}


def test_each_tier_whole():
    got = {
        "zone": T.entry_tier(_e("z1:x", [QUOTA], matched=()), binds=False),
        "pointer": T.entry_tier(_e("r1:a@1-1", see=[{"entries": ["r1:b@1-1"]}]), binds=True),
        "closure + cut": T.entry_tier(_e("r1:a@1-1", [dict(CLOSE, extents=[CUT])]), binds=True),
        "closure, no match": T.entry_tier(_e("r1:a@1-1", [CLOSE], matched=()), binds=False),
        "plain closure": T.entry_tier(_e("r1:a@1-1", [CLOSE]), binds=True),
        "quota, two waters": T.entry_tier(_e("r1:a@1-1", [QUOTA], matched=("gnis:1", "gnis:2")), binds=True),
        "quota, flagged": T.entry_tier(_e("r1:a@1-1", [dict(QUOTA, review_reason="?")]), binds=True),
        "plain quota": T.entry_tier(_e("r1:a@1-1", [QUOTA]), binds=True),
    }
    assert got == {"zone": "Z", "pointer": "sweep", "closure + cut": "P1", "closure, no match": "P1",
                   "plain closure": "P2", "quota, two waters": "P3", "quota, flagged": "P3",
                   "plain quota": "P4"}
    assert T.risks(_e("r1:a@1-1", [dict(CLOSE, extents=[CUT], unresolved_locators=["x"])],
                      matched=("gnis:1", "gnis:2")), binds=False) == [
        "cut", "several waters", "flag", "no match"]


def test_a_closure_is_the_one_closure_predicate():
    """A closure with a condition ("partial") is a closure; a release you may still fish for is not
    — `rules.closure_grade`, never a second reading."""
    partial = dict(CLOSE, undrawn_part="near the dam")
    release = {"rule_id": "x.r3", "type": "retention_limit", "take": 0, "may_target": 1,
               "extents": [{"op": "whole"}]}
    assert [T.entry_tier(_e("r1:a@1-1", [r]), binds=True) for r in (partial, release)] == ["P2", "P4"]


def test_s0_is_an_open_auto_proposal_or_a_verify_note():
    got = [T.split_needs_verify({"_note": n}) for n in (
        "[auto-proposal L] Hwy 19 bridge …", "Verify: confirm the bridge", "[human-review] done",
        "[osm-candidate] (VERIFY). [human-review 2026-08-06]", "", None)]
    assert got == [True, True, False, False, False, False]
    assert [T.split_grade({"_note": n}) for n in ("[auto-proposal M] x", "y [auto-proposal L]")] \
        == ["M", None]


def test_the_corpus_tiers_match_plan_section_3():
    """PLAN §3 counted P1 227, Z 132, P2 261, P3 116, P4 730, sweep 59 and S0 165 (120 L) on
    2026-10-22. Measured here from the corpus alone (no match = an empty `matched`; the served
    build decides it in the tool): within the day's drift, and S0 is the 150 notes that still
    ask for a check (PLAN counted 15 more whose "VERIFY" was answered by a later human review)."""
    from collections import Counter
    got = Counter(T.entry_tier(e, binds=bool(e.get("matched"))) for e in _corpus())
    assert got["Z"] == 132
    assert sum(got.values()) == 1525
    for tier, plan in {"P1": 227, "P2": 261, "P3": 116, "P4": 730, "sweep": 59}.items():
        assert abs(got[tier] - plan) <= 3, (tier, got[tier], plan)
    splits = [s for wb in json.loads(CURATED.waters.splits.read_text())["waterbodies"]
              for s in wb["splits"]]
    s0 = [s for s in splits if T.split_needs_verify(s)]
    assert (len(splits), len(s0), Counter(T.split_grade(s) for s in s0)["L"]) == (409, 150, 119)


def _py_sources():
    for p in sorted((REPO / "pipeline").rglob("*.py")):
        yield p, p.read_text(encoding="utf-8")
    for p in sorted((REPO / "curation-review" / "backend").glob("*.py")):
        yield p, p.read_text(encoding="utf-8")


def test_no_second_copy_of_the_tier_the_grade_the_flag_the_strip_or_the_verdict():
    hits: dict[str, list] = {k: [] for k in (
        "def entry_tier(", "def split_grade(", "def split_needs_verify(", "def flagged(",
        r"\[auto-proposal ([A-Z])\]", "def coverage(", "def verdict_from_answers(", "def find_passage(",
        "def app_part(")}
    for p, text in _py_sources():
        if p.name.startswith("test_"):
            continue
        for k in hits:
            if k in text:
                hits[k].append(str(p.relative_to(REPO)))
    assert hits == {
        "def entry_tier(": ["pipeline/regs/review/tiers.py"],
        "def split_grade(": ["pipeline/regs/review/tiers.py"],
        "def split_needs_verify(": ["pipeline/regs/review/tiers.py"],
        "def flagged(": ["pipeline/regs/review/tiers.py"],
        r"\[auto-proposal ([A-Z])\]": ["pipeline/regs/review/tiers.py"],
        "def coverage(": ["pipeline/regs/review/coverage.py"],
        "def verdict_from_answers(": ["pipeline/regs/review/marks.py"],
        "def find_passage(": ["curation-review/backend/evidence.py"],
        "def app_part(": ["curation-review/backend/answer.py"]}


# --------------------------------------------------------------------------- the row boxes

def test_a_box_from_another_row_is_refused_and_glyph_order_is_only_reported():
    from pipeline.regs.extraction.row_boxes import check_against
    rows = [{"image": "page_016_row_000.png", "water": "ADAM RIVER (except Eve River)",
             "raw_regs": "Artificial fly only"},
            {"image": "page_016_row_001.png", "water": "AHNUHATI RIVER", "raw_regs": "Class II"},
            {"image": "page_018_row_021.png", "water": "HEBER RIVER", "raw_regs": "Fly fishing"},
            {"image": "page_099_row_000.png", "water": "GONE", "raw_regs": ""}]
    boxes = {"page_016_row_000.png": {"water": "ADAM RIVER (except Eve River)",
                                      "raw_regs": "Artificial fly only"},
             "page_016_row_001.png": {"water": "ALICE LAKE", "raw_regs": "Class II"},
             "page_018_row_021.png": {"water": "HEBER RIVER", "raw_regs": "Folpye fnishing"}}
    assert check_against(rows, boxes) == (
        ["page_016_row_001.png: the extractor now reads 'ALICE LAKE' here, the extraction "
         "recorded 'AHNUHATI RIVER'",
         "page_099_row_000.png: no box (the extractor no longer yields this row)"],
        ["page_018_row_021.png"])


@pytest.mark.slow
def test_the_helper_reads_the_adam_river_row_where_the_extractor_cropped_it():
    """The helper runs the extractor itself on page 16 (no credits; ~10 s): the Adam River row's box
    and column divider, in PDF points — the crop the extractor saved is this box at 400 dpi
    (2855 x 109 px; the box is kept to 0.01 pt, so a pixel either way)."""
    from pipeline.common.synopsis_pages import default_pdf
    from pipeline.regs.extraction.row_boxes import _page_boxes
    got = {b["image"]: b for b in _page_boxes((str(default_pdf()), 16))}
    adam = got["page_016_row_000.png"]
    assert {k: adam[k] for k in ("pdf_page", "printed", "box", "div_x", "page_size", "water")} == {
        "pdf_page": 16, "printed": 14, "box": [36.14, 86.88, 539.94, 96.48], "div_x": 173.16,
        "page_size": [576.0, 756.0], "water": "ADAM RIVER (except Eve River)"}
    x0, y0, x1, y1 = adam["box"]
    s = 400 / 72
    w, h = (x1 - x0 + 10) * s, (y1 - y0 + 10) * s
    assert abs(w - 2855) <= 1 and abs(h - 109) <= 1, (w, h)
