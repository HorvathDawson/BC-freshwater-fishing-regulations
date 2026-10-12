"""ANSWERS 2.7 (2026-10-11, ROUND8 A): wording, book links and short part labels — no decision moves.

  A1. THE GROUP ITEMS (RULINGS Q4/Q16 unchanged). Region 1/2's "no more than 2 over 50 cm" beside
      "no more than 1 over 50 cm, not counting steelhead" is ONE item stating Q16: "Over 50 cm: at
      most 2 — only 1 of them may be anything other than a hatchery steelhead". A lift out of the
      stream share names WHOSE rule it is and WHAT it allows: "On this river: up to 4 hatchery
      rainbow trout 50 cm or less — the 2-from-streams limit doesn’t apply to them" (Vedder r9,
      p.22). Where a rainbow over 50 cm is a steelhead (p.80) the first "steelhead" is defined
      inline: "steelhead (rainbow trout over 50 cm)". Generic: pinned on the Vedder, a Region 4
      stream (the Duncan, its own ✱ row's lifts, and Lardeau Creek, which inherits them) and a lake.
  A2. The group line is a <div> on page v36: a <ul> of items inside a <p> closed it early, and the
      items and the kept-elsewhere line fell out after the ⓘ tips ("What these words mean").
  A3. Every licensing record carries its book pages (`display.licensing[].book`), as every rule does.
  A4. Every part carries a SHORT label from its range's end labels ("Mouth → Vedder Crossing Bridge");
      the full label stays for the sheet.

`UI_EXPORT_BUNDLE` / `ANSWERS_EXPORT_DIR` point the suite at a side bundle and its export pair.
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path

import pytest

from pipeline.deliver import calendar as CAL
from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import common as C
from pipeline.deliver.answers import display as D
from pipeline.deliver.answers import encode as E
from pipeline.deliver.answers import model as M
from pipeline.deliver.answers import rows as RW
from pipeline.deliver.bundle import read as R
from pipeline.tests.conftest import need, part_at, BUNDLE_HINT, EXPORT_HINT

BUNDLE = os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE
EXPORT_DIR = Path(os.environ.get("ANSWERS_EXPORT_DIR")
                  or Path(R.BUNDLE).parent.parent / "regs")

VEDDER = "gnis:3062"
DUNCAN = "gnis:22255"                 # Region 4: its own ✱ row lifts DV and rainbow out of the 2
LARDEAU_CREEK = "gnis:16357"          # Region 4: a Duncan tributary, the lift inherited
CHILLIWACK_LAKE = "wbk:329083341"     # Region 2 lake: no steelhead, the Q16 pair is one clause
WATERS = (VEDDER, DUNCAN, LARDEAU_CREEK, CHILLIWACK_LAKE)

Q16 = "Over 50 cm: at most 2 — only 1 of them may be anything other than a hatchery steelhead."
VEDDER_ITEMS = [
    "On this river: up to 4 hatchery rainbow trout 50 cm or less — the 2-from-streams limit "
    "doesn’t apply to them.",
    "Steelhead (rainbow trout over 50 cm) count toward the 4.",
    "No more than 1 Dolly Varden/bull trout or lake trout.",
    Q16,
]


@pytest.fixture(scope="module")
def built(request):
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    need(request, "bundle", EXPORT_DIR / "ui-rules-export.json", EXPORT_HINT)
    data, guide = C.load_export(EXPORT_DIR)
    missing = [w for w in WATERS if w not in data["waters"]]
    if missing:
        pytest.fail(f"pinned waters missing from this export: {missing}")
    model = A.build(BUNDLE, EXPORT_DIR, workers=1, items=WATERS, sections=["display"],
                    export=(data, guide), log=lambda *_: None)
    return data, model


def _groups(built, item, part, md):
    data, model = built
    if isinstance(part, tuple):
        part = part_at(data, item, *part)
    k = model.parts[item][part]
    v = model.sections["rows"][k][E.segment_index(model.segments[k], CAL.day_of(md))]
    return [r["group"] for r in v["rows"] if r.get("group")]


def _trout(built, item, part, md=(10, 10)):
    return next(g for g in _groups(built, item, part, md)
                if "trout and char" in g["says"])


# --------------------------------------------------------------------------------------------
# A1. the group items
# --------------------------------------------------------------------------------------------

@pytest.mark.needs_bundle
def test_vedder_items_say_q16_once_name_the_lift_and_define_steelhead(built):
    g = _trout(built, VEDDER, ("mouth",))
    assert g["says"] == ("Up to 4 trout and char a day in all — no more than 2 of them from streams "
                         "(all streams combined).")
    assert g["items"] == VEDDER_ITEMS
    # the sub-caps (the decision) are untouched: both size clauses still hold their fish
    over = [s for s in g["subcaps"] if s["over_cm"] == 50]
    assert sorted(s["take"] for s in over) == [1, 2]
    M.GroupTotal.model_validate_json(json.dumps(g))


@pytest.mark.needs_bundle
def test_a_region_4_streams_own_lifts_name_the_river_and_what_they_allow(built):
    g = _trout(built, DUNCAN, ("mouth", "duncan_river__lardeau_river_into_duncan_river"))
    lifts = [t for t in g["items"] if t.startswith("On this river: ")]
    assert lifts == ["On this river: up to 2 Dolly Varden/bull trout — the 2-from-streams limit "
                     "doesn’t apply to them.",
                     "On this river: up to 5 rainbow trout — the 2-from-streams limit doesn’t "
                     "apply to them."]
    assert not any("don’t count toward" in t for t in g["items"])


@pytest.mark.needs_bundle
def test_an_inherited_lift_names_the_row_it_comes_from(built):
    g = _trout(built, LARDEAU_CREEK, 0)
    assert g["items"][0] == ("On the Duncan River and its tributaries: up to 5 rainbow trout — the "
                             "2-from-streams limit doesn’t apply to them.")


@pytest.mark.needs_bundle
def test_a_lake_with_no_steelhead_says_one_size_clause_and_no_definition(built):
    g = _trout(built, CHILLIWACK_LAKE, 0)
    assert g["items"] == ["No more than 1 Dolly Varden/bull trout or lake trout.",
                          "No more than 1 over 50 cm."]
    assert not any("steelhead" in t.lower() for t in g["items"])


def test_the_q16_pair_merges_only_with_its_partner():
    """MUTATION: a pair whose smaller clause is not smaller, or carries a lift, stays two items."""
    model = {"R": {"ST": {"H": {"status": "keep"}, "W": {"status": "release"}}}}
    a = {"band": (50, RW.INF), "take": 2, "who": "", "out": [], "lifted": False}
    b = {"band": (50, RW.INF), "take": 1, "who": "", "out": ["ST"], "lifted": False}
    words = ["no more than 2 over 50 cm", "no more than 1 over 50 cm, not counting steelhead"]
    w = list(words)
    RW._merge_size_pairs(w, [a, b], model)
    assert w == ["over 50 cm: at most 2 — only 1 of them may be anything other than a hatchery "
                 "steelhead"]
    for bad in ({**b, "take": 2}, {**b, "lifted": True}, {**b, "band": (60, RW.INF)}):
        w = list(words)
        RW._merge_size_pairs(w, [a, bad], model)
        assert w == words, bad
    # wild and hatchery steelhead both kept: no origin word
    w = list(words)
    RW._merge_size_pairs(w, [a, b], {"R": {"ST": {"H": {"status": "keep"},
                                                   "W": {"status": "keep"}}}})
    assert w[0].endswith("anything other than a steelhead")


def test_steelhead_is_defined_once_at_its_first_mention():
    got = RW._define_steelhead(["No more than 1 over 50 cm (hatchery steelhead over 50 cm excepted).",
                                "Steelhead count toward the 5.", "Steelhead again."])
    assert got[1] == "Steelhead (rainbow trout over 50 cm) count toward the 5."
    assert got[0].count("(rainbow") == 0 and got[2] == "Steelhead again."


# --------------------------------------------------------------------------------------------
# A2. the group line is a block, so its items stay inside it
# --------------------------------------------------------------------------------------------

def test_the_group_line_is_never_a_paragraph_holding_a_list():
    js = (Path(E.__file__).parent / "reference" / "page_v36.js").read_text()
    assert '<p class="gline">${tot}' not in js
    assert js.count('<div class="gline">${tot}') == 2
    assert not re.search(r"<p [^>]*>\$\{tot\}", js)


# --------------------------------------------------------------------------------------------
# A3. book pages for every licensing record; A4. short part labels
# --------------------------------------------------------------------------------------------

@pytest.mark.needs_bundle
def test_every_licensing_record_and_rule_carries_its_book_pages(built):
    data, model = built
    guide = json.loads((EXPORT_DIR / "ui-rules-guide.json").read_text())
    pdf = guide["guide"]["pdf_pages"]
    lic = model.statics["display"]["licensing"]
    assert len(lic) == len(data["licensing_ids"]) > 0
    for f in lic:
        assert f["book"] and all(pdf[str(p)] == q for p, q in f["book"])
    assert all(f["book"] for f in model.statics["display"]["rules"])


@pytest.mark.needs_bundle
def test_every_part_has_a_short_label_from_its_ends(built):
    data, model = built
    ws = model.statics["display"]["waters"]
    v = ws[VEDDER]["parts"][part_at(data, VEDDER, "mouth")]
    assert v["short"] == "Mouth → Vedder Crossing Bridge"
    assert v["label"] == "From the mouth up to the Vedder Crossing Bridge"
    assert ws[CHILLIWACK_LAKE]["parts"][0]["short"] == "Whole water"
    for item, w in ws.items():
        shorts = [p["short"] for p in w["parts"] if p]
        assert all(0 < len(s) <= D.SHORT_MAX + 24 for s in shorts), item
        assert len(set(shorts)) == len(shorts), item          # tells the parts apart


def test_a_short_label_reads_the_run_ends():
    doc = {"splits": {"x__bridge": {"name": "Big Bridge"}}, "waters": {}}
    one = {"runs": [{"from": "x__bridge", "to": "mouth", "km_from": 3.0, "km_to": 0.0}]}
    assert D.short_label(doc, one, "full") == "Mouth → Big Bridge"
    two = {"runs": [{"from": "source", "to": "x__bridge", "km_from": 9.0},
                    {"from": "x__bridge", "to": "mouth", "km_from": 3.0}]}
    assert D.short_label(doc, two, "full") == "Mouth → Source (2 stretches)"
    side = {"runs": [{"from": "a", "to": "b", "branch": True}]}
    assert D.short_label(doc, side, "full") == "Side channel"
    assert D.short_label(doc, {"runs": []}, "Whole water") == "Whole water"
