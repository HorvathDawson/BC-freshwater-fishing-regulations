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
                if "CT" in g["kinds"] or "DV" in g["kinds"])


# --------------------------------------------------------------------------------------------
# A1. the group items
# --------------------------------------------------------------------------------------------

# answers 2.8 rewords A1 (ROUND9): lifts positive with whose rule, over-50 on the kind rows — pinned
# here on the real waters, and on the no-data fixture in test_answers_28_card.py

def _lifts(g):
    return [L["says"] for L in g["lifts"]]


def _kinds(g):
    return {"/".join(k["species"]): k for b in g["buckets"] for k in b["kinds"]}


@pytest.mark.needs_bundle
def test_vedder_card_says_the_lift_once_and_over_50_on_the_rows(built):
    g = _trout(built, VEDDER, ("mouth",))
    assert g["headline"]["says"] == "2 a day from this stream"
    assert _lifts(g) == ["On this river, hatchery rainbow trout 50 cm or less: up to 4 a day — this "
                         "river’s rule replaces Region 2’s 2-from-streams limit for them."]
    k = _kinds(g)
    assert k["EB/GB/CT"]["notes"] == ["only 1 over 50 cm"]
    assert k["DV/LT"]["notes"] == ["uses your 1 over 50 cm"]
    assert k["ST"]["name"] == "Steelhead" and k["ST"]["n"] == 2
    assert k["RB"]["n"] == 4 and k["RB"]["notes"] == ["counts toward the 4 a day"]
    # the sub-caps (the decision) are untouched: both size clauses still hold their fish
    over = [s for s in g["subcaps"] if s["over_cm"] == 50]
    assert sorted(s["take"] for s in over) == [1, 2]
    M.GroupTotal.model_validate_json(json.dumps(g))


@pytest.mark.needs_bundle
def test_a_region_4_streams_own_lifts_name_the_river_and_what_they_allow(built):
    g = _trout(built, DUNCAN, ("mouth", "duncan_river__lardeau_river_into_duncan_river"))
    assert _lifts(g) == [
        "On this river, Dolly Varden/bull trout: up to 2 a day — this river’s rule replaces Region "
        "4’s 2-from-streams limit for them.",
        "On this river, rainbow trout: up to 5 a day — this river’s rule replaces Region 4’s "
        "2-from-streams limit for them."]
    assert [b["title"] for b in g["buckets"]] == ["Share the 2 from streams", "This river’s rule"]
    assert [w["kept"] for w in g["worked"]] == [5, 4, 3, 2]


@pytest.mark.needs_bundle
def test_an_inherited_lift_names_the_row_it_comes_from(built):
    g = _trout(built, LARDEAU_CREEK, 0)
    assert _lifts(g) == ["On the Duncan River and its tributaries, rainbow trout: up to 5 a day — the "
                         "Duncan River’s rule replaces Region 4’s 2-from-streams limit for them."]


@pytest.mark.needs_bundle
def test_a_lake_card_is_its_total_with_over_50_on_the_rows(built):
    g = _trout(built, CHILLIWACK_LAKE, 0)
    assert g["headline"]["says"] == "4 a day" and g["lifts"] == [] and g["total_line"] is None
    k = _kinds(g)
    assert k["EB/GB/CT/RB"]["notes"] == ["only 1 over 50 cm"]
    assert k["DV/LT"]["notes"] == ["uses your 1 over 50 cm"]
    assert not any("steelhead" in t.lower() for kk in k.values() for t in kk["notes"])


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
