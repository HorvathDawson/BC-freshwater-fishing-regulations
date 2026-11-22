"""ANSWERS 2.7 (2026-10-11, ROUND8 A): wording, book links and short part labels — no decision moves.

  A1. THE GROUP ITEMS (RULINGS Q4/Q16 unchanged). Region 1/2's "no more than 2 over 50 cm" beside
      "no more than 1 over 50 cm, not counting steelhead" is ONE item stating Q16: "Over 50 cm: at
      most 2 — only 1 of them may be anything other than a hatchery steelhead". A lift out of the
      stream share names WHOSE rule it is and WHAT it allows: "On this river: up to 4 hatchery
      rainbow trout 50 cm (20 in) or less — the 2-from-streams limit doesn’t apply to them" (Vedder r9,
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
from pathlib import Path

import pytest

from pipeline.deliver import calendar as CAL
from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import common as C
from pipeline.deliver.answers import display as D
from pipeline.deliver.answers import encode as E
from pipeline.deliver.answers import model as M
from pipeline.deliver.answers import rows as RW
from pipeline.tests.conftest import need, part_at, BUNDLE_HINT, EXPORT_HINT
from pipeline.tests import under_test

BUNDLE = under_test.path("bundle")
EXPORT_DIR = under_test.path("export_dir")

VEDDER = "gnis:3062"
DUNCAN = "gnis:22255"                 # Region 4: its own ✱ row lifts DV and rainbow out of the 2
LARDEAU_CREEK = "gnis:16357"          # Region 4: a Duncan tributary, the lift inherited
CHILLIWACK_LAKE = "wbk:329083341"     # Region 2 lake: no steelhead, the Q16 pair is one clause
WATERS = (VEDDER, DUNCAN, LARDEAU_CREEK, CHILLIWACK_LAKE)


def _name(doc, part):
    """T517 (owner 2026-11-09): a part's name from its end tokens, as the app's `spanName` composes it; None: no main
    stem (the app says the water's name)."""
    from pipeline.deliver.answers.display.part import end_name_of, end_tokens
    t = end_tokens(doc, part)
    return end_name_of(doc, t[0], t[1]) if t else None


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

def _kinds(g):
    return {"/".join(k["species"]): k for b in g["buckets"] for k in b["kinds"]}


@pytest.mark.needs_bundle
def test_vedder_card_says_the_lift_once_and_over_50_on_the_rows(built):
    # (T866) the headline, the lift, the share's size group and the kind rows' words: card_words_live LIVE
    g = _trout(built, VEDDER, ("mouth",))
    k = _kinds(g)
    # ROUND11 A1: the share's over-50 cap is its bucket's note, said once; no size note on a row
    # (ROUND12 2: the river's own bucket first, the share second)
    # ROUND15 1: a size group naming the kinds it binds (steelhead not among them)
    assert g["buckets"][1]["sizes"][0]["not_counting"] == ["ST"]
    assert k["EB/GB/CT"]["notes"] == [] and k["DV/LT"]["notes"] == []
    # the sub-caps (the decision) are untouched: both size clauses still hold their fish
    over = [s for s in g["subcaps"] if s["over_cm"] == 50]
    assert sorted(s["take"] for s in over) == [1, 2]
    M.GroupTotal.model_validate_json(json.dumps(g))


@pytest.mark.needs_bundle
def test_a_region_4_streams_own_lifts_name_the_river_and_what_they_allow(built):
    # (T866) the lift line (ROUND10 W1, T834: rainbow alone), the buckets' titles (ROUND12 2, T834) and the headline's
    # `also`, word for word: card_words_live LIVE
    g = _trout(built, DUNCAN, ("mouth", "duncan_river__lardeau_river_into_duncan_river"))
    assert [w["kept"] for w in g["worked"] if w["at"] == "here"] == [5, 2]                 # ROUND20b C4: the ends of the range


# (T866) Lardeau Creek's inherited lift ("On the Duncan River and its tributaries, …"): card_words_live LIVE


@pytest.mark.needs_bundle
def test_a_lake_card_is_its_total_with_over_50_on_the_rows(built):
    # (T866) the headline/lifts/total, the buckets' titles, notes and size groups (round 9 gate, ROUND11 A1, ROUND12 3,
    # ROUND15 1) and the Dolly Varden's note, word for word: card_words_live LIVE
    g = _trout(built, CHILLIWACK_LAKE, 0)
    k = _kinds(g)
    assert k["EB/GB/CT/RB"]["notes"] == [] and k["LT"]["notes"] == []
    assert not any("steelhead" in t.lower() for kk in k.values() for t in kk["notes"])


# --------------------------------------------------------------------------------------------
# A2. the group line is a block, so its items stay inside it
# --------------------------------------------------------------------------------------------


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
    # (T931 review, 2026-11-22) T981 (0.8.5): a local authority's closure prints on no page and cites its authority
    # instead (`cited`); every other rule carries its book pages (test_answers_26 pins the pairing)
    assert all(f["book"] or f.get("cited") for f in model.statics["display"]["rules"])


@pytest.mark.needs_bundle
def test_every_part_has_a_name_from_its_ends(built):
    """T517 (owner 2026-11-09): a part ships its two END TOKENS (`short` only without them); the name is composed from
    `display.ends` the way the app's `spanName` does (`name_check.words`), and no two parts of a water say one name."""
    from pipeline.deliver.answers.display.name_check import problems, words
    data, model = built
    ws = model.statics["display"]["waters"]
    ends = model.statics["display"]["ends"]

    def name(p, water="the water"):
        return water if p["lo"] is None else f"{words(ends, p['lo'], 'lo')} → {words(ends, p['hi'], 'hi')}"
    # T517: the Vedder's one rule set is two parts, cut where DFO's salmon area changes at the cable car crossing
    cable = "Cable car crossing (200 m downstream of Vedder Crossing bridge)"
    v = ws[VEDDER]["parts"][part_at(data, VEDDER, "mouth")]
    assert ("short" not in v, name(v)) == (True, f"Mouth → {cable}")
    assert v["label"] == "From the mouth up to the Vedder Crossing Bridge"
    up = ws[VEDDER]["parts"][part_at(data, VEDDER, "chilliwack_vedder_rivers__vedder_crossing_bridge")]
    assert name(up) == f"{cable} → Vedder Crossing Bridge"
    # the whole run, as the app joins it: the first piece's lower end to the last piece's upper end
    assert f"{words(ends, v['lo'], 'lo')} → {words(ends, up['hi'], 'hi')}" == "Mouth → Vedder Crossing Bridge"
    lake = ws[CHILLIWACK_LAKE]["parts"][0]
    assert (lake["lo"], lake["hi"]) == (None, None)           # a lake: no ends, the app says the water's name
    names = {k: (data["waters"][k]["name"] if k in data.get("waters", {}) else k) for k in ws}
    assert problems(sorted((k, names[k], w["parts"]) for k, w in ws.items()), ends) == []
    for item, w in ws.items():
        assert all(name(p) and "…" not in name(p) for p in w["parts"] if p), item      # ROUND10 W3: never cut


def test_a_short_label_reads_the_run_ends():
    doc = {"splits": {"x__bridge": {"name": "Big Bridge"}}, "waters": {}}
    one = {"runs": [{"from": "x__bridge", "to": "mouth", "km_from": 3.0, "km_to": 0.0}]}
    assert _name(doc, one) == "Mouth → Big Bridge"
    two = {"runs": [{"from": "source", "to": "x__bridge", "km_from": 9.0},
                    {"from": "x__bridge", "to": "mouth", "km_from": 3.0}]}
    assert _name(doc, two) == "Mouth → Source"      # T517: one run, its own two ends
    side = {"runs": [{"from": "a", "to": "b", "branch": True}]}
    assert _name(doc, side) is None                                 # no main stem: the water's name (app)
    assert _name(doc, {"runs": []}) is None


def test_t517_a_rule_set_group_says_its_salmon_cut_pieces_as_the_one_run_they_are():
    """T517: the picker and the full label read the water by rule-set group; two parts of one run the DFO salmon chain
    cut meet end to end at the cut, so the group's label says one run (as before T517), never "Two stretches"."""
    from pipeline.deliver.answers.display import builders as B
    w = {"kind": "stream", "name": "X", "parts": [
        {"ruleset": "1", "grp": 0, "run": 0, "sections": 2,
         "runs": [{"from": "cut", "to": "mouth", "km_from": 3.0, "km_to": 0.0}]},
        {"ruleset": "1", "grp": 0, "run": 0, "sections": 1,
         "runs": [{"from": "source", "to": "cut", "km_from": 9.0, "km_to": 3.0}]}]}
    from pipeline.deliver.export.read import group_runs
    w["grp_runs"] = group_runs(w["parts"])          # (T679 #17) joined once by the export, as `export.read` writes it
    Wg, members = B._by_grp("x", w, {"waters": {"x": w}, "splits": {}, "rulesets": {"1": {}}, "rules": {}})
    assert Wg.parts[0]["runs"] == [{"from": "source", "to": "mouth", "km_from": 9.0, "km_to": 0.0}]
    assert members == {0: [0, 1]} and Wg.parts[0]["sections"] == 3
    # two runs of ONE part (a border between) stay two, as before T517
    one = {"kind": "stream", "name": "Y", "parts": [{"ruleset": "1", "grp": 0, "run": 0, "sections": 2, "runs": [
        {"from": "source", "to": "bc_border", "km_from": 9.0, "km_to": 3.0},
        {"from": "bc_border", "to": "mouth", "km_from": 3.0, "km_to": 0.0}]}]}
    Wy, _ = B._by_grp("y", one, {"waters": {"y": one}, "splits": {}, "rulesets": {"1": {}}, "rules": {}})
    assert len(Wy.parts[0]["runs"]) == 2
