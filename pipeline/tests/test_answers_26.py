"""ANSWERS 2.6 (2026-10-11, zero hand corrections in the app design): every word the design showed
by hand now comes from the pipeline.

  1. THE OPEN-DAYS STRIP. The display frame's `strip` code per segment (closed 1 alone, release 2,
     fly_only 4, artificial_fly_only 8, bait_ban 16), composed from the display status, the card's
     decided answers and the gear answer at the same key and segment; the legend is `display.strip`.
     Vedder (mouth part) May 15: fly fishing only + bait ban (p.22); Oct 10: open. Cowichan (p.15):
     "Fly fishing only from signs at Greendale Trestle to CNR Bridge (mile 70.2)" all year; "Fly
     fishing only upstream of CNR Trestle (Mile 66) Sept 1-Nov 15" — fly only on Oct 10, not May 15.
  2. GROUP WORDING (user): the lead "Up to 4 trout and char a day in all — no more than 2 of them
     from streams (all streams combined)." and the rest as short sub-items (`group.items`).
  3. "(Mile 66)": the Cowichan's cut is "CNR Mile 66 Trestle" (p.15), "CNR Bridge (mile 70.2)" —
     curated `splits.json` labels, carried by `splits.resolved.json` (`sidecars --splits-only`).
  4. EMPTY ARCTIC GRAYLING. Clearwater Creek gnis:7628 (Region 7B, a Williston Lake tributary):
     the creek's own "Catch and release", Zone B's "Arctic grayling: 2" and the inherited "Arctic
     grayling catch and release" beat each other in a CYCLE, and every one fell. The reader now
     breaks a cycle at the own row (Q5/T4, before naming); `rows.no_answer_gate` refuses a null
     answer where a zone's quota took part.
  6. A LINE IS A ROD (owner): "1 line (rod)", "2 lines (rods)" in every rule label and the gear
     answers' `count_nouns`; a set line keeps "line".
  5. HOW THIS WAS DECIDED: every rule's `book` [[printed, PDF page]], `applies` (and
     `applies_trib`), and `display.reasons`.

`UI_EXPORT_BUNDLE` / `ANSWERS_EXPORT_DIR` point the suite at a side bundle and its export pair.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from pipeline.deliver import calendar as CAL
from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import common as C
from pipeline.deliver.answers import display as D
from pipeline.deliver.answers import encode as E
from pipeline.deliver.answers import model as M
from pipeline.deliver.answers import rows as RW
from pipeline.deliver.answers.common import AnswersError
from pipeline.deliver.bundle import read as R
from pipeline.tests.conftest import need, part_at, BUNDLE_HINT, EXPORT_HINT

BUNDLE = os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE
EXPORT_DIR = Path(os.environ.get("ANSWERS_EXPORT_DIR")
                  or Path(R.BUNDLE).parent.parent / "regs")

VEDDER = "gnis:3062"
COWICHAN = "gnis:12227"
CLEARWATER = "gnis:7628"             # Clearwater Creek, Region 7B, a Williston Lake tributary
WATERS = (VEDDER, COWICHAN, CLEARWATER)
COW = "r1:cowichan_river_see_map_below@1-4::cowichan_river"
FLY, BAIT, REL, CLOSED, AFLY = 4, 16, 2, 1, 8


# --------------------------------------------------------------------------------------------
# 1. the strip code (no data)
# --------------------------------------------------------------------------------------------

def _rows(*statuses):
    return {"fish": {S: {"hatchery": {"status": st}, "wild": {"status": st}}
                     for S, st in zip(("RB", "CT", "DV", "MW"), statuses)}}


def test_the_strip_code_reads_status_rows_and_gear_at_one_segment():
    own, shut = {"status": "own"}, {"status": "closed"}
    assert D.strip_code(own, _rows("keep"), {"fly": None, "bait_ban": False}) == 0
    assert D.strip_code(own, _rows("keep"), {"fly": "fly_fishing_only", "bait_ban": True}) == FLY | BAIT
    assert D.strip_code(own, _rows("keep"), {"fly": "artificial_fly_only", "bait_ban": False}) == AFLY
    # release only: no game fish on the card may be kept
    assert D.strip_code(own, _rows("release", "closed"), {"fly": None, "bait_ban": True}) == REL | BAIT
    assert D.strip_code(own, _rows("release", "keep"), {"fly": None, "bait_ban": False}) == 0
    assert D.strip_code(own, _rows("closed", "closed"), {"fly": None, "bait_ban": False}) == 0
    # a closed water draws closed alone: its gear is moot
    assert D.strip_code(shut, _rows("closed"), {"fly": "fly_fishing_only", "bait_ban": True}) == CLOSED
    # a tidal frame has no strip
    assert D.compose({"status": "tidal"}, {"rows": {}, "gear": {}}) == {"status": "tidal"}


def test_the_strip_legend_and_the_reason_words_are_their_models():
    M.STATICS[("display", "strip")].validate_json(json.dumps(D.strip_legend()))
    words = D.reasons_table()
    assert set(words) == set(R.LOSS_REASONS)
    M.STATICS[("display", "reasons")].validate_json(json.dumps(words))
    assert {x["bit"] for x in D.strip_legend()} == {0, 1, 2, 4, 8, 16}
    assert all("{by}" in w["lost"] and "{rule}" in w["won"] for w in words.values())


def test_a_reader_reason_with_no_words_is_refused(monkeypatch):
    monkeypatch.setitem(R.LOSS_REASONS, "new_step", "displaced")
    with pytest.raises(AnswersError):
        D.reasons_table()


# --------------------------------------------------------------------------------------------
# the built answers (Vedder, Cowichan, Clearwater Creek)
# --------------------------------------------------------------------------------------------

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


def at(built, item, part, md, section):
    data, model = built
    if isinstance(part, str):
        part = part_at(data, item, *part.split("|"))
    k = model.parts[item][part]
    return model.sections[section][k][E.segment_index(model.segments[k], CAL.day_of(md))]


@pytest.mark.needs_bundle
def test_vedder_may_is_fly_fishing_only_with_a_bait_ban_and_october_is_open(built):
    """p.22: "Fly fishing only May 1-31", "Bait ban May 1-31" (the Vedder's mouth part)."""
    for d in ((5, 1), (5, 15), (5, 31)):
        assert at(built, VEDDER, "mouth", d, "display")["strip"] == FLY | BAIT, d
    for d in ((4, 30), (6, 1), (10, 10)):
        assert at(built, VEDDER, "mouth", d, "display")["strip"] & (FLY | BAIT) == 0, d
    assert at(built, VEDDER, "mouth", (10, 10), "display")["strip"] == 0


@pytest.mark.needs_bundle
def test_the_cowichans_fly_only_stretches_are_drawn_from_its_gear(built):
    """p.15: Greendale Trestle to CNR Bridge (mile 70.2) fly fishing only (all year); upstream of the
    CNR Mile 66 Trestle fly fishing only Sept 1-Nov 15; the river's bait ban all year (Region 1's
    streams)."""
    mid = "cowichan_river__cnr_bridge|cowichan_river__greendale_trestle"
    above_66 = "cowichan_river__cnr_trestle|cowichan_river__cnr_bridge"
    for d in ((1, 15), (5, 15), (10, 10), (12, 1)):
        f = at(built, COWICHAN, mid, d, "display")
        assert f["strip"] == CLOSED or f["strip"] & FLY, d
        assert at(built, COWICHAN, mid, d, "gear")["fly"] == "fly_fishing_only"
    assert at(built, COWICHAN, above_66, (10, 10), "display")["strip"] == FLY | BAIT
    assert at(built, COWICHAN, above_66, (5, 15), "display")["strip"] == BAIT
    # the mouth part is closed Sept 1-Nov 15 below the Mile 66 trestle
    assert at(built, COWICHAN, "cowichan_river__cnr_trestle|mouth", (10, 10), "display")["strip"] == \
        CLOSED


@pytest.mark.needs_bundle
def test_every_strip_code_is_its_three_answers(built):
    """The strip code is a function of the display status and the rows and gear frames at the same
    key and segment — over every segment built (MUTATION: flipping a gear answer moves it)."""
    _, model = built
    n = 0
    for k in range(len(model.keys)):
        for s, f in enumerate(model.sections["display"][k]):
            if f.get("status") == "tidal":
                continue
            g, r = model.sections["gear"][k][s], model.sections["rows"][k][s]
            assert f["strip"] == D.strip_code(f, r, g)
            if f["status"] != "closed":
                assert D.strip_code(f, r, {**g, "bait_ban": not g["bait_ban"]}) != f["strip"]
            n += 1
    assert n > 10


# --------------------------------------------------------------------------------------------
# 2. the group wording
# --------------------------------------------------------------------------------------------

@pytest.mark.needs_bundle
def test_the_group_lead_says_the_total_and_the_stream_share_of_it_all(built):
    """answers 2.8 supersedes the 2.6 lead + items: the headline is the stream's 2, the total is a line
    under it (test_answers_28_card.py pins the whole card on the no-data fixture)."""
    rows = at(built, VEDDER, "mouth", (10, 10), "rows")["rows"]
    g = next(r["group"] for r in rows if r.get("group") and r["group"]["total"] == 4)
    assert g["headline"]["says"] == "2 a day from this stream"
    assert g["total_line"] == "No more than 4 trout and char a day across Region 2, lakes included."
    M.GroupTotal.model_validate_json(json.dumps(g))


def test_the_group_model_requires_the_card():
    ok = {"pool": 1, "total": 4, "kinds": ["CT", "RB"], "subcaps": [], **{"headline": {"n": 4, "unit": "a day", "says": "4 a day"}, "origin": None, "lifts": [],
            "total_line": None, "worked": [], "not_kept": [], "states": [{"r": 1, "line": "headline"}],
            "buckets": [{"title": None, "cap": 4, "by": 1, "why": "total",
                         "kinds": [{"species": ["CT", "RB"], "name": "Cutthroat and rainbow trout",
                                    "detail": "any size", "n": 4, "notes": [], "duties": []}],
                         "groups": [], "notes": [], "replaces": None}],
            # ROUND16 1: the table and cap system (empty systems are valid shapes)
            "table": {"bands": {S: [{"from": 0, "to": None, "says": "any size"}] for S in ("CT", "RB")},
                      "cells": {S: [{"n": 4, "why": None, "origin": None, "by_origin": None, "as": None}]
                                for S in ("CT", "RB")}, "steelhead_cm": None},
            "cap_kinds": [], "caps": []}}
    M.GroupTotal.model_validate_json(json.dumps(ok))
    for bad in ({k: v for k, v in ok.items() if k != "buckets"}, {**ok, "says": "Up to 4."},
                {**ok, "states": ok["states"] * 2}):
        with pytest.raises(Exception):
            M.GroupTotal.model_validate_json(json.dumps(bad))


def test_the_page_shows_the_card():
    js = (Path(E.__file__).parent / "reference" / "page_v36.js").read_text()
    assert "grp.items" not in js and "cardBlock(row, id)" in js


# --------------------------------------------------------------------------------------------
# 3. the Cowichan's cut names, as the book prints them
# --------------------------------------------------------------------------------------------

@pytest.mark.needs_bundle
def test_the_cowichans_cuts_are_named_as_the_book_prints_them(built):
    """p.15: "downstream of the CNR Mile 66 Trestle", "to CNR Bridge (mile 70.2)"."""
    data, model = built
    labels = [p["label"] for p in model.statics["display"]["waters"][COWICHAN]["parts"] if p]
    # R9 D5/D7 (ce74eaef): the delta piece (sid 716, a line from a pond's outlet to the sea) is placed
    # by its flow path into the mouth part; ROUND10 W2: a part with a main run never mentions its side
    # channels (it read "… Trestle and 1 side channel")
    assert "From the mouth up to the CNR Mile 66 Trestle" in labels
    assert "From the CNR Mile 66 Trestle up to the CNR Bridge (mile 70.2)" in labels
    assert not any(l.endswith("CNR Trestle") or "CNR Trestle " in l for l in labels)


def test_the_splits_sidecar_carries_the_curated_label():
    from types import SimpleNamespace as NS
    from pipeline.atlas.sidecars import enrich_resolved
    d = NS(id="a__t", label="CNR Mile 66 Trestle", anchor=NS(offset_m=None, offset_dir=None))
    rows = [{"split_id": "a__t", "blk": "1", "route_measure": 1.0, "label": "CNR Trestle",
             "anchor_type": "point"}]
    got = enrich_resolved(rows, [d])
    assert got[0]["label"] == "CNR Mile 66 Trestle" and got[0]["source"] == "curated"


# --------------------------------------------------------------------------------------------
# 4. no empty answer: Clearwater Creek's Arctic grayling
# --------------------------------------------------------------------------------------------

@pytest.mark.needs_bundle
@pytest.mark.parametrize("md, win", [((1, 15), "r7:clearwater_creek@7-31::clearwater_creek.r1"),
                                     ((5, 20), "z7b:arctic_grayling::arctic_grayling.r4"),
                                     ((8, 1), "r7:clearwater_creek@7-31::clearwater_creek.r1")])
def test_clearwater_creeks_arctic_grayling_are_released(built, md, win):
    """The creek's row: "Catch and release"; Zone B: "Arctic grayling … all from Williston Lake and
    its tributaries" released, "any size, May 1-June 15" released (p.64). It had no answer at all."""
    data, _ = built
    t = at(built, CLEARWATER, 0, md, "rows")
    for o in ("hatchery", "wild"):
        d = t["fish"]["GR"][o]
        assert d is not None and d["status"] == "release", (md, o)
        assert data["rule_ids"][d["win"]] == win


def test_the_reader_breaks_a_cycle_at_the_own_row():
    """Unit: three keepers beat each other in a ring (naming, place, own row); the own row's edge
    decides first, so exactly one stands."""
    from pipeline.deliver.bundle.read import _step4_competition

    class S:
        def __init__(self):
            self.cand = {"own", "zone", "inh"}
            self.keyed = {("retention_limit", "daily"): ["own", "zone", "inh"]}
            self.why, self.out_, self.overridden = {}, set(), set()
            self._beats = {("zone", "own"), ("inh", "zone"), ("own", "inh")}

        def family(self, k):
            return k

        def peers(self, o, k):
            return False

        def beats(self, o, k):
            return (o, k) in self._beats

        def closure(self, k):
            return False

        def own_over_inherited(self, o, k):
            return (o, k) == ("own", "inh")

        def nearer_over_farther(self, o, k):        # ROUND14 1: no nearer ✱ row in this ring
            return False

        def precedes(self, o, k):
            return self.own_over_inherited(o, k) or self.nearer_over_farther(o, k)

        def water_dates_override(self, o, k):
            return False

        def lose(self, k, reason, by):
            self.out_.discard(k)
            self.why.setdefault(k, (reason, by))

    s = S()
    _step4_competition(s)
    assert s.out_ == {"zone"} and s.why == {"inh": ("own_row", "own"), "own": ("ladder", "zone")}
    # MUTATION: with no own-row edge the ring falls whole (the old reading)
    s = S()
    s.own_over_inherited = lambda o, k: False
    _step4_competition(s)
    assert s.out_ == set()


def test_a_null_answer_where_the_zone_took_part_is_refused():
    out = {"fish": {"GR": {"hatchery": None, "wild": {"status": "release"}}}}
    lad = {"GR": {"hatchery": {"z7b:arctic_grayling::arctic_grayling.r1":
                               ["displaced", None, "ladder", "x"]}}}
    by_id = {"z7b:arctic_grayling::arctic_grayling.r1": {"type": "retention_limit"}}
    with pytest.raises(AnswersError):
        RW.no_answer_gate(out, lad, by_id, "test")
    # no zone quota took part: null is an answer ("no rule in scope speaks")
    RW.no_answer_gate(out, {"GR": {"hatchery": {}}}, by_id, "test")
    RW.no_answer_gate(out, {"GR": {"hatchery": {"z7b:arctic_grayling::arctic_grayling.r1":
                                                ["shown", None, None, None]}}}, by_id, "test")


@pytest.mark.needs_bundle
def test_no_fish_has_an_empty_answer_anywhere(request):
    """Every decided answer of the shipped answers file is non-null wherever a zone's quota or
    release for the fish took part (the build refuses it; this reads the file as shipped)."""
    p = EXPORT_DIR / "ui-rules-answers.json"
    need(request, "bundle", p, EXPORT_HINT)
    w = json.loads(p.read_text())
    sec = w["sections"]["rows"]
    bad = [(k, s) for k, ats in enumerate(sec["at"]) for s, f in enumerate(ats)
           if any(h is None or x is None for h, x in sec["frames"][f][1].values())
           and "GR" in sec["frames"][f][1]
           and None in sec["frames"][f][1]["GR"]]
    assert not bad, bad[:5]


# --------------------------------------------------------------------------------------------
# 5. how this was decided: book pages and why a rule applies
# --------------------------------------------------------------------------------------------

@pytest.mark.needs_bundle
def test_every_rule_carries_its_book_pages_and_why_it_applies(built):
    data, model = built
    guide = json.loads((EXPORT_DIR / "ui-rules-guide.json").read_text())
    pdf = guide["guide"]["pdf_pages"]
    rules = model.statics["display"]["rules"]
    assert len(rules) == len(data["rule_ids"])
    for f in rules:
        assert f["book"] and all(pdf[str(p)] == q for p, q in f["book"])
        assert f["applies"].endswith(".")
    ix = {k: i for i, k in enumerate(data["rule_ids"])}
    r5 = rules[ix[f"{COW}.r5"]]
    assert r5["book"] == [[15, 17]]                       # printed p.15 = PDF p.17
    assert r5["applies"] == ("This water’s own row in the 2025–2027 BC Freshwater Fishing Regulations Synopsis: "
                            "Cowichan River.")             # ROUND13 P: the book by its title
    g5 = rules[ix["z7b:arctic_grayling::arctic_grayling.r5"]]
    assert g5["applies"] == "From the Region 7B table, for Williston Lake."
    assert g5["applies_trib"] == ("Inherited from Williston Lake: the Region 7B table covers its "
                                  "tributaries.")
    assert rules[ix["z2:trout_char_quota::trout_char_quota.r1"]]["applies"] == \
        "From the Region 2 table — all of Region 2."
    lift = rules[ix["r1:nitinat_river@1-4::nitinat_river.r1"]]
    assert "It lifts “No fishing in streams, Jul 15-Aug 31" in lift["applies"]


# --------------------------------------------------------------------------------------------
# 6. a line count says rod
# --------------------------------------------------------------------------------------------

def _gear_rule(**kw):
    from pipeline.regs.parsing.catalogue import CatalogueRule, RuleType
    return CatalogueRule(rule_id="t.r1", verbatim="v", type=RuleType.tackle_restriction, **kw)


def test_a_line_count_says_rod():
    from pipeline.regs.parsing.catalogue import label
    from pipeline.deliver.answers import gear as GE
    say = lambda **kw: label(_gear_rule(**kw), place_of=lambda e: None)  # noqa: E731
    assert say(gear=[{"slot": "lines_per_angler", "max": 1}]) == "At most 1 line (rod) per angler"
    assert say(gear=[{"slot": "lines_per_angler", "max": 2}]) == "At most 2 lines (rods) per angler"
    assert say(gear=[{"slot": "lines_per_angler", "unlimited": True}]) == \
        "Unlimited lines (rods) per angler"
    # a set line is no rod
    assert say(gear=[{"slot": "lines_per_angler", "max": 1}], **{"while": ["set_lining"]}) == \
        "At most 1 line per angler, while set lining"
    assert GE.static_tables()["count_nouns"] == {"lines_per_angler": ["line (rod)", "lines (rods)"]}
    js = (Path(E.__file__).parent / "reference" / "page_v36.js").read_text()
    assert "count_nouns" in js and "line${L.c?.max > 1" not in js


@pytest.mark.needs_bundle
def test_the_shipped_labels_say_rod():
    data = json.loads((EXPORT_DIR / "ui-rules-export.json").read_text())
    from pipeline.tools.export_codec import expand
    guide = json.loads((EXPORT_DIR / "ui-rules-guide.json").read_text())
    labels = [r["label"] for r in expand(data, guide)["rules"].values()]
    assert any("line (rod) per angler" in t for t in labels)
    bare = [t for t in labels if re_bare(t) and "set lining" not in t]
    assert not bare, bare[:3]


def re_bare(t: str) -> bool:
    import re
    return bool(re.search(r"\blines? per angler", t))
