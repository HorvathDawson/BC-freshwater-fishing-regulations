"""ANSWERS 2.5 (2026-10-10, the angler-eval round): group totals in Python (rows F14), a size
release is an answer (rows F13), and T3 as clarified by the user (a global ✱ carries the row's
exemptions to its tributaries).

  * F14 Vedder (mouth part), Oct 10 / Jan 15: Region 2's trout/char total is 4 — steelhead
        included (user 2026-10-10) — with "2 from streams" holding every kind but the hatchery
        rainbow the water's r9 lifts (p.21-22); the page read "2 a day in total" and "Kept 2 on
        another stream? Keep 0 here" off the narrowed number.
  * F13 Vedder (mouth part), May 15: r6 "hatchery rainbow trout catch and release (50 cm or less)"
        (p.22) releases the hatchery rainbow — the decided answer was null and the fish vanished.
  * T3  Duncan, Kitimat: the quota a global-✱ row prints walks to the tributaries WITH the
        exemption it prints ("replaces '2 from streams'").

`UI_EXPORT_BUNDLE` / `ANSWERS_EXPORT_DIR` point the suite at a side bundle and its export pair.
"""
from __future__ import annotations

import copy
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from pipeline.deliver import calendar as CAL
from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import common as C
from pipeline.deliver.answers import encode as E
from pipeline.deliver.answers import model as M
from pipeline.deliver.answers import rows as RW
from pipeline.deliver.bundle import read as R
from pipeline.tests.conftest import need, part_at, BUNDLE_HINT, EXPORT_HINT

BUNDLE = os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE
EXPORT_DIR = Path(os.environ.get("ANSWERS_EXPORT_DIR")
                  or Path(R.BUNDLE).parent.parent / "regs")

VEDDER = "gnis:3062"
MOUTH = "mouth"                      # the Vedder part ending at the mouth (Vedder Crossing Bridge down)
GLENN = "gnis:10457"                 # Glenn Creek, a ✱ tributary of the Duncan (Region 4)
HUMPHRYS = "gnis:2046"               # Humphrys Creek, a ✱ tributary of the Kitimat (Region 6)
WATERS = (VEDDER, GLENN, HUMPHRYS)
VR = "r2:chilliwack_vedder_rivers_does_not_include_sumas_river_see_ma@2-4::chilliwack_vedder_rivers"
Z2 = "z2:trout_char_quota::trout_char_quota"


@pytest.fixture(scope="module")
def built(request):
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    need(request, "bundle", EXPORT_DIR / "ui-rules-export.json", EXPORT_HINT)
    data, _ = C.load_export(EXPORT_DIR)
    missing = [w for w in WATERS if w not in data["waters"]]
    if missing:
        pytest.fail(f"pinned waters missing from this export: {missing}")
    model = A.build(BUNDLE, EXPORT_DIR, workers=1, items=WATERS, sections=["rows"],
                    log=lambda *_: None)
    return data, model, C.load(BUNDLE)


def card(built, item, part, md, mutate=None):
    """The card's rows for one part and day: as built, or recomputed from the built ladder after
    `mutate(ladder, rid)` (an input mutation)."""
    data, model, B = built
    if isinstance(part, str):                    # an endpoint label: the part by what it covers
        part = part_at(data, item, part)
    k = model.parts[item][part]
    s = E.segment_index(model.segments[k], CAL.day_of(md))
    if mutate is None:
        return model.sections["rows"][k][s]
    ladder = copy.deepcopy(model.sections["ladder"][k][s])
    mutate(ladder, lambda name: next(i for i in data["rule_ids"] if i == name or i.endswith(name)))
    key = model.keys[k]
    rk, kd = C.rule_key(key), C.key_dict(key)
    ctx = A.Context(BUNDLE, {}, verdicts=str(Path(BUNDLE).with_name("verdicts.sqlite")))
    P = RW.Part(B, rk.set_id, rk.steelhead_water, rk.steelhead_rules, kd["kind"], kd["steelhead"],
                ladder, md, RW.open_states(ctx, rk, md))
    rix = {x: i for i, x in enumerate(data["rule_ids"])}
    return RW.refs(json.loads(json.dumps(RW.produce(P))), rix.__getitem__)


def rid(built, i) -> str:
    return built[0]["rule_ids"][i]


def row_of(built, t, pool):
    return next(r for r in t["rows"] if r["pool"] is not None and rid(built, r["pool"]) == pool)


def subcap(built, g, rule):
    return next(s for s in g["subcaps"] if rid(built, s["r"]) == rule)


def speak(rule, fish, origin):
    """An input mutation: `rule` speaks for `fish` of `origin` (a lift taken away)."""
    def m(ladder, find):
        ladder[fish][origin][find(rule)] = ["speaks", None, None, None]
    return m


def silence(rule, fish, origins=("none", "hatchery", "wild")):
    def m(ladder, find):
        k = find(rule)
        for o in origins:
            if k in ladder[fish].get(o, {}):
                ladder[fish][o][k] = ["displaced", None, "ladder", k]
    return m


# --------------------------------------------------------------------------------------------
# F14 group totals
# --------------------------------------------------------------------------------------------

# answers 2.8 (ROUND9 A1): the lift said positively, with whose rule it is (the 2.6 lead sentence and
# the 2.7 items are gone — they restated the kind rows)
VEDDER_LIFT = ("On this river, hatchery rainbow trout 50 cm (20 in) or less: up to 4 a day — this river’s rule "
               "replaces Region 2’s 2-from-streams limit for them.")


@pytest.mark.needs_bundle
@pytest.mark.parametrize("md", [(10, 10), (1, 15)])
def test_f14_vedder_trout_and_char_total_is_4_with_the_stream_share_lifted_for_hatchery_rainbow(
        built, md):
    """p.21: "Trout/char: 4, but not more than • 1 over 50 cm (2 hatchery steelhead over 50 cm
    allowed) • 2 from streams (must be hatchery)"; p.22 (c): "hatchery rainbow trout of any length
    50 cm or less: daily quota = 4, July 1-Apr 30"."""
    t = card(built, VEDDER, MOUTH, md)
    row = row_of(built, t, f"{Z2}.r1")
    assert row["daily"] == 2                                     # the stream share, unchanged
    g = row["group"]
    assert rid(built, g["pool"]) == f"{Z2}.r1" and g["total"] == 4
    assert {"RB", "ST", "CT", "DV"} <= set(g["kinds"])         # steelhead count toward the 4
    share = subcap(built, g, f"{Z2}.r4")
    assert share["take"] == 2 and share["water"] == "stream" and "RB" not in share["of"]
    assert [(L["members"], rid(built, L["by"])) for L in share["lifted"]] == [(["RB"], f"{VR}.r9")]
    # answers 2.8: the card is water-first (headline, lifts, total line; test_answers_28_card.py)
    assert g["headline"]["says"] == "2 a day from this stream"
    assert [L["says"] for L in g["lifts"]] == [VEDDER_LIFT]
    assert g["total_line"] == "No more than 4 trout and char a day per angler across Region 2, lakes included."
    assert row["elsewhere"]["says"] == "Shared with every Region 2 stream you fish today."
    # ROUND12 4: the examples are the card's one list; the stream-tally one is the worked keep-N count
    # read backwards, so the lifted water drops it
    assert row["elsewhere"]["ask"] == []
    assert [[w["q"], w["a"]] for w in g["worked"] if w["at"] != "here"][:1] == [
        ["Kept 1 at a lake?", "Keep 3 more here. Fish from lakes count toward the 4 a day, not toward the 2 from streams."]]
    # the hatchery rainbow's own row names no second group, and says what it counts toward
    own = row_of(built, t, f"{VR}.r9")
    assert own["group"] is None
    assert own["elsewhere"]["says"].endswith("They also count toward Region 2’s 4 trout and char a day.")
    # MUTATION: were r4 not lifted for the hatchery rainbow, the 2 would hold it and nothing is excepted
    m = row_of(built, card(built, VEDDER, MOUTH, md, speak(f"{Z2}.r4", "RB", "hatchery")), f"{Z2}.r1")
    assert not subcap(built, m["group"], f"{Z2}.r4")["lifted"]
    assert m["group"]["worked"][0]["a"] == "None more here"
    assert m["group"]["headline"]["says"] == "2 a day from this stream"
    assert m["group"]["lifts"] == [] and not [w for w in m["group"]["worked"] if w["at"] == "here"]


@pytest.mark.needs_bundle
def test_f14_one_group_per_total_and_every_keep_row_says_where_else_counts(built):
    for md in ((1, 15), (5, 15), (10, 10)):
        t = card(built, VEDDER, MOUTH, md)
        pools = [r["group"]["pool"] for r in t["rows"] if r.get("group")]
        assert len(pools) == len(set(pools))
        assert all(r.get("elsewhere") for r in t["rows"] if r["kind"] == "keep")
        for r in t["rows"]:
            M.Row.model_validate_json(json.dumps(r))


def test_f14_the_group_model_refuses_a_subcap_of_a_kind_outside_the_total():
    ok = {"pool": 1, "total": 4, "kinds": ["CT", "RB"], **{"headline": {"n": 4, "unit": "a day", "says": "4 a day"}, "origin": None, "lifts": [],
              "total_line": None, "worked": [], "not_kept": [], "states": [{"r": 1, "line": "headline"}],
              "buckets": [{"title": None, "cap": 4, "by": 1, "why": "total",
                           "kinds": [{"species": ["CT", "RB"], "name": "Cutthroat and rainbow trout",
                                      "detail": "any size", "n": 4, "notes": [], "duties": []}],
                           "groups": [], "notes": [], "replaces": None}],
              # ROUND16 1: the table and cap system (empty systems are valid shapes)
              "table": {"bands": {S: [{"from": 0, "to": None, "says": "any size", "from_in": True, "to_in": False}] for S in ("CT", "RB")},
                        "cells": {S: [{"n": 4, "why": None, "origin": None, "by_origin": None, "as": None}]
                                  for S in ("CT", "RB")}, "steelhead_cm": None},
              # ROUND18 1/4: the tree and the header
              "tree": [{"node": "tier", "id": "t1", "tier": "total", "n": 4, "says": "4 a day", "by": 1,
                        "children": [{"node": "kind", "species": ["CT", "RB"], "name": "Cutthroat and rainbow trout",
                                      "n": 4, "says": "Up to 4 may be cutthroat or rainbow trout", "detail": "any size",
                                      "sizes": {"cm": [[0, None]], "in": [[0, None]]}, "also_counts": [],
                                      "duties": [], "sentence": "Of the 4 a day: up to 4 may be cutthroat or rainbow trout (any size)"}]}],
              "header": {"says": "4 a day", "tag": None},
              "cap_kinds": [], "caps": []},
          "subcaps": [{"r": 2, "take": 2, "water": "stream", "over_cm": None, "under_cm": None,
                       "of": ["CT"], "lifted": [{"members": ["RB"], "by": 3}]}]}
    M.GroupTotal.model_validate_json(json.dumps(ok))
    with pytest.raises(Exception):
        M.GroupTotal.model_validate_json(json.dumps({**ok, "subcaps": [{**ok["subcaps"][0],
                                                                         "of": ["DV"]}]}))
    with pytest.raises(Exception):
        M.GroupTotal.model_validate_json(json.dumps({**ok, "kinds": ["CT"], "subcaps": []}))


def test_f14_the_page_reads_the_group_sentence_and_the_kept_elsewhere_line():
    """Page v36 shows `group.says` and `elsewhere` — it no longer words a total or a worked
    example from the row's (narrowed) number."""
    js = (Path(E.__file__).parent / "reference" / "page_v36.js").read_text()
    assert "a day in total, all kinds together" not in js
    assert "Keep 0 here" not in js and "more here</strong>" not in js
    assert "row.raw.group" in js and "row.raw.elsewhere" in js


# --------------------------------------------------------------------------------------------
# F13 a size release is an answer
# --------------------------------------------------------------------------------------------

@pytest.mark.needs_bundle
def test_f13_vedder_may_15_hatchery_rainbow_50_cm_or_less_and_hatchery_cutthroat_are_released(built):
    """p.22 (a): "hatchery rainbow trout catch and release (50 cm or less), and hatchery cutthroat
    catch and release, May 1-31". Over 50 cm a rainbow is a steelhead here: 2 hatchery a day."""
    t = card(built, VEDDER, MOUTH, (5, 15))
    rb, ct, st = (t["fish"][S]["hatchery"] for S in ("RB", "CT", "ST"))
    assert rb["status"] == "release" and rid(built, rb["win"]) == f"{VR}.r6"
    assert ct["status"] == "release" and rid(built, ct["win"]) == f"{VR}.r7"
    assert st["status"] == "keep" and st["daily"] == 2
    row = row_of(built, t, f"{Z2}.r1")
    back = {tuple(c["who"]): rid(built, c["r"]) for c in row["conds"] if c["c"] == "back"}
    assert back[("RB",)] == f"{VR}.r6" and back[("CT",)] == f"{VR}.r7"
    assert "RB" in row["all_members"] and "CT" in row["all_members"]
    # MUTATION: without r6 speaking, no rule speaks for the hatchery rainbow (the old null)
    m = card(built, VEDDER, MOUTH, (5, 15), silence(f"{VR}.r6", "RB"))
    assert m["fish"]["RB"]["hatchery"] is None


def _stub_part(steelhead_water):
    return SimpleNamespace(steelhead_water=steelhead_water)


def _size_rule(lengths):
    x = {"lengths": lengths, "take": None}
    return SimpleNamespace(k="size", rank=0, key="e::r", x=x, f=x)


def test_f13_a_size_release_answers_only_when_it_releases_the_whole_range():
    r = _size_rule([{"max_cm": 50, "take": 0, "closed": True}])
    # on a steelhead water a rainbow's range ends at 50 cm: released whole
    assert RW._size_release(_stub_part(True), "RB", [r]) is r
    # elsewhere the rainbow over 50 cm has no answer: refused, never a fallback
    with pytest.raises(C.AnswersError):
        RW._size_release(_stub_part(False), "RB", [r])
    assert RW._size_release(_stub_part(False), "RB", []) is None


# --------------------------------------------------------------------------------------------
# T3 as clarified (user, 2026-10-10)
# --------------------------------------------------------------------------------------------

@pytest.mark.needs_bundle
@pytest.mark.parametrize("item,zone,share,lifter,fish", [
    (GLENN, "z4:trout_char_quota::trout_char_quota.r1", "z4:trout_char_quota::trout_char_quota.r3",
     "duncan_river.r4", "RB"),
    (HUMPHRYS, "z6:trout_char_quota::trout_char_quota.r1", "z6:trout_char_quota::trout_char_quota.r4",
     "kitimat_river.r4", "RB"),
])
def test_t3_a_global_star_row_carries_its_exemption_to_its_tributaries(built, item, zone, share,
                                                                       lifter, fish):
    """RULINGS T3 (clarified 2026-10-10): a global ✱ carries the row's exemptions to the
    tributaries too — the Duncan's and Kitimat's rainbow quotas walk WITH "replaces '2 from
    streams'" / the stream share they lift, so on a ✱ tributary the rainbow is out of the share
    and still counts toward the region's total."""
    t = card(built, item, 0, (10, 10))
    g = row_of(built, t, zone)["group"]
    lifts = subcap(built, g, share)["lifted"]
    assert any(fish in L["members"] and rid(built, L["by"]).endswith(lifter) for L in lifts)
    assert fish in g["kinds"]
    # the walk carried it: the lifter is a TRIBUTARY member of the part's rule set
    data, model, B = built
    k = model.parts[item][0]
    rk = C.rule_key(model.keys[k])
    assert any(r == lifter and v == "trib" for _, r, v in B.sets[rk.set_id])
    # MUTATION: the stream share speaking for the fish takes the exemption away
    m = row_of(built, card(built, item, 0, (10, 10), speak(share, fish, "hatchery")), zone)["group"]
    assert not any(fish in L["members"] for L in subcap(built, m, share)["lifted"])
