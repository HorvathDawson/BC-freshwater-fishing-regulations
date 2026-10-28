"""ROUND 19 (owner review of v20, 2026-10-17) — the tree's grouping and words, no data, CI's no-data tier.

  1  A kind keeping one origin says it with the fish ("Up to 2 may be hatchery brook, brown or cutthroat trout"); the
     detail and the header's tag no longer repeat it.
  2  Every size reads "50 cm (20 in)" — tree, header, also_counts, sentences, details, lift lines, kept-elsewhere
     lines, table bands, cap sentences and rule titles.
  3  A size tier HOLDS every kind it caps (each that may be kept over its size), nested in the tightest tier holding
     them all; a kind that can never pass the size stays outside; RULINGS Q16's steelhead sits INSIDE ("Only 1 of
     these may be over 50 cm (20 in) — or 2, if one is a hatchery steelhead"). The gate proves the placement.
  5  The boundary standard: "X or less" / "X or longer" hold X, "under X" / "over X" do not; a fish exactly at a cut
     belongs to the band whose words include it (table bands and cap kinds carry `from_in` / `to_in`).
  6  "1 cutthroat or rainbow trout in all"; "Keep hatchery fish only — wild trout and char go back."; replaces
     names the fish ("for hatchery rainbow trout"); the nesting in the words ("2 a day from this stream (part of the
     4)").
"""
from __future__ import annotations

import copy

from pipeline.deliver.answers import card_check as CC
from pipeline.deliver.answers import common, display
from pipeline.tests import answers28_fixture as F

OCT10, MAY1 = (10, 10), (5, 1)


def _walk(nodes):
    for n in nodes:
        yield n
        yield from _walk(n.get("children") or [])


def _g(water, md=OCT10):
    out, P = F.card(water, md)
    (g,) = [r["group"] for r in out["rows"] if r.get("group") and set(r["group"]["kinds"]) & {"RB", "EB", "GB"}]
    return g, out, P


# 2 ---------------------------------------------------------------------------------------------
def test_2_sizes_read_cm_then_inches_in_brackets():
    assert common.cm_in(50) == "50 cm (20 in)"
    assert common.range_cm_in(25, 33) == "25–33 cm (10–13 in)"
    x = {"type": "retention_limit", "species": ["RB"], "take": 2, "lengths": [{"max_cm": 30, "take": 0}]}
    assert display.title(x) == "Keep up to 2 rainbow trout a day (none under 30 cm (12 in))"
    slot = {"type": "retention_limit", "species": ["LT"], "lengths": [{"min_cm": 60, "max_cm": 90, "take": 0}]}
    assert display.title(slot) == "No lake trout over 60 cm (24 in), under 90 cm (35 in)"


def test_2_lift_and_kept_elsewhere_lines_say_inches_in_brackets():
    g, out, _P = _g(F.VEDDER)
    assert g["lifts"][0]["says"].startswith("On this river, hatchery rainbow trout 50 cm (20 in) or less: up to 4 a day")
    lines = [w["q"] for w in g["worked"]] + [w["a"] for w in g["worked"]]
    assert not [x for x in lines if " / " in x]


# 1, 3, 6: the Vedder ---------------------------------------------------------------------------
def test_3_the_vedder_tree():
    g, _o, _P = _g(F.VEDDER)
    # ROUND20 1: the header IS the root tier
    assert g["header"] == {"says": "4 a day per angler across Region 2, lakes included", "tier": "t1", "tag": None}
    assert g["origin"] is None                                   # ROUND20g 1: the share's tier says it
    (total,) = g["tree"]
    share, own = total["children"]                               # ROUND20b C3: the share first
    assert own["replaces"] == ("Replaces Region 2’s 2-from-streams limit and 30 cm (12 in) minimum size for hatchery "
                               "rainbow trout")
    (rb,) = own["children"]
    assert (rb["says"], rb["detail"]) == ("Up to 4 may be hatchery rainbow trout", "any length, 50 cm (20 in) or less")
    assert rb["sentence"] == "Of the 4 a day: up to 4 may be hatchery rainbow trout, any length, 50 cm (20 in) or less"
    assert share["says"] == "Only 2 of the 4 may be from streams"   # ROUND UR7 A: the lift said once, by the own tier's relation   # ROUND20 2
    assert share["origin"] == "From streams, keep hatchery fish only — wild trout and char go back."   # ROUND20g 1
    (size,) = share["children"]           # every kind of the share may pass 50 cm: the size tier holds them all
    assert size["says"] == "Of these, only 1 may be over 50 cm (20 in) — 2 if one is a hatchery steelhead"
    assert (size["extra"]["n"], size["extra"]["species"]) == (1, ["ST"])
    trout, dv, st = size["children"]
    # T128: a kind's line names the total it is part of
    assert trout["sentence"] == ("Of your 2 trout and char a day in streams, up to 2 may be hatchery brook, brown or "
                                 "cutthroat trout, 30 cm (12 in) or longer")
    assert trout["also_counts"] == []     # the grouping says the cross-kind over-50 line
    assert dv["sentence"] == ("Of your 2 trout and char a day in streams, only 1 may be hatchery Dolly Varden/bull trout "
                              "or lake trout, 60 cm (24 in) or longer")
    # ROUND20g 2: the size tier's "— 2 if one is a hatchery steelhead" says it once; no size note on the kind
    assert st["sentence"] == ("Of your 2 trout and char a day in streams, up to 2 may be hatchery steelhead, rainbow trout "
                              "over 50 cm (20 in)")
    assert "size_note" not in st


def test_3_a_kind_that_cannot_pass_the_size_stays_outside_and_a_count_tier_names_its_fish():
    g, _o, _P = _g(F.BELLA_MAY_ST, MAY1)
    (own,) = g["tree"][0]["children"]
    assert own["says"] == "This river’s rule: up to 2 a day (part of the 4)"
    size, count = own["children"]
    assert size["says"] == "Of these, only 1 may be over 50 cm (20 in)"
    assert [k["species"] for k in size["children"]] == [["EB", "GB"], ["DV", "LT"]]
    assert count["says"] == "1 cutthroat or rainbow trout in all (part of the 2)"     # UR3 3; ≤33 and ≤50 cm: never over 50
    # T128: a kind under a count tier names the total around it (the count tier's line says the 1 in all)
    assert [k["sentence"] for k in count["children"]] == [
        "Of your 2 trout and char a day under this river’s rule, only 1 may be hatchery cutthroat trout, 30–33 cm (12–13 in)",
        "Of your 2 trout and char a day under this river’s rule, only 1 may be hatchery rainbow trout, 30–50 cm (12–20 in)"]


def test_3_a_size_tier_spanning_tiers_wraps_them():
    g, _o, _P = _g(F.CLAKE)             # Chilliwack Lake's own Dolly Varden rule under Region 2's 1 over 50 cm
    (size,) = g["tree"][0]["children"]
    assert size["says"] == "Of these, only 1 may be over 50 cm (20 in)"
    assert [x.get("tier") or x["species"] for x in size["children"]] == ["own", ["EB", "GB", "CT", "RB"], ["LT"]]


def test_3_a_count_tier_crossing_the_cap_is_split_not_said_on_its_kinds():
    # T203 (owner 2026-10-28) overrules ROUND19's "no tier": the count tier holds a cutthroat that may pass 50 cm beside
    # a rainbow that never may — the size tier takes the cutthroat (in its count tier), the rainbow leaves both
    g, _o, _P = _g(F.COUNT)
    (size,) = [t for t in _walk(g["tree"]) if t.get("tier") == "size"]
    ct = next(k for k in _walk(size["children"]) if k["node"] == "kind" and k["species"] == ["CT"])
    assert ct["also_counts"] == []
    assert not [k for k in _walk(size["children"]) if k["node"] == "kind" and k["species"] == ["RB"]]


# 5 ---------------------------------------------------------------------------------------------
def test_5_the_boundary_standard_on_the_table_bands():
    g, _o, _P = _g(F.VEDDER)
    bands = g["table"]["bands"]
    assert [(b["says"], b["from_in"], b["to_in"]) for b in bands["RB"]] == [
        ("50 cm (20 in) or less", True, True), ("over 50 cm (20 in)", False, False)]     # p.80: OVER 50 is steelhead
    assert [b["says"] for b in bands["CT"]] == ["under 30 cm (12 in)", "30–50 cm (12–20 in)", "over 50 cm (20 in)"]
    assert [b["says"] for b in bands["DV"]] == ["under 60 cm (24 in)", "60 cm (24 in) or longer"]
    g, _o, _P = _g(F.SLOT)           # none between 60 and 90 cm: both bounds legal
    assert [b["says"] for b in g["table"]["bands"]["RB"]] == [
        "60 cm (24 in) or less", "over 60 cm (24 in), under 90 cm (35 in)", "90 cm (35 in) or longer"]
    assert all({"from_in", "to_in"} <= set(k) for k in g["cap_kinds"])


def _mutated(fn, water=F.VEDDER, md=OCT10):
    out, P = F.card(water, md)
    (g,) = [copy.deepcopy(r["group"]) for r in out["rows"] if r.get("group") and "RB" in r["group"]["kinds"]]
    fn(g)
    return CC.check_group(P, g)


def test_the_gate_refuses_a_wrong_placement_or_band():
    def bound_outside(g):                        # brook/brown/cutthroat out of the over-50 tier
        share = g["tree"][0]["children"][0]
        share["children"].append(share["children"][0]["children"].pop(0))
    def never_over_inside(g):                    # the rainbow (50 cm or less) into the over-50 tier
        share, own = g["tree"][0]["children"]
        share["children"][0]["children"].append(own["children"].pop(0))
    def steelhead_outside(g):                    # Q16's steelhead out of the tier that lets it past
        share = g["tree"][0]["children"][0]
        share["children"].append(share["children"][0]["children"].pop(2))
    def no_tier(g):                              # the size tier dropped, its kinds left in the share
        share = g["tree"][0]["children"][0]
        share["children"] = share["children"][0]["children"]
    def extra(g):
        g["tree"][0]["children"][0]["children"][0]["extra"]["n"] = 2
    def band_cut(g):                             # the cut at 50 cm held by both bands
        g["table"]["bands"]["CT"][2]["from_in"] = True
    def band_words(g):
        g["table"]["bands"]["DV"][1]["says"] = "over 60 cm (24 in)"
    for fn in (bound_outside, never_over_inside, steelhead_outside, no_tier, extra, band_cut, band_words):
        assert _mutated(fn), fn.__name__


def test_the_gate_refuses_a_cap_said_on_kinds_a_tier_could_hold():
    def untiered(g):                             # Region 2's 1 over 50 cm as kind lines on a lake with no reason
        total = g["tree"][0]
        size = total["children"].pop(0)
        for k in size["children"]:
            k["also_counts"] = [{"tier": None, "by": size["by"], "n": 1, "over_cm": 50, "then": [],
                                 "species": size["species"], "says": "only 1 over 50 cm (20 in)"}]
        total["children"][:0] = size["children"]
    assert _mutated(untiered, F.LAKE)
