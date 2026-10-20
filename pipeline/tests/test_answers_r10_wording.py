"""ROUND 10 WORDING (owner review of v11/v36, 2026-10-16) — no data, CI's no-data tier.

  W1  a water lifting several kinds out of the share says it in ONE line, each kind with its number
      (the Duncan repeated "… replaces Region 4’s 2-from-streams limit" once per kind), and the
      headline's "or N if they’re …" lists every lifted number above it, highest first;
  W2  a part with a main run never mentions its side channels ("From the mouth up to the CNR Mile 66
      Trestle and 1 side channel", the Cowichan); a part that IS side channels keeps its words;
  W3  no label cuts a name mid-way ("Sikanni Chief River… → B.C. border", the Fontas).

W1 runs on `answers28_fixture` (the real reader and rows): TWO is the Vedder with a made-up second lift
of its own row (hatchery cutthroat trout 3), the Duncan's shape.
"""
from __future__ import annotations

import json

from pipeline.deliver.answers import card_check as CC
from pipeline.deliver.answers import display as D
from pipeline.deliver.answers import model as M
from pipeline.tests import answers28_fixture as F

OCT10 = (10, 10)


def _g(water):
    out, _P = F.card(water, OCT10)
    return F.trout_group(out)


# W1 --------------------------------------------------------------------------------------------
def test_w1_one_lift_line_per_water_naming_each_kind_with_its_number():
    g = _g(F.TWO)
    assert [L["says"] for L in g["lifts"]] == [
        "On this river, its own rule replaces Region 2’s 2-from-streams limit for hatchery rainbow "
        "trout 50 cm (20 in) or less (up to 4 a day) and hatchery cutthroat trout (up to 3 a day)."]
    L = g["lifts"][0]
    assert (L["by"], L["kinds"], L["n"]) == (F.LIFT, ["RB", "CT"], 4)
    assert L["each"] == [
        {"kinds": ["RB"], "n": 4, "by": F.LIFT, "fish": "hatchery rainbow trout 50 cm (20 in) or less"},
        {"kinds": ["CT"], "n": 3, "by": F.LIFT2, "fish": "hatchery cutthroat trout"}]


def test_w1_the_headline_lists_every_lifted_number_above_it_highest_first():
    assert _g(F.TWO)["headline"]["also"] == [
        {"n": 4, "says": "4 if they’re hatchery rainbow trout 50 cm (20 in) or less"},
        {"n": 3, "says": "3 if they’re hatchery cutthroat trout"}]


def test_w1_the_worked_count_follows_the_main_lift_and_the_gate_proves_the_card():
    out, P = F.card(F.TWO, OCT10)
    g = F.trout_group(out)
    assert [(w["q"], w["a"]) for w in g["worked"] if w["at"] == "here"] == [
        ("If you keep 4 hatchery rainbow trout here", "that’s your trout and char for the day, everywhere."),
        ("If you keep 2 hatchery rainbow trout here", "they count as your 2 from streams anywhere else today — up to 2 more "
                                                 "of any kind, from here or from lakes.")]
    assert CC.check_card(P, out) == []
    ix = {}
    def r(k):
        return ix.setdefault(k, len(ix))
    M.GroupTotal.model_validate_json(json.dumps(CC_ix(g, r)))


def CC_ix(g, r):
    from pipeline.deliver.answers.tree import map_rules
    """The group with every rule id an index again (what the answers ship)."""
    return {**g, "pool": r(g["pool"]),
            "subcaps": [{**s, "r": r(s["r"]), "lifted": [{**L, "by": r(L["by"])} for L in s["lifted"]]}
                        for s in g["subcaps"]],
            "lifts": [{**L, "by": r(L["by"]), "each": [{**e, "by": r(e["by"])} for e in L["each"]]}
                      for L in g["lifts"]],
            "buckets": [{**b, "by": r(b["by"]), "sizes": [{**z, "by": r(z["by"])} for z in b.get("sizes") or []],
                         "groups": [{**z, "by": r(z["by"])} for z in b.get("groups") or []],
                         "replaces": None if b["replaces"] is None else
                         {**b["replaces"], "rules": [r(k) for k in b["replaces"]["rules"]]}} for b in g["buckets"]],
            "not_kept": [{**x, "by": r(x["by"])} for x in g["not_kept"]],
            "worked": [{**w, "share": None if w["share"] is None else r(w["share"])} for w in g["worked"]],
            "states": [{**x, "r": r(x["r"])} for x in g["states"]],
            "caps": [{**c, "by": r(c["by"])} for c in g["caps"]],
            "tree": map_rules(g["tree"], r)}


def test_w1_one_lift_reads_exactly_as_before():
    g = _g(F.VEDDER)
    assert [L["says"] for L in g["lifts"]] == [
        "On this river, hatchery rainbow trout 50 cm (20 in) or less: up to 4 a day — this river’s rule "
        "replaces Region 2’s 2-from-streams limit for them."]
    assert g["lifts"][0]["each"] == [{"kinds": ["RB"], "n": 4, "by": F.LIFT,
                                      "fish": "hatchery rainbow trout 50 cm (20 in) or less"}]
    assert g["headline"]["also"] == [{"n": 4, "says": "4 if they’re hatchery rainbow trout 50 cm (20 in) or less"}]


# W2 --------------------------------------------------------------------------------------------
DOC = {"splits": {"cnr66": {"name": "CNR Mile 66 Trestle"}}, "waters": {}}
MAIN = {"from": "cnr66", "to": "mouth", "km_from": 9.0, "km_to": 0.0}
SIDE = {"from": "source", "to": "mouth", "km_from": 4.1, "km_to": 4.1, "branch": True}


def test_w2_a_part_with_a_main_run_does_not_mention_its_side_channels():
    assert D.runs_label(DOC, {"runs": [MAIN, SIDE]}) == "From the mouth up to the CNR Mile 66 Trestle"
    assert D.runs_label(DOC, {"runs": [MAIN, SIDE, dict(SIDE, km_from=2.0)]}) == \
        "From the mouth up to the CNR Mile 66 Trestle"
    assert D.short_label(DOC, {"runs": [MAIN, SIDE]}, "x") == "Mouth → CNR Mile 66 Trestle"


def test_w2_a_part_of_side_channels_only_keeps_its_words():
    assert D.runs_label(DOC, {"runs": [SIDE]}) == "Side channel, joining 4.1 km from the mouth"
    assert D.short_label(DOC, {"runs": [SIDE]}, "x") == "Side channel"


# W3 --------------------------------------------------------------------------------------------
def test_w3_a_short_label_never_cuts_an_end_name():
    doc = {"splits": {"prnp": {"name": "Pacific Rim National Park Reserve boundary"}},
           "waters": {"gnis:1": {"name": "Sikanni Chief River"}}}
    fontas = {"runs": [{"from": "bc_border", "to": "confluence:gnis:1", "km_from": 282.6, "km_to": 0.0}]}
    assert D.short_label(doc, fontas, "x") == "Sikanni Chief River confluence → B.C. border"
    park = {"runs": [{"from": "prnp", "to": "mouth", "km_from": 3.0, "km_to": 0.0}]}
    assert D.short_label(doc, park, "x") == "Mouth → Pacific Rim National Park Reserve boundary"
    assert "…" not in D.short_label(doc, {"runs": []}, "Located immediately north and south of "
                                                        "Bluey Lake · Whole water and more words")


def test_w3_a_full_label_hint_is_never_cut():
    """Two parts with the same runs are told apart by what sets each apart, said whole."""
    where = "below the Highway 97 bridge"
    what = "no fishing for any game fish from the first of April to the end of June"
    rules = {"a": {"entry_id": "r7b:x", "family": "retention", "provenance": {"rank": 0},
                   "fields": {"species": ["RB"]}, "type": "closure", "dimension": "closure",
                   "parts": {"where": where, "what": what}, "label": what, "verbatim": what},
             "b": {"entry_id": "r7b:x", "family": "retention", "provenance": {"rank": 0},
                   "fields": {"species": ["RB"]}, "type": "retention_limit", "dimension": "daily",
                   "parts": {"what": "Rainbow trout — 2 per day"}, "label": "Rainbow trout — 2 per day",
                   "verbatim": "2"}}
    run = {"from": "source", "to": "mouth", "km_from": 9.0, "km_to": 0.0}
    w = {"parts": [{"ruleset": 0, "sections": 2, "runs": [run]}, {"ruleset": 1, "sections": 1, "runs": [run]}]}
    doc = {"rules": rules, "rulesets": [{"reach": ["a"]}, {"reach": ["b"]}], "entries": {},
           "splits": {}, "waters": {}}
    labels = D.part_labels(D.Water("w", w, doc))
    assert labels[0] == f"From the mouth up to the top of the river — {what.lower()}", labels
    assert not any("…" in t for t in labels), labels


# W4 --------------------------------------------------------------------------------------------
# The Lines row (design v12: "make clear lines in a boat and out of a boat"): a plain statement per
# water, derived from the clauses that decide it, and every rule deciding it cited (Kootenay Lake's
# boat clause was shown but never cited, so its Book page never showed). The real clauses:
PROVINCE = "zp:terminal_tackle::terminal_tackle.r1"      # "At most 2 lines (rods) per angler (lake,
KOOTENAY = "r4:kootenay_lake_main_body::kootenay_lake_main_body.r1"   # alone in a boat); at most 1"


def _lines(kind, kootenay=False):
    from pipeline.deliver.answers import gear as G
    tackle = {"family": "gear_and_method", "type": "tackle_restriction"}
    rules = [G.Rule(PROVINCE, 4, {**tackle, "gear": [
        {"max": 2.0, "slot": "lines_per_angler", "when": {"angler": "alone_in_boat", "water": "lake"}},
        {"max": 1.0, "slot": "lines_per_angler"}]}),
        G.Rule("zp:allowable_methods::allowable_methods.r2b", 4, {**tackle, "while": ["ice_fishing"],
               "gear": [{"max": 1.0, "slot": "lines_per_angler"}]})]
    if kootenay:
        rules.append(G.Rule(KOOTENAY, 0, {**tackle, "gear": [
            {"slot": "lines_per_angler", "unlimited": True, "when": {"angler": "in_boat"}}]}))
    return G.resolve(rules, kind, ["angling"])["counts"]["lines_per_angler"]


def test_w4_a_stream_says_its_one_line():
    """ROUND11 A2 (owner): on a stream the boat changes nothing, so just "1 line (rod)" (it said ",
    in a boat too")."""
    L = _lines("stream")
    assert (L["says"], L["cites"]) == ("1 line (rod)", [PROVINCE])


def test_w4_a_lake_says_two_while_alone_in_a_boat():
    L = _lines("lake")
    assert (L["says"], L["cites"]) == ("1 line (rod); 2 while alone in a boat", [PROVINCE])
    assert _lines(None)["says"] == "1 line (rod); 2 while alone in a boat on a lake"


def test_w4_kootenay_lake_cites_its_boat_rule():
    L = _lines("lake", kootenay=True)
    assert (L["says"], L["cites"]) == ("1 line (rod) from shore; no limit from a boat", [PROVINCE, KOOTENAY])
