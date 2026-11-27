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



def _name(doc, part):
    """T517 (owner 2026-11-09): a part's name from its end tokens, as the app's `spanName` composes it; None: no main
    stem (the app says the water's name)."""
    from pipeline.deliver.answers.display.part import end_name_of, end_tokens
    t = end_tokens(doc, part)
    return end_name_of(doc, t[0], t[1]) if t else None

def _g(water):
    out, _P = F.card(water, OCT10)
    return F.trout_group(out)


# W1 --------------------------------------------------------------------------------------------
def test_w1_one_lift_line_per_water_naming_each_kind_with_its_number():
    # (T866) TWO's one lift line, each kind's words, the headline's `also` (highest first) and the worked lines (and
    # the Vedder's one lift, "exactly as before"): test_answers_card_words.py CARDS
    L = _g(F.TWO)["lifts"][0]
    assert (L["by"], L["kinds"], L["n"]) == (F.LIFT, ["RB", "CT"], 4)


def test_w1_the_worked_count_follows_the_main_lift_and_the_gate_proves_the_card():
    out, P = F.card(F.TWO, OCT10)
    g = F.trout_group(out)
    assert CC.check_card(P, out) == []
    ix = {}
    def r(k):
        return ix.setdefault(k, len(ix))
    M.GroupTotal.model_validate_json(json.dumps(CC_ix(g, r)))


def CC_ix(g, r):
    from pipeline.deliver.answers.tree import map_rules
    """The group with every rule id an index again (what the answers ship)."""
    h = g["headline"]
    return {**g, "pool": r(g["pool"]),
            "headline": h if h.get("info") is None else {**h, "info": {**h["info"], "by": r(h["info"]["by"])}},
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


# W2 --------------------------------------------------------------------------------------------
DOC = {"splits": {"cnr66": {"name": "CNR Mile 66 Trestle"}}, "waters": {}}
MAIN = {"from": "cnr66", "to": "mouth", "km_from": 9.0, "km_to": 0.0}
SIDE = {"from": "source", "to": "mouth", "km_from": 4.1, "km_to": 4.1, "branch": True}


def test_w2_a_part_with_a_main_run_does_not_mention_its_side_channels():
    assert D.runs_label(DOC, {"runs": [MAIN, SIDE]}) == "From the mouth up to the CNR Mile 66 Trestle"
    assert D.runs_label(DOC, {"runs": [MAIN, SIDE, dict(SIDE, km_from=2.0)]}) == \
        "From the mouth up to the CNR Mile 66 Trestle"
    assert _name(DOC, {"runs": [MAIN, SIDE]}) == "Mouth → CNR Mile 66 Trestle"


def test_w2_a_part_of_side_channels_only_keeps_its_words():
    # (T1172) the one such part of its water says no distance ("Distance only to tell parts apart")
    assert D.runs_label(DOC, {"runs": [SIDE]}) == "Side channel"
    assert _name(DOC, {"runs": [SIDE]}) is None                    # no main stem: the water's name (app)


# W3 --------------------------------------------------------------------------------------------
def test_w3_a_short_label_never_cuts_an_end_name():
    doc = {"splits": {"prnp": {"name": "Pacific Rim National Park Reserve boundary"}},
           "waters": {"gnis:1": {"name": "Sikanni Chief River"}}}
    fontas = {"runs": [{"from": "bc_border", "to": "confluence:gnis:1", "km_from": 282.6, "km_to": 0.0}]}
    assert _name(doc, fontas) == "Sikanni Chief River confluence → B.C. border"
    park = {"runs": [{"from": "prnp", "to": "mouth", "km_from": 3.0, "km_to": 0.0}]}
    assert _name(doc, park) == "Mouth → Pacific Rim National Park Reserve boundary"
    assert _name(doc, {"runs": []}) is None


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
# The Lines row (design v12: "make clear lines in a boat and out of a boat"): a plain statement per water, derived
# from the clauses that decide it, and every rule deciding it cited (Kootenay Lake's boat clause was shown but never
# cited, so its Book page never showed). ROUND11 A2 (owner): on a stream the boat changes nothing, so just "1 line
# (rod)"; a lake says two while alone in a boat; Kootenay Lake cites its boat rule. (T866) The clause set and every
# line it says, with the rules it cites: test_answers_card_words.py GEAR ("lines: ...").
