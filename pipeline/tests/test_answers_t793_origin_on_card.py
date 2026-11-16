"""T793 (general audit 2026-11-16, answers A1/A2): a kind keeping BOTH origins says, on its card line, where one origin
keeps less. Comox Lake (p.15): "No wild rainbow trout over 50 cm" — the stored row held it in its conditions and caps, but
the card read "Up to 4 brown, cutthroat or rainbow trout combined" with no "wild" anywhere; Dodd Lake (p.23): "Wild
trout/char daily quota = 2 (no wild trout over 40 cm)" — the card said Region 2's 4. The words come from the group's
table (`caps.system`, gate-proved cell by cell) in the caps' grammar; the card gate proves each clause again
(`card_check.check_tree`); the answers build stops on a row whose origin condition the card does not say
(`tree.origin_unsaid`).

The cards stand on data built here: the answers 2.8 fixture's plain Region 2 lake plus the bundle's records of
`comox_lake.r1` and `dodd_lake.r1/r2` (verbatim, copied onto Region 2 lake entries), run through the REAL reader and the
REAL rows.
"""
from __future__ import annotations

import copy
import json

from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import card_check as CC
from pipeline.deliver.answers import rows as RW
from pipeline.deliver.answers.tree import origin_clauses, origin_unsaid, walk
from pipeline.deliver.bundle import read
from pipeline.tests import answers28_fixture as F

PATH = "fixture:t793_origin_on_card"
OCT10 = (10, 10)
COMOX, DODD = "r2:comox_shape_lake@2-3", "r2:dodd_lake@2-12"

#: the bundle's records (fields as the bundle stores them), each on a Region 2 lake entry
_ROWS = {
    COMOX: ("Comox Lake", [
        ("comox_lake.r1", {"label": "Rainbow trout (none over 50 cm), wild only", "species": ["RB"], "take": None,
                           "dimension": "daily/size@origin=wild", "origin": "wild", "may_target": 1,
                           "lengths": [{"min_cm": 50, "take": 0}],
                           "verbatim": "No wild rainbow trout over 50 cm"})]),
    DODD: ("Dodd Lake", [
        ("dodd_lake.r1", {"label": "Trout and char — 2 per day, all species combined, wild only",
                          "species": ["TROUT_CHAR"], "take": 2, "dimension": "daily@origin=wild", "origin": "wild",
                          "verbatim": "Wild trout/char daily quota = 2"}),
        ("dodd_lake.r2", {"label": "Trout and char (none over 40 cm), wild only", "species": ["TROUT_CHAR"],
                          "take": None, "dimension": "daily/size@origin=wild", "origin": "wild", "may_target": 1,
                          "lengths": [{"min_cm": 40, "take": 0}], "verbatim": "no wild trout over 40 cm"})]),
}
_SETS = {COMOX: 1, DODD: 2}


def _bundle():
    got = read._RULES_BY_PATH.get(PATH)
    if got is None:
        rules = json.loads(F.FIXTURE.read_text(encoding="utf-8"))["rules"]
        got = {(x["entry"], x["rule"]): x for x in rules}
        for e, (name, rows) in _ROWS.items():
            for r, f in rows:
                got[(e, r)] = {"entry_id": e, "rule_id": r, "type": "retention_limit", "family": "retention",
                               "parts": json.dumps({"what": f["label"]}), "scope": "section", "species_except": [],
                               "uncertain": 0, "unresolved": None, "extent_text": None, "undrawn_part": None,
                               "extents": [{"op": "whole"}], "period": "daily", "rule": r, "entry": e,
                               "entry_name": name, **f}
        for x in got.values():
            x["_rank"] = read.source_of(x).rank
        read._RULES_BY_PATH[PATH] = got
    lift = tuple(F.LIFT.split("::"))
    lake = [(e, r) for (e, r), x in got.items() if (e, r) != lift and x.get("water") != "stream" and e not in _ROWS]
    sets = {sid: sorted((e, r, "reach") for e, r in lake + [(e, r) for (e, r) in got if e == entry])
            for entry, sid in _SETS.items()}
    return F._B(rules=got, sets=sets)


def _card(entry: str):
    B = _bundle()
    bound = B.sets[_SETS[entry]]
    lad = {f: {o: A.ladder_verdict(read.effective_rules_bound(
        bound, False, OCT10, f, PATH, steelhead_rules_here=True, origin=None if o == "none" else o, trace=True))
        for o in F.ORIGINS} for f in F.FISH}
    P = RW.Part(B, _SETS[entry], False, True, "lake", None, lad, OCT10, {})
    return RW.produce(P), P


def _group(out: dict) -> dict:
    (g,) = [r["group"] for r in out["rows"] if r.get("group") and "RB" in r["group"]["kinds"]]
    return g


def _kinds(g: dict) -> dict:
    return {tuple(n["species"]): n for n, _p in walk(g["tree"]) if n["node"] == "kind"}


def test_comox_says_no_wild_rainbow_over_50_on_the_card():
    out, _P = _card(COMOX)
    k = _kinds(_group(out))
    rb = next(n for sp, n in k.items() if "RB" in sp)
    assert rb["says"] == ("Up to 4 brook, brown, cutthroat or rainbow trout combined — "
                          "no wild rainbow trout over 50 cm (20 in)")
    assert rb["sentence"] == ("Up to 4 brook, brown, cutthroat or rainbow trout combined, any size — "
                              "no wild rainbow trout over 50 cm (20 in)")
    assert rb["folded"] is False


def test_dodd_says_the_wild_two_and_none_over_40_on_the_card():
    out, _P = _card(DODD)
    k = _kinds(_group(out))
    rb = next(n for sp, n in k.items() if "RB" in sp)
    assert rb["says"] == ("Up to 4 brook, brown, cutthroat or rainbow trout combined — "
                          "up to 2 wild brook, brown, cutthroat or rainbow trout combined 40 cm (16 in) or less; "
                          "no wild brook, brown, cutthroat or rainbow trout over 40 cm (16 in)")


def test_a_plain_region_2_lake_says_no_origin():
    out, _P = F.card(F.LAKE, OCT10)
    for n, _p in walk(F.trout_group(out)["tree"]):
        if n["node"] == "kind":
            assert "wild" not in n["says"] and "hatchery" not in n["says"], n["says"]


def test_the_card_gate_proves_the_clauses_and_refuses_a_dropped_or_wrong_one():
    for entry in (COMOX, DODD):
        out, P = _card(entry)
        g = _group(out)
        assert CC.check_tree(P, g) == [], entry
        dropped = copy.deepcopy(g)
        for n, _p in walk(dropped["tree"]):
            if n["node"] == "kind":
                n["says"] = n["says"].split(" — ")[0]
        assert any("not what one origin keeps less of" in p for p in CC.check_tree(P, dropped)), entry
    out, P = _card(DODD)
    g = copy.deepcopy(_group(out))
    for n, _p in walk(g["tree"]):
        if n["node"] == "kind" and " — up to 2 wild" in n["says"]:
            n["says"] = n["says"].replace("up to 2 wild", "up to 3 wild")
    assert any("states [3.0]" in p for p in CC.check_tree(P, g))


def test_origin_clauses_read_the_table_by_the_boundary_standard():
    def band(a, b, fi, ti, says):
        return {"from": a, "to": b, "from_in": fi, "to_in": ti, "says": says}

    def cell(n, origin=None, by=None, as_=None):
        return {"n": n, "why": None, "origin": origin, "by_origin": by, "as": as_}
    table = {"bands": {"RB": [band(0, 30, True, False, "under 30 cm (12 in)"),
                              band(30, 50, True, True, "30–50 cm (12–20 in)"),
                              band(50, None, False, False, "over 50 cm (20 in)")]},
             "cells": {"RB": [cell(5, origin="hatchery"), cell(5, by={"hatchery": 5, "wild": 1}),
                              cell(2, origin="hatchery", as_="ST")]}}
    assert [c["says"] for c in origin_clauses(["RB"], table)] == [            # the Kitimat's rainbow, Jul 15
        "no wild rainbow trout under 30 cm (12 in)", "up to 1 wild rainbow trout 30–50 cm (12–20 in)"]
    assert origin_clauses(["RB"], None) == []


def test_the_build_check_names_a_row_whose_card_leaves_the_origin_out():
    out, _P = _card(COMOX)
    assert origin_unsaid(out) == []
    bad = copy.deepcopy(out)
    for r in bad["rows"]:
        for n, _p in walk((r.get("group") or {}).get("tree") or []):
            if n["node"] == "kind":
                n["says"] = n["says"].split(" — ")[0]
    got = origin_unsaid(bad)
    assert len(got) == 1 and got[0].startswith("row ") and "(RB: wild origin2) is drawn as" in got[0], got
