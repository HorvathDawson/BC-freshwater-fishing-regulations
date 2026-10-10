"""ANSWERS 2.3 (2026-10-09): the possession quota's exception in words (Q15/Q39), the national park
closure's proviso (Z12/Q41), and the three answers/2 gaps the page v36 still computed itself — other
anglers' and guides' requirements with their printed paths (G2), a rule said for some of its fish
(G3), and each fish's keep range on a cross-reference of several fish (G4). Q38 (the snag duty) is
`test_snag_duty.py`.

The pure tests need no data; the data tests read the live set (`ANSWERS_EXPORT_DIR` /
`UI_EXPORT_BUNDLE` point at a side set).
"""
from __future__ import annotations

import json
import os
import re
from functools import lru_cache
from pathlib import Path

import pytest

from pipeline.deliver.answers import display as X
from pipeline.deliver.answers import licence as L
from pipeline.deliver.answers import model as M
from pipeline.deliver.answers.common import lc, lc_names, load_export, sp_name
from pipeline.deliver.bundle import read as R
from pipeline.tests.conftest import EXPORT_HINT, need

EXPORT_DIR = Path(os.environ.get("ANSWERS_EXPORT_DIR") or Path(R.BUNDLE).parent.parent / "regs")
ANSWERS = Path(__file__).resolve().parents[1] / "deliver" / "answers"
PARK = "zp:superior_closures::superior_closures.r1"
RESERVES = "zp:national_park_reserves::national_park_reserves.r1"
PARK_WORDS = "Closed unless opened by Parks Canada — a national park fishing permit is required."


# --------------------------------------------------------------------------------------------
# Q15 / Q39 — the possession quota says "fish at home don't count", in one wording
# --------------------------------------------------------------------------------------------

@pytest.mark.parametrize("x, want", [
    ({"type": "retention_limit", "species": ["ALL_GAME_FISH"], "period": "possession",
      "per_daily": 2}, "Possession: twice the daily quota (fish at home don’t count)."),
    ({"type": "retention_limit", "species": ["KO"], "period": "possession", "per_daily": 1},
     "Possession: the daily quota for kokanee (fish at home don’t count)."),
    ({"type": "retention_limit", "species": ["LT"], "period": "possession", "take": 1,
      "lengths": [{"min_cm": 60}]},
     "Only 1 lake trout over 60 cm in possession (fish at home don’t count)."),
    ({"type": "retention_limit", "species": ["GR"], "period": "possession", "take": 1},
     "Have no more than 1 Arctic grayling in possession (fish at home don’t count)."),
])
def test_every_possession_line_says_fish_at_home_dont_count(x, want):
    assert X.plain(x) == want


def test_the_possession_exception_is_worded_once():
    """One source: the words live in `display.POSSESSION_HOME` and nowhere else in the answers."""
    hits = [p.name for p in ANSWERS.glob("*.py") if "at home don’t count" in p.read_text()]
    assert hits == ["display.py"]
    assert X.POSSESSION_HOME == "fish at home don’t count"


# --------------------------------------------------------------------------------------------
# Z12 / Q41 — "Closed unless opened by Parks Canada — a national park fishing permit is required"
# --------------------------------------------------------------------------------------------

def _park_rules():
    park = {"entry": "zp:superior_closures", "rule": "superior_closures.r1",
            "type": "retention_limit", "species": ["ALL_GAME_FISH"], "take": 0, "may_target": 0,
            "authority": "superior", "extents": [{"op": "within", "area_kind": "national_parks"}]}
    proviso = {"entry": "zp:superior_closures", "rule": "superior_closures.r1b",
               "type": "advisory", "condition_of": "superior_closures.r1"}
    reserves = {"entry": "zp:national_park_reserves", "rule": "national_park_reserves.r1",
                "type": "retention_limit", "species": ["ALL_GAME_FISH"], "take": 0,
                "may_target": 0, "authority": "superior",
                "extents": [{"op": "within", "area_id": "area:national_parks:x"}]}
    return {(x["entry"], x["rule"]): x for x in (park, proviso, reserves)}


def test_a_park_closure_with_its_proviso_says_unless_opened_and_a_reserve_does_not():
    rules = _park_rules()
    park, reserves = rules[("zp:superior_closures", "superior_closures.r1")], \
        rules[("zp:national_park_reserves", "national_park_reserves.r1")]
    assert X.unless_opened(park, rules) == PARK_WORDS
    assert X.rule_facts(park, rules)["unless_opened"] == PARK_WORDS
    assert X.unless_opened(reserves, rules) is None
    # MUTATION: without its proviso the park closure is plainly closed
    del rules[("zp:superior_closures", "superior_closures.r1b")]
    assert X.unless_opened(park, rules) is None


def test_a_proviso_on_a_closure_with_no_words_is_refused():
    rules = _park_rules()
    rules[("zp:superior_closures", "superior_closures.r1")]["extents"] = [
        {"op": "within", "area_kind": "ecological_reserves"}]
    with pytest.raises(Exception, match="no words"):
        X.unless_opened(rules[("zp:superior_closures", "superior_closures.r1")], rules)


def test_where_a_reserve_closes_the_same_fish_the_park_proviso_is_not_the_answer():
    g = ["RB", "CT"]
    words = {"park": PARK_WORDS, "reserve": None}
    sup = ["park", "reserve"]
    assert X.unless_opened_here({"park": g}, words, sup) == ["park"]
    assert X.unless_opened_here({"park": g, "reserve": g}, words, sup) == []
    # a plain closure of only SOME of the fish leaves the proviso standing for the rest
    assert X.unless_opened_here({"park": g, "reserve": ["CT"]}, words, sup) == ["park"]
    # a provincial closure beside it (Region 4's spring stream closure) is shown beside it: the
    # proviso stays (only the same superior authority's plain closure says "plainly closed")
    words["spring"] = None
    assert X.unless_opened_here({"park": g, "spring": g}, words, sup) == ["park"]


def test_the_frame_model_holds_unless_opened_to_the_closing_rules():
    ok = {"status": "own", "closing": [[3, ["RB"]]], "unless_opened": [3]}
    M.FRAMES["display"].validate_json(json.dumps(ok))
    for bad in ({**ok, "unless_opened": [4]}, {**ok, "unless_opened": []}):
        with pytest.raises(Exception):
            M.FRAMES["display"].validate_json(json.dumps(bad))


# --------------------------------------------------------------------------------------------
# G2 — another angler's / a guide's requirement carries how it is met, as printed
# --------------------------------------------------------------------------------------------

def test_another_anglers_requirement_carries_its_printed_paths():
    r = {"satisfied_by": [{"hold": ["basic_licence", "classified_waters_licence"]},
                          {"accompanied_by": {"holding": "basic_licence",
                                              "who": {"age": "16_plus"}}, "quota": "own"}]}
    assert L.printed(r, 5) == {"req": 5, "paths": [
        {"need": ["basic_licence", "classified_waters_licence"]},
        {"need": [], "accompanied_by": {"holding": "basic_licence", "who": {"age": "16_plus"}},
         "quota": "own"}]}
    M.TypeAdapter(M.PrintedRequirement).validate_json(json.dumps(L.printed(r, 5)))


# --------------------------------------------------------------------------------------------
# G3 — a rule said for some of its fish
# --------------------------------------------------------------------------------------------

def _frame():
    """A row of rainbow and cutthroat: rule 1 (trout/char 5) governs both, rule 2 (a rainbow
    release) speaks for the rainbow only; an item of the cutthroat alone."""
    dec = lambda roles: {"roles": roles}  # noqa: E731
    return {"fish": {"RB": {"hatchery": dec([[1, "governs", None], [2, "floor", None]]),
                            "wild": None},
                     "CT": {"hatchery": dec([[1, "governs", None]]), "wild": None}},
            "rows": [{"all_members": ["RB", "CT"], "members": ["RB", "CT"], "pool": 1,
                      "items": [{"members": ["RB"]}, {"members": ["CT"]}]}]}


def test_the_ladders_ask_for_a_rule_said_for_the_fish_it_is_listed_for():
    f = _frame()
    assert X.row_sources(f, f["rows"][0]) == [(1, ["RB", "CT"]), (2, ["RB"])]
    # the row's ladder lists rule 2 for the rainbow alone; an item's ladder adds nothing new
    assert X.subset_asks(f) == {(2, ("RB",))}


def test_a_subset_sentence_is_the_rules_own_said_for_those_fish():
    x = {"type": "retention_limit", "species": ["TROUT_CHAR"], "take": 0, "may_target": True}
    got = X.subset_says(x, ["RB", "CT"])
    assert got == {"fish": ["RB", "CT"], "for": f"for {lc_names(['RB', 'CT'])}",
                   "plain": f"Release every {lc_names(['RB', 'CT'])}."}
    # a rule with no sentence of its own gets only the "for …"
    assert "plain" not in X.subset_says({"type": "method_rule"}, ["RB"])


# --------------------------------------------------------------------------------------------
# G4 — a cross-reference of several fish carries each fish's keep range
# --------------------------------------------------------------------------------------------

def _item(**kw):
    base = {"members": ["RB", "CT"], "bands": None, "back": False, "xref": True, "sub": None,
            "conds": [], "against": 5}
    return json.dumps({**base, **kw})


def test_the_item_model_holds_ranges_to_a_cross_reference_of_several_fish():
    M.TypeAdapter(M.Item).validate_json(_item(ranges=[["RB", 30, 50], ["CT", 0, None]]))
    for bad in (_item(ranges=[["DV", 0, None]]),                           # not the item's fish
                _item(members=["RB"], ranges=[["RB", 0, None]]),           # one fish: `origins`
                _item(xref=False, against=None, ranges=[["RB", 0, None]])):
        with pytest.raises(Exception):
            M.TypeAdapter(M.Item).validate_json(bad)


# --------------------------------------------------------------------------------------------
# On a built answers file
# --------------------------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _files():
    from pipeline.tools.export_codec import expand
    need(None, "bundle", EXPORT_DIR / "ui-rules-export.json", EXPORT_HINT)
    need(None, "bundle", EXPORT_DIR / "ui-rules-answers.json", EXPORT_HINT)
    data, guide = load_export(EXPORT_DIR)
    wire = json.loads((EXPORT_DIR / "ui-rules-answers.json").read_text())
    return data, expand(data, guide), wire


@pytest.mark.needs_bundle
def test_built_every_possession_sentence_says_fish_at_home_dont_count():
    _, _, wire = _files()
    poss = [r for r in wire["sections"]["display"]["rules"]
            if r["kind"] in ("possession", "possession_cap") and r.get("plain")]
    assert len(poss) > 20
    assert all(X.POSSESSION_HOME in r["plain"] for r in poss)


@pytest.mark.needs_bundle
def test_built_a_national_park_says_unless_opened_and_a_park_reserve_is_plainly_closed():
    data, _, wire = _files()
    park, res = data["rule_ids"].index(PARK), data["rule_ids"].index(RESERVES)
    D = wire["sections"]["display"]
    assert D["rules"][park]["unless_opened"] == PARK_WORDS
    assert "unless_opened" not in D["rules"][res]
    frames = [f for f in D["frames"] if f.get("closing")]
    shut = lambda f, r: any(x == r for x, _ in f["closing"])  # noqa: E731
    parks = [f for f in frames if shut(f, park) and not shut(f, res)]
    # (a park water's own row or a region's closure may close beside it: still "unless opened")
    reserves = [f for f in frames if shut(f, res)]
    assert parks and reserves
    assert all(f.get("unless_opened") == [park] for f in parks if f["status"] == "closed")
    assert all("unless_opened" not in f for f in reserves)
    # nothing else is printed "unless opened"
    assert {r for f in frames for r in f.get("unless_opened") or []} == {park}


@pytest.mark.needs_bundle
def test_built_other_anglers_requirements_carry_their_printed_paths():
    data, doc, wire = _files()
    lic = data["licensing_ids"]
    seen = 0
    for a in wire["sections"]["licence"]["answers"]:
        for o in (a.get("others") or []) + (a.get("guiding") or []):
            f = doc["licensing"][lic[o["req"]]]["fields"]
            assert [p["need"] for p in o["paths"]] == \
                [list(s.get("hold") or []) for s in f.get("satisfied_by") or []]
            seen += 1
    assert seen


@pytest.mark.needs_bundle
def test_built_subset_sentences_name_their_fish():
    _, _, wire = _files()
    subs = [s for r in wire["sections"]["display"]["rules"] for s in r.get("subsets") or []]
    assert len(subs) > 50
    for s in subs:
        assert s["for"] == f"for {lc_names(s['fish'])}"
        if s.get("plain") and not re.search(r"No fishing|Possession", s["plain"]):
            # each fish by name, or their group's name ("trout and char", "trout or char")
            assert all(lc(sp_name(f)) in s["plain"] for f in s["fish"]) or \
                any(n in s["plain"] for n in (lc_names(s["fish"]),
                                              lc_names(s["fish"]).replace(" and ", " or "))), s


@pytest.mark.needs_bundle
def test_built_every_cross_reference_of_several_fish_carries_their_ranges():
    _, _, wire = _files()
    items = [it for r in wire["sections"]["rows"]["rows"] for it in r.get("items") or []
             if it["xref"] and len(it["members"]) > 1]
    assert items
    for it in items:
        assert it.get("ranges") is not None
        assert all(f in it["members"] for f, _, _ in it["ranges"])


def test_one_fish_of_several_kinds_is_or_and_more_is_and():
    """User 2026-10-10: a quota of 1 across several kinds keeps ANY ONE of them ("or"); more than
    one reads "and … all kinds together" — for a subset sentence (G3) as for the rule's own."""
    from pipeline.deliver.answers.display import _plain
    names = "cutthroat trout, brown trout and rainbow trout"
    x = {"type": "retention_limit", "species": ["CT", "RB"], "take": 1, "period": "daily"}
    assert _plain(x, names) == "Keep up to 1 cutthroat trout, brown trout or rainbow trout a day."
    x["take"] = 4
    assert _plain(x, names) == ("Keep up to 4 cutthroat trout, brown trout and rainbow trout a day, "
                                "all kinds together.")
