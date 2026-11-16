"""T805 part C (adversarial review of the trout redesign, finding 3, 2026-11-16): ONE WILD LIMIT ACROSS ROWS, and the char
of the seven "Wild trout/char daily quota = 2" lakes stored one way (RULINGS C5).

THE TEXT (synopsis 2025-2027, printed pages):
  LOIS LAKE (2-12), p.24: "Wild trout/char daily quota = 2 (no wild trout 40 cm or more), hatchery rainbow trout = 6 /
    Rainbow trout/hatchery steelhead quota = 6 in the aggregate / Single barbless hook". Khartoum Lake (p.23) prints the
    same. The rainbow trout are counted apart from Region 2's 4 (their own row of 6), but the wild 2 holds every wild
    trout and char of BOTH rows: 2 wild fish in all. The cards read "up to 2 wild …" on each row (2 + 2).
  DODD LAKE (2-12), p.23: "Wild trout/char daily quota = 2 (no wild trout over 40 cm)", as Horseshoe, Nanton, Ireland
    (p.23-26) — the size clause a rule of its own; Lois and Windsor store it as a clause OF the quota (`within`). RULINGS
    C5 (owner 2026-11-16): the 40 cm cap covers char, so beside Region 2's "none under 60 cm" no wild char may be kept
    — on all seven lakes. Lois and Windsor read "no wild Dolly Varden/bull trout or lake trout 60 cm or longer" (an
    empty wild range said as a keepable one).

The cards stand on data built here: the answers 2.8 fixture's plain Region 2 lake plus the bundle's records of
`lois_lake.r1/r2/r4/r4b` and `dodd_lake.r1/r2` (verbatim, copied onto Region 2 lake entries), through the REAL reader
and the REAL rows. The live twins (the real lakes) are in `test_answers_t805_live.py`.
"""
from __future__ import annotations

import copy
import json

import pytest

from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import card_check as CC
from pipeline.deliver.answers import rows as RW
from pipeline.deliver.answers import tree as TR
from pipeline.deliver.answers.rows import evaluate as EV
from pipeline.deliver.answers.tree import across_unsaid, walk
from pipeline.deliver.bundle import read
from pipeline.tests import answers28_fixture as F

PATH = "fixture:t805_wild_limit"
JUL15 = (7, 15)
LOIS, DODD = "r2:lois_shape_lake@2-12", "r2:dodd_shape_lake@2-12"

_WILD2 = {"label": "Trout and char — 2 per day, all species combined, wild only", "species": ["TROUT_CHAR"], "take": 2,
          "dimension": "daily@origin=wild", "origin": "wild", "verbatim": "Wild trout/char daily quota = 2"}
#: the bundle's records (fields as the bundle stores them)
_ROWS = {
    LOIS: ("Lois Lake", [
        ("lois_lake.r1", dict(_WILD2)),
        ("lois_lake.r2", {"label": "Trout and char (none 40 cm or more), wild only", "species": ["TROUT_CHAR"],
                          "take": None, "dimension": "daily/size@origin=wild", "origin": "wild", "may_target": 1,
                          "lengths": [{"closed": True, "min_cm": 40, "take": 0}], "within": "lois_lake.r1",
                          "verbatim": "no wild trout 40 cm or more"}),
        ("lois_lake.r4", {"label": "Rainbow trout and Steelhead — 6 per day, all species combined", "species": ["RB", "ST"],
                          "take": 6, "dimension": "daily",
                          "verbatim": "Rainbow trout/hatchery steelhead quota = 6 in the aggregate",
                          "exempts": [{"entry_id": "z2:daily_quotas", "rule_id": "trout_char_quota.r1",
                                       "species": ["RB", "ST"], "note": "counted apart"},
                                      {"entry_id": "z2:daily_quotas", "rule_id": "trout_char_quota.r2",
                                       "species": ["RB"], "note": "a larger number"},
                                      {"entry_id": "z2:daily_quotas", "rule_id": "trout_char_quota.r2b",
                                       "species": ["RB", "ST"], "note": "a larger number"}]}),
        ("lois_lake.r4b", {"label": "Steelhead — release all, wild only", "species": ["ST"], "take": 0, "may_target": 1,
                           "dimension": "daily@origin=wild", "origin": "wild", "within": "lois_lake.r4",
                           "verbatim": "Rainbow trout/hatchery steelhead quota = 6 in the aggregate"})]),
    DODD: ("Dodd Lake", [
        ("dodd_lake.r1", dict(_WILD2)),
        ("dodd_lake.r2", {"label": "Trout and char (none over 40 cm), wild only", "species": ["TROUT_CHAR"],
                          "take": None, "dimension": "daily/size@origin=wild", "origin": "wild", "may_target": 1,
                          "lengths": [{"min_cm": 40, "take": 0}], "verbatim": "no wild trout over 40 cm"})]),
}
_SETS = {LOIS: 1, DODD: 2}
CHAR = "Up to 1 hatchery Dolly Varden/bull trout or lake trout combined, 60 cm (24 in) or longer"


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
                               "entry_name": name, "may_target": None, **f}
        for x in got.values():
            x["_rank"] = read.source_of(x).rank
        read._RULES_BY_PATH[PATH] = got
    lift = tuple(F.LIFT.split("::"))
    lake = [(e, r) for (e, r), x in got.items() if (e, r) != lift and x.get("water") != "stream" and e not in _ROWS]
    sets = {sid: sorted((e, r, "reach") for e, r in lake + [(e, r) for (e, r) in got if e == entry])
            for entry, sid in _SETS.items()}
    return F._B(rules=got, sets=sets)


def _frame(entry: str):
    B = _bundle()
    bound = B.sets[_SETS[entry]]
    lad = {f: {o: A.ladder_verdict(read.effective_rules_bound(
        bound, False, JUL15, f, PATH, steelhead_rules_here=True, origin=None if o == "none" else o, trace=True))
        for o in F.ORIGINS} for f in F.FISH}
    P = RW.Part(B, _SETS[entry], False, True, "lake", None, lad, JUL15, {})
    return RW.produce(P), P


def _kinds(out: dict) -> dict:
    """Every trout kind of every card: species -> its node."""
    return {tuple(n["species"]): n for r in out["rows"] if r.get("group")
            for n, _p in walk(r["group"]["tree"]) if n["node"] == "kind" and set(n["species"]) & set(F.FISH)}


def test_lois_says_the_wild_two_once_across_both_cards():
    out, P = _frame(LOIS)
    k = _kinds(out)
    trout, rb = k[("EB", "GB", "CT")], k[("RB",)]
    assert trout["sentence"] == (
        "Up to 4 brook, brown or cutthroat trout combined, any size — up to 2 wild brook, brown or cutthroat trout combined under "
        "40 cm (16 in); no wild brook, brown or cutthroat trout 40 cm (16 in) or longer; no more than 2 wild trout and "
        "char in all, counting any wild rainbow trout you keep")
    assert rb["sentence"] == (
        "Up to 6 rainbow trout, any size — up to 2 wild rainbow trout under 40 cm (16 in); no wild rainbow trout 40 cm "
        "(16 in) or longer; no more than 2 wild trout and char in all, counting any wild brook, brown or cutthroat trout you keep")
    (a,) = rb["also_counts"]
    assert a == {"tier": None, "by": "r2:lois_shape_lake@2-12::lois_lake.r1", "n": 2, "over_cm": 0, "then": [],
                 "species": ["RB", "CT", "GB", "EB"],
                 "says": "no more than 2 wild trout and char in all, counting any wild brook, brown or cutthroat trout "
                         "you keep"}
    assert across_unsaid(out) == []
    for r in out["rows"]:
        if r.get("group"):
            assert CC.check_tree(P, r["group"]) == [], r["members"]


def test_the_char_of_a_wild_two_lake_are_hatchery_only_however_its_size_clause_is_stored():
    """RULINGS C5: Lois (the clause `within` the quota) and Dodd (a rule of its own) say the same of their char."""
    for entry in (LOIS, DODD):
        out, _P = _frame(entry)
        assert _kinds(out)[("DV", "LT")]["sentence"] == CHAR, entry
        assert out["fish"]["DV"]["wild"]["status"] == "release", entry


def test_mutation_the_size_clause_of_a_quota_releasing_nothing_is_lois_old_card(monkeypatch):
    monkeypatch.setattr(EV, "A_SIZE_CLAUSE_RELEASES_AS_A_RULE", False)
    out, _P = _frame(LOIS)
    assert _kinds(out)[("DV", "LT")]["sentence"] != CHAR
    assert out["fish"]["DV"]["wild"]["status"] == "keep"


def test_mutation_a_card_saying_its_own_two_alone_stops_the_build(monkeypatch):
    monkeypatch.setattr(TR, "A_WILD_LIMIT_ACROSS_ROWS_SAID", False)
    out, _P = _frame(LOIS)
    got = across_unsaid(out)
    assert len(got) == 2 and all("of r2:lois_shape_lake@2-12::lois_lake.r1 also holds" in p for p in got), got


def test_the_gate_refuses_a_wrong_number_and_one_that_does_not_bind():
    out, P = _frame(LOIS)
    for i, r in enumerate(out["rows"]):
        g = r.get("group")
        if not g or "RB" not in g["kinds"]:
            continue
        three = copy.deepcopy(g)
        for n, _p in walk(three["tree"]):
            for a in n.get("also_counts") or []:
                a["n"] = 3
                a["says"] = a["says"].replace("no more than 2", "no more than 3")
        assert any("which the rules do not hold" in p or "states [3.0]" in p for p in CC.check_tree(P, three)), i
        wrong = copy.deepcopy(g)                               # the brook trout named, but the wild 2 is not theirs
        for n, _p in walk(wrong["tree"]):
            for a in n.get("also_counts") or []:
                a["by"] = "z2:daily_quotas::trout_char_quota.r1"
        assert CC.check_tree(P, wrong) != []


def test_a_plain_region_2_lake_and_a_one_row_wild_lake_say_no_cross_row_limit():
    out, _P = F.card(F.LAKE, JUL15)
    assert across_unsaid(out) == []
    assert all("in all, counting any wild" not in a["says"] for n in _kinds(out).values() for a in n["also_counts"])
    out, _P = _frame(DODD)                                     # one row holds every wild fish: nothing to say across
    assert across_unsaid(out) == []
    assert all("in all, counting any wild" not in n["sentence"] for n in _kinds(out).values())


@pytest.mark.parametrize("rows, home, kinds, want", [
    ([{"kind": "keep", "members": ["EB"], "conds": [{"c": "origin2", "r": "x::r1", "o": "wild", "daily": 2, "who": ["EB"]}]},
      {"kind": "keep", "members": ["RB"], "conds": [{"c": "origin2", "r": "x::r1", "o": "wild", "daily": 2, "who": ["RB"]}]}],
     0, ["EB"], [{"r": "x::r1", "o": "wild", "n": 2, "mine": ["EB"], "others": ["RB"]}]),
    # another rule, or another number: not the same limit
    ([{"kind": "keep", "members": ["EB"], "conds": [{"c": "origin2", "r": "x::r1", "o": "wild", "daily": 2, "who": ["EB"]}]},
      {"kind": "keep", "members": ["RB"], "conds": [{"c": "origin2", "r": "x::r9", "o": "wild", "daily": 2, "who": ["RB"]}]}],
     0, ["EB"], []),
    # a release row holds nothing
    ([{"kind": "keep", "members": ["EB"], "conds": [{"c": "origin2", "r": "x::r1", "o": "wild", "daily": 2, "who": ["EB"]}]},
      {"kind": "release", "members": ["RB"], "conds": [{"c": "origin2", "r": "x::r1", "o": "wild", "daily": 2, "who": ["RB"]}]}],
     0, ["EB"], []),
])
def test_across_rows_reads_one_limit_per_rule_origin_and_number(rows, home, kinds, want):
    assert list(TR.across_rows(rows, home, kinds)) == want
