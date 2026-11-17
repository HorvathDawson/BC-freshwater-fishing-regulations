"""T805 part B (adversarial review of the trout redesign, 2026-11-16; owner "Keep the page line (Recommended)"): THE PAGE
KEEPS ITS ONE POSSESSION LINE, and a trout or char kind whose own possession differs says it on its row.

THE TEXT (synopsis 2025-2027, printed pages):
  Zone 7A, p.58: "Possession quotas = 2 daily quotas for most species. … Lake trout: possession quota = 1 daily quota.
    Bull trout (Dolly Varden): possession quota = 1 daily quota." (Zone 7B, p.64, the same for lake trout and bull trout.)
  CUNNINGHAM LAKE (7-25), p.59: "Lake trout possession quota = 2 (only one over 50 cm)".
  TAGISH LAKE (6-27), p.53: "Lake trout daily and possession quotas = 2 (only 1 over 90 cm, none between 60 cm and 90 cm)".
  BOWRON LAKE PARK waters, p.43: "Trout/char daily and possession quotas = 1 (none under 30 cm or over 50 cm)".

The kinds stand on data built here: the answers 2.8 fixture's plain Region 2 lake (p.21 "Possession quotas = 2 daily
quotas" added, as `test_answers_r20h.py` does) plus a lake trout "1 daily quota" line in Zone 7A's words and a water's
possession count with its clause in Cunningham Lake's words (each a bundle-shaped record on a Region 2 lake entry),
through the REAL reader and the REAL rows. The live twins are in `test_answers_t805_live.py`.
"""
from __future__ import annotations

import copy
import json

from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import card as CD
from pipeline.deliver.answers import card_check as CC
from pipeline.deliver.answers import rows as RW
from pipeline.deliver.answers.tree import walk
from pipeline.deliver.bundle import read
from pipeline.tests import answers28_fixture as F

PATH = "fixture:t805_possession"
JUL15 = (7, 15)
PAGE = ("z2:possession_quota", "possession_quota.r1")
PER, CAP = "r2:per_daily_lake@2-12", "r2:cunningham_shape_lake@2-12"
_SETS = {PER: 1, CAP: 2, None: 3}

_ROWS = {
    PER: ("Per Daily Lake", [
        ("possession_exceptions.r1", {"label": "Lake trout — possession quota is 1 daily quota", "species": ["LT"],
                                      "per_daily": 1, "period": "possession", "dimension": "possession",
                                      "verbatim": "Lake trout: possession quota = 1 daily quota."})]),
    CAP: ("Cunningham Lake", [
        ("cunningham_lake.r2", {"label": "Lake trout — 2 in possession", "species": ["LT"], "take": 2,
                                "period": "possession", "dimension": "possession",
                                "verbatim": "Lake trout possession quota = 2 (only one over 50 cm)"}),
        ("cunningham_lake.r4", {"label": "Lake trout (no more than 1 over 50 cm), in possession", "species": ["LT"],
                                "take": 1, "period": "possession", "dimension": "possession",
                                "within": "cunningham_lake.r2", "lengths": [{"min_cm": 50}],
                                "verbatim": "only one over 50 cm"})]),
}


def _bundle():
    got = read._RULES_BY_PATH.get(PATH)
    if got is None:
        rules = json.loads(F.FIXTURE.read_text(encoding="utf-8"))["rules"]
        got = {(x["entry"], x["rule"]): x for x in rules}
        got[PAGE] = {
            "entry": PAGE[0], "entry_id": PAGE[0], "entry_name": "Region 2", "rule": PAGE[1], "rule_id": PAGE[1],
            "dimension": "possession", "extent_text": None, "extents": [{"area_id": "area:region:2", "op": "within"}],
            "family": "retention", "label": "Possession quotas = 2 daily quotas", "may_target": None, "parts": "{}",
            "per_daily": 2, "period": "possession", "scope": "area", "species": ["ALL_GAME_FISH"], "species_except": [],
            "type": "retention_limit", "uncertain": 0, "undrawn_part": None, "unresolved": None,
            "verbatim": "Possession quotas = 2 daily quotas"}
        for e, (name, rows) in _ROWS.items():
            for r, f in rows:
                got[(e, r)] = {"entry_id": e, "rule_id": r, "type": "retention_limit", "family": "retention",
                               "parts": json.dumps({"what": f["label"]}), "scope": "section", "species_except": [],
                               "uncertain": 0, "unresolved": None, "extent_text": None, "undrawn_part": None,
                               "extents": [{"op": "whole"}], "rule": r, "entry": e, "entry_name": name,
                               "may_target": None, **f}
        for x in got.values():
            x["_rank"] = read.source_of(x).rank
        read._RULES_BY_PATH[PATH] = got
    lift = tuple(F.LIFT.split("::"))
    lake = [(e, r) for (e, r), x in got.items() if (e, r) != lift and x.get("water") != "stream" and e not in _ROWS]
    sets = {sid: sorted((e, r, "reach") for e, r in lake + [(e, r) for (e, r) in got if e == entry])
            for entry, sid in _SETS.items()}
    return F._B(rules=got, sets=sets)


def _frame(entry):
    B = _bundle()
    bound = B.sets[_SETS[entry]]
    lad = {f: {o: A.ladder_verdict(read.effective_rules_bound(
        bound, False, JUL15, f, PATH, steelhead_rules_here=True, origin=None if o == "none" else o, trace=True))
        for o in F.ORIGINS} for f in F.FISH}
    P = RW.Part(B, _SETS[entry], False, True, "lake", None, lad, JUL15, {})
    return RW.produce(P), P


def _group(out):
    (g,) = [r["group"] for r in out["rows"] if r.get("group") and "LT" in r["group"]["kinds"]]
    return g


def _duties(out) -> dict:
    return {tuple(n["species"]): n["duties"] for n, _p in walk(_group(out)["tree"]) if n["node"] == "kind"}


def test_a_fish_whose_possession_is_one_days_limit_says_it_on_its_row():
    out, P = _frame(PER)
    d = _duties(out)
    lt = next(v for k, v in d.items() if "LT" in k)
    assert lt == ["in your possession: 1 day’s limit, not 2 (fish at home don’t count)"]
    assert all(v == [] for k, v in d.items() if "LT" not in k)       # the page's line is theirs: nothing on the row
    assert CC.check_tree(P, _group(out)) == []


def test_a_waters_possession_count_says_its_size_clause():
    out, P = _frame(CAP)
    lt = next(v for k, v in _duties(out).items() if "LT" in k)
    assert lt == ["in your possession: no more than 2 lake trout, only 1 over 50 cm (20 in) (fish at home don’t count)"]
    assert CC.check_tree(P, _group(out)) == []


def test_a_plain_lake_says_no_possession_on_any_row():
    out, P = _frame(None)
    assert all(v == [] for v in _duties(out).values())
    assert CC.check_tree(P, _group(out)) == []


def _mutate(g, fn):
    g = copy.deepcopy(g)
    for n, _p in walk(g["tree"]):
        if n["node"] == "kind":
            n["duties"] = [fn(d) for d in n["duties"] if fn(d) is not None]
    return g


def test_mutation_the_gate_refuses_a_dropped_size_clause_a_larger_number_and_a_missing_line():
    out, P = _frame(CAP)
    g = _group(out)
    dropped = _mutate(g, lambda d: d.replace(", only 1 over 50 cm (20 in)", ""))
    assert any("the rules" in p for p in CC.check_tree(P, dropped))
    larger = _mutate(g, lambda d: d.replace("no more than 2", "no more than 4"))
    assert any("the rules" in p for p in CC.check_tree(P, larger))
    gone = _mutate(g, lambda d: None if d.startswith("in your possession") else d)
    assert any("is on no line" in p for p in CC.check_tree(P, gone))
    out, P = _frame(PER)
    two = _mutate(_group(out), lambda d: d.replace("1 day’s limit", "2 day’s limit"))
    assert any("the rules" in p for p in CC.check_tree(P, two))
    out, P = _frame(None)                                            # a line where the page's line is the fish's
    extra = copy.deepcopy(_group(out))
    for n, _p in walk(extra["tree"]):
        if n["node"] == "kind" and "LT" in n["species"]:
            n["duties"] = ["in your possession: 1 day’s limit, not 2"]
    assert any("is the page's" in p for p in CC.check_tree(P, extra))


def test_mutation_no_possession_line_on_the_row(monkeypatch):
    monkeypatch.setattr(CD, "A_POSSESSION_ON_THE_FISH_ROW", False)
    out, P = _frame(CAP)
    assert all(v == [] for v in _duties(out).values())
    # with no line the lake trout fold in with the Dolly Varden: the gate names the count holding only one of them
    assert any("is on no line" in p or "holds only some of its fish" in p for p in CC.check_tree(P, _group(out)))
