"""T607 (owner 2026-11-10) — the extent kind "ALL THE OTHER PLACES ON THIS WATER" (`op: other_places`).

DFO's "all open portions of the Little Qualicum River" was saved as the whole river; as a sibling of the place "all
waters unless indicated below" (a `rest`) it took the whole river out of the rest, which then bound nothing. Owner:
"this is wrong (this has to apply to all already matched items instead)". A wording of this kind has no stretch of its
own: it binds the UNION of what the other placed places of its water bind (the rest included), and is never subtracted
from a rest.

  the builder     the union of its siblings, each with its own walk; an unbound sibling adds nothing; none bound = unresolved
  the DFO input   the Little Qualicum's shape: rest + two stretches + open-portions — the rest keeps the river less
                  the two stretches, open-portions names all three as its siblings
  the model       it takes no cut, item or area; it is a place's only piece
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.atlas.reach.build import build_reach
from pipeline.atlas.reach.models import Outcome, Reason
from pipeline.tests import under_test
from pipeline.regs.dfo_salmon import entries as E
from pipeline.regs.parsing.entry_models import Extent
from pipeline.regs.review import marks as M
from pipeline.tests.test_complement_rest import G, REG, _entry, _walk  # noqa: F401  (the four-piece river and its walk)

REACH = {"rule_id": "reach", "extents": [{"op": "whole", "item_id": "gnis:reach"}]}             # m2, m3
REST = {"rule_id": "rest", "extents": [{"op": "rest", "siblings": ["reach"]}]}                  # m1, m4
OPEN = {"rule_id": "open", "extents": [{"op": "other_places", "siblings": ["reach", "rest"]}]}


def test_it_binds_the_union_of_its_siblings_and_leaves_the_rest_whole():
    entry = _entry([REACH, REST, OPEN])
    b, diags = build_reach(entry, OPEN, REG, G)
    assert b.outcome is Outcome.bound and b.sections == ("m1", "m2", "m3", "m4")
    d = next(d for d in diags if d.kind == "union")
    assert d.payload["siblings"] == {"reach": 2, "rest": 2} and d.payload["unbound"] == {}
    rest, _ = build_reach(entry, REST, REG, G)
    assert rest.sections == ("m1", "m4"), "the rest is the river less the reach — never less the open-portions place"


def test_each_sibling_keeps_its_own_tributary_walk():
    reach = {**REACH, "includes_tributaries": True}                       # the reach walks its tributary t1; nothing else does
    b, _ = build_reach(_entry([reach, {**OPEN, "extents": [{"op": "other_places", "siblings": ["reach"]}]}]),
                       {**OPEN, "extents": [{"op": "other_places", "siblings": ["reach"]}]}, REG, G)
    assert b.sections == ("m2", "m3", "t1")


def test_a_sibling_that_does_not_bind_adds_nothing_and_none_bound_is_unresolved():
    gone = {"rule_id": "gone", "extents": [{"op": "whole", "item_id": "gnis:nowhere"}]}
    both = {"rule_id": "open", "extents": [{"op": "other_places", "siblings": ["reach", "gone"]}]}
    b, diags = build_reach(_entry([REACH, gone, both]), both, REG, G)
    assert b.outcome is Outcome.bound and b.sections == ("m2", "m3")
    assert list(next(d for d in diags if d.kind == "union").payload["unbound"]) == ["gone"]
    only = {"rule_id": "open", "extents": [{"op": "other_places", "siblings": ["gone"]}]}
    b, _ = build_reach(_entry([gone, only]), only, REG, G)
    assert b.outcome is Outcome.unresolved and b.reason is Reason.no_sections, "never guessed as the whole water"
    stray = {"rule_id": "open", "extents": [{"op": "other_places", "siblings": ["nobody"]}]}
    assert build_reach(_entry([stray]), stray, REG, G)[0].reason is Reason.complement_unknown


def test_the_model_takes_no_water_of_its_own():
    assert Extent.model_validate({"op": "other_places", "siblings": ["a"]}).siblings == ["a"]
    assert Extent.model_validate({"op": "other_places"}, context={"rest_at_build": True}).op.value == "other_places"
    with pytest.raises(ValueError, match="needs `siblings`"):
        Extent.model_validate({"op": "other_places"})
    with pytest.raises(ValueError):
        Extent.model_validate({"op": "other_places", "siblings": ["a"], "splits": ["x"]})
    with pytest.raises(ValueError):
        Extent.model_validate({"op": "other_places", "siblings": ["a"], "item_id": "gnis:1"})
    assert M.OP_POINTS["other_places"] == 0 and "no stretch of its own" in M.OP_WORDS["other_places"]


# --------------------------------------------------------------------------- the Little Qualicum's shape (stored rows)

LQ = "1:little-qualicum-river:"
REST_ID, OPEN_ID = LQ + "all-waters-unless-indicated-below", LQ + "all-open-portions-of-the-little-qu"
STRETCHES = {LQ + "from-the-falls-in-little-qualicum", LQ + "from-the-hatchery-fence-to-signs-l"}


def _lq(open_extents=None) -> dict:
    doc = json.loads((Path(under_test.curated().dfo_salmon.places) / "region-1.json").read_text(encoding="utf-8"))
    locs = {}
    for l in doc["locations"]:
        if l["location_id"].startswith(LQ) and (l.get("binding") or {}).get("extents"):
            if open_extents is not None and l["location_id"] == OPEN_ID:
                l = {**l, "binding": {**l["binding"], "extents": open_extents}}
            locs[l["location_id"]] = E.EntryLocation.from_dict(l)
    return locs


def test_the_little_qualicum_rest_is_not_emptied_and_open_portions_binds_all_three():
    locs = _lq([{"op": "other_places"}])
    assert set(locs) == STRETCHES | {REST_ID, OPEN_ID}
    entry, rules = E.to_reach_input(locs[REST_ID], [{}], None, siblings=locs)
    [rest] = rules[0]["extents"]
    assert rest["op"] == "rest" and set(rest["siblings"]) == STRETCHES, "open-portions is NOT taken out of the rest"
    assert {r["rule_id"] for r in entry["rules"][1:]} == STRETCHES
    entry, rules = E.to_reach_input(locs[OPEN_ID], [{}], None, siblings=locs)
    [ex] = rules[0]["extents"]
    assert ex["op"] == "other_places" and set(ex["siblings"]) == STRETCHES | {REST_ID}, "it binds all three"
    by = {r["rule_id"]: r for r in entry["rules"][1:]}
    assert set(by) == STRETCHES | {REST_ID}
    assert set(by[REST_ID]["extents"][0]["siblings"]) == STRETCHES, "the rest inside the union is the same rest"
    # a PREVIEW: the file still holds the place as the whole river, the extent handed in is the new kind — the rest
    # inside its union must not subtract the place's old extent (seen in the sandbox: the union bound 3 of 8 sections)
    stale = {**_lq([{"op": "whole", "item_id": "gnis:10886"}])}
    entry, rules = E.to_reach_input(locs[OPEN_ID], [{}], None, siblings=stale)
    by = {r["rule_id"]: r for r in entry["rules"][1:]}
    assert set(by[REST_ID]["extents"][0]["siblings"]) == STRETCHES and OPEN_ID not in by
    # an UNPLACED place is simply not in the union (the kind covers only placed places)
    fewer = {k: v for k, v in locs.items() if k != sorted(STRETCHES)[0]}
    _e, rules = E.to_reach_input(fewer[OPEN_ID], [{}], None, siblings=fewer)
    assert set(rules[0]["extents"][0]["siblings"]) == {sorted(STRETCHES)[1], REST_ID}


def test_what_was_wrong_before_a_whole_water_sibling_emptied_the_rest():
    """the cause on file: saved as the WHOLE river, the open-portions place is one of the rest's siblings"""
    locs = _lq([{"op": "whole", "item_id": "gnis:10886"}])
    _e, rules = E.to_reach_input(locs[REST_ID], [{}], None, siblings=locs)
    assert OPEN_ID in rules[0]["extents"][0]["siblings"]


def test_it_is_a_places_only_piece_in_a_mark():
    fix = {"scope": "entry", "op": "other_places", "points": [], "items": []}
    assert M.ExtentFix.model_validate(fix).op == "other_places"
    with pytest.raises(ValueError, match="only piece"):
        M.pieces_ok([M.ExtentFix.model_validate(fix),
                     M.ExtentFix.model_validate({"scope": "entry", "op": "whole", "points": [], "items": []})])


# --------------------------------------------------------------------------- "check again" (the recheck list)

def _dfo_mark(target: str, date: str) -> M.Mark:
    return M.Mark.model_validate({
        "scope": "dfo", "target": target, "verdict": "verified", "answers": {"T": "y", "R": "y", "P": "y"},
        "reviewer": "dawson", "date": date, "pdf_sha256": "0" * 64,
        "extent_fix": [{"scope": "entry", "op": "whole", "points": [], "items": []}],
        "prints": {"text": "t", "records": [{"key": "entry", "meaning": "m", "binding": "b"}]}})


def test_a_listed_place_asks_until_any_newer_mark_is_in_force(tmp_path):
    """owner 2026-11-10: "can you flag it so it doesn't look done anymore?" — no mark is touched; the entry names the
    mark it was made on and goes inert by itself once he saves again"""
    from pipeline.regs.review import recheck as R
    old, new = _dfo_mark("x:place", "2026-11-03"), _dfo_mark("x:place", "2026-11-11")
    f = tmp_path / "recheck.json"
    f.write_text(json.dumps({"recheck": [{"scope": "dfo", "target": "x:place", "reason": "saved as the whole river",
                                          "by": "owner", "date": "2026-11-10", "mark": R.mark_digest(old)}]}))
    rows = R.load(f)
    assert R.active(rows, {("dfo", "x:place"): old}) == {("dfo", "x:place"): {
        "reason": "saved as the whole river", "by": "owner", "date": "2026-11-10"}}
    assert R.active(rows, {("dfo", "x:place"): new}) == {}, "he saved again: done by his new mark, the entry inert"
    assert R.active(rows, {}) == {} and R.active(rows, {("dfo", "x:other"): old}) == {}
    assert R.load(tmp_path / "none.json") == []
    f.write_text(json.dumps({"recheck": [{"scope": "dfo", "target": "x:place", "reason": "", "by": "owner",
                                          "date": "2026-11-10", "mark": "abc"}]}))
    with pytest.raises(ValueError, match="lacks"):
        R.load(f)


def test_the_curated_list_names_the_little_qualicum_place_on_its_mark_in_force():
    from pipeline.regs.review import recheck as R
    rows = R.load()
    inforce = {(m.scope, m.target): m for m in M.latest(M.load(under_test.curated().review.marks)).values()}
    assert [(r["scope"], r["target"]) for r in rows] == [("dfo", OPEN_ID)]
    live = R.active(rows, inforce)
    # asking while his 2026-11-03 save is the mark in force; inert (and this line then reads {}) once he re-saves
    assert live in ({("dfo", OPEN_ID): {"reason": rows[0]["reason"], "by": "owner", "date": "2026-11-10"}}, {})


def test_the_checklist_reads_the_list(monkeypatch, tmp_path):
    import sys
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "curation-review" / "backend"))
    sys.modules.pop("checks", None)
    import checks as B
    sys.modules.pop("checks", None)
    from pipeline.regs.review import recheck as R
    old, new = _dfo_mark("x:place", "2026-11-03"), _dfo_mark("x:place", "2026-11-11")
    f = tmp_path / "recheck.json"
    f.write_text(json.dumps({"recheck": [{"scope": "dfo", "target": "x:place", "reason": "why", "by": "owner",
                                          "date": "2026-11-10", "mark": R.mark_digest(old)}]}))
    monkeypatch.setattr(R, "path", lambda: f)
    assert B.recheck_of("dfo", "x:place", old) == {"reason": "why", "by": "owner", "date": "2026-11-10"}
    assert B.recheck_of("dfo", "x:place", new) is None and B.recheck_of("dfo", "x:place", None) is None
    assert "stale" not in B.DONE_STATES, "a listed place is given the not-done state `stale` (`checks.dfo_rows`)"


def test_the_review_app_names_the_places_it_covers_and_warns_when_there_is_only_one(monkeypatch):
    """T611: with exactly ONE placed sibling the salmon chain orders the two by id (`deliver.salmon.chain_order`: tier,
    fewer sections, id) — the app says so on the place, naming the ticket, until the chain has the rule"""
    import sys
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "curation-review" / "backend"))
    sys.modules.pop("dfo", None)
    import dfo as BD
    sys.modules.pop("dfo", None)

    def loc(i, extents, status="active"):
        return ("1", {"location_id": f"x:{i}", "water_id": "x", "status": status, "binding": {"extents": extents},
                      "source_text": {"specific_area": f"words of {i}"}}, None)
    me = loc("open", [{"op": "other_places"}])[1]
    world = dict([("x:open", loc("open", [{"op": "other_places"}])), ("x:a", loc("a", [{"op": "whole"}])),
                  ("x:b", loc("b", [])), ("x:gone", loc("gone", [], "dormant")),
                  ("y:z", ("1", {"location_id": "y:z", "water_id": "y", "binding": {"extents": [{"op": "whole"}]}}, None))])
    monkeypatch.setattr(BD, "_locations", lambda: world)
    got = BD.other_places_of(me)
    assert [o["id"] for o in got["others"]] == ["x:a"] and [o["id"] for o in got["others_unplaced"]] == ["x:b"]
    assert "T611" in got["one_sibling"] and "words of a" in got["one_sibling"]
    world["x:c"] = loc("c", [{"op": "rest"}])
    got = BD.other_places_of(me)
    assert [o["id"] for o in got["others"]] == ["x:a", "x:c"] and "one_sibling" not in got
