"""THE KNOWN STEELHEAD WATERS CARRY THE PROVINCIAL STEELHEAD SET WHEREVER THEY ARE (user rulings
2026-10-02) — on a temporary corpus and a toy atlas, so each mechanism is pinned without a build.

  - the curated known-steelhead list (`steelhead.load_list` / `resolve_list`): a listed river that
    was only "possible" becomes KNOWN, with the whole provincial set — on its steelhead-region
    stream by the base rules, past the steelhead regions by the twins (`Extent` op
    `steelhead_waters`) — and the report records the list; an unknown, ambiguous or duplicate entry
    stops the run;
  - a lake a steelhead row binds by ANOTHER line (Tenas Lake, by the Atnarko's spring closure) is
    known and carries the twins;
  - a twin whose sibling does not bind is unresolved, never the whole known set; `build_reach`
    alone (the review app) cannot resolve the op.
"""
from __future__ import annotations

import json

import pytest

from pipeline.atlas.reach import steelhead as SH
from pipeline.atlas.reach.build import build_reach, build_reaches
from pipeline.atlas.reach.models import Outcome, Reason
from pipeline.common.models import NodeKind, StreamGraph, StreamNode
from pipeline.common.models.registry import RegistryItem
from pipeline.regs.parsing.catalogue import CatalogueEntry


def _node(nid, kind=NodeKind.stream):
    return StreamNode(node_id=nid, kind=kind, blk="1" if kind is NodeKind.stream else "",
                      down_m=0.0, up_m=1.0, length_m=1.0)


def _graph(*nodes):
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in nodes}
    return g


def _item(iid, secs, name="", kind="stream", mus=()):
    return RegistryItem(id=iid, name=name or iid, kind=kind, section_ids=tuple(secs), mus=mus)


# Cowichan River: c1 in Region 1 (a steelhead region), c8 in Region 8 (none). Tenas Lake (lk) and
# the Atnarko (a5) in Region 5. Two creeks called "Mill Creek", one in each of Regions 1 and 8.
G = _graph(_node("c1"), _node("c8"), _node("a5"), _node("lk", NodeKind.lake),
           _node("m1"), _node("m8"))
REG = {
    "gnis:1": _item("gnis:1", ["c1", "c8"], "Cowichan River", mus=("1-4", "8-1")),
    "gnis:5": _item("gnis:5", ["a5"], "Atnarko River", mus=("5-11",)),
    "wbk:9": _item("wbk:9", ["lk"], "Tenas Lake", kind="lake", mus=("5-11",)),
    "gnis:21": _item("gnis:21", ["m1"], "Mill Creek", mus=("1-2",)),
    "gnis:28": _item("gnis:28", ["m8"], "Mill Creek", mus=("8-3",)),
    "area:region:1": _item("area:region:1", ["c1", "m1"]),
    "area:region:5": _item("area:region:5", ["a5", "lk"]),
    "area:region:8": _item("area:region:8", ["c8", "m8"]),
}

STREAMS = [{"op": "within", "area_id": f"area:region:{n}", "feature_types": ["stream"]}
           for n in ("1", "5")]
TWIN = lambda base, **kw: [{"op": "steelhead_waters", "siblings": [base], **kw}]  # noqa: E731


def _rule(rid, **kw):
    return {"rule_id": rid, "type": "retention_limit", "species": ["ST"],
            "verbatim": "All wild steelhead must be released.", **kw}


def _province():
    rules = []
    for base in ("steelhead.r1", "steelhead.r2", "steelhead.r4"):
        rules.append(_rule(base, extents=STREAMS, water="stream"))
        rules.append(_rule(base + "b", extents=TWIN(base)))
    stamp = {"kind": "requirement", "id": "steelhead_targeting", "extents": STREAMS,
             "water": "stream", "verbatim": "Steelhead Stamp"}
    twin = dict(stamp, id="steelhead_targeting_known",
                extents=TWIN("steelhead_targeting", outside_area_kind="national_parks"))
    twin.pop("water")
    return {"entry_id": SH.PROVINCE_STEELHEAD, "matched": [], "rules": rules,
            "licensing": [stamp, twin]}


def _atnarko():
    """A steelhead row: its steelhead line binds the river; its spring closure binds the river
    "plus Tenas Lake" — the lake's only steelhead row is by that other line."""
    return {"entry_id": "r5:atnarko_river@5-11", "matched": ["gnis:5"], "anadromous_rainbow": True,
            "rules": [_rule("atnarko.r13", extents=[{"op": "whole"}]),
                      {"rule_id": "atnarko.r1", "type": "closure", "verbatim": "No Fishing",
                       "extents": [{"op": "whole"}, {"op": "whole", "item_id": "wbk:9"}]}]}


def _run(entries, listed=()):
    return build_reaches(entries, REG, G, steelhead_list=[SH.ListWater(**w) for w in listed])


def _on(result, sid) -> set[str]:
    return {f"{b.entry_id}::{b.rule_id}" for b in result.bindings if sid in b.sections}


def _recs_on(result, sid) -> set[str]:
    return {p.record_id for p in result.licensing if sid in p.sections}


def _code(result, sid):
    return next((r["steelhead"] for r in result.steelhead if r["section_id"] == sid), None)


SET = {"zp:steelhead::steelhead.r1", "zp:steelhead::steelhead.r2", "zp:steelhead::steelhead.r4"}
TWINS = {f"{x}b" for x in SET}


def test_the_corpus_twins_are_steelhead_waters_of_their_base():
    """The corpus's provincial entry: each twin's one extent is the known waters minus its base."""
    from pipeline.regs.parsing.io import read_all_entries
    e = CatalogueEntry.model_validate(read_all_entries()["zp:steelhead"])
    got = {r.rule_id: r.extents for r in e.rules if r.rule_id.endswith("b")}
    assert got == {f"{b}b": TWIN(b) for b in ("steelhead.r1", "steelhead.r2", "steelhead.r4")}
    lic = {x.id: x.extents for x in e.licensing}
    assert lic["steelhead_targeting_known"] == TWIN("steelhead_targeting",
                                                    outside_area_kind="national_parks")
    from pipeline.regs.parsing.entry_models import Extent, Op
    x = Extent.model_validate(TWIN("steelhead.r1")[0])
    assert x.op is Op.STEELHEAD_WATERS and x.siblings == ["steelhead.r1"]
    with pytest.raises(ValueError):
        Extent.model_validate({"op": "steelhead_waters", "item_id": "gnis:1"})


def test_without_the_list_cowichan_is_possible_in_region_1_and_absent_in_region_8():
    got = _run([_province()])
    assert _code(got, "c1") == "possible" and _code(got, "c8") is None
    assert SET <= _on(got, "c1") and not _on(got, "c8")
    assert "steelhead_targeting" in _recs_on(got, "c1") and not _recs_on(got, "c8")
    # no known water: the twins bind nothing and say so
    tw = {b.rule_id: b for b in got.bindings if b.rule_id.endswith("b")}
    assert all(b.outcome is Outcome.unresolved and b.reason is Reason.no_sections
               for b in tw.values()), tw


def test_a_listed_river_is_known_with_the_whole_provincial_set():
    """THE CURATED LIST: Cowichan River, "possible" before, is KNOWN on every section — in Region 1
    by the base rules and the stamp; in Region 8 (no steelhead region) by the twins and the stamp
    twin — and the run records the list."""
    got = _run([_province()], [{"name": "Cowichan River", "region": "1", "note": "winter run"}])
    assert _code(got, "c1") == "known" and _code(got, "c8") == "known"
    assert SET <= _on(got, "c1") and not TWINS & _on(got, "c1")
    assert TWINS <= _on(got, "c8") and not SET & _on(got, "c8")
    assert "steelhead_targeting" in _recs_on(got, "c1")
    assert "steelhead_targeting_known" in _recs_on(got, "c8")
    rows = {r["section_id"]: r for r in got.steelhead}
    assert rows["c1"]["entry_id"] == rows["c8"]["entry_id"] == SH.CURATED_LIST
    assert got.report.steelhead["curated"] == [
        {"item_id": "gnis:1", "listed": "'Cowichan River' (region 1)", "own": 2, "trib": 0}]
    # the other Mill Creek and the rest of Region 1 are untouched
    assert _code(got, "m1") == "possible" and _code(got, "m8") is None


def test_the_list_by_item_id_is_the_same_water():
    a = _run([_province()], [{"name": "Cowichan River"}])
    b = _run([_province()], [{"item_id": "gnis:1"}])
    assert a.steelhead == b.steelhead


@pytest.mark.parametrize("listed,why", [
    ([{"name": "Mill Creek"}], "ambiguous"),
    ([{"name": "Nowhere River"}], "no water of that name"),
    ([{"name": "Cowichan River", "region": "5"}], "no water of that name"),
    ([{"item_id": "gnis:404"}], "no such water"),
    ([{"item_id": "gnis:1", "mu": "5-11"}], "the registry puts"),
    ([{"item_id": "gnis:1"}, {"name": "Cowichan River"}], "already listed"),
])
def test_the_list_refuses_what_it_cannot_resolve(listed, why):
    with pytest.raises(SystemExit) as e:
        _run([_province()], listed)
    assert why in str(e.value)


def test_a_region_or_mu_resolves_the_ambiguous_name():
    got = _run([_province()], [{"name": "mill  creek", "mu": "8-3"}])
    assert _code(got, "m8") == "known" and TWINS <= _on(got, "m8")


@pytest.mark.parametrize("bad", [{}, {"item_id": "a", "name": "b"}, {"name": "x", "region": "9"},
                                 {"name": "x", "mu": "1"}, {"name": "x", "extra": 1}])
def test_a_list_entry_is_refused_by_shape(bad):
    with pytest.raises(ValueError):
        SH.ListWater(**bad)


def test_the_curated_file_loads_and_is_documented():
    from pipeline.common.curated import CURATED
    path = CURATED.regulations.steelhead_waters
    doc = json.loads(path.read_text(encoding="utf-8"))
    assert "item_id" in " ".join(doc["$comment"]) and "region" in " ".join(doc["$comment"])
    assert isinstance(SH.load_list(), list)


def test_a_missing_list_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        SH.load_list(tmp_path / "nope.json")


def test_a_lake_a_steelhead_row_binds_by_another_line_carries_the_set():
    """Tenas Lake: the Atnarko row's spring closure binds it; the row's steelhead line does not.
    It is a steelhead row's water, so it is KNOWN and carries the twins and the stamp twin; the
    river carries the base rules. A rainbow over 50 cm stays a rainbow on the lake."""
    got = _run([_province(), _atnarko()])
    assert _code(got, "lk") == "known" and _code(got, "a5") == "known"
    assert TWINS <= _on(got, "lk") and not SET & _on(got, "lk")
    assert "steelhead_targeting_known" in _recs_on(got, "lk")
    assert SET <= _on(got, "a5") and not TWINS & _on(got, "a5")
    kinds = {r["section_id"]: r["kind"] for r in got.steelhead}
    assert kinds["lk"] == "lake"
    # mutation: the row is not a steelhead row (no ST line, not flagged) -> the lake is absent
    plain = _atnarko()
    plain.pop("anadromous_rainbow")
    plain["rules"] = plain["rules"][1:]
    got = _run([_province(), plain])
    assert _code(got, "lk") is None and not TWINS & _on(got, "lk")


def test_a_stamp_waiver_row_is_not_a_steelhead_row():
    e = {"entry_id": "r7:stellako_river@7-12", "matched": ["gnis:28"],
         "rules": [{"rule_id": "s.r1", "type": "advisory", "verbatim": "(Steelhead Stamp not "
                    "required)", "extents": [{"op": "whole"}]}]}
    assert not SH.steelhead_row(e)
    assert SH.steelhead_row(dict(e, anadromous_rainbow=True))
    assert SH.steelhead_row(dict(e, rules=[_rule("s.r2", extents=[{"op": "whole"}])]))


def test_a_twin_whose_sibling_does_not_bind_is_unresolved():
    p = _province()
    for r in p["rules"]:
        if r["rule_id"] == "steelhead.r1":
            r["extents"] = [{"op": "within", "area_id": "area:region:7", "feature_types": ["stream"]}]
    got = _run([p], [{"item_id": "gnis:1"}])
    b = next(b for b in got.bindings if b.rule_id == "steelhead.r1b")
    assert b.outcome is Outcome.unresolved and b.reason is Reason.complement_unknown


def test_build_reach_alone_cannot_resolve_the_known_waters():
    p = _province()
    twin = next(r for r in p["rules"] if r["rule_id"] == "steelhead.r1b")
    b, _ = build_reach(p, twin, REG, G)
    assert b.outcome is Outcome.unresolved and b.reason is Reason.needs_corpus


def test_the_catalogue_refuses_the_op_on_a_water_row_and_beside_another_extent():
    base = {"entry_id": "r1:x@1-1", "name": "X", "regs_verbatim": "All wild steelhead must be "
            "released.", "matched": ["gnis:1"], "extents": [{"op": "whole"}]}
    rule = {"rule_id": "x.r1", "type": "retention_limit", "species": ["ST"], "take": 0,
            "may_target": True, "verbatim": "All wild steelhead must be released.",
            "extents": TWIN("x.r0")}
    with pytest.raises(ValueError, match="steelhead_waters"):
        CatalogueEntry.model_validate({**base, "rules": [rule]})
