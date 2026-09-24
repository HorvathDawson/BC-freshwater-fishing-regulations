"""The limits the reach builder applies to every binding: the province's border, a regional row's
own region, a rule's feature kinds after the walk — and where a requirement with a place AND an
`on` holds.

Each is pinned by the case that motivated it, and by a mutation: turn the limit off and the test
must go red.
"""

from __future__ import annotations

import pytest

from pipeline.atlas.reach import classify as C
from pipeline.atlas.reach import outside as O
from pipeline.atlas.reach.build import build_reach, entry_scope
from pipeline.atlas.reach.licensing import NO_DESIGNATION, LicensingPlacement, on_designations
from pipeline.atlas.reach.models import Outcome, Reason
from pipeline.common.models import NodeKind, StreamGraph, StreamNode
from pipeline.common.models.registry import RegistryItem
from pipeline.regs.parsing.catalogue import Designation


def _node(nid, kind=NodeKind.stream, out_of_bc=False):
    return StreamNode(node_id=nid, kind=kind, blk="1" if kind is NodeKind.stream else "",
                      down_m=0.0, up_m=1.0, length_m=1.0, out_of_bc=out_of_bc)


def _graph(*nodes):
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in nodes}
    return g


def _item(iid, secs, name=""):
    return RegistryItem(id=iid, name=name or iid, kind="stream", section_ids=tuple(secs))


# A river crossing from Region 5 into Region 3 and on past the border; `s5b` is a border sliver in
# no region; `lk` is a lake in Region 5.
G = _graph(_node("s5"), _node("s5b"), _node("s3"), _node("usa", out_of_bc=True),
           _node("lk", NodeKind.lake))
REG = {"gnis:1": _item("gnis:1", ["s5", "s5b", "s3", "usa", "lk"], "Fraser River"),
       "area:region:5": _item("area:region:5", ["s5", "lk", "usa"]),
       "area:region:3": _item("area:region:3", ["s3"])}


def _entry(eid, rules, **kw):
    return {"entry_id": eid, "matched": ["gnis:1"], "rules": rules, **kw}


# --------------------------------------------------------------------------------- outside B.C.

def test_outside_bc_is_the_border_and_the_regionless_slivers():
    assert O.outside_bc(REG, G) == {"usa", "s5b"}


def test_a_registry_with_no_regions_cannot_say_what_is_outside_a_region():
    """A unit-test registry has no region polygons; then only `out_of_bc` counts, never "all"."""
    g = _graph(_node("a"), _node("b", out_of_bc=True))
    assert O.outside_bc({"i": _item("i", ["a", "b"])}, g) == {"b"}


def test_every_binding_loses_the_water_past_the_border():
    rule = {"rule_id": "r1", "type": "retention_limit", "extents": [{"op": "whole"}]}
    b, diags = build_reach(_entry("z5:x", [rule]), rule, REG, G)
    assert b.outcome is Outcome.bound and set(b.sections) == {"s5", "s3", "lk"}
    assert any(d.kind == "outside_bc" and d.payload["removed"] == 2 for d in diags)


def test_a_rule_wholly_outside_bc_is_unresolved_not_bound_to_nothing(monkeypatch):
    rule = {"rule_id": "r1", "type": "retention_limit",
            "extents": [{"op": "whole", "item_id": "gnis:9"}]}
    reg = dict(REG, **{"gnis:9": _item("gnis:9", ["usa"])})
    b, _ = build_reach(_entry("z5:x", [rule]), rule, reg, G)
    assert b.outcome is Outcome.unresolved and b.reason is Reason.outside_bc
    # mutation: switch the policy off and the water past the border binds
    monkeypatch.setattr(C, "OUTSIDE_BC_SUBTRACTED", False)
    b, _ = build_reach(_entry("z5:x", [rule]), rule, reg, G)
    assert b.outcome is Outcome.bound and b.sections == ("usa",)


# --------------------------------------------------------------------------------- a row's region

@pytest.mark.parametrize("eid,want", [
    ("r5:fraser_river@5-2", ("5",)),
    ("r5:toms_lake@6-1", ("5", "6")),
    ("r1:yakoun_river@6-12", ("1", "6")),
    ("r7:nation_arm_williston_lake@7-30", ("7",)),
    ("z5:spring_stream_closure", None),
    ("zp:basic_licence", None),
])
def test_a_regional_row_names_its_own_region_and_its_mus(eid, want):
    assert O.entry_regions(eid) == want


def test_region_7_is_7a_and_7b():
    reg = {"area:region:7a": _item("area:region:7a", ["a"]),
           "area:region:7b": _item("area:region:7b", ["b"]),
           "area:region:8": _item("area:region:8", ["c"])}
    assert O.region_sections(("7",), reg) == {"a", "b"}


def test_a_regional_row_binds_only_its_own_region():
    """Every Fraser section carried all four regions' Fraser rows: Region 5's bait ban applied at
    Mission."""
    rule = {"rule_id": "r1", "type": "bait_restriction", "extents": [{"op": "whole"}]}
    e5 = _entry("r5:fraser_river@5-2", [rule], extents=[{"op": "whole"}])
    clip, failed = entry_scope(e5, ["gnis:1"], REG, G)
    assert not failed
    b, diags = build_reach(e5, rule, REG, G, clip=clip)
    assert set(b.sections) == {"s5", "lk"}
    assert any(d.kind == "region_clip" for d in diags)
    e3 = _entry("r3:fraser_river@3-17", [rule], extents=[{"op": "whole"}])
    b3, _ = build_reach(e3, rule, REG, G, clip=entry_scope(e3, ["gnis:1"], REG, G)[0])
    assert set(b3.sections) == {"s3"}


def test_the_region_holds_even_when_the_caller_passes_no_clip():
    """The review app and the builder share `build_reach`; the limit lives there, not in a caller."""
    rule = {"rule_id": "r1", "type": "bait_restriction", "extents": [{"op": "whole"}]}
    b, _ = build_reach(_entry("r3:fraser_river@3-17", [rule]), rule, REG, G)
    assert set(b.sections) == {"s3"}


def test_a_reach_the_row_states_is_not_held_to_the_region():
    """The Region 7 Stellako row's fly-only reach (between two sign cut-points below the François
    Lake bridge) lies inside Region 6's polygon. Only `whole` — "the row's water" — is held."""
    from pipeline.atlas.reach.build import _row_water
    assert _row_water({"op": "whole"})
    # a one-sided cut runs to the end of the water: Region 3's "upstream of the Thompson" must
    # stop where Region 3 does
    assert _row_water({"op": "upstream_of", "splits": ["a"]})
    assert _row_water({"op": "downstream_of", "splits": ["a"], "item_id": "gnis:1"})
    assert not _row_water({"op": "whole", "item_id": "gnis:1"})
    # the row's OWN matched water named by id is still the row's water ("Fraser River mainstem")
    assert _row_water({"op": "whole", "item_id": "gnis:1"}, ["gnis:1"])
    assert not _row_water({"op": "whole", "item_id": "gnis:2"}, ["gnis:1"])
    assert not _row_water({"op": "between", "splits": ["a", "b"]})
    assert not _row_water({"op": "within", "area_id": "area:park:x"})
    assert not _row_water({"op": "upstream_of", "splits": ["a"], "within_area": "area:region:3"})
    reg = dict(REG, **{"gnis:2": _item("gnis:2", ["s5", "s3"])})
    rule = {"rule_id": "r1", "type": "tackle_restriction",
            "extents": [{"op": "whole", "item_id": "gnis:2"}]}
    b, _ = build_reach(_entry("r3:fraser_river@3-17", [rule]), rule, reg, G)
    assert set(b.sections) == {"s5", "s3"}


def test_a_zone_entry_is_not_held_to_a_region_by_its_id():
    rule = {"rule_id": "r1", "type": "bait_restriction", "extents": [{"op": "whole"}]}
    b, _ = build_reach(_entry("z3:anything", [rule]), rule, REG, G)
    assert set(b.sections) == {"s5", "s3", "lk"}


# --------------------------------------------------------------------------------- feature kinds

def _reach(sections, **kw):
    return {"sections": list(sections), "unclassified": [], "ambiguous_cut": [], "waters": [], **kw}


def _kind(s):
    return "lake" if s.startswith("lk") else "stream"


def test_feature_types_filter_the_walk_not_just_the_seed():
    """"Lake trout from the Fraser and Skeena watersheds" is every LAKE the walk from those rivers
    finds; applied to the seed alone it kept only lakes on the mainstem — none."""
    rule = {"rule_id": "r1", "type": "retention_limit", "extents": [{"op": "whole"}]}
    per = [_reach(["m1", "m2"], feature_types=["lake"])]
    b, diags = C.classify("z6:x", rule, per, registry={}, covered_ids=[], scope_clipped=False,
                          entry_has_registry=True, tributaries=True,
                          expand_tributaries=lambda s, only=False: set(s) | {"t1", "lk1", "lk2"},
                          kind_of=_kind)
    assert set(b.sections) == {"lk1", "lk2"} and set(b.via_tributary) == {"lk1", "lk2"}
    assert any(d.kind == "feature_types" for d in diags)


def test_a_kind_filter_that_leaves_nothing_is_unresolved(monkeypatch):
    rule = {"rule_id": "r1", "type": "retention_limit", "extents": [{"op": "whole"}]}
    per = [_reach(["m1"], feature_types=["lake"])]
    b, _ = C.classify("z6:x", rule, per, registry={}, covered_ids=[], scope_clipped=False,
                      entry_has_registry=True, kind_of=_kind)
    assert b.outcome is Outcome.unresolved and b.reason is Reason.no_sections
    monkeypatch.setattr(C, "FEATURE_TYPES_AFTER_WALK", False)       # mutation: filter off
    b, _ = C.classify("z6:x", rule, per, registry={}, covered_ids=[], scope_clipped=False,
                      entry_has_registry=True, kind_of=_kind)
    assert b.outcome is Outcome.bound


def test_the_resolver_carries_feature_types_forward_on_any_op():
    from pipeline.atlas.reach import extent as R
    got = R.resolve_extent(REG, G, ["gnis:1"],
                           {"op": "whole", "item_id": "gnis:1", "feature_types": ["Lake"]})
    assert got["feature_types"] == ["lake"]


# --------------------------------------------------------------------------------- `on` + a place

def _placed(eid, rid, kind, secs, via=()):
    return LicensingPlacement(eid, rid, kind, "sections", sections=tuple(secs),
                              via_tributary=tuple(via))


def _d(**kw):
    base = {"id": "d", "classified": "I", "unit": "u", "unit_name": "U",
            "verbatim": "Class I water", "steelhead_stamp_waived": {"verbatim": "Steelhead Stamp not required"}}
    base.update(kw)
    return Designation.model_validate(base)


def test_a_requirement_with_a_place_and_an_on_holds_only_where_a_designation_is():
    """The Dean: "a Classified Waters Licence to fish the classified portions of the Dean River"
    was placed on all 76 Dean sections; on the 40 no designation covers it could never fire."""
    lic = [_placed("r5:dean", "unit1", "designation", ["a", "b"]),
           _placed("z5:dean_cw", "cwl", "requirement", ["a", "b", "c", "d"])]
    records = {("z5:dean_cw", "cwl"): {"kind": "requirement", "on": "classified_period",
                                       "extents": [{"op": "whole", "item_id": "gnis:1"}]}}
    out, diags = on_designations(lic, records, {("r5:dean", "unit1"): _d()})
    req = next(p for p in out if p.record_id == "cwl")
    assert req.sections == ("a", "b")
    assert any(d.kind == "on_narrowed" and d.payload == {"on": "classified_period", "before": 4,
                                                          "after": 2} for d in diags)


def test_a_steelhead_period_needs_a_designation_that_runs_the_stamp():
    lic = [_placed("r5:dean", "unit1", "designation", ["a"]),
           _placed("z5:x", "st", "requirement", ["a"])]
    records = {("z5:x", "st"): {"kind": "requirement", "on": "steelhead_period",
                                "extents": [{"op": "whole"}]}}
    out, _ = on_designations(lic, records, {("r5:dean", "unit1"): _d()})
    req = next(p for p in out if p.record_id == "st")
    assert req.placement == "unresolved" and req.reason == NO_DESIGNATION


def test_an_on_requirement_with_no_extents_is_untouched():
    """With no place of its own it is `on_designation`, and never narrowed here."""
    p = LicensingPlacement("zp:x", "cwl", "requirement", "on_designation")
    out, diags = on_designations([p], {("zp:x", "cwl"): {"on": "classified_period"}}, {})
    assert out == [p] and diags == []


# ------------------------------------------------------------------ licensing and the region line

def test_a_designation_is_not_held_to_the_region_that_prints_it():
    """A classified water is one water, whichever table prints it: held to its row's region, the
    Sustut's Class I tributaries in Region 7A, the Horsefly's in Region 3 and the West Road's in 6
    and 7A lost their designation, and no other row gave it back. The row's RULES still stop at the
    line; the border still holds for both."""
    from pipeline.atlas.reach.build import build_reaches
    rule = {"rule_id": "r1", "type": "bait_restriction", "extents": [{"op": "whole"}]}
    rec = {"kind": "designation", "id": "d", "classified": "II", "unit": "u", "unit_name": "U",
           "verbatim": "Class II water", "extents": [{"op": "whole"}]}
    e = _entry("r3:fraser_river@3-17", [rule], extents=[{"op": "whole"}], licensing=[rec])
    got = build_reaches([e], REG, G)
    assert set(next(b for b in got.bindings).sections) == {"s3"}
    lic = next(p for p in got.licensing if p.record_id == "d")
    assert set(lic.sections) == {"s5", "s3", "lk"}          # every B.C. section of the water
    # mutation: hold licensing to the region too, and the designation shrinks to the row's region
    b, _ = build_reach(e, rec | {"rule_id": "d", "type": "advisory"}, REG, G, regional=True)
    assert set(b.sections) == {"s3"}
