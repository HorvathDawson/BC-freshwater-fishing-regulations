"""A cut AT A CONFLUENCE names the joining water only to say where the reach ends (2026-09-29).

"No Fishing upstream of Morice/Bulkley River confluence" is about the Bulkley; walked, it closed the
Morice and its 2,175 sections all year. The same shape closed the Muchalat (Gold River), the Lardeau
(Duncan River), the Iltasyuko (Dean), Cameron Creek (Mitchell): 36 rules, 10,492 sections (SP-1,
N-1, LS-4, LS-7). The joining water and everything above it stay out of the walk unless the rule's
own words take it in ("upstream of and including Hemmingsen Creek").

Also here: a carve-out that `walk_past`s its water (the Elk River's tributaries row excepts the lower
Fording, whose own row does not take its tributaries, so the creeks feeding it stay the Elk's).
"""

from __future__ import annotations

import pytest

from pipeline.atlas.graph import tributaries as T
from pipeline.atlas.reach import build as B
from pipeline.atlas.reach.build import build_reach, confluence_excludes, resolve_carve_outs
from pipeline.common.models import FlowEdge, NodeKind, StreamGraph, StreamNode
from pipeline.common.models.enums import BoundaryKind
from pipeline.common.models.registry import RegistryBoundary, RegistryItem
from pipeline.common.models.sections import SectionBoundary

S = NodeKind.stream


def _node(nid, blk, lo, hi, *, name="", wsc="", order=2, lower=None, upper=None):
    return StreamNode(node_id=nid, kind=S, blk=blk, wsc=wsc, display_name=name, down_m=lo,
                      up_m=hi, length_m=hi - lo, stream_order=order,
                      lower_bound=lower, upper_bound=upper)


def _cut(sid, m, kind="confluence"):
    return SectionBoundary(boundary_id=f"split:{sid}", kind=BoundaryKind(kind), route_measure=m)


@pytest.fixture
def world():
    """The Main river M in two pieces cut at m=1000 by a CONFLUENCE cut `c` (X Creek's mouth).

        M:1000  (above the cut)  <- T (a creek joining at 1500) <- T2
           |
        M:0     (below the cut)  <- X (X Creek, mouth AT 1000, hung on the piece below)
                                     <- X_up (X Creek carrying on) <- X_trib
                                 <- Y (a creek joining the lower piece at 400)
    A point cut `p` sits at m=1600, labelled "signs near the T Creek confluence" (T joins at 1500).
    """
    nodes = [
        _node("M:0", "M", 0, 1000, name="Main River", wsc="100-1", order=5,
              upper=_cut("c", 1000)),
        _node("M:1000", "M", 1000, 3000, name="Main River", wsc="100-1", order=5,
              lower=_cut("c", 1000), upper=_cut("p", 1600, "split")),
        _node("X", "X", 0, 500, name="X Creek", wsc="100-1-000500", order=3),
        _node("X_up", "X", 500, 900, name="X Creek", wsc="100-1-000500", order=3),
        _node("X_trib", "XT", 0, 100, wsc="100-1-000500-1", order=1),
        _node("T", "T", 0, 300, name="T Creek", wsc="100-1-000700", order=2),
        _node("T2", "T2", 0, 100, wsc="100-1-000700-1", order=1),
        _node("Y", "Y", 0, 300, name="Y Creek", wsc="100-1-000200", order=2),
    ]
    edges = [("M:1000", "M:0", "continuation", 1000), ("X", "M:0", "confluence", 1000),
             ("X_up", "X", "continuation", 500), ("X_trib", "X_up", "confluence", 50),
             ("T", "M:1000", "confluence", 1500), ("T2", "T", "confluence", 100),
             ("Y", "M:0", "confluence", 400)]
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in nodes}
    g.edges = [FlowEdge(from_node=a, to_node=b, kind=k, at_measure=m) for a, b, k, m in edges]
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    bds = (RegistryBoundary(id="c", label="X Creek → Main River", kind="confluence", ref="split:c"),
           RegistryBoundary(id="p", label="signs near the T Creek confluence", kind="split",
                            ref="split:p"))
    reg = {"gnis:1": RegistryItem(id="gnis:1", name="Main River", kind="stream",
                                  section_ids=("M:0", "M:1000"), boundaries=bds),
           "gnis:2": RegistryItem(id="gnis:2", name="X Creek", kind="stream",
                                  section_ids=("X", "X_up")),
           "gnis:3": RegistryItem(id="gnis:3", name="T Creek", kind="stream", section_ids=("T",))}
    return g, reg


ENTRY = {"entry_id": "r9:main@9-1", "matched": ["gnis:1"], "includes_tributaries": True}
X_WATER = {"X", "X_up", "X_trib"}


def _bind(world, verbatim, *extents, entry=ENTRY, **rule):
    g, reg = world
    b, d = build_reach(entry, {"rule_id": "main.r1", "verbatim": verbatim,
                               "extents": list(extents), **rule}, reg, g)
    return set(b.sections), [x for x in d if x.kind == "confluence_cut"]


UP = {"op": "upstream_of", "splits": ["c"]}
DOWN = {"op": "downstream_of", "splits": ["c"]}


def test_the_water_joining_at_the_cut_is_not_walked(world):
    got, diags = _bind(world, "No Fishing upstream of X Creek", UP)
    assert got == {"M:1000", "T", "T2"}
    assert [(x.payload["water"], x.payload["included"], x.payload["kept_out"]) for x in diags] \
        == [("X Creek", False, 3)]


def test_not_on_either_side_of_the_cut(world):
    """Hung on the piece BELOW the cut, the mouth was seeded into "downstream of" and walked; the
    joining water is on neither side."""
    got, _ = _bind(world, "No Fishing downstream of X Creek", DOWN)
    assert got == {"M:0", "Y"}


def test_including_the_joining_water_takes_it_in(world):
    got, diags = _bind(world, "No Fishing upstream of and including X Creek", UP)
    assert X_WATER <= got
    assert diags[0].payload["included"] is True


def test_not_including_does_not(world):
    got, _ = _bind(world, "No Fishing downstream of X Creek, but not including the X Creek", DOWN)
    assert not got & X_WATER


def test_the_book_s_including_carried_in_extent_text(world):
    """The Mitchell's "(including Cameron Creek)" is printed once for a sentence of three rules."""
    got, _ = _bind(world, "bait ban", DOWN,
                   extent_text="downstream of X Creek (including X Creek)")
    assert X_WATER <= got


def test_the_joining_water_that_is_the_reach_itself_is_walked(world):
    """The Atnarko's closure runs on up the South Atnarko: a rule whose reach includes the joining
    water's own mouth is not cut off from that water's tributaries."""
    got, diags = _bind(world, "No Fishing from Tenas Lake", UP, {"op": "whole", "item_id": "gnis:2"})
    assert "X_trib" in got
    assert diags[0].payload["in_reach"] is True and diags[0].payload["kept_out"] == 0


def test_a_rule_that_does_not_walk_is_untouched(world):
    got, diags = _bind(world, "Bait ban upstream of X Creek", UP, includes_tributaries=False)
    assert got == {"M:1000"} and not diags


def test_a_point_cut_near_a_named_confluence(world):
    """"Fishing boundary signs near the Mobbs Creek confluence": the named creek joining within
    CONFLUENCE_NEAR_M of the cut is kept out; its name must be in the label."""
    rows = confluence_excludes(ENTRY, {"rule_id": "r", "verbatim": "No Fishing downstream of signs",
                                       "extents": [{"op": "downstream_of", "splits": ["p"]}]},
                               world[1], world[0], ["gnis:1"])
    assert [(r["water"], sorted(r["sections"])) for r in rows] == [("T Creek", ["T", "T2"])]


def test_a_point_cut_a_stated_distance_from_a_confluence_is_not_at_it(world, monkeypatch):
    """"signs 100 m below the Slesse Creek confluence" closes the Chilliwack upstream, Slesse
    Creek with it: only a label saying NEAR makes a point cut a confluence cut."""
    g, reg = world
    item = reg["gnis:1"]
    bds = (item.boundaries[0], RegistryBoundary(id="p", label="signs 100 m below the T Creek "
                                                "confluence", kind="split", ref="split:p"))
    reg = {**reg, "gnis:1": RegistryItem(id="gnis:1", name=item.name, kind="stream",
                                         section_ids=item.section_ids, boundaries=bds)}
    rows = confluence_excludes(ENTRY, {"rule_id": "r", "verbatim": "No Fishing",
                                       "extents": [{"op": "downstream_of", "splits": ["p"]}]},
                               reg, g, ["gnis:1"])
    assert rows == []


SIGNS_BELOW = ("No Fishing from white triangular fishing boundary signs located downstream of the "
               "X Creek confluence, and upstream to the Hwy 37 bridge, Aug 8-Sept 15")


def test_signs_below_the_confluence_put_it_inside_the_reach(world):
    """The Nass: its closure runs UP from signs below the Meziadin confluence. The curated cut sits
    on the confluence, but the book puts the confluence inside the reach — the Meziadin is walked,
    like Slesse Creek above the Chilliwack's signs (review 2026-09-29)."""
    got, diags = _bind(world, SIGNS_BELOW, UP)
    assert X_WATER <= got
    assert diags[0].payload["in_reach"] is True and diags[0].payload["kept_out"] == 0


def test_signs_below_the_confluence_do_not_pull_it_into_a_reach_below_them(world):
    got, _ = _bind(world, SIGNS_BELOW, DOWN)
    assert not got & X_WATER


def test_mutation_without_the_signs_reading_the_joining_water_is_kept_out(world, monkeypatch):
    monkeypatch.setattr(B, "_signs_below", lambda words, water: False)
    got, _ = _bind(world, SIGNS_BELOW, UP)
    assert not got & X_WATER, "the signs reading is what walks the joining water"


def test_signs_near_a_confluence_are_not_below_it():
    """Lardeau: "signs approximately 600m downstream near the confluence of Mobbs Creek"."""
    assert not B._signs_below("to fishing boundary signs approximately 600m downstream\nnear the "
                              "confluence of Mobbs Creek", "Mobbs Creek")
    assert not B._signs_below("No Fishing downstream of the Muchalat River confluence",
                              "Muchalat River")


def test_mutation_the_policy_switch_is_what_keeps_it_out(world, monkeypatch):
    monkeypatch.setattr(T, "CONFLUENCE_CUT_EXCLUDES_THE_JOINING_WATER", False)
    got, diags = _bind(world, "No Fishing downstream of X Creek", DOWN)
    assert X_WATER <= got and not diags, "without the guard the walk takes X Creek"


def test_mutation_a_negated_including_is_read_as_including(world, monkeypatch):
    """Guard for the "not including" reading: if the words check ignored "not", the Gold River's
    "but not including the Muchalat" would take the Muchalat in."""
    assert B._rule_includes("upstream of and including X Creek", "X Creek")
    assert not B._rule_includes("but not including the X Creek", "X Creek")
    monkeypatch.setattr(B, "_rule_includes", lambda words, water: "includ" in words)
    got, _ = _bind(world, "downstream of X Creek, but not including the X Creek", DOWN)
    assert X_WATER <= got


# ------------------------------------------------------------------ walk_past carve-outs

def test_walk_past_removes_the_water_and_keeps_what_feeds_it(world):
    g, reg = world
    rule = {"rule_id": "r", "verbatim": "No Fishing", "extents": [{"op": "whole"}],
            "tributaries_only": True,
            "tributary_excludes": [{"op": "whole", "item_id": "gnis:2", "walk_past": True}]}
    entry = {**ENTRY, "entry_id": "r9:main_tribs@9-1"}
    detail, blocked = resolve_carve_outs(entry, rule, reg, g, ["gnis:1"])
    assert not blocked and detail[0]["walk_past"] and detail[0]["sections"] == ["X", "X_up"]
    b, _ = build_reach(entry, rule, reg, g)
    assert "X_trib" in b.sections and not {"X", "X_up"} & set(b.sections)


def test_mutation_without_walk_past_the_feeders_go_too(world):
    g, reg = world
    rule = {"rule_id": "r", "verbatim": "No Fishing", "extents": [{"op": "whole"}],
            "tributaries_only": True,
            "tributary_excludes": [{"op": "whole", "item_id": "gnis:2"}]}
    b, _ = build_reach({**ENTRY, "entry_id": "r9:main_tribs@9-1"}, rule, reg, g)
    assert not X_WATER & set(b.sections)


def test_walk_past_is_refused_on_a_place():
    from pipeline.regs.parsing.catalogue import _extent_errors, CatalogueEntry
    e = CatalogueEntry.model_construct(
        entry_id="r9:x", extents=[{"op": "whole", "walk_past": True}], rules=[], licensing=[])
    assert any("walk_past belongs on a tributary_excludes" in x for x in _extent_errors(e))


def test_a_designation_still_walks_into_the_joining_water(world, monkeypatch):
    """Licensing's unsafe direction is requiring too little: a designation cut at a confluence keeps
    the joining water (`LICENSING_WALKS_INTO_CONFLUENCE_WATERS`). MUTATION: switched off, the
    designation drops it."""
    got, diags = _bind(world, "Class II water downstream of X Creek", DOWN, type="designation")
    assert X_WATER <= got and not diags
    monkeypatch.setattr(T, "LICENSING_WALKS_INTO_CONFLUENCE_WATERS", False)
    got, _ = _bind(world, "Class II water downstream of X Creek", DOWN, type="designation")
    assert not got & X_WATER
