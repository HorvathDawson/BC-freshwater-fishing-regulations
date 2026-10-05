"""ONE CLASSIFIED WATERS UNIT PER SECTION (user rulings 2026-10-03; RU-13, LI-2).

A walked section takes the designation of the FIRST classified water it flows into (the nearest
downstream water with a designation of its own): Gosnell Creek joins the Morice before the
Bulkley, so it is the Morice's; the Nanika above Morice Lake flows into the Morice, so it is the
Morice's although the Morice's own walk stops at the lake. A tributary with a row of its own that
prints no designation is not classified at all (the Endako under the Stellako's "[Includes
Tributaries]"). `licensing.first_classified_downstream` is the pass; this is its tiny world.
"""
from __future__ import annotations

import pytest

from pipeline.atlas.reach import licensing as L
from pipeline.atlas.reach.licensing import LicensingPlacement, first_classified_downstream
from pipeline.common.models import FlowEdge, NodeKind, StreamGraph, StreamNode
from pipeline.common.models.registry import RegistryItem
from pipeline.regs.parsing.catalogue import Designation

S = NodeKind.stream


def _node(nid, blk, lo, hi, name=""):
    return StreamNode(node_id=nid, kind=S, blk=blk, display_name=name, down_m=lo, up_m=hi,
                      length_m=hi - lo, stream_order=2)


@pytest.fixture
def world():
    """Bulkley B <- Morice M (own designation) <- lake:ML <- Nanika N (no row)
                                <- Gosnell G (own row, no designation) <- G_up
       Bulkley B <- Suskwa K (own designation, Class I) <- K_trib
       Bulkley B <- Creek C (no row)
    The Bulkley's walk reaches everything; the Morice's walk stops at its lake."""
    nodes = [_node("B", "B", 0, 1000, "Bulkley River"), _node("M", "M", 0, 500, "Morice River"),
             StreamNode(node_id="lake:ML", kind=NodeKind.lake, wbk="ML", display_name="Morice Lake"),
             _node("N", "N", 0, 300, "Nanika River"), _node("G", "G", 0, 200, "Gosnell Creek"),
             _node("G_up", "G", 200, 400, "Gosnell Creek"), _node("K", "K", 0, 300, "Suskwa River"),
             _node("K_trib", "KT", 0, 100), _node("C", "C", 0, 100),
             _node("B_hi", "B", 1000, 1500, "Bulkley River"), _node("C2", "C2", 0, 100),
             _node("K_hi", "K", 300, 600, "Suskwa River"), _node("K_hi_trib", "KH", 0, 100),
             _node("N2", "N2", 0, 100, "Nanika River")]
    # N and N2 are a BRAID of the Nanika: each joins the other by a confluence edge (the real
    # Nanika's pieces join each other eleven times before one enters Morice Lake), and the
    # braid edge comes FIRST in the edge order — the walk down must still find the lake.
    edges = [("M", "B", "confluence", 500), ("N", "N2", "confluence", 50),
             ("N2", "N", "confluence", 80), ("lake:ML", "M", "lake_out", 500),
             ("N", "lake:ML", "lake_in", 0), ("G", "M", "confluence", 200),
             ("G_up", "G", "continuation", 200), ("K", "B", "confluence", 800),
             ("K_trib", "K", "confluence", 100), ("C", "B", "confluence", 300),
             ("B_hi", "B", "continuation", 1000), ("C2", "B_hi", "confluence", 1200),
             ("K_hi", "K", "continuation", 300), ("K_hi_trib", "K_hi", "confluence", 400)]
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in nodes}
    g.edges = [FlowEdge(from_node=a, to_node=b, kind=k, at_measure=m) for a, b, k, m in edges]
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    reg = {"gnis:b": RegistryItem(id="gnis:b", name="Bulkley River", kind="stream",
                                  section_ids=("B", "B_hi")),
           "gnis:m": RegistryItem(id="gnis:m", name="Morice River", kind="stream", section_ids=("M",)),
           "gnis:g": RegistryItem(id="gnis:g", name="Gosnell Creek", kind="stream",
                                  section_ids=("G", "G_up")),
           "gnis:k": RegistryItem(id="gnis:k", name="Suskwa River", kind="stream",
                                  section_ids=("K", "K_hi")),
           "gnis:n": RegistryItem(id="gnis:n", name="Nanika River", kind="stream",
                                  section_ids=("N", "N2")),
           "wbk:ML": RegistryItem(id="wbk:ML", name="Morice Lake", kind="lake", section_ids=("lake:ML",))}
    return g, reg


BULKLEY, MORICE, SUSKWA, GOSNELL = "r6:bulkley@6-9", "r6:morice@6-9", "r6:suskwa@6-8", "r6:gosnell@6-9"
BULKLEY2 = "r6:bulkley_upper@6-9"           # a second row of the Bulkley, no designation
OWNED = {"gnis:b": ((BULKLEY, ()), (BULKLEY2, ())), "gnis:m": ((MORICE, ()),),
         "gnis:k": ((SUSKWA, ()),), "gnis:g": ((GOSNELL, ()),)}


def _desig(cls="II", unit="u"):
    return Designation.model_validate({"kind": "designation", "id": "d", "classified": cls, "verbatim": "Class " + cls + " water",
                                       "unit": unit, "unit_name": unit})


def _placements():
    return [
        # the Bulkley's designation is "downstream of B_hi" (B only) and walks everything; C2
        # joins the Bulkley ABOVE its reach, K_hi is the Suskwa above the Suskwa's designation
        LicensingPlacement(BULKLEY, "d", "designation", "sections",
                           sections=("B", "M", "N", "N2", "G", "G_up", "K", "K_trib", "C", "C2",
                                     "K_hi", "K_hi_trib"),
                           via_tributary=("M", "N", "N2", "G", "G_up", "K", "K_trib", "C", "C2",
                                          "K_hi", "K_hi_trib")),
        LicensingPlacement(MORICE, "d", "designation", "sections", sections=("M", "G", "G_up"),
                           via_tributary=("G", "G_up")),
        LicensingPlacement(SUSKWA, "d", "designation", "sections", sections=("K", "K_trib"),
                           via_tributary=("K_trib",)),
    ]


RECORDS = {(BULKLEY, "d"): _desig("II", "bulkley"), (MORICE, "d"): _desig("II", "morice"),
           (SUSKWA, "d"): _desig("I", "suskwa")}


def _run(world, placements=None):
    g, reg = world
    out, diags = first_classified_downstream(
        placements or _placements(), RECORDS, g, reg, OWNED,
        lambda s: "lake" if s.startswith("lake:") else "stream")
    return {(p.entry_id): set(p.sections) for p in out}, diags


def test_one_unit_per_section_the_first_classified_water_downstream(world):
    got, diags = _run(world)
    assert got[BULKLEY] == {"B", "C", "C2"}                # the Morice's and the Suskwa's are theirs
    assert got[MORICE] == {"M", "N", "N2"}                 # the Nanika, re-homed through the lake
    assert got[SUSKWA] == {"K", "K_trib", "K_hi", "K_hi_trib"}
    kinds = {(d.entry_id, d.kind): d.payload for d in diags}
    assert kinds[(BULKLEY, "first_classified_downstream")]["rehomed"] >= 1
    assert kinds[(MORICE, "inherited_from_upstream_walk")] == {"added": 2, "from": [f"{BULKLEY}#d"]}
    units = {}
    for eid, secs in got.items():
        for s in secs:
            units.setdefault(s, set()).add(RECORDS[(eid, "d")].unit)
    assert all(len(u) == 1 for u in units.values())


def test_a_rowed_tributary_printing_no_designation_is_not_classified(world):
    """LI-2: Gosnell Creek has a row of its own and no designation — not the Morice's, not the
    Bulkley's, and nothing above it either."""
    got, diags = _run(world)
    assert not ({"G", "G_up"} & (got[MORICE] | got[BULKLEY]))
    assert any(d.kind == "own_row_not_classified" and d.payload["rows"] == [GOSNELL]
               and d.payload["removed"] == 2 for d in diags if d.entry_id == MORICE)


def test_a_rowed_lake_on_the_way_is_no_stop(world):
    """"Tributaries" are streams: the Nanika flows through Morice Lake, which has a row of its
    own here, and is still the Morice's."""
    owned = {**OWNED, "wbk:ML": (("r6:morice_lake@6-9", ()),)}
    g, reg = world
    out, _ = first_classified_downstream(_placements(), RECORDS, g, reg, owned,
                                         lambda s: "lake" if s.startswith("lake:") else "stream")
    assert "N" in {s for p in out if p.entry_id == MORICE for s in p.sections}


def test_class_and_period_do_not_keep_a_second_unit(world):
    """The Suskwa is Class I under the Bulkley's Class II walk: its tributaries are the Suskwa's
    alone (one unit per section); `why_not_yield` is reported, not obeyed."""
    got, diags = _run(world)
    assert "K_trib" not in got[BULKLEY]
    assert not any(d.payload.get("why_not_yield") for d in diags if d.payload.get("to") == f"{SUSKWA}#d")


def test_mutation_switching_the_policy_off_leaves_two_units(world, monkeypatch):
    monkeypatch.setattr(L, "FIRST_CLASSIFIED_WATER_DOWNSTREAM", False)
    got, diags = _run(world)
    assert got[BULKLEY] >= {"N", "N2", "G", "K_trib"} and not diags


def test_a_braid_is_walked_through_to_the_lake(world):
    """MUTATION GUARD: with the braid edge taken first and never backed out of, the Nanika's
    pieces cycled and stayed the Bulkley's (reach run #3, 18 sections)."""
    g, reg = world
    out, _ = first_classified_downstream(_placements(), RECORDS, g, reg, OWNED,
                                         lambda s: "lake" if s.startswith("lake:") else "stream")
    morice = {s for p in out if p.entry_id == MORICE for s in p.sections}
    assert {"N", "N2"} <= morice


def test_the_walkers_own_water_beyond_its_reach_is_no_stop(world):
    """A second row of the Bulkley (no designation) prints another stretch of the same water: a
    creek joining there is still the Bulkley designation's (its own water is never a stop) —
    the Kootenay above Koocanusa, clipped by its park, kept 606 tributaries this way."""
    got, diags = _run(world)
    assert "C2" in got[BULKLEY]
    assert not any(d.kind == "own_row_not_classified" and BULKLEY2 in d.payload["rows"]
                   for d in diags)


def test_a_rowed_water_with_a_designation_takes_a_section_above_its_reach(world):
    """K_hi is the Suskwa above the Suskwa designation's reach: its row prints a designation, so
    a creek joining there flows first into a classified water — the Suskwa's, not dropped."""
    got, _ = _run(world)
    assert {"K_hi", "K_hi_trib"} <= got[SUSKWA] and not {"K_hi", "K_hi_trib"} & got[BULKLEY]


def test_a_tributaries_only_designation_keeps_its_own_river_as_no_stop(world):
    """"Elk River's tributaries": no reach of its own, every section flows into the Elk first.
    `rowed_waters` leaves a tributaries-only row out, so its river comes from `matched_of`;
    without it every section re-homed to the Elk's mainstem rows and the record was refused."""
    g, reg = world
    tribs_only = "r6:bulkley_tribs@6-9"
    recs = {**RECORDS, (tribs_only, "d"): _desig("II", "bulkley")}
    placements = _placements() + [LicensingPlacement(
        tribs_only, "d", "designation", "sections", sections=("C", "C2"), via_tributary=("C", "C2"))]
    owned = {**OWNED, "gnis:b": ((BULKLEY, ()), (BULKLEY2, ()))}
    out, diags = first_classified_downstream(
        placements, recs, g, reg, owned, lambda s: "lake" if s.startswith("lake:") else "stream",
        matched_of={tribs_only: ["gnis:b"]})
    assert {s for p in out if p.entry_id == tribs_only for s in p.sections} == {"C", "C2"}
    with pytest.raises(AssertionError, match="bound to nothing"):
        first_classified_downstream(placements, recs, g, reg, owned,
                                    lambda s: "lake" if s.startswith("lake:") else "stream")


def test_one_entry_two_designations_each_section_takes_the_reach_it_joins(world):
    """F1 (the Zymoetz): ONE entry prints unit A on the lower stretch (B) and unit B above it
    (B_hi), and both designations walk the same tributaries. The entry's water is each walker's
    own water — but not the reach of its sibling designation: C joins B (A's), C2 joins B_hi
    (B's). Before the fix both kept both, and 115 Limonite/Zymoetz sections read two units."""
    g, reg = world
    recs = {(BULKLEY, "a"): _desig("I", "zymoetz_a"), (BULKLEY, "b"): _desig("II", "zymoetz_b")}
    placements = [
        LicensingPlacement(BULKLEY, "a", "designation", "sections", sections=("B", "C", "C2"),
                           via_tributary=("C", "C2")),
        LicensingPlacement(BULKLEY, "b", "designation", "sections", sections=("B_hi", "C", "C2"),
                           via_tributary=("C", "C2")),
    ]
    out, diags = first_classified_downstream(
        placements, recs, g, reg, {"gnis:b": ((BULKLEY, ()),)},
        lambda s: "lake" if s.startswith("lake:") else "stream")
    got = {p.record_id: set(p.sections) for p in out}
    assert got == {"a": {"B", "C"}, "b": {"B_hi", "C2"}}
    per: dict[str, set[str]] = {}
    for rid, secs in got.items():
        for s in secs:
            per.setdefault(s, set()).add(recs[(BULKLEY, rid)].unit)
    assert all(len(u) == 1 for u in per.values()), per
    assert {(d.rule_id, d.payload["to"]) for d in diags
            if d.kind == "first_classified_downstream"} == {("a", f"{BULKLEY}#b"),
                                                          ("b", f"{BULKLEY}#a")}
