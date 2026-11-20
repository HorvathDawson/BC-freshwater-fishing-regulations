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


def _excepting_gosnell(walk_past: bool) -> Designation:
    """The Bulkley's designation printing "(EXCEPT Gosnell Creek downstream of X)": the Elk's
    "ALL tributaries (EXCEPT Coal Creek downstream of old MF&M Railway Bridge …) are Class II"."""
    return Designation.model_validate({
        "kind": "designation", "id": "d", "classified": "II", "verbatim": "Class II water",
        "unit": "bulkley", "unit_name": "bulkley",
        "tributary_excludes": [{"op": "downstream_of", "item_id": "gnis:g", "splits": ["g_bridge"],
                                **({"walk_past": True} if walk_past else {})}]})


def _coal_creek_run(world, walk_past: bool):
    """G is Gosnell below the bridge (carved out of the walk), G_up above it. Gosnell's own row
    ("Part described is NOT a Classified Water") prints no designation."""
    g, reg = world
    recs = {(BULKLEY, "d"): _excepting_gosnell(walk_past)}
    placements = [LicensingPlacement(BULKLEY, "d", "designation", "sections",
                                     sections=("B", "M", "G_up"), via_tributary=("M", "G_up"))]
    out, diags = first_classified_downstream(
        placements, recs, g, reg, {"gnis:b": ((BULKLEY, ()),), "gnis:g": ((GOSNELL, ()),)},
        lambda s: "lake" if s.startswith("lake:") else "stream")
    return {s for p in out for s in p.sections}, diags


def test_a_part_the_designation_excepts_and_walks_past_is_no_stop(world):
    """T798 item 1 (owner 2026-11-16, question 31: "Class II above the bridge"): the Elk's
    designation excepts ONLY Coal Creek below the old MF&M Railway bridge and walks past it
    (`walk_past`); Coal Creek's row un-classifies "the part described". Above the bridge Coal
    Creek is still an Elk tributary and Class II — Li5's "a rowed tributary printing no
    designation, and everything above it" does not reach past a part the designation itself
    carves out and walks past. 190 sections were dropped (`own_row_not_classified`)."""
    got, diags = _coal_creek_run(world, walk_past=True)
    assert "G_up" in got
    assert not any(d.kind == "own_row_not_classified" for d in diags)


def test_without_walk_past_the_rowed_water_still_stops_the_walk(world):
    """MUTATION GUARD for the exception's narrowness: a carve-out that does not walk past (or no
    carve-out at all) leaves Li5 as it was — the rowed creek and everything above it drop."""
    got, diags = _coal_creek_run(world, walk_past=False)
    assert "G_up" not in got
    assert any(d.kind == "own_row_not_classified" and d.payload["rows"] == [GOSNELL] for d in diags)


def test_mutation_switching_the_walk_past_exception_off_drops_it(world, monkeypatch):
    monkeypatch.setattr(L, "A_WALKED_PAST_EXCEPTION_IS_NO_STOP", False)
    got, _ = _coal_creek_run(world, walk_past=True)
    assert "G_up" not in got


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


# --- T944: A DEAD END FLOWS WHERE ITS WATER FLOWS (`tributaries.A_DEAD_END_FLOWS_WHERE_ITS_WATER_FLOWS`) ---------

def _with_dead_ends(world):
    """The world plus two creeks with a DEAD END each (`tributaries.A_DEAD_END_GOES_WITH_ITS_WATER` hands the walks
    those sinks): Shea Creek SH (code g-1) joins Gosnell's G_up, and its side piece SH_dead leaves SH and stops —
    Shea Creek 773423, which kept the Bulkley's AND the Morice's units; creek T (code m-1) joins the Morice, and its
    dead end T_dead was left with the Bulkley's walk alone (774476)."""
    import dataclasses
    g, reg = world
    for nid, blk, code in (("SH", "SH", "g-1"), ("SH_dead", "SHD", "g-1"), ("T", "T", "m-1"),
                           ("T_dead", "TD", "m-1")):
        g.nodes[nid] = dataclasses.replace(_node(nid, blk, 0, 100), wsc=code)
    for a, b in (("SH", "G_up"), ("SH", "SH_dead"), ("T", "M"), ("T", "T_dead")):
        g.edges.append(FlowEdge(from_node=a, to_node=b, kind="confluence", at_measure=50))
        i = len(g.edges) - 1
        g.up_adj.setdefault(b, []).append(i)
        g.down_adj.setdefault(a, []).append(i)
    pl = _placements()
    more = ("SH", "SH_dead", "T", "T_dead")
    pl[0] = LicensingPlacement(BULKLEY, "d", "designation", "sections", sections=pl[0].sections + more,
                               via_tributary=pl[0].via_tributary + more)
    pl[1] = LicensingPlacement(MORICE, "d", "designation", "sections", sections=pl[1].sections + more,
                               via_tributary=pl[1].via_tributary + more)
    return (g, reg), pl


def test_a_dead_end_takes_the_unit_its_water_takes(world):
    w, pl = _with_dead_ends(world)
    got, _ = _run(w, pl)
    assert not {"SH", "SH_dead"} & (got[BULKLEY] | got[MORICE])   # Shea flows into Gosnell: no unit, nor its end
    assert {"T", "T_dead"} <= got[MORICE] and not {"T", "T_dead"} & got[BULKLEY]
    per: dict[str, int] = {}
    for secs in got.values():
        for s in secs:
            per[s] = per.get(s, 0) + 1
    assert all(n == 1 for n in per.values()), per


def test_a_rivers_mouth_keeps_nothing_below(world):
    """The Bulkley's mouth B has no code that leaves itself elsewhere: not a dead end, nothing to follow."""
    import dataclasses
    from pipeline.atlas.graph import tributaries as T
    w, _pl = _with_dead_ends(world)
    for nid in ("B", "B_hi"):                  # the Bulkley's own code: its mouth B is a sink, the code never leaves
        w[0].nodes[nid] = dataclasses.replace(w[0].nodes[nid], wsc="400")
    out = T.dead_end_outlets(w[0])
    assert out == {"SH_dead": ("G_up",), "T_dead": ("M",)}


def _mouth_with_a_distributary():
    """A river R (code r, main flow 1000) whose mouth R_mouth is a sink (the sea); upstream of it, at R_hi, a distributary D
    leaves the code (code d, to the sea on its own), and a side piece R_side (1100) leaves R_hi and stops (a dead end)."""
    import dataclasses
    g = StreamGraph()
    for nid, code, et in (("R_hi", "r", ("1000",)), ("R_mouth", "r", ("1000",)), ("R_side", "r", ("1100",)),
                          ("D", "d", ("1100",))):
        g.nodes[nid] = dataclasses.replace(_node(nid, nid, 0, 100), wsc=code, edge_types=et)
    g.edges = [FlowEdge(from_node=a, to_node=b, kind="confluence", at_measure=0)
               for a, b in (("R_hi", "R_mouth"), ("R_hi", "D"), ("R_hi", "R_side"))]
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    return g


def test_a_mouth_never_flows_back_up(monkeypatch):
    """(T944 review, LF) `A_MOUTH_NEVER_FLOWS_BACK_UP`: a sink on its code's MAIN FLOW is the river's mouth. Its code
    leaving itself upstream (a distributary at R_hi) is not where the mouth's water goes: no outlet, "nothing below" as
    before T944. The side piece off R_hi is a dead end and still flows where its water flows.
    MUTATION: the guard off, the mouth is sent back up into the distributary."""
    from pipeline.atlas.graph import tributaries as T
    assert T.dead_end_outlets(_mouth_with_a_distributary()) == {"R_side": ("D",)}
    monkeypatch.setattr(T, "A_MOUTH_NEVER_FLOWS_BACK_UP", False)
    assert T.dead_end_outlets(_mouth_with_a_distributary()) == {"R_mouth": ("D",), "R_side": ("D",)}


def test_mutation_switching_dead_end_outlets_off_leaves_two_units(world, monkeypatch):
    from pipeline.atlas.graph import tributaries as T
    monkeypatch.setattr(T, "A_DEAD_END_FLOWS_WHERE_ITS_WATER_FLOWS", False)
    w, pl = _with_dead_ends(world)
    got, _ = _run(w, pl)
    assert "SH_dead" in got[BULKLEY] and "SH_dead" in got[MORICE]


# --- T973: A UNIT CARRIES THROUGH A ROWED NO-UNIT WATER, AS A "MAY NEED" (`A_ROWED_NO_UNIT_WATER_CARRIES_THE_UNIT`) ----

def _carry(world, placements=None, records=None, owned=None):
    """The pass with and without `carried`: (placements, diagnostics, carried) of the carrying run, after checking that
    asking for the carried units moves no placement and no diagnostic (the empty-`through` placements are as before)."""
    g, reg = world
    args = (records or RECORDS, g, reg, owned or OWNED, lambda s: "lake" if s.startswith("lake:") else "stream")
    before, d0 = first_classified_downstream(placements or _placements(), *args)
    carried: list[L.CarriedUnit] = []
    after, d1 = first_classified_downstream(placements or _placements(), *args, carried=carried)
    assert after == before and d1 == d0
    kept, stats = L.carried_units(carried, after)
    return after, d1, kept, stats


def test_a_section_above_a_rowed_no_unit_water_keeps_the_unit_below_it_with_its_path(world):
    """Gosnell Creek's row prints no unit (LI-2 drops it and everything above it); the walk goes on down to the Morice:
    each Gosnell section keeps the Morice's unit as a "may need", through Gosnell Creek."""
    out, diags, kept, stats = _carry(world)
    assert not {"G", "G_up"} & {s for p in out for s in p.sections}            # still dropped from the placements
    assert {(c.section, c.entry_id, c.record_id, c.through) for c in kept} == {
        ("G", MORICE, "d", (("gnis:g", "Gosnell Creek"),)),
        ("G_up", MORICE, "d", (("gnis:g", "Gosnell Creek"),))}
    assert stats == {"placed_elsewhere": 0, "disagree": 0}


def _with_more(world):
    """Hidden Creek H (its own row, no unit) joins Gosnell's G_up, and creek H_up joins H; Upper Class I creek X (its own
    designation) joins G_up too, with X_trib above it. The Morice's walk reaches all of them; X's own walk, X_trib."""
    g, reg = world
    for nid, blk, name in (("H", "H", "Hidden Creek"), ("H_up", "HU", ""), ("X", "X", "Upper Creek"),
                           ("X_trib", "XT", "")):
        g.nodes[nid] = _node(nid, blk, 0, 100, name)
    for a, b in (("H", "G_up"), ("H_up", "H"), ("X", "G_up"), ("X_trib", "X")):
        g.edges.append(FlowEdge(from_node=a, to_node=b, kind="confluence", at_measure=50))
        i = len(g.edges) - 1
        g.up_adj.setdefault(b, []).append(i)
        g.down_adj.setdefault(a, []).append(i)
    reg = {**reg, "gnis:h": RegistryItem(id="gnis:h", name="Hidden Creek", kind="stream", section_ids=("H",)),
           "gnis:x": RegistryItem(id="gnis:x", name="Upper Creek", kind="stream", section_ids=("X",))}
    upper = "r6:upper@6-9"
    owned = {**OWNED, "gnis:h": (("r6:hidden@6-9", ()),), "gnis:x": ((upper, ()),)}
    more = ("H", "H_up", "X", "X_trib")
    pl = _placements()
    pl[1] = LicensingPlacement(MORICE, "d", "designation", "sections", sections=pl[1].sections + more,
                               via_tributary=pl[1].via_tributary + more)
    pl.append(LicensingPlacement(upper, "d", "designation", "sections", sections=("X", "X_trib"),
                                 via_tributary=("X_trib",)))
    recs = {**RECORDS, (upper, "d"): _desig("I", "upper")}
    return (g, reg), pl, recs, owned, upper


def test_two_rowed_waters_are_both_in_through_in_order_up_from_the_classified_water(world):
    w, pl, recs, owned, _ = _with_more(world)
    _, _, kept, _ = _carry(w, pl, recs, owned)
    got = {c.section: (c.entry_id, c.through) for c in kept}
    assert got["H"] == got["H_up"] == (MORICE, (("gnis:g", "Gosnell Creek"), ("gnis:h", "Hidden Creek")))


def test_an_upstream_classified_waters_own_reach_still_wins(world):
    """RU-13: Upper Creek's own Class I reach is the first classified water X_trib flows into; the Morice's Class II
    below Gosnell is never carried past it."""
    w, pl, recs, owned, upper = _with_more(world)
    out, _, kept, _ = _carry(w, pl, recs, owned)
    assert not {"X", "X_trib"} & {c.section for c in kept}
    assert {s for p in out if p.entry_id == upper for s in p.sections} == {"X", "X_trib"}


def test_a_carried_unit_never_lands_where_a_designation_holds_the_section(world):
    """One walk drops a section, another places it: it has its unit, and no note beside it. Two carried units for one
    section collapse to one."""
    c = L.CarriedUnit("G", MORICE, "d", (("gnis:g", "Gosnell Creek"),))
    other = L.CarriedUnit("G", BULKLEY, "d", (("gnis:g", "Gosnell Creek"),))
    held = [LicensingPlacement(SUSKWA, "d", "designation", "sections", sections=("K",))]
    assert L.carried_units([c, c], held) == ([c], {"placed_elsewhere": 0, "disagree": 0})
    assert L.carried_units([c, other], held) == ([other], {"placed_elsewhere": 0, "disagree": 1})
    k = L.CarriedUnit("K", MORICE, "d", (("gnis:g", "Gosnell Creek"),))
    assert L.carried_units([k], held) == ([], {"placed_elsewhere": 1, "disagree": 0})
    with pytest.raises(ValueError, match="passed no rowed water"):
        L.CarriedUnit("G", MORICE, "d", ())


def test_mutation_switching_the_carry_off_carries_nothing(world, monkeypatch):
    """The pin: with `A_ROWED_NO_UNIT_WATER_CARRIES_THE_UNIT` off, Gosnell Creek keeps no unit at all (LI-2 as shipped)."""
    monkeypatch.setattr(L, "A_ROWED_NO_UNIT_WATER_CARRIES_THE_UNIT", False)
    _, _, kept, _ = _carry(world)
    assert kept == []


# --- T973 (owner 2026-11-20): nearest wins whatever the class; only a unit that reaches its tributaries claims them ----

ALL_CLAIM = {(BULKLEY, "d"), (MORICE, "d"), (SUSKWA, "d")}


@pytest.mark.parametrize("down, up", [("II", "I"), ("I", "II")])
def test_the_nearest_classified_water_wins_whatever_its_class(world, down, up):
    """"An upstream Class I overrules a downstream Class II too" (owner): the Suskwa (`up`) over the Bulkley's walk
    (`down`), and the Morice over it on the Nanika, both ways round."""
    g, reg = world
    recs = {(BULKLEY, "d"): _desig(down, "bulkley"), (MORICE, "d"): _desig(up, "morice"),
            (SUSKWA, "d"): _desig(up, "suskwa")}
    out, _ = first_classified_downstream(_placements(), recs, g, reg, OWNED,
                                         lambda s: "lake" if s.startswith("lake:") else "stream", claims=ALL_CLAIM)
    got = {p.entry_id: set(p.sections) for p in out}
    assert {"K_trib", "K_hi_trib"} <= got[SUSKWA] and {"N", "N2"} <= got[MORICE]
    assert not {"K_trib", "K_hi_trib", "N", "N2"} & got[BULKLEY]


def _suskwa_on_its_own_water(world, claims):
    """The Suskwa's row covers ONLY its own water (no "[Includes Tributaries]"): its designation walks nothing."""
    pl = _placements()
    pl[2] = LicensingPlacement(SUSKWA, "d", "designation", "sections", sections=("K",))
    g, reg = world
    args = (RECORDS, g, reg, OWNED, lambda s: "lake" if s.startswith("lake:") else "stream")
    plain, d0 = first_classified_downstream(pl, *args, claims=claims)
    carried: list[L.CarriedUnit] = []
    out, d1 = first_classified_downstream(pl, *args, claims=claims, carried=carried)
    assert out == plain and d1 == d0
    return {p.entry_id: set(p.sections) for p in out}, d1, L.carried_units(carried, out)[0]


def test_a_unit_that_stops_at_its_own_water_claims_nothing_above_it(world):
    """The Bulkley's walk meets the Suskwa (Class I on its own water only): K_trib, K_hi and K_hi_trib are not the
    Suskwa's; they leave the placements and keep the Bulkley's unit as a "may need", through the Suskwa."""
    got, diags, kept = _suskwa_on_its_own_water(world, {(BULKLEY, "d"), (MORICE, "d")})
    assert got[SUSKWA] == {"K"} and not {"K_trib", "K_hi", "K_hi_trib"} & got[BULKLEY]
    assert {c.section: (c.entry_id, c.through) for c in kept if c.section.startswith("K")} == {
        s: (BULKLEY, (("gnis:k", "Suskwa River"),)) for s in ("K_trib", "K_hi", "K_hi_trib")}
    assert any(d.kind == "unit_not_on_tributaries" and d.payload == {"removed": 3, "passed": [f"{SUSKWA}#d"]}
               for d in diags)


def test_mutation_switching_the_tributary_claim_off_lets_any_unit_claim(world, monkeypatch):
    monkeypatch.setattr(L, "ONLY_A_UNIT_ON_ITS_TRIBUTARIES_CLAIMS_THEM", False)
    got, diags, kept = _suskwa_on_its_own_water(world, {(BULKLEY, "d"), (MORICE, "d")})
    assert {"K_trib", "K_hi", "K_hi_trib"} <= got[SUSKWA]
    assert not any(c.section.startswith("K") for c in kept)
    assert not any(d.kind == "unit_not_on_tributaries" for d in diags)
