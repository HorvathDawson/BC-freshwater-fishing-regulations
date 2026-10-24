"""THE BRAID SWEEP (T67): a cut also cuts a side channel of its river — but only one that attaches to the
host on BOTH sides of the cut (owner 2026-10-24: "a cut may only take a braid that attaches to the mainstem
on both sides of the cut point"; South Atnarko, a network touching one side only, is never cut).

The twelve R14 fixtures (`handoff/PLAN/braids/r14_braid_split_plan.md` §Tests) and the mutation pins:
the gate off cuts the one-sided South-Atnarko network; the edge pass off leaks the braid across the cut;
the own-water check off cuts another water's network. Fixture 12 (the reach) is `test_reach_braid_cuts.py`;
the length cuts' junction choice is `test_length_braids.py`. No data: each graph is a few hand-drawn lines.

Coordinates are metres; the host H runs along y = 0 from its mouth at x = 0, so its route measure IS x.
"""

from __future__ import annotations

import random

import pytest
from shapely.geometry import LineString

from pipeline.atlas.splits import braid_sweep as BS
from pipeline.atlas.splits.sectionizer import split_graph_at
from pipeline.common.models import AnchorType, FlowEdge, NodeKind, SplitPoint, StreamGraph, StreamNode

S = NodeKind.stream


def _piece(blk, coords, *, down=0.0, gnis="", name="", wsc="100", et=("1000",)):
    ln = LineString(coords)
    up = down + ln.length
    return StreamNode(node_id=f"{blk}:{int(down)}", kind=S, blk=blk, wsc=wsc, gnis_id=gnis,
                      display_name=name, down_m=down, up_m=up, length_m=ln.length,
                      edge_types=tuple(et)), ln


class _G:
    """A host H (gnis 9, 'X River', wsc 100) plus side lines; `join(a, b, at)` = a flows into b."""

    def __init__(self, host=((0, 0), (3000, 0))):
        self.g = StreamGraph()
        self.geoms: dict = {}
        self.add("H", list(host), gnis="9", name="X River")

    def add(self, blk, coords, **kw):
        n, ln = _piece(blk, coords, **kw)
        self.g.nodes[n.node_id] = n
        self.geoms[n.node_id] = ln
        return n.node_id

    def join(self, a, b, at, xy=None):
        x, y = xy if xy is not None else (0.0, 0.0)
        self.g.edges.append(FlowEdge(from_node=a, to_node=b, at_measure=float(at), x=x, y=y))

    def braid(self, blk, coords, mouth_on_host, head_on_host, xy_head=None, **kw):
        """A side channel leaving the host at `head_on_host` and rejoining it at `mouth_on_host`."""
        nid = self.add(blk, coords, **kw)
        n = self.g.nodes[nid]
        self.join(nid, "H:0", mouth_on_host, xy=coords[0])
        self.join("H:0", nid, n.up_m, xy=xy_head if xy_head is not None else coords[-1])
        return nid

    def done(self):
        up, down = {}, {}
        for i, e in enumerate(self.g.edges):
            up.setdefault(e.to_node, []).append(i)
            down.setdefault(e.from_node, []).append(i)
        self.g.up_adj, self.g.down_adj = up, down
        return self.g, self.geoms


def _cut(sid, m, blk="H", source="curated"):
    return SplitPoint(split_id=sid, blk=blk, route_measure=float(m), fid="", label=sid,
                      anchor_type=AnchorType.point, source=source)


def _apply(g, geoms, pts):
    """What build.py does: sweep on the uncut graph, cut the hosts, cut the braids, edges, re-homes."""
    res = BS.sweep(g, geoms, pts)
    split_graph_at(g, geoms, pts, proximity_pickup=True)
    split_graph_at(g, geoms, res.points, proximity_pickup=False)
    BS.apply_rehomes(g, res.rehomes)
    BS.make_edges_consistent(g, geoms, res)
    return res, BS.residual_straddles(g, geoms, pts)


def _pieces(g, blk):
    return sorted((n for n in g.nodes.values() if n.blk == blk), key=lambda n: n.down_m)


def _host_sides_of(g, nid, M):
    """Which side of M each host piece this node touches lies on."""
    out = set()
    for e in g.edges:
        o = e.to_node if e.from_node == nid else e.from_node if e.to_node == nid else None
        if o is None or e.kind == "continuation":
            continue
        n = g.nodes[o]
        if n.blk == "H":
            out.add("lo" if n.up_m <= M + 1e-6 else "hi")
    return out


# the standard braid: rejoins the host at 800, leaves it at 1200, runs 100 m north of it
BRAID = [(800, 0), (850, 100), (1150, 100), (1200, 0)]


def _one_braid(**kw):
    G = _G()
    G.braid("B", BRAID, 800, 1200, **kw)
    return G.done()


# ---------------------------------------------------------------- 1. one braid across one cut


def test_1_a_braid_across_one_cut_is_cut_once_and_each_half_meets_its_own_side():
    g, geoms = _one_braid()
    res, resid = _apply(g, geoms, [_cut("c", 1000)])
    assert [c.gate for c in res.cases] == ["cut"]
    assert len(res.points) == 1 and res.points[0].blk == "B" and res.points[0].split_id == "c"
    lo, hi = _pieces(g, "B")
    assert _host_sides_of(g, lo.node_id, 1000) == {"lo"}
    assert _host_sides_of(g, hi.node_id, 1000) == {"hi"}
    assert lo.upper_bound.boundary_id == "split:c"          # the split's OWN id, on the braid
    assert resid == []


# ---------------------------------------------------------------- 2. both cuts of a between


def test_2_a_braid_across_both_cuts_of_a_between_is_three_pieces_the_middle_inside():
    g, geoms = _one_braid()
    res, resid = _apply(g, geoms, [_cut("lo", 950), _cut("hi", 1050)])
    ps = _pieces(g, "B")
    assert len(ps) == 3
    assert ps[1].lower_bound.boundary_id == "split:lo" and ps[1].upper_bound.boundary_id == "split:hi"
    assert _host_sides_of(g, ps[1].node_id, 950) == set()   # the middle touches no host piece
    assert resid == []


# ---------------------------------------------------------------- 3. an uncrossed partner


def test_3_a_network_partner_the_line_does_not_cross_follows_by_topology():
    G = _G()
    G.braid("B", BRAID, 800, 1200)
    # C leaves the host at 1600 and flows into B's top half: through, never crossed
    c = G.add("C", [(1150, 100), (1400, 150), (1600, 0)])
    G.join(c, "B:0", G.g.nodes["B:0"].up_m - 60)
    G.join("H:0", c, G.g.nodes[c].up_m, xy=(1600, 0))
    g, geoms = G.done()
    res, resid = _apply(g, geoms, [_cut("c", 1000)])
    assert {p.blk for p in res.points} == {"B"}             # only the crossed piece is cut
    assert len(_pieces(g, "C")) == 1
    assert resid == []


# ---------------------------------------------------------------- 4. junction within 10 m


def test_4_a_junction_within_10_m_of_the_cut_is_rehomed_not_cut():
    G = _G()
    G.braid("B", [(995, 0), (1000, 3), (1400, 3), (1500, 0)], 995, 1500)
    g, geoms = G.done()
    res, resid = _apply(g, geoms, [_cut("c", 1000)])
    assert res.points == []
    assert [c.gate for c in res.cases] == ["rehomed: junction <10 m"]
    assert len(_pieces(g, "B")) == 1                        # no sliver
    assert _host_sides_of(g, "B:0", 1000) == {"hi"}
    assert resid == []


# ---------------------------------------------------------------- 5. South Atnarko (one side, far)


def _south_atnarko():
    """A long side network touching the host ONLY ABOVE the cut (at 1500 and 2600), routed back across
    the cut's line 1.5 km south of the host — South Atnarko → Atnarko, which the first review cut."""
    G = _G()
    G.braid("SA", [(1500, 0), (1500, -1500), (900, -1500), (900, -1800), (2600, -1800), (2600, 0)], 1500, 2600)
    return G.done()


def test_5_a_network_touching_one_side_only_is_never_cut_however_the_line_crosses_it():
    g, geoms = _south_atnarko()
    res, resid = _apply(g, geoms, [_cut("c", 1000)])
    assert res.points == []
    assert [c.gate for c in res.cases] == ["refused: one side"]
    assert len(_pieces(g, "SA")) == 1


def test_5_mutation_the_gate_off_cuts_south_atnarko(monkeypatch):
    monkeypatch.setattr(BS, "GATE_REFUSES_ONE_SIDE", False)
    g, geoms = _south_atnarko()
    res, _ = _apply(g, geoms, [_cut("c", 1000)])
    assert res.points, "with the gate off the one-sided network is cut — the gate is what refuses it"


# ---------------------------------------------------------------- 6. a bulge / ditch grid


def test_6_a_bulge_on_one_side_is_not_cut():
    G = _G()
    # leaves and rejoins the host above the cut, bulging back west across the line (an oxbow)
    G.braid("O", [(1100, 0), (900, -200), (1500, -200), (1500, 0)], 1100, 1500)
    # and a Serpentine-style 2300 ditch grid below it, one side only
    G.braid("D", [(300, 0), (300, 150), (700, 150), (700, 0)], 300, 700, et=("2300",))
    g, geoms = G.done()
    res, _ = _apply(g, geoms, [_cut("c", 1000)])
    assert res.points == []
    assert {c.gate for c in res.cases} <= {"refused: one side"}


# ---------------------------------------------------------------- 7. FWA x/y 1 km off


def test_7_an_edge_whose_xy_is_1_km_off_is_rehomed_by_end_geometry():
    G = _G()
    G.braid("B", BRAID, 800, 1200, xy_head=(200.0, 900.0))  # FWA's x/y for the head: 1 km away
    g, geoms = G.done()
    res, resid = _apply(g, geoms, [_cut("c", 1000)])
    assert [c.gate for c in res.cases] == ["cut"]           # sided by geometry, so it straddles
    lo, hi = _pieces(g, "B")
    assert _host_sides_of(g, hi.node_id, 1000) == {"hi"}    # the head edge left the wrong piece
    assert _host_sides_of(g, lo.node_id, 1000) == {"lo"}
    assert resid == []


def test_7_mutation_the_edge_pass_off_leaks_the_braid_across_the_cut(monkeypatch):
    monkeypatch.setattr(BS, "EDGES_REHOMED_BY_GEOMETRY", False)
    G = _G()
    G.braid("B", BRAID, 800, 1200, xy_head=(200.0, 900.0))
    g, geoms = G.done()
    _apply(g, geoms, [_cut("c", 1000)])
    lo, hi = _pieces(g, "B")
    # _split_one trusted the x/y: the upper half meets only the host BELOW the cut — a leak
    assert _host_sides_of(g, hi.node_id, 1000) == {"lo"}


# ---------------------------------------------------------------- 8. another water's network


def _other_water():
    G = _G()
    G.braid("B", BRAID, 800, 1200, gnis="77", name="Other Slough")
    return G.done()


def test_8_a_straddling_network_of_another_water_is_not_cut():
    g, geoms = _other_water()
    res, _ = _apply(g, geoms, [_cut("c", 1000)])
    assert res.points == []
    assert [c.gate for c in res.cases] == ["refused: another water"]


def test_8_mutation_the_own_water_check_off_cuts_it(monkeypatch):
    monkeypatch.setattr(BS, "ONLY_THE_HOSTS_OWN_WATER", False)
    g, geoms = _other_water()
    res, _ = _apply(g, geoms, [_cut("c", 1000)])
    assert res.points


def test_8b_a_ditch_is_not_the_rivers_own_water():
    G = _G()
    G.braid("D", BRAID, 800, 1200, et=("2300",))            # 2300-only, no gnis: owner Q1
    g, geoms = G.done()
    res, _ = _apply(g, geoms, [_cut("c", 1000)])
    assert res.points == [] and [c.gate for c in res.cases] == ["refused: another water"]


# ---------------------------------------------------------------- 9. the line re-crosses the host


def test_9_the_line_is_clipped_where_it_recrosses_the_host():
    host = [(0, 0), (1200, 0), (1200, -300), (800, -300), (800, -600), (3000, -600)]
    G = _G(host=host)
    hl = G.g.nodes["H:0"]
    # B joins the host at x=500 (measure 500) and at (2500,-600), passing 900 m south: beyond the clip
    b = G.add("B", [(500, 0), (500, -900), (2500, -900), (2500, -600)])
    G.join(b, "H:0", 500.0, xy=(500, 0))
    head_m = BS.View(G.g, G.geoms).measure_of("H", __import__("shapely").geometry.Point(2500, -600))[0]
    G.join("H:0", b, G.g.nodes[b].up_m, xy=(2500, -600))
    g, geoms = G.done()
    assert hl.up_m > head_m > 1000
    res, _ = _apply(g, geoms, [_cut("c", 1000)])
    assert res.points == []
    assert [c.gate for c in res.cases] == ["refused: line misses"]


# ---------------------------------------------------------------- 10. multi-crossing


def test_10_a_piece_crossing_the_line_several_times_is_cut_once_at_the_nearest():
    G = _G()
    G.braid("B", [(800, 0), (800, 100), (1100, 100), (1100, 200), (900, 200), (900, 300),
                  (1200, 300), (1200, 0)], 800, 1200)
    g, geoms = G.done()
    view = BS.View(g, geoms)
    res = BS.sweep(g, geoms, [_cut("c", 1000)], view=view)
    assert len(res.points) == 1
    q = view.point_on_piece("B:0", res.points[0].route_measure)
    assert abs(q.x - 1000) < 1e-6 and abs(q.y - 100) < 1e-6  # the crossing nearest the cut point


# ---------------------------------------------------------------- 11. order does not matter


def test_11_shuffled_inputs_give_identical_split_points():
    def build(seed):
        G = _G()
        G.braid("B", BRAID, 800, 1200)
        G.braid("E", [(1800, 0), (1850, -80), (2150, -80), (2200, 0)], 1800, 2200)
        g, geoms = G.done()
        rnd = random.Random(seed)
        items = list(g.nodes.items())
        rnd.shuffle(items)
        g.nodes = dict(items)
        pts = [_cut("a", 1000), _cut("b", 2000), _cut("g", 1990, source="gauge")]
        rnd.shuffle(pts)
        return BS.sweep(g, geoms, pts).points

    first = build(0)
    assert first and all(build(s) == first for s in range(1, 6))


# ---------------------------------------------------------------- the gate as a predicate


def test_spans_a_braid_is_the_gate():
    g, geoms = _one_braid()
    v = BS.View(g, geoms)
    assert BS.spans_a_braid(v, "H", 1000.0)
    assert not BS.spans_a_braid(v, "H", 1500.0)             # above the braid
    assert not BS.spans_a_braid(v, "H", 800.0)              # at its mouth: both touches at/above
