"""A BRAID STRADDLING A CUT takes the side holding most of its length, and is MARKED (R10, owner
2026-10-16; `extent.STRADDLING_BRAIDS_TAKE_THEIR_MAJORITY_SIDE`, RULINGS E15).

A side channel that leaves the main line above a cut and rejoins it below touches both sides, so the
neighbour pass and the flow path (D5) left it unplaced (`straddles_the_cut`) and EVERY rule of the cut
skipped it — on the water it read as unregulated. The owner's ruling: simplify it to no braid. It is
placed on the side of the cut that holds MOST OF ITS LENGTH, measured by projecting its geometry onto
the cut's own line (the main line's route measure at each point of the braid), and marked straddling
so a reader can say "this channel runs across the boundary". A piece joining such a braid follows it.

Never a guess: an even split, or a braid with no geometry, stays unplaced with its typed reason.

No data: each graph is a few nodes with hand-drawn lines, so this runs in the no-data CI tier.
"""

from __future__ import annotations

import pytest
from shapely.geometry import LineString

from pipeline.atlas.reach import extent as X
from pipeline.common.models import FlowEdge, NodeKind, StreamGraph, StreamNode
from pipeline.common.models.enums import BoundaryKind
from pipeline.common.models.registry import RegistryBoundary, RegistryItem
from pipeline.common.models.sections import SectionBoundary

S, L = NodeKind.stream, NodeKind.lake
CUT = SectionBoundary(boundary_id="split:cut", kind=BoundaryKind("split"), route_measure=1000)


def _graph(nodes, edges, lines=None):
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in nodes}
    g.edges = [FlowEdge(from_node=a, to_node=b, kind=k, at_measure=m) for a, b, k, m in edges]
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    if lines is not None:
        g._position_geoms = ({}, lines)            # what `position.attach` loads from the atlas
    return g


def _s(nid, blk, lo, hi, **kw):
    return StreamNode(node_id=nid, kind=S, blk=blk, wsc="100", down_m=lo, up_m=hi,
                      length_m=hi - lo, **kw)


def _reg(secs):
    return {"gnis:1": RegistryItem(id="gnis:1", name="River", kind="stream", section_ids=tuple(secs),
                                   boundaries=(RegistryBoundary(id="cut", label="cut", kind="split",
                                                                ref="split:cut"),))}


def _resolve(g, op):
    secs = sorted(n for n in g.nodes if not n.startswith("lake:"))
    got = X.resolve_extent(_reg(secs), g, ["gnis:1"], {"op": op, "splits": ["cut"]})
    return got


def _braid(x_lo: float, x_hi: float, lines: bool = True):
    """The main line M runs along y=0 from its mouth (x=0) up to x=2000, cut at 1000. A braid B
    leaves M above the cut and rejoins it below; its line runs beside M at y=40 from x_lo (where it
    rejoins, downstream) to x_hi (where it leaves) — digitized from its mouth up, as FWA's are."""
    blen = x_hi - x_lo
    nodes = [_s("M:0", "M", 0, 1000, upper_bound=CUT), _s("M:1000", "M", 1000, 2000, lower_bound=CUT),
             _s("B:0", "B", 0, blen)]
    edges = [("M:1000", "M:0", "continuation", 1000), ("M:1000", "B:0", "confluence", blen),
             ("B:0", "M:0", "confluence", x_lo)]
    geo = {"M:0": LineString([(0, 0), (1000, 0)]), "M:1000": LineString([(1000, 0), (2000, 0)]),
           "B:0": LineString([(x_lo, 40), (x_hi, 40)])}
    return _graph(nodes, edges, geo if lines else None)


def test_the_braid_straddles_on_head():
    """The shape itself: B touches M:1000 (above) and M:0 (below), so the neighbour pass cannot
    place it — the case the ruling is about. Projection is what decides it, not topology."""
    got = _resolve(_braid(700, 1100, lines=False), "downstream_of")
    assert got["unclassified"] == ["B:0"]
    assert got["unclassified_why"] == {"B:0": "straddles_the_cut"}


def test_a_braid_mostly_below_the_cut_is_placed_below_and_marked():
    """300 m of B lies below the cut and 100 m above: it is in "downstream of" and out of
    "upstream of", and both say it straddles with 75 % of its length on its side."""
    down = _resolve(_braid(700, 1100), "downstream_of")
    assert "B:0" in down["sections"] and not down["unclassified"]
    assert down["straddle_placed"]["B:0"]["side"] == "in"
    assert down["straddle_placed"]["B:0"]["share"] == pytest.approx(0.75, abs=0.02)
    assert down["straddle_placed"]["B:0"]["how"] == "projected"
    up = _resolve(_braid(700, 1100), "upstream_of")
    assert "B:0" not in up["sections"] and not up["unclassified"]
    assert up["straddle_placed"]["B:0"]["side"] == "out"
    assert up["straddle_placed"]["B:0"]["share"] == pytest.approx(0.75, abs=0.02)


def test_a_braid_mostly_above_the_cut_is_placed_above():
    up = _resolve(_braid(900, 1500), "upstream_of")
    assert "B:0" in up["sections"] and not up["unclassified"]
    assert up["straddle_placed"]["B:0"]["side"] == "in"
    assert up["straddle_placed"]["B:0"]["share"] == pytest.approx(500 / 600, abs=0.02)


def test_an_even_split_stays_unplaced():
    """Half above, half below: no side holds most of it — reported, never guessed."""
    got = _resolve(_braid(800, 1200), "downstream_of")
    assert "B:0" not in got["sections"] and got["unclassified"] == ["B:0"]
    assert got["unclassified_why"] == {"B:0": "straddles_the_cut"}
    assert not got.get("straddle_placed")


def test_without_geometry_a_straddling_braid_stays_unplaced():
    """A graph with no geometry attached (no atlas named) cannot measure the braid: unplaced."""
    got = _resolve(_braid(700, 1100, lines=False), "downstream_of")
    assert got["unclassified"] == ["B:0"] and not got.get("straddle_placed")


def test_a_braid_in_a_between_takes_the_side_inside_or_outside_the_window():
    """`between` on one line: B straddles the window's lower cut (1000) of [1000, 1800]; 75 % of
    it lies below, so it is OUTSIDE the window."""
    g = _braid(700, 1100)
    top = SectionBoundary(boundary_id="split:top", kind=BoundaryKind("split"), route_measure=1800)
    g.nodes["M:1000"] = _s("M:1000", "M", 1000, 1800, lower_bound=CUT, upper_bound=top)
    g.nodes["M:1800"] = _s("M:1800", "M", 1800, 2000, lower_bound=top)
    g.edges.append(FlowEdge(from_node="M:1800", to_node="M:1000", kind="continuation",
                            at_measure=1800))
    g.up_adj.setdefault("M:1000", []).append(len(g.edges) - 1)
    g.down_adj.setdefault("M:1800", []).append(len(g.edges) - 1)
    g._position_geoms[1]["M:1000"] = LineString([(1000, 0), (1800, 0)])
    g._position_geoms[1]["M:1800"] = LineString([(1800, 0), (2000, 0)])
    reg = _reg(sorted(g.nodes))
    reg["gnis:1"] = RegistryItem(id="gnis:1", name="River", kind="stream",
                                 section_ids=tuple(sorted(g.nodes)),
                                 boundaries=(RegistryBoundary(id="cut", label="cut", kind="split",
                                                              ref="split:cut"),
                                             RegistryBoundary(id="top", label="top", kind="split",
                                                              ref="split:top")))
    got = X.resolve_extent(reg, g, ["gnis:1"], {"op": "between", "splits": ["cut", "top"]})
    assert set(got["sections"]) == {"M:1000"} and not got["unclassified"]
    assert got["straddle_placed"]["B:0"]["side"] == "out"


def _joined():
    """J drains through a lake into the braid B and touches nothing else of the water — the
    Chowade's shape (`joins_a_straddling_braid`)."""
    g = _braid(700, 1100)
    g.nodes["J:0"] = _s("J:0", "J", 0, 300)
    g.nodes["lake:k"] = StreamNode(node_id="lake:k", kind=L, wbk="k", wsc="100")
    for a, b, k, m in (("J:0", "lake:k", "lake_in", 0), ("lake:k", "B:0", "lake_out", 200)):
        g.edges.append(FlowEdge(from_node=a, to_node=b, kind=k, at_measure=m))
        g.up_adj.setdefault(b, []).append(len(g.edges) - 1)
        g.down_adj.setdefault(a, []).append(len(g.edges) - 1)
    g._position_geoms[1]["J:0"] = LineString([(950, 400), (950, 700)])   # beside the cut
    return g


def test_a_piece_joining_a_straddling_braid_follows_the_braid():
    """J's water reaches only B; B is placed below, so J is too — however J itself lies."""
    down = _resolve(_joined(), "downstream_of")
    assert {"B:0", "J:0"} <= set(down["sections"]) and not down["unclassified"]
    assert down["straddle_placed"]["J:0"]["how"] == "follows"
    assert down["straddle_placed"]["J:0"]["braids"] == ["B:0"]
    assert down["straddle_placed"]["J:0"]["side"] == "in"
    up = _resolve(_joined(), "upstream_of")
    assert not ({"B:0", "J:0"} & set(up["sections"])) and not up["unclassified"]


def test_a_piece_joining_an_unplaced_braid_stays_unplaced():
    """The braid splits evenly, so it stays unplaced — and so does what joins it."""
    g = _joined()
    g._position_geoms[1]["B:0"] = LineString([(800, 40), (1200, 40)])
    got = _resolve(g, "downstream_of")
    assert set(got["unclassified"]) == {"B:0", "J:0"}
    assert got["unclassified_why"]["J:0"] == "joins_a_straddling_braid"


def test_a_chain_of_joining_pieces_follows_the_braid_together():
    """The Chowade's lakes: J2 drains through a lake into J, which drains through another into
    the braid. J2 touches only J; judged alone it would wait on J forever."""
    g = _joined()
    g.nodes["J2:0"] = _s("J2:0", "J2", 0, 100)
    g.nodes["lake:k2"] = StreamNode(node_id="lake:k2", kind=L, wbk="k2", wsc="100")
    for a, b, k, m in (("J2:0", "lake:k2", "lake_in", 0), ("lake:k2", "J:0", "lake_out", 300)):
        g.edges.append(FlowEdge(from_node=a, to_node=b, kind=k, at_measure=m))
        g.up_adj.setdefault(b, []).append(len(g.edges) - 1)
        g.down_adj.setdefault(a, []).append(len(g.edges) - 1)
    g._position_geoms[1]["J2:0"] = LineString([(950, 800), (950, 900)])
    down = _resolve(g, "downstream_of")
    assert {"B:0", "J:0", "J2:0"} <= set(down["sections"]) and not down["unclassified"]
    assert down["straddle_placed"]["J2:0"] == {"how": "follows", "side": "in", "braids": ["B:0"]}


def test_the_mark_rides_the_binding_into_the_runs_straddle_table():
    """`build_reach` reports the placement (`straddle_placed`), and the run writes it as the
    `straddle` table the bundle reads (`io.straddle_rows`) — one source, no second derivation."""
    from pipeline.atlas.reach import build as B
    from pipeline.atlas.reach import io as RIO
    g = _braid(700, 1100)
    reg = _reg(sorted(g.nodes))
    entry = {"entry_id": "r1:river@1-1", "matched": ["gnis:1"], "rules": []}
    rule = {"rule_id": "river.r1", "type": "closure",
            "extents": [{"op": "downstream_of", "splits": ["cut"]}]}
    binding, diags = B.build_reach(entry, rule, reg, g, covered=["gnis:1"], outside=frozenset())
    assert "B:0" in binding.sections
    assert not [d for d in diags if d.kind == "unclassified"]
    assert RIO.straddle_rows(diags) == [{
        "entry_id": "r1:river@1-1", "rule_id": "river.r1", "extent": 0, "section_id": "B:0",
        "how": "projected", "side": "in", "share": 0.75, "braids": []}]


# =========================================================================== the real atlas (slow)
import os                                                                    # noqa: E402
from pathlib import Path                                                     # noqa: E402

from pipeline.tests.conftest import need, ATLAS_HINT                         # noqa: E402


@pytest.fixture(scope="module")
def real(request):
    from pipeline.atlas.reach import position
    from pipeline.atlas.registry import load_registry
    from pipeline.common.curated import GENERATED
    from pipeline.common.io.serialize import read_artifact
    from pipeline.regs.parsing import io as parse_io
    from pipeline.atlas.reach.models import iter_entries
    b = Path(os.environ.get("ATLAS_BUILD") or GENERATED.build())
    need(request, "atlas", b / "graph.pkl", ATLAS_HINT)
    g = read_artifact(str(b / "graph.pkl"))
    position.attach(g, b)                                  # as the reach CLI and review app do
    reg = load_registry(str(b / "registry.json"))
    corpus = {e["entry_id"]: e for e in iter_entries(parse_io.read_all_entries(registry=reg))}
    return g, reg, corpus


def _bind(real, eid, rid):
    from pipeline.atlas.reach.build import build_reach
    g, reg, corpus = real
    e = corpus[eid]
    b, d = build_reach(e, next(r for r in e["rules"] if r["rule_id"] == rid), reg, g)
    unplaced = {p for x in d if x.kind == "unclassified" for p in x.payload["pieces"]}
    marks = {p: m for x in d if x.kind == "straddle_placed" for p, m in x.payload["pieces"].items()}
    return set(b.sections), unplaced, marks


DEAN = "r5:dean_river@5-9"
MORICE = "r6:morice_river@6-9"
CHOWADE = "r7:chowade_river@7-43"


@pytest.mark.needs_atlas
@pytest.mark.slow
def test_the_deans_canyon_braids_take_the_side_holding_most_of_them(real):
    """The two braids at the sign 0.5 km above the canyon: 360227738 (1,976 m) lies 91 % above it,
    inside r10 (0.5-3.5 km above the canyon); 360227736 (645 m) lies 69 % below, inside r4 (the
    canyon). Both were unplaced in every one of the four rules before R10."""
    got, unplaced, marks = _bind(real, DEAN, "dean_river.r10")
    assert "360227738:0" in got and "360227736:0" not in got and not unplaced
    assert marks["360227738:0"]["share"] > 0.85 and marks["360227736:0"]["side"] == "out"
    got, unplaced, _ = _bind(real, DEAN, "dean_river.r4")
    assert "360227736:0" in got and "360227738:0" not in got and not unplaced


@pytest.mark.needs_atlas
@pytest.mark.slow
@pytest.mark.parametrize("rid", ["morice_river.r4", "morice_river.r5"])
def test_the_morice_braid_network_at_gosnell_creek_is_placed_channel_by_channel(real, rid):
    """Five channels across the Gosnell Creek confluence: the three above it are in "Gosnell to
    Lamprey", the two below it are out — none left unplaced."""
    got, unplaced, _ = _bind(real, MORICE, rid)
    assert {"360213357:0", "360213358:0", "360213366:0"} <= got
    assert not ({"360213368:0", "360213355:609"} & got) and not unplaced


@pytest.mark.needs_atlas
@pytest.mark.slow
def test_the_chowade_channels_threading_lakes_follow_their_braid(real):
    """Above Horseshoe Road Bridge: the four pieces reaching the water only through the braid
    359002030:2197 (wholly below the bridge) follow it out of r1."""
    got, unplaced, marks = _bind(real, CHOWADE, "chowade_river.r1")
    joiners = {"359002029:829", "359002030:1256", "359002030:2056", "359002030:2134"}
    assert not unplaced and not (joiners & got)
    assert all(marks[p]["how"] == "follows" and marks[p]["braids"] == ["359002030:2197"]
               for p in joiners)
