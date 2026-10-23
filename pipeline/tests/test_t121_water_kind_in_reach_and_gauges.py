"""T121 — "is this water a stream, lake or wetland?" in the reach builder and the gauge sheds is the
registry's answer (`reach.water_kind.kind_of`), never the graph node's `NodeKind` (owner 2026-10-22,
unification audit reach 8 / cross 8).

The node's kind is the SHAPE drawn: a river's own polygon and a slough FWA draws as a lake are
`NodeKind.lake`, while the page (and every `feature_types` filter) calls them the river. Three places
still read the shape: the joining water's stem (`build._stem_to_first_lake`, which stopped at a
river's own polygon), the watershed part (`extent._watershed_part`: the river's code and the
"touching nothing" lakes) and the gauge sheds (`shed.is_lake_station`, which treated a station on a
river polygon as a lake's level).

Mutation pins:
  * `_stem_to_first_lake` reading `n.kind != NodeKind.stream` again fails
    `test_the_stem_runs_on_through_the_river_s_own_polygon`.
  * `is_lake_station` reading the node's kind again fails `test_a_station_on_a_river_polygon_is_a_river_station`
    and `test_a_river_polygon_station_sheds_like_a_river_station`.
  * any `NodeKind` / node-kind read in `pipeline/atlas/reach/` or `pipeline/gauges/` fails
    `test_no_node_kind_is_read_for_the_water_kind`.
"""
from __future__ import annotations

import re
from pathlib import Path
from types import SimpleNamespace

import pytest

from pipeline.atlas.reach import build as B
from pipeline.common.models.enums import NodeKind
from pipeline.common.models.graph import FlowEdge, StreamGraph, StreamNode
from pipeline.gauges.consume.shed import build_gauge_sheds, is_lake_station, lake_gauge_links

ROOT = Path(__file__).resolve().parents[1]
W = "100-000001"


def _graph(nodes, edges):
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in nodes}
    for ix, (a, b, kind) in enumerate(edges):
        g.edges.append(FlowEdge(from_node=a, to_node=b, at_measure=0.0, kind=kind))
        g.up_adj.setdefault(b, []).append(ix)
        g.down_adj.setdefault(a, []).append(ix)
    return g


def _stream(nid, mag=100, blk="1", wsc=W, lo=0.0):
    return StreamNode(node_id=nid, kind=NodeKind.stream, blk=blk, wsc=wsc, stream_magnitude=mag,
                      down_m=lo, up_m=lo + 100.0, length_m=100.0, stream_order=3)


def _poly(nid, mag=100, wsc=W):
    return StreamNode(node_id=nid, kind=NodeKind.lake, wbk=nid.split(":")[1], wsc=wsc,
                      stream_magnitude=mag)


def _reg(**items):
    """`{id: (kind, sections)}` as the registry readers take it."""
    return {k: SimpleNamespace(kind=kind, section_ids=list(secs)) for k, (kind, secs) in items.items()}


# --------------------------------------------------------------------------- the reach builder

def _stem_world():
    """A joining river C: its mouth piece C:0, its own wide polygon lake:7 (owned by the river item),
    C:900 above it, then Lake L (a real lake) and C:2000 above that."""
    nodes = [_stream("C:0", blk="C"), _poly("lake:7"), _stream("C:900", blk="C", lo=900.0),
             _poly("lake:8"), _stream("C:2000", blk="C", lo=2000.0)]
    edges = [("lake:7", "C:0", "continuation"), ("C:900", "lake:7", "continuation"),
             ("lake:8", "C:900", "continuation"), ("C:2000", "lake:8", "continuation")]
    reg = _reg(**{"gnis:C": ("stream", ["C:0", "lake:7", "C:900", "C:2000"]),
                  "wbk:8": ("lake", ["lake:8"])})
    return _graph(nodes, edges), reg


def test_the_stem_runs_on_through_the_river_s_own_polygon():
    """RU-14's stem stops at the joining water's FIRST LAKE — a lake by the registry, so the river's own
    polygon is the river going on. MUTATION (the node's kind): the stem stops at lake:7."""
    g, reg = _stem_world()
    assert B._stem_to_first_lake(g, {"C:0"}, reg) == {"C:0", "lake:7", "C:900"}


def test_without_a_registry_the_stem_reads_the_node_as_kind_of_does():
    g, _ = _stem_world()
    assert B._stem_to_first_lake(g, {"C:0"}) == {"C:0"}


# --------------------------------------------------------------------------- the gauge sheds

def _gauge_world():
    """A river R with its own wide polygon lake:7 (owned by the river item, mag 100) between R:0 below
    and R:up above; a real lake lake:8 (a lake item) feeding R:up."""
    nodes = [_stream("R:0", 110), _poly("lake:7", 100), _stream("R:up", 90), _poly("lake:8", 80),
             _stream("in", 70)]
    edges = [("lake:7", "R:0", "confluence"), ("R:up", "lake:7", "confluence"),
             ("lake:8", "R:up", "confluence"), ("in", "lake:8", "confluence")]
    reg = _reg(**{"gnis:R": ("stream", ["R:0", "lake:7", "R:up"]), "wbk:8": ("lake", ["lake:8"]),
                  "wbk:9": ("wetland", [])})
    return _graph(nodes, edges), reg


def test_a_station_on_a_river_polygon_is_a_river_station():
    g, reg = _gauge_world()
    assert [is_lake_station(g, n, reg) for n in ("R:0", "lake:7", "lake:8", "nowhere")] == \
        [False, False, True, False]
    # no registry: the node's kind, as `kind_of` reads it
    assert [is_lake_station(g, n) for n in ("R:0", "lake:7", "lake:8")] == [False, True, True]


def test_a_wetland_is_standing_water():
    g, reg = _gauge_world()
    reg["wbk:8"] = SimpleNamespace(kind="wetland", section_ids=["lake:8"])
    assert is_lake_station(g, "lake:8", reg) is True


def test_a_river_polygon_station_sheds_like_a_river_station():
    """The station on the river's polygon speaks for the river around it (and never for Lake 8); the
    real lake's station is linked to the lake only. MUTATION (node kind): the polygon station sheds
    nothing and is linked as a lake."""
    g, reg = _gauge_world()
    st = [{"station": "08POLY"}, {"station": "08LAKE"}]
    at = {"08POLY": "lake:7", "08LAKE": "lake:8"}
    got = sorted((l.section_id, l.station) for l in build_gauge_sheds(g, st, at, registry=reg))
    assert got == [("R:0", "08POLY"), ("R:up", "08POLY"), ("in", "08POLY"), ("lake:7", "08POLY")]
    assert lake_gauge_links(g, st, at, reg) == [("lake:8", "08LAKE")]


# --------------------------------------------------------------------------- the gate

#: Reading a graph node's kind: `NodeKind`, a node's `.kind` (`n.kind`, `src.kind`, `node.kind`,
#: `nodes[...].kind`, `nodes.get(...).kind`) or the old string test `endswith("lake")`.
NODE_KIND = re.compile(r"\bNodeKind\b|\b(?:n|src|node|gauge_node)\.kind\b|nodes\[[^\]]*\]\.kind\b"
                       r"|nodes\.get\([^)]*\)\.kind\b|endswith\([\"']lake[\"']\)")
#: The one reader of the node's kind: `water_kind.kind_of` (its fallback for a section no water owns).
ALLOWED = {ROOT / "atlas" / "reach" / "water_kind.py"}
SCANNED = sorted(p for p in [*(ROOT / "atlas" / "reach").glob("*.py"), *(ROOT / "gauges").rglob("*.py")]
                 if p not in ALLOWED)


@pytest.mark.parametrize("path", SCANNED, ids=lambda p: str(p.relative_to(ROOT)))
def test_no_node_kind_is_read_for_the_water_kind(path):
    bad = [f"{i}: {ln.strip()}" for i, ln in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
           if NODE_KIND.search(ln) and not ln.lstrip().startswith("#")]
    assert not bad, f"{path.relative_to(ROOT)} reads a node's kind — ask water_kind.kind_of: {bad}"


def test_the_gate_covers_the_reach_and_the_gauges():
    rel = {str(p.relative_to(ROOT)) for p in SCANNED}
    assert {"atlas/reach/build.py", "atlas/reach/extent.py", "gauges/consume/shed.py"} <= rel
