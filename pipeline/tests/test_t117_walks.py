"""T117 — `may_enter` everywhere: every walk that climbs a water steps through the tributary walk's one
entry check (`tributaries.may_enter`), passing a looser context where it has one, never a rule of its own.

The walks: `tributaries.subtree` (a confluence cut's joining water, `reach.build`), `reach.build.
_stem_to_first_lake`, the QGIS check layer (`export_gpkg.export_tributaries`), the gauge shed's climb
(`gauges.consume.shed`), the T57 sweep (`tools.unnamed_trib_sweep`, see its own tests), the added-streams
harness, and the review app's tributary picker and one-level map layer (`tributaries.joining`).

Mutations, each named where it fails:
  M1 `may_enter` always None in `subtree`        -> test_subtree_climbs_as_the_walk_climbs
  M2 the stem's step not asked                   -> test_the_stem_to_the_first_lake_stops_at_a_canal
  M3 the shed's climb not asked (switch off)     -> test_the_gauge_shed_climbs_as_the_walk_climbs
  M4 the picker reads raw edges again            -> test_the_picker_offers_what_the_walk_would_take
  M5 a walk outside tributaries.py climbs by hand -> test_no_production_walk_climbs_by_its_own_rules
"""
from __future__ import annotations

import ast
import dataclasses
import inspect
import sys
import textwrap
from pathlib import Path

from pipeline.atlas.graph import tributaries as T
from pipeline.common.models import NodeKind
from pipeline.tests.test_reach_tributaries import _g, _n, _w

ROOT = Path(__file__).resolve().parents[2]


def _joining_river():
    """The Chehalis (C, code 100-7) joins the Harrison (H:0, code 100); its own line above its mouth
    (C:500), a creek (K), a Harrison side channel braiding INTO the Chehalis creek (HS, code 100, order
    2) with a Harrison creek above it (HC), and a canal (X, 2300) with a creek above it (XU)."""
    nodes = [_w("H:0", order=7, wsc="100"),
             dataclasses.replace(_w("C:0", order=5, wsc="100-7"), blk="C"),
             dataclasses.replace(_w("C:500", order=5, wsc="100-7", down_m=500.0), blk="C"),
             _w("K:0", order=2, wsc="100-7-3"), _w("HS:0", order=2, wsc="100"),
             _w("HC:0", order=1, wsc="100-9"),
             dataclasses.replace(_n("X:0", barrier=True), wsc="100-7-5"), _w("XU:0", order=1, wsc="100-7-5-1")]
    edges = [("C:0", "H:0", "confluence"), ("C:500", "C:0", "continuation"), ("K:0", "C:500", "confluence"),
             ("HS:0", "K:0", "confluence"), ("HC:0", "HS:0", "confluence"), ("X:0", "C:0", "confluence"),
             ("XU:0", "X:0", "confluence")]
    return _g(nodes, edges)


def test_subtree_climbs_as_the_walk_climbs(monkeypatch):
    """M1: the joining water's own line above its mouth is in (no "river continuing"); the receiving
    river's side channel braiding in, and what feeds it, are not; nor the canal and what is above it."""
    g = _joining_river()
    assert T.subtree(g, {"C:0"}) == {"C:0", "C:500", "K:0"}
    assert T.subtree(g, {"C:0"}, guarded=False) == {"C:0", "C:500", "K:0", "X:0", "XU:0"}
    monkeypatch.setattr(T, "may_enter", lambda *a, **k: None)
    assert T.subtree(g, {"C:0"}) == {"C:0", "C:500", "K:0", "HS:0", "HC:0", "X:0", "XU:0"}


def test_subtree_keeps_both_halves_of_the_code_ruling():
    """Adversarial review (T117): `subtree` refused a code run's divide but never took the run, so a run
    reached from its OWN lake was in no subtree at all. The Dewar fixture: climbing from Five Mile Creek
    (the lake's main outlet) takes the lake's run (`chan_up`, `lake:pond`, `chan_dn`, and `chan_trib`
    joining it); climbing from South Hawks Creek stops at the divide, as the walk does.
    Mutations: drop the run block in `subtree` -> the first assert fails; drop `CODE_DIVIDE` in
    `may_enter` -> the second fails."""
    from pipeline.tests.test_tributary_lakes import dewar
    g = dewar.__wrapped__() if hasattr(dewar, "__wrapped__") else dewar()
    assert T.subtree(g, {"five_mile"}) == {"five_mile", "lake:dewar", "dewar_in", "chan_up", "lake:pond",
                                           "chan_dn", "chan_trib"}
    assert T.subtree(g, {"south_hawks"}) == {"south_hawks", "lake:seven", "seven_in"}
    # the walk from the river Five Mile joins takes the same run with the lake
    assert T.subtree(g, {"five_mile"}) <= T.tributaries_of_reach(g, {"wlr"})


def test_tributary_node_ids_is_the_subtree_less_the_node():
    g = _joining_river()
    assert T.tributary_node_ids(g, "C:0") == {"C:500", "K:0"}


def test_the_stem_to_the_first_lake_stops_at_a_canal(monkeypatch):
    """M2: the stem follows its own line's `continuation` edges, each one `may_enter` allows."""
    from pipeline.atlas.reach import build
    canal = dataclasses.replace(_n("C:900", barrier=True, blk="C"), wsc="100-7", down_m=900.0)
    g = _joining_river()
    g = _g(list(g.nodes.values()) + [canal, dataclasses.replace(_w("C:1200", order=5, wsc="100-7",
                                                                    down_m=1200.0), blk="C")],
           [(e.from_node, e.to_node, e.kind) for e in g.edges]
           + [("C:900", "C:500", "continuation"), ("C:1200", "C:900", "continuation")])
    assert build._stem_to_first_lake(g, {"C:0"}) == {"C:0", "C:500"}
    monkeypatch.setattr(T, "may_enter", lambda *a, **k: None)
    assert build._stem_to_first_lake(g, {"C:0"}) == {"C:0", "C:500", "C:900", "C:1200"}


def _shed_graph():
    """A gauge on the Chehalis (G, mag 100); a creek above it (K, mag 40); a canal above it (X, mag 30) with
    a creek above that (XU, mag 20) — every code under the gauge's, so `drains_through` would claim all."""
    from pipeline.common.models import StreamNode
    from pipeline.common.models.graph import FlowEdge, StreamGraph

    def node(nid, mag, wsc, barrier=False):
        return StreamNode(node_id=nid, kind=NodeKind.stream, blk=nid, wsc=wsc, stream_magnitude=mag,
                          edge_types=("2300",) if barrier else ())
    g = StreamGraph()
    for n in (node("G", 100, "100-7"), node("K", 40, "100-7-3"), node("X", 30, "100-7-5", True),
              node("XU", 20, "100-7-5-1")):
        g.nodes[n.node_id] = n
    for ix, (a, b) in enumerate([("K", "G"), ("X", "G"), ("XU", "X")]):
        g.edges.append(FlowEdge(from_node=a, to_node=b, at_measure=0.0))
        g.up_adj.setdefault(b, []).append(ix)
        g.down_adj.setdefault(a, []).append(ix)
    return g


def test_the_gauge_shed_climbs_as_the_walk_climbs(monkeypatch):
    """M3: the canal and the creek above it are not in the gauge's shed; off, the raw graph climbs them."""
    from pipeline.gauges.consume import shed
    g = _shed_graph()
    got = {l.section_id for l in shed.build_gauge_sheds(g, [{"station": "08A"}], {"08A": "G"})}
    assert got == {"G", "K"}
    monkeypatch.setattr(shed, "SHED_CLIMBS_THROUGH_MAY_ENTER", False)
    got = {l.section_id for l in shed.build_gauge_sheds(g, [{"station": "08A"}], {"08A": "G"})}
    assert got == {"G", "K", "X", "XU"}


def test_the_gauge_shed_walk_down_is_not_a_tributary_step(monkeypatch):
    """Adversarial review (T117): only the climb UP asks `may_enter`; the walk DOWN from a gauge goes on
    down its own river, where every step is "a bigger river" by order. A gauge G (order 2) on a river
    that grows below it (D, order 3, same code): D is in the shed. Mutation: ask `may_enter` on the walk
    down too (`if forward and SHED_...` -> `if SHED_...`) -> D is refused as `bigger_river` and this fails."""
    from pipeline.common.models import StreamNode
    from pipeline.common.models.graph import FlowEdge, StreamGraph
    from pipeline.gauges.consume import shed
    g = StreamGraph()
    for nid, mag, order in (("G", 100, 2), ("D", 110, 3), ("K", 60, 1)):
        g.nodes[nid] = StreamNode(node_id=nid, kind=NodeKind.stream, blk=nid, wsc="100-7",
                                  stream_magnitude=mag, stream_order=order)
    for ix, (a, b) in enumerate([("G", "D"), ("K", "G")]):
        g.edges.append(FlowEdge(from_node=a, to_node=b, at_measure=0.0, kind="continuation"))
        g.up_adj.setdefault(b, []).append(ix)
        g.down_adj.setdefault(a, []).append(ix)
    got = {l.section_id for l in shed.build_gauge_sheds(g, [{"station": "08A"}], {"08A": "G"})}
    assert got == {"G", "D", "K"}


def test_joining_is_the_walk_s_first_step():
    """`tributaries.joining`: what joins the reach directly, by `may_enter` — the creek, never the receiving
    river's side channel or the canal; the joining water's own line above its cut is the river going on."""
    g = _joining_river()
    assert T.joining(g, {"C:0"}) == frozenset()            # C:500 is the river going on; X is a canal
    assert T.joining(g, {"C:500"}) == {"K:0"}
    assert T.joining(g, {"K:0"}) == frozenset()            # HS:0 is the Harrison's own side channel
    assert T.joining(g, {"H:0"}) == {"C:0"}


def test_the_picker_offers_what_the_walk_would_take(monkeypatch):
    """M4: the review app's tributary picker offers the named streams the walk's first step enters."""
    backend = ROOT / "curation-review" / "backend"
    if str(backend) not in sys.path:
        sys.path.insert(0, str(backend))
    import reuse

    @dataclasses.dataclass
    class Item:
        id: str
        name: str
        kind: str
        section_ids: tuple
    g = _joining_river()
    items = {"k": Item("k", "Kay Creek", "stream", ("K:500",)), "c": Item("c", "Chehalis", "stream", ("C:0", "C:500")),
             "kc": Item("kc", "Kay Creek", "stream", ("K:0",)), "hs": Item("hs", "Harrison", "stream", ("HS:0",)),
             "x": Item("x", "Canal", "stream", ("X:0",))}
    monkeypatch.setattr(reuse, "_graph", lambda: g)
    monkeypatch.setattr(reuse, "_registry", lambda: items)
    monkeypatch.setattr(reuse, "_blk_to_item", lambda: {"K": "kc", "HS": "hs", "X": "x", "C": "c"})
    reuse.item_tributaries.cache_clear()
    try:
        assert reuse.item_tributaries("kc") == []                       # the Harrison's channel is not offered
        assert reuse.item_tributaries("c") == [{"id": "kc", "name": "Kay Creek"}]   # the canal is not
    finally:
        reuse.item_tributaries.cache_clear()


# --- M5: the pin ------------------------------------------------------------------------------------------------

#: Every production walk that climbs a water, and the name through which it asks the one entry check.
WALKS = {
    ("pipeline/atlas/graph/tributaries.py", "subtree"): "may_enter",
    ("pipeline/atlas/graph/tributaries.py", "joining"): "may_enter",
    ("pipeline/atlas/graph/tributaries.py", "tributaries_of_reach"): "may_enter",
    ("pipeline/atlas/graph/tributaries.py", "tributary_node_ids"): "subtree",
    ("pipeline/atlas/reach/build.py", "_stem_to_first_lake"): "may_enter",
    ("pipeline/common/io/export_gpkg.py", "export_tributaries"): "may_enter",
    ("pipeline/gauges/consume/shed.py", "_walk"): "may_enter",
    ("pipeline/tools/audit/unnamed_trib_sweep.py", "step_refusal"): "may_enter",
    ("pipeline/tools/audit/unnamed_trib_sweep.py", "_subtree"): "may_enter",
    ("pipeline/atlas/waters/added_streams/harness.py", "run"): "subtree",
    ("curation-review/backend/reuse.py", "item_tributaries"): "joining",
    ("curation-review/backend/reuse.py", "item_tributary_geojson"): "joining",
}


def _fn(rel: str, name: str) -> ast.FunctionDef:
    tree = ast.parse((ROOT / rel).read_text(encoding="utf-8"))
    return next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == name)


def _names(fn) -> set[str]:
    return {n.id for n in ast.walk(fn) if isinstance(n, ast.Name)} | \
           {n.attr for n in ast.walk(fn) if isinstance(n, ast.Attribute)}


def test_no_production_walk_climbs_by_its_own_rules():
    """M5 (no second copy): each walk asks the one entry check; none reads the barrier, the order guard or
    the mainstem edge kinds itself; and no production module climbs `graph.ancestors` (the raw closure
    is the graph's own primitive, kept for the graph's tests)."""
    asks = {k: v in _names(_fn(*k)) for k, v in WALKS.items()}
    assert asks == {k: True for k in WALKS}
    rules = {"is_barrier", "_breaks_strahler", "_order_breaks", "_refusal", "_code_says_tributary",
             "MAINSTEM_EDGE_KINDS", "_TRIB_EDGE_KINDS", "ancestors"}
    owner = "pipeline/atlas/graph/tributaries.py"
    reads = {k: sorted(_names(_fn(*k)) & rules) for k in WALKS if k[0] != owner}
    assert {k: v for k, v in reads.items() if v} == {}
    callers = []
    for top in ("pipeline", "curation-review"):
        for p in sorted((ROOT / top).rglob("*.py")):
            rel = p.relative_to(ROOT).as_posix()
            if "/tests/" in rel or "/archive/" in rel or rel == "pipeline/atlas/graph/graph.py":
                continue
            tree = ast.parse(p.read_text(encoding="utf-8"))
            for n in ast.walk(tree):
                if isinstance(n, ast.ImportFrom) and n.module == "pipeline.atlas.graph.graph" \
                        and any(a.name == "ancestors" for a in n.names):
                    callers.append(rel)
    assert callers == []


#: Adversarial review (T117): the pin above checks the walks it LISTS; a new climb written elsewhere slipped
#: past it. Every production function outside tributaries.py that reads `up_adj` is named here with why it is
#: not a tributary climb (or which entry check it asks). A new reader fails until it is classified.
UP_ADJ_READERS = {
    # graph construction and pruning: they BUILD or rewire the adjacency, they do not climb it
    ("pipeline/atlas/build.py", "main"): "in-degree counts for the build summary",
    ("pipeline/atlas/build.py", "summarize"): "in-degree counts",
    ("pipeline/atlas/graph/graph.py", "_finalize_lake_bounds"): "a lake's inlets, one level, for its bounds",
    ("pipeline/atlas/graph/graph.py", "ancestors"): "the raw closure, kept for the graph's own tests",
    ("pipeline/atlas/graph/leaf_prune.py", "hops_above_named"): "prune: hops to a named water",
    ("pipeline/atlas/graph/leaf_prune.py", "prunable"): "prune: leaf test",
    ("pipeline/atlas/graph/nests.py", "nest_ports"): "a nest's ports, one level",
    ("pipeline/atlas/graph/prune.py", "loop_nodes"): "mainstem loop detection",
    ("pipeline/atlas/graph/prune.py", "prune_mainstem_loops"): "rebuilds the adjacency",
    ("pipeline/atlas/graph/windows.py", "polygon_window"): "a polygon's inlets, one level, for its window",
    ("pipeline/atlas/splits/sectionizer.py", "split_graph_at"): "rebuilds the adjacency",
    ("pipeline/atlas/splits/braid_sweep.py", "_rebuild_adj"): "rebuilds the adjacency after a re-home",
    ("pipeline/atlas/waters/added_streams/ingest.py", "attach_connectors"): "adds edges",
    ("pipeline/atlas/registry/flowing.py", "merge_flowing_polygons"): "polygon neighbours, both directions",
    ("pipeline/atlas/registry/flowing.py", "merge_flowing_polygons.neighbours"): "polygon neighbours",
    ("pipeline/atlas/registry/flowing.py", "merge_flowing_polygons.threading"): "polygon threading",
    # placement on a cut's own line (AGENTS 11: by measure, never a flow walk)
    ("pipeline/atlas/reach/extent.py", "_across_the_junction"): "pieces meeting a line end, one level",
    ("pipeline/atlas/reach/extent.py", "_by_measure._nbrs"): "braid neighbours inside the universe",
    ("pipeline/atlas/reach/extent.py", "_flow_reach"): "a detached piece's flow path to its own water",
    ("pipeline/atlas/reach/extent.py", "_line_length"): "a line's confluence measures",
    ("pipeline/atlas/reach/extent.py", "_watershed_part"): "connected groups of the river's own code",
    ("pipeline/atlas/reach/extent.py", "river_widenings"): "T153: the unnamed lake widenings a river's main flow runs through",
    # exports and the review app's anabranch test: one level, no set collected
    ("pipeline/common/io/export_gpkg.py", "export_graph_gpkg"): "inlet counts",
    ("pipeline/common/io/export_gpkg.py", "export_lake_io"): "a lake's inlets, one level",
    ("pipeline/deliver/bundle/continuation.py", "graph_flow"): "edges into a top piece, one level",
    ("pipeline/deliver/tiles/export.py", "export_streams.neighbour_weight"): "neighbour weights, one level",
    ("curation-review/backend/reuse.py", "item_side_channels"): "leaves-and-rejoins test, one level",
    # the climbs: each asks `may_enter` (see WALKS above)
    ("pipeline/atlas/reach/build.py", "_stem_to_first_lake"): "may_enter",
    ("pipeline/common/io/export_gpkg.py", "export_tributaries"): "may_enter",
    ("pipeline/gauges/consume/shed.py", "build_gauge_sheds"): "hands up_adj to `_walk`, which asks may_enter",
    ("pipeline/tools/audit/unnamed_trib_sweep.py", "TopoGraph.__init__"): "builds the sweep's graph view",
    ("pipeline/tools/audit/unnamed_trib_sweep.py", "_subtree"): "may_enter",
}


def _up_adj_readers() -> set:
    out = set()
    for top in ("pipeline", "curation-review"):
        for p in sorted((ROOT / top).rglob("*.py")):
            rel = p.relative_to(ROOT).as_posix()
            if "/tests/" in rel or "/archive/" in rel or "node_modules" in rel \
                    or rel == "pipeline/atlas/graph/tributaries.py":
                continue

            def visit(node, stack):
                for ch in ast.iter_child_nodes(node):
                    if isinstance(ch, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                        visit(ch, stack + [ch.name])
                        continue
                    if isinstance(ch, ast.Attribute) and ch.attr == "up_adj":
                        out.add((rel, ".".join(stack) or "<module>"))
                    visit(ch, stack)
            visit(ast.parse(p.read_text(encoding="utf-8")), [])
    return out


def test_every_reader_of_up_adj_outside_the_walk_is_classified():
    """Pin: a function outside tributaries.py that reads `up_adj` is on `UP_ADJ_READERS` — a new climb has
    to say which entry check it asks. Mutation: add `graph.up_adj` to any unlisted production function ->
    this fails naming it."""
    assert _up_adj_readers() == set(UP_ADJ_READERS)
