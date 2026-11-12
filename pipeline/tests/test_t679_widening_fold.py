"""T679 #5 (owner D1, 2026-11-11/12): a river's unnamed widening pond IS the river — folded into its stream item at the
atlas build (`registry.flowing.fold_widening_ponds`), so its water kind is the river's for every reader.

MUTATION PINS:
  * `WIDENING_PONDS_ARE_THE_RIVER = False`         -> test_the_pond_joins_the_river_item_as_a_stream_section
  * drop the boundary drop (F6)                    -> test_the_pond_joins_the_river_item_as_a_stream_section
  * fold a pond whose lake boundary holds a cut    -> test_a_pond_with_a_cut_on_its_edge_is_left_and_listed
  * pick one of two streams                        -> test_a_pond_two_streams_run_through_is_left_and_listed
  * fold a named lake                              -> test_a_named_lake_is_never_folded
No data: a few nodes, so this runs in the no-data CI tier.
"""
from __future__ import annotations

from pipeline.atlas.reach import extent as X
from pipeline.atlas.reach.water_kind import kind_of
from pipeline.atlas.registry import flowing as F
from pipeline.common.models import FlowEdge, NodeKind, StreamGraph, StreamNode
from pipeline.common.models.registry import RegistryBoundary, RegistryItem

S, L = NodeKind.stream, NodeKind.lake
MAIN = ("1000",)


def _graph(nodes, edges):
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in nodes}
    g.edges = [FlowEdge(from_node=a, to_node=b, kind=k, at_measure=m) for a, b, k, m in edges]
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    return g


def _s(nid, blk, lo, hi):
    return StreamNode(node_id=nid, kind=S, blk=blk, wsc="100", down_m=lo, up_m=hi, length_m=hi - lo,
                      edge_types=MAIN, display_name="River")


def _river(pond):
    return _graph([_s("M:0", "M", 0, 1000), pond, _s("M:1200", "M", 1200, 2000)],
                  [("M:1200", pond.node_id, "lake_in", 1200), (pond.node_id, "M:0", "lake_out", 1000)])


def _item(iid, secs, bounds=(), name="River"):
    return RegistryItem(id=iid, name=name, kind="stream", section_ids=tuple(secs), ref_ids=(iid,),
                        boundaries=tuple(bounds))


POND_EDGE = RegistryBoundary(id="river__lake_u", label="lake u", kind="lake", ref="lake:u", wbk="u")


def test_the_pond_joins_the_river_item_as_a_stream_section():
    g = _river(StreamNode(node_id="lake:u", kind=L, wbk="u", wsc="100"))
    reg = {"gnis:1": _item("gnis:1", ["M:0", "M:1200"], [POND_EDGE])}
    rep: dict = {}
    out = F.fold_widening_ponds(reg, g, rep)
    it = out["gnis:1"]
    assert "lake:u" in it.section_ids and "wbk:u" in it.ref_ids
    assert all(b.id != "river__lake_u" for b in it.boundaries)          # F6: the river does not stop at its pond
    assert kind_of(g, out, "lake:u") == "stream"                          # every kind filter reads the river
    assert rep["counts"] == {"ponds": 1, "items": 1, "held": 0}
    # the reach's per-rule widening has nothing left to take: the pond is the item's own section
    assert X.river_widenings(g, out, set(it.section_ids)) == set()


def test_a_pond_with_a_cut_on_its_edge_is_left_and_listed():
    g = _river(StreamNode(node_id="lake:u", kind=L, wbk="u", wsc="100"))
    edge = RegistryBoundary(id="river__lake_u", label="lake u", kind="lake", ref="lake:u", wbk="u",
                            aliases=("split:gauge__08XX001",))
    rep: dict = {}
    out = F.fold_widening_ponds({"gnis:1": _item("gnis:1", ["M:0", "M:1200"], [edge])}, g, rep)
    assert "lake:u" not in out["gnis:1"].section_ids and out["gnis:1"].boundaries == (edge,)
    assert rep["held"] == [{"pond": "lake:u", "why": "cut on its edge", "items": ["gnis:1"],
                            "aliases": ["split:gauge__08XX001"]}]


def test_a_pond_two_streams_run_through_is_left_and_listed():
    g = _river(StreamNode(node_id="lake:u", kind=L, wbk="u", wsc="100"))
    reg = {"gnis:1": _item("gnis:1", ["M:0", "M:1200"]), "gnis:2": _item("gnis:2", ["M:0", "M:1200"], name="Other")}
    rep: dict = {}
    out = F.fold_widening_ponds(reg, g, rep)
    assert all("lake:u" not in it.section_ids for it in out.values())
    assert rep["held"][0]["why"] == "two streams" and rep["held"][0]["items"] == ["gnis:1", "gnis:2"]


def test_a_named_lake_is_never_folded():
    g = _river(StreamNode(node_id="lake:u", kind=L, wbk="u", wsc="100", gnis_id="77", display_name="Bar Lake"))
    out = F.fold_widening_ponds({"gnis:1": _item("gnis:1", ["M:0", "M:1200"])}, g, {})
    assert "lake:u" not in out["gnis:1"].section_ids
