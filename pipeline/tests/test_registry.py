"""Unit tests for build_registry — synthetic StreamGraph, no FWA needed."""

from shapely.geometry import LineString, box

from pipeline.models import (
    BoundaryKind, NameSource, NameTuple, NodeKind, SectionBoundary, StreamGraph, StreamNode,
)
from pipeline.registry import add_mu_sets, build_registry


def _node(nid, *, blk="", wsc="", gnis="", name="", tuples=(), in_areas=(),
          lo=None, hi=None, kind=NodeKind.stream, wbk=""):
    return StreamNode(node_id=nid, kind=kind, blk=blk, wbk=wbk, wsc=wsc, gnis_id=gnis,
                      display_name=name, name_tuples=tuples, in_areas=in_areas,
                      lower_bound=lo, upper_bound=hi)


def _reg():
    g = StreamGraph()
    falls = SectionBoundary("split:foo_falls", BoundaryKind.split, 100.0, "Foo Falls")
    lake_b = SectionBoundary("lake:W1", BoundaryKind.lake, 200.0, "Bar Lake")
    tx = (NameTuple("River X", NameSource.gazette, gnis_id="1"),)
    g.nodes["m1"] = _node("m1", blk="M", wsc="100", gnis="1", name="River X", tuples=tx, hi=falls)
    g.nodes["m2"] = _node("m2", blk="M", wsc="100", gnis="1", name="River X", tuples=tx,
                          lo=falls, hi=lake_b, in_areas=("GARIBALDI PARK",))
    # side channel: NO own gnis, inherits gnis via a side-channel name tuple -> must join gnis:1
    g.nodes["s1"] = _node("s1", blk="S", wsc="100", name="River X",
                          tuples=(NameTuple("River X", NameSource.side_channel, gnis_id="1"),))
    g.nodes["lakeW"] = _node("lakeW", name="Bar Lake", kind=NodeKind.lake, wbk="W1",
                             tuples=(NameTuple("Bar Lake", NameSource.gazette),))
    return build_registry(g)


def test_river_is_one_item_via_tuple_gnis():
    reg = _reg()
    assert "gnis:1" in reg
    item = reg["gnis:1"]
    assert item.name == "River X" and item.kind == "stream"
    assert set(item.section_ids) == {"m1", "m2", "s1"}     # side channel unified via tuple gnis


def test_readable_boundaries():
    reg = _reg()
    bids = {b.id: b for b in reg["gnis:1"].boundaries}
    assert "foo_falls" in bids and bids["foo_falls"].ref == "split:foo_falls"
    assert "bar_lake" in bids and bids["bar_lake"].kind == "lake" and bids["bar_lake"].wbk == "W1"


def test_lake_and_area_items():
    reg = _reg()
    assert reg["wbk:W1"].kind == "lake" and reg["wbk:W1"].name == "Bar Lake"
    assert "area:garibaldi_park" in reg
    area = reg["area:garibaldi_park"]
    assert area.kind == "area" and set(area.section_ids) == {"m2"}


def test_add_mu_sets():
    reg = _reg()
    geoms = {"m1": LineString([(0, 0), (10, 0)]), "m2": LineString([(10, 0), (30, 0)]),
             "s1": LineString([(0, 1), (5, 1)])}
    mu_polys = {"2-1": box(-1, -2, 15, 2), "2-2": box(15, -2, 40, 2)}
    add_mu_sets(reg, geoms, mu_polys)
    assert set(reg["gnis:1"].mus) == {"2-1", "2-2"}          # river spans both MUs
