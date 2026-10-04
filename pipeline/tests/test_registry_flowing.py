"""ONE FLOWING WATER, LINES AND POLYGONS (user rulings 2026-10-03) — `registry.flowing` and the one
water kind it decides. Synthetic graphs, no FWA; the corpus half is `test_one_water_kind.py`.

Each test names the mutation that turns it red."""
from __future__ import annotations

import json
import sqlite3

import pytest
from shapely.geometry import box

from pipeline.atlas.graph.windows import polygon_window
from pipeline.atlas.reach.water_kind import kind_of, owner_kinds
from pipeline.atlas.registry import flowing as F
from pipeline.atlas.registry.flowing import (
    absorbed_refs, alias_map, canonical, canonical_ids, merge_flowing_polygons,
)
from pipeline.common.models import (
    BoundaryKind, FlowEdge, NodeKind, RegistryBoundary, RegistryItem, SectionBoundary,
    StreamGraph, StreamNode,
)

S, L, W = NodeKind.stream, NodeKind.lake, NodeKind.wetland


def _graph(nodes, edges, *, blks=None, measures=None, bounds=None):
    """`nodes` {id: kind}; `edges` [(from, to[, at])]: a flows into b; `blks` {id: blk};
    `measures` {id: (down_m, up_m)}; `bounds` {id: (lower, upper)}."""
    g = StreamGraph()
    blks, measures, bounds = blks or {}, measures or {}, bounds or {}
    for nid, kind in nodes.items():
        lo, hi = measures.get(nid, (0.0, 0.0))
        lb, ub = bounds.get(nid, (None, None))
        g.nodes[nid] = StreamNode(node_id=nid, kind=kind, blk=blks.get(nid, ""),
                                  wbk=nid.split(":", 1)[1] if nid.startswith("lake:") else "",
                                  down_m=lo, up_m=hi, length_m=hi - lo,
                                  lower_bound=lb, upper_bound=ub)
    for e in edges:
        a, b = e[0], e[1]
        at = e[2] if len(e) > 2 else 0.0
        kind = ("lake_in" if b.startswith("lake:") else "lake_out" if a.startswith("lake:")
                else "confluence")
        i = len(g.edges)
        g.edges.append(FlowEdge(a, b, at, kind=kind))
        g.down_adj.setdefault(a, []).append(i)
        g.up_adj.setdefault(b, []).append(i)
    return g


def _item(iid, name, kind, secs, refs=(), variants=(), boundaries=()):
    return RegistryItem(id=iid, name=name, kind=kind, variants=tuple(variants) or (name,),
                        section_ids=tuple(secs), ref_ids=tuple(refs) or (iid,),
                        boundaries=tuple(boundaries))


def _river():
    """X River: line r1 -> polygon lake:1 -> line r2 -> polygon lake:2 -> line r3, all on blk X; a
    real lake (Bar Lake, lake:9) below r1; a creek joining r2."""
    # measures run UP from the mouth (r0 at 0); flow runs the other way
    g = _graph({"r3": S, "lake:2": L, "r2": S, "lake:1": L, "r1": S, "lake:9": L, "r0": S,
                "c1": S},
               [("r3", "lake:2", 600), ("lake:2", "r2", 500), ("r2", "lake:1", 400),
                ("lake:1", "r1", 300), ("r1", "lake:9", 200), ("lake:9", "r0", 100),
                ("c1", "r2", 450)],
               blks={"r0": "X", "r1": "X", "r2": "X", "r3": "X", "c1": "C"},
               measures={"r0": (0, 100), "r1": (200, 300), "r2": (400, 500), "r3": (600, 700),
                         "c1": (0, 50)},
               bounds={"r1": (SectionBoundary("lake:9", BoundaryKind.lake, 200.0, "Bar Lake"),
                              SectionBoundary("lake:1", BoundaryKind.lake, 300.0, "lake 1")),
                       "r2": (SectionBoundary("lake:1", BoundaryKind.lake, 400.0, "lake 1"),
                              SectionBoundary("lake:2", BoundaryKind.lake, 500.0, "lake 2")),
                       "r3": (SectionBoundary("lake:2", BoundaryKind.lake, 600.0, "lake 2"), None)})
    reg = {
        "gnis:1": _item("gnis:1", "X River", "stream", ["r0", "r1", "r2", "r3"], boundaries=[
            RegistryBoundary("x_river__lake_1", "lake 1", "lake", ref="lake:1", wbk="1"),
            RegistryBoundary("x_river__lake_2", "lake 2", "lake", ref="lake:2", wbk="2"),
            RegistryBoundary("x_river__bar_lake", "Bar Lake", "lake", ref="lake:9", wbk="9")]),
        "wbk:1": _item("wbk:1", "X River", "lake", ["lake:1"]),
        "wbk:2": _item("wbk:2", "X RIVER", "lake", ["lake:2"]),
        "wbk:9": _item("wbk:9", "Bar Lake", "lake", ["lake:9"]),
        "gnis:5": _item("gnis:5", "Y Creek", "stream", ["c1"], boundaries=[
            RegistryBoundary("y_creek__lake_2", "lake 2", "lake", ref="lake:2", wbk="2")]),
    }
    return g, reg


# ------------------------------------------------------------------ the merge, rule by rule

def test_a_rivers_polygons_join_its_line_by_name_and_contact():
    g, reg = _river()
    rep: dict = {}
    out = merge_flowing_polygons(reg, g, report=rep)
    x = out["gnis:1"]
    assert set(x.section_ids) == {"r0", "r1", "r2", "r3", "lake:1", "lake:2"}
    assert x.kind == "stream" and x.id == "gnis:1"            # the stream keeps its id
    assert set(x.aliases) == {"wbk:1", "wbk:2"}
    assert {"wbk:1", "wbk:2"} <= set(x.ref_ids)              # a wbk pin still resolves
    assert "wbk:1" not in out and "wbk:2" not in out
    assert out["wbk:9"] == reg["wbk:9"]                       # a lake stays a lake
    assert rep["absorbed"] == 2 and {r["rule"] for r in rep["merged"]} == {"name+contact"}
    # F6: the river's lake boundaries onto its own polygons are gone; Bar Lake's stays
    assert {b.id for b in x.boundaries} == {"x_river__bar_lake"}
    # ...and the creek's mouth at the river's polygon is a confluence with the river
    yb = out["gnis:5"].boundaries[0]
    assert (yb.kind, yb.label, yb.ref) == ("confluence", "X River", "lake:2")
    assert rep["boundaries_rewritten"] == [{"item": "gnis:5", "boundary": "y_creek__lake_2",
                                            "river": "gnis:1"}]


def test_a_lake_is_never_folded_and_a_stranger_name_never_joins_by_contact():
    g, reg = _river()
    reg["wbk:2"] = _item("wbk:2", "Other River", "lake", ["lake:2"])
    out = merge_flowing_polygons(reg, g)
    assert "wbk:2" in out and "lake:2" not in out["gnis:1"].section_ids
    assert out["wbk:2"].kind == "stream"                       # F2: it flows, under its own id
    assert "wbk:9" in out and out["wbk:9"].kind == "lake"


def test_rule_1_a_polygon_carrying_a_streams_gnis_joins_it_whatever_its_name():
    """The Vedder Canal: FWA stamps the polygon with the Vedder River's gnis (user ruling)."""
    g, reg = _river()
    reg["wbk:2"] = _item("wbk:2", "X Canal", "lake", ["lake:2"], refs=["wbk:2", "gnis:1"])
    rep: dict = {}
    out = merge_flowing_polygons(reg, g, report=rep)
    assert "lake:2" in out["gnis:1"].section_ids and "wbk:2" not in out
    assert "x canal" in {v.lower() for v in out["gnis:1"].variants}     # still searchable
    assert [r["rule"] for r in rep["merged"] if "wbk:2" in r["absorbed"]] == ["gnis"]
    # MUTATION: the gnis on the polygon is what joins it
    reg["wbk:2"] = _item("wbk:2", "X Canal", "lake", ["lake:2"], refs=["wbk:2", "gnis:99"])
    out = merge_flowing_polygons(reg, g)
    assert "wbk:2" in out and out["wbk:2"].kind == "stream"


def test_two_streams_gnis_on_one_polygon_is_refused_never_picked():
    g, reg = _river()
    reg["gnis:7"] = _item("gnis:7", "Z River", "stream", ["r3"])
    reg["gnis:1"] = _item("gnis:1", "X River", "stream", ["r0", "r1", "r2"])
    reg["wbk:2"] = _item("wbk:2", "X Canal", "lake", ["lake:2"], refs=["wbk:2", "gnis:1", "gnis:7"])
    with pytest.raises(SystemExit, match="answers to two streams"):
        merge_flowing_polygons(reg, g)


def test_rule_3_threaded_by_one_stream_is_reported_and_not_joined():
    """Hansen Slough / Lewis Slough: the creek of another name threads them; the user ruled NO."""
    g = _graph({"s1": S, "lake:7": L, "s2": S, "t1": S, "lake:8": L}, [
        ("s2", "lake:7"), ("lake:7", "s1"), ("t1", "lake:8")])
    reg = {"gnis:2": _item("gnis:2", "Hawks Creek", "stream", ["s1", "s2"]),
           "gnis:3": _item("gnis:3", "Other Creek", "stream", ["t1"]),
           "wbk:7": _item("wbk:7", "Hansen Slough", "lake", ["lake:7"]),
           "wbk:8": _item("wbk:8", "Lone Slough", "lake", ["lake:8"])}
    rep: dict = {}
    out = merge_flowing_polygons(reg, g, report=rep)
    assert out["wbk:7"].kind == "stream" and "lake:7" not in out["gnis:2"].section_ids
    assert rep["threaded_by"][0] == {"name": "Hansen Slough", "members": ["wbk:7"],
                                     "streams": ["gnis:2"], "joined": False}
    assert rep["threaded_by"][1]["one_side"] is True            # entered only: never a join
    assert F.THREADED_JOINS is False                             # the ruling, as a switch
    # the switch is real: turned on, the slough joins the one stream threading it
    F.THREADED_JOINS = True
    try:
        out2 = merge_flowing_polygons(reg, g)
        assert "lake:7" in out2["gnis:2"].section_ids and "lake:8" not in out2["gnis:3"].section_ids
    finally:
        F.THREADED_JOINS = False


def test_a_polygon_only_slough_is_one_stream_item_with_its_unnamed_line():
    """Six Mile Slough: polygons joined by an UNNAMED line piece (u1), plus a second arm far away
    carrying the same gnis — one item named by the gnis, kind stream."""
    g = _graph({"lake:30": L, "u1": S, "lake:20": L, "lake:40": L, "u0": S},
               [("lake:30", "u1"), ("u1", "lake:20"), ("lake:20", "u0")])
    reg = {
        "wbk:20": _item("wbk:20", "Six Mile Slough", "lake", ["lake:20"], ["gnis:6", "wbk:20"]),
        "wbk:30": _item("wbk:30", "Six Mile Slough", "lake", ["lake:30"], ["gnis:6", "wbk:30"]),
        "wbk:40": _item("wbk:40", "Six Mile Slough", "lake", ["lake:40"], ["gnis:6", "wbk:40"]),
    }
    out = merge_flowing_polygons(reg, g)
    assert set(out) == {"gnis:6"}
    x = out["gnis:6"]
    assert set(x.section_ids) == {"lake:20", "lake:30", "lake:40", "u1"}
    assert "u0" not in x.section_ids                           # only the line BETWEEN pieces
    assert x.kind == "stream" and set(x.aliases) == {"wbk:20", "wbk:30", "wbk:40"}


def test_every_candidate_leaves_as_a_stream_touching_or_alone():
    g = _graph({"lake:30": L, "lake:20": L, "lake:90": L}, [("lake:30", "lake:20")])
    reg = {f"wbk:{w}": _item(f"wbk:{w}", "Camp Slough", "lake", [f"lake:{w}"])
           for w in (20, 30, 90)}
    reg["wbk:5"] = _item("wbk:5", "Taylor Slough", "wetland", ["lake:5"])
    g.nodes["lake:5"] = StreamNode(node_id="lake:5", kind=W, wbk="5")
    out = merge_flowing_polygons(reg, g)
    assert set(out["wbk:20"].section_ids) == {"lake:20", "lake:30"}
    assert out["wbk:20"].kind == "stream"                     # F2, though drawn as polygons
    assert out["wbk:90"].kind == "stream" and out["wbk:90"].section_ids == ("lake:90",)
    assert out["wbk:5"].kind == "stream"                      # the wetland slough too
    # ...and outlines touching join as contact
    polys = {"lake:20": box(0, 0, 1, 1), "lake:30": box(5, 5, 6, 6), "lake:90": box(1, 0, 2, 1)}
    out = merge_flowing_polygons(reg, g, polys)
    assert set(out["wbk:20"].section_ids) == {"lake:20", "lake:30", "lake:90"}


def test_a_slough_takes_the_unnamed_polygons_it_runs_through_a_creek_does_not():
    g = _graph({"s1": S, "lake:7": L, "s2": S, "lake:8": L, "c1": S, "c2": S},
               [("s2", "lake:7"), ("lake:7", "s1"), ("c2", "lake:8"), ("lake:8", "c1")])
    reg = {"gnis:21105": _item("gnis:21105", "Nicomen Slough", "stream", ["s1", "s2"]),
           "gnis:2": _item("gnis:2", "Z Creek", "stream", ["c1", "c2"])}
    rep: dict = {}
    out = merge_flowing_polygons(reg, g, report=rep)
    assert "lake:7" in out["gnis:21105"].section_ids
    assert "wbk:7" in out["gnis:21105"].ref_ids
    assert "lake:8" not in out["gnis:2"].section_ids         # a creek's unnamed lake is a lake
    assert rep["unnamed_polygons"] == [{"id": "gnis:21105", "name": "Nicomen Slough",
                                        "polygons": ["lake:7"]}]


def test_a_cut_aliased_onto_the_rivers_own_polygon_edge_is_refused_not_dropped():
    g, reg = _river()
    reg["gnis:1"] = _item("gnis:1", "X River", "stream", ["r0", "r1", "r2", "r3"], boundaries=[
        RegistryBoundary("x_river__lake_1", "lake 1", "lake", ref="lake:1", wbk="1",
                         aliases=("split:x_river__the_bridge",))])
    with pytest.raises(SystemExit, match="aliased onto its own polygon"):
        merge_flowing_polygons(reg, g)


def test_merging_twice_changes_nothing():
    g, reg = _river()
    once = merge_flowing_polygons(reg, g)
    assert merge_flowing_polygons(once, g) == once


# ------------------------------------------------------------------ the one water kind

def test_kind_of_is_the_owners_kind_else_the_nodes():
    g, reg = _river()
    out = merge_flowing_polygons(reg, g)
    assert kind_of(g, out, "lake:1") == "stream"              # a river's polygon
    assert kind_of(g, out, "lake:9") == "lake"
    assert kind_of(g, out, "r1") == "stream" and kind_of(g, out, "missing") == ""
    assert kind_of(g, None, "lake:1") == "lake"                # no registry: the shape
    assert owner_kinds(g, out) == {"lake:1": "stream", "lake:2": "stream"}
    # MUTATION: the kind is read from the item, never from the name
    bad = dict(out)
    bad["gnis:1"] = RegistryItem(**{**out["gnis:1"].__dict__, "kind": "lake"})
    assert kind_of(g, bad, "lake:1") == "lake" and kind_of(g, bad, "r1") == "lake"


def test_a_polygon_window_is_the_gap_the_line_leaves_and_enters_it_at():
    g, _ = _river()
    assert polygon_window(g, "lake:1") == ("X", 300.0, 400.0)   # (blk, lo = lake_out, hi = lake_in)
    assert polygon_window(g, "lake:9") == ("X", 100.0, 200.0)
    assert polygon_window(g, "lake:1", "C") is None
    assert polygon_window(g, "r1") is None
    head = _graph({"lake:5": L, "s": S}, [("lake:5", "s", 50)], blks={"s": "H"})
    assert polygon_window(head, "lake:5") == ("H", 50.0, None)   # a head polygon: open above


def test_the_reach_places_a_rivers_polygon_by_its_window():
    """Stellako: a `between` around its wide reach binds the polygon; a cut at its edge puts it
    on the right side; a cut inside it leaves it straddling (reported, never guessed)."""
    from pipeline.atlas.reach.extent import _by_measure
    g = _graph({"a": S, "lake:1": L, "b": S}, [("a", "lake:1", 300), ("lake:1", "b", 200)],
               blks={"a": "X", "b": "X"}, measures={"a": (300, 400), "b": (100, 200)})
    universe = {"a", "lake:1", "b"}
    assert _by_measure(g, universe, "X", 100, 400) == ({"lake:1", "b", "a"}, set())
    assert _by_measure(g, universe, "X", 150, 350) == ({"lake:1"}, set())   # pieces cut: out
    assert _by_measure(g, universe, "X", 100, 200) == ({"b"}, set())        # cut at the edge
    assert _by_measure(g, universe, "X", 300, 400) == ({"a"}, set())
    assert _by_measure(g, universe, "X", 100, 250) == ({"b"}, {"lake:1"})   # cut inside it


def test_region_membership_keeps_a_lake_in_both_regions_and_a_rivers_polygon_home():
    from pipeline.atlas.registry import regions
    g, reg = _river()
    out = merge_flowing_polygons(reg, g)
    home = {"lake:1": "3", "lake:9": "3", "r2": "3"}
    both = regions.lakes_among(g, out, home)
    assert both == {"lake:9"}
    regions.attach(g, home, both)
    got = regions.in_region(g, "8", {"lake:1", "lake:9", "r2", "r3"})
    assert got == {"lake:9", "r3"}                     # the river's polygon is held to its home
    # MUTATION: the `lake:` prefix is the shape, never the kind
    regions.attach(g, home, {s for s in home if s.startswith("lake:")})
    assert regions.in_region(g, "8", {"lake:1", "lake:9", "r2", "r3"}) == {"lake:1", "lake:9", "r3"}


def test_the_bundle_span_runs_through_the_rivers_own_polygon():
    from pipeline.deliver.bundle import spans as SP
    g, reg = _river()
    out = merge_flowing_polygons(reg, g)
    handles = {n: i + 1 for i, n in enumerate(sorted(g.nodes))}
    items = [(0, "gnis:1", "stream", [handles[s] for s in out["gnis:1"].section_ids]),
             (1, "wbk:9", "lake", [handles["lake:9"]]),
             (2, "gnis:5", "stream", [handles["c1"]])]
    lake_items = {"9": ("wbk:9", "lake"), "1": ("gnis:1", "stream"), "2": ("gnis:1", "stream")}
    rows = {r[0]: r[1:] for r in SP.compute(g.nodes, g.edges, handles, items, lake_items, graph=g)}
    assert rows[handles["lake:1"]] == (300, 400, SP.POLYGON, SP.POLYGON, 0, 1)   # window, shape
    assert rows[handles["r1"]] == (200, 300, "lake_inlet:wbk:9", SP.POLYGON, 0, 0)
    assert rows[handles["r2"]] == (400, 500, SP.POLYGON, SP.POLYGON, 0, 0)
    assert rows[handles["c1"]][:2] == (0, 50) and rows[handles["c1"]][3] == "source"   # the creek
    # one run through the polygon: r1 -> lake:1 -> r2 -> lake:2 -> r3 chain by measure
    span = {s: r for s, r in rows.items()}
    runs = SP.compose_runs(span, {}, [handles[s] for s in ("r1", "lake:1", "r2", "lake:2", "r3")])
    assert len(runs) == 1 and (runs[0]["km_from"], runs[0]["km_to"]) == (0.7, 0.2)
    # MUTATION: without the graph the polygon is off-stem and the run breaks in two
    rows2 = {r[0]: r[1:] for r in SP.compute(g.nodes, g.edges, handles, items, lake_items)}
    assert rows2[handles["lake:1"]][4] == 1
    assert len(SP.compose_runs(rows2, {}, [handles[s] for s in ("r1", "lake:1", "r2")])) > 1


def test_a_head_polygon_ends_the_stem_at_its_own_source():
    """Six Mile Slough: a polygon no line enters is the water's top — on the stem, so the run
    reaches it and ends at `source`, never at a dangling `polygon`."""
    from pipeline.deliver.bundle import spans as SP
    g = _graph({"lake:5": L, "u1": S, "lake:6": L, "u0": S},
               [("lake:5", "u1", 300), ("u1", "lake:6", 200), ("lake:6", "u0", 100)],
               blks={"u1": "H", "u0": "H"}, measures={"u1": (200, 300), "u0": (0, 100)},
               bounds={"u1": (SectionBoundary("lake:6", BoundaryKind.lake, 200.0, "lake 6"),
                              SectionBoundary("lake:5", BoundaryKind.lake, 300.0, "lake 5")),
                       "u0": (None, SectionBoundary("lake:6", BoundaryKind.lake, 100.0, "lake 6"))})
    handles = {n: i + 1 for i, n in enumerate(sorted(g.nodes))}
    items = [(0, "gnis:6", "stream", [handles[n] for n in ("lake:5", "u1", "lake:6", "u0")])]
    lake_items = {"5": ("gnis:6", "stream"), "6": ("gnis:6", "stream")}
    rows = {r[0]: r[1:] for r in SP.compute(g.nodes, g.edges, handles, items, lake_items, graph=g)}
    assert rows[handles["lake:5"]] == (300, 300, SP.POLYGON, "source", 0, 1)
    assert rows[handles["lake:6"]] == (100, 200, SP.POLYGON, SP.POLYGON, 0, 1)
    runs = SP.compose_runs(rows, {}, list(handles.values()))
    assert len(runs) == 1 and (runs[0]["from"], runs[0]["to"]) == ("source", "mouth")
    assert (runs[0]["km_from"], runs[0]["km_to"]) == (0.3, 0.0)


def test_end_token_names_another_streams_polygon_as_a_confluence():
    from pipeline.deliver.bundle.spans import end_token
    b = SectionBoundary("lake:2", BoundaryKind.lake, 500.0, "lake 2")
    lake_item = {"2": ("gnis:1", "stream"), "9": ("wbk:9", "lake")}.get
    assert end_token(b, "up", lake_item=lake_item, own="gnis:5") == "confluence:gnis:1"
    assert end_token(b, "up", lake_item=lake_item, own="gnis:1") == "polygon"
    aliased = SectionBoundary("lake:2", BoundaryKind.lake, 500.0, "lake 2",
                              aliases=("split:x_river__the_bridge",))
    assert end_token(aliased, "up", lake_item=lake_item, own="gnis:1") == "x_river__the_bridge"
    b9 = SectionBoundary("lake:9", BoundaryKind.lake, 200.0, "Bar Lake")
    assert end_token(b9, "up", lake_item=lake_item, own="gnis:1") == "lake_outlet:wbk:9"
    assert end_token(b9, "down", lake_item=lake_item, own="gnis:1") == "lake_inlet:wbk:9"


# ------------------------------------------------------------------ absorbed ids, once

def test_an_absorbed_id_names_the_item_that_took_it():
    g, reg = _river()
    out = merge_flowing_polygons(reg, g)
    assert alias_map(out) == {"wbk:1": "gnis:1", "wbk:2": "gnis:1"}
    assert canonical("wbk:2", out) == "gnis:1" and canonical("wbk:9", out) == "wbk:9"
    rule = {"extents": [{"op": "whole", "item_id": "wbk:2"},
                        {"op": "whole", "item_ids": ["wbk:1", "wbk:9"],
                         "outside_items": ["wbk:2"]}]}
    got = canonical_ids(rule, out)
    assert got["extents"][0]["item_id"] == "gnis:1"
    assert got["extents"][1]["item_ids"] == ["gnis:1", "wbk:9"]
    assert got["extents"][1]["outside_items"] == ["gnis:1"]
    assert rule["extents"][0]["item_id"] == "wbk:2"           # the input is not mutated
    plain = {"extents": [{"op": "whole", "item_id": "wbk:9"}]}
    assert canonical_ids(plain, out) is plain                   # nothing to change: same object
    bad = absorbed_refs([{"entry_id": "r1:x@1-1", "matched": ["wbk:2"], "rules": [rule]}], out)
    assert len(bad) == 4 and all("folded into gnis:1 (X River)" in b for b in bad)
    assert absorbed_refs([("1", plain)], out) == []


def test_the_corpus_loader_is_the_one_read_point_and_everything_else_refuses(tmp_path):
    from pipeline.regs.parsing.io import read_entryfile
    from pipeline.regs.parsing.validate_catalogue import absorbed_ids_named
    g, reg = _river()
    out = merge_flowing_polygons(reg, g)
    p = tmp_path / "region-1.json"
    e = {"entry_id": "r1:x_river@1-1", "matched": ["wbk:2", "gnis:1"], "rules": []}
    p.write_text(json.dumps({"region": "1", "entries": [e]}))
    assert read_entryfile(p)["r1:x_river@1-1"]["matched"] == ["wbk:2", "gnis:1"]   # raw: a writer
    assert read_entryfile(p, out)["r1:x_river@1-1"]["matched"] == ["gnis:1"]
    assert absorbed_ids_named(e, out) and absorbed_ids_named(e, reg) == []
    (tmp_path / "bad.json").write_text(json.dumps({"region": "1"}))
    with pytest.raises(SystemExit, match="no `entries`"):
        read_entryfile(tmp_path / "bad.json", require_entries=True)
    # the reach refuses what the loader did not map
    from pipeline.atlas.reach.build import build_reaches
    with pytest.raises(SystemExit, match="folded into another"):
        build_reaches([("1", e)], out, g)


def test_the_steelhead_list_maps_an_absorbed_id_and_refuses_the_duplicate_it_makes():
    from pipeline.atlas.reach.steelhead import ListWater, resolve_list
    g, reg = _river()
    out = merge_flowing_polygons(reg, g)
    got = resolve_list([ListWater(item_id="wbk:2", note="its polygon")], out)
    assert got[0][1] == "gnis:1"
    with pytest.raises(SystemExit, match="already listed"):
        resolve_list([ListWater(item_id="gnis:1"), ListWater(item_id="wbk:2")], out)


def test_the_dfo_reader_maps_a_waters_item_ids(tmp_path):
    from pipeline.regs.dfo_salmon import entries as E
    g, reg = _river()
    out = merge_flowing_polygons(reg, g)
    doc = {"region": "2", "region_number": 2, "region_name": "Lower Mainland", "structure": {},
           "scopes": [], "locations": [],
           "waters": [{"water_id": "w1", "region": "2", "region_number": 2, "name": "X River",
                       "item_ids": ["wbk:2", "gnis:1"]}]}
    (tmp_path / "region-2.json").write_text(json.dumps(doc))
    assert E.load("2", tmp_path).waters[0].item_ids == ["wbk:2", "gnis:1"]
    assert E.load("2", tmp_path, out).waters[0].item_ids == ["gnis:1"]


def test_the_registry_round_trips_aliases_and_a_split_lake_parent_owns_no_section(tmp_path):
    from pipeline.atlas.registry import add_lake_parts, load_registry, write_registry
    g, reg = _river()
    out = merge_flowing_polygons(reg, g)
    write_registry(out, tmp_path / "registry.json")
    back = load_registry(tmp_path / "registry.json")
    assert back["gnis:1"].aliases == ("wbk:1", "wbk:2") and back["wbk:9"].aliases == ()
    raw = json.loads((tmp_path / "registry.json").read_text())
    assert all("aliases" not in i for i in raw["items"] if i["id"] != "gnis:1")
    reg = {"wbk:9": _item("wbk:9", "Bar Lake", "lake", ["lake:9"]),
           "wbk:-1": _item("wbk:-1", "Bar Lake — east", "lake", ["lake:-1"]),
           "wbk:-2": _item("wbk:-2", "Bar Lake — west", "lake", ["lake:-2"])}
    got = add_lake_parts(reg, {"-1": "9", "-2": "9"})
    assert got["wbk:9"].section_ids == () and got["wbk:-1"].part_of == "wbk:9"


def test_the_bundle_refuses_a_split_lake_parent_that_keeps_a_section(tmp_path):
    import importlib
    B = importlib.import_module("pipeline.deliver.bundle.build")
    from pipeline.common.section_handles import write as write_handles
    items = [{"id": "wbk:9", "name": "Bar Lake", "kind": "lake", "section_ids": ["lake:9"],
              "variants": [], "ref_ids": ["wbk:9"]},
             {"id": "wbk:-1", "name": "Bar Lake — east", "kind": "lake", "section_ids": ["lake:-1"],
              "part_of": "wbk:9", "variants": [], "ref_ids": ["wbk:-1"]}]
    (tmp_path / "registry.json").write_text(json.dumps({"items": items}))
    write_handles(["lake:-1", "lake:9"], tmp_path)
    db = sqlite3.connect(":memory:")
    db.executescript(B.SCHEMA.read_text())

    class Cov:
        def filled(self, *a): pass
        def skip(self, *a): pass
    with pytest.raises(SystemExit, match="still owns a section"):
        B._items(db, tmp_path / "registry.json", Cov())
    items[0]["section_ids"] = []
    items[1]["aliases"] = ["wbk:77"]
    (tmp_path / "registry.json").write_text(json.dumps({"items": items}))
    B._items(db, tmp_path / "registry.json", Cov())
    assert db.execute("SELECT COUNT(*) FROM item_section").fetchone()[0] == 1
    assert db.execute("SELECT * FROM item_alias").fetchall() == [("wbk:77", "wbk:-1")]


def test_a_lake_cut_into_parts_named_whole_means_its_parts_and_its_ghost():
    """`outside_items: [Kootenay Lake]` (AGENTS 13) takes out the Main Body, the West Arms and the
    whole polygon the graph still holds — a parent owning no section would otherwise subtract
    nothing. MUTATION: reading the parent's `section_ids` alone gives the empty set."""
    from pipeline.atlas.reach.extent import whole_water_sections
    g = _graph({"lake:9": L, "lake:-1": L, "lake:-2": L, "s": S}, [])
    reg = {"wbk:9": _item("wbk:9", "Bar Lake", "lake", []),
           "wbk:-1": RegistryItem(id="wbk:-1", name="Bar Lake — east", kind="lake",
                                  section_ids=("lake:-1",), part_of="wbk:9"),
           "wbk:-2": RegistryItem(id="wbk:-2", name="Bar Lake — west", kind="lake",
                                  section_ids=("lake:-2",), part_of="wbk:9"),
           "gnis:1": _item("gnis:1", "A Creek", "stream", ["s"])}
    assert whole_water_sections(reg, g, "wbk:9") == {"lake:9", "lake:-1", "lake:-2"}
    assert whole_water_sections(reg, g, "wbk:-1") == {"lake:-1"}
    assert whole_water_sections(reg, g, "gnis:1") == {"s"}
    assert set(reg["wbk:9"].section_ids) == set()
