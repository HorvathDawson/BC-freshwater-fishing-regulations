"""A lake's watershed code, and the parts of a split lake — rulings 4 and 6b of 2026-09-24.

4. A LAKE'S CODE IS ITS OWN POLYGON'S. The graph stamped each lake with the code of its first member
   fid, which can be a tributary's connector: Seven Mile Lake (South Hawks Creek, `100-394295-295494`)
   carried Dewar Lake's side channel's code, and 11,797 lake nodes differed from their polygon, 42 on
   another branch. Minted nodes carried no code at all (the Williston/Kootenay/Shannon leftovers, Big
   Horn Reservoir, seven small lakes).

6b. THE PARTS OF A SPLIT LAKE FOLLOW THE RIVER'S ROUTE. Majority overlap stamped the Peace's route
   under Williston -18, -19, -18 (144 m), -19, so Zone B became an inflow of Zone A.
"""

from __future__ import annotations

from shapely.geometry import LineString

from pipeline.atlas.graph import cutting
from pipeline.atlas.graph.blk_chains import FidRow, build_blk_chains
from pipeline.atlas.graph.graph import build_stream_graph
from pipeline.atlas.graph.names import mint_waterbody_nodes
from pipeline.atlas.waters.added_lakes.ingest import _parts_follow_the_route
from pipeline.common.models import NameSource, StreamGraph


def _fid(fid, blk, wsc, coords, down_m, up_m, wbk=""):
    geom = LineString(coords)
    dn, un = cutting.blk_endpoints(geom)
    return FidRow(fid=fid, blk=blk, wsc=wsc, edge_type="1000", wbk=wbk, gnis_id="", gnis_name="",
                  stream_order=1, stream_magnitude=1, down_m=down_m, up_m=up_m,
                  geometry=geom, down_node=dn, up_node=un)


def _seven_mile():
    """A side channel C (code W5) routed into lake S by a connector fid that comes FIRST, then
    the lake's own line H7 carrying on through it and out."""
    fids = [
        _fid("h_out", "H", "100-394295-295494", [(0, 0), (10, 0)], 0, 10),
        _fid("c_conn", "C", "100-382626-061403", [(10, 0), (10, 5)], 0, 5, wbk="S"),
        _fid("h_in", "H", "100-394295-295494", [(10, 0), (20, 0)], 10, 20, wbk="S"),
        _fid("c_up", "C", "100-382626-061403", [(10, 5), (10, 30)], 5, 30),
    ]
    return fids, {"S": "lake"}


def test_a_lake_takes_its_polygons_code_not_its_first_fids():
    """Whichever fid comes first, the polygon's code wins — asked both ways round, so one of the two
    always differs from the first fid's."""
    fids, kind = _seven_mile()
    for code in ("100-394295-295494", "100-382626-061403"):
        g = build_stream_graph(build_blk_chains(fids, kind), fids, kind, {}, lake_wsc={"S": code})
        assert g.nodes["lake:S"].wsc == code


def test_without_a_polygon_code_the_lake_falls_back_to_its_fids():
    """A `999` polygon is left out of `lake_wsc`, so the lake keeps what a fid of its says."""
    fids, kind = _seven_mile()
    g = build_stream_graph(build_blk_chains(fids, kind), fids, kind, {})
    assert g.nodes["lake:S"].wsc in {"100-394295-295494", "100-382626-061403"}


def test_a_minted_lake_carries_its_polygons_code():
    g = StreamGraph()
    mint_waterbody_nodes(g, {"328961697": (("Williston Lake", "1"),), "5": ()},
                         NameSource.gazette, allow_unnamed=True,
                         wsc_of={"328961697": "200-948755"})
    assert g.nodes["lake:328961697"].wsc == "200-948755"
    assert g.nodes["lake:5"].wsc == ""


class _Row:
    __slots__ = ("fid", "blk", "wbk", "down_m", "up_m")

    def __init__(self, fid, blk, wbk, down_m, up_m):
        self.fid, self.blk, self.wbk, self.down_m, self.up_m = fid, blk, wbk, down_m, up_m


def _claimed(rows):
    out: dict = {}
    for r in rows:
        if r.wbk:
            out.setdefault(r.wbk, []).append(r.fid)
    return out


def test_a_wiggle_along_the_part_line_is_absorbed():
    """-18, -19, -18 (144 m), -19 along the Peace: the 144 m run goes to -19, so -18 drains into
    -19 and never the other way."""
    rows = [_Row("a", "P", "-18", 0, 1000), _Row("b", "P", "-19", 1000, 1300),
            _Row("c", "P", "-18", 1300, 1444), _Row("d", "P", "-19", 1444, 5000)]
    claimed = _claimed(rows)
    parent = {"-18": "328961697", "-19": "328961697"}
    assert _parts_follow_the_route(rows, parent, claimed) == 1
    assert [r.wbk for r in rows] == ["-18", "-19", "-19", "-19"]
    assert claimed == {"-18": ["a"], "-19": ["b", "d", "c"]}


def test_parts_already_in_order_are_left_alone():
    rows = [_Row("a", "P", "-16", 0, 50), _Row("b", "P", "-18", 50, 900),
            _Row("c", "P", "-19", 900, 2000)]
    parent = {"-16": "W", "-18": "W", "-19": "W"}
    assert _parts_follow_the_route(rows, parent, _claimed(rows)) == 0


def test_a_part_seen_again_after_the_river_leaves_the_lake_is_not_a_wiggle():
    """A gap (the river out of the lake and back in) ends the run: the parts on each side are two
    crossings, not a back-and-forth."""
    rows = [_Row("a", "P", "-20", 0, 100), _Row("b", "P", "-21", 100, 200),
            _Row("c", "P", "-20", 500, 600)]
    parent = {"-20": "K", "-21": "K"}
    assert _parts_follow_the_route(rows, parent, _claimed(rows)) == 0


def test_another_lake_is_never_absorbed():
    rows = [_Row("a", "P", "-18", 0, 1000), _Row("b", "P", "-1", 1000, 1010),
            _Row("c", "P", "-18", 1010, 3000)]
    parent = {"-18": "W", "-1": ""}
    assert _parts_follow_the_route(rows, parent, _claimed(rows)) == 0
    assert rows[1].wbk == "-1"


# ============================================================================ 3. basin_wsc

def _lake(nid, wsc=""):
    from pipeline.common.models import NodeKind, StreamNode
    return StreamNode(node_id=nid, kind=NodeKind.lake, wbk=nid.split(":")[1], wsc=wsc)


def test_a_lake_with_no_code_takes_the_smallest_named_watershed_containing_it():
    """Four Lakes (FWA `999`) sits in the Babine, inside the Skeena: it takes the Babine's code, the
    deepest named watershed whose land it is on — not the Skeena's, and not the nearest stream's."""
    from shapely.geometry import box
    from pipeline.atlas.registry.basins import basin_members, derive_basin_wsc
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in (_lake("lake:1"), _lake("lake:2"),
                                      _lake("lake:3", wsc="100-567134"))}
    polys = {"1": box(10, 10, 12, 12), "2": box(500, 500, 502, 502), "3": box(10, 20, 12, 22)}
    ws = [("400", 1e6, box(0, 0, 100, 100)), ("400-536025", 1e4, box(0, 0, 50, 50))]
    got = derive_basin_wsc(g, polys, ws)
    assert got == {"codeless_lakes": 2, "given": 1}
    assert g.nodes["lake:1"].basin_wsc == "400-536025"
    assert g.nodes["lake:2"].basin_wsc == "", "on no named watershed: a coastal pond keeps none"
    assert g.nodes["lake:3"].basin_wsc == "", "a lake with its own code is never re-coded"
    assert g.nodes["lake:1"].wsc == "", "derived, never the FWA code"
    assert basin_members(g.nodes.values(), "400-") == ["lake:1"]
    assert basin_members(g.nodes.values(), "100-") == ["lake:3"]


def test_the_own_code_wins_over_a_derived_one():
    import dataclasses
    from pipeline.atlas.registry.basins import node_basin_code
    n = dataclasses.replace(_lake("lake:9", wsc="100-1"), basin_wsc="400")
    assert node_basin_code(n) == "100-1"
    assert node_basin_code(_lake("lake:8")) == ""


# ============================================================================ 6c. outline membership

def _one_lake(route):
    from pipeline.common.models import NodeKind, StreamNode
    g = StreamGraph()
    g.nodes = {"lake:18": StreamNode(node_id="lake:18", kind=NodeKind.lake, wbk="18",
                                     member_fids=("f",))}
    return g, {"lake:18": route}


def test_a_noded_lakes_region_is_its_outlines_not_its_routes():
    """Zone A's route under Williston ran along the zone line and 144 m of it lay in Zone B, so the
    Zone A lake was in both zones. Tested by its outline it is in Zone A only."""
    from shapely.geometry import LineString, box
    from pipeline.atlas.splits.border import mark_inside_areas
    g, geoms = _one_lake(LineString([(10, 50), (150, 50)]))          # the route strays past x=100
    zones = {"area:region:7a": box(0, 0, 100, 100), "area:region:7b": box(100, 0, 200, 100)}
    mark_inside_areas(g, geoms, zones, extra={"lake:18": box(5, 5, 99.99, 95)})
    assert g.nodes["lake:18"].in_areas == ("area:region:7a",)


def test_without_its_outline_a_noded_lake_keeps_its_route():
    from shapely.geometry import LineString, box
    from pipeline.atlas.splits.border import mark_inside_areas
    g, geoms = _one_lake(LineString([(10, 50), (150, 50)]))
    mark_inside_areas(g, geoms, {"a": box(0, 0, 100, 100), "b": box(100, 0, 200, 100)})
    assert set(g.nodes["lake:18"].in_areas) == {"a", "b"}


def test_a_sliver_of_outline_across_a_line_is_not_membership():
    """91.7 m² of a 1,415 km² lake is where two surveys disagree about a line, not water in the
    other zone. Half of a small pond is."""
    from shapely.geometry import box
    from pipeline.atlas.splits.border import mark_inside_areas
    g, _geoms = _one_lake(None)
    big = box(0, 0, 1000, 1000)                                  # 100 ha
    mark_inside_areas(g, {}, {"7a": box(-10, -10, 1000, 1010), "7b": box(1000 - 0.05, 0, 2000, 1000)},
                      extra={"lake:18": big})
    assert g.nodes["lake:18"].in_areas == ("7a",)
    g, _geoms = _one_lake(None)
    pond = box(990, 0, 1010, 30)                                 # 600 m², half across the line
    mark_inside_areas(g, {}, {"7a": box(0, 0, 1000, 1000), "7b": box(1000, 0, 2000, 1000)},
                      extra={"lake:18": pond})
    assert set(g.nodes["lake:18"].in_areas) == {"7a", "7b"}


# ============================================================================ 6a. region units

def test_a_region_item_carries_the_units_lying_in_it():
    """Majority of the MU's area, so a neighbour touching along the shared line is not taken."""
    from shapely.geometry import box
    from pipeline.atlas.registry.build import add_region_units
    from pipeline.common.models import RegistryItem
    reg = {k: RegistryItem(id=k, name=k, kind="area") for k in ("area:region:7a", "area:region:7b")}
    mus = {"7-30": box(0, 0, 10, 10), "7-31": box(10, 0, 20, 10), "7-9": box(8, 0, 12, 10)}
    add_region_units(reg, {"area:region:7a": box(0, 0, 10, 10),
                           "area:region:7b": box(10, 0, 20, 10)}, mus)
    assert reg["area:region:7a"].mus == ("7-30",)
    assert reg["area:region:7b"].mus == ("7-31",), "7-9 is split evenly: neither holds a majority"


def test_a_route_crossing_another_part_between_two_runs_of_its_own_stays_its_own():
    """blk 359001899 runs under Zone A, 10 km through the Nation Arm polygon, then Zone A again: the
    crossing goes to Zone A. Handing the LAST run to its only neighbour moved 70 km of Zone A's
    route to Nation Arm."""
    rows = [_Row("a", "N", "-18", 0, 163_281), _Row("b", "N", "-16", 163_281, 173_759),
            _Row("c", "N", "-18", 173_759, 244_002)]
    assert _parts_follow_the_route(rows, {"-16": "W", "-18": "W"}, _claimed(rows)) == 1
    assert [r.wbk for r in rows] == ["-18", "-18", "-18"]
