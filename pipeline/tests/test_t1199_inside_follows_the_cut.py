"""T1199 (RULINGS E30; owner 2026-11-26: "be intelligent about the split then where the river is cut will define the
section that falls inside the area when cut happens it gets inside area"). In a CUT area the part's lines are inside
when the atlas's cut puts them there (the registry's area item holds them, `side_channels.cut_area_members`), never by
their shapes' share: E30's 100 m floor grows a short inside piece outward, and the piece is the area's water all the
same. The two real shapes that stopped the answers step after the cut rule (`builders.reviewed_still_said`):

- the Gordon River's side line 354088108: its 47 m in Pacific Rim National Park Reserve (97.9 % inside the polygon)
  grown to 100 m (337-437 m, 46.3 % inside), held by the park's area item: still "Gordon River inside Pacific Rim
  National Park Reserve of Canada";
- the Okanagan River Oxbows' line 356529638: its 37 m piece grown to 100 m (866-966 m, 36 % inside), the part's lines
  71.1 % inside Hayne's Lease by shape, all held by the reserve's area item; its lake polygons straddle the edge: still
  "at Hayne's Lease Ecological Reserve".

Only the CLEAN-cut areas (national parks, ecological reserves, the Chilkoot: `area_splits.cut_mode` "clean") follow
the cut. A `first_last` area (restricted land, `membership: both_sides`: a whole piece is in it when any of it is) and a
membership-only area are measured on the shapes as before; a disagreement is logged in either kind. The reviewed list
(`part.REVIEWED_INSIDE`) is unchanged: the parts are still inside."""
import pytest
from shapely.geometry import LineString, Polygon, box

from pipeline.deliver.answers.display import part as P
from pipeline.deliver.bundle import side_channels as sc
from pipeline.tests.test_t1140_inside_area import _catalog, _inside

PARK = box(0, 0, 1000, 1000)                                  # the park's polygon: x 0-1000
GORDON = ("Pacific Rim National Park Reserve of Canada", "national_parks", PARK)
# the floor-grown piece: 100 m along y 500, from x 953.7 to 1053.7: 46.3 m inside the polygon
GROWN = {10: LineString([(953.7, 500), (1053.7, 500)])}


def test_a_floor_grown_piece_the_area_holds_is_inside_whatever_its_shape(tmp_path):
    log = []
    got = _inside(tmp_path, GROWN, [GORDON], iid="gnis:18167", log=log, held={"a0": frozenset({10})})
    assert got == [(1, 1, "Pacific Rim National Park Reserve of Canada", 1.0, "inside", 1)]
    # the disagreement is said, never silent
    assert any("INSIDE BY THE CUT: gnis:18167 part 1 'Pacific Rim National Park Reserve of Canada': the cut holds "
               "100.0% of its lines, their shapes lie 46.3% in it" in x for x in log)


def test_the_share_is_of_the_held_length_at_95_percent(tmp_path):
    # the cut's share is by length: 95 m of 100 held is inside, 94 m is not — whatever the shapes say (both lines lie
    # wholly OUTSIDE the polygon here, so a shape measure would give 0 % either way)
    two = {10: LineString([(1100, 500), (1195, 500)]), 11: LineString([(1100, 600), (1105, 600)])}
    got = _inside(tmp_path, two, [GORDON], held={"a0": frozenset({10})})
    assert got == [(1, 1, "Pacific Rim National Park Reserve of Canada", 0.95, "inside", 1)]
    two = {10: LineString([(1100, 500), (1194, 500)]), 11: LineString([(1100, 600), (1106, 600)])}
    assert _inside(tmp_path, two, [GORDON], held={"a0": frozenset({10})}) == []


def test_a_first_last_area_is_measured_by_its_shapes(tmp_path):
    # restricted land (`first_last`, both_sides): its item holds a WHOLE piece any of which lies in it; a side line 10 %
    # inside would be "inside" by its item. Measured by shape as before, the disagreement logged
    land = ("SOME RESTRICTED LAND", "restricted_land_access", PARK)
    tenth = {10: LineString([(990, 500), (1090, 500)])}                       # 10 m of 100 inside
    log = []
    assert _inside(tmp_path, tenth, [land], log=log, held={"a0": frozenset({10})}, follow=frozenset()) == []
    assert any("the cut holds 100.0% of its lines, their shapes lie 10.0% in it — a first_last area: measured by its "
               "shapes" in x for x in log)
    whole = {10: LineString([(100, 500), (900, 500)])}
    assert _inside(tmp_path, whole, [land], held={"a0": frozenset({10})}, follow=frozenset())[0][2:] == \
        ("SOME RESTRICTED LAND", 1.0, "inside", 1)


def test_a_line_wholly_in_the_polygon_the_cut_leaves_outside_is_not_inside(tmp_path):
    # E30: the cut says it both ways (a line that never crosses the edge is never inside unless the cut puts it there)
    log = []
    ch = {10: LineString([(100, 100), (900, 900)])}
    assert _inside(tmp_path, ch, [GORDON], log=log, held={"a0": frozenset()}) == []
    assert any("the cut holds 0.0% of its lines, their shapes lie 100.0% in it" in x for x in log)


def test_the_oxbows_lines_held_and_their_lake_straddling_are_at_the_reserve(tmp_path):
    hayne = ("HAYNE'S LEASE ECOLOGICAL RESERVE", "ecological_reserves", PARK)
    shapes = {10: LineString([(963.6, 300), (1063.6, 300)]),                 # the grown piece: 36.4 m inside
              11: LineString([(100, 200), (177, 200)]),                       # three short lines wholly inside
              12: LineString([(100, 400), (112, 400)]),
              13: LineString([(100, 600), (131, 600)]),
              14: Polygon([(800, 700), (1200, 700), (1200, 900), (800, 900)])}  # its lake, half inside
    held = {"a0": frozenset({10, 11, 12, 13})}
    got = _inside(tmp_path, shapes, [hayne], iid="wsc:300-432687-459615", held=held)
    assert got == [(1, 1, "HAYNE'S LEASE ECOLOGICAL RESERVE", 1.0, "at", 1)]
    # the lake polygon is still measured on its shape (the cut never cuts a lake): wholly inside it is "inside"
    shapes[14] = Polygon([(800, 700), (900, 700), (900, 900), (800, 900)])
    assert _inside(tmp_path, shapes, [hayne], iid="wsc:300-432687-459615", held=held)[0][4] == "inside"


def test_a_membership_only_area_is_still_measured_by_shape(tmp_path):
    park = ("STRATHCONA - WESTMIN PARK", "park", PARK, False)
    assert _inside(tmp_path, GROWN, [park], held={}) == []                   # 46.3 %: not inside
    whole = {10: LineString([(900, 500), (995, 500)])}
    assert _inside(tmp_path, whole, [park], held={})[0][2:] == ("STRATHCONA - WESTMIN PARK", 1.0, "inside", 0)


def test_a_part_the_cut_holds_outside_the_polygons_box_is_still_measured(tmp_path):
    # edge water held by the cut but beyond the polygon's box (the 50 m band): found by the cut, not by the tree
    off = {10: LineString([(1010, 500), (1040, 500)])}
    assert _inside(tmp_path, off, [GORDON], held={"a0": frozenset({10})})[0][2:4] == \
        ("Pacific Rim National Park Reserve of Canada", 1.0)


def test_a_cut_area_with_no_held_sections_given_stops(tmp_path):
    with pytest.raises(sc.SideChannelError, match="no held sections given"):
        _inside(tmp_path, GROWN, [GORDON], held={}, follow=frozenset())
    with pytest.raises(sc.SideChannelError, match="clean-cut area"):
        _inside(tmp_path, GROWN, [GORDON], held={"a0": frozenset()}, follow=frozenset({"a0", "a9"}))


def test_the_bundle_step_refuses_to_run_without_the_held_sections(tmp_path):
    from pipeline.tests.test_t1140_inside_area import _Cov, _db
    db, geo = _db()
    with pytest.raises(sc.SideChannelError, match="cut_area_members"):
        sc.write(db, geo, tmp_path, _Cov(), log=lambda *_: None, added={}, unnamed={}, marks={}, held={})


DEFS = [{"id": "national_parks", "cut": "clean"}, {"id": "ecological_reserves", "cut": "clean"},
        {"id": "restricted_land_access", "cut": "first_last", "membership": "both_sides"},
        {"id": "provincial_parks", "kind": "park", "cut": False},
        {"id": "regions", "kind": "region", "cut": "first_last", "membership": "both_sides"}]


def test_cut_area_members_reads_the_registrys_area_items(tmp_path):
    _catalog(tmp_path, [GORDON, ("SOME PARK", "park", PARK, False), ("Region 6", "region", PARK),
                        ("SOME LAND", "restricted_land_access", PARK),
                        ("TRIANGLE ISLAND ECOLOGICAL RESERVE", "ecological_reserves", PARK)])
    items = [{"id": "a0", "kind": "area", "section_ids": ["354088108:337", "354088108:437"]},
             {"id": "a1", "kind": "area", "section_ids": ["9:0"]},               # membership-only: not a cut area
             {"id": "a2", "kind": "area", "section_ids": ["9:0"]},               # a region: never a place
             {"id": "a3", "kind": "area", "section_ids": ["9:0"]},               # first_last: held, not followed
             {"id": "gnis:18167", "kind": "stream", "section_ids": ["354088108:337"]}]
    handles = {"354088108:337": 569, "354088108:437": 570, "9:0": 1}
    log = []
    held, follow = sc.cut_area_members(items, handles, tmp_path, DEFS, log=log.append)
    # a4, a reserve with no water in it, has no registry item: it holds nothing, named and counted — never a stop
    assert held == {"a0": frozenset({569, 570}), "a3": frozenset({1}), "a4": frozenset()}
    assert follow == frozenset({"a0", "a4"})
    assert any("3 cut places, 2 cut clean (measured by the cut); 1 hold no water (no registry item): TRIANGLE ISLAND "
               "ECOLOGICAL RESERVE (ecological_reserves)" in x for x in log)
    with pytest.raises(sc.SideChannelError, match="not in the handle table"):
        sc.cut_area_members([{**items[0], "section_ids": ["1:1"]}], handles, tmp_path, DEFS, log=log.append)


def test_the_reviewed_inside_list_is_unchanged():
    # the parts are still inside: the list a reviewer checked is not edited to make a build pass
    assert P.REVIEWED_INSIDE == frozenset({
        ("gnis:25671", "BAYNES ISLAND ECOLOGICAL RESERVE", "inside"),
        ("gnis:18167", "Pacific Rim National Park Reserve of Canada", "inside"),
        ("wsc:300-432687-459615", "HAYNE'S LEASE ECOLOGICAL RESERVE", "at"),
        ("gnis:2123", "CRESTON VALLEY WILDLIFE MANAGEMENT AREA", "inside"),
        ("gnis:21681", "STRATHCONA - WESTMIN PARK", "inside")})
