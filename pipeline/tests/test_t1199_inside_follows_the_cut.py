"""T1199 (RULINGS E35; owner 2026-11-27: "how can skeena river inside skeena river eco reseve be 72% inside. we cut on
eco reserves so it should be 1005 always for eco reserves"; 2026-11-28: "yes but we need to make sure the rule is
consistent across the board here not just a ranom special case"). In a CLEAN-CUT area a part of side channels only is
inside exactly when the atlas's cut puts all its lines there (the registry's area item holds them, `side_channels.cut_area_members`), never by
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
(`part.REVIEWED_INSIDE`) keeps the Gordon and the Oxbows (still inside) and takes the Skeena's side channel (his yes of
2026-11-28); it stays a gate."""
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


def test_inside_exactly_when_the_cut_holds_every_line(tmp_path):
    # (E35) "exactly": all of its lines held is inside, whatever the shapes say (both lines lie wholly OUTSIDE the
    # polygon here, so a shape measure gives 0 %); some held and some not is a part carrying the area's rule on some
    # sections only: the build stops, by name — never named by a share
    two = {10: LineString([(1100, 500), (1195, 500)]), 11: LineString([(1100, 600), (1105, 600)])}
    got = _inside(tmp_path, two, [GORDON], held={"a0": frozenset({10, 11})})
    assert got == [(1, 1, "Pacific Rim National Park Reserve of Canada", 1.0, "inside", 1)]
    with pytest.raises(sc.SideChannelError, match="the cut puts 95.0% of its lines in 'Pacific Rim"):
        _inside(tmp_path, two, [GORDON], held={"a0": frozenset({10})})


def test_a_part_the_cut_puts_in_a_reserve_is_never_left_unnamed(tmp_path):
    # the mutation pin of E35's second direction: carrying the area's rule without the area in its name stops the build
    # (a lake of the part lying outside the reserve: it can be named neither inside nor at)
    shapes = {**GROWN, 11: Polygon([(1300, 400), (1400, 400), (1400, 600), (1300, 600)])}
    with pytest.raises(sc.SideChannelError, match="gnis:18167 part 1: carries the rule of .'Pacific Rim"):
        _inside(tmp_path, shapes, [GORDON], iid="gnis:18167", held={"a0": frozenset({10})})


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


def test_the_reviewed_inside_list_holds_the_reviewed_parts_and_his_one_new_yes():
    # the Gordon and the Oxbows are still inside: their entries stay (never edited to make a build pass); the Skeena's
    # side channel is his yes of 2026-11-28 (E35). Still a GATE: any other part newly said inside a cut area stops the
    # build until he has looked (`builders._names`)
    assert P.REVIEWED_INSIDE == frozenset({
        ("gnis:2936", "SKEENA RIVER ECOLOGICAL RESERVE", "inside"),
        ("gnis:25671", "BAYNES ISLAND ECOLOGICAL RESERVE", "inside"),
        ("gnis:18167", "Pacific Rim National Park Reserve of Canada", "inside"),
        ("wsc:300-432687-459615", "HAYNE'S LEASE ECOLOGICAL RESERVE", "at"),
        ("gnis:2123", "CRESTON VALLEY WILDLIFE MANAGEMENT AREA", "inside"),
        ("gnis:21681", "STRATHCONA - WESTMIN PARK", "inside")})


# --- on a built world: the mini chain's atlas and bundle -----------------------------------------------------------

def name_rule_disagreements(bundle, atlas_dir) -> list:
    """(E35) Every LISTED part with no main stem whose area in its name (`part_inside`, a clean-cut area) differs from
    the clean-cut areas whose cut puts ALL its sections in them (the registry's area items), both ways."""
    import json
    import sqlite3
    from collections import defaultdict
    from pathlib import Path
    import pyogrio
    from pipeline.atlas.splits.area_splits import cut_mode, load_area_split_defs
    atlas_dir = Path(atlas_dir)
    clean_kinds = {ad.get("kind", ad["id"]) for ad in load_area_split_defs() if cut_mode(ad) == "clean"}
    cat = pyogrio.read_dataframe(atlas_dir / "area_catalog.gpkg", layer="areas", read_geometry=False)
    clean = {a: n for a, n, k, c in zip(cat["area_id"], cat["name"], cat["kind"], cat["cut"]) if c and k in clean_kinds}
    ids = (atlas_dir / "section_handles.txt").read_text().split("\n")
    hid = {s: i + 1 for i, s in enumerate(ids) if s}
    holds = defaultdict(set)
    for it in json.loads((atlas_dir / "registry.json").read_text())["items"]:
        if it["id"] in clean:
            for s in it.get("section_ids") or ():
                holds[hid[s]].add(clean[it["id"]])
    db = sqlite3.connect(str(bundle))
    off = dict(db.execute("SELECT sid, off_stem FROM section_span"))
    parts = defaultdict(list)
    for iid, o, p, s in db.execute("SELECT i.item_id, ps.ord, ps.part_ix, ps.sid FROM part_section ps JOIN item i ON "
                                   "i.ord = ps.ord JOIN part pt ON pt.ord = ps.ord AND pt.part_ix = ps.part_ix WHERE "
                                   "pt.tidal = 0 AND pt.key_ix IS NOT NULL"):
        parts[(iid, o, p)].append(s)
    named = {(o, p): a for o, p, a in db.execute("SELECT ord, part_ix, area FROM part_inside WHERE cut = 1")}
    out = []
    for (iid, o, p), ss in sorted(parts.items()):
        if not all(off.get(s) == 1 for s in ss):
            continue
        carried = set.intersection(*(holds.get(s, set()) for s in ss))
        said = named.get((o, p))
        if said in clean.values() or carried:
            if not (said in carried and len(carried) == 1):
                out.append((iid, p, said, sorted(carried)))
    return out


def test_on_the_mini_chain_every_side_part_is_named_with_the_reserve_it_carries(tmp_path_factory):
    import json
    import shutil
    import sqlite3
    import geopandas as gpd
    import pandas as pd
    from shapely.geometry import box as _box
    from pipeline.tests import mini_chain
    b = mini_chain.built(tmp_path_factory)
    bundle, atlas = b / "deliver" / "bundle.sqlite", b / "atlas"
    assert name_rule_disagreements(bundle, atlas) == []
    # the mutation pins, on a tampered copy of the built world (the mini world has no part of side channels only and
    # no clean-cut reserve): one listed part made a part of side channels only and put in a clean-cut reserve by its
    # registry item, with no name (carries, not named: caught); named with it (agreed); the item then gone (named, not
    # carrying: caught)
    t = tmp_path_factory.mktemp("tamper")
    shutil.copy(atlas / "section_handles.txt", t / "section_handles.txt")
    copy = t / "bundle.sqlite"
    src, dst = sqlite3.connect(str(bundle)), sqlite3.connect(str(copy))
    src.backup(dst)
    src.close()
    o, p = dst.execute("SELECT pt.ord, pt.part_ix FROM part pt WHERE pt.tidal = 0 AND pt.key_ix IS NOT NULL "
                       "ORDER BY pt.ord, pt.part_ix LIMIT 1").fetchone()
    sids = [s for (s,) in dst.execute("SELECT sid FROM part_section WHERE ord = ? AND part_ix = ?", (o, p))]
    dst.executemany("UPDATE section_span SET off_stem = 1 WHERE sid = ?", [(s,) for s in sids])
    dst.commit()
    cat = gpd.read_file(atlas / "area_catalog.gpkg", layer="areas")
    row = gpd.GeoDataFrame({"area_id": ["area:ecological_reserves:x"], "name": ["X ECOLOGICAL RESERVE"],
                            "kind": ["ecological_reserves"], "cut": [True]}, geometry=[_box(0, 0, 1, 1)], crs=cat.crs)
    gpd.GeoDataFrame(pd.concat([cat, row], ignore_index=True), crs=cat.crs).to_file(t / "area_catalog.gpkg", layer="areas")
    reg = json.loads((atlas / "registry.json").read_text())
    ids = (atlas / "section_handles.txt").read_text().split("\n")
    reg["items"].append({"id": "area:ecological_reserves:x", "name": "area:ecological_reserves:x", "kind": "area",
                         "variants": [], "mus": [], "section_ids": [ids[s - 1] for s in sids]})
    (t / "registry.json").write_text(json.dumps(reg))
    iid = dst.execute("SELECT item_id FROM item WHERE ord = ?", (o,)).fetchone()[0]
    assert name_rule_disagreements(copy, t) == [(iid, p, None, ["X ECOLOGICAL RESERVE"])]
    dst.execute("DELETE FROM part_inside WHERE ord = ? AND part_ix = ?", (o, p))
    dst.execute("INSERT INTO part_inside VALUES (?, ?, 'X ECOLOGICAL RESERVE', 1.0, 'inside', 1)", (o, p))
    dst.commit()
    assert name_rule_disagreements(copy, t) == []
    reg["items"].pop()
    (t / "registry.json").write_text(json.dumps(reg))
    assert name_rule_disagreements(copy, t) == [(iid, p, "X ECOLOGICAL RESERVE", [])]
    dst.close()
