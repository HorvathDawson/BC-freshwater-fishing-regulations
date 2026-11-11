"""T54 — SECTION ACCESS: which no-access, permit, reserve and private-land polygons each section lies in, and how much.

`pipeline.deliver.bundle.access` reads the SAME polygons the tiles draw (the tile layers' geojsonl: `no_access`,
`indigenous_land`, `parcel`) and the sections' own tile geometry, and writes `access_area` + `section_access` into the
bundle. These pin the shares, the tiers, the names and the refusal of a tile set from another atlas, on hand-built
layers — no generated data needed.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from pipeline.deliver.bundle import access as A
from pipeline.tests import under_test

# a 1 x 1 degree square at the equator's scale is fine: the shares are ratios
SQ = [[[-123.0, 49.0], [-122.0, 49.0], [-122.0, 50.0], [-123.0, 50.0], [-123.0, 49.0]]]
EAST = [[[-121.0, 49.0], [-120.0, 49.0], [-120.0, 50.0], [-121.0, 50.0], [-121.0, 49.0]]]
FAR = [[[-110.0, 40.0], [-109.0, 40.0], [-109.0, 41.0], [-110.0, 41.0], [-110.0, 40.0]]]


def _feat(geom_type, coords, props, layer):
    return {"type": "Feature", "geometry": {"type": geom_type, "coordinates": coords}, "properties": props,
            "tippecanoe": {"layer": layer, "minzoom": 9, "maxzoom": 14}}


def _layers(tmp: Path, *, handles="abcd") -> Path:
    tiles = tmp / "tiles"
    lay = tiles / "layers"
    lay.mkdir(parents=True)
    (tiles / "atlas.meta.json").write_text(json.dumps({"section_handles": handles, "build": "full"}))
    rows = {
        "no_access": [
            _feat("Polygon", SQ, {"area_id": "Coquitlam Watershed [13480215]",
                                  "name": "Coquitlam Watershed [13480215]", "kind": "closed"}, "no_access"),
            _feat("Polygon", FAR, {"area_id": "UBC Malcolm Knapp Research Forest [1166294466]",
                                   "name": "UBC Malcolm Knapp Research Forest [1166294466]", "kind": "permit"},
                  "no_access"),
        ],
        "indigenous_land": [
            _feat("Polygon", EAST, {"area_id": "Capilano 5", "name": "Capilano 5"}, "indigenous_land"),
            # a reserve polygon with no name at all: kept, nameless (the wording says "an Indian reserve")
            _feat("Polygon", FAR, {}, "indigenous_land"),
        ],
        # one dissolved feature, no attributes, as the tiles draw it
        "parcel": [_feat("MultiPolygon", [EAST], {}, "parcel")],
        "stream": [
            # wholly inside the watershed
            _feat("LineString", [[-122.8, 49.5], [-122.2, 49.5]], {"section_id": 1, "ord": 1}, "stream"),
            # straddles the edge (x -122.5 .. -121.5, the edge at -122): half its length is inside
            _feat("LineString", [[-122.5, 49.5], [-121.5, 49.5]], {"section_id": 2, "ord": 1}, "stream"),
            # one section drawn as two features: 1 inside + 3 outside = a quarter inside
            _feat("LineString", [[-122.9, 49.2], [-122.8, 49.2]], {"section_id": 3, "ord": 1}, "stream"),
            _feat("LineString", [[-121.9, 49.2], [-121.6, 49.2]], {"section_id": 3, "ord": 1}, "stream"),
            # on reserve AND private land (EAST)
            _feat("LineString", [[-120.8, 49.5], [-120.2, 49.5]], {"section_id": 4, "ord": 1}, "stream"),
            # nowhere
            _feat("LineString", [[-100.0, 30.0], [-99.0, 30.0]], {"section_id": 5, "ord": 1}, "stream"),
            # touches the watershed's edge only: no length inside, no row
            _feat("LineString", [[-122.0, 49.5], [-121.5, 49.5]], {"section_id": 6, "ord": 1}, "stream"),
        ],
        "lake": [
            # a lake half inside the watershed (by AREA)
            _feat("Polygon", [[[-122.5, 49.4], [-121.5, 49.4], [-121.5, 49.6], [-122.5, 49.6], [-122.5, 49.4]]],
                  {"section_id": 7, "water": "lake"}, "lake"),
        ],
        "wetland": [],
    }
    for name, fs in rows.items():
        (lay / f"{name}.geojsonl").write_text("".join(json.dumps(f) + "\n" for f in fs))
    return tiles


def test_display_names_drop_the_osm_id_suffix():
    assert [A.display_name(n) for n in ("Coquitlam Watershed [13480215]", "Capilano 5", "  Seymour Watershed [678689186] ",
                                        "Restricted-access land [2110072]", "")] == \
        ["Coquitlam Watershed", "Capilano 5", "Seymour Watershed", "Restricted-access land", ""]


def test_the_tiers_are_read_from_the_layer_and_its_kind():
    assert [A.tier_of("no_access", "closed"), A.tier_of("no_access", "permit"), A.tier_of("indigenous_land", None),
            A.tier_of("parcel", None)] == ["no_access", "permit", "reserve", "private"]
    with pytest.raises(ValueError, match="no_access kind 'open'"):
        A.tier_of("no_access", "open")


def test_shares_from_the_tile_layers(tmp_path):
    tiles = _layers(tmp_path)
    areas, rows = A.compute(tiles / "layers")
    # the areas, in (tier, name, area_id) order: names without the OSM suffix, the nameless reserve kept nameless
    assert [(a.tier, a.name, a.area_id) for a in areas] == [
        ("no_access", "Coquitlam Watershed", "Coquitlam Watershed [13480215]"),
        ("permit", "UBC Malcolm Knapp Research Forest", "UBC Malcolm Knapp Research Forest [1166294466]"),
        ("reserve", "", ""),
        ("reserve", "Capilano 5", "Capilano 5"),
        ("private", "", "parcel"),
    ]
    ix = {a.name or a.tier + ":" + a.area_id: i for i, a in enumerate(areas)}
    got = {(sid, areas[a].tier, areas[a].name): pm for sid, a, pm in rows}
    assert got == {
        (1, "no_access", "Coquitlam Watershed"): 1000,
        (2, "no_access", "Coquitlam Watershed"): 500,
        (3, "no_access", "Coquitlam Watershed"): 250,
        (4, "reserve", "Capilano 5"): 1000,
        (4, "private", ""): 1000,
        (7, "no_access", "Coquitlam Watershed"): 500,
    }
    # sorted by (sid, area): the bundle writes them as they come, so the table is deterministic
    assert rows == sorted(rows)
    assert ix  # every area has an index


def _db(named=(1, 2, 3, 4, 7)) -> sqlite3.Connection:
    """The module's tables and the named sections (`item_section`, written before access in a build)."""
    db = sqlite3.connect(":memory:")
    db.executescript(A.SCHEMA + "CREATE TABLE item_section (sid INTEGER NOT NULL, ord INTEGER NOT NULL);")
    db.executemany("INSERT INTO item_section VALUES (?, 1)", [(s,) for s in named])
    return db


def test_write_refuses_tiles_from_another_atlas(tmp_path):
    tiles = _layers(tmp_path, handles="ffff")
    db = _db()
    with pytest.raises(SystemExit, match="section handles ffff.*not the bundle's abcd"):
        A.write(db, tiles, handles="abcd")


def test_write_fills_the_tables(tmp_path):
    tiles = _layers(tmp_path)
    db = _db()
    n = A.write(db, tiles, handles="abcd")
    assert n == 6
    assert db.execute("SELECT ix, tier, name FROM access_area ORDER BY ix").fetchall() == [
        (0, "no_access", "Coquitlam Watershed"), (1, "permit", "UBC Malcolm Knapp Research Forest"),
        (2, "reserve", ""), (3, "reserve", "Capilano 5"), (4, "private", "")]
    assert db.execute("SELECT sid, area, permille FROM section_access ORDER BY sid, area").fetchall() == [
        (1, 0, 1000), (2, 0, 500), (3, 0, 250), (4, 3, 1000), (4, 4, 1000), (7, 0, 500)]
    # (T387) the named sections' lengths only (5 and 6 are not named): written, whole metres
    assert [s for (s,) in db.execute("SELECT sid FROM section_length ORDER BY sid")] == [1, 2, 3, 4, 7]
    # (T561) the share inside the no-access union: one no-access polygon here, so the same as its rows
    assert db.execute("SELECT sid, permille FROM section_no_access ORDER BY sid").fetchall() == [
        (1, 1000), (2, 500), (3, 250), (7, 500)]
    assert A.mostly_inside(db) == {1}


def _nested(tmp: Path, *, x0: float = -129.9, x1: float = -129.6) -> Path:
    """Two no-access polygons far from the others, one nested in the other (OSM's "Slesse Creek Demolition Training
    Areas" inside "Slesse RTA"): the outer x -130..-129, the inner x `x0`..`x1`; section 8 runs x -129.85..-129.65 (wholly
    inside both by default), section 9 x -129.5..-128.5 (half inside the outer)."""
    tiles = _layers(tmp)
    lay = tiles / "layers"

    def sq(a, b):
        return [[[a, 55.1], [b, 55.1], [b, 55.9], [a, 55.9], [a, 55.1]]]
    with (lay / "no_access.geojsonl").open("a") as fh:
        for aid, ring in (("Slesse RTA [2]", [[[-130.0, 55.0], [-129.0, 55.0], [-129.0, 56.0], [-130.0, 56.0],
                                                 [-130.0, 55.0]]]), ("Inner [1]", sq(x0, x1))):
            fh.write(json.dumps(_feat("Polygon", ring, {"area_id": aid, "name": aid, "kind": "closed"}, "no_access"))
                     + "\n")
    with (lay / "stream.geojsonl").open("a") as fh:
        for sid, (a, b) in ((8, (-129.85, -129.65)), (9, (-129.5, -128.5))):
            fh.write(json.dumps(_feat("LineString", [[a, 55.5], [b, 55.5]], {"section_id": sid, "ord": 1}, "stream"))
                     + "\n")
    return tiles


def test_nested_no_access_polygons_count_once(tmp_path):
    """T561: a section inside two nested no-access polygons is named in both (a row each, 1000 permille), but its share
    of no-access land is 1000, never 2000 — the union is measured, not the sum."""
    tiles = _nested(tmp_path)
    db = _db()
    A.write(db, tiles, handles="abcd")
    named = db.execute("SELECT a.name, s.permille FROM section_access s JOIN access_area a ON a.ix = s.area "
                       "WHERE s.sid = 8 ORDER BY a.name").fetchall()
    assert named == [("Inner", 1000), ("Slesse RTA", 1000)]
    assert db.execute("SELECT permille FROM section_no_access WHERE sid = 8").fetchone() == (1000,)
    assert db.execute("SELECT permille FROM section_no_access WHERE sid = 9").fetchone() == (500,)
    assert db.execute("SELECT MAX(permille) FROM section_no_access").fetchone()[0] <= 1000


def test_a_section_the_sum_and_the_union_colour_differently_is_refused(tmp_path):
    """T561: two overlapping no-access polygons each holding 300 permille of a section (the same 300) sum to 600 —
    the app's summed rule would paint it, the union (300) would not. Until the app reads the union, write refuses."""
    areas = [A.Area("no_access", "a", "A"), A.Area("no_access", "b", "B"), A.Area("permit", "c", "C")]
    rows = [(1, 0, 300), (1, 1, 300), (2, 0, 1000), (2, 1, 1000), (3, 2, 900)]
    assert A.summed_rule_disagrees(areas, rows, [(1, 300), (2, 1000)]) == [1]
    assert A.summed_rule_disagrees(areas, rows, [(1, 600), (2, 1000)]) == []
    # end to end: the inner polygon over x -129.4..-129.0 — section 9 is half inside the outer and 400 permille inside
    # both: summed 900, union 500 (not "mostly")
    tiles = _nested(tmp_path, x0=-129.4, x1=-129.0)
    with pytest.raises(SystemExit, match=r"1 section\(s\) are mostly inside.*\[9\]"):
        A.write(_db(), tiles, handles="abcd")


def test_section_lengths_along_the_water(tmp_path):
    """T387: a line's length (its longitudes scaled by the cosine of its latitude), a section drawn as two features summed,
    and a section drawn only as a polygon by half its perimeter — metres, at `METRES_PER_DEGREE`."""
    import math
    _, _, lengths, _ = A.measure(_layers(tmp_path) / "layers")
    k = math.cos(math.radians(49.5))
    m = A.METRES_PER_DEGREE
    assert A.METRES_PER_DEGREE == 111_320.0
    assert lengths[1] == round(0.6 * k * m)                          # one line, 0.6 degrees of longitude at 49.5 N
    assert abs(lengths[3] - 0.4 * math.cos(math.radians(49.2)) * m) <= 1   # two features, one section: summed
    assert lengths[7] == round((1.0 * k + 0.2) * m)                  # a lake 1 x 0.2 degrees: half its perimeter


def test_mostly_inside_is_the_apps():
    """T226: the part grouping's "mostly inside" (`MOSTLY_INSIDE_PERMILLE`) is the app's fourth-colour threshold."""
    import re
    ts = (Path(__file__).resolve().parents[2] / "app/packages/core/src/access.ts").read_text()
    m = re.search(r"export const MOSTLY_INSIDE = ([0-9.]+);", ts)
    assert m and round(float(m.group(1)) * 1000) == A.MOSTLY_INSIDE_PERMILLE


def test_the_schema_in_the_bundle_is_the_modules():
    """The bundle's schema.sql carries the two tables exactly as this module declares them (one declaration)."""
    schema = (Path(A.__file__).parent / "schema.sql").read_text()
    for stmt in A.SCHEMA.strip().split(";"):
        if stmt.strip():
            assert " ".join(stmt.split()) in " ".join(schema.split())


# ---- real data: the shipped bundle (needs the bundle rebuilt with T54's step) ------------------------------------------

#: (water, its id, the no-access area it runs through): every one has sections MOSTLY inside — the app's fourth colour
NAMED = [("Coquitlam River", "gnis:19986", "Coquitlam Watershed"), ("Seymour River", "gnis:6105", "Seymour Watershed"),
         ("Capilano River", "gnis:5922", "Capilano Watershed"),
         # Q3 (default yes): lakes inside take the fourth colour too
         ("Coquitlam Lake", "wbk:329291808", "Coquitlam Watershed"), ("Capilano Lake", "wbk:329532436", "Capilano Watershed")]


@pytest.mark.needs_bundle
def test_the_named_watersheds_hold_their_rivers_and_lakes():
    """Measured 2026-10-22 on the live tiles: Coquitlam 785 / Seymour 544 / Capilano 875 sections mostly inside (of 789 /
    544 / 880 touching); 4,028 (sid, area) rows over 50% province-wide (4,023 sections: 5 sit in two nested military
    areas). Held to 95% of those (the atlas build is not bit-identical).
    Re-pinned 2026-10-25 (r-9b51c28ce0fe, T64): the tiles no longer draw unnamed zone-only wetlands (`layers/undrawn.json`,
    333,471) and access is measured on the tiles' own sections, so the undrawn wetlands carry no rows — 3,603 rows over 50%
    (Camp Chilcoten 1,052 -> 743, Greater Victoria Water Supply Area 590 -> 492, the three watersheds -13); the no-access
    land fill still draws there."""
    bundle = under_test.bundle()
    db = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    if not db.execute("SELECT 1 FROM sqlite_master WHERE name = 'section_access'").fetchone():
        pytest.fail("the bundle predates T54 (no section_access): python -m pipeline.deliver all")
    mostly = dict(db.execute("SELECT a.name, COUNT(DISTINCT s.sid) FROM section_access s JOIN access_area a ON a.ix = s.area "
                             "WHERE a.tier = 'no_access' AND s.permille > 500 GROUP BY a.name"))
    assert [mostly.get(n, 0) >= k for n, k in (("Coquitlam Watershed", 745), ("Seymour Watershed", 516),
                                                ("Capilano Watershed", 831))] == [True, True, True], mostly
    assert sum(mostly.values()) >= 3423
    for water, iid, area in NAMED:
        got = db.execute("""SELECT MAX(s.permille) FROM section_access s JOIN access_area a ON a.ix = s.area
                            JOIN item_section i ON i.sid = s.sid JOIN item t ON t.ord = i.ord
                            WHERE t.item_id = ? AND a.name = ? AND a.tier = 'no_access'""", (iid, area)).fetchone()[0]
        assert (water, (got or 0) > 500) == (water, True)
    # the names are the reader's: no OSM id suffix anywhere
    assert db.execute("SELECT COUNT(*) FROM access_area WHERE name GLOB '*[[]*[0-9][]]'").fetchone()[0] == 0
