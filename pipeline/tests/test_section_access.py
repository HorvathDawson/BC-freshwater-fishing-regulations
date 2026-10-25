"""T54 — SECTION ACCESS: which no-access, permit, reserve and private-land polygons each section lies in, and how much.

`pipeline.deliver.bundle.access` reads the SAME polygons the tiles draw (the tile layers' geojsonl: `no_access`,
`indigenous_land`, `parcel`) and the sections' own tile geometry, and writes `access_area` + `section_access` into the
bundle. These pin the shares, the tiers, the names and the refusal of a tile set from another atlas, on hand-built
layers — no generated data needed.
"""
from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

import pytest

from pipeline.deliver.bundle import access as A

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


def test_write_refuses_tiles_from_another_atlas(tmp_path):
    tiles = _layers(tmp_path, handles="ffff")
    db = sqlite3.connect(":memory:")
    db.executescript(A.SCHEMA)
    with pytest.raises(SystemExit, match="section handles ffff.*not the bundle's abcd"):
        A.write(db, tiles, handles="abcd")


def test_write_fills_the_two_tables(tmp_path):
    tiles = _layers(tmp_path)
    db = sqlite3.connect(":memory:")
    db.executescript(A.SCHEMA)
    n = A.write(db, tiles, handles="abcd")
    assert n == 6
    assert db.execute("SELECT ix, tier, name FROM access_area ORDER BY ix").fetchall() == [
        (0, "no_access", "Coquitlam Watershed"), (1, "permit", "UBC Malcolm Knapp Research Forest"),
        (2, "reserve", ""), (3, "reserve", "Capilano 5"), (4, "private", "")]
    assert db.execute("SELECT sid, area, permille FROM section_access ORDER BY sid, area").fetchall() == [
        (1, 0, 1000), (2, 0, 500), (3, 0, 250), (4, 3, 1000), (4, 4, 1000), (7, 0, 500)]


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
    from pipeline.common.curated import GENERATED
    bundle = os.environ.get("UI_EXPORT_BUNDLE") or GENERATED.bundle / "bundle.sqlite"
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
