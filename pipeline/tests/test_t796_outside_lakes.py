"""T796 S3 (owner 2026-11-16: "take out the lakes but add a note"; RULINGS Z10): a lake that lies mostly outside an
ecological reserve or park its polygon only reaches into is taken out of it by name, and its page says so once.

Cold Fish Lake (0.1% inside the Gladys Lake Ecological Reserve) has an item; the Cormier Creek pond (28% inside the
Blue/Dease Rivers reserve) is folded into its creek, so it is named by its section handle — the creek stays inside.
Each test builds its own small data.
"""
from __future__ import annotations

import json
import sqlite3

import pytest

from pipeline.atlas.registry import outside_area_items
from pipeline.common.models.registry import RegistryItem
from pipeline.deliver.bundle import outside_note as N

GLADYS = "area:ecological_reserves:gladys_lake_ecological_reserve"
DEASE = "area:ecological_reserves:blue_dease_rivers_ecological_reserve"
DEFS = [{"id": "ecological_reserves", "tile_layer": "eco_reserve",
         "outside_items": {"gladys_lake_ecological_reserve": ["wbk:329529449"],
                           "blue_dease_rivers_ecological_reserve": ["lake:329638447"]}}]


def _registry():
    return {GLADYS: RegistryItem(id=GLADYS, name=GLADYS, kind="area", section_ids=("lake:329529449", "s1")),
            DEASE: RegistryItem(id=DEASE, name=DEASE, kind="area",
                                section_ids=("lake:329638447", "359561868:53628")),
            "wbk:329529449": RegistryItem(id="wbk:329529449", name="Cold Fish Lake", kind="lake",
                                          section_ids=("lake:329529449",)),
            "gnis:20010": RegistryItem(id="gnis:20010", name="Cormier Creek", kind="stream",
                                       section_ids=("359561868:53588", "lake:329638447", "359561868:53628"))}


def test_a_lake_with_no_item_leaves_the_area_by_its_section_only():
    """The pond named `lake:<id>` leaves the reserve; the creek's inside section stays (naming the creek's item would
    take it out too). Idempotent (the sidecars step applies the pass again); an unknown lake section is refused.
    MUTATION: refuse a `lake:` id that is not an item and the build stops."""
    once = outside_area_items(_registry(), DEFS)
    assert once[DEASE].section_ids == ("359561868:53628",)
    assert once[GLADYS].section_ids == ("s1",)
    assert outside_area_items(dict(once), DEFS) == once
    with pytest.raises(SystemExit, match="no section of this build"):
        outside_area_items(_registry(), [{"id": "ecological_reserves",
                                          "outside_items": {"blue_dease_rivers_ecological_reserve": ["lake:1"]}}])


def _bundle(tmp_path):
    db = sqlite3.connect(":memory:")
    db.executescript("CREATE TABLE item (ord INTEGER, item_id TEXT); CREATE TABLE item_section (ord INTEGER, sid INTEGER);"
                     "CREATE TABLE entry (entry_id TEXT, extents TEXT, pages TEXT);"
                     "CREATE TABLE rule (entry_id TEXT, rule_id TEXT, closure_grade TEXT);"
                     "CREATE TABLE section_note (sid INTEGER PRIMARY KEY, note TEXT NOT NULL);")
    db.executemany("INSERT INTO item VALUES (?, ?)", [(1, "wbk:329529449"), (2, "gnis:20010")])
    db.executemany("INSERT INTO item_section VALUES (?, ?)", [(1, 1804040), (2, 739996), (2, 1912509)])
    db.execute("INSERT INTO entry VALUES ('zp:superior_closures', ?, '[9]')",
               (json.dumps([{"op": "within", "area_kind": "national_parks"},
                            {"op": "within", "area_kind": "ecological_reserves"}]),))
    db.execute("INSERT INTO entry VALUES ('r6:somewhere', ?, '[55]')",
               (json.dumps([{"op": "within", "area_kind": "ecological_reserves"}]),))
    db.execute("INSERT INTO rule VALUES ('zp:superior_closures', 'superior_closures.r3', 'full')")
    db.execute("INSERT INTO rule VALUES ('r6:somewhere', 'somewhere.r1', NULL)")   # no closure: not its page
    layers = tmp_path / "layers"
    layers.mkdir()
    (layers / "eco_reserve.geojsonl").write_text("".join(json.dumps({"type": "Feature", "properties": p}) + "\n" for p in (
        {"area_id": "GLADYS LAKE ECOLOGICAL RESERVE", "name": "Gladys Lake Ecological Reserve"},
        {"area_id": "BLUE/DEASE RIVERS ECOLOGICAL RESERVE", "name": "Blue/Dease Rivers Ecological Reserve"})))
    return db, layers


def test_each_lake_taken_out_carries_one_note_written_here(tmp_path):
    """Cold Fish Lake's section and the Cormier pond's section each get one sentence naming the reserve by its map
    name and the closing row's page; the creek's other sections get none. MUTATION: no note and the page says nothing
    of the reserve beside it."""
    db, layers = _bundle(tmp_path)
    got = N.write(db, DEFS, layers, {"lake:329638447": 1912509})
    assert got == {
        1804040: "Mostly outside Gladys Lake Ecological Reserve: its closure (rule book p.9) does not apply here.",
        1912509: "Mostly outside Blue/Dease Rivers Ecological Reserve: its closure (rule book p.9) does not apply "
                 "here."}
    assert dict(db.execute("SELECT sid, note FROM section_note")) == got


def test_a_listed_water_the_bundle_does_not_hold_stops_the_build(tmp_path):
    """Never a silent missing note: a listed lake with no section, or an area no tile names, stops the build."""
    db, layers = _bundle(tmp_path)
    with pytest.raises(SystemExit, match="lake:329638447 has no section"):
        N.write(db, DEFS, layers, {})
    defs = [{"id": "ecological_reserves", "tile_layer": "eco_reserve",
             "outside_items": {"tow_hill_ecological_reserve": ["wbk:329529449"]}}]
    with pytest.raises(SystemExit, match="no tile names this area"):
        N.write(db, defs, layers, {})


def test_the_curated_lakes_are_the_owners_nine():
    """The owner's list (2026-11-16, S3 option 1): the nine lakes and ponds mostly outside, each named once. A lake with
    an item of its own by its item (Cold Fish Lake), the others by their section handle — never a creek's item."""
    from pipeline.atlas.splits.area_splits import load_area_split_defs
    listed = {i for _d, _k, _s, items in N.outside_listed(load_area_split_defs()) for i in items}
    nine = {"wbk:329529449", "lake:329531505", "lake:329165788", "lake:329167166", "lake:329167101",
            "lake:328981611", "lake:329381384", "lake:329261118", "lake:329638447"}
    assert nine <= listed
    assert listed - nine == {"wbk:329083511"}, "only Kennedy Lake (Pacific Rim, RULINGS Z11) besides the nine"
    assert not any(i.startswith("gnis:") for i in listed)
