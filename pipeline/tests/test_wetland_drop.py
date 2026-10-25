"""T64 — the tiles draw no unnamed wetland that is in no item (owner 2026-10-23), and the bundle build
proves none of them needed drawing (`bundle.undrawn`)."""

from __future__ import annotations

import json
import pickle
import sqlite3

import orjson
import pytest
from shapely.geometry import box

from pipeline.common.models import NodeKind, StreamGraph, StreamNode
from pipeline.common.models.names import NameTuple
from pipeline.common.models.enums import NameSource
from pipeline.deliver.bundle import undrawn as U
from pipeline.deliver.tiles.export import UNDRAWN_FILE, export_waterbodies


def _wet(wbk, **kw):
    return StreamNode(node_id=f"lake:{wbk}", kind=NodeKind.wetland, wbk=str(wbk), **kw)


def _build(tmp_path):
    g = StreamGraph()
    nodes = [_wet(1),                                                       # unnamed, no item: DROPPED
             _wet(2),                                                       # a named wetland item
             _wet(3),                                                       # a slough folded into a river
             _wet(4, display_name="Corn Creek Marsh",
                  name_tuples=(NameTuple("Corn Creek Marsh", NameSource.gazette, "", "9"),)),
             StreamNode(node_id="lake:5", kind=NodeKind.lake, wbk="5")]    # an unnamed LAKE stays
    g.nodes = {n.node_id: n for n in nodes}
    (tmp_path / "graph.pkl").write_bytes(pickle.dumps(g))
    (tmp_path / "waterbody_polys.pkl").write_bytes(pickle.dumps(
        {n.node_id: box(1_000_000 + 1000 * i, 500_000, 1_000_500 + 1000 * i, 500_500)
         for i, n in enumerate(nodes)}))
    (tmp_path / "registry.json").write_text(json.dumps({"items": [
        {"id": "wbk:2", "name": "Witters Marsh", "kind": "wetland", "section_ids": ["lake:2"]},
        {"id": "gnis:7", "name": "Nicomen Slough", "kind": "stream", "section_ids": ["lake:3"]}]}))
    (tmp_path / "section_handles.txt").write_text("\n".join(f"lake:{i}" for i in range(1, 6)) + "\n")
    out = tmp_path / "layers"
    out.mkdir()
    return out


def test_only_the_unnamed_wetland_in_no_item_is_left_out(tmp_path):
    out = _build(tmp_path)
    counts = export_waterbodies(tmp_path, "", out)
    assert counts == {"wetland": 3, "lake": 1}
    drawn = {orjson.loads(x)["properties"]["section_id"]
             for x in (out / "wetland.geojsonl").read_bytes().splitlines()}
    assert drawn == {2, 3, 4}
    got = json.loads((out / UNDRAWN_FILE).read_text())
    assert got["wetland"] == [1]


def _db(**extra):
    db = sqlite3.connect(":memory:")
    db.executescript("""
        CREATE TABLE item (ord INTEGER, item_id TEXT, kind TEXT);
        CREATE TABLE entry (entry_id TEXT, item_id TEXT);
        CREATE TABLE ruleset (set_id INTEGER, entry_id TEXT, via TEXT);
        CREATE TABLE section_ruleset (sid INTEGER, set_id INTEGER);
        CREATE TABLE item_section (ord INTEGER, sid INTEGER);
        CREATE TABLE section_gauge (sid INTEGER);""")
    db.executemany("INSERT INTO item VALUES (?,?,?)",
                   [(0, "area:watershed:liard", "area"), (1, "gnis:5", "stream")])
    db.executemany("INSERT INTO entry VALUES (?,?)", [("z7:base", None),
                                                      ("r7:liard_river_watershed@7-1", "area:watershed:liard"),
                                                      ("r7:some_river@7-1", "gnis:5")])
    db.executemany("INSERT INTO ruleset VALUES (?,?,?)",
                   [(1, "z7:base", "reach"), (1, "r7:liard_river_watershed@7-1", "reach"),
                    (2, "z7:base", "reach"), (2, "r7:some_river@7-1", "trib")])
    db.executemany("INSERT INTO section_ruleset VALUES (?,?)", [(10, 1), (11, 2)])
    for t, rows in extra.items():
        db.executemany(f"INSERT INTO {t} VALUES ({','.join('?' * len(rows[0]))})", rows)
    return db


def test_zone_and_area_only_rules_may_go_unseen():
    """The Liard River watershed's row and the zone table are answered by the land."""
    assert U.problems(_db(), {"wetland": [10]}) == []


def test_a_water_rule_an_item_or_a_gauge_needs_the_map():
    db = _db(item_section=[(1, 12)], section_gauge=[(13,)])
    bad = U.problems(db, {"wetland": [10, 11, 12, 13]})
    assert bad == ["section 12 is in a registry item", "section 13 carries a gauge",
                   "section 11 is bound by a water row: r7:some_river@7-1"]


def test_the_list_is_required_when_the_sidecar_names_one(tmp_path):
    (tmp_path / "layers").mkdir()
    (tmp_path / "atlas.meta.json").write_text(json.dumps({"section_handles": "x"}))
    assert U.load(tmp_path) == {}                       # tiles from before the cut drew everything
    (tmp_path / "atlas.meta.json").write_text(json.dumps({"undrawn": {"wetland": 2}}))
    with pytest.raises(SystemExit, match="missing"):
        U.load(tmp_path)
    (tmp_path / "layers" / UNDRAWN_FILE).write_text(json.dumps({"wetland": [5]}))
    with pytest.raises(SystemExit, match="does not match"):
        U.load(tmp_path)
    (tmp_path / "layers" / UNDRAWN_FILE).write_text(json.dumps({"wetland": [5, 4]}))
    assert U.load(tmp_path) == {"wetland": [4, 5]}
