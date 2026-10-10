"""`item.area_ha` — a lake's size for ranking, from the tile's own polygon (2026-09-30).

Against the built bundle (`UI_EXPORT_BUNDLE`, else the shipped one); FAILS on a bundle that predates
the column (`predates`). The unit half builds the column from a two-lake fixture."""
from __future__ import annotations

import json
import os
import pickle
import sqlite3
from pathlib import Path

import pytest
from shapely.geometry import box

import importlib
B = importlib.import_module("pipeline.deliver.bundle.build")
from pipeline.deliver.bundle.read import BUNDLE as SHIPPED
from pipeline.tests.conftest import BUNDLE_HINT, need, predates

BUNDLE = Path(os.environ.get("UI_EXPORT_BUNDLE") or SHIPPED)


class _Cov:
    def __init__(self):
        self.got = {}

    def filled(self, t, n):
        self.got[t] = n

    def skip(self, t, why):
        self.got[t] = why


def _fixture(tmp_path):
    items = [{"id": "wbk:1", "name": "Big Lake", "kind": "lake", "section_ids": ["lake:1"]},
             {"id": "wbk:2", "name": "Pond", "kind": "lake", "section_ids": ["lake:2"]},
             {"id": "gnis:3", "name": "A Creek", "kind": "stream", "section_ids": ["3:0"]},
             # a wetland has a polygon too, and is not a lake
             {"id": "wbk:4", "name": "A Marsh", "kind": "wetland", "section_ids": ["lake:4"]}]
    reg = tmp_path / "registry.json"
    reg.write_text(json.dumps({"items": items}))
    with (tmp_path / "waterbody_polys.pkl").open("wb") as fh:
        pickle.dump({"lake:1": box(0, 0, 2000, 1000), "lake:2": box(0, 0, 40, 40),
                     "lake:4": box(0, 0, 500, 500)}, fh)
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE item (ord INTEGER PRIMARY KEY, item_id TEXT, name TEXT, kind TEXT, "
               "part_of TEXT, area_ha INTEGER)")
    for n, i in enumerate(items):
        db.execute("INSERT INTO item VALUES (?,?,?,?,NULL,NULL)", (n, i["id"], i["name"], i["kind"]))
    return db, reg


def test_a_lake_s_area_is_its_polygon_in_whole_hectares(tmp_path):
    db, reg = _fixture(tmp_path)
    B._lake_areas(db, tmp_path, json.loads(reg.read_text())["items"], _Cov())
    got = dict(db.execute("SELECT item_id, area_ha FROM item"))
    assert got == {"wbk:1": 200, "wbk:2": 0, "gnis:3": None, "wbk:4": None}   # 2 km² = 200 ha; a pond reads 0


def test_mutation_without_the_polygons_the_build_stops(tmp_path):
    """P2 (no missing-file fallbacks): no polygons used to skip `item.area_ha` and rank every
    lake like a pond; the build now stops, naming the file, and invents nothing."""
    db, reg = _fixture(tmp_path)
    (tmp_path / "waterbody_polys.pkl").unlink()
    with pytest.raises(SystemExit, match="no waterbody_polys.pkl"):
        B._lake_areas(db, tmp_path, json.loads(reg.read_text())["items"], _Cov())
    assert all(a is None for (a,) in db.execute("SELECT area_ha FROM item"))


@pytest.mark.needs_bundle
def test_the_built_bundle_carries_lake_area(request):
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    db = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    if "area_ha" not in {r[1] for r in db.execute("PRAGMA table_info(item)")}:
        predates(f"{BUNDLE} predates item.area_ha — point UI_EXPORT_BUNDLE at a side build")
    kinds = dict(db.execute("SELECT kind, count(area_ha) FROM item GROUP BY kind"))
    assert kinds.get("stream", 0) == 0 and kinds["lake"] > 7000
    (kam,) = db.execute("SELECT area_ha FROM item WHERE name = 'Kamloops Lake'").fetchone()
    assert 4_000 < kam < 6_000
