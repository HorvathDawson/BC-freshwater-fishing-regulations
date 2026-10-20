"""P09: part topology + camera hints (`part_geo`, `part_mark`, `water_exit`), the map navigation's data with no geometry.

No data: the pure rules (km, rank, midpoint walk, mark merge, split labels). On a bundle (`needs_bundle`; point
`UI_EXPORT_BUNDLE` at a SIDE bundle built with `python -m pipeline.deliver bundle --out …/bundle.next.sqlite`, never the
live one before promotion): the Vedder / Chilliwack and the Cowichan against the prototype's water map
(`app/design/regs-v11/watermap.js`, written by `water_map_export` from the same build), and every part's midpoint.
"""
from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

import pytest

from pipeline.deliver.bundle import topology as T


# ---- the rules, no data ------------------------------------------------------------------------------------------

def test_part_km_is_the_highest_main_stem_km_else_the_side_channels():
    assert T.part_km([{"km_from": 3.0}, {"km_from": 7.5}, {"km_from": 9.0, "branch": True}]) == 7.5
    assert T.part_km([{"km_from": 2.0, "branch": True}, {"km_from": None}]) == 2.0
    assert T.part_km([]) is None


def test_parts_rank_mouth_first_and_parts_without_km_go_last_in_part_order():
    parts = [{"v": 0, "km": 30.0}, {"v": 1, "km": 5.0}, {"v": 2, "km": None}, {"v": 3, "km": 12.0}, {"v": 4}]
    assert [p["v"] for p in T.order_parts(parts)] == [1, 3, 0, 2, 4]


def test_the_midpoint_is_the_section_holding_half_the_stem_length():
    assert T.stem_midpoint([(10, 100.0), (11, 100.0), (12, 100.0), (13, 700.0)]) == 13
    assert T.stem_midpoint([(10, 400.0), (11, 100.0), (12, 500.0)]) == 11
    assert T.stem_midpoint([(10, None), (11, None)]) == 10


def test_marks_at_one_point_with_one_down_part_merge():
    raw = [{"lon": -122.0, "lat": 49.1, "down": 0, "up": 2, "label": None},
           {"lon": -122.0, "lat": 49.1, "down": 0, "up": 1, "label": "Bridge"},
           {"lon": -122.1, "lat": 49.2, "down": 1, "up": 3, "label": None}]
    assert T.merge_marks(raw) == [
        {"lon": -122.0, "lat": 49.1, "down": 0, "up": [1, 2], "label": "Bridge"},
        {"lon": -122.1, "lat": 49.2, "down": 1, "up": [3], "label": None}]


def test_a_boundary_is_named_by_the_downstream_runs_start_else_the_upstream_runs_end():
    splits = {"a": {"name": "Vedder Crossing Bridge"}, "b": {"name": "Tamihi"}}
    down, up = {"km": 1.0, "runs": [{"from": "a"}]}, {"km": 5.0, "runs": [{"to": "b"}]}
    assert T.split_label(down, up, splits) == "Vedder Crossing Bridge"
    assert T.split_label({"km": 1.0, "runs": [{"from": None}]}, up, splits) == "Tamihi"
    # a branch is named only by its own runs' ends
    assert T.split_label(down, {"km": None, "runs": [{"to": None, "from": "b"}]}, splits) == "Tamihi"
    assert T.split_label({"runs": []}, {"km": 2.0, "runs": []}, splits) is None


def test_the_water_map_export_reads_the_same_rules():
    from pipeline.tools import water_map_export as W
    assert W.order_parts is T.order_parts and W.split_label is T.split_label


# ---- on a bundle -------------------------------------------------------------------------------------------------

def _bundle(request) -> sqlite3.Connection:
    from pipeline.deliver.bundle import read as R
    from pipeline.tests.conftest import need, BUNDLE_HINT
    p = need(request, "bundle", Path(os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE), BUNDLE_HINT)
    db = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
    if not db.execute("SELECT 1 FROM sqlite_master WHERE name = 'part_geo'").fetchone():
        pytest.fail(f"{p} predates P09 (no part_geo): build a side bundle with `python -m pipeline.deliver bundle "
                    f"--out <dir>/bundle.next.sqlite` and point UI_EXPORT_BUNDLE at it")
    return db


def _watermap() -> dict:
    from pipeline.common.curated import REPO_ROOT
    t = (REPO_ROOT / "app" / "design" / "regs-v11" / "watermap.js").read_text()
    return json.loads(t[t.index("{"):t.rindex("}") + 1])["waters"]


def _ranked(db, item: str) -> list[int]:
    return [p for (p,) in db.execute("SELECT g.part_ix FROM part_geo g JOIN item i ON i.ord = g.ord "
                                     "WHERE i.item_id = ? ORDER BY g.rank", (item,))]


def _marks(db, item: str) -> list[tuple]:
    return [(d, tuple(json.loads(u)), lab) for d, u, lab in db.execute(
        "SELECT m.down_part, m.up_parts, m.label FROM part_mark m JOIN item i ON i.ord = m.ord "
        "WHERE i.item_id = ? ORDER BY m.mark_ix", (item,))]


@pytest.mark.needs_bundle
def test_the_chilliwack_and_the_cowichan_rank_and_meet_as_the_prototype_draws_them(request):
    db = _bundle(request)
    wm = _watermap()
    for group, item in (("vedder", "gnis:8634"), ("cowichan", "gnis:12227")):
        parts = [p for p in wm[group]["parts"] if p["item"] == item]
        assert _ranked(db, item) == [p["v"] for p in parts], group
        # every boundary the prototype draws between two parts of this water, with its words
        at = {i for i, p in enumerate(wm[group]["parts"]) if p["item"] == item}
        want = sorted((wm[group]["parts"][m["down"]]["v"], (wm[group]["parts"][m["up"]]["v"],), m["label"])
                      for m in wm[group]["marks"] if m["down"] in at and m["up"] in at)
        assert sorted(_marks(db, item)) == want, group


@pytest.mark.needs_bundle
def test_the_chilliwack_flows_into_the_vedder_at_vedder_crossing(request):
    db = _bundle(request)
    row = db.execute("SELECT d.item_id, x.down_part, x.label FROM water_exit x JOIN item i ON i.ord = x.ord "
                     "JOIN item d ON d.ord = x.down_ord WHERE i.item_id = 'gnis:8634'").fetchone()
    assert row == ("gnis:3062", 0, "Vedder River")


@pytest.mark.needs_bundle
def test_every_part_has_a_midpoint_of_its_own_inside_its_box(request):
    db = _bundle(request)
    n_parts = db.execute("SELECT COUNT(*) FROM part").fetchone()[0]
    rows = db.execute("SELECT g.ord, g.part_ix, g.mid_sid, g.mid_lon, g.mid_lat, g.w, g.s, g.e, g.n, "
                      "EXISTS (SELECT 1 FROM part_section ps WHERE ps.ord = g.ord AND ps.part_ix = g.part_ix "
                      "AND ps.sid = g.mid_sid) FROM part_geo g").fetchall()
    assert len(rows) == n_parts
    assert not [r[:3] for r in rows if not r[9]], "mid_sid outside its part"
    eps = 1e-6
    assert not [r[:2] for r in rows if not (r[5] - eps <= r[3] <= r[7] + eps and r[6] - eps <= r[4] <= r[8] + eps)]
    sizes = {t: db.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in ("part_geo", "part_mark", "water_exit")}
    print("P09 sizes:", sizes)


def test_keep_named_reads_the_1_based_handle_and_the_lake_polygons(tmp_path):
    """sid N is LINE N of section_handles.txt (1-based; 0 is no section). Reading ids[sid] shifted every shape by one
    section (found by the Chilliwack test). A lake's outline comes from waterbody_polys.pkl."""
    import pickle
    from shapely.geometry import LineString, Point
    build = tmp_path / "full"
    build.mkdir()
    (build / "section_handles.txt").write_text("blk:1:0\nblk:1:100\nlake:7\n")
    with (build / "waterbody_polys.pkl").open("wb") as fh:
        pickle.dump({"lake:7": Point(0, 0).buffer(5)}, fh)
    geoms = {"blk:1:0": LineString([(0, 0), (1, 0)]), "blk:1:100": LineString([(0, 0), (9, 0)])}
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE item_section (ord INTEGER, sid INTEGER)")
    db.executemany("INSERT INTO item_section VALUES (1, ?)", [(1,), (2,), (3,)])
    got = T.keep_named(db, build, geoms)
    assert (got[1].length, got[2].length, got[3].geom_type) == (1.0, 9.0, "Polygon")
