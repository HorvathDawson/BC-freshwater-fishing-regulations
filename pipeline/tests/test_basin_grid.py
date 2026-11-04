"""(T376) The watershed groups by name and by place: `pipeline/deliver/bundle/basin_grid.py`. No data: synthetic groups.

The grid row bytes pinned here are the same literals `app/packages/data/src/basinGrid.test.ts` decodes, so the writer
and the app's reader are held to one format.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from pipeline.deliver.bundle import basin_grid as BG
from pipeline.deliver.release import build as RB
from pipeline.deliver.release import contract as C

#: (run, ix) pairs: 3 × none, 2 × ix 2, 1 × ix 1 — and a run of 200 (a two-byte varint) of ix 5
PINNED = [([0, 0, 0, 2, 2, 1], bytes([3, 0, 2, 2, 1, 1])),
          ([5] * 200, bytes([0xC8, 0x01, 5]))]


@pytest.mark.parametrize("cells,blob", PINNED)
def test_a_row_is_runs_of_length_and_ix_and_decodes_back(cells, blob):
    assert BG.encode_row(cells) == blob
    assert BG.decode_row(blob, len(cells)) == cells


def test_a_row_that_does_not_cover_the_grid_is_refused():
    with pytest.raises(ValueError, match="covers 6 cells, not 7"):
        BG.decode_row(PINNED[0][1], 7)
    with pytest.raises(ValueError, match="inside a run"):
        BG.decode_row(bytes([3]), 3)


def _groups(rows):
    import geopandas as gpd
    from shapely.geometry import box

    return gpd.GeoDataFrame({"basin_id": [r[0] for r in rows], "WATERSHED_GROUP_NAME": [r[1] for r in rows]},
                            geometry=[box(*r[2]) for r in rows], crs=4326)


def test_names_are_the_layers_own_in_basin_id_order_and_a_nameless_group_is_refused():
    g = _groups([("LFRA", "Lower Fraser", (0, 0, 1, 1)), ("CHWK", "Chilliwack River", (1, 0, 2, 1))])
    # as sourced: no "watershed" added, nothing re-cased
    assert BG.names(g) == [(1, "CHWK", "Chilliwack River"), (2, "LFRA", "Lower Fraser")]
    with pytest.raises(ValueError, match="no WATERSHED_GROUP_NAME"):
        BG.names(_groups([("X", " ", (0, 0, 1, 1))]))


SMALL = {"w": 0.0, "s": 0.0, "dlon": 0.5, "dlat": 0.5, "cols": 6, "rows": 2}


def _schema(db):
    ddl = (Path(BG.__file__).with_name("schema.sql")).read_text()
    for t in ("basin", "basin_frame", "basin_grid"):
        start = ddl.index(f"CREATE TABLE {t} ")
        db.execute(ddl[start:ddl.index(";", start)])


def test_each_cell_is_the_group_holding_its_centre_and_none_is_zero():
    # CHWK west of lon 1, LFRA from 1 to 2; nothing past lon 2 (the sea)
    g = _groups([("LFRA", "Lower Fraser", (1, 0, 2, 1)), ("CHWK", "Chilliwack River", (0, 0, 1, 1))])
    grid = BG.cells(g, {"CHWK": 1, "LFRA": 2}, SMALL)
    assert grid.tolist() == [[1, 1, 2, 2, 0, 0], [1, 1, 2, 2, 0, 0]]


def test_a_centre_on_a_shared_border_takes_the_lower_ix():
    g = _groups([("B", "Bee", (0.75, 0, 2, 1)), ("A", "Ay", (0, 0, 0.75, 1))])
    # the second cell's centre (0.75) is on the border of both
    assert BG.cells(g, {"A": 1, "B": 2}, SMALL)[0].tolist()[:3] == [1, 1, 2]


def test_write_fills_the_three_tables_the_release_copies():
    g = _groups([("LFRA", "Lower Fraser", (1, 0, 2, 1)), ("CHWK", "Chilliwack River", (0, 0, 1, 1))])
    db = sqlite3.connect(":memory:")
    _schema(db)
    BG.write(db, g, f=SMALL)
    assert db.execute("SELECT ix, basin_id, name FROM basin ORDER BY ix").fetchall() == [
        (1, "CHWK", "Chilliwack River"), (2, "LFRA", "Lower Fraser")]
    assert db.execute("SELECT w, s, dlon, dlat, cols, rows FROM basin_frame").fetchall() == [(0.0, 0.0, 0.5, 0.5, 6, 2)]
    rows = db.execute("SELECT row, b FROM basin_grid ORDER BY row").fetchall()
    assert [r for r, _b in rows] == [0, 1]
    assert BG.decode_row(rows[0][1], 6) == [1, 1, 2, 2, 0, 0]


def test_the_release_ships_them_in_conditions_as_the_contract_says():
    for t in ("basin", "basin_frame", "basin_grid"):
        assert t in RB.COND_TABLES
        assert t in C.FILES["conditions"]["tables"]
    assert C.FILES["conditions"]["tables"]["basin"] == ["ix", "basin_id", "name"]
    assert C.FILES["conditions"]["tables"]["basin_grid"] == ["row", "b"]
