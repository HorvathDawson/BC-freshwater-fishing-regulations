"""(T388, owner 2026-11-04) `part_line`: each part's own stretch of stream, in the WATER file, so a named water's link
place is made and resolved without the `lines` file (pipeline/deliver/release/part_line.py).

No data: a fixture water of three parts — part 0 two touching sections, a river polygon and a side channel; part 1
on one blue line, with a lake between its own pieces; part 2 a lake — packed into runs, written into the water file,
and cut into the region slice the page fetches (slices.py). Mutation pins: the merge, the gap and the additive slice.
(T517: a part is one connected run, so there is no stretch column.)
"""
from __future__ import annotations

import sqlite3

from pipeline.deliver.release import build as B
from pipeline.deliver.release import contract as C
from pipeline.deliver.release import part_line as PL
from pipeline.deliver.release import slices as SL
from pipeline.tests.test_release_slices import SIDS, _core, _rows, _water

A, SIDE = 356364584, 356364590
ORD = 7
#: (ord, sid, part_ix) — the bundle's part_section
PARTS = [(ORD, 1, 0), (ORD, 2, 0), (ORD, 3, 0), (ORD, 4, 0),
         (ORD, 5, 1), (ORD, 6, 1), (ORD, 7, 1), (ORD, 8, 1), (ORD, 9, 1),
         (ORD, 10, 2)]
#: (sid, blk, from_m, to_m) — the bundle's section_measure (stream sections only: 3 is a polygon, 10 a lake)
MEASURES = [(1, A, 0, 100), (2, A, 100, 250), (4, SIDE, 40, 90),
            (5, A, 300, 500), (6, A, 500, 600), (7, A, 650, 700), (8, A, 800, 900), (9, A, 900, 900)]


def test_a_parts_sections_on_one_line_are_one_run_per_piece_and_a_lake_part_has_none():
    assert PL.runs(PARTS, MEASURES) == [
        (ORD, 0, A, 0, 250),          # sections 1 + 2 end to end: ONE run (the polygon 3 has no line)
        (ORD, 0, SIDE, 40, 90),       # a side channel: its own blue line, its own run
        (ORD, 1, A, 300, 600),        # 5 + 6
        (ORD, 1, A, 650, 700),        # 7, past a gap (a lake between two pieces of the part): another run
        (ORD, 1, A, 800, 900),        # 8 (9 spans no point and is left out)
    ]                                 # part 2, a lake: no rows


def test_mutations_merge_and_gap_each_change_the_rows():
    # touching sections merge (100 == 100); a 1 m gap does not (a second part keeps the water placed)
    two = PARTS[:2] + [(ORD, 5, 1)]
    gapped = [(1, A, 0, 100), (2, A, 101, 250)]
    assert PL.runs(two, gapped)[:2] == [(ORD, 0, A, 0, 100), (ORD, 0, A, 101, 250)]
    assert PL.runs(two, MEASURES)[:1] == [(ORD, 0, A, 0, 250)]


def test_a_water_of_one_part_has_no_rows():
    """Its links name the item alone (T369), so nothing would read them."""
    assert PL.runs([(ORD, 1, 0), (ORD, 2, 0), (ORD, 4, 0)], MEASURES) == []


def _bundle() -> sqlite3.Connection:
    b = sqlite3.connect(":memory:")
    b.execute("CREATE TABLE part_section (ord INTEGER, sid INTEGER, part_ix INTEGER, m INTEGER)")
    b.executemany("INSERT INTO part_section VALUES (?, ?, ?, NULL)", PARTS)
    return b


def test_the_water_file_gets_the_table_from_the_bundle_as_the_contract_says():
    assert C.FILES["water"]["tables"]["part_line"] == PL.COLUMNS
    assert all(C.FILES[f"water-{r}"]["tables"]["part_line"] == PL.COLUMNS for r in SL.SH.REGIONS)
    assert SL.WATER_SLICE_TABLES["part_line"] == "ord"
    con = sqlite3.connect(":memory:")
    assert PL.write(con, PL.from_bundle(_bundle(), MEASURES)) == 5
    assert [r[1] for r in con.execute("PRAGMA table_info(part_line)")] == PL.COLUMNS
    assert con.execute("SELECT * FROM part_line ORDER BY part_ix, blk, m0").fetchall() == PL.runs(PARTS, MEASURES)
    # the release build writes it into the water file (the water_extra step), from the section_measure read once
    import inspect
    src = inspect.getsource(B.build)
    assert "PART_LINE.from_bundle(b, sm)" in src and "PART_LINE.write(con, part_lines)" in src


def test_the_region_slice_holds_its_waters_runs_and_an_older_water_has_none(tmp_path):
    """Alpha Creek (ord 0, R2) and Far Creek (ord 6, R3): R2's slice holds Alpha's runs only — what the page there
    reads by ord. A water cut before T388 has no part_line, and neither do its slices (additive)."""
    core = _core(tmp_path / "core.sqlite")
    water = _water(tmp_path / "water.sqlite")
    c = sqlite3.connect(water)
    PL.write(c, [(0, 0, A, 0, 250), (6, 0, SIDE, 40, 90)])
    c.commit()
    c.close()
    out = SL.build_slices(tmp_path, core, water, SIDS)
    assert _rows(out["water-2"], "SELECT * FROM part_line") == [(0, 0, A, 0, 250)]
    assert _rows(out["water-3"], "SELECT * FROM part_line") == [(6, 0, SIDE, 40, 90)]
    old = tmp_path / "old"
    old.mkdir()
    out = SL.build_slices(old, _core(old / "core.sqlite"), _water(old / "water.sqlite"), SIDS)
    assert _rows(out["water-2"], "SELECT name FROM sqlite_master WHERE name = 'part_line'") == []
