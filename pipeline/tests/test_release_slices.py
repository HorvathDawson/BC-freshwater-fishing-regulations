"""T256 item 1: THE ANSWER SLICES (`pipeline/deliver/release/slices.py`) — per region, a subset of core / water that is
COMPLETE for what it covers, so the app may read a covered key from it and ask the whole file for anything else.

Synthetic: the coverage closure (a water's exit and continuation, its parts), the str cut, the sid bitmap, and the two
refusals. On the live release (a side set's `<UI_EXPORT_DIR>/release` when set; fails when none is on disk): for two
regions, every covered water's rows in every sliced table are EXACTLY the whole file's, and a covered sid's water is the
whole file's.
"""
from __future__ import annotations

import gzip
import json
import sqlite3
from pathlib import Path

import pytest

from pipeline.tests import under_test

from pipeline.deliver.release import shards as SH
from pipeline.deliver.release import slices as SL



def _core(p: Path) -> Path:
    c = sqlite3.connect(p)
    c.executescript("""
        CREATE TABLE item (ord INTEGER PRIMARY KEY, item_id TEXT, name TEXT, kind TEXT, part_of TEXT, area_ha REAL,
                           name_key TEXT, size INTEGER, gauged INTEGER, has_rows INTEGER, pieces INTEGER);
        CREATE TABLE item_section (sid INTEGER NOT NULL, ord INTEGER NOT NULL, PRIMARY KEY (sid, ord)) WITHOUT ROWID;
        CREATE TABLE item_near (ord INTEGER NOT NULL, place_id INTEGER NOT NULL, ckm INTEGER NOT NULL,
                                PRIMARY KEY (ord, place_id)) WITHOUT ROWID;
        CREATE TABLE item_area (ord INTEGER PRIMARY KEY, regions TEXT NOT NULL, mus TEXT NOT NULL) WITHOUT ROWID;
        CREATE TABLE name_source (item_id TEXT NOT NULL, name TEXT NOT NULL, src TEXT NOT NULL, label TEXT NOT NULL,
                                  PRIMARY KEY (item_id, name)) WITHOUT ROWID;
        CREATE TABLE place (place_id INTEGER PRIMARY KEY, osm TEXT, name TEXT, kind TEXT, pop INTEGER, lat REAL, lon REAL);
        CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT NOT NULL) WITHOUT ROWID;
        INSERT INTO meta VALUES ('format', 'core/1'), ('section_handles', 'h');
        -- 0: Alpha Creek (R2, sids 10, 11) flows into 1: Big River (R3 only, sid 4097); 2: Big Lake (R2, sid 20) has a
        -- part 3: Big Lake north (R3, sid 4098); 4: Cut Creek (R2, sid 30) goes on as 5: Vedder (R3, sid 4099);
        -- 6: Far Creek (R3, sid 4100) — nothing in R2 names it
        INSERT INTO item VALUES (0, 'gnis:0', 'Alpha Creek', 'stream', NULL, NULL, NULL, 1, 0, 1, 2),
            (1, 'gnis:1', 'Big River', 'stream', NULL, NULL, NULL, 5, 1, 1, 1),
            (2, 'wbk:2', 'Big Lake', 'lake', NULL, 50, NULL, NULL, 0, 1, 1),
            (3, 'wbk:3', 'Big Lake north', 'lake', 'wbk:2', 10, NULL, NULL, 0, 1, 1),
            (4, 'gnis:4', 'Cut Creek', 'stream', NULL, NULL, NULL, 1, 0, 0, 1),
            (5, 'gnis:5', 'Vedder River', 'stream', NULL, NULL, NULL, 4, 0, 1, 1),
            (6, 'gnis:6', 'Far Creek', 'stream', NULL, NULL, NULL, 1, 0, 0, 1);
        INSERT INTO item_section VALUES (10, 0), (11, 0), (4097, 1), (20, 2), (4098, 3), (30, 4), (4099, 5), (4100, 6);
        INSERT INTO item_near VALUES (0, 7, 100), (6, 8, 200);
        INSERT INTO item_area VALUES (0, '2', '2-8'), (6, '3', '3-1');
        INSERT INTO name_source VALUES ('gnis:0', 'Alpha', 'osm', 'OSM'), ('gnis:6', 'Far', 'osm', 'OSM');
        INSERT INTO place VALUES (7, 'n7', 'Hope', 'town', 1, 49.3, -121.4), (8, 'n8', 'Lytton', 'village', 1, 50.2, -121.5);
    """)
    c.commit()
    c.close()
    return p


def _water(p: Path, loose_str: bool = False) -> Path:
    c = sqlite3.connect(p)
    c.executescript("""
        CREATE TABLE item (item_ord INTEGER PRIMARY KEY, item_id INTEGER, shown INTEGER, picker INTEGER, unresolved TEXT,
                           name TEXT, kind INTEGER, sections INTEGER, outside_bc INTEGER, entries TEXT, steelhead INTEGER,
                           x BLOB);
        CREATE TABLE part (item_ord INTEGER NOT NULL, part_ix INTEGER NOT NULL, akey INTEGER, ord INTEGER, label_s INTEGER,
                           short_s INTEGER, runs_s INTEGER, place_s INTEGER, km REAL,
                           closed_all_year INTEGER, paper_licence TEXT, lo_s INTEGER, hi_s INTEGER, run_s INTEGER,
                           xp INTEGER NOT NULL, sections INTEGER NOT NULL,
                           xruns INTEGER NOT NULL, grp INTEGER NOT NULL DEFAULT 0, run INTEGER NOT NULL DEFAULT 0,
                           PRIMARY KEY (item_ord, part_ix)) WITHOUT ROWID;
        CREATE TABLE part_section (ord INTEGER NOT NULL, sid INTEGER NOT NULL, part_ix INTEGER NOT NULL,
                                   PRIMARY KEY (ord, sid)) WITHOUT ROWID;
        CREATE TABLE part_mark (ord INTEGER NOT NULL, mark_ix INTEGER NOT NULL, lon INTEGER, lat INTEGER, label TEXT,
                                down_part INTEGER, up_parts TEXT, confidence TEXT,
                                alt TEXT, PRIMARY KEY (ord, mark_ix)) WITHOUT ROWID;
        CREATE TABLE water_exit (ord INTEGER PRIMARY KEY, down_ord INTEGER NOT NULL, down_part INTEGER);
        CREATE TABLE water_continue (ord INTEGER PRIMARY KEY, part INTEGER NOT NULL, next_ord INTEGER NOT NULL UNIQUE,
                                     next_part INTEGER NOT NULL);
        CREATE TABLE xpart (id INTEGER PRIMARY KEY, j BLOB NOT NULL);
        CREATE TABLE picker (id INTEGER PRIMARY KEY, j BLOB NOT NULL);
        CREATE TABLE str (id INTEGER PRIMARY KEY, text TEXT NOT NULL);
        CREATE TABLE akey (akey INTEGER PRIMARY KEY, ruleset INTEGER, licensing_set INTEGER, steelhead_water INTEGER,
                           steelhead INTEGER, steelhead_rules INTEGER, province_except INTEGER, home_region INTEGER,
                           kind INTEGER, tidal INTEGER, seg INTEGER, mom INTEGER, nseg INTEGER);
        CREATE TABLE akey_region (akey INTEGER NOT NULL, region TEXT NOT NULL, PRIMARY KEY (akey, region)) WITHOUT ROWID;
        CREATE TABLE place (place INTEGER PRIMARY KEY, regions TEXT NOT NULL, label INTEGER NOT NULL, names TEXT NOT NULL);
        CREATE TABLE ruleset (set_id INTEGER PRIMARY KEY, j BLOB NOT NULL);
        CREATE TABLE seglist (seg INTEGER PRIMARY KEY, starts TEXT NOT NULL);
        CREATE TABLE segmom (mom INTEGER PRIMARY KEY, ix TEXT NOT NULL);
        CREATE TABLE rule (rix INTEGER PRIMARY KEY, j BLOB NOT NULL);
        CREATE TABLE display_rule (id INTEGER PRIMARY KEY, j BLOB NOT NULL);
        CREATE TABLE lic (lix INTEGER PRIMARY KEY, j BLOB NOT NULL);
        CREATE TABLE static (k TEXT PRIMARY KEY, j BLOB NOT NULL) WITHOUT ROWID;
        CREATE TABLE zdict (tbl TEXT PRIMARY KEY, d BLOB NOT NULL) WITHOUT ROWID;
        CREATE TABLE entry (e INTEGER PRIMARY KEY, id TEXT NOT NULL, j BLOB NOT NULL);
        CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT NOT NULL) WITHOUT ROWID;
        INSERT INTO meta VALUES ('format', 'water/1'), ('section_handles', 'h'), ('reach_digest', 'r');
        INSERT INTO str VALUES (100, 'gnis:0'), (101, 'gnis:1'), (102, 'wbk:2'), (103, 'wbk:3'), (104, 'gnis:4'),
            (105, 'gnis:5'), (106, 'gnis:6'), (110, 'stream'), (111, 'lake'), (120, 'Far Creek label'),
            (121, 'Alpha label'), (130, 'Region 2'), (131, '[]');
        INSERT INTO item VALUES (0, 100, 1, 1, NULL, 'Alpha Creek', 110, 2, 0, '[0]', NULL, NULL),
            (1, 101, 1, NULL, NULL, 'Big River', 110, 1, 0, '[1]', NULL, NULL),
            (2, 102, 1, NULL, NULL, 'Big Lake', 111, 1, 0, '[2]', NULL, NULL),
            (3, 103, 1, NULL, NULL, 'Big Lake north', 111, 1, 0, '[2]', NULL, NULL),
            (4, 104, 1, NULL, NULL, 'Cut Creek', 110, 1, 0, '[]', NULL, NULL),
            (5, 105, 1, NULL, NULL, 'Vedder River', 110, 1, 0, '[3]', NULL, NULL),
            (6, 106, 1, 2, NULL, 'Far Creek', 110, 1, 0, '[]', NULL, NULL);
        INSERT INTO part VALUES (0, 0, 1, 0, 121, 121, 121, NULL, 1.5, 0, '[]', NULL, NULL, NULL, 1, 2, 0, 0, 0),
            (6, 0, 2, 0, 120, 120, 120, NULL, 2.0, 0, '[]', NULL, NULL, NULL, 2, 1, 0, 0, 0);
        INSERT INTO part_section VALUES (0, 10, 0), (0, 11, 0), (6, 4100, 0);
        INSERT INTO part_mark VALUES (0, 0, 1, 2, NULL, 0, '[]', NULL, NULL);
        INSERT INTO water_exit VALUES (0, 1, 0), (6, 1, 0);
        INSERT INTO water_continue VALUES (4, 0, 5, 0);
        INSERT INTO xpart VALUES (1, x'01'), (2, x'02');
        INSERT INTO picker VALUES (1, x'0a'), (2, x'0b');
        INSERT INTO akey VALUES (1, 1, NULL, 0, NULL, 0, 131, 131, 110, 0, 1, NULL, 1),
                                (2, 1, NULL, 0, NULL, 0, 131, 131, 110, 0, 1, NULL, 1);
        INSERT INTO akey_region VALUES (1, '2'), (2, '3');
        INSERT INTO place VALUES (0, '["2"]', 130, '{}');
        INSERT INTO ruleset VALUES (1, x'00'); INSERT INTO seglist VALUES (1, '[0]'); INSERT INTO rule VALUES (0, x'00');
        INSERT INTO display_rule VALUES (0, x'00'); INSERT INTO lic VALUES (0, x'00');
        INSERT INTO static VALUES ('answers.glossary', x'00'); INSERT INTO zdict VALUES ('rule', x'00');
        INSERT INTO entry VALUES (0, 'e', x'00');
    """)
    if loose_str:
        c.execute("INSERT INTO str VALUES (999, 'named by a blob only')")
    c.commit()
    c.close()
    return p


SIDS = {10: ["2"], 11: ["2"], 20: ["2"], 30: ["2"], 31: ["2"], 4097: ["3"], 4098: ["3"], 4099: ["3"], 4100: ["3"]}


def _rows(p: Path, sql: str) -> list:
    c = sqlite3.connect(p)
    try:
        return c.execute(sql).fetchall()
    finally:
        c.close()


def test_a_slice_covers_its_regions_waters_whole_and_what_they_name(tmp_path):
    """Region 2's slice holds its own waters (0, 2, 4), the river Alpha flows into (1, R3), Big Lake's part (3, R3) and
    the river Cut Creek goes on as (5, R3) — each with every row of its own; Far Creek (6, R3), which nothing in R2
    names, is not held. MUTATION: dropping the exit / continuation / part_of closure drops 1 / 5 / 3."""
    core, water = _core(tmp_path / "core.sqlite"), _water(tmp_path / "water.sqlite")
    out = SL.build_slices(tmp_path, core, water, SIDS)
    c2, w2 = out["core-2"], out["water-2"]
    assert _rows(c2, "SELECT ord FROM item ORDER BY ord") == [(0,), (1,), (2,), (3,), (4,), (5,)]
    assert _rows(w2, "SELECT item_ord FROM item ORDER BY item_ord") == [(0,), (1,), (2,), (3,), (4,), (5,)]
    # every row of a covered water, the whole file's own
    assert _rows(c2, "SELECT * FROM item_section ORDER BY sid") == \
        [r for r in _rows(core, "SELECT * FROM item_section ORDER BY sid") if r[1] != 6]
    assert _rows(w2, "SELECT * FROM part ORDER BY item_ord") == _rows(water, "SELECT * FROM part WHERE item_ord = 0")
    assert _rows(w2, "SELECT * FROM water_exit") == [(0, 1, 0)]
    assert _rows(w2, "SELECT * FROM water_continue") == [(4, 0, 5, 0)]
    assert _rows(c2, "SELECT item_id FROM name_source") == [("gnis:0",)]
    # what a kept row names: its xpart, picker, strings and towns; nothing else
    assert _rows(w2, "SELECT id FROM xpart") == [(1,)] and _rows(w2, "SELECT id FROM picker") == [(1,)]
    assert (106,) not in _rows(w2, "SELECT id FROM str") and (120,) not in _rows(w2, "SELECT id FROM str")
    assert {(100,), (110,), (121,)} <= set(_rows(w2, "SELECT id FROM str"))
    assert _rows(c2, "SELECT place_id FROM place") == [(7,)]
    # (T370) the region-wide tables are in no slice: `water-shared` holds them whole, with the strings they name
    sh = out[SL.WATER_SHARED]
    assert _rows(sh, "SELECT akey FROM akey") == [(1,), (2,)] and _rows(sh, "SELECT count(*) FROM akey_region") == [(2,)]
    assert {(130,), (131,)} <= set(_rows(sh, "SELECT id FROM str"))
    tables = {t for (t,) in _rows(w2, "SELECT name FROM sqlite_master WHERE type = 'table'")}
    assert tables.isdisjoint(SL.WATER_SHARED_TABLES)
    assert {t for (t,) in _rows(sh, "SELECT name FROM sqlite_master WHERE type = 'table'")} == \
        {*SL.WATER_SHARED_TABLES, "str", "meta"}
    assert dict(_rows(sh, "SELECT k, v FROM meta"))["format"] == SL.WATER_SHARED_FORMAT
    assert "region" not in dict(_rows(sh, "SELECT k, v FROM meta"))
    # not held: what no app read asks
    assert "entry" not in tables and "runs" not in tables and "lset" not in tables
    meta = dict(_rows(w2, "SELECT k, v FROM meta"))
    assert (meta["format"], meta["slice_of"], meta["region"], meta["whole_format"], meta["reach_digest"]) == \
        (SL.WATER_SLICE_FORMAT, "water", "2", "water/1", "r")
    assert dict(_rows(c2, "SELECT k, v FROM meta"))["format"] == SL.CORE_SLICE_FORMAT


def test_the_sid_bitmap_is_every_sid_of_the_region_and_nothing_else(tmp_path):
    """`cover_sid`: bit i of block b is sid (b << 12) | i — R2's 10, 11, 20, 30 and the unnamed 31; not Big River's
    4097 although its rows are held (it is covered as a WATER, its sids are R3's)."""
    out = SL.build_slices(tmp_path, _core(tmp_path / "core.sqlite"), _water(tmp_path / "water.sqlite"), SIDS)
    got = set()
    for blk, b in _rows(out["core-2"], "SELECT block, b FROM cover_sid"):
        got |= {(blk << SL.BLOCK_BITS) | i for i in range(SL.BLOCK) if b[i >> 3] >> (i & 7) & 1}
    assert got == {10, 11, 20, 30, 31}


def test_a_str_only_a_blob_names_and_a_section_in_two_waters_are_refused(tmp_path):
    core = _core(tmp_path / "core.sqlite")
    with pytest.raises(SL.SliceError, match="str ids no column names"):
        SL.build_slices(tmp_path, core, _water(tmp_path / "w1.sqlite", loose_str=True), SIDS)
    c = sqlite3.connect(core)
    c.execute("INSERT INTO item_section VALUES (10, 6)")
    c.commit()
    c.close()
    with pytest.raises(SL.SliceError, match="two waters"):
        SL.build_slices(tmp_path, core, _water(tmp_path / "w2.sqlite"), SIDS)


def _live(d):
    """(release root, manifest) of the release folder `d` under test, or None when it has no sliced files."""
    root = d.parent
    m = json.loads((d / "manifest.json").read_text()) if (d / "manifest.json").exists() else None
    if not m or not all(k in m["files"] for k in ("core", "water", "regs-sid")):
        return None
    return root, m


@pytest.mark.needs_bundle
def test_on_the_live_release_a_slice_is_the_whole_file_for_what_it_covers(tmp_path, request):
    """For Regions 2 and 7B (the small and the far): every covered water's rows in every sliced table equal the whole
    file's; every covered sid's water (or none) is the whole core's; every str a kept row names is the whole's."""
    live = _live(under_test.release(request))
    assert live is not None, "the live release has no core / water / regs-sid (an older release: re-run pipeline.deliver)"
    root, m = live
    paths = {}
    for n in ("core", "water", "regs-sid"):
        p = tmp_path / f"{n}.sqlite"
        p.write_bytes(gzip.decompress((root / m["files"][n]["url"]).read_bytes()))
        paths[n] = p
    st = sqlite3.connect(":memory:")
    st.execute("ATTACH ? AS s", (str(paths["regs-sid"]),))
    st.execute("ATTACH ? AS w", (str(paths["water"]),))
    st.execute("CREATE TABLE section_place AS SELECT * FROM s.section_place")
    st.execute("CREATE TABLE place AS SELECT place, regions FROM w.place")
    sid_regions = SH.sid_regions(st)
    c = sqlite3.connect(paths["core"])
    w = sqlite3.connect(paths["water"])
    sh = SL.water_shared(paths["water"], tmp_path / "water-shared.sqlite")      # (T370) the region-wide tables
    for t in SL.WATER_SHARED_TABLES:
        assert _rows(sh, f"SELECT * FROM {t} ORDER BY 1") == w.execute(f"SELECT * FROM {t} ORDER BY 1").fetchall(), t
    sh_strs = dict(_rows(sh, "SELECT id, text FROM str"))
    sh_used = {i for t, col in SL.STR_COLUMNS if t in SL.WATER_SHARED_TABLES
               for (i,) in _rows(sh, f"SELECT {col} FROM {t} WHERE {col} IS NOT NULL")}
    assert sh_used <= set(sh_strs)
    for r in ("2", "7b"):
        sids = SL.region_sids(sid_regions, r)
        ords = SL.covered_ords(c, w, sids)
        cs = SL.core_slice(paths["core"], tmp_path / f"core-{r}.sqlite", r, ords, sids)
        ws = SL.water_slice(paths["water"], tmp_path / f"water-{r}.sqlite", r, ords)
        q = ",".join(map(str, sorted(ords)))
        for t, col in (("item", "ord"), ("item_section", "ord"), ("item_near", "ord"), ("item_area", "ord")):
            assert _rows(cs, f"SELECT * FROM {t} ORDER BY 1, 2") == \
                c.execute(f"SELECT * FROM {t} WHERE {col} IN ({q}) ORDER BY 1, 2").fetchall(), (r, t)
        for t, col in (("item", "item_ord"), ("part", "item_ord"), ("part_section", "ord"), ("part_mark", "ord"),
                       ("water_exit", "ord")):
            assert _rows(ws, f"SELECT * FROM {t} ORDER BY 1, 2") == \
                w.execute(f"SELECT * FROM {t} WHERE {col} IN ({q}) ORDER BY 1, 2").fetchall(), (r, t)
        whole_sid = dict(c.execute("SELECT sid, ord FROM item_section"))
        slice_sid = dict(_rows(cs, "SELECT sid, ord FROM item_section"))
        assert all(slice_sid.get(s) == whole_sid.get(s) for s in sids), r
        strs = dict(_rows(ws, "SELECT id, text FROM str"))
        used = {i for t, col in SL.STR_COLUMNS if t in SL.WATER_SLICE_TABLES
                for (i,) in _rows(ws, f"SELECT {col} FROM {t} WHERE {col} IS NOT NULL")}
        assert used <= set(strs) and all(strs[i] == w.execute("SELECT text FROM str WHERE id = ?", (i,)).fetchone()[0]
                                         for i in list(used)[:2000])
