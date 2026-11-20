"""T926 (every entry of `name_variants.json` finds its water) and T927 (search finds the book's boundary places): the
pipeline's halves, on data each test builds.

- `registry.attach_wbk_variants`: a whole-feature wbk name reaches its item even when the lake has no stream through it.
- `bundle.build.alias_rows`: an alias is new by the SEARCH key, so "SNOW LAKE *" is its own row.
- `graph.names._display_case`: "PETE'S" reads "Pete's", "O'CONNOR" still "O'Connor".
- `bundle.spans.cut_place_rows` / `release.build.core_cut_place`: every curated cut, once, at its host row's place, on its
  waters, with the part ends' words (the confidence clause off) and whether the words name nothing of their own.
"""
from __future__ import annotations

import json
import sqlite3

from pipeline.atlas.graph.names import _display_case
from pipeline.atlas.registry import attach_wbk_variants
from pipeline.common.models import NameRecord, RegistryItem
from pipeline.deliver.bundle.build import alias_rows
from pipeline.deliver.bundle.spans import cut_place_rows
from pipeline.deliver.release import build as B


def test_t926_a_lone_letter_after_an_apostrophe_is_lower_case():
    assert _display_case("PETE'S POND") == "Pete's Pond"
    assert _display_case("SAM'S FOLLY LAKE") == "Sam's Folly Lake"
    assert _display_case("O'CONNOR RIVER") == "O'Connor River"          # a name going on keeps its capital
    assert _display_case("D'ARCY CREEK") == "D'Arcy Creek"
    assert _display_case("Pete's Pond") == "Pete's Pond"                # already cased: untouched


def _lake(iid: str, name: str, variants=(), aliases=()) -> RegistryItem:
    return RegistryItem(id=iid, name=name, kind="lake", variants=tuple(variants),
                        name_sources=(NameRecord(name=name, source="gazette", label="BC gazette", official=True),),
                        aliases=tuple(aliases))


def test_t926_every_wbk_name_reaches_its_item_and_nothing_else_moves():
    reg = {"wbk:1": _lake("wbk:1", "Lilypad Lake", ["Lilypad Lake"]),
           "wbk:2": _lake("wbk:2", "Teanook Lake", ["Teanook Lake", "TEANOOK L."]),
           "gnis:9": _lake("gnis:9", "Big Lake", aliases=["wbk:3"])}
    nv = [{"target": {"wbks": ["1"]}, "names": [{"name": "LAKE #905", "source": "stocking"},
                                                 {"name": "SCOTT #2", "source": "stocking"}]},
          {"target": {"wbks": ["2"]}, "names": [{"name": "TEANOOK L.", "source": "bathymetry"}]},
          {"target": {"wbks": ["3"]}, "names": [{"name": "BIG L.", "source": "bathymetry"}]},       # absorbed id
          {"target": {"wbks": ["4"]}, "names": [{"name": "Nowhere", "source": "stocking"}]},       # no such item
          {"target": {"wbks": ["1"], "reach": {"from_m": 0, "to_m": 1}},                          # a stream's
           "names": [{"name": "Reach Only", "source": "regulation"}]}]
    out = attach_wbk_variants(dict(reg), nv)
    a = out["wbk:1"]
    assert a.name == "Lilypad Lake" and a.kind == "lake"
    assert a.variants == ("LAKE #905", "Lilypad Lake", "SCOTT #2")
    assert [(r.name, r.source) for r in a.name_sources] == [
        ("Lilypad Lake", "gazette"), ("LAKE #905", "stocking"), ("SCOTT #2", "stocking")]
    assert out["wbk:2"] is reg["wbk:2"]                                  # held already: untouched
    assert "BIG L." in out["gnis:9"].variants
    assert set(out) == set(reg)
    assert attach_wbk_variants(dict(out), nv) == out                     # idempotent


def test_t926_an_alias_is_new_by_the_search_key():
    items = [{"id": "wbk:1", "name": "Iver Lake",
              "variants": ["IVER LAKE", "SNOW LAKE", "SNOW LAKE *", "Snow Lake", ".", "Pigeon Lake # 2"]}]
    assert [a for _, a, _ in alias_rows(items)] == ["SNOW LAKE", "SNOW LAKE *", "Pigeon Lake # 2"]


def test_t927_cut_place_rows_are_the_curated_cuts_at_their_host_row():
    resolved = [
        {"split_id": "a__falls", "blk": "200", "route_measure": 1500.7, "source": "curated"},
        {"split_id": "a__falls", "blk": "100", "route_measure": 900.2, "source": "curated",
         "concern": "braid cut across the side channel"},
        {"split_id": "gauge__08XX001", "blk": "200", "route_measure": 10.0, "source": "gauge"},
        {"split_id": "length:x", "blk": "200", "route_measure": 20.0, "source": "length"},
        {"split_id": "b__bridge", "blk": "-2400000001", "route_measure": 33.9, "source": "curated"}]
    assert cut_place_rows(resolved) == [("a__falls", 200, 1500), ("b__bridge", -2400000001, 33)]


def test_t927_cut_words_and_generic():
    assert B.cut_words("Ten Mile Pool (100 m upstream), approximate location (low confidence)") == \
        "Ten Mile Pool (100 m upstream)"
    assert B.cut_words("Greendale Trestle") == "Greendale Trestle"
    assert B.cut_generic("lower falls") and B.cut_generic("tidal boundary (DFO Subarea 3-18)")
    assert not B.cut_generic("Greendale Trestle") and not B.cut_generic("Hwy 19 bridge")
    assert not B.cut_generic("fishing boundary signs at Ten Mile Pool (100 m upstream)")


def _cut_db():
    con = sqlite3.connect(":memory:")
    con.execute("ATTACH ':memory:' AS b")
    con.executescript("""
        CREATE TABLE main.item (ord INTEGER PRIMARY KEY, item_id TEXT, kind TEXT);
        CREATE TABLE main.item_section (sid INTEGER, ord INTEGER);
        CREATE TABLE b.cut_place (split_id TEXT PRIMARY KEY, blk INTEGER, m INTEGER);
        CREATE TABLE b.split (split_id TEXT PRIMARY KEY, name TEXT, kind TEXT, at TEXT, official_name TEXT,
                              same_place_as TEXT, lake_id TEXT, confidence TEXT);
        CREATE TABLE b.part_mark (ord INTEGER, mark_ix INTEGER, lon REAL, lat REAL, label TEXT, down_part INTEGER,
                                  up_parts TEXT, confidence TEXT, alt TEXT, token TEXT);
        CREATE TABLE b.section_measure (sid INTEGER PRIMARY KEY, blk INTEGER, from_m INTEGER, to_m INTEGER);
        INSERT INTO main.item VALUES (1, 'gnis:1', 'stream'), (2, 'gnis:2', 'stream'), (3, 'wbk:9', 'lake');
        INSERT INTO main.item_section VALUES (10, 2), (11, 3);
        INSERT INTO b.section_measure VALUES (10, 77, 0, 500), (11, 88, 0, 100);
        INSERT INTO b.cut_place VALUES ('a__pool', 55, 13632), ('b__falls', 77, 120);
        INSERT INTO b.split VALUES
          ('a__pool', 'fishing boundary signs at Ten Mile Pool (100 m upstream)', 'point', '[["gnis:1", 13.63]]',
           NULL, NULL, NULL, 'low'),
          ('b__falls', 'lower falls', 'point', '[]', NULL, NULL, NULL, NULL);
        INSERT INTO b.part_mark VALUES (1, 0, -127.1924531, 51.7741559, NULL, 0, '[1]', 'low', NULL, 'a__pool');
    """)
    return con


def test_t927_core_cut_place_names_each_place_on_its_waters():
    con = _cut_db()
    ends = {"a__pool": ["Ten Mile Pool (100 m upstream), approximate location (low confidence)",
                        "fishing boundary signs at Ten Mile Pool (100 m upstream)", "to:the pool"]}
    B.core_cut_place(con, ends)
    rows = con.execute("SELECT ord, words, alt, generic, blk, m, lon, lat FROM cut_place ORDER BY ord").fetchall()
    assert rows == [
        (1, "Ten Mile Pool (100 m upstream)", json.dumps(["fishing boundary signs at Ten Mile Pool (100 m upstream)"]),
         0, 55, 13632, -127192453, 51774156),
        # no main-stem position: the stream whose section holds the place; no ends words: the split's own; no mark
        (2, "lower falls", None, 1, 77, 120, None, None)]


def test_t927_a_bundle_without_cut_place_gives_no_table():
    con = sqlite3.connect(":memory:")
    con.execute("ATTACH ':memory:' AS b")
    con.execute("CREATE TABLE main.item (ord INTEGER PRIMARY KEY, item_id TEXT, kind TEXT)")
    B.core_cut_place(con, {})
    assert con.execute("SELECT 1 FROM sqlite_master WHERE name = 'cut_place'").fetchone() is None
