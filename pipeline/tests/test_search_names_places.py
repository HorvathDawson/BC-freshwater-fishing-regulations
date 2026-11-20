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


def test_t926_a_name_whose_matcher_key_names_another_water_is_not_attached_and_is_listed():
    """(T926 review) The matchers' name index (`matcher.build_name_index`) must never see a key that named one water come
    to name two: "SCOTT #2" stays off Lilypad Lake while Scott #2 Lake answers to "scott 2" — listed, not dropped
    silently. A key the water already answers to ("LILYPAD LAKE" beside "Lilypad Lake") is no change, and attaches."""
    import pytest

    from pipeline.regs.matching.matcher import build_name_index
    reg = {"wbk:1": _lake("wbk:1", "Lilypad Lake", ["Lilypad Lake"]),
           "wbk:2": _lake("wbk:2", "Scott #2", ["Scott #2"])}
    before = build_name_index(reg)
    rep: dict = {}
    nv = [{"target": {"wbks": ["1", "404"]}, "names": [{"name": "SCOTT #2", "source": "stocking"},
                                                        {"name": "LILYPAD LAKE", "source": "stocking"},
                                                        {"name": "LAKE #905", "source": "stocking"}]}]
    out = attach_wbk_variants(dict(reg), nv, rep)
    assert out["wbk:1"].variants == ("LAKE #905", "LILYPAD LAKE", "Lilypad Lake")
    assert rep == {"skipped": [{"item": "wbk:1", "name": "SCOTT #2", "key": "scott 2", "names": ["wbk:2"]}],
                   "no_item": ["wbk:404"]}
    after = build_name_index(out)
    assert all(set(after[k]) == set(v) for k, v in before.items())          # no key gains or loses a water
    assert after["lake 905"] == ["wbk:1"]
    with pytest.raises(ValueError, match="no NameSource"):
        attach_wbk_variants(dict(reg), [{"target": {"wbks": ["1"]}, "names": [{"name": "X", "source": "rumour"}]}])


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
    assert B.cut_words("  Ten Mile Pool  (100 m upstream) ") == "Ten Mile Pool (100 m upstream)"
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
        INSERT INTO b.cut_place VALUES ('a__pool', 55, 13632), ('b__falls', 77, 120), ('c__slope', 99, 5);
        INSERT INTO b.split VALUES
          ('a__pool', 'fishing boundary signs at Ten Mile Pool (100 m upstream)', 'point', '[["gnis:1", 13.63]]',
           NULL, NULL, NULL, 'low'),
          ('b__falls', 'lower falls', 'point', '[]', NULL, NULL, NULL, NULL),
          ('c__slope', 'foot of the east slope', 'point', '[]', NULL, NULL, NULL, NULL);
        INSERT INTO b.part_mark VALUES (1, 0, -127.1924531, 51.7741559, NULL, 0, '[1]', 'low', NULL, 'a__pool');
    """)
    return con


def test_t927_core_cut_place_names_each_place_on_its_waters():
    con = _cut_db()
    ends = {"a__pool": ["Ten Mile Pool (100 m upstream)",
                        "fishing boundary signs at Ten Mile Pool (100 m upstream)", "to:the pool"]}
    # a cut no water holds is not a row, and is COUNTED AND SAID (returned + printed), never dropped silently
    assert B.core_cut_place(con, ends) == ["c__slope"]
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


def test_t926_a_number_is_never_the_shown_name_when_a_lettered_one_exists():
    """(T926, owner 2026-11-20 ~06:55 "Never show a number name") The stocking number "5566" outranked the bathymetry
    sheet's "PIGEON LAKE #2" (stocking is the better source): a name with no letter ("5566", "#15") is never shown while
    the water has a lettered name — the one most of its names agree on, stored exactly as given (the bundle and tiles case
    it) — and stays a search name. The matcher's keys are the same set; a water with only numbers keeps its number."""
    import tempfile
    from pathlib import Path

    from pipeline.atlas.registry.build import is_number_name, show_lettered_names
    from pipeline.atlas.registry.io import write_registry
    from pipeline.deliver.bundle.build import name_source_rows
    from pipeline.regs.matching.matcher import build_name_index
    assert is_number_name("5566") and is_number_name("#15") and is_number_name(" 5566-2 ")
    assert not is_number_name("Lake 5567") and not is_number_name("Pigeon Lake #2") and not is_number_name("")
    src = lambda n, s: NameRecord(name=n, source=s, label=s)              # noqa: E731
    reg = {"wbk:1": RegistryItem(id="wbk:1", name="5566", kind="lake",
                                 variants=("5566", "CONTROL (PIGEON)", "PIGEON LAKE #2", "Pigeon Lake # 2"),
                                 name_sources=(src("5566", "stocking"), src("CONTROL (PIGEON)", "stocking"),
                                               src("PIGEON LAKE #2", "stocking"), src("Pigeon Lake # 2", "marker"))),
           "wbk:2": RegistryItem(id="wbk:2", name="3135", kind="lake", variants=("3135",),
                                 name_sources=(src("3135", "stocking"),)),
           "wbk:3": _lake("wbk:3", "Lilypad Lake", ["Lilypad Lake", "LAKE #905"]),
           # a lettered name that is a variant no record names: shown, with the honest "source not recorded" record
           "wbk:4": RegistryItem(id="wbk:4", name="#825", kind="lake", variants=("#825", "HEIDI LAKE"),
                                 name_sources=(src("#825", "stocking"),))}
    before = build_name_index(reg)
    rep: list = []
    out = show_lettered_names(dict(reg), rep)
    a = out["wbk:1"]
    assert a.name == "PIGEON LAKE #2" and a.variants == reg["wbk:1"].variants                 # the number stays one
    assert a.name_sources[0].name == "PIGEON LAKE #2" and a.name_sources[0].source == "stocking"
    assert {r.name for r in a.name_sources} == {"5566", "CONTROL (PIGEON)", "PIGEON LAKE #2", "Pigeon Lake # 2"}
    assert out["wbk:2"] is reg["wbk:2"] and out["wbk:3"] is reg["wbk:3"]
    h = out["wbk:4"]
    assert h.name == "HEIDI LAKE" and (h.name_sources[0].source, h.name_sources[0].label) == (
        "alias", "Alternate name (source not recorded)")
    assert rep == [{"item": "wbk:1", "was": "5566", "now": "PIGEON LAKE #2"},
                   {"item": "wbk:4", "was": "#825", "now": "HEIDI LAKE"}]
    after = build_name_index(out)
    assert {k: set(v) for k, v in after.items()} == {k: set(v) for k, v in before.items()}
    assert show_lettered_names(dict(out), []) == out                      # idempotent
    # the bundle's name-source rows take the "not recorded" record as any other
    with tempfile.TemporaryDirectory() as t:
        write_registry(out, Path(t) / "r.json")
        items = json.loads((Path(t) / "r.json").read_text())["items"]
    assert name_source_rows([i for i in items if i["id"] == "wbk:4"], set()) == [
        ("wbk:4", "HEIDI LAKE", "alias", "Alternate name (source not recorded)")]


def test_t926_a_shown_name_is_never_recased_into_another_waters_key():
    """(T926 review) "TEANOOK L." is shown as given: the old casing step made it "Teanook Lake", a matcher key that names
    ANOTHER water (a second Teanook Lake), and a key may never come to name two waters."""
    from pipeline.atlas.registry.build import show_lettered_names
    from pipeline.regs.matching.matcher import build_name_index
    src = lambda n, s: NameRecord(name=n, source=s, label=s)              # noqa: E731
    reg = {"wbk:1": RegistryItem(id="wbk:1", name="3301", kind="lake", variants=("3301", "TEANOOK L."),
                                 name_sources=(src("3301", "stocking"), src("TEANOOK L.", "bathymetry"))),
           "wbk:2": _lake("wbk:2", "Teanook Lake", ["Teanook Lake"])}
    before = build_name_index(reg)
    out = show_lettered_names(dict(reg), [])
    assert out["wbk:1"].name == "TEANOOK L."
    after = build_name_index(out)
    assert {k: set(v) for k, v in after.items()} == {k: set(v) for k, v in before.items()}
    assert set(after["teanook lake"]) == {"wbk:2"}


def test_t926_the_last_name_step_is_one_helper_run_last_by_the_build_and_by_sidecars(tmp_path):
    """(T926 review) `finish_names` runs the wbk variants BEFORE the shown names (a lake whose only lettered name comes
    from the variants file gets it) and writes both reports; the atlas build and `pipeline.atlas.sidecars` both call it,
    neither runs the two passes on its own, and the build calls it after the mouth boundaries, right before the registry
    is written (a mouth id is slugged from the name it was made with)."""
    from pathlib import Path

    from pipeline.atlas.registry.build import SHOWN_NAME_REPORT, WBK_VARIANT_REPORT, finish_names
    reg = {"wbk:7": RegistryItem(id="wbk:7", name="3135", kind="lake", variants=("3135",),
                                 name_sources=(NameRecord(name="3135", source="stocking", label="s"),))}
    out = finish_names(reg, [{"target": {"wbks": ["7"]}, "names": [{"name": "MOON LAKE", "source": "stocking"}]}],
                       tmp_path, log=lambda *_: None)
    assert out["wbk:7"].name == "MOON LAKE" and "3135" in out["wbk:7"].variants
    assert json.loads((tmp_path / SHOWN_NAME_REPORT).read_text()) == [
        {"item": "wbk:7", "was": "3135", "now": "MOON LAKE"}]
    assert (tmp_path / WBK_VARIANT_REPORT).exists()
    root = Path(__file__).resolve().parents[2]
    build = (root / "pipeline/atlas/build.py").read_text()
    side = (root / "pipeline/atlas/sidecars.py").read_text()
    for src in (build, side):
        assert src.count("finish_names(registry,") == 1
        assert "attach_wbk_variants(registry" not in src and "show_lettered_names(registry" not in src
    at = build.index("finish_names(registry,")
    assert build.index("add_mouth_boundaries(registry") < at < build.index('write_registry(registry, out / "registry.json")')
    assert side.index("finish_names(registry,") < side.index('write_registry(registry, dest / "registry.json")')
