"""THE BUNDLE'S DERIVED TABLES (DATAFLOW P2, `pipeline/deliver/bundle/derived.py`): rule_ix,
rule.closure_grade, rule_key + section_ruleset.key_ix, part + part_section. The constraints refuse
a bad value or a broken invariant (IntegrityError); the builder's proofs refuse what SQL cannot
state; the slow test holds the built bundle's parts to the grouping the export used to compute."""
from __future__ import annotations

import importlib
import os
import sqlite3
from pathlib import Path

import pytest

from pipeline.common.curated import GENERATED
from pipeline.deliver.bundle import derived
from pipeline.tests.bundle_fixture import finish, section_sets

SCHEMA = importlib.import_module("pipeline.deliver.bundle.build").SCHEMA


def test_the_schema_checks_are_the_enums():
    text = SCHEMA.read_text()
    v = derived.schema_check_values()
    assert f"province_except IN\n                                     ({v['province_except']})" in text
    assert f"kind            TEXT CHECK (kind IN ({v['kind']}))" in text
    assert f"closure_grade TEXT CHECK (closure_grade IN ({v['closure_grade']}))" in text


def _db():
    db = sqlite3.connect(":memory:")
    db.execute("PRAGMA foreign_keys = ON")
    db.executescript(SCHEMA.read_text())
    db.executemany("INSERT INTO item (ord, item_id, name, kind) VALUES (?,?,?,?)",
                   [(1, "gnis:1", "One River", "stream"), (2, "wbk:5", "Five Lake", "lake")])
    db.executemany("INSERT INTO item_section (ord, sid) VALUES (?,?)", [(1, 1), (1, 2), (2, 3)])
    return db


def test_a_tiny_bundle_derives_its_keys_and_parts():
    db = _db()
    section_sets(db, {1: 7, 2: 8, 3: 9, 4: 7})          # 4: an unnamed section
    finish(db)
    assert db.execute("SELECT key_ix, set_id, kind, sections, rep_sid FROM rule_key").fetchall() \
        == [(0, 7, "stream", 2, 1), (1, 8, "stream", 1, 2), (2, 9, "lake", 1, 3)]
    assert db.execute("SELECT ord, part_ix, set_id, key_ix, sections, rep_sid FROM part "
                      "ORDER BY ord, part_ix").fetchall() == [(1, 0, 7, 0, 1, 1), (1, 1, 8, 1, 1, 2),
                                                             (2, 0, 9, 2, 1, 3)]
    P = derived.parts(db)
    assert [p.part_ix for p in P["gnis:1"]] == [0, 1] and P["wbk:5"][0].kind == "lake"


@pytest.mark.parametrize("sql", [
    "INSERT INTO rule_key VALUES (5, 1, 1, 0, 'stream', 1, 1)",            # sw without sr
    "INSERT INTO rule_key VALUES (5, 1, 0, 0, 'pond', 1, 1)",              # not a water kind
    "INSERT INTO rule_key VALUES (5, 1, 0, 2, 'lake', 1, 1)",              # not a bool
    "INSERT INTO rule_key VALUES (5, 1, 0, 0, 'lake', 0, 1)",              # no sections
    "INSERT INTO section_ruleset (sid, set_id, key_ix) VALUES (9, 1, NULL)",
    "INSERT INTO section_ruleset (sid, set_id, key_ix) VALUES (9, 1, 404)",   # dangling key
    "INSERT INTO part VALUES (1, 0, 1, NULL, NULL, '', 0, NULL, 0, '', 0, 1, 1)",  # set w/o key
    "INSERT INTO part VALUES (1, 0, NULL, NULL, NULL, 'parks', 0, NULL, 0, '', 0, 1, 1)",
    "INSERT INTO part VALUES (1, 0, NULL, NULL, NULL, '', 0, 3, 0, '', 0, 1, 1)",
    "INSERT INTO part VALUES (99, 0, NULL, NULL, NULL, '', 0, NULL, 0, '', 0, 1, 1)",  # no item
    "INSERT INTO part_section VALUES (1, 1, 7)",                              # no such part
    "INSERT INTO rule (entry_id, rule_id, type, family, dimension, label, parts, take, may_target)"
    " VALUES ('r1:a', 'a.r1', 'retention_limit', 'retention', 'daily', 'x', '{}', 3, 0)",
    "INSERT INTO rule (entry_id, rule_id, type, family, dimension, label, parts, take, may_target,"
    " closure_grade) VALUES ('r1:a', 'a.r1', 'retention_limit', 'retention', 'daily', 'x', '{}', 0,"
    " 1, 'full')",
    "INSERT INTO rule (entry_id, rule_id, type, family, dimension, label, parts, take, may_target,"
    " closure_grade) VALUES ('r1:a', 'a.r1', 'retention_limit', 'retention', 'daily', 'x', '{}', 0,"
    " 0, 'maybe')",
])
def test_the_constraints_refuse_a_bad_row(sql):
    db = _db()
    with pytest.raises(sqlite3.IntegrityError):
        db.execute(sql)


def test_rule_keys_refuse_a_steelhead_water_without_the_rules_and_two_kinds():
    with pytest.raises(derived.DerivedError, match="no steelhead rule applies"):
        derived.rule_keys({1: 7}, {1}, set(), {})
    with pytest.raises(derived.DerivedError, match="two kinds"):
        derived.rule_keys({1: 7, 2: 7}, set(), set(), {1: "lake", 2: "stream"})


def test_a_part_whose_sections_disagree_is_refused():
    db = _db()
    section_sets(db, {1: 7, 2: 7, 3: 9})
    db.execute("INSERT INTO tidal (sid, entry_id) VALUES (1, 'r1:x')")      # one of two is tidal
    with pytest.raises(derived.DerivedError, match="tidal"):
        finish(db)
    db = _db()
    section_sets(db, {1: 7, 3: 9})                                          # sid 2: no set, in B.C.
    with pytest.raises(derived.DerivedError, match="outside B.C."):
        finish(db)


# --------------------------------------------------------------------------------------------
# On the built bundle (slow): the parts ARE the grouping the export used to compute
# --------------------------------------------------------------------------------------------

#: THE ORACLE: the export's part SQL as it stood before P2 (copy 1 of the three), kept here only
#: to prove the stored partition reproduces it.
_OLD_PART_SQL = (
    "SELECT i.item_id, r.set_id, l.set_id, "
    "(SELECT group_concat(k, ',') FROM (SELECT p.area_kind AS k FROM province_except p "
    " WHERE p.sid = s.sid ORDER BY p.area_kind)) AS pe, "
    "EXISTS (SELECT 1 FROM _sw w WHERE w.sid = s.sid) AS sw, "
    "(SELECT CASE h.code WHEN 1 THEN 'known' WHEN 2 THEN 'possible' END FROM _st h "
    " WHERE h.sid = s.sid) AS st, COUNT(*), MIN(s.sid) "
    "FROM item i JOIN item_section s ON s.ord = i.ord "
    "LEFT JOIN section_ruleset r ON r.sid = s.sid "
    "LEFT JOIN section_licensing l ON l.sid = s.sid "
    "GROUP BY i.item_id, r.set_id, l.set_id, pe, sw, st "
    "ORDER BY i.item_id, r.set_id IS NULL, r.set_id, l.set_id IS NULL, l.set_id, "
    "pe IS NOT NULL, pe, sw, st IS NULL, st")


@pytest.mark.slow
def test_the_stored_parts_are_the_old_grouping_on_the_built_bundle():
    bundle = Path(os.environ.get("UI_EXPORT_BUNDLE") or GENERATED.bundle / "bundle.sqlite")
    db = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    db.execute("PRAGMA temp_store = MEMORY")
    if not db.execute("SELECT 1 FROM sqlite_master WHERE name = 'part'").fetchone():
        pytest.skip(f"{bundle} predates the part table")
    # the views materialised, as the export did (correlated per row they cost a scan each)
    db.execute("CREATE TEMP TABLE _st (sid INTEGER PRIMARY KEY, code INTEGER NOT NULL)")
    db.execute("INSERT INTO _st SELECT sid, code FROM section_steelhead")
    db.execute("CREATE TEMP TABLE _sw (sid INTEGER PRIMARY KEY)")
    db.execute("INSERT INTO _sw SELECT DISTINCT sid FROM steelhead_water")
    old = [(it, rs, ls, pe or "", bool(sw), st, n, rep) for it, rs, ls, pe, sw, st, n, rep
           in db.execute(_OLD_PART_SQL)]
    names = dict(derived.read_steelhead_codes())
    new = [(it, p.set_id, p.licensing_set, ",".join(p.province_except), p.steelhead_water,
            p.steelhead, p.sections, p.rep_sid)
           for it, ps in sorted(derived.parts(db).items()) for p in ps]
    assert names and new == old and len(new) > 20_000
    derived.prove(db)
