"""THE BUNDLE'S DERIVED TABLES (DATAFLOW P2, `pipeline/deliver/bundle/derived.py`): rule_ix,
rule.closure_grade, rule_key + section_ruleset.key_ix, part + part_section. The constraints refuse
a bad value or a broken invariant (IntegrityError); the builder's proofs refuse what SQL cannot
state; the slow test holds the built bundle's parts to the grouping the export used to compute."""
from __future__ import annotations

import importlib
import sqlite3

import pytest

from pipeline.deliver.bundle import derived
from pipeline.tests.bundle_fixture import finish, section_sets
from pipeline.tests import under_test
from pipeline.tests.conftest import predates

SCHEMA = importlib.import_module("pipeline.deliver.bundle.build").SCHEMA


def test_the_schema_checks_are_the_enums():
    text = SCHEMA.read_text()
    v = derived.schema_check_values()
    assert f"province_except IN\n                                     ({v['province_except']})" in text
    assert f"kind            TEXT CHECK (kind IN ({v['kind']}))" in text
    assert f"closure_grade TEXT CHECK (closure_grade IN ({v['closure_grade']}))" in text
    # the unnamed water (S1)
    assert f"province_except TEXT NOT NULL CHECK (province_except IN\n" \
           f"                                             ({v['province_except']}))" in text
    assert f"kind            TEXT NOT NULL CHECK (kind IN ({v['kind']}))" in text


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


def test_part_section_carries_each_sections_length():
    """T387: `part_section.m` is the section's `section_length` (access.py), what the app weighs a part's access shares
    by; a section the access step did not measure is NULL (the app then claims no share), never a guess."""
    db = _db()
    db.executemany("INSERT INTO section_length (sid, m) VALUES (?,?)", [(1, 1200), (3, 40)])
    section_sets(db, {1: 7, 2: 7, 3: 9})
    finish(db)
    assert db.execute("SELECT ord, sid, part_ix, m FROM part_section ORDER BY ord, sid").fetchall() \
        == [(1, 1, 0, 1200), (1, 2, 0, None), (2, 3, 0, 40)]


@pytest.mark.parametrize("sql", [
    "INSERT INTO rule_key VALUES (5, 1, 1, 0, 'stream', 1, 1)",            # sw without sr
    "INSERT INTO rule_key VALUES (5, 1, 0, 0, 'pond', 1, 1)",              # not a water kind
    "INSERT INTO rule_key VALUES (5, 1, 0, 2, 'lake', 1, 1)",              # not a bool
    "INSERT INTO rule_key VALUES (5, 1, 0, 0, 'lake', 0, 1)",              # no sections
    "INSERT INTO section_ruleset (sid, set_id, key_ix) VALUES (9, 1, NULL)",
    "INSERT INTO section_ruleset (sid, set_id, key_ix) VALUES (9, 1, 404)",   # dangling key
    "INSERT INTO part VALUES (1, 0, 1, NULL, NULL, '', 0, NULL, 0, '', 0, 1, 1, 0, 0, 0, 0)",  # set w/o key
    "INSERT INTO part VALUES (1, 0, NULL, NULL, NULL, 'parks', 0, NULL, 0, '', 0, 1, 1, 0, 0, 0, 0)",
    "INSERT INTO part VALUES (1, 0, NULL, NULL, NULL, '', 0, 3, 0, '', 0, 1, 1, 0, 0, 0, 0)",
    "INSERT INTO part VALUES (99, 0, NULL, NULL, NULL, '', 0, NULL, 0, '', 0, 1, 1, 0, 0, 0, 0)",  # no item
    "INSERT INTO part_section (ord, sid, part_ix) VALUES (1, 1, 7)",                              # no such part
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
    " WHERE h.sid = s.sid) AS st, COUNT(*), MIN(s.sid), "
    # T228: mostly inside no-access land is a part of its own (the last order key)
    "COALESCE((SELECT SUM(sa.permille) > 500 FROM section_access sa JOIN access_area a ON a.ix = sa.area "
    " WHERE sa.sid = s.sid AND a.tier = 'no_access'), 0) AS na "
    "FROM item i JOIN item_section s ON s.ord = i.ord "
    "LEFT JOIN section_ruleset r ON r.sid = s.sid "
    "LEFT JOIN section_licensing l ON l.sid = s.sid "
    "GROUP BY i.item_id, r.set_id, l.set_id, pe, sw, st, na "
    "ORDER BY i.item_id, r.set_id IS NULL, r.set_id, l.set_id IS NULL, l.set_id, "
    "pe IS NOT NULL, pe, sw, st IS NULL, st, na")


@pytest.mark.slow
@pytest.mark.needs_bundle
def test_the_stored_parts_are_the_old_grouping_on_the_built_bundle():
    bundle = under_test.bundle()
    db = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    db.execute("PRAGMA temp_store = MEMORY")
    if not db.execute("SELECT 1 FROM sqlite_master WHERE name = 'part'").fetchone():
        predates(f"{bundle} predates the part table")
    # the views materialised, as the export did (correlated per row they cost a scan each)
    db.execute("CREATE TEMP TABLE _st (sid INTEGER PRIMARY KEY, code INTEGER NOT NULL)")
    db.execute("INSERT INTO _st SELECT sid, code FROM section_steelhead")
    db.execute("CREATE TEMP TABLE _sw (sid INTEGER PRIMARY KEY)")
    db.execute("INSERT INTO _sw SELECT DISTINCT sid FROM steelhead_water")
    old = [(it, rs, ls, pe or "", bool(sw), st, n, rep) for it, rs, ls, pe, sw, st, n, rep, _na
           in db.execute(_OLD_PART_SQL)]
    names = dict(derived.read_steelhead_codes())
    # T517: a part is one connected run, so the OLD grouping is each water's `grp` (its runs joined)
    by: dict = {}
    for it, ps in sorted(derived.parts(db).items()):
        for p in ps:
            g = by.setdefault((it, p.grp), [it, p.set_id, p.licensing_set, ",".join(p.province_except),
                                            p.steelhead_water, p.steelhead, 0, p.rep_sid])
            g[6] += p.sections
            g[7] = min(g[7], p.rep_sid)
    new = sorted((tuple(v) for v in by.values()), key=lambda r: (r[0], r[7]))
    assert names and new == sorted(old, key=lambda r: (r[0], r[7])) and len(new) > 20_000
    derived.prove(db)


def test_no_access_land_is_a_hard_break_between_parts():
    """T226 (owner 2026-10-28, the Coquitlam): sections with the same rules, one MOSTLY inside no-access land (> 500
    permille, summed over its no-access areas) and one outside, are two parts (T561: the share inside their union, `section_no_access`); a graze (<= 500) or a gentler tier
    (permit, reserve, private) does not split. The part outside comes first (the last order key: only a split water's indexes move)."""
    db = _db()
    db.executemany("INSERT INTO item_section (ord, sid) VALUES (?,?)", [(1, 4), (1, 5), (1, 6)])
    db.executemany("INSERT INTO access_area (ix, tier, area_id, name) VALUES (?,?,?,?)",
                   [(0, "no_access", "a", "Coquitlam Watershed"), (1, "no_access", "b", "Range"),
                    (2, "permit", "c", "Forest")])
    db.executemany("INSERT INTO section_access (sid, area, permille) VALUES (?,?,?)",
                   [(1, 0, 1000), (2, 0, 300), (2, 1, 300), (4, 0, 500), (5, 2, 1000), (6, 0, 100)])
    # (T561) "mostly" is read from the share inside the no-access polygons' UNION (access.write measures it)
    db.executemany("INSERT INTO section_no_access (sid, permille) VALUES (?,?)", [(1, 1000), (2, 600), (4, 500), (6, 100)])
    section_sets(db, {1: 7, 2: 7, 3: 9, 4: 7, 5: 7, 6: 7})
    finish(db)
    rows = db.execute("SELECT ord, part_ix, set_id, no_access, sections, rep_sid FROM part ORDER BY ord, part_ix").fetchall()
    assert rows == [(1, 0, 7, 0, 3, 4), (1, 1, 7, 1, 2, 1), (2, 0, 9, 0, 1, 3)]
    assert sorted(db.execute("SELECT sid FROM part_section WHERE ord = 1 AND part_ix = 1")) == [(1,), (2,)]


# ---- T517: one part per connected run ------------------------------------------------------------------------------

def _stream(chains=None):
    """One River, sections 1-2-3-4 end to end mouth up (100 m each, `length:k` tokens between them; `bridge` between 2
    and 3). `chains`: each section's DFO salmon chain."""
    db = _db()
    db.execute("DELETE FROM item_section WHERE ord = 1")
    db.executemany("INSERT INTO item_section (ord, sid) VALUES (?,?)", [(1, s) for s in (1, 2, 3, 4)])
    toks = ["mouth", "length:1", "bridge", "length:3", "source"]
    db.executemany("INSERT INTO span_end (eid, token) VALUES (?,?)", list(enumerate(toks)))
    db.executemany("INSERT INTO section_span (sid, lo_m, hi_m, lo, hi) VALUES (?,?,?,?,?)",
                   [(s, (s - 1) * 100, s * 100, s - 1, s) for s in (1, 2, 3, 4)])
    db.executemany("INSERT INTO section_touch (a, b) VALUES (?,?)", [(1, 2), (2, 3), (3, 4)])
    if chains:
        db.executemany("INSERT INTO salmon_section (sid, chain) VALUES (?,?)", sorted(chains.items()))
    return db


def test_a_rule_set_on_both_sides_of_another_is_two_parts_in_river_order():
    """T517 (owner 2026-11-08, "one part per connected adjacent stretch"): sets 7, 8, 7, 7 mouth up — set 7 is two parts
    (one grp, runs 0 and 1), numbered from the mouth: [1], [2], [3, 4]."""
    db = _stream()
    section_sets(db, {1: 7, 2: 8, 3: 7, 4: 7, 5: 9})
    db.execute("DELETE FROM item_section WHERE sid = 5")
    finish(db)
    rows = db.execute("SELECT part_ix, set_id, grp, run, sections, rep_sid FROM part WHERE ord = 1 ORDER BY part_ix")
    assert rows.fetchall() == [(0, 7, 0, 0, 1, 1), (1, 8, 1, 0, 1, 2), (2, 7, 0, 1, 2, 3)]


def test_a_salmon_chain_change_cuts_a_run_into_parts_of_one_grp_and_run():
    """T517: the chain is in the part key — every part paints one salmon colour; the pieces share (grp, run), what the
    regular map joins back into one entry."""
    db = _stream({1: 5, 2: 5, 3: 6, 4: 6})
    section_sets(db, {1: 7, 2: 7, 3: 7, 4: 7})
    finish(db)
    rows = db.execute("SELECT part_ix, grp, run, chain, rep_sid FROM part WHERE ord = 1 ORDER BY part_ix").fetchall()
    assert rows == [(0, 0, 0, 5, 1), (1, 0, 0, 6, 3)]


def test_two_parts_of_one_water_never_touch_with_the_same_key():
    """T517 maximal runs: same rules all along make ONE part, whatever cuts the spans carry."""
    db = _stream()
    section_sets(db, {1: 7, 2: 7, 3: 7, 4: 7})
    finish(db)
    assert db.execute("SELECT COUNT(*), MIN(sections) FROM part WHERE ord = 1").fetchone() == (1, 4)


def _braid():
    """One River, stem 1-2-3 mouth up (sets 7, 8, 7) and a side channel 5 of set 7 that re-enters inside section 1's
    range (measure 50) and leaves from section 3 — the braid round section 2 (Wap Creek's channel inside Mount Griffin
    Ecological Reserve, past the 275 m of stem outside it)."""
    db = _stream()
    db.execute("DELETE FROM item_section WHERE sid = 4")
    db.execute("DELETE FROM section_span WHERE sid = 4")
    db.execute("DELETE FROM section_touch WHERE b = 4")
    db.execute("INSERT INTO item_section (ord, sid) VALUES (1, 5)")
    db.execute("INSERT INTO section_span (sid, lo_m, hi_m, lo, hi, off_stem) VALUES (5, 50, 50, 0, 4, 1)")
    db.executemany("INSERT INTO section_touch (a, b) VALUES (?,?)", [(1, 5), (3, 5)])
    section_sets(db, {1: 7, 2: 8, 3: 7, 5: 7})
    return db


def test_a_side_channel_that_braids_round_another_rule_set_joins_the_two_runs_it_touches():
    """T517 review (Wap Creek 1/3, Nimpkish 2/4, Skeena 1/3): `stretches` filed the channel under the run it re-enters,
    so the run above section 2 was a second part of set 7 that the channel TOUCHES — one painted run, two parts, no
    mark between them. Touching stretches are one run: set 7 is one part (1, 3, 5), numbered after set 8 (its top is higher)."""
    db = _braid()
    finish(db)
    rows = db.execute("SELECT part_ix, set_id, grp, run, sections FROM part WHERE ord = 1 ORDER BY part_ix").fetchall()
    assert rows == [(0, 8, 1, 0, 1), (1, 7, 0, 0, 3)]       # river order: by the top of each part
