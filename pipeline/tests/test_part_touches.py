"""Which parts of a water border each other (`waters[].parts[].touches`), and the bundle table it
is read from (`section_touch`).

The UI merged every closed-all-year part of a water into one, touching or not, because nothing
said which parts were neighbours. The build now writes the graph's adjacency between sections of
one water into the bundle; the export turns it into part-to-part `touches` and ships no section.

The synthetic tests build a tiny bundle by hand, so each claim is checked against the OUTPUT of
the export for a known input. The corpus tests read the side bundle `UI_EXPORT_BUNDLE` (default
the shipped one) and recompute from its rows, never from the export's own code.
"""
from __future__ import annotations

import copy
import os
import sqlite3
from collections import defaultdict
from pathlib import Path

import pytest

from pipeline.tests.bundle_fixture import finish, section_sets

from pipeline.deliver.bundle.build import SCHEMA, touching_pairs
from pipeline.tools import export_ui_rules as X

BUNDLE = Path(os.environ.get("UI_EXPORT_BUNDLE") or X.BUNDLE)


# ---------------------------------------------------------------------------------------------
# The build: which section pairs are written
# ---------------------------------------------------------------------------------------------
SID = {"r:0": 0, "r:10": 1, "r:20": 2, "r:30": 3, "trib:0": 4, "side:0": 5}
#: river r in four pieces, a side channel of r (same water) and a tributary (another water)
WATERS_OF = {0: {1}, 1: {1}, 2: {1}, 3: {1}, 5: {1}, 4: {2}}


def test_pieces_joined_end_to_end_touch_and_only_those():
    # upstream flows into downstream: r:10 -> r:0, r:20 -> r:10, r:30 -> r:20
    got = touching_pairs([("r:10", "r:0"), ("r:20", "r:10"), ("r:30", "r:20")], SID, WATERS_OF)
    assert got == [(0, 1), (1, 2), (2, 3)]
    assert (0, 2) not in got and (1, 3) not in got, "a gap of one piece is not a touch"


def test_a_branch_of_the_same_water_touches_where_it_flows_in():
    assert touching_pairs([("side:0", "r:10")], SID, WATERS_OF) == [(1, 5)]


def test_another_water_never_touches_and_nothing_touches_itself():
    assert touching_pairs([("trib:0", "r:10")], SID, WATERS_OF) == [], "a tributary is its own water"
    assert touching_pairs([("r:10", "r:10")], SID, WATERS_OF) == []
    assert touching_pairs([("r:10", "gone:0")], SID, WATERS_OF) == [], "no handle, no row"


def test_each_pair_is_stored_once_low_handle_first():
    got = touching_pairs([("r:10", "r:0"), ("r:0", "r:10"), ("r:10", "r:0")], SID, WATERS_OF)
    assert got == [(0, 1)]


# ---------------------------------------------------------------------------------------------
# The export: part-to-part `touches`, from a hand-built bundle
# ---------------------------------------------------------------------------------------------
def _bundle(tmp: Path, sets: dict[int, int | None], touch: list[tuple[int, int]],
            water: dict[int, int] | None = None) -> Path:
    """One river `gnis:1` (and `gnis:2` where `water` says so): section handle -> rule set."""
    path = tmp / "bundle.sqlite"
    path.unlink(missing_ok=True)
    db = sqlite3.connect(path)
    db.executescript(SCHEMA.read_text())
    db.executemany("INSERT INTO item (ord, item_id, name, kind) VALUES (?,?,?,?)",
                   [(1, "gnis:1", "One River", "stream"), (2, "gnis:2", "Two Creek", "stream")])
    water = water or {}
    db.executemany("INSERT INTO item_section (ord, sid) VALUES (?,?)",
                   [(water.get(s, 1), s) for s in sets])
    section_sets(db, {s: r for s, r in sets.items() if r is not None})
    # a named section with no rule set is outside B.C. (the bundle refuses any other)
    db.executemany("INSERT INTO outside_bc (sid) VALUES (?)",
                   [(s,) for s, r in sets.items() if r is None])
    db.executemany("INSERT INTO section_touch (a, b) VALUES (?,?)", touch)
    # every stream section says where it lies (`section_span`) or the export refuses the bundle:
    # here, one km per handle, ending at the river's own mouth and source
    db.execute("INSERT INTO span_end (eid, token) VALUES (1, 'mouth'), (2, 'source')")
    db.executemany("INSERT INTO section_span (sid, lo_m, hi_m, lo, hi) VALUES (?,?,?,1,2)",
                   [(s, 1000 * s, 1000 * s + 1000) for s in sets])
    finish(db)
    db.commit()
    db.close()
    return path


def _parts(path: Path, item: str = "gnis:1") -> dict[str, list[str]]:
    """ruleset -> the rulesets of the parts it touches, so a test names parts, not indexes."""
    parts = X.read(path)["waters"][item]["parts"]
    return {p["ruleset"]: sorted(parts[j]["ruleset"] for j in p["touches"]) for p in parts}


def test_consecutive_stretches_touch_and_ones_with_a_stretch_between_do_not(tmp_path):
    # 0 closed (7) | 1 open (8) | 2 closed-all-year (9): 7 and 9 are both "closed", not neighbours
    got = _parts(_bundle(tmp_path, {0: 7, 1: 8, 2: 9}, [(0, 1), (1, 2)]))
    assert got == {"7": ["8"], "8": ["7", "9"], "9": ["8"]}


def test_a_part_never_touches_itself_though_its_own_sections_meet(tmp_path):
    # 0 and 1 are both set 7 and joined: one part, and it lists only its real neighbour
    got = _parts(_bundle(tmp_path, {0: 7, 1: 7, 2: 8}, [(0, 1), (1, 2)]))
    assert got == {"7": ["8"], "8": ["7"]}


def test_a_part_of_two_stretches_touches_what_either_stretch_touches(tmp_path):
    # 7 | 8 | 7 | 9 : set 7 is two stretches; it borders 8 and 9, and 8 does not border 9
    got = _parts(_bundle(tmp_path, {0: 7, 1: 8, 2: 7, 3: 9}, [(0, 1), (1, 2), (2, 3)]))
    assert got == {"7": ["8", "9"], "8": ["7"], "9": ["7"]}


def test_a_stretch_with_no_rule_set_is_a_part_and_touches_like_one(tmp_path):
    got = _parts(_bundle(tmp_path, {0: 7, 1: None, 2: 9}, [(0, 1), (1, 2)]))
    assert got == {"7": [None], None: ["7", "9"], "9": [None]}


def test_touching_is_symmetric_on_every_part(tmp_path):
    parts = X.read(_bundle(tmp_path, {0: 7, 1: 8, 2: 9, 3: 7}, [(0, 1), (1, 2), (2, 3)])
                   )["waters"]["gnis:1"]["parts"]
    for i, p in enumerate(parts):
        assert i not in p["touches"]
        for j in p["touches"]:
            assert i in parts[j]["touches"]


def test_a_pair_across_two_waters_makes_no_touch(tmp_path):
    """The build never writes one; if a bundle did, the export still keeps waters apart."""
    got = X.read(_bundle(tmp_path, {0: 7, 1: 8}, [(0, 1)], water={1: 2}))["waters"]
    assert got["gnis:1"]["parts"][0]["touches"] == []
    assert got["gnis:2"]["parts"][0]["touches"] == []


def test_mutation_the_touch_comes_from_the_bundle_row_not_from_the_grouping(tmp_path):
    """Mutation pin: the same parts with and without the one row that joins them. If `touches`
    were inferred (say, every part of a water touching every other) both reads would agree."""
    joined = _parts(_bundle(tmp_path, {0: 7, 1: 9}, [(0, 1)]))
    apart = _parts(_bundle(tmp_path, {0: 7, 1: 9}, []))
    assert joined == {"7": ["9"], "9": ["7"]}
    assert apart == {"7": [], "9": []}


def test_a_bundle_without_the_table_is_refused(tmp_path):
    path = _bundle(tmp_path, {0: 7}, [])
    db = sqlite3.connect(path)
    db.execute("DROP TABLE section_touch")
    db.commit()
    db.close()
    with pytest.raises(SystemExit, match="section_touch"):
        X.read(path)


# ---------------------------------------------------------------------------------------------
# The corpus
# ---------------------------------------------------------------------------------------------
@pytest.fixture(scope="module")
def db():
    con = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    yield con
    con.close()


@pytest.fixture(scope="module")
def waters():
    return X.read(BUNDLE)["waters"]


def test_every_written_pair_is_two_sections_of_one_water(db):
    n = db.execute("SELECT COUNT(*) FROM section_touch").fetchone()[0]
    assert n > 10_000, "the build wrote no adjacency"
    assert db.execute("SELECT COUNT(*) FROM section_touch WHERE a >= b").fetchone()[0] == 0
    assert db.execute(
        "SELECT COUNT(*) FROM section_touch t WHERE NOT EXISTS (SELECT 1 FROM item_section x "
        "JOIN item_section y ON y.ord = x.ord WHERE x.sid = t.a AND y.sid = t.b)"
    ).fetchone()[0] == 0


def test_touches_is_symmetric_irreflexive_and_in_range_on_every_water(waters):
    for item, w in waters.items():
        for i, p in enumerate(w["parts"]):
            assert p["touches"] == sorted(set(p["touches"])), item
            assert i not in p["touches"], item
            for j in p["touches"]:
                assert 0 <= j < len(w["parts"]) and i in w["parts"][j]["touches"], item


def test_touches_is_what_the_bundle_rows_say(waters, db):
    """Recomputed from `section_touch` and the per-section sets in SQL, independently of the
    export's own join."""
    key = {}
    for item, s, rs, ls in db.execute(
            "SELECT i.item_id, s.sid, r.set_id, l.set_id FROM item i "
            "JOIN item_section s ON s.ord = i.ord "
            "LEFT JOIN section_ruleset r ON r.sid = s.sid "
            "LEFT JOIN section_licensing l ON l.sid = s.sid"):
        key[s] = (item, None if rs is None else str(rs), None if ls is None else str(ls))
    want = defaultdict(set)
    for a, b in db.execute("SELECT a, b FROM section_touch"):
        (ia, ra, la), (ib, rb, lb) = key[a], key[b]
        if (ra, la) != (rb, lb):
            want[ia].add(frozenset({(ra, la), (rb, lb)}))
    for item, w in waters.items():
        got = {frozenset({(w["parts"][i]["ruleset"], w["parts"][i]["licensing_set"]),
                          (w["parts"][j]["ruleset"], w["parts"][j]["licensing_set"])})
               for i, p in enumerate(w["parts"]) for j in p["touches"]}
        # parts that differ only by province_except / anadromous_rainbow share a set pair
        got = {g for g in got if len(g) == 2}
        assert got == want.get(item, set()), item


def test_the_touch_check_catches_a_one_sided_self_or_stray_touch(waters):
    """Mutation pin for `X.touch_problems` (which `X.dangling`, and so `problems`, runs): each
    breakage must go red on a real water, and the real water must be clean."""
    item = next(i for i, w in waters.items() if any(p["touches"] for p in w["parts"]))
    w = waters[item]
    i = next(k for k, p in enumerate(w["parts"]) if p["touches"])
    j = w["parts"][i]["touches"][0]
    assert X.touch_problems({item: w}) == []

    one_sided = copy.deepcopy(w)
    one_sided["parts"][j]["touches"].remove(i)
    assert X.touch_problems({item: one_sided}), "a one-sided touch passed"
    itself = copy.deepcopy(w)
    itself["parts"][i]["touches"] = sorted(itself["parts"][i]["touches"] + [i])
    assert X.touch_problems({item: itself}), "a self touch passed"
    stray = copy.deepcopy(w)
    stray["parts"][i]["touches"].append(len(w["parts"]))
    assert X.touch_problems({item: stray}), "a touch naming no part passed"
    missing = copy.deepcopy(w)
    del missing["parts"][i]["touches"]
    assert X.touch_problems({item: missing}), "a part with no touches passed"
