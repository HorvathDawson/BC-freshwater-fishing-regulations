"""T1251 (owner 2026-11-27, verbatim: "The most upstream row, in 0.10.0"): when two inherited closures close a water on
the same day, the card NAMES the most upstream row. Words only: both still close (RULINGS T14), the day, the fish and the
status are the same; only which row governs (is cited) and which agrees moves.

The bundle side (`nearer.compute_trib_upstream`, table `trib_upstream`): `trib_nearer`'s pairs plus the rows whose
tributary walks nest. Lower Campbell Lake and John Hart Lake both exit into the Campbell River, which runs on through
John Hart, so `trib_nearer` (first named water down) leaves them unordered; Lower Campbell's walk lies inside John
Hart's. The answers side (`rows.evaluate._closure_cmp`): asked after rank and before T206's year-round-before-dated.

Mutation: `nearer.WALKS_THAT_NEST_ORDER_THE_ROWS = False` -> the two-lakes pair is gone;
`evaluate.THE_MOST_UPSTREAM_ROW_IS_NAMED = False` -> the farther row is named by entry key again.
"""
from __future__ import annotations

import sqlite3
from types import SimpleNamespace

import pytest

from pipeline.deliver.answers.rows import evaluate as EV
from pipeline.deliver.bundle import nearer as N
from pipeline.deliver.bundle import read

LOWER = "r1:lower_campbell_lake_s_tributaries@1-6"      # the upper lake of the chain (its row's name says Lower)
HART = "r1:john_hart_lake_s_tributaries@1-10"           # the lower lake
RIVER = "r1:campbell_river@1-10"                        # the river both lakes exit into, running on through John Hart
SQUAM, SQUAM_T = "r2:squamish_river@2-6", "r2:squamish_river_s_tributaries@2-6"


def _db(*, walks: dict, matched: dict, exit_down=(), down=()):
    """A hand-made bundle: items, their outlets (`item_down` / `item_exit_down`), the rows' matched waters and the
    sections each row's walk reaches (`walks`: entry -> sids, every one in a rule set of its own via `trib`)."""
    db = sqlite3.connect(":memory:")
    db.executescript("""
        CREATE TABLE item (ord INTEGER PRIMARY KEY, item_id TEXT);
        CREATE TABLE item_down (ord INTEGER, down_ord INTEGER);
        CREATE TABLE item_exit_down (ord INTEGER, down_ord INTEGER);
        CREATE TABLE ruleset (set_id INTEGER, entry_id TEXT, rule_id TEXT, via TEXT);
        CREATE TABLE section_ruleset (sid INTEGER PRIMARY KEY, set_id INTEGER);
        CREATE TABLE entry (entry_id TEXT, matched TEXT);""")
    items = sorted({i for ms in matched.values() for i in ms})
    ords = {i: n for n, i in enumerate(items, 1)}
    db.executemany("INSERT INTO item VALUES (?,?)", [(o, i) for i, o in ords.items()])
    db.executemany("INSERT INTO item_exit_down VALUES (?,?)", [(ords[a], ords[b]) for a, b in exit_down])
    db.executemany("INSERT INTO item_down VALUES (?,?)", [(ords[a], ords[b]) for a, b in list(down) + list(exit_down)])
    db.executemany("INSERT INTO entry VALUES (?,?)", [(e, f'["{m[0]}"]') for e, m in matched.items()])
    # one rule set per distinct combination of the rows reaching a section
    by_sid: dict = {}
    for e, sids in walks.items():
        for s in sids:
            by_sid.setdefault(s, set()).add(e)
    sets: dict = {}
    for s, es in sorted(by_sid.items()):
        set_id = sets.setdefault(frozenset(es), len(sets) + 1)
        db.execute("INSERT INTO section_ruleset VALUES (?,?)", (s, set_id))
    for es, set_id in sets.items():
        db.executemany("INSERT INTO ruleset VALUES (?,?,?,?)", [(set_id, e, "r1", "trib") for e in sorted(es)])
    return db


def _campbell():
    """Lower Campbell's walk (sids 1-3) lies inside John Hart's (1-5); the river's own walk reaches 1-6. Both lakes exit
    into the river; the river exits into nothing named."""
    return _db(walks={LOWER: {1, 2, 3}, HART: {1, 2, 3, 4, 5}, RIVER: {1, 2, 3, 4, 5, 6}},
               matched={LOWER: ["wbk:lower"], HART: ["wbk:hart"], RIVER: ["gnis:campbell"]},
               exit_down=[("wbk:lower", "gnis:campbell"), ("wbk:hart", "gnis:campbell")],
               down=[("gnis:campbell", "wbk:lower"), ("gnis:campbell", "wbk:hart")])


def test_two_lakes_on_one_river_are_ordered_by_their_nested_walks(monkeypatch):
    db = _campbell()
    assert N.compute_trib_nearer(db) == {(LOWER, RIVER), (HART, RIVER)}, "first named water down: the river, for both"
    assert N.compute_trib_upstream(db) == {(LOWER, RIVER), (HART, RIVER), (LOWER, HART)}
    monkeypatch.setattr(N, "WALKS_THAT_NEST_ORDER_THE_ROWS", False)
    assert N.compute_trib_upstream(db) == {(LOWER, RIVER), (HART, RIVER)}, "the mutation pin: trib_nearer alone"


def test_two_rows_of_one_water_are_never_ordered_by_nesting():
    """A river's row and its tributaries' row: one water, no "more upstream"; the old order names one."""
    db = _db(walks={SQUAM_T: {1, 2}, SQUAM: {1, 2, 3}},
             matched={SQUAM_T: ["gnis:squamish"], SQUAM: ["gnis:squamish"]})
    assert N.compute_trib_upstream(db) == set()


def test_trib_nearer_decides_first():
    """A nesting the other way round never overturns a pair `trib_nearer` orders."""
    db = _db(walks={LOWER: {1, 2, 3, 4}, HART: {1, 2}},
             matched={LOWER: ["wbk:lower"], HART: ["wbk:hart"]}, exit_down=[("wbk:lower", "wbk:hart")])
    assert N.compute_trib_upstream(db) == {(LOWER, HART)}


def _closure(entry, *, rank=1, dated=True, fish=("RB", "CT", "DV"), via="trib"):
    return SimpleNamespace(key=f"{entry}::r1", entry_id=entry, rule_id="r1", rank=rank, species=list(fish), via=via,
                           f={"when": {"dates": [{"from_month": 4, "from_day": 15, "to_month": 7, "to_day": 15}]}}
                           if dated else {})


def _named(cands, upstream):
    return EV._stable_sort(cands, key=EV.closure_order_for(frozenset(upstream)))[0].entry_id


UP = {(LOWER, HART), (LOWER, RIVER), (HART, RIVER)}


def test_the_most_upstream_lakes_closure_is_named_whatever_the_keys(monkeypatch):
    """Lower Campbell's creeks, Jun 1-Jul 15: John Hart's Apr 15-Jul 15 and Lower Campbell's Feb 1-Jul 15 both close;
    the card names Lower Campbell's (before: John Hart's, by entry key)."""
    for order in ([_closure(HART), _closure(LOWER)], [_closure(LOWER), _closure(HART)]):
        assert _named(order, UP) == LOWER
    monkeypatch.setattr(EV, "THE_MOST_UPSTREAM_ROW_IS_NAMED", False)
    assert _named([_closure(HART), _closure(LOWER)], UP) == HART, "the mutation pin: the input order (the entry key)"


def test_a_lakes_row_is_named_before_the_river_it_drains_into():
    """The river row's closure climbs through both lakes (the walk); on the lake's creeks the lake's row is the more
    upstream, even where the river's closure names more fish."""
    assert _named([_closure(RIVER, fish=("RB", "CT", "DV", "ST")), _closure(HART)], UP) == HART


def test_the_most_upstream_row_is_asked_before_year_round_before_dated():
    """The owner's words name the most upstream row; T206's year-round-before-dated breaks only an unordered tie."""
    assert _named([_closure(RIVER, dated=False), _closure(LOWER)], UP) == LOWER


def test_rank_still_comes_first_and_a_tie_without_a_pair_falls_to_the_old_order():
    assert _named([_closure(LOWER, rank=1), _closure(RIVER, rank=0)], UP) == RIVER, "a better rung, as before"
    a, b = "r4:a_lake_s_tributaries@4-1", "r4:b_lake_s_tributaries@4-2"
    assert _named([_closure(b, dated=False), _closure(a)], UP) == b, "no pair: year-round before dated (T206)"
    assert _named([_closure(a, fish=("RB",)), _closure(b)], UP) == b, "no pair: the one naming more fish"
    assert _named([_closure(b), _closure(a)], UP) == b, "no pair, a full tie: the input order"


def test_two_closures_on_their_own_rows_water_keep_the_old_order():
    """Only INHERITED closures: the Campbell River's 100 m below Strathcona Dam is printed in two rows' own extents (the
    river's all-year spot closure, Lower Campbell Lake's row "including Campbell River between Strathcona Dam and (Lower)
    Campbell Lake", Feb 1-Jul 15): neither is inherited there, and T206 names the year-round one, as before."""
    got = _named([_closure(LOWER, rank=0, via="reach"), _closure(RIVER, rank=0, dated=False, via="reach")], UP)
    assert got == RIVER


def test_the_reader_refuses_a_bundle_without_the_table(tmp_path):
    p = tmp_path / "bundle.sqlite"
    db = sqlite3.connect(p)
    db.execute("CREATE TABLE rule (entry_id TEXT)")
    db.commit()
    db.close()
    with pytest.raises(SystemExit, match="trib_upstream"):
        read._upstream_of(str(p))
    db = sqlite3.connect(p)
    db.execute("CREATE TABLE trib_upstream (near TEXT, far TEXT)")
    db.execute("INSERT INTO trib_upstream VALUES (?,?)", (LOWER, HART))
    db.commit()
    db.close()
    read._UPSTREAM_BY_PATH.pop(str(p), None)
    assert read._upstream_of(str(p)) == {(LOWER, HART)}
    read._UPSTREAM_BY_PATH.pop(str(p), None)
