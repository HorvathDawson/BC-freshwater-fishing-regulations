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


NORTH, ALOUETTE = "r2:north_alouette_river@2-8", "r2:alouette_river@2-8"


def _named_own(cands, upstream, own):
    return EV._stable_sort(cands, key=EV.closure_order_for(frozenset(upstream), frozenset(own)))[0].entry_id


def test_the_waters_own_row_is_named_first_north_alouette(monkeypatch):
    """Owner 2026-11-27, verbatim: "(a) Own row first, then most upstream (Recommended)". The North Alouette River:
    its own "No Fishing upstream of 216th Street (Fifth Ave), May 1-June 30" and the Alouette River's "... (including
    North Alouette River), May 1-June 30", both printed by the rows' own extents on one rung: the own row is named."""
    cands = [_closure(ALOUETTE, rank=0, via="reach"), _closure(NORTH, rank=0, via="reach")]
    assert _named_own(cands, {(NORTH, ALOUETTE)}, {NORTH}) == NORTH
    assert _named_own(cands, set(), {NORTH}) == NORTH, "own row first, before any upstream pair"
    monkeypatch.setattr(EV, "OWN_ROW_FIRST", False)
    assert _named_own(cands, {(NORTH, ALOUETTE)}, {NORTH}) == ALOUETTE, "the pin: T206's order again"


def test_the_campbell_rivers_100_m_keeps_its_own_all_year_closure():
    """The Campbell River below Strathcona Dam: its own all-year closure and Lower Campbell Lake's "including Campbell
    River between Strathcona Dam and (Lower) Campbell Lake" (Lower Campbell is the more upstream row): own row first."""
    cands = [_closure(LOWER, rank=0, via="reach"), _closure(RIVER, rank=0, dated=False, via="reach")]
    assert _named_own(cands, UP, {RIVER}) == RIVER


def test_set_own_is_the_row_whose_water_holds_the_set():
    db = _db(walks={}, matched={NORTH: ["gnis:north"], ALOUETTE: ["gnis:alouette"]})
    db.executescript("CREATE TABLE item_section (ord INTEGER, sid INTEGER);")
    ords = dict(db.execute("SELECT item_id, ord FROM item"))
    db.executemany("INSERT INTO item_section VALUES (?,?)", [(ords["gnis:north"], 1), (ords["gnis:north"], 2),
                                                             (ords["gnis:alouette"], 3)])
    db.executemany("INSERT INTO ruleset VALUES (?,?,?,?)", [(9, NORTH, "r1", "reach"), (9, ALOUETTE, "r2", "reach")])
    db.executemany("INSERT INTO section_ruleset VALUES (?,?)", [(1, 9), (2, 9)])
    assert N.compute_set_own(db) == {(9, NORTH)}


def test_a_waters_own_extent_comes_before_what_it_inherits_on_one_rung():
    """Review round 1: an own-extent closure and an inherited one tied on rank are ranked, never left to input order."""
    own, inh = _closure(RIVER, via="reach"), _closure(LOWER)
    assert _named([inh, own], UP) == RIVER and _named([own, inh], UP) == RIVER


def test_the_banners_closers_put_the_most_upstream_row_first_and_keep_the_rest_in_order():
    """The banner's order asks the card's comparison (`upstream_cmp`): the West Kettle creek's case (the Kettle River's
    tributaries closure first by rule order, the West Kettle's second) comes out West Kettle first; a zone closure and
    a closure of another rung keep their places."""
    kettle, west = "r8:kettle_river_s_tributaries@8-14", "r8:west_kettle_river_s_tributaries@8-12"
    zone = _closure("zp:superior_closures", rank=4, via="reach")
    got = EV.most_upstream_first([_closure(kettle), _closure(west), zone], frozenset({(west, kettle)}))
    assert [c.entry_id for c in got] == [west, kettle, "zp:superior_closures"]
    three = [_closure(RIVER), _closure(HART), _closure(LOWER)]
    assert [c.entry_id for c in EV.most_upstream_first(three, UP)] == [LOWER, HART, RIVER]
    assert [c.entry_id for c in EV.most_upstream_first(three, frozenset())] == [RIVER, HART, LOWER], "no pairs: as given"


def test_the_banner_reorders_within_a_rung_only():
    """Review round 2 (Greenstone Creek, Feb 1-May 31): campbell_river (inherited, rung 1) > greenstone_creek (its own
    row, rung 0) > lower_campbell (inherited, rung 1). Only the two inherited closures swap, in the places rung 1 holds;
    the own row keeps its place, and a zone closure on another rung never moves."""
    green = "r1:greenstone_creek@1-10"
    zone = _closure("zp:superior_closures", rank=4, via="reach")
    got = EV.most_upstream_first([_closure(RIVER), _closure(green, rank=0, via="reach"), _closure(LOWER), zone], UP)
    assert [c.entry_id for c in got] == [LOWER, green, RIVER, "zp:superior_closures"]
    got = EV.most_upstream_first([zone, _closure(RIVER), _closure(LOWER)], UP)
    assert [c.entry_id for c in got] == ["zp:superior_closures", LOWER, RIVER]


def test_two_closures_no_chain_relates_keep_their_order():
    """A pulled closure moves to the place of the one it precedes; an unrelated one keeps its place relative to the
    first (the banner's two all-year closures are not reordered by a third)."""
    other = "r4:a_lake_s_tributaries@4-1"
    got = EV.most_upstream_first([_closure(other), _closure(RIVER), _closure(LOWER)], UP)
    assert [c.entry_id for c in got] == [other, LOWER, RIVER]


def test_own_extent_before_inherited_follows_the_own_row_pin(monkeypatch):
    """False = an own-extent and an inherited closure on one rung fall to the input order (T206) again."""
    own, inh = _closure(RIVER, via="reach"), _closure(LOWER)
    monkeypatch.setattr(EV, "OWN_ROW_FIRST", False)
    assert _named([inh, own], UP) == LOWER and _named([own, inh], UP) == RIVER


def test_one_read_of_the_pairs():
    """A bundle object with no path (built in memory by a test) has no pairs; a path reads the table (seeded here)."""
    assert EV.upstream_pairs(SimpleNamespace()) == frozenset() and EV.own_rows(SimpleNamespace(), 9) == frozenset()
    read._UPSTREAM_BY_PATH["mem:t1251"] = frozenset({(LOWER, HART)})
    try:
        assert EV.upstream_pairs(SimpleNamespace(path="mem:t1251")) == {(LOWER, HART)}
    finally:
        read._UPSTREAM_BY_PATH.pop("mem:t1251")


def test_a_circle_in_the_pairs_is_refused():
    assert N.upstream_cycle({(LOWER, HART), (HART, RIVER)}) == []
    loop = N.upstream_cycle({(LOWER, HART), (HART, RIVER), (RIVER, LOWER)})
    assert loop[0] == loop[-1] and set(loop) == {LOWER, HART, RIVER} and len(loop) == 4
    db = _campbell()
    db.execute("CREATE TABLE trib_nearer (near TEXT, far TEXT)")
    db.execute("CREATE TABLE trib_upstream (near TEXT, far TEXT)")
    db.executemany("INSERT INTO trib_nearer VALUES (?,?)", sorted(N.compute_trib_nearer(db)))
    db.executemany("INSERT INTO trib_upstream VALUES (?,?)", [(LOWER, HART), (HART, RIVER), (RIVER, LOWER)])
    with pytest.raises(SystemExit, match="circle"):
        N.prove(db)


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


def test_the_reader_refuses_a_bundle_without_set_own(tmp_path):
    p = tmp_path / "bundle.sqlite"
    db = sqlite3.connect(p)
    db.execute("CREATE TABLE rule (entry_id TEXT)")
    db.commit()
    db.close()
    with pytest.raises(SystemExit, match="set_own"):
        read._own_of(str(p))
    db = sqlite3.connect(p)
    db.execute("CREATE TABLE set_own (set_id INTEGER, entry_id TEXT)")
    db.execute("INSERT INTO set_own VALUES (?,?)", (9, NORTH))
    db.commit()
    db.close()
    read._OWN_BY_PATH.pop(str(p), None)
    assert read._own_of(str(p)) == {9: frozenset({NORTH})}
    read._OWN_BY_PATH.pop(str(p), None)


def _own_db():
    db = _campbell()
    db.executescript("""CREATE TABLE item_section (ord INTEGER, sid INTEGER);
        CREATE TABLE trib_nearer (near TEXT, far TEXT); CREATE TABLE trib_upstream (near TEXT, far TEXT);
        CREATE TABLE set_own (set_id INTEGER, entry_id TEXT);""")
    db.executemany("INSERT INTO trib_nearer VALUES (?,?)", sorted(N.compute_trib_nearer(db)))
    db.executemany("INSERT INTO trib_upstream VALUES (?,?)", sorted(N.compute_trib_upstream(db)))
    return db


def test_prove_refuses_a_set_own_that_is_not_what_the_rows_give():
    db = _own_db()
    N.prove(db)                                      # empty and right: no set where two rows print by their own extents
    db.execute("INSERT INTO set_own VALUES (?,?)", (1, LOWER))
    with pytest.raises(SystemExit, match="set_own"):
        N.prove(db)


def test_the_banner_order_takes_the_own_rows():
    """The banner path (`display.section` passes `own_rows`): the water's own row first within its rung, the rest kept."""
    zone = _closure("zp:superior_closures", rank=4, via="reach")
    got = EV.most_upstream_first([_closure(ALOUETTE, rank=0, via="reach"), zone, _closure(NORTH, rank=0, via="reach")],
                                 frozenset({(NORTH, ALOUETTE)}), frozenset({NORTH}))
    assert [c.entry_id for c in got] == [NORTH, "zp:superior_closures", ALOUETTE]


def _gate(entry, *, when=None, species=("ALL_GAME_FISH",), exempts=None):
    x = {"_rank": 0, "dimension": "daily", "entry": entry, "entry_id": entry, "extents": [{"op": "whole"}],
         "family": "retention", "may_target": 0, "period": "daily", "rule": "r1", "rule_id": "r1", "scope": "section",
         "species": list(species), "species_except": [], "take": 0, "type": "retention_limit", "label": "No fishing",
         "verbatim": "No Fishing"}
    if when:
        x["when"] = when
    if exempts:
        x["exempts"] = exempts
    return x


def test_what_the_card_shows_when_a_dated_own_closure_and_a_year_round_one_both_close():
    """Review round 3 (pinned): the North Alouette's own dated closure and the Alouette's year-round one printed for it,
    one rung: the own row governs (named), the year-round one AGREES (still listed, never hidden); the lift notes and the
    trout date line read the named closure (`liftNotes` of the winner; `closure_scope` of its fish)."""
    from pipeline.deliver.answers.rows.part import Part
    from pipeline.deliver.answers.trout.timeline import closure_scope
    dated = {"dates": [{"from_month": 5, "from_day": 1, "to_month": 6, "to_day": 30}]}
    own, far = f"{NORTH}::r1", f"{ALOUETTE}::r1"
    B = SimpleNamespace(sets={7: [(NORTH, "r1", "reach"), (ALOUETTE, "r1", "reach")]},
                        rules={(NORTH, "r1"): _gate(NORTH, when=dated, species=("RB",)), (ALOUETTE, "r1"): _gate(ALOUETTE)},
                        path="mem:t1251-own")
    read._OWN_BY_PATH["mem:t1251-own"] = {7: frozenset({NORTH})}
    read._UPSTREAM_BY_PATH["mem:t1251-own"] = frozenset({(NORTH, ALOUETTE)})
    try:
        speak = {own: ("speaks", False, None, None), far: ("speaks", False, None, None)}
        P = Part(B, 7, False, False, "stream", None, {"RB": {"hatchery": speak, "wild": speak}}, (6, 1))
        got = EV.eval_sp(P, "RB", "wild")
        roles = {k: v["role"] for k, v in got["roles"].items()}
        assert (got["status"], got["win"].key, roles[far]) == ("closed", own, "agrees")
        assert got["liftNotes"] == []
        assert closure_scope(got["win"].species) == ("RB",) and closure_scope(B.rules[(ALOUETTE, "r1")]["species"]) == "trout_char"
    finally:
        read._OWN_BY_PATH.pop("mem:t1251-own")
        read._UPSTREAM_BY_PATH.pop("mem:t1251-own")


def test_a_lift_note_the_other_closure_on_the_rung_does_not_share_is_refused():
    """The named closure (the North Alouette's own) carries a qualified lift (a rule exempting it when targeting
    kokanee); the Alouette's closure on the same rung closes the fish without it: the note would read as an opening the
    other closure does not give. Refused by name; shared by both closures, it is shown as before."""
    from pipeline.deliver.answers.common import AnswersError
    from pipeline.deliver.answers.rows.part import Part
    lift = {"entry_id": NORTH, "rule_id": "r1", "when_targeting": ["KO"]}
    lifter = {**_gate("r2:lifter@2-8"), "family": "gear_and_method", "type": "tackle_restriction", "take": None,
              "rule": "r9", "rule_id": "r9", "exempts": [lift]}
    rules = {(NORTH, "r1"): _gate(NORTH), (ALOUETTE, "r1"): _gate(ALOUETTE), ("r2:lifter@2-8", "r9"): lifter}
    B = SimpleNamespace(sets={7: [(NORTH, "r1", "reach"), (ALOUETTE, "r1", "reach"), ("r2:lifter@2-8", "r9", "reach")]},
                        rules=rules, path="mem:t1251-lift")
    read._OWN_BY_PATH["mem:t1251-lift"] = {7: frozenset({NORTH})}
    read._UPSTREAM_BY_PATH["mem:t1251-lift"] = frozenset()
    speak = {f"{NORTH}::r1": ("speaks", False, None, None), f"{ALOUETTE}::r1": ("speaks", False, None, None)}
    try:
        P = Part(B, 7, False, False, "stream", None, {"RB": {"hatchery": speak, "wild": speak}}, (6, 1))
        with pytest.raises(AnswersError, match="would show beside a closure that still closes"):
            EV.eval_sp(P, "RB", "wild")
        lifter["exempts"] = [lift, {**lift, "entry_id": ALOUETTE}]          # both closures lifted alike: shown
        P = Part(B, 7, False, False, "stream", None, {"RB": {"hatchery": speak, "wild": speak}}, (6, 1))
        assert EV.eval_sp(P, "RB", "wild")["liftNotes"]
    finally:
        read._OWN_BY_PATH.pop("mem:t1251-lift")
        read._UPSTREAM_BY_PATH.pop("mem:t1251-lift")
