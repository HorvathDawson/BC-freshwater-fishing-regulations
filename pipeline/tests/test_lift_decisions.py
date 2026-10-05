"""THE LIFT DECISIONS OF 2026-10-05, each pinned on real sections (handoff P3 lift_audit.md).

The STRICT LIFT RULING: a zone closure and a water's row BOTH apply — the water is closed on the
union of their closed dates. A row beats a zone closure only where (a) the book prints an
exemption, or (b) the row prints a dated catch and release, opening or quota INSIDE the
closure — then on exactly those dates, for exactly those fish. Every `not in` below has a
positive control on the same section or the same rule (review F3: a `not in` on a key the rule
does not carry passes vacuously).

The bundle is `UI_EXPORT_BUNDLE`, else the shipped one; the tests skip on a bundle from before
these decisions (one still carrying `stein_river.r1x`).
"""
from __future__ import annotations

import json
import os
import sqlite3
from collections import defaultdict
from pathlib import Path

import pytest

from pipeline.deliver.bundle import read as R
from pipeline.deliver.bundle import rules as rules_mod

BUNDLE = str(Path(os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE))

STEIN = "r3:stein_river@3-16"
NAHATLATCH = "r3:nahatlatch_river@3-15"
NICOLA = "r3:nicola_river@3-13"
WEST_ROAD = "r5:west_road_blackwater_river@5-12+5-13"
WEST_ROAD_ITEM = "gnis:26104"
Z3_SPRING = "z3:spring_stream_closure::spring_stream_closure.r1"
Z5_SPRING = "z5:spring_stream_closure::spring_stream_closure.r1"
Z6_FRASER = "z6:iskut_fraser_closure::iskut_fraser_closure.r2"
Z7A_SPRING = "z7a:spring_stream_closure::spring_stream_closure.r1"
Z6_STEELHEAD = "z6:steelhead_stream_closure::steelhead_stream_closure.r1"
Z6_MAINSTEMS = "z6:steelhead_stream_closure::steelhead_stream_closure.r2"


@pytest.fixture(scope="module")
def db():
    if not Path(BUNDLE).exists():
        pytest.skip("no bundle")
    con = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    if con.execute("select count(*) from rule where entry_id = ? and rule_id = ?",
                   (STEIN, "stein_river.r1x")).fetchone()[0]:
        pytest.skip(f"{BUNDLE} predates the 2026-10-05 lift decisions — point UI_EXPORT_BUNDLE "
                    f"at a side build")
    yield con
    con.close()


def _sid(db, eid, rid, beside: str | None = None):
    """The first section the rule binds — with `beside` ("entry::rule"), one where that rule is
    bound too (a stream under the zone's stream closure, not a lake the row also names)."""
    q = ("select min(sr.sid) from ruleset r join section_ruleset sr on sr.set_id = r.set_id "
         "where r.entry_id = ? and r.rule_id = ?")
    args = [eid, rid]
    if beside:
        q += " and r.set_id in (select set_id from ruleset where entry_id = ? and rule_id = ?)"
        args += beside.split("::")
    sid = db.execute(q, args).fetchone()[0]
    assert sid is not None, (eid, rid, beside)
    return sid


def _closures(sid, on, fish) -> set:
    """Every FULL closure speaking for the fish on the day."""
    return {f"{x['entry']}::{x['rule']}" for x in R.effective_rules(sid, on, fish, BUNDLE)
            if x["state"] == "speaks" and rules_mod.closure_grade(x) == "full"}


def _speaks(sid, on, fish) -> set:
    return {f"{x['entry']}::{x['rule']}" for x in R.effective_rules(sid, on, fish, BUNDLE)
            if x["state"] == "speaks" and x["type"] == "retention_limit"}


def _bound(db, sid) -> set:
    return {f"{e}::{r}" for e, r in db.execute(
        "select r.entry_id, r.rule_id from section_ruleset sr join ruleset r "
        "on r.set_id = sr.set_id where sr.sid = ?", (sid,))}


# ---- 1. Stein: no printed basis, closed to Jun 30 -----------------------------------------------
def test_the_stein_is_closed_on_june_15_by_region_3s_spring_closure(db):
    """The p.28 line naming the Stein is a steelhead closure list, not a spring-closure exception
    (user decision 2026-10-05): the row's "No Fishing Jan 1-May 31" and Region 3's Jan 1-June 30
    spring closure both hold. MUTATION: restoring `stein_river.r1x` opens June."""
    sid = _sid(db, STEIN, "stein_river.r1", Z3_SPRING)
    assert Z3_SPRING in _closures(sid, (6, 15), "RB")
    # both on May 15; and from July 1 the water is open (the zone quota speaks)
    assert {Z3_SPRING, f"{STEIN}::stein_river.r1"} <= _closures(sid, (5, 15), "RB")
    assert _closures(sid, (7, 15), "RB") == set()
    assert "z3:trout_char_quota::trout_char_quota.r2" in _speaks(sid, (7, 15), "RB")


# ---- 2. Nahatlatch below the lake: both closures, closed to Jun 30 -----------------------------
def test_the_nahatlatch_below_its_lake_is_closed_on_june_15(db):
    """"No Fishing Jan 1-May 31" below the lake and Region 3's Jan 1-June 30 both hold (user
    decision 2026-10-05). MUTATION: restoring `nahatlatch_river.r2x` opens June."""
    sid = _sid(db, NAHATLATCH, "nahatlatch_river.r2", Z3_SPRING)
    assert Z3_SPRING in _closures(sid, (6, 15), "RB")
    assert {Z3_SPRING, f"{NAHATLATCH}::nahatlatch_river.r2"} <= _closures(sid, (5, 15), "RB")
    assert _closures(sid, (7, 15), "RB") == set()
    assert f"{NAHATLATCH}::nahatlatch_river.r3" in _speaks(sid, (7, 15), "RB")


# ---- 3. Nicola: trout catch and release Jan 1-Feb 28 only --------------------------------------
def test_the_nicola_below_its_lake_is_trout_catch_and_release_on_jan_15_whitefish_closed(db):
    """The row prints "Trout catch and release downstream of Nicola Lake, Jan 1-Feb 28": the lift
    is for trout, on those dates (rule (b)). A mountain whitefish stays under Region 3's spring
    closure. MUTATION: `nicola_river.r3x` back to ALL_GAME_FISH opens the whitefish."""
    lift = json.loads(db.execute("select exempts from rule where entry_id = ? and rule_id = ?",
                                 (NICOLA, "nicola_river.r3x")).fetchone()[0])
    assert [x["rule_id"] for x in lift] == ["spring_stream_closure.r1"] and lift[0]["species"]
    assert "MW" not in lift[0]["species"] and "RB" in lift[0]["species"]
    sid = _sid(db, NICOLA, "nicola_river.r3", Z3_SPRING)
    rb = _speaks(sid, (1, 15), "RB")
    assert f"{NICOLA}::nicola_river.r3" in rb and Z3_SPRING not in rb
    assert _closures(sid, (1, 15), "RB") == set()
    assert Z3_SPRING in _closures(sid, (1, 15), "MW")
    # Mar 1 on: the row's own closure and the spring closure, for trout too
    assert {Z3_SPRING, f"{NICOLA}::nicola_river.r2"} <= _closures(sid, (3, 15), "RB")


# ---- 4. G3: an exemption never carries into a region where the water has its own entry --------
def _entry(eid, matched, *, trib_only=False):
    return type("E", (), {"entry_id": eid, "matched": matched,
                          "rules": [type("Rl", (), {"tributaries_only": trib_only})()]})()


def test_an_exemption_carries_only_where_the_water_has_no_entry_of_its_own():
    """User ruling 2026-10-05 (G3 caveat): the Fraser has rows in Regions 3, 5 and 7, so Region 5's
    "Mainstem open all year" reaches neither Region 3 nor Zone 7A; West Road River has only
    TRIBUTARY rows in Region 6 and Zone 7A, so its Region 5 row reaches both; the Similkameen
    reaches Region 3 through its tributaries (co-bound), having no Region 3 row.
    MUTATION: dropping the `own` subtraction in `equivalent_regions` gives the Fraser {3, 7a}."""
    docs = [_entry("r3:fraser", ["F"]), _entry("r5:fraser", ["F"]), _entry("r7:fraser", ["F"]),
            _entry("r5:west_road", ["W"]), _entry("r6:west_road_tribs", ["W"], trib_only=True),
            _entry("r7:west_road_tribs", ["W"], trib_only=True), _entry("r8:similkameen", ["S"])]
    own = rules_mod.own_entry_regions(docs)
    assert own == {"F": {"3", "5", "7"}, "W": {"5"}, "S": {"8"}}
    water = {"r5:fraser": frozenset({"3", "5", "7a"}), "r5:west_road": frozenset({"5", "6", "7a"}),
             "r8:similkameen": frozenset({"8"})}
    co = {("r8:similkameen", "s.r3"): frozenset({"3", "8"})}
    assert rules_mod.equivalent_regions(docs[1], "f.r2", water, co, own) == {"5"}
    assert rules_mod.equivalent_regions(docs[3], "w.r6", water, co, own) == {"5", "6", "7a"}
    assert rules_mod.equivalent_regions(docs[6], "s.r3", water, co, own) == {"3", "8"}


def _equivalents(db, eid, rid) -> set:
    got = json.loads(db.execute("select exempts from rule where entry_id = ? and rule_id = ?",
                                (eid, rid)).fetchone()[0] or "[]")
    return {f"{x['entry_id']}::{x['rule_id']}" for x in got if x.get("equivalent")}


def test_the_bundles_equivalent_lifts_follow_the_own_entry_caveat(db):
    """On the bundle: the Fraser's and the Canim's carried lifts are gone; West Road keeps its
    Region 6 / Zone 7A ones; the Similkameen gains Region 3's (W6, 28 tributary sections)."""
    fraser = "r5:fraser_river@5-2"
    # positive control: the Fraser row still lifts its OWN region's closure, by name
    own = {f"{x['entry_id']}::{x['rule_id']}" for x in json.loads(db.execute(
        "select exempts from rule where entry_id = ? and rule_id = 'fraser_river.r2'",
        (fraser,)).fetchone()[0])}
    assert Z5_SPRING in own and _equivalents(db, fraser, "fraser_river.r2") == set()
    canim = "r5:canim_river_also_in_m_u_3_46@5-15"
    canim_lifts = {f"{x['entry_id']}::{x['rule_id']}" for x in json.loads(db.execute(
        "select exempts from rule where entry_id = ? and rule_id = 'canim_river.r1x'",
        (canim,)).fetchone()[0])}
    assert canim_lifts == {Z5_SPRING}
    assert {Z6_FRASER, Z7A_SPRING} <= _equivalents(db, WEST_ROAD, "west_road_blackwater_river.r6")
    assert Z3_SPRING in _equivalents(db, "r8:similkameen_river@8-2", "similkameen_river.r3")
    # and it takes effect: a Similkameen tributary section in Region 3 is open on Jun 15
    sid = db.execute(
        "select min(sr.sid) from ruleset a join ruleset b on b.set_id = a.set_id "
        "join section_ruleset sr on sr.set_id = a.set_id where a.entry_id = ? and a.rule_id = ? "
        "and b.entry_id = 'z3:spring_stream_closure'",
        ("r8:similkameen_river@8-2", "similkameen_river.r3")).fetchone()[0]
    assert sid is not None and Z3_SPRING in _bound(db, sid)
    assert _closures(sid, (6, 15), "RB") == set()


# ---- 6. West Road: the mainstem is lifted, no tributary is ------------------------------------
def _west_road(db):
    main = {s for (s,) in db.execute("select s.sid from item i join item_section s "
                                     "on s.ord = i.ord where i.item_id = ?", (WEST_ROAD_ITEM,))}
    up = defaultdict(list)
    for s, d in db.execute("select sid, down_sid from section_down"):
        up[d].append(s)
    seen, todo = set(main), list(main)
    while todo:
        for u in up.get(todo.pop(), ()):
            if u not in seen:
                seen.add(u)
                todo.append(u)
    tribs = (seen - main) | {s for (s,) in db.execute(
        "select distinct sr.sid from ruleset r join section_ruleset sr on sr.set_id = r.set_id "
        "where r.entry_id like 'r%:west_road_blackwater_river_s_tributaries@%'")}
    return main, tribs


@pytest.mark.parametrize("zone", [Z5_SPRING, Z6_FRASER, Z7A_SPRING])
def test_west_roads_tributaries_are_closed_on_june_20_and_its_mainstem_is_open(db, zone):
    """"No Fishing in mainstem (only) Nov 1-June 14; tributaries subject to spring closure"
    (p.47): in each of Region 5, Region 6 and Zone 7A a tributary is closed by its region's
    spring closure on Jun 20 and the mainstem is open (user decision 2026-10-05: no lift reaches
    a tributary). MUTATION: `includes_tributaries: true` on `west_road_blackwater_river.r6`
    lifts the tributaries; dropping its equivalents closes the Region 6 / 7A mainstem."""
    main, tribs = _west_road(db)
    in_zone = lambda s: zone in _bound(db, s)  # noqa: E731
    m = min(s for s in main if in_zone(s))
    t = min(s for s in tribs if in_zone(s))
    assert _closures(m, (6, 20), "RB") == set()
    assert zone in _closures(t, (6, 20), "RB")
    # the mainstem's own closure still holds to Jun 14 (control: the mainstem is not "open always")
    assert f"{WEST_ROAD}::west_road_blackwater_river.r1" in _closures(m, (6, 10), "RB")


# ---- 7. G4: Region 6's steelhead closure, the five mainstems exempt ---------------------------
def test_the_skeena_mainstem_is_open_for_steelhead_on_may_20_and_a_tributary_is_closed(db):
    """"No fishing: in all rivers and streams for steelhead, May 15-June 15. Exemptions include
    mainstem portions of the Skeena, Nass, Iskut, Stikine and Taku" (p.49). On a Skeena mainstem
    section below Cedarvale (no winter closure) a steelhead is not closed on May 20; on a Skeena
    tributary it is; and "open all year" (Station Creek) lifts only the winter closure (G4).
    MUTATION: the mainstem exemption binding the whole Skeena basin opens the tributary."""
    main = db.execute(
        "select min(sr.sid) from ruleset r join section_ruleset sr on sr.set_id = r.set_id "
        "join item_section s on s.sid = sr.sid join item i on i.ord = s.ord "
        "where r.entry_id = 'z6:steelhead_stream_closure' and r.rule_id = 'steelhead_stream_closure.r2' "
        "and i.item_id = 'gnis:2936' and r.set_id not in (select set_id from ruleset "
        "where entry_id = 'z6:skeena_nass_winter_closure')").fetchone()[0]
    assert main is not None and Z6_STEELHEAD in _bound(db, main)          # bound, and lifted
    assert _closures(main, (5, 20), "ST") == set()
    trib = db.execute(
        "select min(sr.sid) from ruleset r join section_ruleset sr on sr.set_id = r.set_id "
        "where r.entry_id = 'z6:skeena_nass_bait_ban' and r.rule_id = 'skeena_nass_bait_ban.r1' "
        "and r.set_id not in (select set_id from ruleset where entry_id = "
        "'z6:steelhead_stream_closure' and rule_id = 'steelhead_stream_closure.r2') "
        "and r.set_id not in (select set_id from ruleset where entry_id = "
        "'z6:skeena_nass_winter_closure') and r.set_id in (select set_id from ruleset where "
        "entry_id = 'z6:steelhead_stream_closure')").fetchone()[0]
    assert trib is not None and Z6_STEELHEAD in _closures(trib, (5, 20), "ST")
    station = _sid(db, "r6:station_creek@6-9", "station_creek.r3")
    assert Z6_STEELHEAD in _closures(station, (5, 20), "ST")
    assert _closures(station, (7, 20), "ST") == set()
    # the exemption binds the five mainstems and nothing else
    items = {i for (i,) in db.execute(
        "select distinct i.item_id from ruleset r join section_ruleset sr on sr.set_id = r.set_id "
        "join item_section s on s.sid = sr.sid join item i on i.ord = s.ord "
        "where r.entry_id = 'z6:steelhead_stream_closure' and r.rule_id = 'steelhead_stream_closure.r2'")}
    assert items == {"gnis:2936", "gnis:3206", "gnis:10765", "gnis:15430", "gnis:8231"}
