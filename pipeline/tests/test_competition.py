"""WHO SPEAKS FOR A FISH — the ladder, as `read.effective_rules`, on real sections.

The user's rulings (2026-09-24), each pinned on a water the book prints:

  * Competition is PER FISH: two rules compete only for the fish both speak for.
  * NAMING before PLACE: for a fish, a rule that names it beats a rule naming a group that holds
    it, even when the group rule is written for a more specific place; within one naming level
    the more specific place wins; so a water row naming the fish beats the zone's rule for it.
  * Competition is decided among the rules IN FORCE on the date asked.

The bundle is `UI_EXPORT_BUNDLE`, else the shipped one. The pure functions are checked without
one. Mutation: each test below was run against a broken `effective_rules` (naming ignored,
species ignored, dates ignored, families competing, closures displaceable) and fails.
"""
from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

import pytest

from pipeline.deliver.bundle import read as R

BUNDLE = str(Path(os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE))
DAILY = ("retention_limit", "daily")

KAKWA, CECILIA = "r7:kakwa_lake@7-19", "r7:cecilia_lake@7-19"
ZB = "z7b:trout_char_quota"
LIARD = "r7:liard_river_watershed_see_map_on_page_63@7-53"
WILLISTON = "r7:williston_lake_in_zone_b@7-31+7-36"
TCHES = "r6:tchesinkut_lake@6-4"
KOOC = "r4:koocanusa_reservoir@4-2+4-22+4-3"


# --------------------------------------------------------------------------- pure pieces
def test_a_rule_speaks_for_the_fish_its_species_hold():
    assert R.speaks_for({"species": ["TROUT_CHAR"]}, "BT")
    assert not R.speaks_for({"species": ["BT"]}, "RB")
    assert R.speaks_for({"species": []}, "RB")                      # names none: every fish
    assert R.speaks_for({"species": ["ALL_FIN_FISH"]}, "BB")
    assert not R.speaks_for({"species": ["ALL_GAME_FISH"], "species_except": ["RB"]}, "RB")
    assert not R.speaks_for({"when_targeting": ["WSG"]}, "RB")
    # "fin fish" is not crayfish (the trap rule releases fin fish and keeps the crayfish)
    assert not R.speaks_for({"species": ["ALL_FIN_FISH"]}, "CRA")


def test_naming_is_the_fish_itself_or_a_one_fish_group():
    assert R.names_fish({"species": ["BT"]}, "BT")
    assert not R.names_fish({"species": ["TROUT_CHAR"]}, "BT")
    assert R.names_fish({"species": ["CT"]}, "WCT")                 # "cutthroat" is one fish
    assert not R.names_fish({"species": ["ALL_GAME_FISH"]}, "BB")


def test_in_force_reads_dates_hours_and_unread_seasons():
    feb_jul_off = {"dates": [{"from_month": 8, "from_day": 1, "to_month": 1, "to_day": 31},
                             {"from_month": 3, "from_day": 1, "to_month": 6, "to_day": 30}]}
    assert R.in_force(feb_jul_off, (2, 10)) == "no" and R.in_force(feb_jul_off, (3, 1)) == "yes"
    assert R.in_force(None, (2, 10)) == "yes"
    assert R.in_force({"weekdays": ["Saturday"]}, (2, 10)) == "part"
    assert R.in_force({"unparsed": ["To be determined"]}, (2, 10)) == "part"


def _tiny(tmp_path, rules: list, via: dict | None = None) -> str:
    """A bundle of one section carrying `rules` — enough for `effective_rules`, which reads the
    section's ruleset from sqlite and the rules through `_rules_of` (seeded here)."""
    path = str(tmp_path / "tiny.sqlite")
    con = sqlite3.connect(path)
    con.execute("create table section_ruleset (sid integer, set_id integer)")
    con.execute("create table ruleset (set_id integer, entry_id text, rule_id text, via text)")
    con.execute("insert into section_ruleset values (1, 0)")
    con.executemany("insert into ruleset values (0, ?, ?, ?)",
                    [(x["entry"], x["rule"], (via or {}).get(x["rule"], "reach")) for x in rules])
    con.commit()
    con.close()
    R._RULES_BY_PATH[path] = {(x["entry"], x["rule"]): dict(
        {"type": "retention_limit", "dimension": "daily", "family": "retention"}, **x)
        for x in rules}
    return path


def test_a_quota_and_its_clause_never_displace_each_other(tmp_path):
    """A clause bound to a narrower place than its parent ranks better by place; it is still one
    statement with its parent, and the parent keeps speaking beside it."""
    path = _tiny(tmp_path, [
        {"entry": "z9:q", "rule": "q.r1", "species": ["TROUT_CHAR"], "take": 5, "_rank": 3},
        {"entry": "z9:q", "rule": "q.r2", "species": ["TROUT_CHAR"], "take": 1, "_rank": 2,
         "within": "q.r1"}])
    assert {x["rule"] for x in R.effective_rules(1, (7, 1), "RB", path)} == {"q.r1", "q.r2"}


def test_a_water_rule_reached_by_the_tributary_walk_speaks_at_the_inherited_rung(tmp_path):
    """The creek's own quota beats the downstream river's, which reaches it by the walk."""
    path = _tiny(tmp_path, [
        {"entry": "r9:creek", "rule": "creek.r1", "species": ["TROUT"], "take": 2, "_rank": 0},
        {"entry": "r9:river", "rule": "river.r1", "species": ["TROUT"], "take": 4, "_rank": 0}],
        via={"river.r1": "trib"})
    assert [x["rule"] for x in R.effective_rules(1, (7, 1), "RB", path)] == ["creek.r1"]


def test_a_closure_is_never_displaced_and_a_part_day_rule_stands_beside(tmp_path):
    """The zone's closure is outranked by the water's bull trout rule (same naming, better place)
    and still speaks; the part-day rule neither displaces nor is displaced."""
    path = _tiny(tmp_path, [
        {"entry": "z9:z", "rule": "z.r0", "species": ["ALL_GAME_FISH"], "take": 0,
         "may_target": 0, "_rank": 3},
        {"entry": "r9:w", "rule": "w.r1", "species": ["BT"], "take": 0, "may_target": 1,
         "_rank": 0},
        {"entry": "z9:z", "rule": "z.r2", "species": ["BT"], "take": 2, "_rank": 3,
         "when": {"weekdays": ["Saturday"]}}])
    got = {x["rule"]: x["state"] for x in R.effective_rules(1, (7, 1), "BT", path)}
    assert got == {"z.r0": "speaks", "w.r1": "speaks", "z.r2": "beside"}


# --------------------------------------------------------------------------- the bundle
@pytest.fixture(scope="module")
def db():
    if not Path(BUNDLE).exists():
        pytest.skip("no bundle")
    con = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    if not con.execute("select count(*) from rule where entry_id=? and rule_id=?",
                       (ZB, "trout_char_quota.r10")).fetchone()[0]:
        pytest.skip(f"{BUNDLE} predates the zone decisions — point UI_EXPORT_BUNDLE at a side build")
    yield con
    con.close()


def _sid(db, eid, rid, *, without=()):
    """The first section a rule binds, optionally on a set that holds none of `without`."""
    q = ("select min(sr.sid) from ruleset r join section_ruleset sr on sr.set_id = r.set_id "
         "where r.entry_id = ? and r.rule_id = ?")
    args = [eid, rid]
    for e_like, r_id in without:
        q += (" and r.set_id not in (select set_id from ruleset where entry_id like ?"
              + (" and rule_id = ?" if r_id else "") + ")")
        args += [e_like] + ([r_id] if r_id else [])
    sid = db.execute(q, args).fetchone()[0]
    assert sid is not None, (eid, rid, without)
    return sid


def _speaks(sid, on, fish, key=DAILY) -> set:
    return {f"{x['entry']}::{x['rule']}" for x in R.effective_rules(sid, on, fish, BUNDLE)
            if x["state"] == "speaks" and (x["type"], x["dimension"]) == key}


@pytest.mark.parametrize("eid", [KAKWA, CECILIA])
def test_kakwa_and_cecilia_bull_trout_are_released_other_trout_keep_the_lakes_two(db, eid):
    """The lake prints "Trout/char daily quota = 2 (none under 40 cm)"; Zone B prints bull trout
    release in the Peace River watershed all year. For a bull trout — 40 cm and over included —
    the zone's release NAMES the fish and speaks: none may be kept. For every other trout or char
    the lake's 2 speaks — and, since 2026-09-25, BESIDE the zone's "Trout/char: 5" and its "1 over
    50 cm" (quotas sit beside: the lake's 2 with its 40 cm floor is not the zone's statement, and the
    zone's 5 is a day's total over every water of Zone B)."""
    slug = eid.split(":")[1].split("@")[0]
    sid = _sid(db, eid, f"{slug}.r2")
    assert _speaks(sid, (7, 1), "BT") == {f"{ZB}::trout_char_quota.r9"}
    got = R.effective_rules(sid, (7, 1), "BT", BUNDLE)
    bt = [x for x in got if x["state"] == "speaks" and (x["type"], x["dimension"]) == DAILY]
    assert [(x["take"], x["may_target"]) for x in bt] == [(0, 1)]
    for fish in ("RB", "LT", "EB"):
        assert _speaks(sid, (7, 1), fish) == {f"{eid}::{slug}.r2", f"{ZB}::trout_char_quota.r1",
                                              f"{ZB}::trout_char_quota.r2"} | (
            {f"{ZB}::trout_char_quota.r4"} if fish == "LT" else set()), fish      # "2 lake trout"
    # the lake's own closure is never displaced, whatever names the fish
    assert f"{eid}::{slug}.r1" in _speaks(sid, (12, 1), "BT")


def test_zone_b_bull_trout_release_speaks_only_for_bull_trout(db):
    """Off every water row and outside the Peace: "Bull trout … release" (r10) takes bull trout
    from "Trout/char: 5" and nothing else — the 5 (and its clauses) still speak for the rest."""
    sid = _sid(db, ZB, "trout_char_quota.r10",
               without=(("r7:%", None), (ZB, "trout_char_quota.r9")))
    assert _speaks(sid, (7, 1), "BT") == {f"{ZB}::trout_char_quota.r10"}
    assert _speaks(sid, (7, 1), "RB") == {f"{ZB}::trout_char_quota.r1",
                                          f"{ZB}::trout_char_quota.r2"}
    assert _speaks(sid, (7, 1), "LT") == {f"{ZB}::trout_char_quota.r1",
                                          f"{ZB}::trout_char_quota.r2",
                                          f"{ZB}::trout_char_quota.r4"}


@pytest.mark.parametrize("eid,rid,out_of_season", [
    (WILLISTON, "williston_lake_zone_b.r4", {f"{ZB}::trout_char_quota.r9"}),
    (LIARD, "liard_river_watershed.r2", {f"{LIARD}::liard_river_watershed.r1"}),
])
def test_a_water_row_naming_bull_trout_beats_the_zone(db, eid, rid, out_of_season):
    """Williston Lake (Zone B) and the Liard watershed print "Bull trout daily quota = 1 (none
    under 30 cm or over 50 cm), Oct 16-Aug 14". Both name bull trout, as the zone does, so the
    water's quota speaks on its dates; on Aug 15-Oct 15 it is not in force and the release
    speaks (the Liard's own, Williston's from the zone's Peace line)."""
    sid = _sid(db, eid, rid)
    assert _speaks(sid, (7, 1), "BT") == {f"{eid}::{rid}"}
    assert _speaks(sid, (9, 1), "BT") == out_of_season


def test_a_within_clause_is_named_at_its_parents_level(db):
    """Region 4's "Trout/char: 5, but not more than … 1 bull trout" is a trout/char quota. Read
    as a bull trout rule it would outrank Quinn Creek's "Trout/char catch and release" for bull
    trout and let one be kept on a catch-and-release stream."""
    sid = _sid(db, "r4:quinn_creek@4-22", "quinn_creek.r1")
    got = _speaks(sid, (7, 1), "BT")
    assert "r4:quinn_creek@4-22::quinn_creek.r1" in got
    assert not {k for k in got if k.startswith("z4:trout_char_quota::")}


def test_a_lift_for_one_fish_lifts_the_rule_for_that_fish_only(db):
    """Duncan River: "exempt from regional Nov 1-Mar 31 bull trout catch and release" lifts
    Region 4's winter trout/char release for BULL TROUT; a rainbow on Dec 1 is still released."""
    sid = _sid(db, "r4:duncan_river@4-19", "duncan_river.r2")
    rel = "z4:trout_char_winter_release::trout_char_winter_release.r1"
    stream = ("retention_limit", "daily@water=stream")
    assert rel in _speaks(sid, (12, 1), "RB", stream)
    assert rel not in _speaks(sid, (12, 1), "BT", stream)


def test_tchesinkut_february_and_july_are_the_regions(db):
    """"Lake trout catch and release EXCEPT during months of February and July (when regional
    quotas apply)". In February the region's quota speaks WHOLE — the 5, its "1 over 50 cm" and
    its "3 Dolly Varden/bull trout and/or lake trout" are one statement and do not displace one
    another; in March the lake's release speaks."""
    sid = _sid(db, TCHES, "tchesinkut_lake.r1")
    region = {f"z6:trout_char_quota::trout_char_quota.r{k}" for k in (1, 2, 3)}
    assert _speaks(sid, (2, 10), "LT") == region
    assert _speaks(sid, (7, 10), "LT") == region
    assert _speaks(sid, (3, 10), "LT") == {f"{TCHES}::tchesinkut_lake.r1"}
    assert _speaks(sid, (8, 1), "LT") == {f"{TCHES}::tchesinkut_lake.r1"}


def test_koocanusa_bull_trout_are_released_nov_to_mar_only(db):
    """"Bull trout catch and release Nov 1-Mar 31; no bull trout under 75 cm when open". In
    force, the release names bull trout on the water and speaks; from Apr 1 the region's quota
    does. The size limit is its own subject and holds all year. A rainbow is never touched."""
    sid = _sid(db, KOOC, "koocanusa_reservoir.r1")
    rel = {f"{KOOC}::koocanusa_reservoir.r1"}
    assert _speaks(sid, (11, 1), "BT") == rel and _speaks(sid, (3, 31), "BT") == rel
    assert _speaks(sid, (12, 1), "BT") == rel
    zone = {"z4:trout_char_quota::trout_char_quota.r1", "z4:trout_char_quota::trout_char_quota.r4"}
    assert _speaks(sid, (4, 1), "BT") == zone and _speaks(sid, (6, 1), "BT") == zone
    size = ("retention_limit", "daily/size")
    assert _speaks(sid, (12, 1), "BT", size) == _speaks(sid, (6, 1), "BT", size) == {
        f"{KOOC}::koocanusa_reservoir.r2"}
    assert f"{KOOC}::koocanusa_reservoir.r1" not in _speaks(sid, (12, 1), "RB")


def test_a_fish_is_asked_about_by_its_leaf_code(tmp_path):
    """"CT" is the book's word for two fish; a rule about cutthroat expands to WCT and CCT, so a
    question about "CT" would match nothing and read as "no rule speaks"."""
    path = _tiny(tmp_path, [{"entry": "z9:q", "rule": "q.r1", "species": ["CT"], "take": 2,
                             "_rank": 3}])
    with pytest.raises(ValueError, match="is a group"):
        R.effective_rules(1, (7, 1), "CT", path)
    assert [x["rule"] for x in R.effective_rules(1, (7, 1), "WCT", path)] == ["q.r1"]


def test_a_water_closure_silences_a_zone_quota_that_names_the_fish(tmp_path):
    """A closure counts as naming every fish it covers: Clearwater Lake's "No Fishing Nov 1-Apr
    30" is not a group rule the zone's "Burbot: 5" outranks by naming — on its dates the lake is
    closed and the 5 is not an answer."""
    path = _tiny(tmp_path, [
        {"entry": "r9:lake", "rule": "lake.r1", "take": 0, "may_target": 0, "_rank": 0,
         "when": {"dates": [{"from_month": 11, "from_day": 1, "to_month": 4, "to_day": 30}]}},
        {"entry": "z9:q", "rule": "q.r1", "species": ["BB"], "take": 5, "_rank": 3}])
    assert [x["rule"] for x in R.effective_rules(1, (11, 15), "BB", path)] == ["lake.r1"]
    assert [x["rule"] for x in R.effective_rules(1, (7, 1), "BB", path)] == ["q.r1"]


def test_clearwater_lake_is_closed_to_burbot_in_winter(db):
    sid = _sid(db, "r7:clearwater_lake@7-31", "clearwater_lake.r2")
    assert _speaks(sid, (11, 15), "BB") == {"r7:clearwater_lake@7-31::clearwater_lake.r1"}
    assert _speaks(sid, (7, 1), "BB") == {"r7:clearwater_lake@7-31::clearwater_lake.r2"}


def test_a_catch_and_release_row_lifts_the_zone_quotas_that_name_a_fish(db):
    """Pine River: "Catch and release all fish upstream of the Hasler Road Bridge". The zone's
    "Burbot: 5" and "Arctic grayling: 2" name their fish and would outrank the row for them; the
    row lifts them, so the release is what speaks."""
    sid = _sid(db, "r7:pine_river@7-32", "pine_river.r1")
    for fish in ("BB", "GR", "NP", "WP", "KO"):
        assert _speaks(sid, (7, 1), fish) == {"r7:pine_river@7-32::pine_river.r1"}, fish


def test_the_guide_states_the_ruling(db):
    from pipeline.tools import export_ui_rules as X
    g = X.build(Path(BUNDLE))["guide"]["ladder"]
    assert "effective_rules" in g["reference"]
    assert "PER FISH" in g["per_fish"] and "Trout/char: 5" in g["per_fish"]
    words = g["who_speaks"]
    for said in ("NAMES the fish", "Kakwa Lake", "40 cm and over", "Williston Lake",
                 "Liard River watershed", "within"):
        assert said in words, said
    # the earlier rulings stay
    assert "counts as naming every fish it covers" in g["closures"]
    assert "IN FORCE" in g["competition"] and "A lift is in force only while its lifter is" in \
        g["competition"]
    # the water-release ruling (2026-09-25)
    for said in ("WATER'S RELEASE SILENCES THE ZONE", "Coquihalla", "Chilliwack",
                 "every origin", "another fish"):
        assert said in g["water_release"], said


# --------------------------------------------------------------------------- corpus-wide guards
def _sets(db):
    rows: dict = {}
    for s, e, r in db.execute("select set_id, entry_id, rule_id from ruleset"):
        rows.setdefault(s, []).append((e, r))
    rep = dict(db.execute("select set_id, min(sid) from section_ruleset group by set_id"))
    return rows, rep


#: The days a guard asks about: the 1st and the 15th of every month.
DAYS = [(m, d) for m in range(1, 13) for d in (1, 15)]


@pytest.mark.slow
def test_no_zone_rule_naming_a_fish_reopens_what_a_water_row_withheld(db):
    """THE NAMING RULING'S ONE HAZARD. A zone rule that names a fish outranks a water rule that
    names a group — right when the zone rule is the stricter (Kakwa's bull trout), wrong when it
    is the looser: "Burbot: 5" outranking a water's "Catch and release" lets an angler keep what
    the row says to release. Five rows were in that state; each now LIFTS the zone quotas it must
    speak over. This finds any new one: a water rule displaced ONLY by the naming ruling, by a
    zone rule of the same subject that allows more of that fish."""
    every = R._rules_of(BUNDLE)
    sets, rep = _sets(db)
    from pipeline.regs.parsing.catalogue import _ONE_FISH_GROUPS, expand_species

    def allows(x):
        return 999 if x.get("unlimited") else x.get("take")

    bad = set()
    for s, rows in sets.items():
        fish = set()
        for k in rows:
            x = every.get(k)
            if x is None or k[0].startswith("r") or x.get("within"):
                continue
            for c in x.get("species") or []:
                fish |= (set(expand_species([c])) if c in _ONE_FISH_GROUPS
                         else {c} if expand_species([c]) == [c] else set())
        here = [every[k] for k in rows if k in every]
        for f in sorted(fish):
            # only where a water rule for a GROUP holding the fish shares a subject with a zone
            # rule NAMING it can the ruling have moved anything
            named = {(x["type"], x["dimension"]) for x in here if not x["entry"].startswith("r")
                     and R.names_fish(x, f) and not x.get("within")}
            group = {(x["type"], x["dimension"]) for x in here if x["entry"].startswith("r")
                     and R.speaks_for(x, f) and not R.names_fish(x, f)}
            if not named & group:
                continue
            for on in DAYS:
                now = {(x["entry"], x["rule"]) for x in R.effective_rules(rep[s], on, f, BUNDLE)
                       if x["state"] == "speaks"}
                was = {(x["entry"], x["rule"]) for x in
                       R.effective_rules(rep[s], on, f, BUNDLE, by_naming=False)
                       if x["state"] == "speaks"}
                for w in was - now:
                    if not w[0].startswith("r"):
                        continue
                    for z in now - was:
                        W, Z = every[w], every[z]
                        if (W["type"], W["dimension"]) == (Z["type"], Z["dimension"]) and \
                                allows(W) is not None and allows(Z) is not None and \
                                allows(Z) > allows(W):
                            bad.add((f"{w[0]}::{w[1]}", f"{z[0]}::{z[1]}", f))
    assert not bad, sorted(bad)


# --------------------------------------------------------------------------- ruling of 2026-09-25
#: A water's outright release, and Region 2's conditioned quotas it must silence.
def _release_case(tmp_path, water: dict, extra: list | None = None) -> str:
    return _tiny(tmp_path, [
        dict({"entry": "r9:w", "rule": "w.r1", "take": 0, "may_target": 1, "_rank": 0}, **water),
        {"entry": "z9:q", "rule": "q.r1", "species": ["TROUT_CHAR"], "take": 4, "_rank": 3},
        {"entry": "z9:q", "rule": "q.r3", "species": ["ST"], "take": 2, "origin": "hatchery",
         "dimension": "daily@origin=hatchery", "_rank": 3, "within": "q.r1"},
        {"entry": "z9:q", "rule": "q.r4", "species": ["TROUT_CHAR"], "take": 2,
         "origin": "hatchery", "water": "stream", "dimension": "daily@origin=hatchery&water=stream",
         "_rank": 3, "within": "q.r1"},
        {"entry": "z9:q", "rule": "q.r6", "species": ["TROUT_CHAR"], "take": 0, "may_target": 1,
         "origin": "wild", "water": "stream", "dimension": "daily@origin=wild&water=stream",
         "_rank": 3},
        {"entry": "z9:q", "rule": "q.r8", "species": ["TROUT_CHAR"], "origin": "hatchery",
         "lengths": [{"max_cm": 30, "take": 0}], "dimension": "daily/size@origin=hatchery",
         "_rank": 3},
        {"entry": "zp:s", "rule": "s.r1", "species": ["ST"], "take": 10, "origin": "hatchery",
         "period": "annual", "dimension": "annual@origin=hatchery", "_rank": 4},
        {"entry": "zp:s", "rule": "s.r4", "species": ["ST"], "origin": "hatchery",
         "record_retention": True, "dimension": "daily@origin=hatchery&record", "_rank": 4},
    ] + (extra or []))


def _kept(sid, on, fish) -> set:
    """Every retention rule that speaks for the fish, whatever its key."""
    return {f"{x['entry']}::{x['rule']}" for x in R.effective_rules(sid, on, fish, BUNDLE)
            if x["state"] == "speaks" and x["type"] == "retention_limit"}


def _speak(path, fish, on=(12, 1)) -> set:
    return {x["rule"] for x in R.effective_rules(1, on, fish, path) if x["state"] == "speaks"}


def test_a_water_release_silences_the_zones_conditioned_quotas_for_that_fish(tmp_path):
    """USER RULING (2026-09-25). Coquihalla's "Trout/char (including steelhead) catch and release"
    never met Region 2's "2 hatchery steelhead" — the zone quota's conditions are in its key — so
    both spoke. A water's outright release displaces every zone/provincial quota that would keep
    the fish, whatever its conditions; the zone's own release and its record duty stand beside."""
    path = _release_case(tmp_path, {"species": ["TROUT_CHAR"]})
    assert _speak(path, "ST") == {"w.r1", "q.r6", "s.r4"}
    assert _speak(path, "RB") == {"w.r1", "q.r6"}


def test_a_water_release_for_another_fish_displaces_nothing(tmp_path):
    """Per fish: a water's bull trout release says nothing about a rainbow or a steelhead."""
    path = _release_case(tmp_path, {"species": ["BT"]})
    assert _speak(path, "ST") == {"q.r1", "q.r3", "q.r4", "q.r6", "q.r8", "s.r1", "s.r4"}
    assert _speak(path, "RB") == {"q.r1", "q.r4", "q.r6", "q.r8"}
    assert _speak(path, "BT") == {"w.r1", "q.r6"}


def test_a_release_of_one_origin_silences_a_quota_only_when_every_origin_is_released(tmp_path):
    """Chilliwack's "hatchery cutthroat catch and release" + the zone's "wild trout/char from
    streams" release every cutthroat: "Trout/char: 4" is silent. Morris Lake's "Wild trout/char
    catch and release" on a lake (no zone release of hatchery fish there) leaves the 4 speaking
    for hatchery trout — and never touches a hatchery-only quota."""
    path = _release_case(tmp_path, {"species": ["CT"], "origin": "hatchery",
                                    "dimension": "daily@origin=hatchery"})
    assert _speak(path, "WCT") == {"w.r1", "q.r6"}
    (tmp_path / "lake").mkdir()
    lake = _tiny(tmp_path / "lake", [
        {"entry": "r9:m", "rule": "m.r1", "species": ["TROUT_CHAR"], "take": 0, "may_target": 1,
         "origin": "wild", "dimension": "daily@origin=wild", "_rank": 0},
        {"entry": "z9:q", "rule": "q.r1", "species": ["TROUT_CHAR"], "take": 4, "_rank": 3},
        {"entry": "z9:q", "rule": "q.r9", "species": ["TROUT_CHAR"], "take": 2,
         "origin": "hatchery", "dimension": "daily@origin=hatchery", "_rank": 3}])
    assert _speak(lake, "RB") == {"m.r1", "q.r1", "q.r9"}


def test_a_size_class_release_silences_nothing(tmp_path):
    """"No wild trout over 50 cm" releases a size class; the zone's 4 still speaks for the rest."""
    path = _release_case(tmp_path, {"species": ["TROUT"], "origin": "wild",
                                    "lengths": [{"min_cm": 50, "take": 0}],
                                    "dimension": "daily@origin=wild"})
    assert "q.r1" in _speak(path, "RB") and "q.r4" in _speak(path, "RB")


def test_a_zone_release_does_not_silence_the_zone(tmp_path):
    """Only a release written for the WATER (or reaching it by the tributary walk) speaks over the
    zone; a zone release beside a zone quota is the ladder's business, as before."""
    path = _tiny(tmp_path, [
        {"entry": "z9:a", "rule": "a.r1", "species": ["TROUT_CHAR"], "take": 0, "may_target": 1,
         "_rank": 2},
        {"entry": "z9:q", "rule": "q.r4", "species": ["TROUT_CHAR"], "take": 2,
         "origin": "hatchery", "dimension": "daily@origin=hatchery", "_rank": 3}])
    assert _speak(path, "RB") == {"a.r1", "q.r4"}


def test_the_release_guard_is_what_silences_them(tmp_path, monkeypatch):
    """MUTATION: with the guard's predicate broken the conditioned quotas speak again — the
    tests above fail against the ladder without step 5."""
    from pipeline.deliver.bundle import rules as rules_mod
    path = _release_case(tmp_path, {"species": ["TROUT_CHAR"]})
    monkeypatch.setattr(rules_mod, "release_origins", lambda x: None)
    assert {"q.r3", "q.r4", "s.r1"} <= _speak(path, "ST")
    monkeypatch.undo()
    monkeypatch.setattr(rules_mod, "yields_to_release", lambda x: None)
    assert {"q.r3", "q.r4", "s.r1"} <= _speak(path, "ST")


def test_coquihalla_releases_every_trout_and_steelhead_below_the_tunnels_in_winter(db):
    """p.22: "Trout/char (including steelhead) catch and release, bait ban, downstream of the
    southern entrance to the lower most railway tunnel, Nov 1-Mar 31". Region 2's "2 hatchery
    steelhead over 50 cm", "2 from streams (must be hatchery)", "hatchery trout/char under 30 cm
    from streams" and the province's annual 10 hatchery steelhead no longer speak there."""
    eid = "r2:coquihalla_river@2-17"
    sid = _sid(db, eid, "coquihalla_river.r6")
    for fish in ("ST", "RB", "WCT", "BT"):
        got = _kept(sid, (12, 1), fish)
        assert f"{eid}::coquihalla_river.r6" in got, fish
        assert not {x for x in got if x.startswith(("z2:trout_char_quota::", "zp:steelhead::"))
                    and x.split("::")[1] in ("trout_char_quota.r1", "trout_char_quota.r2",
                                             "trout_char_quota.r3", "trout_char_quota.r4",
                                             "trout_char_quota.r5", "trout_char_quota.r5b",
                                             "trout_char_quota.r8", "steelhead.r1")}, (fish, got)


def test_chilliwack_releases_every_cutthroat_in_may(db):
    """p.22, Chilliwack/Vedder downstream of Vedder Crossing, (a) May 1-31: "hatchery cutthroat
    catch and release". With the zone's release of wild trout/char from streams, every cutthroat is
    released: the zone's cutthroat quotas are silent in May and speak again in August."""
    eid = "r2:chilliwack_vedder_rivers_does_not_include_sumas_river_see_ma@2-4"
    sid = _sid(db, eid, "chilliwack_vedder_rivers.r7")
    may = _kept(sid, (5, 15), "WCT")
    assert f"{eid}::chilliwack_vedder_rivers.r7" in may
    assert "z2:trout_char_quota::trout_char_quota.r6" in may
    for rid in ("trout_char_quota.r1", "trout_char_quota.r2", "trout_char_quota.r4",
                "trout_char_quota.r8"):
        assert f"z2:trout_char_quota::{rid}" not in may, rid
    assert "z2:trout_char_quota::trout_char_quota.r1" in _kept(sid, (8, 1), "WCT")


def test_a_looser_zone_rule_naming_the_fish_never_beats_a_waters_release(tmp_path):
    """REVIEW FIX (2026-09-25). Adams River's "Rainbow trout and char catch and release" lost step 4
    to Region 3's "Lake trout: none under 60 cm" (it NAMES lake trout; the water row names a
    group), so for a lake trout the size rule spoke and the release did not. Naming lets a
    STRICTER zone rule beat a water's group (Kakwa); a looser one never reopens what the water
    released: the release speaks again and the zone's keeping rule is silenced."""
    path = _tiny(tmp_path, [
        {"entry": "r9:w", "rule": "w.r1", "species": ["TROUT_CHAR"], "take": 0, "may_target": 1,
         "_rank": 0},
        {"entry": "z9:q", "rule": "q.r4", "species": ["LT"], "take": 1, "_rank": 3},
        {"entry": "z9:q", "rule": "q.r4b", "species": ["LT"], "lengths": [{"max_cm": 60, "take": 0}],
         "dimension": "daily/size", "_rank": 3, "within": "q.r4"}])
    assert _speak(path, "LT") == {"w.r1"}


def test_a_water_release_beaten_by_a_named_zone_release_still_silences_the_zone(tmp_path):
    """REVIEW FIX. Pine River's "Catch and release all fish" loses step 4 to Zone B's "Bull trout …
    release" (both releases; the zone names the fish) — and Zone B's "2 from streams" kept
    speaking for bull trout on 6,818 sections, because step 5 read only step 4's survivors. The
    water still released the fish."""
    path = _tiny(tmp_path, [
        {"entry": "r9:w", "rule": "w.r1", "species": ["ALL_GAME_FISH"], "take": 0,
         "may_target": 1, "_rank": 0},
        {"entry": "z9:q", "rule": "q.r9", "species": ["BT"], "take": 0, "may_target": 1,
         "_rank": 2},
        {"entry": "z9:q", "rule": "q.r3", "species": ["TROUT_CHAR"], "take": 2, "water": "stream",
         "dimension": "daily@water=stream", "_rank": 3}])
    assert _speak(path, "BT") == {"q.r9"}


def test_a_release_beaten_by_a_superior_quota_releases_nothing(tmp_path):
    """A release displaced by a SUPERIOR authority that keeps the fish stays displaced and silences
    nothing; one displaced by a superior closure (a national park) still silences the region's
    conditioned quota — nothing the region keeps survives a park closure."""
    rows = [
        {"entry": "r9:w", "rule": "w.r1", "species": ["BT"], "take": 0, "may_target": 1,
         "_rank": 0},
        {"entry": "z9:q", "rule": "q.r3", "species": ["TROUT_CHAR"], "take": 2, "water": "stream",
         "dimension": "daily@water=stream", "_rank": 3}]
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    keeps = _tiny(tmp_path / "a", rows + [
        {"entry": "zp:x", "rule": "x.r1", "species": ["BT"], "take": 1, "_rank": -1}])
    assert _speak(keeps, "BT") == {"x.r1", "q.r3"}
    park = _tiny(tmp_path / "b", rows + [
        {"entry": "zp:x", "rule": "x.r1", "species": ["ALL_GAME_FISH"], "take": 0,
         "may_target": 0, "_rank": -1}])
    assert _speak(park, "BT") == {"x.r1"}


# --------------------------------------------------------------------------- rulings of 2026-09-25 (R2)
# C. QUOTAS SIT BESIDE. A water's quota and the zone's both speak unless they state EXACTLY the
#    same thing; only then does the water's number displace the zone's.
def _quota_case(tmp_path, water: dict) -> str:
    return _tiny(tmp_path, [
        dict({"entry": "r9:w", "rule": "w.r1", "_rank": 0}, **water),
        {"entry": "z9:q", "rule": "q.r1", "species": ["TROUT_CHAR"], "take": 5, "_rank": 3},
        {"entry": "z9:k", "rule": "k.r1", "species": ["KO"], "take": 5, "_rank": 3}])


def test_a_water_quota_sits_beside_the_zones_total(tmp_path):
    """"Rainbow trout daily quota = 2" at a lake and the region's "Trout/char: 5": the 5 is a day's
    total over every water of the region, so both hold — never the lake's 2 alone."""
    path = _quota_case(tmp_path, {"species": ["RB"], "take": 2})
    assert _speak(path, "RB", (7, 1)) == {"w.r1", "q.r1"}


def test_a_size_bound_or_an_origin_is_a_different_statement(tmp_path):
    """Kakwa Lake's "Trout/char daily quota = 2 (none under 40 cm)" and Dodd Lake's "Wild trout/char
    daily quota = 2" are not the zone's "Trout/char: 5": each sits beside it."""
    for i, water in enumerate(({"species": ["TROUT_CHAR"], "take": 2,
                                "lengths": [{"min_cm": 40}, {"max_cm": 40, "take": 0}]},
                               {"species": ["TROUT_CHAR"], "take": 2, "origin": "wild"})):
        (tmp_path / str(i)).mkdir()
        path = _quota_case(tmp_path / str(i), water)
        assert _speak(path, "RB", (7, 1)) == {"w.r1", "q.r1"}, water


def test_the_same_statement_with_another_number_displaces_the_zones(tmp_path):
    """A lake's "Kokanee daily quota = 10" states exactly what the region's "Kokanee: 5" states: the
    water's number replaces the zone's. MUTATION: with `same_statement` always false the zone's 5
    speaks beside it."""
    path = _quota_case(tmp_path, {"species": ["KO"], "take": 10})
    assert _speak(path, "KO", (7, 1)) == {"w.r1"}
    import pipeline.deliver.bundle.rules as rules_mod
    real = rules_mod.same_statement
    try:
        rules_mod.same_statement = lambda a, b: False
        assert _speak(path, "KO", (7, 1)) == {"w.r1", "k.r1"}
    finally:
        rules_mod.same_statement = real


def test_the_sit_beside_rule_is_what_keeps_the_zone_speaking(tmp_path):
    """MUTATION: with `same_statement` always true (the ladder before the ruling) the water's
    rainbow quota silences the zone's 5 — the test above fails against it."""
    import pipeline.deliver.bundle.rules as rules_mod
    path = _quota_case(tmp_path, {"species": ["RB"], "take": 2})
    real = rules_mod.same_statement
    try:
        rules_mod.same_statement = lambda a, b: True
        assert _speak(path, "RB", (7, 1)) == {"w.r1"}
    finally:
        rules_mod.same_statement = real


def test_kitimat_dodd_and_morris_quotas_sit_beside_the_regions(db):
    """The reviewer's pairs: Kitimat's "Hatchery steelhead … daily quota = 2" beside Region 6's
    "Trout/char: 5"; Dodd Lake's "Wild trout/char daily quota = 2" beside Region 2's 4; Morris
    Lake's "hatchery trout/char daily quota = 2 (none under 30 cm)" beside the 4."""
    kit = "r6:kitimat_river_angling_regulations_for_the_kitimat_river_are@6-3"
    sid = _sid(db, kit, "kitimat_river.r5")
    got = _kept(sid, (8, 1), "ST")
    assert {f"{kit}::kitimat_river.r5", "z6:trout_char_quota::trout_char_quota.r1"} <= got
    for eid, rid in (("r2:dodd_lake@2-12", "dodd_lake.r1"), ("r2:morris_lake@2-19", "morris_lake.r2")):
        sid = _sid(db, eid, rid)
        got = _kept(sid, (8, 1), "RB")
        assert {f"{eid}::{rid}", "z2:trout_char_quota::trout_char_quota.r1"} <= got, eid


# G. A WATER ROW NAMING A FISH ITS REGION CLOSES LIFTS THAT CLOSURE FOR THAT FISH.
def test_a_water_row_naming_the_closed_fish_lifts_the_closure(db):
    """Okanagan River prints "bass daily quota = 8" under Region 8's "Bass: 0 quota, CLOSED TO
    FISHING (see tables for exceptions)" — the row is the exception. The lift is in the data
    (`basis: names_the_fish`), not inferred."""
    eid = "r8:okanagan_river@8-1"
    sid = _sid(db, eid, "okanagan_river.r2")
    for fish in ("LMB", "SMB"):
        got = _kept(sid, (7, 15), fish)
        assert f"{eid}::okanagan_river.r2" in got and \
            "z8:species_quotas::species_quotas.r1" not in got, fish
    (ex,) = db.execute("select exempts from rule where entry_id = ? and rule_id = ?",
                       (eid, "okanagan_river.r2")).fetchone()
    lift = [x for x in json.loads(ex) if x["rule_id"] == "species_quotas.r1"]
    assert lift and lift[0]["basis"] == "names_the_fish" and lift[0]["entry_id"] == "z8:species_quotas"


def test_a_group_row_never_reopens_a_named_closure(db):
    """West Road's tributaries print "Trout daily quota = 1": "trout" names no steelhead, so Region
    6's "No fishing: in all rivers and streams for steelhead, May 15 – June 15" stands there."""
    eid = "r6:west_road_blackwater_river_s_tributaries@6-1"
    sid = _sid(db, eid, "west_road_river_tributaries.r1")
    got = {f"{x['entry']}::{x['rule']}" for x in R.effective_rules(sid, (6, 1), "ST", BUNDLE)
           if x["state"] == "speaks"}
    assert "z6:steelhead_stream_closure::steelhead_stream_closure.r1" in got
    (ex,) = db.execute("select exempts from rule where entry_id = ? and rule_id = ?",
                       (eid, "west_road_river_tributaries.r1")).fetchone()
    assert "steelhead_stream_closure" not in (ex or "")


def test_named_lifts_need_both_rules_to_name_the_fish():
    """The derivation itself: a water's "Bass: 8" lifts a zone's "Bass: CLOSED"; its "Trout: 1"
    never lifts a steelhead closure; a group closure ("No fishing in any stream") is never lifted
    by naming. MUTATION: `named_leaves` reading an aggregate as naming its members lifts the
    steelhead closure."""
    from pipeline.deliver.bundle import rules as rules_mod
    from pipeline.regs.parsing.catalogue import CatalogueRule

    def rule(**kw):
        return CatalogueRule.model_validate(dict({"type": "retention_limit",
                                                  "extents": [{"op": "whole"}]}, **kw))
    bass_closed = rule(rule_id="s.r1", verbatim="Bass: 0 quota, CLOSED", species=["BASS"], take=0,
                       may_target=False)
    st_closed = rule(rule_id="c.r1", verbatim="No fishing for steelhead", species=["ST"], take=0,
                     may_target=False)
    streams = rule(rule_id="k.r1", verbatim="No fishing in any stream", species=["ALL_GAME_FISH"],
                   take=0, may_target=False)
    closures = rules_mod.zone_closures([type("E", (), {"entry_id": "z8:s", "rules": [bass_closed]}),
                                        type("E", (), {"entry_id": "z8:c", "rules": [st_closed]}),
                                        type("E", (), {"entry_id": "z8:k", "rules": [streams]})])
    assert sorted(eid for rows in closures.values() for eid, _ in rows) == ["z8:c", "z8:s"]
    bass = rule(rule_id="w.r1", verbatim="bass daily quota = 8", species=["BASS"], take=8)
    trout = rule(rule_id="w.r2", verbatim="Trout daily quota = 1", species=["TROUT"], take=1)
    got = rules_mod._named_lifts("r8:w", bass, closures)
    assert [(x["entry_id"], x["rule_id"], x.get("species")) for x in got] == [("z8:s", "s.r1", None)]
    assert rules_mod._named_lifts("r8:w", trout, closures) == []
    real = rules_mod.AGGREGATE_GROUPS
    try:
        rules_mod.AGGREGATE_GROUPS = frozenset()
        assert [x["entry_id"] for x in rules_mod._named_lifts("r8:w", trout, closures)] == ["z8:c"]
    finally:
        rules_mod.AGGREGATE_GROUPS = real


# H. REGION 8's BROOK TROUT FROM STREAMS ARE COUNTED APART FROM THE TROUT/CHAR QUOTA.
def test_region_8_brook_trout_from_streams_are_counted_apart(db):
    """p.68: "Trout/char: 5, but not more than … 4 from streams … And you may retain: 20 brook trout
    from streams". On a stream a brook trout answers to its 20 only; on a lake (where the 20 does
    not bind) to the trout/char 5."""
    tc = "z8:trout_char_quota"
    sid = _sid(db, tc, "trout_char_quota.r5")
    got = _kept(sid, (8, 1), "EB")
    assert f"{tc}::trout_char_quota.r5" in got
    assert not {f"{tc}::trout_char_quota.r{k}" for k in (1, 2, 3, 4)} & got, got
    # a rainbow on the same stream is still under the 5 and the 4 from streams
    assert {f"{tc}::trout_char_quota.r1", f"{tc}::trout_char_quota.r3"} <= _kept(sid, (8, 1), "RB")
    lake = _sid(db, tc, "trout_char_quota.r1", without=((tc, "trout_char_quota.r5"),))
    assert f"{tc}::trout_char_quota.r1" in _kept(lake, (8, 1), "EB")


# E. CHILLIWACK: A RAINBOW OVER 50 CM IS A STEELHEAD (p.86).
def test_as_rainbow_reads_a_rule_over_rainbow_of_50_cm_or_less():
    assert R.as_rainbow({"take": 1, "lengths": [{"min_cm": 50}]}) is None          # "1 over 50 cm"
    assert R.as_rainbow({"take": 0, "lengths": [{"max_cm": 50, "take": 0}]}) == {"take": 0}
    assert R.as_rainbow({"take": 4, "lengths": [{"max_cm": 50}]}) == {"take": 4}
    x = {"take": 2, "lengths": [{"min_cm": 30, "max_cm": 50}, {"max_cm": 30, "take": 0}]}
    assert R.as_rainbow(x) == x
    assert R.as_rainbow({"take": 2}) == {"take": 2}
    assert R.as_rainbow({"lengths": [{"max_cm": 30, "take": 0}, {"min_cm": 60, "take": 0}]}) == \
        {"lengths": [{"max_cm": 30, "take": 0}]}


def test_chilliwack_rainbow_over_50_cm_is_a_steelhead(db):
    """Chilliwack/Vedder downstream of Vedder Crossing. May: "hatchery rainbow trout catch and
    release (50 cm or less)" releases every hatchery rainbow there (every rainbow is 50 cm or less),
    so Region 2's "Trout/char: 4" keeps no rainbow — and its "1 over 50 cm" speaks for no rainbow.
    A fish over 50 cm is a steelhead: "2 hatchery steelhead over 50 cm allowed", "All wild steelhead"
    released. July-April: the row's "hatchery rainbow trout … 50 cm or less: daily quota = 4" beside
    the region's 4."""
    eid = "r2:chilliwack_vedder_rivers_does_not_include_sumas_river_see_ma@2-4"
    tc = "z2:trout_char_quota"
    sid = _sid(db, eid, "chilliwack_vedder_rivers.r6")
    assert db.execute("select 1 from steelhead_water where sid = ?", (sid,)).fetchone()
    may = _kept(sid, (5, 15), "RB")
    assert f"{eid}::chilliwack_vedder_rivers.r6" in may
    assert f"{tc}::trout_char_quota.r1" not in may and f"{tc}::trout_char_quota.r2" not in may
    st = _kept(sid, (5, 15), "ST")
    assert {f"{tc}::trout_char_quota.r3", f"{tc}::trout_char_quota.r7"} <= st
    jul = _kept(sid, (7, 15), "RB")
    assert {f"{eid}::chilliwack_vedder_rivers.r9", f"{tc}::trout_char_quota.r1"} <= jul
    assert f"{tc}::trout_char_quota.r2" not in jul
    # elsewhere in Region 2 a rainbow of any size is a rainbow: "1 over 50 cm" speaks for it
    other = _sid(db, tc, "trout_char_quota.r2",
                 without=(("r2:chilliwack_vedder%", None),))
    assert not db.execute("select 1 from steelhead_water where sid = ?", (other,)).fetchone()
    assert f"{tc}::trout_char_quota.r2" in _kept(other, (7, 15), "RB")


def test_the_guide_states_the_rulings_of_the_second_round(db):
    from pipeline.tools import export_ui_rules as X
    g = X.build(Path(BUNDLE))["guide"]
    lad = g["ladder"]
    assert "SITS BESIDE" in lad["quotas_sit_beside"] and "EXACTLY" in lad["quotas_sit_beside"]
    assert "NAMES THE" in lad["closures"] and "West Road" in lad["closures"]
    assert "20 brook trout from streams" in lad["counted_apart"]
    assert "p.86" in lad["steelhead_definition"] and "Chilliwack" in lad["steelhead_definition"]
    assert "Mara Lake" in lad["region"]
    assert "BINDS NOTHING" in g["entries"]["pointers"]["reading"]
    assert g["exempts"]["derived_lifts"] > 0


# --------------------------------------------------------------------------- round R4 (2026-09-25)
# 1. A WATER QUOTA CAN NEVER MAKE THE NUMBER BIGGER: the zone total caps it.
def test_a_larger_water_quota_sits_beside_the_zone_total_that_caps_it(tmp_path):
    """Perry Creek's "Brook trout daily quota = 20" beside Region 4's "Trout/char: 5": both speak,
    so the angler keeps at most 5. MUTATION: `same_statement` answering True lets the 20 displace
    the 5 — the number grows — and this fails."""
    from pipeline.deliver.bundle import rules as rules_mod
    path = _tiny(tmp_path, [
        {"entry": "z4:q", "rule": "q.r1", "species": ["TROUT_CHAR"], "take": 5, "_rank": 3},
        {"entry": "r4:perry", "rule": "p.r3", "species": ["EB"], "take": 20, "_rank": 0}])
    assert _speak(path, "EB", (8, 1)) == {"q.r1", "p.r3"}
    real = rules_mod.same_statement
    try:
        rules_mod.same_statement = lambda a, b: True
        assert _speak(path, "EB", (8, 1)) == {"p.r3"}
    finally:
        rules_mod.same_statement = real


def test_perry_creek_brook_trout_20_is_capped_by_the_zone_5(db):
    eid = "r4:perry_creek@4-20"
    sid = _sid(db, eid, "perry_creek.r3")
    got = _kept(sid, (8, 1), "EB")
    assert {f"{eid}::perry_creek.r3", "z4:trout_char_quota::trout_char_quota.r1"} <= got


def test_the_guide_says_a_larger_water_quota_is_capped(db):
    from pipeline.tools import export_ui_rules as X
    lad = X.build(Path(BUNDLE))["guide"]["ladder"]
    assert "CAN NEVER MAKE THE NUMBER BIGGER" in lad["quotas_sit_beside"]
    assert "capped" in lad["quotas_sit_beside"] and "counted apart" in lad["quotas_sit_beside"]
    assert "MOST STRICT" in lad["two_regions"] and "LOWER" in lad["two_regions"]
    assert "whole length" in lad["region"] and "PER-REGION" in lad["region"]
    assert "PRINTS ITS OWN EXEMPTION LIST" in lad["closures"]


# 2. A DERIVED LIFT NEVER REOPENS MORE THAN ITS LIFTER COVERS (Kitimat).
def _crule(**kw):
    from pipeline.regs.parsing.catalogue import CatalogueRule
    return CatalogueRule.model_validate(dict({"type": "retention_limit",
                                              "extents": [{"op": "whole"}]}, **kw))


def _E(eid, rules):
    return type("E", (), {"entry_id": eid, "rules": rules})


def test_a_closure_printing_its_own_exemptions_takes_no_derived_lift():
    """Region 6 (p.49): "No fishing: in all rivers and streams for steelhead, May 15 – June 15.
    Exemptions include mainstem portions of the Skeena, Nass, …" — the entry's own rule exempts the
    mainstems, so no water row adds to the list by naming steelhead. "(No exceptions)" refuses too.
    Region 8's "(see tables for exceptions)" does neither. MUTATION: `prints_its_exemptions`
    answering False lets Kitimat's hatchery quota lift the steelhead closure again."""
    from pipeline.deliver.bundle import rules as rules_mod
    st = _crule(rule_id="c.r1", verbatim="No fishing for steelhead", species=["ST"], take=0,
                may_target=False)
    mains = _crule(rule_id="c.r2", verbatim="Exemptions include mainstems", species=["ST"],
                   exempts=[{"target": "c.r1"}])
    wsg = _crule(rule_id="s.r9", verbatim="White Sturgeon: 0 quota, CLOSED TO FISHING (No "
                 "exceptions)", species=["WSG"], take=0, may_target=False)
    bass = _crule(rule_id="s.r1", verbatim="Bass: 0 quota, CLOSED TO FISHING", species=["BASS"],
                  take=0, may_target=False)
    docs = [_E("z6:c", [st, mains]), _E("z6:s", [wsg, bass])]
    got = {(e, r.rule_id) for rows in rules_mod.zone_closures(docs).values() for e, r in rows}
    assert got == {("z6:s", "s.r1")}
    kit = _crule(rule_id="k.r5", verbatim="Hatchery steelhead daily quota = 2", species=["ST"],
                 take=2, origin="hatchery", lengths=[{"min_cm": 50}, {"max_cm": 50, "take": 0}])
    assert rules_mod._named_lifts("r6:kit", kit, rules_mod.zone_closures(docs)) == []
    real = rules_mod.prints_its_exemptions
    try:
        rules_mod.prints_its_exemptions = lambda ce, r: False
        assert rules_mod._named_lifts("r6:kit", kit, rules_mod.zone_closures(docs))
    finally:
        rules_mod.prints_its_exemptions = real


def test_a_derived_lift_carries_the_lifters_origin_and_sizes():
    """A hatchery lifter lifts for hatchery fish only; a band restating the fish's definition (a
    steelhead is over 50 cm) is not a size term, a real size limit is. MUTATION: dropping the
    `origin` term makes a hatchery quota reopen the closure for wild fish."""
    from pipeline.deliver.bundle import rules as rules_mod
    closed = _crule(rule_id="c.r1", verbatim="No fishing for steelhead", species=["ST"], take=0,
                    may_target=False)
    closures = {"9": [("z9:c", closed)]}
    hatch = _crule(rule_id="w.r1", verbatim="Hatchery steelhead (>50 cm) daily quota = 2",
                   species=["ST"], take=2, origin="hatchery",
                   lengths=[{"min_cm": 50}, {"max_cm": 50, "take": 0}])
    (x,) = rules_mod._named_lifts("r9:w", hatch, closures)
    assert x["origin"] == "hatchery" and "lengths" not in x
    big = _crule(rule_id="w.r2", verbatim="Steelhead daily quota = 1 (none under 80 cm)",
                 species=["ST"], take=1, lengths=[{"min_cm": 80}, {"max_cm": 80, "take": 0}])
    (y,) = rules_mod._named_lifts("r9:w", big, closures)
    assert y["lengths"] == [{"min_cm": 80}] and "origin" not in y


def test_a_lift_for_some_fish_leaves_the_closure_standing_partly_lifted(tmp_path):
    """An `origin` or `lengths` term is known only once the fish is caught: the closure stays,
    marked partly lifted. MUTATION: reading such an item as outright reopens the closure."""
    for i, term in enumerate(({"origin": "hatchery"}, {"lengths": [{"min_cm": 80}]})):
        (tmp_path / str(i)).mkdir()
        path = _tiny(tmp_path / str(i), [
            {"entry": "z9:c", "rule": "c.r1", "species": ["ST"], "take": 0, "may_target": 0,
             "_rank": 3},
            {"entry": "r9:w", "rule": "w.r1", "species": ["ST"], "take": 2, "_rank": 0,
             "exempts": [dict({"entry_id": "z9:c", "rule_id": "c.r1",
                               "basis": "names_the_fish"}, **term)]}])
        got = {x["rule"]: x for x in R.effective_rules(1, (5, 20), "ST", path)}
        assert "c.r1" in got and got["c.r1"].get("partly_lifted"), term


def test_kitimat_steelhead_closure_stands_for_wild_and_hatchery(db):
    """p.49 + p.51: Region 6's steelhead stream closure (May 15 – June 15) prints its own exemption
    list, and the Kitimat is not on it. On the Kitimat mainstem and its tributaries, May 20, the
    closure speaks for steelhead; the row's own hatchery quota stands beside it. No lift of the
    closure ships on the Kitimat's rules."""
    kit = "r6:kitimat_river_angling_regulations_for_the_kitimat_river_are@6-3"
    for rid in ("kitimat_river.r5", "kitimat_river.r6"):
        (ex,) = db.execute("select exempts from rule where entry_id = ? and rule_id = ?",
                           (kit, rid)).fetchone()
        assert "steelhead_stream_closure" not in (ex or ""), rid
    for via in ("reach", "trib"):
        sid = db.execute("select min(sr.sid) from ruleset r join section_ruleset sr on "
                         "sr.set_id = r.set_id where r.entry_id = ? and r.rule_id = ? and "
                         "r.via = ?", (kit, "kitimat_river.r5", via)).fetchone()[0]
        assert sid is not None, via
        got = {f"{x['entry']}::{x['rule']}": x for x in
               R.effective_rules(sid, (5, 20), "ST", BUNDLE) if x["state"] == "speaks"}
        close = got.get("z6:steelhead_stream_closure::steelhead_stream_closure.r1")
        assert close is not None and not close.get("partly_lifted"), via


def test_every_derived_lift_is_region_8s_bass_or_perch(db):
    """After the fix, the derived lifts are Region 8's "(see tables for exceptions)" closures
    only — bass (species_quotas.r1) and yellow perch (species_quotas.r9)."""
    n = 0
    for eid, rid, ex in db.execute("select entry_id, rule_id, exempts from rule "
                                   "where exempts like '%names_the_fish%'"):
        for x in json.loads(ex):
            if x.get("basis") == "names_the_fish":
                n += 1
                assert (x["entry_id"], x["rule_id"]) in {
                    ("z8:species_quotas", "species_quotas.r1"),
                    ("z8:species_quotas", "species_quotas.r9")}, (eid, rid, x)
    assert n == 34


# 3(c). TWO REGIONS' BASES ON ONE LAKE: THE MOST STRICT APPLIES.
def test_stricter_decides_between_two_regions_rules():
    closed = {"type": "retention_limit", "species": ["BASS"], "take": 0, "may_target": 0}
    quota = {"type": "retention_limit", "species": ["BASS"], "take": 8}
    release = {"type": "retention_limit", "species": ["TROUT_CHAR"], "take": 0}
    tc5 = {"type": "retention_limit", "species": ["TROUT_CHAR"], "take": 5}
    tc4 = {"type": "retention_limit", "species": ["TROUT_CHAR"], "take": 4}
    wild = {"type": "retention_limit", "species": ["TROUT_CHAR"], "take": 2, "origin": "wild"}
    gear = {"type": "gear_restriction", "species": []}
    assert R.stricter(closed, quota) and not R.stricter(quota, closed)
    assert not R.stricter(closed, dict(closed))
    assert R.stricter(release, tc5) and not R.stricter(tc5, release)
    assert R.stricter(tc4, tc5) and not R.stricter(tc5, tc4) and not R.stricter(tc4, dict(tc4))
    assert not R.stricter(tc4, wild) and not R.stricter(wild, tc4)     # different statements
    assert not R.stricter(closed, gear)                                # gear both apply


def test_two_regions_on_one_lake_the_most_strict_speaks(tmp_path):
    """Region 3's "Bass: CLOSED" beside Region 8's (made-up) "Bass: 8" on a straddling lake: the
    closure speaks. "Trout/char: 4" beats "Trout/char: 5" even when the 5 is an area rule. MUTATION:
    removing step 6 lets Region 8's 8 and 5 speak; letting step 4 rank two regions' rules by place
    (`peers` ignored) lets the area's 5 displace the 4."""
    path = _tiny(tmp_path, [
        {"entry": "z3:s", "rule": "s.r1", "species": ["BASS"], "take": 0, "may_target": 0,
         "_rank": 3},
        {"entry": "z8:s", "rule": "s.r1", "species": ["BASS"], "take": 8, "_rank": 3},
        {"entry": "z3:t", "rule": "t.r1", "species": ["TROUT_CHAR"], "take": 4, "_rank": 3},
        # an AREA rule of Region 8 (rank 2) does not outrank Region 3's table by place here
        {"entry": "z8:t", "rule": "t.r1", "species": ["TROUT_CHAR"], "take": 5, "_rank": 2},
        {"entry": "z8:g", "rule": "g.r1", "type": "gear_restriction", "dimension": "hook",
         "species": [], "_rank": 3},
        {"entry": "z3:g", "rule": "g.r1", "type": "gear_restriction", "dimension": "hook",
         "species": [], "_rank": 3}])
    got = {f"{x['entry']}::{x['rule']}" for x in R.effective_rules(1, (7, 1), "SMB", path)
           if x["state"] == "speaks"}
    assert "z3:s::s.r1" in got and "z8:s::s.r1" not in got
    assert {"z3:g::g.r1", "z8:g::g.r1"} <= got
    rb = {f"{x['entry']}::{x['rule']}" for x in R.effective_rules(1, (7, 1), "RB", path)
          if x["state"] == "speaks"}
    assert "z3:t::t.r1" in rb and "z8:t::t.r1" not in rb
