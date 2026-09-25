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
    the lake's 2 speaks, over the zone's "Trout/char: 5"."""
    slug = eid.split(":")[1].split("@")[0]
    sid = _sid(db, eid, f"{slug}.r2")
    assert _speaks(sid, (7, 1), "BT") == {f"{ZB}::trout_char_quota.r9"}
    got = R.effective_rules(sid, (7, 1), "BT", BUNDLE)
    bt = [x for x in got if x["state"] == "speaks" and (x["type"], x["dimension"]) == DAILY]
    assert [(x["take"], x["may_target"]) for x in bt] == [(0, 1)]
    for fish in ("RB", "LT", "EB"):
        assert _speaks(sid, (7, 1), fish) == {f"{eid}::{slug}.r2"}, fish
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
