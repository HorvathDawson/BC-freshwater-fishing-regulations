"""THE READER'S THREE ADDITIONS (2026-10-06, team round B), each on real sections and pinned by
mutation:

  * DENETIAH (user ruling): a water's OWN full closure in force is the most dominant rule — it
    silences every keeping rule for the fish it covers, of any source and any key, save a superior
    authority's (`read.effective_rules` step 4c, `read.WATER_CLOSURE_DOMINANT`).
  * LOSERS + BY (gap G1): `trace=True` returns every rule that took part and lost, with its
    `state`, `reason` and `by` — the same code path: the speakers are the untraced answer.
  * PER ORIGIN (gap G2): `origin="hatchery"|"wild"` — a lift limited to that origin lifts outright,
    one for the other origin not at all; `origin=None` is today's answer.

`UI_EXPORT_BUNDLE` points the suite at a side bundle; the bundle tests skip without one.
"""
from __future__ import annotations

import os
import sqlite3
from collections import defaultdict
from pathlib import Path

import pytest

from pipeline.deliver.bundle import read as R
from pipeline.tests.test_competition import _tiny as _tiny_one

BUNDLE = os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE

DENETIAH = "r7:denetiah_creek@7-52::denetiah_creek.r1"            # "No fishing, Jul 1-15"
LIARD = "r7:liard_river_watershed_see_map_on_page_63@7-53::liard_river_watershed"
KITIMAT = "r6:kitimat_river_angling_regulations_for_the_kitimat_river_are@6-3::kitimat_river"
Z6 = "z6:trout_char_quota::trout_char_quota"


def _tiny(tmp_path, rules, via=None):
    """`test_competition._tiny`, in a fresh directory per call (it writes one fixed file name)."""
    _tiny.n = getattr(_tiny, "n", 0) + 1
    d = tmp_path / f"b{_tiny.n}"
    d.mkdir()
    return _tiny_one(d, rules, via)


@pytest.fixture(scope="module")
def db():
    if not Path(BUNDLE).is_file():
        pytest.skip("no bundle")
    con = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    yield con
    con.close()


def _sid(db, item: str, rule: str | None = None) -> int:
    q = ("SELECT MIN(s.sid) FROM item i JOIN item_section s ON s.ord = i.ord "
         "JOIN section_ruleset sr ON sr.sid = s.sid WHERE i.item_id = ?")
    args: tuple = (item,)
    if rule:
        q += " AND sr.set_id IN (SELECT set_id FROM ruleset WHERE entry_id = ? AND rule_id = ?)"
        args += tuple(rule.split("::", 1))
    got = db.execute(q, args).fetchone()[0]
    assert got is not None, (item, rule)
    return got


def _said(sid, on, fish, **kw) -> dict:
    return {R.rid(x): x for x in R.effective_rules(sid, on, fish, BUNDLE, **kw)}


# ============================================================================== DENETIAH
def test_denetiah_creek_its_own_closure_is_all_that_speaks_for_bull_trout(db, monkeypatch):
    """Denetiah Creek, Jul 5, bull trout: the creek's 'No fishing, Jul 1-15' and the Liard River
    watershed row's 'Dolly Varden/bull trout — 1 in possession (30-50 cm only)' (an AREA row of
    the water tables, another key) both spoke. Now only the closure keeps nothing — no keeping
    rule of any source speaks. MUTATION: `WATER_CLOSURE_DOMINANT` off, the possession 1 speaks
    again (the reader before the ruling)."""
    sid = _sid(db, "gnis:39298", DENETIAH)
    from pipeline.deliver.bundle.rules import yields_to_release
    now = _said(sid, (7, 5), "DV")
    assert now[DENETIAH]["state"] == "speaks"
    assert f"{LIARD}.r3" not in now and f"{LIARD}.r2" not in now
    assert not [k for k, x in now.items() if x["state"] == "speaks" and yields_to_release(x)]
    monkeypatch.setattr(R, "WATER_CLOSURE_DOMINANT", False)
    then = _said(sid, (7, 5), "DV")
    assert then[f"{LIARD}.r3"]["state"] == "speaks"
    assert set(then) - set(now) == {f"{LIARD}.r3"}, "the possession 1 and nothing else"


def test_denetiah_creek_outside_its_dates_the_liard_row_speaks(db):
    """Jul 20: the creek's closure is not in force — the Liard row's 1 a day and 1 in possession
    speak again (the ruling holds on the closure's dates only)."""
    now = _said(_sid(db, "gnis:39298", DENETIAH), (7, 20), "DV")
    assert DENETIAH not in now
    assert now[f"{LIARD}.r2"]["state"] == "speaks" and now[f"{LIARD}.r3"]["state"] == "speaks"


def test_the_trace_names_the_closure_as_the_winner(db):
    """The possession 1 is returned, traced, as displaced by the creek's closure."""
    got = _said(_sid(db, "gnis:39298", DENETIAH), (7, 5), "DV", trace=True)
    assert got[f"{LIARD}.r3"]["state"] == "displaced"
    assert got[f"{LIARD}.r3"]["reason"] == "water_closure"
    assert got[f"{LIARD}.r3"]["by"] == DENETIAH


def test_a_region_7_lakes_winter_closure_silences_its_own_possession_limit(db, monkeypatch):
    """Cunningham Lake, Dec 1, lake trout: the row's 'No fishing, Nov 1-Apr 30' and its own 'Lake
    trout — 2 in possession' (possession, another key from the closure's daily) both spoke.
    MUTATION: off, the 2 in possession speaks."""
    c = "r7:cunningham_lake@7-25::cunningham_lake"
    sid = db.execute("SELECT MIN(sid) FROM section_ruleset WHERE set_id IN (SELECT set_id FROM "
                     "ruleset WHERE entry_id = ? AND rule_id = ?)",
                     (c.split("::")[0], "cunningham_lake.r1")).fetchone()[0]
    now = _said(sid, (12, 1), "LT")
    assert now[f"{c}.r1"]["state"] == "speaks" and f"{c}.r2" not in now
    monkeypatch.setattr(R, "WATER_CLOSURE_DOMINANT", False)
    assert _said(sid, (12, 1), "LT")[f"{c}.r2"]["state"] == "speaks"


# ---- synthetic: the edges of the ruling ----------------------------------------------------
SHUT = {"entry": "r9:creek", "rule": "creek.r1", "species": ["ALL_GAME_FISH"], "take": 0,
        "may_target": 0, "_rank": 0}


def _speaking(path, fish="DV", on=(7, 5)):
    return {x["rule"] for x in R.effective_rules(1, on, fish, path) if x["state"] == "speaks"}


def test_a_water_closure_silences_every_keeper_of_any_source_and_key(tmp_path):
    """Possession and annual limits, an area row, a zone size clause, another row by the walk."""
    path = _tiny(tmp_path, [
        SHUT,
        {"entry": "r9:area", "rule": "area.r3", "species": ["DV"], "take": 1, "_rank": 2,
         "dimension": "possession"},
        {"entry": "z9:q", "rule": "q.r1", "species": ["TROUT_CHAR"], "take": 5, "_rank": 3,
         "dimension": "annual"},
        {"entry": "z9:q", "rule": "q.r2", "species": ["DV"], "_rank": 3,
         "dimension": "daily/size", "lengths": [{"max_cm": 30, "take": 0}]},
        {"entry": "r9:river", "rule": "river.r1", "species": ["DV"], "take": 2, "_rank": 0,
         "dimension": "possession"}], via={"river.r1": "trib"})
    assert _speaking(path) == {"creek.r1"}


def test_a_superior_rule_and_the_closures_own_lifter_still_speak(tmp_path):
    """A superior authority's keeping rule stands (the ladder's top); a row rule that lifts the
    closure in part (for hatchery fish) speaks beside it."""
    lifter = {"entry": "r9:creek", "rule": "creek.r2", "species": ["DV"], "take": 2, "_rank": 0,
              "origin": "hatchery", "dimension": "daily@origin=hatchery",
              "exempts": [{"entry_id": "r9:creek", "rule_id": "creek.r1",
                           "origin": "hatchery"}]}
    path = _tiny(tmp_path, [
        SHUT, lifter,
        {"entry": "zp:park", "rule": "park.r1", "species": ["DV"], "take": 1, "_rank": -1,
         "dimension": "possession", "authority": "superior"}])
    assert _speaking(path) == {"creek.r1", "creek.r2", "park.r1"}


def test_a_zone_closure_and_a_walked_closure_keep_the_ladder(tmp_path):
    """Only the water's OWN closure is dominant: a zone closure (RU-7: by the ladder, its own base
    dimension) leaves a water row's possession limit naming the fish, and a closure reaching the
    water by the tributary walk leaves the water's own quota (the tributary's row is the more
    specific place)."""
    zone = _tiny(tmp_path, [
        dict(SHUT, entry="z9:shut", rule="shut.r1", _rank=3),
        {"entry": "r9:creek", "rule": "creek.r2", "species": ["DV"], "take": 1, "_rank": 0,
         "dimension": "possession"}])
    assert _speaking(zone) == {"shut.r1", "creek.r2"}
    walked = _tiny(tmp_path, [
        dict(SHUT, entry="r9:river", rule="river.r1"),
        {"entry": "r9:creek", "rule": "creek.r2", "species": ["DV"], "take": 1, "_rank": 0,
         "dimension": "possession"}], via={"river.r1": "trib"})
    assert _speaking(walked) == {"river.r1", "creek.r2"}


def test_off_the_synthetic_keepers_speak_again(tmp_path, monkeypatch):
    """MUTATION for the synthetic case: with the ruling off the possession limit of the area row
    speaks beside the closure."""
    path = _tiny(tmp_path, [
        SHUT, {"entry": "r9:area", "rule": "area.r3", "species": ["DV"], "take": 1, "_rank": 2,
               "dimension": "possession"}])
    monkeypatch.setattr(R, "WATER_CLOSURE_DOMINANT", False)
    assert _speaking(path) == {"creek.r1", "area.r3"}


# ============================================================================== LOSERS + BY
def _keys(con):
    """Every rule key (set, steelhead water, steelhead rules) with its bindings, and one day per
    distinct reading of its year (every bound `when` and every lift's `when`, as the status
    index reads it)."""
    from pipeline.deliver.status_index import month_day
    sets = defaultdict(list)
    for s, e, r, v in con.execute("SELECT set_id, entry_id, rule_id, via FROM ruleset "
                                  "ORDER BY set_id, entry_id, rule_id"):
        sets[s].append((e, r, v))
    steel = {s for (s,) in con.execute("SELECT DISTINCT sid FROM steelhead_water")}
    st_rules = {s for (s,) in con.execute("SELECT sid FROM section_steelhead_rules")}
    keys = sorted({(s, sid in steel, sid in st_rules) for sid, s in con.execute(
        "SELECT sid, set_id FROM section_ruleset")})
    every = R._rules_of(BUNDLE)
    for k in keys:
        bound = sets.get(k[0], [])
        ks = [(e, r) for e, r, _ in bound if (e, r) in every]
        whens = [every[x].get("when") for x in ks]
        lifts = [y.get("when") for x in ks for y in every[x].get("exempts") or [] if "when" in y]
        seen = {}
        for day in range(1, 367):
            md = month_day(day)
            seen.setdefault((tuple(R.in_force(w, md) for w in whens),
                             tuple(R.in_force(w, md) for w in lifts)), md)
        yield k, bound, list(seen.values())


def _fish():
    from pipeline.regs.parsing.catalogue import BOOK_SPECIES, SALMON_FISH
    return list(BOOK_SPECIES) + sorted(SALMON_FISH)


TRACE_ONLY = ("lifted_in_part_by",)


def _check_trace(bound, sw, sr, on, fish):
    plain = R.effective_rules_bound(bound, sw, on, fish, BUNDLE, steelhead_rules_here=sr)
    tr = R.effective_rules_bound(bound, sw, on, fish, BUNDLE, steelhead_rules_here=sr,
                                 trace=True)
    speakers = [{a: b for a, b in x.items() if a not in TRACE_ONLY}
                for x in tr if x["state"] in R.SPEAKER_STATES]
    assert speakers == plain, (bound[:1], on, fish)
    names = {R.rid(x) for x in tr}
    for x in tr:
        if x["state"] in R.SPEAKER_STATES:
            assert "by" not in x and "reason" not in x
            continue
        assert x["reason"] in R.LOSS_REASONS and x["state"] == R.LOSS_REASONS[x["reason"]], x
        assert x.get("by") and x["by"] != R.rid(x), (R.rid(x), on, fish)
        assert x["by"] in {f"{e}::{r}" for e, r, _ in bound}
    assert len(names) == len(tr), "a rule is returned once"
    return sum(x["state"] not in R.SPEAKER_STATES for x in tr)


def _sweep(db, pick):
    calls = losers = 0
    fish = _fish()
    for i, (k, bound, days) in enumerate(_keys(db)):
        if not pick(i, k):
            continue
        for on in days:
            for f in fish:
                losers += _check_trace(bound, k[1], k[2], on, f)
                calls += 1
    return calls, losers


def test_traced_speakers_are_the_untraced_answer_on_a_sample_of_keys(db):
    """Every 50th rule key (and Denetiah's and the Kitimat's), every reading, every fish: the
    speakers of a traced answer are the untraced answer, every loser has a reason and a `by`
    bound on the same set. The whole of B.C. is the slow test below."""
    want = {s for (s,) in db.execute(
        "SELECT DISTINCT set_id FROM ruleset WHERE entry_id IN (?, ?)",
        (DENETIAH.split("::")[0], KITIMAT.split("::")[0]))}
    calls, losers = _sweep(db, lambda i, k: i % 50 == 0 or k[0] in want)
    assert calls > 1000 and losers > 1000, (calls, losers)


@pytest.mark.slow
def test_traced_speakers_are_the_untraced_answer_on_every_key(db):
    """All rule keys × readings × fish (145,843 calls on the 2026-10-06 bundle; every loser
    attributed)."""
    calls, losers = _sweep(db, lambda i, k: True)
    assert calls > 100_000 and losers > 300_000


def test_trace_off_is_the_answer_unchanged(tmp_path):
    """With the default off nothing is added to a rule (no `by`, no `reason`, no
    `lifted_in_part_by`) and no loser is returned."""
    path = _tiny(tmp_path, [
        {"entry": "z9:q", "rule": "q.r1", "species": ["TROUT_CHAR"], "take": 5, "_rank": 3},
        {"entry": "r9:w", "rule": "w.r1", "species": ["TROUT_CHAR"], "take": 2, "_rank": 0}])
    plain = R.effective_rules(1, (7, 1), "RB", path)
    assert [x["rule"] for x in plain] == ["w.r1"]
    assert not {"by", "reason", "lifted_in_part_by"} & set(plain[0])
    traced = R.effective_rules(1, (7, 1), "RB", path, trace=True)
    lost = [x for x in traced if x["state"] == "displaced"]
    assert [(x["rule"], x["reason"], x["by"]) for x in lost] == [("q.r1", "ladder", "r9:w::w.r1")]


def test_a_lifted_rule_and_a_moot_clause_are_traced(db):
    """Kootenay Lake, Jul 1, rainbow: Region 4's 5 is LIFTED by the lake's 10 (`by` the lifter);
    Bonaparte Lake, Nov 1, lake trout: Region 3's 'none under 60 cm' is MOOT under its release."""
    kootenay = "r4:kootenay_lake_main_body_for_location_see_map_on_page_34@4-19::" \
               "kootenay_lake_main_body.r4"
    got = _said(_sid(db, "wbk:-20", kootenay), (7, 1), "RB", trace=True)
    z4 = got["z4:trout_char_quota::trout_char_quota.r1"]
    assert (z4["state"], z4["reason"], z4["by"]) == ("lifted", "lifted", kootenay)
    z3 = "z3:trout_char_quota::trout_char_quota"
    got = _said(_sid(db, "wbk:329054723"), (11, 1), "LT", trace=True)
    clause = got[f"{z3}.r4b"]
    assert (clause["state"], clause["reason"], clause["by"]) == \
        ("moot", "moot_size_clause", f"{z3}.r7")


# ============================================================================== PER ORIGIN
def test_kitimat_hatchery_rainbow_lifts_region_6_outright(db):
    """Kitimat River, Jul 1, rainbow. The row's 'hatchery rainbow trout quota = 5' lifts Region
    6's '1 trout from streams July 1-Oct 31' and 'Trout under 30 cm from any stream' for HATCHERY
    rainbow (the third lift, the Nov 1-June 30 release, is not in force). Not knowing the origin the
    reader says 'partly lifted' (today's answer, unchanged); asked for a hatchery rainbow they are
    lifted outright (the consumer page's reading); asked for a wild one the lift does not apply
    and they speak in full. MUTATION: dropping the origin branch of the lift step leaves them
    partly lifted for hatchery."""
    sid = _sid(db, "gnis:3225", f"{KITIMAT}.r4")
    lifted = {f"{Z6}.r4", f"{Z6}.r6"}
    unknown = _said(sid, (7, 1), "RB")
    assert {k for k, x in unknown.items() if x.get("partly_lifted") and Z6 in k} == lifted
    hatch = _said(sid, (7, 1), "RB", origin="hatchery")
    assert not lifted & set(hatch) and hatch[f"{KITIMAT}.r4"]["state"] == "speaks"
    wild = _said(sid, (7, 1), "RB", origin="wild")
    assert all(wild[k]["state"] == "speaks" and not wild[k].get("partly_lifted") for k in lifted)
    traced = _said(sid, (7, 1), "RB", origin="hatchery", trace=True)
    assert {(traced[k]["state"], traced[k]["by"]) for k in lifted} == \
        {("lifted", f"{KITIMAT}.r4")}


def test_hirsch_creek_hatchery_steelhead_by_the_walk(db):
    """Hirsch Creek, a Kitimat tributary the row reaches by the walk, Jul 1, steelhead: the row's
    'hatchery steelhead quota = 2' lifts Region 6's '1 over 50 cm' and '1 trout from streams' for a hatchery
    steelhead — outright when that origin is asked, not at all for a wild one."""
    sid = _sid(db, "gnis:3518", f"{KITIMAT}.r5")
    lifted = {f"{Z6}.r2", f"{Z6}.r4"}
    unknown = _said(sid, (7, 1), "ST")
    assert {k for k, x in unknown.items() if x.get("partly_lifted") and Z6 in k} == lifted
    hatch = _said(sid, (7, 1), "ST", origin="hatchery")
    assert not lifted & set(hatch)
    wild = _said(sid, (7, 1), "ST", origin="wild")
    assert all(k in wild and not wild[k].get("partly_lifted") for k in lifted)


def test_origin_none_is_todays_answer_and_a_bad_origin_is_refused(tmp_path):
    path = _tiny(tmp_path, [
        {"entry": "z9:q", "rule": "q.r1", "species": ["TROUT_CHAR"], "take": 5, "_rank": 3},
        {"entry": "r9:w", "rule": "w.r1", "species": ["RB"], "take": 2, "_rank": 0,
         "origin": "hatchery", "dimension": "daily@origin=hatchery",
         "exempts": [{"entry_id": "z9:q", "rule_id": "q.r1", "origin": "hatchery"}]}])
    base = R.effective_rules(1, (7, 1), "RB", path)
    assert base == R.effective_rules(1, (7, 1), "RB", path, origin=None)
    assert {x["rule"]: bool(x.get("partly_lifted")) for x in base} == {"q.r1": True, "w.r1": False}
    assert [x["rule"] for x in R.effective_rules(1, (7, 1), "RB", path, origin="hatchery")] \
        == ["w.r1"]
    assert {x["rule"]: bool(x.get("partly_lifted"))
            for x in R.effective_rules(1, (7, 1), "RB", path, origin="wild")} == \
        {"q.r1": False, "w.r1": False}
    with pytest.raises(ValueError):
        R.effective_rules(1, (7, 1), "RB", path, origin="unknown")
