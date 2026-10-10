"""A ZONE SIZE CLAUSE MADE MOOT BY AN OUTRIGHT RELEASE IS NOT SHOWN (user ruling 2026-10-05,
option A; `read.effective_rules` step 5b).

A zone-side clause stating only sizes ("none under 60 cm", dimension `daily/size`) has nothing left
to keep when an outright release or closure of the same fish is in force — whoever wrote it: the
water, the zone (with or without a water kind) or a superior authority (an ecological reserve).
Before the ruling the reader hid it only under a WATER's release (step 5) and showed it beside every
zone release and closure. The release or closure itself is untouched — a closure still means no
gear in the water, a release still means fish and let go — and a clause keeping an origin the
release does not cover stays.

Every bundle case is pinned BY MUTATION: with `read.MOOT_SIZE_CLAUSE_HIDDEN` off the clause comes
back, so the step is what hides it. `UI_EXPORT_BUNDLE` points the suite at a side bundle.
"""
from __future__ import annotations

import os
import sqlite3

import pytest

from pipeline.deliver.bundle import read as R
from pipeline.tests.test_competition import _tiny as _tiny_one
from pipeline.tests.conftest import need, BUNDLE_HINT


def _tiny(tmp_path, rules):
    """`test_competition._tiny`, in a fresh directory per call (it writes one fixed file name)."""
    _tiny.n = getattr(_tiny, "n", 0) + 1
    d = tmp_path / f"b{_tiny.n}"
    d.mkdir()
    return _tiny_one(d, rules)

BUNDLE = os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE
Z3 = "z3:trout_char_quota::trout_char_quota"
R4B = f"{Z3}.r4b"                                           # Region 3 "none under 60 cm"
BONAPARTE, ELEVEN_MILE, PEACE = "wbk:329054723", "gnis:8623", "gnis:14619"
RESERVE = ("zp:superior_closures", "superior_closures.r3")  # "prohibited in Ecological Reserves"


@pytest.fixture(scope="module")
def db(request):
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    con = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    yield con
    con.close()


def _sid(db, item: str, rule: tuple | None = None) -> int:
    q = ("SELECT MIN(s.sid) FROM item i JOIN item_section s ON s.ord = i.ord "
         "JOIN section_ruleset sr ON sr.sid = s.sid WHERE i.item_id = ?")
    args: tuple = (item,)
    if rule:
        q += " AND sr.set_id IN (SELECT set_id FROM ruleset WHERE entry_id = ? AND rule_id = ?)"
        args += rule
    got = db.execute(q, args).fetchone()[0]
    assert got is not None, (item, rule)
    return got


def _said(sid, on, fish) -> dict:
    return {f"{x['entry']}::{x['rule']}": x for x in R.effective_rules(sid, on, fish, BUNDLE)}


def _both_ways(monkeypatch, sid, on, fish):
    """(answer with the ruling, answer with the step switched off)."""
    now = _said(sid, on, fish)
    monkeypatch.setattr(R, "MOOT_SIZE_CLAUSE_HIDDEN", False)
    then = _said(sid, on, fish)
    monkeypatch.setattr(R, "MOOT_SIZE_CLAUSE_HIDDEN", True)
    return now, then


@pytest.mark.needs_bundle
def test_bonaparte_lake_nov_1_lake_trout(db, monkeypatch):
    """Region 3's 'Lake trout from Oct 15-Jan 31' (a zone release, no water kind) speaks; its
    'none under 60 cm' is not shown. Off: the clause speaks beside it (the reader before)."""
    now, then = _both_ways(monkeypatch, _sid(db, BONAPARTE), (11, 1), "LT")
    assert now[f"{Z3}.r7"]["state"] == "speaks" and R4B not in now
    assert then[R4B]["state"] == "speaks"
    assert set(then) - set(now) == {R4B}, "the clause and nothing else"


@pytest.mark.needs_bundle
def test_eleven_mile_creek_aug_1_bull_trout(db, monkeypatch):
    """A zone release LIMITED TO STREAMS ('Bull trout (Dolly Varden) from streams, Aug 1-Oct 31')."""
    now, then = _both_ways(monkeypatch, _sid(db, ELEVEN_MILE), (8, 1), "DV")
    assert now[f"{Z3}.r6"]["state"] == "speaks" and R4B not in now
    assert R4B in then and set(then) - set(now) == {R4B}


@pytest.mark.needs_bundle
def test_eleven_mile_creek_may_1_lake_trout_under_the_spring_closure(db, monkeypatch):
    """A zone CLOSURE: the spring stream closure stays a closure (may not fish for it), and the
    clause is not shown under it."""
    closure = "z3:spring_stream_closure::spring_stream_closure.r1"
    now, then = _both_ways(monkeypatch, _sid(db, ELEVEN_MILE), (5, 1), "LT")
    assert now[closure]["state"] == "speaks"
    assert now[closure]["take"] == 0 and not now[closure].get("may_target")
    assert R4B not in now and set(then) - set(now) == {R4B}


@pytest.mark.needs_bundle
def test_a_superior_closure_hides_the_zone_clause(db, monkeypatch):
    """The Peace River inside the Clayhurst Ecological Reserve (Zone 7B), Jan 1, lake trout:
    'Fishing is prohibited in Ecological Reserves' is a superior closure; Zone 7B's 'none under
    30 cm' for lake trout is not shown under it. Off: it was."""
    z7b = "z7b:trout_char_quota::trout_char_quota.r7"
    now, then = _both_ways(monkeypatch, _sid(db, PEACE, RESERVE), (1, 1), "LT")
    assert now["::".join(RESERVE)]["state"] == "speaks"
    assert z7b not in now and z7b in then
    assert set(then) - set(now) == {z7b}


@pytest.mark.needs_bundle
def test_a_water_release_still_hides_it_as_before(db, monkeypatch):
    """Griffin Lake's own 'Lake trout and bull trout catch and release' (step 5) — unchanged."""
    now, then = _both_ways(monkeypatch, _sid(db, "wbk:329518152"), (11, 1), "LT")
    assert R4B not in now and R4B not in then


# ---- synthetic: the edges of the rule ------------------------------------------------------------
REL = {"entry": "z9:q", "rule": "q.rel", "species": ["RB"], "take": 0, "may_target": 1,
       "_rank": 3, "dimension": "daily"}
CLAUSE = {"entry": "z9:q", "rule": "q.size", "species": ["RB"], "dimension": "daily/size",
          "lengths": [{"max_cm": 30, "take": 0}], "_rank": 3}


def _ids(path, fish="RB") -> set:
    return {f"{x['entry']}::{x['rule']}" for x in R.effective_rules(1, (7, 1), fish, path)}


def test_a_hatchery_only_clause_under_a_wild_only_release_stays(tmp_path):
    """NEGATIVE: the release frees wild fish only; the clause keeps hatchery fish — it still
    has something to say, so it stays."""
    wild = dict(REL, origin="wild", dimension="daily@origin=wild")
    hatch = dict(CLAUSE, origin="hatchery", dimension="daily/size@origin=hatchery")
    assert _ids(_tiny(tmp_path, [wild, hatch])) == {"z9:q::q.rel", "z9:q::q.size"}


def test_a_clause_with_no_origin_under_a_wild_only_release_stays(tmp_path):
    wild = dict(REL, origin="wild", dimension="daily@origin=wild")
    assert "z9:q::q.size" in _ids(_tiny(tmp_path, [wild, CLAUSE]))


def test_an_all_origin_release_hides_a_hatchery_clause(tmp_path):
    hatch = dict(CLAUSE, origin="hatchery", dimension="daily/size@origin=hatchery")
    assert _ids(_tiny(tmp_path, [REL, hatch])) == {"z9:q::q.rel"}


def test_a_release_of_another_fish_or_a_size_band_hides_nothing(tmp_path):
    other = dict(REL, species=["DV"])
    assert "z9:q::q.size" in _ids(_tiny(tmp_path, [other, CLAUSE]))
    banded = dict(REL, take=None, dimension="daily/size", lengths=[{"min_cm": 50, "take": 0}])
    assert "z9:q::q.size" in _ids(_tiny(tmp_path, [banded, CLAUSE])), \
        "a release of big fish only is no outright release"


def test_a_release_not_in_force_hides_nothing(tmp_path):
    dated = dict(REL, when={"dates": [{"from_month": 11, "from_day": 1, "to_month": 12,
                                        "to_day": 31}]})
    assert "z9:q::q.size" in _ids(_tiny(tmp_path, [dated, CLAUSE]))


def test_a_rows_own_size_clause_is_not_touched(tmp_path):
    """Only a ZONE-side clause: a row's size clause is its own subject (Koocanusa, RU-3)."""
    row = dict(CLAUSE, entry="r9:lake", rule="lake.r2", _rank=0)
    assert "r9:lake::lake.r2" in _ids(_tiny(tmp_path, [REL, row]))


def test_the_closure_and_the_release_stay_what_they_are(tmp_path):
    shut = dict(REL, rule="q.shut", may_target=0)
    got = {f"{x['entry']}::{x['rule']}": x for x in
           R.effective_rules(1, (7, 1), "RB", _tiny(tmp_path, [shut, CLAUSE]))}
    assert set(got) == {"z9:q::q.shut"} and got["z9:q::q.shut"]["may_target"] == 0
    got = {f"{x['entry']}::{x['rule']}": x for x in
           R.effective_rules(1, (7, 1), "RB", _tiny(tmp_path, [REL, CLAUSE]))}
    assert set(got) == {"z9:q::q.rel"} and got["z9:q::q.rel"]["may_target"] == 1
