"""A ZONE RELEASE LIMITED TO A KIND OF WATER displaces its own table's quotas for the fish (ZS-2).

Region 3 prints "you must release: Bull trout (Dolly Varden) from streams, Aug 1-Oct 31" (p.28)
and Region 4 "Trout/char release: in streams from Nov 1-Mar 31" (p.34). Both are stored
with `water: stream`, which is part of the rule's dimension, so step 4 of `read.effective_rules`
never set them against the region's "Trout/char: 5" and its clauses — "Dolly Varden — 1 per day"
spoke beside "Dolly Varden — release all, from streams" on the same stream on the same day. Region
3's "Lake trout from Oct 15-Jan 31", printed with no `water`, displaced the same quotas. Step 4b
makes the stream release do on a stream what the no-water release does everywhere.

The bundle is `UI_EXPORT_BUNDLE`, else the shipped one. Mutation: `released_on_water` forced to
`None` (the old reader) fails the Anderson and Andreen cases (`test_the_step_is_what_...`).
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest

from pipeline.deliver.bundle import read as R

BUNDLE = str(Path(os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE))
Z3, Z4 = "z3:trout_char_quota", "z4:trout_char_quota"
Z4W = "z4:trout_char_winter_release"
ANDERSON, ANDREEN, CAHILTY = "gnis:10017", "gnis:10035", "wbk:-6"     # R3 creek, R4 creek, R3 lake


@pytest.fixture(scope="module")
def db():
    if not Path(BUNDLE).exists():
        pytest.skip("no bundle")
    con = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    yield con
    con.close()


def _only_section(db, item_id: str) -> int:
    got = db.execute("select s.sid from item i join item_section s on s.ord = i.ord "
                     "where i.item_id = ?", (item_id,)).fetchall()
    assert len(got) == 1, (item_id, got)
    return got[0][0]


def _quotas(sid, on, fish) -> set:
    """The retention rules that speak, less the province's standing ones (snagging, trapping,
    the possession multiplier), which no step here touches."""
    return {f"{x['entry']}::{x['rule']}" for x in R.effective_rules(sid, on, fish, BUNDLE)
            if x["state"] == "speaks" and x["type"] == "retention_limit"
            and not x["entry"].startswith("zp:")}


def _anderson(db):
    sid = _only_section(db, ANDERSON)
    return (_quotas(sid, (9, 15), "DV"), _quotas(sid, (7, 15), "DV"),
            _quotas(sid, (1, 15), "LT"))


def _andreen(db):
    sid = _only_section(db, ANDREEN)
    return {f: _quotas(sid, (1, 15), f) for f in ("RB", "DV", "EB")}, _quotas(sid, (7, 15), "RB")


def test_anderson_creek_bull_trout_on_sep_15_is_release_only(db):
    """R3, a zone-only stream. Sep 15: the stream release speaks ALONE for bull trout — "Trout/
    char: 5", "4 from streams", "1 over 50 cm" and "1 bull trout or lake trout" are displaced
    (step 4b) and the size clause "none under 60 cm" is moot under the release (step 5b, user
    ruling 2026-10-05). Jul 15 (release not in force): the quotas and the clause speak. Jan 15,
    lake trout: the no-water release displaces the same, the clause included."""
    sep, jul, lt = _anderson(db)
    assert sep == {f"{Z3}::trout_char_quota.r6"}
    assert {f"{Z3}::trout_char_quota.r{n}" for n in ("1", "2", "3", "4", "4b")} <= jul
    assert f"{Z3}::trout_char_quota.r6" not in jul
    assert {k for k in lt if k.startswith(Z3)} == {f"{Z3}::trout_char_quota.r7"}


def test_andreen_creek_trout_and_char_on_jan_15_are_release_only(db):
    """R4, a zone-only stream. Jan 15 is inside "in streams from Nov 1-Mar 31": for a rainbow,
    a bull trout and a brook trout the release speaks alone — "Trout/char: 5", "1 rainbow or
    cutthroat over 50 cm", "2 from streams", "1 bull trout" are displaced. Jul 15 the quotas
    speak again."""
    jan, jul = _andreen(db)
    for fish, got in jan.items():
        assert {k for k in got if k.startswith("z4:")} == {f"{Z4W}::trout_char_winter_release.r1"}, fish
    assert {f"{Z4}::trout_char_quota.r1", f"{Z4}::trout_char_quota.r2",
            f"{Z4}::trout_char_quota.r3"} <= jul
    assert f"{Z4W}::trout_char_winter_release.r1" not in jul


def test_a_lake_keeps_its_quotas_when_the_stream_release_is_in_force(db):
    """Cahilty Lake (R3), Sep 15: the stream release does not bind a lake, so the region's
    quotas for bull trout all speak there."""
    sid = _only_section(db, CAHILTY)
    got = _quotas(sid, (9, 15), "DV")
    assert f"{Z3}::trout_char_quota.r6" not in got
    assert {f"{Z3}::trout_char_quota.r1", f"{Z3}::trout_char_quota.r3",
            f"{Z3}::trout_char_quota.r4", f"{Z3}::trout_char_quota.r4b"} <= got


def test_the_step_is_what_displaces_them(db, monkeypatch):
    """Mutation: with `released_on_water` answering None (the reader before ZS-2), the quotas
    speak beside the stream releases again and both bundle cases fail."""
    monkeypatch.setattr(R, "released_on_water", lambda x: None)
    sep, _, _ = _anderson(db)
    assert f"{Z3}::trout_char_quota.r1" in sep and f"{Z3}::trout_char_quota.r4" in sep
    jan, _ = _andreen(db)
    assert f"{Z4}::trout_char_quota.r1" in jan["RB"]


# --------------------------------------------------------------------------- tiny bundles
def _tiny(tmp_path, rules: list) -> str:
    path = str(tmp_path / f"tiny{len(list(tmp_path.iterdir()))}.sqlite")
    con = sqlite3.connect(path)
    con.execute("create table section_ruleset (sid integer, set_id integer)")
    con.execute("create table ruleset (set_id integer, entry_id text, rule_id text, via text)")
    con.execute("insert into section_ruleset values (1, 0)")
    con.executemany("insert into ruleset values (0, ?, ?, 'reach')",
                    [(x["entry"], x["rule"]) for x in rules])
    con.commit()
    con.close()
    R._RULES_BY_PATH[path] = {(x["entry"], x["rule"]): dict(
        {"type": "retention_limit", "dimension": "daily", "family": "retention"}, **x)
        for x in rules}
    return path


STREAM = [{"op": "within", "area_id": "area:region:9", "feature_types": ["stream"]}]
REL = {"entry": "z9:q", "rule": "q.rel", "species": ["DV"], "take": 0, "may_target": 1,
       "water": "stream", "dimension": "daily@water=stream", "extents": STREAM, "_rank": 3}
QUOTA = {"entry": "z9:q", "rule": "q.r1", "species": ["TROUT_CHAR"], "take": 5, "_rank": 3}


def _speaking(path, fish="DV") -> set:
    return {f"{x['entry']}::{x['rule']}" for x in R.effective_rules(1, (7, 1), fish, path)
            if x["state"] == "speaks"}


def test_a_stream_release_displaces_its_own_tables_quota(tmp_path):
    assert _speaking(_tiny(tmp_path, [REL, QUOTA])) == {"z9:q::q.rel"}
    # ... for the fish it releases only
    assert _speaking(_tiny(tmp_path, [REL, QUOTA]), "RB") == {"z9:q::q.r1"}


def test_a_release_whose_extents_do_not_draw_only_streams_displaces_nothing(tmp_path):
    loose = dict(REL, extents=[{"op": "within", "area_id": "area:region:9"}])
    assert _speaking(_tiny(tmp_path, [loose, QUOTA])) == {"z9:q::q.rel", "z9:q::q.r1"}


def test_a_closure_a_water_row_and_another_region_are_untouched(tmp_path, monkeypatch):
    """Step 4b's reach. The size clause is not 4b's: it goes by step 5b (moot under the
    release), pinned in test_moot_size_clause.py."""
    # step 6 (two regions: the stricter applies) would take the other region's quota out on
    # its own; switched off here so what is left is step 4b's reach alone
    monkeypatch.setattr(R, "stricter", lambda a, b: False)
    water = {"entry": "r9:creek", "rule": "creek.r1", "species": ["DV"], "take": 2,
             "_rank": 0}
    other = {"entry": "z8:q", "rule": "q.r1", "species": ["TROUT_CHAR"], "take": 5, "_rank": 3}
    size = {"entry": "z9:q", "rule": "q.r4b", "species": ["DV"], "dimension": "daily/size",
            "lengths": [{"max_cm": 60, "take": 0}], "_rank": 3}
    got = _speaking(_tiny(tmp_path, [REL, QUOTA, water, other, size]))
    assert {"z8:q::q.r1", "r9:creek::creek.r1", "z9:q::q.rel"} <= got
    assert "z9:q::q.r1" not in got
    assert _speaking(_tiny(tmp_path, [REL, QUOTA, size])) == {"z9:q::q.rel"}
    shut = dict(QUOTA, take=0, may_target=0, rule="q.r0")
    assert "z9:q::q.r0" in _speaking(_tiny(tmp_path, [REL, shut]))


def test_an_origin_release_leaves_a_quota_keeping_the_other_origin(tmp_path):
    wild = dict(REL, origin="wild", dimension="daily@origin=wild&water=stream")
    assert _speaking(_tiny(tmp_path, [wild, QUOTA])) == {"z9:q::q.rel", "z9:q::q.r1"}


def test_a_water_row_lifting_the_stream_release_leaves_the_quota_speaking(tmp_path):
    """A water row EXEMPT from the stream release (Columbia River, Lardeau River, Upper Arrow's
    drawdown area: "EXEMPT from the regional Nov 1-Mar 31 trout/char catch and release") lifts it
    in step 3; a lifted release is no candidate, so step 4b never runs for it and the region's
    quota speaks. A lift for one fish only (Duncan River: bull trout) leaves the release — and
    its displacement — standing for every other fish."""
    lift = {"entry": "r9:creek", "rule": "creek.lift", "species": ["DV"], "dimension": "lift",
            "exempts": [{"entry_id": "z9:q", "rule_id": "q.rel"}], "_rank": 0}
    assert _speaking(_tiny(tmp_path, [REL, QUOTA, lift])) == {"z9:q::q.r1"}
    trout = dict(REL, species=["TROUT_CHAR"])
    only_dv = dict(lift, exempts=[{"entry_id": "z9:q", "rule_id": "q.rel", "species": ["DV"]}])
    assert _speaking(_tiny(tmp_path, [trout, QUOTA, only_dv])) == {"z9:q::q.r1"}
    assert _speaking(_tiny(tmp_path, [trout, QUOTA, only_dv]), "RB") == {"z9:q::q.rel"}


def test_a_stream_release_out_of_its_dates_displaces_nothing(tmp_path):
    """Region 3's bull trout release holds Aug 1-Oct 31; on Jul 1 it is not in force and the
    quota speaks alone."""
    dated = dict(REL, when={"dates": [{"from_month": 8, "from_day": 1, "to_month": 10,
                                       "to_day": 31}]})
    assert _speaking(_tiny(tmp_path, [dated, QUOTA])) == {"z9:q::q.r1"}
