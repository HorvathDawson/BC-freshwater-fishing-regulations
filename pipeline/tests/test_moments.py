"""WEEKDAY AND HOURS RULES DECIDE (user ruling 2026-10-08; review D2, gap G5 — answers 2.1).

A rule held on some weekdays ("Kokanee — 5 per day, on Saturdays and Sundays", Kootenay Lake's Lower
West Arm, p.37) or some hours ("No fishing, one hour after sunset to one hour before sunrise", the
Fraser above Mission) used to stand "beside" and decide nothing: the arm's card said Region 4's 15
kokanee a day. The reader is now asked at a MOMENT (`calendar.Moment`) and such a rule decides at the
moments it covers.

Pinned here: the moment partition (`calendar.moments`), `read.in_force` at a moment, the stored
verdicts of the arm and of the Fraser, the status index's day (a night never closes the day), the
answers file's moments and `display.closing`, and that no shipped angler text carries a developer
instruction (review B17).

`UI_EXPORT_BUNDLE` (verdicts beside it) and `ANSWERS_EXPORT_DIR` point the data tests at a side set.
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path

import pytest

from pipeline.deliver import calendar as CAL
from pipeline.deliver.bundle import read as R

BUNDLE = Path(os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE)
EXPORT_DIR = Path(os.environ.get("ANSWERS_EXPORT_DIR") or Path(R.BUNDLE).parent.parent / "regs")

LWA = "r4:kootenay_lake_lower_west_arm_for_location_see_map_on_page_34@4-7::kootenay_lake_lower_west_arm"
LWA_ITEM = "wbk:-22"
FRASER_NIGHT = ("r2:fraser_river_upstream_of_the_cpr_bridge_at_mission@2-4::"
                "fraser_river_upstream_of_mission.r3")
SAT_SUN = {"weekdays": ["Saturday", "Sunday"]}
MON_FRI = {"weekdays": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]}
NIGHT = {"hours": {"start": {"solar": "sunset", "offset_min": 60},
                   "end": {"solar": "sunrise", "offset_min": -60}}}


# --------------------------------------------------------------------------------------------
# The moment partition and `in_force` at a moment (no data)
# --------------------------------------------------------------------------------------------

def test_a_key_with_no_weekday_or_hours_rule_has_one_moment():
    assert CAL.moments([{"when": None}, {"when": {"dates": [{"from_month": 1, "from_day": 1,
                                                             "to_month": 3, "to_day": 31}]}}]) \
        == [CAL.ALWAYS]


def test_weekday_sets_cut_the_week_and_an_hours_window_cuts_the_day():
    ms = CAL.moments([{"when": SAT_SUN}, {"when": MON_FRI}])
    assert [CAL.weekday_names(m.weekdays) for m in ms] == [MON_FRI["weekdays"], SAT_SUN["weekdays"]]
    assert all(m.hours is None for m in ms)
    ms = CAL.moments([{"when": NIGHT}])
    assert [(m.weekdays, m.inside) for m in ms] == [(CAL.ALL_WEEK, False), (CAL.ALL_WEEK, True)]
    # Kitsumkalum: Saturday on one stretch, Sunday on another -> three classes
    ms = CAL.moments([{"when": {"weekdays": ["Saturday"]}}, {"when": {"weekdays": ["Sunday"]}}])
    assert sorted(m.weekdays for m in ms) == sorted([31, 32, 64])


def test_two_hours_windows_on_one_key_are_refused():
    other = {"hours": {"start": {"at": "21:00", "offset_min": 0},
                       "end": {"at": "05:00", "offset_min": 0}}}
    with pytest.raises(CAL.CalendarError):
        CAL.moments([{"when": NIGHT}, {"when": other}])


def test_in_force_at_a_moment_decides_and_without_one_stands_beside():
    sat, mon = CAL.Moment(CAL.weekday_mask(["Saturday", "Sunday"])), \
        CAL.Moment(CAL.weekday_mask(MON_FRI["weekdays"]))
    assert R.in_force(SAT_SUN, (7, 4), sat) == "yes"
    assert R.in_force(SAT_SUN, (7, 6), mon) == "no"
    assert R.in_force(SAT_SUN, (7, 4)) == "part"            # the day only: beside, as before
    with pytest.raises(ValueError):
        R.in_force(SAT_SUN, (7, 4), CAL.ALWAYS)               # a moment straddling the weekdays
    out, inside = CAL.moments([{"when": NIGHT}])
    assert R.in_force(NIGHT, (7, 1), inside) == "yes"
    assert R.in_force(NIGHT, (7, 1), out) == "no"
    dated = {**NIGHT, "dates": [{"from_month": 8, "from_day": 1, "to_month": 12, "to_day": 31}]}
    assert R.in_force(dated, (7, 1), inside) == "no"          # out of its dates at any hour


# --------------------------------------------------------------------------------------------
# The stored verdicts and the status index (the bundle and its verdicts)
# --------------------------------------------------------------------------------------------

@pytest.fixture(scope="module")
def store():
    from pipeline.deliver.verdicts.store import VerdictStore
    v = BUNDLE.with_name("verdicts.sqlite")
    if not BUNDLE.is_file() or not v.is_file():
        pytest.skip("no bundle / verdicts (UI_EXPORT_BUNDLE)")
    return VerdictStore.open(v, BUNDLE)


def _keys_holding(store, rid: str):
    import sqlite3
    e, r = rid.split("::")
    db = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    try:
        return [k for (k,) in db.execute(
            "SELECT k.key_ix FROM rule_key k JOIN ruleset s ON s.set_id = k.set_id "
            "WHERE s.entry_id = ? AND s.rule_id = ? ORDER BY k.key_ix", (e, r))]
    finally:
        db.close()


def _states(store, key, day, fish, moment):
    return {store.rule_ids[x.rule]: x.state.value
            for x in store.on_day(key, day, fish, "none", moment)}


def test_the_lower_west_arm_keeps_5_kokanee_at_weekends_and_releases_them_on_weekdays(store):
    (key,) = _keys_holding(store, f"{LWA}.r3")
    ms = store.moments(key)
    assert [CAL.weekday_names(m.weekdays) for m in ms] == [MON_FRI["weekdays"], SAT_SUN["weekdays"]]
    jul_4 = CAL.day_of((7, 4))
    weekday, weekend = _states(store, key, jul_4, "KO", 0), _states(store, key, jul_4, "KO", 1)
    assert weekday[f"{LWA}.r4"] == "speaks" and f"{LWA}.r3" not in weekday
    assert weekend[f"{LWA}.r3"] == "speaks" and f"{LWA}.r4" not in weekend
    # Region 4's kokanee quota is displaced on both
    assert "displaced" in {s for r, s in weekday.items() if r.startswith("z4:species_quotas")}
    with pytest.raises(SystemExit):
        store.on_day(key, jul_4, "KO", "none")                # a moment must be named


def test_a_night_closure_closes_its_hours_never_the_day(store):
    from pipeline.deliver import status_index as SI
    keys = _keys_holding(store, FRASER_NIGHT)
    assert keys
    for key in keys:
        ms = store.moments(key)
        assert [m.inside for m in ms] == [False, True]
        for d in (CAL.day_of((1, 15)), CAL.day_of((7, 1))):
            out, inside = _states(store, key, d, "RB", 0), _states(store, key, d, "RB", 1)
            assert FRASER_NIGHT not in out
            assert inside[FRASER_NIGHT] == "speaks"
            assert store.closed(key, store.reading_of(key, d, 1))
        prof = SI.key_profile(store, key)
        night = SI.moment_profiles(store, key)[1]
        assert all(c == SI.CLOSED for c in night)
        assert prof == SI.moment_profiles(store, key)[0]      # the day is the day's


def test_exactly_the_keys_holding_a_weekday_or_hours_rule_have_moments(store):
    import sqlite3
    db = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    try:
        held = {k for (k,) in db.execute(
            "SELECT DISTINCT k.key_ix FROM rule_key k JOIN ruleset s ON s.set_id = k.set_id "
            "JOIN rule r ON r.entry_id = s.entry_id AND r.rule_id = s.rule_id "
            "WHERE json_extract(r.when_, '$.weekdays') IS NOT NULL "
            "OR json_extract(r.when_, '$.hours') IS NOT NULL")}
    finally:
        db.close()
    assert held
    assert {k for k in store.keys if len(store.moments(k)) > 1} == held
    assert all(store.moments(k) == [CAL.ALWAYS] for k in store.keys if k not in held)


# --------------------------------------------------------------------------------------------
# The answers file
# --------------------------------------------------------------------------------------------

@pytest.fixture(scope="module")
def shipped():
    a, e = EXPORT_DIR / "ui-rules-answers.json", EXPORT_DIR / "ui-rules-export.json"
    if not a.is_file() or not e.is_file():
        pytest.skip("no answers file (ANSWERS_EXPORT_DIR)")
    wire = json.loads(a.read_text())
    if "moments" not in wire:
        pytest.skip("an answers file from before 2.1")
    return wire, json.loads(e.read_text())


def _decided(t, fish):
    h = t["rows"]["fish"][fish]
    return {o: (d["status"], d["daily"]) for o, d in h.items()}


def test_the_card_answers_the_weekday_asked(shipped):
    from pipeline.deliver.answers import encode as E
    wire, data = shipped
    sat = E.tap(wire, data, LWA_ITEM, 0, 7, 4, weekday="Saturday")
    mon = E.tap(wire, data, LWA_ITEM, 0, 7, 6, weekday="Monday")
    assert _decided(sat, "KO") == {"hatchery": ("keep", 5), "wild": ("keep", 5)}
    assert _decided(mon, "KO") == {"hatchery": ("release", None), "wild": ("release", None)}
    with pytest.raises(Exception):
        E.tap(wire, data, LWA_ITEM, 0, 7, 4)                  # the weekday must be named


def test_display_closing_is_the_closed_predicate(shipped):
    from pipeline.deliver.types import GAME_FISH
    wire, _ = shipped
    sec = wire["sections"]["display"]
    assert sec["version"] == 3
    for f in sec["frames"]:
        if f.get("status") == "tidal":
            continue
        shut = {x for _, fs in f["closing"] for x in fs}
        assert (f["status"] == "closed") == (set(GAME_FISH) <= shut)


# --------------------------------------------------------------------------------------------
# Review B17: no shipped angler text tells a developer what to do
# --------------------------------------------------------------------------------------------

DEVELOPER_WORDS = re.compile(r"\bShow this\b|\bshow it\b|\bdisplay\b|\bthe page\b|\bUI\b|"
                             r"\bthe client\b|\brender\b|\bnever read its\b", re.I)


def _angler_texts(data: dict, wire: dict):
    from pipeline.deliver.answers.common import TIDAL_NOTE, TIDAL_STATE
    from pipeline.tools.export_ui_rules import STEELHEAD_NO_RULES
    yield "TIDAL_NOTE", TIDAL_NOTE
    yield "TIDAL_STATE.licence", TIDAL_STATE["licence"]
    yield "STEELHEAD_NO_RULES", STEELHEAD_NO_RULES
    for item, w in data["waters"].items():
        if w.get("tidal"):
            yield f"{item}.tidal.guide", w["tidal"]["guide"]
    for i, r in enumerate(wire["sections"]["display"]["rules"]):
        if r.get("plain"):
            yield f"display.rules[{i}].plain", r["plain"]
    for item, w in wire["sections"]["display"]["waters"].items():
        for p in w["parts"]:
            if p:
                for k in ("label", "runs", "place", "hint"):
                    if p.get(k):
                        yield f"{item}.{k}", p[k]


def test_no_shipped_angler_text_carries_a_developer_instruction(shipped):
    wire, data = shipped
    from pipeline.tools.export_codec import expand
    guide = json.loads((EXPORT_DIR / "ui-rules-guide.json").read_text())
    doc = expand(data, guide)
    bad = [(where, t) for where, t in _angler_texts(doc, wire) if DEVELOPER_WORDS.search(t)]
    assert not bad, bad[:5]


def test_the_check_catches_the_old_tidal_note():
    old = ("Tidal water: … Show this note at the top of the water, and never read its sections as "
           "'open under the general rules'.")
    assert DEVELOPER_WORDS.search(old)
