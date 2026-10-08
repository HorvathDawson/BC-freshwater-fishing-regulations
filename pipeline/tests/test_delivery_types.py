"""THE DELIVERY'S TYPES (`pipeline/deliver/types.py`) and ITS ONE CALENDAR (`pipeline/deliver/
calendar.py`), DATAFLOW P1: every enum equals its registry, an unknown value raises, the stage
artifacts refuse a broken invariant, and the calendar round-trips every day of the leap year."""
from __future__ import annotations

import datetime as dt

import pytest

from pipeline.deliver import calendar as K
from pipeline.deliver import types as T
from pipeline.deliver.bundle import read
from pipeline.deliver.bundle import rules as bundle_rules
from pipeline.regs.parsing import catalogue as C


def _vals(e):
    return [m.value for m in e]


def test_every_enum_is_its_registry():
    assert _vals(T.FishCode) == list(C.BOOK_SPECIES) + list(C.SALMON_FISH) + list(C.PROTECTED_FISH)
    assert T.GAME_FISH == tuple(f for f in C.BOOK_SPECIES if f != "CRA") == C.GAME_FISH
    assert set(_vals(T.SpeciesGroup)) == set(C.SPECIES_GROUPS)
    assert _vals(T.AskOrigin) == ["none", *read.ASKABLE_ORIGINS]
    assert _vals(T.InForce) == list(read.IN_FORCE)
    assert set(_vals(T.RuleState)[:4]) == read.SPEAKER_STATES
    assert set(_vals(T.RuleState)[4:]) == set(read.LOSS_REASONS.values())
    assert _vals(T.LossReason) == list(read.LOSS_REASONS)
    assert _vals(T.Via) == list(read.VIAS)
    assert _vals(T.ClosureGrade) == list(bundle_rules.CLOSURE_GRADES)
    from pipeline.common.registry_kinds import WATER_KINDS
    assert set(_vals(T.WaterKind)) == WATER_KINDS
    assert {T.presence_code(p) for p in _vals(T.SteelheadPresence)} == set(read.STEELHEAD_CODES)
    from pipeline.deliver.bundle.licensing import PROVINCE_EXCEPT_KINDS
    assert _vals(T.ProvinceExceptKind) == list(PROVINCE_EXCEPT_KINDS)
    from pipeline.deliver import status_index as SI
    assert [T.code(T.StatusCode(n)) for n in SI.CODE_NAMES.values()] == list(SI.CODE_NAMES)


def test_every_fish_code_is_a_leaf_the_reader_answers():
    for f in T.FishCode:
        assert C.expand_species([f.value]) == [f.value], f


def test_each_loss_reason_gives_one_loser_state():
    for r in T.LossReason:
        assert T.loss_state(r).value == read.LOSS_REASONS[r.value]
        assert T.code(T.loss_state(r)) not in T.SPEAKER_CODES


def test_an_unknown_value_or_code_raises():
    with pytest.raises(ValueError):
        T.RuleState("won")
    with pytest.raises(ValueError):
        T.by_code(T.AskOrigin, 3)
    with pytest.raises(ValueError):
        T.by_code(T.AskOrigin, True)
    assert T.by_code(T.AskOrigin, 0) is T.AskOrigin("none")
    assert [T.by_code(T.RuleState, T.code(s)) for s in T.RuleState] == list(T.RuleState)


def test_province_except_values_are_every_sorted_subset():
    assert T.province_except_values() == ("", "national_parks", "tidal", "national_parks,tidal")


def test_the_stage_artifacts_refuse_a_broken_invariant():
    T.RuleKey(5, False, True)
    with pytest.raises(TypeError):
        T.RuleKey(5, 0, 1)
    ok = dict(item_id="wbk:1", part_ix=0, set_id=3, key=7, licensing_set=None,
              province_except=(), steelhead_water=False, steelhead=None, steelhead_rules=False,
              home_regions=(), kind="lake", tidal=False, sections=2, rep_sid=11)
    T.Part(**ok)
    for bad in (dict(key=None), dict(province_except=("tidal", "national_parks")),
                dict(steelhead="maybe"), dict(home_regions=("9",)), dict(kind="pond"),
                dict(steelhead_water=True), dict(sections=0)):
        with pytest.raises(ValueError):
            T.Part(**{**ok, **bad})
    T.VerdictRow(1, T.RuleState.speaks, None, None, ()).check()
    T.VerdictRow(1, T.RuleState.displaced, T.LossReason.ladder, 4, ()).check()
    for bad in (T.VerdictRow(1, T.RuleState.speaks, T.LossReason.ladder, 4, ()),
                T.VerdictRow(1, T.RuleState.moot, T.LossReason.ladder, 4, ()),
                T.VerdictRow(1, T.RuleState.displaced, T.LossReason.ladder, 4, (2,)),
                T.VerdictRow(1, T.RuleState.speaks, None, None, (3, 2))):
        with pytest.raises(ValueError):
            bad.check()


def test_the_calendar_round_trips_every_day():
    assert [K.day_of(K.month_day(d)) for d in range(1, K.DAYS + 1)] == list(range(1, 367))
    assert K.day_of(dt.date(2024, 2, 29)) == 60 == K.LEAP
    assert K.day_of(dt.date(2025, 3, 1)) == 61 == K.day_of((3, 1))
    assert K.day_of((12, 31)) == 366 and len(K.MD) == 366
    with pytest.raises(K.CalendarError):
        K.month_day(367)


def test_runs_read_a_year_without_feb_29():
    assert K.runs([]) == []
    assert K.runs(range(1, 367)) == [[1, 1, 12, 31]]
    oct_may = [d for d in range(1, 367) if d >= K.day_of((10, 1)) or d <= K.day_of((5, 31))]
    assert K.runs(oct_may) == [[10, 1, 5, 31]]
    assert K.runs([59, 61]) == [[2, 28, 3, 1]]                   # Feb 28 runs on into Mar 1
    assert K.run_days([[2, 28, 3, 1]]) == [59, 61]
    assert sorted(K.run_days(K.runs(oct_may))) == sorted(set(oct_may) - {K.LEAP})


def test_segments_cut_where_a_when_changes():
    a = K.when_vector({"dates": [{"from_month": 7, "from_day": 1, "to_month": 7, "to_day": 31}]})
    runs, readings = K.segments([a])
    assert runs == [[1, 0], [K.day_of((7, 1)), 1], [K.day_of((8, 1)), 0]]
    assert readings == [1, K.day_of((7, 1))]
    assert K.per_day(runs)[K.day_of((7, 15)) - 1] == 1
    assert K.reading_of(runs, K.day_of((12, 1))) == 0
    assert K.segments_of([0] * 366) == [1]
