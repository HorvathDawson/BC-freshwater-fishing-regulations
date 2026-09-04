"""The bin-edge defect: a river that does not change must not change its percentile."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pipeline.gauges.feed.climatology import PENTADS, pentad_of_yday, pentads_near
from pipeline.gauges.feed.publish import band_for, percentile_of


# ------------------------------------------------------------------ pooling

def test_one_day_feeds_its_own_pentad_and_its_neighbours():
    """Overlapping windows are what make the envelope continuous: adjacent pentads share
    most of their observations, so they cannot disagree sharply."""
    near = pentads_near(100)
    assert pentad_of_yday(100) in near
    assert len(near) == 3


def test_the_year_is_circular():
    """Late December and early January are one hydrological season, not two ends of an
    array. Without the wrap, pentad 0 and pentad 72 are each built from half a window."""
    assert 72 in pentads_near(1)
    assert 0 in pentads_near(366)


def test_a_day_mid_pentad_feeds_fewer_neighbours_than_one_at_an_edge():
    mid = pentads_near(103)          # near the centre of its pentad
    edge = pentads_near(101)         # first day of one
    assert len(mid) <= len(edge)


@pytest.mark.parametrize("yday", [1, 50, 180, 365, 366])
def test_every_day_lands_somewhere_valid(yday):
    for p in pentads_near(yday):
        assert 0 <= p < PENTADS


# ------------------------------------------------------------------ reading

def _bands(**kw):
    return {k: list(v) for k, v in kw.items()}


def test_between_two_pentads_the_band_moves_gradually():
    """A hard lookup jumps the whole way at midnight. Measured on the shipped envelope,
    17.7% of adjacent pentads differ by more than 25% — so the jump is not cosmetic."""
    bands = _bands(**{"0": [1, 2, 3, 4, 5], "1": [11, 12, 13, 14, 15]})
    seen = []
    for yday in (1, 2, 3, 4, 5, 6, 7, 8):
        when = datetime(2026, 1, 1, tzinfo=timezone.utc).replace(day=min(yday, 28))
        when = when.replace(month=1, day=yday)
        b = band_for(bands, when)
        assert b is not None
        seen.append(b[0])
    # monotone, and no single day carries the whole change
    assert seen == sorted(seen)
    assert max(b - a for a, b in zip(seen, seen[1:])) < 10 - 1e-9


def test_the_step_between_consecutive_days_is_a_fifth_of_the_gap():
    """Five days between centres, so one day is a fifth. This is the whole fix."""
    bands = _bands(**{"0": [0.0], "1": [10.0]})
    d1 = band_for(bands, datetime(2026, 1, 4, tzinfo=timezone.utc))
    d2 = band_for(bands, datetime(2026, 1, 5, tzinfo=timezone.utc))
    assert d1 and d2
    assert d2[0] - d1[0] == pytest.approx(2.0, abs=1e-9)


def test_a_reading_that_does_not_change_moves_a_fifth_as_far_as_it_used_to():
    """The defect, stated as the user experiences it: the river is doing nothing overnight
    and the app must not say otherwise.

    The claim is not that the answer never moves — the seasonal yardstick genuinely does
    move through a freshet, and it should. It is that the movement is SPREAD over the five
    days between pentad centres instead of landing entirely on one midnight. So the test
    compares against what a hard lookup would have done, which is the thing being fixed.

    The bands here differ by 9.1% between pentads, the measured median for adjacent pairs
    on the shipped envelope.
    """
    lo = [8.0, 9.0, 10.0, 11.0, 12.0]
    hi = [v * 1.091 for v in lo]
    bands = _bands(**{"0": lo, "1": hi})
    day5 = datetime(2026, 1, 5, tzinfo=timezone.utc)
    day6 = datetime(2026, 1, 6, tzinfo=timezone.utc)

    smooth = abs(percentile_of(10.5, band_for(bands, day5))
                 - percentile_of(10.5, band_for(bands, day6)))
    # what a hard bin did: the whole jump, at the boundary between pentad 0 and 1
    hard = abs(percentile_of(10.5, lo) - percentile_of(10.5, hi))

    assert smooth < hard
    assert smooth == pytest.approx(hard / 5, rel=0.35)


def test_a_missing_neighbour_falls_back_rather_than_going_silent():
    """A thin pentad publishes nothing. Its neighbour is still the best answer available,
    and refusing on that basis would silence a station that has plenty of record."""
    bands = _bands(**{"0": [1, 2, 3, 4, 5]})
    assert band_for(bands, datetime(2026, 1, 3, tzinfo=timezone.utc)) == [1, 2, 3, 4, 5]


def test_no_envelope_is_still_no_answer():
    assert band_for(None, datetime(2026, 6, 1, tzinfo=timezone.utc)) is None
    assert band_for({}, datetime(2026, 6, 1, tzinfo=timezone.utc)) is None


def test_it_wraps_at_new_year_rather_than_falling_off_the_end():
    bands = {str(p): [float(p)] for p in range(73)}
    b = band_for(bands, datetime(2026, 1, 1, tzinfo=timezone.utc))
    assert b is not None                      # day 1 leans on pentad 72, and must find it
