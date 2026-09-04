"""The donor panel: who may speak for a point, how loudly, and when to say nothing."""

from __future__ import annotations

import pytest

from pipeline.atlas.gauges.panel import (
    MAX_AREA_RATIO, MIN_RECORD_YEARS, MAX_MEMBERS, combine, eligible, panel_for,
    share_of, trust_of, weight_of,
)


# ------------------------------------------------------------------ the gates

def test_a_regulated_gauge_cannot_speak_for_other_water():
    """A percentile at a dammed station is a percentile of a dispatch decision. The Nechako
    runs HIGH in a dry winter and flows into the Fraser above prime water — a downweighted
    donor would still drag the answer the wrong way, so it is barred."""
    assert not eligible(100.0, 120.0, 40, regulated=True, crossed_lake=False)


def test_a_lake_in_the_path_ends_the_relationship():
    """Below a large lake the daily signal is storage-integrated and weeks lagged, so an
    inflow gauge carries no daily information about the outflow — the lower Adams, the
    Cowichan below its weir. A lag in hours cannot express that, so it is refused."""
    assert not eligible(100.0, 120.0, 40, regulated=False, crossed_lake=True)


def test_catchments_too_different_in_size_are_refused_outright():
    """There is still a bound — but it is where the MEASUREMENT put it, not where the
    textbook rule for transferring a discharge did. See MAX_AREA_RATIO."""
    assert eligible(100.0, 100.0 * MAX_AREA_RATIO, 40, False, False)
    assert not eligible(100.0, 100.0 * MAX_AREA_RATIO * 1.01, 40, False, False)
    assert not eligible(100.0, 100.0 / (MAX_AREA_RATIO * 1.01), 40, False, False)


def test_trust_is_a_measured_number_and_not_a_label():
    """`good`/`fair`/`weak` were names for bands nobody had measured, so they could not be
    compared, combined, or used to tell a reader how wrong the answer might be."""
    close_name, close_err = trust_of(2.0)
    remote_name, remote_err = trust_of(90_000.0)
    assert close_name != remote_name
    assert 0 < close_err < remote_err


def test_the_error_grows_with_distance_but_never_explodes():
    """The finding that made widening the gate defensible: four orders of magnitude of
    area ratio costs less than a doubling of the error."""
    errs = [trust_of(r)[1] for r in (2, 40, 400, 4_000, 90_000)]
    assert errs == sorted(errs)
    assert errs[-1] < errs[0] * 2


def test_even_the_closest_donor_is_not_precise():
    """The top row of the table is the real lesson: no answer this produces is precise at
    any ratio, which is why the estimate must be shown as a band and not a number."""
    assert trust_of(1.0)[1] > 10.0


def test_a_short_record_is_not_a_percentile():
    """At ten years and p=0.9 the sampling standard error is about 9.5 points at one
    sigma — larger than most of the signal being conveyed."""
    assert not eligible(100.0, 110.0, MIN_RECORD_YEARS - 1, False, False)
    assert eligible(100.0, 110.0, MIN_RECORD_YEARS, False, False)


def test_a_missing_area_is_not_a_small_one():
    assert not eligible(None, 100.0, 40, False, False)
    assert not eligible(100.0, None, 40, False, False)
    assert not eligible(0.0, 100.0, 40, False, False)


# ------------------------------------------------------------------ the weights

def test_share_is_symmetric_and_peaks_when_the_two_are_the_same_water():
    assert share_of(100.0, 100.0) == 1.0
    assert share_of(50.0, 100.0) == share_of(100.0, 50.0) == 0.5


def test_a_gauge_below_you_counts_for_less_than_one_above():
    """Not a preference: a donor below is diluted by everything that joins between, and one
    above is missing it. Same size of error, opposite sign, and only the upstream one is
    already in transit toward you."""
    assert weight_of(0.8, "down", 40) < weight_of(0.8, "up", 40)


def test_a_thin_record_votes_at_less_than_full_strength():
    assert weight_of(0.9, "up", 10) < weight_of(0.9, "up", 40)
    assert weight_of(0.9, "up", 40) == weight_of(0.9, "up", 90)   # capped, not unbounded


def test_the_panel_keeps_the_best_and_caps_its_size():
    """Past a few donors the extra ones are redundant with each other rather than
    independent, and this estimator does not model that redundancy yet."""
    cand = [(f"S{i}", "up", 100.0, 40, False, False) for i in range(10)]
    got = panel_for(cand, 100.0)
    assert len(got) == MAX_MEMBERS
    assert [d.weight for d in got] == sorted((d.weight for d in got), reverse=True)


def test_an_ineligible_candidate_never_reaches_the_panel():
    cand = [("good", "up", 100.0, 40, False, False),
            ("dammed", "up", 100.0, 40, True, False),
            ("past a lake", "up", 100.0, 40, False, True),
            ("too big", "up", 9_000_000.0, 40, False, False)]
    assert [d.station for d in panel_for(cand, 100.0)] == ["good"]


# ------------------------------------------------------------------ combining

def test_agreeing_donors_give_a_tight_answer_near_them():
    got = combine([(0.10, 1.0), (0.14, 0.5)])
    assert got is not None
    value, spread = got
    assert 0.09 < value < 0.15
    assert spread < 0.10


def test_averaging_does_not_drag_the_answer_toward_normal():
    """PERCENTILES ARE UNIFORM, and averaging uniforms concentrates toward 0.5 — so a point
    served by several donors would report closer to normal than one served by one. That is
    backwards: the app would play down extremes exactly where it knows the most. Combining
    in probit space is what removes it, and the test is that three donors agreeing at the
    10th do NOT drift upward."""
    one = combine([(0.10, 1.0)])
    three = combine([(0.10, 1.0), (0.10, 1.0), (0.10, 1.0)])
    assert one is not None and three is not None
    assert three[0] == pytest.approx(one[0], abs=0.01)


def test_disagreement_comes_back_as_a_wide_spread_not_a_confident_average():
    """Three gauges spanning the 10th to the 60th is a catchment doing something
    complicated, and the screen should show a range rather than their midpoint."""
    got = combine([(0.10, 1.0), (0.60, 1.0)])
    assert got is not None
    assert got[1] > 0.3


def test_below_the_floor_it_says_nothing_at_all():
    """The best property of the design it replaces: it refuses. A weighted average makes
    that easy to lose — three bad donors will happily average to a plausible number."""
    assert combine([(0.4, 0.001)]) is None
    assert combine([]) is None


def test_a_zero_weight_donor_cannot_vote():
    assert combine([(0.9, 0.0)]) is None
