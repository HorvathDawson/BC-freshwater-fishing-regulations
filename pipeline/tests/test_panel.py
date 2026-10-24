"""The donor panel: who may speak for a point, how loudly, and when to say nothing."""

from __future__ import annotations

import re

import pytest

from pipeline.gauges.consume.panel import (
    MAX_AREA_RATIO, MIN_RECORD_YEARS, MEMBERS_PER_QUANTITY, combine, eligible, panel_for,
    share_of, trust_of, weight_of,
)


# ------------------------------------------------------------------ the gates

def test_a_regulated_gauge_cannot_be_carried_onto_other_water():
    """A percentile at a dammed station is a percentile of a dispatch decision. The Nechako
    runs HIGH in a dry winter and flows into the Fraser above prime water — a downweighted
    donor would still drag the answer the wrong way, so it is barred from being CARRIED."""
    assert not eligible(100.0, 900.0, 40, regulated=True, crossed_lake=False)


def test_a_regulated_gauge_may_still_speak_for_its_own_water():
    """The gate conflated two things. A dammed river's reading cannot be carried to the
    creek over the ridge — and IS what the water is doing where the gauge stands. Barring
    both left the Skagit at the International Boundary, 69 years of record, unable to say
    anything about the Skagit."""
    assert eligible(100.0, 120.0, 40, regulated=True, crossed_lake=False)


def test_the_regulated_allowance_is_much_tighter_than_the_ordinary_one():
    """Past REGULATED_MAX_RATIO the reading is being carried onto water the dam does not
    govern, and the original reasoning applies again."""
    ordinary = eligible(100.0, 300.0, 40, regulated=False, crossed_lake=False)
    dammed = eligible(100.0, 300.0, 40, regulated=True, crossed_lake=False)
    assert ordinary and not dammed


def test_a_regulated_gauge_past_a_lake_is_still_refused():
    """The lake gate is about the daily signal being storage-integrated, which a dam does
    not excuse — the two gates are independent."""
    assert not eligible(100.0, 105.0, 40, regulated=True, crossed_lake=True)


def test_a_regulated_gauge_with_a_short_record_is_still_refused():
    assert not eligible(100.0, 105.0, 2, regulated=True, crossed_lake=False)


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
    """A ranked reserve, `MEMBERS_PER_QUANTITY` deep (test_live_panels.py): deep enough that silent members leave an
    answer, bounded because every member is a row in the paint file."""
    cand = [(f"S{i}", "up", 100.0, 40, False, False) for i in range(10)]
    got = panel_for(cand, 100.0)
    assert len(got) == MEMBERS_PER_QUANTITY
    assert [d.weight for d in got] == sorted((d.weight for d in got), reverse=True)


def test_an_ineligible_candidate_never_reaches_the_panel():
    cand = [("good", "up", 100.0, 40, False, False),
            # Dammed AND far — barred. A dammed donor on this very water is admitted now
            # (see the eligibility tests above), so the one used here has to be distant.
            ("dammed elsewhere", "up", 900.0, 40, True, False),
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


def test_a_short_record_is_admitted_and_discounted_rather_than_barred():
    """A short record does not make a percentile WRONG, it makes it COARSE — six years is
    six observations of the first week of September, so the answer moves in sixths. That is
    a precision problem, and precision is already priced: the weight scales with the record
    and the interval on screen widens to match. Barring it as well charged for the same
    fact twice."""
    assert eligible(100.0, 110.0, 6, regulated=False, crossed_lake=False)
    # ...and it counts for a fraction of what a long record counts.
    short = weight_of(1.0, "up", 6)
    long_ = weight_of(1.0, "up", 40)
    assert short < long_ / 3


def test_a_record_below_the_floor_is_still_refused():
    """Below three there are not enough observations of a given week to call the shape a
    season at all."""
    assert not eligible(100.0, 110.0, 2, regulated=False, crossed_lake=False)


def test_the_record_scale_is_linear_up_to_the_full_mark_and_flat_after():
    """So a station gains influence for every year it has until it has enough, and gains
    nothing for having ninety instead of twenty."""
    assert weight_of(1.0, "up", 10) == pytest.approx(0.5)
    assert weight_of(1.0, "up", 20) == pytest.approx(1.0)
    assert weight_of(1.0, "up", 90) == pytest.approx(1.0)



# ------------------------------------------------------- T125 item 7: one combine, imported

def test_answering_is_the_apps_answer_shape_and_refusals():
    """`answering` mirrors trust.ts `answering` (parity run in app/tools/one-formula.test.ts); here its whole values."""
    from pipeline.gauges.consume.panel import Contribution, answering, estimate
    C = Contribution
    assert estimate(100, []) == {"ok": False, "why": "no-station"}
    assert estimate(100, [C(0.0, "up", 100, 20)]) == {"ok": False, "why": "no-record"}
    assert estimate(100, [C(0.03, "up", 100, 20, False), C(0.97, "up", 100, 20, False)]) == \
        {"ok": False, "why": "too-uncertain"}
    got = estimate(100, [C(0.03, "up", 100, 20), C(0.97, "up", 100, 20, False)])
    assert got["ok"] and got["value"]["donors"] == 2 and got["value"]["trust"] == "close"
    assert got["value"]["shares"] == pytest.approx({0: 0.8, 1: 0.2})
    # the reserve stands back, lowest weight first, while a core member reports
    ans, stood = answering(100, [C(0.02, "up", 100, 20, False), C(0.98, "up", 100, 20, False, reserve=True),
                                 C(0.97, "up", 400, 20, False, reserve=True)])
    assert ans["ok"] and stood == frozenset({1, 2})


def test_no_evaluation_script_rolls_its_own_combine():
    """The handoff scripts that measured owner decisions import `answering`; none ports probit, the refusal or the
    combine again (mutation: paste a `def probit` into one and this fails)."""
    from pathlib import Path
    root = Path(__file__).resolve().parents[2] / "handoff" / "PLAN" / "app-data"
    scripts = sorted(root.rglob("scripts/*.py"))
    users = [p for p in scripts if "estimate" in p.read_text()]
    assert len(users) >= 4
    for p in users:
        src = p.read_text()
        for copy in ("def probit", "def phi", "def _combine", "MAX_USEFUL_SPREAD", "def shares(", "def err("):
            assert copy not in src, f"{p.relative_to(root)}: {copy}"
    for p in users:
        src = p.read_text()
        assert "answering" in src or re.search(r"^from (compare|evaluate) import", src, re.M), p.relative_to(root)


def test_every_evaluation_script_marks_the_reserve_as_the_app_does():
    """The app's `contributionsOf` (app/packages/data/src/panel.ts) marks a member past `CORE_MEMBERS` by panel rank
    as reserve, and `answering` stands it back. Adversarial review (T125 item 7): three of the four scripts built their
    `Contribution`s with no reserve flag, so on a panel of five or more their "app's answer" kept a reserve member the
    app stands back. Every script that builds a `Contribution` asks `reserve_at` (mutation: drop it from one)."""
    from pathlib import Path
    from pipeline.gauges.consume.panel import CORE_MEMBERS, reserve_at
    assert [reserve_at(i) for i in range(CORE_MEMBERS + 2)] == [False] * CORE_MEMBERS + [True, True]
    repo = Path(__file__).resolve().parents[2]
    ts = (repo / "app/packages/data/src/panel.ts").read_text()
    assert "i >= CORE_MEMBERS ? { reserve: true }" in ts            # the rule reserve_at mirrors
    builders = [p for p in sorted((repo / "handoff/PLAN/app-data").rglob("scripts/*.py"))
                if "Contribution(" in p.read_text()]
    assert len(builders) == 4
    for p in builders:
        calls = re.findall(r"Contribution\((?:[^()]|\([^()]*\))*\)", p.read_text())
        assert calls and all("reserve_at(" in c for c in calls), (p.name, calls)
