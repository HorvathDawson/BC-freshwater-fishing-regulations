"""The donor panel: every gauge that can honestly speak for a point, with a weight.

WHAT THIS REPLACES. Today a section gets ONE station, chosen by a symmetric magnitude
ratio, with a trust band — and if none qualifies, silence. That answers 7.6% of the water.
A panel is instead the SET of gauges that each know something: one upstream, one downstream
on the mainstem, and the arithmetic to combine them. "Donor" is the hydrology term for a
gauge that lends its reading to an ungauged point.

NESTED DONORS ONLY, and that is a deliberate v1 boundary rather than an oversight. A gauge
in the next valley shares no water at all — it shares WEATHER, which is a different and
weaker claim, and using it safely needs gates this repo has no data for yet: glacier
fraction, catchment elevation, karst. Worse than weak, an ungated neighbour can be
ANTI-correlated: in August a glacier-fed river sits near its seasonal high while the
rain-fed creek over the ridge sits near its low, so a distance kernel would confidently
pull the answer the wrong way. Nested donors share actual water and need no such gate.

FOUR GATES, and they are gates rather than weights on purpose. A weight says "trust this
less"; a gate says "this cannot speak here", and for a categorical invalidity that is the
only correct treatment — no amount of downweighting makes a dam's release schedule into a
statement about rainfall.

    regulation   A percentile at a regulated station is a percentile of somebody's dispatch
                 decision. The Nechako is the sharp case: its seasons are INVERTED, high in
                 a dry winter, and it flows into the Fraser above prime water. HYDAT's
                 STN_REGULATION carries year_from/year_to, so a station that was natural
                 and later dammed is barred only for the period it was dammed.
    area ratio   The drainage-area ratio method is defensible while the two catchments are
                 within a factor of about three, and degrades to meaningless past ten. A
                 soft weight never reaches zero, and the far-apart pairs are exactly the
                 ones that dominate where gauges are sparse — which is where the app most
                 wants an answer and least deserves one.
    lake         Below a large lake the daily signal is storage-integrated and weeks
                 lagged; an inflow gauge carries essentially no daily information about the
                 outflow. Named cases: the lower Adams below Adams Lake, the Cowichan below
                 its weir, the Stamp below Great Central. A travel time in hours is
                 meaningless there, so the relationship is refused rather than lagged.
    record       Below ten years there is no percentile worth publishing: at n=10, p=0.9,
                 the sampling standard error is about 9.5 points at one sigma.

WHAT IS NOT HERE YET, so nobody has to rediscover it: regime similarity (centre-of-volume
date), glacier fraction, karst, and the travel-time lag. The first is computable from HYDAT
today and is the next thing to add; the rest need data that is not on disk.
"""

from __future__ import annotations

import collections
import math
from dataclasses import dataclass

from pipeline.atlas.graph.drainage import AreaModel

#: Beyond this ratio between the two catchments, a transferred reading is not evidence.
#:
#: 1000, NOT 3, AND THAT IS A MEASUREMENT RATHER THAN A LOOSENING. Both hydrology reviews
#: said the drainage-area ratio is defensible to about a factor of three. That is correct
#: about transferring a DISCHARGE, and this transfers a PERCENTILE, which is a much weaker
#: and much more robust claim. Measured on 9,495 nested gauge pairs — every pair in the
#: province where both ends have a real ECCC area and ten years of record — the error grows
#: barely at all across four orders of magnitude:
#:
#:     area ratio     median error   p90    days over 25 points
#:        1-10x          11.7 pts   38.5          23%
#:       10-100x         15.5       45.5          31%
#:      100-1,000x       18.5       52.4          38%
#:    1,000-10,000x      20.6       54.4          43%
#:   10,000-100,000x     22.3       57.3          46%
#:
#: A hard gate at 3 covers 0.1% of stream sections — seventy times WORSE than the design it
#: replaces — for an error of 11.7 points instead of 18.5. At 1,000 it covers 26.9%, which
#: is 2.8x what ships today, and the error is still small enough to separate a low river
#: from a high one.
#:
#: THE REAL LESSON OF THAT TABLE IS THE TOP ROW. Even a donor of nearly identical size is
#: out by 11.7 points at the median, so NO answer this produces is precise, at any ratio.
#: That is why the estimate must be shown as a band rather than a number, and why widening
#: the gate is safe: the honesty lives in the interval, not in the threshold.
MAX_AREA_RATIO = 1000.0

#: Measured error, in percentile points, by how far apart the two catchments are. From the
#: table above. `trust_of` turns a ratio into the pair a screen should show.
ERROR_BY_RATIO: tuple[tuple[float, str, float], ...] = (
    (10.0, "close", 11.7),
    (100.0, "near", 15.5),
    (1_000.0, "distant", 18.5),
    (10_000.0, "far", 20.6),
    (float("inf"), "remote", 22.3),
)

#: Refuse when the interval is so wide it cannot separate a low river from a high one.
#: Reviewer's rule, and it is the quantitative version of the silence the old design got
#: from a threshold chosen by taste.
MAX_USEFUL_SPREAD = 40.0


def trust_of(area_ratio: float) -> tuple[str, float]:
    """`(class, +/- percentile points)` for a donor this far from the target in size.

    A NAME AND A NUMBER, because the name alone was the old mistake: `good`/`fair`/`weak`
    were labels for bands nobody had measured, so they could not be compared, could not be
    combined, and could not tell a reader how wrong the answer might be.
    """
    for limit, name, err in ERROR_BY_RATIO:
        if area_ratio <= limit:
            return name, err
    return ERROR_BY_RATIO[-1][1], ERROR_BY_RATIO[-1][2]

#: How sharply weight falls with the share of catchment the two have in common.
SHARE_ALPHA = 1.0

#: A donor BELOW you is diluted by everything that joins between; one above is missing it.
#: The asymmetry is small at nowcast and grows with forecast horizon, which this does not
#: model yet — so it stays near 1 rather than pretending to a precision it has not earned.
DOWNSTREAM_PENALTY = 0.85

#: A percentile from ten years and one from ninety are not the same claim.
RECORD_FULL_YEARS = 20

#: The shortest record that may rank a reading at all.
#:
#: WAS TEN, AND TEN WAS ARBITRARY. The number that matters is not the floor but what the
#: floor costs: 24 of the 431 stations transmitting today have a record between three and
#: ten years and were silent because of it — among them the Skagit above Klesilkwa, six
#: years old, on a river whose two other gauges are dead since 1955 and dam-controlled.
#:
#: A short record does not make a percentile WRONG, it makes it coarse: six years gives six
#: observations of "the first week of September", so the answer moves in sixths and the
#: extremes are unmeasured. That is a precision problem, and precision is already carried —
#: `RECORD_FULL_YEARS` scales such a donor's weight to 5/20, so it counts a quarter of what
#: a ninety-year station counts and the interval on screen widens to match. Barring it as
#: well was pricing the same fact twice.
#:
#: Three, and the scaling does the rest. A three-year station counts 3/20 — fifteen per
#: cent of what a ninety-year one counts — so it can tip an answer only where nothing better
#: exists, which is exactly where it should be allowed to. Below three there are not enough
#: observations of a given week to call the shape a season at all.
MIN_RECORD_YEARS = 3

#: Below this total weight the panel says nothing. Silence is a real answer.
#:
#: It is now a floor on the TOTAL rather than on each member, which is the thing a weighted
#: average makes easy to lose: three bad donors will happily average to a plausible-looking
#: percentile, and each one individually passing a gate says nothing about the sum.
MIN_TOTAL_WEIGHT = 0.05

#: How many donors a panel keeps. Past a few the extra ones are redundant with each other
#: rather than independent, and this estimator does not yet model that redundancy.
MAX_MEMBERS = 4

#: How close two catchments must be before a REGULATED station may speak.
#:
#: The regulation gate is right about TRANSFER and wrong about measurement, and the two were
#: not separated. A percentile from a dammed river says nothing about rainfall on the creek
#: over the ridge — that is the Nechako case the gate was written for. But on the dammed
#: river ITSELF the reading is not an inference at all: it is what the water is doing, at
#: the place it is doing it, and refusing it leaves the reader with nothing where the app
#: has a live measurement.
#:
#: Measured: 137 of the 431 stations transmitting today are flagged regulated in HYDAT —
#: nearly a third of the live network, and among them the Skagit at the International
#: Boundary, 69 years of record on a river with three gauges and, until now, no answer.
#:
#: 1.5 is deliberately tight. At that ratio the two catchments are all but the same
#: drainage, so the release schedule that makes the reading wrong elsewhere is exactly what
#: this water is doing. Past it the reading is being carried onto water the dam does not
#: govern, and the original gate is right again.
REGULATED_MAX_RATIO = 1.5


@dataclass(frozen=True)
class Donor:
    station: str
    role: str                 # "up" | "down"
    share: float              # 0..1, how much of the smaller catchment is in the larger
    weight: float

    def as_row(self) -> tuple:
        return (self.station, self.role, round(self.share, 4), round(self.weight, 4))


#: The error of the best donor there is, in percentile points — the top row of the ladder.
#: Weights are expressed against it, so a donor of identical catchment weighs 1.
BEST_ERROR = ERROR_BY_RATIO[0][2]


def error_for(area_ratio: float) -> float:
    """The donor's error in percentile points, INTERPOLATED between the measured rows.

    `trust_of` returns the row a ratio falls in, which is what a LABEL needs: "close" is a
    word and a word cannot be interpolated. A WEIGHT needs the number, and taking the row's
    number makes the ladder a step function — every donor between 1x and 10x weighs exactly
    the same, so a gauge on nearly the same catchment counts no more than one draining six
    times as much. Measured on the Harrison: a 4,313 km2 donor for a 4,584 km2 reach tied
    with a 795 km2 one, and the tie was an artefact of the bins rather than of the data.

    The measurement is five points on a log axis, so this is linear in log10(ratio) between
    them: 11.7 at 10x, 15.5 at 100x, 18.5 at 1,000x, 20.6 at 10,000x, 22.3 beyond. Below
    10x it is flat at 11.7, because that is the best the method was ever measured to do and
    pretending otherwise would invent precision the calibration does not support.
    """
    r = area_ratio if area_ratio and area_ratio >= 1.0 else 1.0
    lo_limit, _lo_name, lo_err = ERROR_BY_RATIO[0]
    if r <= lo_limit:
        return lo_err
    prev_limit, prev_err = float(lo_limit), lo_err
    for limit, _name, err in ERROR_BY_RATIO[1:]:
        if not math.isfinite(limit):
            return err
        if r <= limit:
            f = ((math.log10(r) - math.log10(prev_limit))
                 / (math.log10(limit) - math.log10(prev_limit)))
            return prev_err + (err - prev_err) * f
        prev_limit, prev_err = float(limit), err
    return ERROR_BY_RATIO[-1][2]


#: How much worse a donor on ANOTHER river is than one on your own, in error terms.
#:
#: THE MODEL KNEW ONLY CATCHMENT SIZE, and two gauges of the same size can be two entirely
#: different relationships: one sits on the same blue line as you — the water literally flows
#: past both points — and one sits on a tributary, where you share the weather and nothing
#: else. The Skeena is the case that exposed it. At Usk the panel held four donors: two on
#: the Skeena reading the 77th and 78th percentile, the Babine at the 24th and the Bulkley at
#: the 62nd. Weighted identically they disagreed by more than the interval can express, so
#: the app refused and drew "no baseline" over a river with two of its own gauges reporting.
#:
#: IT IS A JUDGEMENT, NOT A MEASUREMENT, and that is the difference between it and
#: `ERROR_BY_RATIO` above. The 9,495-pair calibration was run over nested pairs without
#: asking whether the pair shared a channel, so it has no opinion here. Doubling the error —
#: quartering the weight — says "a tributary is about twice as uncertain as your own river at
#: the same size", which is conservative next to the 54-point disagreement on the Skeena and
#: is the number to replace first when the pairs are re-measured with this split.
TRIBUTARY_ERROR_FACTOR = 2.0


def weight_of(share: float, role: str, years: int, same_river: bool = True) -> float:
    """`(best error / this donor's error)^2 * direction * record` — inverse variance.

    IT WAS `share`, AND `share` CONTRADICTS THE MEASUREMENT.

    Using the catchment overlap as the weight assumes the error grows in proportion to the
    size difference: a donor ten times bigger is a tenth as good, a hundred times bigger a
    hundredth. The 9,495-pair calibration says otherwise — the error goes 11.7, 15.5, 18.5,
    20.6, 22.3 points across FOUR ORDERS OF MAGNITUDE of size difference. A 1,000x donor is
    not a thousandth as informative; it is about half as informative.

    The two disagreements compounded into a visible failure. `MAX_AREA_RATIO` was widened to
    1,000 on the strength of that calibration, so panels were built from donors the weighting
    then valued at 0.001 — below `MIN_TOTAL_WEIGHT` on their own and usually together. The
    result: 244,719 of 249,237 sections had a panel and said nothing, and the map drew the
    "no baseline" purple across most of the province while a tap on the same water answered
    perfectly well.

    Inverse variance is also simply the right combiner for what this does. Averaging
    estimates of differing precision, the weight that minimises the variance of the result is
    1/sigma^2, and sigma is exactly what `trust_of` returns. Squaring the ratio of errors
    keeps a perfect donor at 1 and puts the most distant at 0.28 — the spread the measurement
    actually found, rather than the thousandfold spread the old formula invented.

    `direction`, `record` and `same_river` stay multiplicative: where the donor sits, how
    well its own percentile is pinned, and whether it is even on your river. None of the
    three is captured by the area ratio, which is all the ladder knows about.
    """
    rec = min(1.0, max(0.0, years / RECORD_FULL_YEARS))
    direction = 1.0 if role == "up" else DOWNSTREAM_PENALTY
    if share <= 0.0:
        return 0.0
    err = error_for(1.0 / share)
    if not same_river:
        err *= TRIBUTARY_ERROR_FACTOR
    return ((BEST_ERROR / err) ** 2) * direction * rec


def eligible(area_target: float | None, area_donor: float | None,
             years: int, regulated: bool, crossed_lake: bool) -> bool:
    """The four gates, in one place so the reasons cannot drift apart."""
    if crossed_lake:
        return False
    if years < MIN_RECORD_YEARS:
        return False
    if not area_target or not area_donor or area_target <= 0 or area_donor <= 0:
        return False
    ratio = max(area_target, area_donor) / min(area_target, area_donor)
    # A REGULATED STATION MAY SPEAK FOR ITS OWN WATER AND NOTHING ELSE — see
    # REGULATED_MAX_RATIO. It used to be barred outright, which conflated "this reading
    # cannot be carried elsewhere" with "this reading is not a measurement".
    if regulated:
        return ratio <= REGULATED_MAX_RATIO
    return ratio <= MAX_AREA_RATIO


def share_of(area_target: float, area_donor: float) -> float:
    """How much of the smaller catchment sits inside the larger. Nested pairs only."""
    return min(area_target, area_donor) / max(area_target, area_donor)


def combine(values: list[tuple[float, float]]) -> tuple[float, float] | None:
    """Weighted percentiles -> one percentile, and a spread. `[(percentile, weight)]`.

    IN PROBIT SPACE, NOT DIRECTLY. Percentiles are uniform on [0,1] and averaging uniforms
    concentrates toward 0.5 — so a point served by three donors would report closer to
    normal than one served by one, which is backwards: the app would play down extremes
    exactly where it knows the most. Mapping each to a normal quantile first, averaging
    there, and mapping back removes that.

    The returned spread is the weighted standard deviation IN PROBIT SPACE, converted at
    the mean, and it is the honest half of the answer: three donors spanning the 10th to
    the 60th is a catchment doing something complicated, and the screen should say so
    rather than print their average.
    """
    kept = [(p, w) for p, w in values if w > 0 and 0.0 < p < 1.0]
    total = sum(w for _, w in kept)
    if not kept or total < MIN_TOTAL_WEIGHT:
        return None
    zs = [(_probit(p), w) for p, w in kept]
    mean = sum(z * w for z, w in zs) / total
    var = sum(w * (z - mean) ** 2 for z, w in zs) / total
    lo, hi = _norm_cdf(mean - math.sqrt(var)), _norm_cdf(mean + math.sqrt(var))
    return _norm_cdf(mean), hi - lo


def _probit(p: float) -> float:
    """Inverse normal CDF, via the error function. Clamped off the asymptotes."""
    q = min(0.999, max(0.001, p))
    # erfinv through a Newton step on erf, which is in the standard library.
    lo, hi = -6.0, 6.0
    for _ in range(60):
        mid = (lo + hi) / 2
        if _norm_cdf(mid) < q:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2


def _norm_cdf(z: float) -> float:
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def panel_for(candidates: list[tuple],
              area_target: float | None) -> list[Donor]:
    """Gates, weights and a cap, over
    `(station, role, area, years, regulated, lake[, same_river])`.

    `same_river` is optional so a synthetic caller can leave it off; absent, a donor is
    treated as being on your own river, which is the assumption the model made everywhere
    before the Skeena showed what it costs.

    Sorted by weight so the cap keeps the best, and so the screen can list them in the order
    they actually mattered.
    """
    out: list[Donor] = []
    for cand in candidates:
        station, role, area_donor, years, regulated, crossed_lake = cand[:6]
        same_river = bool(cand[6]) if len(cand) > 6 else True
        if not eligible(area_target, area_donor, years, regulated, crossed_lake):
            continue
        assert area_target is not None
        sh = share_of(area_target, area_donor)
        out.append(Donor(station, role, sh, weight_of(sh, role, years, same_river)))
    out.sort(key=lambda d: -d.weight)
    return out[:MAX_MEMBERS]
