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
MIN_RECORD_YEARS = 10

#: Below this total weight the panel says nothing. Silence is a real answer.
#:
#: It is now a floor on the TOTAL rather than on each member, which is the thing a weighted
#: average makes easy to lose: three bad donors will happily average to a plausible-looking
#: percentile, and each one individually passing a gate says nothing about the sum.
MIN_TOTAL_WEIGHT = 0.05

#: How many donors a panel keeps. Past a few the extra ones are redundant with each other
#: rather than independent, and this estimator does not yet model that redundancy.
MAX_MEMBERS = 4


@dataclass(frozen=True)
class Donor:
    station: str
    role: str                 # "up" | "down"
    share: float              # 0..1, how much of the smaller catchment is in the larger
    weight: float

    def as_row(self) -> tuple:
        return (self.station, self.role, round(self.share, 4), round(self.weight, 4))


def weight_of(share: float, role: str, years: int) -> float:
    """`share^alpha * direction * record`. See the module docstring for each term."""
    rec = min(1.0, max(0.0, years / RECORD_FULL_YEARS))
    direction = 1.0 if role == "up" else DOWNSTREAM_PENALTY
    return (share ** SHARE_ALPHA) * direction * rec


def eligible(area_target: float | None, area_donor: float | None,
             years: int, regulated: bool, crossed_lake: bool) -> bool:
    """The four gates, in one place so the reasons cannot drift apart."""
    if regulated or crossed_lake:
        return False
    if years < MIN_RECORD_YEARS:
        return False
    if not area_target or not area_donor or area_target <= 0 or area_donor <= 0:
        return False
    ratio = max(area_target, area_donor) / min(area_target, area_donor)
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


def panel_for(candidates: list[tuple[str, str, float, int, bool, bool]],
              area_target: float | None) -> list[Donor]:
    """Gates, weights and a cap, over `(station, role, area, years, regulated, lake)`.

    Sorted by weight so the cap keeps the best, and so the screen can list them in the
    order they actually mattered.
    """
    out: list[Donor] = []
    for station, role, area_donor, years, regulated, crossed_lake in candidates:
        if not eligible(area_target, area_donor, years, regulated, crossed_lake):
            continue
        assert area_target is not None
        sh = share_of(area_target, area_donor)
        out.append(Donor(station, role, sh, weight_of(sh, role, years)))
    out.sort(key=lambda d: -d.weight)
    return out[:MAX_MEMBERS]
