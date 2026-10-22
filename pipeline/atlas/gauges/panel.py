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

#: The quantities a donor is ranked in, as `gauge_stats.parameter` names them and the app's Flow / Level views ask.
QUANTITIES = ("discharge", "level")

#: How many donors a panel keeps PER QUANTITY, best weight first — of every station with a record in that quantity,
#: live or not.
#:
#: OWNER 2026-10-22, "estimates use every gauge that could inform a section, chosen live": WHICH of them answers is
#: decided at READ time from the feed (the app re-weights the members reporting the asked quantity), so a station
#: joining or leaving ECCC's real-time list changes the answer on the next feed, with no rebuild. The panel used to be
#: the best 4 OVERALL — historic and live, flow and level mixed — so where those 4 were silent in the asked quantity
#: the section said nothing while a reporting gauge stood fifth. The panel is now a ranked reserve, deep enough per
#: quantity that losing a few members still leaves an answer.
#:
#: EIGHT, MEASURED (handoff/PLAN/app-data/live-panels/scripts, live graph, feed 2026-10-21T18:00Z, against the shipped
#: panels): 4 / 6 / 8 / 12 per quantity give 12,417 / 14,194 / 15,050 / 16,990 member rows (10,589 shipped) and a
#: paint file of +3.9 / +6.9 / +8.2 / +11.2 KB gzip against a 66 KB margin under its 1 MB gate. Flow gains an answer
#: on 69 / 75 / 75 / 75 sections. Past 6 nothing more answers TODAY — 168k of 247k sections have a single candidate
#: at all, so the walk, not this cap, bounds most panels — but where many gauges are in reach (the lower Fraser, the
#: Liard) the deeper reserve is what still answers when several go quiet, for 1.4 KB more than 6.
MEMBERS_PER_QUANTITY = 8

#: THE CORE of a panel: its first this-many members, best weight first — the panel as it stood before the reserve (the
#: old cap, the best four overall). The app's `estimate` (app/packages/core/src/trust.ts `answering`, mirrored for
#: measurement by handoff/PLAN/app-data/live-panels/scripts/evaluate.py) lets the lowest-weight RESERVE members stand
#: back when bringing them in makes the answer too uncertain while a core member reports: a reserve member is there to
#: answer, never to silence. Measured (live graph, feed 2026-10-21T18:00Z): with every reserve member voting, 54 Level
#: sections (384 km) lost the answer their core gave on the same feed; with the back-off, none does, and the shipped
#: panels' answers are unchanged. app/tools/trust-ladder.test.ts holds the app's number equal to this one.
CORE_MEMBERS = 4

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


# ------------------------------------------------------------------ a water's own gauge comes first
#
# OWNER 2026-10-22 (handoff/PLAN/app-integration/decisions.md): "the pitt could use its own gauge" — "it can still use
# auxiliary gauges but own gauge should be main (unless far from click) low magnitude compared to click spot."
#
# A water's OWN gauge is a station matched to that water (gauge.item_id is the section's water). It LEADS the answer:
# it carries at least `OWN_LEAD_SHARE` of it and the other donors split the rest in their usual proportion. It stops
# leading where it is FAR along the water from the tapped section, or drains MUCH LESS than it; there the usual
# weighting applies. Never across quantities: the lead is chosen among the donors REPORTING the quantity asked for, so
# a level-only own gauge (the Pitt's 08MH035) leads Level and is simply absent from Flow.
#
# WHO MAY LEAD is decided here at build time (`may_lead`, from the along-water distance and both catchments, which
# only the pipeline has) and ships as each member's `lead` rank. WHAT LEADING MEANS (`leader_of`, `lead_shares`) runs
# on every tap in the app; `leaderOf` / `leadShares` in app/packages/core/src/trust.ts are the mirror, and
# app/tools/one-formula.test.ts executes both. Real-data evaluation: handoff/PLAN/app-data/own-gauge/REPORT.md.

#: The least share of the answer a leading own gauge carries. "Main" — and the auxiliaries keep the other 30%,
#: split in their usual proportion. A gauge already carrying more on its own merits keeps what it had.
OWN_LEAD_SHARE = 0.7

#: "Low magnitude compared to click spot": an own gauge draining less than a tenth of the tapped section does not
#: lead. Ten is where the measured error ladder leaves its flat best row (`ERROR_BY_RATIO`, "close" to 10x) — inside
#: it a donor is as good as the method has ever been measured to be. A gauge LARGER than the section is not limited
#: by this; the owner named only the smaller case, and `OWN_FAR_*` still bounds it.
OWN_SMALLER_RATIO = 10.0

#: "Far from click", scaled to the river: past a fifth of the river's own length at the larger of the two catchments
#: (Hack's law, `river_length_km`), and never closer than `OWN_FAR_MIN_KM`. A fixed distance cannot serve both ends of
#: the province: 50 km on the lower Fraser adds 5% to its catchment, on a creek it is another valley.
#:
#: HACK'S LAW IS FED REAL CATCHMENTS, NOT THE AREA MODEL'S. The model runs 2-3.6x under the published areas on big
#: rivers (Fraser at Hope 216,600 km² published, 60,356 modelled; Similkameen near Hedley 5,568 vs 2,084), and the
#: law was fitted to real ones, so fed the model it cut the limit 1.6-2.2x short. Both catchments are scaled by the
#: gauge's published / modelled ratio (`area_scale`) — the tapped section is on the gauge's own water, so the model's
#: bias there is the gauge's. Measured: Hope's limit is 415 km, so it leads the whole Fraser below it (158 km to the
#: mouth), Hansard 807 km up does not; Hedley leads the Similkameen 47 km below it (limit 58 km, 32 km at the model's
#: areas); Usk leads the lower Skeena (126 km); a 100 km² creek's gauge leads 10 km.
OWN_FAR_FRACTION = 0.2
OWN_FAR_MIN_KM = 10.0

#: How far the published catchment may sit from the modelled one, either way, and still be believed as the scale for
#: `far_limit_km`. Past it the gauge or the model is wrong about where the gauge stands (08MH028, a Fraser gauge of
#: 232,000 km² matched to a 1.4 km² stub, is 163,265x) and the modelled areas are used as they are. Ten is the
#: "much smaller" bound again; 95% of real-time gauges sit within 0.62-3.66x.
OWN_AREA_TRUST = 10.0

#: How many own gauges per quantity a section keeps as lead candidates, nearest first — live or not (owner
#: 2026-10-22, "chosen live"): the app leads with the best-ranked one REPORTING the quantity asked for, so a quiet
#: first choice hands over to the next. They ride on top of `MEMBERS_PER_QUANTITY` (`panel_for`), so this bounds how
#: big a panel can grow.
OWN_LEADS_PER_QUANTITY = 4

#: Hack's law, L = c * A^h — a river's mainstem length from its catchment. Hack (1957) gives 1.4 * A^0.6 in miles and
#: square miles, which is 1.27 * A^0.6 in km and km². A yardstick for "far", not a measurement of any one river.
HACK_C = 1.27
HACK_H = 0.6


def river_length_km(area_km2: float) -> float:
    """The length of a river draining `area_km2`, by Hack's law."""
    return HACK_C * max(0.0, area_km2) ** HACK_H


def area_scale(area_gauge: float | None, published_km2: float | None) -> float:
    """Published / modelled catchment at the gauge: what the area model's figures are multiplied by to be real ones.
    1 when there is no published area, or it is past `OWN_AREA_TRUST` from the model either way."""
    if not area_gauge or not published_km2 or area_gauge <= 0 or published_km2 <= 0:
        return 1.0
    k = published_km2 / area_gauge
    return k if 1.0 / OWN_AREA_TRUST <= k <= OWN_AREA_TRUST else 1.0


def far_limit_km(area_target: float, area_gauge: float, scale: float = 1.0) -> float:
    """How far along the water an own gauge may sit from the tapped section and still lead. The areas are the model's;
    `scale` (`area_scale`) makes them the real catchments Hack's law speaks of."""
    return max(OWN_FAR_MIN_KM, OWN_FAR_FRACTION * river_length_km(max(area_target, area_gauge) * scale))


def may_lead(area_target: float | None, area_gauge: float | None, distance_m: float,
             published_km2: float | None = None) -> bool:
    """Whether an own gauge this far along the water, this big against the tapped section, leads it.

    `area_*` are the area model's (as every weight is); `published_km2` is the gauge's published catchment
    (`gauge.area_km2`), which only sets the scale of "far". "Much smaller" is a ratio, the same at either scale."""
    if not area_target or not area_gauge or area_target <= 0 or area_gauge <= 0:
        return False
    if area_target > area_gauge * OWN_SMALLER_RATIO:
        return False
    scale = area_scale(area_gauge, published_km2)
    return distance_m <= far_limit_km(area_target, area_gauge, scale) * 1000.0


def leader_of(ranks: list[int], weights: list[float]) -> int | None:
    """The index of the vote that leads: the best-ranked own gauge (rank 1 is the nearest) that is reporting.

    `ranks[i]` is a member's `lead` (0: not an own gauge that may lead); `weights[i]` its usual weight, 0 when it has
    no reading in the quantity asked for. A quiet rank 1 hands the lead to rank 2, not to nobody.
    """
    best: int | None = None
    for i, (r, w) in enumerate(zip(ranks, weights)):
        if r > 0 and w > 0 and (best is None or r < ranks[best]):
            best = i
    return best


def lead_shares(weights: list[float], lead: int | None) -> list[float]:
    """Each vote's share of the answer, 0..1, summing to 1: the usual weights normalised, then — when an own gauge
    leads — that gauge raised to at least `OWN_LEAD_SHARE` and the others scaled into what is left."""
    total = sum(weights)
    if total <= 0:
        return [0.0 for _ in weights]
    norm = [w / total for w in weights]
    if lead is None or weights[lead] <= 0 or sum(1 for w in weights if w > 0) < 2:
        return norm
    own = max(OWN_LEAD_SHARE, norm[lead])
    rest = total - weights[lead]
    return [own if i == lead else (1.0 - own) * w / rest for i, w in enumerate(weights)]


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


def combine(values: list[tuple[float, float]], lead: int | None = None) -> tuple[float, float] | None:
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

    `lead` is the index in `values` of the own gauge that leads (`leader_of`), or None: its share is raised by
    `lead_shares`, as the app's `estimate` does.
    """
    kept = [(i, p, w) for i, (p, w) in enumerate(values) if w > 0 and 0.0 < p < 1.0]
    total = sum(w for _, _, w in kept)
    if not kept or total < MIN_TOTAL_WEIGHT:
        return None
    at = next((k for k, (i, _p, _w) in enumerate(kept) if i == lead), None)
    shares = lead_shares([w for _, _, w in kept], at)
    zs = [(_probit(p), s) for (_i, p, _w), s in zip(kept, shares)]
    mean = sum(z * s for z, s in zs)
    var = sum(s * (z - mean) ** 2 for z, s in zs)
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
              area_target: float | None,
              keep: list[str] | None = None,
              measures: dict[str, set[str]] | None = None) -> list[Donor]:
    """Gates, weights and a cap, over
    `(station, role, area, years, regulated, lake[, same_river])`.

    `same_river` is optional so a synthetic caller can leave it off; absent, a donor is
    treated as being on your own river, which is the assumption the model made everywhere
    before the Skeena showed what it costs.

    Sorted by weight so the cap keeps the best, and so the screen can list them in the order
    they actually mattered.

    THE CAP IS PER QUANTITY (`MEMBERS_PER_QUANTITY`): `measures` is each station's quantities with a record
    (`gauge_stats` parameters), and each quantity keeps its own best few among the stations that have one. The app
    answers Flow from discharge and Level from level, never mixed, so a seat held by a level-only gauge is a seat Flow
    cannot use. A station measuring both sits in both lists and in the panel once. A station with a record in neither
    can answer nothing and is not kept. None (synthetic callers): every station counts in both.

    `keep` names the own gauges that may lead this section: they pass the same gates, and ride ON TOP of the cap
    rather than inside it. A three-year own gauge weighs 3/20 of a long-lived neighbour and would otherwise lose its
    seat to four of them, and then it could not lead. Inside the cap it would take a seat instead — measured, 393
    sections swapped a donor for their own gauge, and where that own gauge reports only level the Flow answer lost a
    donor it had (125 sections fell silent). On top, nothing a panel holds today is ever displaced.
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
    chosen: set[int] = set()
    for q in QUANTITIES:
        chosen.update([i for i, d in enumerate(out)
                       if measures is None or q in measures.get(d.station, ())][:MEMBERS_PER_QUANTITY])
    wanted = set(keep or ())
    return [d for i, d in enumerate(out) if i in chosen or d.station in wanted]
