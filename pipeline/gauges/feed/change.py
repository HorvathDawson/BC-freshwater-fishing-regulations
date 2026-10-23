"""EXPECTED CHANGE (T52): the largest move each forecast run expects over the next ten days, read DAY BY DAY.

    owner 2026-10-22: "even if it is a quick spike then drop we want to show this river will have a big change within
    the CLEVER forecast (it could also use multiple forecasts) … this will look at the daily forecast, not just the 1,
    3, 5, 10, so that we don't miss day-long spikes."

WHY THE SAMPLED HORIZONS ARE NOT ENOUGH. `forecast_percentiles` reads one model step at +1, +3, +5 and +10 days. On the
feed of 2026-10-22, 33 of the 72 CLEVER runs whose peak is at least twice today's flow peaked on a day none of those four
samples, and 08MH090's 26-fold rise on day six was invisible to the summary layer AND to +5. So this reads every day of
the run: the day's highest and lowest step (an hourly model's day has 24; a daily model's, one).

ANCHORED ON THE MODEL'S OWN TODAY, NOT ON THE GAUGE. A model's first step is not the gauge's reading: COFFEE's day 0 runs
a median 1.45x the observed flow (125 of 151 runs more than 25% off; six at 0.0), ELF's 1.06x, CLEVER's 1.02x. Measuring
the forecast against the observed reading would turn that bias into a rise nobody forecast. So every ratio here is the
model against ITSELF (`peak / base`, base = the model's value now), and only the percentile — which needs a flow on the
gauge's scale — rescales the model's day by `observed / base` before ranking it.

ITS DAYS ARE PACIFIC STANDARD TIME. The Centre's stamps are a PST wall clock (measured against the gauges; T126), so a
run's days are Centre days. The clock is read in ONE place, `forecast.instant` / `forecast.CENTRE_ZONE`; this file asks
it and keeps no zone of its own.

WHAT THIS DOES NOT DO: decide where a section's change is. A station's change rides the donor panels like any other
reading (the index row's `change`, coded by `coded()`), so a section's expected change is weighed, led and refused by
exactly the rules every other horizon is.
"""

from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from typing import Callable

from pipeline.gauges.feed.forecast import CENTRE_ZONE, instant, is_day

# THE WINDOW, in days after today: the same ten days the map's last horizon (+10) reaches. ELF publishes thirty; a
# seasonal low-flow model's day 25 is not "about to change", and a window that differed per model would make one map
# colour mean three things.
CHANGE_DAYS = 10

# WHAT "BIG" MEANS — named, and the same numbers the app words and colours with (`pipeline.tools.emit_gauge_policy`
# writes them into core). A RISE is big at twice today's flow or a quarter of the record up for the date; a FALL at half
# today's flow or a quarter of the record down. Either measure is enough: 0.2 -> 0.6 m3/s on a creek is a percentile
# jump and a tripling; 400 -> 520 m3/s on the Fraser in freshet may be a quarter of the record and only 1.3x.
RISE_RATIO = 2.0
FALL_RATIO = 0.5
BIG_POINTS = 0.25

# A RISE THAT COMES BACK is a SPIKE: some later day in the window whose highest step is back under today plus half the
# rise (a fall's mirror: back over today less half the drop). Otherwise it is a STEP — it holds to the end of the window —
# unless the extreme IS the last day read: then the run stops while it is still moving, and whether it holds is not
# something the forecast says. That is OPEN (T52 review: 30 of the 57 big moves with no return on 2026-10-22 were on the
# last day, 18 of them COFFEE's five-day runs; calling them steps put "and stays up" in the panel on no evidence).
SPIKE_BACK = 0.5

# A RUN OLDER THAN THIS IS NOT A FORECAST ANY MORE (`STALE_AFTER.forecast` in core, 36 h): ELF and COFFEE issue daily,
# CLEVER once or twice a day. The live feed still carried an ELF run issued 09-24 and a COFFEE run from 10-15.
STALE_HOURS = 36.0

# Which run answers for the station on the map when more than one speaks: CLEVER is the owner's named model and the
# calibrated one (day 0 within 2% of the gauge); COFFEE next (daily, five days, biased high at day 0); ELF last (a
# thirty-day low-flow model). The panel names EVERY run; this picks only the one colour (owner question Q1 in T52).
CHANGE_ORDER = ("CLEVER", "COFFEE", "ELF")

# HOW A RATIO RIDES THE DONOR PATHWAY. `estimate` (core/trust.ts) combines percentiles in probit space and nothing else,
# so a station's change is carried as Φ(log2(ratio) / CHANGE_SCALE): steady is 0.5, twice today 0.69, four times 0.84,
# half 0.31. Averaging those in probit space is averaging log-ratios — a section's expected change is the weighted
# geometric mean of its donors' — and the panel's weights, own-gauge lead, reserve and refusals apply unchanged. Kept
# inside (CODE_FLOOR, 1 − CODE_FLOOR) because `estimate` drops a vote at exactly 0 or 1.
CHANGE_SCALE = 2.0
CODE_FLOOR = 0.005

Rank = Callable[[float, date], "float | None"]


def coded(ratio: float) -> float:
    """A change ratio as the percentile-shaped number the donor pathway combines (see CHANGE_SCALE)."""
    z = math.log2(ratio) / CHANGE_SCALE
    p = 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))
    return min(1.0 - CODE_FLOOR, max(CODE_FLOOR, p))


def _local(at: str) -> datetime | None:
    """One forecast stamp as a naive Centre wall clock (`forecast.instant`, then the Centre's zone); a bare date is that
    day's midnight."""
    t = instant(at)
    return None if t is None else t.astimezone(CENTRE_ZONE).replace(tzinfo=None)


def issued_age_h(issued_at: str | None, now: datetime) -> float | None:
    """Hours since the run was issued (its stamp read on the Centre's clock like every other), or None when it carries no time."""
    t = _local(issued_at) if issued_at else None
    if t is None:
        return None
    return (now.astimezone(CENTRE_ZONE).replace(tzinfo=None) - t).total_seconds() / 3600.0


def stale(run: dict | None, now: datetime) -> bool:
    """A run issued more than STALE_HOURS ago. One whose issue time is unknown is not called stale: every live run
    carries one (`forecast._issued`), and refusing the unknown would silently drop a run on a format change."""
    age = issued_age_h((run or {}).get("issuedAt"), now)
    return age is not None and age > STALE_HOURS


def daily_extremes(series: dict, start: datetime, last: date) -> list[tuple[str, float, float]]:
    """`[(day, lowest, highest)]` of the forecast trace (`mid`, never the bounds) from `start` to the end of `last`.

    An hourly model's day is its 24 steps; a daily model's is its one. A step with no value (ELF's thirty hindcast days
    carry `mid` null) is no step. `start` is the local now: the hours of today already past are not ahead of anyone.
    """
    days: dict[str, list[float]] = {}
    for at, v in zip(series.get("at") or [], series.get("mid") or []):
        if v is None:
            continue
        t = _local(at)
        if t is None or t.date() > last:
            continue
        # a daily step stamped midnight is the WHOLE day, so today's counts though midnight has passed
        if t < start and not (t.date() == start.date() and (series.get("step") == "1d" or is_day(at))):
            continue
        days.setdefault(t.date().isoformat(), []).append(float(v))
    return [(d, min(vs), max(vs)) for d, vs in sorted(days.items())]


def base_of(series: dict, now_local: datetime) -> float | None:
    """The model's own value NOW: the hourly step nearest the local clock (within 3 h), or a daily model's today."""
    best: tuple[float, float] | None = None
    for at, v in zip(series.get("at") or [], series.get("mid") or []):
        if v is None:
            continue
        t = _local(at)
        if t is None:
            continue
        if series.get("step") == "1d" or is_day(at):
            if t.date() == now_local.date():
                return float(v)
            continue
        off = abs((t - now_local).total_seconds()) / 3600.0
        if off <= 3.0 and (best is None or off < best[0]):
            best = (off, float(v))
    return None if best is None else best[1]


def _back(days: list[tuple[str, float, float]], after: int, rise: bool, base: float, extreme: float) -> str | None:
    """The first day after the extreme that is back within half of it — a spike's end — or None (a step, or no move)."""
    if (extreme <= base) if rise else (extreme >= base):
        return None                 # no rise (or no fall) to come back from
    line = base + SPIKE_BACK * (extreme - base)
    for d, lo, hi in days[after + 1:]:
        if (hi <= line) if rise else (lo >= line):
            return d
    return None


def run_change(run: dict, now: datetime, observed: float | None, rank: Rank | None) -> dict | None:
    """One run's expected change over the window, or None when it cannot say (no series, no base, not fresh).

    `observed` is the gauge's flow now (m3/s); `rank(flow, day)` is that day's percentile for a flow (None: no
    envelope). Without either the ratios still stand and the percentile moves are absent.
    """
    series = run.get("series") or {}
    if not series or stale(run, now):
        return None
    now_local = now.astimezone(CENTRE_ZONE).replace(tzinfo=None)
    base = base_of(series, now_local)
    if base is None or base <= 0:
        return None                 # a run whose today is 0.0 (six COFFEE runs) cannot carry a ratio
    days = daily_extremes(series, now_local.replace(minute=0, second=0, microsecond=0),
                          now_local.date() + timedelta(days=CHANGE_DAYS))
    if not days:
        return None
    ip = max(range(len(days)), key=lambda i: (days[i][2], -i))
    it = min(range(len(days)), key=lambda i: (days[i][1], i))
    scale = observed / base if observed and observed > 0 else None

    def pct(v: float, day: str) -> float | None:
        if rank is None or scale is None:
            return None
        return rank(v * scale, date.fromisoformat(day))

    p0 = rank(observed, now_local.date()) if rank is not None and observed and observed > 0 else None

    def end(i: int, rise: bool) -> dict:
        d, lo, hi = days[i]
        v = hi if rise else lo
        p = pct(v, d)
        return {"day": d, "ratio": round(v / base, 4), "value": round(v, 4),
                **({"percentile": round(p, 4)} if p is not None else {}),
                **({"points": round(p - p0, 4) + 0.0} if p is not None and p0 is not None else {}),
                "back": _back(days, i, rise, base, v)}

    peak, trough = end(ip, True), end(it, False)
    big_rise = peak["ratio"] >= RISE_RATIO or peak.get("points", 0.0) >= BIG_POINTS
    big_fall = trough["ratio"] <= FALL_RATIO or trough.get("points", 0.0) <= -BIG_POINTS
    direction = ("both" if big_rise and big_fall else "rise" if big_rise else "fall" if big_fall else "steady")
    return {"model": run.get("model"), "issuedAt": run.get("issuedAt"), "base": round(base, 4),
            "days": [[d, round(lo, 4), round(hi, 4)] for d, lo, hi in days],
            "peak": peak, "trough": trough, "dir": direction}


def _extreme(c: dict) -> tuple[str, dict]:
    """Which end of a run answers for it: the big one, or (both, or neither) the larger move in log terms."""
    if c["dir"] == "rise":
        return "peak", c["peak"]
    if c["dir"] == "fall":
        return "trough", c["trough"]
    up, down = c["peak"]["ratio"], c["trough"]["ratio"]
    if abs(math.log(max(up, 1e-9))) >= abs(math.log(max(down, 1e-9))):
        return "peak", c["peak"]
    return "trough", c["trough"]


def station_change(runs: dict | None, now: datetime, observed: float | None,
                   rank: Rank | None) -> tuple[dict[str, dict], dict | None]:
    """`(per model, summary)`: every fresh run's change, and the one the map reads (the index row's `change`), or None.

    The summary is FLOW ONLY — `discharge` is the coded ratio (see CHANGE_SCALE) — and carries what the words need:
    which model, the direction, the ratio, the day and whether it comes back (`shape`, `back`).
    """
    per: dict[str, dict] = {}
    for model, run in sorted((runs or {}).items()):
        c = run_change({**run, "model": run.get("model") or model}, now, observed, rank)
        if c is not None:
            per[model] = c
    lead = next((m for m in CHANGE_ORDER if m in per), None) or next(iter(sorted(per)), None)
    if lead is None:
        return per, None
    c = per[lead]
    _, e = _extreme(c)
    # KEPT SMALL: the index is fetched to paint the map (~180 KB). One coded number colours; the ratio, the day and the
    # shape say what it is in a line. Every run's own detail is in the station file, opened on a tap.
    summary = {"discharge": round(coded(e["ratio"]), 4), "model": lead, "dir": c["dir"],
               "ratio": e["ratio"], "day": e["day"],
               "shape": "spike" if e["back"] else "open" if e["day"] == c["days"][-1][0] else "step",
               **({"back": e["back"]} if e["back"] else {})}
    return per, summary
