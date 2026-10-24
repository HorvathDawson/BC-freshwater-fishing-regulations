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

from pipeline.gauges.feed.forecast import CENTRE_ZONE, instant, step_day

# THE WINDOW, in days after today: the same ten days the map's last horizon (+10) reaches. ELF publishes thirty; a
# seasonal low-flow model's day 25 is not "about to change", and a window that differed per model would make one map
# colour mean three things.
CHANGE_DAYS = 10

# WHAT "BIG" MEANS — named, and the same numbers the app words and colours with (`pipeline.tools.emit.gauge_policy`
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

# WHICH MODELS MAKE A STATION'S CHANGE (T144; the research, with sources, is in the ticket). The decision is
# `change_source`, and nothing else makes it.
#
#   FLOOD MODELS — CLEVER and COFFEE. Both are driven by the Meteorological Service's 10-day weather forecast, so both
#   can see rain or melt coming. CLEVER: a semi-distributed hydrologic model with kinematic-wave channel routing, hourly,
#   recalibrated over the last 20 days each run and shifted to start AT the observed flow (Luo 2015, CLEVER technical
#   reference). COFFEE: a daily unit-hydrograph model for 94 coastal, storm-driven watersheds, run when a rain event is
#   on or forecast; its numbers are INSTANTANEOUS PEAKS (each day's flow times the station's historical
#   instantaneous-to-daily peak ratio — Luo 2018, COFFEE technical reference §3.3), which is why its day 0 sits a median
#   1.45x the gauge. Every ratio here is a model against its OWN today, so that factor cancels.
#   Where both reach a day, that day is their WEIGHTED GEOMETRIC MEAN of ratios (logs averaged — ratios multiply, and the
#   donor pathway's code is log2). EQUAL WEIGHTS: no head-to-head verification of the two is published. CLEVER's own
#   (2015, 71 interior freshet stations) put 75% of stations' 10-day relative error at or under 30% but 75% of stations'
#   r² under 0.5 ("the trend of the streamflow could be very difficult to forecast"); COFFEE's reference reports
#   "acceptable" on worked examples only. Where both run is mostly the coast in the rain season — COFFEE's own domain,
#   not the one CLEVER was built for — and on the 2026-10-23 feed their peaks agree in the median (log2 ratio −0.007;
#   same big-rise call at 82 of 122 stations). A one-line change here if the owner wants CLEVER ahead (owner question).
#   LOW-WATER MODEL — ELF. An empirical recession extrapolation with NO weather input (Luo 2023, ELF technical reference:
#   "forecasts of rise … are not recommended … because the model has no meteorological data input"; its skill is lowest
#   in April–May). It speaks for a station ONLY when no flood model does, and then the change carries `lowWater`, which
#   the app turns into a caveat. Never beside a flood model: it cannot see the rain the other two are forecasting.
FLOOD_MODELS = ("CLEVER", "COFFEE")
FLOOD_WEIGHTS = {"CLEVER": 1.0, "COFFEE": 1.0}
LOW_WATER_MODEL = "ELF"
# The order the panel names runs in (generated into core as `CHANGE_POLICY.order`).
CHANGE_ORDER = FLOOD_MODELS + (LOW_WATER_MODEL,)

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
        if t < start and not (t.date() == start.date() and step_day(series, at) is not None):
            continue
        days.setdefault(t.date().isoformat(), []).append(float(v))
    return [(d, min(vs), max(vs)) for d, vs in sorted(days.items())]


def base_of(series: dict, now_local: datetime, key: str = "mid") -> float | None:
    """The model's own value NOW: the hourly step nearest the local clock (within 3 h), or a daily model's today.
    `key` is the trace read (`mid`, or `level_mid` for the stage)."""
    best: tuple[float, float] | None = None
    for at, v in zip(series.get("at") or [], series.get(key) or []):
        if v is None:
            continue
        t = _local(at)
        if t is None:
            continue
        if step_day(series, at) is not None:
            if t.date() == now_local.date():
                return float(v)
            continue
        off = abs((t - now_local).total_seconds()) / 3600.0
        if off <= 3.0 and (best is None or off < best[0]):
            best = (off, float(v))
    return None if best is None else best[1]


def _back(days: list[tuple[str, float, float]], after: int, rise: bool, base: float, extreme: float,
          until: str | None = None) -> str | None:
    """The first day after the extreme that is back within half of it — a spike's end — or None (a step, or no move).
    `until`: the last day the models that made the extreme still speak; a day past it is other models', not a return."""
    if (extreme <= base) if rise else (extreme >= base):
        return None                 # no rise (or no fall) to come back from
    line = base + SPIKE_BACK * (extreme - base)
    for d, lo, hi in days[after + 1:]:
        if until is not None and d > until:
            return None
        if (hi <= line) if rise else (lo >= line):
            return d
    return None


def _measure(days: list[tuple[str, float, float]], base: float, scale: float | None, rank: Rank | None,
             p0: float | None, stops: tuple[str, ...] = ()) -> dict:
    """THE MEASURE, for a run or a source: the peak and the trough over `days` against `base`, each dated, its
    percentile for the date where it can be ranked (`v * scale` is the flow on the gauge's scale), spike or step, and the
    direction against the named thresholds. One copy, asked by `run_change` and `station_change`.

    `stops` (a source only): the days, before the window's last, where one of the joined models stops (COFFEE's fifth).
    A HANDOVER IS NOT A RETURN (T144 review): the day after COFFEE's last is CLEVER alone, so a joined 3.5x falling to
    CLEVER's own 1.1x the next day is the average losing a voice, not the river coming back. An end's spike is looked
    for only up to the first stop at or after it; not back by then, the end carries `until` (that stop): the models that
    made it stopped while it was still up (or down), and whether it holds is not something they say — `open`."""
    ip = max(range(len(days)), key=lambda i: (days[i][2], -i))
    it = min(range(len(days)), key=lambda i: (days[i][1], i))

    def end(i: int, rise: bool) -> dict:
        d, lo, hi = days[i]
        v = hi if rise else lo
        p = rank(v * scale, date.fromisoformat(d)) if rank is not None and scale is not None else None
        until = min((s for s in stops if s >= d), default=None)
        back = _back(days, i, rise, base, v, until)
        moved = (v > base) if rise else (v < base)
        return {"day": d, "ratio": round(v / base, 4), "value": round(v, 4),
                **({"percentile": round(p, 4)} if p is not None else {}),
                **({"points": round(p - p0, 4) + 0.0} if p is not None and p0 is not None else {}),
                "back": back,
                **({"until": until} if until is not None and back is None and moved else {})}

    peak, trough = end(ip, True), end(it, False)
    big_rise = peak["ratio"] >= RISE_RATIO or peak.get("points", 0.0) >= BIG_POINTS
    big_fall = trough["ratio"] <= FALL_RATIO or trough.get("points", 0.0) <= -BIG_POINTS
    direction = ("both" if big_rise and big_fall else "rise" if big_rise else "fall" if big_fall else "steady")
    return {"base": round(base, 4), "days": [[d, round(lo, 4), round(hi, 4)] for d, lo, hi in days],
            "peak": peak, "trough": trough, "dir": direction}


def _today_rank(now: datetime, observed: float | None, rank: Rank | None) -> float | None:
    """Today's percentile for the gauge's own reading, the zero every `points` is measured from."""
    if rank is None or not observed or observed <= 0:
        return None
    return rank(observed, now.astimezone(CENTRE_ZONE).date())


Days = list[tuple[str, float, float]]


def run_days(run: dict, now: datetime) -> tuple[float, Days] | None:
    """`(base, days)` of one run, unrounded — its own value now and each window day's lowest and highest step — or None
    when it cannot say (no series, not fresh, no base, a today of 0.0, no day in the window)."""
    series = run.get("series") or {}
    if not series or stale(run, now):
        return None
    now_local = now.astimezone(CENTRE_ZONE).replace(tzinfo=None)
    base = base_of(series, now_local)
    if base is None or base <= 0:
        return None                 # a run whose today is 0.0 (six COFFEE runs) cannot carry a ratio
    days = daily_extremes(series, now_local.replace(minute=0, second=0, microsecond=0),
                          now_local.date() + timedelta(days=CHANGE_DAYS))
    return (base, days) if days else None


def run_change(run: dict, now: datetime, observed: float | None, rank: Rank | None) -> dict | None:
    """One run's expected change over the window, or None when it cannot say (no series, no base, not fresh).

    `observed` is the gauge's flow now (m3/s); `rank(flow, day)` is that day's percentile for a flow (None: no
    envelope). Without either the ratios still stand and the percentile moves are absent.
    """
    got = run_days(run, now)
    return None if got is None else _of_run(run, got, observed, rank, _today_rank(now, observed, rank))


def _of_run(run: dict, got: tuple[float, Days], observed: float | None, rank: Rank | None, p0: float | None) -> dict:
    """A run's change from its `run_days`: the model's own days against its own today, ranked on the gauge's scale."""
    base, days = got
    scale = observed / base if observed and observed > 0 else None
    return {"model": run.get("model"), "issuedAt": run.get("issuedAt"), **_measure(days, base, scale, rank, p0)}


def change_source(runs: dict[str, tuple[float, Days]]) -> dict | None:
    """THE ONE DECISION (T144): which runs make a station's Change, day by day over the window.

    `runs` is every run that can say, by model, as `run_days` reads it. Returns `{"models": [...], "lowWater": bool,
    "days": [(day, lowest ratio, highest ratio)], "reach": {model: its last day}}` — each day as a ratio to today — or
    None when no run can say:
      - the FLOOD MODELS present make the window; each day is `combine_day` of those that reach it (T177: both → their
        weighted geometric mean, one → its own); a day past one model's end (COFFEE stops at five) is the other's alone,
        and `reach` says where each stops;
      - no flood model → the LOW-WATER MODEL alone, with `lowWater` true (the app's caveat);
      - never the low-water model beside a flood model, and never a model this file does not name.
    The station-wide choice of WHICH runs (flood ones if any, ELF only when none) is this function's; how a day's
    values join is `combine_day`'s, the one combination every forecast day in the feed is made by.
    """
    flood = [m for m in FLOOD_MODELS if m in runs]
    use = flood or ([LOW_WATER_MODEL] if LOW_WATER_MODEL in runs else [])
    if not use:
        return None
    by_day: dict[str, dict[str, tuple[float, float]]] = {}
    for m in use:
        base, days = runs[m]
        for d, lo, hi in days:
            by_day.setdefault(d, {})[m] = (lo / base, hi / base)

    def joined(d: str, k: int) -> float:
        return combine_day({m: v[k] for m, v in by_day[d].items()})[1]      # type: ignore[index]

    return {"models": use, "lowWater": not flood,
            "days": [(d, joined(d, 0), joined(d, 1)) for d in sorted(by_day)],
            "reach": {m: runs[m][1][-1][0] for m in use}}


# ------------------------------------------------------------------------------- one forecast day, ranked ----
#
# T171 (owner 2026-10-23: "sometimes it has gaps and says weather only despite gauge forecast on both sides"). The
# horizons (`publish.forecast_percentiles`) rank one step at +1/+3/+5/+10; "Like your visit?" needs EVERY day, so the
# feed also ranks each forecast day's DAILY MEAN (`forecast_days`). Both ask ONE rule for which model answers a day.

# A MODEL WHOSE NUMBERS ARE NOT ON THE GAUGE'S SCALE. COFFEE publishes instantaneous PEAKS (each day's flow times the
# station's historical instantaneous-to-daily peak ratio, Luo 2018 §3.3): on the 2026-10-23 feed its day 0 sat a median
# 2.17x the gauge at the 29 stations COFFEE forecasts without CLEVER. Ranked as it stands against a record of daily
# MEANS, that is a high-flow percentile nobody forecast. So before ranking, a peak model's value is put on the gauge's
# scale by `observed / base` (base = the model's own today) — the move `run_change` makes for every run's percentile.
# CLEVER (shifted onto the observed flow at issue) and ELF (1.06x) are ranked as published, as the horizons always were.
PEAK_MODELS = ("COFFEE",)

# How many forecast days are ranked: the Change's window, the map's last horizon (+10).
AHEAD_DAYS = CHANGE_DAYS

# An hourly day counts only when its steps cover it: at least this share of the day's steps carry a value. A run that
# stops partway through a day (issued at 09:00, ending at 08:00 ten days on) has a morning, not a day, and a morning's
# mean ranked against a record of daily means is a different number.
DAY_COVER = 0.75


def combine_day(values: dict[str, float | None]) -> tuple[tuple[str, ...], float, bool] | None:
    """THE ONE COMBINATION OF A FORECAST DAY (T177; T171 made the per-day rule one function, T144 the policy).

    `values` is each fresh run's value for ONE day and ONE quantity, already on the gauge's scale (`gauge_scale`) — or
    a ratio to the model's own today (the Change) — None where it cannot say. Returns `(models, value, lowWater)`:
      - every FLOOD MODEL that answers, joined: their weighted geometric mean (`FLOOD_WEIGHTS`; logs averaged, the way
        the donor pathway averages a change) — CLEVER and COFFEE together where both reach the day, one alone where
        only it does, its value exactly as given. In a JOIN a value at or under zero counts as 1e-9 (a forecast of no
        flow pulls the mean down; never a crash);
      - else the LOW-WATER MODEL alone, `lowWater` true (T144: ELF only where no flood model reaches, carrying its
        caveat; never beside a flood model);
      - else None. A model this file does not name never answers.
    The Change (`change_source`), the +1/+3/+5/+10 horizons (`publish.forecast_percentiles`) and every ranked day
    (`forecast_days`) ask this and nothing else; it replaced T171's first-model `day_model`.
    """
    flood = [(m, float(values[m])) for m in FLOOD_MODELS if values.get(m) is not None]   # type: ignore[arg-type]
    if len(flood) == 1:
        return (flood[0][0],), flood[0][1], False         # one model is its own value, exactly (no log round trip)
    if flood:
        w = sum(FLOOD_WEIGHTS[m] for m, _ in flood)
        v = math.exp(sum(FLOOD_WEIGHTS[m] * math.log(max(x, 1e-9)) for m, x in flood) / w)
        return tuple(m for m, _ in flood), v, False
    if values.get(LOW_WATER_MODEL) is not None:
        return (LOW_WATER_MODEL,), float(values[LOW_WATER_MODEL]), True    # type: ignore[arg-type]
    return None


def model_name(models: tuple[str, ...]) -> str:
    """How the feed names the model(s) behind a day: `CLEVER`, or `CLEVER+COFFEE` where they were joined."""
    return "+".join(models)


def gauge_scale(model: str, run: dict, now: datetime, observed: float | None, quantity: str = "discharge") -> float | None:
    """What a model's value is multiplied by before it is ranked: 1 for a model on the gauge's scale; for a PEAK model
    `observed / base` (its own today), or None — it cannot be ranked — with no reading or no base to scale by."""
    if model not in PEAK_MODELS:
        return 1.0
    key = "level_mid" if quantity == "level" else "mid"
    base = base_of(run.get("series") or {}, now.astimezone(CENTRE_ZONE).replace(tzinfo=None), key)
    if not observed or observed <= 0 or base is None or base <= 0:
        return None
    return observed / base


def day_means(series: dict, key: str = "mid") -> dict[str, float]:
    """`{Centre day: the mean of the trace's steps that day}` — only for days the trace covers: a daily step names its
    whole day; an hourly day needs DAY_COVER of its steps (the step read from the run's own spacing). A null is no step."""
    acc: dict[str, list[float]] = {}
    times: list[datetime] = []
    daily: dict[str, float] = {}
    for at, v in zip(series.get("at") or [], series.get(key) or []):
        t = _local(at)
        if t is not None:
            times.append(t)
        if v is None or t is None:
            continue
        if (named := step_day(series, at)) is not None:
            daily[named] = float(v)
            continue
        acc.setdefault(t.date().isoformat(), []).append(float(v))
    gaps = sorted(g for g in ((b - a).total_seconds() / 3600.0 for a, b in zip(sorted(times), sorted(times)[1:])) if g > 0)
    per_day = 24.0 / gaps[len(gaps) // 2] if gaps else 24.0
    out = {d: sum(vs) / len(vs) for d, vs in acc.items() if len(vs) >= DAY_COVER * per_day}
    out.update(daily)
    return out


def forecast_days(runs: dict | None, now: datetime, observed: float | None, rank: Rank | None) -> dict | None:
    """EVERY FORECAST DAY'S DISCHARGE PERCENTILE (T171): `{"from": first day, "discharge": [...], "model": [...],
    "lowWater": [i, ...]}` for the AHEAD_DAYS Centre days after today, or None when no day can be ranked.

    Each day is every fresh run's DAILY MEAN (`day_means`; a daily model's one step) on the gauge's scale
    (`gauge_scale`), joined by `combine_day` (T177: CLEVER and COFFEE together where both reach the day — `model` then
    reads `CLEVER+COFFEE`, `model_name`), then ranked by `rank(flow, day)` — the station's discharge envelope for THAT
    day, the same one the horizons and the Change rank against. A day no model can rank is null in both
    lists (the app's "weather only", said). `lowWater` lists the indexes of the days the low-water model made — absent
    when none. A stale run never speaks (`stale`). Discharge only: "Like your visit?" compares flow, never a level.
    """
    if not runs or rank is None:
        return None
    first = now.astimezone(CENTRE_ZONE).date() + timedelta(days=1)
    means: dict[str, tuple[dict[str, float], float | None]] = {}
    for model, run in sorted(runs.items()):
        series = (run or {}).get("series")
        if not series or stale(run, now):
            continue
        means[model] = (day_means(series), gauge_scale(model, run, now, observed))
    ps: list[float | None] = []
    ms: list[str | None] = []
    low: list[int] = []
    for k in range(AHEAD_DAYS):
        d = first + timedelta(days=k)
        key = d.isoformat()
        got = combine_day({m: (v[key] * s if key in v and s is not None else None) for m, (v, s) in means.items()})
        p = rank(got[1], d) if got else None
        ps.append(round(p, 4) if p is not None else None)
        ms.append(model_name(got[0]) if got and p is not None else None)
        if got and p is not None and got[2]:
            low.append(k)
    if all(p is None for p in ps):
        return None
    return {"from": first.isoformat(), "discharge": ps, "model": ms, **({"lowWater": low} if low else {})}


def _extreme(c: dict) -> tuple[str, dict]:
    """Which end of a change answers for it: the big one, or (both, or neither) the larger move in log terms."""
    if c["dir"] == "rise":
        return "peak", c["peak"]
    if c["dir"] == "fall":
        return "trough", c["trough"]
    up, down = c["peak"]["ratio"], c["trough"]["ratio"]
    if abs(math.log(max(up, 1e-9))) >= abs(math.log(max(down, 1e-9))):
        return "peak", c["peak"]
    return "trough", c["trough"]


def station_change(runs: dict | None, now: datetime, observed: float | None,
                   rank: Rank | None) -> tuple[dict[str, dict], dict | None, dict | None]:
    """`(per model, source, summary)`: every fresh run's own change; the station's change from `change_source` (the
    station file's `change`, the panel's first line); and the index row's `change` the map reads, or None.

    The summary is FLOW ONLY — `discharge` is the coded ratio (see CHANGE_SCALE) — and carries what the words need:
    which models, the direction, the ratio, the day and whether it comes back (`shape`, `back`), and `lowWater` when
    only the low-water model spoke.
    """
    raw: dict[str, tuple[float, Days]] = {}
    per: dict[str, dict] = {}
    p0 = _today_rank(now, observed, rank)
    for model, run in sorted((runs or {}).items()):
        if (got := run_days(run, now)) is not None:
            raw[model] = got
            per[model] = _of_run({**run, "model": run.get("model") or model}, got, observed, rank, p0)
    chosen = change_source(raw)
    if chosen is None:
        return per, None, None
    # the station's own days are ratios to today already: measured against a base of 1, and ranked on the gauge's scale
    src = {"models": chosen["models"], "lowWater": chosen["lowWater"], "reach": chosen["reach"],
           **_measure(chosen["days"], 1.0, observed if observed and observed > 0 else None, rank, p0,
                      tuple(sorted({d for d in chosen["reach"].values() if d < chosen["days"][-1][0]})))}
    _, e = _extreme(src)
    # KEPT SMALL: the index is fetched to paint the map (~180 KB). One coded number colours; the ratio, the day and the
    # shape say what it is in a line. Every run's own detail, and the source's days, are in the station file.
    summary = {"discharge": round(coded(e["ratio"]), 4), "models": src["models"], "dir": src["dir"],
               "ratio": e["ratio"], "day": e["day"],
               "shape": "spike" if e["back"] else "open" if e.get("until") or e["day"] == src["days"][-1][0]
               else "step",
               **({"back": e["back"]} if e["back"] else {}),
               **({"lowWater": True} if src["lowWater"] else {})}
    return per, src, summary
