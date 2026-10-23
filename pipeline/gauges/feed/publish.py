"""The gauge feed publisher: ECCC readings in, `id -> data` out.

    python -m pipeline.gauges.feed.publish --out app/packages/data/dev/feeds/gauge

WHAT MAKES THIS A FEED. It knows station ids and nothing else. No atlas, no graph, no
bundle, no matching — it fetches by id and writes files named for that id. That is the whole
contract, and it is what lets the same code run as a 30-minute cron against R2 and as a
one-shot command against a dev directory.

DEV AND PROD ARE THE SAME BYTES IN A DIFFERENT PLACE. This writes a directory. In dev that
directory is served by `app/tools/serve-tiles.mjs`; in production the cron syncs it to R2 and
the app reads the R2 origin. Nothing about the artifacts differs, so a bug cannot hide on one
side — and the local run is what stops us hammering ECCC from a browser.

TWO FILES PER STATION IS ONE TOO MANY, so there are not two:

    index.json        {station: {percentile, observedAt, forecast}}   — the map, one fetch
    {station}.json    now + 72 h fine + this year + last year daily   — the sheet, on tap

`index.json` is deliberately tiny (~35 KB for 450 stations) because it is fetched to paint
the map before anything is tapped. Everything a person only wants after tapping is in the
per-station file. See `pipeline/docs/15-live-data-flow.md`.

WHY THE PERCENTILE IS COMPUTED HERE. It needs today's discharge AND the envelope for today's
day-of-year. Doing it once in the publisher means every client agrees by construction, and
means a client can colour the map from a 35 KB file without opening a 63 MB bundle. The
envelope is read from `clim.json`, which the yearly HYDAT tier writes — this never reads the
bundle.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import math
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from pipeline.common.curated import CURATED, SOURCE

BASE = "https://dd.weather.gc.ca/today/hydrometric/csv/BC"
UA = "BC-FishRegs-Hydro/1.0"

# ECCC serves per station; there is no province-wide bulk file. v1 measured ~68 s for the
# full roster over 8 workers, which is comfortably inside a 30-minute tick.
WORKERS = 8
TIMEOUT = 30

# Column names in ECCC's own CSV. Kept as constants because the headers are bilingual and
# contain characters that make them easy to typo into a silent None.
_LEVEL = "Water Level / Niveau d'eau (m)"
_FLOW = "Discharge / Débit (cms)"

# WATER TEMPERATURE COMES FROM A DIFFERENT SERVICE, and it has to.
#
# The datamart CSV above carries level and discharge and nothing else — checked across the
# roster, there is no temperature column at all. The Water Office's real-time service does
# carry it, as parameter 5, and 274 of the 439 transmitting BC stations publish one.
#
# IT IS WORTH THE SECOND REQUEST. British Columbia closes rivers on temperature — the
# Thompson, the Nicola, the Cowichan — and a warm river is one where a released fish dies.
# For an angling app that is not a nice-to-have beside flow; in July it is the question.
# 24 stations were at or above 19 C on the afternoon this was written.
#
# ONE REQUEST FOR THE WHOLE ROSTER, not one per station: the service takes repeated
# `stations[]` parameters, so the entire province is a handful of batched calls rather
# than 439. Batched at 80 to keep each URL and each response a sane size.
_WATEROFFICE = "https://wateroffice.ec.gc.ca/services/real_time_data/csv/inline"
_TEMP_PARAM = "5"
_TEMP_BATCH = 80
# THE SERVICE ANSWERS BURSTS WITH A BARE 422 that succeeds on the next try (measured
# 2026-10-21: three of six batches, 146 of 270 temperatures gone from the map). So a batch
# is tried _TEMP_TRIES times, the wait doubling from _TEMP_BACKOFF_S.
_TEMP_TRIES = 4
_TEMP_BACKOFF_S = 5.0
_sleep = time.sleep

# ECCC's missing-value sentinel. It is not documented in the CSV and it is not blank — it
# is 99999, which reads as a perfectly valid float and renders as a hundred-thousand-degree
# river. Anything at or above this is absent, not hot.
_SENTINEL = 99_000.0


def _get(url: str) -> str | None:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=TIMEOUT) as fh:
            return fh.read().decode("utf-8-sig", errors="replace")
    except (urllib.error.URLError, OSError, TimeoutError):
        return None                 # a station that did not answer is simply absent


def _num(v: str | None) -> float | None:
    try:
        return float(v) if v not in (None, "") else None
    except ValueError:
        return None


def fetch_station(station: str) -> dict | None:
    """One station's recent readings, or None if it did not answer.

    Returning None rather than an empty record is the point: a station that failed to
    respond must not end up in the index, because presence in the index is how the whole
    app decides a gauge is transmitting.
    """
    text = _get(f"{BASE}/hourly/BC_{station}_hourly_hydrometric.csv")
    return _parse_hourly(station, text) if text else None


def _parse_hourly(station: str, text: str) -> dict | None:
    """`fetch_station` without the request: one hourly CSV, or None if it holds no reading."""
    rows = [r for r in csv.DictReader(io.StringIO(text)) if r.get("Date")]
    if not rows:
        return None

    series = [(r["Date"], _num(r.get(_LEVEL)), _num(r.get(_FLOW))) for r in rows]
    series.sort()
    # The most recent row that actually carries a number. ECCC pads the head of the file
    # with rows that have a timestamp and no reading; taking the last row regardless would
    # report "no data" for a station that is reporting perfectly well.
    last = next((s for s in reversed(series) if s[1] is not None or s[2] is not None), None)
    if last is None:
        return None

    return {
        "station": station,
        "at": last[0],
        "level": last[1],
        "discharge": last[2],
        # 5-minute native resolution is denser than anything can be drawn. Thin to the
        # half-hour, which is what v1 settled on after shipping raw 5-minute by accident.
        "recent": [[t, lv, q] for i, (t, lv, q) in enumerate(series) if i % 6 == 0],
    }


# WHO IS TRANSMITTING TODAY, from ECCC itself — the same file `data/fetch_data.py` reads to
# set the roster's `realtime` flag, but read EVERY RUN instead of once.
#
# The roster is a snapshot, and a snapshot drifts. Fetched 1 Sep 2026; by 22 Oct ECCC had
# dropped 13 of its 439 "live" stations (08EG012 Exchamsiks, 08LG028 Nicola at Douglas Lake,
# five Fraser mainstem level gauges...) and begun transmitting from six it marked silent
# (08NM139, 10CD005 among the matched). Driving the feed from the snapshot asked 13 stations
# for 404s every half hour and never asked the six. One request makes the feed's own answer
# to "who is live" as fresh as the readings in it.
STATION_LIST = "https://dd.weather.gc.ca/today/hydrometric/doc/hydrometric_StationList.csv"


def listed_stations(text: str | None) -> set[str] | None:
    """The BC stations ECCC lists as real-time, or None when the list says nothing usable.

    NONE, NOT EMPTY, for a failed or empty answer: an empty set would claim no gauge in the
    province is transmitting and empty the feed on the strength of one bad response.
    """
    if not text:
        return None
    out = {(r.get("ID") or "").strip() for r in csv.DictReader(io.StringIO(text))
           if (r.get("Prov/Terr") or "").strip() == "BC"}
    out.discard("")
    return out or None


def stations_to_ask(roster: list[dict], listed: set[str] | None) -> tuple[list[str], str]:
    """`(ids, source)`: ECCC's list when there is one, else the roster's `realtime` flag.

    The list wins outright, including over a station the roster never heard of — the feed is
    addressed by station id and knows nothing else (see the module note). A station the
    roster calls live and ECCC no longer lists is not asked: its answer would be a 404.
    """
    if listed is not None:
        return sorted(listed), "eccc"
    return sorted(s["station"] for s in roster if s.get("realtime")), "roster"


def roster_drift(roster: list[dict], listed: set[str] | None) -> dict | None:
    """How far the roster's `realtime` flag is from ECCC's list today. None without the list.

    `joined`: listed now, not live in the roster. `left`: live in the roster, no longer
    listed. The donor panels no longer read the roster flag (owner 2026-10-22, "chosen live":
    which members answer is decided from this feed at read time), so drift here costs only the
    roster's own accuracy — said every run, like a stale HYDAT.
    """
    if listed is None:
        return None
    was = {s["station"] for s in roster if s.get("realtime")}
    return {"joined": sorted(listed - was), "left": sorted(was - listed)}


# OWNER-RUN UPSTREAM UPDATES, SAID IN THE INDEX (owner 2026-10-22 late). Each run already learns two things only the
# owner can act on: a newer HYDAT on the listing, and ECCC's real-time list drifting from the roster snapshot. Both
# were console lines nobody reads on a cron. They are now `maintenance` notices in index.json — ONE list for every
# owner task, so another kind joins it without a new field — sent to the owner once (`pipeline.gauges.feed.notify`)
# and read by the app (the Data sources page's HYDAT line). Each names the exact command that clears it.
HYDAT_ACTION = "scripts/update-hydat.sh"
ROSTER_PATH = "data/source/bc_hydrometric_stations.json"
ROSTER_ACTION = (f"rm {ROSTER_PATH} && "
                 "(cd data && PYTHONPATH=.. ../.venv/bin/python fetch_data.py --layers hydrometric_stations) && "
                 "scripts/heavy.sh build 40 -- .venv/bin/python -m pipeline build")


def roster_shown(path: Path) -> str:
    """The roster file as a notice names it: the repo-relative default when `path` IS the default roster."""
    try:
        if path.resolve() == Path(SOURCE / "bc_hydrometric_stations.json").resolve():
            return ROSTER_PATH
    except Exception:                                           # noqa: BLE001 — no data tree: name it as given
        pass
    return str(path)


def roster_action(roster: str) -> str:
    """The command that clears a `roster` notice for THIS roster file (T68: `--stations` named another file and the
    notice still told the owner to refresh the default one, which this feed never read). `fetch_data.py` writes only
    the default path, so another roster is refreshed by copying the fresh default over it."""
    if roster == ROSTER_PATH:
        return ROSTER_ACTION
    fetch, build = ROSTER_ACTION.rsplit(" && ", 1)
    return f"{fetch} && cp {ROSTER_PATH} {roster} && {build}"


def newest_forecast_issue(forecast: dict | None) -> str | None:
    """The newest run's `issuedAt` across every station and model, AS THE STATION FILES CARRY IT; None when no run has
    a readable one. The index's `forecastIssuedAt` (T68: the More tab's Forecasts line reads only the index)."""
    best: tuple[datetime, str] | None = None
    for runs in (forecast or {}).values():
        for run in (runs or {}).values():
            s = (run or {}).get("issuedAt")
            t = _parse(s) if isinstance(s, str) else None
            if t is not None and (best is None or t > best[0]):
                best = (t, s)
    return best[1] if best else None


def maintenance_notices(*, release: str | None, latest: str | None, drift: dict | None,
                        previous: list | None, at: str, roster: str = ROSTER_PATH) -> list[dict]:
    """The owner tasks due this run, sorted by kind: `[{kind, since, current, latest, action}]`; empty when none.

    `since` is the first run that saw the SAME notice (kind and latest), carried from the previous index; a different
    latest is a new notice. A check that could not run this run (`latest` None: the HYDAT listing failed or was
    skipped; `drift` None: no station list) KEEPS the previous notice of its kind rather than calling the task done on
    one failed GET — except a HYDAT notice the envelope has since caught up with. No envelope (`release` None): no
    HYDAT claim either way, there is nothing in use to compare.
    """
    prev = {n.get("kind"): n for n in previous or [] if isinstance(n, dict)}

    def notice(kind: str, current: str, newest: str, action: str) -> dict:
        p = prev.get(kind)
        since = p["since"] if p and p.get("latest") == newest and p.get("since") else at
        return {"kind": kind, "since": since, "current": current, "latest": newest, "action": action}

    out: list[dict] = []
    if release:
        if latest:
            if latest != release:
                out.append(notice("hydat", release, latest, HYDAT_ACTION))
        elif (p := prev.get("hydat")) and p.get("latest") and p["latest"] != release:
            out.append(notice("hydat", release, p["latest"], HYDAT_ACTION))
    if drift is not None:
        if drift.get("joined") or drift.get("left"):
            said = " ".join([f"+{s}" for s in drift.get("joined", [])] + [f"-{s}" for s in drift.get("left", [])])
            out.append(notice("roster", roster, said, roster_action(roster)))
    elif (p := prev.get("roster")) and p.get("latest"):
        out.append(notice("roster", roster, p["latest"], roster_action(roster)))
    return sorted(out, key=lambda n: n["kind"])


def percentile_of(value: float | None, band: list[float | None] | None) -> float | None:
    """Where `value` sits in a p10..p90 band, as 0..1. None when either side is unknown.

    Linear between the published percentiles, clamped outside them. The band deliberately
    stops at p10/p90 because the extremes do not interpolate — a record maximum is one
    storm, not a function of day-of-year (82% error, measured; data contract §5). So a
    reading below p10 reports 0.05 and one above p90 reports 0.95: "at or beyond the
    bottom tenth", which is all the data supports.
    """
    if value is None or not band or len(band) < 5 or any(b is None for b in band):
        return None
    ps = [0.10, 0.25, 0.50, 0.75, 0.90]
    vs = [float(b) for b in band]           # type: ignore[arg-type]
    if value <= vs[0]:
        return 0.05
    if value >= vs[-1]:
        return 0.95
    for i in range(len(vs) - 1):
        lo, hi = vs[i], vs[i + 1]
        if lo <= value <= hi:
            span = hi - lo
            f = 0.0 if span == 0 else (value - lo) / span
            return ps[i] + f * (ps[i + 1] - ps[i])
    return None


# HOW FAR AHEAD THE MAP WILL LOOK.
#
# Not a horizon the models publish — CLEVER runs ten days hourly and ELF thirty daily — but
# the days a person plans a trip around. Mirrored by HORIZONS in
# `app/packages/core/src/conditions.ts` (with 0 for now).
#
# TEN DAYS (owner, 2026-10-20). Past about five the bounds on both models are wide, and the
# answer often reads "normal for the season"; the owner wants the ten-day view on the map
# regardless, for planning a trip a week and a half out. +10 sits at CLEVER's edge, so it is
# usually ELF that answers it (see MODEL_ORDER: per horizon, first model that reaches wins),
# and where no run reaches ten days the horizon is ABSENT — the app reads that as no
# forecast, never as the +5 value.
HORIZONS = (1, 3, 5, 10)

# How far from the requested instant a model step may sit and still answer for it. CLEVER
# steps hourly and ELF daily, so a day-grained model needs most of a day of slack; more than
# this and the value is about a different day.
FORECAST_TOLERANCE_H = 18.0

# CLEVER FIRST. It is the short-range model — hourly, ten days, and calibrated against the
# observed hydrograph — and +1/+3/+5 are inside its range; +10 is at its very end and is
# answered by CLEVER only while the run is fresh enough to reach it. ELF is a thirty-day
# seasonal-volume model; it answers a different question and is the fallback, not the peer.
MODEL_ORDER = ("CLEVER", "ELF")


def _parse(when: str) -> datetime | None:
    """One forecast timestamp. Naive stamps are UTC — the models publish in UTC."""
    try:
        t = datetime.fromisoformat(when.replace("Z", "+00:00"))
    except ValueError:
        return None
    return t if t.tzinfo else t.replace(tzinfo=timezone.utc)


def forecast_at(series: dict | None, when: datetime,
                quantity: str) -> float | None:
    """The model's value NEAREST `when`, or None if no step is close enough.

    NEAREST AND NOT INTERPOLATED. A forecast is a sequence of the model's own steps, and
    a value between two of them is a number no model produced — which matters here because
    the result is then ranked against a record and presented as a percentile. Reporting the
    step the model actually published, or nothing, keeps every number on the map traceable
    to a run.

    Returns None past the end of the horizon rather than the last value, which would report
    day thirty's forecast as day five's.

    HALF A STEP OF SLACK, capped at FORECAST_TOLERANCE_H. Inside a run the nearest step is
    never more than half a step away, so anything further is past the run's end. This
    matters at +10 days, which sits at CLEVER's last hour: a flat 18 h of slack let an hourly
    run that stopped six hours short answer for day ten with its last value — day nine's
    water under day ten's name. A daily model (ELF) keeps its twelve hours.
    """
    if not series:
        return None
    key = "level_mid" if quantity == "level" else "mid"
    vals = series.get(key) or []
    ats = series.get("at") or []
    times = sorted(t for t in (_parse(str(a)) for a in ats) if t is not None)
    gaps = sorted(g for g in ((b - a).total_seconds() / 3600.0
                              for a, b in zip(times, times[1:])) if g > 0)
    tol = (min(FORECAST_TOLERANCE_H, gaps[len(gaps) // 2] / 2.0) if gaps
           else FORECAST_TOLERANCE_H)
    best: tuple[float, float] | None = None                 # (hours off, value)
    for at, v in zip(ats, vals):
        if v is None:
            continue
        t = _parse(str(at))
        if t is None:
            continue
        off = abs((t - when).total_seconds()) / 3600.0
        if off <= tol and (best is None or off < best[0]):
            best = (off, float(v))
    return None if best is None else best[1]


def forecast_percentiles(runs: dict | None, envelopes: dict | None,
                         now: datetime) -> dict[str, dict]:
    """`{"1": {"discharge": 0.42, "model": "CLEVER"}, ...}` — where the river is HEADING.

    THE SAME RANKING AS TODAY'S, AGAINST A DIFFERENT DAY. The whole point of a percentile
    for the date is that low in September is not low in June; a forecast three days out must
    therefore be ranked against the envelope for THAT day, not today's. `band_for` already
    interpolates between pentad centres, so this is one call per horizon with a moved clock.
    Getting this wrong is invisible — the number looks reasonable either way — which is why
    it is stated here rather than left to the reader of the call.

    PER QUANTITY, like everything else in this file: CLEVER publishes discharge only, ELF
    both, and a level forecast ranked against a discharge envelope is arithmetic across two
    units. A horizon with nothing to say is ABSENT rather than null-filled — the map draws
    ungauged grey for an absent one, which is the honest answer.
    """
    if not runs or not envelopes:
        return {}
    out: dict[str, dict] = {}
    for days in HORIZONS:
        when = now + timedelta(days=days)
        for model in MODEL_ORDER:
            run = runs.get(model) or {}
            series = run.get("series")
            if not series:
                continue
            got: dict = {}
            for q in ("discharge", "level"):
                v = forecast_at(series, when, q)
                p = percentile_of(v, band_for(envelopes.get(q), when))
                if p is not None:
                    got[q] = p
            if got:
                # WHICH MODEL SAID SO travels with the number. Two models disagreeing about
                # the same day is a real and frequent thing in the shoulder months, and a
                # percentile with no attribution cannot be checked against the chart the
                # sheet draws from the same run.
                out[str(days)] = {**got, "model": model}
                break                       # first model that can answer wins; see MODEL_ORDER
    return out


def fetch_temperatures(stations: list[str], hours: int = 24) -> dict[str, tuple[str, float]]:
    """station -> (timestamp, degrees C) for whoever is reporting one.

    A station absent from the result is a station with no temperature sensor, which is the
    normal case for 165 of the 439 — never an error, and never a reason to drop the station
    from the index.
    """
    now = datetime.now(timezone.utc)
    start = (now - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
    end = now.strftime("%Y-%m-%d %H:%M:%S")
    out: dict[str, tuple[str, float]] = {}
    for i in range(0, len(stations), _TEMP_BATCH):
        batch = stations[i:i + _TEMP_BATCH]
        q = urllib.parse.urlencode(
            [("stations[]", s) for s in batch]
            + [("parameters[]", _TEMP_PARAM), ("start_date", start), ("end_date", end)])
        text = None
        for i in range(_TEMP_TRIES):
            if i:
                _sleep(_TEMP_BACKOFF_S * 2 ** (i - 1))
            if (text := _get(f"{_WATEROFFICE}?{q}")):
                break
        if not text:
            continue
        for row in csv.reader(io.StringIO(text)):
            # id, date, parameter, value, ...
            if len(row) < 4 or row[2].strip() != _TEMP_PARAM:
                continue
            v = _num(row[3])
            if v is None or v >= _SENTINEL:
                continue
            stn, when = row[0].strip(), row[1].strip()
            # Last one wins; the service returns ascending time.
            if stn:
                out[stn] = (when, v)
    return out


# WHERE THE WATER STOPS BEING SAFE TO FISH, in degrees.
#
# THESE ARE POLICY, NOT PHYSICS, and they are a placeholder until curated. British Columbia
# issues in-season angling closures on water temperature — the Thompson, the Nicola, the
# Cowichan — and the trigger differs by river, by species and by year: a bull trout or
# steelhead river closes cooler than a general-interest one. The numbers below are a
# defensible general reading (mortality on release climbs steeply through the high teens),
# NOT a citation of any specific order, and the band must never be presented as one.
#
# When the real per-river thresholds are curated they belong beside the regulations, keyed
# by water, and this becomes their fallback.
_TEMP_BANDS = ((18.0, "cool"), (20.0, "warm"), (float("inf"), "critical"))


def temperature_band(celsius: float) -> str:
    """A class for a water temperature. See `_TEMP_BANDS` — policy, not physics."""
    for ceiling, name in _TEMP_BANDS:
        if celsius < ceiling:
            return name
    return "critical"


#: How many readings a pentad keeps. Five years of half-hourly runs would be 87,000 per
#: bucket and the file would be useless; 400 is well past what a percentile needs and keeps
#: the whole province under a megabyte.
TEMP_SAMPLES_PER_PENTAD = 400

TEMP_HISTORY = "temp_history.json"


def accumulate_temperatures(out: Path, temps: dict[str, tuple[str, float]],
                            pent: int) -> dict:
    """Fold today's readings into a growing per-station, per-pentad record. Returns it.

    THIS EXISTS BECAUSE THERE IS NO HISTORY TO READ. HYDAT carries level, flow and
    sediment; it has no water temperature at all, so unlike every other quantity here
    there is no archive to rank a reading against and no envelope to publish. The only way
    to ever have one is to start keeping it.

    It costs one small file per run and nothing else, it is useless for a season and then
    it is the only temperature climatology in the app, so it starts now rather than when it
    is wanted. Until a pentad has enough samples the app must say degrees and no ranking —
    which is the right thing to say about temperature anyway.

    ONE SAMPLE PER STATION PER RUN, capped and FIFO. A station reporting every half hour
    would otherwise fill a bucket with one warm afternoon.
    """
    path = out / TEMP_HISTORY
    try:
        hist = json.loads(path.read_text())
    except (OSError, ValueError):
        hist = {}
    if not isinstance(hist, dict):
        hist = {}
    key = str(pent)
    for station, (_at, c) in temps.items():
        buckets = hist.setdefault(station, {})
        vals = buckets.setdefault(key, [])
        vals.append(round(c, 2))
        if len(vals) > TEMP_SAMPLES_PER_PENTAD:
            del vals[:len(vals) - TEMP_SAMPLES_PER_PENTAD]
    path.write_text(json.dumps(hist, separators=(",", ":")))
    return hist


def temperature_envelope(hist: dict, min_obs: int = 10) -> dict:
    """`{station: {pentad: [p10, p25, p50, p75, p90]}}` from what has been accumulated.

    Same shape and the same 10-observation gate as the HYDAT envelopes, so a client that
    can read one can read the other without learning a second format. Empty for a long
    while, and that is not a failure state — it is the honest one.
    """
    out: dict[str, dict[str, list[float]]] = {}
    for station, buckets in hist.items():
        for pentad, vals in buckets.items():
            if len(vals) < min_obs:
                continue
            xs = sorted(vals)
            def q(p: float) -> float:
                i = min(len(xs) - 1, max(0, int(round(p * (len(xs) - 1)))))
                return xs[i]
            out.setdefault(station, {})[pentad] = [q(0.10), q(0.25), q(0.50), q(0.75), q(0.90)]
    return out


def pentad_of(when: datetime) -> int:
    """0..72. Five-day buckets, which is what the envelope is sampled at."""
    return min(72, (when.timetuple().tm_yday - 1) // 5)


def band_for(bands: dict | None, when: datetime) -> list[float | None] | None:
    """The envelope for THIS DAY, interpolated between the two nearest pentad centres.

    THE OTHER HALF OF THE BIN-EDGE FIX. Widening the pooling window (see
    `climatology.WINDOW_DAYS`) makes neighbouring pentads similar; reading one of them
    still steps, because a pentad is a step function and the reader crosses its edge
    overnight. Between the centres the answer should move a fifth of the way each day, and
    here it does.

    A pentad's centre is day 2.5 of its five. So a reading on the first day of a pentad is
    mostly about the PREVIOUS one, which is exactly the asymmetry a hard lookup ignored.
    Wraps at the year boundary, because late December and early January are one season.
    """
    if not bands:
        return None
    yday = when.timetuple().tm_yday
    pos = (yday - 1) / 5.0 - 0.5          # position in pentad units, centres at integers
    lo = math.floor(pos)
    frac = pos - lo
    a = bands.get(str(lo % 73))
    b = bands.get(str((lo + 1) % 73))
    if a is None or b is None:
        return a or b                      # one side missing: the other is the best we have
    if len(a) != len(b):
        return a
    out: list[float | None] = []
    for x, y in zip(a, b):
        out.append(None if x is None or y is None else x + (y - x) * frac)
    return out


# The 30-day archive ECCC keeps beside the 2-day hourly file. Read for any station whose
# record is missing a day the archive still holds — see `daily_gaps`.
_DAILY = f"{BASE}/daily/BC_{{station}}_daily_hydrometric.csv"

# HOW FAR BACK THE ARCHIVE REACHES, in whole local days before today. The file holds thirty
# days ending yesterday (21 Sep .. 20 Oct, fetched 21 Oct); the oldest of them rolls off
# during the day, so the window trusts one fewer. A day older than this is beyond the
# feed's reach: reported by every run, never invented. (Older days: `year_backfill`.)
ARCHIVE_DAYS = 29

# The archive is static files on ECCC's datamart, but it is still somebody else's server:
# fewer hands than the hourly pull, and only for stations that have a hole to fill.
ARCHIVE_WORKERS = 4


def _record_start(today: date) -> date:
    """The first day the daily record keeps: 1 January, or 1 December while it is January (S1)."""
    return date(today.year - 1, 12, 1) if today.month == 1 else date(today.year, 1, 1)


def daily_gaps(rows: list[list], today: date) -> tuple[list[str], list[str]]:
    """`(inside, beyond)`: the days this year's record lacks, split at the archive's edge.

    `inside` is yesterday back through ARCHIVE_DAYS — the archive can refill those. `beyond`
    is everything older, back to the start of the record (`_record_start`), which only
    `year_backfill` can reach. Today is never a gap: it is still being written. A row that
    carries neither a level nor a discharge is a gap, not a day.
    """
    have = {str(r[0]) for r in rows or []
            if len(r) >= 3 and (r[1] is not None or r[2] is not None)}
    edge = today - timedelta(days=ARCHIVE_DAYS)
    inside: list[str] = []
    beyond: list[str] = []
    d = _record_start(today)
    while d < today:
        s = d.isoformat()
        if s not in have:
            (inside if d >= edge else beyond).append(s)
        d += timedelta(days=1)
    return inside, beyond


def day_runs(days: list[str]) -> list[list[str]]:
    """Consecutive days as `[[first, last], ...]` — a gap report a person can read."""
    out: list[list[str]] = []
    for s in sorted(days):
        d = date.fromisoformat(s)
        if out and date.fromisoformat(out[-1][1]) + timedelta(days=1) == d:
            out[-1][1] = s
        else:
            out.append([s, s])
    return out


def _local_day(at: str | None, fallback: date) -> date:
    """The station's own calendar day from a reading stamped in its local standard time."""
    try:
        return date.fromisoformat(str(at)[:10])
    except ValueError:
        return fallback


def daily_means(text: str) -> dict[str, tuple[float | None, float | None]]:
    """`{YYYY-MM-DD: (level, discharge)}` — the mean of every reading on each day.

    A DAILY MEAN, not the reading that happened to be last. A river can fall 20% between
    dawn and dusk, so "the value at 23:55" and "what the river did that day" are different
    numbers, and the envelope this gets compared against is built from HYDAT daily MEANS.
    Mixing the two would put an instantaneous value on a mean's scale.
    """
    acc: dict[str, list[list[float]]] = {}
    for r in csv.DictReader(io.StringIO(text)):
        day = (r.get("Date") or "")[:10]
        if not day:
            continue
        lv, q = _num(r.get(_LEVEL)), _num(r.get(_FLOW))
        cell = acc.setdefault(day, [[], []])
        if lv is not None:
            cell[0].append(lv)
        if q is not None:
            cell[1].append(q)
    return {d: (round(sum(a) / len(a), 4) if a else None,
                round(sum(b) / len(b), 4) if b else None)
            for d, (a, b) in acc.items()}


def merge_daily(previous: list[list], fresh: dict[str, tuple[float | None, float | None]],
                year: int, month: int = 12) -> list[list]:
    """This calendar year's daily record, grown one publish at a time.

    THE FEED IS ITS OWN ARCHIVE, and it has to be: HYDAT is the only published daily record
    and it lags a year, so the seasonal chart had an envelope built from decades and nothing
    at all for the year you are standing in. ECCC's own 30-day file backfills the first run;
    every run after that contributes the days it can see, and the file keeps them.

    Bounded to the calendar year on purpose. It resets in January rather than growing without
    limit, and "where this year sits against the record" is the question the chart asks.

    EXCEPT LAST DECEMBER, WHILE IT IS JANUARY. A spot's lead-up is the week before the visit
    (S1), so a visit on 3 January needs 27-31 December, and nothing else holds those days:
    HYDAT is a year behind. They ride along through January and drop on 1 February. Readers
    place them on last year's line (`gaugeSeries`), never on this year's.
    """
    keep = (str(year),) + ((f"{year - 1}-12",) if month == 1 else ())
    have = {str(row[0]): row for row in previous or [] if str(row[0]).startswith(keep)}
    for day, (lv, q) in fresh.items():
        if not day.startswith(keep):
            continue
        # The freshest reading of a day wins: a day still in progress is re-averaged on
        # every tick until it is over, so the last write of the day is the complete one.
        have[day] = [day, lv, q]
    return [have[d] for d in sorted(have)]


def publish(out: Path, stations: list[str], clim: dict | None = None,
            forecast: dict | None = None, latest_release: str | None = None,
            now: datetime | None = None, listed: set[str] | None = None,
            drift: dict | None = None, roster: str = ROSTER_PATH) -> dict:
    """Fetch every station and write the feed. Returns a small summary for the caller.

    `now` is the clock, injectable so a test can stand on a known day; a run uses the wall.
    `listed` is ECCC's real-time list this run (`listed_stations`); published in the index
    when known, so "not a real-time gauge" and "real-time but quiet today" can be told apart.
    `drift` is `roster_drift` this run (None: no list), written as a `roster` maintenance notice naming `roster`, the
    roster file this run read (`roster_shown`).
    """
    out.mkdir(parents=True, exist_ok=True)
    # the previous index's notices, read before it is overwritten: each notice's `since` is carried from there
    try:
        before = json.loads((out / "index.json").read_text(encoding="utf-8")).get("maintenance")
    except Exception:                                           # noqa: BLE001 — no or unreadable index: a first run
        before = None
    envelope = (clim or {}).get("stations", {})
    release = (clim or {}).get("release")
    forecast = forecast or {}
    now = now or datetime.now(timezone.utc)
    pent = pentad_of(now)

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        got = [r for r in pool.map(fetch_station, sorted(stations)) if r]

    # WHAT THIS FEED ALREADY KNOWS. The per-station files are the archive of the current
    # year's daily record (see `merge_daily`), so a publish reads them before it overwrites
    # them. A missing or unreadable file is simply a station starting from nothing.
    def previous(station: str) -> dict:
        f = out / f"{station}.json"
        if not f.exists():
            return {}
        try:
            return json.loads(f.read_text(encoding="utf-8"))
        except Exception:                              # noqa: BLE001
            return {}

    prior = {r["station"]: previous(r["station"]) for r in got}

    # What the 2-hour window can see, as daily means — yesterday and today, usually.
    seen_by: dict[str, dict] = {}
    for r in got:
        seen_by[r["station"]] = daily_means("\n".join(
            [f"Date,{_LEVEL},{_FLOW}"] +
            [f"{t},{'' if lv is None else lv},{'' if q is None else q}"
             for t, lv, q in r["recent"]]))
    today_of = {r["station"]: _local_day(r["at"], now.date()) for r in got}

    # REFILL ANY HOLE THE ARCHIVE CAN STILL SEE (ticket 2026-10-21). This used to run only
    # for a station with no record at all, so a station that had ever been published never
    # refilled a night the publisher did not run — and lost that day for good once it aged
    # out of the archive. Now every run asks, but only for stations with a hole inside the
    # window: a complete record costs nothing. One request a station, ARCHIVE_WORKERS wide.
    def holes(st: str) -> list[str]:
        have = merge_daily(prior.get(st, {}).get("daily") or [], seen_by[st],
                           today_of[st].year, today_of[st].month)
        return daily_gaps(have, today_of[st])[0]

    gappy = [r["station"] for r in got if holes(r["station"])]
    backfill: dict[str, dict] = {}
    if gappy:
        def grab(st: str) -> tuple[str, dict]:
            text = _get(_DAILY.format(station=st))
            return st, (daily_means(text) if text else {})
        with ThreadPoolExecutor(max_workers=ARCHIVE_WORKERS) as pool:
            backfill = dict(pool.map(grab, sorted(gappy)))
        print(f"  daily    refilled {sum(1 for v in backfill.values() if v)} "
              f"of {len(gappy)} station records with a gap from ECCC's 30-day archive")
    beyond_window: dict[str, list[list[str]]] = {}

    # ONE EXTRA REQUEST FOR THE WHOLE PROVINCE, before the per-station loop. See
    # `fetch_temperatures`: it is a different service from the datamart CSV, because the
    # datamart does not carry a temperature column at all.
    temps = fetch_temperatures([r["station"] for r in got])
    # And keep them. There is no temperature history to read anywhere, so the only way to
    # have an envelope one day is to begin one — see `accumulate_temperatures`.
    t_hist = accumulate_temperatures(out, temps, pent)
    t_env = temperature_envelope(t_hist)
    print(f"  water temperature: {len(temps)} of {len(got)} stations"
          f"  ({len(t_env)} now have enough history for an envelope)")

    index: dict[str, dict] = {}
    for r in got:
        # AGAINST THE SAME QUANTITY THE ENVELOPE IS MADE OF. 237 BC stations measure stage
        # and never discharge — the Skagit at the International Boundary among them — so
        # their envelope is in metres. Comparing a discharge to it, or a level to a
        # discharge envelope, is a percentile computed across two different units: a number
        # that looks perfectly reasonable and means nothing.
        # DISCHARGE FIRST WHERE THERE IS ONE, and this is a judgement rather than a
        # default: "12 m3/s, 4th percentile" is a statement about the whole river, while a
        # stage percentile is about one cross-section and moves when the channel does. Level
        # is used where discharge cannot answer — 237 BC stations never measure it — and the
        # parameter travels with the number so nothing presents them as the same claim.
        have = envelope.get(r["station"]) or {}
        param = ("discharge" if "discharge" in have and r["discharge"] is not None
                 else "level" if "level" in have and r["level"] is not None
                 else "discharge")
        band = band_for(have.get(param), now)
        observed = r["level"] if param == "level" else r["discharge"]
        # BOTH PERCENTILES WHERE THERE ARE BOTH, each against its own envelope.
        #
        # The map can be coloured by flow or by level and they are different questions: a
        # regulated river can sit at its normal STAGE while its discharge is in the bottom
        # tenth, because the dam is holding the pond and letting nothing through. One
        # number per station made that switch impossible to offer honestly — the client
        # would have been recolouring the same value under two labels.
        t_at, t_c = temps.get(r["station"], (None, None))
        per = {}
        for q in ("discharge", "level"):
            v = r["level"] if q == "level" else r["discharge"]
            b = band_for(have.get(q), now)
            p_ = percentile_of(v, b)
            if p_ is not None:
                per[q] = p_
        # TEMPERATURE IS NOT A PERCENTILE, and cannot be one today.
        #
        # There is no historical water-temperature record to rank against: HYDAT carries
        # level, flow and sediment and nothing else. So there is no envelope, and a
        # percentile computed against a missing envelope would silently be nothing at all.
        #
        # It is also the wrong shape for the question. "Unusually warm for early September"
        # is a fact about the weather; "19 degrees" is a fact about whether a released fish
        # survives, and the second is what an angler acts on. The threshold is absolute and
        # biological, not relative and seasonal. So temperature ships as DEGREES, with a
        # class attached, and no ranking is implied.
        if t_c is not None:
            per["temperatureBand"] = temperature_band(t_c)
            # A ranking ONLY once this station has enough of its own accumulated history
            # for the day of year. Absent for a long while, and absent is correct.
            p_t = percentile_of(t_c, (t_env.get(r["station"]) or {}).get(str(pent)))
            if p_t is not None:
                per["temperature"] = p_t
        index[r["station"]] = {
            "percentile": percentile_of(observed, band),
            # Which quantity `percentile` above is about — the station's own default.
            "parameter": param,
            **per,
            "observedAt": r["at"],
            # THE DEGREES THEMSELVES, not only their percentile — and this is the one
            # quantity where that is true. A flow of 12 m3/s means nothing without its
            # record, but 19 C is a number an angler acts on directly: it is the threshold
            # the province closes rivers at, and a fish released into it often dies. So the
            # reading is published beside its ranking rather than behind it.
            **({"temperatureC": t_c, "temperatureAt": t_at} if t_c is not None else {}),
            # WHETHER there is a forecast, not the forecast itself. This file is fetched to
            # paint the map before anything is tapped, and the full model series is ~15 KB a
            # station — 6 MB to answer a question the map does not ask. The series lives in
            # the per-station file, which is opened on a tap.
            "forecast": any(run.get("series")
                            for run in (forecast.get(r["station"]) or {}).values()),
            # WHERE IT IS HEADING, at +1, +3, +5 and +10 days — one percentile per horizon per
            # quantity, and nothing else from the run. This is the same trade as `forecast`
            # above: the map wants to be paintable for a future day from the one file it
            # already fetches, and three numbers a station is affordable where three series
            # are not. Absent when no model reaches that far, which the map draws as
            # unmeasured rather than as normal.
            **({"ahead": ahead} if (ahead := forecast_percentiles(
                forecast.get(r["station"]), have, now)) else {}),
        }
        # This year's daily record: whatever the file already held, plus every day the
        # 2-hour window can see, plus the archive where the record had a hole. The archive
        # wins inside its window: its day is the mean of every reading, while a day cut off
        # by a run that stopped is the mean of a morning.
        st = r["station"]
        seen = dict(seen_by[st])
        seen.update(backfill.get(st) or {})
        day = today_of[st]
        daily = merge_daily(prior.get(st, {}).get("daily") or [], seen, day.year, day.month)
        # WHAT NOTHING HERE CAN REACH is said, every run, and left empty: older than the
        # archive, so a line across it would be a line nobody measured.
        if (old := daily_gaps(daily, day)[1]):
            beyond_window[st] = day_runs(old)

        (out / f"{r['station']}.json").write_text(json.dumps({
            "fetchedAt": now.isoformat(timespec="seconds").replace("+00:00", "Z"),
            "station": r["station"],
            "now": {"discharge": r["discharge"], "level": r["level"], "at": r["at"],
                    "percentile": index[r["station"]]["percentile"],
                    # Which quantity the percentile is ABOUT. A client showing "p4th" beside
                    # a discharge when the percentile came from stage would be inventing a
                    # claim the data does not make.
                    "parameter": param},
            "recent": r["recent"],
            # `[day, level, discharge]` for THIS CALENDAR YEAR, as daily means — the line the
            # seasonal chart draws across its envelope. Grown by this feed rather than read
            # from anywhere: HYDAT is the only published daily record and it lags a year.
            "daily": daily,
            # THE LAST COMPLETE YEARS, day by day, out of the same HYDAT release the
            # envelope came from: `{parameter: {year: [366 values]}}`. A band shows what is
            # normal and has no shape in time — it cannot show that last summer was dry too,
            # which is the question a person actually asks standing on a low river.
            "priorYears": (clim or {}).get("recentYears", {}).get(r["station"]),
            # EVERY MODEL RUNNING for this station, keyed by name. Keeping only the freshest
            # meant only CLEVER ever appeared: the freshet model publishes later in the
            # morning than the low-flow model, so it won the tie in September, when freshet
            # is months over. They answer different questions; the reader picks.
            "forecasts": forecast.get(r["station"]) or {},
        }, separators=(",", ":")), encoding="utf-8")

    # PROVENANCE TRAVELS WITH THE NUMBERS. Every percentile in this file was computed
    # against one HYDAT release; saying which lets a reader — and a later bug hunt — know
    # what the number was measured against, and lets the app show the record length behind
    # "below normal for the date" instead of asserting it bare.
    stale = bool(latest_release and release and latest_release != release)
    fetched_at = now.isoformat(timespec="seconds").replace("+00:00", "Z")
    maintenance = maintenance_notices(release=release, latest=latest_release, drift=drift,
                                      previous=before, at=fetched_at, roster=roster)
    (out / "index.json").write_text(json.dumps({
        "fetchedAt": fetched_at,
        # (T68) the newest forecast run's issue time, feed-wide, for the More tab's Forecasts line; null: no run held
        "forecastIssuedAt": newest_forecast_issue(forecast),
        # `hydat` is this run's own check (kept for any reader of it); `maintenance` is what the owner must do — the
        # app's HYDAT line reads the notice, which survives a run whose listing check failed.
        "hydat": {"release": release, "latest": latest_release, "stale": stale},
        "maintenance": maintenance,
        "stations": index,
        # WHICH STATIONS ECCC CALLS REAL-TIME this run. Absent from `stations` still means
        # nothing to show; this says why. Listed and absent: a live gauge silent this run.
        # Not listed: ECCC publishes no real-time data for it at all — a HYDAT-only or
        # seasonal station, or one retired since the bundle's roster. The key is left out
        # when the list could not be fetched: unknown, never "nobody".
        **({"listed": sorted(listed)} if listed is not None else {}),
    }, separators=(",", ":")), encoding="utf-8")

    # NO stats.json (P20, owner Q16): nothing read it. A record length shown beside a percentile comes from the
    # bundle's climatology, never from a second file that can fall out of step with it.

    return {"asked": len(stations), "answered": len(got), "release": release,
            "stale": stale, "latest": latest_release, "maintenance": maintenance,
            "withPercentile": sum(1 for v in index.values() if v["percentile"] is not None),
            "archive": {"asked": len(gappy), "filled": sum(1 for v in backfill.values() if v)},
            "gapsBeyondWindow": beyond_window}


def main() -> None:
    ap = argparse.ArgumentParser(description="Publish the gauge feed")
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--stations", type=Path,
                    default=SOURCE / "bc_hydrometric_stations.json")
    ap.add_argument("--clim", type=Path,
                    help="envelope from the HYDAT tier; percentiles are null without it")
    ap.add_argument("--limit", type=int, help="fetch only the first N — for a dev run")
    ap.add_argument("--no-release-check", action="store_true",
                    help="skip the HYDAT listing check (one HTTP GET)")
    ap.add_argument("--no-forecast", action="store_true",
                    help="skip the BC River Forecast Centre pull (three HTTP GETs)")
    args = ap.parse_args()

    roster = json.loads(args.stations.read_text(encoding="utf-8"))
    # Only stations ECCC lists as transmitting — TODAY, from ECCC's own list (see
    # STATION_LIST); the roster's snapshot flag only when that list cannot be had. Asking
    # the other ~1,885 is 1,885 requests for 1,885 404s.
    listed = listed_stations(_get(STATION_LIST))
    ids, source = stations_to_ask(roster, listed)
    drift = roster_drift(roster, listed)
    shown = roster_shown(args.stations)
    if source == "roster":
        print("  ⚠️  ECCC real-time station list unavailable; asking the roster's "
              f"{len(ids)} realtime stations", file=sys.stderr)
    if drift and (drift["joined"] or drift["left"]):
        print(f"  ⚠️  ROSTER DRIFT: ECCC now lists {len(drift['joined'])} station(s) the roster "
              f"calls silent ({', '.join(drift['joined'])}) and no longer lists "
              f"{len(drift['left'])} it calls live ({', '.join(drift['left'])}).\n"
              f"     The feed follows ECCC; the bundle follows the roster. Refresh with:\n"
              f"       {roster_action(shown)}")
    if args.limit:
        ids = ids[: args.limit]

    clim = json.loads(args.clim.read_text(encoding="utf-8")) if args.clim else None

    # THE CHEAP HALF OF THE YEARLY JOB. One HTML GET, no download. If HYDAT has moved on
    # since the envelope was built, every run says so — which is how a yearly task gets
    # noticed without anybody remembering to look for it.
    latest = None
    if not args.no_release_check:
        try:
            sys.path.insert(0, str(Path("data").resolve()))
            from fetch_data import hydat_latest_release       # noqa: PLC0415
            latest = hydat_latest_release()
        except Exception:
            latest = None

    # THE FORECAST IS A DIFFERENT AGENCY AND A DIFFERENT KIND OF CLAIM — see
    # `pipeline.gauges.feed.forecast`. Three requests, seasonal, and frequently empty: outside
    # freshet, fall floods and low-flow season no model is running, which is normal and not
    # a failure. A forecast pull that fails costs the map nothing.
    fcast: dict = {}
    if not args.no_forecast:
        try:
            from pipeline.gauges.feed import forecast as _forecast   # noqa: PLC0415
            fcast = _forecast.fetch()
            # THE SERIES, not just the summary. One number per station drawn on a chart is a
            # single point, and a single point joined to today's reading is a triangle —
            # which is exactly what it looked like. Fetched only where the issue time has
            # moved since the last publish, so the steady state costs nothing.
            have: dict[str, dict[str, dict]] = {}
            for st in fcast:
                f = args.out / f"{st}.json"
                if not f.exists():
                    continue
                try:
                    prev = json.loads(f.read_text(encoding="utf-8")).get("forecasts") or {}
                except Exception:                               # noqa: BLE001
                    continue
                # THE WHOLE RUN, not just its issue time. `with_series` carries a
                # still-current run across into the new summary; given only a timestamp it
                # could decline to re-fetch but had nothing to carry, so the station file
                # was rewritten without the series it already had.
                have[st] = {m: run for m, run in prev.items() if run.get("series")}
            fcast = _forecast.with_series(fcast, stations=set(ids), keep=have)
        except Exception as exc:                                # noqa: BLE001
            print(f"  forecast unavailable: {exc}", file=sys.stderr)

    summary = publish(args.out, ids, clim=clim, forecast=fcast, latest_release=latest,
                      listed=listed, drift=drift, roster=shown)
    print(f"  ✅ {args.out}  {summary['answered']}/{summary['asked']} answered, "
          f"{summary['withPercentile']} with a percentile")
    if fcast:
        runs = sum(len(v) for v in fcast.values())
        print(f"     {runs} model runs across {len(fcast)} stations "
              f"from the BC River Forecast Centre")
    if summary["gapsBeyondWindow"]:
        days = sum(date.fromisoformat(b).toordinal() - date.fromisoformat(a).toordinal() + 1
                   for runs in summary["gapsBeyondWindow"].values() for a, b in runs)
        print(f"     {len(summary['gapsBeyondWindow'])} station records lack {days} days older "
              f"than the {ARCHIVE_DAYS}-day archive — left empty; "
              f"`python -m pipeline.gauges.feed.year_backfill --out {args.out}` reaches them")
    if summary["release"]:
        print(f"     envelope from HYDAT {summary['release']}")
    for n in summary["maintenance"]:
        if n["kind"] == "hydat":
            print(f"  ⚠️  A NEWER HYDAT IS OUT: {n['latest']} (envelope is on {n['current']}, "
                  f"noticed {n['since']}). Refresh with:\n       {n['action']}")
    # TELL THE OWNER ONCE (FEED_NOTIFY_URL; off when unset). Never fails the run.
    from pipeline.gauges.feed import notify as _notify        # noqa: PLC0415
    _notify.send_due(summary["maintenance"], args.out)


if __name__ == "__main__":
    main()
