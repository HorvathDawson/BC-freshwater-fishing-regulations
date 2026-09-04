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
import sys
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
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
    if not text:
        return None
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
        text = _get(f"{_WATEROFFICE}?{q}")
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


# The 30-day archive ECCC keeps beside the 2-day hourly file. Read ONCE per station, to
# start that station's daily record off with a month rather than with today.
_DAILY = f"{BASE}/daily/BC_{{station}}_daily_hydrometric.csv"


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
                year: int) -> list[list]:
    """This calendar year's daily record, grown one publish at a time.

    THE FEED IS ITS OWN ARCHIVE, and it has to be: HYDAT is the only published daily record
    and it lags a year, so the seasonal chart had an envelope built from decades and nothing
    at all for the year you are standing in. ECCC's own 30-day file backfills the first run;
    every run after that contributes the days it can see, and the file keeps them.

    Bounded to the calendar year on purpose. It resets in January rather than growing without
    limit, and "where this year sits against the record" is the question the chart asks.
    """
    have = {str(row[0]): row for row in previous or [] if str(row[0]).startswith(str(year))}
    for day, (lv, q) in fresh.items():
        if not day.startswith(str(year)):
            continue
        # The freshest reading of a day wins: a day still in progress is re-averaged on
        # every tick until it is over, so the last write of the day is the complete one.
        have[day] = [day, lv, q]
    return [have[d] for d in sorted(have)]


def publish(out: Path, stations: list[str], clim: dict | None = None,
            forecast: dict | None = None, latest_release: str | None = None) -> dict:
    """Fetch every station and write the feed. Returns a small summary for the caller."""
    out.mkdir(parents=True, exist_ok=True)
    envelope = (clim or {}).get("stations", {})
    stats = (clim or {}).get("stats", {})
    release = (clim or {}).get("release")
    forecast = forecast or {}
    now = datetime.now(timezone.utc)
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

    # Backfill a month for any station whose record is empty — first run, or a new station.
    # One request each, and only ever once: after this its own `daily` is non-empty.
    cold = [r["station"] for r in got if not (prior.get(r["station"], {}).get("daily"))]
    backfill: dict[str, dict] = {}
    if cold:
        def grab(st: str) -> tuple[str, dict]:
            text = _get(_DAILY.format(station=st))
            return st, (daily_means(text) if text else {})
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            backfill = dict(pool.map(grab, sorted(cold)))
        print(f"  daily    backfilled {sum(1 for v in backfill.values() if v)} "
              f"of {len(cold)} station records from ECCC's 30-day archive")

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
        band = (have.get(param) or {}).get(str(pent))
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
            b = (have.get(q) or {}).get(str(pent))
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
        }
        # This year's daily record: whatever the file already held, plus every day the
        # 2-hour window can see, plus the one-time 30-day backfill.
        seen = daily_means("\n".join(
            [f"Date,{_LEVEL},{_FLOW}"] +
            [f"{t},{'' if lv is None else lv},{'' if q is None else q}"
             for t, lv, q in r["recent"]]))
        seen.update(backfill.get(r["station"]) or {})
        daily = merge_daily(prior.get(r["station"], {}).get("daily") or [], seen, now.year)

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
    (out / "index.json").write_text(json.dumps({
        "fetchedAt": now.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "hydat": {"release": release, "latest": latest_release, "stale": stale},
        "stations": index,
    }, separators=(",", ":")), encoding="utf-8")

    # The envelope's own statistics ride along so a client can say "97 years of record"
    # without opening the bundle. Small: a few numbers per station.
    if stats:
        (out / "stats.json").write_text(json.dumps(
            {"release": release, "stations": stats}, separators=(",", ":")),
            encoding="utf-8")

    return {"asked": len(stations), "answered": len(got), "release": release,
            "stale": stale, "latest": latest_release,
            "withPercentile": sum(1 for v in index.values() if v["percentile"] is not None)}


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
    # Only stations ECCC lists as transmitting. Asking the other 1,885 is 1,885 requests
    # for 1,885 404s.
    ids = [s["station"] for s in roster if s.get("realtime")]
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
            have: dict[str, dict[str, str]] = {}
            for st in fcast:
                f = args.out / f"{st}.json"
                if not f.exists():
                    continue
                try:
                    prev = json.loads(f.read_text(encoding="utf-8")).get("forecasts") or {}
                except Exception:                               # noqa: BLE001
                    continue
                have[st] = {m: (run.get("issuedAt") or "")
                            for m, run in prev.items() if run.get("series")}
            fcast = _forecast.with_series(fcast, stations=set(ids), keep=have)
        except Exception as exc:                                # noqa: BLE001
            print(f"  forecast unavailable: {exc}", file=sys.stderr)

    summary = publish(args.out, ids, clim=clim, forecast=fcast, latest_release=latest)
    print(f"  ✅ {args.out}  {summary['answered']}/{summary['asked']} answered, "
          f"{summary['withPercentile']} with a percentile")
    if fcast:
        runs = sum(len(v) for v in fcast.values())
        print(f"     {runs} model runs across {len(fcast)} stations "
              f"from the BC River Forecast Centre")
    if summary["release"]:
        print(f"     envelope from HYDAT {summary['release']}")
    if summary["stale"]:
        print(f"  ⚠️  A NEWER HYDAT IS OUT: {summary['latest']} (envelope is on "
              f"{summary['release']}). Refresh with:\n"
              f"       python data/fetch_data.py --layers hydat\n"
              f"       python -m pipeline.gauges.feed.climatology --out <clim.json>")


if __name__ == "__main__":
    main()
