"""BC River Forecast Centre model forecasts — what the river is expected to do next.

WHY THIS IS A SEPARATE SOURCE FROM EVERYTHING ELSE IN `pipeline/hydro`. The rest of this
package reads Environment and Climate Change Canada: an observation is a measurement of
something that happened. A forecast is a MODEL OUTPUT from a different agency (the Province
of British Columbia), it carries its own attribution and disclaimer, and it can be wrong in
ways an observation cannot. Mixing the two into one number would put a prediction and a
reading behind the same word.

THREE MODELS, EACH SEASONAL, AND NONE OF THEM ALWAYS RUNNING:

    CLEVER   freshet, hourly, 10 days   — spring melt, the high-flow question
    COFFEE   fall floods, daily, 5 days — autumn rain
    ELF      low flow, daily, 30 days   — summer drought, the one anglers care about

Outside its season a model publishes nothing, so a station with no forecast today is the
NORMAL case and never an error. Where two models both speak for a station, the one with the
more recent issue time wins — they disagree at the shoulders of their seasons, and the
fresher run is the one the Centre stands behind.

ONE REQUEST PER MODEL, NOT ONE PER STATION. The Centre publishes a per-station CSV as well,
and v1 fetched all of them; that is ~450 requests every half hour against a provincial
endpoint for data that the summary layer already carries. The summary gives the forecast
minimum, average and maximum over the model's horizon, which is what a person actually reads
("it will fall to about 8 m3/s over the next month"). Three requests is polite; four hundred
and fifty is a habit that gets an open dataset closed.

ATTRIBUTION IS NOT OPTIONAL. `ATTRIBUTION` below is the exact string the Province requires,
confirmed with the River Forecast Centre, and it must be displayed verbatim wherever a
forecast appears.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

# THE CENTRE'S CLOCK (T126). Its CSV stamps carry no zone, and they are PACIFIC STANDARD TIME all year — not UTC, and not
# the PDT the province keeps in summer. Measured 2026-10-22 against the observed hourly readings of every CLEVER station
# in the feed (`tests/fixtures/gauge_feed/clever_clock/`, `test_forecast_clock.py`): read as UTC-8 the forecast's own
# first hours sit a median 0.88% from the gauge, against 0.99% at UTC-7 and 1.78% read as UTC. Read as UTC, every
# +1/+3/+5/+10 sampled the step eight hours off.
#
# ONE READING OF THE CLOCK: `instant` below. The publisher's horizons (`publish.forecast_at`), its staleness and the
# expected change (`change.py`) all ask it; none keeps its own zone. And the stamps are WRITTEN zoned at the source
# (`series`), so the app's `Date.parse` reads the same instant instead of the device's own clock.
CENTRE_UTC_OFFSET_H = -8
CENTRE_ZONE = timezone(timedelta(hours=CENTRE_UTC_OFFSET_H))

_BARE_DAY = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def is_day(at: object) -> bool:
    """A stamp that names a calendar DAY (ELF's "2026-10-22"), not an instant: its value is the whole Centre day."""
    return isinstance(at, str) and bool(_BARE_DAY.match(at.strip()))


def instant(at: object) -> datetime | None:
    """A forecast stamp as an aware instant — THE ONE READING OF THE CENTRE'S CLOCK (see CENTRE_ZONE).

    A zoned stamp is the instant it states. A naive one — every stamp before T126, and `issuedAt` from the summary
    layer's "Updated at: 10:35 AM" wording — is a Centre wall clock. A bare day is that Centre day's midnight (ask
    `is_day` when the whole day is meant). Unreadable is None, never a guess.
    """
    if at is None:
        return None
    try:
        t = datetime.fromisoformat(str(at).strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return t if t.tzinfo else t.replace(tzinfo=CENTRE_ZONE)


def zoned(at: str) -> str:
    """A stamp as the feed writes it: an instant with its offset ("2026-10-22T05:00:00-08:00"); a bare day stays a day.

    Idempotent, so a run carried from an older station file (naive stamps) is rewritten the same way as a fresh one.
    Unreadable is kept as it stands — the Centre's own words beat nothing.
    """
    if is_day(at):
        return at
    t = instant(at)
    return t.isoformat(timespec="seconds") if t is not None else at

# The Centre's ArcGIS org. Each map hub is one FeatureServer layer of per-station rows.
_ARCGIS = "https://services6.arcgis.com/ubm4tcTYICKBpist/arcgis/rest/services"

_UA = "BC-freshwater-fishing-regulations/1.0 (+https://canifishthis.ca)"

ATTRIBUTION = (
    "Forecast data provided by the BC River Forecast Centre, "
    "Province of British Columbia, and used under the Province's "
    "copyright terms (https://www2.gov.bc.ca/gov/content/home/copyright). "
    "Users should use the information on this website with caution and "
    "at their own risk."
)

# model -> where its numbers live in the service's own column names.
#
# `rep` is which of min/ave/max is THE number for that model, and it is not a formatting
# choice: a flood model is asked "how high", a low-flow model "how low". Showing the average
# of an ELF run would smooth away the entire question it exists to answer.
MODELS: dict[str, dict] = {
    # CLEVER'S SUMMARY IS TAKEN FROM ITS SERIES (T126, `summary_from_series`). Its layer's only forecast column is the
    # maximum in FIVE days, on a ten-day run: 08MH090 on 2026-10-22 read 0.263 m3/s while the run peaks at 5.431 on day
    # six. The column still says the run is in season (no series is fetched for a run not in the summary); once the
    # series is attached, min/ave/max/value come from it.
    "CLEVER": {
        "service": "CLM_MapHub_forecast", "horizon_days": 10, "rep": "max",
        "obs": "Latest_Reading", "issued": "Issued_at",
        "fmax": "Forecast_maximum_in_5_days",
        "summary_from_series": True,
        "csv": "https://bcrfc.env.gov.bc.ca/freshet/clever/{s}.CSV",
        "shape": "clever", "step": "1h",
    },
    "COFFEE": {
        "service": "coffee_MapHub_forecast", "horizon_days": 5, "rep": "max",
        "obs": "Latest_Reading", "issued": "Issued_at",
        "fmin": "Forecast_minimum_in_5_days",
        "fave": "Forecast_average_in_5_days",
        "fmax": "Forecast_maximum_in_5_days",
        "csv": "https://bcrfc.env.gov.bc.ca/fallfloods/coffee/COFFEE_{s}.CSV",
        "shape": "clever", "step": "1d",
    },
    "ELF": {
        "service": "MapHub_ELF_Forecast", "horizon_days": 30, "rep": "min",
        "obs": "Qobs_m3_s_", "issued": "Issued_at",
        "fmin": "Qfor_MIN_30_Days_m3_s_",
        "fave": "Qfor_AVE_30_Days_m3_s_",
        "fmax": "Qfor_MAX_30_Days_m3_s_",
        "csv": "https://bcrfc.env.gov.bc.ca/lowflow/elf/csv/{s}_elf_forecast.csv",
        "shape": "elf", "step": "1d",
    },
}

# The Centre's own words, carried out of the CSV headers verbatim. Required wherever a
# forecast appears — this is the sentence, not a paraphrase of it.
CSV_DISCLAIMER = ("USERS OF THIS DATA MUST ACCEPT ALL RESPONSIBILITY FOR THE USE AND "
                  "INTERPRETATION.")


def _num(v) -> float | None:
    """The first number in whatever the service put in the cell.

    These columns are typed as text and arrive as "12.4", "12.4 m3/s", "" and null in the
    same layer. A parse failure is None — never 0.0, which would read as a river that has
    stopped.
    """
    if v is None:
        return None
    m = re.search(r"-?\d+(?:\.\d+)?", str(v))
    return float(m.group()) if m else None


def _issued(v) -> str | None:
    """When this run was made, as ISO-8601 — from whichever of three forms the layer used.

    NOT COSMETIC. Two models overlap at the shoulders of their seasons and the fresher run
    wins, which is a comparison — and comparing "Updated at: 09:38 AM Tue 2026-09-01"
    against "2026-09-02T00:00:00" as strings sorts on the letter U and picks the older run
    every time. Normalising here is what makes that comparison mean what it says.

    An unrecognised form is returned as it stands rather than dropped: a reader is still
    better off seeing the Centre's own words than nothing, and the tie-break falls back to
    model order, which is stated.
    """
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(v / 1000, timezone.utc).isoformat(timespec="seconds")
    text = str(v).strip()
    if not text:
        return None
    m = re.search(r"(\d{1,2}):(\d{2})\s*([AP]M).*?(\d{4}-\d{2}-\d{2})", text, re.I)
    if m:
        hour = int(m.group(1)) % 12 + (12 if m.group(3).upper() == "PM" else 0)
        return f"{m.group(4)}T{hour:02d}:{m.group(2)}:00"
    m = re.search(r"\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2})?)?", text)
    return m.group(0).replace(" ", "T") if m else text


def _query(service: str, timeout: float = 30.0) -> list[dict]:
    """Every row of one FeatureServer layer, paged."""
    url = f"{_ARCGIS}/{service}/FeatureServer/0/query"
    out: list[dict] = []
    offset, page = 0, 1000
    while True:
        q = urllib.parse.urlencode({
            "where": "1=1", "outFields": "*", "returnGeometry": "false",
            "f": "json", "resultOffset": offset, "resultRecordCount": page,
        })
        req = urllib.request.Request(f"{url}?{q}", headers={"User-Agent": _UA})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            data = json.loads(r.read().decode("utf-8", "replace"))
        feats = data.get("features", [])
        out.extend(f.get("attributes", {}) for f in feats)
        if len(feats) < page or not data.get("exceededTransferLimit"):
            return out
        offset += page


def fetch(models: list[str] | None = None) -> dict[str, dict[str, dict]]:
    """`{station: {model: forecast}}` — EVERY model running for that station.

    ALL OF THEM, not the freshest. The first version kept one run per station, broken by
    issue time, and the effect was that only CLEVER ever appeared: the freshet model
    publishes later in the morning than ELF, so it won the tie in September — when freshet
    is months over and the low-flow model is the one anybody wants. There is no rule that
    picks correctly for every station on every day, because the models answer different
    questions: CLEVER asks how HIGH over ten days, ELF asks how LOW over thirty. Publishing
    both and letting the reader choose is the only version that is not sometimes wrong.

    A model that fails is SKIPPED with a note, not raised. This runs inside a 30-minute cron
    beside the observations; losing a seasonal forecast must not cost the map its readings.
    """
    out: dict[str, dict[str, dict]] = {}
    for name in (models or list(MODELS)):
        cfg = MODELS[name]
        try:
            rows = _query(cfg["service"])
        except Exception as exc:                      # noqa: BLE001 — see docstring
            print(f"  {name}: unavailable ({exc})", file=sys.stderr)
            continue
        kept = 0
        for a in rows:
            station = str(a.get("Station_ID") or "").strip()
            if not station:
                continue
            lo = _num(a.get(cfg.get("fmin"))) if cfg.get("fmin") else None
            av = _num(a.get(cfg.get("fave"))) if cfg.get("fave") else None
            hi = _num(a.get(cfg.get("fmax"))) if cfg.get("fmax") else None
            rep = {"min": lo, "ave": av, "max": hi}[cfg["rep"]]
            rep = rep if rep is not None else (hi if hi is not None else av if av is not None else lo)
            if rep is None:
                continue                # a row with no forecast in it is not a forecast
            row = {
                "model": name,
                "issuedAt": _issued(a.get(cfg.get("issued"))),
                "horizonDays": cfg["horizon_days"],
                # The number to show, and which end of the range it is. A client that only
                # printed `value` would say "8 m3/s" for a flood warning and a drought alike.
                "value": rep, "extreme": cfg["rep"],
                "min": lo, "ave": av, "max": hi,
                "observed": _num(a.get(cfg.get("obs"))),
                "unit": "m3/s",
            }
            out.setdefault(station, {})[name] = row
            kept += 1
        print(f"  {name:7} {kept:>4} station forecasts")
    return out


def _csv_rows(text: str) -> tuple[list[str], list[list[str]]]:
    """The header row and the data rows of a BCRFC CSV.

    Their files open with a disclaimer, a link, a title, the station, the units and an issue
    time before the actual header — a plain `csv.reader` reads those as data. The header is
    the first line that starts with DATE.
    """
    lines = [ln for ln in text.splitlines() if ln.strip()]
    for i, ln in enumerate(lines):
        if ln.upper().startswith("DATE"):
            head = [c.strip().upper() for c in ln.split(",")]
            return head, [r.split(",") for r in lines[i + 1:]]
    return [], []


def _f(v: str | None) -> float | None:
    try:
        return float(v) if v not in (None, "") else None
    except ValueError:
        return None


def series(station: str, model: str, timeout: float = 25.0) -> dict | None:
    """The forecast as a SERIES — every step the model published, with its bounds.

    WHY THIS EXISTS AND THE SUMMARY LAYER DOES NOT REPLACE IT. The ArcGIS summary carries
    one number per station: the forecast minimum, average and maximum over the whole
    horizon. Drawn on a chart that is a single point, and a single point joined to today's
    reading is a triangle — which is what it looked like, because that is all it was. The
    per-station CSV is the actual model output: hourly for ten days (CLEVER) or daily for
    thirty (ELF), each step with a lower and upper bound.

    THE COST IS WHY THIS IS NOT FETCHED EVERY TICK. One request per station, and the app
    carries ~440 of them, against a provincial endpoint the observations do not even come
    from. But a forecast is ISSUED once or twice a day, not every thirty minutes — so the
    publisher re-fetches a station's series only when the issue time on the summary has
    moved. In the steady state that is zero requests.

    ELF publishes water level beside discharge (HOBS/HFOR), so a level chart can carry a
    forecast too. CLEVER publishes discharge only.
    """
    cfg = MODELS.get(model)
    if not cfg or not cfg.get("csv"):
        return None
    url = str(cfg["csv"]).format(s=station)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": _UA})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            text = r.read().decode("utf-8", "replace")
    except Exception:                                  # noqa: BLE001 — 404 = not in season
        return None
    return parse_series(text, model)


def parse_series(text: str, model: str) -> dict | None:
    """One model's per-station CSV as the feed's series (`series` without the request), stamps ZONED (`zoned`)."""
    cfg = MODELS.get(model)
    if not cfg:
        return None
    head, rows = _csv_rows(text)
    if not head or not rows:
        return None
    ix = {name: i for i, name in enumerate(head)}

    def col(row: list[str], name: str) -> float | None:
        i = ix.get(name)
        return _f(row[i]) if i is not None and i < len(row) else None

    out: dict[str, list] = {"at": [], "mid": [], "lo": [], "hi": [],
                            "level_mid": [], "level_lo": [], "level_hi": [],
                            "observed": [], "level_observed": []}
    for row in rows:
        if not row or not row[0].strip():
            continue
        when = row[0].strip()
        if cfg["shape"] == "clever":
            hour = row[ix["HOUR"]].strip() if "HOUR" in ix and len(row) > ix["HOUR"] else "00"
            out["at"].append(zoned(f"{when}T{hour.zfill(2)}:00:00"))     # a Centre wall clock (CENTRE_ZONE)
            out["mid"].append(col(row, "FORECAST_DISCHARGE"))
            out["lo"].append(col(row, "LOWER_BOUND"))
            out["hi"].append(col(row, "UPPER_BOUND"))
            out["observed"].append(None)
            out["level_mid"].append(None)
            out["level_lo"].append(None)
            out["level_hi"].append(None)
            out["level_observed"].append(None)
        else:                                           # ELF
            out["at"].append(when)
            out["mid"].append(col(row, "QFOR_AVE"))
            out["lo"].append(col(row, "QFOR_MIN"))
            out["hi"].append(col(row, "QFOR_MAX"))
            out["observed"].append(col(row, "QOBS"))
            out["level_mid"].append(col(row, "HFOR_AVE"))
            out["level_lo"].append(col(row, "HFOR_MIN"))
            out["level_hi"].append(col(row, "HFOR_MAX"))
            out["level_observed"].append(col(row, "HOBS"))
    if not out["at"]:
        return None
    # A CSV whose forecast columns are entirely empty is a station the model did not run
    # for, served as a file rather than as a 404. Drawing it would be an empty ribbon.
    if not any(v is not None for v in out["mid"] + out["level_mid"]):
        return None
    out["step"] = cfg["step"]
    out["disclaimer"] = CSV_DISCLAIMER
    return out


def ahead_of_issue(run: dict) -> list[float]:
    """The run's forecast trace (`mid`) from its issue instant to its end, in step order.

    The CSV opens at midnight of the issue day, so its first hours are before the run was made; they are not ahead of
    anyone. A run whose issue time does not read keeps every step (the Centre's own file, rather than nothing).
    """
    series = run.get("series") or {}
    issued = instant(run.get("issuedAt"))
    out: list[float] = []
    for at, v in zip(series.get("at") or [], series.get("mid") or []):
        if v is None:
            continue
        t = instant(at)
        if issued is not None and (t is None or t < issued):
            continue
        out.append(float(v))
    return out


def summary_from_series(run: dict) -> dict:
    """A run whose summary layer cannot be trusted for its horizon (`summary_from_series`: CLEVER) with min/ave/max and
    `value` taken from its own series, ahead of the issue time. Any other run, or one with no series, as it stands.

    ONE SOURCE (T126): T52's expected change already reads every day of the series; the headline now reads the same
    numbers, so a peak on day six cannot be in the change and missing from the summary.
    """
    cfg = MODELS.get(str(run.get("model") or ""), {})
    vals = ahead_of_issue(run) if cfg.get("summary_from_series") else []
    if not vals:
        return run
    lo, hi, av = min(vals), max(vals), round(sum(vals) / len(vals), 4)
    return {**run, "min": lo, "ave": av, "max": hi, "value": {"min": lo, "ave": av, "max": hi}[cfg["rep"]]}


def settle(run: dict) -> dict:
    """A run as the feed writes it once its series is attached: stamps zoned (a carried run's too) and, where the model
    says so, the summary from the series. Idempotent."""
    series = run.get("series")
    if series and series.get("at"):
        run = {**run, "series": {**series, "at": [zoned(str(a)) for a in series["at"]]}}
    return summary_from_series(run)


# THE MOST SERIES REQUESTS ONE PUBLISH MAY MAKE (T52: "fetched politely … bounded and logged"). A request is made only
# for a run whose issue time moved since the last publish, so a normal morning asks about one per (station, model) that
# reissued — 830 on 2026-10-22 across CLEVER, COFFEE and ELF, then nothing for the rest of the day. Every in-season run at
# once is ~1,000 (325 + 157 + 369 runs that day, plus a margin); past this something is wrong (an issue time that changes
# every tick) and the rest wait for the next run, which still carries what it has.
SERIES_REQUEST_CAP = 1200


def plan_series(summary: dict[str, dict[str, dict]], stations: set[str] | None,
                keep: dict[str, dict[str, dict]] | None,
                now: datetime | None = None) -> tuple[list[tuple[str, str]], int, int]:
    """`(asked, carried, stale)`: which runs need their series fetched, and how many were carried or skipped as stale.

    Pure, so the politeness is pinned in a test rather than in a log. A run is ASKED only when it is in season (it is
    in the summary at all), for a station the feed publishes, issued within `change.STALE_HOURS` of `now` (an old run is
    not a forecast to fetch), and not already on disk with the same issue time (then it is CARRIED, free). Capped at
    SERIES_REQUEST_CAP, in station order.
    """
    from pipeline.gauges.feed.change import stale as _stale     # noqa: PLC0415 — change imports nothing from here
    want: list[tuple[str, str]] = []
    carried = old = 0
    for st, runs in sorted(summary.items()):
        if stations is not None and st not in stations:
            continue
        for model, row in sorted(runs.items()):
            prev = ((keep or {}).get(st) or {}).get(model) or {}
            if prev.get("series") and (prev.get("issuedAt") or "") == (row.get("issuedAt") or ""):
                row["series"] = prev["series"]
                carried += 1
            elif now is not None and _stale(row, now):
                old += 1
            else:
                want.append((st, model))
    return want[:SERIES_REQUEST_CAP], carried, old


def with_series(summary: dict[str, dict[str, dict]], stations: set[str] | None = None,
                keep: dict[str, dict[str, dict]] | None = None,
                workers: int = 8, now: datetime | None = None) -> dict[str, dict[str, dict]]:
    """Attach the per-station series to every run, skipping the ones that have not moved.

    ``keep`` maps ``station -> {model: previous run}`` — the runs already on disk, WITH
    their series. A run whose summary still names the same issue time is carried across
    intact and costs no request; in the steady state that is every run and this pass is
    free.

    IT MUST CARRY THE SERIES ACROSS, not merely decline to re-fetch it. `keep` used to hold
    only issue TIMES, so a run that was up to date was skipped — and then the publisher
    wrote the station file from this summary, which had no series for it. Every publish
    therefore erased the series it had just decided not to re-download, and the next one
    re-downloaded it: measured, 897 of 925 runs were "already current" while exactly ONE
    station file on disk still had a series in it. The whole point of the ribbon on a chart,
    and of ranking a forecast against its envelope, is a series — so both were silently
    empty almost everywhere.
    """
    from concurrent.futures import ThreadPoolExecutor

    want, carried, old = plan_series(summary, stations, keep, now)
    total = sum(len(r) for r in summary.values())
    n = 0
    if want:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            got = list(pool.map(lambda k: (k, series(k[0], k[1])), sorted(want)))
        for (st, model), ser in got:
            if ser:
                summary[st][model]["series"] = ser
                n += 1
    # fetched and carried alike: zoned stamps, and CLEVER's summary from its series (T126)
    for runs in summary.values():
        for model, run in list(runs.items()):
            if run.get("series"):
                runs[model] = settle(run)
    if not want:
        print(f"  series   0 requests, 0 fetched ({carried} carried forward, {old} stale skipped, {total} runs)")
    else:
        print(f"  series   {len(want)} requests, {n} fetched, {carried} carried forward, {old} stale skipped "
              f"({total} runs)")
    return summary


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch BC River Forecast Centre forecasts")
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--models", nargs="*", choices=sorted(MODELS))
    a = ap.parse_args()
    got = fetch(a.models)
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(json.dumps(
        {"fetchedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
         "attribution": ATTRIBUTION, "stations": got},
        separators=(",", ":")), encoding="utf-8")
    runs = sum(len(v) for v in got.values())
    print(f"wrote {a.out}  ({len(got)} stations, {runs} runs)")


if __name__ == "__main__":
    main()
