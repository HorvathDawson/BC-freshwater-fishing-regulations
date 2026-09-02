"""The gauge feed publisher: ECCC readings in, `id -> data` out.

    python -m pipeline.hydro.publish --out app/packages/data/dev/feeds/gauge

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
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

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


def pentad_of(when: datetime) -> int:
    """0..72. Five-day buckets, which is what the envelope is sampled at."""
    return min(72, (when.timetuple().tm_yday - 1) // 5)


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
        index[r["station"]] = {
            "percentile": percentile_of(observed, band),
            "observedAt": r["at"],
            # Three days of CLEVER, already as percentiles — see the docs for why the
            # publisher rather than the client turns a discharge into a colour.
            "forecast": forecast.get(r["station"]),
        }
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
            "forecast": forecast.get(r["station"]),
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
                    default=Path("data/bc_hydrometric_stations.json"))
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
    # `pipeline.hydro.forecast`. Three requests, seasonal, and frequently empty: outside
    # freshet, fall floods and low-flow season no model is running, which is normal and not
    # a failure. A forecast pull that fails costs the map nothing.
    fcast: dict = {}
    if not args.no_forecast:
        try:
            from pipeline.hydro import forecast as _forecast   # noqa: PLC0415
            fcast = _forecast.fetch()
        except Exception as exc:                                # noqa: BLE001
            print(f"  forecast unavailable: {exc}", file=sys.stderr)

    summary = publish(args.out, ids, clim=clim, forecast=fcast, latest_release=latest)
    print(f"  ✅ {args.out}  {summary['answered']}/{summary['asked']} answered, "
          f"{summary['withPercentile']} with a percentile")
    if fcast:
        print(f"     {len(fcast)} station forecasts from the BC River Forecast Centre")
    if summary["release"]:
        print(f"     envelope from HYDAT {summary['release']}")
    if summary["stale"]:
        print(f"  ⚠️  A NEWER HYDAT IS OUT: {summary['latest']} (envelope is on "
              f"{summary['release']}). Refresh with:\n"
              f"       python data/fetch_data.py --layers hydat\n"
              f"       python -m pipeline.hydro.climatology --out <clim.json>")


if __name__ == "__main__":
    main()
