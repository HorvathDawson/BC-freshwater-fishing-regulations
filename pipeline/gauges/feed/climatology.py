"""The percentile envelope: what a river USUALLY does, by time of year.

    python -m pipeline.gauges.feed.climatology --hydat data/hydat.sqlite3 --out output/feeds/gauge/clim.json

WHAT IT IS FOR. "12 m³/s" tells a person nothing. "12 m³/s, which is the 4th percentile for
early September" tells them the river is very low. That second sentence needs 97 years of
daily record, and this is the only thing that reads it.

TWO DECISIONS THAT ARE SCHEMA, NOT TUNING — both measured, both from the data contract §5:

  · PENTAD, NOT DAILY. Percentile-versus-day curves are smooth, so sampling every 5 days
    costs 0.44% mean error and takes the envelope from 4.5 MB to about 325 KB. That size is
    what lets the 30-minute feed job hold it in memory on a worker instead of reaching for
    a 63 MB bundle.

  · p10–p90 ONLY. p0 and p100 do NOT interpolate — 82% error, and every worst case was the
    record maximum. A record maximum is one storm, not a function of day-of-year. The
    extremes are cosmetic (the outer band of a chart) and are better absent than wrong.

TWO PUBLICATION GATES, so a thin record cannot masquerade as a long one:

  · a station needs >= 3 years of record to be published at all
  · a pentad needs >= 10 pooled observations to emit a percentile; otherwise it is null

THE FLOOR IS 3 YEARS, NOT 10, and that is a deliberate loosening. Ten was the data
contract's figure and it silenced 87 of BC's 439 reporting stations — the Skagit among them
— which showed on the map as water nobody measures, beside a sheet saying the river was
gauged. Three years is enough to say "low for the date" even though it is not enough to say
"fourth percentile", and the difference between those two claims is carried by `gauge_stats`
rather than by refusing to answer: every envelope states the record it was built from, and
a short one should be shown qualified, never hidden.

A pentad still needs 10 pooled observations. Three years of daily record gives about 15 per
pentad, so the shape of the gate is unchanged — it is only the station-level floor that moved.

`gauge_stats` alongside records what each envelope was built from, because "below normal for
the date" means something different backed by 97 years than by 11.

PROVENANCE IS PART OF THE OUTPUT. The HYDAT release the numbers came from is stamped into
the file, and the feed job carries it through into the index. A percentile with no stated
source is a number claiming more authority than it has.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from collections import defaultdict
from datetime import date
from pathlib import Path
from pipeline.curated import CURATED, SOURCE

MIN_YEARS = 3
MIN_OBS = 10
PENTADS = 73                 # 365 / 5, with the 73rd absorbing day 366
PCTILES = (10, 25, 50, 75, 90)

# How many complete years of the daily record travel with the envelope.
#
# The seasonal chart shows this year against normal, and "normal" is a band with no shape —
# it cannot show whether last summer was also dry, which is the question a person actually
# asks standing on a low river. Two recent years is enough to answer it and small enough to
# ride in the feed: 2 x 366 x 4 bytes is ~3 KB a station beside a file already 23 KB.
RECENT_YEARS = 2


def pentad_of_yday(yday: int) -> int:
    return min(PENTADS - 1, (yday - 1) // 5)


def _sig4(v: float) -> float:
    """Four significant figures, not four decimal places.

    The difference is most of the file. A flow of 0.0012345 needs its small digits and one
    of 12345.6 does not, so fixed decimals store noise at the top of the range and lose
    signal at the bottom: `1234.5678` is eight characters of JSON for four figures of real
    precision. Measured on the full BC roster this is 2,464 KB -> a fraction of it, with no
    loss anybody could see on a chart.
    """
    if v == 0 or v != v:
        return 0.0
    from math import floor, log10

    return round(v, -int(floor(log10(abs(v)))) + 3)


def _quantile(xs: list[float], q: float) -> float:
    """Linear-interpolated quantile. `xs` must be sorted."""
    if not xs:
        return float("nan")
    i = (len(xs) - 1) * q
    lo, hi = int(i), min(int(i) + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (i - lo)


def _read(db, table: str, col: str, where: str,
          args: list) -> tuple[dict, dict, dict, dict]:
    """Pool one HYDAT daily table into pentads. Returns (pooled, years, days, recent).

    ``recent`` is the raw daily record for the last few complete years, keyed
    ``{station: {year: [366 values]}}`` — the LINE the seasonal chart draws across its band,
    for years the envelope has already absorbed. It is the same read either way, so pulling
    it out here costs one dict rather than a second pass over 40 million cells.
    """
    pooled: dict[str, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    years: dict[str, set[int]] = defaultdict(set)
    days: dict[str, int] = defaultdict(int)
    recent: dict[str, dict[int, list[float | None]]] = defaultdict(dict)
    # "Complete" means a year HYDAT has finished publishing. The current calendar year is
    # never that, and the one before it usually is not either at the start of a release.
    newest = date.today().year - 1
    keep = set(range(newest - RECENT_YEARS + 1, newest + 1))
    cols = ", ".join(f"{col}{d}" for d in range(1, 32))
    for row in db.execute(f"SELECT STATION_NUMBER, YEAR, MONTH, {cols} FROM {table}{where}",
                          args):
        st, yr, mo = row[0], row[1], row[2]
        for d, v in enumerate(row[3:], start=1):
            if v is None:
                continue
            try:
                when = date(yr, mo, d)
            except ValueError:
                continue          # day 31 of a 30-day month; HYDAT pads the row
            yday = when.timetuple().tm_yday
            pooled[st][pentad_of_yday(yday)].append(float(v))
            years[st].add(yr)
            days[st] += 1
            if yr in keep:
                slot = recent[st].setdefault(yr, [None] * 366)
                slot[yday - 1] = _sig4(float(v))
    return pooled, years, days, recent


def build(hydat: Path, stations: list[str] | None = None,
          release: str | None = None) -> dict:
    """Read HYDAT's daily record and return the envelope plus its statistics.

    BOTH ENVELOPES WHERE BOTH EXIST. A station is keyed by parameter — `{"discharge": {...},
    "level": {...}}` — because the two answer different questions and a reader may want
    either. "Is the river low" and "is the river deep" are not the same query, and a station
    with 60 years of both should not have to pick one at build time.

    DISCHARGE WHERE THERE IS ANY, LEVEL WHERE THERE IS NOT. A great many BC stations measure
    stage and never discharge — the Skagit at the International Boundary has 540 months of
    daily LEVELS back to 1953 and ten of flows — and 53 of the 69 reporting stations that
    came back with no percentile were exactly this. They were rendering as water nobody
    measures, beside a sheet saying the river was gauged.

    A level percentile is a real answer to a real question: "is the river high or low for
    the date". It is NOT interchangeable with a discharge percentile, so `gauge_stats`
    records which one each station's envelope is, and nothing may present them as the same
    number. What they share is the only thing the map asks — where today sits against
    normal.
    """
    db = sqlite3.connect(f"file:{hydat}?mode=ro", uri=True)

    # DLY_FLOWS is one row per station-month with 31 value columns. Unpacking it here
    # rather than in SQL keeps the query trivial and the reshaping readable.
    where, args = "", []
    if stations:
        where = f" WHERE STATION_NUMBER IN ({','.join('?' * len(stations))})"
        args = list(stations)

    series = {
        "discharge": _read(db, "DLY_FLOWS", "FLOW", where, args),
        "level": _read(db, "DLY_LEVELS", "LEVEL", where, args),
    }
    db.close()

    envelope: dict[str, dict[str, dict[str, list[float | None]]]] = {}
    stats: dict[str, dict] = {}
    # `{station: {parameter: {year: [366 daily values]}}}` — the recent complete years, for
    # the seasonal chart to draw beside this one. Only for stations that get an envelope:
    # a year trace with no band to read it against says nothing.
    recent_years: dict[str, dict[str, dict[str, list[float | None]]]] = {}
    for param, (pooled, years, days, recent) in series.items():
        for st, by_pentad in pooled.items():
            if len(years[st]) < MIN_YEARS:
                continue          # under three years there is no "normal" to speak of
            bands: dict[str, list[float | None]] = {}
            for p, vals in by_pentad.items():
                if len(vals) < MIN_OBS:
                    continue      # absent, not guessed
                vals.sort()
                bands[str(p)] = [_sig4(_quantile(vals, q / 100)) for q in PCTILES]
            if not bands:
                continue
            envelope.setdefault(st, {})[param] = bands
            if recent.get(st):
                recent_years.setdefault(st, {})[param] = {
                    str(y): v for y, v in sorted(recent[st].items())}
            # One stats row per station-parameter, so a reader can be told the record
            # behind the number they are actually looking at.
            stats.setdefault(st, {})[param] = {
                "from_year": min(years[st]), "to_year": max(years[st]),
                "years": len(years[st]), "days": days[st]}

    return {
        "release": release or "unknown",
        "percentiles": list(PCTILES),
        "pentads": PENTADS,
        "stations": envelope,
        "stats": stats,
        # The last complete years, day by day. NOT part of the envelope — the envelope is
        # every year pooled and has no shape in time; this is what lets a reader see that
        # last summer was dry too, which a band cannot show however wide it is.
        "recentYears": recent_years,
        "recentYearCount": RECENT_YEARS,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Build the percentile envelope from HYDAT")
    ap.add_argument("--hydat", type=Path, default=SOURCE / "hydat.sqlite3")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--stations", type=Path,
                    default=SOURCE / "bc_hydrometric_stations.json",
                    help="restrict to this roster; BC only, so the file stays small")
    args = ap.parse_args()

    if not args.hydat.exists():
        raise SystemExit(f"{args.hydat} not found — "
                         f"run: python data/fetch_data.py --layers hydat")

    ids = None
    if args.stations.exists():
        ids = [s["station"] for s in json.loads(args.stations.read_text(encoding="utf-8"))]

    marker = args.hydat.with_suffix(".release")
    release = marker.read_text().strip() if marker.exists() else None

    out = build(args.hydat, ids, release)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(out, separators=(",", ":")), encoding="utf-8")
    n = len(out["stations"])
    size = args.out.stat().st_size
    yrs = [p["years"] for s in out["stats"].values() for p in s.values()]
    print(f"  ✅ {args.out}  {n:,} stations, {size / 1000:.0f} KB, HYDAT {out['release']}")
    if yrs:
        print(f"     record length: median {sorted(yrs)[len(yrs) // 2]} years, "
              f"shortest {min(yrs)}, longest {max(yrs)}")


if __name__ == "__main__":
    main()
