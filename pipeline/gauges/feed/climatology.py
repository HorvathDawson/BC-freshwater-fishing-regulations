"""The percentile envelope: what a river USUALLY does, by time of year.

    python -m pipeline.gauges.feed.climatology --hydat data/source/hydat.sqlite3 --out data/generated/gauges/feeds/clim.json

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
from pipeline.common.curated import CURATED, SOURCE
from pipeline.gauges.feed.record import MIN_RECORD_YEARS

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


#: Half-width, in days, of the window each pentad pools from. 0 restores hard bins.
#:
#: A CENTRED WINDOW, NOT A BUCKET, and the difference is a defect measured on the shipped
#: file: with hard 5-day bins the median flow moves 9.1% from one pentad to the next, and
#: 17.7% of adjacent pairs move by more than 25%. That is the YARDSTICK moving, not the
#: river — so a reading that does not change at all reports a different percentile the next
#: morning, purely because the calendar crossed a bin edge.
#:
#: Pooling +/- 5 days around each pentad's centre makes neighbouring pentads share most of
#: their observations, so the envelope varies smoothly along the year instead of stepping.
#: It also triples the sample behind every band, which is the other half of the problem:
#: five days x N years is thin in the tails, and thin tails are what made the steps large.
#:
#: The window is deliberately NOT wider. During the freshet rise the seasonal median moves
#: fast and a wide window smears it, biasing the early limb low and the late limb high —
#: the opposite error, and a worse one because it is systematic rather than jumpy.
WINDOW_DAYS = 5

#: The year is circular. Late December pools with early January, which is one continuous
#: hydrological season and two ends of an array.
_YDAYS = 366


def pentads_near(yday: int, half: int = WINDOW_DAYS) -> set[int]:
    """Which pentads an observation on this day of year contributes to.

    One day feeds every pentad whose centre is within `half` days of it — which is what
    makes the windows overlap and the envelope continuous. Wraps at the year boundary.
    """
    out: set[int] = set()
    for d in range(yday - half, yday + half + 1):
        wrapped = (d - 1) % _YDAYS + 1
        out.add(pentad_of_yday(wrapped))
    return out


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


# ── (T550 item 2) LEVEL DATUM SHIFTS ───────────────────────────────────────────────────────────────────────────────
#
# A STAGE RECORD IS ONLY COMPARABLE TO ITSELF WHILE ITS DATUM HOLDS. HYDAT pools every DLY_LEVELS value a station ever
# had, and a gauge that was re-surveyed, moved or re-zeroed steps: 08HD006 (Salmon River near Sayward) reads 3.3 m at
# mid flows 2011-2015 and 6.9 m from 2016 (HYDAT 20261019; STN_DATUM_CONVERSION has no row for it), 08LB024 0.3 m to
# 2017 and 1.9 m from 2018. Pooled, the envelope spans both datums and a live level ranks against a band that is half
# another gauge. So the level record is cut at its LAST SHIFT and only the years since rank a level; a record left
# shorter than MIN_RECORD_YEARS ranks none. `gauge_stats` says so (`level_since`, `level_shifts`).
#
# THE STEP TEST, per station, on one number a year: the median level on days whose flow is in the middle third of the
# station's flow record (so a wet or a dry year cannot pass for a shift), or, with no flow, the year's median level.
# The best split (least squared deviation from the two sides' means, each side >= 2 years) is a shift when the jump
# between the sides' medians is at least SHIFT_MIN_M and SHIFT_SPREAD_X times the larger side's median absolute
# deviation; each side is searched again (`level_shifts`), and the LAST shift starts the kept record.
SHIFT_MIN_M = 0.5
SHIFT_SPREAD_X = 5.0
SHIFT_SIDE_YEARS = 2
# a year needs this many mid-flow days (or level days, with no flow) to be a point of the test
SHIFT_YEAR_DAYS = 20
#
# THE LIVE READING'S DATUM. A live level can be on yet another datum than the newest HYDAT year (08HD006 reads 2.34 m
# at 118 m³/s on 2026-11-10, where its 2016-2024 record says ~6.9 m at that flow): no cut of the archive can rank it.
# So the envelope carries the station's RATING over the kept years — at each flow decile knot, the 1st and 99th
# percentile level seen between it and the next — and the publisher ranks no level whose (flow, level) pair is off it
# by more than RATING_PAD_M (`publish.level_on_rating`). Stations with no flow record carry none (nothing to pair).
RATING_KNOTS = 11
RATING_PAD_M = 0.5
RATING_MIN_PAIRS = 200


def _daily(db, table: str, col: str, station: str) -> dict[date, float]:
    """One station's daily values from one HYDAT table, by day."""
    cols = ", ".join(f"{col}{d}" for d in range(1, 32))
    out: dict[date, float] = {}
    for row in db.execute(f"SELECT YEAR, MONTH, {cols} FROM {table} WHERE STATION_NUMBER = ?", [station]):
        for d, v in enumerate(row[2:], start=1):
            if v is None:
                continue
            try:
                out[date(row[0], row[1], d)] = float(v)
            except ValueError:
                continue
    return out


def _median(xs: list[float]) -> float:
    xs = sorted(xs)
    return _quantile(xs, 0.5)


def _mad(xs: list[float]) -> float:
    m = _median(xs)
    return _median([abs(x - m) for x in xs])


def level_shifts(points: list[tuple[int, float]]) -> list[int]:
    """The years a level record shifts datum at (each the first year AFTER a shift), oldest first. See SHIFT_MIN_M.

    Binary segmentation: the split with the least squared deviation from its two sides' means is a shift when the jump
    between the sides' medians is at least SHIFT_MIN_M and SHIFT_SPREAD_X times the larger side's median absolute
    deviation (robust to a further shift inside a side); each side is then searched again.
    """
    pts = sorted(points)
    if len(pts) < 2 * SHIFT_SIDE_YEARS:
        return []
    best: tuple[float, int] | None = None
    for k in range(SHIFT_SIDE_YEARS, len(pts) - SHIFT_SIDE_YEARS + 1):
        a, b = [v for _, v in pts[:k]], [v for _, v in pts[k:]]
        ma, mb = sum(a) / len(a), sum(b) / len(b)
        sse = sum((v - ma) ** 2 for v in a) + sum((v - mb) ** 2 for v in b)
        if best is None or sse < best[0]:
            best = (sse, k)
    k = best[1]
    a, b = [v for _, v in pts[:k]], [v for _, v in pts[k:]]
    jump = abs(_median(b) - _median(a))
    if jump < SHIFT_MIN_M or jump < SHIFT_SPREAD_X * max(_mad(a), _mad(b)):
        return []
    return level_shifts(pts[:k]) + [pts[k][0]] + level_shifts(pts[k:])


def rating_of(pairs: list[tuple[float, float]]) -> list[list[float]] | None:
    """[[flow knot, level p1, level p99], ...] over flow deciles; None under RATING_MIN_PAIRS pairs. See RATING_PAD_M."""
    if len(pairs) < RATING_MIN_PAIRS:
        return None
    pairs = sorted(pairs)
    qs = [q for q, _ in pairs]
    knots = [_quantile(qs, i / (RATING_KNOTS - 1)) for i in range(RATING_KNOTS)]
    out: list[list[float]] = []
    for i in range(RATING_KNOTS - 1):
        lo, hi = knots[i], knots[i + 1]
        hs = sorted(h for q, h in pairs if lo <= q <= hi)
        if not hs:
            return None
        out.append([_sig4(lo), _sig4(_quantile(hs, 0.01)), _sig4(_quantile(hs, 0.99))])
    out.append([_sig4(knots[-1]), out[-1][1], out[-1][2]])
    return out


def level_record(db, station: str) -> dict:
    """{since, shifts, rating}: the year the station's comparable level record starts (None: all of it), the shift years,
    and the rating over the kept years (None: no flow to pair, or too few pairs)."""
    lv = _daily(db, "DLY_LEVELS", "LEVEL", station)
    if not lv:
        return {"since": None, "shifts": [], "rating": None}
    fl = _daily(db, "DLY_FLOWS", "FLOW", station)
    by_year: dict[int, list[float]] = defaultdict(list)
    if fl:
        qs = sorted(fl.values())
        lo, hi = qs[len(qs) // 3], qs[2 * len(qs) // 3]
        for d, h in lv.items():
            q = fl.get(d)
            if q is not None and lo <= q <= hi:
                by_year[d.year].append(h)
    if not fl or sum(1 for v in by_year.values() if len(v) >= SHIFT_YEAR_DAYS) < 2 * SHIFT_SIDE_YEARS:
        by_year = defaultdict(list)
        for d, h in lv.items():
            by_year[d.year].append(h)
    points = [(y, _median(v)) for y, v in by_year.items() if len(v) >= SHIFT_YEAR_DAYS]
    shifts = level_shifts(points)
    since = shifts[-1] if shifts else None
    pairs = [(fl[d], h) for d, h in lv.items() if d in fl and (since is None or d.year >= since)]
    return {"since": since, "shifts": shifts, "rating": rating_of(pairs) if fl else None}


def _read(db, table: str, col: str, where: str,
          args: list, since: dict[str, int] | None = None) -> tuple[dict, dict, dict, dict]:
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
        if since and st in since and yr < since[st]:
            continue              # before the station's last datum shift (T550): another gauge, in effect
        for d, v in enumerate(row[3:], start=1):
            if v is None:
                continue
            try:
                when = date(yr, mo, d)
            except ValueError:
                continue          # day 31 of a 30-day month; HYDAT pads the row
            yday = when.timetuple().tm_yday
            # ONE OBSERVATION, SEVERAL PENTADS — see `pentads_near`. This used to land in
            # exactly one bucket, which is what made the envelope step at bin edges.
            val = float(v)
            for pent in pentads_near(yday):
                pooled[st][pent].append(val)
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

    # (T550) every level record's datum shifts and rating, BEFORE the level table is pooled: the pool keeps only the
    # years since the last shift
    level_ids = stations if stations else [r[0] for r in db.execute("SELECT DISTINCT STATION_NUMBER FROM DLY_LEVELS")]
    records = {st: level_record(db, st) for st in sorted(set(level_ids))}
    since = {st: r["since"] for st, r in records.items() if r["since"] is not None}
    series = {
        "discharge": _read(db, "DLY_FLOWS", "FLOW", where, args),
        "level": _read(db, "DLY_LEVELS", "LEVEL", where, args, since),
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
            if len(years[st]) < MIN_RECORD_YEARS:
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
            if param == "level" and records.get(st, {}).get("shifts"):
                stats[st][param].update(level_since=records[st]["since"], level_shifts=records[st]["shifts"])
    # (T550) the stations whose level record shifted: listed for the owner (printed by `main`), every one of them,
    # including those left with too short a record to rank a level at all
    shifted = {st: {"since": r["since"], "shifts": r["shifts"],
                    "ranked": "level" in envelope.get(st, {})} for st, r in records.items() if r["shifts"]}
    ratings = {st: r["rating"] for st, r in records.items() if r["rating"] and "level" in envelope.get(st, {})}

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
        # (T550) each level envelope's rating over its kept years: the publisher ranks no level whose live (flow, level)
        # pair is off it (`publish.level_on_rating`) — the live gauge is then on another datum than the record
        "ratings": ratings,
        "ratingPadM": RATING_PAD_M,
        "levelShifts": shifted,
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
    shifted = out["levelShifts"]
    if shifted:
        print(f"  ⚠️  {len(shifted)} level record(s) shift datum; only the years since the last shift rank a level "
              f"(owner review: the list is clim.json `levelShifts`):")
        for st, r in sorted(shifted.items()):
            print(f"     {st}  shifts {', '.join(map(str, r['shifts']))}  "
                  + ("ranked from " + str(r["since"]) if r["ranked"] else "no level ranked (record since too short)"))


if __name__ == "__main__":
    main()
