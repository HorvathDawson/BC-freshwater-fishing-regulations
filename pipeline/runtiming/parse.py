"""The fetched run-timing sources -> `data/curated/runtiming/timing.json` (+ the CU polygons deliver binds PSF runs by).

    python -m pipeline.runtiming.parse          # seconds; after `fetch.explorer` / `fetch.boundaries` / `fetch.nuseds`

PSF (the Pacific Salmon Explorer's saved run-timing page, `generate.extract.runs_from_page`): every Conservation Unit that has a
curve, its curve scaled to its own peak. The CU polygons are stitched from the cached Explorer vector tiles into
`data/generated/runtiming/cu_polygons.geojson` (not tracked: it is PSF geometry, 60 MB, and re-made from the tiles).

DFO (NuSEDS, `fetch.nuseds`): per surveyed stream (`GFE_ID` + its FWA watershed code), species and run type, the
years whose survey dated the fish in the stream. A year's PRESENCE runs from the first date the survey gives (stream
arrival, else start of spawning, else the peak) to the last (end of spawning, else the peak's end). The curve is, per
pentad, the percent of those years the fish were present; the words are the medians (arrival, peak of spawning, end).
Only the most recent `MAX_YEARS` dated years count, and a stream with fewer than `MIN_YEARS` gives no record — two
surveys are an anecdote, not a timing.
"""
from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from pipeline.common import calendar_spec as SPEC
from pipeline.common.utils.wsc import is_no_code, trim_wsc
from pipeline.runtiming.timing import CREDIT, PENTADS, slot_of, write_timing

MIN_YEARS = 3
MAX_YEARS = 25
#: a survey year whose dates span more than this is a typo (or two runs entered as one), not a season
MAX_SPAN_DAYS = 200
#: the season starts Mar 1 for medians and spans (coho spawn into January; spring chinook arrive in April)
SEASON_ORIGIN = SPEC.DAYS_BEFORE[2] - 1           # day index of Mar 1 on the common year (0-based): 59
AGGREGATES = {"Stream Aggregate"}
SALMON = ("Chinook", "Chum", "Coho", "Pink", "Sockeye")
_MON = {m.upper(): i + 1 for i, m in enumerate(SPEC.MONTHS)}
#: NuSEDS RUN_TYPE -> the label a reader is told; the numeric codes state no run and are kept apart, unlabelled
RUN_LABELS = {"FALL": "fall", "SUMMER": "summer", "SPRING": "spring", "LATE": "late", "EARLY SUMMER": "early summer",
              "EARLY STUART": "Early Stuart"}


# ---- days --------------------------------------------------------------------------------------------------------

def common_day(month: int, day: int) -> int:
    """0-based day of the COMMON year (Feb 29 is Feb 28's day)."""
    return SPEC.DAYS_BEFORE[month - 1] - (1 if month > 2 else 0) + min(day, SPEC.COMMON_YEAR_LAST_DAY[month - 1]) - 1


def fmt_day(d: int) -> str:
    d %= 365
    for m, n in enumerate(SPEC.COMMON_YEAR_LAST_DAY):
        if d < n:
            return f"{SPEC.MONTHS[m]} {d + 1}"
        d -= n
    return "Dec 31"


def nuseds_day(text: str) -> Optional[int]:
    """'10-AUG-02' -> its common-year day; None for an empty or unreadable date."""
    t = (text or "").strip().upper()
    parts = t.split("-")
    if len(parts) != 3 or parts[1] not in _MON or not parts[0].isdigit():
        return None
    return common_day(_MON[parts[1]], int(parts[0]))


def season(d: int) -> int:
    return (d - SEASON_ORIGIN) % 365


def unseason(s: float) -> int:
    return (int(round(s)) + SEASON_ORIGIN) % 365


# ---- PSF -----------------------------------------------------------------------------------------------------------

def scaled(pentads: Iterable[float]) -> List[int]:
    """A curve scaled to its own peak, 0..100; a non-zero pentad never rounds to 0 (it is in the river)."""
    xs = list(pentads)
    mx = max(xs) if xs else 0
    if mx <= 0:
        raise ValueError("a curve with no peak")
    return [0 if v <= 0 else max(1, int(round(100 * v / mx))) for v in xs]


def psf_records(runs) -> List[dict]:
    out = []
    for cid, r in sorted(runs.items()):
        if not r.has_curve:
            continue
        out.append({"key": f"psf:{cid}", "source": "psf", "species": r.species,
                    "slot": slot_of(r.species, r.form if r.species == "Pink" else None), "name": r.name,
                    "label": r.label, "curve": scaled(r.pentads), "peak": r.peak_date,
                    "window": list(r.span) if r.span else None, "quality": r.quality_word, "cuid": cid})
    return out


# ---- DFO -----------------------------------------------------------------------------------------------------------

def year_window(row: dict) -> Optional[Tuple[int, int, Optional[int]]]:
    """(first, last, peak) season days the survey puts the fish in the stream, or None."""
    first = next((d for d in (nuseds_day(row.get(k, "")) for k in
                              ("STREAM_ARRIVAL_DT_FROM", "START_SPAWN_DT_FROM", "PEAK_SPAWN_DT_FROM")) if d is not None),
                 None)
    last = next((d for d in (nuseds_day(row.get(k, "")) for k in
                             ("END_SPAWN_DT_TO", "END_SPAWN_DT_FROM", "PEAK_SPAWN_DT_TO", "PEAK_SPAWN_DT_FROM"))
                 if d is not None), None)
    if first is None or last is None:
        return None
    a, b = season(first), season(last)
    if b < a or b - a > MAX_SPAN_DAYS:
        return None
    pk = nuseds_day(row.get("PEAK_SPAWN_DT_FROM", ""))
    return a, b, (season(pk) if pk is not None else None)


def presence_curve(windows: List[Tuple[int, int]]) -> List[int]:
    """Percent of the years present in each pentad (a year counts once per pentad it touches)."""
    hits = [0] * PENTADS
    for a, b in windows:
        ps = {min(PENTADS - 1, unseason(s) // 5) for s in range(a, b + 1)}
        for p in ps:
            hits[p] += 1
    n = len(windows)
    return [int(round(100 * h / n)) for h in hits]


def dfo_records(rows: Iterable[dict], sites: Dict[str, Tuple[float, float, str]]) -> Tuple[List[dict], dict]:
    """The NuSEDS rows -> one record per (site, species, run type[, pink brood line]) with enough dated years. A site
    DFO types as an aggregate ("Stream Aggregate": the uncounted remainder of a system) is not a place, and gives no
    record."""
    by: Dict[tuple, Dict[int, tuple]] = defaultdict(dict)
    names: Dict[tuple, str] = {}
    seen = 0
    for row in rows:
        sp = row.get("SPECIES", "")
        raw = row.get("FWA_WATERSHED_CDE", "")
        if sp not in SALMON or not raw or is_no_code(raw) or not row.get("GFE_ID"):
            continue
        if sites.get(row["GFE_ID"].strip(), (None, None, ""))[2] in AGGREGATES:
            continue
        try:
            year = int(row["ANALYSIS_YR"])
        except (KeyError, ValueError):
            continue
        w = year_window(row)
        if w is None:
            continue
        seen += 1
        run = (row.get("RUN_TYPE") or "").strip().upper()
        slot = slot_of(sp, year=year)
        k = (row["GFE_ID"].strip(), trim_wsc(raw.strip()), sp, slot, run)
        by[k][year] = w                                     # one survey per stream, species, run and year
        # WATERBODY, not GAZETTED_NAME: it names the surveyed stretch ("BABINE RIVER - SECTION 4")
        names[k] = (row.get("WATERBODY") or row.get("GAZETTED_NAME") or "").strip()
    out, dropped = [], 0
    for k in sorted(by):
        gfe, wsc, sp, slot, run = k
        years = sorted(by[k])[-MAX_YEARS:]
        if len(years) < MIN_YEARS:
            dropped += 1
            continue
        ws = [by[k][y] for y in years]
        peaks = [w[2] for w in ws if w[2] is not None]
        ll = sites.get(gfe)
        out.append({
            "key": f"dfo:{gfe}:{slot}:{run or '-'}", "source": "dfo", "species": sp, "slot": slot,
            "name": titled(names[k]), "label": RUN_LABELS.get(run),
            "curve": presence_curve([(a, b) for a, b, _ in ws]),
            "peak": fmt_day(unseason(statistics.median(peaks))) if peaks else None,
            "window": [fmt_day(unseason(statistics.median(w[0] for w in ws))),
                       fmt_day(unseason(statistics.median(w[1] for w in ws)))],
            "years": len(years), "first_year": years[0], "last_year": years[-1], "wsc": wsc,
            "lon": ll[0] if ll else None, "lat": ll[1] if ll else None, "site_kind": ll[2] if ll else None})
    return out, {"dated_survey_rows": seen, "records": len(out), f"under_{MIN_YEARS}_years": dropped}


_SMALL = {"of", "and", "the", "de"}


def titled(name: str) -> str:
    """'LITTLE QUALICUM RIVER' -> 'Little Qualicum River' (DFO names are upper case)."""
    words = name.lower().split()
    return " ".join(w if (i and w in _SMALL) else w[:1].upper() + w[1:] for i, w in enumerate(words))


def read_sites(path: Path) -> Dict[str, Tuple[float, float, str]]:
    """{GFE_ID: (lon, lat, DFO's site type)} from the census-site list (one row per site and species)."""
    out = {}
    with open(path, newline="", encoding="latin-1") as f:
        for r in csv.DictReader(f):
            try:
                out[r["GFE_ID"].strip()] = (round(float(r["X_LONGT"]), 5), round(float(r["Y_LAT"]), 5),
                                            (r.get("GFE_TYPE") or "").strip())
            except (KeyError, ValueError):
                continue
    return out


# ---- polygons ------------------------------------------------------------------------------------------------------

def write_polygons(tiles: Path, out: Path) -> int:
    """The CU polygons stitched from the cached Explorer tiles (the tile's feature id is the cuid)."""
    import gzip
    import math
    from collections import defaultdict as dd

    import mapbox_vector_tile as mvt
    from shapely import make_valid
    from shapely.geometry import mapping, shape
    from shapely.ops import unary_union
    zoom, pieces = 8, dd(list)
    for p in sorted(tiles.glob(f"{zoom}_*.pbf")):
        b = p.read_bytes()
        if not b:
            continue
        _, x, y = p.stem.split("_")
        x, y, n = int(x), int(y), 2 ** zoom
        t = mvt.decode(gzip.decompress(b) if b[:2] == b"\x1f\x8b" else b)
        layer = t.get("conservation-units")
        if not layer:
            continue
        ext = layer["extent"]
        w, e = x / n * 360.0 - 180.0, (x + 1) / n * 360.0 - 180.0
        lat = lambda yy: math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * yy / n))))     # noqa: E731
        s, nn = lat(y + 1), lat(y)
        to_ll = lambda pt: (w + pt[0] / ext * (e - w), s + pt[1] / ext * (nn - s))         # noqa: E731
        for f in layer["features"]:
            cid, g = f.get("id"), f["geometry"]
            depth = {"Polygon": 2, "MultiPolygon": 3}.get(g["type"])
            if cid is None or not depth:
                continue
            conv = lambda c, d: [conv(i, d - 1) for i in c] if d > 1 else [to_ll(q) for q in c]  # noqa: E731
            geo = make_valid(shape({"type": g["type"], "coordinates": conv(g["coordinates"], depth)}))
            if not geo.is_empty:
                pieces[cid].append(geo)
    if not pieces:
        raise SystemExit(f"parse: no CU tiles under {tiles} — fetch them: python -m pipeline.runtiming.fetch.boundaries")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"type": "FeatureCollection", "features": [
        {"type": "Feature", "id": c, "properties": {"cuid": c}, "geometry": mapping(unary_union(pieces[c]).buffer(0))}
        for c in sorted(pieces)]}))
    return len(pieces)


def main(src: Path, out: Path, polygons: Optional[Path], review: Optional[Path]) -> None:
    from pipeline.runtiming.generate.extract import runs_from_page
    runs = runs_from_page(src, review)
    psf = psf_records(runs)
    dfo_dir = src / "dfo"
    for need in ("nuseds.csv", "census_sites.csv", "fetched.json"):
        if not (dfo_dir / need).is_file():
            raise SystemExit(f"parse: no {dfo_dir / need} — fetch it: python -m pipeline.runtiming.fetch.nuseds")
    with open(dfo_dir / "nuseds.csv", newline="", encoding="latin-1") as f:
        dfo, rep = dfo_records(csv.DictReader(f), read_sites(dfo_dir / "census_sites.csv"))
    psf_fetch = json.loads((src / "fetched.json").read_text())
    dfo_fetch = json.loads((dfo_dir / "fetched.json").read_text())
    up = psf_fetch["upstream_dataset"]
    sources = {
        "psf": {"credit": CREDIT["psf"], "title": up["name"], "dataset": up["landing"], "updated": up["updated"],
                "fetched": psf_fetch["fetched_at"], "runs": len(psf)},
        "dfo": {"credit": CREDIT["dfo"], "title": dfo_fetch["title"], "dataset": dfo_fetch["record"],
                "licence": "Open Government Licence – Canada", "file": dfo_fetch["files"]["nuseds"]["name"],
                "updated": dfo_fetch["files"]["nuseds"]["date"], "fetched": dfo_fetch["fetched_at"], **rep},
    }
    write_timing(out, psf, dfo, sources)
    print(f"parse: {len(psf)} PSF runs, {len(dfo)} DFO stream records ({rep}) -> {out}")
    if polygons is not None:
        print(f"parse: {write_polygons(src / 'cu_tiles', polygons)} CU polygons -> {polygons}")


if __name__ == "__main__":
    from pipeline.common.curated import CURATED, GENERATED, SOURCE
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--src", type=Path, default=SOURCE / "runtiming")
    ap.add_argument("--out", type=Path, default=CURATED.runtiming.timing)
    ap.add_argument("--polygons", type=Path, default=GENERATED.runtiming / "cu_polygons.geojson")
    ap.add_argument("--no-polygons", action="store_true", help="leave the CU polygons as they are")
    a = ap.parse_args()
    main(a.src, a.out, None if a.no_polygons else a.polygons, CURATED.runtiming.review)
