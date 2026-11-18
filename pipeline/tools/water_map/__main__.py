"""python -m pipeline.tools.water_map "<water name or item id>" [--release DIR] [--date YYYY-MM-DD]
                                     [--mode salmon|freshwater|both] [--out DIR] [--focus KM-KM] [--max-tribs N]

Draws the labelled rules map of one water's system (T853); see the package docstring.
"""
from __future__ import annotations

import argparse
import math
import re
import sys
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from pipeline.tools.water_map import geo as G
from pipeline.tools.water_map import model as M
from pipeline.tools.water_map.release import Release, layers_dir, tile_vintage_note

DEFAULT_OUT = Path(tempfile.gettempdir()) / "water_map"


def today_bc() -> date:
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("America/Vancouver")).date()


def _focus(s: Optional[str]) -> Optional[Tuple[float, float]]:
    if not s:
        return None
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*[-–:]\s*(\d+(?:\.\d+)?)\s*", s)
    if not m or float(m.group(1)) >= float(m.group(2)):
        raise SystemExit(f"water_map: --focus wants KM-KM (low-high), not {s!r}")
    return float(m.group(1)), float(m.group(2))


def slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def build_maps(rel: Release, query: str, day: date, modes: List[str], out_dir: Path, *,
               layers: Optional[Path] = None, focus=None, max_tribs: int = M.MAX_TRIBS) -> List[Path]:
    from pipeline.tools.water_map import render as R
    from shapely.ops import unary_union

    target = M.resolve(rel, query)
    sysm = M.system(rel, target, focus=focus, max_tribs=max_tribs)
    reader = M.Reader(rel, day)
    lay = layers_dir(layers, rel)
    boxes = G.part_boxes(rel, {p.water.ord for p in sysm.parts})
    pb = [boxes[(p.water.ord, p.ix)] for p in sysm.parts if (p.water.ord, p.ix) in boxes]
    ub = G.union_box(pb)
    if ub is None:
        raise SystemExit(f"water_map: the release has no part boxes for {target.name}")
    w, s, e, n = ub
    padx, pady = max((e - w) * 0.06, 0.01), max((n - s) * 0.06, 0.008)
    ext0 = (w - padx, s - pady, e + padx, n + pady)
    span_km = max((e - w) * 111 * math.cos(math.radians((s + n) / 2)), (n - s) * 111)
    min_order = 5 if span_km > 400 else 4 if span_km > 150 else 3 if span_km > 40 else 2
    sids = {sid for p in sysm.parts for sid in p.sids}
    geo = G.read(lay, sids, (ext0[0] - 0.4, ext0[1] - 0.3, ext0[2] + 0.4, ext0[3] + 0.3), min_order=min_order,
                 min_lake_m2=200_000 if span_km > 40 else 20_000)
    all_lines = unary_union([geo.sections[s_] for s_ in sids if s_ in geo.sections]) if geo.sections else None
    basin = None
    if all_lines is not None and not all_lines.is_empty and geo.basins and span_km > 30:
        total = all_lines.length
        keep = [g for _, g in geo.basins if all_lines.intersection(g).length >= 0.12 * total]
        if keep:
            basin = unary_union(keep).buffer(0.002).buffer(-0.002)
    missing = [p for p in sysm.parts if p.sids and (p.key is not None or p.tidal)
               and not any(s_ in geo.sections for s_ in p.sids)]

    regions = sorted({r for p in sysm.parts for r in p.water.regions})
    sources = rel.manifest.get("sources") or {}
    syn = (sources.get("synopsis") or {}).get("title") or "BC Freshwater Fishing Regulations Synopsis"
    outs = []
    for mode in modes:
        units, base = M.units(reader, sysm, mode)
        brks = M.breaks(reader, sysm, units)
        from shapely.ops import nearest_points
        for b, top, bottom in M.continuation_breaks(reader, sysm, units):
            ga = unary_union([geo.sections[s_] for s_ in top.sids if s_ in geo.sections])
            gb = unary_union([geo.sections[s_] for s_ in bottom.sids if s_ in geo.sections])
            if ga.is_empty or gb.is_empty:
                continue
            pa, pb_ = nearest_points(ga, gb)
            b.lon, b.lat = (pa.x + pb_.x) / 2, (pa.y + pb_.y) / 2
            brks.append(b)
        drawn = {(p.water.ord, p.ix) for u in units for p in u.parts}
        loose = [(4, geo.sections[s_]) for p in sysm.parts if (p.water.ord, p.ix) not in drawn
                 for s_ in p.sids if s_ in geo.sections]
        geoms: Dict[int, list] = {}
        points: Dict[int, Tuple[float, float]] = {}
        names: Dict[int, str] = {}
        seen_water = set()
        for u in units:
            gs = [geo.sections[s_] for s_ in u.sids if s_ in geo.sections]
            geoms[u.number] = gs
            if not gs:
                for p in u.parts:
                    b = boxes.get((p.water.ord, p.ix))
                    if b:
                        points[u.number] = ((b[0] + b[2]) / 2, (b[1] + b[3]) / 2)
                        break
            wid = u.parts[0].water.ord
            if wid not in seen_water and gs and u.role != "context":
                seen_water.add(wid)
                names[u.number] = u.parts[0].water.name
        words = {"salmon": "salmon", "freshwater": "freshwater (B.C. rules)"}[mode]
        title = f"{target.name} — {words} as the app holds them"
        what = ("Salmon status on the day, part by part, with the cascade behind each answer" if mode == "salmon"
                else "B.C. freshwater status on the day, part by part: the rule-book rows, the gear and the trout "
                     "and char answer")
        subtitle = f"{what}   ·   as of {day.isoformat()}, release {rel.id}"
        footer = [f"Sources: {syn} (pages cited per row) · release {rel.id} answers (regs store, export subset)"
                  + (f" · DFO freshwater salmon pages, regions {', '.join(regions)} (DFO as of "
                     f"{(sources.get('dfo') or {}).get('date') or '?'})" if mode == "salmon" else ""),
                  "km = distance up each water from its mouth (FWA route measure; a continuation counts on from the "
                  "water below it). Lines: the app's tile layers"
                  + (f" ({'; '.join(tile_vintage_note(lay, rel))})" if tile_vintage_note(lay, rel) else "")
                  + ". Region base (BC) lists the region table's rules that reach these waters."]
        notes = list(sysm.notes)
        if missing:
            names_missing = sorted({p.water.name for p in missing})
            notes.append("No line in the map tiles for part of: " + ", ".join(names_missing)
                         + " (drawn as a square at its location; tidal pieces are not in the app's tiles).")
        mi = R.MapInput(mode=mode, title=title, subtitle=subtitle, units=units, base_rows=base, breaks=brks,
                        geoms=geoms, points=points, context_streams=geo.streams + loose, context_lakes=geo.lakes,
                        sea=geo.sea, basin_outline=basin, footer=footer, notes=notes, names=names)
        out = Path(out_dir) / f"{slug(target.name)}-{mode}-{day.isoformat()}.png"
        R.render(mi, ext0, out, target.ord)
        outs.append(out)
    return outs


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m pipeline.tools.water_map", description=__doc__.split("\n\n")[0])
    ap.add_argument("water", help="a water's name (e.g. 'Nass River') or item id (e.g. gnis:3206)")
    ap.add_argument("--release", type=Path, default=None,
                    help="a release folder, or a release root holding LATEST (default data/release/LATEST)")
    ap.add_argument("--date", default=None, help="YYYY-MM-DD (default today, B.C. time)")
    ap.add_argument("--mode", choices=("salmon", "freshwater", "both"), default="both")
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT, help=f"output folder (default {DEFAULT_OUT})")
    ap.add_argument("--layers", type=Path, default=None,
                    help="the tile build's layers folder (default data/generated/tiles/layers)")
    ap.add_argument("--focus", default=None, help="KM-KM on the main water: only that reach and its tributaries")
    ap.add_argument("--max-tribs", type=int, default=M.MAX_TRIBS,
                    help=f"rule-carrying tributaries numbered before the rest are named in a note (default "
                         f"{M.MAX_TRIBS})")
    a = ap.parse_args(argv)
    day = date.fromisoformat(a.date) if a.date else today_bc()
    modes = ["salmon", "freshwater"] if a.mode == "both" else [a.mode]
    with Release(a.release) as rel:
        try:
            outs = build_maps(rel, a.water, day, modes, a.out, layers=a.layers, focus=_focus(a.focus),
                              max_tribs=a.max_tribs)
        except M.Ambiguous as e:
            print(e.code, file=sys.stderr)
            return 2
    for o in outs:
        print(o)
    return 0


if __name__ == "__main__":
    sys.exit(main())
