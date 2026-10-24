"""THE WATER MAP EXPORT — one chosen water, drawn part by part, coloured by the status index.

    python -m pipeline.tools.audit.water_map_export \
        --group vedder=gnis:3062,gnis:8634 --group cowichan=gnis:12227 \
        --group klwa=wbk:-22,wbk:-21,wbk:-20 \
        --date 10-10 --date 10-12 --date 05-15 --date 06-15 \
        --out app/design/regs-v11/watermap.js

For the regulations view's water map (design prototype regs-v11, ROUND9 C / D3): NOT the app's
Map tab, an overlay for moving along a water the angler has ALREADY chosen. It replaces the hand
map `app/design/regs-mock/maps.js`. Nothing here is drawn or worded by hand:

  parts      `part` / `part_section` (bundle) — each part of each item, in the export's part
             order (`waters[id].parts[i]` is bundle `part_ix` i: the counts are checked), named
             by the answers' display (`display.waters[id].parts[i]`: label, short, hint).
  order      mouth first. Within an item, by the display's `km` (the highest km of the part's
             main-stem runs); a part with no km (a side channel) goes after them. Between items,
             by the bundle's flow table (`section_down`: item B before item A when A drains into
             B); with no flow link (Kootenay Lake's three parts) the --group order stands.
  boundary   every place two parts touch (within TOUCH_M). Its label is the split the
             export's runs name there (the downstream part's `from`, else the upstream part's
             `to`): "Vedder Crossing Bridge". Where no split is named (a lake cut into parts),
             `label` is null and the two parts' names say where it is.
  tribs      the sections the group's entries bind THROUGH THE TRIBUTARY WALK (`ruleset.via =
             'trib'`): the reach builder's own answer to "its tributaries" — clipped to the map.
  status     per section and date, `status_index.Index.code` — the Python twin of the app's
             `statusOn` (packages/core/src/status.ts) — on the same file the app reads. A part's
             word is the item roll-up rule (status_index docstring) over its sections.
  colours    `color.status.{closed,own,base}` resolved in the map's themes
             (`app/packages/map/style/themes/{light,dark}.json`) and the words of
             `WATER_STATUS` (`app/packages/core/src/status.ts`) — read, never retyped.
  geometry   the tile layers' own features (`GENERATED.tiles/layers/{stream,lake}.geojsonl`, keyed
             by `section_id`, the same handle as the status index), simplified and projected.

VINTAGE: tiles, bundle, status index and export must be one build — the tool refuses a pair
whose `section_handles` / `reach_digest` disagree (`tiles-bundle-same-vintage`).

The functions above `main` take plain Python and are tested on tiny synthetic inputs
(`pipeline/tests/test_water_map_export.py`); only `main` touches `data/generated`.
"""
from __future__ import annotations

import argparse
import json
import math
import re
import sqlite3
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from pipeline.common.tolerances import TOUCH_M as _TOUCH_M

REPO = Path(__file__).resolve().parents[3]
THEMES = REPO / "app" / "packages" / "map" / "style" / "themes"
STATUS_TS = REPO / "app" / "packages" / "core" / "src" / "status.ts"
YEAR = 2026                                    # the prototype's year (Oct 10 2026 is a Saturday)

#: one letter per status code, so a section's statuses on N dates are an N-letter string
LETTER = {0: "b", 1: "o", 2: "c", 3: "t", 4: "x"}       # base own closed tidal outside
GRID = 10000                                    # projected units across the map's long side
TOL = {"part": 2.0, "trib": 6.0, "other": 12.0}  # simplification tolerance, projected units
TRIB_MIN_ORDER = 2                              # Strahler order a tributary line must reach
OTHER_MIN_ORDER = 4                             # ... and a line of other water
OTHER_MIN_LAKE_M2 = 200_000                     # a lake of other water
TOUCH_M = _TOUCH_M                              # two parts closer than this meet (the bundle's, tolerances.py)
MARGIN = 0.12                                   # the map's frame past the water, each side

Coords = List[Tuple[float, float]]


class VintageError(SystemExit):
    """Tiles, bundle, status index and export are not one build."""


# --------------------------------------------------------------------------------------------
# Status
# --------------------------------------------------------------------------------------------

def status_string(code_on, sid: int, dates: Sequence[date]) -> str:
    """The section's code on each date, one letter each — `code_on` is `Index.code`."""
    return "".join(LETTER[code_on(sid, d)] for d in dates)


def rollup(strings: Iterable[str]) -> str:
    """A part's status per date over its sections — the status index's item rule: closed when
    every section is, tidal / outside when every section is, own when any is own, else base."""
    strings = list(strings)
    if not strings:
        raise ValueError("a part with no sections")
    out = []
    for i in range(len(strings[0])):
        day = {s[i] for s in strings}
        inside = day - {"x"} or day
        if len(inside) == 1:
            out.append(next(iter(inside)))
        elif "o" in inside:
            out.append("o")
        else:
            out.append("b")
    return "".join(out)


# --------------------------------------------------------------------------------------------
# Order and boundaries
# --------------------------------------------------------------------------------------------

def order_items(items: Sequence[str], drains: Dict[str, set]) -> List[str]:
    """Mouth first. `drains[b]` = the items b's sections flow into. Items with no flow link keep
    their given order (stable)."""
    rest, out = list(items), []
    while rest:
        # the next item: one that drains into no item still waiting (it is the lowest left)
        pick = next((it for it in rest if not (drains.get(it, set()) & set(rest) - {it})), rest[0])
        out.append(pick)
        rest.remove(pick)
    return out


# THE ORDER AND THE BOUNDARY NAMES ARE THE BUNDLE'S (P09): one implementation, in pipeline/deliver/bundle/topology.py,
# which writes them into `part_geo` / `part_mark` for the app.
from pipeline.deliver.bundle.topology import order_parts, split_label  # noqa: E402,F401


# --------------------------------------------------------------------------------------------
# Geometry
# --------------------------------------------------------------------------------------------

def rings(geom: dict) -> List[Tuple[Coords, bool]]:
    """A GeoJSON geometry as [(coords, closed)]."""
    t, c = geom["type"], geom["coordinates"]
    if t == "LineString":
        return [(c, False)]
    if t == "MultiLineString":
        return [(x, False) for x in c]
    if t == "Polygon":
        return [(x, True) for x in c]
    if t == "MultiPolygon":
        return [(x, True) for poly in c for x in poly]
    return []


def bbox(geoms: Iterable[dict]) -> Tuple[float, float, float, float]:
    xs, ys = [], []
    for g in geoms:
        for ring, _ in rings(g):
            for x, y in ring:
                xs.append(x)
                ys.append(y)
    if not xs:
        raise ValueError("no geometry")
    return min(xs), min(ys), max(xs), max(ys)


class Projection:
    """Equirectangular about the frame's middle latitude, scaled so the long side is GRID units,
    y down (SVG)."""

    def __init__(self, box: Tuple[float, float, float, float], margin: float = MARGIN):
        x0, y0, x1, y1 = box
        k = math.cos(math.radians((y0 + y1) / 2))
        w, h = (x1 - x0) * k, (y1 - y0)
        pad = max(w, h) * margin
        self.k, self.x0, self.y1 = k, x0 - pad / k, y1 + pad
        span = max(w, h) + 2 * pad
        self.s = GRID / span
        self.w = round((w + 2 * pad) * self.s)
        self.h = round((h + 2 * pad) * self.s)
        # metres per unit, for a scale bar (a degree of latitude ~ 111.32 km)
        self.m_per_unit = 111_320 / self.s

    def __call__(self, lon: float, lat: float) -> Tuple[float, float]:
        return ((lon - self.x0) * self.k * self.s, (self.y1 - lat) * self.s)

    def box(self) -> Tuple[float, float, float, float]:
        """The frame as lon/lat."""
        return (self.x0, self.y1 - self.h / self.s, self.x0 + self.w / self.s / self.k, self.y1)


def simplify(pts: Coords, tol: float) -> Coords:
    """Douglas-Peucker, iterative."""
    if len(pts) < 3:
        return list(pts)
    keep = [False] * len(pts)
    keep[0] = keep[-1] = True
    stack = [(0, len(pts) - 1)]
    while stack:
        a, b = stack.pop()
        (ax, ay), (bx, by) = pts[a], pts[b]
        dx, dy = bx - ax, by - ay
        n = math.hypot(dx, dy)
        far, at = -1.0, -1
        for i in range(a + 1, b):
            px, py = pts[i]
            d = abs(dy * (px - ax) - dx * (py - ay)) / n if n else math.hypot(px - ax, py - ay)
            if d > far:
                far, at = d, i
        if far > tol:
            keep[at] = True
            stack += [(a, at), (at, b)]
    return [p for p, k in zip(pts, keep) if k]


def path(geoms: Iterable[dict], proj: Projection, tol: float) -> str:
    """SVG path data, integer units: an absolute move, then relative steps."""
    out = []
    for g in geoms:
        for ring, closed in rings(g):
            pts = simplify([proj(x, y) for x, y in ring], tol)
            q = [(round(x), round(y)) for x, y in pts]
            q = [p for i, p in enumerate(q) if i == 0 or p != q[i - 1]]
            if len(q) < 2 or (closed and len(q) < 3):
                continue
            s = f"M{q[0][0]} {q[0][1]}l"
            nums = []
            for (x0, y0), (x1, y1) in zip(q, q[1:]):
                nums += [x1 - x0, y1 - y0]
            for i, v in enumerate(nums):
                s += (str(v) if i == 0 or v < 0 else " " + str(v))
            out.append(s + ("z" if closed else ""))
    return "".join(out)


def meeting_point(a: Sequence[dict], b: Sequence[dict], proj: Projection) -> Tuple[int, int, float]:
    """Where two parts meet, and how far apart they are there (projected units): a vertex of `a`
    that lies on `b` (a shared end, or the shared edge of two lake parts — the one nearest the
    middle of them), distance 0; with none shared, the middle of the closest pair."""
    va = {(round(x, 1), round(y, 1)) for g in a for r, _ in rings(g) for x, y in (proj(*p) for p in r)}
    vb = {(round(x, 1), round(y, 1)) for g in b for r, _ in rings(g) for x, y in (proj(*p) for p in r)}
    shared = va & vb
    if not shared:
        cell = defaultdict(list)
        for p in vb:
            cell[(int(p[0] // 50), int(p[1] // 50))].append(p)
        best, pair = float("inf"), None
        for p in va:
            cx, cy = int(p[0] // 50), int(p[1] // 50)
            for dx in (-1, 0, 1):
                for dy in (-1, 0, 1):
                    for q in cell.get((cx + dx, cy + dy), ()):
                        d = (p[0] - q[0]) ** 2 + (p[1] - q[1]) ** 2
                        if d < best:
                            best, pair = d, (p, q)
        if pair is None:          # nothing within 50 units: brute force (small inputs only)
            pair = min(((p, q) for p in va for q in vb),
                       key=lambda pq: (pq[0][0] - pq[1][0]) ** 2 + (pq[0][1] - pq[1][1]) ** 2)
        (x0, y0), (x1, y1) = pair
        return round((x0 + x1) / 2), round((y0 + y1) / 2), math.hypot(x1 - x0, y1 - y0)
    pts = sorted(shared)
    # the shared vertex nearest the middle of them (a point ON the boundary, not off a curve)
    mx, my = sum(p[0] for p in pts) / len(pts), sum(p[1] for p in pts) / len(pts)
    x, y = min(pts, key=lambda p: (p[0] - mx) ** 2 + (p[1] - my) ** 2)
    return round(x), round(y), 0.0


# --------------------------------------------------------------------------------------------
# The app's words and colours
# --------------------------------------------------------------------------------------------

def status_words(ts: str) -> Dict[str, dict]:
    """`WATER_STATUS` as written in packages/core/src/status.ts: {status: {label, token}}."""
    got = {k: {"label": lab, "token": tok} for k, lab, tok in re.findall(
        r'(closed|own|base):\s*\{\s*label:\s*"([^"]+)",\s*token:\s*"([^"]+)"\s*\}', ts)}
    if set(got) != {"closed", "own", "base"}:
        raise ValueError("status.ts: WATER_STATUS not found (closed / own / base)")
    return got


def theme_colours(words: Dict[str, dict], themes: Dict[str, dict]) -> Dict[str, Dict[str, str]]:
    """Each status's token resolved in each theme, plus the plain map's water and lake for
    the faint context."""
    out = {}
    for name, t in themes.items():
        v = t["values"]
        out[name] = {s: v[w["token"]] for s, w in words.items()}
        out[name]["water"] = v["color.water.mapped"]
        out[name]["lake"] = v["color.lake.fill"]
    return out


# --------------------------------------------------------------------------------------------
# The model
# --------------------------------------------------------------------------------------------

def build_group(gid: str, items: Sequence[str], *, export_waters: Dict[str, dict],
                display_waters: Dict[str, dict], splits: Dict[str, dict],
                part_sids: Dict[Tuple[str, int], List[int]], drains: Dict[str, set],
                trib_sids: Iterable[int], geom: Dict[int, dict], order_of: Dict[int, int],
                code_on, dates: Sequence[date], other: Sequence[dict] = (),
                proj: Optional[Projection] = None) -> dict:
    """One water's map. `part_sids[(item, part_ix)]` its sections; `geom[sid]` a section's
    GeoJSON geometry (lon/lat); `order_of[sid]` its Strahler order (lines; absent for areas);
    `code_on(sid, date)` the status index; `other` faint context geometries."""
    parts = []
    for item in order_items(items, drains):
        w, d = export_waters[item], display_waters[item]
        if len(w["parts"]) != len(d["parts"]):
            raise ValueError(f"{item}: the export has {len(w['parts'])} parts, the display "
                             f"{len(d['parts'])}")
        same = {}
        for ch in (d.get("picker") or {}).get("choices") or []:
            for v in ch["parts"]:
                same[v] = ch["parts"][0]
        mine = []
        for v, (p, dp) in enumerate(zip(w["parts"], d["parts"])):
            sids = sorted(part_sids.get((item, v), []))
            if len(sids) != p["sections"]:
                raise ValueError(f"{item} part {v}: the bundle has {len(sids)} sections, the "
                                 f"export {p['sections']}")
            mine.append({"item": item, "name": w["name"], "kind": w.get("kind"), "v": v,
                         "picker": same.get(v, v), "label": dp.get("label"), "short": dp.get("short"),
                         "hint": dp.get("hint"), "km": dp.get("km"), "runs": p.get("runs") or [],
                         "sids": sids})
        parts += order_parts(mine)

    group_sids = {s for p in parts for s in p["sids"]}
    if proj is None:
        proj = Projection(bbox(geom[s] for s in group_sids if s in geom))
    fx0, fy0, fx1, fy1 = proj.box()

    def inside(g):
        x0, y0, x1, y1 = bbox([g])
        return x1 >= fx0 and x0 <= fx1 and y1 >= fy0 and y0 <= fy1

    out_parts = []
    for p in parts:
        by_status = defaultdict(list)
        for s in p["sids"]:
            if s in geom:
                by_status[status_string(code_on, s, dates)].append(s)
        segs = [{"s": st, "d": path([geom[s] for s in ss], proj, TOL["part"])}
                for st, ss in sorted(by_status.items())]
        xs = bbox([geom[s] for s in p["sids"] if s in geom])
        a, b = proj(xs[0], xs[3]), proj(xs[2], xs[1])
        out_parts.append({k: p[k] for k in ("item", "name", "kind", "v", "picker", "label", "short",
                                            "hint", "km")}
                         | {"s": rollup(status_string(code_on, s, dates) for s in p["sids"]),
                            "segs": segs, "box": [round(a[0]), round(a[1]), round(b[0]), round(b[1])],
                            "area": all(rings(geom[s])[0][1] for s in p["sids"] if s in geom)})

    # a boundary wherever two parts touch (within TOUCH_M): one part ending where the next
    # begins, a side channel leaving the main stem, two lake parts sharing an edge
    marks, touch = [], TOUCH_M / proj.m_per_unit
    for i in range(len(parts)):
        for j in range(i + 1, len(parts)):
            down, up = parts[i], parts[j]
            x, y, gap = meeting_point([geom[s] for s in down["sids"] if s in geom],
                                      [geom[s] for s in up["sids"] if s in geom], proj)
            if gap <= touch:
                marks.append({"x": x, "y": y, "label": split_label(down, up, splits),
                              "down": i, "up": j})
    tribs = defaultdict(list)
    for s in sorted(set(trib_sids) - group_sids):
        g = geom.get(s)
        if g is None or not inside(g):
            continue
        if s in order_of and order_of[s] < TRIB_MIN_ORDER:
            continue
        tribs[status_string(code_on, s, dates)].append(g)
    return {
        "id": gid,
        "view": {"w": proj.w, "h": proj.h, "m_per_unit": round(proj.m_per_unit, 3)},
        "parts": out_parts,
        "marks": marks,
        "tribs": [{"s": st, "d": path(gs, proj, TOL["trib"])} for st, gs in sorted(tribs.items())],
        "other": {"lines": path([g for g in other if rings(g) and not rings(g)[0][1] and inside(g)],
                                proj, TOL["other"]),
                  "areas": path([g for g in other if rings(g) and rings(g)[0][1] and inside(g)],
                                proj, TOL["other"])},
    }


# --------------------------------------------------------------------------------------------
# Braid review (ROUND11 owner 6): the braids the round-10 placement marked, to be SEEN
# --------------------------------------------------------------------------------------------

BRAID_MIN_M = 1500                              # a braid map frames at least this much ground
BRAID_MARGIN = 0.4                              # ... and this much past the braid and its cut


def stretch_side(extent: dict, km: Dict[str, float], cut: str) -> Optional[str]:
    """Which side of `cut` (a split of `extent`) the rule's stretch lies on, km measured from the
    mouth: 'above' (upstream), 'below', or None when the extent does not say (whole, within, ...)."""
    op, sp = extent.get("op"), list(extent.get("splits") or [])
    if op == "upstream_of":
        return "above"
    if op == "downstream_of":
        return "below"
    if op == "between" and cut in sp and len(sp) == 2:
        other = sp[1] if sp[0] == cut else sp[0]
        if cut in km and other in km and km[cut] != km[other]:
            return "above" if km[other] > km[cut] else "below"
    return None


def placed_words(side: str, stretch: Optional[str]) -> Optional[str]:
    """The side of the cut a braid was COUNTED on: the stretch's side when it was placed in the
    rule, the other when out; None when the stretch's side is unknown."""
    if stretch is None:
        return None
    return stretch if side == "in" else {"above": "below", "below": "above"}[stretch]


def _ends(g: dict) -> List[Tuple[float, float]]:
    return [p for ring, closed in rings(g) if not closed for p in (tuple(ring[0]), tuple(ring[-1]))]


def cut_point(below: dict, above: dict) -> Tuple[float, float]:
    """Where a cut is: the closest pair of line ends of the main-stem section ending at it and the
    one starting at it (lon/lat), their middle — the shared end when they share one."""
    a, b = _ends(below), _ends(above)
    (x0, y0), (x1, y1) = min(((p, q) for p in a for q in b),
                             key=lambda pq: (pq[0][0] - pq[1][0]) ** 2 + (pq[0][1] - pq[1][1]) ** 2)
    return ((x0 + x1) / 2, (y0 + y1) / 2)


def nearest_cut(braid: Sequence[dict], cuts: Sequence[Tuple[str, float, float]]) -> int:
    """The index of the cut nearest the braid (its vertices; x scaled by cos latitude)."""
    pts = [p for g in braid for ring, _ in rings(g) for p in ring]
    k = math.cos(math.radians(sum(p[1] for p in pts) / len(pts)))
    return min(range(len(cuts)), key=lambda i: min(((x - cuts[i][1]) * k) ** 2 + (y - cuts[i][2]) ** 2
                                                   for x, y in pts))


def crossed_cuts(braid: Sequence[dict], cuts: Sequence[Tuple[str, float, float]],
                 reach_m: float = 100) -> List[int]:
    """The cuts a braid runs across: those within `reach_m` of its frame (its bbox, grown)."""
    x0, y0, x1, y1 = bbox(braid)
    gy = reach_m / 111_320
    gx = gy / math.cos(math.radians((y0 + y1) / 2))
    return [i for i, (_, x, y) in enumerate(cuts) if x0 - gx <= x <= x1 + gx and y0 - gy <= y <= y1 + gy]


def build_braid(*, water: str, kind: Optional[str], braid: Sequence[int], lines: Iterable[int],
                cuts: Sequence[dict], rows: Sequence[dict], geom: Dict[int, dict], code_on,
                dates: Sequence[date], other: Sequence[dict] = ()) -> dict:
    """One marked braid's review map. `braid` its sections (one channel); `lines` its water's
    sections (drawn where they fall in the frame, by status); `cuts` [{split, name, at: (lon, lat)}];
    `rows` one per (entry, rule) the braid was placed for: {entry_id, rule_id, label, side, share,
    how, cut (split id or None), stretch ('above' | 'below' | None), sids (the rule's sections)}.
    Section ids never leave: only paths, statuses and words do."""
    bg = [geom[s] for s in braid if s in geom]
    x0, y0, x1, y1 = bbox(bg + [{"type": "LineString", "coordinates": [list(c["at"]), list(c["at"])]} for c in cuts])
    k = math.cos(math.radians((y0 + y1) / 2))
    need = BRAID_MIN_M / 111_320
    if (y1 - y0) < need:
        y0, y1 = (y0 + y1) / 2 - need / 2, (y0 + y1) / 2 + need / 2
    if (x1 - x0) * k < need:
        x0, x1 = (x0 + x1) / 2 - need / k / 2, (x0 + x1) / 2 + need / k / 2
    proj = Projection((x0, y0, x1, y1), BRAID_MARGIN)
    fx0, fy0, fx1, fy1 = proj.box()

    def inside(g):
        a0, b0, a1, b1 = bbox([g])
        return a1 >= fx0 and a0 <= fx1 and b1 >= fy0 and b0 <= fy1

    tol = 1.0
    by_status = defaultdict(list)
    for s in sorted(set(lines) - set(braid)):
        if s in geom and inside(geom[s]):
            by_status[status_string(code_on, s, dates)].append(geom[s])
    bx = bbox(bg)
    a, b = proj(bx[0], bx[3]), proj(bx[2], bx[1])
    out_cuts, at = [], {}
    for c in cuts:
        x, y = proj(*c["at"])
        at[c["split"]] = len(out_cuts)
        out_cuts.append({"x": round(x), "y": round(y), "name": c["name"]})
    out_rows = []
    for r in rows:
        stretch = [geom[s] for s in r.get("sids") or () if s in geom and inside(geom[s])]
        out_rows.append({"entry": r["entry_id"], "rule_id": r["rule_id"], "rule": r["label"],
                         "side": r["side"], "share": r["share"], "how": r["how"],
                         "cut": at.get(r.get("cut")), "words": placed_words(r["side"], r.get("stretch")),
                         "stretch": path(stretch, proj, tol)})
    return {"water": water, "kind": kind,
            "view": {"w": proj.w, "h": proj.h, "m_per_unit": round(proj.m_per_unit, 3)},
            "box": [round(a[0]), round(a[1]), round(b[0]), round(b[1])],
            "lines": [{"s": st, "d": path(gs, proj, tol)} for st, gs in sorted(by_status.items())],
            "braid": {"s": rollup(status_string(code_on, s, dates) for s in braid),
                      "d": path(bg, proj, tol)},
            "cuts": out_cuts, "rows": out_rows,
            "other": {"lines": path([g for g in other if rings(g) and not rings(g)[0][1] and inside(g)], proj, TOL["other"] / 4),
                      "areas": path([g for g in other if rings(g) and rings(g)[0][1] and inside(g)], proj, TOL["other"] / 4)}}


# --------------------------------------------------------------------------------------------
# Reading the build (main only)
# --------------------------------------------------------------------------------------------

_SID = re.compile(r'"section_id":\s*(\d+)')
_ORD = re.compile(r'"ord":\s*(\d+)')
_AREA = re.compile(r'"area_m2":\s*(\d+)')
_FIRST = re.compile(r'"coordinates":\[+(-?\d+(?:\.\d+)?),(-?\d+(?:\.\d+)?)')


def scan_layers(files: Sequence[Path], want: set, log=print):
    """One pass over the tile layers: the geometry of every wanted section, and the first
    coordinate + byte offset of every line of Strahler order >= OTHER_MIN_ORDER and every lake of
    OTHER_MIN_LAKE_M2 or more, for the frame pass (`read_other`)."""
    geom, order_of, index = {}, {}, []
    for f in files:
        with open(f, "rb") as fh:
            off = 0
            for raw in fh:
                tail = raw[-200:].decode("utf-8", "replace")
                m = _SID.search(tail)
                if m:
                    sid = int(m.group(1))
                    o = _ORD.search(tail)
                    if sid in want:
                        g = json.loads(raw)["geometry"]
                        geom[sid] = g
                        if o:
                            order_of[sid] = int(o.group(1))
                    a = _AREA.search(tail)
                    big = (o and int(o.group(1)) >= OTHER_MIN_ORDER) or \
                        (a and int(a.group(1)) >= OTHER_MIN_LAKE_M2)
                    fm = big and _FIRST.search(raw[:160].decode("utf-8", "replace"))
                    if fm:
                        index.append((f, off, float(fm.group(1)), float(fm.group(2))))
                off += len(raw)
        log(f"  read {f.name}: {len(geom):,} wanted sections so far")
    return geom, order_of, index


def read_other(index, box, skip: set) -> List[dict]:
    """Faint context: the bigger lines and lakes (`scan_layers` indexed only those) whose first
    point falls in the frame."""
    x0, y0, x1, y1 = box
    out, fh, cur = [], None, None
    for f, off, x, y in index:
        if not (x0 <= x <= x1 and y0 <= y <= y1):
            continue
        if cur != f:
            fh and fh.close()
            fh, cur = open(f, "rb"), f
        fh.seek(off)
        feat = json.loads(fh.readline())
        if feat["properties"].get("section_id") in skip:
            continue
        out.append(feat["geometry"])
    fh and fh.close()
    return out


def _grow(box, by: float = 0.03):
    """A lon/lat box grown on every side (the context read around a small braid frame)."""
    x0, y0, x1, y1 = box
    return (x0 - by, y0 - by, x1 + by, y1 + by)


def read_braids(db, meta: dict, reach_dir: Path) -> List[dict]:
    """Every braid section the bundle marks (`section_straddle`, read through
    `pipeline.deliver.bundle.read.straddles`), grouped one map per section, with what the review map
    needs: its water, its water's sections, and per (entry, rule): the rule's words, the placed side
    and share, the candidate cuts (each split of the rule's extents, as the main-stem sections ending
    and starting at it) and the extents themselves. A rule's extents are the reach run's
    (`rule_extent.jsonl` of the run the bundle was built from — the digests must agree); the row's own
    scope (rule_id '') is the entry's `extents`."""
    from pipeline.deliver.bundle import read as bundle_read

    report = json.loads((reach_dir / "report.json").read_text())
    if report.get("digest") != meta["reach_digest"]:
        raise VintageError(f"water_map_export: reach run {reach_dir} is {report.get('digest')}, the bundle "
                           f"{meta['reach_digest']}")
    rule_ext = defaultdict(list)
    with open(reach_dir / "rule_extent.jsonl") as fh:
        for line in fh:
            d = json.loads(line)
            rule_ext[(d["entry_id"], d["rule_id"])].append(d)
    splits = {sid: (name, json.loads(at)) for sid, name, at in db.execute("SELECT split_id, name, at FROM split")}
    out = []
    for (sid,) in db.execute("SELECT DISTINCT sid FROM section_straddle ORDER BY sid").fetchall():
        marks = bundle_read.straddles(db, sid)
        item_id, water, kind, ord_ = db.execute(
            "SELECT i.item_id, i.name, i.kind, i.ord FROM item_section x JOIN item i ON i.ord = x.ord "
            "WHERE x.sid = ? ORDER BY i.ord LIMIT 1", (sid,)).fetchone()
        lines = [s for (s,) in db.execute("SELECT sid FROM item_section WHERE ord = ?", (ord_,))]
        stem = {s: (lo, hi) for s, lo, hi in db.execute(
            f"SELECT sid, lo_m, hi_m FROM section_span WHERE off_stem = 0 AND lo_m IS NOT NULL AND sid IN "
            f"({','.join('?' * len(lines))})", lines)}
        rows, cut_sids = [], []
        for m in marks:
            if m["how"] != "projected":
                continue
            if m["rule_id"]:
                label = db.execute("SELECT label FROM rule WHERE entry_id = ? AND rule_id = ?",
                                   (m["entry_id"], m["rule_id"])).fetchone()[0]
                extents = rule_ext.get((m["entry_id"], m["rule_id"]), [])
                sets = "r.entry_id = ? AND r.rule_id = ?", (m["entry_id"], m["rule_id"])
            else:
                label, ex = db.execute("SELECT full_name, extents FROM entry WHERE entry_id = ?",
                                       (m["entry_id"],)).fetchone()
                extents = json.loads(ex or "[]")
                sets = "r.entry_id = ?", (m["entry_id"],)
            sids = sorted({s for (s,) in db.execute(
                f"SELECT DISTINCT s.sid FROM ruleset r JOIN section_ruleset s ON s.set_id = r.set_id "
                f"WHERE r.via = 'reach' AND {sets[0]}", sets[1])})
            km, cands = {}, []
            for sp in sorted({x for e in extents for x in e.get("splits") or []}):
                name, at = splits.get(sp, (None, []))
                k = next((v for it, v in at if it == item_id), None)
                if k is None:
                    continue
                km[sp] = k
                lo = [s for s, (a, b) in stem.items() if abs(b - k * 1000) <= 60]
                hi = [s for s, (a, b) in stem.items() if abs(a - k * 1000) <= 60]
                if lo and hi:
                    cands.append((sp, name, lo[0], hi[0]))
                    cut_sids.append([lo[0], hi[0]])
            rows.append({"entry_id": m["entry_id"], "rule_id": m["rule_id"], "label": label,
                         "side": m["side"], "share": m["share"], "how": m["how"], "sids": sids,
                         "extents": extents, "km": km, "cands": cands})
        if rows:
            out.append({"water": water, "kind": kind, "braid": [sid], "lines": lines, "rows": rows,
                        "cut_sids": cut_sids})
    return out


def main(argv: Optional[Sequence[str]] = None) -> int:
    from pipeline.common.curated import GENERATED
    from pipeline.deliver.status_index import Index
    from pipeline.tools import export_codec
    from pipeline.tools.export_ui_rules import GUIDE_NAME, OUT as EXPORT

    ap = argparse.ArgumentParser(prog="pipeline.tools.audit.water_map_export",
                                 description=__doc__.split("\n\n")[0])
    ap.add_argument("--group", action="append", required=True,
                    help="name=item_id,item_id,... (mouth first where the flow table cannot tell)")
    ap.add_argument("--date", action="append", required=True, help="MM-DD (year %d)" % YEAR)
    ap.add_argument("--bundle", type=Path, default=GENERATED.bundle / "bundle.sqlite")
    ap.add_argument("--index", type=Path, default=None, help="default: status_index.bin beside it")
    ap.add_argument("--export", type=Path, default=EXPORT)
    ap.add_argument("--tiles", type=Path, default=GENERATED.tiles)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--braids", action="store_true",
                    help="also every braid the reach builder placed across a cut (bundle "
                         "section_straddle, RULINGS E15): one review map each")
    a = ap.parse_args(argv)

    dates = [date(YEAR, *map(int, d.split("-"))) for d in a.date]
    groups = [(g.split("=", 1)[0], g.split("=", 1)[1].split(",")) for g in a.group]
    items = [i for _, its in groups for i in its]

    # --- one build: tiles, bundle, status index, export ---
    db = sqlite3.connect(f"file:{a.bundle}?mode=ro", uri=True)
    meta = dict(db.execute("SELECT k, v FROM meta"))
    idx = Index((a.index or a.bundle.with_name("status_index.bin")).read_bytes())
    tiles = json.loads((a.tiles / "atlas.meta.json").read_text())
    data = json.loads(a.export.read_text())
    doc = export_codec.expand(data, json.loads(a.export.with_name(GUIDE_NAME).read_text()))
    answers = json.loads(a.export.with_name("ui-rules-answers.json").read_text())
    got = {"tiles": tiles["section_handles"], "index": idx.handles, "bundle": meta["section_handles"],
           "export": doc["about"]["bundle"]["section_handles"],
           "answers": answers["about"]["bundle"]["section_handles"]}
    reach = {"index": idx.reach_digest, "bundle": meta["reach_digest"],
             "export": doc["about"]["bundle"]["reach_digest"],
             "answers": answers["about"]["bundle"]["reach_digest"]}
    if len(set(got.values())) != 1 or len(set(reach.values())) != 1:
        raise VintageError(f"water_map_export: not one build — section_handles {got}, reach {reach}")

    ords = dict(db.execute(f"SELECT item_id, ord FROM item WHERE item_id IN "
                           f"({','.join('?' * len(items))})", items))
    missing = [i for i in items if i not in ords]
    if missing:
        raise SystemExit(f"water_map_export: no such item {missing}")
    by_ord = {o: i for i, o in ords.items()}
    part_sids = defaultdict(list)
    for o, sid, px in db.execute(f"SELECT ord, sid, part_ix FROM part_section WHERE ord IN "
                                 f"({','.join('?' * len(ords))})", list(ords.values())):
        part_sids[(by_ord[o], px)].append(sid)
    sid_item = {s: it for (it, _), ss in part_sids.items() for s in ss}
    drains = defaultdict(set)
    for (it, _), ss in part_sids.items():
        for s in ss:
            row = db.execute("SELECT down_sid FROM section_down WHERE sid = ?", (s,)).fetchone()
            if row and sid_item.get(row[0], it) != it:
                drains[it].add(sid_item[row[0]])
    trib = {}
    for gid, its in groups:
        entries = sorted({e for it in its for e in doc["waters"][it].get("entries") or []})
        trib[gid] = {s for (s,) in db.execute(
            f"SELECT DISTINCT s.sid FROM ruleset r JOIN section_ruleset s ON s.set_id = r.set_id "
            f"WHERE r.via = 'trib' AND r.entry_id IN ({','.join('?' * len(entries))})", entries)}
    braids = read_braids(db, meta, GENERATED.reaches / meta["reach_run"]) if a.braids else []
    db.close()

    want = set(sid_item) | set().union(*trib.values()) | \
        {s for b in braids for s in b["braid"] + b["lines"] + [x for r in b["rows"] for x in r["sids"]]} | \
        {s for b in braids for c in b["cut_sids"] for s in c}
    files = [a.tiles / "layers" / "stream.geojsonl", a.tiles / "layers" / "lake.geojsonl"]
    print(f"water map: {len(groups)} waters, {len(sid_item)} sections, {len(want) - len(sid_item):,} "
          f"tributary sections; reading {', '.join(f.name for f in files)}")
    geom, order_of, index = scan_layers(files, want)

    words = status_words(STATUS_TS.read_text())
    themes = {n: json.loads((THEMES / f"{n}.json").read_text()) for n in ("light", "dark")}
    waters = []
    for gid, its in groups:
        sids = [s for it in its for (i2, _), ss in part_sids.items() if i2 == it for s in ss]
        proj = Projection(bbox(geom[s] for s in sids if s in geom))
        other = read_other(index, proj.box(), want)
        waters.append(build_group(
            gid, its, export_waters=doc["waters"],
            display_waters=answers["sections"]["display"]["waters"], splits=doc["splits"],
            part_sids=part_sids, drains=drains, trib_sids=trib[gid], geom=geom, order_of=order_of,
            code_on=idx.code, dates=dates, other=other, proj=proj))
        print(f"  {gid}: {len(waters[-1]['parts'])} parts, {len(waters[-1]['marks'])} boundaries, "
              f"{len(other):,} context features")

    braid_maps = []
    for b in braids:
        cuts, rows = [], []
        for r in b["rows"]:
            cand = [(sp, nm, cut_point(geom[lo], geom[hi])) for sp, nm, lo, hi in r["cands"]
                    if lo in geom and hi in geom]
            bg = [geom[s] for s in b["braid"] if s in geom]
            crossed = crossed_cuts(bg, [(sp, x, y) for sp, _, (x, y) in cand])
            # one cut the braid runs across: that one; several (a braid over both ends of a short
            # stretch): no single cut and no above / below; none within reach: the nearest
            pick = cand[crossed[0]] if len(crossed) == 1 else None if crossed else \
                cand[nearest_cut(bg, [(sp, x, y) for sp, _, (x, y) in cand])] if cand else None
            for c in ([cand[i] for i in crossed] if len(crossed) > 1 else [pick] if pick else []):
                if c[0] not in {x["split"] for x in cuts}:
                    cuts.append({"split": c[0], "name": c[1], "at": c[2]})
            ext = next((e for e in r["extents"] if pick and pick[0] in (e.get("splits") or [])), {})
            rows.append(r | {"cut": pick[0] if pick else None,
                             "stretch": stretch_side(ext, r["km"], pick[0]) if pick else None})
        frame = Projection(bbox([geom[s] for s in b["braid"] if s in geom]))
        braid_maps.append(build_braid(water=b["water"], kind=b["kind"], braid=b["braid"], lines=b["lines"],
                                      cuts=cuts, rows=rows, geom=geom, code_on=idx.code, dates=dates,
                                      other=read_other(index, _grow(frame.box()), want)))
        print(f"  braid {b['water']}: {len(rows)} rules, cuts {[c['name'] for c in cuts]}")

    out = {"about": {"generated_by": "python -m pipeline.tools.audit.water_map_export",
                     "section_handles": got["bundle"], "reach_digest": reach["bundle"],
                     "registry": tiles.get("registry"),
                     "dates": [f"{d.month}{d.day:02d}" for d in dates], "year": YEAR,
                     "codes": {v: k for k, v in {"base": "b", "own": "o", "closed": "c", "tidal": "t",
                                                  "outside": "x"}.items()}},
           "words": {k: v["label"] for k, v in words.items()},
           "colours": theme_colours(words, themes),
           "waters": {w["id"]: w for w in waters},
           "braids": braid_maps}
    head = ("/* The water map's data: written by `python -m pipeline.tools.audit.water_map_export` from the "
            "bundle, its status index,\n   the UI export + answers and the tile layers of ONE build "
            f"(section_handles {got['bundle']}, reach {reach['bundle']}). Not hand-edited. */\n")
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(head + "window.WATERMAP = " + json.dumps(out, separators=(",", ":"),
                                                             ensure_ascii=False) + ";\n")
    print(f"  wrote {a.out} ({a.out.stat().st_size:,} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
