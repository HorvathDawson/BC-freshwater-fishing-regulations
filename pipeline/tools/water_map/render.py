"""Draw one mode's map (T853): the system coloured by today's answer, numbered callouts, named breaks with km, a zoom
inset where parts are too small, the cascade panels, legend, scale, north arrow and lat/lon ticks. Every word drawn is
handed in (model.py); this file only lays it out.
"""
from __future__ import annotations

import math
import textwrap
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyBboxPatch, Polygon as MPoly, Rectangle  # noqa: E402
from shapely.geometry import LineString, MultiLineString, MultiPolygon, Point, Polygon, box  # noqa: E402
from shapely.ops import unary_union  # noqa: E402

from pipeline.tools.water_map.layout import Placer  # noqa: E402
from pipeline.tools.water_map.model import Break, Unit  # noqa: E402

DPI = 100
FIG_W = 2600
INSET_H = 520
plt.rcParams["font.family"] = ["Arial", "DejaVu Sans"]

C = dict(
    land="#F4F2EC", sea="#D5E0E8", sea_edge="#B7C8D4", lake="#DCE6EE", lake_edge="#A9BCCB", ctx="#BFCBD6",
    basin="#8A8F96", ink="#22262B", sub="#59606A", panel_edge="#D8DCE1", base_fill="#F1F3F5",
    open="#00897B", release="#D99A00", closed="#9AA3AD", closed_all="#3B4048", tidal="#5866C9",
    check="#8E5BB5", none="#C9CED4", bait="#FFFFFF", fly="#C2185B",
)
STATE_WORD = {
    "salmon": {"open": "SALMON OPEN", "release": "SALMON CATCH AND RELEASE", "closed": "SALMON CLOSED",
               "closed_all": "CLOSED TO ALL FISHING", "tidal": "TIDAL", "check": "DFO SEASON UNREADABLE",
               "none": "NO RULE SET"},
    "freshwater": {"open": "OPEN", "release": "OPEN · RELEASE ALL GAME FISH", "closed": "CLOSED",
                   "closed_all": "CLOSED TO ALL FISHING", "tidal": "TIDAL", "check": "?", "none": "NO RULE SET"},
}
LEGEND = {
    "salmon": [("open", "Salmon open today (a species may be kept)"), ("release", "Salmon catch and release only"),
               ("closed", "Salmon closed today (other fishing open)"), ("closed_all", "Closed to ALL fishing today"),
               ("check", "DFO printed a season nobody can read"), ("tidal", "Tidal water (federal tidal rules)")],
    "freshwater": [("open", "Open today (B.C. rules)"), ("release", "Open, every game fish released"),
                   ("closed_all", "Closed to ALL fishing today"), ("tidal", "Tidal water (B.C. rules do not apply)"),
                   ("none", "No rule set in the release")],
}
TAG_COLOR = {"BASE": "#6B7280", "BC": "#2F5D8A", "GEAR": "#2F5D8A", "DFO": "#8A5A2F", "TODAY": C["ink"]}


@dataclass
class MapInput:
    mode: str
    title: str
    subtitle: str
    units: List[Unit]
    base_rows: List[Tuple[str, str]]
    breaks: List[Break]
    geoms: Dict[int, list]                         # unit number -> shapely geometries
    points: Dict[int, Tuple[float, float]]         # unit number -> a location where no line is drawn
    context_streams: list
    context_lakes: list
    sea: list
    basin_outline: Optional[object]
    footer: List[str]
    notes: List[str] = field(default_factory=list)
    names: Dict[int, str] = field(default_factory=dict)   # unit number -> water name label (one per water)


# ------------------------------------------------------------------------------------------------------------ helpers
def px(pt: float) -> float:
    return pt * DPI / 72.0


def wrap(text: str, width_px: float, fs: float, bold: bool = False) -> List[str]:
    chars = max(10, int(width_px / (px(fs) * (0.56 if bold else 0.52))))
    return textwrap.wrap(text, width=chars, break_long_words=True, break_on_hyphens=False) or [""]


def color(state: str) -> str:
    return C.get(state, C["closed"])


def lines_of(g) -> List[LineString]:
    if g is None or g.is_empty:
        return []
    if isinstance(g, LineString):
        return [g]
    if isinstance(g, MultiLineString):
        return list(g.geoms)
    if isinstance(g, (Polygon, MultiPolygon)):
        return []
    return [x for part in getattr(g, "geoms", []) for x in lines_of(part)]


def polys_of(g) -> List[Polygon]:
    if g is None or g.is_empty:
        return []
    if isinstance(g, Polygon):
        return [g]
    if isinstance(g, MultiPolygon):
        return [p for p in g.geoms if not p.is_empty]
    return [x for part in getattr(g, "geoms", []) for x in polys_of(part)] if hasattr(g, "geoms") else []


def nice_step(span: float, target: int = 5) -> float:
    raw = span / target
    mag = 10 ** math.floor(math.log10(raw))
    for m in (1, 2, 2.5, 5, 10):
        if raw <= m * mag:
            return m * mag
    return 10 * mag


def fit_extent(b: Tuple[float, float, float, float], w_px: float, h_px: float) -> Tuple[float, float, float, float]:
    """Grow box (w, s, e, n) to the w_px x h_px frame's shape at its latitude (lon squeezed by cos lat)."""
    w, s, e, n = b
    lat0 = (s + n) / 2
    k = math.cos(math.radians(lat0))
    dx, dy = (e - w) * k, n - s
    if dx / max(dy, 1e-9) > w_px / h_px:
        need = dx * h_px / w_px
        s, n = lat0 - need / 2, lat0 + need / 2
    else:
        need = dy * w_px / h_px / k
        cx = (w + e) / 2
        w, e = cx - need / 2, cx + need / 2
    return w, s, e, n


def natural_h(b, w_px: float) -> float:
    w, s, e, n = b
    k = math.cos(math.radians((s + n) / 2))
    return w_px * (n - s) / max((e - w) * k, 1e-9)


# ------------------------------------------------------------------------------------------------------------ panels
PANEL_FS = 10.2
TAG_W = 58


def panel_lines(u: Unit, width: float) -> List[Tuple[str, List[str]]]:
    tw = width - 22 - TAG_W - 14
    return [(tag, wrap(txt, tw, PANEL_FS, bold=(tag == "TODAY"))) for tag, txt in u.rows]


def panel_height(u: Unit, width: float) -> float:
    lh = px(PANEL_FS) * 1.32
    head = 26 + 20 + (len(wrap(u.where, width - 70, PANEL_FS * 0.86)) * px(PANEL_FS * 0.86) * 1.3 if u.where else 0)
    n = sum(len(ls) for _, ls in panel_lines(u, width))
    return 14 + head + 8 + n * lh + 12


def draw_panel(fax, x: float, ytop: float, w: float, u: Unit, mode: str) -> float:
    h = panel_height(u, w)
    col = color(u.today.state)
    fax.add_patch(FancyBboxPatch((x, ytop - h), w, h, boxstyle="round,pad=0,rounding_size=8", fc="white",
                                 ec=C["panel_edge"], lw=1.0, zorder=2))
    fax.add_patch(Rectangle((x, ytop - h), 7, h, fc=col, ec="none", zorder=3))
    fs = PANEL_FS
    cy = ytop - 14 - px(fs) * 0.6
    fax.text(x + 30, cy, str(u.number), fontsize=fs, fontweight="bold", color="white", ha="center", va="center",
             zorder=4, bbox=dict(boxstyle="circle,pad=0.28", fc=col, ec="white", lw=1.2))
    title = u.title
    tl = wrap(title, w - 70, fs * 1.05, bold=True)
    fax.text(x + 52, cy, tl[0] + (" …" if len(tl) > 1 else ""), fontsize=fs * 1.05, fontweight="bold",
             color=C["ink"], va="center", zorder=4)
    chip = STATE_WORD[mode].get(u.today.state, u.today.state.upper())
    if u.today.marks:
        chip += "  ·  " + " · ".join(m.replace("_", " ").upper() for m in u.today.marks)
    if u.span:
        chip += f"  ·  {u.span}"
    yy = cy - 20
    fax.text(x + 52, yy, chip, fontsize=fs * 0.84, fontweight="bold", va="center", zorder=4,
             color=col if u.today.state not in ("closed", "none") else "#5F6873")
    if u.where:
        for ln in wrap(u.where, w - 70, fs * 0.86):
            yy -= px(fs * 0.86) * 1.3
            fax.text(x + 52, yy, ln, fontsize=fs * 0.86, style="italic", color=C["sub"], va="center", zorder=4)
    lh = px(fs) * 1.32
    ty = yy - 8 - lh * 0.4
    for tag, lines in panel_lines(u, w):
        fax.text(x + 18, ty - lh * 0.5, tag, fontsize=fs * 0.74, fontweight="bold", color=TAG_COLOR.get(tag, C["sub"]),
                 va="center", zorder=4)
        for i, ln in enumerate(lines):
            fax.text(x + 18 + TAG_W, ty - lh * (i + 0.5), ln, fontsize=fs, color=C["ink"], va="center",
                     fontweight="bold" if tag == "TODAY" else "normal", zorder=4)
        ty -= lh * len(lines)
    return h


def base_height(rows, width) -> float:
    lh = px(PANEL_FS * 0.95) * 1.3
    n = sum(len(wrap(t, width - 40 - 110, PANEL_FS * 0.95)) for _, t in rows)
    return 46 + n * lh + 14


def draw_base(fax, x, ytop, w, rows, heading) -> float:
    h = base_height(rows, w)
    fax.add_patch(FancyBboxPatch((x, ytop - h), w, h, boxstyle="round,pad=0,rounding_size=8", fc=C["base_fill"],
                                 ec=C["panel_edge"], lw=1.0, zorder=2))
    fax.text(x + 18, ytop - 24, heading, fontsize=PANEL_FS * 1.08, fontweight="bold", color=C["ink"], va="center",
             zorder=4)
    lh = px(PANEL_FS * 0.95) * 1.3
    ty = ytop - 46
    for tag, txt in rows:
        lines = wrap(txt, w - 40 - 110, PANEL_FS * 0.95)
        fax.text(x + 18, ty - lh * 0.5, tag, fontsize=PANEL_FS * 0.8, fontweight="bold", color=TAG_COLOR.get(
            tag.split()[0], C["sub"]), va="center", zorder=4)
        for i, ln in enumerate(lines):
            fax.text(x + 18 + 110, ty - lh * (i + 0.5), ln, fontsize=PANEL_FS * 0.95, color=C["ink"], va="center",
                     zorder=4)
        ty -= lh * len(lines)
    return h


# ------------------------------------------------------------------------------------------------------------ the map
@dataclass
class Frame:
    ax: object
    ext: Tuple[float, float, float, float]
    zoom: float                  # display px per km, for widths


def to_px(ax, xs, ys):
    pts = ax.transData.transform(list(zip(xs, ys)))
    return [(float(a), float(b)) for a, b in pts]


def draw_background(ax, mi: MapInput, ext, fine: bool):
    W, S, E, N = ext
    clip = box(W - 0.02, S - 0.02, E + 0.02, N + 0.02)
    ax.set_facecolor(C["land"])
    for g in mi.sea:
        gg = g.intersection(clip)
        for poly in polys_of(gg):
            ax.add_patch(MPoly(list(poly.exterior.coords), closed=True, fc=C["sea"], ec=C["sea_edge"], lw=0.6,
                               zorder=1))
            for hole in poly.interiors:
                ax.add_patch(MPoly(list(hole.coords), closed=True, fc=C["land"], ec=C["sea_edge"], lw=0.4, zorder=1.1))
    span = max(E - W, 1e-6)
    for order, g in mi.context_streams:
        if not g.intersects(clip):
            continue
        lw = 0.35 + 0.18 * max(0, order - 2) * (1.6 if fine else 1.0)
        for ln in lines_of(g):
            xs, ys = ln.xy
            ax.plot(xs, ys, color=C["ctx"], lw=min(lw, 2.0), zorder=2, solid_capstyle="round")
    for _, g in mi.context_lakes:
        if not g.intersects(clip):
            continue
        for poly in polys_of(g):
            ax.add_patch(MPoly(list(poly.exterior.coords), closed=True, fc=C["lake"], ec=C["lake_edge"], lw=0.5,
                               zorder=3))
    if mi.basin_outline is not None:
        for poly in polys_of(mi.basin_outline):
            xs, ys = poly.exterior.xy
            ax.plot(xs, ys, color=C["basin"], lw=1.1, ls=(0, (6, 4)), zorder=2.5, alpha=0.8)
    del span


def unit_width(u: Unit, fine: bool) -> float:
    base = {"main": 4.0, "trib": 2.6, "context": 2.2}.get(u.role, 2.4)
    return base * (1.25 if fine else 1.0)


def draw_units(ax, mi: MapInput, fine: bool, placer: Optional[Placer]):
    order = sorted(mi.units, key=lambda u: ({"context": 0, "trib": 1, "main": 2}.get(u.role, 1),
                                           u.today.state != "closed", u.number))
    for u in order:
        col = color(u.today.state)
        lw = unit_width(u, fine)
        z = 5 + {"context": 0, "trib": 0.2, "main": 0.4}.get(u.role, 0) + (u.today.state not in ("closed",)) * 0.1
        for g in mi.geoms.get(u.number, []):
            for poly in polys_of(g):
                ax.add_patch(MPoly(list(poly.exterior.coords), closed=True, fc=col, alpha=0.30, ec="none", zorder=3.5))
                ax.add_patch(MPoly(list(poly.exterior.coords), closed=True, fill=False, ec=col, lw=lw * 0.6,
                                   zorder=z))
                if placer is not None:
                    placer.add_line(to_px(ax, *poly.exterior.xy), 0.5)
            for ln in lines_of(g):
                xs, ys = ln.xy
                if u.today.state == "tidal":
                    ax.plot(xs, ys, color="white", lw=lw + 2.2, zorder=z - 0.05, solid_capstyle="round")
                    ax.plot(xs, ys, color=col, lw=lw, zorder=z, ls=(0, (1.6, 0.9)), solid_capstyle="butt")
                else:
                    ax.plot(xs, ys, color="white", lw=lw + 1.8, zorder=z - 0.05, solid_capstyle="round")
                    ax.plot(xs, ys, color=col, lw=lw, zorder=z, solid_capstyle="round")
                    if "bait_ban" in u.today.marks:
                        ax.plot(xs, ys, color=C["bait"], lw=max(0.8, lw * 0.28), zorder=z + 0.01,
                                ls=(0, (0.6, 3.2)), solid_capstyle="round", dash_capstyle="round")
                    if "fly_only" in u.today.marks or "artificial_fly_only" in u.today.marks:
                        ax.plot(xs, ys, color=C["fly"], lw=max(0.9, lw * 0.35), zorder=z + 0.02, ls=(0, (4, 4)))
                if placer is not None:
                    placer.add_line(to_px(ax, xs, ys), 1.0 if u.role == "main" else 0.8)
        pt = mi.points.get(u.number)
        if pt is not None and not mi.geoms.get(u.number):
            ax.plot([pt[0]], [pt[1]], marker="s", ms=8, mfc="white", mec=col, mew=2.2, zorder=8)


def anchors_of(ax, mi: MapInput, u: Unit, ext, max_n: int = 2) -> List[Tuple[float, float]]:
    """Where a unit's callouts point: the middle of its longest visible stretch, and of its second-longest when that
    one is a separate stretch far from the first."""
    W, S, E, N = ext
    clip = box(W, S, E, N)
    pieces = []
    for g in mi.geoms.get(u.number, []):
        gg = g.intersection(clip) if not g.within(clip) else g
        for poly in polys_of(gg):
            pieces.append((poly.exterior.length * 0.5, poly.representative_point()))
        merged = []
        ls = lines_of(gg)
        if ls:
            m = unary_union(ls)
            try:
                from shapely.ops import linemerge
                m = linemerge(m) if not isinstance(m, LineString) else m
            except Exception:  # noqa: BLE001 — a merge failure only means separate pieces
                pass
            merged = lines_of(m)
        for ln in merged:
            pieces.append((ln.length, ln.interpolate(0.5, normalized=True)))
    if not pieces:
        pt = mi.points.get(u.number)
        if pt is None or not (W <= pt[0] <= E and S <= pt[1] <= N):
            return []
        return [ax.transData.transform([pt])[0]]
    # join pieces that are near each other (one stretch drawn in bits)
    pieces.sort(key=lambda t: -t[0])
    out: List[Tuple[float, float]] = []
    taken: List[Point] = []
    tot = sum(p[0] for p in pieces)
    span = max(E - W, N - S)
    for length, pt in pieces:
        if any(pt.distance(t) < span * 0.18 for t in taken):
            continue
        if taken and length < tot * 0.15:
            continue
        taken.append(pt)
        out.append(tuple(ax.transData.transform([(pt.x, pt.y)])[0]))
        if len(out) >= max_n:
            break
    return out


def place_callouts(ax, mi: MapInput, placer: Placer, ext, fs: float, numbers: Optional[set] = None):
    r = px(fs) * 0.95
    inv = ax.transData.inverted()
    todo = []
    for u in mi.units:
        if numbers is not None and u.number not in numbers:
            continue
        for a in anchors_of(ax, mi, u, ext, max_n=2 if numbers is None else 1):
            todo.append((u, a))
    todo.sort(key=lambda t: ({"main": 0, "trib": 1, "context": 2}.get(t[0].role, 1), t[0].number))
    for u, (axp, ayp) in todo:
        b = placer.place(axp, ayp, 2 * r + 4, 2 * r + 4, circle=True, pad=2)
        if b is None:
            continue
        cx, cy = (b[0] + b[2]) / 2, (b[1] + b[3]) / 2
        col = color(u.today.state)
        (dx, dy), (ex, ey) = inv.transform([(axp, ayp)])[0], inv.transform([(cx, cy)])[0]
        if math.hypot(cx - axp, cy - ayp) > r + 2:
            ax.annotate("", xy=(dx, dy), xytext=(ex, ey), textcoords="data", zorder=13,
                        arrowprops=dict(arrowstyle="-", color=col if u.today.state != "closed" else "#7D858F",
                                        lw=1.3, shrinkA=r, shrinkB=1))
            ax.plot([dx], [dy], marker="o", ms=3.4, color=col, zorder=13)
        ax.text(ex, ey, str(u.number), fontsize=fs, fontweight="bold", color="white", ha="center", va="center",
                zorder=14, bbox=dict(boxstyle="circle,pad=0.3", fc=col, ec="white", lw=1.5))


def break_label(b: Break, main_ord: Optional[int]) -> str:
    km = f"km {b.km:.1f}" if b.km is not None else ""
    if main_ord is not None and b.water.ord != main_ord:
        km = f"{b.water.name} {km}".strip()
    words = textwrap.fill(b.words, 30)
    return f"{words}\n{km}" if km else words


def place_breaks(ax, mi: MapInput, placer: Placer, ext, fs: float, main_ord: Optional[int], label: bool = True,
                 skip_boxes: Sequence[Tuple[float, float, float, float]] = ()):
    W, S, E, N = ext
    inv = ax.transData.inverted()
    pts = []
    for b in mi.breaks:
        if not (W <= b.lon <= E and S <= b.lat <= N):
            continue
        x, y = ax.transData.transform([(b.lon, b.lat)])[0]
        ax.plot([b.lon], [b.lat], marker="o", ms=8 if fs > 9 else 7, mfc="white", mec=C["ink"], mew=1.6, zorder=12)
        placer.block((x - 5, y - 5, x + 5, y + 5))
        pts.append((b, x, y))
    if not label:
        return
    for b, x, y in pts:
        if any(sb[0] <= b.lon <= sb[2] and sb[1] <= b.lat <= sb[3] for sb in skip_boxes):
            continue
        text = break_label(b, main_ord)
        lines = text.split("\n")
        w = max(len(s) for s in lines) * px(fs) * 0.55 + 12
        h = len(lines) * px(fs) * 1.22 + 8
        bb = placer.place(x, y, w, h, pad=3, radii=(30, 46, 66, 90, 120, 160, 210, 270, 340))
        if bb is None:
            continue
        cx, cy = (bb[0] + bb[2]) / 2, (bb[1] + bb[3]) / 2
        from pipeline.tools.water_map.layout import nearest_on_box
        ex, ey = nearest_on_box(x, y, bb)
        d0, d1, d2 = inv.transform([(x, y), (cx, cy), (ex, ey)])
        ax.plot([d0[0], d2[0]], [d0[1], d2[1]], color="#6B727B", lw=1.0, zorder=11)
        ax.text(d1[0], d1[1], text, fontsize=fs, color=C["ink"], ha="center", va="center", zorder=12,
                linespacing=1.12, bbox=dict(boxstyle="round,pad=0.28", fc="white", ec="#C9CED4", lw=0.8, alpha=0.96))


def place_names(ax, mi: MapInput, placer: Placer, ext, fs: float):
    inv = ax.transData.inverted()
    for n, name in sorted(mi.names.items()):
        u = next((x for x in mi.units if x.number == n), None)
        if u is None:
            continue
        anchors = anchors_of(ax, mi, u, ext, max_n=1)
        if not anchors:
            continue
        x, y = anchors[0]
        w = len(name) * px(fs) * 0.52 + 6
        h = px(fs) * 1.3
        bb = placer.place(x, y, w, h, pad=2, radii=(10, 16, 24, 34, 46), line_weight=12.0, max_line_cost=3.0)
        if bb is None:
            continue
        d = inv.transform([((bb[0] + bb[2]) / 2, (bb[1] + bb[3]) / 2)])[0]
        ax.text(d[0], d[1], name, fontsize=fs, style="italic", color="#3E5872", ha="center", va="center", zorder=10)


def scale_bar(ax, ext, fs, frame_px):
    W, S, E, N = ext
    lat = S + (N - S) * 0.06
    k = 111.32 * math.cos(math.radians((S + N) / 2))
    span_km = (E - W) * k
    km = nice_step(span_km * 0.22, 1)
    deg = km / k
    x0 = W + (E - W) * 0.04
    h = (N - S) * 0.006
    for i in range(2):
        ax.add_patch(Rectangle((x0 + i * deg / 2, lat), deg / 2, h, fc=C["ink"] if i == 0 else "white", ec=C["ink"],
                               lw=1.0, zorder=15))
    for i, v in enumerate((0, km / 2, km)):
        ax.text(x0 + i * deg / 2, lat + h * 1.6, f"{v:g}", fontsize=fs, ha="center", va="bottom", color=C["ink"],
                zorder=15)
    ax.text(x0 + deg + (E - W) * 0.012, lat + h / 2, "km", fontsize=fs, ha="left", va="center", color=C["ink"],
            zorder=15)
    ax.add_patch(Rectangle((x0 - (E - W) * 0.012, lat - (N - S) * 0.012), deg + (E - W) * 0.06,
                           (N - S) * 0.045, fc="white", ec="none", alpha=0.75, zorder=14.5))
    p = ax.transData.transform([(x0 - (E - W) * 0.012, lat - (N - S) * 0.012),
                                (x0 + deg + (E - W) * 0.05, lat + (N - S) * 0.035)])
    return (p[0][0], p[0][1], p[1][0], p[1][1])


def north_arrow(ax, ext, fs):
    W, S, E, N = ext
    x, y = E - (E - W) * 0.05, N - (N - S) * 0.13
    size = (N - S) * 0.06
    ax.annotate("", xy=(x, y + size), xytext=(x, y), zorder=15,
                arrowprops=dict(arrowstyle="-|>,head_width=0.5,head_length=0.9", color=C["ink"], lw=2.2))
    ax.text(x, y + size * 1.12, "N", fontsize=fs, fontweight="bold", ha="center", va="bottom", color=C["ink"],
            zorder=15)
    p = ax.transData.transform([(x - (E - W) * 0.02, y - (N - S) * 0.01), (x + (E - W) * 0.02, y + size * 1.5)])
    return (p[0][0], p[0][1], p[1][0], p[1][1])


def ticks(ax, ext, fs):
    W, S, E, N = ext
    k = math.cos(math.radians((S + N) / 2))
    sx = nice_step((E - W), 5)
    sy = nice_step((N - S), 5)
    xs = [round(v, 4) for v in _frange(math.ceil(W / sx) * sx, E, sx)]
    ys = [round(v, 4) for v in _frange(math.ceil(S / sy) * sy, N, sy)]
    ax.set_xticks(xs)
    ax.set_yticks(ys)
    ax.set_xticklabels([f"{abs(v):.{_dec(sx)}f}°W" for v in xs], fontsize=fs, color=C["sub"])
    ax.set_yticklabels([f"{v:.{_dec(sy)}f}°N" for v in ys], fontsize=fs, color=C["sub"])
    ax.grid(True, color="#FFFFFF", lw=0.8, alpha=0.7, zorder=0.5)
    ax.tick_params(length=4, color=C["sub"])
    for s in ax.spines.values():
        s.set_edgecolor("#9CA3AB")
        s.set_linewidth(1.0)
    del k


def _frange(a, b, s):
    v = a
    while v <= b + 1e-9:
        yield v
        v += s


def _dec(step: float) -> int:
    return 0 if step >= 1 else (1 if step >= 0.1 else (2 if step >= 0.01 else 3))


def frame_box(ax) -> Tuple[float, float, float, float]:
    bb = ax.get_window_extent()
    return (bb.x0, bb.y0, bb.x1, bb.y1)


# ------------------------------------------------------------------------------------------------------------ insets
@dataclass
class Inset:
    letter: str
    ext: Tuple[float, float, float, float]
    numbers: set


def find_insets(ax, mi: MapInput, ext, small_px: float = 26.0, max_insets: int = 3) -> List[Inset]:
    """Clusters of units too small to read at this scale (or breaks crowded together), each with an extent that
    shows its smallest unit at a readable size."""
    W, S, E, N = ext
    items = []      # (x_px, y_px, data box, unit number or None)
    for u in mi.units:
        gs = mi.geoms.get(u.number, [])
        if not gs:
            continue
        g = unary_union(gs)
        if g.is_empty:
            continue
        b = g.bounds
        p = ax.transData.transform([(b[0], b[1]), (b[2], b[3])])
        size = max(abs(p[1][0] - p[0][0]), abs(p[1][1] - p[0][1]))
        if size < small_px:
            items.append(((p[0][0] + p[1][0]) / 2, (p[0][1] + p[1][1]) / 2, b, u.number))
    bpx = [(b, *ax.transData.transform([(b.lon, b.lat)])[0]) for b in mi.breaks
           if W <= b.lon <= E and S <= b.lat <= N]
    for i, (b1, x1, y1) in enumerate(bpx):
        for b2, x2, y2 in bpx[i + 1:]:
            if math.hypot(x1 - x2, y1 - y2) < 22:
                items.append(((x1 + x2) / 2, (y1 + y2) / 2,
                              (min(b1.lon, b2.lon), min(b1.lat, b2.lat), max(b1.lon, b2.lon), max(b1.lat, b2.lat)),
                              None))
    if not items:
        return []
    # single-linkage clusters within 140 px
    clusters: List[List[int]] = []
    for i, it in enumerate(items):
        hit = [c for c in clusters if any(math.hypot(it[0] - items[j][0], it[1] - items[j][1]) < 140 for j in c)]
        merged = [i] + [j for c in hit for j in c]
        clusters = [c for c in clusters if c not in hit] + [merged]
    clusters.sort(key=lambda c: (-len(c), min(items[j][0] for j in c)))
    out: List[Inset] = []
    lake_units = {u.number for u in mi.units if mi.geoms.get(u.number) and all(polys_of(g) and not lines_of(g)
                                                                                for g in mi.geoms[u.number])}
    clusters = [c for c in clusters if not (len(c) == 1 and items[c[0]][3] in lake_units)]
    for k, c in enumerate(clusters[:max_insets]):
        bw = min(items[j][2][0] for j in c)
        bs = min(items[j][2][1] for j in c)
        be = max(items[j][2][2] for j in c)
        bn = max(items[j][2][3] for j in c)
        cx, cy = (bw + be) / 2, (bs + bn) / 2
        kx = math.cos(math.radians(cy))
        half = max((be - bw) * kx, bn - bs) * 0.5 * 1.7
        half = max(half, (E - W) * kx * 0.012)
        ins_ext = (cx - half / kx, cy - half, cx + half / kx, cy + half)
        if (ins_ext[2] - ins_ext[0]) > (E - W) * 0.45:
            continue
        nums = set()
        clip = box(*ins_ext)
        for u in mi.units:
            if any(g.intersects(clip) for g in mi.geoms.get(u.number, [])):
                nums.add(u.number)
        out.append(Inset("ABCDEF"[len(out)], ins_ext, nums))
    return out


# ------------------------------------------------------------------------------------------------------------ legend
def draw_legend(fax, x, ytop, w, mi: MapInput, fs: float, has_inset: bool) -> float:
    present = {u.today.state for u in mi.units}
    rows = [("line", color(s), label) for s, label in LEGEND[mi.mode] if s in present]
    marks = {m for u in mi.units for m in u.today.marks}
    if "bait_ban" in marks:
        rows.append(("bait", None, "Bait ban (white dots on the line)"))
    if marks & {"fly_only", "artificial_fly_only"}:
        rows.append(("fly", None, "Fly fishing only (pink dashes on the line)"))
    rows += [("num", None, "Part number → its panel"), ("dot", None, "Break: the answer changes here")]
    if any(mi.points.get(u.number) and not mi.geoms.get(u.number) for u in mi.units):
        rows.append(("square", None, "Part with no line in the map tiles (location only)"))
    if mi.basin_outline is not None:
        rows.append(("basin", None, "Watershed group boundary (FWA)"))
    rows.append(("ctx", None, "Other streams and lakes"))
    if has_inset:
        rows.append(("zoom", None, "Area of a zoom inset"))
    cols = 2
    per = math.ceil(len(rows) / cols)
    rh = px(fs) * 1.9
    h = 46 + per * rh + 10
    fax.add_patch(FancyBboxPatch((x, ytop - h), w, h, boxstyle="round,pad=0,rounding_size=8", fc="white",
                                 ec=C["panel_edge"], lw=1.0, zorder=2))
    fax.text(x + 18, ytop - 24, "Legend", fontsize=fs * 1.08, fontweight="bold", color=C["ink"], va="center", zorder=4)
    cw = w / cols
    for i, (kind, col, label) in enumerate(rows):
        c, r = divmod(i, per)
        yy = ytop - 52 - r * rh
        x0 = x + 18 + c * cw
        x1 = x0 + 46
        z = 4
        if kind == "line":
            fax.plot([x0, x1], [yy, yy], color=col, lw=6, solid_capstyle="round", zorder=z,
                     ls=(0, (1.6, 0.9)) if col == C["tidal"] else "-")
        elif kind == "bait":
            fax.plot([x0, x1], [yy, yy], color=C["open"], lw=6, solid_capstyle="round", zorder=z)
            fax.plot([x0, x1], [yy, yy], color="white", lw=1.6, ls=(0, (0.6, 3.2)), zorder=z + 0.1,
                     dash_capstyle="round")
        elif kind == "fly":
            fax.plot([x0, x1], [yy, yy], color=C["open"], lw=6, solid_capstyle="round", zorder=z)
            fax.plot([x0, x1], [yy, yy], color=C["fly"], lw=2.0, ls=(0, (4, 4)), zorder=z + 0.1)
        elif kind == "dot":
            fax.plot([(x0 + x1) / 2], [yy], marker="o", ms=10, mfc="white", mec=C["ink"], mew=1.6, zorder=z)
        elif kind == "square":
            fax.plot([(x0 + x1) / 2], [yy], marker="s", ms=9, mfc="white", mec=C["tidal"], mew=2.0, zorder=z)
        elif kind == "basin":
            fax.plot([x0, x1], [yy, yy], color=C["basin"], lw=1.4, ls=(0, (6, 4)), zorder=z)
        elif kind == "ctx":
            fax.plot([x0, x1 - 16], [yy, yy], color=C["ctx"], lw=1.4, zorder=z)
            fax.add_patch(Rectangle((x1 - 14, yy - 7), 14, 14, fc=C["lake"], ec=C["lake_edge"], lw=0.6, zorder=z))
        elif kind == "zoom":
            fax.add_patch(Rectangle((x0 + 4, yy - 8), 38, 16, fill=False, ec="#6B727B", lw=1.3, ls=(0, (4, 3)),
                                    zorder=z))
        elif kind == "num":
            fax.text((x0 + x1) / 2, yy, "3", fontsize=fs * 0.9, fontweight="bold", color="white", ha="center",
                     va="center", zorder=z, bbox=dict(boxstyle="circle,pad=0.3", fc=C["open"], ec="white", lw=1.4))
        fax.text(x1 + 14, yy, label, fontsize=fs, va="center", color=C["ink"], zorder=z)
    return h


# ------------------------------------------------------------------------------------------------------------ figure
def render(mi: MapInput, ext0: Tuple[float, float, float, float], out: Path, main_ord: Optional[int]) -> Path:
    """Lay out and write one PNG."""
    header, footer_h, margin, gap = 140, 34 + 18 * len(mi.footer), 40, 18
    right_x, right_w = 1300, FIG_W - 1300 - margin
    col_w = (right_w - gap) / 2
    map_x0, map_w = 120, 1130

    # right column: base box, then panels in two columns, split where the heights balance
    base_h = base_height(mi.base_rows, right_w) if mi.base_rows else 0
    hs = [panel_height(u, col_w) for u in mi.units]
    total = sum(h + 12 for h in hs)
    acc, split = 0.0, len(hs)
    for i, h in enumerate(hs):
        if acc + (h + 12) / 2 > total / 2:
            split = i
            break
        acc += h + 12
    col1, col2 = mi.units[:split], mi.units[split:]
    col1_h = sum(panel_height(u, col_w) + 12 for u in col1)
    col2_h = sum(panel_height(u, col_w) + 12 for u in col2)
    note_lines = [ln for n in mi.notes for ln in wrap(n, right_w - 20, 9.5)]
    notes_h = len(note_lines) * px(9.5) * 1.35 + (10 if note_lines else 0)
    right_h = base_h + (gap if base_h else 0) + 30 + max(col1_h, col2_h) + notes_h

    # the map fills the left column beside the panels; zoom insets sit in the right column under the panels (two
    # to a row). The insets depend on the map's scale and the map's height on the right column: settle in passes.
    ext_nat = ext0
    h_nat = natural_h(ext_nat, map_w)
    legend_h_est = 46 + math.ceil(11 / 2) * px(10.5) * 1.9 + 10
    ins_rows_h = 0
    ins_left = False
    fig = None
    for _pass in range(4):
        def layout(left: bool):
            """(H, map_h, right_total, left_extra) with the insets under the legend (left) or the panels."""
            right_total = right_h + (ins_rows_h + 40 if ins_rows_h and not left else 0)
            left_extra = (ins_rows_h + 40) if ins_rows_h and left else 0
            target = right_total - legend_h_est - gap * 2 - 60 - left_extra
            map_h = min(max(h_nat, 700.0, min(target, h_nat * 1.5)), 2600)
            left_h = map_h + gap * 2 + legend_h_est + 60 + left_extra
            return int(header + max(left_h, right_total) + footer_h + margin), map_h, right_total, left_extra

        # the insets go to whichever column makes the shorter image
        ins_left = bool(ins_rows_h) and layout(True)[0] < layout(False)[0]
        H, map_h, right_total, left_extra = layout(ins_left)
        if fig is not None:
            plt.close(fig)
        fig = plt.figure(figsize=(FIG_W / DPI, H / DPI), dpi=DPI)
        fig.patch.set_facecolor("#FAFAF8")
        ext = fit_extent(ext_nat, map_w, map_h)
        ax = fig.add_axes([map_x0 / FIG_W, (H - header - map_h) / H, map_w / FIG_W, map_h / H])
        ax.set_xlim(ext[0], ext[2])
        ax.set_ylim(ext[1], ext[3])
        ax.set_aspect("auto")
        insets = find_insets(ax, mi, ext, max_insets=4)
        want = (math.ceil(len(insets) / 2) * (INSET_H + 50)) if insets else 0
        if want == ins_rows_h and _pass:
            break
        ins_rows_h = want
    legend_h = legend_h_est
    if ins_left:
        ins_x0, ins_w = map_x0 - 70, (map_w + 70 - gap) / 2
    else:
        ins_x0, ins_w = right_x, col_w

    fax = fig.add_axes([0, 0, 1, 1], zorder=-1)
    fax.set_xlim(0, FIG_W)
    fax.set_ylim(0, H)
    fax.axis("off")
    fax.set_zorder(0)
    ax.set_zorder(1)

    # title
    fig.text(margin / FIG_W, 1 - 50 / H, mi.title, fontsize=30, fontweight="bold", color=C["ink"], va="center")
    fig.text(margin / FIG_W, 1 - 96 / H, mi.subtitle, fontsize=15, color=C["sub"], va="center")

    # the map
    draw_background(ax, mi, ext, fine=False)
    placer = Placer(frame_box(ax))
    draw_units(ax, mi, False, placer)
    placer.block(scale_bar(ax, ext, 10, None))
    placer.block(north_arrow(ax, ext, 12))
    ticks(ax, ext, 10)
    for ins in insets:
        w_, s_, e_, n_ = ins.ext
        ax.add_patch(Rectangle((w_, s_), e_ - w_, n_ - s_, fill=False, ec="#4B5259", lw=1.3, ls=(0, (4, 3)),
                               zorder=16))
        p = ax.transData.transform([(w_, s_), (e_, n_)])
        placer.block((p[0][0] - 2, p[0][1] - 2, p[1][0] + 2, p[1][1] + 2))
        lb = placer.place(p[0][0], p[1][1], 16, 18, pad=1, radii=(8, 14, 22, 32))
        if lb is not None:
            d = ax.transData.inverted().transform([((lb[0] + lb[2]) / 2, (lb[1] + lb[3]) / 2)])[0]
            ax.text(d[0], d[1], ins.letter, fontsize=11, fontweight="bold", color="#4B5259", ha="center",
                    va="center", zorder=16)
    place_breaks(ax, mi, placer, ext, 9.5, main_ord, skip_boxes=[i.ext for i in insets])
    place_callouts(ax, mi, placer, ext, 11)
    place_names(ax, mi, placer, ext, 10)
    draw_legend(fax, map_x0 - 70, H - (header + map_h + gap + 30), map_w + 70, mi, 10.5, bool(insets))

    # right column
    y = H - header + 10
    if mi.base_rows:
        draw_base(fax, right_x, y, right_w, mi.base_rows,
                  "Base: the region table" + (" and DFO defaults" if mi.mode == "salmon" else "")
                  + " — what holds unless a panel says otherwise")
        y -= base_h + gap
    fax.text(right_x, y - 8, "Each panel: BC = the B.C. rule-book rows (page) · GEAR = the gear the app shows · "
             "DFO = federal salmon places, narrowest first · TODAY = the app's answer on this date.",
             fontsize=9.5, style="italic", color=C["sub"], va="center")
    y -= 30
    for colx, us in ((right_x, col1), (right_x + col_w + gap, col2)):
        yy = y
        for u in us:
            h = draw_panel(fax, colx, yy, col_w, u, mi.mode)
            yy -= h + 12
    yy = y - max(col1_h, col2_h) - 4
    for ln in note_lines:
        fax.text(right_x, yy, ln, fontsize=9.5, color="#8A4B00", va="center")
        yy -= px(9.5) * 1.35

    # zoom insets: under the legend or under the panels
    y_ins = (header + map_h + gap + 30 + legend_h + 50) if ins_left else (H - yy) + 40
    for k, ins in enumerate(insets):
        row, colk = divmod(k, 2)
        x0 = ins_x0 + colk * (ins_w + gap)
        yt = y_ins + row * (INSET_H + 50)
        iext = fit_extent(ins.ext, ins_w, INSET_H)
        iax = fig.add_axes([x0 / FIG_W, (H - yt - INSET_H) / H, ins_w / FIG_W, INSET_H / H])
        iax.set_zorder(1)
        iax.set_xlim(iext[0], iext[2])
        iax.set_ylim(iext[1], iext[3])
        iax.set_xticks([])
        iax.set_yticks([])
        for sp in iax.spines.values():
            sp.set_edgecolor("#4B5259")
            sp.set_linewidth(1.2)
        fig.text(x0 / FIG_W, (H - yt + 12) / H, f"Inset {ins.letter} — zoom", fontsize=13, fontweight="bold",
                 color=C["ink"], va="bottom")
        draw_background(iax, mi, iext, fine=True)
        ip = Placer(frame_box(iax))
        draw_units(iax, mi, True, ip)
        ip.block(scale_bar(iax, iext, 9, None))
        place_breaks(iax, mi, ip, iext, 9, main_ord)
        place_callouts(iax, mi, ip, iext, 10, numbers=None)

    # footer
    for i, ln in enumerate(mi.footer):
        fig.text(1 - margin / FIG_W, (14 + 18 * (len(mi.footer) - 1 - i)) / H, ln, fontsize=9, color=C["sub"],
                 ha="right", va="bottom")
    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=DPI, facecolor=fig.get_facecolor())
    plt.close(fig)
    return out
