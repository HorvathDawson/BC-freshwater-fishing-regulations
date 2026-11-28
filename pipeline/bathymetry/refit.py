"""The wider fitter (T1188): a vector depth sheet's shore searched onto its lake far wider than `place.fit` does, one
step at a time, each step its own flag. MEASURES ONLY: every placing stays a proposal for the owner's eye.

    rigid          every rotation (2.5 deg starts; the best shift for every angle at once, by FFT cross-correlation of
                   rasterised shapes), the best STARTS distinct orientations polished on the real shapes; the mirror
                   image the same way. Scale 1: a turn and a shift, never a stretch.
    two_lake       a sheet that draws two or three lakes is fitted (rigid) to the UNION of its lake and its neighbours'
                   outlines; its rings and contour lines are then split per lake (`split_per_lake`).
    other_lake     a sheet whose coded lake still fits poorly is searched against the lakes within WIDE_M by shape
                   (the area band a loose or strict grade allows, then the closest shapes, a raster screen, the rigid
                   search): a wrong-lake code. The shape picks; the name confirms or refuses (`name_check`): a find
                   named differently from the sheet is never adopted, an unnamed one only within NEAR_M.
    small_stretch  a uniform scale of at most SMALL_STRETCH (5 %): adopted, its own flagged class.
    big_stretch    up to BIG_STRETCH (15 %), or the feet / metres factor on a sheet whose units are not stated: NEVER
                   adopted; the sheet is listed with what the stretch would change (`stretch_change`).

Then, on EVERY placed sheet (strict ones too), the checks that say how far a placing can be trusted, never moving it:
    neighbour      the lakes within NEIGHBOUR_M of the placed lake, searched by shape like `other_lake`: the best one,
                   its IoU and the gap to the placing; within MARGIN (or better) it is a rival (`neighbour_check`).
    orientation    a strict placing the repo's fitter made is searched at every turn and the mirror: another turn or
                   the mirror within MARGIN of it makes it ambiguous (`orientation_check`).
    shape          how much the sheet looks like itself turned or mirrored (`self_similarity`): a round or oval shore
                   fits many turns and many lakes, so a good score on it is weak evidence.

A step's fit is adopted only when it LIFTS THE CLASS (strict > loose > poor) and is not AMBIGUOUS: another orientation
(more than SEP_DEG away), the other mirror, or another target within MARGIN IoU of it. An ambiguous lift is reported,
never counted. The measures are the repo's own (`place._iou`, `place.shore_distances`, `place_more.grade`).
Deterministic: no randomness, ties broken by order (angles ascending, mirror False first, targets by key).
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import numpy as np

from pipeline.bathymetry import place
from pipeline.bathymetry.place_more import grade

MARGIN = 0.03           # a rival this close in IoU makes a fit ambiguous
SEP_DEG = 20.0          # orientations further apart than this are different placings
STEP_DEG = 2.5          # the coarse rotation step
STARTS = 3              # distinct orientations polished per mirror
CELL_N = 50             # the coarse raster: about this many cells across the sheet
SMALL_STRETCH = 0.05    # the largest stretch ever adopted (flagged)
BIG_STRETCH = 0.15      # the stretch tried for the "would help" list (never adopted)
FEET = 0.3048
NEAR_M = 3000.0         # neighbours for a two-lake union: within max(NEAR_M, sheet radius) of the coded lake
NEIGHBOURS = 8          # at most this many neighbours, nearest first
WIDE_M = 50_000.0       # the wrong-lake search radius around the coded lake
SHAPES = 12             # the closest shapes screened per wrong-lake search
SCREEN_MIN = 0.65       # a screened target is searched in full from this coarse IoU
SEARCHED = 5            # at most this many screened targets searched in full
RING_MIN = 0.02         # a ring smaller than this share of the sheet is not split to a lake
DRAWN_MIN = 0.5         # a union member counts as drawn when the placed sheet covers this share of it
NEIGHBOUR_M = WIDE_M    # the neighbour check's radius around the placed lake (every sheet)
NEIGHBOUR_SEARCHED = 3  # at most this many screened neighbours searched in full
SCREEN_SLACK = 0.15     # a neighbour screened this far below the placing's IoU (less MARGIN) cannot be a rival

SLIVER = 9.0           # the elongation given a shape with no rectangle (ranked last)

RANK = {"strict": 0, "loose": 1, "poor": 2}


@dataclass(frozen=True)
class Pose:
    """mirror (x -> -x) then scale, then turn (deg, counter-clockwise), all about the sheet's origin; then a shift."""
    mirror: bool = False
    scale: float = 1.0
    angle: float = 0.0
    dx: float = 0.0
    dy: float = 0.0


def apply(g, pose: Pose, origin: Tuple[float, float]):
    """The sheet geometry `g` (any shapely geometry) placed by `pose`. Same order as `place._place`."""
    from shapely import affinity
    g = affinity.scale(g, xfact=(-pose.scale if pose.mirror else pose.scale), yfact=pose.scale, origin=origin)
    g = affinity.rotate(g, pose.angle, origin=origin)
    return affinity.translate(g, pose.dx, pose.dy)


def measures(sheet, target, pose: Pose, origin) -> Dict[str, object]:
    """The repo's measures of one placing, the grade and the pose."""
    g = apply(sheet, pose, origin)
    iou = place._iou(g, target)
    med, p90 = place.shore_distances(g, target)
    f = {"iou": round(iou, 4), "median_m": round(med, 1), "p90_m": round(p90, 1),
         "area_ratio": round(g.area / target.area, 4), "angle_deg": round(pose.angle % 360, 2),
         "dx": round(pose.dx, 2), "dy": round(pose.dy, 2), "mirror": bool(pose.mirror), "scale": round(pose.scale, 4)}
    f["grade"] = grade(f)
    return f


def pose_of(f: Mapping[str, object]) -> Pose:
    return Pose(bool(f.get("mirror", False)), float(f.get("scale", 1.0)), float(f["angle_deg"]), float(f["dx"]),
                float(f["dy"]))


def angdiff(a: float, b: float) -> float:
    d = abs(a - b) % 360
    return min(d, 360 - d)


def better(a: str, b: str) -> bool:
    """class a is above class b"""
    return RANK[a] < RANK[b]


# ---------------------------------------------------------------- the coarse raster search

def _grid_points(poly, h: float) -> np.ndarray:
    import shapely
    minx, miny, maxx, maxy = poly.bounds
    xs = np.arange(minx + h / 2, maxx, h)
    ys = np.arange(miny + h / 2, maxy, h)
    if len(xs) == 0 or len(ys) == 0:
        c = poly.representative_point()
        return np.array([[c.x, c.y]])
    X, Y = np.meshgrid(xs, ys)
    X, Y = X.ravel(), Y.ravel()
    k = shapely.contains_xy(poly, X, Y)
    return np.c_[X[k], Y[k]]


class _Raster:
    """A target water rasterised (2 x 2 supersampled cells) with room around it for the sheet, and its FFT."""

    def __init__(self, geom, cell: float, pad: float):
        import shapely
        self.cell, self.area = cell, geom.area
        minx, miny, maxx, maxy = geom.bounds
        self.x0, self.y0 = minx - pad - cell, miny - pad - cell
        self.nx = int(math.ceil((maxx + pad + cell - self.x0) / cell))
        self.ny = int(math.ceil((maxy + pad + cell - self.y0) / cell))
        h = cell / 2
        xs = self.x0 + (np.arange(self.nx * 2) + 0.5) * h
        ys = self.y0 + (np.arange(self.ny * 2) + 0.5) * h
        X, Y = np.meshgrid(xs, ys)
        m = shapely.contains_xy(geom, X.ravel(), Y.ravel()).reshape(self.ny * 2, self.nx * 2).astype(np.float64)
        self.F = np.fft.rfft2(m.reshape(self.ny, 2, self.nx, 2).mean(axis=(1, 3)))


def coarse(sheet, target, origin, *, scales: Sequence[float] = (1.0,), mirror: bool = False,
           cell_n: int = CELL_N) -> Tuple[np.ndarray, np.ndarray, np.ndarray, List[float], float]:
    """Raster IoU of the sheet on the target at every STEP_DEG angle and scale, each at its best integer-cell shift.
    Returns iou[s, a], dx[s, a], dy[s, a], the angles and the cell (m)."""
    R = max(math.hypot(x - origin[0], y - origin[1]) for p in place._polys(sheet) for x, y in p.exterior.coords)
    smin, smax = min(scales), max(scales)
    minx, miny, maxx, maxy = target.bounds
    ext = max(maxx - minx, maxy - miny) + 2 * R * smax
    cell = max(math.sqrt(sheet.area) * smin / cell_n, ext / 600, 0.5)
    h = cell / 1.6 / smax
    P = _grid_points(sheet, h) - np.asarray(origin)
    if mirror:
        P = P * np.array([-1.0, 1.0])
    Rs = R * smax
    tgt = _Raster(target, cell, Rs + cell)
    nS = int(math.ceil(2 * Rs / cell)) + 1
    angles = [float(a) for a in np.arange(0.0, 360.0, STEP_DEG)]
    out = np.zeros((len(scales), len(angles)))
    tx, ty = np.zeros_like(out), np.zeros_like(out)
    vy, vx = tgt.ny - nS + 1, tgt.nx - nS + 1
    for si, s in enumerate(scales):
        w = (h * s) ** 2 / cell ** 2
        A_S = len(P) * (h * s) ** 2
        for k0 in range(0, len(angles), 36):
            angs = angles[k0:k0 + 36]
            stack = np.zeros((len(angs), tgt.ny * tgt.nx))
            for k, a in enumerate(angs):
                t = math.radians(a)
                ca, sa = math.cos(t), math.sin(t)
                ix = ((s * (ca * P[:, 0] - sa * P[:, 1]) + Rs) // cell).astype(np.int64)
                iy = ((s * (sa * P[:, 0] + ca * P[:, 1]) + Rs) // cell).astype(np.int64)
                stack[k] = np.bincount(iy * tgt.nx + ix, minlength=tgt.ny * tgt.nx)[:tgt.ny * tgt.nx] * w
            Fs = np.fft.rfft2(stack.reshape(len(angs), tgt.ny, tgt.nx), axes=(1, 2))
            C = np.fft.irfft2(tgt.F[None] * np.conj(Fs), s=(tgt.ny, tgt.nx), axes=(1, 2))[:, :vy, :vx]
            inter = C * cell ** 2
            iou = (inter / (A_S + tgt.area - inter)).reshape(len(angs), -1)
            j = iou.argmax(axis=1)
            for k in range(len(angs)):
                uy, ux = divmod(int(j[k]), vx)
                out[si, k0 + k] = iou[k, j[k]]
                tx[si, k0 + k] = tgt.x0 + ux * cell + Rs - origin[0]
                ty[si, k0 + k] = tgt.y0 + uy * cell + Rs - origin[1]
    return out, tx, ty, angles, cell


def screen(sheet, target, origin, *, mirrored: bool = False) -> float:
    """The cheap look: rigid, a coarser raster; the best raster IoU. No mirror unless `mirrored`, then the better of
    the plain and the mirrored look (the neighbour check: a lake that fits only mirrored must reach the full search)."""
    v = float(coarse(sheet, target, origin, cell_n=30)[0].max())
    if mirrored:
        v = max(v, float(coarse(sheet, target, origin, mirror=True, cell_n=30)[0].max()))
    return round(v, 4)


# ---------------------------------------------------------------- polishing on the real shapes

def polish(sheet_s, target_s, origin, pose: Pose, step_xy: float,
           bounds: Optional[Tuple[float, float]] = None, max_evals: int = 90) -> Tuple[Pose, float]:
    """Pattern search (turn, shift and, inside `bounds`, scale) on simplified shapes, by real IoU."""
    x = [pose.angle, pose.dx, pose.dy, pose.scale]
    steps = [0.5, step_xy, step_xy, 0.01 if bounds else 0.0]

    def score(v):
        return place._iou(apply(sheet_s, Pose(pose.mirror, v[3], v[0], v[1], v[2]), origin), target_s)

    best, n = score(x), 1
    for _ in range(4):
        improved = True
        while improved and n < max_evals:
            improved = False
            for i in range(4):
                if steps[i] == 0:
                    continue
                for sg in (1, -1):
                    v = list(x)
                    v[i] += sg * steps[i]
                    if i == 3 and not (bounds[0] <= v[3] <= bounds[1]):
                        continue
                    sc = score(v)
                    n += 1
                    if sc > best + 1e-9:
                        best, x, improved = sc, v, True
        steps = [st / 2 for st in steps]
    return Pose(pose.mirror, x[3], x[0], x[1], x[2]), best


def search(sheet, target, origin, *, mirrors: Sequence[bool] = (False, True), scales: Sequence[float] = (1.0,),
           bounds: Optional[Tuple[float, float]] = None, starts: int = STARTS) -> List[Dict[str, object]]:
    """Every orientation of the sheet on one target: per mirror, the `starts` best distinct orientations of the coarse
    search (more than SEP_DEG apart), each polished from its angle and from half a step either side. Returns the
    refined fits (the repo's measures + pose), best IoU first."""
    tol = math.sqrt(target.area) / 300.0
    sheet_s, target_s = sheet.simplify(math.sqrt(sheet.area) / 300.0), target.simplify(tol)
    fits = []
    for m in mirrors:
        out, tx, ty, angs, cell = coarse(sheet, target, origin, scales=scales, mirror=m)
        best_by_angle = out.max(axis=0)
        order = sorted(range(len(angs)), key=lambda k: (-best_by_angle[k], k))
        picked: List[int] = []
        for k in order:
            if len(picked) >= starts:
                break
            if all(angdiff(angs[k], angs[p]) > SEP_DEG for p in picked):
                picked.append(k)
        for k in picked:
            si = int(out[:, k].argmax())
            p0 = Pose(m, float(scales[si]), angs[k], float(tx[si, k]), float(ty[si, k]))
            best_pose, best_sc = polish(sheet_s, target_s, origin, p0, cell, bounds)
            for da in (-STEP_DEG / 2, STEP_DEG / 2):
                p1, s1 = polish(sheet_s, target_s, origin, Pose(m, p0.scale, p0.angle + da, p0.dx, p0.dy), cell,
                                bounds, max_evals=40)
                if s1 > best_sc + 1e-9:
                    best_pose, best_sc = p1, s1
            f = measures(sheet, target, best_pose, origin)
            f["coarse_iou"] = round(float(out[si, k]), 4)
            f["start_deg"] = angs[k]
            fits.append(f)
    fits.sort(key=lambda f: (-f["iou"], f["mirror"], f["angle_deg"]))
    return fits


def verdict(best: Mapping[str, object], rivals: Iterable[Tuple[str, float]]) -> Dict[str, object]:
    """Ambiguous when a rival (another orientation, the mirror, another target) is within MARGIN IoU of the best."""
    rv = sorted(rivals, key=lambda r: (-r[1], r[0]))
    if not rv:
        return {"ambiguous": False, "rival": None, "rival_iou": None, "gap": None}
    gap = round(float(best["iou"]) - rv[0][1], 4)
    return {"ambiguous": gap < MARGIN, "rival": rv[0][0], "rival_iou": rv[0][1], "gap": gap}


def rivals_of(best: Mapping[str, object], fits: Sequence[Mapping[str, object]], label: str = "") -> List[Tuple[str, float]]:
    """The fits on the same target that are a different placing from `best`: the other mirror, or the same mirror at an
    orientation more than SEP_DEG away."""
    out = []
    for f in fits:
        if f is best:
            continue
        if f["mirror"] != best["mirror"]:
            out.append((f"{label}mirror", float(f["iou"])))
        elif angdiff(float(f["angle_deg"]), float(best["angle_deg"])) > SEP_DEG:
            out.append((f"{label}orientation {f['angle_deg']:g}", float(f["iou"])))
    return out


def best_over(targets: Mapping[str, Sequence[Mapping[str, object]]],
              extra_rivals: Sequence[Tuple[str, float]] = ()) -> Optional[Dict[str, object]]:
    """The best fit over several targets (key -> its `search` fits), with its verdict against the rest (and against
    `extra_rivals`, the best of targets searched in an earlier step)."""
    flat = [(k, f) for k in sorted(targets) for f in targets[k]]
    if not flat:
        return None
    k0, b = min(flat, key=lambda kf: (-kf[1]["iou"], kf[0], kf[1]["mirror"], kf[1]["angle_deg"]))
    rv = rivals_of(b, targets[k0])
    rv += [(f"target {k}", max(float(f["iou"]) for f in fs)) for k, fs in sorted(targets.items()) if k != k0 and fs]
    rv += list(extra_rivals)
    return {"target": k0, "fit": dict(b), **verdict(b, rv)}


# ---------------------------------------------------------------- the two-lake split

def split_per_lake(placed_sheet, placed_lines: Sequence[Tuple[Optional[float], object]],
                   lakes: Mapping[int, object]) -> Dict[str, object]:
    """A placed two-lake sheet cut per lake. Each depth contour goes to the lake polygon that holds most of its length
    (two lakes joined by a channel are one ring on the sheet, so the lake outlines cut, not the rings); a contour in no
    lake is counted `unassigned`, never guessed. Per lake: the sheet water on it (ha), the share of the lake the sheet
    covers, the contours; each sheet ring of at least RING_MIN of the sheet is reported with the lake it overlaps most."""
    keys = sorted(lakes)
    per = {w: {"sheet_ha": round(placed_sheet.intersection(lakes[w]).area / 1e4, 2), "contours": 0,
               "lake_covered": round(placed_sheet.intersection(lakes[w]).area / lakes[w].area, 3)} for w in keys}
    unassigned = 0
    for depth, line in placed_lines:
        if depth in (None, 0):
            continue
        held = [(line.intersection(lakes[w]).length, w) for w in keys]
        ln, w = max(held, key=lambda t: (t[0], -t[1]))
        if ln <= 0:
            unassigned += 1
            continue
        per[w]["contours"] += 1
    rings = []
    for p in sorted(place._polys(placed_sheet), key=lambda p: (-p.area, p.bounds)):
        if p.area < RING_MIN * placed_sheet.area:
            continue
        a, w = max(((p.intersection(lakes[x]).area, x) for x in keys), key=lambda t: (t[0], -t[1]))
        rings.append({"ring_ha": round(p.area / 1e4, 2), "lake": f"wbk:{w}" if a > 0 else None})
    return {"per_lake": {str(w): per[w] for w in keys}, "unassigned_contours": unassigned, "rings": rings}


# ---------------------------------------------------------------- the wrong-lake shape search

def shape_of(g) -> Tuple[float, float]:
    """(elongation, compactness): the long / short side of the minimum rotated rectangle (log), and 4 pi A / P^2. A
    shape whose rectangle does not come out (a sliver: GEOS divides by zero) gets elongation SLIVER, ranked last."""
    with np.errstate(divide="ignore", invalid="ignore"):
        r = g.minimum_rotated_rectangle
    xy = list(r.exterior.coords) if r.geom_type == "Polygon" else []
    s1, s2 = (math.dist(xy[0], xy[1]), math.dist(xy[1], xy[2])) if len(xy) >= 3 else (0.0, 0.0)
    lo, hi = min(s1, s2), max(s1, s2)
    elong = math.log(hi / lo) if lo > 0 and math.isfinite(hi / lo) else SLIVER
    comp = 4 * math.pi * g.area / (g.length ** 2) if g.length > 0 else 0.0
    return elong, comp


def shape_rank(sheet, lakes: Mapping[int, object], *, limit: int = SHAPES) -> List[Tuple[int, float]]:
    """The lakes a rigid placing could grade loose or strict on (sheet / lake area 0.8-1.25), closest shapes first:
    distance = |log area ratio| + |elongation difference| + |compactness difference|. Ties by key."""
    es, cs = shape_of(sheet)
    out = []
    for w in sorted(lakes):
        g = lakes[w]
        ar = sheet.area / g.area if g.area > 0 else 0.0
        if not 0.8 <= ar <= 1.25:
            continue
        el, cl = shape_of(g)
        out.append((w, round(abs(math.log(ar)) + abs(es - el) + abs(cs - cl), 4)))
    out.sort(key=lambda t: (t[1], t[0]))
    return out[:limit]


# ---------------------------------------------------------------- stretches

def stretch_change(before: Mapping[str, object], after: Mapping[str, object]) -> str:
    """What a stretch would change, in words: the scale, the area, the IoU, the shore distance and the class."""
    s = float(after["scale"])
    pct = (s - 1) * 100
    what = (f"feet drawn as metres? scale {s:.4f}" if abs(s - FEET) < 0.02 else
            f"metres drawn as feet? scale {s:.4f}" if abs(s - 1 / FEET) < 0.2 else
            f"{'grows' if pct > 0 else 'shrinks'} the chart {abs(pct):.1f}% (area {(s * s - 1) * 100:+.0f}%)")
    return (f"{what}; IoU {before['iou']:.2f} -> {after['iou']:.2f}; shore median {before['median_m']:.0f} -> "
            f"{after['median_m']:.0f} m; {before['grade']} -> {after['grade']}")


def stretch_modes(units: str, raw_area_ratio: float) -> List[Tuple[str, Tuple[float, ...], Tuple[float, float]]]:
    """The big-stretch searches for the "would help" list: +-BIG_STRETCH always; the feet factor (sheet drawn in feet,
    read as metres: shrink by 0.3048) or its inverse only when the sheet does not state its units and its area is
    that far off."""
    modes = [("stretch", (1 - BIG_STRETCH, 1 - BIG_STRETCH / 2, 1.0, 1 + BIG_STRETCH / 2, 1 + BIG_STRETCH),
              (1 - BIG_STRETCH, 1 + BIG_STRETCH))]
    if units == "not stated" and 6 <= raw_area_ratio <= 18:
        modes.append(("feet", (0.29, FEET, 0.32), (0.2896, 0.32)))
    if units == "not stated" and 0.055 <= raw_area_ratio <= 0.17:
        modes.append(("feet_up", (3.117, 1 / FEET, 3.445), (3.117, 3.445)))
    return modes


SMALL_SCALES = (1 - SMALL_STRETCH, 1 - SMALL_STRETCH / 2, 1.0, 1 + SMALL_STRETCH / 2, 1 + SMALL_STRETCH)


# ---------------------------------------------------------------- the trust checks, on every placed sheet

def self_similarity(sheet, origin) -> Dict[str, object]:
    """How much the sheet looks like itself turned more than SEP_DEG or mirrored (any turn): the best raster IoU of the
    sheet on itself that way, over the raster IoU of it unturned (the raster's own loss), and `complexity` = 1 - that
    share. A circle or an ellipse scores about 0 (it fits itself turned half round); an irregular shore scores high."""
    ident, best, at = 0.0, -1.0, ""
    for m in (False, True):
        out, _tx, _ty, angs, _cell = coarse(sheet, sheet, origin, mirror=m)
        for k, a in enumerate(angs):
            v = float(out[0, k])
            if not m and a == 0.0:
                ident = v
            if not m and angdiff(a, 0.0) <= SEP_DEG:
                continue
            if v > best + 1e-12:
                best, at = v, ("mirror, turn " if m else "turn ") + f"{a:g}"
    share = min(1.0, best / ident) if ident > 0 else 1.0
    return {"self_iou": round(best, 4), "identity_iou": round(ident, 4), "self_rival": at,
            "complexity": round(1.0 - share, 4)}


def orientation_check(sheet, target, origin, placed: Mapping[str, object]) -> Dict[str, object]:
    """A placing made elsewhere (the repo's fitter) against every turn and the mirror on its own lake: its verdict
    against the fits that are a different placing (`rivals_of`)."""
    fits = search(sheet, target, origin)
    p = dict(placed, mirror=bool(placed.get("mirror", False)))
    return dict(verdict(p, rivals_of(p, fits)), best_iou=fits[0]["iou"] if fits else None)


def unstretched_iou(got: Mapping[str, object]) -> float:
    """The placing's IoU before any stretch (`refit_sheet`'s result): what an unstretched rival is compared with."""
    ss = got["steps"]["small_stretch"]
    return float((ss["before"] if ss.get("adopted") else got["after"]["fit"])["iou"])


def neighbour_check(sheet, origin, placed_iou: float, target, exclude: Iterable[int], near_lakes,
                    radius: float = NEIGHBOUR_M) -> Tuple[Dict[str, object], Optional[object]]:
    """Every lake within `radius` of the placed lake (`exclude`: the placed lake or lakes), the closest shapes in the
    area band (`shape_rank`) screened, the best few searched in full (every turn, the mirror): the best neighbour, its
    IoU and the gap to the placing. `rival` when it fits better or within MARGIN. `similar_size` counts the lakes in
    the area band: 0 means the placed lake is the only lake of its size around. Returns the record and the best
    neighbour's outline (None when none was searched)."""
    ex = set(exclude)
    wide = {w: e for w, e in sorted(near_lakes(target.bounds, radius).items()) if w not in ex}
    band = shape_rank(sheet, {w: e["geom"] for w, e in wide.items()}, limit=len(wide))
    ranked = band[:SHAPES]
    scr = sorted(((screen(sheet, wide[w]["geom"], origin, mirrored=True), w) for w, _d in ranked),
                 key=lambda t: (-t[0], t[1]))
    floor = min(SCREEN_MIN, float(placed_iou) - MARGIN - SCREEN_SLACK)
    chosen = [w for v, w in scr if v >= floor][:NEIGHBOUR_SEARCHED]
    rec: Dict[str, object] = {"radius_m": radius, "lakes_in_radius": len(wide), "similar_size": len(band),
                              "screened": [[f"wbk:{w}", v] for v, w in scr[:3]], "searched": len(chosen),
                              "lake": None, "rival": False}
    best = None
    for w in chosen:
        fits = search(sheet, wide[w]["geom"], origin)
        if fits and (best is None or fits[0]["iou"] > best[1]["iou"]):
            best = (w, fits[0])
    if best is None:
        return rec, None
    w, f = best
    gap = round(float(placed_iou) - float(f["iou"]), 4)
    rec.update(lake=f"wbk:{w}", name=wide[w].get("name"), layer=wide[w].get("layer"),
               dist_m=round(wide[w]["geom"].distance(target)), iou=f["iou"], grade=f["grade"], gap=gap,
               fit=dict(f), rival=gap < MARGIN)
    return rec, wide[w]["geom"]

# ---------------------------------------------------------------- one sheet, every step

STEPS = ("baseline", "rigid", "two_lake", "other_lake", "small_stretch")


def _key(ws: Iterable[int]) -> str:
    return "+".join(f"wbk:{w}" for w in sorted(ws))


def _members(key: str) -> List[int]:
    return [int(k.split(":")[1]) for k in key.split("+")]


def name_check(name: Optional[str], titles: Sequence[str], coded_names: Sequence[str], dist_m: float) -> str:
    """How the name and the position stand on a wrong-lake find (the SHAPE picked it; a name never picks a lake, it
    only confirms or refuses): `title` (the lake's name is the sheet's title, `lake_facts.core` or word order),
    `coded_name` (it shares the coded lake's name: a lake of the same group), `unnamed_near` (no name, within NEAR_M
    of the coded lake), `unnamed_far`, or `differs` (a different name refuses it)."""
    from pipeline.bathymetry.propose import word_order
    from pipeline.deliver.lake_facts import core
    if not name:
        return "unnamed_near" if dist_m <= NEAR_M else "unnamed_far"
    forms = {core(name), word_order(name)}
    if forms & {f for t in titles for f in (core(t), word_order(t))}:
        return "title"
    if forms & {f for n in coded_names if n for f in (core(n), word_order(n))}:
        return "coded_name"
    return "differs"


NAME_CONFIRMS = ("title", "coded_name", "unnamed_near")


def refit_sheet(sheet, lines: Sequence[Tuple[Optional[float], object]], coded: Mapping[int, Mapping[str, object]],
                near_lakes, *, base_fit: Mapping[str, object], units: str,
                titles: Sequence[str] = (), taken: Optional[Mapping[int, Sequence[str]]] = None) -> Dict[str, object]:
    """Every step on one sheet that the repo's fitter left loose or poor.

    sheet      the sheet's water (`place.shore`), in sheet metres
    lines      (depth, LineString) of the sheet's arcs, in sheet metres (split per lake on a two-lake fit)
    coded      wbk -> {"geom", "name", "layer"}: the lakes the code names (`place_more.candidates`)
    near_lakes callable (bounds, pad) -> wbk -> {"geom", "name", "layer"}: the FWA lakes in that box
    base_fit   the repo's fit (`place.fit` + its `wbk`), the baseline this improves on
    units      the units the sheet states ("not stated" when none)
    titles     the sheet's map titles (they confirm or refuse a wrong-lake find, `name_check`)
    taken      wbk -> the other sheets that end on it (`refit_all.claims`): a wrong-lake find on one is refused

    Returns per step what it found and whether it was adopted, the class after each step, the placing after all
    adopted steps, its flags, and the bigger stretch that would help (never adopted). `_geoms` (not serialisable) holds
    the lake outlines used, for pictures."""
    from shapely.ops import unary_union
    c = sheet.centroid
    origin = (c.x, c.y)
    A = sheet.area
    R = max(math.hypot(x - origin[0], y - origin[1]) for p in place._polys(sheet) for x, y in p.exterior.coords)
    geoms: Dict[int, object] = {w: coded[w]["geom"] for w in sorted(coded)}
    info: Dict[int, Dict[str, object]] = {w: {"name": coded[w].get("name"), "layer": coded[w].get("layer"),
                                              "dist_m": 0} for w in sorted(coded)}
    bw = int(str(base_fit["wbk"]).split(":")[1])
    cur = {"grade": grade(base_fit), "fit": dict(base_fit, grade=grade(base_fit), mirror=False, scale=1.0),
           "target": f"wbk:{bw}", "step": "baseline",
           "geom": geoms[bw]}
    flags: List[str] = []
    steps: Dict[str, object] = {}
    classes = {"baseline": cur["grade"]}

    def adopt(step, target, fit, geom):
        cur.update(grade=fit["grade"], fit=dict(fit), target=target, step=step, geom=geom)

    # 1. rigid, wider: every rotation, several starts, the mirror; scale 1
    rig = {f"wbk:{w}": search(sheet, geoms[w], origin) for w in sorted(geoms)}
    # the best fit on every target any step searched: the rivals of a stretch on another target
    target_best: Dict[str, float] = {k: max(float(f["iou"]) for f in fs) for k, fs in sorted(rig.items()) if fs}
    b = best_over(rig)
    lifts = better(b["fit"]["grade"], cur["grade"])
    steps["rigid"] = dict(b, lifts=lifts, adopted=lifts and not b["ambiguous"])
    if steps["rigid"]["adopted"]:
        adopt("rigid", b["target"], b["fit"], geoms[_members(b["target"])[0]])
        flags.append("rigid_wider")
        if b["fit"]["mirror"]:
            flags.append("mirror")
    elif lifts:
        flags.append("rigid_ambiguous")
    elif b["fit"]["iou"] >= float(base_fit["iou"]) + 0.02:
        flags.append("rigid_better_same_class")
    single_best = [(f"single {k}", max(float(f["iou"]) for f in fs)) for k, fs in sorted(rig.items()) if fs]
    classes["rigid"] = cur["grade"]

    # 2. two lakes: the union of the coded lake and its neighbours
    coded_u = unary_union([geoms[w] for w in sorted(geoms)])
    rings = [p for p in place._polys(sheet) if p.area >= 0.05 * A]
    pad = max(NEAR_M, R)
    if cur["grade"] == "strict":
        steps["two_lake"] = {"skipped": "already strict"}
    elif len(rings) < 2 and float(base_fit["area_ratio"]) <= 1.25:
        steps["two_lake"] = {"skipped": "one ring, not bigger than its lake"}
    else:
        near = {w: e for w, e in sorted(near_lakes(coded_u.bounds, pad).items()) if w not in geoms}
        dist = {w: e["geom"].distance(coded_u) for w, e in near.items()}
        pool = sorted((w for w, e in near.items() if 0.02 * A <= e["geom"].area <= 1.5 * A and dist[w] <= pad),
                      key=lambda w: (dist[w], w))[:NEIGHBOURS]
        for w in pool:
            geoms[w] = near[w]["geom"]
            info[w] = {"name": near[w].get("name"), "layer": near[w].get("layer"), "dist_m": round(dist[w])}
        combos = set()
        for w0 in sorted(coded):
            for w in [x for x in sorted(coded) if x != w0] + pool:
                combos.add(tuple(sorted((w0, w))))
        scr = sorted(((screen(sheet, unary_union([geoms[w] for w in ws]), origin), ws) for ws in combos),
                     key=lambda t: (-t[0], t[1]))
        if len(rings) >= 3 and scr:
            top = scr[0][1]
            for w in sorted(set(sorted(coded) + pool) - set(top)):
                ws = tuple(sorted(top + (w,)))
                scr.append((screen(sheet, unary_union([geoms[x] for x in ws]), origin), ws))
            scr.sort(key=lambda t: (-t[0], t[1]))
        full = {_key(ws): search(sheet, unary_union([geoms[w] for w in ws]), origin)
                for v, ws in scr[:2] if v >= SCREEN_MIN}
        target_best.update({k: max(float(f["iou"]) for f in fs) for k, fs in sorted(full.items()) if fs})
        st: Dict[str, object] = {"screened": [[_key(ws), v] for v, ws in scr[:6]]}
        if not full:
            st["skipped"] = f"no union screened at {SCREEN_MIN} or more"
        else:
            b = best_over(full, single_best)
            members = _members(b["target"])
            placed = apply(sheet, pose_of(b["fit"]), origin)
            placed_lines = [(d, apply(l, pose_of(b["fit"]), origin)) for d, l in lines]
            split = split_per_lake(placed, placed_lines, {w: geoms[w] for w in members})
            drawn = all(split["per_lake"][str(w)]["lake_covered"] >= DRAWN_MIN for w in members)
            lifts = better(b["fit"]["grade"], cur["grade"])
            held = {f"wbk:{w}": sorted(taken[w]) for w in members if taken and w in taken}
            st.update(b, lifts=lifts, every_lake_drawn=drawn, split=split,
                      adopted=lifts and not b["ambiguous"] and drawn and not held)
            if held:
                st["taken_by"] = held          # a union member another sheet ends on: refused, listed
            if st["adopted"]:
                adopt("two_lake", b["target"], b["fit"], unary_union([geoms[w] for w in members]))
                flags.append("two_lake")
                if b["fit"]["mirror"]:
                    flags.append("mirror")
            elif lifts:
                flags.append("two_lake_ambiguous" if b["ambiguous"] else
                             "two_lake_not_drawn" if not drawn else "two_lake_taken")
        steps["two_lake"] = st
    classes["two_lake"] = cur["grade"]

    # 3. a wrong-lake code: the lakes within WIDE_M by shape, only when the sheet still fits poorly
    if cur["grade"] != "poor":
        steps["other_lake"] = {"skipped": f"{cur['grade']} on its coded lake"}
    else:
        wide = {w: e for w, e in sorted(near_lakes(coded_u.bounds, WIDE_M).items()) if w not in coded}
        ranked = shape_rank(sheet, {w: e["geom"] for w, e in wide.items()})
        scr = sorted(((screen(sheet, wide[w]["geom"], origin), w) for w, _d in ranked), key=lambda t: (-t[0], t[1]))
        chosen = [w for v, w in scr if v >= SCREEN_MIN][:SEARCHED]
        st = {"lakes_in_radius": len(wide), "shape_ranked": len(ranked),
              "screened": [[f"wbk:{w}", v] for v, w in scr[:6]]}
        if not chosen:
            st["skipped"] = f"no lake within {WIDE_M / 1000:g} km screened at {SCREEN_MIN} or more"
        else:
            for w in chosen:
                geoms[w] = wide[w]["geom"]
                info[w] = {"name": wide[w].get("name"), "layer": wide[w].get("layer"),
                           "dist_m": round(wide[w]["geom"].distance(coded_u))}
            full = {f"wbk:{w}": search(sheet, geoms[w], origin) for w in chosen}
            target_best.update({k: max(float(f["iou"]) for f in fs) for k, fs in sorted(full.items()) if fs})
            b = best_over(full, single_best)
            lifts = better(b["fit"]["grade"], cur["grade"])
            w = _members(b["target"])[0]
            nc = name_check(info[w]["name"], titles, [coded[x].get("name") for x in sorted(coded)], info[w]["dist_m"])
            held = sorted((taken or {}).get(w, ()))
            st.update(b, lifts=lifts, dist_m=info[w]["dist_m"], name=info[w]["name"], name_check=nc,
                      adopted=lifts and not b["ambiguous"] and nc in NAME_CONFIRMS and not held)
            if held:
                st["taken_by"] = held          # another sheet ends on that lake: the find is refused, listed
            if st["adopted"]:
                adopt("other_lake", b["target"], b["fit"], geoms[w])
                flags.append("other_lake")
                if b["fit"]["mirror"]:
                    flags.append("mirror")
            elif lifts:
                flags.append("other_lake_ambiguous" if b["ambiguous"] else
                             f"other_lake_name_{nc}" if nc not in NAME_CONFIRMS else "other_lake_taken")
        steps["other_lake"] = st
    classes["other_lake"] = cur["grade"]

    # 4. a small stretch (<= 5 %), its own flagged class; then the bigger stretch, never adopted. Both mirrors are
    # searched (the mirror is held only when an earlier step adopted a placing); the rivals are the other turns, the
    # other mirror and the best fit on every other target an earlier step searched (the same rule as every step).
    mirror = bool(cur["fit"].get("mirror", False))
    would_help = None
    if cur["grade"] == "strict":
        steps["small_stretch"] = {"skipped": "already strict"}
    else:
        fits = search(sheet, cur["geom"], origin, scales=SMALL_SCALES, bounds=(1 - SMALL_STRETCH, 1 + SMALL_STRETCH))
        pool = [f for f in fits if f["mirror"] == mirror] if cur["step"] != "baseline" else fits
        b = pool[0]
        # the other turns and the mirror are stretched too: against the stretched fit. The other targets were
        # searched without a stretch: against the placing before the stretch (a stretch must not out-score them)
        v = verdict(b, rivals_of(b, fits))
        vt = verdict(cur["fit"], [(f"target {k}", x) for k, x in sorted(target_best.items()) if k != cur["target"]])
        if vt["rival"] is not None and (v["rival"] is None or vt["gap"] < v["gap"]):
            v = vt
        lifts = better(b["grade"], cur["grade"])
        steps["small_stretch"] = dict(v, target=cur["target"], fit=b, before=dict(cur["fit"]), lifts=lifts,
                                      adopted=lifts and not v["ambiguous"])
        if steps["small_stretch"]["adopted"]:
            adopt("small_stretch", cur["target"], b, cur["geom"])
            flags.append("small_stretch")
            if b["mirror"]:
                flags.append("mirror")
            if abs(float(b["scale"]) - 1) >= SMALL_STRETCH - 0.001:
                flags.append("small_stretch_at_limit")       # it wanted more: a bigger stretch may fit better still
        elif lifts:
            flags.append("small_stretch_ambiguous")
    classes["small_stretch"] = cur["grade"]
    if cur["grade"] != "strict":
        found = []
        for mode, scales, bounds in stretch_modes(units, A / cur["geom"].area):
            for f in search(sheet, cur["geom"], origin, mirrors=(mirror,), scales=scales, bounds=bounds)[:STARTS]:
                found.append((mode, f))
        found.sort(key=lambda mf: (-mf[1]["iou"], mf[0], mf[1]["angle_deg"]))
        mode, b = found[0]
        same = [f for m, f in found if m == mode]
        v = verdict(b, rivals_of(b, same))
        if better(b["grade"], cur["grade"]) and abs(float(b["scale"]) - 1) > SMALL_STRETCH + 1e-3:
            would_help = dict(v, mode=mode, target=cur["target"], before=dict(cur["fit"]), fit=b,
                              change=stretch_change(cur["fit"], b))
            flags.append("bigger_stretch_would_help")
    steps["big_stretch"] = would_help or {"helps": False}

    if mirror and "mirror" not in flags and cur["step"] != "baseline":
        flags.append("mirror")
    return {"origin": [round(origin[0], 3), round(origin[1], 3)], "steps": steps, "classes": classes,
            "after": {"grade": cur["grade"], "step": cur["step"], "target": cur["target"], "fit": cur["fit"]},
            "flags": sorted(set(flags)), "would_help": would_help,
            "lakes": {f"wbk:{w}": info[w] for w in sorted(info)}, "_geoms": geoms}
