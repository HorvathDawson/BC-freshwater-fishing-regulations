"""Label placement for the map (T853): numbered callouts and boxed labels with leader lines, no two overlapping.

Pure geometry in display pixels (x right, y up), no matplotlib: the renderer hands it the axes frame, the lines it
drew (sampled into a coarse density grid, so a label avoids sitting on a river) and the boxes already taken, and asks
for a spot near an anchor. Candidates ring the anchor at growing distances in 16 directions; a candidate that leaves
the frame or touches a placed box is refused, the rest are scored by the lines they cover, their distance and the
leaders they cross. Deterministic: same inputs, same spots.
"""
from __future__ import annotations

import math
from typing import List, Optional, Sequence, Tuple

import numpy as np

Box = Tuple[float, float, float, float]            # x0, y0, x1, y1
Seg = Tuple[float, float, float, float]

RADII = (26, 40, 58, 80, 108, 140, 180, 230, 290, 360, 440)
ANGLES = tuple(i * math.pi / 8 for i in (2, 1, 3, 0, 4, 15, 5, 14, 6, 13, 7, 12, 8, 11, 9, 10))


def overlaps(a: Box, b: Box, pad: float = 0.0) -> bool:
    return not (a[2] + pad <= b[0] or b[2] + pad <= a[0] or a[3] + pad <= b[1] or b[3] + pad <= a[1])


def _ccw(ax, ay, bx, by, cx, cy):
    return (cy - ay) * (bx - ax) > (by - ay) * (cx - ax)


def crosses(s: Seg, t: Seg) -> bool:
    a, b, c, d = (s[0], s[1]), (s[2], s[3]), (t[0], t[1]), (t[2], t[3])
    return (_ccw(*a, *c, *d) != _ccw(*b, *c, *d)) and (_ccw(*a, *b, *c) != _ccw(*a, *b, *d))


def seg_hits_box(s: Seg, b: Box) -> bool:
    """Whether a leader passes through a box (either end inside, or crossing an edge)."""
    for x, y in ((s[0], s[1]), (s[2], s[3])):
        if b[0] < x < b[2] and b[1] < y < b[3]:
            return True
    edges = ((b[0], b[1], b[2], b[1]), (b[2], b[1], b[2], b[3]), (b[2], b[3], b[0], b[3]), (b[0], b[3], b[0], b[1]))
    return any(crosses(s, e) for e in edges)


def nearest_on_box(x: float, y: float, b: Box) -> Tuple[float, float]:
    """The point of box `b` nearest (x, y) — where a leader from (x, y) meets the label."""
    return min(max(x, b[0]), b[2]), min(max(y, b[1]), b[3])


class Placer:
    def __init__(self, frame: Box, cell: float = 6.0):
        self.frame = frame
        self.cell = cell
        self.nx = max(1, int((frame[2] - frame[0]) / cell) + 1)
        self.ny = max(1, int((frame[3] - frame[1]) / cell) + 1)
        self.grid = np.zeros((self.ny, self.nx), dtype=np.float32)
        self.boxes: List[Box] = []
        self.segs: List[Seg] = []
        self._integral: Optional[np.ndarray] = None

    # ---- what is already drawn
    def add_line(self, pts: Sequence[Tuple[float, float]], weight: float = 1.0) -> None:
        """A drawn line, in display pixels: sampled every ~cell/2 px into the density grid."""
        self._integral = None
        for (x0, y0), (x1, y1) in zip(pts[:-1], pts[1:]):
            n = max(1, int(math.hypot(x1 - x0, y1 - y0) / (self.cell / 2)))
            for k in range(n + 1):
                x = x0 + (x1 - x0) * k / n
                y = y0 + (y1 - y0) * k / n
                i = int((x - self.frame[0]) / self.cell)
                j = int((y - self.frame[1]) / self.cell)
                if 0 <= i < self.nx and 0 <= j < self.ny:
                    self.grid[j, i] += weight

    def block(self, b: Box) -> None:
        self.boxes.append(b)

    def _ii(self) -> np.ndarray:
        if self._integral is None:
            g = np.minimum(self.grid, 4.0)
            self._integral = np.pad(g.cumsum(0).cumsum(1), ((1, 0), (1, 0)))
        return self._integral

    def line_cost(self, b: Box) -> float:
        ii = self._ii()
        i0 = int(max(0, (b[0] - self.frame[0]) / self.cell))
        i1 = int(min(self.nx, (b[2] - self.frame[0]) / self.cell + 1))
        j0 = int(max(0, (b[1] - self.frame[1]) / self.cell))
        j1 = int(min(self.ny, (b[3] - self.frame[1]) / self.cell + 1))
        if i1 <= i0 or j1 <= j0:
            return 0.0
        return float(ii[j1, i1] - ii[j0, i1] - ii[j1, i0] + ii[j0, i0])

    def inside(self, b: Box, margin: float = 4.0) -> bool:
        f = self.frame
        return b[0] >= f[0] + margin and b[1] >= f[1] + margin and b[2] <= f[2] - margin and b[3] <= f[3] - margin

    # ---- placing
    def place(self, ax: float, ay: float, w: float, h: float, *, circle: bool = False, pad: float = 3.0,
              radii: Sequence[float] = RADII, line_weight: float = 6.0, near: float = 0.04,
              max_line_cost: Optional[float] = None) -> Optional[Box]:
        """The best free box of size w x h near anchor (ax, ay), or None when none is free. A circle's box is centred
        on its spot; a label box hangs off the spot on the side facing away from the anchor."""
        best, best_cost = None, None
        # the anchor's own marker (a break's dot) never blocks the leader that starts inside it
        others = [o for o in self.boxes if not (o[0] <= ax <= o[2] and o[1] <= ay <= o[3])]
        for r in radii:
            for th in ANGLES:
                dx, dy = math.cos(th), math.sin(th)
                cx, cy = ax + r * dx, ay + r * dy
                if not circle:
                    cx += (w / 2) * (1 if dx > 0.35 else -1 if dx < -0.35 else 0)
                    cy += (h / 2) * (1 if dy > 0.35 else -1 if dy < -0.35 else 0)
                b = (cx - w / 2, cy - h / 2, cx + w / 2, cy + h / 2)
                if not self.inside(b) or any(overlaps(b, o, pad) for o in self.boxes):
                    continue
                ex, ey = (cx, cy) if circle else nearest_on_box(ax, ay, b)
                leader = (ax, ay, ex, ey)
                if any(seg_hits_box(leader, o) for o in others):
                    continue
                lc = self.line_cost(b)
                if max_line_cost is not None and lc > max_line_cost:
                    continue
                cost = lc * line_weight + r * near * 10 + 40 * sum(crosses(leader, s) for s in self.segs)
                if best_cost is None or cost < best_cost - 1e-9:
                    best, best_cost = (b, leader), cost
            if best is not None and best_cost is not None and best_cost < r * near * 10 + 1.0:
                break           # a clean spot at this distance: nothing farther can beat it
        if best is None:
            return None
        b, leader = best
        self.boxes.append(b)
        if math.hypot(leader[2] - leader[0], leader[3] - leader[1]) > 2:
            self.segs.append(leader)
        return b
