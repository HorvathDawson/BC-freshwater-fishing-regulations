"""(T1029) THE BANK-SIDE RULE, shared by the atlas build (`StreamNode.drawn_banks`) and the bundle (`deliver.bank_side`):
which side of a DRAWN line a compass bank is, measured on the line as written, never assumed. The rule and its typed
reasons are documented in `pipeline/deliver/bank_side.py`; this module is the one implementation of the arithmetic.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import numpy as np

#: the compass words a bank is named by, as (east, north) unit vectors
WORDS = {"west": (-1.0, 0.0), "east": (1.0, 0.0), "north": (0.0, 1.0), "south": (0.0, -1.0)}
#: a segment running within this many degrees of the word's axis cannot say which bank the word means
AXIS_DEG = 30.0
#: the share of the section's length (of the segments that can say) that must agree
AGREE = 0.8


class Why(str, Enum):
    """Why a section has no side (module docstring)."""
    along_axis = "along_axis"
    pieces_disagree = "pieces_disagree"
    no_line = "no_line"
    written_twice = "written_twice"
    polygon_reach = "polygon_reach"


@dataclass(frozen=True)
class Side:
    """+1 right / -1 left of the drawn line, or 0: no answer, and `why` (a `Why`) says which."""
    sign: int
    share: float          #: the share of the deciding length on the winning side (0 when nothing decides)
    why: Optional[Why] = None

    def __post_init__(self):
        if (self.sign == 0) != (self.why is not None):
            raise ValueError(f"a side is +1/-1 with no reason, or 0 with one: {self.sign} {self.why}")


def _segments(coords: list) -> list[tuple[float, float, float]]:
    """(dx, dy, length) in local metres for each segment of a WGS84 line."""
    out = []
    for (x0, y0), (x1, y1) in zip(coords, coords[1:]):
        k = math.cos(math.radians((y0 + y1) / 2))
        dx, dy = (x1 - x0) * 111_320 * k, (y1 - y0) * 110_540
        n = math.hypot(dx, dy)
        if n > 0:
            out.append((dx, dy, n))
    return out


def bank_side(coords: list, word: str) -> Side:
    """Which side of the line drawn through `coords` (as written, first point first) the `word` bank is."""
    return bank_side_of_parts([coords], word)


def bank_side_of_parts(parts: list[list], word: str) -> Side:
    """`bank_side` over every part of a multi-line (a route can be written in pieces), each part as written."""
    if word not in WORDS:
        raise ValueError(f"bank word {word!r}: not one of {sorted(WORDS)}")
    wx, wy = WORDS[word]
    sin_axis = math.sin(math.radians(AXIS_DEG))
    right = left = 0.0
    for dx, dy, n in (seg for coords in parts for seg in _segments(coords)):
        # the right-hand normal of (dx, dy) is (dy, -dx); its dot with the word's direction, per metre
        along = (dy * wx - dx * wy) / n
        if abs(along) < sin_axis:           # the segment runs along the word's axis: it cannot tell the banks apart
            continue
        if along > 0:
            right += n
        else:
            left += n
    total = right + left
    if total == 0:
        return Side(0, 0.0, Why.along_axis)
    share = max(right, left) / total
    if share < AGREE:
        return Side(0, share, Why.pieces_disagree)
    return Side(1 if right > left else -1, share)


def drawn_banks(parts: list[list]) -> str:
    """The two-character code `StreamNode.drawn_banks` stores for a line drawn through `parts` (each as written): the
    "east" word's side, then the "north" word's — "R" / "L" / "-" — by the same rule as `bank_side_of_parts`, in numpy
    (the atlas stamps every stream piece). "west" / "south" are the opposite sides."""
    xs, ys = [], []
    for coords in parts:
        a = np.asarray(coords, dtype=float)[:, :2]
        if len(a) < 2:
            continue
        xs.append(a[:, 0])
        ys.append(a[:, 1])
    if not xs:
        return "--"
    dx_l, dy_l = [], []
    for x, y in zip(xs, ys):
        k = np.cos(np.radians((y[:-1] + y[1:]) / 2))
        dx_l.append((x[1:] - x[:-1]) * 111_320 * k)
        dy_l.append((y[1:] - y[:-1]) * 110_540)
    dx, dy = np.concatenate(dx_l), np.concatenate(dy_l)
    n = np.hypot(dx, dy)
    keep = n > 0
    dx, dy, n = dx[keep], dy[keep], n[keep]
    sin_axis = math.sin(math.radians(AXIS_DEG))
    out = ""
    for wx, wy in (WORDS["east"], WORDS["north"]):
        along = (dy * wx - dx * wy) / n if len(n) else n
        says = np.abs(along) >= sin_axis
        right = float(n[says & (along > 0)].sum())
        left = float(n[says & (along < 0)].sum())
        total = right + left
        if total == 0 or max(right, left) / total < AGREE:
            out += "-"
        else:
            out += "R" if right > left else "L"
    return out


def side_from_code(code: str, word: str) -> int:
    """+1 right / -1 left / 0 none: `word`'s side of the drawn line from a `drawn_banks` code."""
    if word not in WORDS:
        raise ValueError(f"bank word {word!r}: not one of {sorted(WORDS)}")
    if len(code) != 2:
        return 0
    c = code[0] if word in ("east", "west") else code[1]
    s = {"R": 1, "L": -1}.get(c, 0)
    return -s if word in ("west", "south") else s
