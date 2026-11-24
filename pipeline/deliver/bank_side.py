"""(T1029) WHICH SIDE OF THE DRAWN LINE A BANK IS — for the one-bank cue on the map (owner 2026-11-24, look B).

A DFO place for one bank ("on west bank between signs at Kitimat hatchery outfall", T754) or a synopsis `side` rule
("west half of river") names a compass bank. The map can only draw a one-side cue as an OFFSET from the line it draws
(`line-offset`: positive is RIGHT of the line's own direction, as it is stored in the tiles). So per section the
pipeline says +1 (the bank is right of the drawn line) or -1 (left), measured on the EXACT exported geometry (the
`stream` layer file the tiles are built from), never assumed from the river's flow: FWA lines most likely run upstream
(T754 §3 B), and the Kitimat's section at the signs does (written d/s sign first, u/s sign last).

The rule, per segment of the drawn line: its right-hand normal (local metres at its latitude). The bank's compass word
gives a direction (west = (-1, 0), north = (0, 1), ...). The bank is RIGHT where the right normal points the word's way,
LEFT where it points away. Weighted by length over the section.

NEVER A GUESS: a section that cannot be answered gets NO cue and a TYPED reason (`Why`), which the caller reports:
  along_axis        the drawn line runs within `AXIS_DEG` of the word's own axis everywhere ("west bank" on a river
                    flowing west or east)
  pieces_disagree   the segments that can say give opposite answers (a meander; the winning share under `AGREE`)
  no_line           the section has no line in the stream layer file (the tiles were cut from another atlas)
  written_twice     the section is written on two lines of the file (measure each line, never one)
  polygon_reach     the section is drawn as a POLYGON (the river wide enough to be a polygon reach): its cue belongs
                    on the polygon's bank edge (owner 2026-11-24 Q4, "Along the bank edge"), which needs a tile layer
                    of its own — designed, not built (T1029); reported so the day one appears it is seen
Deterministic: sections in sorted order, only the file's bytes read, no clock.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Dict, Iterable, Mapping, Optional, Tuple

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


def _section_of(line: str) -> Optional[int]:
    """A layer line's `section_id` (the properties object is the line's last key, as the export writes it)."""
    i = line.rfind('"properties":')
    if i < 0:
        return None
    return json.loads("{" + line[i:].rstrip().rstrip(",")).get("properties", {}).get("section_id")


def polygon_sections(lake_file: Path, sections: Iterable[int]) -> frozenset:
    """The `sections` drawn as a polygon reach: a feature of the tiles' `lake` layer with `water: stream` (a river wide
    enough to be a polygon), which the stream layer only routes through."""
    want, out = set(sections), set()
    if not want:
        return frozenset()
    with Path(lake_file).open() as fh:
        for line in fh:
            sid = _section_of(line)
            if sid in want and '"water":"stream"' in line:
                out.add(sid)
    return frozenset(out)


def sides_from_layer(stream_file: Path, sections: Mapping[int, str], lake_file: Optional[Path] = None) -> Dict[int, Side]:
    """{section id: its Side} for `sections` ({id: bank word}), read from the tiles' `stream` layer file (and, when given,
    the `lake` layer file, for the polygon reaches). Every section asked for gets an answer or a typed reason."""
    for w in set(sections.values()):
        if w not in WORDS:
            raise ValueError(f"bank word {w!r}: not one of {sorted(WORDS)}")
    lines: Dict[int, list] = {}
    twice: set = set()
    with Path(stream_file).open() as fh:
        for line in fh:
            sid = _section_of(line)
            if sid not in sections:
                continue
            if sid in lines:
                twice.add(sid)
                continue
            g = json.loads(line)["geometry"]
            if g["type"] not in ("LineString", "MultiLineString"):
                raise ValueError(f"{stream_file}: section {sid} is a {g['type']} in the stream layer, not a line")
            lines[sid] = [g["coordinates"]] if g["type"] == "LineString" else g["coordinates"]
    poly = polygon_sections(lake_file, sections) if lake_file is not None else frozenset()
    out: Dict[int, Side] = {}
    for sid in sorted(sections):
        if sid in poly:
            out[sid] = Side(0, 0.0, Why.polygon_reach)
        elif sid in twice:
            out[sid] = Side(0, 0.0, Why.written_twice)
        elif sid not in lines:
            out[sid] = Side(0, 0.0, Why.no_line)
        else:
            out[sid] = bank_side_of_parts(lines[sid], sections[sid])
    return out


def report(sides: Mapping[int, Side], label: str = "") -> Tuple[Dict[int, int], Dict[str, list]]:
    """({section: +1/-1} for the sections with a side, {reason: [sections]} for those without): what the bundle
    ships, and what the build prints (never a silent skip)."""
    ok = {s: v.sign for s, v in sorted(sides.items()) if v.sign}
    bad: Dict[str, list] = {}
    for s, v in sorted(sides.items()):
        if not v.sign:
            bad.setdefault(v.why.value, []).append(s)
    return ok, bad


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("layers", type=Path, help="the tiles' layers directory (stream.geojsonl, lake.geojsonl)")
    ap.add_argument("word", choices=sorted(WORDS))
    ap.add_argument("sections", type=int, nargs="+")
    a = ap.parse_args()
    res = sides_from_layer(a.layers / "stream.geojsonl", {s: a.word for s in a.sections}, a.layers / "lake.geojsonl")
    print(json.dumps({str(s): {"bk": v.sign, "share": round(v.share, 3), "why": v.why and v.why.value}
                      for s, v in res.items()}, indent=1))
