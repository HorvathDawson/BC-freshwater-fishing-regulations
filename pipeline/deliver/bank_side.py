"""(T1029, PROTOTYPE for the owner's pictures; draft PR, not wired into the bundle) WHICH SIDE OF THE DRAWN LINE A BANK IS.

A DFO place for one bank ("on west bank between signs at Kitimat hatchery outfall", T754) or a synopsis `side` rule
("west half of river") names a compass bank. The map can only draw a one-side stripe as an OFFSET from the line it
draws (`line-offset`: positive is RIGHT of the line's own direction, as it is stored in the tiles). So per section the
pipeline must say +1 (the bank is right of the drawn line) or -1 (left), measured on the EXACT exported geometry (the
`stream` layer file the tiles are built from), never assumed from the river's flow: FWA lines most likely run upstream
(T754 §3 B), and the Kitimat's section at the signs does (written d/s sign first, u/s sign last).

The rule, per segment of the drawn line: its right-hand normal (local metres at its latitude). The bank's compass word
gives a direction (west = (-1, 0), north = (0, 1), ...). The bank is RIGHT where the right normal points the word's way,
LEFT where it points away. Weighted by length over the section. AMBIGUOUS (no answer, never a guess): where the drawn
line runs within `AXIS_DEG` of the word's own axis (a "west bank" on a river flowing west or east), or the segments
disagree (a meander whose pieces give opposite answers, share under `AGREE`).
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path

#: the compass words a bank is named by, as (east, north) unit vectors
WORDS = {"west": (-1.0, 0.0), "east": (1.0, 0.0), "north": (0.0, 1.0), "south": (0.0, -1.0)}
#: a segment running within this many degrees of the word's axis cannot say which bank the word means
AXIS_DEG = 30.0
#: the share of the section's length (of the segments that can say) that must agree
AGREE = 0.8


@dataclass(frozen=True)
class Side:
    """+1 right / -1 left of the drawn line, or 0: no answer (`why` says which)."""
    sign: int
    share: float          #: the share of the deciding length on the winning side (0 when nothing decides)
    why: str = ""


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
    """`bank_side` over every part of a multi-line (a polygon reach's route can be written in pieces), each part as
    written."""
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
        return Side(0, 0.0, f"the line runs along the {word}'s axis (within {AXIS_DEG:g} degrees) everywhere")
    share = max(right, left) / total
    if share < AGREE:
        return Side(0, share, f"the line's pieces disagree ({share:.0%} on one side, under {AGREE:.0%})")
    return Side(1 if right > left else -1, share)


def sides_from_layer(stream_file: Path, sections: dict[int, str]) -> dict[int, Side]:
    """{section id: its Side} for `sections` ({id: bank word}), read from the `stream` layer file as the tiles are built
    from it. A section with no line there, or more than one, is refused (the stripe would be drawn on a line it was not
    measured on)."""
    lines: dict[int, list] = {}
    with stream_file.open() as fh:
        for line in fh:
            i = line.rfind('"properties":')
            sid = json.loads("{" + line[i:])["properties"].get("section_id")
            if sid in sections:
                if sid in lines:
                    raise SystemExit(f"{stream_file}: section {sid} is written twice — measure each line, not one")
                g = json.loads(line)["geometry"]
                if g["type"] not in ("LineString", "MultiLineString"):
                    raise SystemExit(f"{stream_file}: section {sid} is a {g['type']}, not a line")
                lines[sid] = [g["coordinates"]] if g["type"] == "LineString" else g["coordinates"]
    gone = sorted(set(sections) - set(lines))
    if gone:
        raise SystemExit(f"{stream_file}: no line for section(s) {gone}")
    return {sid: bank_side_of_parts(lines[sid], word) for sid, word in sections.items()}


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("stream_file", type=Path, help="the tiles' stream layer file (layers/stream.geojsonl)")
    ap.add_argument("word", choices=sorted(WORDS))
    ap.add_argument("sections", type=int, nargs="+")
    a = ap.parse_args()
    res = sides_from_layer(a.stream_file, {s: a.word for s in a.sections})
    print(json.dumps({str(s): {"bk": v.sign, "share": round(v.share, 3), "why": v.why} for s, v in res.items()}, indent=1))
