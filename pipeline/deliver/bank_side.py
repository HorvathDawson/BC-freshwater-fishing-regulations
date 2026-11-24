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
from pathlib import Path
from typing import Dict, Iterable, Mapping, Optional, Tuple

from pipeline.common.bank_side import (AGREE, AXIS_DEG, WORDS, Side, Why, bank_side,  # noqa: F401  (re-exported)
                                      bank_side_of_parts)


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


def stamp_payload(payload: dict, sections, layers: Path) -> Dict[str, Dict[str, list]]:
    """(T1029, look B) THE ONE-BANK CUE'S DATA, in the salmon payload: for every DFO place worded for one bank
    (`p[lid]["side"]`), on each section it stands BESIDE (its lid in the section's chain's `u`), which side of the
    drawn line its bank is — `p[lid]["bk"] = {"<section>": +1 | -1}`, measured on the tiles' own `layers/stream.geojsonl`
    (and `lake.geojsonl`: a polygon reach is reported, never striped). Additive: an app that does not read `bk` draws as
    before. Returns {lid: {reason: [sections]}} for every section with no side (the build prints it), never a guess."""
    beside: Dict[str, list] = {}
    chains = payload.get("chains") or []
    for sid, c in sorted(sections):
        for lid in (chains[c].get("u") or []) if c < len(chains) else []:
            beside.setdefault(lid, []).append(sid)
    out: Dict[str, Dict[str, list]] = {}
    taken: set = set()
    for lid, pl in sorted((payload.get("p") or {}).items()):
        word = pl.get("side") if isinstance(pl, dict) else None
        if not word or not beside.get(lid):
            continue
        sides = sides_from_layer(Path(layers) / "stream.geojsonl", {s: word for s in beside[lid]},
                                 Path(layers) / "lake.geojsonl")
        ok, bad = report(sides)
        if ok:
            pl["bk"] = {str(s): v for s, v in sorted(ok.items())}
            twice = sorted(set(ok) & taken)
            if twice:
                # the app refuses such a file (core `salmon.ts`): stop here, naming them, never ship it
                raise ValueError(f"salmon: section(s) {twice} stand beside two one-bank places ({lid} and another): "
                                 "one cue per section — the curated places overlap")
            taken |= set(ok)
        if bad:
            out[lid] = bad
    return out


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
