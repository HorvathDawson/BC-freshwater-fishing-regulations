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


def read_layer(stream_file: Path, sids: Iterable[int], lake_file: Optional[Path] = None) -> tuple:
    """ONE PASS over the tiles' `stream` layer file (and the `lake` file, when given) for `sids`: ({section: its line's
    parts}, {sections written twice}, {sections drawn as a polygon reach}) — what `sides_of` answers from. (Review #112:
    the stream layer is ~2.5 GB; a build reads it once for all its one-bank places, not once per place.)"""
    want = set(sids)
    lines: Dict[int, list] = {}
    twice: set = set()
    with Path(stream_file).open() as fh:
        for line in fh:
            sid = _section_of(line)
            if sid not in want:
                continue
            if sid in lines:
                twice.add(sid)
                continue
            g = json.loads(line)["geometry"]
            if g["type"] not in ("LineString", "MultiLineString"):
                raise ValueError(f"{stream_file}: section {sid} is a {g['type']} in the stream layer, not a line")
            lines[sid] = [g["coordinates"]] if g["type"] == "LineString" else g["coordinates"]
    poly = polygon_sections(lake_file, want) if lake_file is not None else frozenset()
    return lines, frozenset(twice), poly


def sides_of(read: tuple, sections: Mapping[int, str]) -> Dict[int, Side]:
    """{section id: its Side} for `sections` ({id: bank word}) from a `read_layer` read that holds them all. Every
    section asked for gets an answer or a typed reason."""
    for w in set(sections.values()):
        if w not in WORDS:
            raise ValueError(f"bank word {w!r}: not one of {sorted(WORDS)}")
    lines, twice, poly = read
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


def sides_from_layer(stream_file: Path, sections: Mapping[int, str], lake_file: Optional[Path] = None) -> Dict[int, Side]:
    """{section id: its Side} for `sections` ({id: bank word}), read from the tiles' `stream` layer file (and, when given,
    the `lake` layer file, for the polygon reaches). Every section asked for gets an answer or a typed reason."""
    for w in set(sections.values()):
        if w not in WORDS:
            raise ValueError(f"bank word {w!r}: not one of {sorted(WORDS)}")
    return sides_of(read_layer(stream_file, sections, lake_file), sections)


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
    places = [(lid, pl, pl["side"]) for lid, pl in sorted((payload.get("p") or {}).items())
              if isinstance(pl, dict) and pl.get("side") and beside.get(lid)]
    # one pass over the layers for every place (review #112), not one per place
    read = read_layer(Path(layers) / "stream.geojsonl", {s for lid, _pl, _w in places for s in beside[lid]},
                      Path(layers) / "lake.geojsonl") if places else None
    for lid, pl, word in places:
        sides = sides_of(read, {s: word for s in beside[lid]})
        ok, bad = report(sides)
        if ok:
            # (review #112) the app's decode refuses a `bk` place whose OWN chain (an `unmapped` chain whose places are
            # exactly [lid]) is not exactly one (core `salmon.ts`): refused here first, never shipped
            own = sorted({u["chain"] for u in payload.get("unmapped") or []
                          if u["chain"] < len(chains) and list(chains[u["chain"]].get("l") or []) == [lid]})
            if len(own) != 1:
                raise ValueError(f"salmon: one-bank place {lid} has {len(own)} own chain(s) in unmapped {own} (l exactly "
                                 f"[{lid}]), not 1: the app refuses such a file")
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


#: (T1029 Q6) why a rule book `side` rule draws no cue on the Standard map (the card still says it)
BOOK_DATED = "dated_side_rule"            # the map has no day for a rule book side rule: its dates cannot be drawn
BOOK_NOT_CLOSURE = "side_rule_not_a_closure"  # the cue says "this half is closed"; any other rule would be a new look


def stamp_book_sides(payload: dict, db, layers: Path) -> Dict[str, Dict[str, list]]:
    """(T1029 Q6, owner 2026-11-24: "Yes, stripe it too") THE RULE BOOK'S OWN `side` RULES ("No Fishing on the west half
    of river …", `kitimat_river.r1`), drawn as look B on the Standard / Trout map: per section a closure worded for one
    half binds, which side of the drawn line that half is — `payload["bs"] = {"<section>": +1 | -1}` (additive: the salmon
    payload is the release's one JSON file the Map tab reads in every colouring; an app that does not read `bs` draws as
    before). Only an UNDATED CLOSURE is drawn (no `when`, nothing kept): a dated one, or any other kind, is reported with a
    typed reason (`BOOK_DATED`, `BOOK_NOT_CLOSURE`) and draws nothing; a section whose side cannot be read too (`Why`).
    Two side rules on one section naming different halves stop the build. Returns {rule: {reason: [sections]}}."""
    import json as _json
    rules = db.execute("SELECT entry_id, rule_id, conditions, when_, take, parts FROM rule "
                       "WHERE conditions LIKE ? ORDER BY entry_id, rule_id", ('%"side"%',)).fetchall()
    rows = []
    for entry, rule, cond, when, take, parts in rules:
        sets = [x for (x,) in db.execute("SELECT set_id FROM ruleset WHERE entry_id = ? AND rule_id = ?", (entry, rule))]
        if not sets:
            continue
        q = ",".join("?" * len(sets))
        for (sid,) in db.execute(f"SELECT sid FROM section_ruleset WHERE set_id IN ({q}) ORDER BY sid", sets):
            rows.append((sid, entry, rule, cond, when, take, parts))
    out: Dict[str, Dict[str, list]] = {}
    want: Dict[str, Dict[int, str]] = {}
    for sid, entry, rule, cond, when, take, parts in rows:
        word = (_json.loads(cond or "{}") or {}).get("side")
        if not word:
            continue
        key = f"{entry}::{rule}"
        what = ((_json.loads(parts or "{}") or {}).get("what") or "").lower()
        if when:
            out.setdefault(key, {}).setdefault(BOOK_DATED, []).append(sid)
        elif take != 0 or not what.startswith("no fishing"):
            out.setdefault(key, {}).setdefault(BOOK_NOT_CLOSURE, []).append(sid)
        else:
            want.setdefault(key, {})[sid] = word
    signs: Dict[int, int] = {}
    for key, secs in sorted(want.items()):
        ok, bad = report(sides_from_layer(Path(layers) / "stream.geojsonl", secs, Path(layers) / "lake.geojsonl"))
        for reason, ss in bad.items():
            out.setdefault(key, {}).setdefault(reason, []).extend(ss)
        for s, v in ok.items():
            if signs.get(s, v) != v:
                raise ValueError(f"B.C. Synopsis: section {s} has side rules naming both halves ({key} and another)")
            signs[s] = v
    if signs:
        payload["bs"] = {str(s): v for s, v in sorted(signs.items())}
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
