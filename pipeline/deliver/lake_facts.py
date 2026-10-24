"""LAKE FACTS — PREPARED, NOT FED. Per lake SECTION, a survey state and a last stocked day: what the map's Depth and
Stocked lake modes colour by, through the one colouring engine, from DATA ALONE (the tiles' lake features carry only
`section_id, name, water, area_m2`, so a new release recolours the lakes with no tile change).

OWNER 2026-10-21 (handoff/PLAN/app-integration/decisions.md): "we dont have the stocking and bathy map data in a feed
yet. but we just want it prepared for it when we do have the feed." So this module holds only what is source-agnostic:
the file format (`lakes/1`), the binding rule a feed's identifiers must go through, and the per-section fold. NOTHING
produces a lakes file today: there is no fetch, `deliver all` makes none, and the release ships no `lakes` unless
`python -m pipeline.deliver.release --lake-facts FILE` is handed one. The app greys its Stocked and Depth toggles while
the loaded release carries no lake values and enables them, with no app change, when one does.

WHAT A FUTURE FEED MUST PROVIDE (a first lane used the FFSBC stocking report and the BC Data Catalogue's survey-sheet
rosters; both were dropped — the first has no open licence, the catalogue entries are "Access Only"):
  * a licence that permits redistributing a derived per-lake value;
  * per water, an identifier the FWA lakes layer carries — the 50K waterbody identifier (`00330VICT`) is
    `WATERBODY_KEY_GROUP_CODE_50K` in the lakes layer of `bc_fisheries_data.gpkg`, and the lake's section is
    `lake:<WATERBODY_KEY>` in the atlas's handle table (`pipeline.common.section_handles`) — PLUS the water's name(s);
  * survey: per water, none / sheet (a bathymetric sheet exists) / digitised (the sheet was traced into contours);
  * stocking: per water, the release dates (ISO days); the file keeps the LATEST release on or before the feed's own
    as-of day, and that as-of day itself (the app bands "how long ago" from today, `stock_recency`).
  The output is keyed by section_id (integer handle), never by name: values are pinned to the bundle's
  `section_handles`, and the release refuses a file built against another atlas.

THE BINDING RULE (`bind`, learned the hard way): a 50K code can cover several FWA lakes. A source name that agrees with
one of the FWA's three gazetted names (generic word dropped, "(formerly)" read as the same water) binds ONLY the lakes
under the code whose names agree, plus the code's unnamed parts — never a differently-named neighbour sharing the code
(the first cut bound every lake under the code, so Buntzen's record coloured McCombe Lake, Mara's Rosemond, Meadow's
Pigeon). When the FWA names none of the code's lakes, every lake under it binds. A code whose names all disagree is
REFUSED: a migrated key landing on the wrong water tells somebody there are fish where there are none. A feed reports
the count of every verdict; nothing is dropped silently.

THE FILE (`lakes/1`), little-endian, varints LEB128:
    magic    b"BCLK"
    version  u8 = 1
    handles  8 bytes: the bundle's `meta.section_handles` (16 hex digits) as raw bytes
    asof     varint: the feed's as-of day (days since 1900-01-01, `EPOCH`) + 1, 0 = none
    count    varint: sections listed
    then per section, ascending sid:
      sid delta  varint (from the previous sid; the first from 0)
      survey     u8: 0 none, 1 sheet, 2 digitised
      stocked    varint: last release day + 1, 0 = never stocked (or not known)
A section the file does not list has no survey and no stocking: absence is the claim.
"""
from __future__ import annotations

import datetime as dt
import re
from collections import Counter
from typing import Dict, List, Mapping, NamedTuple, Optional, Sequence, Set, Tuple

from pipeline.deliver.binary import read_varint, varint
from pipeline.deliver.tiles.names import normalise

FORMAT = "lakes/1"
MAGIC = b"BCLK"
VERSION = 1
NONE, SHEET, DIGITISED = 0, 1, 2
#: day numbers count from here: stocking records reach back past 1901, so 1970 would make the oldest negative
EPOCH = dt.date(1900, 1, 1)

#: the generic word a name may carry or drop ("LANGFORD" for Langford Lake; "SOUTH BURNIE L." a sheet's title)
_GENERIC = re.compile(r"\s+(lake|lakes|l|pond|ponds|reservoir)$")
_FORMERLY = re.compile(r"\s*\(\s*formerly\s*\)\s*", re.I)


class FwaLake(NamedTuple):
    wbk: str
    gnis: Tuple[str, str, str]


Facts = Dict[int, Tuple[int, Optional[int]]]


def core(name: str) -> str:
    """A name for comparison: normalised, "(formerly)" removed, ONE trailing generic word dropped."""
    n = normalise(_FORMERLY.sub(" ", name or ""))
    return _GENERIC.sub("", n)


def bind(ident: str, names: Set[str], lakes: Mapping[str, Sequence[FwaLake]]) -> Tuple[Tuple[str, ...], str]:
    """(the waterbody keys an identifier binds to, the verdict): `verified` binds the lakes under the code whose names
    agree, plus its unnamed parts — never a differently-named neighbour that shares the code; `fwa_unnamed` binds every
    (unnamed) lake under the code; `no_polygon` | `name_disagrees` bind none."""
    cands = lakes.get(ident) or ()
    if not cands:
        return (), "no_polygon"
    fwa = {core(g) for c in cands for g in c.gnis if g}
    if not fwa:
        return tuple(sorted({c.wbk for c in cands})), "fwa_unnamed"
    src = {core(n) for n in names if n}
    if not src & fwa:
        return (), "name_disagrees"
    agree = lambda c: not any(c.gnis) or bool({core(g) for g in c.gnis if g} & src)    # noqa: E731
    return tuple(sorted({c.wbk for c in cands if agree(c)})), "verified"


def facts(survey: Mapping[str, Tuple[Set[str], int]], stocked: Mapping[str, Tuple[Set[str], int]],
          lakes: Mapping[str, Sequence[FwaLake]], handles: Mapping[str, int]) -> Tuple[Facts, Dict[str, Dict[str, int]]]:
    """{sid: (survey, last stocked day | None)} and, per source, how many identifiers got each verdict (`:no_section`
    where the bound lake has no section in this atlas)."""
    out: Dict[int, List] = {}
    report: Dict[str, Counter] = {"survey": Counter(), "stocked": Counter()}
    for kind, src in (("survey", survey), ("stocked", stocked)):
        for ident, (names, value) in sorted(src.items()):
            wbks, verdict = bind(ident, names, lakes)
            sids = [handles[f"lake:{w}"] for w in wbks if f"lake:{w}" in handles]
            if wbks and not sids:
                verdict += ":no_section"
            report[kind][verdict] += 1
            for s in sids:
                cur = out.setdefault(s, [NONE, None])
                if kind == "survey":
                    cur[0] = max(cur[0], value)
                else:
                    cur[1] = value if cur[1] is None else max(cur[1], value)
    return ({s: (v[0], v[1]) for s, v in sorted(out.items())},
            {k: dict(sorted(c.items())) for k, c in report.items()})


# ---- bytes ---------------------------------------------------------------------------------------------------------

def _varint(n: int) -> bytes:
    return varint(n, "lake facts")


def encode(f: Facts, *, handles: str, asof: Optional[int]) -> bytes:
    out = bytearray(MAGIC + bytes([VERSION]) + bytes.fromhex(handles))
    out += _varint(0 if asof is None else asof + 1)
    out += _varint(len(f))
    prev = 0
    for sid in sorted(f):
        s, d = f[sid]
        out += _varint(sid - prev) + bytes([s]) + _varint(0 if d is None else d + 1)
        prev = sid
    return bytes(out)


def decode(b: bytes) -> dict:
    if b[:4] != MAGIC or b[4] != VERSION:
        raise ValueError("lake facts: not a lakes/1 file")
    handles = b[5:13].hex()
    a, i = read_varint(b, 13)
    n, i = read_varint(b, i)
    out: Facts = {}
    sid = 0
    for _ in range(n):
        d, i = read_varint(b, i)
        sid += d
        s = b[i]
        i += 1
        st, i = read_varint(b, i)
        out[sid] = (s, None if st == 0 else st - 1)
    if i != len(b):
        raise ValueError(f"lake facts: {len(b) - i} trailing bytes")
    return {"handles": handles, "asof": None if a == 0 else a - 1, "facts": out}


def handles_of(b: bytes) -> str:
    """The handle digest a lakes file was built against."""
    if b[:4] != MAGIC:
        raise ValueError("lake facts: not a lakes/1 file")
    return b[5:13].hex()
