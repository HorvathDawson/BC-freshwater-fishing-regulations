"""LAKE FACTS: per lake SECTION, whether it was surveyed and when it was last stocked — what the map's Depth and
Stocked lake modes colour by (owner 2026-10-21: one colouring pathway for every map view and mode, lake stocked and
depth included, and colours that update from DATA ALONE — nothing about a lake's survey or stocking is baked into the
tiles, whose lake features carry only `section_id, name, water, area_m2`).

    python -m pipeline.deliver lake_facts [--bundle FILE] [--out FILE] [--build DIR] [--gpkg FILE]
                                          [--sheets-csv FILE] [--sheets-json FILE] [--stocking FILE]

Written beside the bundle (`lake_facts.bin`) and pinned to its handle table, like `status_index.bin`; the release
copies it as the file `lakes` (`release/build.py`, format `lakes/1`) and refuses one from another atlas.

THE SOURCES (each a fetch away, `data/source/`):
  survey    the two survey-sheet rosters, `wsa_bathymetry_maps.csv` and `bc_bathymetry_sheets.json`
            (BATH_SURVEY_MAP_SHEETS_SVW; neither is complete — see `fetch_data.fetch_bathymetry_sheets`). A lake with a
            sheet is SURVEYED; one whose sheet the province traced into contours (`vectorized_flag` Y) is DIGITISED.
  stocking  the Freshwater Fisheries Society of BC's release roster (`stocking/ffsbc_releases.json`,
            `python -m pipeline.stocking.fetch`): every release since 1901, by FIDQ waterbody identifier.

THE JOIN IS THE FWA'S OWN KEY, CONFIRMED BY A NAME (`pipeline/bathymetry`, `pipeline/stocking`). Both rosters key on
the 50K waterbody identifier (`02322SAJR`), the same string as the lakes layer's `WATERBODY_KEY_GROUP_CODE_50K`; the
lake's section is `lake:<WATERBODY_KEY>` in the atlas's handle table. When one of the source's names agrees with one
of the FWA's three gazetted names (generic word dropped, "(formerly)" read as the same water), the identifier binds to
EVERY lake under its code whose names agree, plus the code's unnamed parts — a differently-named lake sharing the code
(BUNTZEN's code also carries McCombe Lake) is not bound. When the FWA names none of them, it binds every lake under the
code. A code whose names all disagree is REFUSED — a migrated
50K key landing on the wrong water is the failure the audits found, and a stocking record on the wrong lake tells
somebody there are fish where there are none. The counts of every verdict are printed; nothing is dropped silently.

THE FILE (`lakes/1`), little-endian, varints LEB128:
    magic    b"BCLK"
    version  u8 = 1
    handles  8 bytes: the bundle's `meta.section_handles` (16 hex digits) as raw bytes
    asof     varint: the stocking roster's day (days since 1900-01-01, `EPOCH`) + 1, 0 = no roster
    count    varint: sections listed
    then per section, ascending sid:
      sid delta  varint (from the previous sid; the first from 0)
      survey     u8: 0 none, 1 sheet, 2 digitised
      stocked    varint: last release day + 1, 0 = never stocked (or no roster)
A section the file does not list has no survey and no stocking: absence is the claim.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
import sqlite3
import sys
from collections import Counter
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, NamedTuple, Optional, Sequence, Set, Tuple

from pipeline.common.curated import GENERATED, SOURCE
from pipeline.deliver.tiles.names import normalise

FORMAT = "lakes/1"
MAGIC = b"BCLK"
VERSION = 1
NONE, SHEET, DIGITISED = 0, 1, 2
#: day numbers count from here: FFSBC's roster begins in 1901, so 1970 would make the oldest releases negative
EPOCH = dt.date(1900, 1, 1)

GPKG = "bc_fisheries_data.gpkg"
SHEETS_CSV = "wsa_bathymetry_maps.csv"
SHEETS_JSON = "bc_bathymetry_sheets.json"
STOCKING = "stocking/ffsbc_releases.json"

#: the generic word a name may carry or drop ("LANGFORD" is FFSBC's Langford Lake; "SOUTH BURNIE L." a sheet's title)
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


def surveys(csv_rows: Iterable[Mapping], wfs_rows: Iterable[Mapping]) -> Dict[str, Tuple[Set[str], int]]:
    """{identifier: (every name either source gives it, SHEET | DIGITISED)}."""
    out: Dict[str, Tuple[Set[str], int]] = {}
    for row in csv_rows:
        ident = (row.get("WATERBODY_IDENTIFIER_WSA_50K") or "").strip()
        if ident:
            names, kind = out.get(ident, (set(), SHEET))
            names |= {row.get(k) for k in ("GAZETTED_NAME", "MAP_TITLE") if row.get(k)}
            out[ident] = (names, kind)
    for row in wfs_rows:
        ident = (row.get("waterbody_identifier") or "").strip()
        if ident:
            names, kind = out.get(ident, (set(), SHEET))
            names |= {row.get(k) for k in ("gazetted_name", "map_title") if row.get(k)}
            if (row.get("vectorized_flag") or "").upper() == "Y":
                kind = DIGITISED
            out[ident] = (names, kind)
    return dict(sorted(out.items()))


def day_of(iso: str) -> int:
    return (dt.date.fromisoformat(iso[:10]) - EPOCH).days


def last_stocked(rows: Iterable[Mapping], asof: int) -> Dict[str, Tuple[Set[str], int]]:
    """{identifier: (the names its releases carry, the latest release day on or before `asof`)}."""
    out: Dict[str, Tuple[Set[str], int]] = {}
    for r in rows:
        ident = (r.get("wbid") or "").strip()
        if not ident or not r.get("date"):
            continue
        d = day_of(r["date"])
        if d > asof:
            continue
        names, best = out.get(ident, (set(), d))
        if r.get("water"):
            names.add(r["water"])
        out[ident] = (names, max(best, d))
    return out


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
    if n < 0:
        raise ValueError(f"lake facts: a negative varint {n}")
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        out.append(b | (0x80 if n else 0))
        if not n:
            return bytes(out)


def _read_varint(b: bytes, i: int) -> Tuple[int, int]:
    n = shift = 0
    while True:
        x = b[i]
        i += 1
        n |= (x & 0x7F) << shift
        shift += 7
        if not x & 0x80:
            return n, i


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
    a, i = _read_varint(b, 13)
    n, i = _read_varint(b, i)
    out: Facts = {}
    sid = 0
    for _ in range(n):
        d, i = _read_varint(b, i)
        sid += d
        s = b[i]
        i += 1
        st, i = _read_varint(b, i)
        out[sid] = (s, None if st == 0 else st - 1)
    if i != len(b):
        raise ValueError(f"lake facts: {len(b) - i} trailing bytes")
    return {"handles": handles, "asof": None if a == 0 else a - 1, "facts": out}


def handles_of(b: bytes) -> str:
    """The handle digest a lakes file was built against."""
    if b[:4] != MAGIC:
        raise ValueError("lake facts: not a lakes/1 file")
    return b[5:13].hex()


# ---- the sources ---------------------------------------------------------------------------------------------------

def fwa_lakes(gpkg: Path) -> Dict[str, List[FwaLake]]:
    """{group code: the FWA lakes carrying it} — two columns of the lakes layer, read straight from the GeoPackage."""
    con = sqlite3.connect(f"file:{gpkg}?mode=ro", uri=True)
    out: Dict[str, List[FwaLake]] = {}
    for code, wbk, g1, g2, g3 in con.execute(
            "SELECT WATERBODY_KEY_GROUP_CODE_50K, WATERBODY_KEY, GNIS_NAME_1, GNIS_NAME_2, GNIS_NAME_3 FROM lakes "
            "WHERE WATERBODY_KEY_GROUP_CODE_50K IS NOT NULL AND WATERBODY_KEY_GROUP_CODE_50K <> '' "
            "AND WATERBODY_KEY IS NOT NULL ORDER BY WATERBODY_KEY_GROUP_CODE_50K, WATERBODY_KEY"):
        out.setdefault(code.strip(), []).append(FwaLake(str(int(wbk)), (g1 or "", g2 or "", g3 or "")))
    con.close()
    return out


def build(*, bundle: Path, build_dir: Path, gpkg: Path, sheets_csv: Path, sheets_json: Path,
          stocking: Optional[Path]) -> Tuple[bytes, dict]:
    from pipeline.common import section_handles as SHD
    meta = dict(sqlite3.connect(f"file:{bundle}?mode=ro", uri=True).execute("SELECT k, v FROM meta"))
    handles = meta["section_handles"]
    if SHD.digest_for(build_dir) != handles:
        raise SystemExit(f"lake facts: {build_dir} is not the atlas the bundle was built from")
    _, index = SHD.read(build_dir)
    with sheets_csv.open(encoding="utf-8-sig") as fh:
        csv_rows = list(csv.DictReader(fh))
    survey = surveys(csv_rows, json.loads(sheets_json.read_text()))
    asof, stocked = None, {}
    if stocking is not None:
        roster = json.loads(stocking.read_text())
        asof = day_of(roster["fetched"])
        stocked = last_stocked(roster["rows"], asof)
    f, report = facts(survey, stocked, fwa_lakes(gpkg), index)
    return encode(f, handles=handles, asof=asof), {"sections": len(f), "asof": asof, **report}


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="pipeline.deliver lake_facts", description=__doc__.split("\n\n")[0])
    ap.add_argument("--bundle", type=Path, default=GENERATED.bundle / "bundle.sqlite")
    ap.add_argument("--out", type=Path, default=GENERATED.bundle / "lake_facts.bin")
    ap.add_argument("--build", type=Path, default=None, help="the atlas build (default: the configured one)")
    ap.add_argument("--gpkg", type=Path, default=SOURCE / GPKG)
    ap.add_argument("--sheets-csv", type=Path, default=SOURCE / SHEETS_CSV)
    ap.add_argument("--sheets-json", type=Path, default=SOURCE / SHEETS_JSON)
    ap.add_argument("--stocking", type=Path, default=SOURCE / STOCKING)
    a = ap.parse_args(argv)
    if not a.stocking.exists():
        print(f"lake facts: no stocking roster at {a.stocking} — run `python -m pipeline.stocking.fetch`",
              file=sys.stderr)
        return 1
    raw, report = build(bundle=a.bundle, build_dir=a.build or GENERATED.require_build(), gpkg=a.gpkg,
                        sheets_csv=a.sheets_csv, sheets_json=a.sheets_json, stocking=a.stocking)
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_bytes(raw)
    print(json.dumps({"out": str(a.out), "bytes": len(raw), **report}, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
