"""(T1047) Every datum / relocation / gauge-zero remark HYDAT holds for the B.C. roster, parsed: a PROPOSAL file.

    python -m pipeline.tools.audit.datum_remarks [--hydat data/source/hydat.sqlite3] [--out <path>]

WHAT IT IS FOR. A gauge moved or re-zeroed after its last HYDAT year reports live levels on a new zero, so its level is
left out (T550 `publish.level_on_rating`) and the app draws the live line with no band (T1047). The record can be put on
today's zero only by an ACCEPTED row of the curated `datum_change_stations.json` (`pipeline.gauges.feed.datum`). This
tool finds the candidates: every `STN_REMARKS` row of a roster station (`SOURCE/bc_hydrometric_stations.json`, the
stations the app uses) whose wording names a datum, a relocation or a gauge zero (`WORDING`).

WHAT A ROW SAYS. `station`, `name`, `type` (HYDAT's remark type), `year` (null: a standing
remark, HYDAT's -999), `remark` (VERBATIM, as HYDAT stores it), `words` (the wording it matched), `kind` (a SORT, not a
decision: `change` a relocation / new zero / step, `kept` a move with the datum kept, `conversion` a constant to another
datum, `quality` a datum's stability or definition, `other`), `date` (the one full date the remark states, else null with
`date_reason`), `offset_m` (metres to ADD to a level read on the old zero to put it on the new one, ONLY where the remark
says which way the readings moved: "2.370m jump in stage" +2.370, "~5m drop in stage" -5, "datum ... lowered by 3.516m"
+3.516; else null with `offset_reason`), `approximate` (the remark hedges the size: "~", "approx"), `metres` (every size in
metres the remark states, for the reader).

NOTHING HERE IS IN EFFECT (rule: everything added is a proposal; AGENTS 36a: machine output lives in data/generated/).
Deterministic: sorted rows, no clock, the HYDAT release stamped in.
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
from datetime import date, timedelta
from pathlib import Path

from pipeline.common.curated import GENERATED, SOURCE

#: The wording that makes a remark a candidate (case-insensitive). Broad on purpose: a false row costs a glance, a
#: missed relocation costs a station ranked on the wrong zero.
WORDING = re.compile(r"DATUM|RELOCAT\w*|\bMOVED\b|GAUGE ZERO|ZERO OF (THE )?GAUGE|BENCH ?MARK|ELEVATION|"
                     r"(JUMP|DROP|STEP)( CHANGE)? IN (STAGE|WATER LEVEL)|RE-?SURVEY\w*|NEW SITE|NEW LOCATION|"
                     r"\bRESETS?\b|\bOFFSET\b", re.I)

_CHANGE = re.compile(r"RELOCAT|\bMOVED\b|NEW SITE|NEW LOCATION|DATUM CHANGE|NEW (ASSUMED )?DATUM|DIFFERENT DATUM|"
                     r"(JUMP|DROP|STEP)( CHANGE)? IN (STAGE|WATER LEVEL)|\bSTEP IN WATER LEVEL|LOWERED|RAISED|"
                     r"ADDED TO THE DATUM|NOT (BEEN )?(PRESERVED|TRANSFERRED|MAINTAINED)|NO DATUM CONTINUITY|"
                     r"DATUM WAS NOT MAINTAINED|RE-ESTABLISHED", re.I)
# "every effort was made to ensure the new station datum is similar to the historical station datum" (08GA026): kept
_KEPT = re.compile(r"DATUM (WAS )?(MAINTAINED|CONSERVED|PRESERVED)|DATUM IS SIMILAR TO", re.I)
_STAGE_MOVED = re.compile(r"(JUMP|DROP|STEP)|LOWERED|RAISED|DATUM CHANGE|NOT (BEEN )?(PRESERVED|TRANSFERRED)", re.I)
_CONVERSION = re.compile(r"\bTO CONVERT\b|\bTO GET TO\b|GEODETIC", re.I)
_QUALITY = re.compile(r"STABILITY|UNCERTAIN|INSTABILITY|POORLY DEFINED|NOT DEFINED|VARIABLE( OPERATING)? DATUM|"
                      r"DATUM IS VARIABLE|NOT TIGHT|SENSOR RESETS?", re.I)

_MONTHS = {m: i for i, ms in enumerate(
    (("JAN", "JANUARY"), ("FEB", "FEBRUARY"), ("MAR", "MARCH"), ("APR", "APRIL"), ("MAY",), ("JUN", "JUNE"),
     ("JUL", "JULY"), ("AUG", "AUGUST"), ("SEP", "SEPT", "SEPTEMBER"), ("OCT", "OCTOBER"), ("NOV", "NOVEMBER"),
     ("DEC", "DECEMBER")), start=1) for m in ms}
_MON = "|".join(sorted(_MONTHS, key=len, reverse=True))
_DATES = (
    (re.compile(r"\b(\d{4})[-/](\d{2})[-/](\d{2})\b"), lambda m: (int(m[1]), int(m[2]), int(m[3]))),
    (re.compile(rf"\b({_MON})\.?\s+(\d{{1,2}})(?:ST|ND|RD|TH)?(?:,\s*|\s+|/)(\d{{4}})\b", re.I),
     lambda m: (int(m[3]), _MONTHS[m[1].upper()], int(m[2]))),
)
_NUM = r"(\d+(?:\.\d+)?)\s*(?:M|METRES|METERS)\b"
#: a size whose DIRECTION in reading terms the wording states: (pattern, sign of the change in the readings)
_SIGNED = (
    (re.compile(rf"(~|APPROX\.?\s*|APPROXIMATELY\s*)?{_NUM}\s+(JUMP|RISE|INCREASE)\s+IN\s+(STAGE|WATER LEVEL)", re.I), +1),
    (re.compile(rf"(~|APPROX\.?\s*|APPROXIMATELY\s*)?{_NUM}\s+(DROP|FALL|DECREASE)\s+IN\s+(STAGE|WATER LEVEL)", re.I), -1),
    # the zero going DOWN raises every reading, and the other way round
    (re.compile(rf"DATUM\s+(?:BEING\s+|WAS\s+)?LOWERED\s+(?:BY\s+)?(~|APPROX\.?\s*)?{_NUM}", re.I), +1),
    (re.compile(rf"LOWERED\s+(?:THE\s+)?(?:GAUGE\s+)?DATUM\s+(?:BY\s+)?(~|APPROX\.?\s*)?{_NUM}", re.I), +1),
    (re.compile(rf"DATUM\s+(?:BEING\s+|WAS\s+)?RAISED\s+(?:BY\s+)?(~|APPROX\.?\s*)?{_NUM}", re.I), -1),
    (re.compile(rf"RAISED\s+(?:THE\s+)?(?:GAUGE\s+)?DATUM\s+(?:BY\s+)?(~|APPROX\.?\s*)?{_NUM}", re.I), -1),
    # a SIGNED change OF THE DATUM (08MG014: "assumed datum changed ... approximately -4.923 m difference from the 1938
    # to 1984 datum"; HYDAT reads ~5.3 m HIGHER from 2013): the readings move the other way
    (re.compile(rf"DATUM CHANGED\b.*?(~|APPROX\.?\s*|APPROXIMATELY\s*)?(-?\d+(?:\.\d+)?)\s*M\s+DIFFERENCE", re.I), -1),
    # "1.000m was added to the datum to avoid negative water levels": every reading 1 m higher
    (re.compile(rf"(~|APPROX\.?\s*)?{_NUM}\s+WAS\s+ADDED\s+TO\s+THE\s+DATUM", re.I), +1),
)
_METRES = re.compile(r"(?<![\d.])([-+]?\d+(?:\.\d+)?)\s*(?:M|METRES|METERS)\b(?!\s*(?:DOWNSTREAM|UPSTREAM|FROM|ABOVE|BELOW))", re.I)
_DISTANCE = re.compile(r"\d+(?:\.\d+)?\s*(?:KM|KMS|M)\s+(?:DOWNSTREAM|UPSTREAM|FROM|ABOVE|BELOW)", re.I)


def words(text: str) -> list[str]:
    return sorted({m.group(0).upper() for m in WORDING.finditer(text)})


def kind(text: str) -> str:
    """A sort for the reader, never a decision (see the module doc)."""
    # "STATION WILL HAVE TO BE MOVED TO A NEW LOCATION" (08KD006): a move not made is no change
    if _CHANGE.search(text) and not (_KEPT.search(text) and not _STAGE_MOVED.search(text)) \
            and not re.search(r"\bWILL HAVE TO BE MOVED\b", text, re.I):
        return "change"
    if _CONVERSION.search(text):
        return "conversion"
    if _QUALITY.search(text):
        return "quality"
    if _KEPT.search(text):
        return "kept"
    return "other"


#: a date right after these words ends a record, it does not change a zero (08EE003 "DISCONTINUED DEC 31 1971",
#: 08KD006 "NO DATA AFTER SEP 15, 2011"): never the change's date
_ENDS = re.compile(r"(DISCONTINUED|NO DATA (PUBLISHED )?AFTER|ENDED AS OF|DESTROYED ON)\s*(ON\s*)?$", re.I)
#: "TO BE MOVED UPSTREAM AFTER DEC 31, 2000" (08NE087): the first day on the new site is the day after
_AFTER = re.compile(r"\bAFTER\s*$", re.I)


def dates(text: str) -> list[str]:
    """Every full date the remark states as a change's day, ISO, in order, distinct (an end-of-record date left out; a
    date after "AFTER" read as the next day)."""
    found: list[tuple[int, str]] = []
    for rx, ymd in _DATES:
        for m in rx.finditer(text):
            before = text[max(0, m.start() - 40):m.start()]
            if _ENDS.search(before):
                continue
            try:
                d = date(*ymd(m))
            except ValueError:
                continue
            found.append((m.start(), (d + timedelta(days=1) if _AFTER.search(before) else d).isoformat()))
    out: list[str] = []
    for _, d in sorted(found):
        if d not in out:
            out.append(d)
    return out


def the_date(text: str) -> tuple[str | None, str | None]:
    ds = dates(text)
    if len(ds) == 1:
        return ds[0], None
    if ds:
        return None, f"{len(ds)} dates stated ({', '.join(ds)}): which one the zero changed on is the reader's call"
    years = sorted(set(re.findall(r"\b(19\d{2}|20\d{2})\b", text)))
    ends = "; its full date ends a record (DISCONTINUED / NO DATA AFTER), not a change" if re.search(
        r"(DISCONTINUED|NO DATA (PUBLISHED )?AFTER|ENDED AS OF|DESTROYED ON)\s*(ON\s*)?[A-Z]{3}", text, re.I) else ""
    return None, (f"a year only ({', '.join(years)}), no day" if years else "no date stated") + ends


def metres(text: str) -> list[float]:
    """Every size in metres the remark states (not a distance up or downstream), in order."""
    t = _DISTANCE.sub(" ", text)
    return [float(m[1]) for m in _METRES.finditer(t)]


def the_offset(text: str, k: str) -> tuple[float | None, str | None, bool]:
    """(offset_m, why it is null, approximate). Only a size whose reading direction the wording states."""
    for rx, sign in _SIGNED:
        m = rx.search(text)
        if m:
            approx = bool(m.group(1))
            return round(sign * float(m.group(2)), 3), None, approx
    if k == "conversion":
        return None, "a constant to convert to another datum, not a change of the gauge's zero", False
    if k != "change":
        return None, f"not a change of zero ({k})", False
    sizes = metres(text)
    if sizes:
        return None, (f"a size is stated ({', '.join(f'{s:g} m' for s in sizes)}) but not which way the readings "
                      "moved"), False
    if re.search(r"HIGHER WATER LEVEL", text, re.I):
        return None, "says the readings are higher, not by how much", False
    return None, "no size stated", False


def scan(hydat: Path, roster: list[dict]) -> list[dict]:
    """The candidate rows, sorted by (station, year, type)."""
    if not Path(hydat).exists():
        raise FileNotFoundError(f"datum_remarks: no {hydat} (python data/fetch_data.py --layers hydat)")
    by_id = {s["station"]: s for s in roster}
    out: list[dict] = []
    with sqlite3.connect(f"file:{hydat}?mode=ro", uri=True) as con:
        types = dict(con.execute("SELECT REMARK_TYPE_CODE, REMARK_TYPE_EN FROM STN_REMARK_CODES"))
        rows = con.execute("SELECT STATION_NUMBER, REMARK_TYPE_CODE, YEAR, REMARK_EN FROM STN_REMARKS "
                           "ORDER BY STATION_NUMBER, YEAR, REMARK_TYPE_CODE").fetchall()
    for st, code, year, text in rows:
        if st not in by_id or not text:
            continue
        w = words(text)
        if not w:
            continue
        flat = " ".join(text.split())
        k = kind(flat)
        d, d_why = the_date(flat)
        off, off_why, approx = the_offset(flat, k)
        out.append({"station": st, "name": by_id[st].get("name"), "type": types.get(code, str(code)), "year": None if year == -999 else year, "remark": text,
                    "words": w, "kind": k, "date": d, "date_reason": d_why, "offset_m": off,
                    "offset_reason": off_why, "approximate": approx, "metres": metres(flat)})
    return out


def build(hydat: Path, roster_path: Path) -> dict:
    roster = json.loads(Path(roster_path).read_text(encoding="utf-8"))
    rows = scan(hydat, roster)
    marker = Path(hydat).with_suffix(".release")
    stations = sorted({r["station"] for r in rows})
    changed = sorted({r["station"] for r in rows if r["kind"] == "change"})
    return {
        "_about": ("GENERATED (T1047) by `python -m pipeline.tools.audit.datum_remarks`: every HYDAT STN_REMARKS row of "
                   "a roster station whose wording names a datum, a relocation or a gauge zero, parsed. PROPOSALS — "
                   "nothing here is in effect: only an `accepted` row of data/curated/gauges/datum_change_stations.json "
                   "puts a level record on today's zero. `kind` is a sort, not a decision; `offset_m` is the metres to "
                   "ADD to a level on the old zero, given only where the remark says which way the readings moved."),
        "hydat": marker.read_text().strip() if marker.exists() else None,
        "roster": len(roster),
        "stations": len(stations),
        "change_stations": changed,
        "remarks": rows,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="HYDAT datum / relocation remarks for the roster -> a proposal file")
    ap.add_argument("--hydat", type=Path, default=SOURCE / "hydat.sqlite3")
    ap.add_argument("--stations", type=Path, default=SOURCE / "bc_hydrometric_stations.json")
    ap.add_argument("--out", type=Path, default=GENERATED.gauges.datum_proposals)
    a = ap.parse_args()
    out = build(a.hydat, a.stations)
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(json.dumps(out, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"  {a.out}: {len(out['remarks'])} remarks, {out['stations']} stations of {out['roster']} "
          f"(HYDAT {out['hydat']}); kind=change at {len(out['change_stations'])}: {' '.join(out['change_stations'])}")


if __name__ == "__main__":
    main()
