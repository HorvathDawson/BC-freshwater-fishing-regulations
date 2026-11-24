"""(T1047) A GAUGE'S NEW ZERO: the datum changes the owner accepted, and the one place they are read.

Five water-level stations report live levels on a different zero (datum) than their HYDAT record (T550: the live
(flow, level) pair is off the record's rating, so `publish.level_on_rating` leaves the level out and the app draws the
live line alone). HYDAT's `STN_REMARKS` states three of the changes (08HD006 relocated 2025-06-05, "~5m drop"; 08EF001
moved 2025-01-30, "2.370m jump"; 07FD001 "RELOCATED ON 2026-08-05", no size); two have no remark (08KH019, 08MH153).

THE FILE (`CURATED.gauges.datum_change_stations`, `data/curated/gauges/datum_change_stations.json`) holds one row per
change: `station`, `effective` (the first day on the new zero, YYYY-MM-DD), `offset_m` (metres to ADD to a level read
on the old zero to put it on the new one), `offset_from` (where the number comes from), `source` (`hydat_remark` with
`remark` = the station's own STN_REMARKS text, or `owner` for a change no remark states), `status`, `note`, and
optionally `evidence` (what the saved live feed shows). Written only through `pipeline.common.curated_write`
(`validate_text` is its schema); the machine's list of every datum / relocation remark is a PROPOSAL file,
`GENERATED.gauges.datum_proposals` (`pipeline.tools.audit.datum_remarks`).

ONLY AN ACCEPTED ROW MOVES ANYTHING. `proposed` (everything a program or an agent adds: rule "everything added is a
proposal") and `rejected` rows are read, validated and then ignored: `accepted` returns the accepted rows only, and an
accepted row must carry an effective date and an offset. The one consumer is `climatology.build`: a level dated before
an accepted change's `effective` gets its `offset_m` added (`offset_before`), so the band, the recent years and the
rating are on today's zero and the publishers rank the live level again. With no accepted row nothing changes for any
station: `clim.json` is byte-identical (`test_datum_changes`).
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from datetime import date
from pathlib import Path

from pipeline.common.curated import CURATED

#: The curated list, owned here: climatology and the build's stage key ask this module and never name the file.
DATUM_FILE = CURATED.gauges.datum_change_stations

STATUSES = ("proposed", "accepted", "rejected")
SOURCES = ("hydat_remark", "owner")
REQUIRED = ("station", "effective", "offset_m", "offset_from", "source", "status", "note")
OPTIONAL = ("remark", "evidence")
_STATION = re.compile(r"^\d{2}[A-Z]{2}\d{3}$")


class DatumFileError(ValueError):
    """The datum change file breaks its schema: nothing is read from it."""


def _norm(text: str) -> str:
    return " ".join(str(text or "").split()).upper()


def _day(v: object, where: str) -> date | None:
    if v is None:
        return None
    if not isinstance(v, str):
        raise DatumFileError(f"{where}: effective must be YYYY-MM-DD or null, not {v!r}")
    try:
        return date.fromisoformat(v)
    except ValueError as e:
        raise DatumFileError(f"{where}: effective {v!r} is not a YYYY-MM-DD date") from e


def parse(blob: object, where: str = "datum_change_stations.json") -> list[dict]:
    """The rows of a parsed file, checked (DatumFileError names the row and the rule), sorted by (station, effective)."""
    if not isinstance(blob, dict) or set(blob) != {"_about", "changes"}:
        raise DatumFileError(f"{where}: the file is {{_about, changes}} and nothing else")
    if not isinstance(blob["_about"], str) or not blob["_about"].strip():
        raise DatumFileError(f"{where}: `_about` says what the file is (AGENTS 36a)")
    if not isinstance(blob["changes"], list):
        raise DatumFileError(f"{where}: `changes` is a list of rows")
    seen: set[tuple[str, str | None]] = set()
    out: list[dict] = []
    for i, row in enumerate(blob["changes"]):
        at = f"{where} changes[{i}]"
        if not isinstance(row, dict):
            raise DatumFileError(f"{at}: a row is an object")
        missing = [k for k in REQUIRED if k not in row]
        extra = sorted(set(row) - set(REQUIRED) - set(OPTIONAL))
        if missing or extra:
            raise DatumFileError(f"{at}: missing {missing}, unknown {extra}")
        st = row["station"]
        if not isinstance(st, str) or not _STATION.match(st):
            raise DatumFileError(f"{at}: {st!r} is not a station id")
        at = f"{where} {st}"
        eff = _day(row["effective"], at)
        off = row["offset_m"]
        if off is not None and (isinstance(off, bool) or not isinstance(off, (int, float))):
            raise DatumFileError(f"{at}: offset_m is metres (a number) or null, not {off!r}")
        if (off is None) != (row["offset_from"] is None):
            raise DatumFileError(f"{at}: offset_from says where offset_m comes from, and is null exactly when it is")
        if row["source"] not in SOURCES:
            raise DatumFileError(f"{at}: source {row['source']!r} is not one of {SOURCES}")
        remark = row.get("remark")
        if row["source"] == "hydat_remark" and (not isinstance(remark, str) or not remark.strip()):
            raise DatumFileError(f"{at}: a hydat_remark row quotes the remark (`remark`, verbatim)")
        if row["source"] == "owner" and remark is not None:
            raise DatumFileError(f"{at}: an owner row has no HYDAT remark to quote")
        if row["status"] not in STATUSES:
            raise DatumFileError(f"{at}: status {row['status']!r} is not one of {STATUSES}")
        if row["status"] == "accepted" and (eff is None or off is None):
            raise DatumFileError(f"{at}: an accepted change needs its effective date and offset_m — without both it "
                                 "cannot put the record on today's zero")
        if not isinstance(row["note"], str) or not row["note"].strip():
            raise DatumFileError(f"{at}: note says why the row is here")
        if "evidence" in row and (not isinstance(row["evidence"], str) or not row["evidence"].strip()):
            raise DatumFileError(f"{at}: evidence, when present, is text")
        key = (st, row["effective"])
        if key in seen:
            raise DatumFileError(f"{at}: two rows for the change on {row['effective']}")
        seen.add(key)
        out.append(dict(row))
    return sorted(out, key=lambda r: (r["station"], r["effective"] or ""))


def validate_text(text: str) -> None:
    """`curated_write.write(validate=...)`: the text must parse into a valid file."""
    parse(json.loads(text))


def load(path: Path | None = None) -> list[dict]:
    """Every row of the file (all statuses), checked. RAISES on a missing file (AGENTS 37): absent curated data is a bug."""
    path = Path(path or DATUM_FILE)
    if not path.is_file():
        raise FileNotFoundError(f"datum: no {path} — it is curated (T1047); restore it from git, never an empty list")
    return parse(json.loads(path.read_text(encoding="utf-8")), str(path))


def accepted(rows: list[dict]) -> dict[str, list[tuple[date, float]]]:
    """`{station: [(effective, offset_m), ...]}` of the ACCEPTED rows only, oldest first. A proposed or rejected row is
    never here: it moves nothing."""
    out: dict[str, list[tuple[date, float]]] = {}
    for r in rows:
        if r["status"] != "accepted":
            continue
        out.setdefault(r["station"], []).append((date.fromisoformat(r["effective"]), float(r["offset_m"])))
    return {st: sorted(v) for st, v in sorted(out.items())}


def offset_before(moves: list[tuple[date, float]], when: date) -> float:
    """What to add to a level read on `when` to put it on today's zero: every accepted change AFTER that day (a reading
    on or after `effective` is already on the new zero)."""
    return sum(off for eff, off in moves if when < eff)


def digest(rows: list[dict]) -> str:
    """The accepted rows by content (the `clim` stage key): a proposed row's edit reruns nothing."""
    acc = [r for r in rows if r["status"] == "accepted"]
    return hashlib.sha256(json.dumps(acc, sort_keys=True).encode()).hexdigest()[:16]


def check_remarks(rows: list[dict], hydat: Path) -> None:
    """Every `hydat_remark` row's quote is one of its station's own STN_REMARKS (case and spacing aside), so a row cannot
    claim a change the record does not state. RAISES naming the row."""
    if not Path(hydat).exists():
        raise FileNotFoundError(f"datum: no {hydat} (python data/fetch_data.py --layers hydat)")
    quoted = [r for r in rows if r["source"] == "hydat_remark"]
    with sqlite3.connect(f"file:{hydat}?mode=ro", uri=True) as con:
        for r in quoted:
            said = {_norm(t) for (t,) in con.execute(
                "SELECT REMARK_EN FROM STN_REMARKS WHERE STATION_NUMBER = ?", (r["station"],))}
            if _norm(r["remark"]) not in said:
                raise DatumFileError(f"datum: {r['station']}: HYDAT holds no station remark {r['remark'][:80]!r}")
