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

WHO DECIDED (2026-11-24): an accepted or rejected row carries `decision` = {`by`, `on` (YYYY-MM-DD), `asked` (the
question as it was put, plain words), `words` (the answer, verbatim)}; a proposed row has none. THE PLAIN LINE
(`plain_line`, `record_changes`): what a release says per station when an accepted change starts or stops moving its
record ("08HD006 level record moved 5.0 m down to the gauge's new zero (Environment Canada note, 2025-06-05)"); the
climatology build prints it, and the release's checks list it against the live release's commit (`rows_at`;
`pipeline.release.gauge_records`). Undo, per station: `python -m pipeline.gauges.feed.datum --set <station> proposed`.
The page for the owner: `pipeline/docs/GAUGE-DATUM-CHANGES.md`.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import subprocess
from datetime import date
from pathlib import Path

from pipeline.common.curated import CURATED

#: The curated list, owned here: climatology and the build's stage key ask this module and never name the file.
DATUM_FILE = CURATED.gauges.datum_change_stations

STATUSES = ("proposed", "accepted", "rejected")
SOURCES = ("hydat_remark", "owner")
REQUIRED = ("station", "effective", "offset_m", "offset_from", "source", "status", "note")
OPTIONAL = ("remark", "evidence", "decision")
#: (2026-11-24) an accepted / rejected row's record of who decided, when, the question and the answer verbatim
DECISION_KEYS = ("by", "on", "asked", "words")
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


def this_year_starts(today: date) -> date:
    """The first day of the THIS-YEAR line (the station file's `daily`, `publish._record_start`): a change after it would
    leave that line on two zeros."""
    from pipeline.gauges.feed.publish import _record_start     # lazy: publish imports climatology, which imports this
    return _record_start(today)


def _check_decision(row: dict, at: str) -> None:
    """An accepted or rejected row says who decided, on what day, what was asked and the answer verbatim; a proposed row
    has no decision (nobody has made one)."""
    d = row.get("decision")
    if row["status"] == "proposed":
        if d is not None:
            raise DatumFileError(f"{at}: a proposed row has no `decision` (nobody has decided it yet)")
        return
    if not isinstance(d, dict) or set(d) != set(DECISION_KEYS):
        raise DatumFileError(f"{at}: an {row['status']} row records the decision as {{{', '.join(DECISION_KEYS)}}}: "
                             "who, the day, the question as asked and the answer verbatim")
    for k in DECISION_KEYS:
        if not isinstance(d[k], str) or not d[k].strip():
            raise DatumFileError(f"{at}: decision.{k} is text")
    try:
        date.fromisoformat(d["on"])
    except ValueError as e:
        raise DatumFileError(f"{at}: decision.on {d['on']!r} is not a YYYY-MM-DD date") from e


def parse(blob: object, where: str = "datum_change_stations.json", today: date | None = None) -> list[dict]:
    """The rows of a parsed file, checked (DatumFileError names the row and the rule), sorted by (station, effective).
    `today` (the clock, injectable for a test) dates the this-year line an accepted change may not fall inside."""
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
        # THE THIS-YEAR LINE (T1047 review): the station file's `daily` is this year's live record as it came, so a change
        # inside it leaves the line on BOTH zeros (07FD001: ~0.07 m to ~95 m). Putting the record on today's zero would
        # then draw the early part of the line ~95 m under its band. Until both publishers put `daily` on today's zero
        # too (`publish.merge_daily` and its worker twin, rule 40), such a change cannot be accepted before the year turns.
        if row["status"] == "accepted" and eff is not None and eff > this_year_starts(today or date.today()):
            raise DatumFileError(f"{at}: accepted change on {eff} falls inside the this-year line (from "
                                 f"{this_year_starts(today or date.today())}), which would then mix both zeros: accept it "
                                 "once the year turns, or after both publishers put `daily` on today's zero "
                                 "(publish.merge_daily + the feeds worker twin)")
        if not isinstance(row["note"], str) or not row["note"].strip():
            raise DatumFileError(f"{at}: note says why the row is here")
        if "evidence" in row and (not isinstance(row["evidence"], str) or not row["evidence"].strip()):
            raise DatumFileError(f"{at}: evidence, when present, is text")
        _check_decision(row, at)
        key = (st, row["effective"])
        if key in seen:
            raise DatumFileError(f"{at}: two rows for the change on {row['effective']}")
        seen.add(key)
        out.append(dict(row))
    return sorted(out, key=lambda r: (r["station"], r["effective"] or ""))


def validate_text(text: str) -> None:
    """`curated_write.write(validate=...)`: the text must parse into a valid file."""
    parse(json.loads(text))


def load(path: Path | None = None, today: date | None = None) -> list[dict]:
    """Every row of the file (all statuses), checked. RAISES on a missing file (AGENTS 37): absent curated data is a bug."""
    path = Path(path or DATUM_FILE)
    if not path.is_file():
        raise FileNotFoundError(f"datum: no {path} — it is curated (T1047); restore it from git, never an empty list")
    return parse(json.loads(path.read_text(encoding="utf-8")), str(path), today)


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


def check_remarks(rows: list[dict], hydat: Path) -> list[str]:
    """Every `hydat_remark` row's quote is one of its station's own STN_REMARKS (case and spacing aside), so a row cannot
    claim a change the record does not state. An ACCEPTED row's missing quote RAISES (it moves a record); a proposed or
    rejected row's is a warning, printed and returned (it moves nothing, and a remark ECCC rewords in a new HYDAT must
    not stop the unattended HYDAT workflow)."""
    if not Path(hydat).exists():
        raise FileNotFoundError(f"datum: no {hydat} (python data/fetch_data.py --layers hydat)")
    quoted = [r for r in rows if r["source"] == "hydat_remark"]
    warned: list[str] = []
    with sqlite3.connect(f"file:{hydat}?mode=ro", uri=True) as con:
        for r in quoted:
            said = {_norm(t) for (t,) in con.execute(
                "SELECT REMARK_EN FROM STN_REMARKS WHERE STATION_NUMBER = ?", (r["station"],))}
            if _norm(r["remark"]) not in said:
                msg = f"datum: {r['station']}: HYDAT holds no station remark {r['remark'][:80]!r}"
                if r["status"] == "accepted":
                    raise DatumFileError(msg)
                warned.append(f"{msg} ({r['status']} row, moves nothing: re-quote it)")
    for w in warned:
        print(f"  ⚠️  {w}")
    return warned


# ------------------------------------------------------------------------------------------------ the plain line

def _metres(off: float) -> str:
    """5.0 -> "5.0", 2.37 -> "2.37", 94.826 -> "94.826": the offset as the file holds it, millimetres at most."""
    s = f"{abs(off):.3f}".rstrip("0")
    return s + "0" if s.endswith(".") else s


def plain_line(row: dict) -> str:
    """One accepted change in plain words, as a release lists it: which way and how far the station's level record moved,
    and on whose word ("08HD006 level record moved 5.0 m down to the gauge's new zero (Environment Canada note,
    2025-06-05)"). `offset_m` is what is ADDED to the old record, so a negative offset moves it down."""
    off = float(row["offset_m"])
    way = "down" if off < 0 else "up"
    said = "Environment Canada note" if row["source"] == "hydat_remark" else "measured from the live feed"
    return f"{row['station']} level record moved {_metres(off)} m {way} to the gauge's new zero ({said}, {row['effective']})"


def _moved(rows: list[dict]) -> dict[tuple, dict]:
    return {(r["station"], r["effective"], float(r["offset_m"])): r for r in rows if r["status"] == "accepted"}


def record_changes(before: list[dict], after: list[dict]) -> list[str]:
    """What moved between two versions of the file, one plain line per change, by station: an accepted change the
    `before` file did not have (`plain_line`), and one it had that `after` no longer accepts (the record goes back)."""
    was, now = _moved(before), _moved(after)
    out = [(k, plain_line(now[k])) for k in now if k not in was]
    out += [(k, f"{k[0]} level record back on HYDAT's own zero: the {_metres(k[2])} m "
                f"{'down' if k[2] < 0 else 'up'} change of {k[1]} is no longer accepted") for k in was if k not in now]
    return [line for _, line in sorted(out)]


def rows_at(commit: str, repo: Path) -> list[dict] | None:
    """The file's rows as committed at `commit` of the git repo `repo` (the live release's commit), or None when the file
    did not exist at that commit (a release built before T1047). Any git failure raises."""
    rel = Path(DATUM_FILE).resolve().relative_to(Path(repo).resolve()).as_posix()
    run = dict(cwd=repo, capture_output=True, check=True)
    if not subprocess.run(["git", "ls-tree", "--name-only", commit, "--", rel], **run).stdout.strip():
        return None
    blob = subprocess.run(["git", "show", f"{commit}:{rel}"], **run).stdout
    return parse(json.loads(blob), f"{rel} at {commit[:12]}")


# ------------------------------------------------------------------------------------------------ the one command

def set_status(station: str, status: str, decision: dict | None, path: Path | None = None) -> dict | None:
    """Put `station`'s ONE change row at `status` (with its `decision`, or none for `proposed`), through
    `curated_write` (validated by `validate_text`, backed up, logged with `CURATED_WRITE_BY`). Every other row and every
    other field is left exactly as it is. The undo of an acceptance is `set_status(<station>, "proposed", None)`."""
    from pipeline.common import curated_write
    path = Path(path or DATUM_FILE)
    blob = json.loads(path.read_text(encoding="utf-8"))
    hits = [r for r in blob["changes"] if r["station"] == station]
    if len(hits) != 1:
        raise DatumFileError(f"{path}: {station} has {len(hits)} rows; this command sets exactly one")
    row = hits[0]
    row["status"] = status
    row.pop("decision", None)
    if decision is not None:
        row["decision"] = {k: decision[k] for k in DECISION_KEYS}
    text = json.dumps(blob, indent=1, ensure_ascii=False) + "\n"
    validate_text(text)
    return curated_write.write(path, text, tool="pipeline.gauges.feed.datum --set",
                               source=f"{station} -> {status}", validate=validate_text)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Set one gauge's datum change row (T1047). Undo an acceptance: "
                                             "`--set <station> proposed`.")
    ap.add_argument("--set", nargs=2, metavar=("STATION", "STATUS"), required=True)
    ap.add_argument("--by", help="who decided (accepted / rejected)")
    ap.add_argument("--on", help="the day decided, YYYY-MM-DD")
    ap.add_argument("--asked", help="the question as it was put")
    ap.add_argument("--words", help="the answer, verbatim")
    a = ap.parse_args(argv)
    station, status = a.set
    if status not in STATUSES:
        ap.error(f"STATUS is one of {STATUSES}")
    given = {k: getattr(a, k) for k in DECISION_KEYS}
    if status == "proposed":
        if any(given.values()):
            ap.error("a proposed row has no decision: leave out --by/--on/--asked/--words")
        decision = None
    else:
        if not all(given.values()):
            ap.error(f"an {status} row needs --by, --on, --asked and --words (the answer verbatim)")
        decision = given
    rec = set_status(station, status, decision)
    print(f"  {station}: {status}" + ("  (already so: nothing written)" if rec is None else
                                      f"  written by {rec['by']}, backup {rec['backup']}"))
    rows = load()
    for r in rows:
        if r["station"] == station and r["status"] == "accepted":
            print(f"  the next clim.json: {plain_line(r)}")
    print("  makes it live: the next data release rebuilds clim.json (`pipeline build`'s clim stage) and the bundle")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
