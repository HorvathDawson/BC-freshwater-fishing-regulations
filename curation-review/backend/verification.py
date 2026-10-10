"""Which entries a human has checked against the book — a SIDECAR, never a field of an entry.

The catalogue model has no confirm or lock field and must not grow one: an entry is the book's
row, and "a person read it" is a fact about the review, not about the regulation. So the review
state lives beside the corpus in one file, `verification.json`, next to the `catalogue/`
directory it describes (or `CURATION_VERIFICATION`, for a run against a copy):

    {"version": 1,
     "entries": {"r5:dean_river@5-9": {"state": "verified", "hash": "3f…", "note": ""},
                 "r1:chemainus_river@1-5": {"state": "flagged", "hash": "a1…",
                                            "note": "Bannon Creek closure reads wrong"}}}

`hash` is the content hash of the entry AS IT WAS when it was marked (`entry_hash`). An entry
edited since — in this app, by a re-parse, by hand — no longer matches, and reads `stale`: it
was verified, but not in the form it has now, so it goes back on the pile. Nothing is ever
silently carried over to a changed entry.

The file is written atomically, keys sorted, no timestamps (git is the record of when and who).
A missing file means nothing has been verified yet: it is created by the first mark.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import writes

#: what a reviewer may set; `unverified` clears the record
STATES = ("verified", "flagged", "unverified")
#: what the queue can report: the two stored states, a verified mark on an entry changed since
#: (`stale`), and no mark at all
STATUSES = ("unverified", "verified", "stale", "flagged")


def path_for(entries_dir: Path) -> Path:
    """The sidecar of a catalogue directory: `<entries>/../verification.json`, unless overridden."""
    env = os.environ.get("CURATION_VERIFICATION")
    return Path(env) if env else Path(entries_dir).parent / "verification.json"


def entry_hash(entry: dict) -> str:
    """A stable hash of an entry's STORED content (what the region file holds for it).

    Keys sorted, so it does not depend on serialisation order; the app's served-only fields
    (`label`) must already be stripped. 16 hex chars is plenty to tell one version from the next."""
    blob = json.dumps(entry, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


def load(path: Path) -> dict[str, dict]:
    """entry_id -> {state, hash, note}. A missing file is an empty review, not an error: the file
    does not exist until the first entry is marked."""
    p = Path(path)
    if not p.exists():
        return {}
    data = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or not isinstance(data.get("entries"), dict):
        raise ValueError(f"{p}: not a verification file (expected {{version, entries}})")
    return data["entries"]


def _write(path: Path, records: dict[str, dict]) -> None:
    text = json.dumps({"version": 1, "entries": dict(sorted(records.items()))},
                      indent=1, ensure_ascii=False, sort_keys=True) + "\n"
    writes.commit([(Path(path), text)], "verify")      # backed up first, like every app write


def status_of(rec: dict | None, current_hash: str) -> str:
    """What the queue shows for one entry, given its record and its content now."""
    if not rec:
        return "unverified"
    if rec.get("state") == "flagged":
        return "flagged"                      # a flag stands until a person clears it
    if rec.get("state") == "verified":
        return "verified" if rec.get("hash") == current_hash else "stale"
    return "unverified"


def mark(path: Path, entry_id: str, entry: dict, state: str, note: str = "") -> dict:
    """Record a reviewer's call on one entry, against its content now. Returns the record (or
    `{}` when cleared). Refuses an unknown state, and a flag with no note — a flag that does not
    say what is wrong cannot be acted on."""
    if state not in STATES:
        raise ValueError(f"state must be one of {', '.join(STATES)}, not {state!r}")
    note = (note or "").strip()
    if state == "flagged" and not note:
        raise ValueError("a flag needs a note saying what is wrong")
    records = load(path)
    if state == "unverified":
        records.pop(entry_id, None)
        out: dict = {}
    else:
        out = {"state": state, "hash": entry_hash(entry), "note": note}
        records[entry_id] = out
    _write(path, records)
    return out
