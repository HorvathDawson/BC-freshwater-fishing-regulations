"""STATUS.md keeps the last 7 days; older entries live in STATUS-archive.md (T540 item 2).

    python3 scripts/status_archive.py --move     move every entry dated more than 7 days before today (B.C. date) to the
                                                 top of STATUS-archive.md, unchanged and in their order
    python3 scripts/status_archive.py --check    the fast-fail size guard: STATUS.md over LIMIT bytes fails, naming --move

An entry is a `## ` heading and what follows it, up to the next `## `; its date is the first YYYY-MM-DD in its heading.
An entry with no date in its heading stays (and is listed). Nothing is deleted: the two files hold the same entries
before and after a move, which `--move` checks before it writes.
"""
from __future__ import annotations

import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
STATUS = ROOT / "handoff/PLAN/STATUS.md"
ARCHIVE = ROOT / "handoff/PLAN/STATUS-archive.md"
#: the size guard (T540, 2026-11-12): STATUS.md was 125 KB with 7 days in it; past this, the old days go to the archive
LIMIT = 128_000
KEEP_DAYS = 7
DATE = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")


def split(text: str) -> tuple[str, list[str]]:
    """(the part before the first entry, the entries each starting with '## ')."""
    parts = re.split(r"(?m)^(?=## )", text)
    return parts[0], parts[1:]


def entry_date(entry: str) -> date | None:
    m = DATE.search(entry.splitlines()[0])
    return date.fromisoformat(m.group(1)) if m else None


def move(today: date | None = None) -> int:
    today = today or datetime.now(ZoneInfo("America/Vancouver")).date()
    cutoff = today - timedelta(days=KEEP_DAYS)
    head, entries = split(STATUS.read_text(encoding="utf-8"))
    keep = [e for e in entries if (entry_date(e) or today) >= cutoff]
    old = [e for e in entries if (entry_date(e) or today) < cutoff]
    for e in entries:
        if entry_date(e) is None:
            print(f"kept (no date in its heading): {e.splitlines()[0]}")
    if not old:
        print(f"nothing older than {cutoff}")
        return 0
    a_head, a_entries = split(ARCHIVE.read_text(encoding="utf-8"))
    for e in (keep, old):
        if e and not e[-1].endswith("\n"):
            e[-1] += "\n"
    new_status = head + "".join(keep)
    new_archive = a_head + "".join(old) + "".join(a_entries)
    if sorted(split(new_status)[1] + split(new_archive)[1]) != sorted(entries + a_entries):
        raise SystemExit("status_archive: the entries would not survive the move unchanged; nothing written")
    ARCHIVE.write_text(new_archive, encoding="utf-8")
    STATUS.write_text(new_status, encoding="utf-8")
    print(f"moved {len(old)} entr{'y' if len(old) == 1 else 'ies'} dated before {cutoff} to {ARCHIVE.name}; "
          f"STATUS.md {len(new_status.encode()):,} B")
    return 0


def check() -> int:
    n = STATUS.stat().st_size
    if n > LIMIT:
        print(f"STATUS.md is {n:,} B, over {LIMIT:,}: move the entries older than {KEEP_DAYS} days to the archive with\n"
              f"    python3 scripts/status_archive.py --move\nand commit both files", file=sys.stderr)
        return 1
    print(f"STATUS.md {n:,} B (limit {LIMIT:,})")
    return 0


if __name__ == "__main__":
    if sys.argv[1:] == ["--move"]:
        sys.exit(move())
    if sys.argv[1:] == ["--check"]:
        sys.exit(check())
    raise SystemExit(__doc__)
