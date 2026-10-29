"""EVERY write the app makes to curated data goes through here: back up, then commit, all or nothing.
There is one such write, a reviewer's mark appended to the marks store (`reuse.post_mark`); the
catalogue and splits.json are never written by the app (T185).

    commit([(path, text), ...], action)

1. BACKUP FIRST. Each target is copied, as it is on disk now, into one timestamped snapshot
   directory before anything is written: `<BACKUP_ROOT>/<UTC stamp>-<action>/<parent>/<name>`
   (`<parent>` keeps `catalogue/region-1.json` and `dfo_salmon/region-1.json` apart). A target that
   does not exist yet (a new curated file before its first write) is listed in the snapshot's
   `ABSENT` file instead. BACKUP_ROOT is `GENERATED.regs.entries_backup / "curation-review"` — the
   same root `pipeline.tools.audit.reparse_candidates --backup-dir` snapshots into — or
   `CURATION_BACKUP_DIR` (the tests point it at a temp dir). It is BOUNDED, by count AND by age
   (owner 2026-10-28: 1,110 scripted tidal marks evicted every older snapshot):
     * a BURST takes ONE snapshot — a bulk post is one commit already; a scripted run of single
       posts reuses the snapshot its first write took while that is younger than `BURST_S`
       (default 120 s) and names the same files and action: it holds the files as they were
       before the burst, which is what a restore needs;
     * the newest `CURATION_BACKUP_KEEP` (default 500) snapshots are kept;
     * EVERY snapshot of the previous UTC day is kept (a burst today never evicts yesterday);
     * at least one per day (its last) for `CURATION_BACKUP_DAYS` (default 60) days.

2. ALL OR NOTHING. The caller computes and validates every new file's text first; nothing is
   written until all of it is ready. Each file is then written by temp file + `os.replace`
   (`io.atomic_write`), so no single file is ever half-written. If ANY write fails, every file
   already written in this commit is put back to its pre-commit bytes (or removed, if it did not
   exist), and the error is re-raised.

`LOCK` serialises the app's read-compute-commit sequences (FastAPI runs sync endpoints on a
thread pool), so two marks posted at once cannot interleave and drop one of them.
"""
from __future__ import annotations

import os
import re
import shutil
import tempfile
import threading
from datetime import datetime, timezone
from pathlib import Path

from pipeline.common.curated import GENERATED
from pipeline.regs.parsing import io

BACKUP_ROOT = (Path(os.environ["CURATION_BACKUP_DIR"]) if os.environ.get("CURATION_BACKUP_DIR")
               else GENERATED.regs.entries_backup / "curation-review")
KEEP = int(os.environ.get("CURATION_BACKUP_KEEP", "500"))
#: a burst: writes this close (s) to the snapshot that began it share it
BURST_S = float(os.environ.get("CURATION_BACKUP_BURST_S", "120"))
#: one snapshot a day is kept for this many days, whatever the count
KEEP_DAYS = int(os.environ.get("CURATION_BACKUP_DAYS", "60"))
LOCK = threading.RLock()
_SNAPSHOT_RE = re.compile(r"\d{8}T\d{6}\.\d{6}Z-[a-z0-9_-]+")

#: the per-file writer. A module attribute so a test can inject a failure into one write.
_write = io.atomic_write


def _rel(path: Path) -> Path:
    return Path(path.parent.name) / path.name


def backup(paths: list[Path], action: str) -> Path:
    """Copy every target, as it is now, into a new snapshot directory; return that directory."""
    action = re.sub(r"[^a-z0-9_-]+", "-", action.lower()).strip("-") or "write"
    root = Path(BACKUP_ROOT)
    burst = _burst_snapshot(root, paths, action)
    if burst is not None:
        return burst
    while True:   # microsecond stamps; loop only on a same-microsecond collision
        snap = root / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ')}-{action}"
        try:
            snap.mkdir(parents=True, exist_ok=False)
            break
        except FileExistsError:
            continue
    absent: list[str] = []
    for p in paths:
        p = Path(p)
        if p.exists():
            dst = snap / _rel(p)
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(p, dst)
        else:
            absent.append(str(_rel(p)))
    if absent:
        (snap / "ABSENT").write_text("\n".join(absent) + "\n", encoding="utf-8")
    _prune(root)
    return snap


def _stamp_of(d: Path) -> datetime:
    return datetime.strptime(d.name[:23], "%Y%m%dT%H%M%S.%fZ").replace(tzinfo=timezone.utc)


def _snapshots(root: Path) -> list[Path]:
    try:
        return sorted(d for d in root.iterdir() if d.is_dir() and _SNAPSHOT_RE.fullmatch(d.name))
    except FileNotFoundError:
        return []


def _names(snap: Path) -> set[str]:
    got = {str(f.relative_to(snap)) for f in snap.rglob("*") if f.is_file() and f.name != "ABSENT"}
    a = snap / "ABSENT"
    if a.exists():
        got |= set(a.read_text(encoding="utf-8").split())
    return got


def _burst_snapshot(root: Path, paths: list[Path], action: str) -> Path | None:
    """The snapshot a burst began, when this write belongs to it: the newest snapshot is this
    action's, younger than `BURST_S`, and holds exactly these files."""
    if BURST_S <= 0:
        return None
    snaps = _snapshots(root)
    if not snaps:
        return None
    last = snaps[-1]
    if not last.name.endswith(f"Z-{action}"):
        return None
    if (datetime.now(timezone.utc) - _stamp_of(last)).total_seconds() >= BURST_S:
        return None
    return last if _names(last) == {str(_rel(Path(p))) for p in paths} else None


def keep_set(snaps: list[Path], now: datetime) -> set[Path]:
    """Which snapshots survive a prune: the newest `KEEP`, every one of the previous UTC day, and
    the last of each day within `KEEP_DAYS` that the count left with none."""
    keep = set(snaps[-KEEP:]) if KEEP > 0 else set()
    yesterday = (now.date().toordinal() - 1)
    by_day: dict[int, list[Path]] = {}
    for d in snaps:
        by_day.setdefault(_stamp_of(d).date().toordinal(), []).append(d)
    keep |= set(by_day.get(yesterday, []))
    for day, ds in by_day.items():
        if now.date().toordinal() - day <= KEEP_DAYS and not keep & set(ds):
            keep.add(ds[-1])
    return keep


def _prune(root: Path) -> None:
    snaps = _snapshots(root)
    keep = keep_set(snaps, datetime.now(timezone.utc))
    for d in snaps:
        if d not in keep:
            shutil.rmtree(d, ignore_errors=True)


def _restore(path: Path, data: bytes | None) -> None:
    """Put one file back to `data` (None: it did not exist). Its own temp + replace, deliberately
    not `_write`, so a fault in the writer cannot also break the rollback."""
    path = Path(path)
    if data is None:
        path.unlink(missing_ok=True)
        return
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".restore")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def commit(changes: list[tuple[Path, str]], action: str) -> Path:
    """Back up every target, then write each one; on any failure restore those already written and
    re-raise. Returns the snapshot directory. Texts must already be validated by the caller."""
    changes = [(Path(p), t) for p, t in changes]
    if len({p.resolve() for p, _ in changes}) != len(changes):
        raise ValueError("one commit names the same file twice")
    with LOCK:
        before = {p: (p.read_bytes() if p.exists() else None) for p, _ in changes}
        snap = backup([p for p, _ in changes], action)
        written: list[Path] = []
        try:
            for p, text in changes:
                _write(p, text)
                written.append(p)
        except BaseException:
            for p in reversed(written):
                _restore(p, before[p])
            raise
        return snap


def log_refused(path: Path, line: str) -> None:
    """THE REFUSED-SAVES LOG (owner 2026-10-28): one JSON line appended to a GENERATED log, never
    curated data — kept here so `writes.py` stays the backend's only file writer."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with LOCK, path.open("a", encoding="utf-8") as f:
        f.write(line.rstrip("\n") + "\n")
