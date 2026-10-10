"""EVERY write the app makes to curated data goes through here: back up, then commit, all or nothing.

    commit([(path, text), ...], action)

1. BACKUP FIRST. Each target is copied, as it is on disk now, into one timestamped snapshot
   directory before anything is written: `<BACKUP_ROOT>/<UTC stamp>-<action>/<parent>/<name>`
   (`<parent>` keeps `catalogue/region-1.json` and `dfo_salmon/region-1.json` apart). A target that
   does not exist yet (the verification sidecar before the first mark) is listed in the snapshot's
   `ABSENT` file instead. BACKUP_ROOT is `GENERATED.regs.entries_backup / "curation-review"` — the
   same root `pipeline.tools.reparse_candidates --backup-dir` snapshots into — or
   `CURATION_BACKUP_DIR` (the tests point it at a temp dir). It is BOUNDED: the newest
   `CURATION_BACKUP_KEEP` (default 500) snapshots are kept, older ones are pruned after each write.

2. ALL OR NOTHING. The caller computes and validates every new file's text first; nothing is
   written until all of it is ready. Each file is then written by temp file + `os.replace`
   (`io.atomic_write`), so no single file is ever half-written. If ANY write fails, every file
   already written in this commit is put back to its pre-commit bytes (or removed, if it did not
   exist), and the error is re-raised. A split rename touches splits.json and every region file
   that binds the split: a failure mid-way can no longer leave a rule naming a split that is gone.

`LOCK` serialises the app's read-compute-commit sequences (FastAPI runs sync endpoints on a
thread pool), so two saves to one region file cannot interleave and drop one of them.
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


def _prune(root: Path) -> None:
    snaps = sorted(d for d in root.iterdir() if d.is_dir() and _SNAPSHOT_RE.fullmatch(d.name))
    for d in snaps[:max(0, len(snaps) - KEEP)]:
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
