"""THE ONE WRITER FOR CURATED DATA (T600; T599 findings §4 item 2, owner 2026-11-10).

Every program that changes a file under the curated tree (`CURATED`, or the whole-tree copy `CURATED_ROOT` names)
calls `write` (or `write_any` when its `--out` may also be a scratch path). One call does, in order:

  1. REFUSE a path outside the curated tree (`write`; `write_any` sends it to `atomic_write` instead, said by name).
  2. VALIDATE the new text: a `.json` file must parse, a `.jsonl` file line by line; a dict-shaped file that had an
     `_about` keeps one (AGENTS 36a); a hand-edited WATERS file passes `check_curated.check_waters` read with the new
     text in its place; and the caller's own `validate(text)` (the synopsis files: `CatalogueFile`, in
     `parsing.io.write_entryfile`). A refused write changes nothing.
  3. BACK UP the old file (the last `BACKUPS_KEPT` per file) — AGENTS A2's manual `cp`, made automatic.
  4. WRITE ATOMICALLY (`atomic_write`: a temp file in the same folder + `os.replace`; the ONE copy of that dance).
  5. LOG one line (`log.jsonl`): the file, the tool, who, when, the source, the old and new sha256, the backup.

Same bytes as on disk: nothing is written, backed up or logged. Backups and the log are MACHINE output, so they live
in `generated/curated_writes/` (beside a `CURATED_ROOT` copy, never inside it, for a sandboxed tree); git stays the
history of record. Provenance (who / when / tool / source) is on the LOG RECORD: record schemas such as
`CatalogueEntry` refuse unknown fields, so stamping each data record would change what a build reads (T600 "never
what is built").

What may NOT call this: a build step (atlas, reach, tiles, deliver). `test_atlas_inputs` runs them under a read-only
curated tree; `test_curated_writer` refuses a module that writes the curated tree any other way.
"""
from __future__ import annotations

import getpass
import hashlib
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

#: how many old versions of one curated file the writer keeps
BACKUPS_KEPT = 20
#: who writes, when the caller does not say (`by=`): this environment variable, else the login name
BY_ENV = "CURATED_WRITE_BY"


def atomic_write(path: Path, data: str | bytes) -> None:
    """Write via a temp file in the same folder + `os.replace`, so a crash never leaves a half-written file. THE one
    implementation (`parsing.io.atomic_write` is this)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data.encode("utf-8") if isinstance(data, str) else data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def curated_root() -> Path:
    """The curated tree this process writes: the `CURATED_ROOT` copy when one is set, else the live tree."""
    from pipeline.common import curated as C
    return (C.sandbox_root() or C.CURATED.base).resolve()


def under_curated(path: Path) -> bool:
    p, root = Path(path).resolve(), curated_root()
    return p == root or root in p.parents


def writes_root() -> Path:
    """Where the backups and the log go: `generated/curated_writes/`, or beside a `CURATED_ROOT` copy."""
    from pipeline.common import curated as C
    sandbox = C.sandbox_root()
    d = sandbox.parent / f"{sandbox.name}.writes" if sandbox else C.GENERATED.mkdir("curated_writes")
    d.mkdir(parents=True, exist_ok=True)
    return d


def _tool() -> str:
    main = sys.modules.get("__main__")
    spec = getattr(main, "__spec__", None)
    return (spec.name if spec and spec.name else Path(sys.argv[0] or "python").name) or "python"


def _sha(b: bytes | None) -> str | None:
    return hashlib.sha256(b).hexdigest() if b is not None else None


class CuratedWriteRefused(ValueError):
    """The writer refused: nothing was written."""


def _waters_key(path: Path) -> str | None:
    """The `CURATED.waters` key `path` is (in this tree), or None."""
    from pipeline.common import curated as C
    w = C.CURATED.waters
    for k in type(w).model_fields:
        v = getattr(w, k)
        if isinstance(v, Path) and v.suffix == ".json" and v.resolve() == path.resolve():
            return k
    return None


def _validate(path: Path, text: str, old: bytes | None, validate: Callable[[str], None] | None) -> None:
    name = path.name
    if path.suffix == ".json":
        try:
            doc = json.loads(text)
        except json.JSONDecodeError as e:
            raise CuratedWriteRefused(f"{name}: the new text is not valid JSON ({e}) — nothing written") from e
        if old is not None and isinstance(doc, dict):
            try:
                had = json.loads(old.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                had = None
            if isinstance(had, dict) and "_about" in had and "_about" not in doc:
                raise CuratedWriteRefused(f"{name}: the new text drops the file's `_about` (AGENTS 36a) — nothing written")
    elif path.suffix == ".jsonl":
        for i, line in enumerate(text.splitlines(), 1):
            if line.strip():
                try:
                    json.loads(line)
                except json.JSONDecodeError as e:
                    raise CuratedWriteRefused(f"{name}:{i}: not a JSON line ({e}) — nothing written") from e
    key = _waters_key(path) if path.suffix == ".json" else None
    if key is not None:
        _check_waters_with(key, path, text)
    if validate is not None:
        try:
            validate(text)
        except CuratedWriteRefused:
            raise
        except Exception as e:     # the caller's schema said no: refuse, naming it — never written
            raise CuratedWriteRefused(f"{name}: {type(e).__name__}: {e} — nothing written") from e


def _check_waters_with(key: str, path: Path, text: str) -> None:
    """`check_curated.check_waters` over the live waters files with THIS file's new text in its place; an error
    refuses the write (warnings and notes do not)."""
    from pipeline.common import curated as C
    from pipeline.tools.audit.check_curated import check_waters
    w = C.CURATED.waters
    with tempfile.TemporaryDirectory() as d:
        stand_in = Path(d) / path.name
        stand_in.write_text(text, encoding="utf-8")
        errors, _warn, _notes = check_waters(w.model_copy(update={key: stand_in}), registry=None)
    if errors:
        raise CuratedWriteRefused(f"{path.name}: check_curated --waters refuses the new text: "
                                  + "; ".join(errors[:5]) + (" …" if len(errors) > 5 else "") + " — nothing written")


def _backup(path: Path, old: bytes, stamp: str) -> Path:
    rel = path.resolve().relative_to(curated_root())
    d = writes_root() / "backups" / rel
    d.mkdir(parents=True, exist_ok=True)
    dst = d / f"{stamp}-{_sha(old)[:12]}{path.suffix}"
    atomic_write(dst, old)
    kept = sorted(p for p in d.iterdir() if p.is_file())
    for p in kept[:-BACKUPS_KEPT]:
        p.unlink()
    return dst


def write(path: Path, text: str, *, tool: str | None = None, by: str | None = None, source: str = "",
          validate: Callable[[str], None] | None = None) -> dict | None:
    """Write `text` to the curated file `path`: refuse outside the tree, validate, back up, write atomically, log.
    Returns the log record, or None when the file already holds exactly this text (nothing done)."""
    path = Path(path)
    if not under_curated(path):
        raise CuratedWriteRefused(f"{path} is not under the curated tree ({curated_root()}): `curated_write.write` "
                                  "writes curated files only (`write_any` for a path that may be scratch)")
    old = path.read_bytes() if path.exists() else None
    new = text.encode("utf-8")
    if old == new:
        return None
    _validate(path, text, old, validate)
    now = datetime.now(timezone.utc)
    stamp = now.strftime("%Y%m%dT%H%M%S%fZ")
    backup = _backup(path, old, stamp) if old is not None else None
    atomic_write(path, new)
    rec = {"at": now.isoformat(timespec="seconds"), "file": path.resolve().relative_to(curated_root()).as_posix(),
           "tool": tool or _tool(), "by": by or os.environ.get(BY_ENV) or getpass.getuser(), "source": source,
           "old_sha256": _sha(old), "new_sha256": _sha(new), "bytes": len(new),
           "backup": backup.relative_to(writes_root()).as_posix() if backup else None}
    with (writes_root() / "log.jsonl").open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False, sort_keys=True) + "\n")
    return rec


def write_any(path: Path, text: str, **kw) -> dict | None:
    """`write` when `path` is in the curated tree; any other path (a test's tmp dir, a side copy) gets the plain
    `atomic_write`. For tools whose output path is an argument that defaults to a curated file."""
    if under_curated(path):
        return write(path, text, **kw)
    atomic_write(Path(path), text)
    return None
