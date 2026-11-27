"""THE BIG SOURCE FILES BY CONTENT (T1220, T1223): the sha256 of a file's bytes, hashed once and cached.

Two readers, one cache:
  - the build's stage keys (`pipeline.build.stamp`: clim, tiles, deliver), which WRITE the cache beside their manifest
    (`--side` and tests keep their own; `--dry-run` writes nothing);
  - the atlas record (`pipeline.atlas.inputs.digest`, the `source:` keys), which only READS the live cache
    (`live_cache()`): the atlas check and `behind()` are write-free, and the atlas build may run into a scratch
    directory (`mini_release`) that must hold everything it writes.

THE HIT RULE: an entry answers only when the file's size, mtime_ns, ctime_ns AND inode all equal it (`HIT_FIELDS`).
ctime cannot be set back by `os.utime`, so an in-place edit at the same size with its mtime restored still rehashes. A
miss is hashed and, when the caller persists, stored. Every hash is also kept for the life of the process (`_MEMO`, by
the same fields), so a plan that keys the gpkg and then prints the atlas warning reads 10 GB once, not twice.

No fallback: a missing source file raises (FileNotFoundError, naming it); a cache that is not JSON, not this format, or
holds an entry without integer stat fields and a 64-hex sha256 raises, naming the file (it is only a cache: removing
it costs a rehash).
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path

#: the cache's file name; the build keeps it beside its manifest
NAME = "source-hashes.json"
FORMAT = 1
#: the stat fields an entry must match to be a hit
HIT_FIELDS = ("size", "mtime_ns", "ctime_ns", "ino")
#: {(resolved path, *HIT_FIELDS): sha256 hex}: every hash this process made or read
_MEMO: dict[tuple, str] = {}


def live_cache() -> Path:
    """The cache the live build writes (beside `data/generated/build-manifest.json`, `pipeline.build.MANIFEST`)."""
    from pipeline.common.curated import GENERATED
    return GENERATED.bundle.parent / NAME


def sha256(path: Path, *, cache: Path, root: Path, persist: bool) -> str:
    """The sha256 (64 hex) of `path`'s bytes (module docstring). `cache`: the cache file; `root`: what its entries are
    named relative to (a file outside it by its resolved path); `persist`: store a miss in `cache`."""
    path = Path(path)
    st = path.stat()
    resolved = path.resolve()
    try:
        name = str(resolved.relative_to(Path(root).resolve()))
    except ValueError:
        name = str(resolved)
    now = {"size": st.st_size, "mtime_ns": st.st_mtime_ns, "ctime_ns": st.st_ctime_ns, "ino": st.st_ino}
    memo = (str(resolved), *(now[k] for k in HIT_FIELDS))
    have = read(cache).get(name)
    if have is not None and all(have[k] == now[k] for k in HIT_FIELDS):
        _MEMO[memo] = have["sha256"]
        return have["sha256"]
    digest = _MEMO.get(memo)
    if digest is None:
        with path.open("rb") as f:
            digest = hashlib.file_digest(f, "sha256").hexdigest()
        _MEMO[memo] = digest
    if persist:
        known = read(cache)                   # another process may have added one meanwhile: keep it
        known[name] = {**now, "sha256": digest}
        _write(cache, known)
    return digest


def read(cache: Path) -> dict:
    """The cache's entries; {} when there is none. A damaged one raises, naming it (module docstring)."""
    p = Path(cache)
    if not p.exists():
        return {}
    fix = "it is only a cache of the source files' sha256 — remove it and the next key rehashes them"
    try:
        doc = json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise SystemExit(f"build: {p} is not JSON ({e}): {fix}") from None
    if not isinstance(doc, dict) or doc.get("format") != FORMAT or not isinstance(doc.get("files"), dict):
        raise SystemExit(f"build: {p} is not a source-hash cache of format {FORMAT}: {fix}")
    for name, e in doc["files"].items():
        ok = (isinstance(e, dict) and all(type(e.get(k)) is int for k in HIT_FIELDS)
              and isinstance(e.get("sha256"), str) and re.fullmatch(r"[0-9a-f]{64}", e["sha256"]) is not None)
        if not ok:
            raise SystemExit(f"build: {p}: the entry for {name} is not {{{', '.join(HIT_FIELDS)}: int, "
                             f"sha256: 64 hex}}: {fix}")
    return doc["files"]


def _write(cache: Path, known: dict) -> None:
    """Atomically (a per-process temp file + `os.replace`); two writers at once lose at most an entry (a rehash)."""
    p = Path(cache)
    p.parent.mkdir(parents=True, exist_ok=True)                 # the writer creates its directory (AGENTS 41)
    tmp = p.with_name(f"{p.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps({"format": FORMAT, "files": known}, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, p)
