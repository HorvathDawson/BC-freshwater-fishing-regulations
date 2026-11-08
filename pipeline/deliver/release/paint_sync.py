"""THE PARTS AND THE PAINT CAN NEVER DIVERGE (T517, owner 2026-11-08: "we need to make sure it will always be synced to
the painted colors. They can never diverge" — and the same for the salmon colouring).

Read from the SHIPPED files of a built release, never from the bundle they came from: what the app paints (the status
index inside `paint`, its no-access half, the `salmon` head + shards) against what it lists (the `water` file's
`part`, `part_section`, `part_mark`). Every check is on the outputs, so a bug anywhere upstream that lets them drift
fails here.

  one part      every named section is in exactly one part
  uniform       every section of a part paints the same: one status profile (every day of the year), one no-access
                tier (> 500 permille of no-access land, the fourth colour) and one DFO salmon chain (the salmon colouring)
  maximal       no two parts that meet at a mark carry the same key (akey, salmon chain, no-access): such a pair is one
                run the build failed to join

`check(release_dir)` raises `PaintSyncError` naming the first failures; `problems(...)` is the pure core the tests
mutate.
"""
from __future__ import annotations

import gzip
import json
import re
import sqlite3
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Sequence, Tuple

from pipeline.deliver.binary import read_varint

#: a section is the no-access colour when more than this many permille of it lie in no-access land (`access.mostly_inside`)
NO_ACCESS_PERMILLE = 500


class PaintSyncError(SystemExit):
    """The release lists parts the map would paint in more than one colour (or not as one run)."""


def status_profiles(status: bytes) -> Dict[int, int]:
    """{sid: profile index} from the status index bytes (`status_index` format 2); a sid in no run is base (absent)."""
    if status[:4] != b"BCSI" or status[4] != 2:
        raise PaintSyncError("paint sync: the paint file's status half is not BCSI v2")
    i = 5 + 8 + 8
    n_prof, i = read_varint(status, i)
    for _ in range(n_prof):
        n_runs, i = read_varint(status, i)
        for _ in range(n_runs):
            _, i = read_varint(status, i)
    n, i = read_varint(status, i)
    cols = []
    for _ in range(3):
        col = []
        for _ in range(n):
            v, i = read_varint(status, i)
            col.append(v)
        cols.append(col)
    out: Dict[int, int] = {}
    end = 0
    for gap, count, prof in zip(*cols):
        start = end + gap
        for s in range(start, start + count):
            out[s] = prof
        end = start + count
    return out


def problems(part_section: Iterable[Tuple[int, int, int]], parts: Mapping[Tuple[int, int], tuple],
             marks: Iterable[Tuple[int, int, Sequence[int]]], profile: Mapping[int, int],
             no_access: Iterable[int], chain: Mapping[int, int], limit: int = 20) -> List[str]:
    """The failures, at most `limit`. `part_section` (ord, sid, part_ix); `parts` {(ord, part_ix): akey}; `marks`
    (ord, down, [up]); `profile` {sid: status profile} (absent: base); `no_access` the sids painted no-access; `chain`
    {sid: salmon chain} (absent: 0, the unlisted chain)."""
    out: List[str] = []
    na = set(no_access)
    seen: Dict[int, Tuple[int, int]] = {}
    of: Dict[Tuple[int, int], List[int]] = defaultdict(list)
    for o, sid, ix in part_section:
        if sid in seen:
            out.append(f"section {sid} is in part {seen[sid]} and part {(o, ix)}")
        seen[sid] = (o, ix)
        of[(o, ix)].append(sid)
    for k in parts:
        if k not in of:
            out.append(f"part {k} has no section")
    paint: Dict[Tuple[int, int], tuple] = {}
    for k, ss in sorted(of.items()):
        looks = {(profile.get(s, -1), s in na, chain.get(s, 0)) for s in ss}
        if len(looks) > 1:
            out.append(f"part {k} paints {len(looks)} ways: {sorted(looks)[:3]} (status profile, no-access, salmon chain)")
        paint[k] = next(iter(looks))
    for o, down, ups in marks:
        for u in ups:
            a, b = (o, down), (o, u)
            if a in parts and b in parts and parts[a] == parts[b] and paint.get(a) == paint.get(b):
                out.append(f"parts {a} and {b} meet at a mark with one key and one paint: not one maximal run")
    return out[:limit]


def _sqlite(gz: Path, tmp: Path) -> sqlite3.Connection:
    p = tmp / gz.name.replace(".gz", "")
    p.write_bytes(gzip.decompress(gz.read_bytes()))
    return sqlite3.connect(p)


def check(release_dir: Path) -> None:
    """Hold a built release's shipped files to `problems`; raise naming the first failures."""
    from pipeline.deliver.release.paint import decode as decode_paint
    from pipeline.deliver.salmon import decode_files
    rel = Path(release_dir)
    files = json.loads((rel / "manifest.json").read_text())["files"]

    def path(name: str) -> Path:
        return rel / Path(files[name]["url"]).name

    with tempfile.TemporaryDirectory() as t:
        water = _sqlite(next(p for p in rel.glob("water-*.sqlite.gz")
                             if re.fullmatch(r"water-[0-9a-f]{12}\.sqlite\.gz", p.name)), Path(t))
        ps = water.execute("SELECT ord, sid, part_ix FROM part_section").fetchall()
        parts = {(o, ix): a for o, ix, a in water.execute("SELECT item_ord, part_ix, akey FROM part")}
        marks = [(o, d, json.loads(u)) for o, d, u in water.execute("SELECT ord, down_part, up_parts FROM part_mark")]
        named = {(o, ix) for o, _, ix in ps}
        parts = {k: v for k, v in parts.items() if k in named or v is not None}
    paint = decode_paint(gzip.decompress(path("paint").read_bytes()))
    profile = status_profiles(paint["status"])
    tiers = paint["access"]["areas"]
    permille: Dict[int, int] = defaultdict(int)
    for sid, area, pm in paint["access"]["rows"]:
        if tiers[area][0] == 0:                       # tier code 0: no_access (`access.TIERS` order)
            permille[sid] += pm
    blobs = {name: gzip.decompress(path(name).read_bytes()) for name in files if name.startswith("salmon")}
    _, _, chains = decode_files(blobs)
    bad = problems(ps, parts, marks, profile, (s for s, pm in permille.items() if pm > NO_ACCESS_PERMILLE),
                   dict(chains))
    if bad:
        raise PaintSyncError("paint sync (T517): the release lists parts the map does not paint as one run:\n  "
                             + "\n  ".join(bad))
    print(f"  paint sync: {len(parts):,} parts, {len(ps):,} sections — each part one status profile, one no-access "
          f"tier, one salmon chain; no two meeting parts share a key")
