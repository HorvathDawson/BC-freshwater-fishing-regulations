"""THE PARTS AND THE PAINT CAN NEVER DIVERGE (T517, owner 2026-11-08: "we need to make sure it will always be synced to
the painted colors. They can never diverge" — and the same for the salmon colouring).

Read from the SHIPPED files of a built release, never from the bundle they came from: what the app paints (the status
index inside `paint`, its no-access half, the `salmon` head + shards) against what it lists (the `water` file's
`part`, `part_section`, `part_mark`). Every check is on the outputs, so a bug anywhere upstream that lets them drift
fails here.

  one part      every named section (the bundle's `item_section`) is in exactly one part of its own water: none
                missing, none twice, none in a part of another water or of no water
  uniform       every section of a part paints the same: one status profile (every day of the year), one no-access
                tier (> 500 permille of no-access land, the fourth colour) and one DFO salmon chain (the salmon colouring)
  maximal       no two parts of one water whose sections TOUCH carry the same key (akey) and paint: such a pair is one
                run the build failed to join. The adjacency is the bundle's `section_touch` (the atlas's own section
                adjacency), never the marks: the marks are what the build chose, so a check across them could not fail
                where the build declined to mark (T517 review: Wap Creek 1/3, Nimpkish 2/4, Skeena 1/3). The shipped
                files hold no adjacency of their own that sees a side channel's junction (`part_line` is per blue line,
                `section_down` only the gauge sheds), so the two inputs that are not outputs come from the bundle.
                A lake on a river's course is no boundary (owner 2026-11-08: same rules either side are one entry,
                named lake or not): the stem sections either side of one are adjacent too (`lake_gaps`, the spans).

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

from pipeline.deliver.bundle.access import MOSTLY_INSIDE_PERMILLE
from pipeline.deliver.status_index import Index

#: a section is the no-access colour when more than this many permille of it lie in no-access land: the build's own
#: `access.MOSTLY_INSIDE_PERMILLE` (pinned to the app's `MOSTLY_INSIDE`), never a second copy (T560 item 2)
NO_ACCESS_PERMILLE = MOSTLY_INSIDE_PERMILLE


class PaintSyncError(SystemExit):
    """The release lists parts the map would paint in more than one colour (or not as one run)."""


def status_profiles(status: bytes) -> Dict[int, int]:
    """{sid: profile index} from the status index bytes, read by the status index's own decoder (`status_index.Index`,
    the twin of `encode`: T560 item 2, no hand copy of the layout); a sid in no run is base (absent)."""
    try:
        return Index(status).sections
    except ValueError as e:
        raise PaintSyncError(f"paint sync: the paint file's status half is not a status index this build reads: {e}")


def problems(part_section: Iterable[Tuple[int, int, int]], parts: Mapping[Tuple[int, int], tuple],
             touches: Iterable[Tuple[int, int]], profile: Mapping[int, int],
             no_access: Iterable[int], chain: Mapping[int, int], named: Iterable[Tuple[int, int]],
             limit: int = 20) -> List[str]:
    """The failures, at most `limit`. `part_section` (ord, sid, part_ix); `parts` {(ord, part_ix): akey}; `touches`
    (sid, sid) every pair of sections that touch; `profile` {sid: status profile} (absent: base); `no_access` the sids
    painted no-access; `chain` {sid: salmon chain} (absent: 0, the unlisted chain); `named` (ord, sid) every section a
    named water owns — what the parts must cover."""
    out: List[str] = []
    na = set(no_access)
    seen: Dict[Tuple[int, int], Tuple[int, int]] = {}
    of: Dict[Tuple[int, int], List[int]] = defaultdict(list)
    for o, sid, ix in part_section:
        if (o, sid) in seen:
            out.append(f"section {sid} is in part {seen[(o, sid)]} and part {(o, ix)}")
        seen[(o, sid)] = (o, ix)
        of[(o, ix)].append(sid)
    want = set(named)
    for o, sid in sorted(want - set(seen)):
        out.append(f"section {sid} of water ord {o} is in no part")
    for o, sid in sorted(set(seen) - want):
        out.append(f"section {sid} is in part {seen[(o, sid)]} but water ord {o} does not own it")
    for k in parts:
        if k not in of:
            out.append(f"part {k} has no section")
    paint: Dict[Tuple[int, int], tuple] = {}
    for k, ss in sorted(of.items()):
        looks = {(profile.get(s, -1), s in na, chain.get(s, 0)) for s in ss}
        if len(looks) > 1:
            out.append(f"part {k} paints {len(looks)} ways: {sorted(looks)[:3]} (status profile, no-access, salmon chain)")
        paint[k] = next(iter(looks))
    by_sid: Dict[int, List[Tuple[int, int]]] = defaultdict(list)
    for (o, sid), k in seen.items():
        by_sid[sid].append(k)
    bad = set()
    for x, y in touches:
        for a in by_sid.get(x, ()):
            for b in by_sid.get(y, ()):
                if a != b and a[0] == b[0] and parts.get(a) == parts.get(b) and paint.get(a) == paint.get(b):
                    pair = tuple(sorted((a, b)))
                    if pair not in bad:
                        bad.add(pair)
                        out.append(f"parts {pair[0]} and {pair[1]} touch (sections {x}, {y}) with one key and one "
                                   f"paint: not one maximal run")
    return out[:limit]


def lake_gaps(stem: Iterable[Tuple[int, int, int, int, str, str]]) -> List[Tuple[int, int]]:
    """(T517, owner 2026-11-08) The sections either side of a LAKE on a water's main stem — any lake, named or not:
    `stem` (ord, sid, lo_m, hi_m, lo token, hi token); consecutive by measure, the lower one ending at a lake outlet and
    the upper one starting at a lake inlet."""
    by: Dict[int, List[tuple]] = defaultdict(list)
    for o, sid, lo, hi, a, b in stem:
        by[o].append((lo, hi, sid, a or "", b or ""))
    out: List[Tuple[int, int]] = []
    for o, rows in sorted(by.items()):
        rows.sort()
        for (_, h1, s1, _, top), (l2, _, s2, bottom, _) in zip(rows, rows[1:]):
            if l2 > h1 and top.startswith("lake_outlet") and bottom.startswith("lake_inlet"):
                out.append((s1, s2))
    return out


def no_access_sids(access: Mapping[str, list]) -> set:
    """The sections the map paints no-access: more than `NO_ACCESS_PERMILLE` of them in no-access land, summed over
    their no-access areas (`access.mostly_inside`) — from paint's decoded access half ({areas: [(tier, name)], rows:
    [(sid, area, permille)]}; the tier is its NAME)."""
    permille: Dict[int, int] = defaultdict(int)
    for sid, area, pm in access["rows"]:
        if access["areas"][area][0] == "no_access":
            permille[sid] += pm
    return {s for s, pm in permille.items() if pm > NO_ACCESS_PERMILLE}


def _sqlite(gz: Path, tmp: Path) -> sqlite3.Connection:
    p = tmp / gz.name.replace(".gz", "")
    p.write_bytes(gzip.decompress(gz.read_bytes()))
    return sqlite3.connect(p)


def check(release_dir: Path, bundle: Path) -> None:
    """Hold a built release's shipped files to `problems`, against `bundle`'s section adjacency and named sections;
    raise naming the first failures."""
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
        # every part, keyed or not: a part with no section is a failure (`problems`), never dropped (review finding 9)
        parts = {(o, ix): a for o, ix, a in water.execute("SELECT item_ord, part_ix, akey FROM part")}
    b = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    try:
        touches = b.execute("SELECT a, b FROM section_touch").fetchall()
        touches += lake_gaps(b.execute(
            "SELECT i.ord, s.sid, s.lo_m, s.hi_m, x.token, y.token FROM section_span s JOIN item_section i ON i.sid = s.sid "
            "JOIN span_end x ON x.eid = s.lo JOIN span_end y ON y.eid = s.hi WHERE s.off_stem = 0 AND s.lo_m IS NOT NULL "
            "AND s.hi_m IS NOT NULL"))
        named = b.execute("SELECT ord, sid FROM item_section").fetchall()
    finally:
        b.close()
    paint = decode_paint(gzip.decompress(path("paint").read_bytes()))
    profile = status_profiles(paint["status"])
    no_access = no_access_sids(paint["access"])
    blobs = {name: gzip.decompress(path(name).read_bytes()) for name in files if name.startswith("salmon")}
    _, _, chains = decode_files(blobs)
    bad = problems(ps, parts, touches, profile, no_access, dict(chains), named)
    if bad:
        raise PaintSyncError("paint sync (T517): the release lists parts the map does not paint as one run:\n  "
                             + "\n  ".join(bad))
    print(f"  paint sync: {len(parts):,} parts, {len(ps):,} sections — each part one status profile, one no-access "
          f"tier, one salmon chain; every named section in one part; no two touching parts share a key and paint")
