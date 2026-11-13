"""THE PART NAMES, CHECKED (T517, owner 2026-11-09: "fail fast"). Every named water's parts are said by the app's one
function (`@app/core` `spanName`): "<lower end> → <upper end>" from the part's two END TOKENS through the release's
`display.ends`, or — a part with no ends (no main stem, the only such part of its water) — the water's own name. This
checks the names that rule composes:

  missing   a keyed part with only one end token
  no words  an end token with no `display.ends` entry
  twice     two parts of one water that compose the same name
  numbered  a name ending " (n)" ("probably an error": parts are told apart by their ends, never numbered)
  gauge     a gauge code ("08FC003") in a name: a gauge's code is never a place's words (it is a mark's `alt`)
  within    a "Within <area>" name: every part is said by its two ends

`problems(waters, ends)` is the pure core; the answers step (`display.section.section_static`) runs it the moment the
part facts and the ends table exist — before the store, the release and the paint — and stops on any failure. The
release build runs it AGAIN on the SHIPPED water file (`check_release`, beside the paint sync), with the run rules:

  run gap   the pieces of one run (same grp + run, cut by the salmon chain) not all joined by marks — the app joins
            neighbours only across a mark, so such a run could never read as one (review finding 10)
  run ends  a run of several pieces whose first piece has no lower end or last piece no upper end — its joined name
            ("first lo → last hi") could not be said (review finding 11)
  slice     a web slice (`water-<region>`) whose parts or marks differ from the whole water file's for a water it
            holds (review finding 9: a tap in a region reads the slice)
`python -m pipeline.deliver.answers.display.name_check [RELEASE_DIR]` checks a BUILT release's water file in seconds,
with no rebuild (default: the side release named by `--side`, else `data/release/LATEST`).
"""
from __future__ import annotations

import argparse
import gzip
import re
import sqlite3
import sys
import tempfile
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

GAUGE_CODE = re.compile(r"\b\d{2}[A-Z]{2}\d{3}\b")
NUMBERED = re.compile(r" \(\d+\)$")


def words(ends: Mapping[str, Sequence[str]], token: str, side: str) -> Optional[str]:
    """The words of an end token as the app says them: the upper end its first words, the lower end its `to:` words
    when it has them. None: the token has no entry."""
    v = ends.get(token)
    if not v:
        return None
    if side == "lo":
        for x in v[1:]:
            if x.startswith("to:"):
                return x[3:]
    return v[0]


def problems(waters: Iterable[Tuple[str, str, Sequence[Optional[dict]]]], ends: Mapping[str, Sequence[str]],
             limit: int = 30) -> List[str]:
    """`waters`: (water id, its name, its parts' facts — {lo, hi} — None for a part with no rules); `ends`: the
    `display.ends` table. The failures, at most `limit`."""
    out: List[str] = []
    for wid, water, parts in waters:
        said: Dict[str, int] = {}
        for i, f in enumerate(parts):
            if f is None:
                continue
            lo, hi = f.get("lo"), f.get("hi")
            if (lo is None) != (hi is None):
                out.append(f"missing: {wid} part {i} has lo={lo!r} hi={hi!r}")
                continue
            if lo is not None:
                a, b = words(ends, lo, "lo"), words(ends, hi, "hi")
                if a is None or b is None:
                    out.append(f"no words: {wid} part {i}: {[t for t, w in ((lo, a), (hi, b)) if w is None]} "
                               f"not in display.ends")
                    continue
                # lo = hi = "near:…": a part with no main stem told apart by where it lies, "<water> near <it>"
                name = f"{water} {b[:1].lower() + b[1:]}" if lo == hi and lo.startswith("near:") else f"{a} → {b}"
            else:
                name = water
            if name in said:
                out.append(f"twice: {wid} parts {said[name]} and {i} both say {name!r}")
            said.setdefault(name, i)
            if lo is not None and NUMBERED.search(name):        # a water's own sourced name is said as sourced
                out.append(f"numbered: {wid} part {i} says {name!r}")
            if lo is not None and GAUGE_CODE.search(name):
                out.append(f"gauge: {wid} part {i} says {name!r}")
            if re.search(r"(^|→ )Within ", name):
                out.append(f"within: {wid} part {i} says {name!r}")
    return out[:limit]


class NameCheckError(SystemExit):
    """Two parts of a water would say one name, or a name breaks the rules above."""


def check(waters: Iterable[Tuple[str, str, Sequence[Optional[dict]]]], ends: Mapping[str, Sequence[str]]) -> None:
    bad = problems(waters, ends)
    if bad:
        raise NameCheckError("part names (T517, `answers.display.name_check`):\n  " + "\n  ".join(bad))


def run_problems(parts: Iterable[Tuple[int, int, int, int, Optional[str], Optional[str]]],
                 marks: Iterable[Tuple[int, int, Sequence[int]]], limit: int = 30) -> List[str]:
    """`parts` (ord, part_ix, grp, run, lo, hi); `marks` (ord, down, [up]). The run rules above."""
    runs: Dict[Tuple[int, int, int], List[Tuple[int, Optional[str], Optional[str]]]] = {}
    for o, ix, g, r, lo, hi in parts:
        runs.setdefault((o, g, r), []).append((ix, lo, hi))
    linked: Dict[int, List[Tuple[int, int]]] = {}
    for o, d, ups in marks:
        linked.setdefault(o, []).extend((d, u) for u in ups)
    out: List[str] = []
    for (o, g, r), ps in sorted(runs.items()):
        if len(ps) < 2:
            continue
        ps.sort()
        ixs = {ix for ix, _, _ in ps}
        root = {ix: ix for ix in ixs}

        def find(x: int) -> int:
            while root[x] != x:
                x = root[x]
            return x
        for d, u in linked.get(o, ()):
            if d in ixs and u in ixs:
                root[find(max(d, u))] = find(min(d, u))
        if len({find(ix) for ix in ixs}) > 1:
            out.append(f"run gap: water ord {o} run (grp {g}, run {r}) pieces {sorted(ixs)} are not all joined by marks")
        if ps[0][1] is None or ps[-1][2] is None:
            out.append(f"run ends: water ord {o} run (grp {g}, run {r}): first piece {ps[0][0]} lo={ps[0][1]!r}, last "
                       f"piece {ps[-1][0]} hi={ps[-1][2]!r} — its joined name cannot be said")
    return out[:limit]


def check_release(release: Path) -> None:
    """The names and run rules on a BUILT release's water file; raise naming the first failures."""
    waters, ends = release_waters(release)
    bad = problems(waters, ends) + run_problems(*_release_runs(release)) + slice_problems(release)
    if bad:
        raise NameCheckError(f"part names (T517) on the shipped release {Path(release).name}:\n  " + "\n  ".join(bad))
    print(f"  name check: {sum(1 for _, _, ps in waters for f in ps if f):,} keyed parts — every name its own, every "
          f"multi-piece run joined by marks with two ends")


def _water_db(release: Path, tmp: str) -> sqlite3.Connection:
    gz = next(p for p in Path(release).glob("water-*.sqlite.gz") if re.fullmatch(r"water-[0-9a-f]{12}\.sqlite\.gz",
                                                                                   p.name))
    p = Path(tmp) / "water.sqlite"
    p.write_bytes(gzip.decompress(gz.read_bytes()))
    return sqlite3.connect(p)


_SLICE_Q = (("part", "SELECT p.item_ord, p.part_ix, p.akey, p.grp, p.run, l.text, h.text FROM part p "
                      "LEFT JOIN str l ON l.id = p.lo_s LEFT JOIN str h ON h.id = p.hi_s"),
            ("part_mark", "SELECT ord, mark_ix, token, down_part, up_parts, confidence, alt FROM part_mark"),
            ("part_section", "SELECT ord, sid, part_ix FROM part_section"))


def slice_problems(release: Path, limit: int = 30) -> List[str]:
    """Every `water-<region>` slice holds, for each water it holds, exactly the whole file's parts, marks and
    sections."""
    out: List[str] = []
    with tempfile.TemporaryDirectory() as t:
        whole = _water_db(release, t)
        full = {k: {} for k, _ in _SLICE_Q}
        for k, q in _SLICE_Q:
            for row in whole.execute(q):
                full[k].setdefault(row[0], set()).add(row)
        whole.close()
        for gz in sorted(Path(release).glob("water-*.sqlite.gz")):
            if re.fullmatch(r"water-[0-9a-f]{12}\.sqlite\.gz", gz.name) or gz.name.startswith("water-shared"):
                continue
            p = Path(t) / gz.name.replace(".gz", "")
            p.write_bytes(gzip.decompress(gz.read_bytes()))
            db = sqlite3.connect(p)
            for k, q in _SLICE_Q:
                got: Dict[int, set] = {}
                for row in db.execute(q):
                    got.setdefault(row[0], set()).add(row)
                for o, rows in sorted(got.items()):
                    if rows != full[k].get(o, set()):
                        out.append(f"slice: {gz.name} {k} of water ord {o} differs from the whole water file")
            db.close()
    return out[:limit]


def _release_runs(release: Path):
    import json
    with tempfile.TemporaryDirectory() as t:
        db = _water_db(release, t)
        s = dict(db.execute("SELECT id, text FROM str"))
        parts = [(o, ix, g, r, s.get(lo), s.get(hi)) for o, ix, g, r, lo, hi in db.execute(
            "SELECT item_ord, part_ix, grp, run, lo_s, hi_s FROM part WHERE akey IS NOT NULL")]
        marks = [(o, d, json.loads(u)) for o, d, u in db.execute("SELECT ord, down_part, up_parts FROM part_mark")]
        db.close()
    return parts, marks


def release_waters(release: Path) -> Tuple[List[Tuple[str, str, List[Optional[dict]]]], dict]:
    """(waters, ends) read from a built release's whole water file."""
    import json
    from pipeline.deliver.store.decode import unpack
    gz = next(p for p in Path(release).glob("water-*.sqlite.gz") if re.fullmatch(r"water-[0-9a-f]{12}\.sqlite\.gz",
                                                                                   p.name))
    with tempfile.TemporaryDirectory() as t:
        p = Path(t) / "water.sqlite"
        p.write_bytes(gzip.decompress(gz.read_bytes()))
        db = sqlite3.connect(p)
        s = dict(db.execute("SELECT id, text FROM str"))
        zd = dict(db.execute("SELECT tbl, d FROM zdict"))
        row = db.execute("SELECT j FROM static WHERE k = 'display.ends'").fetchone()
        if row is None:
            raise NameCheckError(f"{gz}: no static display.ends — not a T517 release")
        ends = json.loads(unpack(row[0], zd.get("static")))
        by: Dict[Tuple[str, str], List[Optional[dict]]] = {}
        for o, iid, name, ix, akey, lo, hi in db.execute(
                "SELECT p.item_ord, i.item_id, i.name, p.part_ix, p.akey, p.lo_s, p.hi_s FROM part p "
                "JOIN item i ON i.item_ord = p.item_ord ORDER BY p.item_ord, p.part_ix"):
            by.setdefault((s[iid], name), []).append(None if akey is None else {"lo": s.get(lo), "hi": s.get(hi)})
        db.close()
    return sorted((w, n, ps) for (w, n), ps in by.items()), ends


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("release", nargs="?", help="a release directory (default: data/release/LATEST)")
    a = ap.parse_args(argv)
    if a.release:
        rel = Path(a.release)
    else:
        from pipeline.common.curated import GENERATED
        from pipeline.deliver.release.manifest import live
        rel = live(GENERATED.release)
    waters, ends = release_waters(rel)
    bad = (problems(waters, ends, limit=10_000) + run_problems(*_release_runs(rel), limit=10_000)
           + slice_problems(rel, limit=10_000))
    for b in bad:
        print(b)
    n = sum(1 for _, _, ps in waters for f in ps if f)
    print(f"name check {rel.name}: {n:,} keyed parts of {len(waters):,} waters — {len(bad)} problem(s)")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
