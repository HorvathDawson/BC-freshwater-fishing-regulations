"""(T520) RESOLVE THE IN-SEASON MATCHES WHERE THE ATLAS IS — to FWA ranges on one data release.

    python -m pipeline.regs.inseason.resolve [--release DIR] [--matches FILE] [--check]

Every match in `data/curated/inseason/notice_waters.json` (`matches.Match.extents`) is resolved on the atlas
(`points.resolve`: the regs' resolver, the measure-cut pick-points, the tributary walk) and written back as `waters`,
on the release the file then names (`release`, default `data/release/LATEST`; refused unless the release's
`section_handles` digest is the atlas's):

    {item: {"all": true}}                           every section of the water (in that release's `part_section`)
    {item: {"runs": [[blk, m0, m1], …], "lakes"?: [i, …], "ends": …}}
                                                    the covered stream sections as FWA ranges: the sections of the
                                                    release whose START lies in [m0, m1) on stream `blk`; `lakes` the
                                                    covered LAKE pieces of a river (no line: their places among the
                                                    water's lake pieces, handle order); `ends` the extent's own places
                                                    in words ("From Hell's Gate to Spuzzum Creek confluence")

No part index, label or section id (AGENTS 5): a range survives a renumbering of parts (T517). A lake or wetland is
whole or not at all (it has no line). Each range set is proved on the spot: read back by the device's rule over the
release's own `lines` file it must give exactly the resolved sections, or the match is refused.

Who runs it: the review app on save (`curation-review/backend/inseason.py`, one match), the release promote
(`deploy/staging/upload.sh`, every match, `--release` the release going up). `publish` only formats the result (no atlas).

A match that does not resolve keeps its extents, gets `unresolved` (the reason) and no `waters`, and the run exits 1:
publish refuses it until it is re-matched. Deterministic (sorted everywhere).
"""
from __future__ import annotations

import argparse
import gzip
import json
import re
import sqlite3
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping

from pipeline.regs.inseason import matches as M

#: a lake / wetland item: whole-item only (its sections have no line)
WHOLE_ONLY_KINDS = ("lake", "wetland")


class Unresolved(ValueError):
    pass


def release_dir(arg: Path | str | None = None) -> Path:
    from pipeline.common.curated import GENERATED
    if arg:
        return Path(arg)
    root = Path(GENERATED.release)
    return root / (root / "LATEST").read_text(encoding="utf-8").strip()


def release_name(rel: Path) -> str:
    manifest = json.loads((rel / "manifest.json").read_text(encoding="utf-8"))
    return str(manifest.get("data_release") or rel.name)


def _whole_file(rel: Path, name: str) -> Path:
    """The WHOLE file `name-<sha12>.sqlite.gz` (not T256's per-region slices)."""
    got = [p for p in rel.glob(f"{name}-*.sqlite.gz") if re.fullmatch(rf"{name}-[0-9a-f]{{12}}\.sqlite\.gz", p.name)]
    if len(got) != 1:
        raise SystemExit(f"inseason.resolve: release {rel.name} has {len(got)} whole {name} file(s), not 1")
    return got[0]


def _open(gz: Path, tmp: Path) -> sqlite3.Connection:
    out = tmp / gz.name.replace(".gz", "")
    out.write_bytes(gzip.decompress(gz.read_bytes()))
    return sqlite3.connect(str(out))


@dataclass
class Release:
    """What of one release the ranges are made on and proved against."""
    name: str
    handles: str                                   # meta.section_handles
    ord_of: dict[str, int]                         # item_id -> ord
    kind_of: dict[str, str]                        # item_id -> kind
    members: dict[int, frozenset[int]]             # ord -> its sections (part_section; part_ix never read)
    starts: dict[int, tuple[int, int]]             # stream handle -> (blk, from_m), the release's `lines`

    def item_sections(self, item: str) -> frozenset[int]:
        o = self.ord_of.get(item)
        return self.members.get(o, frozenset()) if o is not None else frozenset()

    def lake_pieces(self, item: str) -> list[int]:
        """The water's sections with no line (its lake pieces), in handle order — `lakes` indexes these, as the
        device does (`@app/core` `inSeasonCover`: a page section with no line)."""
        return sorted(h for h in self.item_sections(item) if h not in self.starts)


def read_release(rel: Path) -> Release:
    from pipeline.deliver.release import lines as L
    with tempfile.TemporaryDirectory() as t:
        core = _open(_whole_file(rel, "core"), Path(t))
        water = _open(_whole_file(rel, "water"), Path(t))
        ln = _open(_whole_file(rel, "lines"), Path(t))
        meta = dict(core.execute("SELECT k, v FROM meta").fetchall())
        ord_of, kind_of = {}, {}
        for iid, o, k in core.execute("SELECT item_id, ord, kind FROM item"):
            ord_of[iid], kind_of[iid] = int(o), str(k or "")
        mem: dict[int, set[int]] = {}
        for o, sid in water.execute("SELECT ord, sid FROM part_section"):
            mem.setdefault(int(o), set()).add(int(sid))
        starts: dict[int, tuple[int, int]] = {}
        for block, n, b in ln.execute("SELECT block, n, b FROM section_line ORDER BY block"):
            for i, (blk, lo, _hi) in enumerate(L.decode_block(bytes(b), int(n))):
                h = (int(block) << L.BLOCK_BITS) | i
                if h:                                  # handle 0: the filler
                    starts[h] = (blk, lo)
        for c in (core, water, ln):
            c.close()
    return Release(release_name(rel), str(meta.get("section_handles") or ""), ord_of, kind_of,
                   {o: frozenset(s) for o, s in mem.items()}, starts)


def covered_by_runs(runs: Iterable[Iterable[int]], sections: Iterable[int], starts: Mapping[int, tuple[int, int]]
                    ) -> set[int]:
    """THE DEVICE'S RULE (`@app/core` `inSeasonCover`): the sections whose start lies in a run [m0, m1) on its stream."""
    by_blk: dict[int, list[tuple[int, int]]] = {}
    for blk, m0, m1 in runs:
        by_blk.setdefault(int(blk), []).append((int(m0), int(m1)))
    out = set()
    for h in sections:
        at = starts.get(h)
        if at and any(m0 <= at[1] < m1 for m0, m1 in by_blk.get(at[0], ())):
            out.add(h)
    return out


def make_runs(secs: Iterable[int], span_of: Mapping[int, tuple[int, int, int]]) -> list[list[int]]:
    """Covered stream handles -> [blk, m0, m1] ranges: consecutive handles (the release orders them by stream key, then
    start) that touch on one stream are one range."""
    runs: list[list[int]] = []
    last = None
    for h in sorted(secs):
        blk, lo, hi = span_of[h]
        if runs and last == h - 1 and runs[-1][0] == blk and runs[-1][2] == lo:
            runs[-1][2] = hi
        else:
            runs.append([blk, lo, hi])
        last = h
    for r in runs:                       # a zero-length last piece still needs a range its start lies in
        if r[2] <= r[1]:
            r[2] = r[1] + 1
    return runs


# --------------------------------------------------------------------------- words


_SIGNS = re.compile(r"^(?:the )?(?:fishing )?(?:boundary )?signs? (?:at |near )?", re.I)


def place_words(label: str) -> str:
    """A place as a sentence names it: "Spuzzum Creek → Fraser River" (a confluence split) is "Spuzzum Creek
    confluence"; a sign-post's words go ("fishing boundary signs near the Mobbs Creek …" -> "the Mobbs Creek …")."""
    t = label.strip()
    m = re.fullmatch(r"(.+?)\s*→\s*.+", t)
    if m:
        t = f"{m.group(1)} confluence"
    return _SIGNS.sub("", t) or t


def ends_words(op: str, places: list[str]) -> str:
    if op == "upstream_of":
        return f"Upstream of {places[0]}"
    if op == "downstream_of":
        return f"Downstream of {places[0]}"
    if op == "between":
        return f"From {places[0]} to {places[1]}"
    return ""


# --------------------------------------------------------------------------- the atlas


@dataclass
class Atlas:
    reg: dict
    g: object
    handle: Mapping[str, int]
    ids: tuple[str, ...]
    digest: str


def load_atlas(reg=None, g=None, build=None) -> Atlas:
    """The atlas (`build`, default the current one; `reg` / `g` handed in: the review app's own, already loaded)."""
    from pipeline.atlas.reach import position
    from pipeline.atlas.registry import load_registry, regions
    from pipeline.common import section_handles as H
    from pipeline.common.curated import GENERATED
    from pipeline.common.io.serialize import read_artifact
    build = Path(build) if build else GENERATED.require_build()
    if reg is None:
        reg = load_registry(str(build / "registry.json"))
    if g is None:
        g = read_artifact(str(build / "graph.pkl"))
        regions.attach_from(build, g, reg)
        position.attach(g, build)
    ids, handle = H.read(build)
    return Atlas(reg, g, handle, ids, H.digest_for(build))


def _labels_for(atlas: Atlas, item: str, splits: dict[str, str]) -> dict[str, str]:
    from pipeline.regs.inseason import points as P
    out = dict(splits)
    for p in P.points(atlas.reg, atlas.g, item):
        out.setdefault(p.id, p.label)
    return out


def resolve_match(atlas: Atlas, rel: Release, m: M.Match, splits: dict[str, str]) -> dict[str, M.Water]:
    """One match -> its `waters` on `rel`. Raises Unresolved with the reason."""
    from pipeline.deliver.bundle.section_measure import span
    from pipeline.regs.inseason import points as P
    secs: set[int] = set()
    named: dict[str, list[str]] = {}            # item -> the ends of the extents naming it (not whole)
    walked_from: dict[str, str] = {}            # extent water -> its name, for a tributary walk
    for e in m.extents:
        try:
            got = P.resolve(atlas.reg, atlas.g, e.model_dump(exclude_none=True))
        except P.Unresolved as ex:
            raise Unresolved(f"{e.op} {e.item_id}: {ex}")
        secs |= {atlas.handle[s] for s in got["sections"] if s in atlas.handle}
        if e.op != "whole":
            labels = _labels_for(atlas, e.item_id, splits)
            named.setdefault(e.item_id, []).append(
                ends_words(e.op, [place_words(labels.get(s) or s) for s in e.splits]))
        if e.tributaries:
            walked_from[e.item_id] = atlas.reg[e.item_id].name or e.item_id
    out: dict[str, M.Water] = {}
    for item in sorted(rel.ord_of):
        members = rel.item_sections(item)
        hit = members & secs
        if not hit:
            continue
        if hit == members:
            out[item] = M.Water(all=True)
            continue
        if rel.kind_of.get(item) in WHOLE_ONLY_KINDS:
            raise Unresolved(f"{item} is a {rel.kind_of[item]}: covered in part ({len(hit)} of {len(members)} "
                             "sections), but a lake is whole or not at all")
        span_of = {}
        for h in sorted(hit & rel.starts.keys()):
            got = span(atlas.ids[h - 1], atlas.g.nodes.get(atlas.ids[h - 1]))
            if got is None:
                raise Unresolved(f"{item}: covered section {atlas.ids[h - 1]} has a line in {rel.name} but is not a "
                                 "stream piece in the atlas")
            span_of[h] = got
        runs = make_runs(span_of, span_of)
        lake_pieces = rel.lake_pieces(item)
        lakes = [i for i, h in enumerate(lake_pieces) if h in hit]
        back = covered_by_runs(runs, members, rel.starts) | {lake_pieces[i] for i in lakes}
        if back != hit:
            raise Unresolved(f"{item}: its ranges read back {len(back)} sections on {rel.name}, not the {len(hit)} "
                             "resolved")
        ends = "; ".join(sorted(set(named.get(item, ())))) or (
            "Tributaries of " + " and ".join(sorted(set(walked_from.values()))) if walked_from else "")
        if not ends:
            raise Unresolved(f"{item}: covered in part but no extent says where")
        out[item] = M.Water(runs=[tuple(r) for r in runs], lakes=lakes, ends=ends)
    if not out:
        raise Unresolved(f"covers no section of any water in {rel.name}")
    return out


def resolve_doc(doc: M.Matches, atlas: Atlas, rel: Release, only: Iterable[str] | None = None,
                log=print) -> tuple[M.Matches, list[str]]:
    """`doc` with every match (or `only` these ids) resolved on `rel`; -> (doc, [problem]). A match that does not
    resolve keeps its extents and says why (`unresolved`), with no `waters`."""
    if rel.handles != atlas.digest:
        raise SystemExit(f"inseason.resolve: release {rel.name} handles {rel.handles} != atlas {atlas.digest}: "
                         "resolve on the release built from this atlas")
    splits = M.split_labels()
    want = set(only) if only is not None else set(doc.matches)
    if only is not None and doc.release and doc.release != rel.name:
        raise SystemExit(f"inseason.resolve: the file is resolved on {doc.release}, not {rel.name}: re-resolve every "
                         "match (no --only) first")
    out, bad = {}, []
    for rid in sorted(doc.matches):
        m = doc.matches[rid]
        if rid not in want:
            out[rid] = m
            continue
        try:
            out[rid] = m.model_copy(update={"waters": resolve_match(atlas, rel, m, splits), "unresolved": None})
        except Unresolved as ex:
            bad.append(f"{rid} ({m.water}): {ex}")
            out[rid] = m.model_copy(update={"waters": {}, "unresolved": str(ex)})
    for b in bad:
        log(f"inseason.resolve: UNRESOLVED {b}")
    return doc.model_copy(update={"matches": out, "release": rel.name}), bad


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--release", type=Path, default=None, help="the release to resolve on (default LATEST)")
    ap.add_argument("--matches", type=Path, default=None, help="the curated file (default CURATED.inseason)")
    ap.add_argument("--check", action="store_true", help="resolve and compare; write nothing (exit 1 on a difference)")
    a = ap.parse_args(argv)
    src = Path(a.matches or M.default_path())
    text = src.read_text(encoding="utf-8")
    doc = M.parse(text)
    rel = read_release(release_dir(a.release))
    new, bad = resolve_doc(doc, load_atlas(), rel)
    out = M.dumps(new)
    if a.check:
        same = out == text
        print(f"in-season matches on {rel.name}: {'unchanged' if same else 'WOULD CHANGE'}"
              f"{f', {len(bad)} unresolved' if bad else ''}")
        return 0 if same and not bad else 1
    if out != text:
        tmp = src.with_suffix(".json.tmp")
        tmp.write_text(out, encoding="utf-8")
        tmp.replace(src)
        print(f"in-season matches re-resolved on {rel.name}: {src} rewritten (commit it)", file=sys.stderr)
    else:
        print(f"in-season matches on {rel.name}: unchanged", file=sys.stderr)
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
