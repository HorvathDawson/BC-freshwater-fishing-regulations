"""(T520) RESOLVE THE IN-SEASON MATCHES WHERE THE ATLAS IS — to FWA ranges on one data release.

    python -m pipeline.regs.inseason.resolve [--release DIR] [--matches FILE] [--check]

Every match in `data/curated/inseason/notice_waters.json` (`matches.Match.extents`) is resolved on the atlas
(`points.resolve`: the regs' resolver, the measure-cut pick-points, the tributary walk) and written back as `waters`,
on the release the file then names (`release`, default `data/release/LATEST`; refused unless the release's
`section_handles` digest is the atlas's):

    {item: {"all": true}}                           every section of the water (in that release's `part_section`)
    {item: {"runs": [[blk, m0, m1], …], "lakes"?: true, "ends": [[low, high], …]}}
                                                    the covered stream sections as FWA ranges on stream `blk`, metres;
                                                    `lakes` (T673): EVERY lake piece of the river is covered (a lake
                                                    piece has no line in any release, so no range can name it; a match
                                                    covering some of them only is unresolved); `ends` each covered
                                                    stretch of the water's main line as two end TOKENS, mouth upward (a
                                                    split id, a pick-point id, `mouth`, `source`) — never words: the
                                                    release's `display.ends` says them (`inSeasonWhere`)

(T673, owner 2026-11-13) THE RANGES ARE RELEASE-INDEPENDENT: a release places a range on every section of the water whose
line [from, to) it TOUCHES (`touched`, the device's rule, `@app/core` `inSeasonCover`), so the one published file is read
by every release — the one it was resolved on reads back exactly the resolved sections (proved below), an older or newer
one whose section boundaries moved shows the notice on every section the stretch touches (with its "Where" words),
never fewer.

and each match is STAMPED (`matches.stamp`: a hash of its extents, the release and its waters) so publish refuses a
hand edit that was not re-resolved.

(T623 F7, F20) A NOTICE COVERS ONLY FRESH WATER OF ITS OWN REGION (`fresh_in_region`). The extents say the place in
the notice's words ("upstream of Hells Gate", "downstream of the Spuzzum Creek confluence"), but the notice is issued
for one region, and the book bounds the same rule at that region's line (p.30: "from Hells Gate upstream to the Region 3
boundary"); owner 2026-11-11: "the banner stops at the Region 3 boundary" (F7) and "a freshwater notice starts at
Mission, not on the tidal stretch" (F20). So the covered sections are those the release places in the match's
`region` (the store's `section_place`); a section it places in no region — tidal water, outside B.C. — or keys as
tidal is never covered. A water the clip cut short says its ends where its covered main line now ends (`stem_ends`),
not by the notice's own places. A match naming no region is refused.

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
from typing import Iterable, Mapping, Optional

from pipeline.regs.inseason import matches as M

#: a lake / wetland item: whole-item only (its sections have no line)
WHOLE_ONLY_KINDS = ("lake", "wetland")


class Unresolved(ValueError):
    pass


def release_dir(arg: Path | str | None = None) -> Path:
    from pipeline.common.curated import GENERATED
    if arg:
        return Path(arg)
    from pipeline.deliver.release.manifest import live
    return live(GENERATED.release)


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
    lines: dict[int, tuple[int, int, int]]         # stream handle -> (blk, from_m, to_m), the release's `lines`
                                                   # (to_m `lines.OPEN`: the stream's one section, to its end)
    #: (T623) handle -> the regions the release places it in (lower case, the store's `section_place`); a handle
    #: absent is in NO region: tidal water, outside B.C., or a key the book calls tidal (`read_release`)
    regions: dict[int, frozenset[str]]

    def item_sections(self, item: str) -> frozenset[int]:
        o = self.ord_of.get(item)
        return self.members.get(o, frozenset()) if o is not None else frozenset()

    def lake_pieces(self, item: str) -> list[int]:
        """The water's sections with no line (its lake pieces), in handle order — `lakes` indexes these, as the
        device does (`@app/core` `inSeasonCover`: a page section with no line)."""
        return sorted(h for h in self.item_sections(item) if h not in self.lines)


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
        lines: dict[int, tuple[int, int, int]] = {}
        for block, n, b in ln.execute("SELECT block, n, b FROM section_line ORDER BY block"):
            for i, (blk, lo, hi) in enumerate(L.decode_block(bytes(b), int(n))):
                h = (int(block) << L.BLOCK_BITS) | i
                if h:                                  # handle 0: the filler
                    lines[h] = (blk, lo, hi)
        for c in (core, water, ln):
            c.close()
    handles = str(meta.get("section_handles") or "")
    return Release(release_name(rel), handles, ord_of, kind_of, {o: frozenset(s) for o, s in mem.items()}, lines,
                   fresh_regions(rel, handles))


def fresh_regions(rel: Path, handles: str) -> dict[int, frozenset[str]]:
    """(T623) handle -> the regions the release places that section in, for FRESH WATER only: the store's
    `section_place` (the region set a card says "In Region 3" by). A section with no place (tidal water, outside
    B.C.: the store keys neither) or whose key the book calls tidal (`akey.tidal`) is left out — in no region."""
    from pipeline.deliver.store.release import province_store
    st = province_store(rel)
    try:
        if str(st.meta.get("section_handles") or "") != handles:
            raise SystemExit(f"inseason.resolve: release {rel.name}'s store names section handles "
                             f"{st.meta.get('section_handles')}, its core {handles}")
        place = {int(p): frozenset(str(r).lower() for r in json.loads(rs))
                 for p, rs in st.con.execute("SELECT place, regions FROM place")}
        tidal = {int(a) for (a,) in st.con.execute("SELECT akey FROM akey WHERE tidal = 1")}
        akey = st.section_akeys()
        return {h: place[p] for h, p in sorted(st.section_places().items()) if akey.get(h) not in tidal}
    finally:
        st.close()


#: the feed's region of a province-wide notice (`app/workers/feeds/src/inseason.ts`: "1" … "8", "7A", "7B", "provincial")
PROVINCIAL = "provincial"


def region_code(region: str) -> str:
    """A notice's region as the release writes it ("7B" -> "7b", "Region 3" -> "3")."""
    return re.sub(r"^region\s*", "", str(region or "").strip().lower())


def fresh_in_region(secs: Iterable[int], region: str, regions: Mapping[int, frozenset[str]]) -> set[int]:
    """(T623 F7, F20) THE SECTIONS A NOTICE OF `region` MAY COVER: those the release places in that region. A section
    on the line (in two regions) is in both; tidal water and water outside B.C. are in none (module doc). A
    `provincial` notice (the feed's province-wide heading) covers fresh water of every region."""
    r = region_code(region)
    if r == PROVINCIAL:
        return {h for h in secs if regions.get(h)}
    return {h for h in secs if r in regions.get(h, ())}


def section_touched(line: tuple[int, int, int], m0: int, m1: int) -> bool:
    """(T673) THE DEVICE'S RULE for one section (`@app/core` `inSeasonCover`, one fixture pins both): a section whose
    line [from, to) on the run's stream OVERLAPS the run [m0, m1) is touched — wholly or partly inside. A section of no
    length is touched when its start lies in the run; an open end (`lines.OPEN`) runs to the stream's end."""
    _blk, lo, hi = line
    return lo < m1 and m0 < max(hi, lo + 1)


def touched(runs: Iterable[Iterable[int]], sections: Iterable[int], lines: Mapping[int, tuple[int, int, int]]
            ) -> set[int]:
    """(T673) THE DEVICE'S RULE: the sections of `sections` a run touches on its stream (`section_touched`). On the
    release a range set was resolved on it reads back exactly the resolved sections (the runs are their own spans);
    on another release a section only partly inside is touched too — the notice is shown on it, never dropped."""
    by_blk: dict[int, list[tuple[int, int]]] = {}
    for blk, m0, m1 in runs:
        by_blk.setdefault(int(blk), []).append((int(m0), int(m1)))
    out = set()
    for h in sections:
        at = lines.get(h)
        if at and any(section_touched(at, m0, m1) for m0, m1 in by_blk.get(at[0], ())):
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


# --------------------------------------------------------------------------- ends (TOKENS, never words)

#: the natural ends of a water's main line (no named place)
MOUTH, SOURCE = "mouth", "source"
#: which named place stands for a run end when several stand at one measure
_KIND_ORDER = {"split": 0, "lake": 1, "region": 2, "mu": 3, "trib": 4, "mouth": 5}
_SIGNS = re.compile(r"^(?:the )?(?:fishing )?(?:boundary )?signs? (?:at |near )?", re.I)


def region_line(token: str) -> str:
    """(T623 F7) a REGION's own boundary (the atlas's area cut `area:3`) as the release names it: `region_line:3` —
    one token for every water it crosses (the release's parts end at it, `bundle.spans.REGION_AREA`); an `area:3`
    end would be said "3", and its atlas label names a neighbour ("Region 3 – Region 5 boundary") that differs per
    water while the file's words are one per token."""
    from pipeline.deliver.bundle.spans import REGION_AREA
    m = REGION_AREA.match(token)
    return f"region_line:{m.group(1)}" if m else token


def interim_words(token: str, label: str) -> str:
    """(INTERIM, until T517 ships a token -> words table in the release) the words a run-end TOKEN is said by: the
    place's own name (a split's curated label, an atlas point's label), cleaned of the confluence arrow and sign-post
    words. A NAME, never a sentence: the sentence ("From A to B") is core's (`inSeasonWhere`, AGENTS 33)."""
    m = re.fullmatch(r"region_line:(\w+)", token)
    if m:
        return f"Region {m.group(1)} boundary"         # the release's words (`display.ends`), one per region
    t = label.strip()
    m = re.fullmatch(r"(.+?)\s*→\s*.+", t)
    if m:
        t = f"{m.group(1)} confluence"
    return _SIGNS.sub("", t) or t


def stem_ends(atlas: "Atlas", item: str, hit: set[int], cut_at=None
              ) -> tuple[list[tuple[str, str]], dict[str, str], list]:
    """GEOMETRY: the covered stretches of `item`'s MAIN LINE as end-token pairs, mouth upward — each the place standing
    at that end (a split, a lake inlet / outlet, a region / MU crossing, a named confluence) or `mouth` / `source` at
    the line's own ends — each token's label, and the stretches' measures [(lo_m, hi_m)]. For a water the notice reaches
    only by its tributary walk (it names no place on it), or one its region cut short. `cut_at(handle)` (T623): the
    token an end takes because of the stem piece just past it (the region's own line when that piece lies in another
    region), or None to say the place standing there. Raises Unresolved when a stretch ends where nothing stands."""
    from pipeline.regs.inseason import points as P
    stem, nodes = P._stem_nodes(atlas.reg, atlas.g, item)
    if not nodes:
        return [], {}, []
    cov = [atlas.handle.get(n.node_id) in hit for n in nodes]
    pts = [p for p in P.points(atlas.reg, atlas.g, item) if p.m is not None]
    label = {p.id: p.label for p in P.points(atlas.reg, atlas.g, item)}

    def at(m: float, natural: str, edge: float) -> str:
        if abs(m - edge) <= P.EPS_M:
            return natural
        near = sorted((_KIND_ORDER.get(p.kind, 9), p.id) for p in pts if abs(p.m - m) <= P.EPS_M and p.id != MOUTH)
        if not near:
            raise Unresolved(f"{atlas.reg[item].name or item}: a covered stretch ends at {m:.0f} m on its main line, "
                             "where no named place stands")
        return region_line(near[0][1])

    def past(k: int) -> Optional[str]:
        """the token the stem piece just past a covered end gives that end (`cut_at`), if any"""
        return cut_at(atlas.handle.get(nodes[k].node_id)) if cut_at and 0 <= k < len(nodes) else None

    out: list[tuple[str, str]] = []
    spans: list = []
    i = 0
    while i < len(nodes):
        if not cov[i]:
            i += 1
            continue
        j = i
        while j + 1 < len(nodes) and cov[j + 1]:
            j += 1
        spans.append((nodes[i].down_m, nodes[j].up_m))
        out.append((past(i - 1) or at(nodes[i].down_m, MOUTH, nodes[0].down_m),
                    past(j + 1) or at(nodes[j].up_m, SOURCE, nodes[-1].up_m)))
        i = j + 1
    return out, label, spans


def _spans(atlas: "Atlas", item: str, hit: set[int]) -> list[tuple[float, float]]:
    """The covered stretches of the main line as measures [(lo, hi)], mouth upward (no names)."""
    from pipeline.regs.inseason import points as P
    _stem, nodes = P._stem_nodes(atlas.reg, atlas.g, item)
    out, i = [], 0
    while i < len(nodes):
        if atlas.handle.get(nodes[i].node_id) not in hit:
            i += 1
            continue
        j = i
        while j + 1 < len(nodes) and atlas.handle.get(nodes[j + 1].node_id) in hit:
            j += 1
        out.append((nodes[i].down_m, nodes[j].up_m))
        i = j + 1
    return out


def extent_ends(atlas: "Atlas", item: str, extents: list, hit: set[int]
                ) -> tuple[list[tuple[str, str]], dict[str, str]]:
    """THE NOTICE'S OWN PLACES on `item` (the extents naming it) as end-token pairs, mouth upward: `between` a, b ->
    (a, b) ordered by their measures on the main line; `upstream_of` a -> (a, source); `downstream_of` a -> (mouth, a);
    `whole` -> (mouth, source). Pairs that meet are one stretch (Hell's Gate: "between Spuzzum and Hell's Gate" +
    "upstream of Hell's Gate" is upstream of Spuzzum)."""
    from pipeline.regs.inseason import points as P
    pts = {p.id: p for p in P.points(atlas.reg, atlas.g, item)}
    m = lambda t: (-1e18 if t == MOUTH else 1e18 if t == SOURCE else (pts[t].m if t in pts else None))  # noqa: E731
    pairs: list[tuple[str, str]] = []
    for e in extents:
        if e.op == "whole":
            pairs.append((MOUTH, SOURCE))
        elif e.op == "upstream_of":
            pairs.append((e.splits[0], SOURCE))
        elif e.op == "downstream_of":
            pairs.append((MOUTH, e.splits[0]))
        else:
            a, b = e.splits
            ma, mb = m(a), m(b)
            if ma is None and mb is None:
                raise Unresolved(f"{atlas.reg[item].name or item}: between {a} and {b}: neither is a place on its "
                                 "main line, so its ends cannot be ordered")
            if ma is None or mb is None:
                # one end stands off the main line's pick-points (a dam re-anchored on a reservoir's outlet): the
                # covered stretch says which end the other one is
                known, km = (b, mb) if ma is None else (a, ma)
                other = a if ma is None else b
                lo, hi = min(_spans(atlas, item, hit), key=lambda sp: min(abs(sp[0] - km), abs(sp[1] - km)))
                pairs.append((known, other) if abs(lo - km) <= abs(hi - km) else (other, known))
                continue
            pairs.append((a, b) if ma <= mb else (b, a))
    def key(p):
        lo = m(p[0]) if m(p[0]) is not None else m(p[1])
        return (lo if lo is not None else 0, m(p[1]) if m(p[1]) is not None else 0)
    pairs = sorted(set(pairs), key=key)
    merged: list[tuple[str, str]] = []
    for lo, hi in pairs:
        if merged and (merged[-1][1] == lo or (m(lo) is not None and m(merged[-1][1]) is not None
                                               and m(lo) <= m(merged[-1][1]))):
            top = merged[-1][1] if (m(merged[-1][1]) or 0) >= (m(hi) or 0) else hi
            merged[-1] = (merged[-1][0], top)
        else:
            merged.append((lo, hi))
    return merged, {k: p.label for k, p in pts.items()}


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


def resolve_match(atlas: Atlas, rel: Release, m: M.Match, splits: dict[str, str]
                  ) -> tuple[dict[str, M.Water], dict[str, str]]:
    """One match -> (its `waters` on `rel`, the words of the end tokens it uses). Raises Unresolved with the reason —
    first when `rel` was not built from this atlas (its handles would mean other sections)."""
    from pipeline.deliver.bundle.section_measure import span
    from pipeline.regs.inseason import points as P
    if rel.handles != atlas.digest:
        raise Unresolved(f"release {rel.name} was not built from this atlas (section handles {rel.handles} != "
                         f"{atlas.digest}): resolve on the release built from it")
    if not region_code(m.region):
        raise Unresolved("the match names no region: a notice covers only its own region's water")
    named_secs: set[int] = set()
    for e in m.extents:
        try:
            got = P.resolve(atlas.reg, atlas.g, e.model_dump(exclude_none=True))
        except P.Unresolved as ex:
            raise Unresolved(f"{e.op} {e.item_id}: {ex}")
        named_secs |= {atlas.handle[s] for s in got["sections"] if s in atlas.handle}
    secs = fresh_in_region(named_secs, m.region, rel.regions)
    if named_secs and not secs:
        raise Unresolved(f"covers no fresh water of Region {m.region} in {rel.name} (its {len(named_secs)} "
                         "section(s) are all in another region, tidal, or outside B.C.)")
    line = f"region_line:{region_code(m.region).upper()}"

    def cut_at(h) -> Optional[str]:
        # a stretch the clip stopped where the notice still named the water, in fresh water of another region: it
        # ends at its region's own line (F7: "the banner stops at the Region 3 boundary"). Past it tidal water or
        # B.C.'s border (no region): the place standing there says it (F20: the CPR Bridge at Mission)
        return line if h in named_secs and h not in secs and rel.regions.get(h) else None

    out: dict[str, M.Water] = {}
    words: dict[str, str] = {}
    for item in sorted(rel.ord_of):
        members = rel.item_sections(item)
        hit = members & secs
        if not hit:
            continue
        clipped = hit != members & named_secs
        if hit == members:
            out[item] = M.Water(all=True)
            continue
        if rel.kind_of.get(item) in WHOLE_ONLY_KINDS:
            raise Unresolved(f"{item} is a {rel.kind_of[item]}: covered in part ({len(hit)} of {len(members)} "
                             "sections), but a lake is whole or not at all")
        span_of = {}
        for h in sorted(hit & rel.lines.keys()):
            got = span(atlas.ids[h - 1], atlas.g.nodes.get(atlas.ids[h - 1]))
            if got is None:
                raise Unresolved(f"{item}: covered section {atlas.ids[h - 1]} has a line in {rel.name} but is not a "
                                 "stream piece in the atlas")
            span_of[h] = got
        runs = make_runs(span_of, span_of)
        lake_pieces = rel.lake_pieces(item)
        lakes_hit = [h for h in lake_pieces if h in hit]
        if lakes_hit and len(lakes_hit) != len(lake_pieces):
            # (T673) a lake piece has no line in any release, so the file names a river's lake pieces all or none
            raise Unresolved(f"{item}: covers {len(lakes_hit)} of its {len(lake_pieces)} lake pieces in {rel.name}; "
                             "a lake piece has no line a release-independent range can name (all or none)")
        lakes = bool(lakes_hit)
        back = touched(runs, members, rel.lines) | (set(lake_pieces) if lakes else set())
        if back != hit:
            raise Unresolved(f"{item}: its ranges read back {len(back)} sections on {rel.name}, not the {len(hit)} "
                             "resolved")
        named = [e for e in m.extents if e.item_id == item]
        # cut short by its region / the tidal limit: the notice's own places no longer bound it, its covered main
        # line does (Hells Gate "upstream" now ends at the Region 3 line)
        ends, label = (extent_ends(atlas, item, named, hit) if named and not clipped
                       else stem_ends(atlas, item, hit, cut_at)[:2])
        for pair in ends:
            for t in pair:
                if t not in (MOUTH, SOURCE):
                    words[t] = interim_words(t, splits.get(t) or label.get(t) or t)
        out[item] = M.Water(runs=[tuple(r) for r in runs], lakes=lakes, ends=ends)
    if not out:
        raise Unresolved(f"covers no section of any water in {rel.name}")
    return out, words


def resolve_doc(doc: M.Matches, atlas: Atlas, rel: Release, only: Iterable[str] | None = None,
                log=print) -> tuple[M.Matches, list[str]]:
    """`doc` with every match (or `only` these ids) resolved on `rel`; -> (doc, [problem]). A match that does not
    resolve keeps its extents and says why (`unresolved`), with no `waters`."""
    splits = M.split_labels()
    want = set(only) if only is not None else set(doc.matches)
    if only is not None and doc.release and doc.release != rel.name:
        raise SystemExit(f"inseason.resolve: the file is resolved on {doc.release}, not {rel.name}: re-resolve every "
                         "match (no --only) first")
    out, bad = {}, []
    words = dict(doc.words) if only is not None else {}
    for rid in sorted(doc.matches):
        m = doc.matches[rid]
        if rid not in want:
            out[rid] = m
            continue
        try:
            waters, w = resolve_match(atlas, rel, m, splits)
            words.update(w)
            out[rid] = M.stamped(m.model_copy(update={"waters": waters, "unresolved": None}), rel.name)
        except Unresolved as ex:
            bad.append(f"{rid} ({m.water}): {ex}")
            out[rid] = m.model_copy(update={"waters": {}, "unresolved": str(ex), "stamp": None})
    for b in bad:
        log(f"inseason.resolve: UNRESOLVED {b}")
    used = {t for m in out.values() for w in m.waters.values() for pair in w.ends for t in pair} - {MOUTH, SOURCE}
    return doc.model_copy(update={"matches": out, "release": rel.name,
                                  "words": {t: words[t] for t in sorted(used)}}), bad


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
        from pipeline.common.curated_write import write_any   # T600: atomic, backed up and logged
        write_any(src, out, tool="pipeline.regs.inseason.resolve", source=f"re-resolved on {rel.name}")
        print(f"in-season matches re-resolved on {rel.name}: {src} rewritten (commit it)", file=sys.stderr)
    else:
        print(f"in-season matches on {rel.name}: unchanged", file=sys.stderr)
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
