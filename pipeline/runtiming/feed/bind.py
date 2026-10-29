"""THE THREE RELATIONS, against a binding base (`base.py`): which runs are on which section, never merged.

Ported from the T162 prototype (handoff/PLAN/runtiming/integration-prototype.patch `pipeline/deliver/runs.py`, every
rule measured there on the live atlas, 2026-10-24) onto the base's columns, so it runs without the atlas:

  spawning   PSF: the section's point lies in the run's Conservation Unit polygon, and the section is a stream of
             Strahler order `MIN_ORDER` or more, or a lake with an outflow. A CU is a whole watershed and the curve is
             the CU's, not each creek's: painting every first-order trickle and pond in it claimed a run in each.
  passage    PSF: the section is on the way from the CU to the sea — the flow walked DOWN from the CU's spawning
             sections, never into another drainage (every step's FWA code an ancestor of a spawning section's: the
             tidal reaches below one river join the next river's); a CU's sections in a drainage it barely touches
             (a polygon simplified across a divide) are dropped first as spill (`SPILL_*`).
  observed   DFO: the stream a NuSEDS survey site is on (its FWA watershed code), the sections of that code that carry
             the site's name — on a long water (over `LONG_WATER_KM`) only those within `NEAR_SITE_KM` of the site.
             Where a section is observed for a slot, that slot's PSF spawning runs are dropped there (owner 2026-10-24:
             DFO where it is better); its passing runs stay. Of several sites on one section, per slot and run label,
             the nearest.
"""
from __future__ import annotations

import math
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

from pipeline.runtiming.feed.base import name_key
from pipeline.runtiming.timing import TimedRun

SPAWNING, PASSAGE, OBSERVED = 0, 1, 2
RELATIONS = ("spawning", "passage", "observed")
SIMPLIFY = 0.001            # ~70 m, on top of the ~200 m the Explorer's z8 tiles already carry
MIN_ORDER = 3
SPILL_SHARE, SPILL_MIN, SPILL_KEEP = 0.01, 5, 100
LONG_WATER_KM = 150.0
NEAR_SITE_KM = 50.0


@dataclass
class Atlas:
    """The base's columns as lookups (sid -> value)."""
    sids: List[int]
    lake: Set[int]
    order: Dict[int, int]
    length_m: Dict[int, float]
    wsc: Dict[int, str]
    blk: Dict[int, str]
    m0: Dict[int, int]
    m1: Dict[int, int]
    wbk: Dict[int, str]
    names: Dict[int, frozenset]
    down: Dict[int, List[int]]
    item: Dict[int, str]
    point: Dict[int, Tuple[float, float]]
    handles: str
    tile_version: Optional[str]


def atlas_of(base: dict) -> Atlas:
    c, st = base["cols"], base["strings"]
    sids, s = [], 0
    for d in c["sid"]:
        s += d
        sids.append(s)
    nm = st["nm"]
    return Atlas(
        sids=sids, lake={x for x, k in zip(sids, c["k"]) if k == 1},
        order=dict(zip(sids, c["o"])), length_m={x: float(v) for x, v in zip(sids, c["len"])},
        wsc={x: st["wsc"][i] for x, i in zip(sids, c["wsc"])}, blk=dict(zip(sids, c["blk"])),
        m0=dict(zip(sids, c["m0"])), m1=dict(zip(sids, c["m1"])),
        wbk={x: st["wbk"][i] for x, i in zip(sids, c["wbk"])},
        names={x: frozenset(nm[i] for i in ns) for x, ns in zip(sids, c["nm"])},
        down={x: list(d) for x, d in zip(sids, c["dn"]) if d},
        item={x: st["it"][i] for x, i in zip(sids, c["it"]) if i >= 0},
        point={x: (a / 1e5, b / 1e5) for x, a, b in zip(sids, c["x"], c["y"])},
        handles=base["section_handles"], tile_version=base.get("tile_version"))


def km_between(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    lat = math.radians((a[1] + b[1]) / 2)
    return 111.32 * math.hypot((a[0] - b[0]) * math.cos(lat), a[1] - b[1])


def in_polygons(points: Iterable[Tuple[int, float, float]], polygons: Mapping[int, object]) -> Dict[int, Set[int]]:
    """{cuid: the sections whose point lies in its polygon}."""
    from shapely.geometry import Point
    from shapely.prepared import prep
    from shapely.strtree import STRtree
    ids = sorted(polygons)
    geoms = [polygons[c] for c in ids]
    pre = [prep(g) for g in geoms]
    tree = STRtree(geoms)
    out: Dict[int, Set[int]] = defaultdict(set)
    for sid, lon, lat in points:
        pt = Point(lon, lat)
        for i in tree.query(pt):
            if pre[i].contains(pt):
                out[ids[i]].add(sid)
    return dict(out)


def spawns(s: int, a: Atlas) -> bool:
    """Whether a section inside a CU takes its runs as spawning: a stream of order `MIN_ORDER`+, or a lake that drains."""
    return bool(a.down.get(s)) if s in a.lake else a.order.get(s, 0) >= MIN_ORDER


def drop_spill(secs: Set[int], wsc_of: Mapping[int, str]) -> Set[int]:
    """A CU's sections without the ones in a drainage it barely touches (its polygon crossing a divide)."""
    from pipeline.common.utils import wsc as W
    roots = Counter(W.root(wsc_of[s]) for s in secs if wsc_of.get(s))
    tot = sum(roots.values()) or 1
    keep = {r for r, n in roots.items() if (n / tot >= SPILL_SHARE and n >= SPILL_MIN) or n >= SPILL_KEEP}
    if roots:                       # its own drainage is never spill, however few sections it has
        keep.add(max(roots.items(), key=lambda kv: (kv[1], kv[0]))[0])
    return {s for s in secs if wsc_of.get(s) and W.root(wsc_of[s]) in keep}


def passage(seeds: Set[int], down: Mapping[int, Sequence[int]], wsc_of: Mapping[int, str]) -> Set[int]:
    """The sections a run crosses from `seeds` (its spawning sections) to the sea: the flow walked DOWN, every step's
    FWA code an ancestor (or the code) of a seed's, the seeds themselves excluded."""
    from pipeline.common.utils import wsc as W
    allowed: Set[str] = set()
    for s in seeds:
        c = wsc_of.get(s)
        if c:
            allowed.add(c)
            allowed.update(W.ancestors(c))
    seen: Set[int] = set()
    q = deque(sorted(seeds))
    while q:
        n = q.popleft()
        for d in down.get(n, ()):
            if d in seen or d in seeds or wsc_of.get(d, "") not in allowed:
                continue
            seen.add(d)
            q.append(d)
    return seen


def km_to_sea(a: Atlas) -> Dict[int, float]:
    """River km from each section to its outlet: lengths summed down its OWN blue line where it goes on, else down the
    shortest way out (the shortest way alone took a braid's dead-end side channel for the sea: the lower Skeena read
    0.8 km from it 48 km up). Iterative: a coastal mainstem is thousands of sections deep."""
    down, length_m, line = a.down, a.length_m, a.blk
    out: Dict[int, float] = {}
    for start in length_m:
        if start in out:
            continue
        stack, onpath = [start], set()
        while stack:
            n = stack[-1]
            if n in out:
                stack.pop()
                continue
            kids = [k for k in down.get(n, ()) if k in length_m]
            pend = [k for k in kids if k not in out and k not in onpath]
            if pend:
                onpath.add(n)
                stack.append(pend[0])
                continue
            same = [out[k] for k in kids if k in out and line.get(k) and line.get(k) == line.get(n)]
            done = same or [out[k] for k in kids if k in out]
            out[n] = (length_m.get(n) or 0.0) + (min(done) if done else 0.0)
            onpath.discard(n)
            stack.pop()
    return {k: v / 1000.0 for k, v in out.items()}


def observed(dfo: Sequence[Tuple[int, TimedRun]], by_wsc: Mapping[str, Sequence[int]], a: Atlas) -> Dict[int, Dict[tuple, int]]:
    """{sid: {(slot, label): run index}} — each DFO record on its stream's sections, the nearest site winning.

    A SITE BINDS THE SECTIONS OF ITS CODE THAT CARRY ITS NAME (every name the atlas gives the section — the Vedder's
    pieces carry "Chilliwack River" too). NuSEDS gives a slough or a side creek the FWA has no line for the code of the
    river it joins: Esker Slough's coho landed on 71 sections of the Skeena. So where the code's sections are named and
    none is the site's name, the site binds nothing; only an unnamed stream's sections take a site by its code alone.

    ONE LEVEL DOWN, BY NAME: where no piece of the site's own code carries its name, the pieces one code level below
    that do are its stream (NuSEDS files the Chilliwack under the code the FWA gives the Vedder)."""
    children: Dict[str, List[str]] = defaultdict(list)
    for c in by_wsc:
        if "-" in c:
            children[c.rsplit("-", 1)[0]].append(c)
    best: Dict[int, Dict[tuple, Tuple[tuple, int]]] = defaultdict(dict)
    for ix, r in dfo:
        secs = list(by_wsc.get(r.wsc or "", ()))
        if any(a.names.get(s) for s in secs):
            want = name_key(r.name)
            secs = [s for s in secs if want in a.names.get(s, ())] or \
                [s for c in sorted(children.get(r.wsc or "", ())) for s in by_wsc[c] if want in a.names.get(s, ())]
        if not secs:
            continue
        site = (r.lon, r.lat) if r.lon is not None and r.lat is not None else None
        long_water = sum(a.length_m.get(s, 0.0) for s in secs) / 1000.0 > LONG_WATER_KM
        for s in secs:
            d = km_between(a.point[s], site) if site and s in a.point else math.inf
            if long_water and site and d > NEAR_SITE_KM:
                continue
            rank = (d, -(r.last_year or 0), -(r.years or 0), r.key)
            k = (r.slot, r.label)
            if k not in best[s] or rank < best[s][k][0]:
                best[s][k] = (rank, ix)
    return {s: {k: v[1] for k, v in m.items()} for s, m in best.items()}


def bind(a: Atlas, psf: Sequence[TimedRun], dfo: Sequence[TimedRun], polygons: Mapping[int, object]):
    """(per section {(run index, relation)}, a report). Run indexes: psf first, then dfo, as given."""
    cu = in_polygons(((s, *a.point[s]) for s in a.sids), polygons)
    spawn: Dict[int, Set[int]] = {}
    passing: Dict[int, Set[int]] = {}
    for ix, r in enumerate(psf):
        secs = {s for s in drop_spill(cu.get(r.cuid, set()), a.wsc) if spawns(s, a)}
        if secs:
            spawn[ix] = secs
            passing[ix] = passage(secs, a.down, a.wsc)
    by_wsc_stream: Dict[str, List[int]] = defaultdict(list)
    by_wsc_lake: Dict[str, List[int]] = defaultdict(list)
    for s in a.sids:
        c = a.wsc.get(s)
        if c:
            (by_wsc_lake if s in a.lake else by_wsc_stream)[c].append(s)
    seen: Dict[int, Dict[tuple, int]] = {}
    indexed = [(len(psf) + j, r) for j, r in enumerate(dfo)]
    # a lake site binds the lake; a stream site the stream's own pieces (never a lake the stream's code also names)
    for kind, idx in (("lake", by_wsc_lake), ("stream", by_wsc_stream)):
        part = [(ix, r) for ix, r in indexed if ((r.site_kind or "").startswith("Lake")) == (kind == "lake")]
        for s, m in observed(part, idx, a).items():
            seen.setdefault(s, {}).update(m)
    runs = list(psf) + list(dfo)
    per: Dict[int, Set[Tuple[int, int]]] = defaultdict(set)
    for ix, secs in spawn.items():
        for s in secs:
            per[s].add((ix, SPAWNING))
    for ix, secs in passing.items():
        for s in secs:
            per[s].add((ix, PASSAGE))
    for s, m in seen.items():
        slots = {k[0] for k in m}
        per[s] = {(i, rel) for i, rel in per[s] if not (rel == SPAWNING and runs[i].slot in slots)}
        per[s].update((i, OBSERVED) for i in m.values())
    per = {s: v for s, v in per.items() if v}
    report = {"psf_runs_bound": len(spawn), "dfo_records_bound": len({i for m in seen.values() for i in m.values()}),
              "spawning_sections": len({s for v in spawn.values() for s in v}),
              "passage_sections": len({s for v in passing.values() for s in v}), "observed_sections": len(seen),
              "sections": len(per)}
    return per, report
