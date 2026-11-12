"""(T668 fix 4, owner 2026-11-11) WHICH DFO PLACE ANSWERED: the card names, per row, the region and the level of DFO's
cascade it comes from ("Region 6 · Skeena River · B(ii)"). Read off the COMPOSED answer,
never re-decided — so whatever rule picks the winner (`salmon.strictest`), the place named is the one whose answer shows.

The release carries (`deliver.salmon.build`):
  * per chain `a` — per species (SPECIES order), runs [start, who, …] of the answering place: an index into the chain's
    `l`; -1 where no place answered (not listed); -2 on a two-region chain where the answer is neither region's own
    (both regions' words together, `salmon.stricter_opening`);
  * payload `p` — per place of any chain: `r` its region slug, `water` its water's name (absent: a region / section /
    area default), `sec` its DFO section exactly as the scraped page prints it ("B(ii)"; absent where the page has no
    lettered sections) — `places_named`. The card reads "Region 6 · Skeena River · B(ii)" (owner 2026-11-11).
The live feed's chains the same (the golden `salmonStraddle.golden.ts`, the app's `composeChain`).
"""
from __future__ import annotations

from typing import Callable, Dict, Iterable, List, Mapping, Optional, Sequence

#: `a` where no place answered / where the answer is both regions' together
NONE, BOTH = -1, -2


def _run_at(runs: Sequence[int], day: int) -> int:
    v = runs[1] if len(runs) > 1 else -1
    for k in range(0, len(runs), 2):
        if runs[k] > day:
            break
        v = runs[k + 1]
    return v


def _runs_of(values: Sequence[int]) -> List[int]:
    out: List[int] = []
    prev = object()
    for i, v in enumerate(values):
        if v != prev:
            out += [i, v]
            prev = v
    return out


def answering_places(sp_runs: Sequence[Sequence[int]], place_sp: Sequence[Optional[Sequence[Sequence[int]]]],
                     groups: Optional[Sequence[Sequence[int]]], same: Callable[[int, int], bool],
                     days: int = 366) -> List[List[int]]:
    """`a` for one chain. `sp_runs`: the chain's composed per-species runs; `place_sp`: each place's own per-species runs
    (-1: says nothing that day; None: a place the feed does not carry); `groups`: the chain's `s` (None: one cascade);
    `same(composed, own)`: whether the composed verdict index is that place's own (own -1: the not-listed verdict).

    Per day, the first group (the chain's `s`, else the whole chain) whose own answer is the composed one names its
    first place with an answer — or, for a silent group of a two-region chain (closed to it), its first place."""
    gs = [list(g) for g in groups] if groups else [list(range(len(place_sp)))]
    out: List[List[int]] = []
    for k in range(len(sp_runs)):
        vals: List[int] = []
        for day in range(days):
            want, who = _run_at(sp_runs[k], day), NONE
            for g in gs:
                own = next(((i, v) for i in g if place_sp[i] is not None
                            for v in (_run_at(place_sp[i][k], day),) if v >= 0), None)
                if own is None:
                    if groups and g and same(want, -1):
                        who = g[0]
                        break
                elif same(want, own[1]):
                    who = own[0]
                    break
            else:
                if groups:
                    who = BOTH
            vals.append(who)
        out.append(_runs_of(vals))
    return out


def places_named(locs: Mapping[str, tuple], lids: Iterable[str]) -> Dict[str, dict]:
    """Payload `p`: each place's region slug, water name and DFO section, as sourced (the scrape's `water` and
    `section`; nothing derived) — what the card names a row by."""
    out: Dict[str, dict] = {}
    for lid in sorted(set(lids)):
        slug, loc, _ef = locs[lid]
        water, sec = getattr(loc, "water", None), getattr(loc, "section", None)
        out[lid] = {"r": slug, **({"water": water} if water else {}), **({"sec": sec} if sec else {})}
    return out
