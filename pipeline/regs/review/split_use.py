"""WHICH CURATED CUTS ARE USED (T207 item 2, owner 2026-10-26) — the one answer the review app's
"curated cut(s) no rule uses" and the unused-splits audit both read.

The review app said "curated cut(s) no rule uses: within Garibaldi Park" on the Pitt River while
`pitt_river.r1` ("No Fishing within Garibaldi Park") binds 411 sections through that very boundary:
the rule names the PARK (`within_area: area:park:garibaldi_park`), and the detector only counted a
split id named under an extent's `splits`. A cut is USED when any of these names it:

  * an extent's `splits` — any extent of any record (`walk.iter_extents`: rules, carve-outs,
    licensing, the entry's own scope), by the split's own id;
  * an AREA an extent names (`area_id`, `within_area`, `outside_area`, an `area:` `item_id` /
    `item_ids` / `outside_items`) whose name is the area-boundary split's `area_name` — the cut IS
    that area's edge on the water;
  * a MERGED PLACE (`RegistryBoundary.answers_to`, the aliases): a rule naming any id a cut-point
    answers to uses every id at that place, and a curated split whose label is one of that place's
    other `names` too;
  * a DFO salmon entry naming the id anywhere (a DFO-only cut: the DFO overrides bind by it).

Usage is CORPUS-WIDE: a cut another entry binds is used, even when the entry on screen does not
(the review app lists, per entry, the cuts on its water that nothing anywhere binds).
"""
from __future__ import annotations

import re
from typing import Iterable, Mapping

#: extent keys that name an AREA (an `area:` id), as the corpus writes them
AREA_KEYS = ("area_id", "within_area", "outside_area", "item_id", "item_ids", "outside_items")


def area_slug(name: str) -> str:
    """An area's name as its registry id ends (`area_catalog._slug`): "GARIBALDI PARK" ->
    "garibaldi_park", the tail of `area:park:garibaldi_park`."""
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", (name or "").lower())).strip("_")


def _as_list(v) -> list:
    return v if isinstance(v, list) else [v] if v is not None else []


def extent_area_slugs(ex: Mapping) -> set[str]:
    """The areas an extent names, each as its id's last segment (`area:park:hamber_park` ->
    `hamber_park`)."""
    out = set()
    for k in AREA_KEYS:
        for v in _as_list(ex.get(k)):
            if isinstance(v, str) and v.startswith("area:"):
                out.add(v.rsplit(":", 1)[-1])
    return out


def split_area_slug(split: Mapping) -> str:
    """The area an `area_boundary` split is the edge of, as an area id ends; "" for any other."""
    a = split.get("anchor") or {}
    return area_slug(a.get("area_name", "")) if a.get("type") == "area_boundary" else ""


def iter_splits(splits_json: Mapping) -> Iterable[dict]:
    for wb in splits_json.get("waterbodies", []) or []:
        for sp in wb.get("splits", []) or []:
            if isinstance(sp, dict) and sp.get("id"):
                yield sp


def _strings(o) -> Iterable[str]:
    if isinstance(o, str):
        yield o
    elif isinstance(o, Mapping):
        for v in o.values():
            yield from _strings(v)
    elif isinstance(o, list):
        for v in o:
            yield from _strings(v)


def named_by_entries(entries: Iterable[Mapping]) -> tuple[set[str], set[str]]:
    """(split ids, area slugs) every extent of `entries` names."""
    from pipeline.regs.parsing.catalogue_parts.walk import iter_extents
    ids: set[str] = set()
    areas: set[str] = set()
    for e in entries:
        for _path, ex, _role in iter_extents(dict(e)):
            ids.update(s for s in ex.get("splits") or () if isinstance(s, str))
            areas |= extent_area_slugs(ex)
    return ids, areas


def used_split_ids(entries: Iterable[Mapping], splits_json: Mapping, *, dfo: Iterable = (),
                   registry: Mapping | None = None) -> set[str]:
    """THE CURATED SPLIT IDS SOMETHING USES (module doc): by id, by area, by merged place, by DFO.
    `entries`: the catalogue entries (dicts); `dfo`: the DFO salmon region files' JSON (any shape —
    every string in them is a candidate id); `registry`: {item id: RegistryItem}, for the merged
    places (without it, aliases are not followed)."""
    from pipeline.atlas.splits.splits import splits_on
    all_splits = list(iter_splits(splits_json))
    known = {sp["id"] for sp in all_splits}
    ids, areas = named_by_entries(entries)
    used = set(ids)
    used |= {sp["id"] for sp in all_splits if split_area_slug(sp) and split_area_slug(sp) in areas}
    used |= known & {s for d in dfo for s in _strings(d)}
    places = []                                   # (every id at a place, its item, its other names)
    for item in (registry or {}).values():
        for b in item.boundaries or ():
            answers = set(b.answers_to())
            if len(answers) > 1 or b.names:
                places.append((answers, item, {str(n).lower() for n in (b.names or ())}))
    grew = True
    while grew:                                   # a place reached through another place's alias
        grew = False
        for answers, item, names in places:
            if not answers & used:
                continue
            more = set(answers)
            if names:
                more |= {sp["id"] for sp in splits_on(splits_json, set(item.ref_ids))
                         if str(sp.get("label", "")).lower() in names}
            if more - used:
                used |= more
                grew = True
    return used & known
