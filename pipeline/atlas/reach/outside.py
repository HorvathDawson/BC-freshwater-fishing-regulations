"""Where B.C.'s regulations stop: the province's border, and a regional row's own region.

TWO LIMITS, both taken from the atlas and applied by the reach builder, so no reader has to apply
them again: the border to every binding, a rule's and a licensing record's; the region to a regional
row's RULES only (a classified water's designation is the water's, whichever table prints it —
`build.build_reach(regional=False)`).

OUTSIDE B.C. The book does not govern water past the provincial border ("BC regs simply don't
apply there", `pipeline/atlas/splits/border.py`), and rules reached it two ways: `whole` on an item
that crosses the line (the Kootenay, the Kettle, the Flathead, the Columbia) and tributary walks
down into Idaho, Montana, Washington, Alberta, the Yukon and Alaska. A section is outside when the
atlas marks it `out_of_bc` (its midpoint is past the outline) OR when it lies in no region polygon —
the 53 border slivers whose midpoint falls just inside the outline but which no region claims.
Measured on the full build: 181 sections carried a rule set with no provincial rule in it, every one
of them in no region; and 391 more `out_of_bc` sections carried provincial and zone rules, because
region membership is by intersection while `mark_out_of_bc` tests the midpoint.

A REGIONAL ROW'S REGION. The synopsis prints a river under each region it crosses — the Fraser has
a row in Regions 2, 3, 5 and 7 — and each row is about the stretch in ITS region. Matched to the
whole river, every one of the 251 Fraser sections carried all four regions' rows: Region 5's "Bait
ban, Sep 15-Jul 15" applied at Mission. A row `r<N>:…@<mus>` is limited to the regions its id
names: its own, and those of the management units it was printed under (`r5:toms_lake@6-1` is
Regions 5 and 6; Haida Gwaii's MUs 6-12/6-13 sit in Region 1's polygon, and the row's own region
keeps them). It is a limit like `within_area`, applied to an extent that runs as far as the row's
water does — a bare `whole`, or a one-sided cut — before the walk and to that rule's reach after
it, and reported, never silent. A reach bounded at both ends, a named item or an area is where the
book put it, even across a region line, and is not held (`build._row_water`).

Both need the registry's `area:region:*` items. A registry with none (a unit-test fixture) cannot
say what is outside a region, so the region half of each limit is skipped there, never guessed.
"""

from __future__ import annotations

import re

REGION_PREFIX = "area:region:"

_ENTRY_REGION = re.compile(r"^r(\d+[a-z]?):")

#: POLICY (user ruling 2026-09-24): a regional row's MUs name its ZONE where the book prints a
#: region as zones (Region 7: 7A and 7B) — `entry_regions`.
ZONES_FROM_UNITS = True


#: Caches keyed by the registry object's id — each value HOLDS the registry, so the id cannot be
#: reused by another object while its entry lives (a freed test fixture's id handed to the next
#: fixture would otherwise answer with the first one's regions).
_ITEMS_CACHE: dict = {}
_REGION_CACHE: dict = {}


def _region_items(registry) -> list[str]:
    hit = _ITEMS_CACHE.get(id(registry))
    if hit is not None and hit[0] is registry:
        return hit[1]
    got = sorted(k for k in registry.keys() if str(k).startswith(REGION_PREFIX))
    if len(_ITEMS_CACHE) > 16:
        _ITEMS_CACHE.clear()
    _ITEMS_CACHE[id(registry)] = (registry, got)
    return got


def outside_bc(registry, graph) -> frozenset[str]:
    """Every section outside British Columbia — `out_of_bc`, or in no region polygon.

    Computed once per (registry, graph) pair and kept on the graph object: 1.96M nodes is a
    second's work, and the review app calls the builder per rule."""
    if graph is None:
        return frozenset()
    cached = getattr(graph, "_reach_outside_cache", None)
    if cached is not None and cached[0] is registry:
        return cached[1]
    out = {nid for nid, n in graph.nodes.items() if getattr(n, "out_of_bc", False)}
    regions = _region_items(registry)
    if regions:
        inside: set[str] = set()
        for k in regions:
            inside.update(registry[k].section_ids)
        out |= {nid for nid in graph.nodes if nid not in inside}
    got = frozenset(out)
    try:
        setattr(graph, "_reach_outside_cache", (registry, got))
    except (AttributeError, TypeError):
        pass
    return got


def entry_regions(entry_id: str, registry=None) -> tuple[str, ...] | None:
    """The regions a regional row may bind in: its own, and those its MUs name. `None` for an
    entry that is not a regional row (`zp:`, `z<n>:`, or any other source).

    A REGION PRINTED AS ZONES. Region 7 is two, 7A and 7B, and a row's id says only "7" — but its
    MUs say which zone it was printed in. With a `registry` whose `area:region:7a`/`7b` items carry
    their MUs (`registry.build.add_region_units`), each of the row's MUs is resolved to its zone:
    `r7:williston_lake_in_zone_a@7-30+7-37+7-38` is 7A only, and `@7-31+7-36` 7B only. Read as
    "7", the Zone A row's tributary walk bound 7,707 Zone B sections. A row whose MUs do not all
    resolve (or that names none) keeps the whole region, as before — never narrowed on a guess."""
    m = _ENTRY_REGION.match(entry_id or "")
    if not m:
        return None
    got = {m.group(1)}
    mus: list[str] = []
    if "@" in entry_id:
        for mu in entry_id.split("@", 1)[1].split("+"):
            mu = mu.strip()
            head = mu.split("-", 1)[0]
            if head:
                got.add(head)
                mus.append(mu)
    if registry is not None and ZONES_FROM_UNITS:
        for r in sorted(got):
            zones = _zones_of(registry, r)
            mine = [mu for mu in mus if mu.split("-", 1)[0] == r]
            if not zones or not mine:
                continue
            hit = [{z for z, units in zones.items() if mu in units} for mu in mine]
            if all(hit):
                got.discard(r)
                got |= set().union(*hit)
    return tuple(sorted(got))


def _zones_of(registry, region: str) -> dict[str, frozenset[str]]:
    """{"7a": MUs, "7b": MUs} for a region the registry holds as lettered zones; {} otherwise."""
    out: dict[str, frozenset[str]] = {}
    for k in _region_items(registry):
        z = k[len(REGION_PREFIX):]
        if z.startswith(region) and z[len(region):].isalpha() and z != region:
            units = frozenset(getattr(registry[k], "mus", ()) or ())
            if units:
                out[z] = units
    return out


def region_sections(regions: tuple[str, ...], registry) -> frozenset[str] | None:
    """The union of the named regions' sections. A region `7` is 7A and 7B together; a region the
    registry does not have contributes nothing. `None` when the registry has no regions at all."""
    items = _region_items(registry)
    if not items:
        return None
    key = (id(registry), regions)
    hit = _REGION_CACHE.get(key)
    got = hit[1] if hit is not None and hit[0] is registry else None
    if got is None:
        want = [k for k in items for r in regions
                if k == f"{REGION_PREFIX}{r}" or (k.startswith(f"{REGION_PREFIX}{r}")
                                                  and k[len(REGION_PREFIX) + len(r):].isalpha())]
        s: set[str] = set()
        for k in want:
            s.update(registry[k].section_ids)
        got = frozenset(s)
        if len(_REGION_CACHE) > 64:
            _REGION_CACHE.clear()
        _REGION_CACHE[key] = (registry, got)
    return got


def region_limit(entry: dict, registry) -> frozenset[str] | None:
    """The sections a regional row may bind, or `None` when it is not limited."""
    regions = entry_regions(str(entry.get("entry_id") or ""), registry)
    if not regions:
        return None
    return region_sections(regions, registry)
