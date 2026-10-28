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

from pipeline.atlas.registry import regions as _regions
from pipeline.common import entry_id as EID

#: POLICY (user ruling 2026-09-25, 9144c975): a WATER row (its water printed by no other region)
#: is not held to any region — neither its reach nor its tributary walk (`region_limit`).
WATER_ROW_WALKS_CROSS_REGIONS = True

#: POLICY (user ruling 2026-09-24): a regional row's MUs name its ZONE where the book prints a
#: region as zones (Region 7: 7A and 7B) — `entry_regions`.
ZONES_FROM_UNITS = True


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
    inside = _regions.area_family_sections(registry, "region", graph)
    if inside is not None:
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
    return EID.parse(entry_id or "", registry).regions


def shared_waters(entries) -> frozenset[str]:
    """THE WATERS THE BOOK PRINTS A ROW FOR IN MORE THAN ONE REGION — each such row is about the
    stretch in its own region (a PER-REGION row).

    The Fraser has a row in Regions 2, 3, 5 and 7; the Stellako in 6 and 7; the Canim in 3 and 5.
    A water is shared when rows of two or more regions carry a rule about the water ITSELF — a row
    whose every rule is `tributaries_only` does not count: "West Road River's tributaries" is
    printed in Regions 6 and 7 ("For regulations on the mainstem of the West Road River, see Region
    5", p.54, p.61), and the mainstem's row is Region 5's alone. Pointer rows carry no rules and do
    not count either."""
    by: dict[str, set[str]] = {}
    for e in entries:
        p = EID.parse(str(e.get("entry_id") or ""))
        rules = [r for r in e.get("rules") or [] if isinstance(r, dict)]
        if p.regions is None or not rules or all(r.get("tributaries_only") for r in rules):
            continue
        region = p.book_region
        for i in e.get("matched") or ():
            by.setdefault(i, set()).add(region)
    return frozenset(i for i, rs in by.items() if len(rs) > 1)


def has_own_row(entry_id, rules) -> bool:
    """IS THIS A TABLE ROW ABOUT ITS OWN WATER? A water row (`entry_id.is_water_row`: `r<region>:…`, never a zone or
    province table) with at least one rule that is not `tributaries_only` — a pointer row ("See Bulkley River") has
    none, and a row whose every rule is about the tributaries ("West Road River's tributaries") speaks for the creeks,
    not the river. ONE predicate (T125 item 8, reach 9): the reach's `rowed_waters` and the bundle's
    `rules._own_rows` each wrote it. `rules` are corpus dicts or typed rules."""
    def tribs_only(r) -> bool:
        return bool(r.get("tributaries_only") if isinstance(r, dict) else getattr(r, "tributaries_only", False))
    return EID.is_water_row(str(entry_id or "")) and any(not tribs_only(r) for r in rules or ())


def rowed_waters(entries) -> dict[str, tuple[tuple[str, tuple[str, ...]], ...]]:
    """THE WATERS THE BOOK PRINTS A ROW OF THEIR OWN FOR: `{item_id: ((entry_id, sees), …)}`,
    `sees` the entries the row's `see` pointers name.

    What a confluence cut asks of its joining water (`build.confluence_excludes`, user ruling
    2026-09-30): a water with a row of its own is left out of the mainstem's rule; one without
    goes with the cut on both sides. A water has a row of its own when a TABLE row (an entry
    `r<region>:…`, never a zone entry) lists the water's registry item in `matched` and carries
    at least one rule about the water itself. A pointer row ("See Bulkley River") carries no rule
    and says the water's regulations are printed elsewhere, so it does not count; nor does a row
    whose every rule is `tributaries_only` ("West Road River's tributaries" is about the creeks,
    not the river). Which row is the CUT's own is the caller's to drop — and so is a row that
    points part of its water BACK at the cut's row: YOUNG CREEK (upstream of Hwy 20) prints
    "Downstream of Hwy 20: see Atnarko/Bella Coola Rivers", so at the Atnarko the creek has no row
    of its own (`build.own_rows`)."""
    out: dict[str, set[tuple[str, tuple[str, ...]]]] = {}
    for e in entries:
        eid = str(e.get("entry_id") or "")
        if not has_own_row(eid, [r for r in e.get("rules") or [] if isinstance(r, dict)]):
            continue
        sees = tuple(sorted({t for s in e.get("see") or [] if isinstance(s, dict)
                             for t in s.get("entry_ids") or ()}))
        for i in e.get("matched") or ():
            out.setdefault(i, set()).add((eid, sees))
    return {i: tuple(sorted(es)) for i, es in sorted(out.items())}


def water_regions(entry: dict, registry, graph=None) -> tuple[str, ...]:
    """The regions (zones, where the registry holds them) the row's own waters lie in — by the one
    region membership (`registry.regions.section_regions`: a straddling section — stream piece or
    lake — in every region it lies in, T183)."""
    member = _regions.section_regions(registry, graph)
    out: set[str] = set()
    for i in entry.get("matched") or ():
        if i in registry:
            for sec in registry[i].section_ids:
                out |= member.get(sec, frozenset())
    return tuple(sorted(out))


#: "(also in M.U. 5-15)" printed in a row's water name (`region_limit`)
_ALSO_IN = re.compile(r"also in M\.?\s*U\.?\s*(\d+[A-Za-z]?-\d+)", re.I)


def region_limit(entry: dict, registry, shared=None, *, graph) -> frozenset[str] | None:
    """The sections a regional row may bind, or `None` when it is not limited.

    A WATER'S OWN ROW APPLIES ALONG ITS WHOLE LENGTH, IN EVERY REGION (user ruling 2026-09-25) —
    West Road River's Region 5 row on its Zone 7A pieces, the Nechako's Region 7 row on its Region
    6 pieces, the Similkameen's Region 8 row on its Region 2 pieces. Only a PER-REGION row — its
    water printed by another region too (`shared`, from `shared_waters`) — is held to the regions
    its id names, as before.

    AND SO DOES ITS TRIBUTARY WALK (`WATER_ROW_WALKS_CROSS_REGIONS`, the same ruling, second half:
    "the row and its tributary walk extend into other regions"). A whole-water row is not limited
    at all: it used to be held to the regions its own water lies in, which let the row reach
    wherever its water did and stopped its walk at the line — the Skagit's "No Fishing Nov 1-June
    30" lost 163 tributary sections in Region 8, the Dean's closure 17 in Region 6 (226 in all).
    A row whose own words hold it to one zone says so in its extents (`within_area`: Williston
    Lake "in Zone B").

    `shared=None` (a caller that did not compute it) holds every row to its id's regions — the
    narrower answer, never a widening.

    THE REGION'S SECTIONS ARE THE ONE MEMBERSHIP (T115, `registry.regions.region_sections`), the
    zone rules' own: a stream piece straddling the line is in both regions (T183, owner 2026-10-24),
    so both regions' per-region rows bind it; `graph` matters only with the policy pin off."""
    regions = entry_regions(str(entry.get("entry_id") or ""), registry)
    if not regions:
        return None
    # A ROW THAT PRINTS ITS OTHER UNITS IN ITS NAME — "CANIM RIVER (also in M.U. 5-15)" (p.29) and its
    # Region 5 twin "(also in M.U. 3-46)" (p.43) — is a row of those regions too (user Q24,
    # 2026-10-07): the entry id names only the row's own MU column.
    also = {mu.split("-", 1)[0].lower() for mu in _ALSO_IN.findall(str(entry.get("name") or ""))}
    if also - set(regions):
        regions = tuple(sorted(set(regions) | also))
    if shared is not None and not (set(entry.get("matched") or ()) & set(shared)):
        if WATER_ROW_WALKS_CROSS_REGIONS:
            return None
        regions = tuple(sorted(set(regions) | set(water_regions(entry, registry, graph))))
    return _regions.region_sections(regions, registry, graph)


def tidal_sections(entries, registry) -> frozenset[str]:
    """THE SECTIONS OF THE WATERS THE BOOK CALLS TIDAL — every matched item of a row marked
    `tidal` (`CatalogueEntry.tidal`). "Nitinat Lake is tidal water; tidal regulations apply and a
    (federal) Tidal Waters Sport Fishing Licence is required" (p.17): no provincial rule, zone base,
    park closure or licensing record holds there — only the row's own note. The reach builder takes
    them out of every OTHER row's binding, the way it takes out water past the border
    (`build.build_reach`, `tidal`); the tidal row's own rules keep them.

    `entries` is the corpus as `(region, entry)` pairs or bare entry dicts."""
    return frozenset(tidal_owner(entries, registry, strict=False))


def tidal_owner(entries, registry, *, strict: bool = True) -> dict[str, str]:
    """`{section: entry_id}` — every tidal section with the row that calls its water tidal (the
    first such row by entry id). The one body behind `tidal_sections`; the run writes it as
    `tidal.jsonl` and the bundle's `tidal` table is read from that file.

    REFUSED (`strict`, the run): a row marked `tidal` whose matched waters place no section would
    state the fact nowhere, and a reader would show its water as open under the general rules. The
    review app (`tidal_sections`) reads the corpus as a curator is editing it, and skips such a row."""
    out: dict[str, str] = {}
    rows = sorted((e for x in entries for e in ((x[1] if isinstance(x, tuple) else x),)
                   if isinstance(e, dict) and e.get("tidal")),
                  key=lambda e: str(e.get("entry_id") or ""))
    for e in rows:
        got = {s for i in e.get("matched") or () if i in registry
               for s in registry[i].section_ids}
        if not got:
            if strict:
                raise SystemExit(f"tidal: {e.get('entry_id')} says its water is tidal, but its "
                                 f"matched waters {list(e.get('matched') or ())} place no section")
            continue
        for s in got:
            out.setdefault(s, str(e.get("entry_id") or ""))
    return out


# ------------------------------------------------------------------------------------------------
# T166 step 5 (owner 2026-10-28, RULINGS C-2): THE PIECE BELOW A TIDAL BOUNDARY IS TIDAL WATER.
# ------------------------------------------------------------------------------------------------

#: The resolved-cuts sidecar of an atlas (`splits.write_resolved`); a curated split flagged
#: `tidal: true` in splits.json carries `tidal` on its rows there (the atlas build, or the
#: `pipeline.atlas.sidecars` migration for a reused cut flagged after the build).
RESOLVED_SPLITS = "splits.resolved.json"

#: THE TIDAL PIECE IS THE WATER BELOW THE CUT, AND ONLY THAT WATER (owner 2026-10-28;
#: handoff/PLAN/curation-review/tidal/tributary-impact.md): `downstream_of` the tidal split on the
#: water(s) that carry it, by the same resolver a rule's extent uses (route measure on the cut's own
#: blue line, braid channels cut at build). No tributary is part of it: a stream entering the tidal
#: piece keeps its own water, and stays its river's tributary — the walk runs on the full network and
#: the tidal piece is taken out of each binding AFTER it (`build._without_tidal`). Walking the
#: freshwater-only graph instead cut 29 rules off whole tributary trees.
TIDAL_PIECE_HAS_NO_TRIBUTARIES = True


def tidal_splits(build_dir) -> tuple[str, ...]:
    """The split ids this atlas resolved as TIDAL BOUNDARIES (rows of `splits.resolved.json` with
    `tidal`), sorted. An atlas with no such file is refused: a missing sidecar is not "no tidal
    boundary" (AGENTS 37)."""
    import json
    from pathlib import Path
    p = Path(build_dir) / RESOLVED_SPLITS
    if not p.exists():
        raise SystemExit(f"tidal: {p} is missing — the atlas writes it; without it the tidal "
                         "boundaries cannot be read")
    rows = json.loads(p.read_text(encoding="utf-8"))
    return tuple(sorted({str(r["split_id"]) for r in rows if r.get("tidal")}))


def tidal_cut_owner(registry, graph, split_ids, *, strict: bool = True,
                    report: list | None = None) -> dict[str, str]:
    """`{section: "split:<id>"}` — THE TIDAL PIECE below each tidal split (`TIDAL_PIECE_HAS_NO_
    TRIBUTARIES`): `downstream_of` the cut, scoped to every registry item that carries it (its
    boundary, or an alias of one). A section below two cuts keeps the first by split id.

    REFUSED (`strict`, the run) — never a silent "nothing is tidal here": a tidal split no item
    carries (the atlas did not cut it), or one the resolver cannot place on its water. `report`
    (optional) collects one row per split: its items, the sections it makes tidal, and the pieces
    the resolver could not place (`unclassified`: a braid or arm reaching the sea unjoined) — those
    stay freshwater and are listed, never guessed (AGENTS 12)."""
    from pipeline.atlas.reach import extent as _resolve
    carriers: dict[str, set[str]] = {}
    for iid, it in registry.items():
        for b in getattr(it, "boundaries", ()) or ():
            for a in b.answers_to():
                carriers.setdefault(a, set()).add(iid)
    out: dict[str, str] = {}
    for sid in sorted(set(split_ids)):
        items = sorted(carriers.get(sid, ()))
        if not items:
            if strict:
                raise SystemExit(f"tidal: split {sid!r} is a tidal boundary but no registry item "
                                 "carries it — the atlas did not cut it (rebuild the atlas)")
            continue
        why: list = []
        got = _resolve.resolve_extent(registry, graph, [], {
            "op": "downstream_of", "splits": [sid], "item_ids": items}, reasons=why)
        if got is None:
            if strict:
                raise SystemExit(f"tidal: the piece below tidal split {sid!r} on {items} cannot be "
                                 f"resolved: {why}")
            continue
        secs = sorted(got.get("sections") or ())
        for s in secs:
            out.setdefault(s, f"split:{sid}")
        if report is not None:
            report.append({"split": sid, "items": items, "sections": len(secs),
                           "unplaced": sorted(got.get("unclassified") or ()),
                           "ambiguous_cut": list(got.get("ambiguous_cut") or ())})
    return out


def tidal_water(entries, registry, graph=None, split_ids=(), *, strict: bool = True,
                report: list | None = None) -> dict[str, str]:
    """`{section: owner}` — ALL THE TIDAL WATER the reach run takes out of every binding: the waters
    of the rows marked `tidal` (owner: the row's entry id, `tidal_owner`), then the piece below every
    tidal split (owner `split:<id>`, `tidal_cut_owner`). A row's claim stands where both hold. The
    run writes it as `tidal.jsonl`; the bundle's `tidal` table is read from that file."""
    out = dict(tidal_owner(entries, registry, strict=strict))
    if split_ids:
        for s, o in tidal_cut_owner(registry, graph, split_ids, strict=strict,
                                    report=report).items():
            out.setdefault(s, o)
    return out


def is_cut_owner(owner: str) -> bool:
    """Is a `tidal` owner a tidal split (`split:<id>`), not a row marked tidal?"""
    return str(owner or "").startswith("split:")
