"""The regions a row's own water lies in, asked of the one region membership (`regions_of_waters`,
`WATER_REGIONS_BY_HOME`), and the regions rules are bound in together (`co_bound_regions`)."""
from __future__ import annotations

from pipeline.common import entry_id as EID
from pipeline.deliver.bundle.rules.predicates import _zone_region


#: POLICY (T140, owner Q99 2026-10-23): THE REGIONS A ROW'S OWN WATER LIES IN are asked of the ONE
#: region membership (`registry.regions`, T115) with the atlas's home map (`home`): a stream piece
#: straddling a region line lies in its HOME region only, a lake in every region it touches — the
#: same members the zone rules and the per-region row limit read. Before, the bundle called
#: `outside.water_regions` with no graph, i.e. plain polygon membership, so a printed lift carried
#: (G3) into a region where the water has only a straddling piece homed elsewhere. False = plain
#: polygon membership (the mutation pin).
WATER_REGIONS_BY_HOME = True


def regions_of_waters(registry, docs, home=None) -> dict[str, frozenset[str]]:
    """`{entry_id: regions}` — the regions each row's OWN waters (`matched`) lie in. What a row's
    printed lift of a blanket closure reaches in the other regions (`_equivalent_closures`). The
    arithmetic is the reach builder's (`outside.water_regions` over `registry.regions`, the one
    region membership), called once per row — never a second copy of it. `home` is the atlas's home
    map (`regions.home_view`); `None` is plain membership (a fixture's)."""
    from pipeline.atlas.reach.outside import water_regions
    view = home if WATER_REGIONS_BY_HOME else None
    return {ce.entry_id: frozenset(water_regions({"matched": list(ce.matched)}, registry, view))
            for ce in docs if EID.is_water_row(ce.entry_id) and ce.matched}


def _own_rows(docs) -> dict[str, frozenset[str]]:
    """`{entry_id: matched items}` for every water row with at least one rule about the water
    itself — a row whose every rule is `tributaries_only` (Region 6's and Region 7's "West Road
    River's tributaries") speaks for the tributaries, not for the water."""
    from pipeline.atlas.reach.outside import has_own_row
    return {ce.entry_id: frozenset(ce.matched) for ce in docs if ce.matched and has_own_row(ce.entry_id, ce.rules)}


def co_bound_regions(sets, own_rows: dict | None = None) -> dict[tuple[str, str], frozenset[str]]:
    """`{(entry_id, rule_id): zone regions}` — for every water row's rule, the regions whose OWN
    zone tables (`z<region>:`, never the province's) the reach run bound on a section the rule
    reaches THROUGH THE TRIBUTARY WALK (`via == "trib"`). Read off the run's rule sets
    (`intern_sets`), never derived again: the region a section takes its zone rules from is the
    run's decision (`section_home`). This is where a row's lift reaches another region through
    its tributaries (the Similkameen's "exempt from spring closure", printed with the tributary
    symbol, reaches 28 tributary sections that lie in Region 3), which the row's own matched
    water (`regions_of_waters`) never shows. Only the walk: an AREA row's sections (Bowron Lake
    Park's, in Zone 7A) are no water lying in another region, and its lift stays in its own.

    A TRIBUTARY WITH AN ENTRY OF ITS OWN THERE (`own_rows`: a row of that region bound on the
    section by its own reach, not the walk) is the G3 caveat at the tributary: the region does
    not count from that section. The lift is region-wide (`exempts` carries regions, not
    sections), so a region still counts when ANY walked section there has no row of its own."""
    out: dict[tuple[str, str], set[str]] = {}
    for rows in sets:
        zr = {_zone_region(e) for e, _, _ in rows if EID.is_zone(e)}
        if own_rows is not None:
            mine = {EID.book_region(_zone_region(e)) for e, _, via in rows
                    if via != "trib" and e in own_rows}
            zr = {z for z in zr if EID.book_region(z) not in mine}
        if not zr:
            continue
        for e, r, via in rows:
            if EID.is_water_row(e) and via == "trib":
                out.setdefault((e, r), set()).update(zr)
    return {k: frozenset(v) for k, v in out.items()}


def own_entry_regions(docs) -> dict[str, frozenset[str]]:
    """`{item_id: book regions}` — the regions in whose tables a water has an ENTRY OF ITS OWN: a
    row (`r<region>:`) matching the water with at least one rule about the water itself
    (`_own_rows`). A row whose every rule is `tributaries_only` (Region 6's and Region 7's "West
    Road River's tributaries") speaks for the tributaries, not for the water, and is not one.

    AN ENTRY, NOT A COVERAGE TEST (review L1, 2026-10-05): `r3:sicamous_narrows` matches the
    Shuswap River and binds its only Region 3 section, so it is the river's Region 3 entry. A
    test that every section of the water in the region is bound by its own rows was tried and
    refused: about 90 waters' own rows miss a boundary sliver (the Shuswap's Region 8 row binds
    58 of its 59 Region 8 sections), so it would unmake real entries for a geometry artefact."""
    out: dict[str, set[str]] = {}
    for eid, items in _own_rows(docs).items():
        for item in items:
            out.setdefault(item, set()).add(EID.book_region(_zone_region(eid)))
    return {k: frozenset(v) for k, v in out.items()}


def equivalent_regions(ce, rule_id: str, water: dict[str, frozenset[str]],
                       co_bound: dict[tuple[str, str], frozenset[str]],
                       own: dict[str, frozenset[str]]) -> frozenset[str]:
    """THE OTHER REGIONS A ROW'S PRINTED LIFT CARRIES INTO (`_equivalent_closures`; user rulings
    2026-09-25/26, 2026-10-05): the regions the row's own water lies in (`regions_of_waters`) and
    those whose zone tables the reach bound beside this rule (`co_bound_regions`: its tributaries)
    — LESS every region where the row's water has an entry of its own (`own_entry_regions`). The
    Fraser has rows in Regions 3, 5 and 7, so Region 5's "Mainstem open all year" never lifts
    Region 3's or Zone 7A's spring closure; the Canim's Region 5 row never lifts Region 3's (its
    Region 3 row prints no exemption). West Road River has only TRIBUTARY rows in Region 6 and
    Zone 7A, so its mainstem pieces there keep the row's lift."""
    home = EID.book_region(_zone_region(ce.entry_id))
    mine = set(water.get(ce.entry_id, frozenset())) | set(co_bound.get((ce.entry_id, rule_id),
                                                                        frozenset()))
    theirs = set().union(*(own.get(i, frozenset()) for i in ce.matched)) - {home}
    return frozenset(r for r in mine if EID.book_region(r) not in theirs)
