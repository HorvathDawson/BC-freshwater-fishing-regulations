"""What a rule lifts: the `exempts` column, each lift resolved to the entry it reaches (`_exempts`).

A lift the book prints, one derived from a row NAMING a fish its region closes (`_named_lifts`), one derived
from "(any size)" (`_any_size_lifts`), and one carried to the same kind of closure in another region the
water lies in (`_equivalent_closures`); `LIFT_KEYS` are the only keys a lift may carry."""
from __future__ import annotations

import json
import re
from pipeline.common import entry_id as EID
from pipeline.regs.parsing.catalogue_parts.vocab import says_no_fishing
from pipeline.deliver.bundle.rules.predicates import AGGREGATE_GROUPS, _zone_region


#: The keys of one resolved lift in the `exempts` column — and nothing else. Two name the lifted
#: rule; `note` is the book's words; `species`, `when_targeting`, `while` and `when`, when present,
#: say the lift holds only IN PART (see `_lift_terms`); so do `origin` (the lifter keeps only
#: hatchery fish, or only wild — printed or derived) and `lengths`, which only a derived lift
#: carries (the lifter keeps only some sizes — `_named_lifts`);
#: `basis` marks a lift derived from the lifter NAMING the fish (`_named_lifts`) and `equivalent`
#: one resolved to ANOTHER region's closure of the same kind (`_equivalent_closures`). The client
#: refuses any other key.
LIFT_KEYS = ("entry_id", "rule_id", "note", "species", "when_targeting", "while", "when",
             "origin", "lengths", "basis", "equivalent", "caution")

#: `caution.kind` on a lift of a REGION'S SIZE CLAUSE (`size_clause_caution`).
SIZE_CLAUSE_OVERRIDE = "size_clause_override"


def is_size_clause(lifted_eid: str, z) -> bool:
    """A REGION'S SIZE CLAUSE: a `within` clause of a region's own table (`z<region>:`, never the
    province's) that caps how many fish over a length the quota may hold — Region 5's "Trout/char:
    5, but not more than 1 over 50 cm", Region 4's "1 rainbow trout or cutthroat trout over 50
    cm". One band, a lower bound only, the clause's own count."""
    if not EID.is_zone(lifted_eid):
        return False
    if z.type.value != "retention_limit" or not z.within or not z.take or not z.lengths:
        return False
    if len(z.lengths) != 1:
        return False
    b = z.lengths[0]
    return b.min_cm is not None and b.max_cm is None and b.take is None


def overrides_size_clause(lifter_eid: str, by, lifted_eid: str, z) -> bool:
    """A WATER ROW'S LARGER NUMBER lifting a region's size clause (`is_size_clause`): the lifter is
    a water row (`r<region>:`) that keeps MORE of the fish than the clause allows (a larger
    `take`, or `unlimited`). A release that lifts the clause keeps none and overrides nothing;
    the region's own "20 brook trout from streams" is the table's own word, not a water's."""
    if not EID.is_water_row(lifter_eid) or not is_size_clause(lifted_eid, z):
        return False
    return by.type.value == "retention_limit" and (
        bool(by.unlimited) or (by.take is not None and by.take > z.take))


#: "(any size)" as a water row prints it: Kootenay Lake's "rainbow trout daily quota = 10 (any
#: size)", the Duncan and Lardeau rivers', Quesnel Lake's "Lake trout daily quota = 5 (any size)".
ANY_SIZE = re.compile(r"\(\s*any\s+size\s*\)", re.I)


def prints_any_size(by) -> bool:
    """Does the lifter's own sentence print "(any size)"? The ONE case where overriding the
    region's size clause is hard to read (RULINGS Q5): does "any size" mean no size
    limit at all — overriding "only 1 over 50 cm" — or only no minimum?"""
    return bool(ANY_SIZE.search(by.verbatim or ""))


def size_clause_caution(z) -> dict:
    """THE WARNING A LIFT OF A REGION'S SIZE CLAUSE CARRIES — ONLY WHERE THE ROW PRINTS "(ANY
    SIZE)" (RULINGS Q4, Q5). A water row printing a larger number for a fish
    than its region allows overrides the region's "only 1 over 50 cm" too, "(any size)" or not.
    Only "(any size)" is ambiguous — no size limit at all, or just no minimum? — so only that lift
    says so, in a field the reader can show (`guide.gotchas`). A row printing its own sizes
    (Gwillim Lake's "none under 40 cm or over 60 cm") or none at all (Jewel Lake's "Brook trout
    daily quota = 20") overrides the clause with no caution (`prints_any_size`)."""
    return {"kind": SIZE_CLAUSE_OVERRIDE,
            "says": f"overrides the region's 'only {z.take} over {z.lengths[0].min_cm:g} cm'; "
                    f"the row prints '(any size)' — whether that means no size limit at all or "
                    f"only no minimum size, the book does not say"}

#: `basis` on a lift the book does not print as an exemption but states by NAMING THE FISH (see
#: `_named_lifts`). Absent = the rule's own printed `exempts`.
NAMES_THE_FISH = "names_the_fish"
#: `basis` on a lift derived from a water row printing "(ANY SIZE)" (`_any_size_lifts`).
ANY_SIZE_BASIS = "any_size"


def _species_of(r) -> frozenset[str] | None:
    """The fish a rule speaks about, as leaves — `None` when it names none (it binds every angler
    whatever they catch). `species_except` is subtracted."""
    from pipeline.regs.parsing.catalogue import expand_species
    if not r.species:
        return None
    return frozenset(expand_species(list(r.species))) - frozenset(
        expand_species(list(r.species_except)))


def _targets(r) -> frozenset[str]:
    """What a rule is about when fishing FOR something: its `when_targeting`, or — on a method
    rule, where the condition sits on the clause — every clause's `when.targeting`, when every
    clause has one. "Spear fishing for burbot allowed" lifts "no spear fishing for game fish" only
    WHEN FISHING FOR BURBOT; read without its clause's target it lifted the ban for every fish."""
    if r.when_targeting:
        return frozenset(r.when_targeting)
    clauses = [c for c in r.gear if c.when is not None]
    if r.gear and len(clauses) == len(r.gear) and all(c.when.targeting for c in clauses):
        return frozenset(t for c in clauses for t in c.when.targeting)
    return frozenset()


def _lift_terms(by, lifted) -> dict | None:
    """HOW FAR `by` LIFTS `lifted`: `{}` wholly, a dict of qualifiers partly, `None` not at all.

    A LIFT IS NEVER WIDER THAN ITS LIFTER. This used to lift every rule it named whole, whatever the
    lifter said, and three rules showed what that costs:

      species         Duncan River's "exempt from the regional bull trout catch and release" is
                      about BULL TROUT and lifted the whole trout/char winter release — rainbow and
                      cutthroat included. Only the intersection is lifted. When the lifter's fish
                      cover the lifted rule's, the lift is whole; when they meet it in part, the
                      lift names the fish it holds for (`species`); when they do not meet, the
                      rule is not lifted at all.
      when_targeting  "Dead fin fish may be used when fishing FOR STURGEON" lifted the province's
                      fin-fish bait ban for every angler. The angler is always unknown, so a lift
                      that holds only for some target is a CONDITION (`when_targeting`), never a
                      removal — unless the lifted rule is itself only about those targets.
      while           a lift that holds only WHILE doing something (set lining, spearing) leaves
                      the rule standing for everyone else, unless the lifted rule binds only while
                      doing the same thing (`while`).
      origin          a lifter about HATCHERY (or WILD) fish only lifts only for them: the rule
                      stays for the other origin, partly lifted (Kitimat River's hatchery rainbow).
      when            A LIFT IS IN FORCE ONLY WHILE ITS LIFTER IS. The item carried no time, and
                      the guide read an item with no qualifier as "lifted outright" — so a lifter
                      printed for Jul 1-Apr 30 (Chilliwack/Vedder's hatchery rainbow quota) lifted
                      Region 2's "2 from streams" in May and June too, when its own quota was not
                      in force. See `_when_term`.

    `water` is not a qualifier: it is enforced where the lifter is PLACED (its extents carry
    `feature_types`), and a lifter whose `water` its placement does not enforce stops the build —
    see `_exempts`. The client lifts a rule outright only on an item with no qualifier."""
    terms: dict = {}
    mine, theirs = _species_of(by), _species_of(lifted)
    # "ALL FISH" IS EVERY FIN FISH. `ALL_FIN_FISH` does not expand (it is an open complement), so
    # read as a set it met no fish and Pine River's "Catch and release all fish" could lift no
    # zone quota. It covers every fish the lifted rule names, crayfish excepted — "fin fish and
    # crayfish" are two things to the book.
    if mine is not None and "ALL_FIN_FISH" in mine and theirs is not None:
        mine = mine | (theirs - {"CRA"})
    if mine is not None:
        if theirs is None:
            terms["species"] = sorted(mine)
        else:
            both = mine & theirs
            if not both:
                return None                     # it speaks about other fish: nothing is lifted
            if not theirs <= mine:
                terms["species"] = sorted(both)
    # ORIGIN, printed or derived. A printed lift from a hatchery-only quota lifts only for hatchery
    # fish; a wild one still answers to the lifted rule. Kitimat River's "Hatchery rainbow trout
    # … daily quota = 5, all year" lifted Region 6's "Trout of any size from streams, Nov 1-June
    # 30" release for EVERY rainbow — wild ones too —
    # because only a derived lift carried the lifter's origin. Which a fish is shows only once it
    # is caught, so the lifted rule stays, partly lifted (`read.effective_rules` step 3).
    if by.origin is not None and by.origin != lifted.origin:
        terms["origin"] = by.origin.value
    when = _when_term(by, lifted)
    if when is None:
        return None                             # never in force on a day the lifted rule is
    terms.update(when)
    want, have = _targets(by), _targets(lifted)
    if want:
        if not (have and have <= want):
            terms["when_targeting"] = sorted(want)
    if by.while_:
        acts = frozenset(by.while_)
        if not (lifted.while_ and frozenset(lifted.while_) <= acts):
            terms["while"] = sorted(acts)
    return terms


def _when_term(by, lifted) -> dict | None:
    """The lift's TIME: `{}` when the lifter is in force every day the lifted rule is, `None` when
    it is in force on none of them, else `{"when": <the lifter's own When>}`.

    Only the calendar decides the two ends. A lifter with hours, weekdays or an unparsed season
    is in force only on part of a day it names, so its lift always carries its `when` — unless
    its days miss the lifted rule's altogether, which the calendar can prove. `None` lifts
    nothing, exactly as a lifter about other fish does (`_lift_terms`): the two rules speak on
    different days and neither needs the other lifted."""
    from pipeline.regs.parsing.catalogue import held_days
    mine, theirs = by.when, lifted.when
    if mine is None or mine.is_empty():
        return {}
    if theirs is not None and mine == theirs:
        return {}
    my_days, their_days = held_days(mine), held_days(theirs)
    if not my_days & their_days:
        return None
    whole_days = not (mine.hours or mine.weekdays or mine.unparsed)
    if whole_days and their_days <= my_days:
        return {}
    return {"when": mine.model_dump(mode="json", by_alias=True, exclude_none=True)}


def named_leaves(r) -> frozenset[str]:
    """The fish a rule NAMES — its species codes, each group that is the name of a fish expanded to
    its members, less `species_except`. An aggregate (`AGGREGATE_GROUPS`) names none: "Trout daily
    quota = 1" does not name the steelhead it holds."""
    from pipeline.regs.parsing.catalogue import expand_species
    got = set()
    for c in r.species:
        if c not in AGGREGATE_GROUPS:
            got |= set(expand_species([c]))
    return frozenset(got) - frozenset(expand_species(list(r.species_except)))


def _is_species_closure(r) -> bool:
    """A zone rule closing named fish: `take: 0`, `may_target: false`, no means (`while`), not a
    standing rule, not a superior authority's (nothing below a superior authority opens what it
    closed), and naming at least one fish (`named_leaves`) — "Bass: 0 quota, CLOSED TO FISHING",
    never "No fishing in any stream" (ALL_GAME_FISH names no fish)."""
    return (says_no_fishing(r.type, r.take, r.may_target)
            and not r.while_ and not r.caught and not r.standing and r.authority != "superior"
            and bool(named_leaves(r)))


#: "(No exceptions)" — Region 4's "White Sturgeon: 0 quota, CLOSED TO FISHING (No exceptions)".
_NO_EXCEPTIONS = re.compile(r"\bno exceptions?\b", re.I)
#: "(see tables for exceptions)" — Region 8's "Region 8 Daily Quotas (See tables for exceptions)"
#: box, where "Bass: 0 quota, CLOSED TO FISHING" is printed (p.68).
_SEE_TABLES = re.compile(r"\bsee\s+(?:the\s+)?tables?\s+for\s+(?:\w+\s+)*?exceptions\b",
                         re.I)


def sends_to_tables(ce, r) -> bool:
    """Does the closure, or the table it is printed in, SEND THE READER TO THE TABLES for its
    exceptions — "(see tables for exceptions)"? Only such a closure takes a derived lift by
    naming the fish (RULINGS L11): the water row naming the fish is then one of the
    exceptions the closure points to. Read off the rule's words, or its entry's (the box
    heading is quoted in the zone entry's `regs_verbatim`)."""
    return bool(_SEE_TABLES.search(r.verbatim or "") or _SEE_TABLES.search(ce.regs_verbatim or ""))


def prints_its_exemptions(ce, r) -> bool:
    """Does the closure's OWN entry say what is exempt from it — so no water row may add to the
    list by naming the fish (`_named_lifts`)?

    Two shapes, both printed. A sibling rule lifts it: Region 6's "No fishing: in all rivers and
    streams for steelhead, May 15 – June 15. Exemptions include mainstem portions of the Skeena,
    Nass, Iskut, Stikine and Taku Rivers …" (p.49) is followed, in the same entry, by the rule that
    exempts those mainstems. Or its words refuse any: "(No exceptions)". Region 8's "Bass: 0 quota,
    CLOSED TO FISHING (see tables for exceptions)" (p.68) does neither — it sends the reader to the
    water rows, and a row naming bass IS one of those exceptions."""
    if _NO_EXCEPTIONS.search(r.verbatim or ""):
        return True
    return any(x.target == r.rule_id and (not x.entry_id or x.entry_id == ce.entry_id)
               for s in ce.rules if s.rule_id != r.rule_id for x in s.exempts)


def zone_closures(docs) -> dict[str, list[tuple[str, object]]]:
    """`{region: [(entry_id, rule), …]}` — every species closure of a region's own table
    (`z<region>:`, never the province's `zp:`) that a water row may lift by naming the fish, for
    `_named_lifts`. A closure that prints its own exemptions (`prints_its_exemptions`) is not one:
    Kitimat River's "Hatchery steelhead daily quota = 2" lifted Region 6's steelhead stream closure
    on 4,733 sections — wild steelhead and every tributary included — though the closure's own
    exemption list does not name the Kitimat (RULINGS L12). And only a closure that
    SENDS THE READER TO THE TABLES for its exceptions (`sends_to_tables`) is one (RULINGS
    L11): any other closure is lifted only by an exemption the row prints."""
    out: dict[str, list] = {}
    for ce in docs:
        eid = ce.entry_id
        if not EID.is_zone(eid):
            continue
        for r in ce.rules:
            if _is_species_closure(r) and not prints_its_exemptions(ce, r) \
                    and sends_to_tables(ce, r):
                out.setdefault(_zone_region(eid), []).append((eid, r))
    return out


def _kept_lengths(r, fish: frozenset[str]) -> list[dict] | None:
    """THE SIZES A LIFTER KEEPS, as a lift term — `None` when it keeps every size of `fish` the
    book counts as that fish. The bands with a `take` of 0 are what it releases; a band that only
    restates the fish's definition ("Hatchery steelhead (>50 cm)" — a steelhead IS a rainbow over
    50 cm, p.80) narrows nothing."""
    from pipeline.regs.parsing.catalogue import DEFINITIONAL_SIZE
    keep = [b.model_dump(mode="json", exclude_none=True)
            for b in (r.lengths or []) if b.take != 0]
    keep = [{k: v for k, v in b.items() if k in ("min_cm", "max_cm")} for b in keep]
    if not r.lengths or not keep:
        return None
    if all(not b for b in keep):
        return None
    defs = [DEFINITIONAL_SIZE.get(f) for f in sorted(fish)]
    if len(keep) == 1 and all(d is not None for d in defs) and all(
            keep[0].get("min_cm") == d.get("min_cm") and keep[0].get("max_cm") == d.get("max_cm")
            for d in defs):
        return None
    return keep


def _named_lifts(entry_id: str, r, closures: dict[str, list]) -> list[dict]:
    """A WATER ROW NAMING A FISH ITS REGION CLOSES LIFTS THAT CLOSURE FOR THAT FISH (RULINGS
    L11). "Bass: 0 quota, CLOSED TO FISHING (see tables for exceptions)" is Region 8's; the
    Okanagan River's row prints "bass daily quota = 8" and no exemption, and the ladder never
    displaces a closure — so 5,063 sections showed a water quota under a closure of the same fish,
    and the page had to infer that the row is one of the "exceptions". The book's reading is the
    water row's: it overrides the region-wide rule, but ONLY for a fish BOTH NAME. West Road's
    "Trout daily quota = 1" names no steelhead ("trout" is an aggregate), so Region 6's steelhead
    stream closure stands there.

    Derived HERE, once, into the same `exempts` column as a printed lift, with `basis:
    names_the_fish` — the client never infers it. The lifter is a water row's retention rule that
    lets the fish be fished for (`may_target` not false; a quota, a size limit or a catch and
    release), and not a clause of another (`within`: its parent lifts). How far it lifts is
    `_lift_terms` (its dates, means and target), narrowed to the fish both name.

    A DERIVED LIFT NEVER REOPENS MORE THAN ITS LIFTER COVERS (RULINGS L12, Kitimat):
      origin   a lifter about HATCHERY fish ("Hatchery steelhead … daily quota = 2") lifts only for
               them (`origin`): the closure stands for wild fish;
      size     a lifter keeping only some sizes lifts only for them (`lengths`, the kept bands); a
               band that restates the fish's own definition narrows nothing (`_kept_lengths`);
      dates    its `when` (`_when_term`);
      place    the lift is in force only where the lifter is bound — never further (the reader
               lifts on a section only while the lifter speaks there).
    An origin or size term lifts IN PART: the angler and the fish are unknown until it is caught,
    so the closure stays and the reader marks it partly lifted. And a closure that prints its own
    exemption list accepts no derived lift at all (`zone_closures`)."""
    if not EID.is_water_row(entry_id) or r.type.value != "retention_limit" or r.may_target is False \
            or r.within or r.standing or r.lift_only or r.record_retention or r.undrawn_part:
        return []
    mine = named_leaves(r)
    if not mine:
        return []
    region = _zone_region(entry_id)
    out: list[dict] = []
    for zr in sorted(closures):
        if region not in (zr, EID.book_region(zr)):
            continue
        for eid, z in closures[zr]:
            both = mine & named_leaves(z)
            if not both:
                continue
            terms = _lift_terms(r, z)
            if terms is None:
                continue
            theirs = _species_of(z) or frozenset()
            terms.pop("species", None)
            if both != theirs:
                terms["species"] = sorted(both)
            # `origin` is `_lift_terms`' own (a printed lift carries it too).
            sizes = _kept_lengths(r, both)
            if sizes:
                terms["lengths"] = sizes
            out.append({"entry_id": eid, "rule_id": z.rule_id, **terms, "basis": NAMES_THE_FISH})
    return out


def is_size_constraint(lifted_eid: str, z) -> bool:
    """A SIZE CONSTRAINT of a region's own table (`z<region>:`, never the province's) for "(any size)"
    to lift (`_any_size_lifts`): a size clause (`is_size_clause`: "only 1 over 50 cm"), or a size-only
    rule — bands and no count of its own ("none under 30 cm", "none over 50 cm"). A quota printing
    its own sizes ("Bull trout: 1, 30-50 cm only") is not one: it competes with the water's quota on
    its own key, and the water's number already wins there."""
    if not EID.is_zone(lifted_eid):
        return False
    if z.type.value != "retention_limit" or not z.lengths or z.may_target is False:
        return False
    return is_size_clause(lifted_eid, z) or (z.take is None and not z.unlimited)


def _any_size_lifts(entry_id: str, r, rules_of: dict[str, dict]) -> list[dict]:
    """"(ANY SIZE)" DROPS EVERY SIZE CONSTRAINT OF ITS REGION FOR ITS FISH (RULINGS Q5a,
    settling the Q5 caution's question: no size limit at all, not just no minimum). A
    water row's keeping quota printing "(any size)" lifts every size constraint of its region's tables
    (`is_size_constraint`: the "only 1 over 50 cm" clauses, the "none under/over X cm" rules) for the
    fish both name, whether or not the book prints the exemption: Robb Lake's "Bull trout daily quota
    = 1 (any size)" lifts Zone 7A's "no more than 1 over 50 cm" for bull trout (its printed number is
    no larger, so Q5 alone never lifted it). Derived HERE, once, into `exempts` with `basis:
    any_size` — the client never infers it. How far it lifts is `_lift_terms` (fish both name, its
    dates, origin); the lift holds only where the lifter is bound (the reader lifts on a section only
    while the lifter speaks there)."""
    if not EID.is_water_row(entry_id) or r.type.value != "retention_limit" or r.may_target is False \
            or not (r.unlimited or (r.take or 0) > 0) or r.within or r.lift_only \
            or not prints_any_size(r):
        return []
    region = _zone_region(entry_id)
    out: list[dict] = []
    for eid in sorted(rules_of):
        zr = _zone_region(eid)
        if region not in (zr, EID.book_region(zr)):
            continue
        for rid in sorted(rules_of[eid]):
            z = rules_of[eid][rid]
            if not is_size_constraint(eid, z):
                continue
            terms = _lift_terms(r, z)
            if terms is None:
                continue
            out.append({"entry_id": eid, "rule_id": rid, **terms, "basis": ANY_SIZE_BASIS})
    return out


def is_blanket_closure(r) -> bool:
    """A region's BLANKET closure of a kind of water: "No fishing in any stream in Region 8, Apr
    1-June 30". `take: 0`, `may_target: false`, over every game fish (no species, or the aggregate
    of all game or fin fish — never a named fish: `_is_species_closure` is the other kind), on one
    kind of water (`water`), over a season (`when.dates`), with no means (`while`), not standing and
    not a superior authority's."""
    sp = set(r.species or ())
    return (says_no_fishing(r.type, r.take, r.may_target)
            and sp <= {"ALL_GAME_FISH", "ALL_FIN_FISH"} and not r.species_except
            and r.water is not None and r.when is not None and bool(r.when.dates)
            and not r.while_ and not r.caught and not r.standing and r.authority != "superior")


def blanket_closures(docs) -> dict[str, list[tuple[str, object]]]:
    """`{region: [(entry_id, rule), …]}` — every blanket closure of a region's own table
    (`z<region>:`, never the province's), for `_equivalent_closures`."""
    out: dict[str, list] = {}
    for ce in docs:
        eid = ce.entry_id
        if not EID.is_zone(eid):
            continue
        for r in ce.rules:
            if is_blanket_closure(r):
                out.setdefault(_zone_region(eid), []).append((eid, r))
    return out


def lift_kind(lifter_text: str, lifted_eid: str, lifted) -> str | None:
    """WHICH CLOSURE A ROW'S LIFT NAMES, by kind — "spring", "summer", "winter" — or None.

    The row's own words first ("Exempt from spring closure", "tributaries subject to spring
    closure": `lifter_text` is the lifter's `verbatim` and the exemption's `note`), else the
    closure it names by id (`closure_kind` on the lifted rule: "Mainstem open all year" lifts
    Region 5's "Spring closure"). Two kinds in the row's words, or words that contradict the
    closure named, is a question the build does not answer — it stops."""
    from pipeline.regs.parsing.catalogue import printed_closure_kinds
    printed = printed_closure_kinds(lifter_text)
    if len(printed) > 1:
        raise SystemExit(
            f"lift of {lifted_eid}/{lifted.rule_id}: the row names the "
            f"{'/'.join(sorted(printed))} closures ({lifter_text[:80]!r}) — which one it lifts "
            f"in another region cannot be told; split the exemption")
    named = lifted.closure_kind.value if lifted.closure_kind is not None else None
    if printed and named is not None and printed != {named}:
        raise SystemExit(
            f"lift of {lifted_eid}/{lifted.rule_id}: the row says the {next(iter(printed))} "
            f"closure ({lifter_text[:80]!r}) but names the {named} one — fix the exemption or "
            f"the closure's `closure_kind`")
    return next(iter(printed), named)


def _equivalent_closures(lifted_eid: str, lifted, regions, blankets: dict[str, list],
                         lifter_text: str = "") -> list:
    """THE SAME CLOSURE IN THE OTHER REGIONS THE ROW'S WATER LIES IN (RULINGS L5, L6).

    A water's own row applies along its whole length, whichever region each piece lies in, and so
    do the exemptions it prints; but each piece takes the ZONE rules of its own region. West Road
    River's row (Region 5, p.47) is one of the "other streams listed in the tables" Region 5's
    spring closure excepts (p.42), and it prints "tributaries subject to spring closure"; a
    mainstem piece lying mostly in Zone 7A carries Zone 7A's "No fishing (spring closure): in any
    stream of Zone A, Apr 1 – June 30", not Region 5's, and the row's exemption must reach it
    there. So a lift of a BLANKET closure (`is_blanket_closure`) also lifts every blanket closure
    of the same water (`water`) in each other region of `regions` that is THE SAME KIND OF CLOSURE
    THE ROW NAMES (`lift_kind`): "Exempt from spring closure" lifts another region's spring
    closure and never its winter or summer one, whatever the dates — Region 6's Skeena/Nass winter
    closure (Jan 1-June 15) overlaps every spring closure in the province, and by dates alone the
    Nechako's "Exempt from spring closure" lifted it.

    `regions` is `equivalent_regions`' answer for the lifter: never a region where the row's
    water has an entry of its OWN (RULINGS L6) — there the book speaks for the water
    in that region's table, and a neighbour's exemption does not carry.

    A closure of unknown kind in the way of a lift of known kind is refused: the build does not
    guess whether it is the one. Only a lift of NO known kind (the row's words name none, and
    neither does the closure it names) falls back to the same water over an overlapping season.
    A species closure is never lifted by analogy: only what the row itself names."""
    if not is_blanket_closure(lifted):
        return []
    from pipeline.regs.parsing.catalogue import _days
    kind = lift_kind(lifter_text, lifted_eid, lifted)
    mine = frozenset(_days(lifted.when.dates))
    home = _zone_region(lifted_eid)
    out = []
    for reg in sorted(regions or ()):
        if reg == home:
            continue
        for eid, z in blankets.get(reg, ()):
            if z.water != lifted.water:
                continue
            if kind is None:
                if mine & frozenset(_days(z.when.dates)):
                    out.append((eid, z))
                continue
            if z.closure_kind is None:
                raise SystemExit(
                    f"{eid}/{z.rule_id}: a blanket closure of no known kind, where a lift of "
                    f"{lifted_eid}/{lifted.rule_id} (the {kind} closure) reaches Region {reg} — "
                    f"say which closure it is (`closure_kind`); the build does not guess it from "
                    f"its dates")
            if z.closure_kind.value == kind:
                out.append((eid, z))
    return out


def _exempts(entry_id: str, r, zones: dict[str, list[str]], rules_of: dict[str, dict],
             closures: dict[str, list] | None = None, regions=None,
             blankets: dict[str, list] | None = None):
    """The rule's `exempts`, RESOLVED to the rules it lifts and how far — the `exempts` column.

    EXEMPTIONS WERE APPLIED NOWHERE. 88 rules carry one, 63 of them "Exempt from spring
    closure", and the bundle shipped the field buried in `conditions`, which the client never
    selected — so the North Thompson read CLOSED on May 1 beside its own "Exempt from spring
    closure", and about 27k sections carried a zone default next to the rule that lifts it.

    Resolved HERE, once, to exact rule ids, so the client matches exact ids and never a bare name
    (AGENTS 8), and never a whole entry:

      `default_id`  a zone default by its slug: every rule of every zone entry `z<region>:<slug>`
                    of THIS rule's region (Region 7's rows reach both 7A and 7B). NEVER the rule's
                    own entry — `z6:steelhead_stream_closure` names its own slug, and a rule that
                    lifts itself deletes itself on every water it covers (the self-lift; its
                    `review_reason` carries it).
      `target`      one rule by id, in `entry_id` when the exemption says so, else in this entry.

    Each lifted rule is one item, `{"entry_id", "rule_id"}` (+ the authored `note`), carrying the
    qualifiers `_lift_terms` found — `species`, `when_targeting`, `while`, `when` — when the lift
    holds only in part. An item with no qualifier lifts its rule outright; one with any lifts it only for those
    anglers, so the rule stays in force and the client marks it partly lifted. (This replaced items
    naming a whole zone entry by `default_id`, which the client lifted wholesale: that shape is what
    let a bull-trout exemption lift the whole trout/char release.)

    A lifter's `water` must be enforced by its placement — every extent of its own carrying
    `feature_types == [water]` — or the build stops: the client has no water kind to check it by.

    An exemption that resolves to nothing lifts nothing, which is only allowed when the rule
    says why in `review_reason`; otherwise the build stops, because a lift that silently fails
    leaves a closure standing where the book lifted it. NULL = the rule lifts nothing."""
    if r.exempts and r.water is not None:
        own = r.extents or []
        if not own or any([str(t).lower() for t in (x.get("feature_types") or [])]
                          != [r.water.value] for x in own):
            raise SystemExit(
                f"{entry_id}/{r.rule_id}: lifts only on {r.water.value}s (`water`), but its "
                f"extents do not place it only on {r.water.value}s — give every extent "
                f"`feature_types: [\"{r.water.value}\"]`, or the lift reaches other water")
    out: list[dict] = []
    for x in r.exempts:
        named: list[tuple[str, str]] = []
        if x.default_id:
            region = _zone_region(entry_id)
            for z in zones.get(x.default_id, ()):
                zr = _zone_region(z)
                if z != entry_id and region in (zr, EID.book_region(zr)):
                    named += [(z, rid) for rid in sorted(rules_of.get(z, {}))]
        if x.target:
            in_entry = x.entry_id or entry_id
            if x.target in rules_of.get(in_entry, {}) and not (
                    in_entry == entry_id and x.target == r.rule_id):
                named.append((in_entry, x.target))
        got: list[dict] = []
        for e, rid in named:
            lifted = rules_of[e][rid]
            terms = _lift_terms(r, lifted)
            if terms is None:
                continue
            got.append({"entry_id": e, "rule_id": rid, **terms,
                        **({"note": x.note} if x.note else {}),
                        **({"caution": size_clause_caution(lifted)}
                           if overrides_size_clause(entry_id, r, e, lifted)
                           and prints_any_size(r) else {})})
        # The row's water in another region: the same blanket closure there (`regions` — the
        # regions the row's own water lies in; see `_equivalent_closures`).
        have = {(e, rid) for e, rid in named} | {(y["entry_id"], y["rule_id"]) for y in out}
        for e, rid in named:
            for ee, z in _equivalent_closures(e, rules_of[e][rid], regions, blankets or {},
                                              f"{r.verbatim} {x.note or ''}"):
                if (ee, z.rule_id) in have or ee == entry_id:
                    continue
                have.add((ee, z.rule_id))
                terms = _lift_terms(r, z)
                if terms is None:
                    continue
                got.append({"entry_id": ee, "rule_id": z.rule_id, **terms,
                            "equivalent": f"{e}::{rid}",
                            **({"note": x.note} if x.note else {})})
        if not got and not r.review_reason:
            raise SystemExit(
                f"{entry_id}/{r.rule_id}: exempts {x.model_dump(exclude_none=True)} lifts no "
                f"rule in the corpus. Name the rule (`target` + `entry_id`), or say why in "
                f"`review_reason` — a lift that silently resolves to nothing leaves the "
                f"closure standing where the book lifted it")
        out += got
    printed = {(x["entry_id"], x["rule_id"]) for x in out}
    derived = [x for x in _named_lifts(entry_id, r, closures or {})
               if (x["entry_id"], x["rule_id"]) not in printed]
    have = printed | {(x["entry_id"], x["rule_id"]) for x in derived}
    derived += [x for x in _any_size_lifts(entry_id, r, rules_of)
                if (x["entry_id"], x["rule_id"]) not in have]
    if derived and r.water is not None:
        own = r.extents or []
        if not own or any([str(t).lower() for t in (x.get("feature_types") or [])]
                          != [r.water.value] for x in own):
            raise SystemExit(
                f"{entry_id}/{r.rule_id}: names a fish its region closes, so it lifts that "
                f"closure (`_named_lifts`), but it binds only on {r.water.value}s (`water`) and its "
                f"extents do not place it only there — give every extent `feature_types`")
    out += derived
    return json.dumps(out, separators=(",", ":"), sort_keys=True) if out else None
