"""The regulations, from the reach builder's output into the bundle.

FOUR TABLES, AND ONE OF THEM IS THE WHOLE DESIGN.

`entry` and `rule` are transcription — the curated corpus, carried verbatim. The interesting
pair is `section_ruleset` and `ruleset`, and what they are is a compression that only works
because of something true about regulations rather than about data: a rule applies to a
STRETCH of river, and a stretch is many sections. The obvious table — one row per (section,
rule) — is 1,720,243 rows and 69.6 MB on a 51.8 MB bundle. Those rows carry 1,905 distinct
answers. Interning them costs 12.1 MB and reads four times faster.

THIS MODULE CONSUMES, IT DOES NOT DERIVE. Everything about which water a rule covers —
the four tributary states, the per-rule carve-outs, the reach-scoped walk with its three
guards — is decided in `pipeline.atlas.reach` and arrives here already resolved. Re-deriving
any of it from the curated flags would put a second implementation of "which water does this
rule cover" in the codebase, which is exactly how three copies of the trust rule happened.
The flags are read here for ONE thing (`uncertain`) and that is not a re-derivation.
"""

from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path


def _when(r) -> str | None:
    """The rule's `when` — dates, hours, weekdays and unparsed seasons — as its own column.

    THE SEASON WAS LOST FROM THE APP. This column was `windows`, filled from the rule's raw
    `windows` or, failing that, `dates` — the prose model's date STRINGS, parsed a second time by
    `pipeline.regs.parsing.dates`. A catalogue rule has neither: its season is `when`, already
    structured by the model. So every rule shipped `windows = []`, which the client reads as ALL
    YEAR, and every seasonal closure in the province (0 of 3,269 rules had a window; 617 did before
    the `when` migration) read as in force every day. Nothing failed, because `[]` is a valid
    season.

    Now the model's own `When` ships whole and in the model's own shape (by alias, as
    `conditions` carried it), and there is no fallback: a rule's season comes from `when` or it
    has none. `unparsed` ships too — a season the parser could not read is NOT all year, and the
    client must treat such a rule as uncertain, never as always in force (the reader belongs in
    app/packages/core/src/regulations.ts; the export's `guide.time` says so).
    NULL = no `when` = all year, per the synopsis.
    """
    if r.when is None or r.when.is_empty():
        return None
    return json.dumps(r.when.model_dump(mode="json", by_alias=True, exclude_none=True),
                      separators=(",", ":"), sort_keys=True)


def _mus_of(entry_id: str) -> list[str]:
    """The MUs a catalogue entry covers, from its own id.

    `entry_id` is `r{region}:{slug}@{mus}` and the suffix is the MUs the synopsis ROW was
    printed under — which is exactly what the prose entry carried as `identity.mus`. Reading
    it here rather than storing it twice is what keeps the two from disagreeing."""
    return entry_id.split("@", 1)[1].split("+") if "@" in entry_id else []


def _specificity(rule: dict) -> str:
    """`section | mu | area` — WHERE THE RULE WAS WRITTEN, which is what drives precedence.

    A rule written for this water displaces a zone default that contradicts it, so the app
    has to be able to tell the two apart. Every rule in the corpus today is `section`: all
    3,050 name a river or a lake, and the two extents that name an area name it as a
    boundary rather than as the rule's own scope. `mu` appears when zone regulations are
    parsed; deriving it here rather than assuming "section" everywhere is what stops that
    day being a silent behaviour change.
    """
    for ex in rule.get("extents") or []:
        # `area_kind` counts as much as `area_id`. It names a FAMILY of areas — every
        # national park, every ecological reserve — and a rule written against one is a zone
        # rule by any reading. Checking only `area_id` scoped five of them as `section`,
        # which would have let a province-wide park closure outrank the water-specific
        # regulation it is supposed to sit under.
        if ex.get("area_id") or ex.get("area_kind"):
            return "area"
        # A PART OF A WATERSHED (`Extent.watershed`) is an area cut by code, like the whole
        # watershed (`area:basin:`) it is part of — not the river the cut sits on.
        if ex.get("watershed"):
            return "area"
    return "section"


#: Already a column, or meaningless to a client. Everything else the rule actually set goes
#: into `conditions` as JSON, so adding a condition to the catalogue needs no schema change.
#:
#: `extents` USED TO BE ON THIS LIST and the bundle shipped only `scope` — `section` or `area` —
#: which cannot tell "within Region 4" from "within Management Units 1-1 to 1-6". Those are the
#: two the ladder must separate: the first is a region's standing table, the second an override
#: on a handful of streams. So `corpus.catalogue()` read them back out of the curated files at
#: RUN time, which is a fallback: a reader could get an answer the bundle never agreed to, and
#: staleness stopped being visible. They ship here now and that reader is gone.
_NOT_CONDITIONS = frozenset({
    # `when`, `while` and `standing` are columns of their own, so each has one home.
    "rule_id", "type", "verbatim", "species", "species_except", "when", "while", "standing",
    "take",
    "may_target", "extent_text", "review_reason", "undrawn_part",
    "unresolved_locators",
    # Build-time only: a carve-out the reach builder applies before any section reaches the
    # bundle. Shipping it as a `condition` would put a resolver's input in front of a reader.
    "tributary_excludes",
    # A column of its own, RESOLVED: see `_exempts`.
    "exempts",
    # Build-time only: which closure a lift reaches in another region (`_equivalent_closures`).
    # What it decided ships RESOLVED, as `exempts[].equivalent`.
    "closure_kind",
})


def _zone_region(entry_id: str) -> str:
    """`r4:…` / `z4:…` -> "4", `z7a:…` -> "7a", `zp:…` -> "p"."""
    return entry_id.split(":", 1)[0][1:]


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
    if not lifted_eid.startswith("z") or lifted_eid.startswith("zp:"):
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
    if not lifter_eid.startswith("r") or not is_size_clause(lifted_eid, z):
        return False
    return by.type.value == "retention_limit" and (
        bool(by.unlimited) or (by.take is not None and by.take > z.take))


#: "(any size)" as a water row prints it: Kootenay Lake's "rainbow trout daily quota = 10 (any
#: size)", the Duncan and Lardeau rivers', Quesnel Lake's "Lake trout daily quota = 5 (any size)".
ANY_SIZE = re.compile(r"\(\s*any\s+size\s*\)", re.I)


def prints_any_size(by) -> bool:
    """Does the lifter's own sentence print "(any size)"? The ONE case where overriding the
    region's size clause is hard to read (user ruling 2026-09-28): does "any size" mean no size
    limit at all — overriding "only 1 over 50 cm" — or only no minimum?"""
    return bool(ANY_SIZE.search(by.verbatim or ""))


def size_clause_caution(z) -> dict:
    """THE WARNING A LIFT OF A REGION'S SIZE CLAUSE CARRIES — ONLY WHERE THE ROW PRINTS "(ANY
    SIZE)" (user rulings 2026-09-26, 2026-09-28). A water row printing a larger number for a fish
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

#: Groups that are NOT the name of a fish: an aggregate the book counts or closes as a class
#: ("Trout/char: 5", "No fishing in any stream"). A rule naming one of these names no single fish,
#: so it never takes part in a lift by naming (`_named_lifts`). "Bass" and "whitefish" are the
#: book's names for fish and count as naming each of theirs. "Char" stays an aggregate HERE — a
#: water's "char catch and release" derives no lift of a region's closure naming one char — though
#: the competition reads it as naming char (`read.names_fish`, `catalogue.NAMING_GROUPS`).
AGGREGATE_GROUPS = frozenset({"ALL_GAME_FISH", "ALL_FIN_FISH", "TROUT_CHAR", "CHAR",
                              "PROTECTED_SPECIES", "SALMON"})


#: The two origins a fish can have. A rule with no `origin` holds for both.
ORIGINS = frozenset({"wild", "hatchery"})


def release_origins(x: dict) -> frozenset[str] | None:
    """THE ORIGINS A RULE RELEASES OUTRIGHT — `None` when it is not an outright release.

    A bundle rule (`read.rules` shape) releases a fish outright when it is a `retention_limit`
    with `take: 0` that holds at every length (no `lengths`: "No wild trout over 50 cm" releases a
    size class, and the 4 under it still stand), whatever the means (no `while`), whatever is
    targeted (no `when_targeting`), and as a statement of its own (not a `within` clause, not a
    record-keeping duty). A closure is one. What it releases is its `origin`, or both.

    Shared by the competition (`read.effective_rules`) and anything that reasons about what a
    water row withholds, so the two cannot disagree on what "catch and release" is."""
    if x.get("type") != "retention_limit" or x.get("take") != 0:
        return None
    if x.get("lengths") or x.get("while") or x.get("when_targeting") or x.get("within") \
            or x.get("record_retention") or x.get("dimension") == "lift":
        return None
    return frozenset({x["origin"]}) if x.get("origin") else ORIGINS


#: What keeps a "no fishing" from being the whole answer: a closure carrying any of these holds
#: only for some fish, some gear, some part or some shore — it is PARTIAL (`closure_grade`).
CLOSURE_CONDITIONS = ("lengths", "origin", "while", "when_targeting", "within", "side")


def closure_grade(x: dict) -> str | None:
    """THE ONE CLOSURE PREDICATE: "full" for an unconditional "no fishing" (a `retention_limit`
    with take 0, may not fish for it, and none of `CLOSURE_CONDITIONS`, drawn — not a note held on
    its water for an undrawn part); "partial" for a closure that carries a condition or is such a
    note; None for anything that is not a closure (a quota, a release the fish may still be
    fished for, a lift).

    Every reader asks this function, with the edge it needs stated at the call: the status index
    colours CLOSED on "full" only (`status_index.is_full_closure`); the competition treats any
    closure as a closure that speaks for every fish it covers (`read.effective_rules`) and lets a
    "full" one beat another region's quota (`read.stricter`); the export's sample cases take any
    closure (`export_ui_rules._closure`). Four spellings with four exclusion lists lived at those
    call sites before; a rule with `side` was a closure to the reader and not to the map.

    Reads a bundle rule (`read.rules`: `undrawn_part`) or the export's flattened fields
    (`not_yet_mapped`): both spellings of the same column."""
    # `type` when the dict carries one (a bare {take, may_target} statement is a retention one)
    if x.get("type", "retention_limit") != "retention_limit" or x.get("take") != 0 \
            or x.get("may_target") != 0:
        return None
    if any(x.get(k) for k in CLOSURE_CONDITIONS):
        return "partial"
    if str(x.get("undrawn_part") or "").strip() or x.get("not_yet_mapped"):
        return "partial"
    return "full"


def yields_to_release(x: dict) -> frozenset[str] | None:
    """THE ORIGINS A RULE LETS AN ANGLER KEEP, when an outright release must silence it: a
    `retention_limit` that allows something (`take` above 0, `unlimited`) or states only sizes
    (`take` absent) — whatever conditions it holds under (origin, water, while, size). `None` for
    a rule that keeps nothing (another release: equally strict, it stands beside), for a rule that
    is no count of fish (a record-keeping duty; the possession multiplier "twice the daily quota",
    which has no number of its own to keep), and for a lift."""
    if x.get("type") != "retention_limit" or x.get("record_retention") \
            or x.get("dimension") == "lift":
        return None
    take = x.get("take")
    if not (x.get("unlimited") or (take is not None and take > 0)
            or (take is None and x.get("lengths"))):
        return None
    return frozenset({x["origin"]}) if x.get("origin") else ORIGINS


def _leaves(codes) -> frozenset[str]:
    from pipeline.regs.parsing.catalogue import expand_species
    return frozenset(expand_species(list(codes or [])))


def statement(x: dict) -> tuple:
    """WHAT A QUOTA IS ABOUT, without its number: the fish (leaves, less `species_except`), its
    size bounds (each band's ends, and whether the band keeps none), its origin, its water kind,
    the means it holds under (`while`), what must be targeted (`when_targeting`), and its clock.

    Two quotas with the same statement say the same thing with different numbers — Kokanee: 10 at
    a lake beside the region's Kokanee: 5 — and only then does the water's number replace the
    zone's (`same_statement`).

    THE PRINTED WORD "TROUT" IS ONE STATEMENT, whether or not its row's mention of char takes the
    char out of it (`catalogue.trout_scope_problems`, user ruling 2026-09-28): Amor Lake's "Trout
    daily quota = 2" (its row names no char: trout and char) and Region 1's "Trout: 4" (its box
    releases "All char": trout only) both say "trout", so for a trout the lake's 2 replaces the
    4 — and Region 1's char release still binds the char. So a `CHAR` exception on `TROUT_CHAR` is
    the word's scope, not part of what the statement is about."""
    bands = tuple((b.get("min_cm"), b.get("max_cm"), b.get("take") == 0)
                  for b in (x.get("lengths") or []))
    sp = list(x.get("species") or [])
    out = [c for c in (x.get("species_except") or [])
           if not (c == "CHAR" and "TROUT_CHAR" in sp)]
    return (_leaves(sp) - _leaves(out),
            bands, x.get("origin"), x.get("water"),
            tuple(sorted(x.get("while") or [])), tuple(sorted(x.get("when_targeting") or [])),
            x.get("period") or "daily", bool(x.get("record_retention")))


def same_statement(a: dict, b: dict) -> bool:
    """DO TWO QUOTAS SAY THE SAME THING (`statement`: same fish or group, size bounds, origin,
    water kind, means, target and clock)? Between a water's quota and a zone's (user rulings
    2026-09-26): when they do, the WATER's number replaces the zone's, larger or smaller —
    Tranquille Lake's "kokanee daily quota = 10" over Region 3's "Kokanee: 5", never the smaller
    of the two. When they do not, both speak: the Dean's "Steelhead daily quota = 1" counts
    toward Region 5's "Trout/char: 5". A water row printing a larger number for a fish than the
    zone's aggregate LIFTS the aggregate for that fish (`exempts`) — it is not decided here.
    Both must be keeping quotas (`yields_to_release`)."""
    return statement(a) == statement(b)


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
    from pipeline.regs.parsing.catalogue import DateRange, _days
    mine, theirs = by.when, lifted.when
    if mine is None or mine.is_empty():
        return {}
    if theirs is not None and mine == theirs:
        return {}
    year = _days([DateRange(from_month=1, from_day=1, to_month=12, to_day=31)])
    my_days = _days(mine.dates) if mine.dates else year
    their_days = _days(theirs.dates) if theirs is not None and theirs.dates else year
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
    return (r.type.value == "retention_limit" and r.take == 0 and r.may_target is False
            and not r.while_ and not r.standing and r.authority != "superior"
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
    naming the fish (user ruling 2026-09-28): the water row naming the fish is then one of the
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
    exemption list does not name the Kitimat (user ruling 2026-09-25). And only a closure that
    SENDS THE READER TO THE TABLES for its exceptions (`sends_to_tables`) is one (user ruling
    2026-09-28): any other closure is lifted only by an exemption the row prints."""
    out: dict[str, list] = {}
    for ce in docs:
        eid = ce.entry_id
        if not eid.startswith("z") or eid.startswith("zp:"):
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
    """A WATER ROW NAMING A FISH ITS REGION CLOSES LIFTS THAT CLOSURE FOR THAT FISH (user ruling
    2026-09-25). "Bass: 0 quota, CLOSED TO FISHING (see tables for exceptions)" is Region 8's; the
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

    A DERIVED LIFT NEVER REOPENS MORE THAN ITS LIFTER COVERS (user ruling 2026-09-25, Kitimat):
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
    if not entry_id.startswith("r") or r.type.value != "retention_limit" or r.may_target is False \
            or r.within or r.standing or r.lift_only or r.record_retention or r.undrawn_part:
        return []
    mine = named_leaves(r)
    if not mine:
        return []
    region = _zone_region(entry_id)
    out: list[dict] = []
    for zr in sorted(closures):
        if not (zr == region or (zr[:-1] == region and zr[-1:] in ("a", "b"))):
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


def is_blanket_closure(r) -> bool:
    """A region's BLANKET closure of a kind of water: "No fishing in any stream in Region 8, Apr
    1-June 30". `take: 0`, `may_target: false`, over every game fish (no species, or the aggregate
    of all game or fin fish — never a named fish: `_is_species_closure` is the other kind), on one
    kind of water (`water`), over a season (`when.dates`), with no means (`while`), not standing and
    not a superior authority's."""
    sp = set(r.species or ())
    return (r.type.value == "retention_limit" and r.take == 0 and r.may_target is False
            and sp <= {"ALL_GAME_FISH", "ALL_FIN_FISH"} and not r.species_except
            and r.water is not None and r.when is not None and bool(r.when.dates)
            and not r.while_ and not r.standing and r.authority != "superior")


def blanket_closures(docs) -> dict[str, list[tuple[str, object]]]:
    """`{region: [(entry_id, rule), …]}` — every blanket closure of a region's own table
    (`z<region>:`, never the province's), for `_equivalent_closures`."""
    out: dict[str, list] = {}
    for ce in docs:
        eid = ce.entry_id
        if not eid.startswith("z") or eid.startswith("zp:"):
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
    """THE SAME CLOSURE IN THE OTHER REGIONS THE ROW'S WATER LIES IN (user rulings 2026-09-25,
    2026-09-26).

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
    water has an entry of its OWN (user ruling 2026-10-05) — there the book speaks for the water
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
                if z != entry_id and (zr == region or (region and zr[:-1] == region
                                                       and zr[-1:] in ("a", "b"))):
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


def _rule_row(entry_id: str, raw: dict, uncertain: bool, siblings=None, zones=None,
              rules_of=None, unresolved: str | None = None, place_of=None, entries=None,
              closures=None, regions=None, blankets=None):
    """One `rule` row from one catalogue rule.

    Validated through `CatalogueRule` rather than read off the dict, because `family`,
    `dimension` and `label` are all DERIVED — and deriving them here from raw fields would put
    a second implementation of each in the codebase. `label` in particular has one home for
    the same reason the gauge trust wording does: a rule worded two ways is two rules to a
    reader.
    """
    from pipeline.regs.parsing.catalogue import (_COUNTED_TYPES, CatalogueRule, Obligation,
                                                 compose, label_parts)

    if "type" not in raw:
        # The prose model is gone. Writing NULLs here would give the bundle rule rows that
        # exist and say nothing, which is indistinguishable from a water with no rules.
        raise ValueError(
            f"{entry_id}/{raw.get('rule_id')}: rule has no `type` — this is a retired prose "
            f"rule and the bundle no longer has columns for it")
    r = CatalogueRule.model_validate(raw)
    # BY_ALIAS, OR THE PYTHON NAME SHIPS. `while`, `except` and `with` are Python keywords, so
    # the fields are `while_`/`except_`/`with_` in the model and the JSON name is the alias. A
    # dump without this puts `while_` in the bundle, where a reader looking for `while` finds
    # nothing and the circumstance a rule binds in silently disappears.
    dumped = r.model_dump(exclude_none=True, mode="json", by_alias=True)
    # THE RULE'S OWN EXTENTS, AND ONLY THOSE. A rule with none used to be given its entry's here,
    # so 139 rules the reach builder left UNBOUND — "on parts", a place it could not draw —
    # shipped `extents: [{op: whole}]` beside `uncertain = 1`: the bundle claiming the whole water
    # for a rule placement had refused to widen (AGENTS 13). Inheritance is placement's job, and
    # placement has already done it; what a rule binds is in `ruleset`, not here.
    #
    # No flag in the model means something when False, so False is dropped like any default.
    # (`v is False`, not `v == False`: `0 == False`, and a zero is a value.)
    _EMPTY = ((), [], {}, "")
    conditions = {k: v for k, v in dumped.items()
                  if k not in _NOT_CONDITIONS
                  and not any(v is e or v == e for e in _EMPTY)
                  and v is not False}
    # THE CLOCK, ON THE RULES THAT COUNT AND ONLY THOSE. The model leaves `period` unset where the
    # book's default (daily) holds; a counting rule ships its clock outright so no reader has to
    # know the default, and no other rule ships one (1,549 bait bans and boat rules said "daily").
    if r.type in _COUNTED_TYPES:
        conditions["period"] = r.clock.value
    # LAW IS THE DEFAULT: `obligation` ships only where the book gives ADVICE ("should"). It sat
    # on all 3,348 rules as "must", a key a reader had to learn to ignore.
    if conditions.get("obligation") == Obligation.must.value:
        del conditions["obligation"]
    # `entries`: what a rule lifts in ANOTHER entry is named in words, not by that entry's slug.
    parts = label_parts(r, siblings, place_of, entries)
    return (
        entry_id, r.rule_id, r.type.value, r.family, r.dimension,
        # THE LINE, AS PARTS, and the one preview composed from them (`catalogue.compose`). The
        # place is named from the extents, in the book's words (`place_names`), so two rules of
        # one entry on different reaches never share a line.
        compose(parts, r.verbatim),
        json.dumps(parts, separators=(",", ":"), ensure_ascii=False),
        _specificity(raw),
        _when(r),
        # WHILE, AS A COLUMN, because the client decides an OUTCOME from it: "only non-game fish
        # may be speared" is take 0 on every game fish WHILE spear fishing, and read without the
        # `while` it is "No fishing" on 1,674 of 1,693 rulesets — every river in B.C. The app
        # looked for it as `conditions.method`, a field that no longer exists, in a column its
        # query never selected. NULL = the rule binds whatever method you use.
        json.dumps(list(r.while_), separators=(",", ":")) if r.while_ else None,
        # STANDING: the rule holds everywhere but its place is unknowable — "no fishing within 23 m
        # downstream of any fishway" is bound to every section because no dataset of fishways
        # exists. It must be SHOWN and must never decide a water's colour: take 0 on every game
        # fish, read as a closure, painted every section in the province CLOSED once the spear
        # rule stopped doing it. A column, because the client decides an outcome from it.
        1 if r.standing else 0,
        json.dumps(list(r.species), separators=(",", ":")),
        json.dumps(list(r.species_except), separators=(",", ":")),
        _exempts(entry_id, r, zones or {}, rules_of or {}, closures, regions, blankets),
        r.take,
        None if r.may_target is None else int(r.may_target),
        json.dumps(conditions, separators=(",", ":"), sort_keys=True) or None,
        1 if uncertain else 0,
        # WHY it could not be placed — "reason: detail", as the licensing tables have it.
        unresolved,
        r.verbatim, r.extent_text or None, r.undrawn_part or None,
    )


def _see_column(entries: dict) -> dict[str, str]:
    """`entry_id -> the `see` column`: each pointer with the RELATION it bears (`see_relation`).

    A POINTER MUST LAND. A `see` naming an entry the corpus does not hold would render as a link to
    nothing — the reader is told "see X" and X is not there — so the build stops, as it does for an
    `exempts` that lifts no rule. A pointer that names no entry says why in `unresolved`."""
    from pipeline.regs.parsing.catalogue import see_relation
    out: dict[str, str] = {}
    dangling = []
    for eid, ce in sorted(entries.items()):
        if not ce.see:
            continue
        items = []
        for s in ce.see:
            missing = [t for t in s.entry_ids if t not in entries]
            if missing:
                dangling.append(f"{eid} -> {missing}")
                continue
            if s.entry_ids:
                items.append({"verbatim": s.verbatim, "entry_ids": list(s.entry_ids),
                              "relation": see_relation(ce, [entries[t] for t in s.entry_ids])})
            else:
                items.append({"verbatim": s.verbatim, "unresolved": s.unresolved})
        out[eid] = json.dumps(items, separators=(",", ":"), ensure_ascii=False)
    if dangling:
        raise SystemExit(f"see: {len(dangling)} pointer(s) name an entry the corpus does not hold "
                         f"(e.g. {dangling[:3]}) — point at an existing entry_id, or say why it "
                         f"names none in `unresolved`")
    return out


def regions_of_waters(registry, docs) -> dict[str, frozenset[str]]:
    """`{entry_id: regions}` — the regions each row's OWN waters (`matched`) lie in. What a row's
    printed lift of a blanket closure reaches in the other regions (`_equivalent_closures`). The
    arithmetic is the reach builder's (`outside.water_regions`: a section touching a region polygon
    is in it), called once per row — never a second copy of it."""
    from pipeline.atlas.reach.outside import water_regions
    return {ce.entry_id: frozenset(water_regions({"matched": list(ce.matched)}, registry))
            for ce in docs if ce.entry_id.startswith("r") and ce.matched}


def _book_region(zone_region: str) -> str:
    """A zone table's region as the rows name it: Zones 7A and 7B are Region 7's (`r7:` rows)."""
    return zone_region[:-1] if zone_region[-1:] in ("a", "b") else zone_region


def co_bound_regions(sets) -> dict[tuple[str, str], frozenset[str]]:
    """`{(entry_id, rule_id): zone regions}` — for every water row's rule, the regions whose OWN
    zone tables (`z<region>:`, never the province's) the reach run bound on a section the rule
    reaches THROUGH THE TRIBUTARY WALK (`via == "trib"`). Read off the run's rule sets
    (`intern_sets`), never derived again: the region a section takes its zone rules from is the
    run's decision (`section_home`). This is where a row's lift reaches another region through
    its tributaries (the Similkameen's "exempt from spring closure", printed with the tributary
    symbol, reaches 28 tributary sections that lie in Region 3), which the row's own matched
    water (`regions_of_waters`) never shows. Only the walk: an AREA row's sections (Bowron Lake
    Park's, in Zone 7A) are no water lying in another region, and its lift stays in its own."""
    out: dict[tuple[str, str], set[str]] = {}
    for rows in sets:
        zr = {_zone_region(e) for e, _, _ in rows if e.startswith("z") and not e.startswith("zp:")}
        if not zr:
            continue
        for e, r, via in rows:
            if e.startswith("r") and via == "trib":
                out.setdefault((e, r), set()).update(zr)
    return {k: frozenset(v) for k, v in out.items()}


def own_entry_regions(docs) -> dict[str, frozenset[str]]:
    """`{item_id: book regions}` — the regions in whose tables a water has an ENTRY OF ITS OWN: a
    row (`r<region>:`) matching the water with at least one rule about the water itself. A row
    whose every rule is `tributaries_only` (Region 6's and Region 7's "West Road River's
    tributaries") speaks for the tributaries, not for the water, and is not one."""
    out: dict[str, set[str]] = {}
    for ce in docs:
        if not ce.entry_id.startswith("r") or not ce.matched:
            continue
        if all(r.tributaries_only for r in ce.rules):
            continue
        for item in ce.matched:
            out.setdefault(item, set()).add(_book_region(_zone_region(ce.entry_id)))
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
    home = _book_region(_zone_region(ce.entry_id))
    mine = set(water.get(ce.entry_id, frozenset())) | set(co_bound.get((ce.entry_id, rule_id),
                                                                        frozenset()))
    theirs = set().union(*(own.get(i, frozenset()) for i in ce.matched)) - {home}
    return frozenset(r for r in mine if _book_region(r) not in theirs)


def _jsonl(path: Path):
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                yield json.loads(line)


def intern_sets(rows) -> tuple[dict[str, int], list[list[tuple[str, str, str]]]]:
    """`(section -> set_id, sets)` from the reach builder's (rule, section, scope) rows.

    The set is over (entry_id, rule_id, SCOPE), not just the rule, so a section reached as a
    tributary and a section the rule names directly land in different sets even when the
    rules are identical. That is the point: `scope` is how a reader is told the difference
    between "no fishing here" and "no fishing here, because this creek joins a closed
    stretch of the Skeena", and 98.6% of all bindings are tributary ones. It costs 249 sets.

    Sets are sorted before interning so the same corpus always produces the same ids — a
    bundle whose set numbering moved between builds would diff as though every section had
    changed.
    """
    by_section: dict[str, set[tuple[str, str, str]]] = {}
    for r in rows:
        by_section.setdefault(r["section_id"], set()).add(
            (r["entry_id"], r["rule_id"], r["scope"]))

    intern: dict[frozenset, int] = {}
    sets: list[list[tuple[str, str, str]]] = []
    section_set: dict[str, int] = {}
    for section in sorted(by_section):
        key = frozenset(by_section[section])
        got = intern.get(key)
        if got is None:
            got = intern[key] = len(sets)
            sets.append(sorted(key))
        section_set[section] = got
    return section_set, sets


def write(db: sqlite3.Connection, reaches: Path, entries_dir: Path, cov,
          build_dir: Path | None = None) -> None:
    """Write `entry`, `rule`, `section_ruleset`, `ruleset`, and every licensing table."""
    sections_file = reaches / "rule_section.jsonl"
    if not sections_file.exists():
        for t in ("entry", "rule", "section_ruleset", "ruleset"):
            cov.skip(t, f"no reach run at {reaches}")
        return

    # THE 88, FIRST. A rule nobody could place must never render as "no rules here" — it can
    # only ever raise "unknown" — so the flag has to be on the rule row itself, and it is
    # read from the reach builder's own failure table rather than guessed at from the
    # curation. `rule_unresolved` is the table that must never be silently short.
    unresolved = {(r["entry_id"], r["rule_id"]): f"{r['reason']}: {r['detail']}"
                  for r in _jsonl(reaches / "rule_unresolved.jsonl")}

    from pipeline.regs.parsing.catalogue import CatalogueEntry

    from pipeline.regs.parsing.io import _holds_entries, read_entryfile

    if build_dir is None:
        raise SystemExit("rules.write needs build_dir to resolve section handles")
    # THE ATLAS, READ ONCE, for the two things the rows need from it besides section handles:
    # the names a label gives a rule's place, and the sections B.C. does not govern.
    from pipeline.atlas.registry import load_registry
    from pipeline.deliver.bundle.place_names import PlaceNamer, area_names, resolved_labels
    registry = load_registry(str(Path(build_dir) / "registry.json"))
    namer = PlaceNamer(registry, resolved_labels(build_dir), area_names(build_dir))

    entry_rows, rule_rows = [], []
    ces = []
    docs = []
    # EVERY ENTRY SOURCE, and nothing else. A source is a directory of region files carrying
    # `entries` (see `io.entry_sources`); the DFO directory beside the catalogue holds region
    # files of another shape and is not one. Inside a source, a region file WITHOUT `entries` is
    # a defect, and stops the build (`require_entries`). Read through the corpus' one read point,
    # so an item id this registry folded into another names its item (`io.read_entryfile`).
    for d in sorted({p.parent for p in entries_dir.rglob("region-*.json")}):
        if not _holds_entries(d):
            continue
        for path in sorted(d.glob("region-*.json")):
            for e in read_entryfile(path, registry, require_entries=True).values():
                # Read through the model, so a key from a retired shape is refused, not read.
                docs.append((e, CatalogueEntry.model_validate(e)))
    # What an exemption may name: zone entries by slug, and every entry's rule ids.
    zones: dict[str, list[str]] = {}
    rules_of: dict[str, dict] = {}
    for _, ce in docs:
        rules_of[ce.entry_id] = {r.rule_id: r for r in ce.rules}
        if ce.entry_id.startswith("z"):
            zones.setdefault(ce.entry_id.split(":", 1)[1], []).append(ce.entry_id)
    entries_by_id = {ce.entry_id: ce for _, ce in docs}
    closures = zone_closures([ce for _, ce in docs])
    blankets = blanket_closures([ce for _, ce in docs])
    water_regions = regions_of_waters(registry, [ce for _, ce in docs])
    # One pass over the bindings (149 M rows on the full corpus), noting every rule it names. FIRST,
    # before any rule row: a row's lift carries into the regions whose zone tables the run bound
    # beside it (`co_bound_regions`), which only the sets say.
    bound_rules: set[tuple[str, str]] = set()

    def _noting(rows):
        for r in rows:
            bound_rules.add((r["entry_id"], r["rule_id"]))
            yield r

    section_set, sets = intern_sets(_noting(_jsonl(sections_file)))
    co_bound = co_bound_regions(sets)
    own = own_entry_regions([ce for _, ce in docs])
    see_of = _see_column(entries_by_id)
    for e, ce in docs:
        ces.append(ce)
        matched = list(ce.matched)
        entry_rows.append((
            ce.entry_id,
            # The first match, or nothing. An entry that never matched a water keeps its
            # rules and its text and carries a null item — the app shows "we have a rule
            # for a water we cannot place" rather than dropping it (77 of these).
            matched[0] if matched else None,
            ce.display_name or ce.name,
            # What the page printed, kept whole — see the note in schema.sql.
            ce.name,
            ce.regs_verbatim,
            json.dumps(list(ce.symbols), separators=(",", ":")),
            json.dumps(_mus_of(ce.entry_id), separators=(",", ":")),
            json.dumps(list(ce.source_pages), separators=(",", ":")),
            ce.scope_note or None,
            json.dumps(e.get("extents") or [], separators=(",", ":")),
            # EVERY water matched; `item_id` above is only the first.
            json.dumps(matched, separators=(",", ":")),
            see_of.get(ce.entry_id),
        ))
        # A rule may name another in its entry (`suspended_while`), and its label says what
        # that rule is in words — so each label is built with its siblings to hand.
        siblings = {r.rule_id: r for r in ce.rules}
        place_of = namer.for_entry(ce.matched)
        for r in e.get("rules") or []:
            k = (e["entry_id"], r.get("rule_id"))
            rule_rows.append(_rule_row(e["entry_id"], r, k in unresolved, siblings, zones,
                                       rules_of, unresolved=unresolved.get(k),
                                       place_of=place_of, entries=entries_by_id,
                                       closures=closures,
                                       regions=equivalent_regions(
                                           ce, r.get("rule_id"), water_regions, co_bound, own),
                                       blankets=blankets))

    # NAMED, not positional. A `pages` column was added to the schema while this line kept
    # seven placeholders, and nothing caught it until 90 seconds into a province-wide rebuild
    # — which then wrote a 42 MB bundle with zero entries in it. Naming the columns makes that
    # failure impossible rather than merely tested.
    db.executemany("INSERT INTO entry (entry_id, item_id, name, full_name, verbatim, symbols,"
                   "                   mus, pages, scope_note, extents, matched, see)"
                   " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                   entry_rows)
    cov.filled("entry", len(entry_rows))
    # COLUMNS NAMED, for the third time and the same reason. This was eleven positional
    # placeholders, and adding `limits` to the schema made it eleven values for twelve
    # columns — the fault that once shipped a 42 MB bundle with no entries in it.
    db.executemany("INSERT INTO rule (entry_id, rule_id, type, family, dimension, label, parts,"
                   "                  scope, when_, while_, standing, species, species_except,"
                   "                  exempts, take, may_target, conditions, uncertain, unresolved,"
                   "                  verbatim, extent_text, undrawn_part) "
                   "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rule_rows)
    cov.filled("rule", len(rule_rows))

    # THE REACH RUN MUST BE THIS CORPUS'S. A run built before a rule was removed still binds it,
    # and `ruleset` then names a rule the `rule` table does not have — measured on the bundle
    # built after the licensing rules left: 144 such rules, 31,354 ruleset rows, in 2,347 of the
    # 2,375 sets. The reader's JOIN drops such a row without a word, so the check is here: every
    # rule the run placed exists, and every rule that exists was placed or explained.
    placed_rules = bound_rules | set(unresolved)
    have_rules = {(r[0], r[1]) for r in rule_rows}
    gone, never = sorted(placed_rules - have_rules), sorted(have_rules - placed_rules)
    if gone or never:
        raise SystemExit(
            f"rules: the reach run at {reaches} is not this corpus's — it binds {len(gone):,} "
            f"rule(s) the corpus no longer has (e.g. {gone[:3]}) and never saw {len(never):,} it "
            f"does have (e.g. {never[:3]}). Re-run the reach builder:\n"
            f"    python -m pipeline.atlas.reach.cli --build <atlas> --out {reaches}")

    # The section is named by its HANDLE here, as everywhere else in the bundle. A rule bound
    # to a section the handle table does not know means the reach run and the atlas are not
    # the same build — which would silently bind rules to the wrong water, so it stops here.
    from pipeline.common.section_handles import read as _read_handles

    _, sid = _read_handles(build_dir)
    _unknown = [s for s in section_set if s not in sid]
    if _unknown:
        raise SystemExit(
            f"section_ruleset: {len(_unknown):,} bound sections are not in the handle table "
            f"(e.g. {_unknown[:3]}) — the reach run and section_handles.txt disagree")
    db.executemany("INSERT INTO section_ruleset (sid, set_id) VALUES (?,?)",
                   [(sid[k], v) for k, v in section_set.items()])
    cov.filled("section_ruleset", len(section_set))
    db.executemany("INSERT INTO ruleset VALUES (?,?,?,?)",
                   ((i, e, r, s) for i, rows in enumerate(sets) for e, r, s in rows))
    cov.filled("ruleset", sum(len(s) for s in sets))

    # AND AGAIN AGAINST THE ROWS WRITTEN, not the input they came from: a ruleset row naming a
    # rule the bundle does not hold is a regulation that exists on the map and nowhere else.
    orphan = db.execute(
        "SELECT rs.entry_id, rs.rule_id FROM ruleset rs LEFT JOIN rule r "
        "ON r.entry_id = rs.entry_id AND r.rule_id = rs.rule_id "
        "WHERE r.rule_id IS NULL LIMIT 5").fetchall()
    if orphan:
        raise SystemExit(f"ruleset names rules that are not in `rule`: {orphan}")

    # WATER B.C. DOES NOT GOVERN and WATER THE BOOK CALLS TIDAL: the two sets the reach builder
    # took out of every binding, READ FROM THE RUN (`outside_bc.jsonl`, `tidal.jsonl`) — never
    # derived again here — and written so a reader can say "outside B.C." / "tidal water" instead
    # of "no rules". Written before the licensing, whose province-wide requirements stop at the
    # `tidal` table's sections (`licensing.write`).
    from pipeline.atlas.reach.io import OUTSIDE_TABLE, TIDAL_TABLE, read_table
    outside = run_rows(read_table(reaches, OUTSIDE_TABLE), sid, OUTSIDE_TABLE)
    db.executemany("INSERT INTO outside_bc (sid) VALUES (?)", [(x["sid"],) for x in outside])
    cov.filled("outside_bc", len(outside))
    tidal = [(x["sid"], x["entry_id"]) for x in run_rows(read_table(reaches, TIDAL_TABLE), sid,
                                                          TIDAL_TABLE)]
    db.executemany("INSERT INTO tidal (sid, entry_id) VALUES (?,?)", tidal)
    cov.filled("tidal", len(tidal))
    own = {s: e for s, e in tidal}
    if set(own.values()) - {ce.entry_id for ce in ces if getattr(ce, "tidal", False)}:
        raise SystemExit(f"tidal: the reach run names tidal rows the corpus does not mark tidal "
                         f"({sorted(set(own.values()))}) — re-run the reach builder")

    # Licensing: the other half of each entry, placed by the same reach run.
    from pipeline.deliver.bundle import licensing as _licensing
    _licensing.write(db, reaches, ces, cov, sid, registry)

    # THE PROOF THAT NOTHING BINDS OUTSIDE B.C., against the ROWS JUST WRITTEN — a reach run from
    # before the subtraction binds 181 such sections, and must not ship.
    bound_outside = {t: db.execute(f"SELECT COUNT(*) FROM outside_bc o JOIN {t} t "
                                   f"ON t.sid = o.sid").fetchone()[0]
                     for t in ("section_ruleset", "section_licensing")}
    if any(bound_outside.values()):
        raise SystemExit(
            f"outside_bc: sections outside British Columbia carry regulation sets "
            f"({bound_outside}) — the reach run predates the border subtraction, or the atlas "
            f"changed under it. Re-run the reach builder:\n"
            f"    python -m pipeline.atlas.reach.cli --build <atlas> --out {reaches}")
    # THE PROOF THAT NO OTHER ROW BINDS TIDAL WATER (Nitinat Lake): the reach builder takes it
    # out of every other row (`reach.outside.tidal_owner`); a reach run from before that — or a
    # tidal row with a rule that is not a note — must not ship.
    foreign = [(s, e, r) for s, e, r in db.execute(
        "SELECT t.sid, rs.entry_id, rs.rule_id FROM tidal t JOIN section_ruleset sr ON sr.sid = t.sid "
        "JOIN ruleset rs ON rs.set_id = sr.set_id") if e != own[s]]
    licensed = db.execute("SELECT COUNT(*) FROM tidal t JOIN section_licensing l "
                          "ON l.sid = t.sid").fetchone()[0]
    if foreign or licensed:
        raise SystemExit(
            f"tidal: tidal sections carry provincial regulation — rules of other rows "
            f"{foreign[:5]} ({len(foreign)}), licensing sets on {licensed} section(s). Tidal "
            f"water takes only its own row's note. Re-run the reach builder:\n"
            f"    python -m pipeline.atlas.reach.cli --build <atlas> --out {reaches}")
    # HOW SURE WE ARE THAT STEELHEAD ARE HERE (`section_steelhead`), and WHERE A RAINBOW OVER 50 CM
    # IS A STEELHEAD (p.80, `steelhead_water`). Both from the reach run's `steelhead_presence`
    # (`pipeline.atlas.reach.steelhead`, user rulings 2026-10-01/02): `known` is every water a rule
    # of a steelhead row binds, and every water on the curated known-steelhead list (a presence
    # indicator: it binds no rule); `possible` every other stream the provincial steelhead rules
    # bind. The definition holds on the BOOK's flowing known sections only — never a lake, never
    # "possible", never from the list. A flagged row the run placed nowhere would state the
    # definition nowhere, so it stops the build; so does a run that predates the table, or
    # disagrees with the corpus or the curated list.
    write_steelhead_presence(db, reaches, ces, sid, cov, registry)
    # THE REGION A STRADDLING SECTION TAKES ITS ZONE RULES FROM (`section_home`): the atlas's own
    # `region_home.json` (`registry.regions.write_homes`), by handle — the fact that decided which
    # region's table the reach run bound here, so a reader can say "this piece takes Region 3's".
    from pipeline.atlas.registry.regions import read_homes
    homes = sorted((sid[h], r) for h, r in read_homes(build_dir).items() if h in sid)
    db.executemany("INSERT INTO section_home (sid, region) VALUES (?,?)", homes)
    cov.filled("section_home", len(homes))
    if namer.unnamed:
        print(f"     labels: {len(namer.unnamed)} cut-point(s) have no book name, so the rules "
              f"on them name no place:")
        for s_, why in sorted(namer.unnamed)[:20]:
            print(f"       {s_}: {why}")

    # COUNTED FROM THE SET, NOT FROM A TUPLE INDEX. This read `r[8]` — which is
    # `json.dumps(species)`, a string that is never empty ("[]" at minimum) and therefore
    # always truthy. Every build reported EVERY rule as uncertain: 3,422 of 3,422, a number
    # so obviously wrong it read as normal. The column itself was always right (256), so
    # nothing downstream was affected and nothing failed — only the line a person reads to
    # decide whether a build is healthy. The INSERT below names its columns for exactly this
    # reason; the summary went positional and drifted the moment a field moved.
    n_uncertain = len(set(unresolved) & {(r[0], r[1]) for r in rule_rows})
    print(f"     rules: {len(entry_rows):,} entries · {len(rule_rows):,} rules "
          f"({n_uncertain} uncertain) · {len(section_set):,} sections carry one, "
          f"sharing {len(sets):,} distinct sets")


def write_steelhead_presence(db, reaches, ces, sid, cov, registry=None) -> None:
    """`steelhead_known`, `steelhead_set` and `steelhead_source` from the reach run's
    `steelhead_presence` (`pipeline.atlas.reach.steelhead`), checked against the corpus (every row
    flagged `anadromous_rainbow` was placed, and only steelhead rows were) and against itself: the
    view `section_steelhead` must give every section exactly the code the run gave it,
    `steelhead_water` exactly the run's `anadromous` sections, and `section_steelhead_rules`
    exactly the run's `rules` sections (where the provincial steelhead set applies)."""
    import json as _json
    from pipeline.atlas.reach.io import STEELHEAD_TABLE
    from pipeline.atlas.reach.steelhead import (CURATED_LIST, KNOWN, POSSIBLE, fingerprint,
                                                load_list, steelhead_row)
    CODE = {KNOWN: 1, POSSIBLE: 2}
    path = Path(reaches) / f"{STEELHEAD_TABLE}.jsonl"
    report = Path(reaches) / "report.json"
    if not path.exists() or not report.exists():
        raise SystemExit(
            f"{reaches} has no `{STEELHEAD_TABLE}` — the reach run predates it. Re-run the reach "
            f"builder:\n    python -m pipeline.atlas.reach.cli --build <atlas> --out {reaches}")
    sh_report = _json.loads(report.read_text(encoding="utf-8")).get("steelhead") or {}
    by_entry = sh_report.get("by_entry") or {}
    flagged = {ce.entry_id for ce in ces if ce.anadromous_rainbow}
    rows = {ce.entry_id for ce in ces if steelhead_row(ce)}
    placed = {e for e, n in by_entry.items() if (n.get("sections") or 0) > 0}
    # THE CURATED LIST the run was given must be the list on disk now (its fingerprint).
    want, ran = fingerprint(load_list()), sh_report.get("list_fingerprint")
    if want != ran:
        raise SystemExit(
            f"steelhead: the reach run was made with curated known-steelhead list {ran!r}, not the "
            f"list on disk ({want}) — re-run the reach builder:\n    python -m "
            f"pipeline.atlas.reach.cli --build <atlas> --out {reaches}")
    if flagged - placed or placed - rows:
        raise SystemExit(
            f"steelhead_water: the reach run and the corpus disagree — flagged rows placing no "
            f"section {sorted(flagged - placed)[:5]}, placed rows that are no steelhead row "
            f"{sorted(placed - rows)[:5]}. Re-run the reach builder.")
    code: dict[int, int] = {}
    anadromous: set[int] = set()
    #: WHERE STEELHEAD RULES APPLY (`Presence.rules_apply`: the provincial set binds it) — the
    #: run's answer, carried as `steelhead_known.rules` / `steelhead_set.rules` and proved below.
    applies: set[int] = set()
    kind: dict[int, str] = {}
    source: dict[int, set[str]] = {}
    unknown = []
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            r = _json.loads(line)
            s = sid.get(r["section_id"])
            if s is None:
                unknown.append(r["section_id"])
                continue
            if r["steelhead"] not in CODE:
                raise SystemExit(f"steelhead_presence: {r['steelhead']!r} on {r['section_id']}")
            code[s] = CODE[r["steelhead"]]
            kind[s] = r["kind"]
            if r.get("rules"):
                applies.add(s)
            if r.get("anadromous"):
                if s not in applies:
                    raise SystemExit(f"steelhead_presence: anadromous on {r['section_id']}, "
                                     f"where no steelhead rule applies")
                # known by the book or by the curated list (user ruling 2026-10-03)
                if r["steelhead"] != KNOWN or not (r.get("regulations") or r.get("listed")):
                    raise SystemExit(f"steelhead_presence: anadromous on {r['section_id']}, "
                                     f"which is not known steelhead water")
                anadromous.add(s)
            if r["steelhead"] == KNOWN:
                got = source.setdefault(s, set())
                if r.get("regulations"):
                    got.add(r["entry_id"])
                if r.get("listed"):
                    got.add(CURATED_LIST)
    if unknown:
        raise SystemExit(f"steelhead_presence: {len(unknown):,} sections are not in the handle "
                         f"table (e.g. {unknown[:3]}) — the reach run and the atlas disagree")
    # PER SECTION: every known stream, every steelhead-water section, and any known section with
    # no rule set. Every other section's code is its RULE SET's: one code (or none) per set; a set
    # whose sections disagree only because some are known keeps those known ones per section.
    set_of = dict(db.execute("SELECT sid, set_id FROM section_ruleset"))
    loose = sorted(s for s, c in code.items() if c != 1 and s not in set_of)
    if loose:
        raise SystemExit(f"steelhead_presence: {len(loose)} section(s) with a code and no rule set "
                         f"(e.g. {loose[:3]}) — the code cannot be carried by its set")
    own = {s for s, c in code.items() if c == 1 and (kind[s] == "stream" or s in anadromous
                                                     or s not in set_of)}
    # A SECTION'S VALUE is (code, steelhead rules apply); a set carries one value or none.
    value = lambda s: None if s not in code else (code[s], int(s in applies))     # noqa: E731
    per_set: dict[int, set] = {}
    for s, k in set_of.items():
        if s not in own:
            per_set.setdefault(k, set()).add(value(s))
    mixed = {k for k, v in per_set.items() if len(v) > 1}
    if mixed:
        own |= {s for s, k in set_of.items() if k in mixed and code.get(s) == 1}
        per_set = {}
        for s, k in set_of.items():
            if s not in own:
                per_set.setdefault(k, set()).add(value(s))
        still = sorted(k for k, v in per_set.items() if len(v) > 1)
        if still:
            raise SystemExit(
                f"steelhead_presence: {len(still)} rule set(s) whose sections carry different "
                f"codes (e.g. set {still[0]}: {sorted(map(str, per_set[still[0]]))}) — the code is "
                f"not a fact of the set; store it per section")
    set_rows = sorted((k, *v.pop()) for k, v in per_set.items() if None not in v)
    db.executemany("INSERT INTO steelhead_known (sid, anadromous, rules) VALUES (?,?,?)",
                   [(s, int(s in anadromous), int(s in applies)) for s in sorted(own)])
    cov.filled("steelhead_known", len(own))
    db.executemany("INSERT INTO steelhead_set (set_id, code, rules) VALUES (?,?,?)", set_rows)
    cov.filled("steelhead_set", len(set_rows))
    # why each NAMED water's steelhead is known (per water, not per section)
    ord_of: dict[int, list[int]] = {}
    for o, s in db.execute("SELECT ord, sid FROM item_section"):
        ord_of.setdefault(s, []).append(o)
    src = sorted({(o, e) for s, es in source.items() for e in es for o in ord_of.get(s, ())})
    db.executemany("INSERT INTO steelhead_source (ord, entry_id) VALUES (?,?)", src)
    cov.filled("steelhead_source", len(src))
    got = dict(db.execute("SELECT sid, code FROM section_steelhead"))
    if got != code:
        bad = sorted(set(got.items()) ^ set(code.items()))
        raise SystemExit(f"section_steelhead: the stored form does not reproduce the reach run on "
                         f"{len(bad)} section(s) (e.g. {bad[:3]})")
    sw = {s for (s,) in db.execute("SELECT sid FROM steelhead_water")}
    if sw != anadromous:
        raise SystemExit(f"steelhead_water: the stored form does not reproduce the reach run's "
                         f"anadromous sections ({len(sw ^ anadromous)} differ)")
    sr = {s for (s,) in db.execute("SELECT sid FROM section_steelhead_rules")}
    if sr != applies:
        raise SystemExit(f"section_steelhead_rules: the stored form does not reproduce the reach "
                         f"run's sections where steelhead rules apply ({len(sr ^ applies)} differ)")


def run_rows(rows: list[dict], sid: dict[str, int], what: str) -> list[dict]:
    """A reach-run table keyed by handle: every `section_id` becomes `sid`. REFUSED when the run
    names a section this atlas does not have — the run and the atlas are then different builds."""
    out, unknown = [], []
    for r in rows:
        h = sid.get(r["section_id"])
        if h is None:
            unknown.append(r["section_id"])
            continue
        out.append({**{k: v for k, v in r.items() if k != "section_id"}, "sid": h})
    if unknown:
        raise SystemExit(f"{what}: {len(unknown):,} section(s) are not in the handle table (e.g. "
                         f"{unknown[:3]}) — the reach run and the atlas disagree")
    return sorted(out, key=lambda r: r["sid"])
