"""The rule catalogue — 14 types, their conditions, and what makes two rules comparable.

Replaces hand-written `Rule.details` with a TYPE plus named CONDITIONS. The label is generated
from those (see `label()`), so it cannot drift from the numbers the way the prose did:
`r6:bennett_lake` said "tiered size limit" while its own verbatim said "only 1 over 90 cm, none
between 60 cm and 90 cm".

Spec: pipeline/docs/17-rule-catalogue.md. Source text: data/curated/regulations/reference/.

THE ONE INVARIANT: a type boundary is a wall the override cannot cross. Two rules that could ever
displace one another must share a type and differ only in `dimension`; two rules that never compete
must not. `z4:bass_closed` (closure) and `r4:wasa_lake` (harvest) are one subject at two values, and
filing them apart is why the override never fired.
"""

from __future__ import annotations

import re

from enum import Enum
from typing import Annotated, List, Literal, Optional, Union


from pydantic import BaseModel, ConfigDict, Field, model_serializer, model_validator


class RuleType(str, Enum):
    retention_limit = "retention_limit"
    stop_fishing_after_quota = "stop_fishing_after_quota"
    bait_restriction = "bait_restriction"
    tackle_restriction = "tackle_restriction"
    method_rule = "method_rule"
    vessel_rule = "vessel_rule"
    #: `angling_from_vessel_prohibited` WAS HERE: "No angling from boats" is HOW you may fish, not
    #: what your boat may do — a `method_rule` banning `angling` `when: {angler: in_boat}` (or
    #: `in_powered_boat`). As a vessel type it could not meet the province's angling allow on the
    #: ladder at all. Refused on load, like `document_required`.
    navigation_duty = "navigation_duty"
    #: A CLOSURE FOR ONE KIND OF ANGLER — "Angling prohibited for non-guided non-resident aliens
    #: on Saturdays and Sundays". It was `access_permission` + `permitted: false`, a polarity bit
    #: on a type that also held permits and reciprocity. It is its own type, not `retention_limit`
    #: + a who, because the override ladder keys on (type, dimension) and ignores who: filed as a
    #: retention limit, a section-scoped alien-only closure would share a key with — and could
    #: displace — a zone quota that binds everyone. `document_required` and `access_permission`
    #: are GONE (refused on load); licensing lives on `CatalogueEntry.licensing`.
    angler_closure = "angler_closure"
    handling_rule = "handling_rule"
    hazard = "hazard"
    advisory = "advisory"
    program_membership = "program_membership"
    facility = "facility"


class Method(str, Enum):
    """`sport fishing` is DEFINED as angling, spear fishing, set lining and crayfish trapping."""
    angling = "angling"
    set_lining = "set_lining"
    spear_fishing = "spear_fishing"
    crayfish_trapping = "crayfish_trapping"
    ice_fishing = "ice_fishing"
    netting = "netting"
    snagging = "snagging"
    #: `chumming` had no value here, so `method: "other"` plus `reason: "chumming"` carried it —
    #: the prohibited act's own NAME in a free-text field.
    chumming = "chumming"
    #: `downrigger` AND `light` WERE MEMBERS HERE AND ARE NOT ANY MORE.
    #:
    #: The goal was one vocabulary for `while`, and that is met a better way (below). What putting
    #: a device in `method` cost was a FABRICATED PERMISSION. A "provided that" sentence becomes
    #: two rules — one allowing the means, one holding the condition — and run through that
    #: template both of these produce `method: {allow: [...]}`:
    #:
    #:   zp:allowable_methods   "angle with a downrigger, PROVIDED the line is attached by a
    #:                           quick-release"        — an ALLOWABLE list. The grant is real.
    #:   zp:terminal_tackle     "Use a light … UNLESS submerged and within 1 m of the hook"
    #:                          — from a list headed "It is UNLAWFUL to". It grants nothing.
    #:
    #: "Provided" and "unless" are opposite polarity through one template, so a Region 6 lake
    #: answered "what may I fish with here" with `light` — the one piece of tackle the province
    #: forbids outright. That is `{method: "ice_fishing", permitted: true}` on a hut-removal
    #: warning, rebuilt on a new field; `Conduct` below names that failure.
    #:
    #: `while` DRAWS FROM METHOD MEMBERS (means) AND TWO DEVICES (`downrigger`, `light`) — see
    #: `WHILE_TOKENS`. So `while: ["downrigger"]` has a referent without `downrigger` being a way
    #: of fishing, and the only thing lost is the ability to write the permission the book never
    #: printed.
    other = "other"


class Period(str, Enum):
    daily = "daily"
    possession = "possession"
    annual = "annual"          # the LICENCE year, Apr 1 - Mar 31
    monthly = "monthly"        # defined provincially; unused in this corpus


class WaterKind(str, Enum):
    stream = "stream"
    lake = "lake"


class ClosureKind(str, Enum):
    """The season a region's blanket closure is NAMED for — "Spring closure", "Summer closure",
    "the existing winter/spring closure regulation". A name, not a date range: see
    `CatalogueRule.closure_kind`."""
    spring = "spring"
    summer = "summer"
    winter = "winter"


#: "spring closure", "Summer closure:", "(spring closure)", "spring stream closure",
#: "winter/spring closure" (both).
_PRINTED_CLOSURE_KIND = re.compile(
    r"\b(spring|summer|winter)(?:\s*/\s*(spring|summer|winter))?\s+(?:stream\s+)?closure\b", re.I)


def printed_closure_kinds(text: str) -> frozenset:
    """The kinds of closure a sentence NAMES by their season word ("Exempt from spring closure",
    "Summer closure: No Fishing …"). Empty when it names none — "EXEMPT from the Apr 1-June 14
    closure" names dates, not a kind, and "Mainstem open all year" names nothing."""
    out = set()
    for m in _PRINTED_CLOSURE_KIND.finditer(text or ""):
        out.update(g.lower() for g in m.groups() if g)
    return frozenset(out)


class Origin(str, Enum):
    hatchery = "hatchery"
    wild = "wild"


class ChannelSide(str, Enum):
    """One half of a river's channel, lengthwise, by compass side (`CatalogueRule.side`)."""
    north = "north"
    south = "south"
    east = "east"
    west = "west"

    @property
    def opposite(self) -> "ChannelSide":
        return {ChannelSide.north: ChannelSide.south, ChannelSide.south: ChannelSide.north,
                ChannelSide.east: ChannelSide.west, ChannelSide.west: ChannelSide.east}[self]


#: "on the west half of river" — a rule printed for one half of a channel (`CatalogueRule.side`).
#: A lake's half ("south half only", Premier Lake) is a part of the lake, `undrawn_part`.
HALF_OF_CHANNEL = re.compile(r"\b(north|south|east|west)\s+half\s+of\s+(?:the\s+)?"
                             r"(?:river|stream|creek|channel)\b", re.I)


class VesselAspect(str, Enum):
    propulsion = "propulsion"
    speed = "speed"
    towing = "towing"


class PropulsionLevel(str, Enum):
    """ONE ORDERED SCALE, strictest first. `r2:sasamat_lake@2-8` holds two of these seasonally,
    which is the proof they are one field and not four types."""
    none = "none"                    # no vessels at all
    unpowered = "unpowered"          # no powered boats
    #: "Electric motor only: you may use only battery-powered electric motors - MAX 7.5 kW."
    #: The 7.5 is part of the definition, so the 161 rules repeating it are restating the page,
    #: not setting a per-water cap. Store max_power_kw only when a water differs.
    electric_only = "electric_only"
    power_capped = "power_capped"


class Document(str, Enum):
    basic_licence = "basic_licence"
    steelhead_stamp = "steelhead_stamp"
    salmon_stamp = "salmon_stamp"
    kootenay_rainbow_stamp = "kootenay_rainbow_stamp"
    shuswap_char_stamp = "shuswap_char_stamp"
    shuswap_rainbow_stamp = "shuswap_rainbow_stamp"
    white_sturgeon_licence = "white_sturgeon_licence"
    classified_waters_licence = "classified_waters_licence"
    national_park_permit = "national_park_permit"
    angling_guide_licence = "angling_guide_licence"
    #: "A person commits an offence if they do not hold a valid angling guide OR ASSISTANT angling
    #: guide licence" — two documents, either of which satisfies; it was folded into one.
    assistant_angling_guide_licence = "assistant_angling_guide_licence"
    #: A third-party permit: "A permit is required for fishing on all waters within the Creston
    #: Valley Wildlife Management Area". It was an `access_permission` with a free-text grantor.
    creston_valley_wma_permit = "creston_valley_wma_permit"
    #: RECIPROCITY: "B.C. and Yukon angling licences are valid on all parts of Morley Lake".
    yukon_angling_licence = "yukon_angling_licence"
    #: A landowner's permission, named: "angling access requires permission of the Creston Valley
    #: Rod & Gun Club". Each grantor is its own member, added by reviewed change.
    creston_valley_rod_and_gun_club_permission = "creston_valley_rod_and_gun_club_permission"


class Obligation(str, Enum):
    """Law or advice. `z7b:ice_fishing_huts_notice` carries BOTH in one sentence — huts *should*
    show contact details, and failing to remove one *is an offence*. Rendering advice as law is the
    mirror of rendering law as advice, and both are in this corpus."""
    must = "must"
    should = "should"


#: Dash variants the extraction emits interchangeably; all mean "-" for comparison.
_DASH = dict.fromkeys(map(ord, "‐‑‒–—―−"), "-")


def squash(text: str) -> str:
    """Normalise away what carries no meaning when comparing a quote to its source: EMPHASIS,
    bullets, blockquote markers, dash variants, whitespace, case.

    Markdown is OUR annotation, added by extraction — 629 of 1393 batch rows carry `**`. The
    printed regulation has no asterisks in it, so a model quoting the sentence it can see writes
    it without them, and a raw substring check then calls a perfect quote a fabrication. It called
    700 of them that. What is STORED is still the batch's own text, byte for byte; only the
    comparison is normalised."""
    t = (text or "").translate(_DASH).replace("*", "")
    t = re.sub(r"(?m)^\s*[>|]\s?", " ", t)
    t = re.sub(r"(?m)^\s*[-•]\s+", " ", t)
    return re.sub(r"\s+", " ", t).strip().lower()


#: THE BOOK'S SPECIES — AND NOTHING ELSE (user ruling 2026-09-26). Page 86 ("Freshwater game fish
#: are defined as follows") prints the list, under four headings and an "OTHER":
#:
#:   TROUT      Rainbow Trout, Steelhead, Cutthroat Trout, Brown Trout
#:   CHAR       Dolly Varden, Bull Trout*, Lake Trout, Brook Trout
#:   WHITEFISH  Lake Whitefish, Mountain Whitefish
#:   BASS       Largemouth Bass, Smallmouth Bass
#:   OTHER      Kokanee, Arctic Grayling, Burbot (Ling), White Sturgeon, Black Crappie,
#:              Northern Pike, Yellow Perch, Walleye, Goldeye, Inconnu, Crayfish
#:
#:   "*Any bull trout that you catch and keep must be counted as part of your Dolly Varden quota."
#:
#: So in the regulations a bull trout IS a Dolly Varden: ONE fish, `DV`. `BT` was a second code for
#: it, and the ladder treated "Bull trout daily quota = 1" and Region 4's "1 bull trout (Dolly
#: Varden)" as statements about two fish. It is refused (`REFUSED_SPECIES`). The official table's
#: sub-species (westslope and coastal cutthroat), fish the book never lists (golden trout, arctic
#: char, splake, pygmy and round whitefish, bluegill, pumpkinseed, carp) and the CSV's "General"
#: rows are gone: no rule named them, and a code the book does not print is a code no reader can
#: check.
BOOK_FAMILIES: dict[str, tuple[str, ...]] = {
    "TROUT":     ("RB", "ST", "CT", "GB"),
    "CHAR":      ("DV", "LT", "EB"),
    "WHITEFISH": ("LW", "MW"),
    "BASS":      ("LMB", "SMB"),
    "OTHER":     ("KO", "GR", "BB", "WSG", "BCB", "NP", "YP", "WP", "GE", "IN", "CRA"),
}
#: Every fish a rule may name, in the book's order.
BOOK_SPECIES: tuple[str, ...] = tuple(c for fs in BOOK_FAMILIES.values() for c in fs)

#: THE GROUPS A RULE MAY NAME, and what each one covers. Stored as the word the book printed:
#: "Trout/char: 5" is ONE claim about trout and char, and nine codes would be nine claims that
#: merely coincide. `expand_species` turns a group back into members where a caller needs the set.
#:
#: "TROUT" IS NOT ONE OF THEM. Page 86: "trout/char: all regulations that apply to trout (as a
#: group) also apply to char unless char are specifically excluded." So the printed word "trout"
#: is `TROUT_CHAR` — and where its row or zone table MENTIONS CHAR APART (user ruling 2026-09-28,
#: `mentions_char_apart`), that is the exclusion: the row's "trout" lines are written `TROUT_CHAR`
#: with `species_except: [CHAR]` (`trout_scope_problems`). ONE representation for "trout", scoped
#: by its row; a TROUT-only code would be a second spelling of the same fish set. It is refused
#: (`REFUSED_SPECIES`); the book's TROUT heading lives on as a family (`BOOK_FAMILIES`), for
#: display.
SPECIES_GROUPS: dict[str, tuple[str, ...]] = {
    "TROUT_CHAR": BOOK_FAMILIES["TROUT"] + BOOK_FAMILIES["CHAR"],
    #: "char catch and release", Region 1's "you must release: All char (includes Dolly Varden)".
    #: The one group that NAMES its members (`NAMING_GROUPS`).
    "CHAR":       BOOK_FAMILIES["CHAR"],
    "WHITEFISH":  BOOK_FAMILIES["WHITEFISH"],
    "BASS":       BOOK_FAMILIES["BASS"],
}
#: The closed list — "Freshwater game fish are defined as follows". A rule that applies to
#: "everything" applies to THIS set, never the empty set.
SPECIES_GROUPS["ALL_GAME_FISH"] = BOOK_SPECIES

#: A GROUP THAT NAMES ITS FISH. Once "trout" swallows char (p.86), "char" is how the book names
#: char APART from trout: Region 1's "Trout: 4 … And you must release: All char (includes Dolly
#: Varden)" names the char it releases, and a lake's "Trout daily quota = 2" — a trout/char quota
#: by p.86 — must not reopen them. Read as a group, the zone's char release lost to the water's
#: group quota by place, and 56 Region 1 lakes would have let a char be kept.
NAMING_GROUPS = frozenset({"CHAR"})

#: THE SCOPE OF THE WORD "TROUT" (user ruling 2026-09-28): "trout" includes char UNLESS CHAR ARE
#: MENTIONED. p.86 says trout rules apply to char "unless char are specifically excluded", and the
#: book excludes them by naming char APART in the same row, or in the same zone table: Region 6's
#: box (p.49) prints "Trout/char: 5, but not more than … 3 Dolly Varden/bull trout and/or lake
#: trout combined, 1 trout from streams July 1-Oct 31. And you must release: … Trout under 30 cm
#: from any stream, Trout of any size from streams, Nov 1-June 30" — so "1 trout from streams",
#: "Trout under 30 cm" and "Trout of any size from streams" are about trout alone; Region 1's
#: "Trout: 4 … And you must release: … All char (includes Dolly Varden)" (p.13) likewise. A lake
#: row printing only "Trout daily quota = 2" mentions no char, and its 2 counts char too.
#:
#: A char is mentioned apart when the text names one ON ITS OWN: "char", Dolly Varden, bull trout,
#: lake trout, brook trout. The group word "trout/char" ("trout and char") is not such a mention —
#: it names char IN: Dodd Lake's "Wild trout/char daily quota = 2 (no wild trout over 40 cm)"
#: names no char apart, and its "no wild trout over 40 cm" holds for char too (user confirmation
#: 2026-09-28, "none over 40 cm like trout"). "Rainbow trout and char" names char apart (the trout
#: there is a rainbow).
_CHAR_NAMED = re.compile(r"\bchar\b|\bdolly\s+varden\b|\bbull\s+trout\b|\blake\s+trout\b"
                         r"|\bbrook\s+trout\b", re.I)
#: The group word, printed three ways.
_TROUT_GROUP = re.compile(r"\btrout\s*(?:/|\band\b|&|\bor\b)\s*char\b", re.I)
_TROUT_WORD = re.compile(r"\btrout\b", re.I)
#: A fish's own name before "trout" ("rainbow trout", "lake trout") — then "trout" is not the group.
_TROUT_KIND = re.compile(r"(?:rainbow|cutthroat|brown|lake|brook|bull|golden)[\s-]*$", re.I)


def _is_group_trout(text: str, at: int) -> bool:
    return not _TROUT_KIND.search(text[:at])


def mentions_char_apart(text: str) -> bool:
    """Does this row (or zone table) NAME A CHAR ON ITS OWN — "char", Dolly Varden, bull trout,
    lake trout, brook trout — outside the group word "trout/char"? Then its "trout" lines exclude
    char (see the note above `_CHAR_NAMED`)."""
    masked = text or ""
    for m in reversed(list(_TROUT_GROUP.finditer(masked))):
        if _is_group_trout(masked, m.start()):
            masked = masked[:m.start()] + " " * (m.end() - m.start()) + masked[m.end():]
    return bool(_CHAR_NAMED.search(masked))


def trout_word(text: str) -> Optional[str]:
    """The trout word a line prints: "trout/char" (the group word, char named in), "trout" (the
    bare word, whose scope its row decides), or None (no group trout word — "1 over 50 cm", or
    only a fish's own name, "rainbow trout")."""
    text = text or ""
    if any(_is_group_trout(text, m.start()) for m in _TROUT_GROUP.finditer(text)):
        return "trout/char"
    if any(_is_group_trout(text, m.start()) for m in _TROUT_WORD.finditer(text)):
        return "trout"
    return None


def trout_scope_problems(entry_id: str, regs_verbatim: str, rules) -> List[str]:
    """EVERY "TROUT" LINE IS SCOPED BY ITS ROW (user ruling 2026-09-28; `mentions_char_apart`).
    A `TROUT_CHAR` rule printing the bare word "trout" (itself, or the quota it is a clause
    `within`: Region 1's "1 over 50 cm" under "Trout: 4") carries `species_except: [CHAR]` exactly
    when its row or zone table names a char apart. A line printing "trout/char" names char in and
    never excludes them. A bare "trout" line never excludes one char alone (the book's exclusion
    is of char as a group, `CHAR`) — the seven Region 2 rows that once excluded only the bull trout
    their row gave its own limit are the general rule now."""
    apart = mentions_char_apart(regs_verbatim)
    by_id = {r.rule_id: r for r in rules}
    out: List[str] = []
    for r in rules:
        if "TROUT_CHAR" not in r.species:
            continue
        word, p, seen = trout_word(r.verbatim), r, {r.rule_id}
        while word is None and p.within and p.within in by_id and p.within not in seen:
            p = by_id[p.within]
            seen.add(p.rule_id)
            word = trout_word(p.verbatim)
        out_char = "CHAR" in r.species_except
        if word == "trout/char" and out_char:
            out.append(f"{r.rule_id}: prints 'trout/char', which names char in — it never "
                       f"carries species_except CHAR")
        if word != "trout":
            continue
        lone = sorted(c for c in r.species_except if c in BOOK_FAMILIES["CHAR"])
        if lone:
            out.append(f"{r.rule_id}: species_except {lone} — a 'trout' line excludes char as a "
                       f"group (CHAR) when its row mentions char apart, never one char")
        if apart and not out_char:
            out.append(f"{r.rule_id}: prints 'trout' and its row mentions char apart — its "
                       f"trout exclude char (p.86; user ruling 2026-09-28): species_except [CHAR]")
        elif not apart and out_char:
            out.append(f"{r.rule_id}: prints 'trout' and its row mentions no char apart — trout "
                       f"includes char (p.86): drop CHAR from species_except")
    return out

#: SUBJECTS THE BOOK NAMES THAT ARE NOT GAME FISH, so no fish code lies under them. Each is a word
#: the book prints in a rule, and each is OPEN — it has no member list, on purpose:
#:
#:   ALL_FIN_FISH       "any fish willfully or accidentally snagged must be released" and "release
#:                      all fin fish caught in your trap" (p.86 defines fish as "fin fish, shellfish
#:                      and crustaceans"): every fish, game or not — never crayfish, which the book
#:                      names beside fin fish ("fin fish AND crayfish").
#:   PROTECTED_SPECIES  "It is illegal to fish for … any of the fish listed below" (p.9): eleven
#:                      sculpins, sticklebacks, dace, suckers and lampreys, and Region 2 adds green
#:                      sturgeon (p.21). None is a game fish, so none has a code; the list is in
#:                      the rule's `verbatim`. (White Sturgeon's four populations are stated by the
#:                      regional closures that name `WSG`.)
#:   SALMON             Pacific salmon: federal, not on the provincial list, NOT in
#:                      ALL_GAME_FISH. The book names them (the salmon stamp, "no spear fishing of
#:                      Pacific salmon"); kokanee, a land-locked sockeye, is the game fish `KO`.
#:
#: Asked about one fish (a leaf of `BOOK_SPECIES`), `ALL_FIN_FISH` speaks for every one but
#: crayfish; `PROTECTED_SPECIES` and `SALMON` speak for none (`read.speaks_for`).
OPEN_SUBJECTS: tuple[str, ...] = ("ALL_FIN_FISH", "PROTECTED_SPECIES", "SALMON")
for _s in OPEN_SUBJECTS:
    SPECIES_GROUPS[_s] = ()

#: Every code a synopsis rule may name: the book's fish, its groups, and the open subjects. A rule
#: naming anything else is refused at validation rather than printing a raw code.
KNOWN_SPECIES = frozenset(set(BOOK_SPECIES) | set(SPECIES_GROUPS))

#: CODES THAT ARE REFUSED, each with what to write instead. The two the corpus used carry the
#: book's reason; every other unknown code is "not on the book's list (p.86)".
REFUSED_SPECIES: dict[str, str] = {
    "BT": "a bull trout is a Dolly Varden in the regulations (p.86: 'Any bull trout that you catch "
          "and keep must be counted as part of your Dolly Varden quota') — write DV",
    "TROUT": "trout includes char unless char are specifically excluded (p.86) — write "
             "TROUT_CHAR, with species_except [CHAR] when the row or zone table mentions char "
             "apart (a char named on its own: 'char', Dolly Varden/bull trout, lake trout, brook "
             "trout)",
    "SA": "the salmon subject is SALMON",
}

#: FEDERAL SALMON — NOT A SYNOPSIS VOCABULARY. The DFO salmon feed (`pipeline.regs.dfo_salmon`)
#: types its own pages into `CatalogueRule`s naming chinook, coho, sockeye, pink and chum (`SA`:
#: all salmon on a DFO page). A bare rule accepts them so that feed can be typed; a synopsis ENTRY
#: refuses them (`CatalogueEntry` — the book's list is p.86's, and a salmon rule in it is `SALMON`).
FEDERAL_SALMON = frozenset({"CH", "CO", "SK", "PK", "CM", "SA"})


def species_problems(codes, where: str = "species") -> List[str]:
    """The codes a synopsis rule may not name, each with what to write instead."""
    out = []
    for c in sorted(set(codes) - KNOWN_SPECIES):
        why = REFUSED_SPECIES.get(c) or ("not on the book's species list (p.86) — the fish are "
                                         f"{', '.join(BOOK_SPECIES)}; the groups "
                                         f"{', '.join(sorted(SPECIES_GROUPS))}")
        out.append(f"{where}: unknown species code {c!r} — {why}")
    return out


#: A SIZE THE BOOK PUTS IN THE DEFINITION, NOT IN A QUOTA. Page 86: "steelhead: a rainbow
#: trout longer than 50 cm in waters where anadromous rainbow trout are found." So a steelhead
#: under 50 cm does not exist, and a table that offers a number for one is describing a fish
#: nobody can catch — Region 2 printed "up to 50 cm: 4 / over 50 cm: 2" where only the 2 is
#: real. Held here rather than as a curated rule because no regional table states it: it is
#: what the word MEANS, everywhere in the book.
DEFINITIONAL_SIZE = {
    "ST": {"min_cm": 50,
           #: WHAT IT IS INSTEAD — recorded, but NOT applied when settling. The definition
           #: holds only "in waters where anadromous rainbow trout are found": steelhead are
           #: sea-going, so a table naming trout and not steelhead is describing a landlocked
           #: rainbow the 50 cm boundary says nothing about. Substituting globally also broke
           #: the invariant the table rests on — a verdict about steelhead came to be decided
           #: by a trout/char counter that is not on the steelhead row. Applying this needs a
           #: per-water "are there steelhead here" fact the corpus does not carry.
           "below": "RB",
           "applies_where": "anadromous rainbow trout are found",
           "says": "a steelhead is a rainbow trout longer than 50 cm, so there is no "
                   "such thing as a smaller one",
           "source": "fishing_synopsis.pdf \u00b7 2025-2027 \u00b7 page 86, Definitions"},
}


def expand_species(codes: List[str]) -> List[str]:
    """A species list with every group replaced by its members, de-duplicated, order preserved.
    Anything that is not a group passes through untouched.

    TRANSITIVELY, AND DOWN TO LEAVES. A group may hold a group. Expanding one level left `TROUT`
    unexpanded inside `TROUT_CHAR`, so a rule about trout did not register under a rule about
    trout and char; and `ALL_GAME_FISH` holds the individual fish but not the code `TROUT_CHAR`,
    so comparing sets that still held group codes said "all game fish does not cover trout and
    char" — and a river closed to every game fish reported a keep limit of 5 for trout. The
    group tables happen to be flat today, so one level would give the same answer; this does not
    depend on their staying that way.

    (There were two implementations of this. `table/subject.py` had the transitive one and this
    had the one-level one, and they disagreed on exactly one thing — see below — which is the
    kind of difference that is invisible until it is a wrong number on a page. This is the only
    one now.)

    AN EMPTY GROUP PASSES THROUGH. The open subjects (`OPEN_SUBJECTS`: `ALL_FIN_FISH`,
    `PROTECTED_SPECIES`, `SALMON`) are not memberships, and expanding them to `[]` erases the
    rule — "any fish willfully or accidentally snagged must be released immediately" would name
    nothing. The caller sees the claim that was actually made and decides what to do with it.
    """
    out: List[str] = []
    stack = list(reversed(codes))
    while stack:
        c = stack.pop()
        kids = SPECIES_GROUPS.get(c)
        if kids:
            stack.extend(reversed([k for k in kids]))
        elif c in SPECIES_GROUPS:            # a group that expands to nothing: the claim itself
            if c not in out:
                out.append(c)
        elif c not in out:
            out.append(c)
    return out


#: TIER ONE. Every type belongs to exactly one family; the reader sees these as sections.
#: THIS IS THE ONLY COPY. The bundle ships each rule's `family` (`rule.family`) so no client carries
#: the mapping; the app's `FAMILY_OF` mirror, and the test that compared the two, went with the
#: regulations integration (app/packages/core/src/regulations.ts is where it plugs back in).
_FAMILY = {
    RuleType.retention_limit: "retention",
    RuleType.stop_fishing_after_quota: "retention",
    RuleType.bait_restriction: "gear_and_method",
    RuleType.tackle_restriction: "gear_and_method",
    RuleType.method_rule: "gear_and_method",
    RuleType.vessel_rule: "vessel",
    RuleType.navigation_duty: "vessel",
    #: WHO MAY FISH HERE AT ALL. Not "retention": a closure to one kind of angler is not a limit
    #: on what anyone keeps, and filing it with the quotas is what let it displace them.
    RuleType.angler_closure: "access",
    RuleType.handling_rule: "conduct",
    RuleType.hazard: "information",
    RuleType.advisory: "information",
    RuleType.program_membership: "information",
    RuleType.facility: "information",
}

#: THE TYPES THAT COUNT FISH, and so the only ones a `period` (the clock the count runs on) means
#: anything on. "Stop fishing after your DAILY quota" is the other.
_COUNTED_TYPES = (RuleType.retention_limit, RuleType.stop_fishing_after_quota)


#: THE ZONE DEFAULTS A WATER MAY LIFT BY NAME (`Exempts.default_id`), each the slug of a zone
#: entry (`z<region>:<slug>`). A closed list, like `CONDUCT_ACTS`: a default_id outside it named
#: nothing a reader could find — `set_lining.r1b` (a rule id) sat here — and adding one is a
#: reviewed change. The bundle resolves each to the zone entry of the lifting rule's own region.
EXEMPTABLE_DEFAULTS = frozenset({
    "spring_stream_closure", "summer_stream_closure", "steelhead_stream_closure",
    "trout_char_winter_release", "bait_ban_streams", "single_barbless_hook",
})


class Exempts(BaseModel):
    """What this rule LIFTS. A field, not a type — an exemption takes the type of whatever it
    lifts, which is why `bait_restriction` carries `allowed: true` for the Fraser sturgeon rules.

    ONE OF TWO, never both:

      `default_id`  a ZONE DEFAULT by its slug — `spring_stream_closure` is `z3:spring_stream_closure`
                    on a Region 3 water. It lifts that zone entry's rules in the rule's own region,
                    never the rule's own entry (a rule that lifts itself deletes itself).
      `target`      ONE RULE by `rule_id`: in this entry, or in `entry_id` when the lifted rule is
                    another entry's ("EXEMPT from Columbia Lake's tributaries closure"). A rule id
                    is unique only within its entry (AGENTS 8), so the entry is named, not guessed.

    The bundle resolves each to exact ids and refuses one that names nothing
    (`pipeline.deliver.bundle.rules._exempts`)."""
    model_config = ConfigDict(frozen=True, extra="forbid")
    default_id: Optional[str] = Field(default=None, pattern=r"^[a-z0-9_]+$")
    target: Optional[str] = Field(default=None, pattern=r"^[a-z0-9_]+\.r\d+[a-z]?$")
    entry_id: Optional[str] = None
    note: str = ""

    @model_validator(mode="after")
    def _one_of(self) -> "Exempts":
        if bool(self.default_id) == bool(self.target):
            raise ValueError("exempts needs exactly one of `default_id` (a zone default) or "
                             "`target` (a rule id)")
        if self.entry_id and not self.target:
            raise ValueError("exempts: `entry_id` names the entry a `target` is in — it means "
                             "nothing beside a `default_id`")
        if self.default_id and self.default_id not in EXEMPTABLE_DEFAULTS:
            raise ValueError(f"exempts: default_id {self.default_id!r} is not a registered zone "
                             f"default ({sorted(EXEMPTABLE_DEFAULTS)}) — a rule of this or "
                             f"another entry is a `target`")
        return self


#: Month name -> number, and the last day of each. February is 29 ON PURPOSE: the book says
#: "February", which includes the 29th in the years it exists, and 28 would quietly shorten it.
_MONTHS = {m: i + 1 for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"])}
_LAST_DAY = {1: 31, 2: 29, 3: 31, 4: 30, 5: 31, 6: 30,
             7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}


def parse_clock(text: str) -> Optional["Clock"]:
    """A printed time -> a `Clock`. "21:00" and "21:00 hours" are the same instant spelled two
    ways and both sat in the corpus; "one hour after sunset" is not a clock time at all."""
    t = " ".join((text or "").split()).lower().replace(" hours", "")
    if not t:
        return None
    m = re.fullmatch(r"(\d{1,2}):(\d{2})", t)
    if m:
        return Clock(at=f"{int(m.group(1)):02d}:{m.group(2)}")
    m = re.fullmatch(r"(?:(one|an|\d+)\s*(hour|hours|minute|minutes|min)\s*)?"
                     r"(before|after)?\s*(sunrise|sunset)", t)
    if not m:
        return None
    n, unit, side, ev = m.groups()
    mins = 0
    if n:
        v = 1 if n in ("one", "an") else int(n)
        mins = v * (60 if unit.startswith("hour") else 1)
        if side == "before":
            mins = -mins
    return Clock(solar=Solar(ev), offset_min=mins)


def parse_date_range(text: str) -> Optional["DateRange"]:
    """One printed window -> a `DateRange`. Returns None where the text does not parse, so a
    caller fails loudly rather than inventing a season.

    Handles the four spellings the corpus uses: "Nov 1-Apr 30", "Nov 1 - Apr 30" (the same range,
    and 74 rules split between them), "May 1-31" (same month, end unqualified) and a bare month.
    """
    t = " ".join((text or "").split())
    if not t:
        return None
    parts = re.split(r"\s*(?:-|\u2013|\u2014|\bto\b)\s*", t, maxsplit=1)
    lo = _point(parts[0])
    if lo is None:
        return None
    if len(parts) == 1:
        if lo[1] is not None:
            return None                                  # a lone date, not a range
        return DateRange(from_month=lo[0], from_day=1,
                         to_month=lo[0], to_day=_LAST_DAY[lo[0]])
    hi = _point(parts[1])
    if hi is None:
        m = re.fullmatch(r"(\d{1,2})", parts[1].strip())  # "May 1-31": the month carries over
        if not m or lo[1] is None:
            return None
        hi = (lo[0], int(m.group(1)))
    if lo[1] is None or hi[1] is None:
        return None
    return DateRange(from_month=lo[0], from_day=lo[1], to_month=hi[0], to_day=hi[1])


def _point(text: str):
    """"Nov 1" -> (11, 1); "February" -> (2, None); anything else -> None."""
    t = text.strip().rstrip(",")
    m = re.fullmatch(r"([A-Za-z]+)\.?\s*(\d{1,2})", t)
    if m:
        mo = _month(m.group(1))
        return (mo, int(m.group(2))) if mo else None
    m = re.fullmatch(r"([A-Za-z]+)\.?", t)
    if m:
        mo = _month(m.group(1))
        return (mo, None) if mo else None
    return None


def _month(name: str) -> Optional[int]:
    n = name.strip().lower()
    return _MONTHS.get("sep" if n.startswith("sept") else n[:3])


def _day_index(month: int, day: int) -> int:
    return sum(_LAST_DAY[m] for m in range(1, month)) + day


def complement(ranges: List["DateRange"]) -> List["DateRange"]:
    """THE DAYS THESE RANGES DO NOT COVER, on a circular year.

    This is what retires `windows_are: "excepts"`. "Open June 16-Apr 30 each year" is a CLOSURE
    whose printed dates are the days it does not apply; its complement, May 1 - June 15, is the
    closure's own season and needs no flag to read correctly.
    """
    covered = set()
    for r in ranges:
        a, b = _day_index(r.from_month, r.from_day), _day_index(r.to_month, r.to_day)
        days = range(a, b + 1) if a <= b else list(range(a, 367)) + list(range(1, b + 1))
        covered.update(days)
    total = sum(_LAST_DAY.values())
    free = [d for d in range(1, total + 1) if d not in covered]
    if not free:
        return []
    runs, start = [], free[0]
    for prev, cur in zip(free, free[1:]):
        if cur != prev + 1:
            runs.append((start, prev)); start = cur
    runs.append((start, free[-1]))
    # A run ending on Dec 31 and one starting Jan 1 are ONE run on a circle.
    if len(runs) > 1 and runs[0][0] == 1 and runs[-1][1] == total:
        runs = [(runs[-1][0], runs[0][1])] + runs[1:-1]
    return [DateRange(from_month=_md(a)[0], from_day=_md(a)[1],
                      to_month=_md(b)[0], to_day=_md(b)[1]) for a, b in runs]


def _md(idx: int):
    for m in range(1, 13):
        if idx <= _LAST_DAY[m]:
            return m, idx
        idx -= _LAST_DAY[m]
    raise ValueError(idx)


class Solar(str, Enum):
    sunrise = "sunrise"
    sunset = "sunset"


class Clock(BaseModel):
    """A TIME OF DAY, either off the clock or off the sun.

    The book writes both and the corpus stored both as prose — "21:00", "21:00 hours" and "one
    hour after sunset" all sat in the same string field, so the first two were the same instant
    spelled two ways and the third was not a time at all. A solar time cannot be resolved to a
    clock without a date and a latitude, which is the client's to do and not the parser's, so it
    is carried as what it is.
    """
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    at: Optional[str] = Field(default=None, pattern=r"^([01]\d|2[0-3]):[0-5]\d$")
    solar: Optional[Solar] = None
    #: Minutes from the solar event. NEGATIVE IS BEFORE — "30 minutes before sunrise" is
    #: `{solar: sunrise, offset_min: -30}`, and the sign is the whole difference between
    #: fishing legally and not.
    offset_min: int = 0

    @model_validator(mode="after")
    def _one_kind(self) -> "Clock":
        if bool(self.at) == bool(self.solar):
            raise ValueError("a time is a clock time OR a solar time, not both and not neither")
        if self.at and self.offset_min:
            raise ValueError("an offset belongs to a solar time; put it in the clock time")
        return self

    def words(self) -> str:
        if self.at:
            return self.at
        n = abs(self.offset_min)
        if not n:
            return self.solar.value
        unit = f"{n} minutes" if n % 60 else ("one hour" if n == 60 else f"{n // 60} hours")
        return f"{unit} {'before' if self.offset_min < 0 else 'after'} {self.solar.value}"


class DateRange(BaseModel):
    """A RANGE OF CALENDAR DAYS, no year. Both ends INCLUSIVE, per the synopsis: "When no date
    is listed, the regulations apply ALL YEAR. Start and end dates are INCLUSIVE."

    A range may WRAP the year end — "Nov 1-Apr 30" is one winter, not an error — so `to` before
    `from` is meaningful and is not rejected.
    """
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    from_month: int = Field(ge=1, le=12)
    from_day: int = Field(ge=1, le=31)
    to_month: int = Field(ge=1, le=12)
    to_day: int = Field(ge=1, le=31)

    @model_validator(mode="after")
    def _real_days(self) -> "DateRange":
        for m, d, side in ((self.from_month, self.from_day, "from"),
                           (self.to_month, self.to_day, "to")):
            if d > _LAST_DAY[m]:
                raise ValueError(f"{side}: day {d} does not exist in month {m}")
        return self

    def words(self) -> str:
        nm = {v: k.capitalize() for k, v in _MONTHS.items()}
        return f"{nm[self.from_month]} {self.from_day}-{nm[self.to_month]} {self.to_day}"


class Hours(BaseModel):
    """A RANGE WITHIN THE DAY. Either end may be a clock time or a solar one, so "from one hour
    after sunset to one hour before sunrise" is sayable. It WRAPS midnight the same way a
    `DateRange` wraps the year end, and needs no flag for that either."""
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    start: Clock
    end: Clock

    def words(self) -> str:
        return f"{self.start.words()} to {self.end.words()}"


class _Terse(BaseModel):
    """DUMPS ONLY WHAT WAS SAID. Every list on a gear clause defaults empty and an empty `allow`,
    `only` or `ban` is refused, so an empty list here never carries meaning — yet each one shipped
    as `except: [], members: [], must_be: [], of: []` on every clause in the bundle, four keys of
    noise a reader has to prove are noise."""

    @model_serializer(mode="wrap")
    def _terse(self, handler):
        return {k: v for k, v in handler(self).items()
                if v != [] and v != "" and v is not None and v is not False}


class When(_Terse):
    """WHEN A RULE BINDS — the days, the hours and the weekdays, said once.

    This replaces `windows` (210 distinct FREE-TEXT strings, where "Nov 1-Apr 30" and
    "Nov 1 - Apr 30" were the same range spelled two ways), `windows_are`, `from_time`, `to_time`
    and `weekdays`.

    THERE IS NO `excepts` FLAG. `windows_are: "excepts"` meant "these are the days the rule does
    NOT hold" — a flag that inverted the field beside it, which is exactly what `band` did to the
    size fields. Three rules carried it. The complement of a circular range is another circular
    range, so the days a rule DOES hold are always writable: Fulton River's "Open June 16-Apr 30
    each year" is a closure, and it is stored as the closure's own days, May 1 - June 15.

    `dates` EMPTY MEANS ALL YEAR, per the synopsis: "When no date is listed, the regulations apply
    ALL YEAR. Start and end dates are INCLUSIVE."

    TERSE (`_Terse`): an empty list here is a default and says nothing, so it is never dumped —
    every designation shipped `"unparsed":[],"weekdays":[]` beside its dates.
    """
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    dates: List[DateRange] = Field(default_factory=list)
    hours: Optional[Hours] = None
    weekdays: List[str] = Field(default_factory=list)
    #: SEASONS THE PARSER COULD NOT READ, kept verbatim. This is the time analogue of
    #: `unresolved_locators`, and it exists for the same reason: an unparsed season and an ABSENT
    #: one are opposite facts, and a rule published as though it had no season when its source
    #: says "To be determined" is open all year to a reader. The DFO feed scrapes rows whose date
    #: cell is prose, and refusing them would have meant dropping the rule or inventing a window.
    unparsed: List[str] = Field(default_factory=list)

    def is_empty(self) -> bool:
        return not (self.dates or self.hours or self.weekdays or self.unparsed)

    def words(self) -> str:
        bits = [" and ".join(d.words() for d in self.dates)] if self.dates else []
        if self.hours:
            bits.append(self.hours.words())
        if self.weekdays:
            bits.append(" and ".join(f"{d}s" for d in self.weekdays))
        if self.unparsed:
            bits.append(" and ".join(self.unparsed))
        return ", ".join(b for b in bits if b)


class Slot(str, Enum):
    """WHAT A GEAR CLAUSE CONSTRAINS. One name per MEASURAND, which is the whole point.

    `hook_count` used to be three different quantities under one name — hooks on the line, POINTS
    on one hook, and attachments of any kind on the line — and a treble is one hook with three
    points, so the same record permitted it and banned it. The curator now decides once, in the
    name, instead of leaving it to whichever neighbouring field happens to be present.
    """
    # CHOSEN FROM A SET — `allow` lists what is permitted, and `allow: []` IS the ban.
    bait = "bait"                       # roe, invertebrate, dead_fin_fish, any
    lure = "lure"                       # the terminal object: artificial_fly, artificial_lure
    method = "method"                   # how you fish: fly_fishing, set_lining, ice_fishing
    barb = "barb"                       # barbed | barbless
    # SPEC SLOTS — `must_be`, how the thing must be built or carried.
    #
    # THE SLOT AND THE MEANS SHARE ONE TOKEN. A set line IS how you set-line and a crayfish trap
    # IS how you crayfish-trap, so the slot is named for the means and `while: ["set_lining"]`
    # needs no lookup to reach `set_lining: {must_be: [...]}`. They were `set_line` and
    # `crayfish_trap`, which forced a correspondence map in the register for no gain.
    set_lining = "set_lining"
    crayfish_trapping = "crayfish_trapping"
    downrigger = "downrigger"
    light = "light"
    # …EXCEPT WHERE THE OBJECT IS NOT THE MEANS. An ice hut is not how you ice fish; it is a thing
    # you leave on a lake. So this slot keeps its own name and carries `while: ["ice_fishing"]`.
    ice_hut = "ice_hut"
    # COUNTED — `max`/`min` are whole numbers of the thing the name says.
    hooks_per_line = "hooks_per_line"
    points_per_hook = "points_per_hook"
    lines_per_angler = "lines_per_angler"
    flies_per_line = "flies_per_line"
    terminal_attachments_per_line = "terminal_attachments_per_line"
    # MEASURED — `max`/`min` in the unit the name carries. One unit per quantity: `min_gap_cm: 3`
    # and `max_gap_mm: 15` were the same measurement of the same object in two units.
    hook_gap_mm = "hook_gap_mm"
    weight_per_line_kg = "weight_per_line_kg"
    bait_possession_kg = "bait_possession_kg"
    #: "unless the light is submerged and attached to the fishing line WITHIN 1 M of the hook" —
    #: a distance, so it is its own slot with the unit in the name. Written as a bound on `light`
    #: it read as "at most one light", which is the founding complaint of this enum.
    light_to_hook_mm = "light_to_hook_mm"


#: Which slots take a SET and which take a NUMBER. A slot cannot take both, and `_check` refuses
#: the mixture — that is what stops a count being written where a whitelist belongs.
_SET_SLOTS = frozenset({Slot.bait, Slot.lure, Slot.method, Slot.barb})

#: The slots whose bound is `must_be` — how the thing must be built or carried, never a count and
#: never a whitelist. Presence asserts; absence is silence; there is no negation to write.
_SPEC_SLOTS = frozenset({Slot.set_lining, Slot.crayfish_trapping, Slot.downrigger, Slot.light,
                         Slot.ice_hut})

#: The slots whose bound is a MEASUREMENT rather than a count, and so may be fractional.
_MEASURED = frozenset({Slot.hook_gap_mm, Slot.weight_per_line_kg, Slot.bait_possession_kg,
                       Slot.light_to_hook_mm})


#: WHAT `while` MAY NAME — TWO KINDS OF TOKEN, and a reader must not treat them alike.
#:
#: MEANS are ways of fishing: every `Method` member. "Sport fishing" is DEFINED as angling, spear
#: fishing, set lining and crayfish trapping, and the book grants each of them (with ice fishing)
#: in its "Allowable Fishing Methods" list; a means the book grants nowhere is not a lawful way to
#: sport fish at all ("All other methods of taking fin fish and crayfish are illegal").
#:
#: DEVICES are things you use WHILE angling — a downrigger, a light. A rule may bind while one is
#: in use (its spec slot says how it must be rigged), but a device is not a way of fishing and
#: nothing grants or bans it as one; that is why they left `Method` (see the note there).
#:
#: TWO TOKENS WERE HERE AND ARE NOT. `alone_in_a_boat` was accepted by the validator alone — no rule
#: used it, and it duplicated `GearWhen.angler`, the one place a boat condition is written.
#: `ice_hut` was accepted because it is a spec slot, but an ice hut is not something you DO: its
#: slot binds `while: ["ice_fishing"]`. Both are refused.
WHILE_MEANS = frozenset(m.value for m in Method)
WHILE_DEVICES = frozenset({Slot.downrigger.value, Slot.light.value})
WHILE_TOKENS = WHILE_MEANS | WHILE_DEVICES


class AnglerState(str, Enum):
    alone_in_boat = "alone_in_boat"
    in_boat = "in_boat"
    #: "No angling from POWERED boats": a canoe is still allowed, so this is not `in_boat`.
    in_powered_boat = "in_powered_boat"
    from_shore = "from_shore"


#: An angler's state in words, after a gear clause: "no angling (from a boat)".
_ANGLER_WORDS = {"alone_in_boat": "alone in a boat", "in_boat": "from a boat",
                 "in_powered_boat": "from a powered boat", "from_shore": "from shore"}


class GearWhen(_Terse):
    """WHEN A GEAR CLAUSE APPLIES — and a CLOSED vocabulary on purpose.

    `reason` was a free-text field that ended up carrying six different jobs: a carve-out, a
    gating condition, an obligation, the prohibited act's own name, and once an actual reason.
    An open `when` would be that field again under a better name. So every condition here is a
    named term, and anything the book says that does not fit goes in `note` — which REQUIRES a
    `review_reason` on the rule, so the gap is visible rather than absorbed.
    """
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    water: Optional[WaterKind] = None
    method: Optional[Method] = None          # "…when set lining"
    targeting: List[str] = Field(default_factory=list)   # species codes
    angler: Optional[AnglerState] = None
    gear_in_use: Optional[str] = None        # "…does not apply to downrigger weights"
    note: str = ""                           # the escape, and it costs a review_reason

    @model_validator(mode="after")
    def _book_species(self) -> "GearWhen":
        # A target is a species code like any other: the book's list (p.86), never a raw code.
        bad = species_problems(set(self.targeting) - FEDERAL_SALMON, "when.targeting")
        if bad:
            raise ValueError("; ".join(bad))
        return self

    def is_empty(self) -> bool:
        return not (self.water or self.method or self.targeting or self.angler
                    or self.gear_in_use or self.note)

    def key(self) -> str:
        """The condition as a stable key, for a rule's `dimension`: "angler=in_boat"."""
        bits = [f"{k}={getattr(v, 'value', v)}" for k, v in
                (("water", self.water), ("method", self.method), ("angler", self.angler),
                 ("gear_in_use", self.gear_in_use)) if v]
        if self.targeting:
            bits.append("targeting=" + "+".join(sorted(self.targeting)))
        if self.note:
            bits.append("note")
        return "&".join(bits)


class GearSpec(_Terse):
    """HOW THE THING MUST BE BUILT OR CARRIED, for the clause to hold.

    Three sentences needed this and only this, and without it all three read as unrepresentable:

      "angle with a downrigger, PROVIDED the fishing line is attached to the downrigger by a
       quick-release mechanism"     -> the proviso constrains the downrigger in the same clause,
                                       not some second thing
      "unless the light is SUBMERGED and ATTACHED to the fishing line WITHIN 1 M of the hook"
                                    -> three properties of the one light
      "traps with minimally-sized CIRCULAR openings"
                                    -> the shape is a property; only the size has no number

    A CLOSED vocabulary on purpose. `reason` was open and ended up carrying six different jobs;
    an open spec would be that field again. What does not fit goes in `note`, which costs a
    `review_reason`, so the gap stays visible instead of being absorbed.
    """
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    #: `submerged: Optional[bool]` WAS HERE and it was the last polarity flag in the model.
    #: `submerged: false` reads as "a light is lawful only if it is NOT submerged", the inverse of
    #: the printed clause — `{barbless: true, required: false}` in a new house, evicted from
    #: `GearClause` and re-admitted one object down. A state the thing must be IN is a `must_be`
    #: token on the spec slot, where negation is unwriteable.
    attached_to: Optional[str] = None        # fishing_line
    attachment: Optional[str] = None         # quick_release
    within_m_of_hook: Optional[float] = None
    opening_shape: Optional[str] = None      # circular
    note: str = ""

    def is_empty(self) -> bool:
        return not (self.attached_to or self.attachment
                    or self.within_m_of_hook or self.opening_shape or self.note)


class GearClause(_Terse):
    """ONE THING CONSTRAINED, AND HOW FAR. Ordered within `gear`; FIRST MATCH WINS.

    THERE IS NO POLARITY FLAG. `allowed`, `permitted` and `required` were three booleans on one
    axis, `required` was `false` on all 25 of its uses, and on the province's most-cited stream
    rule `{barbless: true, required: false}` read as "barbless is not required" — barbed hooks
    legal in every stream in B.C. A clause carries its verdict in the same object as its numbers,
    so there is no neighbour left to invert.
    """
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    slot: Slot
    #: WHICH members this clause speaks about. Absent = ALL of them, so "bait ban" is a bare
    #: `{slot: bait, allow: []}` while "fin fish is prohibited" is `of: ["fin_fish"]` with the
    #: same empty allow — a TOTAL ban and a PARTIAL one, which without `of` are one record.
    of: List[str] = Field(default_factory=list)
    #: THREE BOUNDS, ONE MEANING EACH. `allow` permits and says nothing about the rest; `only` is
    #: a whitelist that closes the slot ("fly fishing only"); `ban` prohibits what it names. With
    #: `allow` alone, a permission and a whitelist are the same record and a reader must guess.
    allow: Optional[List[str]] = None
    only: Optional[List[str]] = None
    ban: Optional[List[str]] = None
    #: MEMBERS THIS CLAUSE'S BOUND DOES NOT REACH. "The use of fin fish (dead or alive) or parts
    #: of fin fish OTHER THAN ROE is prohibited throughout the province" is one sentence with one
    #: bound and one carve-out, and `gear` being a map means a ban and an allow cannot both sit on
    #: `bait` in one rule. Without this the exception produced NO FIELD, and its whole force fell
    #: on one register line parenting `roe` to `any_bait` rather than to `fin_fish` — a line that
    #: reads like under-specification, whose correction would ban roe province-wide, and which no
    #: verbatim-seeded check could catch because there was no field to compare against.
    except_: List[str] = Field(default_factory=list, alias="except")
    members: List[str] = Field(default_factory=list)  # a choice of ONE from several kinds
    max: Optional[float] = None
    min: Optional[float] = None
    #: NO CEILING, SAID OUTRIGHT. "A person in a boat may angle with an unlimited number of rods"
    #: is a bound, not an absence of one: without it the clause has nothing to state and the
    #: sentence could only be an exemption, which hides what it grants. It was once written
    #: `max_lines: 0` — a zero standing for infinity — and converted to "no lines at all". JSON has
    #: no infinity, so the word is the value, as `unlimited` is on a retention rule.
    unlimited: bool = False
    when: Optional[GearWhen] = None
    #: HOW THE THING ITSELF MUST BE — see `GearSpec`. A property of the subject this clause
    #: names, not a separate condition on the angler or the water.
    requires: Optional[GearSpec] = None
    must_be: List[str] = Field(default_factory=list)
    #: WHAT LIFTS THIS CLAUSE. "more than 1 kg of weight … (this does not apply to downrigger
    #: weights)" is one clause with one escape; the carve-out used to sit in `reason`, the same
    #: free-text field that elsewhere held an obligation and elsewhere the prohibited act's name.
    unless: List[GearWhen] = Field(default_factory=list)

    @model_validator(mode="after")
    def _shape(self) -> "GearClause":
        counted = self.max is not None or self.min is not None or self.unlimited
        if self.unlimited and (self.max is not None or self.slot in _MEASURED
                               or self.slot in _SET_SLOTS or self.slot in _SPEC_SLOTS):
            raise ValueError(f"{self.slot.value}: `unlimited` lifts the ceiling on a COUNT, and "
                             f"never alongside a `max`")
        bounds = [b for b in (self.allow, self.only, self.ban) if b is not None]
        if self.slot in _SPEC_SLOTS:
            # NO BOUND AT ALL ON A SPEC SLOT. `light: {max: 1}` read as "at most one light" when
            # the printed 1 is METRES — "within 1 m of the hook" — and `light` was the one slot
            # admitted to a number without the unit in its name, which is the founding complaint
            # of this enum.
            if bounds or counted:
                raise ValueError(f"{self.slot.value} says HOW the thing must be: use `must_be` "
                                 f"(a distance is its own slot, with the unit in the name)")
            if not self.must_be and not self.requires:
                raise ValueError(f"{self.slot.value} needs a `must_be`")
        elif self.slot in _SET_SLOTS:
            if counted:
                raise ValueError(f"{self.slot.value} is chosen from a set: "
                                 f"use allow/only/ban, not max/min")
            if len(bounds) != 1:
                raise ValueError(f"{self.slot.value} takes exactly one of allow / only / ban")
            # A BAN NAMES ITS MEMBERS. An empty list is also what a dropped key, a failed parse and
            # a half-filled field produce, so `ban: []` would make the most dangerous statement in
            # the schema the easiest one to write by accident.
            # NO EMPTY LIST, ON ANY OF THE THREE. An empty list is what a dropped key, a failed
            # parse and a half-filled field all produce. `allow: []` was the sanctioned spelling
            # of "everything in this slot is banned" — the widest statement in the schema, written
            # in the one shape three accidents also write. "Bait ban" is `ban: ["any_bait"]`, and
            # the whole-slot member has to be typed.
            if self.except_ and self.ban is None:
                raise ValueError(f"{self.slot.value}: `except` carves members out of a ban")
            for key, val in (("allow", self.allow), ("only", self.only), ("ban", self.ban)):
                if val is not None and not val:
                    raise ValueError(
                        f"{self.slot.value}: `{key}: []` says nothing or says everything, and is "
                        f"what a dropped key also writes — name the members "
                        f"(a total ban is the whole-slot member, e.g. any_bait)")
        else:
            if self.except_:
                raise ValueError(f"{self.slot.value} is counted: `except` carves out set members")
            # ALL THREE SET BOUNDS, not just `allow`. `{hooks_per_line, only: ["single"], max: 1}`
            # rebuilds the exact collapse this enum exists to end — a statement about hook TYPE
            # riding on the slot that COUNTS hooks.
            if bounds:
                raise ValueError(f"{self.slot.value} is counted or measured: "
                                 f"use max/min, not allow/only/ban")
            # `members` QUALIFIES a bound and never substitutes for one. Without this,
            # "only one hook, one lure OR one fly is attached" — the basic licence entitlement,
            # on every angler on every water — inverts into unlimited terminal tackle.
            if not counted:
                raise ValueError(f"{self.slot.value} needs a max, a min or `unlimited`")
        # AN ESCAPE THAT MATCHES EVERYTHING LIFTS EVERYTHING. `GearWhen()` with every field at its
        # default is a valid object, so "(this does not apply to downrigger weights)" with the one
        # key dropped becomes "there is no weight limit in B.C." — and a dropped key is exactly
        # what a parse that scanned the parenthetical and matched nothing produces.
        for u in self.unless:
            if u.is_empty():
                raise ValueError(f"{self.slot.value}: an `unless` with no condition lifts the "
                                 f"clause everywhere — say what lifts it")
        if self.requires is not None and self.requires.is_empty():
            raise ValueError(f"{self.slot.value}: `requires` states nothing")
        if self.max is not None and self.min is not None and self.min > self.max:
            raise ValueError(f"min {self.min} > max {self.max} permits nothing")
        # A COUNT IS A WHOLE NUMBER. Only the measured slots carry a fraction, and letting a
        # count be 1.5 would make "one and a half hooks" a storable rule.
        # THE BOOK PRINTS BOTH UNITS — "gap not less than 3 cm" on a set line, "no hooks greater
        # than 15 mm from point to shank" on a hook. One unit in the slot name is what stops them
        # diverging, and the cost is that an unconverted 3 is a LEGAL value meaning 3 mm, which is
        # no constraint at all. Nothing in the corpus goes below 15.
        if self.slot is Slot.hook_gap_mm:
            for v in (self.max, self.min):
                if v is not None and v < 10:
                    raise ValueError(f"hook_gap_mm {v} is smaller than any gap the book prints — "
                                     f"the synopsis states this in cm as well as mm, so confirm "
                                     f"the unit ('3 cm' is 30)")
        if self.slot not in _MEASURED:
            for v in (self.max, self.min):
                if v is not None and float(v) != int(v):
                    raise ValueError(f"{self.slot.value} counts things; {v} is not a whole number")
        return self


#: WHAT YOU MUST AND MUST NOT DO, as act tokens NAMED IN THEIR LAWFUL DIRECTION.
#:
#: `do_not_waste_catch`, never `waste_catch` plus a flag. A `must`/`must_not` key beside a token
#: that already carries its direction is a second place to state polarity — and `must:
#: "do_not_waste_catch"` is a double negative that reads as law. `required: false` was that flag:
#: it sat on "Waste the fish you catch" AND on "Fish with nets", both off a printed "You must
#: not" list, and read literally it made them optional; it was `false` on all 25 of its uses and
#: ABSENT on the requirements beside them, so its presence meant nothing and its absence meant
#: nothing.
#:
#: The vocabulary is open by necessity — each synopsis edition can print a new duty — so it is
#: held by a registry rather than a type: a token must appear here, and adding one is a reviewed
#: change carrying the verbatim that motivated it.
CONDUCT_ACTS = {
    #: `be_accompanied_by_licensed_adult` WAS HERE, holding up the under-16 non-resident rule: a
    #: `required: false` basic licence with the accompaniment parked on it as a duty. Accompaniment
    #: is a way of SATISFYING a requirement, so it is a `Path` (`accompanied_by`, with the quota
    #: note) on `zp:basic_licence#under_16_non_resident`, and the token is gone.
    "do_not_waste_catch": "Do not waste the fish you catch",
    "do_not_release_harmfully": "Do not release fish in a harmful manner",
    "do_not_buy_sell_or_barter_catch": "Do not buy, sell or barter your catch",
    "do_not_possess_or_move_live_fish": "Do not possess or move live fish or invertebrates",
    # Printed p. 8, "It Is Unlawful To…", the continuation of the live-fish bullet.
    "do_not_keep_catch_alive": "Do not hold your catch alive (livewell, other device or stringer)",
    "do_not_release_aquarium_fish": "Do not release your aquarium fish to the wild",
    "do_not_high_grade": "Do not high-grade",
    "do_not_can_bottle_or_fillet_away_from_residence":
        "Do not can, bottle or fillet your catch away from your residence",
    "do_not_freeze_in_unrecognizable_block": "Do not freeze fish in an unrecognizable block",
    "do_not_interfere_with_furbearer_trap": "Do not damage or interfere with a furbearer trap",
    "no_gear_in_water_during_closure": "Do not place gear in the water during a No Fishing period",
    "leave_head_tail_and_fins_until_residence":
        "Leave the head, tail and fins on your catch until your residence",
    "release_immediately": "Release it immediately, where you caught it",
    "warn_others_of_ice_hole": "Warn others of your ice hole",
    "remove_ice_hut_before_breakup": "Remove your ice hut before breakup",
    "mark_set_line_with_contact_details":
        "Mark your set line with your name, address and telephone number",
    "return_unfit_fish_gently": "Return a fish you cannot keep gently to the water",
    # Licensing duties — what you must DO with a licence, never whether you need one.
    "produce_licence_on_request":
        "Produce your angling licence and photo ID when an officer asks",
    "carry_paper_licence": "Carry your paper licence",
    # Printed p. 10, "Transporting and Exporting Fish": what you must do when you move fish.
    "keep_licence_handy_while_travelling": "Have your angling licence at hand when you travel with fish",
    "transport_no_more_than_legal_limit": "Do not transport or possess more than your legal limit",
    "keep_catch_identifiable": "Make sure your fish can be identified, counted and measured",
    "carry_signed_letter_when_transporting_for_another":
        "Carry a signed letter from the angler when you transport fish for someone else",
    "show_letter_when_exporting":
        "Carry the letter and show it on request when you export the fish from B.C.",
    "keep_signed_letter_for_gifted_fish":
        "Keep a signed letter from the angler until you have eaten fish given to you",
    "do_not_enter_land_without_permission":
        "Do not enter or cross cultivated, posted or private land, or Indian Reserve land, "
        "without permission",
}

#: THE ACTS THAT ARE ABOUT A DOCUMENT. Producing or carrying a licence means nothing to an angler
#: who need not hold one, so a requirement carrying one of these must say which documents it
#: presumes (`Requirement.presumes`) — that is what lets an exemption from them release it too.
DOCUMENT_ACTS = frozenset({"produce_licence_on_request", "carry_paper_licence"})

#: HOW A SENTENCE NAMES A DOCUMENT A DUTY PRESUMES, on the squashed (lower-cased) text. A document
#: missing here cannot be presumed at all — adding one is a reviewed change, like an act.
_PRESUMED_SAID = {
    #: plural too: "paper licences are required when retaining hatchery steelhead, chinook, …"
    "basic_licence": r"\b(?:angling|paper|basic(?: \w+)?|fishing) licen[cs]es?\b",
    "classified_waters_licence": r"\bclassified waters? licen[cs]e\b",
    "steelhead_stamp": r"\bsteelhead (?:conservation surcharge )?stamp\b",
    "salmon_stamp": r"\bsalmon (?:conservation surcharge )?stamp\b",
}


class LengthBand(BaseModel):
    """ONE RANGE OF FISH LENGTHS, AND HOW MANY OF THEM YOU MAY KEEP.

    `min_cm` and `max_cm` are INCLUSIVE, and null is open at that end. `take` is how many of
    THESE you may keep; omitted, the rule's own `take` applies to them.
    """
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    min_cm: Optional[int] = None
    max_cm: Optional[int] = None
    take: Optional[int] = None

    @model_validator(mode="after")
    def _real(self) -> "LengthBand":
        if self.min_cm is None and self.max_cm is None:
            raise ValueError("a length band open at both ends is every fish; say nothing instead")
        if self.min_cm is not None and self.max_cm is not None and self.min_cm >= self.max_cm:
            raise ValueError(f"min_cm {self.min_cm} >= max_cm {self.max_cm} is an empty range")
        if self.take is not None and self.take < 0:
            raise ValueError("take cannot be negative")
        return self

    def holds(self, cm: int) -> bool:
        return ((self.min_cm is None or cm >= self.min_cm) and
                (self.max_cm is None or cm <= self.max_cm))


# --------------------------------------------------------------------------------------- #
# Licensing — a separate list on the entry, not a rule type.
#
# `document_required` and `access_permission` were rule types, and a type is a wall the
# override cannot cross (the invariant at the top of this module). Licensing never competes:
# the only "overriding" in 169 licensing rules was `required: false`, and that was the defect —
# nine steelhead-stamp waivers shared a dimension with the provincial "stamp if you fish for
# steelhead", so a narrower scope struck it and a steelhead angler on the Chilko needed no stamp.
# Licensing also depends on two inputs no rule takes — WHO the angler is and what they are DOING.
#
# So it is six small records on `CatalogueEntry.licensing`, discriminated by `kind`:
#
#   designation     a FACT about a water: this reach is Classified, class I/II, in licence unit U,
#                   during these days; the classified-water steelhead stamp runs / is waived here;
#                   it sleeps while a named closure binds
#   not_classified  an asserted ABSENCE ("Part described is NOT a Classified Water")
#   requirement     an OBLIGATION, stated once: this WHO, DOING this, WHERE/WHEN, must satisfy
#                   one of these paths (hold documents / be accompanied)
#   licence_terms   how a document is SOLD — never bound to a section, never an obligation
#   exemption       a named WHO released from named documents
#   alternative     a place where another document ALSO satisfies a requirement
#
# LICENSING NEVER VOTES ON OPEN/CLOSED. It is consulted only where a water is open for the
# activity, which is why "Class II water WHEN OPEN" needs no field: a closed water needs no licence
# by construction. The one licensing-adjacent closure — "angling prohibited for non-guided
# non-resident aliens on Saturdays" — is a closure, and stays a RULE (`angler_closure`).
#
# THE ANGLER IS ALWAYS UNKNOWN. Every axis of `Who` is a set with the complement written out, so a
# reader can answer "if you are a non-resident …" without assuming a default profile.
# --------------------------------------------------------------------------------------- #

#: Every axis a `Who` may constrain, and every member of it. An axis left out means ANY member.
WHO_AXES: dict[str, tuple[str, ...]] = {
    "residency": ("resident", "non_resident", "non_resident_alien"),
    "age": ("under_16", "16_plus"),
    "guidance": ("guided", "non_guided"),
    "status": ("indian_bc_resident", "metis", "disabled"),
    #: WHAT THE ANGLER IS DOING FOR SOMEONE ELSE. A Youth/Disabled Accompanied Water (printed
    #: p.4) is closed to every angler who is not an "authorized angler" (under 16, or a disabled
    #: resident) or "a companion to an authorized angler" — and a companion is not a residency,
    #: an age or a status: it is the role of the adult standing beside the child. Not a
    #: partition (most anglers are no one's companion), like `status`.
    "role": ("companion",),
}
#: The axes every angler sits on exactly one member of. Naming all of them is "everyone", which
#: has one spelling: say nothing. `status` is not a partition — most anglers hold none of the
#: three — so naming all three is a real (if odd) set and is not refused.
_PARTITION_AXES = ("residency", "age", "guidance")

Residency = Literal["resident", "non_resident", "non_resident_alien"]
Age = Literal["under_16", "16_plus"]
Guidance = Literal["guided", "non_guided"]
Status = Literal["indian_bc_resident", "metis", "disabled"]
Role = Literal["companion"]


class Who(_Terse):
    """WHICH ANGLERS — a set on every axis, the included members listed.

    This replaces `AnglerClass`, whose `residency` held ONE value. "Non-resident anglers" in the
    synopsis covers non-residents AND non-resident aliens, so `basic_licence.r4` ("not a resident
    of B.C.") left the under-16 alien out; "Canadian resident" became `resident` (= B.C.) and
    dropped every other Canadian. `guided: false` was a polarity bit. Here a list says who is IN,
    and there is nothing to invert.
    """
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    residency: List[Residency] = Field(default_factory=list)
    age: List[Age] = Field(default_factory=list)
    guidance: List[Guidance] = Field(default_factory=list)
    status: List[Status] = Field(default_factory=list)
    role: List[Role] = Field(default_factory=list)

    @model_validator(mode="after")
    def _one_spelling(self) -> "Who":
        said = False
        for axis in WHO_AXES:
            vals = getattr(self, axis)
            if len(set(vals)) != len(vals):
                raise ValueError(f"who.{axis}: {vals} names a member twice")
            if axis in _PARTITION_AXES and set(vals) == set(WHO_AXES[axis]):
                raise ValueError(f"who.{axis}: naming every member is everyone — leave the axis "
                                 f"out instead (one spelling for 'any')")
            said = said or bool(vals)
        if not said:
            raise ValueError("an empty `who` is everyone — leave it out instead")
        return self

    def members(self, axis: str) -> frozenset:
        """The members this names on `axis`, or every member when the axis is left out."""
        return frozenset(getattr(self, axis) or WHO_AXES[axis])

    def overlaps(self, other: "Who") -> bool:
        """True when at least one angler is in both sets."""
        return all(self.members(a) & other.members(a) for a in WHO_AXES)

    def key(self) -> str:
        """A canonical spelling, for a dimension or a comparison."""
        return ";".join(f"{a}={','.join(sorted(getattr(self, a)))}"
                        for a in WHO_AXES if getattr(self, a))

    def words(self) -> str:
        """"non-guided non-resident aliens", "non-residents or non-resident aliens under 16"."""
        res = {"resident": "B.C. residents", "non_resident": "non-residents",
               "non_resident_alien": "non-resident aliens"}
        st_noun = {"indian_bc_resident": "Indians resident in B.C.", "metis": "Métis anglers",
                   "disabled": "disabled anglers"}
        st_adj = {"indian_bc_resident": "Indian", "metis": "Métis", "disabled": "disabled"}
        adj = " or ".join({"guided": "guided", "non_guided": "non-guided"}[g]
                          for g in self.guidance)
        if self.residency:
            noun = " or ".join(res[r] for r in self.residency)
            if self.status:
                noun = " or ".join(st_adj[s] for s in self.status) + " " + noun
        elif self.status:
            noun = " or ".join(st_noun[s] for s in self.status)
        elif self.role:
            noun = " or ".join({"companion": "companions of an authorized angler"}[r]
                               for r in self.role)
        else:
            noun = "anglers"
        age = " or ".join({"under_16": "under 16", "16_plus": "16 and over"}[a]
                          for a in self.age)
        return " ".join(x for x in (adj, noun, age) if x)


def residency_said(text: str) -> Optional[frozenset]:
    """THE RESIDENCY A SENTENCE NAMES, read off its own words — the check the model cannot talk
    its way past, because the verbatim is copied, not authored.

    The synopsis's idioms, and what each one MEANS:
      "non-resident alien(s)"          -> non_resident_alien
      "non-resident(s)" (not "…alien") -> non_resident + non_resident_alien — "non-resident
                                          anglers" covers both, which is the Kootenay defect
      "not a resident of B.C."         -> non_resident + non_resident_alien
      "Canadian resident"              -> resident + non_resident
      "B.C. resident" / "resident of B.C." -> resident
    None when the sentence names no residency at all."""
    t = squash(text)
    out: set = set()
    for pat, got in ((r"canadian residents?", {"resident", "non_resident"}),
                     (r"not a resident of b\.?\s?c", {"non_resident", "non_resident_alien"}),
                     (r"non-resident aliens?", {"non_resident_alien"}),
                     (r"non-residents?", {"non_resident", "non_resident_alien"}),
                     (r"b\.?\s?c\.? residents?|resident of b\.?\s?c", {"resident"})):
        if re.search(pat, t):
            out |= got
            t = re.sub(pat, " ", t)
    return frozenset(out) or None


def guidance_said(text: str) -> Optional[frozenset]:
    """THE GUIDANCE A SENTENCE NAMES: "non-guided"/"unguided" -> non_guided, "guided" -> guided,
    both ("whether GUIDED or NON-GUIDED") -> both, which is everyone. None when it names neither."""
    t = squash(text)
    out: set = set()
    if re.search(r"non-?\s?guided|unguided", t):
        out.add("non_guided")
        t = re.sub(r"non-?\s?guided|unguided", " ", t)
    if re.search(r"\bguided\b", t):
        out.add("guided")
    return frozenset(out) or None


def _check_residency(who: Optional["Who"], verbatim: str, where: str) -> Optional[str]:
    """The sentence's own residency and guidance words bind the `who`, in BOTH directions: a
    `who` that names a different set is wrong, and so is one that names NONE when the sentence
    names one — "angling prohibited for non-guided non-resident aliens" with `closed_to:
    {guidance: [non_guided]}` validated and closed the water to every non-guided resident."""
    have = frozenset(who.residency) if who is not None else frozenset()
    said = residency_said(verbatim)
    if said is not None and said != have:
        return (f"{where}: residency {sorted(have) or 'any'} is not what the sentence says "
                f"({sorted(said)}) — 'non-resident' covers aliens too; 'Canadian resident' covers "
                f"non-residents")
    have_g = frozenset(who.guidance) if who is not None else frozenset()
    said_g = guidance_said(verbatim)
    if said_g is not None:
        want = frozenset() if len(said_g) == 2 else said_g      # both named = everyone
        if have_g != want:
            return (f"{where}: guidance {sorted(have_g) or 'any'} is not what the sentence says "
                    f"({sorted(want) or 'guided or not'})")
    return None


#: A licence unit id — what a non-resident's per-day Classified Waters Licence names.
_SLUG = r"^[a-z0-9]+(_[a-z0-9]+)*$"


def slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (text or "").lower()).strip("_")


#: THE PROVINCIAL ANGLER DOCUMENTS — every licence or stamp the Wildlife Act sells an angler.
#: "you are not required to obtain ANY TYPE of fishing licence or stamp" means exactly these, and
#: a test pins the Indian-resident exemption to this set so a stamp added later is not silently
#: left off it.
PROVINCIAL_ANGLER_DOCUMENTS = (
    "basic_licence", "steelhead_stamp", "salmon_stamp", "kootenay_rainbow_stamp",
    "shuswap_char_stamp", "shuswap_rainbow_stamp", "white_sturgeon_licence",
    "classified_waters_licence",
)


class Ref(_Terse):
    """A licensing record in another entry, by `(entry_id, id)` — ids are unique only within an
    entry, as rule ids are (AGENTS 8)."""
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)
    entry_id: str = Field(..., min_length=1)
    id: str = Field(..., min_length=1)


class Quote(_Terse):
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)
    verbatim: str = Field(..., min_length=1)


class StampPeriod(_Terse):
    """WHEN the classified-water steelhead stamp runs on this designation, and the words that
    said so."""
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)
    when: When
    verbatim: str = Field(..., min_length=1)


class Suspension(_Terse):
    """"… not required until reopened to steelhead fishing": a closure rule in THIS entry, by id,
    while which the designation is dormant. Validated at the entry: the rule must exist and must
    be a closure — a designation can be suspended by a closure, never by a quota."""
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)
    rule_id: str = Field(..., min_length=1)
    verbatim: str = Field(..., min_length=1)


class See(_Terse):
    """A POINTER, NOT A RULE: "See Lonzo Creek", "A tributary of Slocan River. See Slocan River",
    "For regulations on the mainstem of the West Road River, see Region 5".

    Such a row (or clause) states no regulation of its own; it names the row whose regulations
    govern. It was an `advisory` rule quoting the pointer, which bound to the water and showed the
    words as a rule — 57 of them — and the reader could not follow it anywhere. As an edge it
    binds nothing and links to the entry it names (`entry_ids`, every one of which must exist:
    the bundle build refuses a dangling one, and `test_see_pointers` checks the corpus).

    A pointer whose target is NOT an entry (prose on another page, a sign at a trailhead) cannot
    be followed; it says so in `unresolved` instead — flagged, never guessed at.

    `verbatim` is the printed pointer, a contiguous run of the row. A row printed IN FULL in two
    region tables (MU 6-1 lakes in both Region 5 and Region 6) points at its twin with the whole
    row as `verbatim`: the book cross-lists it, and the copy under the other region's heading
    binds nothing (`see_relation` names it a `twin`).

    WHAT THE POINTER IS TO THE READER is derived, never stored (`see_relation`): an `alias` when
    this row's water IS the target's (Jones Lake -> Wahleach Lake, one lake under two names), a
    `twin` when the target prints the same row, else `see` (a different water governed by the
    target's rules)."""
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)
    verbatim: str = Field(..., min_length=1)
    entry_ids: List[str] = Field(default_factory=list)
    unresolved: str = ""

    @model_validator(mode="after")
    def _one_way(self) -> "See":
        if bool(self.entry_ids) == bool(self.unresolved.strip()):
            raise ValueError("a `see` names the entries it points at (`entry_ids`) OR says why it "
                             "points at none (`unresolved`) — exactly one")
        if len(set(self.entry_ids)) != len(self.entry_ids):
            raise ValueError(f"see.entry_ids repeats an entry: {self.entry_ids}")
        return self


#: THE WORDS OF A POINTER: "see X" / "also see X" / "see X regulations". Checked on `advisory`
#: rules only — a pointer written as one is refused (`CatalogueEntry`).
_POINTER_WORDS = re.compile(r"\bsee\s+(?!page\b|sign\b|note\b|tables\b|ice hut|mercury|the definition)\w")
#: ...unless what it points at is not a row: prose on another page, a sign, a warning, a
#: definition. Those stay information (a page pointer cannot be followed to an entry).
_NOT_A_WATER = re.compile(r"\bpage\b|\bsign\b|warning|definition")


def _letters(text: str) -> str:
    """Only the letters and digits of a row — two printings of one row differ in punctuation
    ("quota = 2; bait ban" / "quota = 2 Bait ban") and emphasis, never in words."""
    return re.sub(r"[^a-z0-9]", "", squash(text))


def see_relation(entry: "CatalogueEntry", targets: List["CatalogueEntry"]) -> str:
    """What a pointer IS, from the two rows — `alias`, `twin` or `see` (see `See`).

      twin   every target prints this row's words (a row cross-listed under two regions);
      alias  a row that is ONLY the pointer, whose water is among its targets' (the row is
             another name for it) — the book's "See Wahleach Lake" under JONES LAKE, which the
             alias table already holds. A row with rules of its own is never an alias: West
             Road River's tributaries row matches the river and points at the mainstem's row,
             and it is the tributaries' own rules that make it a row;
      see    otherwise: a different water (or part), governed by the target's rules."""
    if targets and all(_letters(t.regs_verbatim) == _letters(entry.regs_verbatim)
                       for t in targets):
        return "twin"
    theirs = {m for t in targets for m in t.matched}
    if entry.pointer_only and entry.matched and set(entry.matched) <= theirs:
        return "alias"
    return "see"


#: "Nov 1-Apr 30", "Jul 24 - Dec 31", "May 1-31" — the ranges a sentence prints.
_RANGE = re.compile(r"([a-z]+\.?\s*\d{1,2})\s*-\s*([a-z]+\.?\s*\d{1,2}|\d{1,2})\b")


def printed_ranges(text: str) -> List["DateRange"]:
    """Every date range the sentence prints, parsed. Unparseable matches are skipped."""
    out = []
    for m in _RANGE.finditer(squash(text)):
        got = parse_date_range(f"{m.group(1)}-{m.group(2)}")
        if got is not None:
            out.append(got)
    return out


def _dates_are_printed(when: Optional[When], verbatim: str, where: str) -> Optional[str]:
    """Every range in `when.dates` must be printed in the sentence it came from."""
    if when is None or not when.dates:
        return None
    printed = printed_ranges(verbatim)
    missing = [d.words() for d in when.dates if d not in printed]
    if missing:
        return f"{where}: dates {missing} are not printed in {verbatim[:60]!r}"
    return None


#: "all year", "year-round", "year round" — a sentence that prints a window for one place and ALL
#: YEAR for another ("Bull trout from the Liard River watershed Aug 15-Oct 15, and from the Peace
#: River watershed all year") may carry no `when`: an absent `when` IS all year.
_ALL_YEAR_SAID = re.compile(r"\ball year\b|\byear[- ]round\b")


#: A MONTH WITH NO DAY AT THE END OF A QUOTE: a date cut off. Coquihalla River's last line wraps
#: "…, Nov" / "1-Mar 31" onto the next printed line, and extraction kept only "Nov" — the row, a
#: rule's verbatim and both rules' seasons were lost. "May" is also a verb, so it is left out.
_CUT_DATE = re.compile(r"\b(jan|feb|mar|apr|jun|june|jul|july|aug|sep|sept|oct|nov|dec)\.?\s*$")


def _year_days() -> set:
    return _days([DateRange(from_month=1, from_day=1, to_month=12, to_day=31)])


def _own_dates_carried(rule: "CatalogueRule") -> Optional[str]:
    """A RULE WHOSE OWN SENTENCE PRINTS DATES CARRIES THEM. An absent `when` is ALL YEAR, so a rule
    quoting "…, May 1-Oct 31" with no `when` binds 365 days where the book printed 184 — the
    shape of all six `within` clauses the model now refuses, and of any quota, bait ban or closure
    split off a dated sentence without its season.

    The `when` must be one of three things, each a reading of the printed words:
      * a non-empty SUBSET of the printed ranges — a sentence naming two places with two windows
        is two rules, each carrying its own ("downstream of Hell's Gate Sept 1-Nov 15, …");
      * exactly their COMPLEMENT on the year — "Open June 16-Apr 30" is a closure on May 1-June 15,
        "catch and release, EXCEPT Apr 1-Apr 3 and July 1-July 2" a release on every other day
        (`When` has no `excepts` flag, so the complement is what is written);
      * ABSENT, when the sentence also prints "all year" — the other half is another rule.
    A LIFT-ONLY rule names the window of the rule it lifts ("Exempt from July 15-Aug 31 summer
    closure"); that is the lifted rule's season, not its own, and it is exempt."""
    if _CUT_DATE.search(squash(rule.verbatim)):
        return (f"its sentence ends in a month with no day ({squash(rule.verbatim)[-12:]!r}) — a "
                f"date cut off; restore it from the page")
    if rule.lift_only:
        return None
    printed = printed_ranges(rule.verbatim)
    if not printed:
        return None
    mine = list(rule.when.dates) if rule.when else []
    said = ", ".join(d.words() for d in printed)
    if not mine:
        if _ALL_YEAR_SAID.search(squash(rule.verbatim)):
            return None
        return (f"its sentence prints {said} but it has no `when` — an absent `when` is ALL YEAR; "
                f"give it the printed dates")
    if all(d in printed for d in mine):
        return None
    if _days(mine) == _year_days() - _days(printed):
        return None
    return (f"its `when` ({', '.join(d.words() for d in mine)}) is neither the dates its sentence "
            f"prints ({said}), some of them, nor their complement")


#: "catch and release ALL OTHER SPECIES", "… all species", "… all fish" — a water row's release
#: of everything it has not named. NOT "Whitefish: 15 (all species combined)", which is a quota
#: over the whitefish, and not "No Fishing … applies to all species" (a note about closures).
_ALL_SPECIES_SAID = re.compile(r"\ball (?:other )?(?:species|fish)\b(?! combined)")


def _all_species_is_game_fish(rule: "CatalogueRule") -> Optional[str]:
    """"ALL OTHER SPECIES" MEANS GAME FISH, AND NEVER CRAYFISH (user ruling, 2026-09-25).

    Whiteswan Lake's inlet and outlet print "rainbow trout daily quota = 5 (catch and release all
    other species)": the release is about the game fish an angler catches there. Written as
    `ALL_GAME_FISH` it reached crayfish (the closed list names them) and released a crayfish
    trapper's catch; written as `ALL_FIN_FISH` (Pine River's "all fish") it reached every sucker
    and carp. So a retention rule saying "all (other) species / all fish" is `ALL_GAME_FISH` with
    crayfish in `species_except` — game fish, less crayfish, less whatever the row names itself."""
    if rule.type is not RuleType.retention_limit:
        return None
    if _ALL_SPECIES_SAID.search(squash(rule.verbatim)):
        if list(rule.species) != ["ALL_GAME_FISH"] or "CRA" not in rule.species_except:
            return (f"its sentence says 'all (other) species / all fish', which is GAME FISH and "
                    f"never crayfish — species ['ALL_GAME_FISH'] with 'CRA' in species_except (it "
                    f"has species {list(rule.species)}, except {list(rule.species_except)})")
        return None
    # A BARE "CATCH AND RELEASE" IS THE SAME RELEASE (user ruling 2026-09-25): Burnt River's and
    # Clearwater Creek's "Catch and release" name no fish, so they release every GAME fish — and
    # crayfish may still be kept. Held structurally, not by the words: a release (take 0, may fish)
    # over ALL_GAME_FISH, under no means of its own, excepts crayfish. (A release WHILE set lining
    # is the book's "any game fish … other than burbot" about what the set line takes, and a
    # closure — `may_target: false` — is not a release.)
    if list(rule.species) == ["ALL_GAME_FISH"] and rule.take == 0 and rule.may_target \
            and not rule.while_ and "CRA" not in rule.species_except:
        return ("a catch and release over every game fish releases GAME FISH and never crayfish — "
                "add 'CRA' to species_except (crayfish may be kept; the zone's crayfish quota "
                "stands)")
    return None


#: PRINTED WINDOWS NO RULE OF THEIR ROW CARRIES, BECAUSE THE BOOK'S MEANING IS HELD ELSEWHERE —
#: `(entry_id, range words)` -> where. Every other printed range must be carried by some `when`
#: in its entry (`_printed_windows_carried`); an entry listed here that no longer needs it is
#: refused too, so the list cannot go stale.
WINDOWS_HELD_ELSEWHERE: dict = {
    ("z7b:trout_char_quota", "Oct 16-Aug 14"):
        "the NOTE's retention window for bull trout. Retention is allowed only in the Liard River "
        "watershed, whose row prints its own 'Oct 16-Aug 14' quota; everywhere else in Zone B "
        "r10 releases bull trout all year (user ruling 2026-09-24)",
    ("z7b:trout_char_quota", "Aug 15-Oct 15"):
        "the Liard half of r9's sentence. Outside the Liard row's Oct 16-Aug 14 its quota is not "
        "in force and r10's all-year release speaks there — which is Aug 15-Oct 15",
}


def _whens_in(obj, out: list) -> None:
    """Every `When` a model holds, at any depth — a rule's, a licensing record's, a stamp
    period's. Walks the model's own fields, so a new place a season can live is found."""
    if isinstance(obj, When):
        out.append(obj)
    elif isinstance(obj, BaseModel):
        for k in type(obj).model_fields:
            _whens_in(getattr(obj, k), out)
    elif isinstance(obj, (list, tuple)):
        for x in obj:
            _whens_in(x, out)


def _unique_span(hay: str, needle: str) -> Optional[tuple]:
    """Where `needle` sits in `hay`, when it sits there ONCE — two rules quoting the same words
    at two places ("no trout under 30 cm", twice on the Kootenay) cannot be told apart by text."""
    i = hay.find(needle)
    if i < 0 or not needle or hay.find(needle, i + 1) >= 0:
        return None
    return i, i + len(needle)


_JOIN_NEXT = re.compile(r"[\s,)\]]*(?:\band\b\s*|\(\s*)?")
_BARE_GAP = re.compile(r"[\s,:)\]]*(?:from\s+)?")


def _dates_lost(entry: "CatalogueEntry") -> List[str]:
    """THE SEASONS OF A ROW, AGAINST ITS RULES. Four ways a printed date has been lost, each a shape
    the corpus once held; the first is on the rule (`_own_dates_carried`), these three need the
    row and the rule's siblings.

      1. A PRINTED WINDOW NO `when` CARRIES. Every range in `regs_verbatim` is some rule's or
         licensing record's `when` (or its complement, or the window a lift names), unless
         `WINDOWS_HELD_ELSEWHERE` says where it is held.
      2. A QUOTE INSIDE A DATED SIBLING'S. "Trout daily quota = 2 (none under 30 cm), May 1-Oct 31"
         split into the quota (dated) and "none under 30 cm" (a rule of its own, undated): the
         clause's words overlap the dated rule's, so it is the same sentence and the same season.
      3. THE DATE STRAIGHT AFTER THE QUOTE. "…quota = 2 (none under 30 cm), May 1-Oct 31" with
         the verbatim cut before the date and no `when`: nothing but punctuation separates the
         rule's words from the window, so the window is the rule's.
      4. "A AND B, <dates>". "Trout/char catch and release and bait ban, June 15-Aug 31" governs
         both; the book repeats a date per clause when it means them apart (Findlay Creek:
         "…, June 15-Oct 31; bait ban, June 15-Oct 31"). A `;` or a list comma is not this shape.
    A lift-only rule is exempt from 2-4 (its dates are the lifted rule's), and so is a rule whose
    own sentence prints "all year" (`_own_dates_carried`)."""
    e: List[str] = []
    # THE ROW, SQUASHED LINE BY LINE, so a line break is still visible: `breaks` are the offsets
    # in `hay` where one printed line ends and the next begins.
    hay, breaks = "", []
    for line in entry.regs_verbatim.split("\n"):
        got = squash(line)
        if got:
            if hay:
                breaks.append(len(hay))
                hay += " "
            hay += got
    if _CUT_DATE.search(hay):
        e.append(f"the row ends in a month with no day ({hay[-12:]!r}) — a date cut off; restore "
                 f"it from the page")
    whens: list = []
    _whens_in(list(entry.rules) + list(entry.licensing), whens)
    carried = [d for w in whens for d in w.dates]
    year = _year_days()
    lifts = [printed_ranges(r.verbatim) for r in entry.rules if r.lift_only]
    for d in printed_ranges(entry.regs_verbatim):
        key = (entry.entry_id, d.words())
        if d in carried or any(d in x for x in lifts) or key in WINDOWS_HELD_ELSEWHERE:
            continue
        if any(w.dates and _days(list(w.dates)) == year - _days([d]) for w in whens):
            continue
        e.append(f"the row prints {d.words()} and no rule or licensing record carries it — a "
                 f"season that reaches no `when` is lost, and the rule it governed reads all year")
    for k, why in WINDOWS_HELD_ELSEWHERE.items():
        if k[0] == entry.entry_id and any(d.words() == k[1] for d in carried):
            e.append(f"WINDOWS_HELD_ELSEWHERE lists {k[1]} for this row, but a rule now carries "
                     f"it — take it off the list")

    def dated(r) -> bool:
        # PRINTED DAYS, not any `when`: an hours- or weekday-only `when` is not a season a
        # sibling can have lost ("Youth/Disabled Accompanied Water" is one quote for two rules).
        return r.when is not None and bool(r.when.dates)

    spans = {r.rule_id: _unique_span(hay, squash(r.verbatim)) for r in entry.rules}
    # 5. A DATE DOES NOT CROSS A SEMICOLON (user ruling, 2026-09-25). "Fly fishing only; bait ban
    #    upstream of …, Jul 1-Oct 31" dates the bait ban; "catch and release; bait ban, June 15-Oct
    #    31" dates the bait ban. The clause on the other side of the `;` is its own, so a rule
    #    whose clause (from the `;` before it to the `;` after it, on its printed line) prints no
    #    date may not carry dates that line prints only in ANOTHER clause.
    for r in entry.rules:
        mine = spans[r.rule_id]
        if not dated(r) or r.lift_only or mine is None or printed_ranges(squash(r.verbatim)):
            continue
        start = max([b for b in breaks if b <= mine[0]] + [0])
        end = min([b for b in breaks if b >= mine[1]] + [len(hay)])
        line = hay[start:end]
        if ";" not in line:
            continue
        a, b = mine[0] - start, mine[1] - start
        lo = line.rfind(";", 0, a) + 1
        hi = line.find(";", b)
        hi = len(line) if hi < 0 else hi
        if printed_ranges(line[lo:hi]):
            continue
        others = printed_ranges(line[:lo] + " " + line[hi:])
        if others and all(d in others for d in r.when.dates):
            e.append(f"{r.rule_id}: its `when` ({r.when.words()}) is printed in another clause of "
                     f"its line, across a `;` — a date after a `;` belongs to its own clause, not "
                     f"to '{squash(r.verbatim)[:30]}'")
    for r in entry.rules:
        if dated(r) or r.lift_only or _ALL_YEAR_SAID.search(squash(r.verbatim)):
            continue
        mine = spans[r.rule_id]
        if mine is None:
            continue
        for s in entry.rules:
            other = spans[s.rule_id]
            if s is r or other is None or not dated(s):
                continue
            if mine[0] < other[1] and other[0] < mine[1]:
                e.append(f"{r.rule_id}: its words are part of {s.rule_id}'s sentence, which holds "
                         f"on {s.when.words()} — it has no `when`, so it reads all year")
        after = hay[mine[1]:]
        gap = _BARE_GAP.match(after)
        if gap and gap.end() < len(after) and printed_ranges(after[gap.end():gap.end() + 25]) \
                and _RANGE.match(after[gap.end():]):
            e.append(f"{r.rule_id}: the row prints a date straight after its words "
                     f"({after[gap.end():gap.end() + 20]!r}) and it has no `when`")
            continue
        # A CHAIN OF CLAUSES ENDING IN ONE DATE: walk the siblings that follow on the same
        # printed line, joined by "and", ",", "(" / ")" or nothing, to the first text that is not
        # a sibling. The chain's date is the one standing AFTER it — or, when the last clause was
        # joined by "and", the one closing that clause's own words ("trout/char catch and release
        # and bait ban, June 15-Aug 31"). If a sibling in the chain carries that date, the date
        # governs the whole chain and this rule has lost it. A date that closes a clause joined by
        # a COMMA is that clause's own: "ALL STEELHEAD, Bull trout from streams, Aug 1-Oct 31" is
        # a list of releases, and the date is the bull trout's. A `;` ends the chain, as does a
        # line break (the next printed line is the next regulation).
        at, chain, via_and = mine[1], [], False
        while True:
            joined = _JOIN_NEXT.match(hay, at)
            if any(at <= b < joined.end() for b in breaks):
                break
            nxt = next((s for s in entry.rules if s is not r and s not in chain
                        and spans[s.rule_id] is not None
                        and spans[s.rule_id][0] == joined.end()), None)
            if nxt is None:
                break
            chain.append(nxt)
            via_and = " and" in f" {joined.group(0)}"
            at = spans[nxt.rule_id][1]
        if not chain:
            continue
        g = _BARE_GAP.match(hay, at)
        m = _RANGE.match(hay, g.end())
        final = printed_ranges(m.group(0)) if m else []
        if not final and via_and:
            final = printed_ranges(hay[spans[chain[-1].rule_id][0]:at][-25:])[-1:]
        if final and any(dated(s) and final[0] in s.when.dates for s in chain):
            e.append(f"{r.rule_id}: '{squash(r.verbatim)[:30]}… {squash(chain[-1].verbatim)[:30]}"
                     f"…, {final[0].words()}' — the date governs every clause before it, and "
                     f"{r.rule_id} has no `when`")
    return e


def _extents_the_resolver_reads(extents: Optional[List[dict]]) -> List[str]:
    """A rule's or a licensing record's extent may not carry a flag the reach builder never reads.

    `includes_tributaries` INSIDE an extent is one: `classify.wants_tributaries` reads the flag on
    the rule or record (inheriting the entry's), never on an extent, so "the Fraser River Watershed
    (including tributaries)" written that way bound the mainstem alone and said nothing. Nineteen
    rules were in that state — every watershed quota and closure in Zones 5, 6, 7A and 7B. The
    flag goes on the rule or record, where the builder walks it."""
    return [f"extent {i} carries includes_tributaries, which the reach builder does not read on "
            f"an extent — set it on the rule or record" for i, x in enumerate(extents or [])
            if isinstance(x, dict) and "includes_tributaries" in x]


#: A LIST MARKER at the head of a phrase — "3.", "4)", "(b)", "(iv)", "• ", "– ". The book prints
#: them on its numbered lists, but they are layout, never the regulation: no VERBATIM may start
#: with one (user ruling 2026-09-25 — the two standing no-fishing buffers read "3. Within 23 m …"
#: on every water; `CatalogueEntry._chain_of_custody`), nor a generated label, nor a place named in
#: `extent_text` — "No fishing — (b) Chimdemash Creek" is a list item, not a place.
LIST_MARKER = re.compile(r"^\s*(\d{1,2}[.)]\s|\([a-z0-9ivx]{1,3}\)\s*|[•–-]\s)")


def strip_list_marker(text: str) -> str:
    """`text` without a leading list marker (see `LIST_MARKER`)."""
    return LIST_MARKER.sub("", text or "", count=1).strip()


def bare_whole(extents: Optional[List[dict]]) -> bool:
    """Extents that say "the whole water" and nothing else — every one `{"op": "whole"}`, with no
    item, area, feature or tributary qualifier. A `whole` that names an item (`item_id`), an area
    (`within_area`) or a kind (`feature_types`) is a place of its own, and its `extent_text` only
    describes it."""
    return bool(extents) and all(isinstance(x, dict) and x.get("op") == "whole"
                                 and set(x) == {"op"} for x in extents)


def _days(ranges: List["DateRange"]) -> set:
    got: set = set()
    for r in ranges:
        a, b = _day_index(r.from_month, r.from_day), _day_index(r.to_month, r.to_day)
        got.update(range(a, b + 1) if a <= b else list(range(a, 367)) + list(range(1, b + 1)))
    return got


_CLASS_SAID = re.compile(r"\bclass (ii|i|1|2) waters?\b")
_WAIVED_SAID = re.compile(r"steelhead stamp not (?:required|mandatory)")
_DURING_SAID = re.compile(r"steelhead stamp (?:is )?mandatory|until\s+reopened")
_UNIT_SAID = re.compile(r"([a-z][a-z .']*?) classified licence required for non-resident")


class Designation(_Terse):
    """A FACT ABOUT A WATER: while the date is in `when` and a section is bound, that section is a
    Classified Water of class `classified`, in licence unit `unit`.

    It obliges nothing by itself. The provincial requirement ("hold a Classified Waters Licence
    when fishing on a stream during the period when it is classified") fires on it, and so does the
    classified-water steelhead stamp during `steelhead_stamp_during`.

    `steelhead_stamp_waived` LIFTS ONLY THAT STAMP. The provincial "stamp if you fish for steelhead"
    is a different requirement with a different trigger, so the waiver has no field that could
    reach it — which is the Chilko/Dean/Horsefly/Skeena-2 defect, made unwriteable.

    `unit` is what a non-resident's per-day licence NAMES. "Class II water when open, including
    tributaries - Michel Creek classified licence required for non-resident anglers" made two
    claims — everyone needs a CWL here (the water is classified), and the non-resident's licence
    says "Michel Creek" — and was stored as one rule scoped to non-residents, which told B.C.
    residents they needed nothing on nine Kootenay waters.
    """
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    kind: Literal["designation"] = "designation"
    id: str = Field(..., min_length=1)
    classified: Literal["I", "II"]
    unit: str = Field(..., pattern=_SLUG)
    unit_name: str = Field(..., min_length=1)
    #: The classified period. Absent or empty = all year — which includes "when open".
    when: Optional[When] = None
    extents: Optional[List[dict]] = None
    includes_tributaries: Optional[bool] = None
    tributaries_only: bool = False
    tributary_excludes: List[dict] = Field(default_factory=list)
    #: AT MOST ONE OF THESE TWO. Direction lives in the key; there is no bool.
    steelhead_stamp_during: Optional[StampPeriod] = None
    steelhead_stamp_waived: Optional[Quote] = None
    suspended_while: List[Suspension] = Field(default_factory=list)
    verbatim: str = Field(..., min_length=1)
    review_reason: str = ""

    @model_validator(mode="after")
    def _check(self) -> "Designation":
        e: List[str] = []
        during, waived = self.steelhead_stamp_during, self.steelhead_stamp_waived
        if during and waived:
            e.append("steelhead_stamp_during and steelhead_stamp_waived are opposite facts — "
                     "one of them, or neither")
        # THE STAMP PERIOD SITS INSIDE THE CLASSIFIED PERIOD, on the circular year. The stamp
        # is a consequence of the water being classified; outside that period there is nothing
        # for it to be a consequence of.
        if during and during.when.dates and self.when and self.when.dates:
            if not _days(during.when.dates) <= _days(self.when.dates):
                e.append(f"the stamp period ({during.when.words()}) runs outside the classified "
                         f"period ({self.when.words()})")
        # IDIOMS SEEDED FROM THE VERBATIM. The quote is copied, not authored, so a field that
        # disagrees with it is the field that is wrong.
        m = _CLASS_SAID.search(squash(self.verbatim))
        if m:
            said = {"i": "I", "1": "I", "ii": "II", "2": "II"}[m.group(1)]
            if said != self.classified:
                e.append(f"verbatim says Class {said}, classified is {self.classified}")
        if during and during.when.is_empty():
            # AN EMPTY PERIOD IS "ALL YEAR" under `When`'s own rule, so an empty stamp period
            # would put the stamp on the water every day of the year — while the page printed a
            # window, or printed that it could not say. A period the page does not give is
            # written in `unparsed` (the Atnarko's "from reopening"), never left empty.
            e.append("steelhead_stamp_during.when is empty, which reads as all year — give the "
                     "printed dates, or say in `unparsed` why there are none")
        if during and not _DURING_SAID.search(squash(during.verbatim)):
            e.append("steelhead_stamp_during quotes no 'Steelhead Stamp mandatory'")
        if during and _WAIVED_SAID.search(squash(during.verbatim)) \
                and "until reopened" not in squash(during.verbatim):
            e.append("steelhead_stamp_during quotes a waiver — that is steelhead_stamp_waived")
        if waived and (not _WAIVED_SAID.search(squash(waived.verbatim))
                       or "until reopened" in squash(waived.verbatim)):
            e.append("steelhead_stamp_waived must quote 'Steelhead Stamp not required/mandatory' "
                     "(and a waiver 'until reopened' is a suspension, not a waiver)")
        quoted = " ".join([self.verbatim] + [x.verbatim for x in (during, waived) if x]
                          + [s.verbatim for s in self.suspended_while])
        if "until reopened" in squash(quoted) and not self.suspended_while:
            e.append("'until reopened' is a suspension — name the closure in suspended_while")
        for err in (_dates_are_printed(self.when, self.verbatim, "when"),
                    _dates_are_printed(during.when if during else None,
                                       during.verbatim if during else "",
                                       "steelhead_stamp_during.when")):
            if err:
                e.append(err)
        m = _UNIT_SAID.search(squash(self.verbatim))
        if m and slug(m.group(1).split(" - ")[-1]) != self.unit:
            e.append(f"verbatim names the {m.group(1).strip()!r} licence; unit is {self.unit!r}")
        if self.tributaries_only and self.includes_tributaries is False:
            e.append("tributaries_only with includes_tributaries: false binds nothing")
        e += _extents_the_resolver_reads(self.extents)
        if e:
            raise ValueError(f"designation {self.id}: " + "; ".join(e))
        return self


class NotClassified(_Terse):
    """"Part described is NOT a Classified Water" — an asserted ABSENCE. It obliges nothing; the
    build (a later stage) refuses any designation that binds a section this binds, which is what
    forces the carve-out the parent's "including tributaries" implies to be written down."""
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    kind: Literal["not_classified"] = "not_classified"
    id: str = Field(..., min_length=1)
    extents: Optional[List[dict]] = None
    verbatim: str = Field(..., min_length=1)
    review_reason: str = ""

    @model_validator(mode="after")
    def _said(self) -> "NotClassified":
        if "not a classified water" not in squash(self.verbatim):
            raise ValueError(f"not_classified {self.id}: the verbatim does not say "
                             f"'not a Classified Water'")
        e = _extents_the_resolver_reads(self.extents)
        if e:
            raise ValueError(f"not_classified {self.id}: " + "; ".join(e))
        return self


class Doing(_Terse):
    """WHAT THE ANGLER IS DOING — the trigger. A closed vocabulary:

      fishing             any sport fishing at all
      targeting           fishing FOR `species` ("if you fish for steelhead … keep or release")
      retaining           KEEPING `species`, of `lengths` when given ("to keep rainbow trout over
                          50 cm") — a stamp you need only if you keep the fish
      retaining_recorded  keeping a fish whose retention must be recorded on the licence; which
                          fish those are is said by the `record_retention` rules, not here
      guiding             acting as a guide for fish
    """
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    act: Literal["fishing", "targeting", "retaining", "retaining_recorded", "guiding"]
    species: List[str] = Field(default_factory=list)
    species_except: List[str] = Field(default_factory=list)
    origin: Optional[Origin] = None
    lengths: Optional[List[LengthBand]] = None

    @model_validator(mode="after")
    def _check(self) -> "Doing":
        e: List[str] = []
        if self.act in ("targeting", "retaining") and not self.species:
            e.append(f"{self.act} needs species — fishing FOR what, keeping what")
        if self.species and self.act not in ("targeting", "retaining"):
            e.append(f"species belongs to targeting or retaining, not {self.act}")
        e += species_problems(set(self.species) | set(self.species_except), "doing.species")
        # AN EXCEPTION THAT SUBTRACTS NOTHING is a claim the data cannot make good on: "a salmon
        # … (other than kokanee)" was `species: [SALMON], species_except: [KO]`, and kokanee
        # is not in SALMON. The sentence is right (it is not a salmon for this purpose); the
        # except was noise that read as a carve-out.
        if self.species_except:
            inside = set(expand_species(list(self.species)))
            idle = [s for s in self.species_except if s not in inside]
            if idle:
                e.append(f"species_except {idle} is not in {self.species} — it subtracts nothing")
        if self.lengths is not None:
            if self.act != "retaining":
                e.append("lengths names WHICH FISH you keep — only with act: retaining")
            if not self.lengths:
                e.append("lengths: [] says nothing — leave it out")
            if any(b.take is not None for b in self.lengths):
                e.append("a band on a requirement names which fish, never how many — no `take`")
        if self.origin is not None and not self.species:
            e.append("origin qualifies a species")
        if e:
            raise ValueError("doing: " + "; ".join(e))
        return self


class Accompaniment(_Terse):
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)
    who: Who
    #: The companion holds whatever THIS fishing requires of them — resolved by the reader, so
    #: the record never has to list documents it cannot know (class, stamp period, species).
    holding: Literal["what_this_fishing_requires"] = "what_this_fishing_requires"


class Path(_Terse):
    """ONE WAY TO SATISFY A REQUIREMENT. Exactly one of `hold` (ALL of these documents),
    `accompanied_by`, or `as` (satisfy the requirements AS this who instead).

    `quota` is a NOTE, not arithmetic: "any fish you keep must be counted as part of the catch and
    possession of your accompanying licence holder" is `quota: counts_to_companion`, which a reader
    renders (an asterisk, a line). The retention model does not aggregate across anglers."""
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    hold: List[Document] = Field(default_factory=list)
    accompanied_by: Optional[Accompaniment] = None
    as_: Optional[Who] = Field(default=None, alias="as")
    quota: Optional[Literal["own", "counts_to_companion"]] = None

    @model_validator(mode="after")
    def _one(self) -> "Path":
        ways = [bool(self.hold), self.accompanied_by is not None, self.as_ is not None]
        if sum(ways) != 1:
            raise ValueError("a path is exactly one of hold / accompanied_by / as")
        if len(set(self.hold)) != len(self.hold):
            raise ValueError(f"hold names a document twice: {self.hold}")
        # A PATH THAT CHANGES WHO CARRIES THE FISH MUST SAY WHOSE QUOTA IT IS.
        if self.hold and self.quota is not None:
            raise ValueError("quota belongs to a path that changes who carries the fish, "
                             "not to `hold`")
        if not self.hold and self.quota is None:
            raise ValueError("an accompanied_by or as path must say whose quota the catch is "
                             "(own | counts_to_companion)")
        return self


class Requirement(_Terse):
    """AN OBLIGATION, stated once: this `who`, `doing` this, `when` and where, must satisfy ANY ONE
    of `satisfied_by` — or, for a duty, do the `conduct`.

    WHERE is `extents` (None takes the entry's, at placement — `reach.licensing.place_record`;
    never `whole` by default. A RULE, unlike a record, always states its own) and/or
    `on`: `classified_period` is met wherever a designation is in force on a stream,
    `steelhead_period` wherever its `steelhead_stamp_during` also holds. Both together means both.

    There is no `required` and no `permitted`. A requirement that does not apply to someone is
    `who` / `who_except`, an `Exemption`, or a designation fact — never "not required" as a value.

    `restates` marks the table's own words for an obligation stated elsewhere (Shuswap Lake's row
    repeating the provincial stamp) — kept so the water screen shows what the page printed, and
    pinned by a corpus test to add nothing to what it restates.

    `presumes` is for a DUTY ABOUT A DOCUMENT — "produce your angling licence", "carry your paper
    licence". The duty binds only an angler who must hold the documents it names, so an `Exemption`
    that releases all of them releases the duty too. Without it the duty named no document and an
    Indian resident of B.C., released from every licence, was still told to produce one.
    """
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    kind: Literal["requirement"] = "requirement"
    id: str = Field(..., min_length=1)
    satisfied_by: List[Path] = Field(default_factory=list)
    conduct: List[str] = Field(default_factory=list)
    #: The documents a `conduct` duty is ABOUT. Named in the sentence, checked against it
    #: (`_PRESUMED_SAID`); required for an act registered in `DOCUMENT_ACTS`.
    presumes: List[Document] = Field(default_factory=list)
    who: Optional[Who] = None
    who_except: Optional[Who] = None
    doing: Doing
    extents: Optional[List[dict]] = None
    includes_tributaries: Optional[bool] = None
    water: Optional[WaterKind] = None
    on: Optional[Literal["classified_period", "steelhead_period"]] = None
    authority: Optional[Literal["superior"]] = None
    when: Optional[When] = None
    restates: Optional[Ref] = None
    verbatim: str = Field(..., min_length=1)
    review_reason: str = ""

    @model_validator(mode="after")
    def _check(self) -> "Requirement":
        e: List[str] = []
        if bool(self.satisfied_by) == bool(self.conduct):
            e.append("exactly one of satisfied_by (what to hold) or conduct (what to do)")
        for act in self.conduct:
            if act not in CONDUCT_ACTS:
                e.append(f"conduct: {act!r} is not a registered act")
        # A DUTY ABOUT A DOCUMENT NAMES IT, and only a duty does: a requirement to HOLD documents
        # already names them in `satisfied_by`.
        if self.presumes and not self.conduct:
            e.append("presumes belongs to a conduct duty — a requirement to hold documents names "
                     "them in satisfied_by")
        if len(set(self.presumes)) != len(self.presumes):
            e.append("presumes names a document twice")
        bound = sorted(a for a in self.conduct if a in DOCUMENT_ACTS)
        if bound and not self.presumes:
            e.append(f"conduct {bound} is a duty about a document — name it in `presumes`, or an "
                     f"exemption from that document cannot release the duty")
        said = squash(self.verbatim)
        for d in self.presumes:
            pat = _PRESUMED_SAID.get(d.value)
            if pat is None or not re.search(pat, said):
                e.append(f"presumes {d.value!r}, which the sentence does not name "
                         f"({self.verbatim[:60]!r})")
        if self.who_except is not None and self.who is not None \
                and not self.who.overlaps(self.who_except):
            e.append("who_except does not overlap who — it subtracts nothing")
        if self.authority == "superior":
            prov = [d.value for p in self.satisfied_by for d in p.hold
                    if d.value in PROVINCIAL_ANGLER_DOCUMENTS]
            if prov:
                e.append(f"a superior authority's requirement cannot be met by a provincial "
                         f"document ({prov})")
        err = _check_residency(self.who, self.verbatim, "who")
        if err:
            e.append(err)
        e += _extents_the_resolver_reads(self.extents)
        if e:
            raise ValueError(f"requirement {self.id}: " + "; ".join(e))
        return self


class LicenceTerms(_Terse):
    """HOW A DOCUMENT IS SOLD — rendered under the obligation it goes with, NEVER bound to a
    section as one. The Dean's non-guided-alien draw was a `document_required` bound to all 76 Dean
    sections, the Class II upper river included; as terms it attaches only where its unit is."""
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    kind: Literal["licence_terms"] = "licence_terms"
    id: str = Field(..., min_length=1)
    document: Document
    who: Optional[Who] = None
    classified: Optional[Literal["I", "II"]] = None
    #: Licence units these terms are about; empty = every unit.
    units: List[str] = Field(default_factory=list)
    sold: Optional[Literal["per_licence_year", "per_day"]] = None
    covers: Optional[Literal["every_unit", "one_unit"]] = None
    max_consecutive_days: Optional[int] = Field(default=None, gt=0)
    max_days_per_licence_year: Optional[int] = Field(default=None, gt=0)
    max_per_licence_year: Optional[int] = Field(default=None, gt=0)
    max_units_per_licence_year: Optional[int] = Field(default=None, gt=0)
    #: NO DAY LIMIT, SAID OUTRIGHT — "There are no limits on the number of days which a Canadian
    #: resident may fish". JSON has no infinity, so the word is the value, as `unlimited` is on a
    #: retention rule; never alongside a day limit.
    unlimited_days: bool = False
    allocation: Optional[Literal["open", "booking", "draw"]] = None
    needs: List[Literal["angling_guide_number"]] = Field(default_factory=list)
    fee_cad: Optional[float] = Field(default=None, gt=0)
    verbatim: str = Field(..., min_length=1)
    review_reason: str = ""

    @model_validator(mode="after")
    def _check(self) -> "LicenceTerms":
        e: List[str] = []
        terms = (self.sold, self.covers, self.max_consecutive_days,
                 self.max_days_per_licence_year, self.max_per_licence_year,
                 self.max_units_per_licence_year, self.allocation, self.fee_cad)
        if all(t is None for t in terms) and not self.needs and not self.unlimited_days:
            e.append("terms that set nothing say nothing")
        if self.unlimited_days and (self.max_consecutive_days or self.max_days_per_licence_year):
            e.append("unlimited_days and a day limit contradict")
        if len(set(self.units)) != len(self.units):
            e.append(f"units named twice: {self.units}")
        for u in self.units:
            if not re.match(_SLUG, u):
                e.append(f"unit {u!r} is not a unit id")
        # "COVERS EVERY UNIT" ABOUT SOME UNITS contradicts itself: the resident's annual licence
        # covers every classified water, and naming units under it would attach it only where
        # those units are — the per-day licence's shape under the annual licence's words.
        if self.covers == "every_unit" and self.units:
            e.append(f"covers: every_unit names units {self.units} — a licence that covers every "
                     f"unit is not about some of them")
        # "ONE ANNUAL LICENCE PER LICENCE YEAR" is about the ANNUAL licence. Without `sold` the
        # count reads as every basic licence, and a visitor may buy as many one-day licences as
        # they like.
        if _ANNUAL_SAID.search(squash(self.verbatim)) and self.sold != "per_licence_year":
            e.append("the sentence is about an annual licence — sold: per_licence_year")
        err = _check_residency(self.who, self.verbatim, "who")
        if err:
            e.append(err)
        if e:
            raise ValueError(f"licence_terms {self.id}: " + "; ".join(e))
        return self


class Exemption(_Terse):
    """A NAMED WHO RELEASED FROM NAMED DOCUMENTS. "you are not required to obtain any type of
    fishing licence or stamp" released only `basic_licence` when it was a `required: false` rule,
    and the CWL and stamps went on applying.

    It releases the `documents` AND every duty whose `presumes` are all among them ("produce your
    angling licence" means nothing to an angler who need not hold one). The angler is unknown, so
    a reader renders the released duty conditionally — "unless you are …" — never drops it."""
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    kind: Literal["exemption"] = "exemption"
    id: str = Field(..., min_length=1)
    who: Who
    documents: List[Document] = Field(..., min_length=1)
    verbatim: str = Field(..., min_length=1)
    review_reason: str = ""

    @model_validator(mode="after")
    def _check(self) -> "Exemption":
        e: List[str] = []
        if len(set(self.documents)) != len(self.documents):
            e.append("a document is named twice")
        # CHECKED AGAINST ITS OWN SENTENCE, like every other `who`. An exemption is the one record
        # that REMOVES an obligation, so a `who` wider than the sentence releases anglers the book
        # never released. A status that implies a residency ("an Indian AND a resident of B.C." is
        # `indian_bc_resident`) satisfies the sentence's residency without restating it.
        t = squash(self.verbatim)
        said_status = frozenset(s for s, pat in _STATUS_SAID if re.search(pat, t))
        if said_status != frozenset(self.who.status):
            e.append(f"who.status {sorted(self.who.status) or 'none'} is not what the sentence "
                     f"names ({sorted(said_status) or 'none'})")
        implied = frozenset().union(*(_STATUS_RESIDENCY.get(s, frozenset())
                                      for s in self.who.status))
        said = residency_said(self.verbatim)
        have = frozenset(self.who.residency) or implied
        if said is not None and said != have:
            e.append(f"who.residency {sorted(have) or 'any'} is not what the sentence says "
                     f"({sorted(said)})")
        if said is None and self.who.residency:
            e.append(f"who.residency {sorted(self.who.residency)} is named nowhere in the sentence")
        g = guidance_said(self.verbatim)
        if (frozenset(self.who.guidance) or None) != (None if g is None or len(g) == 2 else g):
            e.append(f"who.guidance {sorted(self.who.guidance) or 'any'} is not what the "
                     f"sentence says")
        # "ANY TYPE of fishing licence or stamp" is every provincial angler document — a list
        # that leaves one out keeps charging the angler the book released.
        if _ANY_DOCUMENT_SAID.search(t) and \
                {d.value for d in self.documents} != set(PROVINCIAL_ANGLER_DOCUMENTS):
            e.append("the sentence releases ANY licence or stamp — documents must be every "
                     "provincial angler document")
        if e:
            raise ValueError(f"exemption {self.id}: " + "; ".join(e))
        return self


class Alternative(_Terse):
    """A PLACE WHERE ANOTHER DOCUMENT ALSO SATISFIES A REQUIREMENT — "B.C. and Yukon angling
    licences are valid on all parts of Morley Lake". It can only ADD a path, never remove one, and
    it must be place-scoped: a scopeless alternative applies everywhere, which is the Babine
    failure on a new axis."""
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    kind: Literal["alternative"] = "alternative"
    id: str = Field(..., min_length=1)
    alternative_to: Ref
    satisfied_by: List[Path] = Field(..., min_length=1)
    extents: List[dict] = Field(..., min_length=1)
    verbatim: str = Field(..., min_length=1)
    review_reason: str = ""

    @model_validator(mode="after")
    def _a_place(self) -> "Alternative":
        # `within` EVERY REGION is a scope in form and everywhere in fact — the scopeless
        # alternative the non-empty `extents` exists to refuse, spelled so that it passes.
        e = [f"extent {i} is the whole province — an alternative is accepted somewhere, not "
             f"everywhere" for i, x in enumerate(self.extents)
             if x.get("op") == "within" and x.get("area_kind") == "region"
             and not x.get("area_id")]
        e += _extents_the_resolver_reads(self.extents)
        if e:
            raise ValueError(f"alternative {self.id}: " + "; ".join(e))
        return self


#: Who a sentence names by STATUS, and the residency that status already carries.
_STATUS_SAID = (("indian_bc_resident", r"\bindians?\b"), ("metis", r"\bm[ée]tis\b"),
                ("disabled", r"\bdisab"))
_STATUS_RESIDENCY = {"indian_bc_resident": frozenset({"resident"})}
_ANY_DOCUMENT_SAID = re.compile(r"any (?:type of )?(?:fishing )?licen[cs]e or stamp")
_ANNUAL_SAID = re.compile(r"\bannual (?:\w+ )?licen[cs]e")


LicensingRecord = Annotated[
    Union[Designation, NotClassified, Requirement, LicenceTerms, Exemption, Alternative],
    Field(discriminator="kind")]


def licensing_verbatims(rec) -> List[str]:
    """Every quote a record carries — each must be a contiguous run of the entry's passage."""
    out = [rec.verbatim]
    if isinstance(rec, Designation):
        out += [x.verbatim for x in (rec.steelhead_stamp_during, rec.steelhead_stamp_waived) if x]
        out += [s.verbatim for s in rec.suspended_while]
    return out


class CatalogueRule(BaseModel):
    """One regulation, typed. `verbatim` is the synopsis sentence and is REQUIRED — the generated
    label is a summary and never a replacement, so the words the law used must always be reachable.
    """
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    rule_id: str
    type: RuleType
    verbatim: str = Field(..., min_length=1, description="the synopsis sentence, exactly")

    @model_validator(mode="after")
    def _acts_are_registered(self) -> "CatalogueRule":
        for act in self.conduct:
            if act not in CONDUCT_ACTS:
                raise ValueError(f"conduct: {act!r} is not a registered act — adding one is a "
                                 f"reviewed change that carries the verbatim motivating it")
        return self

    @model_validator(mode="after")
    def _ordered_within_a_slot(self) -> "CatalogueRule":
        """A clause with no condition matches everything, so nothing after it on that slot is ever
        reached. Writing the general case first silently deletes its own exception."""
        seen_open: set = set()
        for c in self.gear:
            if c.slot in seen_open:
                raise ValueError(
                    f"{c.slot.value}: a clause follows one that has no `when`, so it can never be "
                    f"reached — put the narrow case first and the general case last")
            if c.when is None or c.when.is_empty():
                seen_open.add(c.slot)
        return self

    @model_validator(mode="after")
    def _circumstances_are_real(self) -> "CatalogueRule":
        for w in self.while_:
            if w not in WHILE_TOKENS:
                raise ValueError(f"while: {w!r} is neither a means of fishing "
                                 f"({sorted(WHILE_MEANS)}) nor a device ({sorted(WHILE_DEVICES)})")
        # AN EXEMPTION WITH NO CIRCUMSTANCE LIFTS EVERYWHERE. The same argument that refuses
        # `ban: []` — an empty list is what a dropped key also writes — applies here, and the
        # stakes are higher: a lift that should have been narrow and is not is the Babine failure.
        # A LIFT MUST BE SCOPED BY SOMETHING. A PLACE counts: `zp:set_lining.r1` permits set
        # lining in the lakes of Region 6 and 7A and lifts the province-wide ban, and its extents
        # are what narrow it — it needs no circumstance because it names where instead. What is
        # refused is a lift scoped by NOTHING, which applies everywhere and deletes the rule it
        # was meant to narrow. That is the Babine failure, and it cost that river a season.
        if self.exempts and self.gear and not self.while_ and not self.extents:
            if not any(c.when is not None and not c.when.is_empty() for c in self.gear):
                raise ValueError("a gear rule that exempts another must say WHERE or WHEN it "
                                 "lifts it — `extents`, `while`, or a `when` on the clause; a "
                                 "lift scoped by nothing applies everywhere and deletes the rule "
                                 "it was meant to narrow")
        return self

    obligation: Obligation = Obligation.must

    # --- who / what / when -------------------------------------------------
    species: List[str] = Field(default_factory=list)
    species_except: List[str] = Field(default_factory=list)
    #: angler_closure only: WHO the water is closed to. The key carries the direction, as `ban`
    #: does in gear — there is no `permitted` bit. `angler_class` (one residency, a `guided`
    #: polarity bit) is gone and refused; see `Who`.
    closed_to: Optional[Who] = None
    #: angler_closure only: the anglers INSIDE `closed_to` the water stays open to. A Youth/Disabled
    #: Accompanied Water is closed to anglers 16 and over EXCEPT disabled B.C. residents and the
    #: companions of an authorized angler — two exceptions no single `Who` can subtract, because a
    #: `Who` is a conjunction of axes. The direction is in the key, as `closed_to`'s is; each
    #: exception must meet `closed_to`, or it subtracts nothing.
    closed_to_except: List[Who] = Field(default_factory=list)
    #: HOW YOU MAY FISH. Clauses on DIFFERENT slots are unordered and all apply — they constrain
    #: different things and never compete. Clauses on the SAME slot are ORDERED and FIRST MATCH
    #: WINS, which is the one place an exception can live inside the rule it modifies.
    #:
    #: "It is unlawful to angle with more than one line, EXCEPT a person who is alone in a boat on
    #: a lake may angle with two lines" is ONE printed sentence. Stored as two rules it produced
    #: two records with BYTE-IDENTICAL extents, so the scope ladder — which orders by place — could
    #: not rank them, and something unwritten decided which won. Stored as two bounds on
    #: `lines_per_angler`, the narrow one first, the general one last, there is nothing to rank
    #: and the general case cannot be lost.
    #:
    #: A conditionless clause on a slot is therefore the LAST word on it: anything after it is
    #: unreachable. See `GearClause`. This replaced `allowed`, `barbless`, `hook_count`,
    #: `lure`, `bait`, `max_lines`, `max_flies`, `max_weight_kg`, `min_gap_cm` and `max_gap_mm`,
    #: which are gone from the model and refused on load.
    #: See pipeline/docs/07-gear-representation.md.
    gear: List[GearClause] = Field(default_factory=list)
    #: A RULE THE BOOK ASSERTS BUT DOES NOT PRINT AS ITS OWN CLAUSE, naming the rule it was read
    #: out of. "You may ONLY fish with a set line in lakes of Region 6 and Region 7A" makes two
    #: claims: a permission in those lakes, which is printed, and a ban everywhere else, which is
    #: the word "only" and is printed nowhere. The corpus stored just the permission, so set
    #: lining in a Region 5 lake was unconstrained in the data and unlawful in the book.
    #:
    #: The second half has to be authored, and a reader auditing against the synopsis would
    #: otherwise find a rule with no source text. This says which sentence licensed it. Such a
    #: rule carries its source's `verbatim`, as every other rule split from one sentence does.
    derived_from: Optional[str] = None

    #: THE OTHER HALF OF ONE SENTENCE, BY ID. "You may use a downrigger, PROVIDED the line has a
    #: quick-release" is two rules — one allows the means, one holds the condition — and the only
    #: thing linking them used to be that they shared a `verbatim`. Two unrelated rules share a
    #: verbatim by accident: `z7b:single_barbless_hook.r1` and `z7b:bait.r1` both read "all
    #: streams of Zone B, all year." and are a hook rule and a bait ban. Matching on the string
    #: glues them together.
    #:
    #: A rule id cannot collide by accident, and the corpus already names a parent this way —
    #: a sub-quota carries `within: "trout_quota.r1"`.
    condition_of: Optional[str] = None

    #: WHAT YOU ARE DOING, for a clause that binds only then. Drawn from `WHILE_TOKENS`: a MEANS
    #: (a `Method` member) or a DEVICE in use (`downrigger`, `light`) — see the note there.
    #: A LIFT WHOSE CIRCUMSTANCE IS NOT MET IS NOT APPLIED, which is the Babine rule on a new
    #: axis: "dead fin fish when set lining" applied everywhere deleted the province-wide fin
    #: fish ban and a water's bait tile went from "banned" to "no rule at all".
    while_: List[str] = Field(default_factory=list, alias="while")

    #: ACTS — what you must and must not DO, kept apart from gear so a duty is never stored as a
    #: permission. "Set lines must be marked with angler's name, address, and telephone number"
    #: was `{permitted: true, reason: "marked with…"}`: a duty demoted to free text with a grant
    #: invented to house it.
    conduct: List[str] = Field(default_factory=list)

    #: WHEN THIS RULE BINDS, said once — see `When`. This replaces `windows` (210 distinct free
    #: text strings), `windows_are` (a flag that INVERTED the field beside it, the `band`
    #: failure), `from_time`, `to_time` and `weekdays`. All five are now REFUSED (`extra=forbid`):
    #: a rule still spelling its season the old way is sent back, not converted.
    when: Optional[When] = None
    #: `when_open` WAS HERE: "Artificial fly only, where open". It said nothing — a restriction or a
    #: licence only matters while the water is open for fishing at all, so every rule holds "while
    #: open" by construction. It is REFUSED on load (`extra=forbid`); the words stay in `verbatim`.

    # --- retention ---------------------------------------------------------
    take: Optional[int] = None
    unlimited: bool = False
    may_target: Optional[bool] = None
    #: THE CLOCK A NUMBER RUNS ON — only on the two types that count fish (`_COUNTED_TYPES`).
    #: It defaulted to `daily` on EVERY rule, so 1,549 bait bans, boat rules and advisories
    #: shipped `period: "daily"`: a field that does not apply, stated as if it did. Absent on a
    #: counting type is the book's default, daily — read it through `clock`, never `period`. On
    #: any other type it is refused.
    period: Optional[Period] = None
    per_daily: Optional[int] = None
    within: Optional[str] = None
    #: WHICH FISH, BY LENGTH, AND HOW MANY — an ORDERED list, FIRST MATCH WINS.
    #:
    #: This replaces reading `over_cm`/`under_cm`/`band` and guessing. Those three said what the
    #: numbers WERE and left what they MEANT to be reconstructed from the fields around them,
    #: and `over_cm` alone meant three different things: the ceiling on a granted fish ("quota 2,
    #: none over 50 cm"), the class a number COUNTS ("only 1 over 40 cm", inside a clause), and
    #: the fish denied outright ("no trout over 50 cm"). Six branches told them apart. Every
    #: consumer that re-derived those branches got them wrong differently — and the one flag that
    #: did carry meaning, `band`, was set backwards on four rules, permitting exactly the fish
    #: they protect.
    #:
    #: A band writes the range and its number, so there is nothing left to infer and the
    #: inversion is not expressible:
    #:
    #:   "Trout daily quota = 2 (none over 50 cm)"   [{max_cm: 50}, {min_cm: 50, take: 0}]
    #:   "1 bull trout over 60 cm"                   [{min_cm: 60}, {max_cm: 60, take: 0}]
    #:   "only 1 over 40 cm" (a clause)              [{min_cm: 40}]
    #:   "20-30 cm only", quota 2                    [{min_cm: 20, max_cm: 30},
    #:                                                {max_cm: 20, take: 0},
    #:                                                {min_cm: 30, take: 0}]
    #:   "none between 70 cm and 100 cm"             [{min_cm: 70, max_cm: 100, take: 0}]
    #:
    #: ORDER SETTLES THE SHARED ENDPOINT. A grant is written before the denial beneath it, so a
    #: fish of exactly 60 cm is granted rather than denied. (The book's "over 60" and "60 cm or
    #: more" differ by one fish and `over_cm`/`under_cm` never stored which was meant; that is a
    #: pre-existing loss and is not invented here.)
    #:
    #: A LENGTH NO BAND COVERS IS NOT SPOKEN ABOUT by this rule — at the top level nothing else
    #: grants it, and inside a `within` clause the parent quota governs it. That is what makes
    #: "northern pike = 4" + "only 1 over 100 cm, none between 70 and 100" come out right: the
    #: old reading capped a 50 cm pike at 1, when the book allows the parent's 4.
    lengths: Optional[List[LengthBand]] = None

    #: `combined` WAS HERE, and it said nothing. It marked 31 rules as "the count is shared
    #: across the species" — and every one of those names more than one fish, as do 1,157 rules
    #: that were never flagged. The flag was a subset of a fact already on the rule, and its
    #: absence was stored as an explicit `false` on 3,379 rules that ARE shared, which reads as a
    #: denial. A consumer that believed it turned Region 2's "Bass: 20" into twenty of each.
    #:
    #: NAMING MORE THAN ONE FISH IS WHAT MAKES A NUMBER SHARED. Nothing in the corpus means "one
    #: each": the four group quotas whose sentence says "each" all say "each day" or "each year".
    #: `aggregation_domain` WAS HERE and said "across what is this number pooled" in prose. Four
    #: of its five uses restated the entry: two were `"province"` on `zp:` rules, which IS
    #: province-wide, and two were the same kokanee rule on the Upper and Lower West Arm entries,
    #: where sitting on both is the sharing. The fifth said "both lakes" and its own verbatim
    #: already says "in the aggregate from both lakes". Nothing could act on any of them.
    #:
    #: The real idea underneath — a pool SHARED across entries, not a number repeated at each —
    #: wants a structural link like `within`, not a sentence. It is not in the corpus today.
    record_retention: bool = False

    # --- shared scoping ----------------------------------------------------
    water: Optional[WaterKind] = None
    origin: Optional[Origin] = None
    #: WHICH SEASONAL CLOSURE THIS IS, BY NAME — a region's blanket stream closure only
    #: ("Spring closure: No Fishing in any stream in Region 3", "Summer closure: …"). A water row
    #: printed "Exempt from spring closure" lifts its own region's spring closure, and — where the
    #: water runs into another region — THAT region's spring closure, never its winter or summer
    #: one (user ruling 2026-09-26; the bundle's `_equivalent_closures`). Dates cannot say which:
    #: Region 6's Skeena/Nass winter closure (Jan 1-June 15) overlaps every spring closure in
    #: the province. Authored where the rule's own sentence does not print it (Region 4's and
    #: Region 6's print only dates); where it does, the two must agree (`_check`).
    closure_kind: Optional[ClosureKind] = None

    # --- gear / tackle / bait: see `gear` -----------------------------------
    #: The species a bait or tackle rule is ABOUT — "no natural bait when fishing for salmon".
    #: Distinct from `species`, which on those types would mean the ban is scoped to what you may
    #: CATCH, and the tables say it is not: "banned for all angling and for all species". A stream
    #: can carry a salmon bait ban and no other, so the scoping is by TARGET, not by catch.
    when_targeting: List[str] = Field(default_factory=list)

    # --- vessel ------------------------------------------------------------
    aspect: Optional[VesselAspect] = None
    #: On `vessel_rule(aspect=propulsion)`: the boats allowed ON the water. Nowhere else: "No
    #: angling from powered boats" is a method rule on `angler: in_powered_boat`.
    level: Optional[PropulsionLevel] = None
    max_power_kw: Optional[float] = None
    max_kmh: Optional[float] = None

    # --- licensing: NOT HERE ------------------------------------------------
    #: `document`, `required`, `water_class`, `licence_name`, `allocation`,
    #: `issuing_jurisdiction`, `on_retention`, `grantor` and `permitted` WERE HERE, for the two
    #: licensing rule types. They are gone and REFUSED on load (`extra=forbid`), like the old gear
    #: fields: licensing is `CatalogueEntry.licensing`. `required: false` was the defect that
    #: mattered — nine steelhead-stamp waivers shared a dimension with the provincial "stamp if
    #: you fish for steelhead" and, being narrower, would strike it.
    includes_tributaries: Optional[bool] = None

    # --- relations / provenance -------------------------------------------
    #: Walk the tributaries WITHOUT the mainstem. `z5:spring_stream_closure` is the case:
    #: "No fishing in any stream in the Fraser River Watershed ... EXCEPT the mainstem of the
    #: Fraser River." Binding it with includes_tributaries CLOSES the mainstem the rule exempts.
    tributaries_only: bool = False
    #: WHERE THIS RULE APPLIES, stated on the rule itself. NOTHING IS INHERITED: a rule with no
    #: extents is not given its entry's by any reader — not the reach builder, not the bundle, not
    #: provenance (`deliver.bundle.read.source_of`). A row routinely binds its rules to different
    #: reaches — "no fishing above the falls, bait ban throughout" — and an implied default is how
    #: the narrower rule silently widens to the whole water.
    #:
    #: So every rule in an entry says where it is (`CatalogueEntry` refuses one that does not):
    #: `extents`, or — for a place nothing can draw — `extent_text` / `unresolved_locators`, which
    #: leave it UNBOUND (AGENTS 13). A rule that says nothing about location is given the entry's
    #: reach explicitly, once, at ingest (`validate_catalogue.default_extents`), so the file states
    #: it. On an AREA rule (every extent a `within`), `unresolved_locators` beside the extents is a
    #: carve-out no cut-point expresses — "Bass: 20, excluding Mill Lake" is Region 2 minus one lake
    #: — and it stays unbound too: the extents say whose rule it is, the locator why it cannot be
    #: drawn (`reach.classify.AREA_CARVE_OUTS_UNBIND`).
    extents: Optional[List[dict]] = None
    #: WATER THIS RULE REACHES BY THE TRIBUTARY WALK AND MUST NOT. Subtracted from this rule's
    #: tributary set only. There is no entry-wide carve-out: one would cut every rule in the row,
    #: which is wrong where one rule is ABOUT the water another must not touch.
    #:
    #: The Atnarko is the case. "No Fishing from Tenas Lake to the Atnarko Park campsite" runs up
    #: the SOUTH Atnarko, and the row says "includes tributaries" — so the walk reaches the fork
    #: and climbs the MAIN Atnarko above it, which the sentence never mentions. Its sibling rule
    #: "No Fishing upstream of Tweedsmuir Park, Apr 1-June 30" is precisely about that upper
    #: water, so an entry-wide exclude would delete the rule that belongs there.
    #:
    #: Each entry is an extent, resolved by the same machinery as any other and passed to the walk
    #: as BLOCKED, so it removes the named water *and everything above it* and the walk cannot
    #: descend through it either (`pipeline/atlas/reach/build.py::resolve_carve_outs`).
    #: Curator-filled — the parser never writes one.
    tributary_excludes: List[dict] = Field(default_factory=list)
    exempts: List[Exempts] = Field(default_factory=list)
    standing: bool = False
    authority: Optional[str] = Field(default=None, pattern="^superior$")
    #: WHERE THE RULE WAS PUBLISHED, when that is a notice and not the synopsis: a DFO fishery
    #: notice number ("FN0679"). This was `reason` — a free-text field that held a citation here,
    #: an explanation ("located in an Ecological Reserve") on three book rules, and a hidden
    #: condition on three more. A citation says where a rule came from, never why or when.
    notice: Optional[str] = Field(default=None, pattern=r"^FN\d{4}$")
    #: THIS RULE IS DORMANT WHILE THAT ONE BINDS — a rule id in the same entry. "Classified Waters
    #: Licence or Steelhead Stamp not required until reopened to steelhead fishing": the licence
    #: requirement sleeps while the steelhead closure stands. It sat in `reason` as prose, so every
    #: reader was shown the requirement and none could tell it was lifted.
    #:
    #: NOT `condition_of`, which points the other way — at the rule this one is the proviso of.
    #: One field for both would be a pointer whose meaning depends on the rule holding it.
    suspended_while: Optional[str] = None
    extent_text: str = ""
    #: Locator phrases that could not be bound to a cut-point — "the outlet", "signs 500 m below
    #: the falls". Non-empty means the rule needs review; curation maps each to a split id.
    #:
    #: The prompt, the batch envelope and the no-registry instructions all told the model to use
    #: this, and it existed only on the RETIRED prose Rule — so a model that followed the
    #: instruction exactly was refused with "extra inputs are not permitted". The reach it could
    #: not express is precisely what must not be dropped silently, which is the whole point of the
    #: field.
    unresolved_locators: List[str] = Field(default_factory=list)
    #: `needs_review` WAS HERE and was exactly `bool(review_reason)`: not one rule in the corpus
    #: carried the flag without a reason, and every producer that set it passed one in the same
    #: call. A flag whose only job is to say that the field beside it is filled in. It is REFUSED
    #: on load (`extra=forbid`): a non-empty `review_reason` is what "needs review" means.
    review_reason: str = ""

    #: THIS RULE HOLDS ONLY IN A PART OF WHAT ITS EXTENTS DRAW, AND THE PART IS NOT DRAWN — the
    #: book's words for the part. "No fishing in Salmon Arm Bay, west of the line between
    #: Engineer's Point and Sunnybrae Point": the atlas has no polygon for the bay, so the rule
    #: binds Shuswap Lake (its `extents`) and says here that it holds only in the bay.
    #:
    #: WHY A FIELD AND NOT `extent_text`. `extent_text` beside the bare whole water is REFUSED
    #: (see `_check`): that was the parser's shape for every part-lake rule and the reach builder,
    #: reading only the extent, closed all of Shuswap Lake. Unbinding them instead dropped the
    #: closures from every screen, and on a closure silence reads as permission. This field is
    #: the sanctioned third way: the rule is SHOWN on the water it is in, and a reader is told in
    #: the key itself that it must NOT colour that water by it — render it as a note, "in <part>".
    #: It is the place phrase for such a rule, so it replaces `extent_text` rather than sitting
    #: beside it, and it needs extents to be a part OF (a part of nothing is a place nothing can
    #: draw — that rule keeps `extent_text` and stays unbound, AGENTS 13).
    #:
    #: When the part is drawn (a cut-point, a sub-lake polygon), the extents become the part and
    #: this field goes: then the rule colours what it binds.
    undrawn_part: str = ""

    #: THE HALF OF THE CHANNEL THIS RULE HOLDS ON, lengthwise — the book's "on the west half of
    #: river" (Kitimat River, p.51: "No Fishing on the west half of river between fishing boundary
    #: signs near Kitimat Hatchery outfall"). The atlas draws a river as ONE line, so the rule is
    #: placed on the reach its extents name (the stretch between the signs is drawn) — but it
    #: holds on that half only, and an angler on the other half follows the water's OTHER rules
    #: for the stretch (user ruling 2026-09-28). So the rule is shown BESIDE them on the reach
    #: and displaces none (`read.effective_rules`), and its line says which half in its own part
    #: (`label_parts` "side"). Not `undrawn_part`: the stretch is drawn and the rule is placed on
    #: it; only the width is not. A sentence printing "<side> half of the river" must set it
    #: (`_check`).
    side: Optional[ChannelSide] = None

    # ------------------------------------------------------------------ #
    @property
    def clock(self) -> Period:
        """The clock a counting rule's number runs on: its `period`, or the book's default,
        daily. Only meaningful on `_COUNTED_TYPES`; every other type refuses a `period`."""
        return self.period or Period.daily

    @property
    def family(self) -> str:
        """The first tier. Types group into families, and the grouping is not decoration — it is
        how the reader's screen is sectioned, and it is the level at which "does this rule compete"
        is *usually* obvious before you look at the dimension."""
        return _FAMILY[self.type]

    @property
    def lift_only(self) -> bool:
        """A RULE WHOSE ONLY CONTENT IS LIFTING ANOTHER — `exempts`, and no number, bound, gear,
        duty or angler of its own. "Exempt from spring closure"; "Exemptions include mainstem
        portions of the Skeena, Nass, Iskut, Stikine and Taku" (z6 steelhead r2).

        Such a rule says nothing a reader could weigh against another rule, so it never COMPETES
        (`dimension` is `lift`, which the ladder never ranks): it only removes what it lifts.
        Keyed by its type, it was a daily steelhead `retention_limit` bound at the water's rank,
        and on the five mainstems it displaced the province's and the region's "release all wild
        steelhead" — a lift with no take silenced a release."""
        return bool(self.exempts) and not (
            self.gear or self.conduct or self.take is not None or self.unlimited
            or self.may_target is not None or self.per_daily is not None or self.lengths
            or self.record_retention or self.closed_to is not None or self.aspect is not None
            or self.level is not None or self.max_kmh is not None
            or self.max_power_kw is not None)

    def _condition_key(self) -> str:
        """The CONDITIONS a retention rule holds under, as a key — the way a method rule's clause
        carries its `when`. A rule that holds only for wild fish, only in streams, only while
        spear fishing, or that is a record-keeping duty is a different subject from the plain
        quota beside it: Region 6's "Trout/char: 5" must not share a key with the province's
        "release all wild steelhead", or the region's number silences the release."""
        bits = []
        if self.origin is not None:
            bits.append(f"origin={self.origin.value}")
        if self.water is not None:
            bits.append(f"water={self.water.value}")
        if self.while_:
            bits.append("while=" + "+".join(sorted(self.while_)))
        if self.record_retention:
            bits.append("record")
        return ("@" + "&".join(bits)) if bits else ""

    @property
    def dimension(self) -> str:
        """WHAT THIS RULE CONTROLS — the second half of the comparison key.

        Without one, every rule of a type on a water shares a single key and they collide. Tackle
        is the sharpest case: a fly-only rule and a barbless rule are ADDITIVE (a fly must be
        barbless), so their dimension is the FACET each constrains, not the type.

        A LIFT-ONLY rule (`lift_only`) is `lift` whatever its type, and never competes."""
        t = self.type
        if self.lift_only:
            return "lift"
        if t is RuleType.retention_limit:
            # THE CLOCK, '/size' WHEN THE RULE IS SIZES WITH NO COUNT OF ITS OWN ("none under 30
            # cm"), AND THE CONDITIONS IT HOLDS UNDER (`_condition_key`).
            return (f"{self.clock.value}{'/size' if self.lengths and self.take is None else ''}"
                    + self._condition_key())
        if t is RuleType.vessel_rule:
            return self.aspect.value if self.aspect else "unspecified"
        if t is RuleType.angler_closure:
            # WHO it closes the water to. Two closures for the same anglers compete (a water's
            # displaces a zone's); a closure for aliens never competes with one for everyone,
            # and — being its own type — never with a quota.
            return ("closed_to:" + (self.closed_to.key() if self.closed_to else "unspecified")
                    + "".join(f"-except:{w.key()}" for w in self.closed_to_except))
        if t is RuleType.method_rule:
            # THE METHODS NAMED, not just the slot: every method rule constrains `method`, so the
            # slot alone would let a water's "no ice fishing" displace the zone's "no set lining".
            # AND THE CLAUSE'S CONDITION: a water's "no angling from boats" is `ban: [angling]`
            # when in a boat, and keyed "method:angling" it would DISPLACE the province's
            # unconditional angling allow — shore angling would read as not allowed there. A
            # conditional clause is its own key ("method:angling@angler=in_boat").
            def at(c):
                return "@" + c.when.key() if c.when is not None and not c.when.is_empty() else ""
            said = sorted({(f"{c.slot.value}:{m}" if c.slot is Slot.method else c.slot.value)
                           + at(c)
                           for c in self.gear
                           for m in ((c.allow or []) + (c.only or []) + (c.ban or []) or [""])})
            return ",".join(said) or ("conduct" if self.conduct else "unspecified")
        if t is RuleType.tackle_restriction:
            # THE SET OF SLOTS CONSTRAINED. A water's "single barbless hook" displaces the zone's
            # "single barbless hook"; its bare "barbless hook" does not, because displacing would
            # drop the zone's one-point cap, which the water never lifted.
            return ",".join(sorted({c.slot.value for c in self.gear})) or "unspecified"
        if t is RuleType.bait_restriction:
            # BAIT IS ONE DOMAIN, ranked by where a rule applies (province < region < water).
            # Invertebrates are a kind of bait, and the province's "you may use freshwater
            # invertebrates in streams as bait UNLESS A BAIT BAN APPLIES" is a PERMISSION: keyed by
            # the bait it names (`bait:invertebrate`) it never met the 412 bait bans
            # (`bait:any_bait`), so every bait-ban stream showed both "Bait ban" and
            # "Invertebrates may be used". So a permission (`allow` / `only`) and a TOTAL ban (one
            # that bans `any_bait`) share ONE key, `bait`: a region's or a water's bait ban speaks
            # over the province's permission, and a water's "EXEMPT from bait ban" meets the
            # region's ban.
            #
            # A PARTIAL BAN KEEPS THE BAIT IT NAMES (`bait:fin_fish`, `bait:live_fin_fish`,
            # `bait:invertebrate`): Zone B's "fin fish may not be used as bait" must not displace the
            # province's ban on invertebrates at a lake, and a water's "EXEMPT from bait ban"
            # (an allow) must not lift the province's live-fish ban. The one printed exemption
            # from a partial ban (dead fin fish for sturgeon) is a lift.
            #
            # Two things still make a different subject. The TARGET: a salmon bait ban and a
            # general bait ban coexist on one stream. And the MEANS (`while`): bait when set
            # lining is not bait when angling ("bait ban … for all angling"). The ROE POSSESSION
            # CAP is not a rule about using bait at all — it is how much you may hold — and has
            # its own key, so no water's bait rule displaces it.
            tgt = ("/" + ",".join(sorted(self.when_targeting))) if self.when_targeting else ""
            means = ("@while=" + "+".join(sorted(self.while_))) if self.while_ else ""
            bait = [c for c in self.gear if c.slot is Slot.bait]
            if self.gear and not bait and all(c.slot is Slot.bait_possession_kg for c in self.gear):
                held = sorted({m for c in self.gear for m in c.of}) or ["any_bait"]
                return f"bait_possession:{','.join(held)}{tgt}{means}"
            whole = any("any_bait" in (c.ban or []) or c.allow is not None or c.only is not None
                        for c in bait)
            if whole or not bait:
                return f"bait{tgt}{means}"
            named = sorted({m for c in bait for m in (c.of or c.ban or [])})
            return f"bait:{','.join(named)}{tgt}{means}"
        return t.value

    @model_validator(mode="after")
    def _check(self) -> "CatalogueRule":
        e: List[str] = []
        t = self.type

        # species=[] is an ERROR, not "all" — 115 rules store it while naming a species in
        # their own text, and the blanket default states a trout limit on bass and burbot.
        if t is RuleType.retention_limit and not self.species:
            e.append("retention_limit needs species (use ALL_GAME_FISH for everything)")
        if self.species_except and not self.species:
            e.append("species_except needs a species set to subtract from")
        # THE BOOK'S LIST (p.86), plus the federal salmon the DFO feed types into this model —
        # which a synopsis ENTRY refuses (`CatalogueEntry._book_species_only`).
        e += species_problems((set(self.species) | set(self.species_except)) - FEDERAL_SALMON)

        if t is RuleType.retention_limit:
            # NO DEFAULT. `may_target=True` as a default meant that forgetting the field turned a
            # closure into a catch-and-release permission — the exact 605-rule defect this module
            # exists to prevent, reintroduced as a default value.
            if self.take == 0 and self.may_target is None:
                e.append("take=0 needs an explicit may_target: false = may not fish for it, "
                         "true = fish for it and release it")
            if self.take is not None and self.take < 0:
                e.append("take cannot be negative")
            if self.unlimited and self.take is not None:
                e.append("unlimited and take are mutually exclusive")
            # take=0 does NOT mean closed. may_target is the bit that separates "do not fish for
            # this" from "fish for it, release it", and 15 kokanee rules depend on it.
            if self.take == 0 and self.may_target and self.clock is not Period.daily:
                e.append("a release rule is a daily-period rule")
            if self.per_daily is not None and self.clock is not Period.possession:
                e.append("per_daily is a possession multiplier")
            # `lengths` IS THE ONLY SIZE FIELD. over_cm/under_cm/band WERE HERE and are refused
            # on load (`extra=forbid`): checked for equality against them, `lengths` could only
            # ever say what they could, and "Wild cutthroat trout daily quota = 2 (none 40 cm or
            # more)" — 40 cm on the forbidden side — was rejected as a mismatch.
        else:
            # `lengths` IS NOT ON THIS LIST. `band` was, because a band was only ever a
            # retention thing — but a size on a document rule names WHICH FISH need the stamp
            # ("required to catch and keep rainbow trout over 50 cm"), so `lengths` belongs to
            # both and refusing it here rejected the two rules that prove the distinction.
            for f in ("take", "unlimited", "per_daily", "within"):
                if getattr(self, f) not in (None, False):
                    e.append(f"{f} belongs to retention_limit, not {t.value}")

        # A bait or tackle rule is not scoped to what you may CATCH — "banned for all angling and
        # for all species". 18 corpus rules carry codes leaked from a co-located catch-and-release
        # clause ("Trout/char catch and release, bait ban"), which reads narrower than the law.
        # But a rule may be scoped to what you are FISHING FOR — a stream can carry a salmon bait
        # ban and no other — and that is `when_targeting`.
        if t in (RuleType.bait_restriction, RuleType.tackle_restriction):
            if self.species:
                e.append(f"{t.value} must not carry `species` — a bait or hook rule binds all "
                         f"species you may catch. If the rule applies only when fishing FOR "
                         f"something, use `when_targeting`.")
            e += species_problems(set(self.when_targeting) - FEDERAL_SALMON, "when_targeting")
        elif self.when_targeting:
            e.append("when_targeting belongs to bait_restriction and tackle_restriction")

        if t is RuleType.vessel_rule:
            if self.aspect is None:
                e.append("vessel_rule needs an aspect")
            elif self.aspect is VesselAspect.propulsion and self.level is None:
                e.append("propulsion needs a level")
            elif self.aspect is VesselAspect.speed and self.max_kmh is None \
                    and not self.review_reason:
                e.append("speed needs max_kmh, or a review_reason if the synopsis states none")
            if self.level is PropulsionLevel.power_capped and self.max_power_kw is None:
                e.append("power_capped needs max_power_kw")
        if t is not RuleType.vessel_rule and self.level is not None:
            e.append("level belongs to vessel_rule — a boat you may not ANGLE from is a method "
                     "rule's `when: {angler: in_boat | in_powered_boat}`")
        # "No angling from POWERED boats" banned as `in_boat` forbids a canoe the book allows.
        if any(c.when is not None and c.when.angler is AnglerState.in_boat for c in self.gear) \
                and re.search(r"\bpowered\s+boats?\b", self.verbatim, re.I):
            e.append("the sentence says POWERED boats — the clause's angler is in_powered_boat, "
                     "or the ban reaches a canoe the book allows")
        if t is not RuleType.vessel_rule and (self.aspect is not None or self.max_power_kw
                                              is not None or self.max_kmh is not None):
            e.append("aspect, max_power_kw and max_kmh belong to vessel_rule")
        if t is RuleType.angler_closure:
            if self.closed_to is None:
                e.append("angler_closure needs closed_to — a closure to everyone is a "
                         "retention_limit with take 0, may_target false")
            if self.species or self.species_except:
                e.append("angler_closure closes the water to an angler, not to a species")
            err = _check_residency(self.closed_to, self.verbatim, "closed_to")
            if err:
                e.append(err)
        elif self.closed_to is not None:
            e.append("closed_to belongs to angler_closure")
        if self.closed_to_except:
            if t is not RuleType.angler_closure or self.closed_to is None:
                e.append("closed_to_except carves anglers out of an angler_closure's closed_to")
            else:
                idle = [w.key() for w in self.closed_to_except if not w.overlaps(self.closed_to)]
                if idle:
                    e.append(f"closed_to_except {idle} does not meet closed_to — it subtracts "
                             f"nothing")
        # A GEAR RULE SAYS WHAT IT CONSTRAINS IN `gear` OR `conduct`. The direction lives inside
        # the clause that carries the subject; with neither, the rule states nothing a reader
        # can act on. An exemption is the one other thing such a rule may be — it lifts a clause
        # stated elsewhere ("EXEMPT from single barbless hooks").
        said = bool(self.gear) or bool(self.conduct) or bool(self.exempts)
        if t in (RuleType.method_rule, RuleType.tackle_restriction,
                 RuleType.bait_restriction, RuleType.handling_rule) and not said:
            e.append(f"{t.value} needs `gear`, `conduct` or `exempts` — it states nothing else")

        if self.take is not None and self.take < 0:
            e.append("take cannot be negative")
        for f in ("max_kmh", "max_power_kw"):
            v = getattr(self, f)
            if v is not None and v <= 0:
                e.append(f"{f} must be positive")
        if set(self.species) & set(self.species_except):
            e.append("a species cannot be both included and excepted")
        if self.unresolved_locators and not self.review_reason:
            e.append("unresolved_locators is set with no review_reason — a locator nobody "
                     "could bind is exactly what a human has to look at")
        if self.standing and not self.review_reason:
            e.append("a standing rule must be flagged: its extent is unknowable, not merely absent")

        # `whole` SAYS THE WHOLE WATER; `extent_text` SAYS A PART NOTHING COULD DRAW. Both at once
        # is the shape the parser wrote on every part-lake rule — "No Fishing in Salmon Arm Bay"
        # stored as `whole` with the bay in `extent_text` — and the reach builder reads only the
        # first, so the closure bound all of Shuswap Lake. A place that cannot be drawn keeps its
        # words and NO extents, and stays unbound (AGENTS 13). A `whole` qualified by an item, an
        # area or a kind is a place of its own; its text only describes it.
        if bare_whole(self.extents) and self.extent_text.strip():
            e.append(f"extents are the whole water but extent_text names a place "
                     f"({self.extent_text[:60]!r}) — a part nothing can draw is written as "
                     f"`undrawn_part` beside the whole water (shown as a note, never coloured), "
                     f"or keeps its words with no extents (unbound), or is bound to the part")
        # `undrawn_part` IS A PART OF WHAT THE EXTENTS DRAW, and it is the rule's place phrase.
        if self.undrawn_part.strip():
            if not self.extents:
                e.append("undrawn_part needs extents — it is a part OF what they draw; a place "
                         "with nothing to be a part of keeps `extent_text` and stays unbound")
            if self.extent_text.strip():
                e.append("undrawn_part and extent_text are both the rule's place phrase — keep "
                         "one: undrawn_part when the rule binds more than it holds in")
            if self.standing:
                e.append("a standing rule holds at places no dataset draws, everywhere — it is "
                         "never also limited to one undrawn part")
        elif self.undrawn_part:
            e.append("undrawn_part is blank")
        # ONE HALF OF THE CHANNEL (`side`): what the sentence prints, both ways — a rule printed
        # "on the west half of river" that lost it would close the whole width on every screen.
        half = HALF_OF_CHANNEL.search(self.verbatim or "")
        printed = ChannelSide(half.group(1).lower()) if half else None
        if printed is not None and self.side is not printed:
            e.append(f"the sentence says the {printed.value} half of the channel — set side: "
                     f"{printed.value} (the rule holds on that half only)")
        if self.side is not None:
            if printed is None:
                e.append(f"side: {self.side.value} — the sentence prints no '{self.side.value} "
                         f"half of the river'")
            if not self.extents:
                e.append("side needs extents — it is a half of the stretch they draw")
            if self.undrawn_part.strip():
                e.append("side and undrawn_part: a half of a drawn stretch is placed on it "
                         "(side); an undrawn part is not — keep one")
            if HALF_OF_CHANNEL.search(self.extent_text or ""):
                e.append("extent_text repeats the half of the channel `side` says — keep only the "
                         "stretch in extent_text")
        # A PLACE IS NOT A LIST ITEM. The book numbers its lists; a place phrase that starts with
        # a marker was cut out of one, and every label built from it would print the marker.
        for f in ("extent_text", "undrawn_part"):
            if LIST_MARKER.match(getattr(self, f) or ""):
                e.append(f"{f} starts with a list marker ({getattr(self, f)[:20]!r}) — it "
                         f"names a place, not a list item")
        # THE CLOCK BELONGS TO A NUMBER OF FISH. On a bait ban or an advisory it says nothing.
        if self.period is not None and t not in _COUNTED_TYPES:
            e.append(f"period belongs to {' and '.join(x.value for x in _COUNTED_TYPES)}, "
                     f"not {t.value} — a clock with no number on it says nothing")
        e += _extents_the_resolver_reads(self.extents)
        # THE DATES ITS OWN SENTENCE PRINTS — see `_own_dates_carried`.
        err = _own_dates_carried(self)
        if err:
            e.append(err)
        # "ALL OTHER SPECIES" IS GAME FISH — see `_all_species_is_game_fish`.
        err = _all_species_is_game_fish(self)
        if err:
            e.append(err)
        # FEATURE TYPES ARE APPLIED TO THE RULE'S WHOLE REACH, after any tributary walk
        # (`reach.classify`), so they must be said on every extent or on none — a union of
        # "lakes of this watershed" with "that river, every kind" has no one filter.
        kinds = [bool(x.get("feature_types")) for x in (self.extents or []) if isinstance(x, dict)]
        if any(kinds) and not all(kinds):
            e.append("feature_types is set on some extents and not others — the builder applies "
                     "one filter to the rule's reach; split the rule")
        # A CLOSURE'S NAME IS A CLOSURE'S. On anything but "no fishing" it names nothing, and
        # where the sentence prints the name itself the field may only repeat it.
        if self.closure_kind is not None:
            if not (t is RuleType.retention_limit and self.take == 0
                    and self.may_target is False):
                e.append("closure_kind names a closure — a retention_limit with take 0 and "
                         "may_target false")
            printed = printed_closure_kinds(self.verbatim)
            if printed and self.closure_kind.value not in printed:
                e.append(f"closure_kind {self.closure_kind.value!r}, but the sentence names "
                         f"the {'/'.join(sorted(printed))} closure")

        if e:
            raise ValueError(f"{self.rule_id}: " + "; ".join(e))
        return self


# --------------------------------------------------------------------------------------- #
# Label generation — ONE place structure becomes English.
#
# `details` used to be typed beside the number and drifted from it. Everything below is derived,
# so it cannot. The verbatim stays on the rule and is always shown underneath.
# --------------------------------------------------------------------------------------- #

_DOC_WORDS = {
    "basic_licence": "basic angling licence",
    "steelhead_stamp": "Steelhead Conservation Surcharge Stamp",
    "salmon_stamp": "Conservation Surcharge Stamp for salmon",
    "kootenay_rainbow_stamp": "Conservation Surcharge Stamp for Kootenay Lake rainbow trout",
    "shuswap_char_stamp": "Conservation Surcharge Stamp for Shuswap Lake char",
    "shuswap_rainbow_stamp": "Conservation Surcharge Stamp for Shuswap rainbow trout",
    "white_sturgeon_licence": "White Sturgeon Conservation Licence",
    "classified_waters_licence": "Classified Waters Licence",
    "national_park_permit": "National Park Fishing Permit",
    "angling_guide_licence": "angling guide licence",
    "assistant_angling_guide_licence": "assistant angling guide licence",
    "creston_valley_wma_permit": "Creston Valley Wildlife Management Area permit",
    "yukon_angling_licence": "Yukon angling licence",
    "creston_valley_rod_and_gun_club_permission":
        "permission of the Creston Valley Rod & Gun Club",
}

_HP = {7.5: 10, 15.0: 20}          # the synopsis PRINTS these. 7.5 kW computes to 10.06 hp.

_SPECIES_WORDS = {
    # groups — the words the synopsis itself prints
    "ALL_GAME_FISH": "All game fish", "TROUT_CHAR": "Trout and char",
    "CHAR": "Char", "WHITEFISH": "Whitefish", "BASS": "Bass",
    # the subjects that are not game fish (`OPEN_SUBJECTS`)
    "ALL_FIN_FISH": "All fish", "PROTECTED_SPECIES": "Protected species", "SALMON": "Salmon",
    # trout (p.86). GB is Brown Trout (Salmo trutta) in the official table.
    "RB": "Rainbow trout", "ST": "Steelhead", "CT": "Cutthroat trout", "GB": "Brown trout",
    # char. ONE fish for Dolly Varden and bull trout: "Any bull trout that you catch and keep must
    # be counted as part of your Dolly Varden quota" (p.86).
    "DV": "Dolly Varden/bull trout", "LT": "Lake trout", "EB": "Brook trout",
    # whitefish and bass
    "LW": "Lake whitefish", "MW": "Mountain whitefish",
    "LMB": "Largemouth bass", "SMB": "Smallmouth bass",
    # other game fish
    "KO": "Kokanee", "GR": "Arctic grayling", "BB": "Burbot", "WSG": "White sturgeon",
    "BCB": "Black crappie", "NP": "Northern pike", "YP": "Yellow perch", "WP": "Walleye",
    "GE": "Goldeye", "IN": "Inconnu", "CRA": "Crayfish",
}

#: The book's own headings (p.86), for the menu and for display.
_FAMILY_WORDS = {"TROUT": "Trout", "CHAR": "Char", "WHITEFISH": "Whitefish", "BASS": "Bass",
                 "OTHER": "Other game fish"}


def species_menu() -> str:
    """The species vocabulary as the parser sees it: the book's list (p.86), the groups, and the
    rule for choosing between them. Generated from `KNOWN_SPECIES`, so a menu can never offer a
    code validation refuses (it once offered seven)."""
    out = ["THE SPECIES ARE THE BOOK'S LIST AND NOTHING ELSE (p.86, 'Freshwater game fish are",
           "defined as follows'). Two facts from that page decide most rows:",
           "  * TROUT INCLUDES CHAR unless char are specifically excluded ('trout/char: all",
           "    regulations that apply to trout (as a group) also apply to char unless char are",
           "    specifically excluded'). 'Trout daily quota = 2' is `TROUT_CHAR`. There is no",
           "    TROUT-only code: the code TROUT is refused. BUT when the SAME ROW (or the same",
           "    zone table) mentions char apart — 'char', Dolly Varden/bull trout, lake trout,",
           "    brook trout on their own; 'trout/char' names char IN and does not count — that",
           "    row's bare 'trout' lines exclude char: `TROUT_CHAR` with species_except [CHAR].",
           "    Region 6's box ('Trout/char: 5 … 3 Dolly Varden/bull trout and/or lake trout",
           "    combined, 1 trout from streams July 1-Oct 31 … Trout under 30 cm from any",
           "    stream') mentions char, so '1 trout from streams' and 'Trout under 30 cm' are",
           "    [TROUT_CHAR] except [CHAR]; 'Trout/char: 5' stays [TROUT_CHAR].",
           "  * A BULL TROUT IS A DOLLY VARDEN ('*Any bull trout that you catch and keep must be",
           "    counted as part of your Dolly Varden quota'). 'Bull trout catch and release' is",
           "    `DV`. The code BT is refused.",
           "",
           "**Use the word the regulation itself uses.** If the line says \"Trout/char: 5\", the",
           "species is `TROUT_CHAR` — one claim, not seven. Name an individual fish only when the",
           "sentence names that fish (\"Rainbow trout: release\" -> `RB`). Never expand a group "
           "yourself.",
           "",
           "GROUPS — prefer these:"]
    gloss = {"ALL_GAME_FISH": "the whole list below; NOT salmon, NOT non-game fish",
             "TROUT_CHAR": ("'trout', 'trout/char', 'trout and char' — trout rules cover char "
                            "(except [CHAR] on a bare 'trout' line whose row mentions char apart)"),
             "CHAR": "'char' — Dolly Varden/bull trout, lake trout, brook trout",
             "ALL_FIN_FISH": ("\"any fish\" / \"fin fish\" — game fish, salmon AND non-game; never "
                              "crayfish"),
             "PROTECTED_SPECIES": "the protected list (p.9; Region 2 adds green sturgeon)",
             "SALMON": "Pacific salmon — federal, not part of ALL_GAME_FISH (kokanee is `KO`)"}
    for code in ("ALL_GAME_FISH", "TROUT_CHAR", "CHAR", "WHITEFISH", "BASS") + OPEN_SUBJECTS:
        members = SPECIES_GROUPS[code]
        g = gloss.get(code, "")
        if not members:
            out.append(f"  `{code}` — {_SPECIES_WORDS[code]} (no fish codes under it)"
                       + (f"  · {g}" if g else ""))
            continue
        names = ", ".join(_SPECIES_WORDS[m] for m in members[:4])
        more = f", +{len(members) - 4} more" if len(members) > 4 else ""
        out.append(f"  `{code}` — {_SPECIES_WORDS[code]} ({len(members)}: {names}{more})"
                   + (f"  · {g}" if g else ""))
    out.append("")
    out.append("THE FISH — only when the sentence names one:")
    for fam, codes in BOOK_FAMILIES.items():
        out.append(f"  {_FAMILY_WORDS[fam]}: " + " · ".join(f"`{c}` {_SPECIES_WORDS[c]}"
                                                         for c in codes))
    out += ["",
            "Leaving `species` empty is NOT 'all species' — it is refused on a retention rule.",
            "Use `ALL_GAME_FISH` for the game-fish list, or `ALL_FIN_FISH` when the sentence",
            "says \"any fish\" / \"all fin fish\" and so covers salmon and non-game fish too.",
            "Bait and tackle rules take no `species` at all (use",
            "`when_targeting` if the rule only applies when fishing FOR something)."]
    return "\n".join(out)


def species_words(codes: List[str], excepts: List[str] | None = None) -> str:
    if not codes:
        return ""
    # "TROUT" WITH CHAR EXCLUDED BY ITS ROW (`trout_scope_problems`) is the book's word, "Trout" —
    # never "Trout and char other than char".
    trout_only = "TROUT_CHAR" in codes and "CHAR" in (excepts or [])
    if trout_only:
        excepts = [c for c in excepts if c != "CHAR"]
    names = ["Trout" if (c == "TROUT_CHAR" and trout_only) else _SPECIES_WORDS.get(c, c)
             for c in codes]
    out = names[0] if len(names) == 1 else ", ".join(names[:-1]) + " and " + names[-1]
    if excepts:
        ex = [_SPECIES_WORDS.get(c, c).lower() for c in excepts]
        out += " other than " + (ex[0] if len(ex) == 1 else ", ".join(ex[:-1]) + " and " + ex[-1])
    return out


def _gear_words(r: CatalogueRule) -> str:
    """`gear` and `conduct`, in words. ONE place, so a reader and a renderer cannot disagree.

    The direction is never inferred: it is the key the clause used (`allow` / `only` / `ban` /
    `must_be`) or the act token's own name. That is what makes the label impossible to invert —
    `{barbless: true, required: false}` printed "barbless" and meant the opposite, because the
    words came from one field and the polarity from another.
    """
    UNIT = {"hook_gap_mm": "mm", "light_to_hook_mm": "mm",
            "weight_per_line_kg": "kg", "bait_possession_kg": "kg"}
    bits = []
    gear = list(r.gear)

    def plain(c: GearClause) -> bool:
        return c.when is None and not c.unless and not c.of and not c.except_

    # THE BOOK'S OWN WORDS FOR ITS COMMONEST SHAPES. 800 waters print "Single barbless hook" and
    # "Bait ban"; spelled clause by clause those came out "barbless only; at most 1 point per
    # hook" and "no any bait", which are right and which nobody says.
    barb = next((c for c in gear if c.slot is Slot.barb and c.only == ["barbless"] and plain(c)), None)
    one = next((c for c in gear if c.slot is Slot.points_per_hook and c.max == 1
                and c.min is None and plain(c)), None)
    if barb or one:
        bits.append(f"{'single ' if one else ''}{'barbless ' if barb else ''}hook")
        gear = [c for c in gear if c is not barb and c is not one]
    for c in gear:
        name = c.slot.value.replace("_", " ")
        # WHO THE CLAUSE IS ABOUT, when it is only about some fish: "no spear fishing FOR GAME
        # FISH", "spear fishing FOR BURBOT allowed". Without it the clause read as a total ban.
        fish = ""
        if c.when is not None and c.when.targeting:
            fish = species_words(list(c.when.targeting)).lower()
            fish = {"all game fish": "game fish"}.get(fish, fish)
        if c.slot is Slot.bait and c.ban == ["any_bait"] and plain(c):
            bits.append("bait ban")
        elif c.slot is Slot.method and fish and c.ban is not None:
            bits.append(f"no {', '.join(c.ban).replace('_', ' ')} for {fish}")
        elif c.slot is Slot.method and fish and c.allow is not None:
            bits.append(f"{', '.join(c.allow).replace('_', ' ')} for {fish} allowed")
        elif c.slot is Slot.bait_possession_kg and c.max is not None and c.min is None:
            # "you must not have more than 1 kg of ROE … for use as bait" — the cap is on what the
            # clause names (`of`), never on bait in general.
            what = ", ".join(c.of).replace("_", " ") if c.of else "bait"
            bits.append(f"no more than {c.max:g} kg of {what} in possession for use as bait")
        elif c.slot is Slot.light_to_hook_mm and c.max is not None and c.min is None:
            # The book prints METRES ("within 1 m of the hook"); the slot stores millimetres.
            bits.append(f"light within {c.max / 1000:g} m of the hook")
        elif c.slot is Slot.hook_gap_mm and c.max is not None and c.min is None and plain(c):
            bits.append(f"no hook more than {c.max:g} mm from point to shank")
        elif c.allow is not None:
            bits.append(f"{', '.join(c.allow).replace('_', ' ')} may be used")
        elif c.only is not None:
            bits.append(f"{', '.join(c.only).replace('_', ' ')} only")
        elif c.ban is not None:
            got = f"no {', '.join(c.ban).replace('_', ' ')}"
            if c.except_:
                got += f" other than {', '.join(c.except_).replace('_', ' ')}"
            bits.append(got)
        elif c.must_be:
            bits.append(f"{name} must be {', '.join(c.must_be).replace('_', ' ')}")
        elif c.unlimited:
            bits.append(f"unlimited {name}")
        else:
            u = UNIT.get(c.slot.value, "")
            for kind, v in (("at most", c.max), ("at least", c.min)):
                if v is None:
                    continue
                if u:
                    bits.append(f"{kind} {v:g}{u} {name.removesuffix(' ' + u)}")
                else:
                    # "at most 1 points per hook" — the slot name is plural because it names a
                    # measurand, and a bound of one reads as a count.
                    head, _, tail = name.partition(" per ")
                    one = ({"flies": "fly"}.get(head, head.removesuffix("s"))
                           if v == 1 else head)
                    bits.append(f"{kind} {v:g} {one}" + (f" per {tail}" if tail else ""))
        if c.when is not None and not c.when.is_empty():
            w = [x for x in (getattr(c.when.water, "value", None),
                             getattr(c.when.angler, "value", None)) if x]
            if w:
                bits[-1] += " (" + ", ".join(_ANGLER_WORDS.get(x, x.replace("_", " "))
                                             for x in w) + ")"
        # WHAT LIFTS THE CLAUSE, said: "at most 1kg weight per line (not downrigger weights)".
        # Dropped, the label stated a limit the book exempts downriggers from.
        esc = [u.gear_in_use.replace("_", " ") + "s" for u in c.unless if u.gear_in_use]
        if esc:
            bits[-1] += f" (not {' or '.join(esc)})"
    for act in r.conduct:
        bits.append(CONDUCT_ACTS.get(act, act.replace("_", " ")))
    out = "; ".join(bits)
    # WHAT IS CONSTRAINED, only: who is fishing for what (`when_targeting`) and while doing what
    # (`while`) are conditions, and `label_parts` says them as such.
    return out[:1].upper() + out[1:]


def _when_words(r: CatalogueRule) -> str:
    """WHEN, in words: the dates, the weekdays, the hours, and any season nobody could read (in
    the book's words — an unread season is never shown as all year by being left out). There is no
    "except …" branch: `When` stores the days a rule DOES hold, so the reader never inverts it."""
    w = r.when
    if w is None or w.is_empty():
        return ""
    bits = []
    if w.dates:
        bits.append(" and ".join(d.words() for d in w.dates))
    if w.weekdays:
        bits.append("on " + " and ".join(f"{d}s" for d in w.weekdays))
    if w.hours:
        bits.append(w.hours.words())
    if w.unparsed:
        bits.append("as printed: " + "; ".join(w.unparsed))
    return ", ".join(bits)


def _scope(r: CatalogueRule, taking: bool = True) -> list:
    """Which fish, by kind of water, origin and means of taking. `taking` distinguishes "2 from
    streams" (a retention limit) from "no fishing in streams" (a prohibition). Same field, opposite
    preposition, and the wrong one reads as nonsense."""
    bits = []
    if r.water:
        bits.append(f"{'from' if taking else 'in'} {r.water.value}s")
    if r.origin:
        bits.append(f"{r.origin.value} only")
    for m in r.while_:
        bits.append("taken on a set line" if m == Method.set_lining.value
                    else f"taken by {m.replace('_', ' ')}")
    return bits


def _size(r: CatalogueRule) -> str:
    """The size limit, in words, READ OFF `lengths`.

    POLARITY USED TO BE THE WHOLE JOB HERE. "not more than 1 over 50 cm" ALLOWS one big fish;
    "none over 50 cm" FORBIDS them — and `over_cm` carried both, so which was meant had to be
    worked out from `take`, `within` and `period`. This function held one copy of that reasoning
    and a migration shim held the other; two copies of a six-way branch is how "1 bull
    trout over 60 cm" got rendered "none over 60 cm", inverting the rule on the fish it exists
    to protect.

    `lengths` has already decided. What is left is reading a shape and naming it: a range with a
    zero is fish going back, a range without one is fish you may keep.
    """
    bands = list(r.lengths or [])
    if not bands:
        return ""
    denied = [b for b in bands if b.take == 0]
    granted = [b for b in bands if b.take != 0]

    # A HOLE protects the middle. `r6:bennett_lake` is "only 1 over 90 cm, NONE between 60 and
    # 90" — two facts, and naming only the hole swallows the number.
    hole = next((b for b in denied if b.min_cm is not None and b.max_cm is not None), None)
    if hole:
        between = f"none between {hole.min_cm} cm and {hole.max_cm} cm"
        return f" ({between})" if r.take is None else f" (no more than {r.take}, {between})"

    # A WINDOW is the opposite: the middle is the only part you may keep. A window inside a
    # parent still carries its own COUNT — `z7a` is "not more than 1 bull trout, 30-50 cm", and
    # dropping the 1 turns a one-fish allowance into an unlimited one.
    win = next((b for b in granted if b.min_cm is not None and b.max_cm is not None), None)
    if win:
        slot = f"{win.min_cm}\u2013{win.max_cm} cm only"
        return f" (no more than {r.take}, {slot})" if (r.within and r.take) else f" ({slot})"

    if not granted:
        # NOTHING IS GRANTED, so the range names the fish that go back and nothing else is
        # claimed. "no trout over 50 cm" says nothing about a 40 cm trout.
        #
        # THE PARENTHESES ARE ABOUT THE SENTENCE, NOT THE SIZE. An explicit `take: 0` has
        # already put "release all" in front of this, so the bound appends bare — "release all
        # over 50 cm". With no take there is no quota phrase to append to, and the bare form
        # reads as a description of a fish rather than a prohibition: "Trout under 25 cm"
        # instead of "Trout (none under 25 cm)". That is the one thing `lengths` cannot say,
        # because both spellings of the prohibition make the same band.
        b = denied[0]
        end, cm = ("over", b.min_cm) if b.min_cm is not None else ("under", b.max_cm)
        return f" {end} {cm} cm" if r.take == 0 else f" (none {end} {cm} cm)"

    g = granted[0]
    if not denied:
        # A GRANT WITH NO DENIAL BENEATH IT COUNTS A SIZE CLASS rather than bounding one.
        # "Rainbow trout: 5 over 50 cm" (annual) counts the big ones and does not forbid keeping
        # smaller ones, which the daily quota governs; printing "(none under 50 cm)" put a
        # minimum size on the page that neither the Shuswap nor the Kootenay chapter states.
        if g.min_cm is not None:
            n = g.take if g.take is not None else r.take
            return (f" (no more than {n} over {g.min_cm} cm)" if r.within and n
                    else f" over {g.min_cm} cm")
        return f" under {g.max_cm} cm"

    # A GRANT WITH A DENIAL BENEATH IT is bounded: the fish you keep must lie on this side.
    # ASYMMETRIC ON PURPOSE — a maximum caps how big a kept fish may be, a minimum is a floor on
    # every fish kept, and "no more than 1 under 60 cm" would say the opposite of
    # `r2:cultus_lake`'s "1 bull trout over 60 cm", where the fish you keep must BE over 60.
    if g.max_cm is not None:
        return f" (none over {g.max_cm} cm)"
    n = g.take if g.take is not None else r.take
    return (f" (no more than {n}, none under {g.min_cm} cm)" if r.within and n
            else f" (none under {g.min_cm} cm)")


def _where(r: CatalogueRule, place_of=None) -> str:
    """WHERE, in words. §5: generation is lossless ONLY where the extent survives alongside.
    226 labels read exactly "No fishing" — everything distinguishing one from another was in the
    reach, and a split or an area never reached the label.

    THE BOOK'S WORDS WIN. `extent_text` is the page's own phrase for this rule's place ("from the
    log boom upstream of the IPP intake to signs at the tail of the canyon pool"), with any list
    marker stripped — a place is never a list item. Only a rule with no such phrase is named from
    what its extents draw (a cut-point, an area), by `place_of`, from the atlas's curated names.
    The other way round, 46 labels traded the book's phrase for a curator's cut-point name: offsets
    the book does not print ("log boom (90 m upstream)" for "approximately 100 m"), a watershed
    read as its river ("Fraser watershed" -> "Fraser River"), and a reach dropped ("…, and Quinn
    Creek"). An undrawn part is NOT a `where`: it is its own part (`in_part`), so the line can
    never read as holding on the whole water."""
    text = strip_list_marker(r.extent_text)
    if not text and place_of is not None and r.extents:
        text = place_of(r.extents) or ""
    # "No Fishing tributaries": the reach is the tributaries, not the water the row names.
    if r.tributaries_only:
        text = f"{text}, tributaries only" if text else "tributaries only"
    return text


def _side_words(r: CatalogueRule) -> str:
    """ONE HALF OF THE CHANNEL (`side`), said so no angler on the other half reads the rule as
    theirs: "west half of the channel only — on the east half, this water's other regulations
    apply" (Kitimat River's hatchery-outfall closure, user ruling 2026-09-28)."""
    if r.side is None:
        return ""
    return (f"{r.side.value} half of the channel only — on the {r.side.opposite.value} half, "
            f"this water's other regulations apply")


def _suspended(r: CatalogueRule, siblings: Optional[dict] = None, place_of=None) -> str:
    """"not while <the rule it sleeps under>", read off that rule's own line. Without the entry's
    other rules to hand it names the rule by id, which is ugly and still true."""
    if not r.suspended_while:
        return ""
    other = (siblings or {}).get(r.suspended_while)
    said = label(other, place_of=place_of) if other is not None else f"rule {r.suspended_while}"
    return f"not while “{said}” is in force"


#: THE BOOK'S EMPHASIS, which the extraction keeps as markdown. It never reaches a part, and the
#: composed preview's fallback strips it too: 20 labels read "**WARNING! Dangerous thin ice…**".
_EMPHASIS = re.compile(r"\*\*|__")


def _is_closure(t: CatalogueRule) -> bool:
    return t.type is RuleType.retention_limit and t.take == 0 and t.may_target is False


def _lifted_rule_words(t: CatalogueRule, siblings: Optional[dict] = None) -> str:
    """A LIFTED RULE BY ITS OWN GENERATED LINE — what, size, conditions, when; never its place,
    which is the lifter's. Empty when the rule has no generated `what` (an advisory)."""
    p = label_parts(t, siblings)
    if not p.get("what"):
        return ""
    return compose({k: p.get(k, "") for k in ("what", "size", "conditions", "when")})


def _lift_name(x: "Exempts", siblings: Optional[dict] = None,
               entries: Optional[dict] = None) -> str:
    """THE NAME OF WHAT ONE `exempts` LIFTS, in words a reader knows — never a slug.

      a zone default     its zone entry's name: "Spring stream closure", "Bait ban"
      a zone rule        by the rule's own generated line, quoted ("“No fishing for bass”");
                         a blanket closure, or a rule with no line, by its entry's name
                         ("Skeena and Nass winter closures")
      another water's    that water's display name and the rule's kind: "Columbia Lake's
                         tributaries closure", "Slocan River's trout and char release"

    `entries` is {entry_id: CatalogueEntry} for the whole corpus (the bundle hands it in). Without
    it, or when the lifted rule cannot be found, the slug is the fallback — still true, less kind:
    "Columbia lake s tributaries lifted" is what that fallback read like."""
    entries = entries or {}
    if x.default_id:
        # A ZONE ENTRY'S `name` is what it is ("Spring stream closure"); its `display_name` is
        # where it is ("Every stream in Region 3"), which is not what was lifted.
        for eid, ce in entries.items():
            if eid.startswith("z") and eid.split(":", 1)[1] == x.default_id:
                return ce.name
        return x.default_id.replace("_", " ")
    owner = entries.get(x.entry_id) if x.entry_id else None
    if x.entry_id:
        t = next((q for q in owner.rules if q.rule_id == x.target), None) if owner else None
        sib = {q.rule_id: q for q in owner.rules} if owner else None
    else:
        t, sib = (siblings or {}).get(x.target), siblings
    slug = ((x.entry_id or "").split(":", 1)[-1].split("@", 1)[0] if x.entry_id
            else (x.target or "").split(".", 1)[0]).replace("_", " ")
    if t is None:
        return slug
    if owner is not None and not x.entry_id.startswith("z"):
        # ANOTHER WATER'S RULE is named by that water: the lifter's reader knows the water.
        disp = owner.display_name or owner.name
        if _is_closure(t):
            kind = "tributaries closure" if t.tributaries_only else "closure"
        elif t.type is RuleType.retention_limit and t.take == 0:
            kind = f"{species_words(t.species, t.species_except).lower()} release"
        else:
            kind = _lifted_rule_words(t, sib).lower() or "rule"
        return f"{disp}'s {kind}"
    words = _lifted_rule_words(t, sib)
    if owner is not None and (not words or (_is_closure(t) and list(t.species) == ["ALL_GAME_FISH"]
                                            and not t.species_except)):
        # "No fishing, in streams, Jan 1-Jun 15" says less than "Skeena and Nass winter
        # closures"; a notice has no generated line at all.
        return owner.name
    return f"“{words}”" if words else slug


def _lifted_names(r: CatalogueRule, siblings: Optional[dict] = None,
                  entries: Optional[dict] = None) -> list:
    """The names of what `r` lifts (`_lift_name`), each once."""
    return list(dict.fromkeys(n for n in (_lift_name(x, siblings, entries) for x in r.exempts)
                              if n))


def _lifts(r: CatalogueRule, siblings: Optional[dict] = None,
           entries: Optional[dict] = None) -> str:
    """"lifts <what>": what an `exempts` names, in words (`_lift_name`). One printed sentence can
    lift two defaults and is then two rules (Kootenay River's "EXEMPT from Apr 1-June 14 closure
    AND from Nov 1-Mar 31 trout/char catch and release"); this part is what tells them apart."""
    said = _lifted_names(r, siblings, entries)
    if not said:
        return ""
    # A LIFT IS NEVER WIDER THAN ITS LIFTER: a rule that names fish lifts only for them —
    # "except burbot, which may also be speared" lifts the spear closure for burbot, not for all.
    every = list(r.species) == ["ALL_GAME_FISH"] and not r.species_except
    fish = species_words(r.species, r.species_except).lower() if r.species and not every else ""
    tail = f" for {fish}" if fish and not _lift_covers_fish(fish, said) else ""
    return f"lifts {', '.join(said)}{tail}"


def _lift_covers_fish(fish: str, names: list) -> bool:
    """Does what a lift names already say the lifter's fish, so "for <fish>" would add nothing?
    The lifted line names it ("“Trout and char — …”" lifted by trout and char), or — "Trout and
    char" lifting "“Trout (none under 30 cm), from streams”", whose row naming char apart made it
    trout only (`trout_scope_problems`) — the lift is of the whole rule, and "for trout and char"
    would claim it reaches char the rule never bound. Used by the `lifts` part and by the line of
    a rule that only lifts, so the two read alike (Seeley Creek, Station Creek)."""
    if any(fish in n.lower() for n in names):
        return True
    return fish.startswith("trout and char") and any(n.lower().startswith("“trout")
                                                     for n in names)


#: THE PARTS A RULE'S LINE IS MADE OF, and the order a reader composes them in (`compose`). Each
#: is generated from structured fields only; a part with nothing to say is ABSENT; no part is ever
#: the verbatim, which is shown underneath as the book's own text.
#:
#:   what        the rule itself: its verdict and subject — "No fishing for bull trout in streams",
#:               "Rainbow trout — 2 per day", "Bait ban", "Speed restriction (10 km/h)". ABSENT
#:               when the rule has no structured content (an advisory, a hazard, a bare exemption):
#:               the reader then shows the verbatim, labelled as the book's text.
#:   size        the length bound — "none under 30 cm", "over 50 cm" (a class released or counted)
#:   conditions  which fish or which fishing — "wild only", "from streams", "when fishing for
#:               salmon", "while set lining", "in possession" (a clause's clock)
#:   when        dates, weekdays, hours, an unread season — "Sep 1-Dec 31, on Saturdays"
#:   where       the place in the book's words, or named from what the extents draw
#:   side        the half of the channel it holds on (`side`): "west half of the channel only —
#:               on the east half, this water's other regulations apply"
#:   in_part     the undrawn part it holds in (`undrawn_part`): a note, never a colour
#:   lifts       what it exempts from
#:   duty        what you must do with it — "record your retention on your licence immediately"
#:   suspended   "not while “<the rule it sleeps under>” is in force"
#:   notice      the DFO fishery notice it was published in
#:
#: A BOOK'S REASON IS NOT A PART. "(located in an Ecological Reserve)" is prose in the verbatim;
#: the model has no field for a reason — `reason` was retired because it held a citation here, an
#: explanation there and a hidden condition elsewhere — and a part is generated from fields only.
#: A reason therefore reaches a reader through the verbatim shown underneath, never paraphrased.
LABEL_PARTS = ("what", "size", "conditions", "when", "where", "side", "in_part", "lifts", "duty",
               "suspended", "notice")


def label_parts(r: CatalogueRule, siblings: Optional[dict] = None, place_of=None,
                entries: Optional[dict] = None) -> dict:
    """The line a reader sees, as PARTS — see `LABEL_PARTS`. `compose` joins them.

    `siblings` is {rule_id: CatalogueRule} for the rule's entry, so a rule that points at another
    (`suspended_while`, `within`) can say what it points at in words.

    `place_of(extents) -> str | None` names WHERE a bound rule applies, in the book's words, from
    its structured extents — a split's curated label, a lake's name, an area's name. It is handed
    in because the names live in the atlas, which this module never reads; without it only
    `extent_text` can name a place (see `_where`).

    `entries` is {entry_id: CatalogueEntry} for the corpus, so a rule that lifts another entry's
    rule names it in words (`_lift_name`); without it the lifted entry's slug is said."""
    p: dict = {"when": _when_words(r), "where": _where(r, place_of),
               "side": _side_words(r),
               "in_part": strip_list_marker(r.undrawn_part),
               "lifts": _lifts(r, siblings, entries),
               "suspended": _suspended(r, siblings, place_of),
               "notice": f"fishery notice {r.notice}" if r.notice else ""}
    cond: list = []
    duty: list = []
    t = r.type
    sp = species_words(r.species, r.species_except)
    # A DUTY ON A RULE THAT IS NOT ABOUT GEAR QUALIFIES IT; it does not replace it. Rendered alone,
    # the rule's own half of the sentence reads as unconditional.
    gear_type = t in (RuleType.tackle_restriction, RuleType.bait_restriction,
                      RuleType.method_rule, RuleType.handling_rule)
    if r.conduct and not r.gear and not gear_type:
        duty += [CONDUCT_ACTS.get(a, a.replace("_", " ")) for a in r.conduct]
        r = r.model_copy(update={"conduct": []})
    # GEAR AND CONDUCT ARE READ FIRST, FOR EVERY TYPE. The direction lives in the clause that
    # carries the subject; wired into one type's branch instead, every OTHER type fell through to
    # its bare verbatim, and a "You must not:" fragment then read as a permission.
    said = _gear_words(r) if (r.gear or r.conduct) else ""
    if r.lift_only:
        # A RULE THAT ONLY LIFTS says so in its own words — "Spring stream closure lifted",
        # "Steelhead stream closure lifted" — never by falling back to the book's sentence. The
        # `lifts` part would repeat it, so it is the `what` instead.
        names = _lifted_names(r, siblings, entries)
        head = " and ".join(names) + " lifted"
        every = list(r.species) == ["ALL_GAME_FISH"] and not r.species_except
        fish = species_words(r.species, r.species_except).lower() if r.species else ""
        if fish and not every and not _lift_covers_fish(fish, names):
            head += f" for {fish}"
        p["what"] = head[:1].upper() + head[1:]
        p["lifts"] = ""
        cond += _scope(r, taking=False)
    elif said:
        p["what"] = said
        if r.when_targeting:
            cond.append(f"when fishing for {species_words(r.when_targeting).lower()}")
        if r.while_:
            cond.append("while " + " or ".join(w.replace("_", " ") for w in r.while_))
        # the `while` is said above; the scope must not say it again as "taken on a set line".
        # A gear rule holds IN streams; "from streams" is how a quota counts fish.
        cond += _scope(r.model_copy(update={"while_": []}), taking=not gear_type)
    elif t is RuleType.retention_limit:
        # BRANCH ON may_target FIRST. take=0 alone is ambiguous, and reading it as "release all"
        # turns all 605 "No fishing" rules into a catch-and-release PERMISSION.
        if r.take == 0 and r.may_target is False:
            if sp == "All game fish" and r.species_except:
                head = f"No fishing except for {species_words(r.species_except).lower()}"
            elif sp == "All game fish" and not r.while_:
                head = "No fishing"
            elif r.while_:
                # WITH A `while` THE WAY OF FISHING LEADS AND THE SPECIES IS THE RULE: "only
                # non-game fish may be speared" is take 0 on every game fish while spear fishing.
                # "No fishing by spear fishing" dropped the species and read as a total spear ban.
                fish = "game fish" if sp in ("", "All game fish") else sp.lower()
                head = f"No {' or '.join(m.replace('_', ' ') for m in r.while_)} for {fish}"
            else:
                head = f"No fishing for {sp.lower()}"
            if r.water:                     # "in streams" reads as part of the phrase
                head += f" in {r.water.value}s"
            if r.while_ and sp == "All game fish" and r.species_except:
                # "No fishing except for X" keeps its verb; the way of fishing follows it
                head += " by " + " or ".join(m.replace("_", " ") for m in r.while_)
            p["what"] = head
            cond += _scope(r.model_copy(update={"water": None, "while_": []}), taking=False)
        else:
            head = None
            if r.take == 0:
                head = f"{sp} — release all"
            elif r.unlimited:
                head = f"{sp} — no limit"
            elif r.take is not None:
                # A CLAUSE COUNTS ON ITS PARENT'S CLOCK. Bennett Lake prints "Lake trout daily and
                # possession quotas = 2 (only 1 over 90 cm …)", a clause under each quota; named
                # without the clock the two clauses read identically.
                parent = (siblings or {}).get(r.within) if r.within else None
                clock = parent.clock if parent is not None else r.clock
                noun = {Period.daily: "per day", Period.possession: "in possession",
                        Period.annual: "per licence year", Period.monthly: "per month"}[clock]
                if r.within and r.lengths:
                    head = sp                       # the size phrase carries the count
                    if clock is not Period.daily:
                        cond.append(noun)
                else:
                    head = f"{sp} — {r.take} {noun}"
                    if len(expand_species(list(r.species or []))) > 1:
                        head += ", all species combined"
            elif r.per_daily is not None:
                head = (f"{sp or 'All game fish'} — possession quota is {r.per_daily} daily "
                        f"quota" + ("s" if r.per_daily != 1 else ""))
            elif r.lengths:
                head = sp                  # a size gate with no count: the region supplies it
            elif r.record_retention:
                head = sp                  # a duty about these fish: "record your retention"
            if head:
                p["what"] = head
                p["size"] = _size(r).strip()
                if p["size"].startswith("(") and p["size"].endswith(")"):
                    p["size"] = p["size"][1:-1]
            cond += _scope(r)
            if r.record_retention:
                duty.append("record your retention on your licence immediately")
    elif t is RuleType.vessel_rule:
        if r.aspect is VesselAspect.speed:
            head = f"Speed restriction ({r.max_kmh:g} km/h)" if r.max_kmh else "Speed restriction"
        elif r.aspect is VesselAspect.towing:
            head = "No towing"
        else:
            kw = r.max_power_kw
            head = {PropulsionLevel.none: "No vessels",
                    PropulsionLevel.unpowered: "No powered boats",
                    PropulsionLevel.electric_only:
                        f"Electric motor only (max {kw:g} kW)" if kw
                        else "Electric motor only (max 7.5 kW)",
                    PropulsionLevel.power_capped:
                        f"Engine power restriction {kw:g} kW ({_HP.get(kw, '')} hp)" if kw
                        else "Engine power restriction"}[r.level]
        p["what"] = head
    elif t is RuleType.angler_closure:
        # The subject is the angler, so the line leads with WHO — "Angling closed to non-guided
        # non-resident aliens". `taking=False`: a closure is "in", never "from".
        who = r.closed_to.words() if r.closed_to else "some anglers"
        p["what"] = f"Angling closed to {who}"
        if r.closed_to_except:
            p["what"] += ", except " + " and ".join(w.words() for w in r.closed_to_except)
        cond += _scope(r, taking=False)
    elif t is RuleType.stop_fishing_after_quota:
        fish = sp.lower() if sp else "fish"
        if r.origin is not None:
            fish = f"{r.origin.value} {fish}"
        p["what"] = (f"Stop fishing the water for the rest of the day once you have kept your "
                     f"daily quota of {fish}")
    # Everything else — navigation_duty, handling_rule without gear, hazard, advisory,
    # program_membership, facility, and a bare exemption — has no `what`: its sentence IS the
    # rule, and the reader shows the verbatim.
    p["conditions"] = ", ".join(cond)
    p["duty"] = "; ".join(d[:1].lower() + d[1:] for d in duty)
    out = {}
    for k in LABEL_PARTS:
        v = re.sub(r"\s+", " ", _EMPHASIS.sub("", p.get(k) or "")).strip()
        if v:
            out[k] = v
    return out


def compose(parts: dict, verbatim: str = "") -> str:
    """ONE line from the parts — the ONE composer, so a preview cannot drift from what a reader
    composes by the guide. Order: what (size), conditions, when — where — side — in part: … — lifts —
    duty — not while … (notice). With no `what`, the line is the book's own sentence (emphasis
    and list marker stripped) and what it lifts — the preview only; no part ever carries it."""
    head = parts.get("what")
    if head and parts.get("size"):
        s = parts["size"]
        # A size CLASS attaches ("release all over 50 cm"); a BOUND is parenthesised
        # ("(none under 30 cm)") — a bare "under 25 cm" after a species reads as a description.
        head += f" {s}" if s.startswith(("over ", "under ")) else f" ({s})"
    if not head:
        # THE BOOK'S SENTENCE IS THE RULE, and it already says its own when and where: appending
        # them would print the place twice. Only what tells two such rules apart is added.
        out = strip_list_marker(re.sub(r"\s+", " ", _EMPHASIS.sub("", verbatim or "")).strip())
        return out + (" — " + parts["lifts"] if parts.get("lifts") else "")
    out = head
    for k in ("conditions", "when"):
        if parts.get(k):
            out += ", " + parts[k]
    if parts.get("where"):
        out += " — " + parts["where"]
    if parts.get("side"):
        out += " — " + parts["side"]
    if parts.get("in_part"):
        out += " — in part: " + parts["in_part"]
    for k in ("lifts", "duty", "suspended"):
        if parts.get(k):
            out += " — " + parts[k]
    if parts.get("notice"):
        out += f" ({parts['notice']})"
    return out


def label(r: CatalogueRule, siblings: Optional[dict] = None, place_of=None,
          entries: Optional[dict] = None) -> str:
    """The composed line — `compose(label_parts(...))`. A convenience preview for tools and the
    review app; a reader composes its own from the parts."""
    return compose(label_parts(r, siblings, place_of, entries), r.verbatim)



# --------------------------------------------------------------------------------------- #
# Licensing labels — generated from the record's fields, like every other label. The verbatim
# stays on the record underneath. A per-angler SENTENCE ("as a non-resident on Skeena River 2
# today you need …") is composed by the reader, where a test can pin the whole string; these are
# the per-record lines it is composed from.
# --------------------------------------------------------------------------------------- #

def _docs(ds, joiner: str = "and") -> str:
    names = [_DOC_WORDS.get(getattr(d, "value", d), str(getattr(d, "value", d)).replace("_", " "))
             for d in ds]
    return names[0] if len(names) == 1 else ", ".join(names[:-1]) + f" {joiner} " + names[-1]


def _a(doc) -> str:
    """One document with its article: "a Classified Waters Licence", "an angling guide licence",
    and none for a permission, which is not a thing you hold one of."""
    w = _docs([doc])
    if w.startswith("permission"):
        return w
    return ("an " if w[:1].lower() in "aeiou" else "a ") + w


def _cap(s: str) -> str:
    return s[:1].upper() + s[1:]


def unit_words(unit: str, units: Optional[dict] = None) -> str:
    """A licence unit as a reader knows it: the name a designation prints for it. A unit no
    designation names yet (Skookumchuck Creek, whose water is not in the catalogue) reads as its
    words, capitalised — never as the slug."""
    got = (units or {}).get(unit)
    return got or " ".join(w.capitalize() for w in unit.split("_"))


def _path_words(p: "Path") -> str:
    """One way to satisfy a requirement, as an object of "need": "a basic angling licence and a
    Classified Waters Licence"."""
    if p.hold:
        return " and ".join(_a(d) for d in p.hold)
    if p.accompanied_by is not None:
        who = p.accompanied_by.who
        # "anglers 16 and over who hold" read as a crowd; the book means one companion.
        companion = ("someone 16 or over" if who == Who(age=["16_plus"])
                     else f"one of the {who.words()}")
        out = (f"be accompanied by {companion} who holds the licences and stamps this fishing "
               f"requires")
    else:
        out = f"hold what {p.as_.words()} must hold"
    out += {"counts_to_companion": " (any fish you keep count toward your companion's limit)",
            "own": " (you keep your own quota)"}[p.quota]
    return out


def _doing_words(d: "Doing") -> str:
    sp = species_words(expand_species(d.species) if len(d.species) == 1
                       and d.species[0] == "CHAR" else d.species,
                       d.species_except).lower()
    # KOKANEE IS A SALMON TO A BIOLOGIST AND NOT TO THIS GROUP. The book says "(other than
    # kokanee)" and the group already leaves it out, so the label says what the group means
    # rather than letting "salmon" read as every salmon.
    if "SALMON" in d.species and "KO" not in expand_species(list(d.species)):
        sp += " (not kokanee)"
    if d.origin is not None and sp:
        sp = f"{d.origin.value} {sp}"
    if d.act == "fishing":
        return "to fish"
    if d.act == "targeting":
        return f"to fish for {sp}"
    if d.act == "retaining":
        size = ""
        if d.lengths:
            b = d.lengths[0]
            size = (f" {b.min_cm}–{b.max_cm} cm" if b.min_cm is not None and b.max_cm is not None
                    else f" over {b.min_cm} cm" if b.min_cm is not None
                    else f" under {b.max_cm} cm")
        return f"to keep {sp}{size}"
    if d.act == "retaining_recorded":
        return "when keeping a fish whose retention you must record on your licence"
    return "to guide anglers"


#: THE PARTS OF A LICENSING RECORD'S LINE, as for a rule (`LABEL_PARTS`): each generated from the
#: record's fields, absent when it has nothing to say, never the verbatim. `compose_licensing`
#: joins them; the kind decides which appear.
#:
#:   who         the anglers it is about — "Anglers 16 and over", "Non-residents"
#:   what        the fact itself: "Class II Classified Water", "Not a Classified Water", the
#:               document sold ("Annual Classified Waters Licence for non-residents"), what an
#:               alternative accepts ("A Yukon angling licence is also accepted here")
#:   need        what must be held — "a basic angling licence or …"; "no basic angling licence"
#:               on an exemption
#:   must        a duty — "carry your paper licence", "produce your licence on request"
#:   way         a way to satisfy it that is not a document — "be accompanied by someone 16 or
#:               over who holds …"
#:   doing       the activity that triggers it — "to fish", "to keep steelhead"
#:   where       "on a classified stream during its classified period", "on streams"
#:   when        the dates, days and hours it holds
#:   unit        the licence unit(s), as the page names them
#:   stamp       the classified-water Steelhead Stamp: its period here, or its waiver
#:   terms       how a document is sold — "sold by the day; at most 8 days per licence year"
#:   instead     what an alternative stands in for — "in place of a basic angling licence"
#:   except      anglers taken out of `who`
#:   suspended   "not in force while “<closure>” applies"
#:   note        "provincial licences are not valid here" (a superior authority)
LICENSING_PARTS = ("who", "what", "need", "must", "way", "doing", "where", "when", "unit", "stamp",
                   "terms", "instead", "except", "suspended", "note")


def licensing_parts(rec, siblings: Optional[dict] = None, *, units: Optional[dict] = None,
                    refs: Optional[dict] = None) -> dict:
    """One licensing record's line, as PARTS — see `LICENSING_PARTS`; `compose_licensing` joins.

    Context a record cannot carry itself, all optional:
      siblings  {rule_id: CatalogueRule} of the record's entry, so a suspension names its
                closure in the closure's own words;
      units     {unit: unit_name} across the corpus, so terms name a licence unit the way the
                page prints it ("Dean River Class I - Main Section"), never as a slug;
      refs      {(entry_id, id): record} across the corpus, so an alternative says WHICH
                licence it stands in for, never an id.
    """
    p: dict = {}
    if isinstance(rec, Designation):
        p["what"] = f"Class {rec.classified} Classified Water"
        if rec.when and not rec.when.is_empty():
            p["when"] = rec.when.words()
        p["unit"] = rec.unit_name
        if rec.steelhead_stamp_during is not None:
            w = rec.steelhead_stamp_during.when
            p["stamp"] = ("Steelhead Stamp required whatever you fish for"
                          + (f", {w.words()}" if not w.is_empty() else ""))
        elif rec.steelhead_stamp_waived is not None:
            p["stamp"] = "Steelhead Stamp not required here unless you fish for steelhead"
        said = []
        for s in rec.suspended_while:
            other = (siblings or {}).get(s.rule_id)
            said.append(label(other) if other is not None else "its closure")
        if said:
            p["suspended"] = "; ".join(f"not in force while “{x}” applies" for x in said)
    elif isinstance(rec, NotClassified):
        p["what"] = "Not a Classified Water"
    elif isinstance(rec, Requirement):
        if rec.who is not None:
            p["who"] = _cap(rec.who.words())
        water = rec.water.value if rec.water is not None else None
        if rec.on is not None:
            period = {"classified_period": "its classified period",
                      "steelhead_period": "its Steelhead Stamp period"}[rec.on]
            p["where"] = f"on a classified {water or 'water'} during {period}"
        elif water:
            p["where"] = f"on {water}s"
        if rec.when and not rec.when.is_empty():
            p["when"] = rec.when.words()
        # "to fish" adds nothing to a duty you have only while fishing.
        if not (rec.conduct and rec.doing.act == "fishing"):
            p["doing"] = _doing_words(rec.doing)
        if rec.conduct:
            # A duty is an instruction: "Anglers 16 and over: carry your paper licence when …".
            p["must"] = "; ".join(CONDUCT_ACTS[a] for a in rec.conduct)
        elif all(q.hold for q in rec.satisfied_by):
            p["need"] = " or ".join(_path_words(q) for q in rec.satisfied_by)
        else:
            # A path that is not a document is an instruction: "…: to fish, be accompanied by …".
            p["way"] = ", or ".join(_path_words(q) for q in rec.satisfied_by)
        if rec.who_except is not None:
            p["except"] = f"except {rec.who_except.words()}"
        if rec.authority == "superior":
            p["note"] = "provincial licences are not valid here"
    elif isinstance(rec, LicenceTerms):
        doc = _docs([rec.document])
        if rec.classified:
            doc = f"Class {rec.classified} {doc}"
        if rec.sold == "per_licence_year":
            doc = "annual " + doc
        p["what"] = _cap(doc) + (f" for {rec.who.words()}" if rec.who is not None else "")
        if rec.units:
            # Semicolons, because a printed unit name may carry its own comma ("Dean River
            # Class I, signs 100 m below the canyon to tidal boundary").
            p["unit"] = "; ".join(unit_words(u, units) for u in rec.units)
        bits = []
        if rec.sold == "per_day":
            bits.append("sold by the day")
        if rec.covers:
            bits.append({"every_unit": "valid on every classified water",
                         "one_unit": "valid only on the one water it names"}[rec.covers])
        if rec.max_consecutive_days:
            bits.append(f"at most {rec.max_consecutive_days} consecutive days per licence")
        if rec.max_days_per_licence_year:
            bits.append(f"at most {rec.max_days_per_licence_year} days per licence year")
        if rec.unlimited_days:
            bits.append("no limit on the number of days")
        if rec.max_per_licence_year:
            n = rec.max_per_licence_year
            bits.append(f"at most {n} {'licence' if n == 1 else 'licences'} per licence year")
        if rec.max_units_per_licence_year:
            n = rec.max_units_per_licence_year
            bits.append(f"at most {n} of these waters per licence year" if len(rec.units) > 1
                        else f"at most {n} {'water' if n == 1 else 'waters'} per licence year")
        if rec.allocation:
            bits.append({"open": "on open sale", "booking": "by first-come-first-served booking",
                         "draw": "by annual limited-entry draw"}[rec.allocation])
        if rec.needs:
            bits.append("needs your angling guide's number")
        if rec.fee_cad is not None:
            bits.append(f"reduced fee ${rec.fee_cad:.2f}")
        p["terms"] = "; ".join(bits)
    elif isinstance(rec, Exemption):
        p["who"] = _cap(rec.who.words())
        p["need"] = f"no {_docs(rec.documents, 'or')}"
    elif isinstance(rec, Alternative):
        target = (refs or {}).get((rec.alternative_to.entry_id, rec.alternative_to.id))
        if isinstance(target, Requirement) and target.satisfied_by:
            instead = " or ".join(_path_words(q) for q in target.satisfied_by)
        else:
            instead = "the licence otherwise required"
        p["what"] = (_cap(" or ".join(_path_words(q) for q in rec.satisfied_by))
                     + " is also accepted here")
        p["instead"] = f"in place of {instead}"
    else:
        raise TypeError(f"not a licensing record: {type(rec).__name__}")
    return {k: re.sub(r"\s+", " ", _EMPHASIS.sub("", p[k])).strip()
            for k in LICENSING_PARTS if (p.get(k) or "").strip()}


def compose_licensing(parts: dict) -> str:
    """ONE sentence from a licensing record's parts — the one composer (see `compose`)."""
    g = parts.get
    tail = (f" {g('where')}" if g("where") else "") + (f", {g('when')}" if g("when") else "")
    if g("need") is not None:
        out = f"{g('who') or 'You'} need {g('need')}" + (f" {g('doing')}" if g("doing") else "")
        out += tail
    elif g("must") is not None or g("way") is not None:
        body = (g("must") + (f" {g('doing')}" if g("doing") else "") + tail if g("must")
                else (g("doing") or "") + tail + ", " + g("way"))
        out = f"{g('who')}: {body[:1].lower() + body[1:]}" if g("who") else _cap(body)
    else:
        out = g("what") or ""
        if g("when"):
            out += f", {g('when')}"
        if g("unit"):
            out += (f" ({g('unit')})" if g("terms") is not None
                    else f" (licence unit: {g('unit')})")
        if g("terms") is not None:
            out += f": {g('terms')}"
        if g("instead"):
            out += f", {g('instead')}"
        if g("stamp"):
            out += f". {g('stamp')}"
        if g("suspended"):
            out += ". " + ". ".join(_cap(x) for x in g("suspended").split("; "))
    if g("except"):
        out += f" ({g('except')})"
    if g("note"):
        out += f"; {g('note')}"
    return out + "."


def licensing_label(rec, siblings: Optional[dict] = None, *, units: Optional[dict] = None,
                    refs: Optional[dict] = None) -> str:
    """The composed sentence — `compose_licensing(licensing_parts(...))`; a preview only."""
    return compose_licensing(licensing_parts(rec, siblings, units=units, refs=refs))



# --------------------------------------------------------------------------------------- #
# Entries
# --------------------------------------------------------------------------------------- #

def _extent_errors(entry: "CatalogueEntry") -> List[str]:
    """Every extent the entry, its rules and its licensing records carry, read through `Extent`.

    The catalogue stores extents as plain dicts, and nothing read them through the model: an arity
    error or a misspelt key reached the reach builder, which ignores what it does not know. Now
    every one is validated (4,900 passed unchanged when this was added).

    A WATERSHED PART MAY NOT WALK. `Extent.watershed` already selects every lake and stream of the
    river's basin on its side of the cut; a tributary walk from it would climb out of the
    downstream part into the upstream one — the side the book excluded. So a rule or record with
    one is refused `includes_tributaries: true`, its own or its entry's. `tributaries_only` stays:
    on a watershed it means the part without the river itself."""
    from pipeline.regs.parsing.entry_models import Extent

    e: List[str] = []

    def check(where: str, extents) -> bool:
        wet = False
        for i, x in enumerate(extents or []):
            if not isinstance(x, dict):
                e.append(f"{where}: extent {i} is not an object")
                continue
            try:
                wet = Extent.model_validate(x).watershed or wet
            except ValueError as err:
                e.append(f"{where}: extent {i}: {squash(str(err))[:200]}")
        return wet

    check("entry extents", entry.extents)
    holders = [(f"{r.rule_id}", r) for r in entry.rules] + \
              [(f"{x.kind} {x.id}", x) for x in entry.licensing]
    for where, h in holders:
        wet = check(where, getattr(h, "extents", None))
        check(f"{where} tributary_excludes", getattr(h, "tributary_excludes", None))
        own = getattr(h, "includes_tributaries", None)
        walks = own if own is not None else entry.includes_tributaries
        if wet and walks:
            e.append(f"{where}: a `watershed` extent already holds the tributaries of its part — "
                     f"includes_tributaries{' (inherited from the entry)' if own is None else ''} "
                     f"would walk out of it into the other side; drop it")
    return e


class CatalogueEntry(BaseModel):
    """One row of the synopsis — a water, a zone, or the province — and its typed rules.

    `regs_verbatim` is the whole printed passage; every rule's `verbatim` must be a contiguous
    substring of it. That chain of custody is what caught an invented 50 cm sub-limit on the first
    sample, and what refused a Classified Waters entry whose text mentioned Kootenay Class II
    waters that no rule covered.
    """
    model_config = ConfigDict(frozen=True, extra="forbid")

    entry_id: str
    name: str
    display_name: str = ""
    region: str = ""
    scope_note: str = ""
    regs_verbatim: str = Field(..., min_length=1)
    source_pages: List[int] = Field(default_factory=list)
    symbols: List[str] = Field(default_factory=list)
    matched: List[str] = Field(default_factory=list)
    extents: List[dict] = Field(default_factory=list)
    includes_tributaries: Optional[bool] = None
    rules: List[CatalogueRule] = Field(default_factory=list)
    #: WHAT YOU MUST HOLD, AND WHAT THIS WATER IS — see the licensing section above. Not rules:
    #: licensing never competes and never votes on open/closed, and it depends on who the angler
    #: is and what they are doing, which no rule takes.
    licensing: List[LicensingRecord] = Field(default_factory=list)
    #: POINTERS — "See Lonzo Creek" (see `See`). Not rules: they bind nothing. A row whose ONLY
    #: content is a pointer has no rules at all, and says so by having only this.
    see: List[See] = Field(default_factory=list)
    #: ANADROMOUS RAINBOW TROUT ARE FOUND IN THIS ROW'S WATER, so the book's definition holds here
    #: (p.86: "steelhead: a rainbow trout longer than 50 cm in waters where anadromous rainbow trout
    #: are found" — `DEFINITIONAL_SIZE`): a rainbow over 50 cm IS a steelhead, governed by the
    #: steelhead rules, and a rainbow rule speaks only for rainbow of 50 cm or less. The book states
    #: the definition for every such water but lists none, so it is set per row where the fact is
    #: known (Chilliwack/Vedder, user ruling 2026-09-25), never inferred. The bundle marks the row's
    #: waters (`steelhead_water`); `read.effective_rules` reads a rainbow there by it.
    anadromous_rainbow: bool = False

    @property
    def pointer_only(self) -> bool:
        """A row that states no regulation of its own — only where to look (`see`)."""
        return bool(self.see) and not self.rules and not self.licensing

    @model_validator(mode="after")
    def _book_species_only(self) -> "CatalogueEntry":
        """A SYNOPSIS ROW NAMES ONLY THE BOOK'S SPECIES (p.86). A bare `CatalogueRule` also
        accepts the federal salmon codes the DFO feed types (`FEDERAL_SALMON`); a row of the
        book does not — its salmon rules name `SALMON`."""
        bad = []
        for r in self.rules:
            bad += species_problems(set(r.species) | set(r.species_except),
                                    f"{r.rule_id}.species")
            bad += species_problems(set(r.when_targeting), f"{r.rule_id}.when_targeting")
            for c in r.gear:
                if c.when is not None:
                    bad += species_problems(set(c.when.targeting),
                                            f"{r.rule_id}.gear.when.targeting")
        if bad:
            raise ValueError("; ".join(bad))
        return self

    @model_validator(mode="after")
    def _trout_scope(self) -> "CatalogueEntry":
        """"TROUT" INCLUDES CHAR UNLESS THE ROW MENTIONS CHAR (user ruling 2026-09-28) — see
        `trout_scope_problems`. The row is this entry: a water's row, or a zone table."""
        bad = trout_scope_problems(self.entry_id, self.regs_verbatim, self.rules)
        if bad:
            raise ValueError(f"{self.entry_id}: " + "; ".join(bad))
        return self

    @model_validator(mode="after")
    def _chain_of_custody(self) -> "CatalogueEntry":
        e: List[str] = []
        seen: set[str] = set()
        haystack = squash(self.regs_verbatim)
        # A POINTER QUOTES THE ROW and never points at the row itself. Whether its targets exist
        # is a question about the corpus (the bundle build and `test_see_pointers` ask it).
        for s in self.see:
            if squash(s.verbatim) not in haystack:
                e.append(f"see {s.verbatim[:40]!r}: not a contiguous substring of regs_verbatim")
            if self.entry_id in s.entry_ids:
                e.append(f"see {s.verbatim[:40]!r}: points at its own entry")
        # A POINTER IS NOT A RULE. An information rule whose words only say where else to look
        # ("See Lonzo Creek", "A tributary of Slocan River. See Slocan River") is a `see` edge;
        # written as an advisory it binds the water and shows the pointer as a regulation.
        for r in self.rules:
            if r.type is RuleType.advisory and _POINTER_WORDS.search(squash(r.verbatim)) \
                    and not _NOT_A_WATER.search(squash(r.verbatim)):
                e.append(f"{r.rule_id}: {squash(r.verbatim)[:50]!r} is a pointer to another "
                         f"row — write it as `see` (with the entry it names), not as a rule")
        for r in self.rules:
            if r.rule_id in seen:
                e.append(f"duplicate rule_id {r.rule_id!r}")
            seen.add(r.rule_id)
            needle = squash(r.verbatim)
            if needle not in haystack:
                e.append(f"{r.rule_id}: verbatim is not a contiguous substring of regs_verbatim")
        # A LIST NUMBER IS LAYOUT, NOT THE SENTENCE. "3. Within 23 m downstream of …" is item 3 of
        # the book's list of no-fishing places; quoted with its number the rule reads "3. Within
        # 23 m …" on every water in the province. A verbatim starts at the sentence.
        quoted = [(r.rule_id, r.verbatim) for r in self.rules] + \
            [(f"licensing {x.id}", x.verbatim) for x in self.licensing] + \
            [("see", s.verbatim) for s in self.see]
        for who, v in quoted:
            m = LIST_MARKER.match(v or "")
            if m:
                e.append(f"{who}: verbatim starts with the list marker {m.group(0).strip()!r} — "
                         f"quote the sentence, not its number")
        ids = {r.rule_id for r in self.rules}
        for r in self.rules:
            # A TARGET NAMES A RULE THAT EXISTS. One in this entry is checked here; one in another
            # entry (`entry_id`) by the file, and across files by the bundle build.
            for x in r.exempts:
                if x.entry_id == self.entry_id:
                    e.append(f"{r.rule_id}: exempts names its own entry in entry_id — leave it "
                             f"out; a target with no entry_id is this entry's")
                elif x.target and not x.entry_id and (x.target not in ids
                                                      or x.target == r.rule_id):
                    e.append(f"{r.rule_id}: exempts target {x.target!r} names no other rule in "
                             f"this entry")
            if r.suspended_while and (r.suspended_while not in ids
                                      or r.suspended_while == r.rule_id):
                e.append(f"{r.rule_id}: suspended_while={r.suspended_while!r} names no other rule "
                         f"in this entry")
        # A CLAUSE IS IN FORCE ONLY WHILE ITS QUOTA IS. "Bull trout daily quota = 1 (none under
        # 80 cm), Jul 1-30 and Nov 1-Dec 31" is one sentence; stored as a quota with the dates
        # and a `within` clause without them, the clause read as ALL YEAR (an absent `when` is all
        # year) and showed "none under 80 cm" on Aug 15 beside the release. A readers' `within`
        # does not carry time, so the clause states it: its days must lie inside its parent's.
        parents = {r.rule_id: r for r in self.rules}
        for r in self.rules:
            p = parents.get(r.within) if r.within else None
            if p is None or p.when is None or p.when.is_empty():
                continue
            mine = r.when
            if mine is None or mine.is_empty():
                e.append(f"{r.rule_id}: a clause `within` {p.rule_id}, which holds only on its "
                         f"own `when`, has none — it would read as all year; give it the "
                         f"parent's `when`")
            elif mine.dates and p.when.dates and not _days(mine.dates) <= _days(p.when.dates):
                e.append(f"{r.rule_id}: its `when` holds on days its parent {p.rule_id} does not")
        # EVERY SEASON THE ROW PRINTS REACHES A RULE — see `_dates_lost`.
        e += _dates_lost(self)
        # EVERY QUOTE A LICENSING RECORD CARRIES is chain of custody too — per printed clause, so
        # a stamp period or a suspension note cannot be paraphrased under a real designation.
        lic_ids: set = set()
        units: set = set()
        by_rule = {r.rule_id: r for r in self.rules}
        region = self.entry_id.split(":", 1)[0]
        for x in self.licensing:
            if x.id in lic_ids:
                e.append(f"duplicate licensing id {x.id!r}")
            lic_ids.add(x.id)
            for q in licensing_verbatims(x):
                if squash(q) not in haystack:
                    e.append(f"{x.kind} {x.id}: {q[:50]!r} is not a contiguous substring of "
                             f"regs_verbatim")
            # AN ALTERNATIVE ON A ROW THAT IS NO WATER must name its place itself. On a
            # provincial or zone row (`zp:basic_licence` is the one it would be written on)
            # `whole` or a bare cut has no water to be the whole OF, so the record would be
            # accepted nowhere or — once someone "fixes" that — everywhere.
            if isinstance(x, Alternative) and not self.matched:
                vague = [i for i, ex in enumerate(x.extents)
                         if not (ex.get("item_id") or ex.get("item_ids") or ex.get("area_id")
                                 or ex.get("area_kind") not in (None, "", "region"))]
                if vague:
                    e.append(f"alternative {x.id}: extent(s) {vague} name no place, and this "
                             f"row is not a water — name the item or area it is accepted on")
            if not isinstance(x, Designation):
                continue
            # "two separate Class II waters … require separate licences": one entry, two units.
            if x.unit in units:
                e.append(f"two designations in one entry share unit {x.unit!r}")
            units.add(x.unit)
            for sw in x.suspended_while:
                c = by_rule.get(sw.rule_id)
                if c is None:
                    e.append(f"designation {x.id}: suspended_while names no rule "
                             f"{sw.rule_id!r} in this entry")
                elif not (c.type is RuleType.retention_limit and c.take == 0
                          and c.may_target is False):
                    e.append(f"designation {x.id}: suspended_while {sw.rule_id!r} is not a "
                             f"closure — a designation sleeps under a closure, never a quota")
            # STEELHEAD COUNTRY PRINTS THE STAMP. Outside the Kootenay (Region 4), where Class II
            # waters print no stamp, a designation that says nothing about it has lost a clause.
            if region != "r4" and x.steelhead_stamp_during is None \
                    and x.steelhead_stamp_waived is None and not x.review_reason:
                e.append(f"designation {x.id}: no steelhead_stamp_during or _waived in "
                         f"steelhead country — record the printed clause, or say why in "
                         f"review_reason")
        e += _extent_errors(self)
        # EVERY RULE SAYS WHERE IT IS. Nothing inherits the entry's extents (see
        # `CatalogueRule.extents`), so a rule with none and no words for its place would have a
        # reach nobody stated — which a reader could only fill by guessing.
        for r in self.rules:
            if not r.extents and not r.extent_text.strip() and not r.unresolved_locators:
                e.append(f"{r.rule_id}: says nothing about where it applies — give it `extents` "
                         f"(the entry's reach, written out, if it covers the whole row), or name "
                         f"the place it cannot bind in `extent_text` / `unresolved_locators`")
        if not self.rules and not self.licensing and not self.see:
            e.append("an entry with no rules, no licensing and no `see` says nothing")
        if e:
            raise ValueError(f"{self.entry_id}: " + "; ".join(e))
        return self


class CatalogueFile(BaseModel):
    model_config = ConfigDict(frozen=True)
    region: str
    entries: List[CatalogueEntry]

    @model_validator(mode="after")
    def _unique_ids(self) -> "CatalogueFile":
        ids = [x.entry_id for x in self.entries]
        dupes = {i for i in ids if ids.count(i) > 1}
        if dupes:
            raise ValueError(f"duplicate entry_id(s): {sorted(dupes)}")
        return self

    @model_validator(mode="after")
    def _exempts_name_real_rules(self) -> "CatalogueFile":
        """A target in ANOTHER entry of this file must be one of its rules. (A target in another
        file cannot be seen here; the bundle build refuses it, and a test runs over the corpus.)"""
        rules = {x.entry_id: {r.rule_id for r in x.rules} for x in self.entries}
        bad = [f"{x.entry_id}/{r.rule_id} -> {t.entry_id}#{t.target}"
               for x in self.entries for r in x.rules for t in r.exempts
               if t.entry_id in rules and t.target not in rules[t.entry_id]]
        if bad:
            raise ValueError(f"exempts target(s) name no rule of that entry: {bad}")
        return self
