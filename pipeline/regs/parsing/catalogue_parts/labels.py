"""Rule labels — ONE place structure becomes English (`label_parts`, `compose`, `label`).

Split out of `pipeline/regs/parsing/catalogue.py`, which re-exports every name; import from
there."""

from __future__ import annotations

import re

from typing import List, Optional

from .vocab import Method, Period, PropulsionLevel, RuleType, VesselAspect
from .species import BOOK_FAMILIES, OPEN_SUBJECTS, PROTECTED_FISH, SPECIES_GROUPS, expand_species
from .gear import CAUGHT_HOW, CONDUCT_ACTS, GearClause, Slot, _ANGLER_WORDS
from .checks import part_words, strip_list_marker
from .licensing import slug
from .rule import CatalogueRule, Exempts


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
    # a salmon the book names (`SALMON_FISH`) — not a game fish
    "CH": "Chinook",
    # the protected fish the book names (`PROTECTED_FISH`) — not game fish
    **{c: n for c, (n, _) in PROTECTED_FISH.items()},
    # trout (p.80). GB is Brown Trout (Salmo trutta) in the official table.
    "RB": "Rainbow trout", "ST": "Steelhead", "CT": "Cutthroat trout", "GB": "Brown trout",
    # char. ONE fish for Dolly Varden and bull trout: "Any bull trout that you catch and keep must
    # be counted as part of your Dolly Varden quota" (p.80).
    "DV": "Dolly Varden/bull trout", "LT": "Lake trout", "EB": "Brook trout",
    # whitefish and bass
    "LW": "Lake whitefish", "MW": "Mountain whitefish",
    "LMB": "Largemouth bass", "SMB": "Smallmouth bass",
    # other game fish
    "KO": "Kokanee", "GR": "Arctic grayling", "BB": "Burbot", "WSG": "White sturgeon",
    "BCB": "Black crappie", "NP": "Northern pike", "YP": "Yellow perch", "WP": "Walleye",
    "GE": "Goldeye", "IN": "Inconnu", "CRA": "Crayfish",
}

#: The book's own headings (p.80), for the menu and for display.
_FAMILY_WORDS = {"TROUT": "Trout", "CHAR": "Char", "WHITEFISH": "Whitefish", "BASS": "Bass",
                 "OTHER": "Other game fish"}


def species_menu() -> str:
    """The species vocabulary as the parser sees it: the book's list (p.80), the groups, and the
    rule for choosing between them. Generated from `KNOWN_SPECIES`, so a menu can never offer a
    code validation refuses (it once offered seven)."""
    out = ["THE SPECIES ARE THE BOOK'S LIST AND NOTHING ELSE (p.80, 'Freshwater game fish are",
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
             "PROTECTED_SPECIES": ("the protected list (p.9; Region 2 adds green sturgeon) — a "
                                   "protected-species rule names the members its row prints: "
                                   + ", ".join(f"`{c}`" for c in PROTECTED_FISH)),
             "SALMON": ("Pacific salmon — federal, not part of ALL_GAME_FISH (kokanee is `KO`); "
                        "the one salmon the book names, chinook, is `CH` — a salmon, never a "
                        "game fish")}
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


def is_protected_list(codes) -> bool:
    """A species list made only of protected fish (`PROTECTED_FISH`) — a protected-species rule."""
    return bool(codes) and all(c in PROTECTED_FISH for c in codes)


def species_words(codes: List[str], excepts: List[str] | None = None) -> str:
    if not codes:
        return ""
    if is_protected_list(codes):
        # THE PROTECTED LIST IS SAID AS ONE, with its names as the book cases them ("Cultus Lake
        # sculpin") — `protected_words`; a caller lower-casing a fish name must not reach these.
        return protected_words(codes)
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


def protected_words(codes) -> str:
    """"Protected species: Cultus Lake sculpin, Nooksack dace, … and Salish sucker" — the group's
    word and every member the rule names, in the rule's order. Never lower-cased: the names carry
    places ("Cultus Lake", "Morrison Creek")."""
    names = [_SPECIES_WORDS[c] for c in codes]
    listed = names[0] if len(names) == 1 else ", ".join(names[:-1]) + " and " + names[-1]
    return f"Protected species: {listed}"


def _lower_fish(sp: str) -> str:
    """A species phrase inside a sentence ("No fishing for rainbow trout"). The protected list keeps
    its own casing after its first word ("protected species: Cultus Lake sculpin, …")."""
    if sp.startswith("Protected species: "):
        return "p" + sp[1:]
    return sp.lower()


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
        if c.slot is Slot.bait and c.ban == ["any_bait"] and not c.of and not c.except_:
            # "Bait ban" on EVERY branch: its condition (`when`, `unless`) is appended below
            # like any clause's. Spelled "no any bait", a token read as English.
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
                             getattr(c.when.method, "value", None),
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
    for c in r.caught:
        bits.append(f"if {CAUGHT_HOW[c]['as']}, {CAUGHT_HOW[c]['even']}")
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
        if b.closed:                    # the book's "40 cm or more": the bound goes back too
            words = f"{cm} cm or {'more' if end == 'over' else 'less'}"
            return f" {words}" if r.take == 0 else f" (none {words})"
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
        if any(b.closed and b.min_cm == g.max_cm for b in denied):
            return f" (none {g.max_cm} cm or more)"
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
               "in_part": part_words(r.undrawn_part),
               "lifts": _lifts(r, siblings, entries),
               "suspended": _suspended(r, siblings, place_of),
               "notice": f"fishery notice {r.notice}" if r.notice else ""}
    cond: list = []
    duty: list = []
    t = r.type
    sp = species_words(r.species, r.species_except)
    if r.life_stage is not None and sp:
        # "adult chinook", as the sentence prints it (`life_stage`)
        sp = f"{r.life_stage.value.capitalize()} {sp.lower()}"
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
                head = f"No fishing for {_lower_fish(sp)}"
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
