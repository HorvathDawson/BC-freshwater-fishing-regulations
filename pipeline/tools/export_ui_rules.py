"""Export stage ③ — the rules, and nothing derived from them.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.tools.export_ui_rules OUT.json

THE SHAPE, and why it is this one (see `pipeline/docs/06-ui-data-contract.md`):

  · RULES ARE INTERNED, keyed by `entry::rule`, and everything else indexes into them. A rule
    binds a region and every water in it; repeating it per place triples the file and makes two
    copies of one sentence that can disagree. The bundle interns rule SETS for the same reason.
  · NOTHING SETTLED IS SHIPPED. No tables. Settling — which counter carves which, what the
    numbers come to — is the pipeline's job and a layer of its own; this is the input to it.
  · THE CORRECTNESS EVIDENCE TRAVELS WITH THE RULES. Each region carries its printed-synopsis
    panel: every line of the book's own table, whether the rules agree with it, and which rule
    proves each one. "Can we generate the base region tables and check they are correct" is
    answered inside the file rather than by trusting it.

Three things done to the raw records, all reversible and all labelled:

  · SPECIES CODES ARE EXPANDED to base codes, with `species_written` keeping what the book said,
    so a consumer answering "is my bull trout in this" does not expand TROUT_CHAR first.
  · EVERY RULE CARRIES ITS PROVENANCE — the sentence, who wrote it, what it binds to, its rank.
  · NOTHING ELSE IS COMPUTED. The rules are dumped as the corpus holds them. `reads_as`,
    `size_rule` and `shared_number` were derived here until 2026-09-22 and are gone: this file
    is data, and a reading is not data. What they encoded was not thrown away — it is in
    `field_dictionary` as the conditions a consumer applies, so there is one written answer
    rather than a computed one that hides how it was reached.

Change `REGIONS` / `WATERS` below to widen it. Nothing here is cached or committed.
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict

from pipeline.regs.parsing.catalogue import (DEFINITIONAL_SIZE, SPECIES_GROUPS,
                                             _SPECIES_WORDS, expand_species)
from pipeline.regs.parsing.species import SPECIES
from pipeline.regs.table import state as ST
from pipeline.regs.table.authority import source_of
from pipeline.regs.table.corpus import rid, rules, sections

#: Every chapter the book has. `1hg` is Haida Gwaii, which is administered as Region 1 and
#: printed as a table of its own — listing it under Region 1 offers one table under two names.
REGIONS = ["province", "1", "1hg", "2", "3", "4", "5", "6", "7a", "7b", "8"]
#: Chosen to exercise the cases, not for coverage:
#:   Cowichan       sits inside Region 1's "MUs 1-1 to 1-6", so the area match is demonstrated
#:   Fording        the only stretch where a rank-0 rule and six inherited ones speak at once
#:   Okanagan Lake  narrows the region AND opens what it closed
#:   Shuswap        the most water rules of any stretch
#:   Fraser 17      four exemptions, three of the spring closure and one of a bait ban
#:   Kootenay R 6   exempts a closure by naming a reach the corpus cannot draw
#:   Okanagan R     a closure and its exemption on the same stretch
WATERS = [("Chilliwack River", 0), ("Cowichan River", 2), ("Okanagan Lake", 0),
          ("Atlin Lake", 0), ("Shuswap Lake", 0), ("Fording River", 1), ("Kootenay Lake", 1),
          ("Fraser River", 17), ("Kootenay River", 6), ("Okanagan River", 0),
          ("Kootenay River", 8), ("Chilliwack River", 3)]
CHAPTER = {"province": "zp", "1": "z1", "1hg": "z1", "2": "z2", "3": "z3", "4": "z4",
           "5": "z5", "6": "z6", "7a": "z7a", "7b": "z7b", "8": "z8"}

#: THE SHAPES A `lengths` LIST TAKES, in words. Nothing here is inferred: each is read straight
#: off the ranges and their `take`, and is listed so a consumer can name what it is looking at.
SIZE_READINGS = {
    "floor": "you may keep none SMALLER than this",
    "ceiling": "you may keep none LARGER than this",
    "window": "you may keep only between these two lengths",
    "hole": "you may keep none BETWEEN these two lengths",
    "counts over": "the number counts only the fish larger than this; smaller ones are not "
                   "limited by this rule",
    "which fish": "not a limit — it says which fish the rule is about (a stamp needed for the "
                  "big ones, say)",
}


FIELDS = {
    "_note": "Every rule is a flat dict. 26 fields are on all 3,422 rules in the corpus; the "
             "rest appear only where they mean something. Nothing parses prose — sizes, dates, "
             "methods and documents are all fields.",
    "identity and provenance": {
        "entry / rule": "the curated entry, and the rule id INSIDE it. A rule id is unique only "
                        "within its entry — `species_quotas.r1` is nine different rules — so key "
                        "on `entry::rule`.",
        "verbatim": "the sentence from the printed book. The one thing a reader can check.",
        "provenance": "who wrote it and what it binds to, spelled out: `who` ('Region 2'), "
                      "`binds_to` (region / area / water / inherited), `says` (the verbatim "
                      "again, for a caller that has only the rule), `rank` (below), and the "
                      "curated entry's own name and region.",
        "label": "a short human label written during curation — NOT from the book.",
    },
    "what it is about": {
        "species": "BASE species codes, expanded here. Look them up in `species`.",
        "species_written": "what the book/curation actually wrote — often a group code. Kept so "
                           "the grouping can be rebuilt; see `groups`.",
        "species_except": "species carved out (also expanded), with `species_except_written`.",
        "origin": "'wild' | 'hatchery' | absent. ABSENT MEANS BOTH, not a third kind.",
        "water": "'stream' | 'lake' | absent (absent = either)",
        "family / dimension / type": "how the rule was classified when curated",
    },
    "the number": {
        "take": "how many you may keep. 0 is a closure, OR a release, OR a size floor — which "
                "one depends on `may_target` and the size fields.",
        "may_target": "0 where you may not even fish for it (a closure, not a release)",
        "unlimited": "true where there is no number",
        "period": "'daily' | 'possession' | 'annual' — which clock the number is on",
        "_shared_or_each": "THERE IS NO FIELD: count `species`. More than one fish named means "
                           "the number is SHARED between them — 'Char daily quota = 1' is one "
                           "char between them, 'Bass: 20' is twenty bass and not twenty of each. "
                           "One fish has no pool to share and is neither. A `combined` field "
                           "used to say this; it marked 31 of the 1,188 rules carrying a number, "
                           "every one of which already named more than one fish, as did 1,157 it "
                           "never marked — and it was written `false` on the other 3,379, so its "
                           "absence read as a denial. It has been removed from the corpus. "
                           "Nothing in the book means 'one each': the four group quotas whose "
                           "sentence contains 'each' all say 'each day' or 'each year'. Count "
                           "`species` (expanded), not `species_written`: ['TROUT_CHAR'] is 13 "
                           "fish, not one.",
        "within": "the rule id this is a clause of — '1 over 50 cm' inside 'Trout/char: 4'. A "
                  "clause counts INSIDE its parent, never against it.",
        "per_daily": "a possession multiple: N times the daily number",
    },
    "size": {
        "lengths": "THE ONLY SIZE FIELD. An ORDERED list of length "
                   "ranges, each with the number you may keep in it; the FIRST range that "
                   "contains a fish's length wins. `min_cm`/`max_cm` are inclusive and null is "
                   "open at that end; a range with no `take` of its own uses the rule's `take`. "
                   "A length NO range covers is not spoken about by this rule — at the top "
                   "level nothing else grants it, and inside a `within` clause the parent quota "
                   "governs it. Present on all 270 rules that carry a size and on no others. "
                   "Every rule with a range closed at both ends is in this file under "
                   "`size_rule_examples`.",
        "_why_lengths_exists":
            "`over_cm` meant three different things depending on the fields around it: the "
            "ceiling on a granted fish ('quota 2, none over 50 cm'), the class a number COUNTS "
            "('only 1 over 40 cm', inside a clause), and the fish denied outright ('no trout "
            "over 50 cm'). Six branches told them apart and every consumer that re-derived them "
            "got it wrong differently — and `band`, the one flag that did carry meaning, was "
            "set backwards on four rules, permitting exactly the fish they protect. `lengths` "
            "writes the range and its number, so there is nothing left to infer.",
        "_worked": {
            "Trout daily quota = 2 (none over 50 cm)":
                "[{max_cm: 50}, {min_cm: 50, take: 0}] — the 2 applies up to 50, none above",
            "1 bull trout over 60 cm":
                "[{min_cm: 60}, {max_cm: 60, take: 0}] — the floor is absolute",
            "only 1 over 40 cm (a clause)":
                "[{min_cm: 40}] — smaller fish are the parent quota's business, not this rule's",
            "20-30 cm only, quota 2":
                "[{min_cm: 20, max_cm: 30}, {max_cm: 20, take: 0}, {min_cm: 30, take: 0}]",
            "none between 70 cm and 100 cm":
                "[{min_cm: 70, max_cm: 100, take: 0}] — the hole, and only the hole",
            "_endpoints": "A grant is written before the denial beneath it, so a fish of "
                          "exactly 60 cm is granted rather than denied. The book's 'over 60' "
                          "and '60 cm or more' differ by one fish and the corpus never stored "
                          "which was meant; that loss predates this field and is not invented.",
        },
        "_read": "take=2 and no lengths → keep 2, any size. A range with take 0 is fish that "
                 "go back; a floor is an allowance of zero on a size class, which is why "
                 "closures, releases and size limits are one kind of thing.",
    },
    "dates": {
        "when": "{dates, hours, weekdays, unparsed}. `dates` is a list of {from_month, "
                "from_day, to_month, to_day}, inclusive, wrapping the year end where to < from; "
                "EMPTY MEANS ALL YEAR. There is no 'except' flag: a rule printed as 'open "
                "except…' stores the days it DOES hold. `hours` is {start, end}, each "
                "{at: 'HH:MM'} or {solar: sunrise|sunset, offset_min}, wrapping midnight the "
                "same way. `unparsed` is a printed season nobody could read, kept verbatim — "
                "treat it as uncertain, never as absent.",
        "when_open": "true where it binds only while the water is open at all; carries no dates",
        "_read": "no window is not 'undated' — it is the standing answer, true on every day "
                 "nothing seasonal speaks.",
    },
    "where": {
        "scope": "'region' | 'area' | 'water' | 'inherited' — WHAT IT BINDS TO",
        "authority": "'superior' | 'province' | 'region' — WHO WROTE IT",
        "extents": "the places it reaches, as area ids / area kinds",
        "extent_text": "a prose extent nothing can draw on a map. Becomes a caveat, not a "
                       "counter — EXCEPT inside the area it names, where the place is drawn.\n"
                       "IT IS ALSO WHAT TELLS TWO RULES APART when nothing else does. Mahood "
                       "Lake has two closure areas, each with its own catch-and-release, bait "
                       "ban and barbless-hook rule; the bundle keeps no structured extent for "
                       "either ('lake has no bindable cut-point'), so the six rules flatten to "
                       "two identical triples and only this field says which is the western tip "
                       "and which the Mahood River outlet. Nine such groups exist. Show it.",
        "includes_tributaries / tributaries_only": "how far up it reaches",
        "via": "on a water's rule: 'reach' (written for this water) or 'trib' (it reached here "
               "from a water downstream, by the tributary walk)",
        "_rank": "DERIVED from (authority, scope), never stored: superior=-1, water=0, "
                 "inherited=1, area=2, region+region=3, region+province=4. SMALLER SPEAKS "
                 "FIRST, and scope beats authority — a province-authored rule for one lake "
                 "(rank 0) speaks there before the region's table (rank 3).",
    },
    "gear": {
        "gear": "an ORDERED list of clauses, each {slot, …}. Within one slot the FIRST "
                "clause whose `when` matches wins; different slots are independent. A set "
                "slot (bait, lure, method, barb) takes exactly one of `allow` (permits what it "
                "names), `only` (a whitelist that closes the slot) or `ban`; `of` narrows which "
                "members it speaks about and `except` carves members out of a ban. A counted or "
                "measured slot takes `max`/`min` in the unit its name carries "
                "(hook_gap_mm, weight_per_line_kg). A spec slot (light, downrigger, …) takes "
                "`must_be`. `unless` lists what lifts the clause. See "
                "pipeline/docs/07-gear-representation.md.",
        "while": "the methods during which the rule binds — 'dead fin fish may be used WHILE "
                 "set lining'",
        "conduct": "acts the angler must do or refrain from, as tokens named in the lawful "
                   "direction ('do_not_waste_catch')",
        "when_targeting": "species the angler is fishing FOR, not what they may catch — 'bait "
                          "ban when fishing for salmon'",
        "closed_to": "angler_closure only: WHO the water is closed to — {residency, age, "
                     "guidance, status}, each a list of the members included",
        "max_power_kw / max_kmh": "boat rules",
        "level / aspect / standing": "how the gear term was classified",
    },
    "licensing": {
        "_note": "NOT a rule. Licensing is its own list on each entry (designation, "
                 "not_classified, requirement, licence_terms, exemption, alternative); it never "
                 "votes on open/closed. `document_required` and `access_permission` are gone.",
        "record_retention": "on a retention rule: record the fish on your licence",
    },
    "_two_rules_that_look_identical": {
        "_note": "No rule in the corpus duplicates another. Every apparent repeat is told apart "
                 "by a field, and three different fields do it — so a comparison that checks "
                 "only the obvious ones will report duplicates that are not there.",
        "extent_text": "9 groups — two places, one sentence, no drawable cut-point",
        "extents": "5 groups — one sentence over several reaches, e.g. the Fraser's trout "
                   "closure downstream of Hell's Gate, between it and the Thompson, and above",
        "max_kmh / aspect": "5 groups — 'speed restriction on parts (8 and 60 km/h)' is TWO "
                            "rules, one per zone; 'speed restrictions or no vessels' is a speed "
                            "rule and a propulsion rule",
        "_and": "the `verbatim` is shared on purpose in all of these: both rules were read from "
                "the one sentence, and that sentence is the provenance of each.",
    },
    "other": {
        "exempts": "what this rule LIFTS — this is how a closure is reopened",
        "obligation": "duties, e.g. 'must be released immediately'",
        "uncertain": "the curator was not sure",
        "notice": "the DFO fishery notice a rule was published in ('FN0679'); provenance only",
        "suspended_while": "a rule id in the same entry: this rule is dormant while that one binds "
                           "('licence not required until reopened to steelhead fishing')",
    },
}


#: The groups that name an open set rather than a list. Empty in the catalogue on purpose.
OPEN_GROUPS = {
    "ALL_FIN_FISH": "every fish with fins — wider than the provincial game-fish list, and "
                    "including salmon, which are federal",
    "NON_GAME_FISH": "every fish that is not on the provincial game-fish list",
}


def _name(code: str) -> str:
    r = SPECIES.get(code)
    return _SPECIES_WORDS.get(code) or (r.common_name if r else code)


def species_object() -> dict:
    """ONE OBJECT PER FISH, with everything known about it in one place.

    `DEFINITIONAL_SIZE` (`pipeline/regs/parsing/catalogue.py`) used to sit in a table of its own,
    which is the wrong shape: it is a fact about a species, not a kind of rule. A steelhead IS a
    rainbow trout over 50 cm, so the 50 belongs on the steelhead the way its name does.
    """
    in_group = defaultdict(list)
    for g in SPECIES_GROUPS:
        for c in set(expand_species([g])) - set(OPEN_GROUPS):
            in_group[c].append(g)
    out = {}
    for code in sorted({c for g in SPECIES_GROUPS for c in set(expand_species([g])) - set(OPEN_GROUPS)}):
        rec = SPECIES.get(code)
        d = {"code": code, "name": _name(code),
             "scientific": rec.scientific if rec else None,
             "species_type": rec.species_type if rec else None,
             "groups": sorted(in_group[code])}
        if code in DEFINITIONAL_SIZE:
            ds = dict(DEFINITIONAL_SIZE[code])
            d["definitional_size"] = {
                "min_cm": ds.get("min_cm"),
                "says": ds.get("says"),
                "below_this_it_is": ds.get("below"),
                "applies_where": ds.get("applies_where"),
                "source": ds.get("source"),
                "_use": "A size the book puts in the DEFINITION, not in a quota. So a cap of "
                        "'1 over 50 cm' on this fish is simply 1, and a regional floor of 30 cm "
                        "can never bite on it. Apply it on a row already labelled with this "
                        "species; deciding whether the species BELOW it is really this one "
                        "needs a per-water fact the corpus does not carry.",
            }
        out[code] = d
    return out


#: WHAT KINDS OF RULE ARE IN HERE — all of them. A quota is the loudest but it is one family of
#: five, and the gear, licensing and conduct rules have to be designed for too.
RULE_TYPES = {
    "retention_limit": "how many you may keep, or that you may keep none — quotas, releases, "
                       "closures and size limits are all this one type",
    "stop_fishing_after_quota": "you must stop fishing for it once your quota is taken",
    "method_rule": "whether a method is allowed at all — angling, ice fishing, spear, nets, "
                   "snagging, traps",
    "tackle_restriction": "the rig — hooks, lines, flies, weights",
    "bait_restriction": "what may be on the hook, and bait bans",
    "angler_closure": "the water is closed to ONE KIND of angler (non-guided non-resident "
                      "aliens on weekends) — a closure, never a quota",
    "handling_rule": "what you must DO — release immediately, do not remove from the water",
    "vessel_rule": "boats — where they may go, under what power, whether at all (384 rules "
                   "corpus-wide, the third largest type and easy to miss)",
    "angling_from_vessel_prohibited": "you may be here, but not fish from a boat",
    "navigation_duty": "what you must do for other traffic",
    "hazard": "a warning about the place",
    "facility": "what is there — a boat launch, a wheelchair-accessible pier",
    "program_membership": "the water is in a named programme (Quality Waters, Family Fishing)",
    "advisory": "information the book prints that governs nothing — and MUST NOT be allowed to "
                "govern anything (Region 8's crayfish are currently governed by a turtle "
                "advisory; see 05-table-generation.md §6.4)",
    "_counts_corpus_wide": {
        "retention_limit": 1769, "bait_restriction": 397, "vessel_rule": 384,
        "tackle_restriction": 355, "method_rule": 136, "advisory": 127, "hazard": 25,
        "program_membership": 19, "angler_closure": 15, "angling_from_vessel_prohibited": 15,
        "facility": 13, "handling_rule": 11, "stop_fishing_after_quota": 2, "navigation_duty": 1,
    },
}

RULE_FAMILIES = {
    "retention": "what you may keep",
    "gear_and_method": "how you may fish",
    "access": "who may fish here at all — a closure to one kind of angler",
    "conduct": "what you must do",
    "vessel": "what your boat may do — 400 rules corpus-wide, a whole half of the book that no "
              "table currently draws",
    "information": "what the book tells you, governing nothing",
    "_counts_corpus_wide": {"retention": 1771, "gear_and_method": 888, "vessel": 400,
                            "information": 184, "access": 15, "conduct": 11},
}


#: HOW A ZERO READS. `take: 0` is three different regulations and the fields tell them apart.
#: Getting this wrong is the single most consequential misreading available: a release drawn as
#: a closure shuts a legal fishery, and a closure drawn as a release sends someone fishing for a
#: protected fish. (In the app's map code, reading `take === 0` alone called 1,674 of 1,693
#: rulesets closed.)
ZERO_READINGS = {
    "closed": "`take: 0` AND `may_target: 0` — you may not fish for it at all",
    "release": "`take: 0`, `may_target` not 0 — you may fish for it and must let every one go",
    "size gate": "a range in `lengths` with `take: 0` — a floor or a ceiling, not a closure. "
                 "'none under 30 cm' is an allowance of zero on a SIZE CLASS.",
    "_and": "a `take: 0` carrying a `method` is not a closure of the water either — it closes "
            "that method.",
}

EXEMPTIONS = {
    "_what": "The book writes closures and then writes exceptions to them. An exemption is a "
             "rule like any other; `exempts` says what it lifts.",
    "two forms": {
        "{default_id: 'spring_stream_closure'}": "names a STANDING DEFAULT by id — the regional "
                                                 "closure entry of that name. 63 rules exempt "
                                                 "the spring stream closure alone.",
        "{target: 'bait.r1'}": "names a specific rule, by its bare id inside the same entry",
    },
    "note": "free text the book gives, e.g. 'Mainstem open all year'. It is often the only thing "
            "that says WHERE the exemption reaches.",
    "_trap_1_place":
        "A LIFT WHOSE PLACE CANNOT BE DRAWN IS NOT APPLIED. Region 6's 'No fishing for steelhead "
        "in streams, May 15 – Jun 15' carries an exemption noting 'mainstem Skeena, Nass, Iskut, "
        "Stikine and Taku'. Applied everywhere, the closure vanished from the Babine — which that "
        "note does not exempt — and a reader was told a river is open in the middle of a "
        "steelhead closure. The closure stands and the exemption rides beside it in the book's "
        "own words. Ignoring an exception fails toward the stricter answer for a quota; for a "
        "CLOSURE it is the other way round.",
    "_trap_2_self":
        "A RULE CANNOT LIFT ITSELF. That same Region 6 rule names its OWN entry as the default it "
        "exempts, so it lifted itself on every water in the region.",
    "_trap_3_ids":
        "A bare rule id is not unique — `species_quotas.r1` is nine different rules — so an "
        "exemption target must be resolved within its own entry, or against the named default "
        "entry. `resolves_to` below is that resolution, done here; where it is empty the target "
        "could not be resolved and the lift must NOT be applied.",
}


def _mus_of(area_id: str) -> set:
    """The management units an `area:mu_group:…` id covers.

    "management_units_1_1_to_1_6" is a RANGE, and the only place the corpus writes which units
    an area rule reaches. Parsed rather than looked up because there is no table of it; a name
    this function cannot read yields nothing, and the caller falls back to saying so.
    """
    tail = area_id.split(":")[-1]
    got = tail.replace("management_units_", "").split("_to_")
    try:
        lo_r, lo_n = got[0].split("_")[0], int(got[0].split("_")[1])
        hi_r, hi_n = (got[1].split("_")[0], int(got[1].split("_")[1])) if len(got) > 1 \
            else (lo_r, lo_n)
    except (ValueError, IndexError):
        return set()
    if lo_r != hi_r:
        return set()
    return {f"{lo_r}-{n}" for n in range(lo_n, hi_n + 1)}




def _rule(x: dict) -> dict:
    """The record as the table layer gets it, with the species expanded and the provenance
    spelled out. Nothing is removed."""
    s = source_of(x)
    out = dict(x)
    written = list(x.get("species") or [])
    out["species_written"] = written
    # ONE EXPANDER, `catalogue.expand_species` — transitive, and it returns an OPEN group's own
    # code rather than nothing, so "everything with fins" survives as a claim instead of
    # vanishing into an empty list. There were two implementations of this and they differed on
    # exactly that.
    got = expand_species(written) if written else []
    out["species"] = sorted(c for c in got if c not in OPEN_GROUPS)
    # AN OPEN SET IS NOT AN EMPTY ONE. `ALL_FIN_FISH` and `NON_GAME_FISH` mean "everything that is
    # a fish", which is not a list the province publishes and would go stale the moment it was
    # written down — so they have no members, and `expand_species` returns the group's own code
    # rather than nothing. Three rules say it, "any fish willfully or accidentally snagged must
    # be released immediately" among them, and a consumer reading an empty list as "no species"
    # drops exactly the rules that reach widest. The claim travels in `species_open`.
    openset = [g for g in got if g in OPEN_GROUPS]
    if openset:
        out["species_open"] = openset
        out["species_note"] = ("This rule is about " + " and ".join(
            OPEN_GROUPS[g] for g in openset) + " — an open set the corpus deliberately does not "
            "list, so `species` is empty. Do not read that as 'no fish'.")
    ex = list(x.get("species_except") or [])
    if ex:
        out["species_except_written"] = ex
        out["species_except"] = sorted(c for c in expand_species(ex) if c not in OPEN_GROUPS)
    # NOTHING IS DERIVED ONTO THE RECORD BELOW THIS LINE. `reads_as`, `size_rule` and
    # `shared_number` were computed here and have been removed: this file is the corpus's data,
    # and a reading of it is not data. Every one of them is now written out as a condition in
    # `field_dictionary` — `closures_and_exemptions.how_a_zero_reads` for what a `take: 0` is,
    # `size._which_reading` for what a bound means, `the number._shared_or_each` for whether a
    # number is split. A consumer applies those; it does not get an answer it cannot see behind.
    if x.get("exempts"):
        out["exempts"] = [dict(e, resolves_to=_targets(x, e)) for e in x["exempts"]]
    e = entries().get(x.get("entry")) or {}
    out["provenance"] = {
        "says": (x.get("verbatim") or "").strip(),
        "who": s.words(),
        "authority": s.authority.value,
        "binds_to": s.scope.value,
        "rank": s.rank,
        "region": s.region or None,
        "place": s.place or None,
        "rule": f'{x.get("entry")}::{x.get("rule")}',
        # WHERE IN THE BOOK. The page is checkable by anyone holding the synopsis; the box is
        # the rule in context, which is how a clause is told from a peer — "1 over 50 cm" under
        # "Trout/char: 4" is a clause, and the same words on their own would not be.
        "entry": x.get("entry"),
        "entry_name": e.get("name") or x.get("entry_name"),
        "entry_display_name": e.get("display_name"),
        "synopsis_pages": e.get("synopsis_pages") or [],
        "printed_box": e.get("printed_box"),
        "scope_note": e.get("scope_note"),
        "symbols": e.get("symbols") or [],
    }
    return out


def _mus(rs) -> set:
    """The management units a water's own rules are filed under — the `@4-19+4-7` on an entry id
    is the book's own area vocabulary for that water."""
    return {p for x in rs
            for p in (x.get("entry", "").split("@")[-1].split("+")
                      if "@" in x.get("entry", "") else [])}


#: A RULE THAT BINDS ONLY INSIDE AN AREA IS HOW YOU KNOW YOU ARE IN ONE.
#:
#: "Inside a National Park or Ecological Reserve: no fishing" looks like a question about
#: geometry, and it is not one for a consumer of this file. The atlas has already asked it: the
#: closure's extent is `within area_kind national_parks`, so it binds to the 10,191 sections that
#: are in one and to no others. A section carrying the rule IS in a park.
AREA_MARKERS = {
    "zp:superior_closures::superior_closures.r1": "a National Park",
    "zp:superior_closures::superior_closures.r3": "an Ecological Reserve",
    "zp:national_park_reserves::national_park_reserves.r1":
        "Pacific Rim, Gwaii Haanas or Gulf Islands National Park Reserve",
    "zp:superior_closures::superior_closures.r2": "a National Park (a permit is required)",
    "zp:basic_licence::basic_licence.r2": "a National Park (a B.C. licence is not valid there)",
}


def _areas_for(regions, kind: str, mus: set, bound_ids: set) -> dict:
    """Which named areas this stretch is inside.

    TWO QUESTIONS, AND THE CORPUS ANSWERS BOTH — the first version of this answered neither,
    because it read the EXTENTS of the water's own rules, which say where that rule reaches and
    nothing about where the water is.

      · An area written as a management-unit range is matched against this water's own units.
      · An area written as an area KIND is answered by the rule itself being bound here. The
        atlas resolved that geometry when it cut the section.
    """
    inside = [{"area": AREA_MARKERS[r], "because": f"this stretch carries {r}"}
              for r in sorted(bound_ids & set(AREA_MARKERS))]
    for reg in regions:
        for a_ in ST.areas(reg, kind):
            ids = {e.get("area_id") for x in a_.rule_dicts
                   for e in (x.get("extents") or []) if e.get("area_id")}
            covered = set().union(*[_mus_of(i) for i in ids]) if ids else set()
            if covered & mus:
                inside.append({"area": a_.name, "region": reg,
                               "because": "its management units include "
                                          + ", ".join(sorted(covered & mus))})
    return {"inside": inside,
            "_note": "Empty means no area rule reaches this stretch — NOT that the question is "
                     "unanswered. An area closure binds to the sections inside it, so its "
                     "absence from this stretch's rules is itself the answer."}

_ALL = None


def _targets(x: dict, e: dict) -> list:
    """The rule ids an `exempts` entry actually points at.

    Resolved here so a consumer never has to match a bare id — which is not unique — against the
    corpus itself. An empty list means the target could not be resolved, and a lift that cannot
    be resolved must not be applied.
    """
    global _ALL
    if _ALL is None:
        _ALL = list(rules())
    out = []
    if e.get("target"):
        want = e["target"]
        out += [rid(y) for y in _ALL
                if y.get("entry") == x.get("entry") and y.get("rule") == want]
    if e.get("default_id"):
        # ...AND ONLY IN ITS OWN REGION. Every region writes its own spring closure, so a
        # `default_id` matched across the corpus resolved the Fraser's exemption to five of
        # them — four belonging to regions the Fraser is not in. A lift reaches the closure it
        # is filed under, not every closure that shares a name.
        did = e["default_id"]
        mine = source_of(x).region
        out += [rid(y) for y in _ALL
                if (y.get("entry") or "").split(":")[-1] == did and rid(y) != rid(x)
                and (not mine or source_of(y).region == mine)]
    return sorted(set(out))


_ENTRIES = None


def entries() -> dict:
    """entry_id -> the paperwork around a rule: which synopsis page it was read from, the whole
    printed box it came out of, the scope note, the symbols.

    ALL OF IT COMES FROM THE BUNDLE. This used to read the curated files at RUN time, which is a
    fallback — a reader answering with something the bundle never agreed to, and staleness that
    says nothing. `scope_note` was the last of the six that had no column; it has one now.

    Verified field by field against what the curated read returned: 1,480 of 1,480 identical on
    five, and on the sixth the bundle is BETTER — `name` is `display_name or name`, so the 32
    entries with no display_name get their name instead of a null.
    """
    global _ENTRIES
    if _ENTRIES is None:
        import sqlite3
        from pipeline.regs.table.corpus import BUNDLE
        db = sqlite3.connect(BUNDLE)
        cols = [r[1] for r in db.execute("PRAGMA table_info(entry)")]
        _ENTRIES = {}
        for row in db.execute("SELECT * FROM entry"):
            e = dict(zip(cols, row))
            _ENTRIES[e["entry_id"]] = {
                "name": e.get("full_name"),          # the book's own heading, shouted
                "display_name": e.get("name"),       # the readable form
                "scope_note": e.get("scope_note") or None,
                "synopsis_pages": json.loads(e.get("pages") or "[]"),
                "printed_box": e.get("verbatim") or None,
                "symbols": json.loads(e.get("symbols") or "[]"),
            }
        db.close()
    return _ENTRIES


#: WHAT WAS LAST MEASURED AGAINST THE PRINTED BOOK — recorded, not recomputed.
#:
#: `quota_print` read the printed box out of the synopsis PDF and matched every line of it to the
#: rule that accounts for it. It settled a ledger to do that, and the settling layer has been
#: removed from this repository until it is rebuilt, so the check cannot run and this export
#: cannot verify itself today.
#:
#: The figure is kept because it is true of these rules and it is the only evidence they are
#: right — but it is a SNAPSHOT of commit 4c1e74c9, not a live result, and it is labelled as one
#: in the output. Anything that would let a reader mistake it for a fresh check is worse than
#: leaving it out.
LAST_CHECKED = {
    "_status": "NOT RECOMPUTED — the settling layer this check needs is not in the repository. "
               "Treat as a historical claim about these rules, not as verification of this file.",
    "as_of_commit": "4c1e74c9",
    "method": "Every line of each region's printed quota box, read out of the synopsis PDF and "
              "matched to the rule that accounts for it (`quota_print.check_region`).",
    "source": "data/source/fishing_synopsis.pdf · 2025-2027",
    "result": "384 of 384 printed lines agreed, over 11 chapters × lake and stream.",
    "history": "382 of 386 before `z1:hg_quota.r3` was corrected from ['DV','BT'] to ['DV'] — "
               "the book prints '3 Dolly Varden' and names no bull trout. That fix needs a "
               "bundle rebuild to reach the rules in this file.",
}


def _size_examples(take) -> dict:
    """A RANGE CLOSED AT BOTH ENDS MEANS ONE OF TWO OPPOSITE THINGS, so every rule carrying one is
    in this file, interned into `rules` like any other, with its verbatim sentence.

    A HOLE is a closed range with `take: 0` — keep none between. A WINDOW is a closed range you may
    keep from, with the lengths either side of it denied. Under the old fields the two were the
    same pair of numbers and one flag, `band`, set backwards on four rules; `lengths` writes the
    `take` on the range, so which one a rule is can be read off it rather than trusted.
    """
    closed = lambda b: b.get("min_cm") is not None and b.get("max_cm") is not None
    by_kind = defaultdict(list)
    for x in rules():
        bands = [b for b in (x.get("lengths") or []) if closed(b)]
        if not bands:
            continue
        hole = any(b.get("take") == 0 for b in bands)
        by_kind["hole" if hole else "window"].append(x)
    return {
        "_note": "Every rule in the corpus whose `lengths` has a range closed at both ends. "
                 "Grouped by that range's own `take`: 0 is a hole, anything else a window.",
        "_readings": {k: SIZE_READINGS[k] for k in ("hole", "window")},
        "by_kind": {k: {"count": len(xs), "means": SIZE_READINGS[k],
                        "rule_ids": take(sorted(xs, key=rid))}
                    for k, xs in sorted(by_kind.items())},
        "read_one_of_each": {
            "hole": "r6:teslin_lake@6-25::teslin_lake.r3",
            "window": "r7:gwillim_lake@7-21::gwillim_lake.r1",
        },
    }


def main(out_path: str) -> int:
    all_rules = list(rules())
    by_chapter = defaultdict(list)
    for x in all_rules:
        by_chapter[(x.get("entry") or "").split(":")[0]].append(x)

    kept: dict = {}                       # entry::rule -> the exported record, interned

    def take(xs) -> list:
        ids = []
        for x in xs:
            k = rid(x)
            if k not in kept:
                kept[k] = _rule(x)
            ids.append(k)
        return sorted(set(ids))

    regions = []
    for reg in REGIONS:
        areas = []
        for kind in ("lake", "stream"):
            for a in ST.areas(reg, kind):
                got = next((e for e in areas if e["name"] == a.name), None)
                if got is None:
                    got = {"name": a.name, "water_kinds": [], "rule_ids": []}
                    areas.append(got)
                got["water_kinds"].append(kind)
                got["rule_ids"] = sorted(set(got["rule_ids"]) | set(take(a.rule_dicts)))
        regions.append({
            "region": reg,
            "name": "British Columbia (province-wide)" if reg == "province"
                    else f"Region {reg.upper()}",
            "chapter": CHAPTER[reg],
            # Region 1 and Haida Gwaii share the `z1` chapter and are different tables;
            # `state.region_rules` is the one place that knows how to split them.
            "rule_ids": take(sorted(ST.region_rules(reg), key=rid)),
            "areas": areas,
            "checked_against_the_book": LAST_CHECKED,
        })

    D = sections()
    waters = []
    for water, run in WATERS:
        rs, here, label = ST.rules_for(kind=ST.section_kind(water), water=water, run=run)
        mine = [x for x in rs if source_of(x).scope.value in ("water", "inherited")]
        w = D.get(water) or {}
        runs = w.get("runs") or []
        seg = runs[run] if run < len(runs) else {}
        waters.append({
            "water": water, "run": run, "label": label or None,
            "stretch_km": [seg.get("from"), seg.get("to")],
            "water_kind": ST.section_kind(water), "item_id": w.get("item"),
            "regions": sorted(here),
            "management_units": sorted(_mus(mine)),
            "areas": _areas_for(here, ST.section_kind(water), _mus(mine),
                                {rid(x) for x in rs}),
            "curated_entry": {k: v for k, v in (w.get("entry") or {}).items()
                              if k in ("name", "full", "verbatim")},
            "_rules_note": "This water's OWN rules only — `scope` water or inherited. The rules "
                           "of its region are under `regions`; a section really gets both, and "
                           "repeating them here would be the same sentence twice. `via: trib` "
                           "means the rule reached this water from another one downstream.",
            "rule_ids": take(mine),
        })

    # BEFORE the document, because it interns rules of its own and `_counts` below reads
    # `len(kept)`: built inside the literal it added 28 rules after the count was taken.
    sizes = _size_examples(take)

    doc = {
        "_what_this_is":
            "Stage ③ of pipeline/docs/05-table-generation.md — the rules as `corpus.rules()` "
            "hands them to the table layer. Interned by `entry::rule`; regions and waters index "
            "into them.",
        "_the_stages":
            "curated entry → bundle → RULE (this file) → allowance → ledger → row → table",
        "_not_included":
            "Nothing settled — no tables, no ledgers, no colours. The settling layer has been "
            "removed from the repository to be rebuilt, so this file is its input and there is "
            "nothing downstream of it today. See pipeline/docs/06-ui-data-contract.md for what "
            "the rebuilt layer owes (3.9 KB per rule set and stretch, ~29 MB for the province) "
            "and pipeline/docs/05-table-generation.md for how the removed one worked.",
        "_what_cannot_be_answered_from_this_file":
            "Which rule wins where two speak; what a number comes to once the rules that carve "
            "it are applied; whether a water is closed today; whether a rule binds at all. Those "
            "are settling, and nothing here does it. A consumer that adds them up itself is "
            "writing the layer that was removed — see the ladder in 05-table-generation.md "
            "Part 1 before assuming a rule means what it says on its own.",
        "_counts": {
            "rules in the whole corpus": len(all_rules),
            "rules in this file": len(kept),
            "regions": len(regions),
            "waters": len(waters),
        },
        "closures_and_exemptions": {
            "how_a_zero_reads": ZERO_READINGS,
            "exemptions": EXEMPTIONS,
            "_on_every_rule": "There is NO `reads_as` field. `how_a_zero_reads` above is the "
                              "whole test and a consumer applies it against `take`, "
                              "`may_target`, `method` and the size bounds — in that order, "
                              "because a zero can be three different things and reading "
                              "`take == 0` alone called 1,674 of 1,693 rule sets closed. "
                              "`exempts[].resolves_to` IS on the rule: it is the rule ids a "
                              "lift points at, which is lookup, not judgement.",
        },
        "rule_types": {
            "_note": "EVERY kind of rule is here, not just quotas — gear, licensing, conduct and "
                     "vessel rules are the same kind of object and the same ladder settles them.",
            "by_type": RULE_TYPES, "by_family": RULE_FAMILIES,
        },
        "size_rule_examples": sizes,
        "field_dictionary": FIELDS,
        "species": species_object(),
        "rules": kept,
        "regions": regions,
        "waters": waters,
        "groups": {
            "_note": "The book writes rules about groups, and a table draws a group as ONE line "
                     "where every member's answer agrees. Every rule above carries base codes, "
                     "so this is for rebuilding that grouping, never for expanding a rule.",
            **{g: {"name": _name(g), "members": sorted(set(expand_species([g])) - set(OPEN_GROUPS))}
               for g in sorted(SPECIES_GROUPS) if set(expand_species([g])) - set(OPEN_GROUPS)},
            **{g: {"name": _name(g), "members": [], "open_set": True, "means": why,
                   "_warning": "Empty ON PURPOSE. A rule about this group has an empty `species` "
                               "and carries `species_open` instead. Reading the empty list as "
                               "'no fish' silently narrows the widest rules in the book."}
               for g, why in OPEN_GROUPS.items()},
        },
    }
    with open(out_path, "w") as fh:
        json.dump(doc, fh, indent=1, ensure_ascii=False)
    import os
    print(f"wrote {out_path} ({os.path.getsize(out_path)/1e6:.2f} MB)")
    for k, v in doc["_counts"].items():
        print(f"  {k}: {v}")
    print("  checked against the printed book: NOT RECOMPUTED — the settling layer is not in "
          "the repository; the file carries the last measured result, labelled as a snapshot")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1] if len(sys.argv) > 1
                          else "data/generated/regs/ui-rules-export.json"))
