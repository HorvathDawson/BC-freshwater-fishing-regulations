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
  · `reads_as` gives the four-way answer a `take: 0` needs (closed / release / size gate /
    method closed), because every consumer that re-derived it got it wrong differently.

Change `REGIONS` / `WATERS` below to widen it. Nothing here is cached or committed.
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict

from pipeline.regs.parsing.catalogue import DEFINITIONAL_SIZE, SPECIES_GROUPS, _SPECIES_WORDS
from pipeline.regs.parsing.species import SPECIES
from pipeline.regs.table import state as ST
from pipeline.regs.table.authority import source_of
from pipeline.regs.table.corpus import rid, rules, sections
from pipeline.regs.table.subject import expand

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
          ("Fraser River", 17), ("Kootenay River", 6), ("Okanagan River", 0)]
CHAPTER = {"province": "zp", "1": "z1", "1hg": "z1", "2": "z2", "3": "z3", "4": "z4",
           "5": "z5", "6": "z6", "7a": "z7a", "7b": "z7b", "8": "z8"}

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
        "method": "the fishing method a gear rule is about",
        "family / dimension / type": "how the rule was classified when curated",
    },
    "the number": {
        "take": "how many you may keep. 0 is a closure, OR a release, OR a size floor — which "
                "one depends on `may_target` and the size fields.",
        "may_target": "0 where you may not even fish for it (a closure, not a release)",
        "unlimited": "true where there is no number",
        "period": "'daily' | 'possession' | 'annual' — which clock the number is on",
        "combined": "true where the number is SHARED across the species, not one each",
        "within": "the rule id this is a clause of — '1 over 50 cm' inside 'Trout/char: 4'. A "
                  "clause counts INSIDE its parent, never against it.",
        "per_daily": "a possession multiple: N times the daily number",
    },
    "size": {
        "over_cm": "the number counts (or the gate forbids) fish OVER this length",
        "under_cm": "…UNDER this length",
        "band": "true where the pair is a forbidden band rather than a slot",
        "_read": "take=2 and no size → keep 2, any size. take=1 over_cm=50 → only 1 may be over "
                 "50 cm, a CAP on a size class. take=0 under_cm=30 → none under 30 cm, a FLOOR. "
                 "take=0 over_cm=50 → a ceiling. A floor is an allowance of zero on a size "
                 "class, which is why closures, releases and size limits are one kind of thing.",
    },
    "dates": {
        "windows": "list of {from:{month,day}, to:{month,day}} — the days it speaks about",
        "windows_are": "'applies' | 'excepts' — whether those are the days it holds, or the days "
                       "it does NOT ('Kokanee catch and release, EXCEPT Apr 1-3…')",
        "when_open": "true where it binds only while the water is open at all; carries no dates",
        "weekdays / from_time / to_time": "rules that never resolve to a date — the client "
                                          "applies these itself",
        "_read": "no window is not 'undated' — it is the standing answer, true on every day "
                 "nothing seasonal speaks.",
    },
    "where": {
        "scope": "'region' | 'area' | 'water' | 'inherited' — WHAT IT BINDS TO",
        "authority": "'superior' | 'province' | 'region' — WHO WROTE IT",
        "extents": "the places it reaches, as area ids / area kinds",
        "extent_text": "a prose extent nothing can draw on a map. Becomes a caveat, not a "
                       "counter — EXCEPT inside the area it names, where the place is drawn.",
        "includes_tributaries / tributaries_only": "how far up it reaches",
        "via": "on a water's rule: 'reach' (written for this water) or 'trib' (it reached here "
               "from a water downstream, by the tributary walk)",
        "_rank": "DERIVED from (authority, scope), never stored: superior=-1, water=0, "
                 "inherited=1, area=2, region+region=3, region+province=4. SMALLER SPEAKS "
                 "FIRST, and scope beats authority — a province-authored rule for one lake "
                 "(rank 0) speaks there before the region's table (rank 3).",
    },
    "gear": {
        "hook_count / barbless / max_lines / max_flies / max_weight_kg": "the rig",
        "bait / lure": "what may be on the hook",
        "permitted / allowed": "whether the method is allowed at all",
        "max_power_kw / max_kmh": "boat rules",
        "level / aspect / standing": "how the gear term was classified",
    },
    "licensing": {
        "document / licence_name": "the licence or stamp required",
        "water_class": "'I' | 'II' for classified waters",
        "angler_class": "who it is about — set on only 38 of 3,422 rules",
        "grantor / allocation / record_retention": "permits and day allocations",
    },
    "other": {
        "exempts": "what this rule LIFTS — this is how a closure is reopened",
        "obligation / on_retention / required": "duties, e.g. 'must be released immediately'",
        "uncertain": "the curator was not sure",
        "reason": "why, where the book gives one",
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
        for c in expand(frozenset({g})):
            in_group[c].append(g)
    out = {}
    for code in sorted({c for g in SPECIES_GROUPS for c in expand(frozenset({g}))}):
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
    "document_required": "a licence, stamp or permit you must hold",
    "access_permission": "whose permission you need to be there",
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
        "tackle_restriction": 356, "document_required": 148, "method_rule": 135,
        "advisory": 127, "hazard": 25, "access_permission": 21, "program_membership": 19,
        "angling_from_vessel_prohibited": 15, "facility": 13, "handling_rule": 10,
        "stop_fishing_after_quota": 2, "navigation_duty": 1,
    },
}

RULE_FAMILIES = {
    "retention": "what you may keep",
    "gear_and_method": "how you may fish",
    "licensing": "what you must hold",
    "conduct": "what you must do",
    "vessel": "what your boat may do — 400 rules corpus-wide, a whole half of the book that no "
              "table currently draws",
    "information": "what the book tells you, governing nothing",
    "_counts_corpus_wide": {"retention": 1771, "gear_and_method": 888, "vessel": 400,
                            "information": 184, "licensing": 169, "conduct": 10},
}


#: HOW A ZERO READS. `take: 0` is three different regulations and the fields tell them apart.
#: Getting this wrong is the single most consequential misreading available: a release drawn as
#: a closure shuts a legal fishery, and a closure drawn as a release sends someone fishing for a
#: protected fish. (In the app's map code, reading `take === 0` alone called 1,674 of 1,693
#: rulesets closed.)
ZERO_READINGS = {
    "closed": "`take: 0` AND `may_target: 0` — you may not fish for it at all",
    "release": "`take: 0`, `may_target` not 0 — you may fish for it and must let every one go",
    "size gate": "`take: 0` with `over_cm` or `under_cm` — a floor or a ceiling, not a closure. "
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
    out["species"] = sorted(expand(frozenset(written))) if written else []
    # AN OPEN SET IS NOT AN EMPTY ONE. `ALL_FIN_FISH` and `NON_GAME_FISH` expand to nothing ON
    # PURPOSE: they mean "everything that is a fish", which is not a list the province publishes
    # and would go stale the moment it was written down. Three rules say it — "any fish willfully
    # or accidentally snagged must be released immediately" is one — and a consumer that reads
    # the empty list as "no species" drops exactly the rules that reach the widest. So the claim
    # travels separately, and `species` is never the whole story for these.
    openset = [g for g in written if g in OPEN_GROUPS]
    if openset:
        out["species_open"] = openset
        out["species_note"] = ("This rule is about " + " and ".join(
            OPEN_GROUPS[g] for g in openset) + " — an open set the corpus deliberately does not "
            "list, so `species` is empty. Do not read that as 'no fish'.")
    ex = list(x.get("species_except") or [])
    if ex:
        out["species_except_written"] = ex
        out["species_except"] = sorted(expand(frozenset(ex)))
    # WHAT THIS RULE READS AS — derived from the fields above, in one place, because every
    # consumer that re-derived it got it wrong in a different way.
    take, may = x.get("take"), x.get("may_target")
    sized = x.get("over_cm") is not None or x.get("under_cm") is not None
    if x.get("unlimited"):
        out["reads_as"] = "no limit"
    elif take == 0 and x.get("method"):
        out["reads_as"] = "method closed"
    elif take == 0 and sized:
        out["reads_as"] = "size gate"
    elif take == 0 and may == 0:
        out["reads_as"] = "closed"
    elif take == 0:
        out["reads_as"] = "release"
    elif take is not None:
        out["reads_as"] = "quota"
    else:
        out["reads_as"] = None
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


def _areas_for(regions, kind: str, mus: set) -> dict:
    """Which of the region's named areas this water is inside.

    ONLY WHERE IT IS KNOWABLE FROM THE RULES. An area written as a management-unit range can be
    matched against the water's own units. An area written as an area KIND — National Parks,
    Ecological Reserves — is a question about geometry, and the section data does not carry
    whether this water is inside one. Saying so is the honest answer; guessing "no" would draw a
    table that omits a closure.
    """
    inside, undecidable = [], []
    for reg in regions:
        for a in ST.areas(reg, kind):
            ids = {e.get("area_id") for x in a.rule_dicts
                   for e in (x.get("extents") or []) if e.get("area_id")}
            kinds = {e.get("area_kind") for x in a.rule_dicts
                     for e in (x.get("extents") or []) if e.get("area_kind")}
            covered = set().union(*[_mus_of(i) for i in ids]) if ids else set()
            if covered:
                (inside if covered & mus else []).append(
                    {"region": reg, "name": a.name, "matched_on": sorted(covered & mus)})
            elif kinds:
                undecidable.append({"region": reg, "name": a.name,
                                    "area_kind": sorted(k for k in kinds if k)})
    return {
        "inside": inside,
        "cannot_be_decided_from_these_rules": undecidable,
        "_note": "An area written as a management-unit range is matched against this water's own "
                 "units. One written as an area KIND (national_parks, ecological_reserves) is a "
                 "geometry question the section data cannot answer — treat it as unknown, never "
                 "as no, because those areas close the water outright.",
    }


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
    """entry_id -> what the CURATED entry knows that the bundle drops.

    The bundle keeps rules, not the paperwork around them: which synopsis page the entry was
    read from, and the whole printed box it was read out of. Both are provenance a reader can
    act on — "page 22" is checkable, and the box is the rule IN CONTEXT, which is how you tell
    a clause from a peer. The curated files are the owner of it.
    """
    global _ENTRIES
    if _ENTRIES is None:
        from pathlib import Path
        from pipeline.common.curated import CURATED
        _ENTRIES = {}
        dirs = (CURATED.regulations.entries.catalogue, CURATED.regulations.entries.dfo_salmon)
        for path in sorted(q for d in dirs for q in Path(d).glob("region-*.json")):
            for e in (json.loads(Path(path).read_text()).get("entries") or []):
                _ENTRIES[e.get("entry_id")] = {
                    "name": e.get("name"), "display_name": e.get("display_name"),
                    "scope_note": e.get("scope_note") or None,
                    "synopsis_pages": e.get("source_pages") or [],
                    "printed_box": e.get("regs_verbatim") or None,
                    "symbols": e.get("symbols") or [],
                }
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
            "areas": _areas_for(here, ST.section_kind(water), _mus(mine)),
            "curated_entry": {k: v for k, v in (w.get("entry") or {}).items()
                              if k in ("name", "full", "verbatim")},
            "_rules_note": "This water's OWN rules only — `scope` water or inherited. The rules "
                           "of its region are under `regions`; a section really gets both, and "
                           "repeating them here would be the same sentence twice. `via: trib` "
                           "means the rule reached this water from another one downstream.",
            "rule_ids": take(mine),
        })

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
            "_on_every_rule": "`reads_as` is the four-way answer derived from take / may_target "
                              "/ size / method. `exempts[].resolves_to` is the rule ids a lift "
                              "points at.",
        },
        "rule_types": {
            "_note": "EVERY kind of rule is here, not just quotas — gear, licensing, conduct and "
                     "vessel rules are the same kind of object and the same ladder settles them.",
            "by_type": RULE_TYPES, "by_family": RULE_FAMILIES,
        },
        "field_dictionary": FIELDS,
        "species": species_object(),
        "rules": kept,
        "regions": regions,
        "waters": waters,
        "groups": {
            "_note": "The book writes rules about groups, and a table draws a group as ONE line "
                     "where every member's answer agrees. Every rule above carries base codes, "
                     "so this is for rebuilding that grouping, never for expanding a rule.",
            **{g: {"name": _name(g), "members": sorted(expand(frozenset({g})))}
               for g in sorted(SPECIES_GROUPS) if expand(frozenset({g}))},
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
