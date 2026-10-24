"""`guide`: how to read the export, generated from the model's registries and the data.

Every example is FOUND in the bundle (`_Pick`), never a remembered id. The ladder's prose and rulings are
`ladder`; the prose examples the reader checks are `examples`; the test waters are `cases`.
"""
from __future__ import annotations

import typing
from collections import Counter, defaultdict
from pipeline.regs.parsing import catalogue as C
from pipeline.deliver.export import ladder as GL
from pipeline.common import entry_id as EID
from pipeline.deliver.export.read import _LIC_TABLES, licence_classes
from pipeline.deliver.export.guide_words import (
    BAIT_GUIDE, BINDS_TEXT, CLAUSE_TEXT, DOING_TEXT, END_TEXT, ENTRY_TEXT, FAMILY_TEXT, FLY_DEFINITIONS,
    GEAR_SPEC_TEXT, GEAR_WHEN_TEXT, LENGTH_TEXT, LICENSING_FIELD_TEXT, LICENSING_KIND_TEXT,
    LICENSING_PART_TEXT, LIFT_TEXT, PART_TEXT, PATH_TEXT, PDF_PAGES_TEXT, PERIOD_TEXT, PLACEMENT_TEXT,
    RULE_FIELD_TEXT, RUN_TEXT, SLOT_TEXT, STAMP_WAIVER_TEXT, STEELHEAD_TEXT, TOUCHES_TEXT, TYPE_CAN_CLOSE,
    TYPE_TEXT, VIA_TEXT, WHEN_TEXT, WHO_TEXT
)
from pipeline.deliver.export.cases import _closure


# --------------------------------------------------------------------------------------------
# The guide, built from the model's registries and the data
# --------------------------------------------------------------------------------------------

def _enum(e) -> list[str]:
    return [m.value for m in e]


def _fields(model) -> list[str]:
    return [f.alias or n for n, f in model.model_fields.items()]


def _licensing_models() -> dict:
    return {typing.get_args(m.model_fields["kind"].annotation)[0]: m
            for m in typing.get_args(typing.get_args(C.LicensingRecord)[0])}


def _example(x: dict, *keys: str) -> dict:
    """A record, cut to what the concept is about. Always a real record, found by id."""
    got = {"id": x["id"], "label": x["label"], "parts": x["parts"], "verbatim": x["verbatim"]}
    if keys:
        got["fields"] = {k: x["fields"][k] for k in keys if k in x["fields"]}
    return got


class _Pick:
    """Examples chosen from the data by a test, never by a remembered id — so every example is
    current, and one that stops existing is simply replaced by the next match."""

    def __init__(self, recs: dict):
        self.recs = recs

    def __call__(self, test, *keys, n: int = 2) -> list[dict]:
        return [_example(x, *keys) for x in self.recs.values() if test(x)][:n]


def _slot_kind(s) -> str:
    if s in C._SET_SLOTS:
        return "set"
    if s in C._SPEC_SLOTS:
        return "spec"
    if s in C._MEASURED:
        return "measured"
    return "counted"


def _f(x):
    return x["fields"]


def _gear(x):
    return _f(x).get("gear") or []


def _closed_range(b):
    return b.get("min_cm") is not None and b.get("max_cm") is not None


def guide(d: dict) -> dict:
    """The guide (`ui-rules-guide.json`), one builder per top-level section, assembled in the
    order it ships (the file is compared byte for byte, so the order is part of the output)."""
    return {
        "contents": guide_contents(),
        "how_to_read": guide_how_to_read(),
        "entries": guide_entries(d),
        "labels": guide_labels(d),
        "rule_types": guide_rule_types(d),
        "families": guide_families(d),
        "ladder": guide_ladder(d),
        "gear": guide_gear(d),
        "sizes": guide_sizes(d),
        "time": guide_time(d),
        "species": guide_species(d),
        "retention": guide_retention(d),
        "vessel": guide_vessel(d),
        "exempts": guide_exempts(d),
        "standing": guide_standing(d),
        "angler_closure": guide_angler_closure(d),
        "licensing": guide_licensing(d),
        "placement": guide_placement(d),
        "status": guide_status(),
        "gotchas": guide_gotchas(d),
    }


def guide_contents() -> dict:
    """`contents`: one line per section of the guide."""
    contents = {
        "how_to_read": "the file's shape, ids and what is not in it",
        "entries": "what an entry is, and its four kinds",
        "labels": "a record's line as parts; how to compose them; verbatim underneath",
        "rule_types": "every rule type: what it means, fields it uses, competition, closing",
        "families": "the six families the types group into",
        "ladder": "which rules compete and who speaks",
        "gear": "slots, bounds, conditions, `while`, `conduct`, first match per slot",
        "sizes": "`lengths`: ordered ranges, first match wins",
        "time": "`when`: dates, hours, weekdays, unparsed, suspended_while",
        "species": "codes, groups, expansion, species_except, open groups",
        "retention": "take, may_target, period, per_daily, within, and the readings of take 0",
        "vessel": "aspect, level, power, speed",
        "exempts": "how a rule lifts another",
        "standing": "rules everywhere at places nobody can draw",
        "angler_closure": "closures to one kind of angler",
        "licensing": "kinds, who, doing, paths, designations and their classified `period`, and "
                     "the rules of reading",
        "placement": "sets, waters and their parts (where each runs: `runs`; the cuts: "
                     "`splits`), outside B.C., tidal water, steelhead known | possible, lake "
                     "parts, via, placement, binds (and undrawn parts: not_yet_mapped), "
                     "uncertain",
        "status": "the map and search colour (closed / own / base) — a separate file, "
                  "`status_index.bin`, computed from the same reference reader",
        "gotchas": "where a page is easy to get wrong: size-clause overrides, places not yet "
                   "mapped, trout includes char, dated zone releases, one side of the channel, "
                   "bull trout is Dolly Varden, open subjects, source artefacts, closures that "
                   "combine (a row's and its zone's: both hold), dated bait bans that replace "
                   "the zone's",
        "examples": "EVERY water, date and fish the prose above uses as an example, as data "
                    "(`pipeline/tools/guide_examples.py`): the rules it says speak and the ones "
                    "it says are silent, and the reference reader's answer (`expect`). The "
                    "export refuses to write when the two disagree, and refuses prose naming a "
                    "water no example checks",
        "cases": "SAMPLE WATERS to build the page against while it is built out — one or more "
                 "per mechanism, each with what to show and the reference answer",
        "pdf_pages": PDF_PAGES_TEXT,
    }
    return contents


def guide_how_to_read() -> dict:
    """`how_to_read`: the file's shape, ids and what is not in it."""
    return {
        "shape": {
            "rules": "every rule, keyed `entry_id::rule_id`",
            "licensing": "every licensing record, keyed `entry_id#record_id`",
            "entries": "every synopsis row; lists its rule and licensing ids",
            "licences": "the document register",
            "rulesets / licensing_sets": "the interned sets of records that sections carry; "
                                         "a set id is valid only within one bundle digest",
            "set_keys": "each set's content key, stable across exports (`set_keys`)",
            "waters": "every named water (by durable item_id): its name, kind, section "
                      "count and matching rows (`entries`); its `parts` (the (ruleset, "
                      "licensing_set) pairs its sections carry together, with "
                      "`province_except`, `anadromous_rainbow` and `steelhead` (known | "
                      "possible) where they hold, `touches`: the other parts each borders, "
                      "and `runs`: where each runs, between which cuts); its "
                      "`outside_bc` count; `part_of` for a lake part; `steelhead` (the "
                      "parts' roll-up), `steelhead_source` (why it is known: "
                      "regulations and/or the curated list) and `steelhead_rows`; and `tidal` on the one tidal water — see "
                      "`field_dictionary.water`",
            "splits": "every cut a run can end at, by id: its name, and where it stands "
                      "(water and km from the mouth) — only the lake edges something names "
                      "(`field_dictionary.splits`)",
            "species": "the book's species list (p.80) under its headings, and the groups "
                       "and open subjects a rule may name",
            "field_dictionary": "every field in the file, and what it means",
            "index": "ids grouped by type and kind",
        },
        "every_record": "Each rule and licensing record reads itself: `label` (generated "
                        "from its fields), `verbatim` (the printed sentence), `fields` "
                        "(exactly as the bundle ships them), and `provenance`. A rule also "
                        "says where it holds (`binds`): on its sections, only in an undrawn "
                        "part of them (a note — never colour a water by it), or nowhere.",
        "ids": "A rule id or record id is unique only within its entry; always use the "
               "full key. item_id is the durable id of a water. A SET ID (a part's "
               "`ruleset` / `licensing_set`) is valid only within one bundle digest "
               "(`about.bundle`) — the next export renumbers the sets; match sets across "
               "exports by their content key (`set_keys`), never by id.",
        "not_included": "Nothing is settled: no quota tables, no open/closed verdicts, no "
                        "colours. This guide says how the fields are read; applying it is "
                        "the reader's job, and the ladder below is the rule for it. The "
                        "map's colour per section and water, per day, is a SEPARATE file "
                        "(`status_index.bin`, see `status`), not this one.",
    }


def guide_entries(d: dict) -> dict:
    """`entries`: what an entry is, its four kinds, and pointer rows."""
    return {
        "kinds": {
            "province": "`zp:` — the provincial regulations; bind everywhere their extents "
                        "reach",
            "zone": "`z<region>:` — a region's chapter: its standing tables and notices",
            "area": "`z<region>:` whose own extents name a place smaller than a region "
                    "(a management-unit group, a wildlife management area, a park)",
            "water": "`r<region>:` — one row of a region's water table",
        },
        "counts": dict(sorted(Counter(e["kind"] for e in d["entries"].values()).items())),
        "fields": ENTRY_TEXT,
        "pointers": {
            "reading": "`see` is a POINTER the row prints — 'See Lonzo Creek', 'A tributary "
                       "of Slocan River. See Slocan River' — and never a rule: it binds "
                       "nothing. Show it as a link to each of `entry_ids` ('see Lonzo "
                       "Creek'). A row whose only content is a pointer has no rules and no "
                       "licensing: it is SKIPPED as a regulation and read as the link. A "
                       "pointer row BINDS NOTHING and moves nothing: the TARGET row carries "
                       "the regulations, on the waters it matches, in the region those "
                       "waters lie in ('MARA LAKE — See Shuswap Lake in Region 3': Shuswap "
                       "Lake's row covers Mara Lake). Where the target does not cover the "
                       "pointer's water (Panther Lake, Bighorn Creek, Nation River, the Arrow "
                       "Lakes rows) that water carries only what else binds it — a gap in "
                       "the target's reach, never something the pointer supplies. "
                       "`unresolved` (no `entry_ids`) is a pointer that names no row — show "
                       "its words, it cannot be followed.",
            "relation": {
                "see": "a different water, governed by the named rows' regulations",
                "alias": "this row's water IS the named row's (one water printed under two "
                         "names — 'JONES LAKE: See Wahleach Lake'); the named row's rules "
                         "already cover it",
                "twin": "the same row printed under two region tables (a MU 6-1 lake in "
                        "both Region 5 and Region 6); the named row is the one that binds",
            },
            "counts": dict(sorted(Counter(
                s.get("relation", "unresolved") for e in d["entries"].values()
                for s in e.get("see") or []).items())),
            "pointer_only": sorted(e for e, v in d["entries"].items() if v.get("see")
                                   and not v["rules"] and not v["licensing"]),
        },
    }


def guide_labels(d: dict) -> dict:
    """`labels`: a record's line as parts; how to compose them."""
    rules = d["rules"]
    pick = _Pick(rules)
    labels = {
        "reading": "A record's line is shipped as PARTS, each generated from its fields and "
                   "never authored. Compose them in the UI; `verbatim` — the book's sentence, "
                   "exact — is the provenance to show underneath, always. A part with nothing to "
                   "say is ABSENT, never a placeholder, and no part is ever the verbatim. With no "
                   "`what`, the rule's sentence IS the rule: show the verbatim as the book's text "
                   "and the other parts as context. `label` is one preview composed the "
                   "suggested way (catalogue.compose); a UI may compose differently from `parts`.",
        "book_reasons": "A reason the book gives ('located in an Ecological Reserve', 'for the "
                        "conservation of chinook') is NOT a part: the model has no field for "
                        "a reason, and a part is generated from fields only. It reaches a reader "
                        "through the verbatim shown underneath, never paraphrased.",
        "rule_parts": {k: PART_TEXT[k] for k in C.LABEL_PARTS},
        "rule_order": {
            "order": list(C.LABEL_PARTS),
            "suggested": "what (size), conditions, when — where — side — in part: in_part — "
                         "lifts — "
                         "duty — suspended (notice)",
        },
        "licensing_parts": {k: LICENSING_PART_TEXT[k] for k in C.LICENSING_PARTS},
        "licensing_order": {
            "need": "{who or 'You'} need {need} {doing} {where}, {when} ({except}); {note}.",
            "must": "{who}: {must} {doing} {where}, {when}",
            "way": "{who}: {doing} {where}, {when}, {way}",
            "otherwise": "{what}, {when} (licence unit: {unit}) | ({unit}): {terms}, {instead}. "
                         "{stamp}. {suspended}.",
        },
        "counts": {
            "rules_by_part": dict(sorted(Counter(k for x in rules.values()
                                                 for k in x["parts"]).items())),
            "rules_without_what": sum("what" not in x["parts"] for x in rules.values()),
        },
        "examples": {
            "with a place": pick(lambda x: "where" in x["parts"] and "when" in x["parts"], n=1),
            "in part": pick(lambda x: "in_part" in x["parts"], n=1),
            "no what": pick(lambda x: "what" not in x["parts"], n=1),
        },
    }
    return labels


def guide_rule_types(d: dict) -> dict:
    """`rule_types`: every rule type — what it means, the fields it uses, whether it can close a water."""
    rules = d["rules"]
    pick = _Pick(rules)
    types = Counter(x["type"] for x in rules.values())
    dims = defaultdict(Counter)
    used = defaultdict(Counter)
    for x in rules.values():
        dims[x["type"]][x["dimension"]] += 1
        for k in _f(x):
            used[x["type"]][k] += 1
    rule_types = {}
    for t in _enum(C.RuleType):
        rule_types[t] = {
            "family": C._FAMILY[C.RuleType(t)],
            "means": TYPE_TEXT.get(t),
            "can_close_a_water": TYPE_CAN_CLOSE.get(t, "No."),
            "rules": types.get(t, 0),
            "fields_used": dict(used[t].most_common()),
            "dimensions": dict(dims[t].most_common(8)),
            "examples": pick(lambda x, t=t: x["type"] == t,
                             *[k for k, _ in used[t].most_common()
                               if k not in ("obligation", "period", "extents")][:5]),
        }
    return rule_types


def guide_families(d: dict) -> dict:
    """`families`: the families the rule types group into."""
    rules = d["rules"]
    types = Counter(x["type"] for x in rules.values())
    families = {f: {"means": FAMILY_TEXT.get(f),
                    "types": [t for t in _enum(C.RuleType) if C._FAMILY[C.RuleType(t)] == f],
                    "rules": sum(types.get(t, 0) for t in _enum(C.RuleType)
                                 if C._FAMILY[C.RuleType(t)] == f)}
                for f in sorted(set(C._FAMILY.values()))}
    return families


def guide_ladder(d: dict) -> dict:
    """`ladder`: which rules compete and who speaks (the prose and its rulings: `ladder`)."""
    return GL.ladder(_Pick(d["rules"]))


def guide_gear(d: dict) -> dict:
    """`gear`: slots, bounds, conditions, `while`, `conduct`, first match per slot."""
    rules = d["rules"]
    lic = d["licensing"]
    pick = _Pick(rules)
    lpick = _Pick(lic)
    slot_use = defaultdict(list)
    members = defaultdict(set)
    for x in rules.values():
        for c in _gear(x):
            slot_use[c["slot"]].append(x["id"])
            for k in ("allow", "only", "ban", "except", "of", "members", "must_be"):
                members[c["slot"]].update(c.get(k) or [])
    slots = {}
    for s in C.Slot:
        kind = _slot_kind(s)
        slots[s.value] = {
            "kind": kind,
            "bound": {"set": "exactly one of allow / only / ban (never empty); of, except",
                      "counted": "max / min (whole numbers) or unlimited; members",
                      "measured": "max / min in the unit in the slot's name",
                      "spec": "must_be and/or requires"}[kind],
            "means": SLOT_TEXT.get(s.value),
            **({"definitions": FLY_DEFINITIONS[s.value]} if s.value in FLY_DEFINITIONS else {}),
            "rules": len(set(slot_use[s.value])),
            "members_seen": sorted(members[s.value]),
            "examples": pick(lambda x, s=s.value: any(c["slot"] == s for c in _gear(x)),
                             "gear", "while", "when_targeting"),
        }
    acts = {}
    for a, words in C.CONDUCT_ACTS.items():
        acts[a] = {"means": words,
                   "rules": sum(a in (_f(x).get("conduct") or []) for x in rules.values()),
                   "licensing": sum(a in (x["fields"].get("conduct") or [])
                                    for x in lic.values()),
                   "examples": (pick(lambda x, a=a: a in (_f(x).get("conduct") or []),
                                     "conduct", n=1)
                                + lpick(lambda x, a=a: a in (x["fields"].get("conduct") or []),
                                        "conduct", n=1))}
    methods_page = sorted((d["entries"].get("zp:allowable_methods") or {}).get("pages") or [])
    allowed = sorted({m for x in rules.values() if EID.is_province(x["entry_id"])
                      for c in _gear(x) if c.get("slot") == "method"
                      for m in (c.get("allow") or []) if not _f(x).get("while")})
    same_slot = lambda x: len([c["slot"] for c in _gear(x)]) > len({c["slot"] for c in _gear(x)})
    gear = {
        "reading": "`gear` is an ORDERED list of clauses. Clauses on DIFFERENT slots are "
                   "independent and all apply. Clauses on the SAME slot are ordered and the "
                   "FIRST whose `when` matches wins; a clause with no `when` is the last word "
                   "on its slot. Nothing in a clause is a polarity flag: the direction is in "
                   "the key (allow / only / ban).",
        "slot_kinds": {
            "set": "chosen from a set of members",
            "counted": "a whole number of things",
            "measured": "a quantity in the unit its name carries",
            "spec": "how the thing must be built or carried; presence asserts, there is no "
                    "negation",
        },
        "slots": slots,
        "clause_fields": {k: CLAUSE_TEXT.get(k) for k in _fields(C.GearClause)},
        "when_fields": {k: GEAR_WHEN_TEXT.get(k) for k in _fields(C.GearWhen)},
        "requires_fields": {k: GEAR_SPEC_TEXT.get(k) for k in _fields(C.GearSpec)},
        "while": {
            "means": "The rule binds only WHILE the angler is doing one of these. A `take: 0` "
                     "on every game fish `while: [spear_fishing]` says which fish you may "
                     "spear; it does not close the water. Absent = whatever you are doing.",
            "means_of_fishing": {
                "tokens": sorted(C.WHILE_MEANS),
                "reading": "A WAY OF FISHING. The rule binds while you fish this way.",
            },
            "devices": {
                "tokens": sorted(C.WHILE_DEVICES),
                "reading": "A DEVICE, used while angling. The rule binds while you use it. It is "
                           "NOT a way of fishing, and nothing grants or bans it as one: a "
                           "downrigger or a light never appears in a `method` slot.",
            },
            "examples": pick(lambda x: bool(_f(x).get("while")), "while", "species", "take",
                             "gear"),
        },
        "methods": {
            "reading": "The ways you may sport fish are those the PROVINCE allows — an `allow` "
                       "on the `method` slot in a `zp:` rule with no `while` — narrowed here by "
                       "`only` and `ban`. A way of fishing no rule allows is NOT a lawful way to "
                       "sport fish anywhere: render an unmentioned method as not permitted, "
                       "never as 'no rule bans it here'. 'Sport fishing' is defined as angling, "
                       "spear fishing, set lining and crayfish trapping.",
            "book": {"text": "Your basic fishing licence entitles you to: angle …; angle with a "
                             "downrigger …; ice fish …; fish with a set line …; fish with a "
                             "spear or an arrow …; trap crayfish …. All other methods of taking "
                             "fin fish and crayfish are illegal.",
                     "where": "the synopsis's 'Allowable Fishing Methods' list",
                     "pages": methods_page},
            "allowed_by_the_province": allowed,
            "examples": pick(lambda x: EID.is_province(x["entry_id"]) and any(
                c.get("slot") == "method" and c.get("allow") for c in _gear(x)), "gear", n=3),
        },
        "conduct": {
            "means": "Acts you must do or must not do, as tokens named in the lawful direction "
                     "(`do_not_waste_catch`). The wording below is the model's own.",
            "acts": acts,
        },
        "bait": BAIT_GUIDE,
        "first_match_per_slot": {
            "means": "Two clauses on one slot: the narrow one first, the general one last.",
            "examples": pick(same_slot, "gear"),
        },
        "examples_by_bound": {
            b: pick(lambda x, b=b: any(b in c for c in _gear(x)), "gear", n=1)
            for b in ("allow", "only", "ban", "except", "of", "members", "max", "min",
                      "unlimited", "when", "unless", "requires", "must_be")},
    }
    return gear


def guide_sizes(d: dict) -> dict:
    """`sizes`: `lengths`, ordered ranges, first match wins."""
    rules = d["rules"]
    pick = _Pick(rules)
    L = lambda x: _f(x).get("lengths") or []
    sizes = {
        "reading": "`lengths` is an ORDERED list of length ranges. For a fish of a given "
                   "length the FIRST range that contains it answers; its `take` (or, absent, "
                   "the rule's `take`) is how many of those you may keep. A length no range "
                   "covers is not spoken about by this rule: at the top level nothing grants "
                   "it; inside a `within` clause the parent quota governs it. A FISH EXACTLY "
                   "ON A PRINTED BOUND IS LEGAL: a range with take 0 does not hold its own "
                   "bounds unless it is `closed` (the book's 'X cm or more' / 'or less'), so a "
                   "shared endpoint always falls to the range that keeps.",
        "range_fields": LENGTH_TEXT,
        "examples": {
            "quota with a ceiling": pick(lambda x: _f(x).get("take") and any(
                b.get("take") == 0 and b.get("min_cm") is not None for b in L(x)),
                "species", "take", "lengths", n=1),
            "floor": pick(lambda x: any(b.get("take") == 0 and b.get("max_cm") is not None
                                        and b.get("min_cm") is None for b in L(x)),
                          "species", "take", "lengths", n=1),
            "window (keep only between)": pick(lambda x: any(
                _closed_range(b) and b.get("take") != 0 for b in L(x)),
                "species", "take", "lengths", n=1),
            "hole (keep none between)": pick(lambda x: any(
                _closed_range(b) and b.get("take") == 0 for b in L(x)),
                "species", "take", "lengths", n=1),
            "a clause counting one size class": pick(lambda x: _f(x).get("within") and L(x)
                                                     and (_f(x).get("take") or 0) > 0,
                                                     "species", "take", "lengths", "within",
                                                     n=1),
        },
    }
    return sizes


def guide_time(d: dict) -> dict:
    """`time`: `when` — dates, hours, weekdays, unparsed, suspended_while."""
    rules = d["rules"]
    lic = d["licensing"]
    pick = _Pick(rules)
    lpick = _Pick(lic)
    W = lambda x: _f(x).get("when") or {}
    wraps = lambda x: any((r["to_month"], r["to_day"]) < (r["from_month"], r["from_day"])
                          for r in W(x).get("dates") or [])
    time = {
        "reading": "`when` says when a rule binds. Absent means ALL YEAR, as the synopsis "
                   "says: 'When no date is listed, the regulations apply all year. Start and end "
                   "dates are inclusive.' There is no 'except' form: a rule printed as 'open "
                   "except …' stores the days it DOES hold.",
        "fields": {k: WHEN_TEXT.get(k) for k in _fields(C.When)},
        "hours_and_weekdays": "Apply them on the angler's own clock and day; they never "
                              "resolve to a date. A rule held some weekdays or hours DECIDES at "
                              "those moments and is out at the others (`ladder.competition`); "
                              "a case asked at one states it in `at`.",
        "unparsed": "Uncertain, never all year.",
        "suspended_while": "On a rule or a designation: dormant while the named closure rule in "
                           "the same entry binds ('not required until reopened').",
        "examples": {
            "dates": pick(lambda x: bool(W(x).get("dates")) and not wraps(x), "when", n=1),
            "wrapping the year end": pick(wraps, "when", n=1),
            "hours": pick(lambda x: bool(W(x).get("hours")), "when", n=1),
            "weekdays": pick(lambda x: bool(W(x).get("weekdays")), "when", n=1),
            "unparsed": pick(lambda x: bool(W(x).get("unparsed")), "when", n=1),
            "suspended_while": (pick(lambda x: bool(_f(x).get("suspended_while")),
                                     "suspended_while", n=1)
                                + lpick(lambda x: bool(x["fields"].get("suspended_while")),
                                        "suspended_while", n=1)),
        },
        "counts": {"rules_with_when": sum(bool(W(x)) for x in rules.values()),
                   "rules_all_year": sum(not W(x) for x in rules.values()),
                   "rules_with_unparsed": sum(bool(W(x).get("unparsed")) for x in rules.values())},
    }
    return time


def guide_species(d: dict) -> dict:
    """`species`: codes, groups, expansion, species_except, open subjects."""
    rules = d["rules"]
    pick = _Pick(rules)
    L = lambda x: _f(x).get("lengths") or []
    species = {
        "reading": "The fish are THE BOOK'S LIST and nothing else (p.80, 'Freshwater game fish "
                   "are defined as follows'): `species.fish`, under the book's own headings "
                   "(`species.families`). `species` on a record holds the codes the book wrote, "
                   "groups included. Expand a group with `species.groups[code].members` (flat). "
                   "`species_except` carves codes out after expansion. The OPEN subjects "
                   "(`ALL_FIN_FISH`, `PROTECTED_SPECIES`, `SALMON`) are words the book prints "
                   "for fish that are not game fish, and have no member list on purpose — an "
                   "empty list is NOT 'no fish': `ALL_FIN_FISH` is every fish (never crayfish), "
                   "and the other two hold no fish you can ask about.",
        "trout_includes_char": "'Trout' includes char unless char are specifically excluded "
                               "(p.80: 'all regulations that apply to trout (as a group) also "
                               "apply to char unless char are specifically excluded'). A row "
                               "or zone table excludes them by MENTIONING CHAR APART (user "
                               "ruling 2026-09-28): where it names a char on its own ('char', "
                               "Dolly Varden/bull trout, lake trout, brook trout — 'trout/char' "
                               "names char in and does not count), its bare 'trout' lines are "
                               "`TROUT_CHAR` with species_except `CHAR` — trout only (Region 6's "
                               "'Trout under 30 cm from any stream', Region 1's 'Trout: 4'). "
                               "Otherwise 'Trout daily quota = 2' is `TROUT_CHAR` and counts "
                               "char too. There is no trout-only code. For the same-statement "
                               "test (`ladder.quotas_sit_beside`) the printed word decides: a "
                               "lake's 'Trout daily quota = 2' and Region 1's 'Trout: 4' are the "
                               "same statement, so the lake's 2 replaces the 4 for trout. "
                               "'Char' (`CHAR`) is how the book "
                               "names char apart from trout, so a rule about char NAMES each "
                               "char (`ladder.who_speaks`): Region 1's 'you must release: All "
                               "char (includes Dolly Varden)' still releases a char on a lake "
                               "printing 'Trout daily quota = 2'.",
        "bull_trout_is_dolly_varden": "A bull trout IS a Dolly Varden in the regulations: '*Any "
                                      "bull trout that you catch and keep must be counted as "
                                      "part of your Dolly Varden quota' (p.80). One code, `DV`, "
                                      "named 'Dolly Varden/bull trout'; a rule printed about "
                                      "bull trout is about it, and its verbatim keeps the "
                                      "book's word.",
        "several_fish_one_number": "A number on a rule that names more than one fish is SHARED "
                                   "between them: 'Trout/char: 5' is five in total.",
        "examples": {
            "a group": pick(lambda x: any(c in C.SPECIES_GROUPS for c in
                                          _f(x).get("species") or []) and
                            (_f(x).get("take") or 0) > 0 and not L(x),
                            "species", "take", n=1),
            "species_except": pick(lambda x: bool(_f(x).get("species_except")),
                                   "species", "species_except", "take", n=1),
            "an open subject": pick(lambda x: any(c in C.OPEN_SUBJECTS
                                                  for c in _f(x).get("species") or []),
                                  "species", "take", "may_target", n=1),
        },
    }
    return species


def guide_retention(d: dict) -> dict:
    """`retention`: take, may_target, period, per_daily, within, and the readings of take 0."""
    rules = d["rules"]
    pick = _Pick(rules)
    L = lambda x: _f(x).get("lengths") or []
    rl = lambda x: x["type"] == "retention_limit"
    F = lambda x, k: _f(x).get(k)
    retention = {
        "fields": {k: RULE_FIELD_TEXT.get(k) for k in ("take", "may_target", "unlimited", "period",
                                                   "per_daily", "within", "lengths",
                                                   "record_retention", "origin")},
        "three_readings_of_take_0": {
            "closed": "take 0 AND may_target false — you may not fish for it at all",
            "release": "take 0, may_target true, no `lengths` — fish for it, release every one",
            "size gate": "a range in `lengths` with take 0 — those sizes go back; not a closure. "
                         "Where `lengths` is present the ranges are the answer; the top-level take "
                         "only fills a range with no take of its own. 'No trout over 50 cm' is "
                         "`take: 0` with one range "
                         "{min_cm: 50, take: 0}, and says nothing about a trout under 50 cm",
            "and": "A take 0 carrying `while` closes that way of fishing, not the water; a "
                   "`standing` one is shown and decides nothing.",
        },
        "examples": {
            "closed": pick(lambda x: C.says_no_fishing(x["type"], F(x, "take"), F(x, "may_target"))
                           and not F(x, "while") and not F(x, "standing"),
                           "species", "take", "may_target", "when", n=1),
            "release": pick(lambda x: rl(x) and F(x, "take") == 0 and F(x, "may_target")
                            and not L(x), "species", "take", "may_target", n=1),
            "size gate": pick(lambda x: rl(x) and F(x, "take") is None and
                              any(b.get("take") == 0 for b in L(x)),
                              "species", "lengths", n=1),
            "possession": pick(lambda x: rl(x) and F(x, "per_daily"),
                               "species", "period", "per_daily", n=1),
            "annual": pick(lambda x: rl(x) and F(x, "period") == "annual",
                           "species", "take", "period", n=1),
            "unlimited": pick(lambda x: rl(x) and F(x, "unlimited"), "species", "unlimited",
                              n=1),
            "within": pick(lambda x: rl(x) and F(x, "within"), "species", "take", "within",
                           n=1),
            "record_retention": pick(lambda x: F(x, "record_retention"),
                                     "species", "record_retention", n=1),
        },
        "record_duty": {
            "reading": "An annual quota's duty to record what you keep is PRINTED, as its own "
                       "rule carrying `record_retention: true` (a `retention_limit`; there is no "
                       "`record_retention` rule type). The quota carries `recorded_by` (that rule's "
                       "id) and the record rule carries `records_for` (the quotas). NEVER "
                       "generate a 'record each one you keep on your licence' line for an annual "
                       "quota: show the linked record rule's text ONCE, under the quota, and do "
                       "not show it a second time as a rule of its own there. An annual quota "
                       "with no `recorded_by` gets nothing extra.",
            "link": "Derived, never authored: the record rule names the same fish (groups "
                    "expanded), the same origin or none, the same dates or none, and is in "
                    "force in every rule set that holds the quota. Exactly one such rule links.",
            "pairs": [{"quota": q, "record": x["recorded_by"], "quota_says": x["verbatim"],
                       "record_says": rules[x["recorded_by"]]["verbatim"]}
                      for q, x in sorted(rules.items()) if x.get("recorded_by")],
            "without_record": sorted(q for q, x in rules.items()
                                     if F(x, "period") == "annual" and not x.get("recorded_by")),
        },
    }
    return retention


def guide_vessel(d: dict) -> dict:
    """`vessel`: aspect, level, power, speed."""
    rules = d["rules"]
    pick = _Pick(rules)
    F = lambda x, k: _f(x).get(k)
    vessel = {
        "fields": {k: RULE_FIELD_TEXT.get(k) for k in ("aspect", "level", "max_power_kw",
                                                   "max_kmh")},
        "levels_strictest_first": _enum(C.PropulsionLevel),
        "aspects": _enum(C.VesselAspect),
        "reading": "Read `aspect` first. Propulsion `level` is one ordered scale, strictest "
                   "first; `power_capped` carries `max_power_kw`. A speed rule carries `max_kmh`.",
        "examples": {a: pick(lambda x, a=a: F(x, "aspect") == a, "aspect", "level",
                             "max_power_kw", "max_kmh", "when", n=1)
                     for a in _enum(C.VesselAspect)},
    }
    return vessel


def guide_exempts(d: dict) -> dict:
    """`exempts`: how a rule lifts another."""
    rules = d["rules"]
    pick = _Pick(rules)
    F = lambda x, k: _f(x).get(k)
    def part(e):
        return any(k in e for k in ("species", "when_targeting", "while", "when"))
    exempts = {
        "reading": "A rule with `exempts` LIFTS the rules it names, where and while it binds. "
                   "Every item is RESOLVED to one rule, `entry_id::rule_id`: match on those "
                   "exact ids, never on a bare name. An item with no `species`, "
                   "`when_targeting`, `while` or `when` lifts that rule outright. One with any "
                   "of them lifts it only for those anglers, or only on those days — the rule stays in force for everyone "
                   "else, and since the angler is unknown the lift is a condition beside it, "
                   "never a removal. A lift is never wider than its lifter. A lift whose place "
                   "cannot be drawn is not applied, and a rule never lifts itself. Some lifts "
                   "are not printed as exemptions but DERIVED at build (`basis`): a water row "
                   "naming a fish its region closes lifts that closure for that fish (see "
                   "`ladder.closures`). Read them exactly like printed ones.",
        "fields": dict(LIFT_TEXT),
        "examples": {
            "whole": pick(lambda x: any(not part(e) for e in F(x, "exempts") or []),
                          "exempts", n=1),
            "in part": pick(lambda x: any(part(e) for e in F(x, "exempts") or []),
                            "exempts", "species", "when_targeting", "while", n=1),
            "with the book's note": pick(
                lambda x: any(e.get("note") for e in F(x, "exempts") or []), "exempts", n=1),
            "derived — the water names the fish": pick(
                lambda x: any(e.get("basis") == "names_the_fish" for e in F(x, "exempts") or []),
                "exempts", n=1),
            "derived — the water prints (any size)": pick(
                lambda x: any(e.get("basis") == "any_size" for e in F(x, "exempts") or []),
                "exempts", n=1),
        },
        "rules": sum(bool(F(x, "exempts")) for x in rules.values()),
        "derived_lifts": sum(1 for x in rules.values() for e in F(x, "exempts") or []
                             if e.get("basis")),
    }
    return exempts


def guide_standing(d: dict) -> dict:
    """`standing`: rules everywhere at places nobody can draw."""
    rules = d["rules"]
    pick = _Pick(rules)
    F = lambda x, k: _f(x).get(k)
    standing = {
        "reading": "`standing: true` — the rule holds everywhere at places no dataset can draw "
                   "('within 23 m downstream of any fishway'). It is bound to every section so "
                   "it is always shown; it never decides a water's outcome and never competes.",
        "rules": [x["id"] for x in rules.values() if F(x, "standing")],
        "examples": pick(lambda x: bool(F(x, "standing")), "species", "take", "may_target"),
    }
    return standing


def guide_angler_closure(d: dict) -> dict:
    """`angler_closure`: closures to one kind of angler."""
    rules = d["rules"]
    pick = _Pick(rules)
    angler_closure = {
        "reading": "The water is closed to the anglers in `closed_to` (a `Who`) on the days in "
                   "`when`, except the anglers in any `closed_to_except` (each a `Who`). It never "
                   "competes with a quota. Because the angler is unknown, the answer is "
                   "conditional: 'if you are a non-guided non-resident alien, you may not angle "
                   "here on Saturdays'; on a Youth/Disabled Accompanied Water, 'if you are 16 or "
                   "over you may angle here only as a disabled B.C. resident or as the companion "
                   "of an authorized angler'.",
        "rules": [x["id"] for x in rules.values() if x["type"] == "angler_closure"],
        "examples": pick(lambda x: x["type"] == "angler_closure", "closed_to", "when"),
    }
    return angler_closure


def guide_licensing(d: dict) -> dict:
    """`licensing`: kinds, who, doing, paths, designations and their classified `period`, and the rules of reading."""
    lic = d["licensing"]
    lpick = _Pick(lic)
    kinds = Counter(x["kind"] for x in lic.values())
    lmodels = _licensing_models()
    lkinds = {}
    for k, m in lmodels.items():
        lkinds[k] = {"means": LICENSING_KIND_TEXT.get(k),
                     "placed": next(p for t, _, p in _LIC_TABLES if t == k),
                     "records": kinds.get(k, 0),
                     "fields": [f for f in _fields(m) if f not in ("kind", "id", "verbatim")],
                     "examples": lpick(lambda x, k=k: x["kind"] == k, n=2)}
    has_path = lambda x, key: any(key in p for p in x["fields"].get("satisfied_by") or [])
    licensing = {
        "rules_of_reading": [
            "Licensing NEVER affects whether a water is open, closed or restricted. It is "
            "consulted only once the water is open for what the angler is doing, so a closed "
            "water needs no licence by construction.",
            "The angler is ALWAYS unknown. Every answer is conditional on who they are ('if "
            "you are a non-resident …'); there is no default angler.",
            "A requirement is met by ANY ONE of its paths; a `hold` path needs ALL of its "
            "documents.",
            "An `exemption` removes documents for the anglers it names, AND every duty whose "
            "`presumes` are all among those documents. The angler is unknown, so render such a "
            "duty conditionally ('unless you are an Indian resident of B.C.'), never drop it. An "
            "`alternative` only ever adds a path.",
            "A record with `restates` is the row's own words for a record stated elsewhere (a "
            "provincial one); read its `who` and `satisfied_by` as that record's — the Dean's "
            "'All anglers are required to buy a Classified Waters Licence' is 16 and over because "
            "the provincial requirement it restates is.",
            "A designation obliges nothing on its own; requirements with `on` fire where it is "
            "in force, and the classified-water steelhead stamp runs during "
            "`steelhead_stamp_during`, unless `steelhead_stamp_waived`.",
            "THE STAMP WAIVER (user ruling 2026-10-02). A designation whose `stamp_waiver.outright` "
            "is true ('(Steelhead Stamp not required)': Chilko upstream of Brittany Creek Jun "
            "11-Oct 31, Dean from Anahim Lake to the Iltasyuko, Horsefly, West Road mainstem, "
            "Stellako) means NO steelhead stamp on its sections while it is in force: a "
            "requirement with `waived_where: steelhead_stamp_waived` (the provincial stamp to "
            "fish for steelhead, `stamp_waiver.lifts`) does not hold there and then. Say "
            "`stamp_waiver.says`. Outside the designation's dates, or off its sections, the "
            "stamp is required as written. The Classified Waters Licence still applies (it is "
            "Class II water), and so do the steelhead rules — release wild steelhead, the annual "
            "10, the record duty. A waiver 'unless fishing for steelhead' (Seymour, Ecstall, "
            "Skeena River 2) lifts only the classified-water stamp. "
            "`pipeline.deliver.bundle.read.requirements_in_force` is the reference reader.",
            "A designation's classified period is its `period`, never inferred from a missing "
            "`when`: `when_open` (Classified whenever the water is open), `all_year`, or "
            "`dates`. Show `period.says`.",
            "An unresolved record renders as 'check', never as 'none needed'.",
            "A requirement with `not_yet_mapped` (`fields.undrawn_part`) holds only in a part of "
            "its water that nothing draws — the Creston Valley WMA permit on the south end of "
            "Kootenay Lake's Main Body. Show it on the water as a place not yet mapped "
            "(`not_yet_mapped.says`); NEVER tell an angler the whole water needs it. "
            "`read.requirements_in_force` lists it under `not_yet_mapped`, never `holds`.",
            "THE PAPER LICENCE. 'Carry your paper licence' (`doing.act: retaining_recorded`) is "
            "about the fish whose retention you must record on the licence: the record's "
            "`records` (every rule carrying `record_retention: true`) and, in words, "
            "`parts.records`. Say "
            "them; never type the list.",
        ],
        "kinds": lkinds,
        "classes": licence_classes(lic),
        "paper_licence": {
            "reading": "Electronic licences are acceptable, but a PAPER licence is required "
                       "when keeping a fish whose retention you must record on it (p.6). Which "
                       "fish is DERIVED from the record-duty rules (`record_retention`), so a "
                       "duty added to the corpus reaches this line by itself.",
            "duties": [{"id": k, "records": x.get("records", []),
                        "says": (x.get("parts") or {}).get("records")}
                       for k, x in lic.items()
                       if (x["fields"].get("doing") or {}).get("act") == "retaining_recorded"],
        },
        "who": {
            "reading": "A set on every axis: the members listed are IN. An axis left out means "
                       "ANY member. `who` absent means every angler.",
            "axes": {a: {"members": list(v), "means": WHO_TEXT.get(a)} for a, v in C.WHO_AXES.items()},
            "examples": lpick(lambda x: bool(x["fields"].get("who")), "who", "who_except"),
        },
        "doing": {
            "acts": {a: DOING_TEXT.get(a) for a in typing.get_args(
                C.Doing.model_fields["act"].annotation)},
            "fields": _fields(C.Doing),
            "examples": [lpick(lambda x, a=a: (x["fields"].get("doing") or {}).get("act") == a,
                               "doing", "who", n=1)
                         for a in typing.get_args(C.Doing.model_fields["act"].annotation)],
        },
        "paths": {
            "fields": {k: PATH_TEXT.get(k) for k in _fields(C.Path)},
            "quota_values": list(typing.get_args(typing.get_args(
                C.Path.model_fields["quota"].annotation)[0])),
            "examples": {
                "hold": lpick(lambda x: has_path(x, "hold"), "satisfied_by", "who", n=1),
                "accompanied_by": lpick(lambda x: has_path(x, "accompanied_by"),
                                        "satisfied_by", "who", n=1),
                "as": lpick(lambda x: has_path(x, "as"), "satisfied_by", "who", n=1),
            },
        },
        "designations": {
            "stamp_during": lpick(lambda x: bool(x["fields"].get("steelhead_stamp_during")),
                                  "classified", "when", "steelhead_stamp_during", n=1),
            "stamp_waived": lpick(lambda x: bool(x["fields"].get("steelhead_stamp_waived")),
                                  "classified", "steelhead_stamp_waived", n=1),
            "stamp_waived_outright": [
                dict(e, stamp_waiver=lic[e["id"]]["stamp_waiver"]) for e in lpick(
                    lambda x: (x.get("stamp_waiver") or {}).get("outright") is True,
                    "classified", "when", "steelhead_stamp_waived", n=1)],
            "waived_where": lpick(lambda x: bool(x["fields"].get("waived_where")),
                                  "waived_where", "doing", "satisfied_by", n=2),
            "on_designation": lpick(lambda x: bool(x["fields"].get("on")), "on", "doing",
                                    "satisfied_by", n=1),
        },
        "stamp_waiver": {
            "reading": "A designation that waives the Steelhead Stamp carries `stamp_waiver`. "
                       "OUTRIGHT ('(Steelhead Stamp not required)', user ruling 2026-10-02): on "
                       "its sections, on the days it is in force, no steelhead stamp is required "
                       "— neither the classified-water stamp nor the requirements in `lifts` "
                       "(`waived_where: steelhead_stamp_waived`, the provincial stamp to fish for "
                       "steelhead). On other days, or other sections, they hold as written. The "
                       "Classified Waters Licence and every steelhead rule still apply. A waiver "
                       "'unless fishing for steelhead' lifts only the classified-water stamp.",
            "fields": STAMP_WAIVER_TEXT,
            "designations": [{"id": k, **x["stamp_waiver"]} for k, x in lic.items()
                             if x.get("stamp_waiver")],
        },
        "period": {
            "reading": "Every designation says when it is in force in `period`. 'Class II water "
                       "when open' prints no dates on purpose: licensing is consulted only while "
                       "the water is open, so the period is the water's own open time — show "
                       "'Classified (Class II) whenever this water is open', never 'during its "
                       "classified period'. 'Class I water all year' is every day. Otherwise the "
                       "period is `dates` (the record's own `when`).",
            "fields": PERIOD_TEXT,
            # Lists, not dicts keyed by kind: `when_open` is a retired FIELD name (licensing
            # decision 6), so it may be a value here and never a key.
            "counts": [{"kind": k, "designations": n} for k, n in sorted(Counter(
                x["period"]["kind"] for x in lic.values() if x["kind"] == "designation").items())],
            "examples": [dict(e, period=lic[e["id"]]["period"])
                         for k in ("when_open", "all_year", "dates")
                         for e in lpick(lambda x, k=k: x["kind"] == "designation"
                                        and x["period"]["kind"] == k, "classified", "when", n=1)],
        },
        "field_text": {k: LICENSING_FIELD_TEXT.get(k) for k in sorted(
            {f for m in lmodels.values() for f in _fields(m)} - {"kind", "id", "verbatim"})},
        "documents": "See `licences`: the register, `provincial` = sold to an angler under the "
                     "Wildlife Act (what 'any type of fishing licence or stamp' means).",
    }
    return licensing


def guide_placement(d: dict) -> dict:
    """`placement`: sets, waters and their parts, outside B.C., tidal, steelhead, lake parts, via, binds, not_yet_mapped, uncertain."""
    rules = d["rules"]
    lic = d["licensing"]
    pick = _Pick(rules)
    rvia = Counter(v for s in d["rulesets"].values() for v in s if v != "sections")
    lvia = Counter(v for s in d["licensing_sets"].values() for v in s if v != "sections")
    placement = {
        "unnamed_sets": "`waters` lists only named waters: a ruleset or licensing set that no "
                        "water lists sits only on unnamed sections. THE APP NEEDS THEM and they "
                        "ship (decision, Phase 4 / E5): an unnamed creek a reader taps is "
                        "resolved from the BUNDLE (tile `section_id` -> `section_ruleset` / "
                        "`section_licensing` -> set id, AGENTS 5), and its records are read here "
                        "(`rulesets[set id]`), exactly as a named water's part is. Dropping them "
                        "would leave that tap to read the bundle's `ruleset` membership instead "
                        "— a second source for the same answer. Reach such a set through a "
                        "section's set id, never through `waters`.",
        "reading":"Where a record applies is exported the way the bundle interns it. Many "
                   "sections carry the same set of records, so each SET is listed once "
                   "(`rulesets`, `licensing_sets`: its members grouped by `via`, and how many "
                   "sections carry it). Each named water lists its `parts`: a part is KEYED BY "
                   "THE FIVE-TUPLE (ruleset, licensing_set, province_except, "
                   "anadromous_rainbow, steelhead) its sections carry TOGETHER, with how many "
                   "sections — to find the part a section of the bundle is in, match all five "
                   "(the bundle's `section_ruleset`, `section_licensing`, `province_except`, "
                   "`steelhead_water`, `section_steelhead`), never the (ruleset, licensing_set) "
                   "pair alone — "
                   "so a licence area joins the rules on the same stretch — and, on a part, "
                   "`province_except` (the families of areas its sections lie in where a "
                   "province-wide requirement stops: a national park), `anadromous_rainbow` "
                   "(a rainbow over 50 cm is a steelhead there; `ladder.steelhead_definition`) "
                   "and `steelhead` (\"known\" | \"possible\": how sure we are that steelhead "
                   "are present; the water carries the roll-up). "
                   "`null` on either side "
                   "is a stretch with no set of that kind. The parts' sections sum to the "
                   "water's `sections`. A part is a class of sections, not one stretch; which "
                   "parts border each other is `touches` (below) — merge only parts connected "
                   "through it. Set ids are local to this file and change with every "
                   "build; never store one. Section handles never leave the bundle.",
        "touches": TOUCHES_TEXT,
        "runs": {
            "reading": "WHERE EACH PART RUNS. A part's `runs` are its stretches, UPSTREAM TO "
                       "DOWNSTREAM: each from one end (`from`, upstream) to another (`to`, "
                       "downstream), with `km_from` / `km_to` measured from the water's MOUTH "
                       "along its main stem (the blue line carrying most of its length), so "
                       "km_from >= km_to and the runs' km fall as you read down the list. Say a run "
                       "'from <from> to <to>' with each end's name: a cut-point id or region line "
                       "is a key of `splits` (its `name`); `lake_inlet:` / `lake_outlet:` / "
                       "`confluence:` name a water (`waters[id].name`); `mouth`, `source` and "
                       "`bc_border` are the water's own ends. A run between the same cut on both "
                       "ends lies inside an area ('within Chilliwack River Ecological Reserve'). "
                       "The pipeline cut the water at exactly these points: this reports them and "
                       "infers nothing. Use km_from / km_to to draw or order a stretch. A river "
                       "runs through a LAKE as two runs (one ending at `lake_inlet`, the next "
                       "starting at `lake_outlet`) — the lake is its own water. A river's OWN "
                       "polygon (its wide reach, drawn as a polygon: `kind: stream`) is not a "
                       "lake: one run passes through it, and a run ending at it says `polygon`.",
            "lakes": "A lake or wetland part is its polygon: ONE run with from, to, km_from and "
                     "km_to all null, and `polygon`: `whole`, or a lake PART's own name. A lake "
                     "has no ends to name; the polygon is the place. A stream part made only of "
                     "polygons its main stem does not pass through (a slough drawn only as "
                     "polygons) is the same shape.",
            "branches": "A run with `branch: true` lies off the main stem (a side channel or braid "
                        "of the same water whose sets differ from the stem beside it); both its km "
                        "are where it rejoins the stem (null when it never does).",
            "ends": END_TEXT,
            "fields": RUN_TEXT,
            "counts": {
                "stream_parts": sum(1 for w in d["waters"].values()
                                    for p in w["parts"] if "polygon" not in p["runs"][0]),
                "runs": sum(len(p["runs"]) for w in d["waters"].values() for p in w["parts"]),
                "parts_with_several_runs": sum(1 for w in d["waters"].values()
                                               for p in w["parts"] if len(p["runs"]) > 1),
                "branch_runs": sum(1 for w in d["waters"].values() for p in w["parts"]
                                   for r in p["runs"] if r.get("branch")),
            },
            "splits": "`splits` (top level) names every cut: id -> {name, kind, water_id, km}, "
                      "km from that water's mouth. A name is unique among the places of each "
                      "water, a confluence is named for the OTHER water that joins there, and no "
                      "name is all capitals; two ids at one place share a name and the second "
                      "says `same_place_as`. Show `name`, never an id. See "
                      "`field_dictionary.splits`.",
        },
        "outside_bc": "A water's `outside_bc` counts its sections outside British Columbia — "
                      "past the border, or in no region. No B.C. regulation applies there and "
                      "the book does not govern them: they carry no set (the build refuses one "
                      "that does), and must read 'outside B.C.', never 'open under the general "
                      "rules'.",
        "waters_with_sections_outside_bc": sum(1 for w in d["waters"].values()
                                               if w.get("outside_bc")),
        "tidal": "A water's `tidal` (present only where the book calls the water tidal — Nitinat "
                 "Lake) is `{sections, entry, guide}`: how many of its sections are tidal, the row "
                 "that says so, and the words to show. No provincial regulation holds there: its "
                 "sections carry only that row's note (the build refuses any other rule or a "
                 "licensing set), and every province-wide requirement stops there "
                 "(`province_except`: `tidal`).",
        "waters_with_tidal_sections": sum(1 for w in d["waters"].values() if w.get("tidal")),
        "steelhead": "A water's `steelhead` (and each part's) says how sure we are that steelhead "
                     "are present: " + STEELHEAD_TEXT + " A water's `steelhead_source` says why it is "
                     "known (\"regulations\", \"curated list\", or both) and `steelhead_rows` "
                     "which steelhead rows bind it. IN THE BUNDLE the same fact is "
                     "`section_steelhead.code` (a view): 1 = \"known\", 2 = \"possible\", no row "
                     "= absent; `steelhead_water` (a view) is the book's steelhead streams.",
        "waters_by_steelhead": dict(sorted(Counter(
            w["steelhead"] for w in d["waters"].values() if w.get("steelhead")).items())),
        "part_of": "A lake the atlas cuts into parts (Kootenay Lake's Main Body and West Arms, "
                   "Williston Lake's arms and zones, Shannon Lake's netted-off portion) lists each "
                   "part as its own water, with `part_of` naming the whole. No record may name "
                   "the whole (the reach refuses it), so the whole's own entry here carries only "
                   "what reaches it by area or province-wide: it is a leftover of the cut, not a "
                   "stretch an angler fishes apart from its parts. Show a whole through its "
                   "parts — the waters whose `part_of` is its id — never through its own `parts`.",
        "lakes_cut_into_parts": sorted({w["part_of"] for w in d["waters"].values()
                                        if w.get("part_of")}),
        "via": {k: VIA_TEXT[k] for k in VIA_TEXT},
        "via_counts": {"rulesets": dict(sorted(rvia.items())),
                       "licensing_sets": dict(sorted(lvia.items()))},
        "licensing_placement": PLACEMENT_TEXT,
        "licensing_placement_counts": dict(sorted(Counter(
            x["placement"] for x in lic.values()).items())),
        "binds": {
            "reading": "Every rule says where it holds in `binds`.",
            "values": BINDS_TEXT,
            "counts": dict(sorted(Counter(x["binds"] for x in rules.values()).items())),
            "in_part": [x["id"] for x in rules.values() if x["binds"] == "sections_in_part"],
            "examples": pick(lambda x: x["binds"] == "sections_in_part", "undrawn_part",
                             "extents", n=2),
        },
        "uncertain": {
            "rules": [x["id"] for x in rules.values() if x["provenance"]["uncertain"]],
            "licensing": [x["id"] for x in lic.values() if x["provenance"]["uncertain"]],
            "reading": "An uncertain record binds nowhere: it can only ever raise 'unknown', "
                       "never 'no rules here'. `provenance.why` says why.",
        },
        # A row that is only a pointer binds nothing, so it has nothing to place. A row with no
        # matched water whose rules DO bind (an area row: Bowron Lake Park waters, the Liard
        # River watershed — thousands of sections through its `within` extents) is placed, and
        # is not listed (SP-14): "unplaced" means NOTHING of the row reaches the map.
        "unplaced_entries": [e for e, v in d["entries"].items() if not v["matched"]
                             and v["kind"] == "water"
                             and (v["rules"] or v["licensing"] or not v.get("see"))
                             and not any(rules[r]["binds"] != "nowhere" for r in v["rules"])],
        "unplaced_entries_reading": "Water rows NOTHING of which reaches the map: no matched "
                                    "water, and no rule of the row bound anywhere. Their rules "
                                    "are `binds: nowhere` and can only raise 'unknown'. An area "
                                    "row with no matched water whose `within` extents bind is "
                                    "placed and is not listed.",
    }

    in_part = [x for x in rules.values() if x.get("not_yet_mapped")]
    placement["not_yet_mapped"] = {
        "reading": "A rule holding only in a part of its water that nothing draws. Show it AT "
                   "THE TOP of the water, prominently, as a place not yet mapped "
                   "(`not_yet_mapped.says`), and never let it decide the water: it never "
                   "competes, never displaces, never lifts (`ladder.not_yet_mapped`).",
        "rules": len(in_part),
        "closures": sorted(x["id"] for x in in_part if _closure(_f(x) | {"type": x["type"]})),
    }
    return placement


def guide_status() -> dict:
    """`status`: the map and search colour, which ships as `status_index.bin`."""
    status = {
        "reading": "THE MAP AND SEARCH COLOUR is not in this file. It ships beside it as "
                   "`status_index.bin` (`python -m pipeline.deliver.status_index`), one of three "
                   "answers per section and per water for every day of the year, computed from the "
                   "bundle by the same reference reader this guide restates "
                   "(`read.effective_rules`), so the colour cannot disagree with the rules the "
                   "page shows. Use it to colour; use this file to explain.",
        "statuses": {
            "closed": "on that day every game fish (the book's list minus crayfish) is answered "
                      "by an unconditional closure that speaks AT EVERY MOMENT of the day — a "
                      "night closure closes its hours, never the day, and a closure of some "
                      "weekdays closes those weekdays, never the day (the answers file carries "
                      "each moment's status); not a closure of an unreadable season, not one half "
                      "of the channel, not a `not_yet_mapped` note, not a closure of some species "
                      "only",
            "own": "not closed, and a water table's row (`r<n>:` — the water's own row, a cut "
                   "piece's, an area row, or one reaching it by the tributary walk) binds it; all "
                   "year, as the synopsis lists the water",
            "base": "neither: only zone, area-of-zone, provincial and superior rules bind it. "
                    "Absent from the file means base",
            "tidal": "tidal water (Nitinat Lake): no freshwater status; show `water.tidal.guide`",
            "outside": "outside B.C.: no B.C. regulation — never 'base'",
        },
        "water": "A water's colour is its parts' roll-up: closed when every part is closed "
                 "(tidal / outside when every part is); own when any part carries a row's rule; "
                 "base otherwise. Licensing never changes the colour (it never affects open or "
                 "closed).",
        "vintage": "The file carries the bundle's `section_handles` digest "
                   "(`about.bundle.section_handles`); a reader refuses one whose digest differs, "
                   "as it refuses tiles and a bundle of different vintages.",
    }
    return status


def guide_gotchas(d: dict) -> dict:
    """`gotchas`: where a page is easy to get wrong (`with_closure_gotchas` adds the ones read from the bundle)."""
    rules = d["rules"]
    F = lambda x, k: _f(x).get(k)
    in_part = [x for x in rules.values() if x.get("not_yet_mapped")]
    cautions = [(x["id"], e) for x in rules.values() for e in F(x, "exempts") or []
                if e.get("caution")]
    gotchas = {
        "reading": "Places where the data is right and a page is still easy to get wrong. Each "
                   "names the field to key on.",
        "size_clause_override": {
            "says": "A water row printing a LARGER number for a fish than its region allows "
                    "overrides the region's size clause ('only 1 over 50 cm') for that fish — "
                    "'(any size)' printed or not (user ruling 2026-09-26). ONLY WHERE THE ROW "
                    "PRINTS '(ANY SIZE)' is that confusing (user ruling 2026-09-28): Kootenay "
                    "Lake's 'rainbow trout daily quota = 10 (any size)' — does 'any size' mean "
                    "no size limit at all, overriding Region 4's 'only 1 rainbow trout or "
                    "cutthroat trout over 50 cm', or only no minimum size? (Duncan and Lardeau "
                    "rivers, Quesnel Lake likewise.) Only those lifts carry a warning: show "
                    "`exempts[].caution.says` beside the lifter wherever the lift is shown, and "
                    "never drop it because the lift itself applied. A row printing its own "
                    "sizes (Gwillim Lake's 'none under 40 cm or over 60 cm', Kitimat's hatchery "
                    "steelhead '>50 cm') or no size at all (Jewel Lake's 'Brook trout daily "
                    "quota = 20') overrides the clause with no warning.",
            "key_on": "rules[*].fields.exempts[*].caution.kind == 'size_clause_override'",
            "lifts": len(cautions),
            "lifters": sorted({i for i, _ in cautions}),
        },
        "not_yet_mapped": {
            "says": "A rule whose part of the water nothing draws ('No Fishing within 200 m of "
                    "Bush-Sullivan Bridge' on Kinbasket Lake) is placed on the WHOLE water only "
                    "so it can be seen. Show it at the top of the water, marked as a place not "
                    "yet mapped; it never closes, colours or decides the water.",
            "key_on": "rules[*].not_yet_mapped (display: 'prominent'); in an answer, state "
                      "'not_yet_mapped'",
            "rules": len(in_part),
        },
        "trout_includes_char": {
            "says": "'Trout' includes char (p.80: 'all regulations that apply to trout (as a group) "
                    "also apply to char unless char are specifically excluded') UNLESS the line "
                    "excludes char in so many words, or its own row or zone table gives char a "
                    "RELATED line of their own — about the same thing (how many you keep or must "
                    "release, sizes, gear), on the same kind of water and days (user ruling "
                    "2026-10-07). Region 1's box releases 'All char', so its 'Trout: 4' and '2 from "
                    "streams (must be hatchery)' count trout only; Region 6's 'Trout of any size from "
                    "streams, Nov 1-June 30' sits beside its own Dolly Varden stream release, so it "
                    "is trout only, but its size line 'Trout under 30 cm from any stream' has no char "
                    "size line beside it and covers char. A row printing only 'no trout under 25 cm' "
                    "and 'Bull trout catch and release' keeps the 25 cm limit on bull trout too. "
                    "'Trout/char' always includes char. Show `note` beside every trout rule, and "
                    "`note_trout_only` beside one whose char are excluded.",
            "note": "Trout includes char (Dolly Varden/bull trout, lake trout, brook trout) "
                    "unless the line excludes them or char have their own line about the same thing.",
            "note_trout_only": "Char have their own line about the same thing here, so 'trout' "
                               "means trout only (rainbow, steelhead, cutthroat, brown); char follow "
                               "their own lines.",
            "key_on": "species contains TROUT_CHAR (note); and species_except contains CHAR "
                      "(note_trout_only)",
            "trout_only_rules": sorted(i for i, x in rules.items()
                                       if "TROUT_CHAR" in (_f(x).get("species") or [])
                                       and "CHAR" in (_f(x).get("species_except") or [])),
        },
        "dated_zone_release_stands": {
            "says": "A DATED zone release or closure is NOT silenced by a water's quota for the "
                    "fish (user ruling 2026-09-28). Shuswap Lake prints 'Char daily quota = 1 "
                    "(none under 60 cm)'; Region 3 prints 'you must release … Lake trout from "
                    "Oct 15-Jan 31'. On those dates both speak and the release binds: no lake "
                    "trout may be kept. Only the exact same statement (same fish, sizes, origin, "
                    "water kind and the same dates), a lift the row prints, or a derived lift of "
                    "a closure that sends the reader to the tables ('see tables for exceptions', "
                    "Region 8's bass) takes the zone rule away. Never hide a dated zone release "
                    "because the water has its own number. BUT a water row printing ITS OWN "
                    "DATES for the fish overrides the dated zone rule on the days both hold "
                    "(user ruling 2026-09-28): Cheslatta Lake's 'Lake trout … quotas = 3' "
                    "(Nov 1-Sept 14) replaces Region 6's 'Lake trout from Fraser and Skeena "
                    "Watersheds, Sept 15-Nov 30' release on Nov 1-30 (`ladder.dated_zone_release` "
                    "(A)); a zone closure is never overridden this way.",
            "key_on": "an answer holding a zone rule with take 0 and `when.dates` beside a water "
                      "quota for the same fish (`ladder.dated_zone_release`)",
        },
        "one_side_of_channel": {
            "says": "A rule for ONE HALF of a river's channel ('No Fishing on the west half of "
                    "river between fishing boundary signs near Kitimat Hatchery outfall') is "
                    "placed on the stretch, because the map draws a river as one line, but it "
                    "holds on that half only: an angler on the other half follows the river's "
                    "other regulations there. The reference answers it `beside` those (it "
                    "displaces nothing). Show `parts.side` with it, prominently; never colour "
                    "or close the whole width by it.",
            "key_on": "rules[*].fields.side",
            "rules": sorted(i for i, x in rules.items() if _f(x).get("side")),
        },
        "bull_trout_is_dolly_varden": {
            "says": "Bull trout and Dolly Varden are one fish in the regulations (p.80). Ask "
                    "about `DV`; there is no bull trout code.",
            "key_on": "species.fish.DV",
        },
        "open_subjects": {
            "says": "`ALL_FIN_FISH`, `PROTECTED_SPECIES` and `SALMON` have empty member lists "
                    "on purpose. An empty list is not 'no fish'. The fish the book NAMES inside "
                    "two of them are listed apart: chinook (`species.salmon`) and the protected "
                    "fish (`species.protected`); a protected-species rule names its fish in "
                    "`fields.species`.",
            "key_on": "species.groups[*].open",
        },
        "source_artefacts": {
            "says": "Text the book prints inside a row that is NOT a regulation, so no rule "
                    "carries it and the page must not look for one. The row's `regs_verbatim` "
                    "still shows it, as printed.",
            "key_on": "entries[*].id + regs_verbatim; no rule quotes these",
            "artefacts": [{"entry": a["entry_id"], "row": a["row"], "text": a["text"],
                           "says": a["says"]} for a in C.SOURCE_ARTEFACTS],
        },
    }
    return gotchas


def pdf_pages() -> dict[str, int]:
    """`guide.pdf_pages`: the printed -> PDF page map, from the one reader of the PDF's footers."""
    from pipeline.common.synopsis_pages import printed_to_pdf
    return {str(k): v for k, v in printed_to_pdf().items()}
