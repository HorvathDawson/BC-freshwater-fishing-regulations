"""`species`, `field_dictionary` and `index`.

`species_table` is the book's list (p.80) and the groups a rule may name; `field_dictionary` explains exactly
the keys the records carry; `dictionary_gaps` refuses a field the file carries that it does not explain.
"""
from __future__ import annotations

from collections import defaultdict
from pipeline.regs.parsing import catalogue as C
from pipeline.deliver.export import codec as K
from pipeline.deliver.export.guide_words import (
    CLAUSE_TEXT, END_TEXT, ENTRY_TEXT, FILE_TEXT, LENGTH_TEXT, LICENSING_FIELD_TEXT, LICENSING_RECORD_TEXT,
    LIFT_TEXT, OP_TEXT, PERIOD_TEXT, PROVENANCE_TEXT, RECORD_TEXT, RULE_FIELD_TEXT, RUN_TEXT, SET_TEXT,
    SPLIT_TEXT, STAMP_WAIVER_TEXT, TIDAL_TEXT, WATER_PART_TEXT, WATER_TEXT, WHEN_TEXT, WHO_TEXT, extent_text
)
from pipeline.deliver.export.guide import _fields, _licensing_models


# --------------------------------------------------------------------------------------------
# Species, the field dictionary, the index
# --------------------------------------------------------------------------------------------

OPEN_GROUPS = C.OPEN_SUBJECTS


def _name(code: str) -> str:
    return C._SPECIES_WORDS.get(code) or code


def species_table() -> dict:
    """THE BOOK'S LIST (p.80) — every fish a rule may name, under the book's headings — and the
    groups and open subjects a rule may name instead. Nothing else: no official-table sub-species,
    no fish the book does not list."""
    members = {g: list(C.expand_species([g])) if g not in OPEN_GROUPS else []
               for g in C.SPECIES_GROUPS}
    in_group = defaultdict(list)
    for g, ms in members.items():
        for c in ms:
            in_group[c].append(g)
    fish = {}
    for fam, codes in C.BOOK_FAMILIES.items():
        for code in codes:
            d = {"name": _name(code), "family": fam,
                 "scientific": C.SCIENTIFIC_NAMES.get(code),
                 "groups": sorted(in_group[code])}
            if code == "DV":
                d["includes"] = "bull trout — '*Any bull trout that you catch and keep must be " \
                                "counted as part of your Dolly Varden quota' (p.80)"
            if code in C.DEFINITIONAL_SIZE:
                d["definitional_size"] = dict(C.DEFINITIONAL_SIZE[code])
                if code == "ST":
                    # WHERE the definition holds in this file (user ruling 2026-10-01)
                    d["definitional_size"]["marked_by"] = (
                        "waters[*].parts[*].anadromous_rainbow — every known stream part where "
                        "steelhead rules apply, known by the book or by the curated list; never "
                        "a lake, a \"possible\" stream, or a part with `steelhead_rules: false` "
                        "(`guide.ladder.steelhead_definition`)")
            fish[code] = d
    groups = {g: ({"name": _name(g), "members": members[g]} if g not in OPEN_GROUPS else
                  {"name": _name(g), "members": [], "open": True})
              for g in sorted(C.SPECIES_GROUPS)}
    return {"source": "fishing_synopsis.pdf 2025-2027, p.80: 'Freshwater game fish are defined "
                      "as follows'",
            "families": {f: {"name": C._FAMILY_WORDS[f], "members": list(cs)}
                         for f, cs in C.BOOK_FAMILIES.items()},
            "fish": fish, "groups": groups,
            "trout_includes_char": "p.80: 'all regulations that apply to trout (as a group) "
                                   "also apply to char unless char are specifically excluded' "
                                   "— the trout group is TROUT_CHAR; a bare 'trout' line is "
                                   "TROUT_CHAR with species_except CHAR only where it excludes "
                                   "char in words or its row or zone table gives char a RELATED "
                                   "line of their own (same aspect, water kind and days; user "
                                   "ruling 2026-10-07)",
            "trout_note": "Trout includes char (Dolly Varden/bull trout, lake trout, brook "
                          "trout) unless the line excludes them or char have their own line about the same thing.",
            # A SALMON THE BOOK NAMES — not a game fish, not in `fish` (user ruling 2026-09-28)
            "salmon": {c: {"name": _name(c), "group": g, "game_fish": False}
                       for c, g in sorted(C.SALMON_FISH.items())},
            # THE PROTECTED FISH THE BOOK NAMES — not game fish, not in `fish` (2026-10-03)
            "protected": {c: {"name": n, "group": "PROTECTED_SPECIES", "game_fish": False}
                          for c, (n, _) in C.PROTECTED_FISH.items()},
            "protected_note": "The fish it is illegal to fish for or keep (p.9; Region 2 adds "
                              "green sturgeon, p.21) are named members of the PROTECTED_SPECIES "
                              "group, as chinook is of SALMON — not game fish, never on p.80's "
                              "list, and PROTECTED_SPECIES stays an open group that speaks for "
                              "no game fish. Each protected-species rule names the fish its row "
                              "prints in `fields.species` (zp:protected_species: twelve; "
                              "z2:protected_species: four). The white sturgeon member is the four "
                              "protected POPULATIONS (Nechako, Upper Fraser, Kootenay, Columbia), "
                              "not the game fish WSG.",
            "salmon_note": "Chinook is a named fish in the SALMON group — not a game fish and "
                           "not on the book's game-fish list (p.80); SALMON stays an open group "
                           "(it speaks for every salmon, named or not). The synopsis says little "
                           "about salmon ('record your retention of adult chinook salmon', the "
                           "salmon stamp, 'no spear fishing of Pacific salmon'): salmon "
                           "regulations will come later, with the DFO salmon implementation.",
            "refused": dict(sorted(C.REFUSED_SPECIES.items()))}


def field_dictionary(d: dict) -> dict:
    """Exactly the keys the records carry, each explained. Unexplained or unknown keys are
    reported by `problems`, never silently listed."""
    rule_keys = sorted({k for x in d["rules"].values() for k in x["fields"]})
    lic_keys = {k: sorted({f for x in d["licensing"].values() if x["kind"] == k
                           for f in x["fields"]}) for k in _licensing_models()}
    shipped = set(rule_keys)
    model = set(_fields(C.CatalogueRule)) - {"rule_id", "type", "verbatim"}
    return {
        "file": FILE_TEXT,
        "rule": {k: RECORD_TEXT[k] for k in RECORD_TEXT},
        "rule.fields": {k: RULE_FIELD_TEXT.get(k) for k in rule_keys},
        "rule.provenance": PROVENANCE_TEXT,
        "rule.fields.gear[]": {k: CLAUSE_TEXT.get(k) for k in _fields(C.GearClause)},
        "rule.fields.when": {k: WHEN_TEXT.get(k) for k in _fields(C.When)},
        "rule.fields.lengths[]": LENGTH_TEXT,
        "rule.fields.exempts[]": dict(LIFT_TEXT),
        "rule.fields.closed_to": {k: WHO_TEXT.get(k) for k in _fields(C.Who)},
        "rule.fields.extents[]": extent_text(),
        "rule.fields.extents[].op": dict(OP_TEXT),
        "model_rule_fields_not_shipped": sorted(model - shipped),
        "licensing": LICENSING_RECORD_TEXT,
        "licensing.fields": {k: {f: LICENSING_FIELD_TEXT.get(f) for f in fs}
                             for k, fs in lic_keys.items()},
        "licensing.provenance": {k: PROVENANCE_TEXT[k] for k in ("entry_name", "uncertain",
                                                                   "why")},
        "licensing.period": PERIOD_TEXT,
        "licensing.stamp_waiver": STAMP_WAIVER_TEXT,
        "entry": ENTRY_TEXT,
        "water": WATER_TEXT,
        "water.tidal": TIDAL_TEXT,
        "water.parts[]": WATER_PART_TEXT,
        "water.parts[].runs[]": RUN_TEXT,
        "run ends (from / to)": END_TEXT,
        "splits": SPLIT_TEXT,
        "rulesets{} / licensing_sets{}": SET_TEXT,
        "encoding": K.ENCODING_TEXT,
    }


#: Where `dictionary_gaps` looks: (what it is called in the report, the dictionary section that
#: must explain it, a function from the doc to the dicts whose KEYS are the fields).
_DICTIONARY_SCOPES = (
    ("top-level key", ("file",), lambda doc: [doc]),
    ("water field", ("water",), lambda doc: doc["waters"].values()),
    ("water.tidal field", ("water.tidal",),
     lambda doc: [w["tidal"] for w in doc["waters"].values() if "tidal" in w]),
    ("part field", ("water.parts[]",),
     lambda doc: [p for w in doc["waters"].values() for p in w["parts"]]),
    ("run field", ("water.parts[].runs[]",),
     lambda doc: [r for w in doc["waters"].values() for p in w["parts"] for r in p["runs"]]),
    ("rule key", ("rule",), lambda doc: doc["rules"].values()),
    ("rule field", ("rule.fields",), lambda doc: [x["fields"] for x in doc["rules"].values()]),
    ("rule provenance key", ("rule.provenance",),
     lambda doc: [x["provenance"] for x in doc["rules"].values()]),
    ("licensing key", ("licensing",), lambda doc: doc["licensing"].values()),
    ("licensing provenance key", ("licensing.provenance",),
     lambda doc: [x["provenance"] for x in doc["licensing"].values()]),
    ("licensing period key", ("licensing.period",),
     lambda doc: [x["period"] for x in doc["licensing"].values() if x.get("period")]),
    ("licensing stamp_waiver key", ("licensing.stamp_waiver",),
     lambda doc: [x["stamp_waiver"] for x in doc["licensing"].values() if x.get("stamp_waiver")]),
    ("entry field", ("entry",), lambda doc: doc["entries"].values()),
    ("set key", ("rulesets{} / licensing_sets{}",),
     lambda doc: list(doc["rulesets"].values()) + list(doc["licensing_sets"].values())),
    ("split field", ("splits",), lambda doc: doc["splits"].values()),
    ("extent field", ("rule.fields.extents[]",), lambda doc: _extents(doc)),
)


def _extents(doc: dict) -> list[dict]:
    """Every extent object the file carries: a rule's or licensing record's `extents` and
    `tributary_excludes`, an entry's `extents`."""
    out = []
    for x in list(doc["rules"].values()) + list(doc["licensing"].values()):
        for k in ("extents", "tributary_excludes"):
            out += [e for e in x["fields"].get(k) or [] if isinstance(e, dict)]
    for e in doc["entries"].values():
        out += [x for x in e.get("extents") or [] if isinstance(x, dict)]
    return out


def dictionary_gaps(doc: dict) -> list[str]:
    """EVERY FIELD IN THE DATA IS DESCRIBED. A top-level key, or a key of a water, a part, a run, a
    rule (its record, `fields`, `provenance`), a licensing record (its record, `fields` by kind,
    `provenance`, `period`), an entry, a set or a split that the file carries but `field_dictionary`
    does not explain in words (a missing or empty entry) is refused."""
    fd, out = doc["field_dictionary"], []
    for what, (section,), records in _DICTIONARY_SCOPES:
        words = fd.get(section) or {}
        seen = set()
        for r in records(doc):
            seen |= set(r)
        out += [f"field_dictionary.{section} does not explain the {what} {k!r}"
                for k in sorted(seen) if not words.get(k)]
    ops = fd.get("rule.fields.extents[].op") or {}
    out += [f"field_dictionary.rule.fields.extents[].op does not explain the op {o!r}"
            for o in sorted({str(e.get("op")) for e in _extents(doc)}) if not ops.get(o)]
    lic = fd.get("licensing.fields") or {}
    for i, x in doc["licensing"].items():
        for k in x["fields"]:
            if not (lic.get(x["kind"]) or {}).get(k):
                out.append(f"field_dictionary.licensing.fields.{x['kind']} does not explain "
                           f"the licensing field {k!r}")
    return sorted(set(out))


def index(d: dict) -> dict:
    by_type, by_family, by_kind = defaultdict(list), defaultdict(list), defaultdict(list)
    for x in d["rules"].values():
        by_type[x["type"]].append(x["id"])
        by_family[x["family"]].append(x["id"])
    for x in d["licensing"].values():
        by_kind[x["kind"]].append(x["id"])
    return {"rules_by_type": dict(sorted(by_type.items())),
            "rules_by_family": dict(sorted(by_family.items())),
            "licensing_by_kind": dict(sorted(by_kind.items()))}
