"""Export stage ③ — the flat rule dicts `corpus.rules()` hands the table layer.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.tools.export_ui_rules OUT.json

For someone designing the UI who has no pipeline to run: the real records for five chapters
and five waters, a field dictionary, the species-code and group maps a reader needs to make
sense of `species`, and — kept separate and labelled — what the table layer MAKES of those
rules. The split matters: a UI must draw the derived half, never re-derive it. Every
derivation that has existed twice in this project has drifted (see
`pipeline/docs/05-table-generation.md`, Part 9).

Change `REGIONS` / `WATERS` below to widen it. Nothing here is cached or committed; it is a
snapshot for a conversation, not an interface.
"""
import sys, json, collections
sys.path.insert(0, '.')
from pipeline.regs.table.corpus import rules, rid
from pipeline.regs.table import state as ST, display as D

REGIONS = ["province", "2", "3", "6", "8"]
WATERS = [("Chilliwack River", 0), ("Okanagan Lake", 0), ("Atlin Lake", 0),
          ("Shuswap Lake", 0), ("Fording River", 1)]
CHAPTERS = {"province": "zp", "2": "z2", "3": "z3", "6": "z6", "8": "z8"}

FIELDS = {
  "_note": "Every rule is a flat dict. 26 fields are on all 3,422 rules; the rest appear only "
           "where they mean something. Nothing downstream parses prose — sizes, dates, methods "
           "and documents are all fields.",
  "identity": {
    "entry": "the curated entry this rule came from, e.g. 'z2:trout_char_quota'",
    "rule": "the rule id INSIDE that entry, e.g. 'trout_char_quota.r4'",
    "entry_id / rule_id": "the same two, as the bundle spells them",
    "verbatim": "the sentence from the printed book. The one thing a reader can check.",
    "entry_name / entry_region": "the water or chapter the entry is about",
    "label": "a short human label for the rule",
  },
  "what it is about": {
    "species": "a list of species codes or GROUP codes (TROUT_CHAR, ALL_GAME_FISH, SALMON…)",
    "species_except": "species carved out of that list",
    "origin": "'wild' | 'hatchery' | absent (absent = both)",
    "water": "'stream' | 'lake' | absent (absent = either)",
    "method": "the fishing method a gear rule is about",
    "family / dimension / type": "how the rule was classified when curated",
  },
  "the number": {
    "take": "how many you may keep. 0 is a closure, a release, or a size floor.",
    "may_target": "0 where you may not even fish for it",
    "unlimited": "true where there is no number",
    "period": "'daily' (3,380) | 'possession' (34) | 'annual' (8) — which clock",
    "combined": "true where the number is SHARED across the species, not per species",
    "within": "the rule id this is a clause of — '1 over 50 cm' inside 'Trout/char: 4'",
    "per_daily": "a possession multiple: N times the daily number",
  },
  "size": {
    "over_cm": "the number counts (or the gate forbids) fish OVER this length",
    "under_cm": "…UNDER this length",
    "band": "true where the pair is a forbidden band rather than a slot",
    "_read": "take=2, no size -> keep 2 any size. take=1 over_cm=50 -> only 1 may be over 50 "
             "cm, a CAP on a size class. take=0 under_cm=30 -> none under 30 cm, a FLOOR. "
             "take=0 over_cm=50 -> a ceiling.",
  },
  "dates": {
    "windows": "list of {from:{month,day}, to:{month,day}} — the days it speaks about",
    "windows_are": "'applies' (3,419) | 'excepts' (3) — whether those are the days it holds, "
                   "or the days it does NOT",
    "when_open": "true (36) where it binds only while the water is open at all; no dates of "
                 "its own",
    "weekdays / from_time / to_time": "rules that never resolve to a date — the client applies "
                                      "them",
    "_read": "no window is not 'undated' — it is the standing answer, true on every day nothing "
             "seasonal speaks.",
  },
  "where": {
    "scope": "'region' | 'area' | 'water' | 'inherited' — WHAT IT BINDS TO",
    "authority": "'superior' | 'province' | 'region' — WHO WROTE IT",
    "extents": "the places it reaches, as area ids / area kinds",
    "extent_text": "prose extent nothing can draw. Becomes a caveat, not a counter.",
    "includes_tributaries / tributaries_only": "how far up it reaches",
    "_rank": "rank is DERIVED from (authority, scope), never stored. superior=-1, water=0, "
             "inherited=1, area=2, region+region=3, region+province=4. Smaller speaks first; "
             "scope beats authority.",
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
    "exempts": "what this rule lifts",
    "obligation / on_retention / required": "duties, e.g. 'must be released immediately'",
    "uncertain": "the curator was not sure",
    "reason": "why, where the book gives one",
    "aggregation_domain / when_targeting / min_gap_cm / max_gap_mm": "narrower qualifiers",
  },
}

def main(out):
    rs = list(rules())
    want_ch = set(CHAPTERS.values())
    by_region = collections.defaultdict(list)
    for x in rs:
        head = (x.get("entry") or "").split(":")[0]
        if head in want_ch:
            for reg, ch in CHAPTERS.items():
                if ch == head:
                    by_region[reg].append(x)

    water_rules = {}
    for water, run in WATERS:
        got = ST.rules_for(kind=None or ST.section_kind(water), water=water, run=run)[0] \
            if hasattr(ST, "section_kind") else None
        water_rules[f"{water} (run {run})"] = got

    tables = {}
    for reg in REGIONS:
        for kind in ("lake", "stream"):
            r_, here, label = ST.rules_for(reg, kind)
            t = ST.build(r_, kind, here, label, province=reg == "province")
            tables[f"{reg} · {kind}"] = D.table(t.ledger, kind, None, label)["merged"]

    from pipeline.regs.table.build import name as fish_name, D as SECTIONS
    from pipeline.regs.parsing.catalogue import SPECIES_GROUPS, DEFINITIONAL_SIZE
    from pipeline.regs.table.subject import expand
    codes = sorted({c for g in SPECIES_GROUPS for c in expand(frozenset({g}))})

    doc = {
        "_what_this_is":
            "Stage ③ of pipeline/docs/05-table-generation.md — the flat rule dicts "
            "`corpus.rules()` hands the table layer, for five chapters and five waters, plus "
            "what the table layer makes of them. Real records, unedited.",
        "_the_stages":
            "curated entry → bundle → RULE (this file) → allowance → ledger → row → table",
        "_counts": {"rules in the whole corpus": len(rs),
                    "chapters exported": len(by_region),
                    "rules exported": sum(len(v) for v in by_region.values())},
        "field_dictionary": FIELDS,
        "species_codes": {c: fish_name(c) for c in codes},
        "species_groups": {
            g: sorted(expand(frozenset({g}))) for g in sorted(SPECIES_GROUPS)
            if expand(frozenset({g}))},
        "definitional_sizes": {
            "_note": "A steelhead IS a rainbow trout over 50 cm. So a cap of '1 over 50 cm' on a "
                     "steelhead row is simply 1, and a regional floor of 30 cm can never bite on "
                     "one. The table applies this on a row already labelled steelhead; deciding "
                     "whether a RAINBOW is a steelhead needs a per-water fact and is NOT applied.",
            **DEFINITIONAL_SIZE},
        "rules_by_chapter": {
            f"{reg} ({CHAPTERS[reg]})": sorted(v, key=rid) for reg, v in by_region.items()},
        "_water_note":
            "Below is every rule the atlas binds to that one stretch — which INCLUDES the "
            "provincial and regional ones above, because a section gets them all. The rules "
            "written for the water itself are the ones whose `scope` is 'water' or 'inherited'. "
            "'inherited' means it reached this water from another one by the tributary walk.",
        "rules_for_one_water": water_rules,
        "_derived_note":
            "Everything below is DERIVED from the rules above by the table layer — it is here "
            "so it is clear which parts of a screen are data and which are computed. A UI must "
            "never re-derive it; see 05-table-generation.md Part 5.",
        "settled_tables": tables,
    }
    with open(out, "w") as fh:
        json.dump(doc, fh, indent=1, ensure_ascii=False)
    import os
    print(f"wrote {out} ({os.path.getsize(out)/1e6:.2f} MB)")
    print("  chapters:", ", ".join(f"{k} ({len(v)})" for k, v in by_region.items()))
    print("  waters  :", ", ".join(f"{k} ({len(v or [])})" for k, v in water_rules.items()))
    print("  tables  :", len(tables))

if __name__ == "__main__":
    main(sys.argv[1])
