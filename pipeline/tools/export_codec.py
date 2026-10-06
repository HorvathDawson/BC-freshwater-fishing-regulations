"""THE UI EXPORT'S WIRE FORMAT, and its reference decoder.

`export_ui_rules.build()` reads the bundle into ONE reading model — the shape every check in
`export_ui_rules.problems` proves and every text in `guide` / `field_dictionary` describes. What
ships is that model ENCODED into two files, written by the same builder from the same bundle and
stamped with the same digests (`about.bundle`):

    ui-rules-export.json   the data a client reads at run time (format `FORMAT`)
    ui-rules-guide.json    `guide` (with `cases`), `field_dictionary` and `species`: how to read it

`compact(doc)` encodes; `expand(data, guide)` is the REFERENCE DECODER and returns exactly the
model `compact` was given (`export_ui_rules.main` refuses to write a pair that does not decode to
it). The encoding loses nothing: every byte it drops is a default, a key, or a value derived from
what it keeps, each listed in `ENCODING_TEXT`, which `field_dictionary["encoding"]` ships — so a
client in another language can decode by reading the dictionary alone.

WHAT CHANGES ON THE WIRE (Phase 4, `FREV/export.md` C0..C-G):
  · no indent (C0; `--pretty` indents for a human);
  · `rules` / `licensing` are ARRAYS; their string ids are the parallel `rule_ids` /
    `licensing_ids` (C-A);
  · a rule set's members are INTEGER indexes into `rules`; its zone/province members are one
    interned `bases[i]` (C-A2); a licensing set's members index `licensing`;
  · a rule drops `id`/`entry_id`/`rule_id` (the parallel id), `family` (`codec.family_of_type`),
    `provenance.entry_name` (its entry's `name`), `provenance.rank` (`codec.rank`) and
    `provenance.who` where `who_default` says it (C-C) — `label` and `verbatim` stay;
  · an entry drops `chapter` (its id's prefix), `rules` / `licensing` (the ids that start with
    `<entry_id>::` / `<entry_id>#`) and its empty values (C-D);
  · a water drops `sections` (its parts' sum), `steelhead` (its parts' roll-up), its empty
    values, and `steelhead_source` where it is just `["regulations"]` beside `steelhead_rows`; a
    part is a positional array, a run a positional array, and a part's one source-to-mouth run
    is its length alone (C-B);
  · `guide`, `field_dictionary`, `species` move to the guide file (C-E); `index` is dropped —
    the decoder groups `rules` by type and family and `licensing` by kind (C-F);
  · a split drops `water_id: null, km: null` (C-G).
"""
from __future__ import annotations

import hashlib
import json
from collections import defaultdict

FORMAT = 2
GUIDE_KEYS = ("guide", "field_dictionary", "species")

#: Entry fields dropped when they hold this value.
ENTRY_DEFAULTS = {"full_name": None, "item_id": None, "matched": [], "mus": [], "pages": [],
                  "symbols": [], "scope_note": None, "extents": [], "printed": None}
#: Rule provenance fields dropped when they hold this value.
PROVENANCE_DEFAULTS = {"uncertain": False, "why": None}
#: Water fields dropped when they hold this value.
WATER_DEFAULTS = {"entries": [], "outside_bc": 0}
#: The part fields that travel in a part's fifth slot, each dropped at its default.
PART_FLAGS = ("province_except", "anadromous_rainbow", "steelhead", "steelhead_rules",
              "home_region", "touches")
PART_FLAG_DEFAULTS = {"touches": []}
#: The run fields that travel in a run's four slots; anything else goes in its fifth.
RUN_SLOTS = ("from", "to", "km_from", "km_to")
#: The one run a part may be reduced to its length.
TRIVIAL_RUN = {"from": "source", "to": "mouth", "km_to": 0.0}
POLYGON = "polygon"


# --------------------------------------------------------------------------------------------
# A set's content key — the one recipe (CLEAN round, 2026-10-05)
# --------------------------------------------------------------------------------------------
#
# A SET ID IS LOCAL TO ONE BUNDLE. The bundle numbers its interned rule and licensing sets as it
# builds them, so set "17" of one export is not set "17" of the next. A consumer matching parts
# across exports needs a name that depends on what the set HOLDS: the content key, a short hash
# of its members WITH THEIR `via` — two sets with the same rule ids reached differently (`reach`
# vs `trib`: a water rule speaks at the `inherited` rung on a walked section) answer differently,
# and 242 of 2,099 rule sets would collide on the ids alone (0 with the `via`). The key is
# DERIVED, so it never travels on the wire (format 2 drops what a decoder can compute): `expand`
# computes it, `compact` refuses a model whose keys are not this recipe's.

#: Hex digits kept of the SHA-256 (48 bits: no collision among ~2,300 sets, measured).
SET_KEY_HEX = 12


def set_key(s: dict) -> str:
    """A decoded set's content key: the first `SET_KEY_HEX` hex digits of the SHA-256 of its
    members as UTF-8 lines `<via>:<member id>` (`reach:z3:trout_char_quota::trout_char_quota.r1`),
    sorted, joined by a single "\\n" — `sections` excluded (a count, not a member)."""
    lines = sorted(f"{via}:{i}" for via, ids in s.items() if via != "sections" for i in ids)
    return hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()[:SET_KEY_HEX]


def set_keys(doc: dict) -> dict:
    """`{"rulesets": {set id: key}, "licensing_sets": {set id: key}}` for a decoded model."""
    return {t: {sid: set_key(s) for sid, s in doc[t].items()}
            for t in ("rulesets", "licensing_sets")}


# --------------------------------------------------------------------------------------------
# What a reader needs to decode, in words (shipped as `field_dictionary["encoding"]`)
# --------------------------------------------------------------------------------------------

def _defaults(d: dict) -> str:
    return ", ".join(f"`{k}`: {json.dumps(v)}" for k, v in d.items())


ENCODING_TEXT = {
    "files": "The export is TWO files written together from one bundle: `ui-rules-export.json` "
             "(the data, read at run time) and `ui-rules-guide.json` (`guide`, "
             "`field_dictionary`, `species`). Both carry the same `about.bundle` (digests "
             "`section_handles`, `reach_digest`); never pair files whose digests differ. The "
             "rest of this dictionary describes the DECODED records; this section says how the "
             "data file encodes them. `pipeline/tools/export_codec.py` `expand(data, guide)` is "
             "the reference decoder",
    "data file keys": {
        "about": "what the file is, its bundle, counts, unresolved references, and `format` "
                 "(" + str(FORMAT) + ") — the wire's own: the decoded `about` is this WITHOUT "
                 "`format`",
        "codec": "the small tables decoding needs: `family_of_type` {rule type: family}, `rank` "
                 "{\"<authority>/<binds_to>\": rank}",
        "licences": "as decoded (`licences`)",
        "entries": "{entry_id: entry} — an entry WITHOUT `chapter` (the id before its first "
                   "':'), `rules` (every `rule_ids[i]` starting `<entry_id>::`, in order), "
                   "`licensing` (every `licensing_ids[i]` starting `<entry_id>#`, in order); "
                   "a field holding its default is absent: " + _defaults(ENTRY_DEFAULTS),
        "rule_ids": "the string id `entry_id::rule_id` of `rules[i]` — the parallel list; an "
                    "integer member of a rule set is an index into both",
        "rules": "[rule], in `rule_ids` order (sorted by entry_id, then rule_id). A rule WITHOUT "
                 "`id` (= rule_ids[i]), `entry_id` / `rule_id` (its id split at the first '::'), "
                 "`family` (`codec.family_of_type[type]`), `provenance.entry_name` (its entry's "
                 "`name`, \"\" when none), `provenance.rank` (`codec.rank[authority/binds_to]`), "
                 "and `provenance.who` when it is `who_default`; provenance fields at their "
                 "default are absent: " + _defaults(PROVENANCE_DEFAULTS),
        "licensing_ids": "the string id `entry_id#record_id` of `licensing[i]`, sorted",
        "licensing": "[licensing record], in `licensing_ids` order: WITHOUT `id`, `entry_id`, "
                     "`record_id` (from the id, split at the first '#') and "
                     "`provenance.entry_name`; provenance at its default is absent: "
                     + _defaults(PROVENANCE_DEFAULTS),
        "bases": "[[rule index]] — the zone and province members (`z…` entries) shared by many "
                 "rule sets, interned: a rule set's `base` indexes this list",
        "rulesets": "[rule set], indexed by set id (a part's `ruleset`). `{sections, base?, "
                    "reach?, <via>?…}`: decoded `reach` is `bases[base]` plus its own `reach` "
                    "indexes, merged in ascending order (= the rules' order); every other `via` "
                    "(`trib`, `trib_pending`, `contested` — `guide.placement`) is its indexes "
                    "as listed. A `via` with no members is absent. DECODED, the sets are an "
                    "object keyed by the set id as a STRING (\"0\", \"1\", …), and a part's "
                    "`ruleset` / `licensing_set` is that string (or null)",
        "licensing_sets": "[licensing set], indexed by set id: `{sections, <via>?…}` (`reach`, "
                          "`trib`, `trib_pending`, `contested`), each member an index into "
                          "`licensing`, in the listed order; decoded keyed by the id as a "
                          "string, like `rulesets`",
        "waters": "{item_id: water} — a water WITHOUT `sections` (the sum of its parts'), "
                  "`steelhead` (\"known\" if any part is, else \"possible\" if any part is, else "
                  "absent), and `steelhead_source` when it is [\"regulations\"] beside "
                  "`steelhead_rows`; a field at its default is absent: "
                  + _defaults(WATER_DEFAULTS) + ". `parts` is a list of PART ARRAYS",
        "splits": "{split id: split} — `water_id` and `km` are absent where null",
    },
    "part array": "[ruleset, licensing_set, sections, runs, flags?] — `ruleset` / "
                  "`licensing_set` an integer set id or null; `sections` the count; `runs` see "
                  "`runs slot`; `flags` (absent when empty) an object holding any of "
                  + ", ".join(f"`{k}`" for k in PART_FLAGS) + " (`touches` absent when [])",
    "runs slot": "a NUMBER L: one run {from: \"source\", to: \"mouth\", km_from: L, km_to: 0.0} "
                 "| \"polygon\": one polygon run {from, to, km_from, km_to all null, polygon: the "
                 "water's `name` on a lake part (`part_of`), \"whole\" otherwise} | a list of RUN "
                 "ARRAYS",
    "run array": "[from, to, km_from, km_to, extra?] — `extra` (absent when empty) holds the "
                 "run's other fields (`branch`, `polygon`)",
    "who_default": "\"<wrote> · <where>\": wrote = \"Federal or Parks\" (authority superior), "
                   "\"Provincial\" (province), else \"Region \" + the entry id's region (its "
                   "prefix after z/r, upper case); where = \"province-wide\" / \"region-wide\" "
                   "(binds_to region, by authority), \"for this water (<entry_name>)\" (water; "
                   "\"for this water\" when the name is empty), <entry_name> (area, on a water "
                   "row `r…` whose entry has a non-empty name). Any other `who` — and every "
                   "`who` no rule above produces — is shipped as is, so a decoder applies this "
                   "only where `who` is absent",
    "guide file keys": {
        "about": "`what`, `bundle` (the same digests as the data file), `format`",
        "guide": "how to read everything — see `guide.contents`",
        "field_dictionary": "this",
        "species": "the book's species list (p.80)",
    },
    "index": "not shipped: `rules_by_type` / `rules_by_family` are `rule_ids` grouped by "
             "`type` / family, `licensing_by_kind` is `licensing_ids` grouped by `kind`, each "
             "sorted by key, members in array order",
    "set_keys": "not shipped: `set_keys.rulesets[set id]` / `set_keys.licensing_sets[set id]` "
                "is the first " + str(SET_KEY_HEX) + " hex digits of the SHA-256 of the DECODED "
                "set's members as UTF-8 lines `<via>:<member id>` (e.g. "
                "`reach:z3:trout_char_quota::trout_char_quota.r1`), sorted (code-point order), joined by one \"\\n\" "
                "(no trailing newline); `sections` is not a member",
}


# --------------------------------------------------------------------------------------------
# Encoding
# --------------------------------------------------------------------------------------------

def _region_of(entry_id: str) -> str:
    head = entry_id.split(":", 1)[0]
    return head[1:] if head[:1] in ("z", "r") else ""


def who_default(entry_id: str, authority: str, binds_to: str, entry_name: str) -> str | None:
    """`provenance.who` as `read.Source.words()` says it for the common shapes (see
    `ENCODING_TEXT["who_default"]`); None where only the shipped value can say it."""
    wrote = ("Federal or Parks" if authority == "superior" else "Provincial"
             if authority == "province" else "Region " + _region_of(entry_id).upper())
    if binds_to == "region":
        where = "province-wide" if authority == "province" else "region-wide"
    elif binds_to == "water":
        where = "for this water" + (f" ({entry_name})" if entry_name else "")
    elif binds_to == "area" and entry_id.startswith("r") and entry_name:
        where = entry_name
    else:
        return None
    return f"{wrote} · {where}"


class EncodeError(ValueError):
    pass


def _split_id(i: str, sep: str) -> tuple[str, str]:
    e, s, r = i.partition(sep)
    if not s:
        raise EncodeError(f"id {i!r} has no {sep!r}")
    return e, r


def _drop(d: dict, defaults: dict) -> dict:
    return {k: v for k, v in d.items() if not (k in defaults and v == defaults[k])}


def _fill(d: dict, defaults: dict) -> dict:
    return {**{k: json.loads(json.dumps(v)) for k, v in defaults.items()}, **d}


def _runs_out(w: dict, runs: list[dict]):
    if len(runs) == 1:
        r = runs[0]
        if set(r) == {"from", "to", "km_from", "km_to"} and all(
                r[k] == v and type(r[k]) is type(v) for k, v in TRIVIAL_RUN.items()) \
                and isinstance(r["km_from"], (int, float)):
            return r["km_from"]
        poly = w["name"] if w.get("part_of") else "whole"
        if r == {"from": None, "to": None, "km_from": None, "km_to": None, "polygon": poly}:
            return POLYGON
    out = []
    for r in runs:
        extra = {k: v for k, v in r.items() if k not in RUN_SLOTS}
        missing = [k for k in RUN_SLOTS if k not in r]
        if missing:
            raise EncodeError(f"a run without {missing}: {r}")
        out.append([r[k] for k in RUN_SLOTS] + ([extra] if extra else []))
    return out


def _runs_in(w: dict, slot) -> list[dict]:
    if slot == POLYGON:
        return [{"from": None, "to": None, "km_from": None, "km_to": None,
                 "polygon": w["name"] if w.get("part_of") else "whole"}]
    if isinstance(slot, (int, float)) and not isinstance(slot, bool):
        return [{"from": "source", "to": "mouth", "km_from": slot, "km_to": 0.0}]
    return [{**dict(zip(RUN_SLOTS, r[:4])), **(r[4] if len(r) > 4 else {})} for r in slot]


def _set_int(s):
    return None if s is None else int(s)


def compact(doc: dict, *, family_of_type: dict, rank: dict) -> tuple[dict, dict]:
    """The reading model -> (data, guide) as shipped. Refuses (EncodeError) anything it could
    not decode back to the same model."""
    rule_ids, lic_ids = list(doc["rules"]), list(doc["licensing"])
    if doc.get("set_keys") != set_keys(doc):
        raise EncodeError("set_keys are not the sets' content keys (`set_key`) — the decoder "
                          "recomputes them, so they cannot travel otherwise")
    if lic_ids != sorted(lic_ids):
        raise EncodeError("licensing ids are not sorted")
    rix = {k: i for i, k in enumerate(rule_ids)}
    lix = {k: i for i, k in enumerate(lic_ids)}
    entries = doc["entries"]
    for eid in entries:
        if "::" in eid or "#" in eid:
            raise EncodeError(f"entry id {eid!r} holds '::' or '#'")

    rules = []
    for k, x in doc["rules"].items():
        eid, rid = _split_id(k, "::")
        if (x["id"], x["entry_id"], x["rule_id"]) != (k, eid, rid):
            raise EncodeError(f"rule {k}: id fields disagree with the key")
        if family_of_type.get(x["type"]) != x["family"]:
            raise EncodeError(f"rule {k}: family {x['family']} is not "
                              f"codec.family_of_type[{x['type']}]")
        p = dict(x["provenance"])
        if p.pop("entry_name") != (entries[eid]["name"] or ""):
            raise EncodeError(f"rule {k}: provenance.entry_name is not its entry's name")
        if p.pop("rank") != rank.get(f"{p['authority']}/{p['binds_to']}"):
            raise EncodeError(f"rule {k}: rank is not codec.rank")
        if p["who"] == who_default(eid, p["authority"], p["binds_to"],
                                   entries[eid]["name"] or ""):
            p.pop("who")
        y = {a: b for a, b in x.items() if a not in ("id", "entry_id", "rule_id", "family")}
        y["provenance"] = _drop(p, PROVENANCE_DEFAULTS)
        rules.append(y)

    licensing = []
    for k, x in doc["licensing"].items():
        eid, rid = _split_id(k, "#")
        if (x["id"], x["entry_id"], x["record_id"]) != (k, eid, rid):
            raise EncodeError(f"licensing {k}: id fields disagree with the key")
        p = dict(x["provenance"])
        if p.pop("entry_name") != (entries[eid]["name"] or ""):
            raise EncodeError(f"licensing {k}: provenance.entry_name is not its entry's name")
        y = {a: b for a, b in x.items() if a not in ("id", "entry_id", "record_id")}
        y["provenance"] = _drop(p, PROVENANCE_DEFAULTS)
        licensing.append(y)

    # entries: the derivable lists and the prefix go; defaults go
    by_entry_r, by_entry_l = defaultdict(list), defaultdict(list)
    for k in rule_ids:
        by_entry_r[_split_id(k, "::")[0]].append(k)
    for k in lic_ids:
        by_entry_l[_split_id(k, "#")[0]].append(k)
    out_entries = {}
    for eid, e in entries.items():
        if e["chapter"] != eid.split(":", 1)[0] or e["rules"] != by_entry_r.get(eid, []) \
                or e["licensing"] != by_entry_l.get(eid, []):
            raise EncodeError(f"entry {eid}: chapter/rules/licensing are not derivable")
        out_entries[eid] = _drop({a: b for a, b in e.items()
                                  if a not in ("chapter", "rules", "licensing")}, ENTRY_DEFAULTS)

    # sets: integers, and the rule sets' zone/province base interned
    def base_of(ids):
        return tuple(rix[i] for i in ids if i.startswith("z"))
    rulesets_in = doc["rulesets"]
    if list(rulesets_in) != [str(i) for i in range(len(rulesets_in))]:
        raise EncodeError("rule set ids are not 0..n-1")
    bases: dict[tuple, int] = {}
    for s in rulesets_in.values():
        b = base_of(s.get("reach") or [])
        if b and b not in bases:
            bases[b] = len(bases)
    rulesets = []
    for sid, s in rulesets_in.items():
        y: dict = {"sections": s["sections"]}
        for via, ids in s.items():
            if via == "sections":
                continue
            ints = [rix[i] for i in ids]
            if via == "reach":
                if ints != sorted(set(ints)):
                    raise EncodeError(f"ruleset {sid}: reach is not in rule order")
                b = base_of(ids)
                if b:
                    y["base"] = bases[b]
                own = [i for i in ints if i not in set(b)]
                if own:
                    y["reach"] = own
                if not ints:
                    raise EncodeError(f"ruleset {sid}: an empty reach")
            else:
                if not ints:
                    raise EncodeError(f"ruleset {sid}: an empty {via}")
                y[via] = ints
        rulesets.append(y)
    lsets_in = doc["licensing_sets"]
    if list(lsets_in) != [str(i) for i in range(len(lsets_in))]:
        raise EncodeError("licensing set ids are not 0..n-1")
    licensing_sets = [{via: (v if via == "sections" else [lix[i] for i in v])
                       for via, v in s.items()} for s in lsets_in.values()]

    waters = {}
    for item, w in doc["waters"].items():
        parts = w["parts"]
        if w["sections"] != sum(p["sections"] for p in parts):
            raise EncodeError(f"water {item}: sections is not its parts' sum")
        if w.get("steelhead") != _rollup(parts):
            raise EncodeError(f"water {item}: steelhead is not its parts' roll-up")
        y = {a: b for a, b in w.items() if a not in ("sections", "steelhead", "parts")}
        if y.get("steelhead_source") == ["regulations"] and y.get("steelhead_rows"):
            y.pop("steelhead_source")
        elif "steelhead_source" not in y and y.get("steelhead_rows"):
            raise EncodeError(f"water {item}: steelhead_rows without steelhead_source")
        y = _drop(y, WATER_DEFAULTS)
        y["parts"] = []
        for p in parts:
            extra = set(p) - {"ruleset", "licensing_set", "sections", "runs", *PART_FLAGS}
            if extra:
                raise EncodeError(f"water {item}: a part field the codec does not know: {extra}")
            flags = _drop({k: p[k] for k in PART_FLAGS if k in p}, PART_FLAG_DEFAULTS)
            if "touches" not in p:
                raise EncodeError(f"water {item}: a part without touches")
            y["parts"].append([_set_int(p["ruleset"]), _set_int(p["licensing_set"]),
                               p["sections"], _runs_out(w, p["runs"])] + ([flags] if flags else []))
        waters[item] = y

    splits = {k: {a: b for a, b in s.items() if not (a in ("water_id", "km") and b is None)}
              for k, s in doc["splits"].items()}

    about = {**doc["about"], "format": FORMAT}
    data = {
        "about": about,
        "codec": {"family_of_type": dict(sorted(family_of_type.items())),
                  "rank": dict(sorted(rank.items()))},
        "licences": doc["licences"],
        "entries": out_entries,
        "rule_ids": rule_ids, "rules": rules,
        "licensing_ids": lic_ids, "licensing": licensing,
        "bases": [list(b) for b in bases],
        "rulesets": rulesets, "licensing_sets": licensing_sets,
        "waters": waters, "splits": splits,
    }
    guide = {"about": {"what": "How to read ui-rules-export.json: the guide, the field "
                               "dictionary and the species list. Written with it, from the same "
                               "bundle, by pipeline/tools/export_ui_rules.py.",
                       "bundle": doc["about"]["bundle"], "format": FORMAT},
             **{k: doc[k] for k in GUIDE_KEYS}}
    return data, guide


def _rollup(parts: list[dict]):
    got = {p.get("steelhead") for p in parts}
    return "known" if "known" in got else "possible" if "possible" in got else None


# --------------------------------------------------------------------------------------------
# The reference decoder
# --------------------------------------------------------------------------------------------

def expand(data: dict, guide: dict) -> dict:
    """(data, guide) as shipped -> the reading model, exactly as `build()` made it."""
    if data["about"].get("format") != FORMAT or guide["about"].get("format") != FORMAT:
        raise ValueError(f"export format is not {FORMAT}")
    if data["about"]["bundle"] != guide["about"]["bundle"]:
        raise ValueError("the data and guide files come from different bundles")
    fam, rank = data["codec"]["family_of_type"], data["codec"]["rank"]
    E = data["entries"]
    name = {eid: (e.get("name") or "") for eid, e in E.items()}

    rules = {}
    for k, x in zip(data["rule_ids"], data["rules"]):
        eid, rid = _split_id(k, "::")
        p = _fill(x["provenance"], PROVENANCE_DEFAULTS)
        who = p.pop("who", None)
        if who is None:
            who = who_default(eid, p["authority"], p["binds_to"], name[eid])
        prov = {"entry_name": name[eid], "authority": p["authority"], "binds_to": p["binds_to"],
                "rank": rank[f"{p['authority']}/{p['binds_to']}"], "who": who,
                **{a: b for a, b in p.items() if a not in ("authority", "binds_to")}}
        rules[k] = {"id": k, "entry_id": eid, "rule_id": rid, "type": x["type"],
                    "family": fam[x["type"]],
                    **{a: b for a, b in x.items() if a not in ("type", "provenance")},
                    "provenance": prov}
    licensing = {}
    for k, x in zip(data["licensing_ids"], data["licensing"]):
        eid, rid = _split_id(k, "#")
        p = _fill(x["provenance"], PROVENANCE_DEFAULTS)
        licensing[k] = {"id": k, "entry_id": eid, "record_id": rid,
                        **{a: b for a, b in x.items() if a != "provenance"},
                        "provenance": {"entry_name": name[eid], **p}}

    by_r, by_l = defaultdict(list), defaultdict(list)
    for k in data["rule_ids"]:
        by_r[_split_id(k, "::")[0]].append(k)
    for k in data["licensing_ids"]:
        by_l[_split_id(k, "#")[0]].append(k)
    entries = {}
    for eid, e in E.items():
        entries[eid] = {"chapter": eid.split(":", 1)[0], **_fill(e, ENTRY_DEFAULTS),
                        "rules": by_r.get(eid, []), "licensing": by_l.get(eid, [])}

    R, L, B = data["rule_ids"], data["licensing_ids"], data["bases"]
    rulesets = {}
    for i, s in enumerate(data["rulesets"]):
        y: dict = {"sections": s["sections"]}
        if "base" in s or "reach" in s:
            ints = sorted((B[s["base"]] if "base" in s else []) + (s.get("reach") or []))
            y["reach"] = [R[j] for j in ints]
        if "trib" in s:
            y["trib"] = [R[j] for j in s["trib"]]
        for via in s:
            if via not in ("sections", "base", "reach", "trib"):
                y[via] = [R[j] for j in s[via]]
        rulesets[str(i)] = y
    licensing_sets = {str(i): {via: (v if via == "sections" else [L[j] for j in v])
                               for via, v in s.items()}
                      for i, s in enumerate(data["licensing_sets"])}

    waters = {}
    for item, w in data["waters"].items():
        parts = []
        for arr in w["parts"]:
            rs, ls, n, runs = arr[:4]
            flags = _fill(arr[4] if len(arr) > 4 else {}, PART_FLAG_DEFAULTS)
            parts.append({"ruleset": None if rs is None else str(rs),
                          "licensing_set": None if ls is None else str(ls), "sections": n,
                          **flags, "runs": _runs_in(w, runs)})
        y = {**_fill({a: b for a, b in w.items() if a != "parts"}, WATER_DEFAULTS),
             "sections": sum(p["sections"] for p in parts), "parts": parts}
        top = _rollup(parts)
        if top:
            y["steelhead"] = top
        if "steelhead_source" not in y and y.get("steelhead_rows"):
            y["steelhead_source"] = ["regulations"]
        waters[item] = y

    splits = {k: {"water_id": None, "km": None, **s} for k, s in data["splits"].items()}
    about = {a: b for a, b in data["about"].items() if a != "format"}
    doc = {"about": about, **{k: guide[k] for k in GUIDE_KEYS},
           "licences": data["licences"], "entries": entries, "rules": rules,
           "licensing": licensing, "rulesets": rulesets, "licensing_sets": licensing_sets,
           "waters": waters, "splits": splits}
    doc["set_keys"] = set_keys(doc)
    doc["index"] = index_of(rules, licensing)
    return doc


def index_of(rules: dict, licensing: dict) -> dict:
    """`index` as the model holds it: rule ids by type and by family, licensing ids by kind."""
    by_type, by_family, by_kind = defaultdict(list), defaultdict(list), defaultdict(list)
    for k, x in rules.items():
        by_type[x["type"]].append(k)
        by_family[x["family"]].append(k)
    for k, x in licensing.items():
        by_kind[x["kind"]].append(k)
    return {"rules_by_type": dict(sorted(by_type.items())),
            "rules_by_family": dict(sorted(by_family.items())),
            "licensing_by_kind": dict(sorted(by_kind.items()))}


# --------------------------------------------------------------------------------------------
# The checks on the WIRE: integer references, the pairing, every key described
# --------------------------------------------------------------------------------------------

def _is_int(v) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def wire_problems(data: dict, guide: dict) -> list[str]:
    """Every integer reference that does not resolve, every parallel list out of step, every
    key of the data file the dictionary does not describe. `export_ui_rules.problems` then runs
    on `expand(data, guide)` for every string reference."""
    out = []
    nR, nL, nB = len(data["rules"]), len(data["licensing"]), len(data["bases"])
    nS, nLS = len(data["rulesets"]), len(data["licensing_sets"])
    if len(data["rule_ids"]) != nR:
        out.append(f"rule_ids has {len(data['rule_ids'])} ids for {nR} rules")
    if len(data["licensing_ids"]) != nL:
        out.append(f"licensing_ids has {len(data['licensing_ids'])} ids for {nL} records")
    for name, ids in (("rule_ids", data["rule_ids"]), ("licensing_ids", data["licensing_ids"])):
        if len(set(ids)) != len(ids):
            out.append(f"{name} repeats an id")
    if data["about"].get("bundle") != guide.get("about", {}).get("bundle"):
        out.append("the guide file's about.bundle is not the data file's")
    if data["about"].get("format") != FORMAT or guide.get("about", {}).get("format") != FORMAT:
        out.append(f"a file's about.format is not {FORMAT}")

    def ref(where, v, n, table):
        if not (_is_int(v) and 0 <= v < n):
            out.append(f"{where} -> {table}[{v!r}] does not resolve")

    for bi, b in enumerate(data["bases"]):
        for j in b:
            ref(f"bases[{bi}]", j, nR, "rules")
        if list(b) != sorted(set(b)):
            out.append(f"bases[{bi}] is not in rule order")
    for si, s in enumerate(data["rulesets"]):
        if "base" in s:
            ref(f"rulesets[{si}].base", s["base"], nB, "bases")
        for via, v in s.items():
            if via in ("sections", "base"):
                continue
            for j in v:
                ref(f"rulesets[{si}].{via}", j, nR, "rules")
        if "base" in s and _is_int(s["base"]) and 0 <= s["base"] < nB:
            if set(data["bases"][s["base"]]) & set(s.get("reach") or []):
                out.append(f"rulesets[{si}] lists a base member again")
    for si, s in enumerate(data["licensing_sets"]):
        for via, v in s.items():
            if via != "sections":
                for j in v:
                    ref(f"licensing_sets[{si}].{via}", j, nL, "licensing")
    fam = data["codec"]["family_of_type"]
    rank = data["codec"]["rank"]
    for i, x in enumerate(data["rules"]):
        if x.get("type") not in fam:
            out.append(f"rules[{i}] type {x.get('type')!r} has no codec.family_of_type")
        p = x.get("provenance") or {}
        if f"{p.get('authority')}/{p.get('binds_to')}" not in rank:
            out.append(f"rules[{i}] has no codec.rank for {p.get('authority')}/"
                       f"{p.get('binds_to')}")
    for item, w in data["waters"].items():
        for pi, arr in enumerate(w["parts"]):
            if not isinstance(arr, list) or len(arr) not in (4, 5):
                out.append(f"water {item} part {pi} is not a part array")
                continue
            if arr[0] is not None:
                ref(f"water {item} part {pi} ruleset", arr[0], nS, "rulesets")
            if arr[1] is not None:
                ref(f"water {item} part {pi} licensing_set", arr[1], nLS, "licensing_sets")
            runs = arr[3]
            if not (runs == POLYGON or (_is_int(runs) or isinstance(runs, float))
                    or (isinstance(runs, list) and all(isinstance(r, list) and len(r) in (4, 5)
                                                       for r in runs))):
                out.append(f"water {item} part {pi} runs slot is not a length, polygon or runs")
    out += encoding_gaps(data, guide)
    return out


def encoding_gaps(data: dict, guide: dict) -> list[str]:
    """Every top-level key of either file must be described by `field_dictionary.encoding`."""
    enc = (guide.get("field_dictionary") or {}).get("encoding") or {}
    out = [f"field_dictionary.encoding does not describe the data file key {k!r}"
           for k in data if not (enc.get("data file keys") or {}).get(k)]
    out += [f"field_dictionary.encoding does not describe the guide file key {k!r}"
            for k in guide if not (enc.get("guide file keys") or {}).get(k)]
    return out


def dumps(obj: dict, pretty: bool = False) -> str:
    if pretty:
        return json.dumps(obj, indent=1, ensure_ascii=False) + "\n"
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n"
