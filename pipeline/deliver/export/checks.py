"""`problems`: every check the export must pass before the pair is written.

A non-empty list refuses the export (`__main__.main`). Each check proves a property of the decoded model.
"""
from __future__ import annotations

import typing
from collections import defaultdict
from pipeline.deliver.bundle import spans as SP
from pipeline.deliver.bundle.rules import LIFT_KEYS
from pipeline.regs.parsing import catalogue as C
from pipeline.regs.parsing.entry_models import Op
from pipeline.deliver.export.contract import RETIRED_ANYWHERE, RETIRED_ON_RULE, RETIRED_TYPES
from pipeline.deliver.export.read import (
    _split_refs, paper_licence_problems, protected_problems, record_link_problems, undrawn_map_problems
)
from pipeline.deliver.export.guide_words import (
    CLAUSE_TEXT, DOING_TEXT, FAMILY_TEXT, GEAR_SPEC_TEXT, GEAR_WHEN_TEXT, LENGTH_TEXT, LICENSING_FIELD_TEXT,
    LICENSING_KIND_TEXT, LICENSING_PART_TEXT, LIFT_TEXT, OP_TEXT, PART_TEXT, PATH_TEXT, RULE_FIELD_TEXT,
    SLOT_TEXT, TYPE_TEXT, WHEN_TEXT, WHO_TEXT
)
from pipeline.deliver.export.guide import _enum, _fields, _gear, _licensing_models
from pipeline.deliver.export.cases import CASE_MECHANISMS, WHAT_TO_SHOW
from pipeline.deliver.export.dictionary import dictionary_gaps
from pipeline.deliver.export.closures import release_under_closure_problems
from pipeline.deliver.export.steelhead import (
    steelhead_lake_problems, steelhead_presence_problems, steelhead_set_problems
)


# --------------------------------------------------------------------------------------------
# The checks: run on the OUTPUT, so they prove what ships
# --------------------------------------------------------------------------------------------

def _keys(o, path=""):
    if isinstance(o, dict):
        for k, v in o.items():
            yield k, f"{path}.{k}"
            yield from _keys(v, f"{path}.{k}")
    elif isinstance(o, list):
        for i, v in enumerate(o):
            yield from _keys(v, f"{path}[{i}]")


def retired_keys(doc: dict) -> list[str]:
    """Every place a retired field name appears as a key, or a retired rule type as a type."""
    out = []
    data = {k: v for k, v in doc.items() if k not in ("rules",)}
    for k, where in _keys(data):
        if k in RETIRED_ANYWHERE:
            out.append(where)
    for rid, x in doc["rules"].items():
        if x["type"] in RETIRED_TYPES:
            out.append(f".rules.{rid}.type={x['type']}")
        for k in x["fields"]:
            if k in RETIRED_ON_RULE:
                out.append(f".rules.{rid}.fields.{k}")
        for k, where in _keys(x, f".rules.{rid}"):
            if k in RETIRED_ANYWHERE:
                out.append(where)
    return sorted(set(out))


def _registries() -> list[tuple[str, dict, set]]:
    """(name, the words, the model's own registry) — they must name exactly the same members."""
    lm = _licensing_models()
    return [
        ("TYPE_TEXT", TYPE_TEXT, set(_enum(C.RuleType))),
        ("FAMILY_TEXT", FAMILY_TEXT, set(C._FAMILY.values())),
        ("SLOT_TEXT", SLOT_TEXT, set(_enum(C.Slot))),
        ("CLAUSE_TEXT", CLAUSE_TEXT, set(_fields(C.GearClause))),
        ("GEAR_WHEN_TEXT", GEAR_WHEN_TEXT, set(_fields(C.GearWhen))),
        ("GEAR_SPEC_TEXT", GEAR_SPEC_TEXT, set(_fields(C.GearSpec))),
        ("WHEN_TEXT", WHEN_TEXT, set(_fields(C.When))),
        ("LIFT_TEXT", LIFT_TEXT, set(LIFT_KEYS)),
        ("LENGTH_TEXT", LENGTH_TEXT, set(_fields(C.LengthBand))),
        ("WHO_TEXT", WHO_TEXT, set(C.WHO_AXES)),
        ("PATH_TEXT", PATH_TEXT, set(_fields(C.Path))),
        ("DOING_TEXT", DOING_TEXT, set(typing.get_args(C.Doing.model_fields["act"].annotation))),
        ("LICENSING_KIND_TEXT", LICENSING_KIND_TEXT, set(lm)),
        ("PART_TEXT", PART_TEXT, set(C.LABEL_PARTS)),
        ("LICENSING_PART_TEXT", LICENSING_PART_TEXT, set(C.LICENSING_PARTS)),
        ("OP_TEXT", OP_TEXT, set(_enum(Op))),
    ]


def unexplained(doc: dict) -> list[str]:
    """Registry members the guide does not explain, words for members the model no longer has,
    and shipped keys the dictionary does not explain."""
    g, out = doc["guide"], []
    for name, words, registry in _registries():
        out += [f"{name} does not explain {k}" for k in sorted(registry - set(words))]
        out += [f"{name} explains {k}, which the model does not have"
                for k in sorted(set(words) - registry)]
        out += [f"{name}[{k}] is empty" for k in sorted(words) if not words[k]]
    model = set(_fields(C.CatalogueRule))
    out += [f"RULE_FIELD_TEXT explains {k}, which CatalogueRule does not have"
            for k in sorted(set(RULE_FIELD_TEXT) - model)]
    lfields = {f for m in _licensing_models().values() for f in _fields(m)}
    out += [f"LICENSING_FIELD_TEXT explains {k}, which no licensing record has"
            for k in sorted(set(LICENSING_FIELD_TEXT) - lfields)]
    for t in _enum(C.RuleType):
        if not (g["rule_types"].get(t) or {}).get("means"):
            out.append(f"rule type {t}")
    for s in C.Slot:
        if not (g["gear"]["slots"].get(s.value) or {}).get("means"):
            out.append(f"slot {s.value}")
    for a in C.CONDUCT_ACTS:
        if not (g["gear"]["conduct"]["acts"].get(a) or {}).get("means"):
            out.append(f"conduct {a}")
    used = {m for x in doc["rules"].values() for c in _gear(x) if c.get("slot") == "bait"
            for k in ("allow", "only", "ban", "except", "of") for m in c.get(k) or []}
    out += [f"gear.bait does not explain the bait member {m}"
            for m in sorted(used - set(g["gear"]["bait"]["members"]))]
    for k in _licensing_models():
        if not (g["licensing"]["kinds"].get(k) or {}).get("means"):
            out.append(f"licensing kind {k}")
    model = set(_fields(C.CatalogueRule))
    for k, v in doc["field_dictionary"]["rule.fields"].items():
        if not v:
            out.append(f"rule field {k} has no text")
        if k not in model:
            out.append(f"rule field {k} is not a CatalogueRule field")
    lmodels = _licensing_models()
    for kind, fs in doc["field_dictionary"]["licensing.fields"].items():
        for f, v in fs.items():
            if not v:
                out.append(f"{kind} field {f} has no text")
            if f not in _fields(lmodels[kind]):
                out.append(f"{kind} field {f} is not a {lmodels[kind].__name__} field")
    for sub, model_ in (("clause_fields", C.GearClause), ("when_fields", C.GearWhen),
                        ("requires_fields", C.GearSpec)):
        if set(g["gear"][sub]) != set(_fields(model_)):
            out.append(f"gear.{sub} differs from {model_.__name__}")
    return out


def touch_problems(waters: dict) -> list[str]:
    """Every `touches` list that is not sorted, names its own part, names no part of the water,
    or is not returned by the part it names."""
    out = []
    for item, w in waters.items():
        for i, p in enumerate(w["parts"]):
            t = p.get("touches")
            if t is None or t != sorted(set(t)) or i in t or any(
                    not 0 <= j < len(w["parts"]) or i not in w["parts"][j].get("touches", ())
                    for j in t):
                out.append(f"water {item} part {i} -> touches {t}")
    return out


def dangling(doc: dict) -> list[str]:
    """Every reference in the file that does not resolve."""
    R, L, E = doc["rules"], doc["licensing"], doc["entries"]
    out = touch_problems(doc["waters"])
    for eid, e in E.items():
        out += [f"entry {eid} -> rule {r}" for r in e["rules"] if r not in R]
        out += [f"entry {eid} -> see {t}" for s in e.get("see") or []
                for t in s.get("entry_ids") or [] if t not in E]
        out += [f"entry {eid} -> licensing {r}" for r in e["licensing"] if r not in L]
    for name, table, known in (("ruleset", doc["rulesets"], R),
                               ("licensing_set", doc["licensing_sets"], L)):
        for sid, s in table.items():
            for via, ids in s.items():
                if via == "sections":
                    continue
                out += [f"{name} {sid} -> {i}" for i in ids if i not in known]
    for item, w in doc["waters"].items():
        for p in w["parts"]:
            if p["ruleset"] is not None and p["ruleset"] not in doc["rulesets"]:
                out.append(f"water {item} -> ruleset {p['ruleset']}")
            if p["licensing_set"] is not None and p["licensing_set"] not in doc["licensing_sets"]:
                out.append(f"water {item} -> licensing_set {p['licensing_set']}")
        out += [f"water {item} -> entry {e}" for e in w["entries"] if e not in E]
        if w.get("part_of") and w["part_of"] not in doc["waters"]:
            out.append(f"water {item} -> part_of {w['part_of']}")
        out += [f"water {item} -> divided_into {c}" for c in w.get("divided_into") or []
                if c not in doc["waters"] or doc["waters"][c].get("part_of") != item]
    # THE CUTS EVERY EXTENT BINDS BY must be named (E2): a page composing "between the Elsie
    # Lake outlet and Dickson Lake" from `extents[].splits` can name either end only if `splits`
    # holds the id — including a lake's edge, the id a cut aliased onto it answers to.
    S = doc.get("splits") or {}
    # Every `splits` list anywhere in a record — a nested carve-out's extents included — since
    # the export ships only the lake edges something names (`referenced_edges`).
    for table, name in ((R, "rule"), (L, "licensing"), (E, "entry")) if "splits" in doc else ():
        for i, x in table.items():
            for sid in _split_refs(x.get("fields") if name != "entry" else x.get("extents")):
                if sid not in S:
                    out.append(f"{name} {i} extents -> split {sid}")
    for sid, x in S.items():
        for wid in [x.get("water_id")] + [a["water_id"] for a in x.get("at") or []]:
            if wid and wid not in doc["waters"]:
                out.append(f"split {sid} -> water {wid}")
        if x.get("same_place_as") and x["same_place_as"] not in S:
            out.append(f"split {sid} -> same_place_as {x['same_place_as']}")
        if x.get("lake_id") and x["lake_id"] not in doc["waters"]:
            out.append(f"split {sid} -> lake_id {x['lake_id']}")
    def examples(o):
        if isinstance(o, dict):
            if {"id", "label", "verbatim"} <= set(o):
                yield o["id"]
                return
            for v in o.values():
                yield from examples(v)
        elif isinstance(o, list):
            for v in o:
                yield from examples(v)
    out += [f"guide example {i}" for i in examples(doc["guide"]) if i not in R and i not in L]
    for i, x in R.items():
        out += [f"rule {i} -> recorded_by {x['recorded_by']}"] if x.get("recorded_by") and \
            x["recorded_by"] not in R else []
        out += [f"rule {i} -> records_for {q}" for q in x.get("records_for") or [] if q not in R]
    g = doc["guide"]
    listed = (g["standing"]["rules"] + g["angler_closure"]["rules"]
              + g["placement"]["uncertain"]["rules"] + g["placement"]["binds"]["in_part"])
    out += [f"guide lists rule {i}" for i in listed if i not in R]
    out += [f"guide lists licensing {i}" for i in g["placement"]["uncertain"]["licensing"]
            if i not in L]
    out += [f"guide lists entry {i}" for i in g["placement"]["unplaced_entries"] if i not in E]
    for name, ids, known in [(f"index {k}", v, R) for k, v in
                             {**doc["index"]["rules_by_type"],
                              **doc["index"]["rules_by_family"]}.items()] + \
            [(f"index {k}", v, L) for k, v in doc["index"]["licensing_by_kind"].items()]:
        out += [f"{name} -> {i}" for i in ids if i not in known]
    return out


def corpus_references(doc: dict) -> list[str]:
    """References the RECORDS make to each other (a clause to its parent, a lift to what it
    lifts, a record to the one it restates) that do not resolve. These are defects in the
    corpus, not in the export: the file ships them under `about.unresolved_references`, and a
    lift whose target does not resolve must not be applied."""
    R, L = doc["rules"], doc["licensing"]
    out = []
    for x in R.values():
        f, eid = x["fields"], x["entry_id"]
        for key in ("within", "condition_of", "derived_from", "suspended_while"):
            if f.get(key) and f"{eid}::{f[key]}" not in R:
                out.append(f"rule {x['id']} {key} -> {f[key]}")
        # Resolved lifts: each names one rule, which must exist and must not be the lifter.
        for ex in f.get("exempts") or []:
            to = f"{ex.get('entry_id')}::{ex.get('rule_id')}"
            if to not in R or to == x["id"]:
                out.append(f"rule {x['id']} exempts -> {to}")
    for x in L.values():
        f, eid = x["fields"], x["entry_id"]
        for key in ("alternative_to", "restates"):
            ref = f.get(key)
            if ref and f"{ref['entry_id']}#{ref['id']}" not in L:
                out.append(f"licensing {x['id']} {key} -> {ref}")
        for s in f.get("suspended_while") or []:
            if f"{eid}::{s['rule_id']}" not in R:
                out.append(f"licensing {x['id']} suspended_while -> {s['rule_id']}")
        for p in f.get("satisfied_by") or []:
            out += [f"licensing {x['id']} hold -> {doc_}" for doc_ in p.get("hold") or []
                    if doc_ not in doc["licences"]]
        for doc_ in ([f["document"]] if f.get("document") else []) + (f.get("documents") or []):
            if doc_ not in doc["licences"]:
                out.append(f"licensing {x['id']} document -> {doc_}")

    return out


def case_problems(doc: dict) -> list[str]:
    """A mechanism with no real case, and a case naming anything the file does not hold."""
    g = doc["guide"]["cases"]
    out = [f"guide.cases: no real case of {m}" for m in g["missing"]]
    out += [f"guide.cases: WHAT_TO_SHOW has no line for {m}"
            for m in sorted(set(CASE_MECHANISMS) - set(WHAT_TO_SHOW))]
    out += [f"guide.cases: WHAT_TO_SHOW names {m}, which is no mechanism"
            for m in sorted(set(WHAT_TO_SHOW) - set(CASE_MECHANISMS))]
    for c in g["cases"]:
        w = doc["waters"].get(c["water"]["item_id"])
        tag = f"guide.cases {c['mechanism']}"
        if w is None:
            out.append(f"{tag}: water {c['water']['item_id']}")
            continue
        if not any(p["ruleset"] == c["ruleset"] and p["licensing_set"] == c["licensing_set"]
                   for p in w["parts"]):
            out.append(f"{tag}: no part of {c['water']['item_id']} carries ruleset "
                       f"{c['ruleset']} / licensing set {c['licensing_set']}")
        out += [f"{tag}: expects {x['id']}" for x in c["expect"] if x["id"] not in doc["rules"]]
        out += [f"{tag}: because {i}" for i in c["because"]
                if i not in doc["rules"] and i not in doc["licensing"] and i not in doc["entries"]]
        out += [f"{tag}: entry {e}" for e in c["entries"] if e not in doc["entries"]]
    return out


def valid_end(token, doc: dict) -> bool:
    """An end token a run may carry: a key of `splits`, a bare natural end, or a prefixed one
    naming a water the file holds (a lake's ends name a lake or wetland)."""
    if token in doc["splits"] or token in SP.NATURAL_ENDS:
        return True
    head, _, item = str(token).partition(":")
    w = doc["waters"].get(item)
    if head in ("lake_inlet", "lake_outlet"):
        return w is not None and w["kind"] in ("lake", "wetland")
    if head == "continues":                       # answers 2.8: the water goes on as that stream
        return w is not None and w["kind"] == "stream"
    return head == "confluence" and w is not None


def run_problems(doc: dict) -> list[str]:
    """Every part says where it runs, in ends the file can name, upstream to downstream."""
    out = []
    for item, w in doc["waters"].items():
        for i, p in enumerate(w["parts"]):
            runs, tag = p.get("runs"), f"water {item} part {i}"
            if not runs:
                out.append(f"{tag}: no runs")
                continue
            if w["kind"] != "stream" or "polygon" in runs[0]:
                if len(runs) != 1 or any(runs[0][k] is not None for k in
                                         ("from", "to", "km_from", "km_to")):
                    out.append(f"{tag}: a polygon is one run with no ends")
                if w["kind"] != "stream" and "polygon" not in runs[0]:
                    out.append(f"{tag}: a {w['kind']} part is a polygon")
                continue
            for r in runs:
                for k in ("from", "to"):
                    if not valid_end(r[k], doc):
                        out.append(f"{tag}: {k} {r[k]!r} is no cut or end the file can name")
                if r["km_from"] is not None and r["km_to"] is not None \
                        and r["km_from"] < r["km_to"]:
                    out.append(f"{tag}: runs uphill ({r['km_to']} -> {r['km_from']})")
                if not r.get("branch") and (r["km_from"] is None or r["km_to"] is None):
                    out.append(f"{tag}: a main-stem run with no km")
            km = [r["km_from"] for r in runs if r["km_from"] is not None]
            if km != sorted(km, reverse=True):
                out.append(f"{tag}: runs not ordered upstream to downstream ({km})")
    return out


def period_problems(doc: dict) -> list[str]:
    """Every designation carries a period that agrees with its own `when`."""
    out = []
    for i, x in doc["licensing"].items():
        per = x.get("period")
        if x["kind"] != "designation":
            if per is not None:
                out.append(f"{i}: a period on a {x['kind']}")
            continue
        when = x["fields"].get("when")
        if not per or per.get("kind") not in ("when_open", "all_year", "dates") \
                or not per.get("says"):
            out.append(f"designation {i}: no period")
        elif (per["kind"] == "dates") != bool(when) or \
                (when and per.get("dates") != when.get("dates")):
            out.append(f"designation {i}: period {per['kind']} disagrees with its when {when}")
    return out


def _split_waters(x: dict) -> list[tuple[str, float]]:
    """(water_id, km) of every place a cut stands on a named water's main stem."""
    if x.get("water_id"):
        return [(x["water_id"], x["km"])]
    return [(a["water_id"], a["km"]) for a in x.get("at", ())]


def name_problems(doc: dict) -> list[str]:
    """THE NAMES A PAGE SHOWS. (1) Within each water, a cut's name is unique among its places — a
    second id at the SAME place says so (`same_place_as`) and shares the name. (2) A confluence is
    never named after the water it stands on. (3) No displayed name is all capitals
    (`place_names.is_shouting`; acronyms allowed): a water's, an entry's, a cut's, a lake part's."""
    from pipeline.deliver.bundle.place_names import is_shouting
    out = []
    S, W = doc["splits"], doc["waters"]
    seen: dict[tuple[str, str], list[tuple[float, str]]] = defaultdict(list)
    for sid, x in sorted(S.items()):
        if x["kind"] in ("lake_edge", "border"):
            continue           # named for the lake (or the border): one name at its inlet AND outlet
        for w, km in _split_waters(x):
            seen[(w, x["name"])].append((km, sid))
        same = x.get("same_place_as")
        if same is not None:
            y = S.get(same)
            if y is None or y["name"] != x["name"] or \
                    not set(_split_waters(x)) & set(_split_waters(y)):
                out.append(f"split {sid}: same_place_as {same!r} is no cut at its place by its name")
    for (w, name), at in sorted(seen.items()):
        places = []
        for km, sid in sorted(at):
            if not places or abs(km - places[-1][0]) > 0.01:
                places.append((km, [sid]))
            else:
                places[-1][1].append(sid)
        if len(places) > 1 and len({sid for _, ids in places for sid in ids}) > 1:
            # (one cut at two places of a water — Adams Lake's inlet and outlet — is one name)
            out.append(f"water {w}: the name {name!r} stands at {len(places)} places "
                       f"({', '.join(ids[0] for _, ids in places)})")
        for _, ids in places:
            for sid in ids[1:]:
                if S[sid].get("same_place_as") not in ids:
                    out.append(f"water {w}: {sid} shares {name!r} and its place with {ids[0]} "
                               f"but says no `same_place_as`")
    for sid, x in sorted(S.items()):
        if x["kind"] != "confluence":
            continue
        for w, _ in _split_waters(x):
            own = (W.get(w) or {}).get("name") or ""
            n = x["name"].lower()
            if own and (n == own.lower() or n == f"{own.lower()} confluence"
                        or n.endswith(f" of the {own.lower()} confluence")):
                out.append(f"split {sid}: a confluence on {w} named after that water ({x['name']!r})")
    shown = ([(f"water {i}", w["name"]) for i, w in W.items()]
             + [(f"entry {i}", e["name"]) for i, e in doc["entries"].items()]
             + [(f"split {i}", x["name"]) for i, x in S.items()]
             + [(f"water {i} part", r["polygon"]) for i, w in W.items() for p in w["parts"]
                for r in p["runs"] if r.get("polygon") not in (None, "whole")])
    out += [f"{where}: the name {n!r} is all capitals" for where, n in shown if is_shouting(n)]
    return out


def guide_example_problems(doc: dict) -> list[str]:
    """A prose example the reader contradicts, and a water the prose names that no declared example
    checks (`examples`)."""
    from pipeline.deliver.export.examples import example_problems, prose_gaps
    g = doc["guide"]
    names = {i: w["name"] for i, w in doc["waters"].items()}
    return example_problems(g.get("examples") or {}) + \
        prose_gaps(g, set(names.values()), g.get("examples") or {}, names)


def pdf_page_problems(doc: dict) -> list[str]:
    """Every printed page an entry cites opens the PDF somewhere: `guide.pdf_pages` maps it."""
    m = doc["guide"].get("pdf_pages") or {}
    if not m:
        return ["guide.pdf_pages is empty (is data/source/fishing_synopsis.pdf missing?)"]
    return [f"entry {eid} cites printed page {n}, which guide.pdf_pages does not map"
            for eid, e in sorted(doc["entries"].items()) for n in e.get("pages") or []
            if str(n) not in m]


def problems(doc: dict) -> list[str]:
    return ([f"retired key {w}" for w in retired_keys(doc)] + unexplained(doc) + dangling(doc)
            + pdf_page_problems(doc) + undrawn_map_problems(doc)
            + guide_example_problems(doc)
            + dictionary_gaps(doc)
            + case_problems(doc) + record_link_problems(doc) + steelhead_lake_problems(doc)
            + steelhead_presence_problems(doc) + steelhead_set_problems(doc)
            + run_problems(doc) + period_problems(doc) + name_problems(doc)
            + paper_licence_problems(doc) + protected_problems(doc)
            + release_under_closure_problems(doc))
