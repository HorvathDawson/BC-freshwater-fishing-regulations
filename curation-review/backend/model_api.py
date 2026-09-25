"""The catalogue model, as the review app sees it: its vocabularies, its labels, its errors.

Nothing here decides anything. Every list is READ OFF `pipeline.regs.parsing.catalogue` (an
enum, a `Literal`, a registry dict), so an option the model adds or drops reaches the editor
without an edit here — the rule-type picker once listed six retired kinds the model refused.
Every label is `catalogue.label` / `catalogue.licensing_label`, the functions the bundle uses.
Every error is the model's own `ValidationError`, re-addressed to the field it is about.

Three things the model does not carry and this module has to say where they come from:

  * gear member tokens (`bait: ban [any_bait]`) are OPEN strings in the model. The editor offers
    the tokens the corpus already uses as suggestions, labelled as such; any token may be typed.
  * `area_kind` is an open string on an extent too; same treatment.
  * `while` is `catalogue.WHILE_TOKENS` — the means (`Method` members) and the two devices
    (`downrigger`, `light`) — read off the model's own named constants (see `_while_tokens`).
"""

from __future__ import annotations

import calendar
import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal, Union, get_args, get_origin

from pydantic import TypeAdapter, ValidationError

from pipeline.common.models.enums import NodeKind
from pipeline.regs.parsing import catalogue as C
from pipeline.regs.parsing.entry_models import Extent, Op

#: Keys the app STAMPS on what it serves and removes before validation — the generated label of
#: each rule and each licensing record. Anything else the model does not know is refused.
SERVED_ONLY = ("label",)

_RECORD = TypeAdapter(C.LicensingRecord)


# --------------------------------------------------------------------------- #
# Vocabulary
# --------------------------------------------------------------------------- #

def _unwrap(tp):
    """Optional[X] / List[X] -> X, until a Literal or an Enum is reached."""
    while True:
        origin = get_origin(tp)
        if origin is Union:
            args = [a for a in get_args(tp) if a is not type(None)]
            tp = args[0]
        elif origin in (list, tuple):
            tp = get_args(tp)[0]
        else:
            return tp


def _values(model, field: str) -> list[str]:
    """The closed set a model field accepts — a `Literal`'s members or an Enum's values."""
    tp = _unwrap(model.model_fields[field].annotation)
    if get_origin(tp) is Literal:
        return [str(a) for a in get_args(tp)]
    if isinstance(tp, type) and issubclass(tp, C.Enum):
        return [m.value for m in tp]
    raise TypeError(f"{model.__name__}.{field} is not a closed vocabulary")


def _slot_shape(s: C.Slot) -> str:
    if s in C._SET_SLOTS:
        return "set"
    if s in C._SPEC_SLOTS:
        return "spec"
    if s in C._MEASURED:
        return "measured"
    return "count"


def _while_tokens() -> list[str]:
    """Every token `CatalogueRule.while` accepts — `catalogue.WHILE_TOKENS`, the named constant
    the validator itself checks against. (This used to PROBE the validator, because one token,
    `alone_in_a_boat`, was a literal known only inside it; that token is gone and the vocabulary
    is a constant: means, `WHILE_MEANS`, and devices, `WHILE_DEVICES`.)"""
    return sorted(C.WHILE_TOKENS)


def _corpus_tokens(entries: list[dict]) -> dict:
    """Open-string values the corpus already uses — offered as suggestions, never as a closed set."""
    gear: dict[str, set] = {}
    must_be: dict[str, set] = {}
    area_kinds: set = set()
    for e in entries:
        exts = list(e.get("extents") or [])
        for r in e.get("rules") or []:
            exts += list(r.get("extents") or []) + list(r.get("tributary_excludes") or [])
            for c in r.get("gear") or []:
                slot = c.get("slot", "")
                for k in ("allow", "only", "ban", "except", "members", "of"):
                    gear.setdefault(slot, set()).update(c.get(k) or [])
                must_be.setdefault(slot, set()).update(c.get("must_be") or [])
        for x in e.get("licensing") or []:
            exts += list(x.get("extents") or [])
        for x in exts:
            if isinstance(x, dict) and x.get("area_kind"):
                area_kinds.add(x["area_kind"])
    return {"gear_members": {k: sorted(v) for k, v in gear.items() if v},
            "must_be": {k: sorted(v) for k, v in must_be.items() if v},
            "area_kinds": sorted(area_kinds)}


def vocab(entries: list[dict]) -> dict:
    """Every option list the editors offer, read off the model."""
    kinds = [get_args(m.model_fields["kind"].annotation)[0]
             for m in get_args(get_args(C.LicensingRecord)[0])]
    species = []
    for code in sorted(C.KNOWN_SPECIES):
        members = C.SPECIES_GROUPS.get(code)
        species.append({"code": code, "name": C._SPECIES_WORDS.get(code, code),
                        "is_group": code in C.SPECIES_GROUPS,
                        "members": list(members) if members else []})
    return {
        "rule_types": [{"type": t.value, "family": C._FAMILY[t]} for t in C.RuleType],
        "licensing_kinds": kinds,
        "slots": [{"slot": s.value, "shape": _slot_shape(s)} for s in C.Slot],
        "methods": [m.value for m in C.Method],
        "while": _while_tokens(),
        "while_means": sorted(C.WHILE_MEANS),
        "while_devices": sorted(C.WHILE_DEVICES),
        "conduct": [{"act": k, "words": v} for k, v in C.CONDUCT_ACTS.items()],
        "documents": [{"doc": d.value, "words": C._DOC_WORDS.get(d.value, d.value),
                       "provincial": d.value in C.PROVINCIAL_ANGLER_DOCUMENTS}
                      for d in C.Document],
        "periods": _values(C.CatalogueRule, "period"),
        "water_kinds": _values(C.CatalogueRule, "water"),
        "origins": _values(C.CatalogueRule, "origin"),
        "obligations": _values(C.CatalogueRule, "obligation"),
        "vessel_aspects": _values(C.CatalogueRule, "aspect"),
        "propulsion_levels": _values(C.CatalogueRule, "level"),
        "angler_states": _values(C.GearWhen, "angler"),
        "solar": [s.value for s in C.Solar],
        "weekdays": list(calendar.day_name),
        "who_axes": {k: list(v) for k, v in C.WHO_AXES.items()},
        "exemptable_defaults": sorted(C.EXEMPTABLE_DEFAULTS),
        "doing_acts": _values(C.Doing, "act"),
        "requirement_on": _values(C.Requirement, "on"),
        "authority": _values(C.Requirement, "authority"),
        "classified": _values(C.Designation, "classified"),
        "terms_sold": _values(C.LicenceTerms, "sold"),
        "terms_covers": _values(C.LicenceTerms, "covers"),
        "terms_allocation": _values(C.LicenceTerms, "allocation"),
        "terms_needs": _values(C.LicenceTerms, "needs"),
        "path_quota": _values(C.Path, "quota"),
        "extent_ops": [o.value for o in Op],
        "extent_keys": list(Extent.model_fields),
        "feature_types": [k.value for k in NodeKind],
        "species": species,
        **_corpus_tokens(entries),
    }


# --------------------------------------------------------------------------- #
# Errors, addressed to the field they are about
# --------------------------------------------------------------------------- #

_KINDS = set(_values(C.Designation, "kind") + _values(C.NotClassified, "kind")
             + _values(C.Requirement, "kind") + _values(C.LicenceTerms, "kind")
             + _values(C.Exemption, "kind") + _values(C.Alternative, "kind"))


def _clean_msg(msg: str) -> str:
    return re.sub(r"^(Value error|Assertion failed), ", "", msg)


def _err(path: list, msg: str) -> dict:
    return {"path": ".".join(str(p) for p in path), "msg": msg}


def _attribute(entry: dict, text: str) -> list[dict]:
    """An ENTRY-level message ("<entry_id>: a; b; c") split into its parts, each addressed to the
    rule or licensing record it names by id. A part naming neither stays on the entry."""
    eid = str(entry.get("entry_id", ""))
    if text.startswith(eid + ": "):
        text = text[len(eid) + 2:]
    rules = [str(r.get("rule_id", "")) for r in entry.get("rules") or [] if isinstance(r, dict)]
    recs = [(str(x.get("kind", "")), str(x.get("id", "")))
            for x in entry.get("licensing") or [] if isinstance(x, dict)]
    out = []
    for part in [p.strip() for p in text.split("; ") if p.strip()]:
        path: list = []
        # the longest id that prefixes the part wins: `a.r1` must not claim `a.r10: …`
        for i, rid in sorted(enumerate(rules), key=lambda t: -len(t[1])):
            if rid and part.startswith(rid + ":"):
                path = ["rules", i]
                break
        if not path:
            for j, (kind, xid) in enumerate(recs):
                if xid and (part.startswith(f"{kind} {xid}:")
                            or part.startswith(f"designation {xid}:")
                            or f"licensing id {xid!r}" in part):
                    path = ["licensing", j]
                    break
        out.append(_err(path, part))
    return out


def errors_of(exc: ValidationError, entry: dict, prefix: list | None = None) -> list[dict]:
    """A `ValidationError` as `[{path, msg}]`, `path` in the JSON's own keys (`while`, `except`)
    with the licensing union's `kind` tag dropped, so `licensing.0.classified` names the field."""
    out: list[dict] = []
    for e in exc.errors():
        loc = list(prefix or []) + list(e["loc"])
        if len(loc) >= 3 and loc[0] == "licensing" and loc[2] in _KINDS:
            del loc[2]
        msg = _clean_msg(e["msg"])
        if not loc:
            out += _attribute(entry, msg)
            continue
        extra = e.get("type") == "extra_forbidden"
        out.append(_err(loc, f"`{loc[-1]}` is not a field of the model — it is refused, not "
                             f"dropped" if extra else msg))
    return out


# --------------------------------------------------------------------------- #
# Labels
# --------------------------------------------------------------------------- #

def _files_key(entries_dir: Path) -> tuple:
    return tuple((p.name, p.stat().st_mtime_ns) for p in sorted(entries_dir.glob("region-*.json")))


@lru_cache(maxsize=4)
def _corpus_context(key: tuple, entries_dir: str) -> tuple[dict, dict]:
    """{unit: unit_name} and {(entry_id, id): record} across the corpus — the context a licensing
    label needs and a record cannot carry itself. Cached on the files' mtimes."""
    units: dict = {}
    refs: dict = {}
    for p in sorted(Path(entries_dir).glob("region-*.json")):
        for e in json.loads(p.read_text(encoding="utf-8")).get("entries", []):
            for x in e.get("licensing") or []:
                try:
                    rec = _RECORD.validate_python(x)
                except ValidationError:
                    continue
                refs[(e["entry_id"], rec.id)] = rec
                if isinstance(rec, C.Designation):
                    units.setdefault(rec.unit, rec.unit_name)
    return units, refs


def corpus_context(entries_dir: Path) -> tuple[dict, dict]:
    return _corpus_context(_files_key(entries_dir), str(entries_dir))


@lru_cache(maxsize=4)
def _corpus_entries(key: tuple, entries_dir: str) -> dict:
    """{entry_id: CatalogueEntry} for every entry that validates — what a rule's `exempts` names,
    so the label says what it lifts in words, as the bundle's does. Cached on the files' mtimes."""
    out: dict = {}
    for p in sorted(Path(entries_dir).glob("region-*.json")):
        for e in json.loads(p.read_text(encoding="utf-8")).get("entries", []):
            try:
                out[e["entry_id"]] = C.CatalogueEntry.model_validate(e)
            except (ValidationError, KeyError, TypeError):
                continue
    return out


def corpus_entries(entries_dir: Path) -> dict:
    return _corpus_entries(_files_key(entries_dir), str(entries_dir))


def strip_served(entry: dict) -> dict:
    """The entry without the fields the app stamps on it."""
    data = dict(entry)
    for key in ("rules", "licensing"):
        if isinstance(data.get(key), list):
            data[key] = [{k: v for k, v in x.items() if k not in SERVED_ONLY}
                         if isinstance(x, dict) else x for x in data[key]]
    return data


def labels(entry: dict, entries_dir: Path, namer=None) -> dict:
    """The generated label of every rule and record that validates ON ITS OWN, and None for one
    that does not — so a curator sees the other rules' labels while fixing one.

    `namer` is the bundle's own `place_names.PlaceNamer` over the build the app serves, so a rule
    bound by a cut-point or an area names its place here exactly as the bundle's label does."""
    rules: list = []
    for r in entry.get("rules") or []:
        try:
            rules.append(C.CatalogueRule.model_validate(r))
        except (ValidationError, TypeError):
            rules.append(None)
    siblings = {r.rule_id: r for r in rules if r is not None}
    units, refs = corpus_context(entries_dir)
    eid = entry.get("entry_id")
    place_of = namer.for_entry(entry.get("matched") or []) if namer is not None else None
    entries = corpus_entries(entries_dir)
    out_rules = []
    for r in rules:
        out_rules.append(C.label(r, siblings, place_of, entries) if r is not None else None)
    recs = []
    for x in entry.get("licensing") or []:
        try:
            recs.append(_RECORD.validate_python(x))
        except (ValidationError, TypeError):
            recs.append(None)
    # this entry's own records override the corpus copy, so an edited unit name reads at once
    local_units = dict(units)
    local_refs = dict(refs)
    for rec in recs:
        if rec is None:
            continue
        local_refs[(eid, rec.id)] = rec
        if isinstance(rec, C.Designation):
            local_units[rec.unit] = rec.unit_name
    out_recs = [C.licensing_label(rec, siblings, units=local_units, refs=local_refs)
                if rec is not None else None for rec in recs]
    return {"rules": out_rules, "licensing": out_recs}


# --------------------------------------------------------------------------- #
# The draft check
# --------------------------------------------------------------------------- #

def warnings_of(entry: dict) -> list[dict]:
    """Extent shapes the model stores as plain dicts, checked against `entry_models.Extent` — the
    shape the resolver reads. REPORTED, NOT REFUSED: `CatalogueEntry` stores extents as dicts and
    refuses only what it checks itself (a bare `whole` beside `extent_text`, an extent-level
    `includes_tributaries`, `feature_types` on some extents and not others). `feature_types` on a
    non-`within` op is valid now — the builder applies it after the walk."""
    out: list[dict] = []
    known = set(Extent.model_fields)

    def visit(exts, path):
        for i, x in enumerate(exts or []):
            if not isinstance(x, dict):
                continue
            extra = sorted(set(x) - known)
            if extra:
                out.append(_err(path + [i], f"extent keys {extra} are not in the Extent model "
                                            f"(the resolver may not read them)"))
            try:
                Extent.model_validate(x)
            except ValidationError as ex:
                for e in ex.errors():
                    out.append(_err(path + [i] + list(e["loc"]), _clean_msg(e["msg"])))

    visit(entry.get("extents"), ["extents"])
    for i, r in enumerate(entry.get("rules") or []):
        if isinstance(r, dict):
            visit(r.get("extents"), ["rules", i, "extents"])
            visit(r.get("tributary_excludes"), ["rules", i, "tributary_excludes"])
    for j, x in enumerate(entry.get("licensing") or []):
        if isinstance(x, dict):
            visit(x.get("extents"), ["licensing", j, "extents"])
            visit(x.get("tributary_excludes"), ["licensing", j, "tributary_excludes"])
    return out


def check(entry: dict) -> tuple[C.CatalogueEntry | None, list[dict]]:
    """Validate a draft through THE INGEST GATE: `CatalogueEntry`, and then what ingest checks
    beyond the model (`validate_catalogue`) — every number in its own verbatim, a sub-limit inside
    its parent, a lift inside what it lifts, no lake that is cut into parts. Returns the model (or
    None) and the addressed errors; an entry with any error is not saved.

    ONE GATE. The app used to run only the model, so a curator saved `take: 15` on a rule whose
    sentence says 20 and the save went through; the next ingest would have refused the entry the
    app had just accepted."""
    from pipeline.regs.parsing import validate_catalogue as V
    pre: list[dict] = []
    if isinstance(entry, dict):
        pre += [_err([], m) for m in V.exemptions_stay_inside_what_they_lift(entry)]
        pre += [_err(_split_parent_path(entry, m), m) for m in V.split_parents_named(entry)]
    try:
        got = C.CatalogueEntry.model_validate(entry)
    except ValidationError as ex:
        return None, pre + errors_of(ex, entry)
    except (TypeError, ValueError) as ex:            # a draft that is not even an object
        return None, pre + [_err([], str(ex))]
    post = [_err(path, msg) for path, msg in V.post_model_checks(got)]
    return got, pre + post


def _split_parent_path(entry: dict, msg: str) -> list:
    """Address a split-parent refusal ("<entry>/<rule> …", "<entry>#<record> …", "<entry> matched:
    …") to the rule, record or field it names."""
    eid = str(entry.get("entry_id", ""))
    if msg.startswith(f"{eid} matched:"):
        return ["matched"]
    if msg.startswith(f"{eid} extents:"):
        return ["extents"]
    for i, r in enumerate(entry.get("rules") or []):
        if isinstance(r, dict) and msg.startswith((f"{eid}/{r.get('rule_id')}:",
                                                   f"{eid}/{r.get('rule_id')} ")):
            return ["rules", i]
    for j, x in enumerate(entry.get("licensing") or []):
        if isinstance(x, dict) and msg.startswith((f"{eid}#{x.get('id')}:",
                                                   f"{eid}#{x.get('id')} ")):
            return ["licensing", j]
    return []


def pass_through_changes(before: dict, after: dict) -> list[dict]:
    """The fields the app may not change: they come from the synopsis row via the batch, and
    `entry_id` encodes them (see the Identity panel). Changing them here would desync the entry
    from the row a re-parse compares it to."""
    out = []
    for k in ("entry_id", "name", "display_name", "region", "regs_verbatim", "source_pages",
              "symbols"):
        if before.get(k) != after.get(k):
            out.append(_err([k], f"`{k}` is passed through from the synopsis row and is not "
                                 f"edited here"))
    return out
