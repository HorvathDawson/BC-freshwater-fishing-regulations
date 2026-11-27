"""The catalogue model, as the review app sees it: its labels and the gate's errors.

Nothing here decides anything. Every label is `catalogue.label` / `catalogue.licensing_label`, the
functions the bundle uses. Every error is the gate's (`validate_catalogue.gate`) or the model's own
`ValidationError`, re-addressed to the field it is about. The app writes no entry (T185), so what
the editors needed — the vocabularies, the draft's extent warnings — is gone.
"""

from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Literal, Union, get_args, get_origin

from pydantic import TypeAdapter, ValidationError

from pipeline.regs.parsing import catalogue as C

#: Keys the app STAMPS on what it serves and removes before validation — the generated label of
#: each rule and each licensing record. Anything else the model does not know is refused.
SERVED_ONLY = ("label",)

_RECORD = TypeAdapter(C.LicensingRecord)


# --------------------------------------------------------------------------- #
# The model's closed vocabularies (the licensing kinds an error path drops)
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
    that does not — so a reviewer sees the other rules' labels beside one the model refuses.

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
# The gate
# --------------------------------------------------------------------------- #

def check(entry: dict, *, menu: dict | None, registry) -> tuple[C.CatalogueEntry | None, list[dict]]:
    """Validate an entry through THE GATE (`validate_catalogue.gate`) — the one ingest runs: the
    model, every number in its own verbatim, a sub-limit inside its parent, a lift inside what it
    lifts, no lake that is cut into parts, no absorbed item id (`registry`), every split id a
    cut-point it may bind (`menu`, `validate_catalogue.cut_point_menu`; an alias is stored
    canonically). Returns the model (or None) — carrying the gate's rewrites — and the addressed
    errors; an entry with any error cannot be marked (`marks.refuse_on_gate`).

    ONE GATE (T119). The app used to run only the model, so a curator saved `take: 15` on a rule
    whose sentence says 20 and the save went through; then it ran its own partial set beside the
    validator's, never resolving an exemption alias or canonicalising a split id, so an entry it
    accepted could still be refused by the full validator."""
    from pipeline.regs.parsing import validate_catalogue as V
    try:
        v = V.gate(entry, menu=menu, registry=registry)
    except (TypeError, ValueError) as ex:            # a draft that is not even an object
        return None, [_err([], str(ex))]
    errs = [_err(list(path), msg) for path, msg in v.refusals]
    if v.model_error is not None:
        errs += errors_of(v.model_error, entry)
    return v.entry, errs


