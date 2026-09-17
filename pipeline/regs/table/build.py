"""THE GENERATOR: rules in, a settled ledger out — in two stages, nothing left for the client.

  STAGE 1  `base(rules, kind)` — the region's standing table. Every region-wide rule (the
           province's and the region's; see `authority.Source.is_base`) for this kind of
           water, settled on its own. It is a pure function of the rule set and the kind of
           water, cached, and it is the thing the printed synopsis lets a person check.

  STAGE 2  `ledger(rules, kind, here, label)` — the base, with the section's overrides laid on
           top: the water's own rules, area rules, rules inherited by the tributary walk, and
           the lifts among them. What a reader sees for one stretch.

Everything the browser used to do — three precedence ladders, a closure override, carrying a
parent's number into a child row — happens here, once, and reaches the client as data.
"""
from __future__ import annotations
import json, re
from dataclasses import replace
from functools import lru_cache
from typing import Dict, FrozenSet, List, Tuple

from pipeline.regs.table.subject import Subject, Origin, Water, note_origin_split
from pipeline.regs.table.size import size_of, ANY as SIZE_ANY
from pipeline.regs.table.outcome import outcome_of, RELEASE
from pipeline.regs.table.applies import applies_of
from pipeline.regs.table.authority import source_of, Source
from pipeline.regs.table.ledger import Allowance, Ledger, LIFTED, REPLACED_BY_CLAUSE
from pipeline.regs.table.clauses import children_of, pooled_of
from pipeline.regs.table.corpus import rid, section_rules as corpus_section
from pipeline.regs.table.lifts import lifts_here

H = open("app/design/regs-v3.html").read()
D = json.loads(re.search(r'<script id="d" type="application/json">(.*?)</script>', H, re.S).group(1))
NAME = D.get("_species") or {}
WATERS = [k for k, v in D.items() if not k.startswith("_") and isinstance(v, dict)]
for _k in WATERS:
    for _x in (D[_k].get("rules") or []):
        if _x.get("origin"): note_origin_split(_x.get("species") or [])

def name(c): return NAME.get(c, c)


def _size(x: dict):
    return size_of(x.get("over_cm"), x.get("under_cm"), take=x.get("take"),
                   within=x.get("within"), band=bool(x.get("band")),
                   period=x.get("period") or "daily")


def subject_of(x, lifted_out=None) -> Subject:
    """What a rule is ABOUT. `lifted_out` is whatever an exception takes out of it here.

    THE SUBJECT CARRIES NO GATE. A size that sends a fish back — "none under 30 cm" — is not
    part of what the rule is about; it is a second thing the rule says, true beside the count,
    and it becomes an allowance of its own (see `gate_of`). A size a number COUNTS — "1 over
    50 cm" inside a 4 — is what that number is about, and stays.
    """
    size = _size(x)
    return Subject(frozenset(x.get("species") or []),
                   Origin(x["origin"]) if x.get("origin") else Origin.both,
                   SIZE_ANY if size.is_gate else size,
                   Water(x["water"]) if x.get("water") else Water.any,
                   x.get("method"),
                   frozenset(x.get("species_except") or []) | (lifted_out or frozenset()))


def gate_of(x, subject: Subject, source: Source, applies, within: str = "") -> Allowance | None:
    """The size bound a rule carries, if it is a bound and not a selector — from EVERY shape
    the book writes one in: "No trout under 25 cm", "quota = 1 (none under 30 cm)", "Hatchery
    trout/char under 30 cm from streams: 0", and a clause "none under 60 cm" inside a 5. One
    kind of statement, one kind of value: an allowance of zero on the forbidden size class."""
    size = _size(x)
    if not size.is_gate:
        return None
    return Allowance(replace(subject, size=size, water=Water.any), RELEASE, source, applies, within)


def _kind_of(x: dict) -> str | None:
    """A rule's kind of water, in BOTH spellings the catalogue uses: the `water` field, or
    `feature_types` on its typed extents ("lakes of Region 6" carries the latter alone).
    The atlas reads both; a kind test that read only `water` let a lake rule reach streams."""
    if x.get("water"):
        return x["water"]
    kinds = {t for e in (x.get("extents") or []) for t in (e.get("feature_types") or [])}
    return next(iter(kinds)) if len(kinds) == 1 else None


def _water_of(c: dict, by_key: Dict[str, dict]) -> str | None:
    """The kind of water a clause is about: its own, or the nearest parent's that names one."""
    seen = set()
    while c is not None and rid(c) not in seen:
        seen.add(rid(c))
        if _kind_of(c):
            return _kind_of(c)
        c = by_key.get(f"{c.get('entry')}::{c.get('within')}") if c.get("within") else None
    return None


def section_rules(water: str, run: int) -> List[dict]:
    return corpus_section(water, run)[0]

def section_regions(water: str, run: int):
    return corpus_section(water, run)[1]

def section_label(water: str, run: int) -> str:
    runs = D[water].get("runs") or []
    return (runs[run].get("label") or "") if run < len(runs) else ""

def section_kind(water: str) -> str:
    return "lake" if (D[water].get("kind") == "lake") else "stream"


def _applies(x: dict, label: str):
    return applies_of(x.get("windows"), x.get("extent_text"), all_year=not x.get("windows"),
                      section_label=label, from_time=x.get("from_time"),
                      to_time=x.get("to_time"), weekdays=x.get("weekdays"),
                      unless=str(x.get("windows_are") or "") == "excepts")


def allowances(rules: List[dict], water_kind: str, here=frozenset(), label: str = ""):
    """Raw rules -> (allowances, lifted, family, multiples, duties, unresolved) for ONE kind of
    water. Nothing is settled here; that is the ledger's job."""
    kid_rules = children_of(rules)
    narrow, drop, unresolved = lifts_here(rules, here)
    lifted = {t: LIFTED for t in drop}
    by_key = {rid(r): r for r in rules}

    # A CLAUSE THAT NAMES THIS KIND OF WATER IS NOT A CLAUSE HERE — IT IS THE ANSWER. Region 3
    # writes "Trout/char: 5" and, inside it, "4 from streams". On a lake the 5 governs and the
    # 4 is irrelevant; on a STREAM the 4 is the daily quota and the 5 is the number it
    # replaces. Promoted, it speaks with its parent's voice, and its parent retires — for this
    # kind of water only, which the clause has to name.
    promoted, promoted_parent = set(), {}
    for parent, cs in kid_rules.items():
        for c in cs:
            direct = by_key.get(f"{c.get('entry')}::{c.get('within')}")
            if (c.get("water") and not (direct or {}).get("water")
                    and c.get("take") is not None):
                promoted.add(rid(c)); promoted_parent[rid(c)] = parent
    for p in promoted:
        c = by_key[p]
        if promoted_parent.get(p) and not c.get("windows") and c.get("water") == water_kind:
            lifted[promoted_parent[p]] = REPLACED_BY_CLAUSE

    # THE FAMILY: who counts inside whom. A clause is nested in every ancestor up the `within`
    # chain, and in the promoted sibling that replaced their common parent — "1 over 50 cm"
    # and "1 char" are constraints on "2 from streams" once that two is the allowance.
    family: Dict[str, set] = {}
    for parent, cs in kid_rules.items():
        ids = {rid(c) for c in cs} | {parent}
        for c in cs:
            chain, key, seen = set(), rid(c), set()
            while key in by_key and by_key[key].get("within") and key not in seen:
                seen.add(key)
                key = f"{by_key[key].get('entry')}::{by_key[key]['within']}"
                chain.add(key)
            family.setdefault(rid(c), set()).update(chain)
            for pr, par in promoted_parent.items():
                if par == parent and pr != rid(c):
                    family.setdefault(rid(c), set()).add(pr)
                    family.setdefault(pr, set()).add(rid(c))

    out: List[Allowance] = []
    multiples, duties = [], []
    for x in rules:
        if str(x.get("type") or "") != "retention_limit" or x.get("method"):
            continue
        # A RULE ABOUT THE OTHER KIND OF WATER IS NOT HERE. Deciding this once, before anything
        # is settled, is what keeps "2 from streams" off Kootenay Lake by construction.
        wk = _water_of(x, by_key)
        if wk and wk != water_kind:
            continue
        is_clause = bool(x.get("within")) and rid(x) not in promoted
        top = by_key.get(f"{x.get('entry')}::{x.get('within')}") if x.get("within") else None
        src = x if (x.get("windows") or not is_clause) else (top or x)
        ap = _applies(src, label)
        source = source_of(x)
        subj = subject_of(x, narrow.get(rid(x)))
        within = f"{x.get('entry')}::{x.get('within')}" if is_clause else ""
        g = gate_of(x, subj, source, ap, within)
        if g is not None:
            out.append(g)
        o = outcome_of(x.get("take"), x.get("may_target"), x.get("unlimited"),
                       x.get("period"), pooled_of(x, subj))
        if g is not None and o is not None and o.kind in ("release", "closed"):
            # A TAKE OF ZERO ON A SIZE CLASS IS THE GATE, AND NOTHING ELSE. "Hatchery trout/char
            # under 30 cm from streams: 0" is not a release of hatchery trout — it is the floor
            # on the two you may keep.
            continue
        if o is None:
            if x.get("per_daily"):
                multiples.append((replace(subj, water=Water.any), int(x["per_daily"]), source))
            elif x.get("record_retention") or x.get("on_retention"):
                duties.append((replace(subj, water=Water.any), source, x.get("label") or ""))
            continue
        out.append(Allowance(replace(subj, water=Water.any), o, source, ap, within))
    fam = {k: frozenset(v) for k, v in family.items()}
    return out, lifted, fam, multiples, duties, unresolved


def _key(rules: List[dict]) -> Tuple[str, ...]:
    return tuple(sorted(rid(x) for x in rules))


@lru_cache(maxsize=None)
def _base(key: Tuple[str, ...], water_kind: str) -> Ledger:
    from pipeline.regs.table.corpus import rules as all_rules
    want = set(key)
    rs = [x for x in all_rules() if rid(x) in want]
    alw, lifted, fam, mults, duties, unresolved = allowances(rs, water_kind)
    return Ledger(alw, lifted=lifted, family=fam, multiples=mults, duties=duties,
                  exemptions=unresolved, water_kind=water_kind)


def base_rules(rules: List[dict]) -> List[dict]:
    """The region-wide rules among a section's rules — the ones its base is made of."""
    return [x for x in rules if source_of(x).is_base]


def base(rules: List[dict], water_kind: str) -> Ledger:
    """STAGE 1. The standing table for this kind of water, from the region-wide rules a
    section carries. Cached on the rule set, so every section in a region shares one."""
    return _base(_key(base_rules(rules)), water_kind)


def ledger(rules: List[dict], water_kind: str = "stream", here=frozenset(),
           label: str = "") -> Ledger:
    """STAGE 2. The base, with this section's overrides on top.

    `here` is the section's region ids, which region-scoped lifts are measured against; `label`
    is the section's own name, which decides whether an extent the atlas already cut is this
    very piece of water (see `where.cut_for_this`)."""
    b = base(rules, water_kind)
    alw, lifted, fam, mults, duties, unresolved = allowances(rules, water_kind, here, label)
    over = [a for a in alw if not a.source.is_base]
    fam_all = dict(b.family); fam_all.update(fam)
    return b.overlay(over, lifted=lifted, family=fam_all, multiples=mults, duties=duties,
                     exemptions=unresolved, water_kind=water_kind, label=label)


def build(rules: List[dict], water_kind: str = "stream", here=frozenset(), label: str = ""):
    """The finished table for one stretch: the derived rows (see `rows.py`)."""
    from pipeline.regs.table.rows import rows
    return rows(ledger(rules, water_kind, here, label))
