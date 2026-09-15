"""PROTOTYPE 8 — THE GENERATOR: rules in, table out, nothing left for the client.

WHAT IT HANDLES
  This is the function the pipeline would call once per interned ruleset (2,376 of them for
  1,956,637 sections). It takes the raw rules of one section and returns the finished table.
  Everything the page does today — three precedence ladders, a closure override, carrying a
  parent's number into a child row, deciding what "release" covers — happens HERE, once, and
  reaches the client as data.
"""
from __future__ import annotations
import json, re
from typing import Dict, List

from pipeline.regs.table.subject import Subject, Origin, Water, note_origin_split
from pipeline.regs.table.size import size_of
from pipeline.regs.table.outcome import outcome_of
from pipeline.regs.table.resolve import Rung, Row, table
from pipeline.regs.table.applies import applies_of
from pipeline.regs.table.clauses import SubLimit, children_of, pooled_of
from pipeline.regs.table.corpus import rid, rule_part, section_rules as corpus_section
from pipeline.regs.table.lifts import lifts_here
from pipeline.regs.table.qualifiers import qualifier_of, attach

H = open("app/design/regs-v3.html").read()
D = json.loads(re.search(r'<script id="d" type="application/json">(.*?)</script>', H, re.S).group(1))
NAME = D.get("_species") or {}
WATERS = [k for k, v in D.items() if not k.startswith("_") and isinstance(v, dict)]
for _k in WATERS:
    for _x in (D[_k].get("rules") or []):
        if _x.get("origin"): note_origin_split(_x.get("species") or [])

def name(c): return NAME.get(c, c)

def _authority(x):
    e = str(x.get("entry") or "")
    if e.startswith("zp:"): return 3, "Provincial"
    if e.startswith("z"):   return 2, "Region " + (e[1:2] if e[1:2].isdigit() else "?")
    return (1, "inherited") if x.get("via") == "trib" else (0, "this water")

def subject_of(x, lifted_out=None) -> Subject:
    """`lifted_out` is whatever an exception takes out of this rule HERE — a lift is a
    subtraction, so it joins what the rule already excepts (see lifts.py)."""
    return Subject(frozenset(x.get("species") or []),
                   Origin(x["origin"]) if x.get("origin") else Origin.both,
                   size_of(x.get("over_cm"), x.get("under_cm"), take=x.get("take"),
                           within=x.get("within"), band=bool(x.get("band")),
                           period=x.get("period") or "daily"),
                   Water(x["water"]) if x.get("water") else Water.any,
                   x.get("method"),
                   frozenset(x.get("species_except") or []) | (lifted_out or frozenset()))

def section_rules(water: str, run: int) -> List[dict]:
    """COMPLETE records for one stretch — see `corpus.py`.

    This used to read the page's own embedded rule list, which made the comparison honest and
    the result wrong: that copy drops `exempts`, so sixty-odd "Exempt from spring closure"
    rules arrived carrying nothing at all, and the closure they lift stood on every one of
    those waters. No work in the browser could have recovered it.
    """
    return corpus_section(water, run)[0]


def section_regions(water: str, run: int):
    return corpus_section(water, run)[1]

def build(rules: List[dict], water_kind: str = "stream", here=frozenset()) -> List[Row]:
    """THE WHOLE GENERATOR. `here` is the section's region ids, which region-scoped rules and
    region-scoped lifts are measured against."""
    kid_rules = children_of(rules)                     # `within` -> clauses of an allowance
    narrow, lifted = lifts_here(rules, here)           # `exempts` -> narrowed / disapplied here

    # sub-limits become a FIELD on their parent, never a row
    kids: Dict[str, List[SubLimit]] = {}
    for parent, cs in kid_rules.items():
        for c in cs:
            if c.get("take") is None: continue
            kids.setdefault(parent, []).append(
                SubLimit(subject_of(c), c["take"], pooled_of(c), c.get("verbatim") or ""))

    rungs, quals = [], []
    for x in rules:
        if x.get("within"): continue                   # a clause, handled above
        # A RULE THAT NAMES A METHOD IS ABOUT THE METHOD. "Only non-game fish may be speared" is
        # a take of zero, so it walked into the quota table and said "Salmon · 0 · you may not
        # fish for it" on a salmon river — off the spear-fishing rule. It belongs in the gear
        # table, under the way of fishing it restricts (see method.py). 90 of 601 rungs.
        if x.get("method"): continue
        o = outcome_of(x.get("take"), x.get("may_target"), x.get("unlimited"),
                       x.get("period"), pooled_of(x))
        if o is None:
            # No count of its own — but a retention rule with no count is still ABOUT a count.
            if str(x.get("type") or "") == "retention_limit":
                q = qualifier_of(x, subject_of(x, narrow.get(rid(x))), rid(x))
                if q is not None: quals.append(q)
            continue
        rank, who = _authority(x)
        rungs.append(Rung(rid(x), who, rank, subject_of(x, narrow.get(rid(x))), o,
                          x.get("verbatim") or "",
                          applies_of(x.get("windows"), x.get("extent_text"),
                                     all_year=not x.get("windows"))))
    rows = table(rungs, water_kind, kids, lifted)
    build.unattached = attach(rows, quals)     # see `attach`: told, never dropped
    return rows

def render(rows: List[Row]) -> str:
    """The page's whole job, for comparison: print what it was given."""
    out = []
    for r in rows:
        who, q = r.subject.words(name, is_release=(r.outcome.kind == "release"))
        out.append(f"{who}|{r.outcome.word()}|{q}")
        for l in (r.limits or []):
            out.append(f"   limit: {l.sentence(name)}")
    return "\n".join(out)
