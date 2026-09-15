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

from pipeline.regs.table.subject import Subject, Origin, Size, Water, note_origin_split
from pipeline.regs.table.outcome import outcome_of
from pipeline.regs.table.resolve import Rung, Row, table
from pipeline.regs.table.applies import applies_of
from pipeline.regs.table.clauses import SubLimit, children_of, lifted_ids, pooled_of

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

def subject_of(x) -> Subject:
    return Subject(frozenset(x.get("species") or []),
                   Origin(x["origin"]) if x.get("origin") else Origin.both,
                   Size(x.get("over_cm"), x.get("under_cm")),
                   Water(x["water"]) if x.get("water") else Water.any,
                   x.get("method"), frozenset(x.get("species_except") or []))

def section_rules(water: str, run: int) -> List[dict]:
    runs = D[water].get("runs") or []
    if run >= len(runs): return []
    lo, hi = runs[run]["from"], runs[run]["to"]
    out = []
    for x in (D[water].get("rules") or []):
        sp = x.get("spans") or []
        if sp and not any(abs(a-lo) < .05 and abs(b-hi) < .05 for a, b in sp): continue
        out.append(x)
    return out

def build(rules: List[dict], water_kind: str = "stream") -> List[Row]:
    """THE WHOLE GENERATOR."""
    kid_rules = children_of(rules)                     # `within` -> clauses of an allowance
    lifted = lifted_ids(rules)                         # `exempts` -> disapplied here

    # sub-limits become a FIELD on their parent, never a row
    kids: Dict[str, List[SubLimit]] = {}
    for parent, cs in kid_rules.items():
        for c in cs:
            if c.get("take") is None: continue
            kids.setdefault(parent, []).append(
                SubLimit(subject_of(c), c["take"], pooled_of(c), c.get("verbatim") or ""))

    rungs = []
    for x in rules:
        if x.get("within"): continue                   # a clause, handled above
        o = outcome_of(x.get("take"), x.get("may_target"), x.get("unlimited"),
                       x.get("period"), pooled_of(x))
        if o is None: continue                         # not about how many -> not a table row
        rank, who = _authority(x)
        rungs.append(Rung(x.get("rule") or "?", who, rank, subject_of(x), o,
                          x.get("verbatim") or "",
                          applies_of(x.get("windows"), x.get("extent_text"),
                                     all_year=not x.get("windows"))))
    return table(rungs, water_kind, kids, lifted)

def render(rows: List[Row]) -> str:
    """The page's whole job, for comparison: print what it was given."""
    out = []
    for r in rows:
        who, q = r.subject.words(name)
        out.append(f"{who}|{r.outcome.word()}|{q}")
        for l in (r.limits or []):
            out.append(f"   limit: {l.sentence(name)}")
    return "\n".join(out)
