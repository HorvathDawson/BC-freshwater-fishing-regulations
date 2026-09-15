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
from dataclasses import replace
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
    """The ladder, closest first. Rank is a sort key: SMALLER WINS.

    `authority: superior` is the catalogue's own mark for a rule no provincial table can write
    over — the federal species-at-risk closures, the National Parks closure, the ecological
    reserves. Six rules carry it and nothing read it, so on the Kootenay a park closure was
    demoted under "wider rule (Provincial), replaced by one closer to this water" while the
    same table offered burbot, bull trout, whitefish and crayfish to keep. Fishing in Kootenay
    National Park is prohibited unless the National Parks regulations open it; a regional quota
    does not open it.
    """
    if str(x.get("authority") or "") == "superior":
        return -1, "Federal or Parks"
    e = str(x.get("entry") or "")
    if e.startswith("zp:"): return 3, "Provincial"
    if e.startswith("z"):
        r = e[1:].split(":")[0]
        return 2, "Region " + (r.upper() if r and r[0].isdigit() else "?")
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


def section_label(water: str, run: int) -> str:
    runs = D[water].get("runs") or []
    return (runs[run].get("label") or "") if run < len(runs) else ""

def build(rules: List[dict], water_kind: str = "stream", here=frozenset(),
          label: str = "") -> List[Row]:
    """THE WHOLE GENERATOR. `here` is the section's region ids, which region-scoped rules and
    region-scoped lifts are measured against."""
    kid_rules = children_of(rules)                     # `within` -> clauses of an allowance
    narrow, _drop, unresolved_lifts = lifts_here(rules, here)
    lifted = {t: None for t in _drop}           # `exempts` -> narrowed / disapplied here

    # sub-limits become a FIELD on their parent, never a row
    # A CLAUSE THAT NAMES THIS KIND OF WATER IS NOT A CLAUSE HERE — IT IS THE ANSWER.
    #
    # Region 3 writes "Trout/char: 5" and, inside it, "4 from streams". On a lake the 5 governs
    # and the 4 is irrelevant; on a STREAM the 4 is the daily quota and the 5 is the number it
    # replaces. Held as a sub-limit either way, 61 stream sections printed "keep up to 5 ... of
    # which no more than 4 in streams" — the lake number as the headline on a river, with the
    # real limit demoted to a condition on it.
    #
    # Promoting it to a rung at its parent's rank needs no new mechanism: the existing sort puts
    # 4 ahead of 5 at equal authority, and `applies_here` drops it on a lake. `within` is a
    # composition constraint only when the clause narrows the FISH; when it narrows the WATER,
    # the context has already decided which one is speaking.
    by_key = {rid(r): r for r in rules}
    promoted, promoted_parent = set(), {}
    for parent, cs in kid_rules.items():
        for c in cs:
            # THE IMMEDIATE PARENT, not the top of the chain. `children_of` flattens a grandchild
            # onto the outermost allowance, so "only 2 over 30 cm" — a clause of "4 from streams",
            # which is itself a clause — passed the "parent names no water" test and was promoted
            # to a row of its own beside the 4 it is a part of.
            direct = by_key.get(f"{c.get('entry')}::{c.get('within')}")
            if (c.get("water") and not (direct or {}).get("water")
                    and c.get("take") is not None):
                promoted.add(rid(c))
                promoted_parent[rid(c)] = parent

    kids: Dict[str, List[SubLimit]] = {}
    for parent, cs in kid_rules.items():
        for c in cs:
            if rid(c) in promoted: continue
            # A PROMOTED CLAUSE TAKES ITS SIBLINGS WITH IT. On a stream, "2 per day from streams"
            # IS the allowance, and "no more than 1 rainbow over 50 cm" and "1 bull trout" are
            # constraints on THAT two — they were written inside the same limit. Keyed only to
            # the parent's id they became "clauses of a rule that is not the answer here", and
            # the Fording told a reader to keep two rainbows over 50 cm where the book allows
            # one. 63 rows, 130 clauses.
            for pr, par in promoted_parent.items():
                if par == parent:
                    kids.setdefault(pr, [])
            # A count-less clause is kept, not skipped: it constrains the composition just as a
            # numbered one does. And a clause can be SEASONAL — "1 trout from streams, July 1 –
            # Oct 31" printed year-round without its dates.
            if c.get("take") is None and not (c.get("over_cm") or c.get("under_cm")
                                              or c.get("band")):
                continue
            ap = applies_of(c.get("windows"), c.get("extent_text"),
                            all_year=not c.get("windows"), section_label=label,
                            from_time=c.get("from_time"), to_time=c.get("to_time"),
                            weekdays=c.get("weekdays"))
            lim = SubLimit(subject_of(c), c.get("take"), pooled_of(c, subject_of(c)),
                           c.get("verbatim") or "", rid(c), "" if ap.always else ap.detail)
            kids.setdefault(parent, []).append(lim)
            for pr, par in promoted_parent.items():
                if par == parent:
                    kids[pr].append(lim)

    # A PARENT WHOSE OWN CLAUSE REPLACED IT LEAVES THE RUNNING — BUT NOT THE PAGE. "2 from
    # streams (must be hatchery)" is promoted as a HATCHERY subject, which does not cover its
    # parent's "either origin", so the lake number stood as the headline on a stream on 23 rows.
    # Skipping the parent outright lost it from every table instead; it retires into the chain,
    # where a reader can see the number that WOULD apply and why it does not. Only for an
    # unconditional clause: where the clause is seasonal, the parent is the answer out of season.
    #
    # FOR THIS KIND OF WATER, WHICH THE CLAUSE HAS TO NAME. "2 from streams" retired its parent
    # on every LAKE as well — the clause was dropped as not-here, the parent as replaced, and
    # eight of nine lake sections had no trout and char row at all. Kootenay Lake's cutthroat
    # and lake trout had no quota; Region 4's 5 is the answer there and it had simply gone.
    for p in promoted:
        par = promoted_parent.get(p)
        c = by_key.get(p) or {}
        if par and not c.get("windows") and c.get("water") == water_kind:
            lifted[par] = "replaced here by its own clause for this kind of water"

    rungs, quals = [], []
    for x in rules:
        if x.get("within") and rid(x) not in promoted:
            continue                                   # a clause, handled above
        # A RULE THAT NAMES A METHOD IS ABOUT THE METHOD. "Only non-game fish may be speared" is
        # a take of zero, so it walked into the quota table and said "Salmon · 0 · you may not
        # fish for it" on a salmon river — off the spear-fishing rule. It belongs in the gear
        # table, under the way of fishing it restricts (see method.py). 90 of 601 rungs.
        if x.get("method"): continue
        subj = subject_of(x, narrow.get(rid(x)))
        o = outcome_of(x.get("take"), x.get("may_target"), x.get("unlimited"),
                       x.get("period"), pooled_of(x, subj))
        if o is None:
            # No count of its own — but a retention rule with no count is still ABOUT a count.
            if str(x.get("type") or "") == "retention_limit":
                q = qualifier_of(x, subject_of(x, narrow.get(rid(x))), rid(x))
                if q is not None: quals.append(q)
            continue
        rank, who = _authority(x)
        if rid(x) in promoted:
            # It speaks with its parent's voice: the same table wrote both.
            par = by_key.get(f"{x.get('entry')}::{x.get('within')}")
            if par is not None:
                rank, who = _authority(par)
        rungs.append(Rung(rid(x), who, rank, subj, o,
                          x.get("verbatim") or "",
                          applies_of(x.get("windows"), x.get("extent_text"),
                                     all_year=not x.get("windows"),
                                     section_label=label,
                                     from_time=x.get("from_time"),
                                     to_time=x.get("to_time"),
                                     weekdays=x.get("weekdays"))))
    rows = table(rungs, water_kind, kids, lifted)

    # A CLAUSE WHOSE PARENT IS IN NO CHAIN. `dormant` picks up the clauses of rules that lost,
    # but a parent can also leave the table entirely — beaten, then absorbed, its chain merged
    # into a row that is about something broader. Its clauses then belong to no row at all.
    # Sweeping for them is the difference between "carried" and "happened to be carried".
    # A RUNG THAT LANDED IN NO CHAIN. `resolve` only ever sees one subject at a time, and a rule
    # whose own subject produced no row — or whose row was absorbed by a host that already had a
    # rung of the same id — leaves the table with nothing saying so. The Shuswap's "Lake trout —
    # release all, Oct 15 – Jan 31" went that way. Sweeping is the difference between "carried"
    # and "happened to be carried", and it is the same principle as the clause sweep below.
    in_chain = {c.rule_id for r in rows
                for c in r.chain + (r.caveats or []) + (r.ceilings or [])}
    for rg in rungs:
        if rg.rule_id in in_chain:
            continue
        host = next((r for r in rows if r.subject.covers(rg.subject)
                     or rg.subject.covers(r.subject)), None)
        if host is not None:
            note = ("instead, " + rg.applies.detail) if not rg.applies.always else "also written here"
            host.chain = host.chain + [replace(rg, status=note)]

    placed = {l.rule_id for r in rows for l in (r.limits or []) + (r.dormant or [])}
    for parent, ls in kids.items():
        for l in ls:
            if l.rule_id in placed:
                continue
            host = next((r for r in rows if r.subject.covers(l.subject)
                         or l.subject.covers(r.subject)), None)
            if host is not None:
                host.dormant = (host.dormant or []) + [l]

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
