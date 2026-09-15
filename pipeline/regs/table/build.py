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
from pipeline.regs.table.size import size_of, ANY as SIZE_ANY
from pipeline.regs.table.gates import Gate, attach as attach_gates
from pipeline.regs.table.outcome import outcome_of
from pipeline.regs.table.resolve import Rung, Row, table, applies_here, resolve, REPLACED_BY_CLAUSE
from pipeline.regs.table.applies import applies_of
from pipeline.regs.table.clauses import SubLimit, children_of, pooled_of
from pipeline.regs.table.corpus import rid, section_rules as corpus_section
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
    subtraction, so it joins what the rule already excepts (see lifts.py).

    THE SUBJECT CARRIES NO GATE. A size that sends a fish back — "none under 30 cm" — is not
    part of what the rule is ABOUT; it is a second thing the rule says, true beside the count,
    and it is split off here (see `gate_of`). Left on the subject it made "trout and char, none
    under 30 cm" a different subject from "trout and char", so the two never competed and the
    bound printed on its own row or on none. A size a number COUNTS — "1 over 50 cm" inside a
    4 — is what that number is about, and stays.
    """
    size = size_of(x.get("over_cm"), x.get("under_cm"), take=x.get("take"),
                   within=x.get("within"), band=bool(x.get("band")),
                   period=x.get("period") or "daily")
    return Subject(frozenset(x.get("species") or []),
                   Origin(x["origin"]) if x.get("origin") else Origin.both,
                   SIZE_ANY if size.is_gate else size,
                   Water(x["water"]) if x.get("water") else Water.any,
                   x.get("method"),
                   frozenset(x.get("species_except") or []) | (lifted_out or frozenset()))


def gate_of(x, subject: Subject, rank: int, who: str, when: str = "",
            status: str = "") -> Gate | None:
    """The size bound a rule carries, if it is a bound and not a selector — from EVERY shape
    the book writes one in. "No trout under 25 cm", "Trout/char daily quota = 1 (none under
    30 cm)", "Hatchery trout/char under 30 cm from streams: 0" and a clause "none under 60 cm"
    inside a 5 are one kind of statement, and they leave here as one kind of value."""
    size = size_of(x.get("over_cm"), x.get("under_cm"), take=x.get("take"),
                   within=x.get("within"), band=bool(x.get("band")),
                   period=x.get("period") or "daily")
    if not size.is_gate:
        return None
    return Gate(replace(subject, size=SIZE_ANY, water=Water.any), size, rid(x), who, rank,
                x.get("verbatim") or "", when, status)

def _water_of(c: dict, by_key: Dict[str, dict]) -> str | None:
    """The kind of water a clause is about: its own, or the nearest parent's that names one.
    `children_of` flattens a grandchild onto the top of the chain, and the top is the one
    parent that usually names NO water — "4 from streams" is the middle."""
    seen = set()
    while c is not None and rid(c) not in seen:
        seen.add(rid(c))
        if c.get("water"):
            return c["water"]
        c = by_key.get(f"{c.get('entry')}::{c.get('within')}") if c.get("within") else None
    return None


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
    narrow, _drop, unresolved = lifts_here(rules, here)
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
    gates: List[Gate] = []
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
            # A clause can be SEASONAL — "1 trout from streams, July 1 – Oct 31" printed
            # year-round without its dates. And a clause of a seasonal allowance is seasonal
            # with it.
            top = by_key.get(parent) or {}
            src = c if c.get("windows") else top
            ap = applies_of(src.get("windows"), c.get("extent_text"),
                            all_year=not src.get("windows"), section_label=label,
                            from_time=c.get("from_time"), to_time=c.get("to_time"),
                            weekdays=c.get("weekdays"),
                            unless=str(src.get("windows_are") or "") == "excepts")
            when = "" if ap.always else ap.detail
            # A CLAUSE'S SIZE BOUND IS A GATE, like anyone else's. "none under 60 cm" inside
            # "Trout/char: 5" sends a small char back whichever number ends up governing char
            # here; as a sub-limit it rode on its parent alone, and went dormant with it.
            # Its authority is its parent's, and so is its kind of water.
            wk = _water_of(c, by_key)
            if (not wk or wk == water_kind) and str(c.get("type") or "") == "retention_limit":
                st = "lifted here — does not apply" if parent in _drop else ""
                rank, who = _authority(top or c)
                g = gate_of(c, subject_of(c), rank, who, when, st)
                if g is not None:
                    gates.append(g)
            # A count-less clause that is not a gate constrains nothing the table can hold.
            if c.get("take") is None:
                continue
            # A CLAUSE ABOUT THE OTHER KIND OF WATER IS NOT A CLAUSE HERE. Region 8's "only 2
            # over 30 cm" sits inside "4 from streams"; flattened onto "Trout/char: 5" it rode
            # on Okanagan LAKE as a condition on the five, wearing "in streams" as a label.
            if wk and wk != water_kind:
                continue
            lim = SubLimit(subject_of(c), c.get("take"), pooled_of(c, subject_of(c)),
                           c.get("verbatim") or "", rid(c), when)
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
            lifted[par] = REPLACED_BY_CLAUSE

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
        rank, who = _authority(x)
        if rid(x) in promoted:
            # It speaks with its parent's voice: the same table wrote both.
            par = by_key.get(f"{x.get('entry')}::{x.get('within')}")
            if par is not None:
                rank, who = _authority(par)
        ap = applies_of(x.get("windows"), x.get("extent_text"),
                        all_year=not x.get("windows"), section_label=label,
                        from_time=x.get("from_time"), to_time=x.get("to_time"),
                        weekdays=x.get("weekdays"),
                        unless=str(x.get("windows_are") or "") == "excepts")
        # THE BOUND, SPLIT FROM THE COUNT. Whatever else the rule says, a size that sends a
        # fish back rides as a gate on every row about that fish — including the rows this
        # rule's own count loses on, because a number does not lift a take of zero.
        # ONLY A RETENTION RULE GATES. "Conservation Surcharge Stamp required to catch and
        # keep rainbow trout over 50 cm" carries `over_cm` too, and it is a licence rule: read
        # as a bound it told the Shuswap "none over 50 cm" on a water where the stamp is
        # exactly what lets you keep one.
        if (subj.water in (Water.any, Water(water_kind))
                and str(x.get("type") or "") == "retention_limit"):
            g = gate_of(x, subj, rank, who, "" if ap.always else ap.detail,
                        "" if rid(x) not in _drop else "lifted here — does not apply")
            if g is not None:
                gates.append(g)
                if o is not None and o.kind in ("release", "closed"):
                    # A TAKE OF ZERO ON A SIZE CLASS IS THE GATE, AND NOTHING ELSE. "Hatchery
                    # trout/char under 30 cm from streams: 0" is not a release of hatchery
                    # trout — it is the floor on the two you may keep. As a rung it was a
                    # release for a subject nobody else wrote about, took a row of its own,
                    # and the keep row beside it never showed the 30 cm.
                    continue
        if o is None:
            # No count of its own — but a retention rule with no count is still ABOUT a count.
            if str(x.get("type") or "") == "retention_limit":
                q = qualifier_of(x, subject_of(x, narrow.get(rid(x))), rid(x))
                if q is not None: quals.append(q)
            continue
        rungs.append(Rung(rid(x), who, rank, subj, o, x.get("verbatim") or "", ap))
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
    #
    # ...BUT NOT A RUNG THAT IS ABOUT THE OTHER KIND OF WATER. `table` drops those before it
    # resolves anything, and sweeping them back in undid that: Region 4's "Trout and char — 2
    # per day, FROM STREAMS" landed in the chain on Kootenay LAKE, where the lake's answer is 5.
    # It sat there labelled "also written here" — harmless until anything re-weighed the chain,
    # and the date-aware pass does exactly that, so the lake read 2. A rule about streams is
    # accounted for on a lake by NOT BEING THERE; `comply` files it under "not-here".
    in_chain = {c.rule_id for r in rows
                for c in r.chain + (r.caveats or []) + (r.ceilings or [])}
    # A SUBJECT ONLY SEASONS SPEAK TO, AND NO ROW COVERS. `resolve` declines to head a row
    # with a seasonal rule, rightly — but where no row covers the subject at all, that rule
    # has nowhere to ride and leaves the table. It gets a row of its own, headed by its
    # season (see `resolve(stranded=True)`).
    stranded = {rg.subject for rg in rungs
                if rg.rule_id not in in_chain and applies_here(rg, water_kind)
                and rg.applies.can_win
                and not any(r.subject.covers(rg.subject) or rg.subject.covers(r.subject)
                            for r in rows)}
    for subj in sorted(stranded, key=lambda x: sorted(x.fish)):
        row = resolve([replace(r, subject=replace(r.subject, water=Water.any))
                       for r in rungs if applies_here(r, water_kind)],
                      replace(subj, water=Water.any), kids, lifted, stranded=True)
        if row is not None:
            row.exemptions = list(unresolved)
            rows.append(row)
            in_chain |= {c.rule_id for c in row.chain + (row.caveats or []) + (row.ceilings or [])}
    for rg in rungs:
        if rg.rule_id in in_chain or not applies_here(rg, water_kind):
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

    # AN EXEMPTION NOBODY CAN PLACE STILL HAS TO REACH THE READER. `lifts_here` declines to
    # apply one — applied blanket, Region 6's steelhead exemption deleted a closure from a river
    # its own note does not name. The justification for holding it back was that it would ride
    # beside the closure in the reader's own words, and that half was never built: `unresolved`
    # was computed by four callers and read by none. It rides on every row now.
    for r in rows:
        r.exemptions = list(unresolved)

    build.unattached = attach(rows, quals)     # see `attach`: told, never dropped
    # A GATE THAT REACHED NO ROW IS A RULE THE READER NEVER SEES. It is not filed with the
    # duties that have nothing to trigger them; it stays visible here and `comply` fails on it.
    build.unattached_gates = attach_gates(rows, gates)
    return rows
