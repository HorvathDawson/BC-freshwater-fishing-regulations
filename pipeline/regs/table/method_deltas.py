"""THE INVARIANT THAT STOPS THE TAIL-CHASE:

    a section's gear table  ==  its region's standing table  +  the overrides that reach it

and EVERY difference from the standing table is attributable to exactly one named cause. Once
the standing tables are frozen against the printed synopsis, a wrong answer on a water points
at one named delta — a water rule, an inherited rule, an area rule, a lift, a closure — and
never at "the pipeline". A difference nothing explains is a test failure.

A delta is one of:

    standing    the method's verdict differs      cause: the override that governs, or the lift
                                                          that removed the base term, or a closure
    rig         a base condition's status differs  cause: the override that folded, replaced,
                                                          lifted or carved it
    rig         a condition prints that the base   cause: the override itself
                has not
    keep        a base counter is lifted, or a     cause: the lift, or the override counter
                counter is added
    closed      the water is shut                  cause: the closure (a water rule) — or a
                                                          region-wide spring closure the base
                                                          carries and a lift removed
    unshipped   a base term the section's rule set cause: the page builder lands a region-wide
                does not carry                            rule with a prose extent on no stretch
                                                          (build_regs_v3_data.py) — the rule's
                                                          own curation defect, named

`cause` is a rule id, never a description; `kind` says what sort of thing it is.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, FrozenSet, List, Optional, Tuple

from pipeline.regs.table.authority import source_of
from pipeline.regs.table.corpus import rid, rules as all_rules
from pipeline.regs.table.ledger import LIFTED
from pipeline.regs.table.method import MethodTable, MethodRow, METHODS, Term
from pipeline.regs.table.method_build import (table, base, region_base, _base, _key, is_gear,
                                              reaches_kind, _bites)


@dataclass(frozen=True)
class Delta:
    method: str
    what: str            # standing | rig | keep | closed | unshipped
    term: str            # the rule id the delta is about ("" for the verdict itself)
    base: str            # what the standing table says
    section: str         # what the section says
    cause: str           # the rule id that explains it — "" means UNATTRIBUTABLE
    cause_kind: str      # water | inherited | area | lift | closure | unshipped | ""

    @property
    def attributed(self) -> bool:
        return bool(self.cause)


def reference(here: FrozenSet[str], water_kind: str) -> MethodTable:
    """The standing table a section is measured against: the union of its regions' tables
    (a boundary stretch draws on two)."""
    keys = set()
    for r in sorted(here):
        keys |= set(region_base(r, water_kind).universe())
    rs = [x for x in all_rules() if rid(x) in keys]
    return _base(_key(rs), water_kind, frozenset(here))


def _cause_of(T: MethodTable, method: str, t: Term, seen=None) -> Tuple[str, str]:
    """Follow `behind` until an override is found: the term that folded/lifted/replaced `t`,
    or the one that folded THAT (a base term can fold behind a base term that an override
    then lifted). An override is any term that is not part of the standing table."""
    seen = seen or set()
    b = T.behind[method].get(t)
    while b is not None and b not in seen:
        seen.add(b)
        if not b.is_base:
            return b.rule_id, _kind(b)
        b = T.behind[method].get(b)
    for c in T.carves[method].get(t, []):
        if not c.is_base:
            return c.rule_id, _kind(c)
    return "", ""


def _kind(t: Term) -> str:
    if t.kind == "lift":
        return "lift"
    return {"water": "water", "inherited": "inherited", "area": "area"}.get(t.source.scope.value, "override")


def deltas(rules: List[dict], water_kind: str, here: FrozenSet[str], label: str = "") -> List[Delta]:
    here = frozenset(here)
    S = table(rules, water_kind, here, label)
    B = reference(here, water_kind)
    out: List[Delta] = []
    base_ids = {t.rule_id for t in B.terms}
    section_rule_ids = {rid(x) for x in rules}
    # UNSHIPPED: a standing-table rule the section's rule set does not carry at all.
    for t in B.terms:
        if t.rule_id not in section_rule_ids:
            x = next((x for x in all_rules() if rid(x) == t.rule_id), {})
            prose = bool((x.get("extent_text") or "").strip()) and x.get("scope") == "section"
            out.append(Delta(t.method or "any hook and line", "unshipped", t.rule_id, "in the standing table",
                             "not in this section's rules",
                             t.rule_id if prose else "", "unshipped" if prose else ""))
    unshipped = {d.term for d in out}
    # CLOSED
    for a in S.closures:
        base_closure = source_of_closure_is_base(a)
        out.append(Delta("every method", "closed", a.rule_id, "open", f"closed {a.applies.detail or 'all year'}",
                         a.rule_id, "closure" if not base_closure else "closure-base"))
    for m in METHODS:
        if not S.speaks_about(m):
            continue
        rs, rb = MethodRow(S, m), MethodRow(B, m)
        # STANDING
        gs, gb = rs.standing(), rb.standing()
        if gs.rule_id != gb.rule_id or gs.kind != gb.kind:
            if not gs.is_base and not gs.is_default:
                cause, ck = gs.rule_id, _kind(gs)
            elif gb.rule_id and gb.rule_id not in {t.rule_id for t in S.terms}:
                cause, ck = (gb.rule_id, "unshipped") if gb.rule_id in unshipped else ("", "")
            else:
                gbs = next((t for t in S.terms if t.rule_id == gb.rule_id and t.kind == gb.kind), None)
                cause, ck = _cause_of(S, m, gbs) if gbs is not None else ("", "")
            out.append(Delta(m, "standing", "", f"{rb.verdict_word()} ({gb.rule_id or 'default'})",
                             f"{rs.verdict_word()} ({gs.rule_id or 'default'})", cause, ck))
        # RIG: base conditions whose status changed; conditions the base has not
        for t in B._for(m, "rig"):
            ts = next((u for u in S.terms if u.rule_id == t.rule_id and u.kind == "rig"), None)
            if ts is None:
                continue          # unshipped, reported above
            sb, ss = B.status_of(m, t), S.status_of(m, ts)
            cb = [c.rule_id for c in B.carves[m].get(t, [])]
            cs = [c.rule_id for c in S.carves[m].get(ts, [])]
            if sb != ss or cb != cs:
                cause, ck = _cause_of(S, m, ts)
                if not cause and sb and not ss:
                    # A STATUS CLEARED is second-order: the term this one stood behind in the
                    # standing table was itself lifted or replaced here. The province's "roe
                    # may be used" is moot under Zone A's bait ban; the Fraser lifts that ban
                    # above the Cottonwood, and roe prints again — because of the lift.
                    b = B.behind[m].get(t)
                    bs = next((u for u in S.terms if b is not None and u.rule_id == b.rule_id and u.kind == b.kind), None)
                    if bs is not None:
                        cause, ck = _cause_of(S, m, bs)
                if not cause and set(cs) - set(cb):
                    extra = next(c for c in S.carves[m][ts] if c.rule_id in set(cs) - set(cb))
                    cause, ck = extra.rule_id, _kind(extra)
                out.append(Delta(m, "rig", t.rule_id, sb or "prints" + (f" (except {', '.join(cb)})" if cb else ""),
                                 ss or "prints" + (f" (except {', '.join(cs)})" if cs else ""), cause, ck))
        for ts in S._for(m, "rig"):
            if ts.rule_id in base_ids or ts.is_base:
                continue
            st = S.status_of(m, ts)
            out.append(Delta(m, "rig", ts.rule_id, "absent", st or "prints", ts.rule_id, _kind(ts)))
        # KEEP
        Ls, Lb = S.keep(m), B.keep(m)
        if Ls is not None:
            for a in Ls.allowances:
                if a.derived_from is not None:
                    continue
                ab = next((b for b in (Lb.allowances if Lb else ()) if b.rule_id == a.rule_id), None)
                if ab is None:
                    out.append(Delta(m, "keep", a.rule_id, "absent", a.word(), a.rule_id,
                                     {"water": "water", "inherited": "inherited", "area": "area"}.get(a.source.scope.value, "override")))
                elif Ls.status[a] != Lb.status[ab] or a.scope != ab.scope:
                    lifter = next((l for l in S.terms if l.kind == "lift" and a.rule_id in l.lifts and not l.is_base), None)
                    out.append(Delta(m, "keep", a.rule_id, Lb.status[ab] or "binds", Ls.status[a] or "binds",
                                     lifter.rule_id if lifter else "", "lift" if lifter else ""))
        freed_s = {(fish, l.rule_id) for fish, l in S.lifted_fish.get(m, [])}
        freed_b = {(fish, l.rule_id) for fish, l in B.lifted_fish.get(m, [])}
        for fish, l in freed_s - freed_b:
            lt = next(t for t in S.terms if t.rule_id == l)
            out.append(Delta(m, "keep", l, "absent", f"frees {', '.join(sorted(fish))}", l if not lt.is_base else "",
                             "lift" if not lt.is_base else ""))
    return out


def source_of_closure_is_base(a) -> bool:
    return a.source.is_base


def unattributed(rules, water_kind, here, label="") -> List[Delta]:
    return [d for d in deltas(rules, water_kind, here, label) if not d.attributed]


if __name__ == "__main__":
    import sys
    from pipeline.regs.table.build import section_rules, section_regions, section_kind, section_label
    w = sys.argv[1] if len(sys.argv) > 1 else "Fording River"
    run = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    for d in deltas(section_rules(w, run), section_kind(w), section_regions(w, run), section_label(w, run)):
        flag = "" if d.attributed else "   <-- UNATTRIBUTABLE"
        print(f"{d.method:18s} {d.what:9s} {d.term[-44:]:44s} {d.base[:28]:28s} -> {d.section[:28]:28s} "
              f"because {d.cause_kind}:{d.cause[-40:]}{flag}")
