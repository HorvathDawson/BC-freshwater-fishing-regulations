"""The gear table's guarantee, which it did not have.

`comply.py` proves every retention rule is findable in the quota table. Nothing proved the same
of the gear table — and `build.py` now EXCLUDES 1,240 method-carrying rules from the quota side
on the grounds that they belong here. A rule that leaves one table and never arrives in the
other is the worst outcome available, and until this existed nothing could tell the difference.
"""
from __future__ import annotations
import collections

from pipeline.regs.table.build import D, WATERS, section_rules, section_regions
from pipeline.regs.table.corpus import rid
from pipeline.regs.table.method import RIG_TYPES
from pipeline.regs.table.method_build import rungs_for, METHODS
from pipeline.regs.table.lifts import lifts_here

from pipeline.regs.table.method import resolve_method


def audit(rules, here, water_kind="stream"):
    """Where each gear rule of this section lands, across every method."""
    where = {}
    def mark(k, how):
        where.setdefault(k, how)
    for m in METHODS:
        rs = rungs_for(m, rules, here, water_kind)
        if not rs:
            continue
        row = resolve_method(m, rs, here)
        if row is None:
            for r in rs:
                mark(r.rule_id, "scoped out of this section")
            continue
        for c in row.chain:      mark(c.rule_id, "verdict")
        for c in row.takes:      mark(c.rule_id, "what you may keep by it")
        for c in row.constraints:mark(c.rule_id, "how you must rig it")
        for c in row.unplaceable:mark(c.rule_id, "somewhere in here")
        for r in rs:
            if r.rule_id not in where and not row.permission.allowed and r.takes:
                mark(r.rule_id, "moot — the method is not permitted here")
            mark(r.rule_id, "scoped out of this section")
    # A rule an exemption disapplies here never becomes a rung — that is the mechanism working,
    # not a rule going missing, and the check has to be able to tell those apart.
    _, dropped = lifts_here(rules, here)
    for k in dropped:
        mark(k, "lifted here by an exemption")
    for x in rules:
        w = x.get("water")
        if w and w != water_kind:
            mark(rid(x), "written about the other kind of water")
    given = {rid(x) for x in rules
             if x.get("method") or str(x.get("type") or "") in RIG_TYPES}
    return where, sorted(given - set(where))


if __name__ == "__main__":
    tally, missing, checked = collections.Counter(), 0, 0
    shown = 0
    for w in WATERS:
        for run in range(len(D[w].get("runs") or [])):
            rules = section_rules(w, run)
            if not rules:
                continue
            checked += 1
            kind = "lake" if (D[w].get("kind") == "lake") else "stream"
            where, gone = audit(rules, section_regions(w, run), kind)
            tally.update(where.values())
            missing += len(gone)
            if gone and shown < 3:
                shown += 1
                print(f"UNACCOUNTED on {w} stretch {run + 1}: "
                      f"{[g.split('::')[-1] for g in gone[:6]]}")
    print(f"\nchecked {checked} section gear tables across {len(WATERS)} waters")
    for k, v in tally.most_common():
        print(f"   {k:34s} {v:6d}")
    print(f"\ngear rules that reached no table: {missing}")
    print("\nCOMPLIES" if missing == 0 else "\nDOES NOT COMPLY")
