"""The gear table's guarantee: every gear rule handed in is findable in the table that comes out.

`comply.py` proves it of the quota ledger, and `build.py` EXCLUDES every method-carrying rule
from that ledger on the grounds that it belongs here. A rule that leaves one table and never
arrives in the other is the worst outcome available, so each gear rule of each section must
land in exactly one of these honest places:

      standing      it is the permit or ban that governs, or one behind it (same / opened / closed by)
      condition     a duty the governing permit carries
      rig           a condition printed on the row
      folded        true here, printed inside another line (same / covered / an exception)
      replaced      a closer rule about the same thing answered differently
      lifted        disapplied here by something that says so
      moot          an allowance under a ban that already covers it
      keep          a counter in the per-method keep ledger (binding, or behind another)
      lift          it lifts another rule, and the row shows what it freed
      caveat        true only somewhere nobody can draw, or at certain hours
      while-closed  the no-gear-in-the-water rule, shown on a closure
      not-here      written about the other kind of water
      scoped-out    the book limits it to regions this section is not in

Anything left over is a rule the reader would never see, and the check fails on it.
"""
from __future__ import annotations
import collections

from pipeline.regs.table.build import D, WATERS, section_rules, section_regions, section_kind, section_label
from pipeline.regs.table.corpus import rid
from pipeline.regs.table.authority import source_of
from pipeline.regs.table.ledger import LIFTED, SAME, ONLY_SOMEWHERE
from pipeline.regs.table.method import (METHODS, COVERED, MOOT, REPLACED, OPENED, CLOSED_BY,
                                        EXCEPTION, CONDITION, HOURS)
from pipeline.regs.table.method_build import table, region_base, is_gear, reaches_kind, _bites


_HOW = {LIFTED: "lifted", SAME: "folded", COVERED: "folded", EXCEPTION: "folded", CONDITION: "condition",
        MOOT: "moot", REPLACED: "replaced", OPENED: "standing", CLOSED_BY: "standing",
        ONLY_SOMEWHERE: "caveat", HOURS: "caveat"}


def audit(rules, here, water_kind="stream", label=""):
    """Where each gear rule of this section lands. Returns (rule -> place, unaccounted)."""
    T = table(rules, water_kind, here, label)
    where = {}
    def mark(k, how):
        if k and k not in where: where[k] = how
    for row in T.rows():
        m = row.method
        for t in row.candidates():
            st = row.status_of(t)
            mark(t.rule_id, _HOW.get(st, "standing") if st else "standing")
        for ts in row.rig().values():
            for t in ts:
                mark(t.rule_id, "rig")
                for c in row.carves(t): mark(c.rule_id, "folded")
        for t, st in row.folded():
            mark(t.rule_id, _HOW.get(st, "caveat"))
        for t in T.within_day(m):
            mark(t.rule_id, "caveat")
        L = row.keep()
        if L is not None:
            for a in L.allowances: mark(a.rule_id, "keep")
        for _, lifter in T.lifted_fish.get(m, []):
            mark(lifter.rule_id, "lift")
    for t in T.terms:
        if t.kind == "lift": mark(t.rule_id, "lift")
        if t.kind == "while_closed": mark(t.rule_id, "while-closed")
        if t.kind == "keep": mark(t.rule_id, "keep")
    # A BUCKET DECIDED BY A FIELD ON THE INPUT PROVES NOTHING. "Written about the other kind
    # of water" and "the book limits it to other regions" are the generator's own reasons for
    # not looking at a rule, so accounting for it by the same test would account for a rule the
    # generator wrongly refused. Each such rule must be FOUND in the table it belongs to instead:
    # the same stretch's table for the other kind of water, or the standing table of a region
    # the rule names.
    other = {}
    for x in rules:
        if not is_gear(x) or rid(x) in where: continue
        if not reaches_kind(x, water_kind):
            w = "lake" if water_kind == "stream" else "stream"
            if "t" not in other:
                other["t"] = _found_in(table(rules, w, here, label))
            if rid(x) in other["t"]: mark(rid(x), "not-here")
            continue
        src = source_of(x)
        if not _bites(src, here):
            for r in sorted(src.regions):
                if rid(x) in _found_in(region_base(r, water_kind)):
                    mark(rid(x), "scoped-out"); break
    given = {rid(x) for x in rules if is_gear(x)}
    return where, sorted(given - set(where))


def _found_in(T) -> set:
    """Every rule a table shows somewhere — the output, not the input."""
    ids = set()
    for row in T.rows():
        ids |= set(row.visible_ids())
    ids |= {t.rule_id for t in T.terms if t.kind in ("lift", "keep", "while_closed")}
    return ids


if __name__ == "__main__":
    tally, missing, checked, shown = collections.Counter(), 0, 0, 0
    for w in WATERS:
        for run in range(len(D[w].get("runs") or [])):
            rules = section_rules(w, run)
            if not rules:
                continue
            checked += 1
            where, gone = audit(rules, section_regions(w, run), section_kind(w), section_label(w, run))
            tally.update(where.values())
            missing += len(gone)
            if gone and shown < 3:
                shown += 1
                print(f"UNACCOUNTED on {w} stretch {run + 1}: {[g.split('::')[-1] for g in gone[:6]]}")
    print(f"\nchecked {checked} section gear tables across {len(WATERS)} waters")
    for k, v in tally.most_common():
        print(f"   {k:14s} {v:6d}")
    print(f"\ngear rules that reached no table: {missing}")
    print("\nCOMPLIES" if missing == 0 else "\nDOES NOT COMPLY")
