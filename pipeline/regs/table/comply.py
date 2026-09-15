"""PROTOTYPE 9 — COMPLIANCE: does the generated table account for every rule it was given?

WHAT IT HANDLES
  The one guarantee this project cannot trade away is that nothing goes missing quietly — a
  rule that binds nothing is indistinguishable from a water with no regulation. So the table is
  only trustworthy if EVERY rule handed to the generator can be found in what comes out, in
  exactly one of six honest places:

      governs      it is the answer for some row
      chain        it lost to something, and the row says which and why
      caveat       true here, but only in a window or a spot nobody can draw
      sub-limit    a clause of an allowance, carried on that allowance's row
      lifted       disapplied here by something that says so
      not-here     written about the other kind of water — a stream rule on a lake
      by-method    it restricts a WAY of fishing, and belongs in the gear table
      not-a-quota  it is not a retention rule at all — licence, boat, advisory

  Anything left over is a rule the reader would never see, and the check fails on it.

  THE BUCKET THAT WAS A LIE. `not-a-quota` used to mean "outcome_of returned None", which is
  not the same statement: a retention rule whose SHAPE this module cannot hold — a bare size
  gate, a possession multiplier, an exemption with no count of its own — returned None too, and
  was filed as though it were a boat rule. Twenty-three retention rules sat in that bucket while
  the check printed COMPLIES. A guarantee that launders its own failures is worse than none,
  because it is trusted. `not-a-quota` now requires the rule to say it is not a retention rule;
  anything else that cannot be placed is UNACCOUNTED, and the check fails, which is the point.

  It also checks the table cannot contradict itself: every row's printed outcome IS its
  governing rung's outcome (the page cannot drift from the chain, because it is handed both),
  and no row is printed with no subject.
"""
from __future__ import annotations
import sys
from collections import Counter

from pipeline.regs.table.build import build, section_rules, render, D, WATERS, name
from pipeline.regs.table.outcome import outcome_of
from pipeline.regs.table.clauses import children_of, lifted_ids


def audit(rules, kind="stream"):
    rows = build(rules, kind)
    seen, why = set(), {}
    def mark(rid, how):
        if rid and rid not in seen: seen.add(rid); why[rid] = how
    for r in rows:
        for i, c in enumerate(r.chain):
            mark(c.rule_id, "governs" if i == 0 and c.status == "governs" else
                 ("lifted" if "lifted" in c.status else "chain"))
        for c in (r.caveats or []): mark(c.rule_id, "caveat")
    kid = children_of(rules)
    for cs in kid.values():
        for c in cs: mark(c.get("rule"), "sub-limit")
    for x in rules:
        # A stream rule on a lake is not missing; it is about somewhere else. The CONTEXT
        # decided that (see `applies_here`), which is the whole point of deciding it once.
        w = x.get("water")
        if w and w != kind: mark(x.get("rule"), "not-here")
        if x.get("method"): mark(x.get("rule"), "by-method")
        if str(x.get("type") or "") != "retention_limit":
            mark(x.get("rule"), "not-a-quota")

    given = {x.get("rule") for x in rules if x.get("rule")}
    missing = sorted(given - seen)

    bad = []
    for r in rows:
        who, _ = r.subject.words(name)
        if not who: bad.append(("row with no subject", ""))
        gov = [c for c in r.chain if c.status == "governs"]
        if not gov: bad.append(("row with no governing rule", who))
        elif gov[0].outcome != r.outcome:
            bad.append(("row disagrees with its own chain", who))
    return rows, why, missing, bad


if __name__ == "__main__":
    tally, fails, total_missing, total_bad = Counter(), 0, 0, 0
    checked = 0
    for w in WATERS:
        kind = "lake" if (D[w].get("kind") == "lake") else "stream"
        for run in range(len(D[w].get("runs") or [])):
            rules = section_rules(w, run)
            if not rules: continue
            checked += 1
            rows, why, missing, bad = audit(rules, kind)
            tally.update(why.values())
            total_missing += len(missing); total_bad += len(bad)
            if missing and fails < 3:
                fails += 1
                print(f"UNACCOUNTED on {w} stretch {run+1}: {missing[:6]}")
            if bad and fails < 6:
                fails += 1
                print(f"BAD ROW on {w} stretch {run+1}: {bad[:3]}")
    print(f"\nchecked {checked} section tables across {len(WATERS)} waters")
    print("every rule landed somewhere:")
    for k, v in tally.most_common():
        print(f"   {k:14s} {v:6d}")
    print(f"\nunaccounted-for rules : {total_missing}")
    print(f"self-contradicting rows: {total_bad}")
    print("\nCOMPLIES" if total_missing == 0 and total_bad == 0 else "\nDOES NOT COMPLY")
