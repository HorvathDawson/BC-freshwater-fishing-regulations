"""COMPLIANCE: does the ledger account for every rule it was given?

The one guarantee this project cannot trade away is that nothing goes missing quietly — a rule
that binds nothing is indistinguishable from a water with no regulation. So the ledger is only
trustworthy if EVERY rule handed to the generator can be found in what comes out, in exactly one
of these honest places:

      binds        it is a counter in force — a number, a zero, a bound, a clause, a ceiling
      replaced     a closer rule took every fish it named, and the ledger says which
      lifted       disapplied here by something that says so
      caveat       true only somewhere nobody can draw
      multiple     a possession multiple — N × every daily number it covers
      duty         what you must do on keeping one
      not-here     written about the other kind of water — a stream rule on a lake
      by-method    it restricts a WAY of fishing, and belongs in the gear table
      not-a-quota  it is not a retention rule at all — licence, boat, advisory

Anything left over is a rule the reader would never see, and the check fails on it.
"""
from __future__ import annotations
from collections import Counter

from pipeline.regs.table.build import (ledger, section_rules, section_regions, section_label,
                                       section_kind, _water_of, D, WATERS)
from pipeline.regs.table.corpus import rid
from pipeline.regs.table.ledger import LIFTED, REPLACED_BY_CLAUSE, ONLY_SOMEWHERE, SAME, WEAKER
from pipeline.regs.table.lifts import lifts_here, self_lifting, contradicted_closures


def audit(rules, kind="stream", here=frozenset(), label=""):
    L = ledger(rules, kind, here, label)
    why = {}
    def mark(r, how):
        if r and r not in why: why[r] = how
    for a in L.allowances:
        if a.derived_from is not None:
            continue
        st = L.status[a]
        how = ("binds" if not st else "lifted" if st == LIFTED else "caveat" if st == ONLY_SOMEWHERE
               else "chain" if st in (SAME, WEAKER) else "replaced")
        mark(a.rule_id, how)
    for _, _, src in L.multiples: mark(src.rule_id, "multiple")
    for _, src, _ in L.duties:    mark(src.rule_id, "duty")
    narrow, dropped, _ = lifts_here(rules, here)
    for t in dropped | set(narrow): mark(t, "lifted")
    by_key = {rid(x): x for x in rules}
    for x in rules:
        if x.get("exempts"): mark(rid(x), "lifts another rule")
        w = _water_of(x, by_key)
        if w and w != kind: mark(rid(x), "not-here")
        if x.get("method"): mark(rid(x), "by-method")
        if str(x.get("type") or "") != "retention_limit": mark(rid(x), "not-a-quota")
    given = {rid(x) for x in rules if x.get("rule")}
    return L, why, sorted(given - set(why))


if __name__ == "__main__":
    tally, fails, total_missing, checked = Counter(), 0, 0, 0
    for w in WATERS:
        kind = section_kind(w)
        for run in range(len(D[w].get("runs") or [])):
            rules = section_rules(w, run)
            if not rules: continue
            checked += 1
            _, why, missing = audit(rules, kind, section_regions(w, run), section_label(w, run))
            tally.update(why.values())
            total_missing += len(missing)
            if missing and fails < 3:
                fails += 1
                print(f"UNACCOUNTED on {w} stretch {run+1}: {missing[:6]}")
    print(f"\nchecked {checked} section ledgers across {len(WATERS)} waters")
    print("every rule landed somewhere:")
    for k, v in tally.most_common():
        print(f"   {k:18s} {v:6d}")
    from pipeline.regs.table.corpus import rules as _all
    selfs = self_lifting(_all())
    if selfs:
        print(f"\n⚠ {len(selfs)} rule(s) exempt THEMSELVES — a curation defect, worked around "
              f"here but not fixed:")
        for x in selfs:
            print(f"   {x['rule']}  “{x['label'][:66]}”")
    contra = contradicted_closures(_all())
    if contra:
        by = {}
        for x in contra: by.setdefault(x["closure"], []).append(x)
        print(f"\n⚠ {len(by)} superior closure(s) are opened by a lower table, and nothing "
              f"lifts them — a curation defect, reported here and not worked around:")
        for c, xs in by.items():
            print(f"   {c}")
            for x in xs: print(f"      opened by {x['opened_by']}")
    print(f"\nunaccounted-for rules : {total_missing}")
    print("\nCOMPLIES" if total_missing == 0 else "\nDOES NOT COMPLY")
