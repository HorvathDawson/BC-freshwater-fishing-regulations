"""COMPLIANCE: does the ledger account for every rule it was given — PROVED AGAINST AN OUTPUT?

The one guarantee this project cannot trade away is that nothing goes missing quietly — a rule
that binds nothing is indistinguishable from a water with no regulation. So every rule handed
to the generator must be found in what comes out, in exactly one of these honest places, and
EACH PLACE IS PROVED BY LOOKING AT AN OUTPUT, never by re-reading the input:

      binds        a counter drawn on some row of the derived table
      replaced     drawn beneath a row, saying which closer rule took its fish
      lifted       drawn beneath a row, saying what lifted it
      caveat       a place nobody can draw, carried on the section
      multiple     a possession multiple — some possession counter cites it, or there was no
                   daily number in its scope to multiply
      duty         drawn on a row that permits keeping, or nothing here permits keeping
      lifts        the rules it lifts were lifted here, or it is carried as an exemption
                   nobody can place
      not-here     about the other kind of water — and PRESENT in that kind's ledger for the
                   same rules
      by-method    a way of fishing — and PRESENT in the gear table's own audit
      not-a-quota  a type on the list of things that are not a retention rule, carrying no
                   count of its own

A bucket filled from the input record's own fields can only ever report what was handed in;
this check has printed COMPLIES over a rule the generator refused to look at three times in
this project's history. Anything not proved is UNACCOUNTED, and the check fails.
"""
from __future__ import annotations
from collections import Counter

from pipeline.regs.table.build import (ledger, section_rules, section_regions, section_label,
                                       section_kind, _water_of, D, WATERS, name)
from pipeline.regs.table.corpus import rid
from pipeline.regs.table.ledger import LIFTED, ONLY_SOMEWHERE, SAME, WEAKER
from pipeline.regs.table.lifts import lifts_here, self_lifting, contradicted_closures
from pipeline.regs.table.rows import rows

#: What is genuinely not a retention rule. A closed list, not `!= "retention_limit"`.
NOT_RETENTION = frozenset({
    "bait_restriction", "vessel_rule", "tackle_restriction", "document_required",
    "method_rule", "advisory", "hazard", "access_permission", "program_membership",
    "angling_from_vessel_prohibited", "facility", "handling_rule", "stop_fishing_after_quota",
    "navigation_duty"})

#: Fields that make a rule a retention rule whatever its `type` says. A length on a
#: document rule ("Conservation Surcharge Stamp required to keep rainbow trout over 50 cm")
#: is a condition of the document, not a count, and stays off this list.
_COUNTS = ("take", "unlimited", "per_daily")


def audit(rules, kind="stream", here=frozenset(), label=""):
    L = ledger(rules, kind, here, label)
    R = rows(L, name)
    on_rows = {a.rule_id for r in R for a in r.counters}
    beneath = {a.rule_id for r in R for a, _ in r.behind}
    permits = [r for r in R if r.headline() is not None and not r.headline().is_zero]
    why = {}
    def mark(r, how):
        if r and how and r not in why: why[r] = how

    # Allowances: proved by where the derived table drew them.
    for a in L.allowances:
        if a.derived_from is not None:
            continue
        st = L.status[a]
        if not st:
            mark(a.rule_id, "binds" if a.rule_id in on_rows
                 else "replaced" if a.rule_id in beneath else None)
        elif st == LIFTED:
            mark(a.rule_id, "lifted" if a.rule_id in beneath else None)
        elif st == ONLY_SOMEWHERE:
            mark(a.rule_id, "caveat")
        else:
            mark(a.rule_id, ("chain" if st in (SAME, WEAKER) else "replaced")
                 if a.rule_id in beneath else None)

    # Possession multiples: some derived counter cites it, or nothing was there to multiply.
    cited = {a.multiplied_by.rule_id for a in L.allowances if a.multiplied_by is not None}
    for subj, n, src in L.multiples:
        covered = [a for a in L.allowances if a.outcome.kind == "quota" and a.period == "daily"
                   and L.in_force(a) and subj.covers(a.scope) and a.derived_from is None]
        if src.rule_id in cited:
            mark(src.rule_id, "multiple")
        elif not covered:
            mark(src.rule_id, "multiple, nothing to multiply")
        elif all(any(d.derived_from is a for d in L.allowances) for a in covered):
            # Every daily number it covers was multiplied by a closer or narrower multiple —
            # the province's ×2 under a region's own possession line.
            mark(src.rule_id, "multiple, beaten by a closer one")
    # Duties: drawn on a row that permits keeping, or nothing here permits.
    for subj, src, _ in L.duties:
        # Drawn on a row that permits keeping its fish — or its fish cannot be kept here (a
        # duty on keeping hatchery steelhead, where all steelhead go back), which the rows
        # say by having no permitting row for it.
        hosts = [r for r in permits if any(subj.contains(f) for f in r.fish)]
        mark(src.rule_id, "duty" if hosts else "duty, nothing to trigger")
    # Lifts: what they lift was lifted here, or they ride as an exemption nobody can place.
    narrow, dropped, unresolved = lifts_here(rules, here)
    lifters = {e.get("lifter") for e in unresolved}
    lifted_ids = dropped | set(narrow)
    for x in rules:
        if not x.get("exempts"):
            continue
        targets = _targets(x, rules)
        if rid(x) in lifters or any(t in lifted_ids for t in targets):
            mark(rid(x), "lifts another rule")
        elif not targets:
            # "Exempt from spring closure" carried onto a stretch whose region has no spring
            # closure to lift: nothing to do here, and the rule says so by naming a target
            # that is not among this stretch's rules.
            mark(rid(x), "lifts a rule not on this stretch")
    # The other kind of water: present in THAT ledger, for the same rules.
    other = "lake" if kind == "stream" else "stream"
    by_key = {rid(x): x for x in rules}
    other_ids = None
    for x in rules:
        w = _water_of(x, by_key)
        if w and w != kind and str(x.get("type") or "") == "retention_limit" and not x.get("method"):
            if other_ids is None:
                O = ledger(rules, other, here, label)
                other_ids = ({a.rule_id for a in O.allowances} | {s.rule_id for _, _, s in O.multiples}
                             | {s.rule_id for _, s, _ in O.duties})
            mark(rid(x), "not-here" if rid(x) in other_ids else None)
    # A way of fishing: present in the gear table's own audit.
    gear = [x for x in rules if x.get("method") or str(x.get("type") or "") in NOT_RETENTION
            and str(x.get("type")) in ("bait_restriction", "tackle_restriction", "method_rule")]
    if gear:
        try:
            from pipeline.regs.table.method_comply import audit as gear_audit
            where, _ = gear_audit(rules, here, kind, label)
            seen = set(where)
        except Exception:                      # the gear table cannot be asked: prove nothing
            seen = set()
        for x in gear:
            if rid(x) in seen:
                mark(rid(x), "by-method")
    # Not a retention rule: on the list, and carrying no count of its own.
    for x in rules:
        t = str(x.get("type") or "")
        if t in NOT_RETENTION and not any(x.get(f) for f in _COUNTS):
            mark(rid(x), "not-a-quota")
    given = {rid(x) for x in rules if x.get("rule")}
    return L, why, sorted(given - set(why))


def _targets(x, rules):
    from pipeline.regs.table.corpus import rule_part
    ids = {rid(r) for r in rules}
    out = set()
    for ex in (x.get("exempts") or []):
        if ex.get("target"):
            out |= {i for i in ids if rule_part(i) == ex["target"]}
        if ex.get("default_id"):
            out |= {i for i in ids if rule_part(i).split(".")[0] == ex["default_id"]}
    return out


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
    print("every rule landed somewhere, and the output says so:")
    for k, v in tally.most_common():
        print(f"   {k:30s} {v:6d}")
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
