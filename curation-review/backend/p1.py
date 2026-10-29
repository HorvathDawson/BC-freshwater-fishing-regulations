"""THE P1 PASS, MADE FAST (owner 2026-10-28): the closures with a risk factor (`tiers` P1), split in two.

    view()   counts (by risk factor, by the second check), the QUICK-CONFIRM rows — every check
             agrees — each with its closure text and the second check's reasons, and the rest of
             P1 in risk order (no match / unresolved first)

A row is QUICK when every machine check agrees with what ships: not marked yet, the entry built
(`vintage` current), its split proposals pre-verified ✓✓ (`preverify.entry`) with every builder pick
the proposal's own, no severity-A pre-check but the ambiguous cut that check settles (no stale split
`split_to_verify`, nothing unresolved / unplaced / unbound, no parser doubt, no gate failure), no
quote off the row, no unused curated split, no low-confidence cut, not awaiting a water. Whether each
closure rule binds sections is the reach builder's (`/api/entries/{id}/reaches`), asked per card by
the screen — a closure that binds nothing is never quick.

READ ONLY. Nothing here marks: the owner's key on the screen posts the normal mark (`POST /api/marks`).
"""
from __future__ import annotations

from collections import Counter

import awaiting_water
import preverify
import reuse

#: the severity-A pre-checks a pre-verified proposal settles; any other A finding needs the full review
SETTLED_BY_PV = frozenset({"ambiguous_cut"})
#: the B pre-checks that are a disagreement (the row's text), not a decision already taken
B_DISAGREE = frozenset({"quote_not_in_row", "unquoted_text"})
OPEN = ("unmarked",)


def _closure_rules(e: dict) -> list[dict]:
    from pipeline.deliver.bundle.rules import closure_grade
    return [{"rule_id": r.get("rule_id"), "type": r.get("type"), "label": r.get("label"), "verbatim": r.get("verbatim"),
             "closure": bool(closure_grade(r))}
            for r in e.get("rules") or [] if isinstance(r, dict)]


def _proposals(entry_id: str) -> list[dict]:
    """The entry's split proposals, compactly: what the second check said, and the builder's pick."""
    out = []
    for i in preverify.entry_items(entry_id):
        pv = i.get("preverify") or {}
        out.append({"rule_id": i.get("rule_id"), "kind": i.get("kind"), "duplicate_of": i.get("duplicate_of"),
                    "words": (i.get("suggestion") or {}).get("words"), "reason": i.get("reason"),
                    "status": pv.get("status"), "why": pv.get("why"), "method": pv.get("method"),
                    "pick": i.get("pick")})
    return out


def why_not_quick(row: dict, findings: list[dict], props: list[dict]) -> list[str]:
    """Every reason the row is NOT a quick confirm ([] = quick, pending the per-card binding check)."""
    why = []
    if row["verify"] not in OPEN:
        why.append(f"mark {row['verify']}")
    if row.get("vintage") != "current":
        why.append("awaiting build")
    st = (row.get("pv") or {}).get("status")
    if st != "pre-verified":
        why.append(f"second check: {st or 'no proposal'}")
    if any(p.get("pick") and not p["pick"].get("agrees") for p in props):
        why.append("a proposal's pick is not the builder's")
    if any(p.get("kind") != "ambiguous_cut" for p in props):
        why.append("an open proposal other than a cut pick")
    a = sorted({f["code"] for f in findings if f["severity"] == "A" and f["code"] not in SETTLED_BY_PV})
    why += [f"pre-check {c}" for c in a]
    why += [f"pre-check {c}" for c in sorted({f["code"] for f in findings} & B_DISAGREE)]
    if row.get("unused_curated_splits"):
        why.append("unused curated split")
    if row.get("low_cuts"):
        why.append("low-confidence cut")
    if row.get("t89"):
        why.append("awaiting water")
    return why


def view() -> dict:
    from pipeline.regs.review import tiers as T
    rows = reuse.queue(tier="P1", order="risk")
    entries = {e["entry_id"]: e for _, e in reuse._all_entries()}
    run, to_verify = reuse.run_facts(), reuse.splits_to_verify()
    todo = [r for r in rows if r["verify"] != "verified"]
    quick, rest, blocked, by_pv = [], [], Counter(), Counter()
    for r in todo:
        e = entries.get(r["entry_id"]) or {}
        r = {**r, "pv": preverify.entry(r["entry_id"]), "t89": awaiting_water.flag(r["entry_id"]),
             "low_cuts": reuse.low_cuts_of(r["entry_id"]) or None}
        by_pv[(r["pv"] or {}).get("status") or "no proposal"] += 1
        props = _proposals(r["entry_id"])
        # the gate only for a row that is otherwise quick (it is the costly check)
        findings = reuse.entry_prechecks(e, run=run, to_verify=to_verify)
        why = why_not_quick(r, findings, props)
        if not why:
            findings = reuse.entry_prechecks(e, run=run, to_verify=to_verify, gate=True)
            why = why_not_quick(r, findings, props)
        blocked.update(why)
        base = {k: r.get(k) for k in ("entry_id", "name", "region", "mus", "page", "verify", "risks",
                                      "severity", "severity_reasons", "pv", "matched_item_id",
                                      "matched_item_name", "also_item_ids")}
        if why:
            rest.append({**base, "why_not_quick": why, "_rank": (
                T.RISKS.index(r["risks"][0]) if r["risks"] else len(T.RISKS),
                -sum(1 for f in findings if f["severity"] == "A"))})
        else:
            quick.append({**base, "regs_verbatim": e.get("regs_verbatim"), "rules": _closure_rules(e),
                          "proposals": [p for p in props if not p.get("duplicate_of")],
                          "notes": [f"{f['code']}: {f['what']}" for f in findings if f["severity"] != "A"]})
    rest.sort(key=lambda x: x["_rank"])          # stable: the queue's own order within a risk
    for x in rest:
        x.pop("_rank")
    return {
        "counts": {
            "total": len(rows), "todo": len(todo), "quick": len(quick),
            "by_verify": dict(Counter(r["verify"] for r in rows)),
            "by_first_risk": dict(Counter(r["risks"][0] if r["risks"] else "none" for r in todo)),
            "by_any_risk": dict(Counter(k for r in todo for k in r["risks"])),
            "by_pv": dict(by_pv),
            "not_quick_because": dict(blocked.most_common()),
        },
        "quick": quick,
        "rest": rest,
    }
