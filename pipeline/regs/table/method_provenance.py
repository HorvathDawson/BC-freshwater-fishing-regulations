"""EVERY LINE OF THE GEAR TABLE, BACK TO THE SENTENCE THAT PUT IT THERE — as data.

For one stretch: every method's row with every term on it, each term with its provenance on
both axes, the rule it came from, the dates it is live, and — where it does not print as a line
of its own — why. And the closure, from the quota ledger, so the two tables cannot disagree
about whether the water is shut. A page renders it; it computes nothing.

    THE STAGE   every term says whether it is part of the region's standing table or an
                override this section carries on top of it.
    THE HOIST   a condition that prints identically on every hook-and-line row is stated ONCE,
                above them, and no row repeats it — computed here as one loop over topics, so
                the page cannot special-case one of them.
    THE DATE    windows ship as data; each row carries its calendar.
"""
from __future__ import annotations
import json
from typing import Dict, List, Optional

from pipeline.regs.table.build import (D, WATERS, section_rules, section_regions, section_label,
                                       section_kind, name, ledger as quota_ledger)
from pipeline.regs.table.corpus import rid, rules as all_rules
from pipeline.regs.table.ledger import Allowance, Ledger
from pipeline.regs.table.method import (MethodTable, MethodRow, Term, METHODS, NAMES, HOOK_AND_LINE,
                                        RIG_ORDER)
from pipeline.regs.table.method_build import table, base, region_base, provincial_base
from pipeline.regs.table.provenance import src_json, region_label, counter_json
from pipeline.regs.table.rows import rows


def term_json(T: MethodTable, method: str, t: Term, status: str = "") -> dict:
    return {
        "rule": t.rule_id, "stage": "base" if t.is_base else "override",
        "kind": t.kind, "text": t.text, "plain": t.plain(), "topic": t.topic, "key": t.key,
        "allows": t.allows, "only_when": t.only_when, "default": t.is_default,
        "adds": t.adds() if t.kind == "permit" else True,
        "source": src_json(t.source),
        "regions": sorted(t.regions),
        "when": t.applies.detail,
        "windows": [{"from": list(f), "to": list(to)} for f, to in t.applies.windows],
        "unless": t.applies.unless, "within_day": t.applies.within_day,
        "somewhere": t.applies.kind == "somewhere",
        "status": status or T.status_of(method, t),
        "exceptions": [term_json(T, method, c) for c in T.carves.get(method, {}).get(t, [])],
        "prose_extent": t.prose_extent,
    }


def closure_json(a: Allowance) -> dict:
    return {"rule": a.rule_id, "source": src_json(a.source), "when": a.applies.detail,
            "windows": [{"from": list(f), "to": list(t)} for f, t in a.applies.windows],
            "unless": a.applies.unless, "plain": "Closed — no fishing here"}


def keep_json(T: MethodTable, method: str) -> List[dict]:
    """What you may keep by this method — the per-method ledger's rows, in the quota table's
    own words, plus the fish a lift set free."""
    out = []
    L = T.keep(method)
    if L is not None:
        for r in rows(L, name):
            h = r.headline()
            if h is None:
                continue
            # THE ROW ALREADY NAMES THE METHOD, so these words are about the method, not the
            # water. "Game fish — No fishing for this" on the spear row read as a closure on
            # game fish everywhere, when what the book says is that you may not SPEAR them.
            words = {"closed": "May not be taken this way", "release": "Put it back",
                     "unlimited": "Keep as many as you like"}.get(h.kind, f"Keep up to {h.n} a day")
            fish = _keep_heading(r, L)
            out.append({"fish": fish, "members": sorted(name(c) for c in r.fish), "word": words,
                        "kind": h.kind, "rule": h.rule_id, "source": src_json(h.source),
                        "when": h.applies.detail,
                        "windows": [{"from": list(f), "to": list(t)} for f, t in h.applies.windows],
                        "counter": counter_json(L, h, r)})
    for fish, lifter in T.lifted_fish.get(method, []):
        out.append({"fish": ", ".join(sorted(name(c) for c in fish)), "members": sorted(name(c) for c in fish),
                    "word": "You may keep it", "kind": "freed", "rule": lifter.rule_id,
                    "source": src_json(lifter.source), "when": lifter.applies.detail, "windows": [],
                    "counter": None})
    return out


def _keep_heading(r, L: Ledger) -> str:
    """"Game fish, other than burbot" — never "Other all game fish"."""
    h = r.headline()
    who, _ = h.scope.words(name)
    exc = sorted(name(c) for c in h.scope.excepts)
    if h.scope.is_everything:
        who = "Everything"
    who = who.replace("All game fish", "Game fish").replace("All fin fish", "Everything")
    return who + (f", other than {', '.join(exc).lower()}" if exc else "")


def row_json(T: MethodTable, m: str, on: Optional[tuple] = None) -> dict:
    row = MethodRow(T, m)
    gov = row.standing()
    today = row.standing(on) if on else gov
    # THE RIG A DATE SEES. `MethodRow.rig` has always taken a date and this never passed one,
    # so a gear table asked about Oct 20 answered with every seasonal condition in the book —
    # a bait ban that runs Nov 1 – Apr 30 printed as if it were in force in July.
    rig = {topic: [term_json(T, m, t) for t in ts] for topic, ts in row.rig(on).items()}
    return {
        "method": m, "name": row.name, "uses_hook": row.uses_hook(),
        "verdict": row.verdict_word(), "verdict_today": row.verdict_word(on) if on else row.verdict_word(),
        "by": term_json(T, m, gov), "today_by": term_json(T, m, today),
        "calendar": row.calendar(),
        "candidates": [term_json(T, m, t) for t in row.candidates()],
        "conditions": [term_json(T, m, t) for t in row.conditions()],
        "rig": rig,
        "folded": [term_json(T, m, t, st) for t, st in row.folded()],
        "hours": [term_json(T, m, t) for t in T.within_day(m)],
        "keep": keep_json(T, m),
        # ON THE PROVINCE'S TABLE ONLY: the terms the book limits to named regions. They do not
        # govern (a ban for Regions 1, 2 and 4 is not a province-wide ban) but they are the
        # answer to "where may I do this?", and without them the province's spear row reads as
        # a blanket permission. Empty on every regional and section table, where such a rule
        # either bites or is not there at all.
        "where": [term_json(T, m, t) for t in T.region_limited(m)],
        "visible": sorted(row.visible_ids()),
    }


def hoist(rows_: List[dict]) -> dict:
    """ONE LOOP OVER EVERY TOPIC. A condition printed identically — same status, same
    exceptions — on every hook-and-line row is hoisted onto a band above them; rows keep only
    what differs. Written generically so no topic can be the one that was forgotten."""
    hook = [r for r in rows_ if r["uses_hook"]]
    if len(hook) < 2:
        return {"rows": [r["method"] for r in hook], "rig": {}}
    def sig(t):
        return (t["rule"], t["status"], tuple(x["rule"] for x in t["exceptions"]))
    shared: Dict[str, List[dict]] = {}
    for topic in RIG_ORDER:
        first = hook[0]["rig"].get(topic, [])
        for t in first:
            if all(any(sig(u) == sig(t) for u in r["rig"].get(topic, [])) for r in hook[1:]):
                shared.setdefault(topic, []).append(t)
    hoisted = {sig(t) for ts in shared.values() for t in ts}
    for r in hook:
        r["rig_own"] = {topic: [t for t in ts if sig(t) not in hoisted]
                        for topic, ts in r["rig"].items()}
        r["rig_own"] = {k: v for k, v in r["rig_own"].items() if v}
    for r in rows_:
        if not r["uses_hook"]:
            r["rig_own"] = r["rig"]
    return {"rows": [r["method"] for r in hook], "rig": shared}


def rule_of(src: dict, key: str) -> dict:
    x = src.get(key) or {}
    return {"id": key, "entry": key.split("::")[0], "label": x.get("label") or "",
            "verbatim": x.get("verbatim") or "", "extent": x.get("extent_text") or "",
            "type": x.get("type") or "", "method": x.get("method") or ""}


def _finish(T: MethodTable, d: dict, on: Optional[tuple]) -> dict:
    d["rows"] = [row_json(T, m, on) for m in METHODS if T.speaks_about(m)]
    d["band"] = hoist(d["rows"])
    d["closures"] = [closure_json(a) for a in T.closures]
    d["closed"] = closure_json(T.shut()) if T.shut() is not None else None
    d["closed_today"] = closure_json(T.shut(on)) if on and T.shut(on) is not None else None
    d["while_closed"] = [term_json(T, "", t) for t in T.terms if t.kind == "while_closed"]
    src = {rid(x): x for x in all_rules()}
    used = set()
    for r in d["rows"]:
        used |= set(r["visible"])
    d["rules"] = {k: rule_of(src, k) for k in sorted(used)}
    return d


def section(water: str, run: int = 0, on: Optional[tuple] = None) -> dict:
    kind = section_kind(water)
    rules = section_rules(water, run)
    here = section_regions(water, run)
    T = table(rules, kind, here, section_label(water, run))
    B = base(rules, kind, here)
    d = {"water": water, "stretch": run + 1,
         "label": section_label(water, run) or f"stretch {run + 1}",
         "kind": kind, "regions": sorted(here), "on": list(on) if on else None,
         "rules_in": sum(1 for x in rules if x.get("method") or str(x.get("type") or "") in
                         ("bait_restriction", "tackle_restriction")),
         "base": {"rules": sorted(B.universe()), "label": f"{region_label(here)} · {kind}s"}}
    return _finish(T, d, on)


def base_table(region: str, kind: str, on: Optional[tuple] = None,
               extra: Optional[List[dict]] = None) -> dict:
    # "province" IS NOT A REGION. Asked for it by name this built `region_base("province")` —
    # a region whose entry prefix is `zprovince`, which nothing matches — so the caller got a
    # table made of the province's region-less rules only, with no `where` and no set lining.
    if str(region) in ("p", "province"):
        return provincial_table(kind)
    T = region_base(region, kind, extra or ())
    d = {"regions": [region], "kind": kind, "label": f"Region {region.upper()} · {kind}s",
         "rules": sorted(T.universe()), "from": ("region", region),
         "on": list(on) if on else None}
    return _finish(T, d, on)


def provincial_table(kind: str) -> dict:
    T = provincial_base(kind)
    d = {"regions": [], "kind": kind, "label": f"All of B.C. · {kind}s",
         "rules": sorted(T.universe()), "from": ("province", "")}
    return _finish(T, d, None)


def base_tables() -> List[dict]:
    out = [provincial_table("stream"), provincial_table("lake")]
    for reg in ["1", "2", "3", "4", "5", "6", "7a", "7b", "8"]:
        for kind in ("stream", "lake"):
            out.append(base_table(reg, kind))
    return out


if __name__ == "__main__":
    import sys
    w = sys.argv[1] if len(sys.argv) > 1 else "Fraser River"
    r = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    on = tuple(int(x) for x in sys.argv[3].split("-")) if len(sys.argv) > 3 else None
    print(json.dumps(section(w, r, on), indent=1))
