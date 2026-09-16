"""EVERY CELL, BACK TO THE SENTENCE THAT PUT IT THERE — as data, for one section.

The tables are only trustworthy if a number can be traced to the book. This emits, for one
stretch, the derived rows with every counter on them, each counter with its provenance on both
axes (who wrote it, what it binds to), the rule it came from, the dates it is live, and — where
it does not bind — why. A page renders it; it computes nothing.

    THE STAGE. Every counter says whether it is part of the region's standing table (`base`)
    or an override this section carries on top of it.

    THE DATE. A counter's window ships as data, so the page can answer for any day, and each
    row carries its calendar — every stretch of the year with the headline in force.

    THE IDENTITY. `entry::rule`, because a bare rule id is shared by up to nine rules.
"""
from __future__ import annotations
import json
from typing import Optional

from pipeline.regs.table.build import (ledger, base, section_rules, section_regions,
                                       section_label, section_kind, name, D)
from pipeline.regs.table.corpus import rid, rules as all_rules
from pipeline.regs.table.ledger import Allowance, Ledger
from pipeline.regs.table.rows import rows, Row


def _win(w) -> Optional[dict]:
    if not isinstance(w, dict):
        return None
    f, t = w.get("from") or {}, w.get("to") or {}
    return {"from": [f.get("month"), f.get("day")], "to": [t.get("month"), t.get("day")]}


def counter_json(L: Ledger, a: Allowance, row: Optional[Row] = None, status: str = "") -> dict:
    who, qual = a.scope.words(name)
    return {
        "rule": a.rule_id,
        "stage": "base" if a.source.is_base else "override",
        "kind": a.kind, "keep": a.word(), "n": a.n, "period": a.period, "pooled": a.pooled,
        "fish": who, "qualifier": qual, "size": a.scope.size.words(),
        "says": a.sentence(name),
        "source": src_json(a.source),
        "when": a.applies.detail, "windows": [{"from": list(f), "to": list(t)}
                                              for f, t in a.applies.windows],
        "unless": a.applies.unless, "within_day": a.applies.within_day,
        "somewhere": a.applies.kind == "somewhere",
        "within": a.within,
        "derived_from": a.derived_from.rule_id if a.derived_from else None,
        "multiplier": a.multiplier,
        "multiplied_by": a.multiplied_by.rule_id if a.multiplied_by else None,
        "carves": [{"rule": c.rule_id, "words": c.source.words(), "fish": c.scope.words(name)[0],
                    "when": c.applies.detail} for c in L.carves.get(a, [])],
        "status": status or L.status.get(a, ""),
        "moot": bool(row and row.moot(a)),
    }


def src_json(s) -> dict:
    return {"authority": s.authority.value, "scope": s.scope.value, "region": s.region,
            "place": s.place, "who": s.who, "tag": s.tag, "words": s.words(), "rank": s.rank,
            "rule": s.rule_id, "verbatim": s.verbatim.strip()}


def row_json(L: Ledger, r: Row, on: Optional[tuple] = None) -> dict:
    """One row: the fish, the size statement, and every counter — the shape a page lays out
    as Fish · Size · Daily · Annual · Possession, and a text table prints the same way."""
    head = r.headline()
    today = r.headline(on) if on else head
    cal = r.calendar()
    counters = [counter_json(L, a, r) for a in r.counters]
    behind = [counter_json(L, a, r, st) for a, st in r.behind]
    size = [{"says": x["says"], "kind": x["kind"], "rule": x["rule"], "n": x["n"],
             "shared": x["shared"], "source": src_json(x["source"]) if x["source"] else None}
            for x in r.size(on, name)]
    # A POOLED HEADLINE IS A GROUP. Rows whose number is one shared counter are one band on
    # the page, so five rows never read as five fives.
    group = (head.rule_id if head is not None and head.pooled
             and len(head.scope.effective()) > len(r.fish) else None)
    return {
        "fish": sorted(r.fish), "members": sorted(name(c) for c in r.fish),
        "heading": r.heading(name), "qualifier": r.qualifier(), "origin": r.origin.value,
        "key": r.heading(name) + "||" + r.qualifier(),
        "keep": head.word() if head else None,
        "set_by": head.rule_id if head else None,
        "group": group,
        "province_only": r.province_only,
        "size": size,
        "means": head.outcome.sentence() if head else "no standing number — see the calendar",
        "answer_today": today.word() if today else None,
        "today_by": today.rule_id if today else None,
        "live_today": [a.rule_id for a in r.live(on)] if on else None,
        "calendar": cal,
        "year_round": any(seg["rule"] == (head.rule_id if head else None) for seg in cal),
        "counters": counters, "behind": behind,
    }


def section(water: str, run: int = 0, on: Optional[tuple] = None) -> dict:
    """One section's table with its whole chain of custody, optionally as of (month, day)."""
    kind = section_kind(water)
    rules = section_rules(water, run)
    L = ledger(rules, kind, section_regions(water, run), section_label(water, run))
    B = base(rules, kind)
    src = {rid(x): x for x in all_rules()}

    def rule_of(key: str) -> dict:
        x = src.get(key) or {}
        return {"id": key, "entry": key.split("::")[0], "label": x.get("label") or "",
                "verbatim": x.get("verbatim") or "",
                "windows": [w for w in (_win(o) for o in (x.get("windows") or [])) if w],
                "unless": str(x.get("windows_are") or "") == "excepts",
                "within_day": bool(x.get("from_time") or x.get("to_time") or x.get("weekdays")),
                "extent": x.get("extent_text") or "", "type": x.get("type") or ""}

    out, used = [], set()
    for r in rows(L, name):
        head = r.headline()
        d = row_json(L, r, on)
        for c in d["counters"] + d["behind"]:
            used.add(c["rule"])
            if c["multiplied_by"]: used.add(c["multiplied_by"])
        d["duties"] = [{"rule": s.rule_id, "says": text, "source": s.words(), "tag": s.tag}
                       for subj, s, text in L.duties
                       if head is not None and not head.is_zero
                       and (subj.covers(r_subject(r)) or any(subj.contains(f, r.origin if r.origin.value != "both" else None) for f in r.fish))]
        out.append(d)
    somewhere = [counter_json(L, a) for a in L.allowances if a.applies.kind == "somewhere"]
    used |= {c["rule"] for c in somewhere}
    while_closed = [rule_of(rid(x)) for x in rules
                    if x.get("permitted") is False
                    and "no fishing period" in (x.get("verbatim") or "").lower()]
    return {"water": water, "stretch": run + 1,
            "label": section_label(water, run) or f"stretch {run + 1}",
            "kind": kind, "regions": sorted(section_regions(water, run)),
            "on": list(on) if on else None,
            "rules_in": len(rules), "rows_out": len(out),
            "base": {"rules": sorted(a.rule_id for a in B.allowances if a.derived_from is None),
                     "label": f"{region_label(section_regions(water, run))} · {kind}s"},
            "rows": out,
            "somewhere": somewhere,
            "exemptions": list(L.exemptions),
            "while_closed": while_closed,
            "rules": {k: rule_of(k) for k in sorted(used)}}


def r_subject(r: Row):
    from pipeline.regs.table.subject import Subject
    return Subject(r.fish, r.origin)


def region_label(regions) -> str:
    from pipeline.regs.table.authority import region_words
    return region_words(regions) if regions else "?"


def base_table(water: str, run: int) -> dict:
    """STAGE 1, printable: the standing table a section draws on, as the book prints it — one
    row per fish the region's rules treat alike."""
    kind, rules = section_kind(water), section_rules(water, run)
    regions = section_regions(water, run)
    B = base(rules, kind)
    out = [row_json(B, r) for r in rows(B, name)]
    return {"regions": sorted(regions), "kind": kind,
            "label": f"{region_label(regions)} · {kind}s",
            "rules": sorted(a.rule_id for a in B.allowances if a.derived_from is None),
            "rows": out, "from": (water, run)}


def base_tables() -> list:
    """Every distinct standing table the shipped sections draw on — keyed by the RULE SET,
    not the region id: Haida Gwaii is administratively Region 1 and draws on a different
    table from the rest of it, and a boundary stretch draws on two regions at once."""
    seen, out = set(), []
    for w in D:
        if w.startswith("_"): continue
        for run in range(len(D[w].get("runs") or [])):
            rules = section_rules(w, run)
            if not rules: continue
            key = (frozenset(a.rule_id for a in base(rules, section_kind(w)).allowances),
                   section_kind(w))
            if key in seen: continue
            seen.add(key)
            t = base_table(w, run)
            t["sections"] = sum(1 for w2 in D if not w2.startswith("_")
                                for r2 in range(len(D[w2].get("runs") or []))
                                if section_rules(w2, r2) and section_kind(w2) == key[1]
                                and frozenset(a.rule_id for a in base(section_rules(w2, r2), key[1]).allowances) == key[0])
            out.append(t)
    return sorted(out, key=lambda t: (t["regions"], t["kind"], t["from"]))


if __name__ == "__main__":
    import sys
    w = sys.argv[1] if len(sys.argv) > 1 else "Fraser River"
    r = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    on = tuple(int(x) for x in sys.argv[3].split("-")) if len(sys.argv) > 3 else None
    print(json.dumps(section(w, r, on), indent=1))
