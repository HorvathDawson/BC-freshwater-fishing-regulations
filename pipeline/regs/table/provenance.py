"""EVERY CELL, BACK TO THE SENTENCE THAT PUT IT THERE — as data, for one section on one day.

The tables are only trustworthy if a number can be traced to the book. `trace` prints that for a
person reading a terminal; this emits it, so a page can let a reader point at a row and see the
rules that produced it, the order they were weighed in, and which of them are speaking on the
date being viewed.

Three things it carries that the terminal view does not:

    THE DATE. A rung's window decides whether it is the answer TODAY. The pipeline cannot know
    the day, so every rung ships with its window as data and the answer is resolved per date
    here, which is what a date control on the page will do.

    THE ENTRY. A rule's `verbatim` is one sentence; the reader wants the passage it came from,
    which is the entry's own words. Both ship, so a row can be shown against the paragraph a
    person would find in the synopsis.

    THE IDENTITY. `entry::rule` — because a bare rule id is shared by up to nine different rules
    and anything keyed on it merges them.
"""
from __future__ import annotations
import json
from typing import Optional

from pipeline.regs.table.build import (build, section_rules, section_regions,
                                       section_label, name, D)
from pipeline.regs.table.corpus import rid, rules as all_rules
from pipeline.regs.table.resolve import head_of


def _win(w) -> Optional[dict]:
    if not isinstance(w, dict):
        return None
    f, t = w.get("from") or {}, w.get("to") or {}
    return {"from": [f.get("month"), f.get("day")], "to": [t.get("month"), t.get("day")]}


def _in_window(win: dict, month: int, day: int) -> bool:
    (fm, fd), (tm, td) = win["from"], win["to"]
    if None in (fm, fd, tm, td):
        return True
    here, lo, hi = (month, day), (fm, fd), (tm, td)
    return lo <= here <= hi if lo <= hi else (here >= lo or here <= hi)


def section(water: str, run: int = 0, on: Optional[tuple] = None) -> dict:
    """One section's table with its whole chain of custody, optionally as of (month, day)."""
    kind = "lake" if (D[water].get("kind") == "lake") else "stream"
    rules = section_rules(water, run)
    rows = build(rules, kind, section_regions(water, run), section_label(water, run))
    src = {rid(x): x for x in all_rules()}

    def rule_of(key: str) -> dict:
        x = src.get(key) or {}
        return {"id": key,
                "entry": key.split("::")[0],
                "label": x.get("label") or "",
                "verbatim": x.get("verbatim") or "",
                "windows": [w for w in (_win(o) for o in (x.get("windows") or [])) if w],
                "extent": x.get("extent_text") or "",
                "type": x.get("type") or ""}

    def live(rung) -> bool:
        if on is None or rung.applies.kind != "window":
            return True
        wins = rule_of(rung.rule_id)["windows"]
        return not wins or any(_in_window(w, *on) for w in wins)

    out = []
    for r in rows:
        who, qual = r.subject.words(name, is_release=(r.outcome.kind == "release"))
        chain = [{"rule": c.rule_id, "authority": c.authority, "rank": c.rank,
                  "outcome": c.outcome.word(), "why": c.status,
                  "when": c.applies.detail if c.applies.kind != "always" else "",
                  "about": c.subject.words(name)[0],
                  "live": live(c)}
                 for c in r.chain]
        # ON A DATE, THE ANSWER IS RE-DECIDED, NOT RE-FILTERED.
        #
        # The chain leads with the YEAR-ROUND answer, because that is the one the pipeline can
        # know. Taking the first LIVE rung therefore always hands back that same head, and a
        # seasonal closure that outranks it never takes over — the Fording read "keep 2" on
        # 15 September with its own Sep 1 – Oct 31 closure sitting live two lines below.
        #
        # So the live rungs are re-weighed by the same rule that weighed them in the first
        # place: one winner per subject, then `head_of`. The page does not re-implement the
        # ladder; it hands the day back to the ladder.
        if on is None:
            answer = chain[0] if chain else None
        else:
            live_rungs = [c for c, d in zip(r.chain, chain) if d["live"]]
            best = {}
            for rg in live_rungs:
                cur = best.get(rg.subject)
                if cur is None or (rg.rank, rg.outcome.rank) < (cur.rank, cur.outcome.rank):
                    best[rg.subject] = rg
            won = head_of(list(best.values())) if best else None
            answer = next((d for c, d in zip(r.chain, chain) if c is won), None)
        out.append({
            "fish": who, "qualifier": qual,
            "keep": r.outcome.word(), "means": r.outcome.sentence(),
            "answer_today": (answer or {}).get("outcome"),
            "set_by": (answer or {}).get("rule"),
            "chain": chain,
            "of_which": [{"rule": l.rule_id, "says": l.sentence(name)} for l in (r.limits or [])],
            "also": [{"rule": c.rule_id, "outcome": c.outcome.word(),
                      "per": c.outcome.period} for c in (r.ceilings or [])],
            "somewhere": [{"rule": c.rule_id, "where": c.applies.detail}
                          for c in (r.caveats or [])],
            "except": list(r.exemptions or []),
        })

    used = sorted({c["rule"] for row in out for c in row["chain"]}
                  | {l["rule"] for row in out for l in row["of_which"]}
                  | {c["rule"] for row in out for c in row["somewhere"]})
    return {"water": water, "stretch": run + 1,
            "label": section_label(water, run) or f"stretch {run + 1}",
            "kind": kind, "regions": sorted(section_regions(water, run)),
            "on": list(on) if on else None,
            "rules_in": len(rules), "rows_out": len(out),
            "rows": out, "rules": {k: rule_of(k) for k in used}}


if __name__ == "__main__":
    import sys
    w = sys.argv[1] if len(sys.argv) > 1 else "Fraser River"
    r = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    on = tuple(int(x) for x in sys.argv[3].split("-")) if len(sys.argv) > 3 else None
    print(json.dumps(section(w, r, on), indent=1))
