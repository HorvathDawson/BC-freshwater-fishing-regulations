"""Rules read from the SHIPPED BUNDLE, which is where they are still whole.

The first prototype read the rule list embedded in `app/design/regs-v3.html`, because that is
exactly what the page is handed and it made the comparison honest. It is also LOSSY: the page
builder drops `exempts`, so the one field that expresses "burbot may be speared in Regions 3,
5, 6, 7 and 8" never reaches the browser at all. No amount of work in the page can recover it.

That is an argument for resolving in the pipeline rather than a detail: the resolver should read
the corpus where the corpus is complete, and ship the answer. Here that is `bundle.sqlite`,
whose `rule` table keeps every condition in a JSON column.
"""
from __future__ import annotations
import json, sqlite3
from typing import Dict, List

BUNDLE = "data/generated/bundle/bundle.sqlite"


def rules(path: str = BUNDLE) -> List[dict]:
    """Every rule, flattened the way the page's own data is flattened — conditions hoisted to
    the top level — so one shape serves both readers."""
    db = sqlite3.connect(path)
    cols = [r[1] for r in db.execute("PRAGMA table_info(rule)")]
    out = []
    for row in db.execute("SELECT * FROM rule"):
        d = dict(zip(cols, row))
        cond = json.loads(d.pop("conditions") or "{}")
        for k in ("species", "species_except", "windows"):
            if isinstance(d.get(k), str):
                try: d[k] = json.loads(d[k])
                except ValueError: d[k] = []
        d.update(cond)
        d["rule"] = d.get("rule_id")
        d["entry"] = d.get("entry_id")
        out.append(d)
    db.close()
    return out


def rid(x: dict) -> str:
    """The identity of a rule, which is NOT its `rule_id`.

    408 rules share 140 rule_ids: `species_quotas.r1` is nine different rules, one per region,
    and `spring_stream_closure.r1` is five, with different windows (Jan 1–Jun 30, Apr 1–Jun 14,
    Apr 1–Jun 30), different regions and different extents. Only `(entry_id, rule_id)` is
    unique. Every map keyed on the bare id silently merges them — the Region 1 kokanee quota
    and the Region 9 one become one rule, and four of the five spring closures cease to exist.
    """
    return f"{x.get('entry') or x.get('entry_id')}::{x.get('rule') or x.get('rule_id')}"


def rule_part(key: str) -> str:
    """The `rule_id` half, for matching `exempts.target` / `within`, which name it bare."""
    return key.split("::", 1)[-1]


def by_id(rs: List[dict]) -> Dict[str, dict]:
    return {rid(r): r for r in rs if r.get("rule")}


def section_rules(water: str, run: int = 0, path: str = BUNDLE):
    """The rules of ONE stretch, as COMPLETE records.

    Two sources, because neither alone is enough: the page data knows which rules fall on which
    stretch (`spans`), and the bundle knows what each rule actually says (`exempts`, which the
    page builder drops). Joining them on rule id gives the real input a section resolver gets,
    and is also a decent argument for the resolver living where the whole record does.

    Returns (rules, regions) — the region ids are what a scoped rule like "Regions 1, 2 and 4"
    is measured against.
    """
    import json, re
    H = open("app/design/regs-v3.html").read()
    D = json.loads(re.search(r'<script id="d" type="application/json">(.*?)</script>',
                             H, re.S).group(1))
    w = D.get(water) or {}
    runs = w.get("runs") or []
    if run >= len(runs):
        return [], frozenset()
    lo, hi = runs[run]["from"], runs[run]["to"]
    # Keyed on (entry, rule) — the bare rule id is shared by up to nine different rules, so
    # matching on it alone pulled Region 7b's bait rule onto a Region 2 river.
    want = set()
    for x in (w.get("rules") or []):
        sp = x.get("spans") or []
        if sp and not any(abs(a - lo) < .05 and abs(b - hi) < .05 for a, b in sp):
            continue
        want.add(f"{x.get('entry')}::{x.get('rule')}")
    out = [x for x in rules(path) if rid(x) in want]
    return out, frozenset(runs[run].get("regions") or [])
