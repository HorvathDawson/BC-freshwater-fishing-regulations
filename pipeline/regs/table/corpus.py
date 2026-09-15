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


def by_id(rs: List[dict]) -> Dict[str, dict]:
    return {r["rule"]: r for r in rs if r.get("rule")}


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
    want = set()
    for x in (w.get("rules") or []):
        sp = x.get("spans") or []
        if sp and not any(abs(a - lo) < .05 and abs(b - hi) < .05 for a, b in sp):
            continue
        want.add(x.get("rule"))
    full = by_id(rules(path))
    return [full[r] for r in want if r in full], frozenset(runs[run].get("regions") or [])
