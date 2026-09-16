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
from typing import List

BUNDLE = "data/generated/bundle/bundle.sqlite"


_CATALOGUE = None


def catalogue() -> dict:
    """`(entry_id, rule_id)` -> the rule's `extents` and its entry's display name and region.

    THE BUNDLE DROPS `extents`. It keeps `scope` — `section` or `area` — which says a rule was
    written against a place or an area, and nothing more: it cannot tell "within Region 4"
    from "within Management Units 1-1 to 1-6", and those are the two things the ladder has to
    tell apart, because the first is the region's standing table and the second is an override
    on a handful of streams. The extents say which, and they are read from the catalogue the
    bundle was built from — curated data, through `CURATED`, never as a literal path.
    """
    global _CATALOGUE
    if _CATALOGUE is None:
        from pipeline.common.curated import CURATED
        idx = {}
        for f in sorted(CURATED.regulations.entries.catalogue.glob("region-*.json")):
            d = json.load(open(f))
            for e in (d.get("entries") if isinstance(d, dict) else d) or []:
                eid = e.get("entry_id")
                for r in e.get("rules") or []:
                    idx[(eid, r.get("rule_id"))] = {
                        "extents": list(r.get("extents") or e.get("extents") or []),
                        "entry_name": e.get("display_name") or e.get("name") or "",
                        "entry_region": str(e.get("region") or "")}
        _CATALOGUE = idx
    return _CATALOGUE


def rules(path: str = BUNDLE) -> List[dict]:
    """Every rule, flattened the way the page's own data is flattened — conditions hoisted to
    the top level — so one shape serves both readers. Joined with the catalogue for the one
    field the bundle drops, `extents` (see `catalogue`)."""
    db = sqlite3.connect(path)
    cols = [r[1] for r in db.execute("PRAGMA table_info(rule)")]
    cat = catalogue()
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
        d.update(cat.get((d["entry"], d["rule"]), {"extents": [], "entry_name": "",
                                                   "entry_region": ""}))
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
    #
    # AN EMPTY SPAN LIST BINDS NO STRETCH. The page's own filter (`runRules`) takes a rule
    # onto a stretch only where one of its spans overlaps it, so `spans: []` reaches nothing.
    # Read here as "no filter, take it everywhere", the tributary-walk copy of the Atnarko's
    # "No Fishing from Tenas Lake to the Atnarko Park campsite" — whose reach copy binds three
    # stretches — shut all six, and the Bella Coola with them.
    #
    # AND THE WALK IS REMEMBERED. `via: trib` lives on the page's copy of a rule and not in
    # the bundle, and this join threw it away — so "Elk River's tributaries" spoke on the
    # Fording with the Fording's own voice, at rank 0, when the ladder has a rung for it.
    want, inherited = set(), {}
    for x in (w.get("rules") or []):
        sp = x.get("spans") or []
        if not any(abs(a - lo) < .05 and abs(b - hi) < .05 for a, b in sp):
            continue
        k = f"{x.get('entry')}::{x.get('rule')}"
        want.add(k)
        inherited[k] = inherited.get(k, True) and x.get("via") == "trib"
    out = []
    for x in rules(path):
        if rid(x) in want:
            x = dict(x)
            x["via"] = "trib" if inherited[rid(x)] else "reach"
            out.append(x)
    return out, frozenset(runs[run].get("regions") or [])
