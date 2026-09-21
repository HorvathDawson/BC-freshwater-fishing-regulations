"""Emit the resolved tables into `app/design/regs-v3.html`, so the page computes nothing.

The page used to resolve precedence itself — four ladders in the browser, each a second
opinion on the pipeline's. Resolution now happens once, in `pipeline/regs/table/`, and this
tool writes what it resolved into a second JSON block on the page:

    <script id="t" type="application/json">
      {"quota": {water: [one table per stretch]}, "gear": {water: [one table per stretch]},
       "pool": [every counter and term, once], "rules": {rule id: label, verbatim, ...}}
    </script>

The `d` block (the rules per water, written by `build_regs_v3_data`) is left exactly as it
is: `corpus.section_rules` reads it, and `comply` and the delta invariant read through that.
This block is DERIVED from it — run this after `build_regs_v3_data`, never instead of it.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.tools.emit_regs_v3_tables          # in place
    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.tools.emit_regs_v3_tables --print  # to stdout

Each table is the JSON the two artifacts render (`provenance.section`,
`method_provenance.section`), in the shape their renderers read: rows with every counter and
its typed source, the bands, the closures on part of the stretch, the exemptions nobody can
place, and — for every base counter the stretch does not show — the override that replaced
it. Nothing here is computed on a date: the page picks the day, and every counter carries
its windows.

SIZE. Written naively the 102 tables are 22 MB, because a region's counter is repeated on
every row of every stretch in the region. Every counter, term, size list and gear row is
therefore written ONCE into `pool`, by content, and rows carry indices; the page rehydrates
on load. That is a 6 MB block. Fields no renderer reads (`carves`, `says`, `means`, the
per-date answers) are not written.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

from pipeline.common.curated import REPO_ROOT

BLOCK = re.compile(r'(<script id="t" type="application/json">).*?(</script>)', re.S)
AFTER = re.compile(r'(<script id="d" type="application/json">.*?</script>)', re.S)

#: what the quota renderer reads from a counter
COUNTER_FIELDS = {"rule", "keep", "n", "kind", "period", "within", "size", "pooled", "plain", "fish",
                  "reaches", "when", "unless", "windows", "somewhere", "within_day", "derived_from",
                  "multiplier", "multiplied_by", "bc_wide", "status", "source", "qualifier", "shared"}
ROW_DROP = {"means", "live_today", "today_by", "answer_today", "set_by", "year_round"}


def log(*a):
    print(*a, file=sys.stderr)


class Pool:
    """Every distinct object once; a row carries its index."""
    def __init__(self):
        self.items, self.index = [], {}

    def add(self, x) -> int:
        k = json.dumps(x, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        if k not in self.index:
            self.index[k] = len(self.items)
            self.items.append(x)
        return self.index[k]


def _src(s: dict) -> dict:
    return {"tag": s["tag"], "words": s["words"], "verbatim": s["verbatim"], "scope": s["scope"],
            "authority": s.get("authority"), "rank": s.get("rank", 0)}


def compact_quota(d: dict, pool: Pool) -> dict:
    def counter(c):
        c = {k: v for k, v in c.items() if k in COUNTER_FIELDS}
        c["source"] = _src(c["source"])
        return pool.add(c)
    rows = []
    for r in d["rows"]:
        r = {k: v for k, v in r.items() if k not in ROW_DROP}
        r["counters"] = [counter(c) for c in r["counters"]]
        r["behind"] = [counter(c) for c in r["behind"]]
        size = [dict(x, source=_src(x["source"]) if x.get("source") else None) for x in r["size"]]
        r["size"] = pool.add(size) if size else -1
        rows.append(r)
    return {"water": d["water"], "stretch": d["stretch"], "label": d["label"], "kind": d["kind"],
            "regions": d["regions"], "base": d["base"], "rows": rows,
            "somewhere": [counter(c) for c in d["somewhere"]],
            "exemptions": d["exemptions"], "while_closed": d["while_closed"], "present": d["present"]}


def compact_gear(d: dict, pool: Pool) -> dict:
    """The gear artifact's shape: terms once per section, rows hold references."""
    terms = {}

    def ref(t):
        return {"rule": t["rule"] or ("__default__:" + t["kind"]), "status": t["status"],
                "exceptions": [ref(x) for x in t.get("exceptions", [])]}

    def keep_term(t):
        k = t["rule"] or ("__default__:" + t["kind"])
        if k not in terms:
            terms[k] = pool.add({"text": t["text"], "plain": t["plain"], "topic": t["topic"], "key": t["key"],
                                 "allows": t["allows"], "default": t["default"], "kind": t["kind"],
                                 "adds": t.get("adds", True), "source": _src(t["source"]),
                                 "when": t["when"], "windows": t["windows"], "unless": t["unless"],
                                 "within_day": t["within_day"], "somewhere": t["somewhere"]})
        for x in t.get("exceptions", []):
            keep_term(x)
        return ref(t)

    rows = []
    for r in d["rows"]:
        rig = {k: [keep_term(t) for t in v] for k, v in r["rig"].items()}
        rig_own = {k: [keep_term(t) for t in v] for k, v in r.get("rig_own", r["rig"]).items()}
        rows.append(pool.add({
            "method": r["method"], "name": r["name"], "uses_hook": r["uses_hook"],
            "verdict": r["verdict"], "by": keep_term(r["by"]), "calendar": r["calendar"],
            "conditions": [keep_term(t) for t in r["conditions"]],
            "rig": rig, "rig_own": rig_own,
            "folded": [dict(keep_term(t), status=t["status"]) for t in r["folded"]],
            "hours": [keep_term(t) for t in r["hours"]],
            "keep": [{"fish": k["fish"], "word": k["word"], "kind": k["kind"], "source": _src(k["source"]),
                      "when": k["when"], "windows": k["windows"]} for k in r["keep"]]}))
    band = {"rows": d["band"]["rows"], "rig": {k: [keep_term(t) for t in v] for k, v in d["band"]["rig"].items()}}
    closures = [{"rule": c["rule"], "when": c["when"], "windows": c["windows"], "unless": c["unless"],
                 "source": _src(c["source"])} for c in d["closures"]]
    return {"water": d["water"], "stretch": d["stretch"], "label": d["label"], "kind": d["kind"],
            "regions": d["regions"], "base": d["base"], "rows": rows, "band": band, "terms": terms,
            "closures": closures, "closed": {"rule": d["closed"]["rule"]} if d["closed"] else None,
            "while_closed": [keep_term(t)["rule"] for t in d["while_closed"]]}


def tables() -> dict:
    from pipeline.regs.table.build import D, WATERS
    from pipeline.regs.table import provenance, method_provenance
    pool, rules = Pool(), {}
    out = {"quota": {}, "gear": {}}
    for w in WATERS:
        runs = D[w].get("runs") or []
        out["quota"][w], out["gear"][w] = [], []
        for i in range(len(runs)):
            q = provenance.section(w, i)
            g = method_provenance.section(w, i)
            rules.update(q.pop("rules")); rules.update(g.pop("rules"))
            out["quota"][w].append(compact_quota(q, pool))
            out["gear"][w].append(compact_gear(g, pool))
        log(f"  {w}: {len(runs)} stretch(es)")
    out["pool"], out["rules"] = pool.items, rules
    return out


def blob_of(t: dict) -> str:
    return json.dumps(t, separators=(",", ":"), ensure_ascii=False).replace("</", "<\\/")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--html", type=Path, default=REPO_ROOT / "app" / "design" / "regs-v3.html")
    ap.add_argument("--print", dest="to_stdout", action="store_true")
    a = ap.parse_args()
    blob = blob_of(tables())
    log(f"  {len(blob) / 1e6:.1f} MB of tables")
    if a.to_stdout:
        print(blob)
        return 0
    html = a.html.read_text(encoding="utf-8")
    new, n = BLOCK.subn(lambda m: m.group(1) + blob + m.group(2), html, count=1)
    if not n:
        new, n = AFTER.subn(lambda m: m.group(1) + '\n<script id="t" type="application/json">' + blob + "</script>", html, count=1)
    if not n:
        log('✗ no <script id="d"> block in the html — nothing written')
        return 1
    a.html.write_text(new, encoding="utf-8")
    log(f"  wrote {a.html}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
