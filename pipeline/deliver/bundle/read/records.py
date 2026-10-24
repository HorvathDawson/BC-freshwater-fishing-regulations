"""The rules, as the bundle ships them: `rules(bundle)` — every rule with `conditions` hoisted to the top
level — and `rules_from(db)`, the same from an open connection. `BUNDLE` is the live bundle's path."""
from __future__ import annotations

import json
import sqlite3
from typing import List
from pipeline.common.curated import GENERATED


BUNDLE = str(GENERATED.bundle / "bundle.sqlite")


def rules(path: str = BUNDLE) -> List[dict]:
    """Every rule in the bundle, flattened: its columns, with `conditions` hoisted to the top level
    and the JSON columns decoded, plus `entry`/`rule` (the pair that is its identity — see `rid`)
    and `entry_name`. A rule's `extents` are its own; nothing carries its entry's beside them.

    `exempts` is a COLUMN (resolved at build to the entry each lift reaches); a bundle from before
    it would read as a corpus in which nothing lifts anything, so it is refused."""
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        return rules_from(db, path)
    finally:
        db.close()


#: Columns a reader never sees on a rule dict: decided once at bundle time from the rule's own
#: fields (`rule.closure_grade`, read through `rules.closure_grade` everywhere in Python).
_DERIVED_COLUMNS = ("closure_grade",)


def rules_from(db: sqlite3.Connection, where: str = "the bundle") -> List[dict]:
    """`rules` on an open connection (the bundle builder reads its own rows back through this:
    `derived.write`)."""
    cols = [r[1] for r in db.execute("PRAGMA table_info(rule)")]
    if "exempts" not in cols:
        raise SystemExit(f"read.rules: {where} has no `rule.exempts` column — rebuild the "
                         f"bundle (`python -m pipeline.deliver.bundle`)")
    names = dict(db.execute("SELECT entry_id, name FROM entry"))
    out = []
    for row in db.execute("SELECT * FROM rule"):
        d = dict(zip(cols, row))
        for c in _DERIVED_COLUMNS:
            d.pop(c, None)
        cond = json.loads(d.pop("conditions") or "{}")
        if "exempts" in cond:
            raise SystemExit(f"read.rules: {d['entry_id']}::{d['rule_id']} carries `exempts` "
                             f"in `conditions` as well as in its column")
        for k in ("species", "species_except"):
            d[k] = json.loads(d[k]) if d.get(k) else []
        d.update(cond)
        for col, name in (("when_", "when"), ("while_", "while"), ("exempts", "exempts")):
            v = d.pop(col, None)
            if v:
                d[name] = json.loads(v)
        if d.pop("standing", 0):
            d["standing"] = True
        d["rule"], d["entry"] = d["rule_id"], d["entry_id"]
        d["entry_name"] = names.get(d["entry_id"]) or ""
        d.setdefault("extents", [])
        out.append(d)
    return out


def rid(x: dict) -> str:
    """The identity of a rule, which is NOT its `rule_id`: 408 rules share 140 rule ids
    (`species_quotas.r1` is nine rules, one per region). Only `(entry, rule)` is unique."""
    return f"{x['entry']}::{x['rule']}"
