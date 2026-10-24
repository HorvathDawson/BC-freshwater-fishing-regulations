"""G3: a rule said for SOME of its fish (the page's `sayRule(r, names)` with its "for …")."""
from __future__ import annotations

import json
from typing import Dict, Iterable, List, Sequence, Tuple
from pipeline.deliver.answers.display.rule import plain


#: The roles that DECIDE in a row's ladder (the page's `WINS`): a rule holding one of them for some
#: fish is listed once, as that role, before any losing role it holds for others.
WINS = ("also", "governs", "contains", "narrows", "limit", "floor", "season", "possession_cap",
        "duty", "possession")


def row_sources(frame: dict, row: dict) -> List[Tuple[int, List[str]]]:
    """The rules a row's ladder lists, each with the fish it is listed for, in the row's order
    (the page's `rowSources`): every (rule, role, by) any member's decided answer holds, hatchery
    then wild, gathered per fish of `all_members`; a rule is listed ONCE, as its first deciding
    entry (else its first), for that entry's fish."""
    agg: Dict[tuple, List[str]] = {}
    for S in row["all_members"]:
        R = frame["fish"].get(S)
        if not R:
            continue
        for res in (R.get("hatchery"), R.get("wild")):
            for r, role, by in (res or {}).get("roles") or []:
                fs = agg.setdefault((r, role, by), [])
                if S not in fs:
                    fs.append(S)
    best: Dict[int, List[str]] = {}
    for (r, role, _), fs in sorted(agg.items(), key=lambda p: p[0][1] not in WINS):
        best.setdefault(r, fs)
    return list(best.items())


def subset_asks(frame: dict) -> set:
    """Every (rule, fish subset) a row's or an item's ladder lists a rule for (the page's `forTxt`):
    a rule listed for fewer fish than the row's, in the row's ladder; and in an item's ladder (that
    row's, or the other row holding a cross-referenced item's fish) for the item's fish it holds,
    when it is listed for fish outside the item and not for every one of them. A subset is its fish
    sorted by code (the page finds it by set)."""
    out = set()
    rows = frame["rows"]
    for row in rows:
        n = len(row["all_members"])
        for r, spp in row_sources(frame, row):
            if 0 < len(spp) < n:
                out.add((r, tuple(sorted(spp))))
        for it in row.get("items") or []:
            M = it["members"]
            lads = [row] + [o for o in rows if o is not row and o.get("pool") is not None
                            and all(S in o["members"] for S in M)][:1]
            for lad in lads:
                n = len(lad["all_members"])
                for r, spp in row_sources(frame, lad):
                    sub = tuple(S for S in spp if S in M)
                    if sub and len(spp) < n and len(sub) < len(spp) and set(sub) != set(M):
                        out.add((r, tuple(sorted(sub))))
    return out


def subset_says(x: dict, fish: Sequence[str]) -> dict:
    """{fish, for, plain?}: the rule said for these of its fish (`plain(x, names)`, null where the
    rule has no sentence), and the "for …" the page writes beside a rule with no sentence."""
    from pipeline.deliver.answers.common import lc_names
    names = lc_names(list(fish))
    out = {"fish": list(fish), "for": f"for {names}"}
    if plain(x) is not None:
        got = plain(x, names)
        if got is not None:
            out["plain"] = got
    return out


def add_subsets(facts: List[dict], rules_in_order: List[dict], frames: Iterable[dict]) -> None:
    """G3: each rule's sentences for the fish subsets the rows' ladders list it for, onto its facts
    (`subsets`, sorted by fish)."""
    asks: Dict[int, set] = {}
    seen = set()
    for f in frames:
        s = json.dumps(f, sort_keys=True)
        if s in seen:
            continue
        seen.add(s)
        for r, sub in subset_asks(f):
            asks.setdefault(r, set()).add(sub)
    for r, subs in asks.items():
        facts[r]["subsets"] = [subset_says(rules_in_order[r], sub) for sub in sorted(subs)]
