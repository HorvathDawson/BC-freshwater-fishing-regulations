"""Build a method table from the corpus, and answer it per region."""
from __future__ import annotations
import json, re, sys
from typing import Dict, FrozenSet, List

from pipeline.regs.table.subject import Subject, Origin, Water
from pipeline.regs.table.size import size_of
from pipeline.regs.table.outcome import outcome_of
from pipeline.regs.table.where import parse_where
from pipeline.regs.table.method import MethodRung, resolve_method, ALLOWED, FORBIDDEN
from pipeline.regs.table.lifts import lifts_here
from pipeline.regs.table.corpus import rules as corpus_rules, section_rules, rid

H = open("app/design/regs-v3.html").read()
D = json.loads(re.search(r'<script id="d" type="application/json">(.*?)</script>', H, re.S).group(1))
NAME = D.get("_species") or {}
def nm(c): return NAME.get(c, c)

def _auth(x):
    e = str(x.get("entry") or "")
    if e.startswith("zp:"): return 3, "Provincial"
    if e.startswith("z"):   return 2, "Region " + (e[1:2] if e[1:2].isdigit() else "?")
    return (1, "inherited") if x.get("via") == "trib" else (0, "this water")

def rungs_for(method: str, rules: List[dict], here=frozenset()) -> List[MethodRung]:
    narrow, drop = lifts_here(rules, here)
    out = []
    for x in rules:
        if x.get("method") != method: continue
        if rid(x) in drop: continue                 # disapplied here outright
        rank, who = _auth(x)
        w = parse_where(x.get("extent_text"))
        perm = (ALLOWED if x.get("permitted") else FORBIDDEN) if x.get("permitted") is not None else None
        o = outcome_of(x.get("take"), x.get("may_target"), x.get("unlimited"),
                       x.get("period"), bool(x.get("combined")))
        takes = None
        if o is not None:
            takes = (Subject(frozenset(x.get("species") or []),
                             Origin(x["origin"]) if x.get("origin") else Origin.both,
                             size_of(x.get("over_cm"), x.get("under_cm"), take=x.get("take"),
                                     within=x.get("within"), band=bool(x.get("band")),
                                     period=x.get("period") or "daily"),
                             Water.any, None,
                             # A LIFT IS A SUBTRACTION: whatever an exception takes out of this
                             # rule HERE joins what the rule already excepts.
                             frozenset(x.get("species_except") or [])
                               | narrow.get(rid(x), frozenset())), o)
        con = "" if (perm is not None or takes is not None) else (x.get("label") or "")
        out.append(MethodRung(rid(x), who, rank, w, perm, takes, con,
                              x.get("verbatim") or ""))
    return out

def all_rules() -> List[dict]:
    """FROM THE BUNDLE, not from the page's copy — see `corpus.py`. The page's data has no
    `exempts`, so read from it and the burbot exception does not exist to be applied."""
    return corpus_rules()

def show(method: str, water: str, run: int = 0):
    """ONE STRETCH. Handing the fold the whole corpus let a Region 3 lake's "No Ice Fishing"
    decide the answer on a Region 5 river, because a water-specific rule outranks everything and
    there were forty-four of them in the pile."""
    rules, here = section_rules(water, run)
    region = ", ".join(sorted(here)) or "?"
    row = resolve_method(method, rungs_for(method, rules, here), here)
    print(f"\n{'─'*76}\n  {method.replace('_',' ').upper()}  ·  {water} (Region {region})\n{'─'*76}")
    if row is None:
        print("   (nothing says)"); return
    print(f"   VERDICT: {row.permission.word()}")
    for c in row.chain:
        print(f"      {c.authority:11s} {c.permission.word():18s} {c.status[:40]}")
        print(f"         “{c.verbatim[:62]}”")
    if row.takes:
        print("   WHAT YOU MAY KEEP BY IT:")
        for t in row.takes:
            who, q = t.takes[0].words(nm)
            print(f"      {t.takes[1].word():>8s}  {who[:34]:34s} {q[:22]}")
            print(f"         “{t.verbatim[:62]}”")
    if row.unplaceable:
        print("   ALSO, SOMEWHERE IN HERE:")
        for u in row.unplaceable[:3]:
            print(f"      {u.where.words()[:56]}")

if __name__ == "__main__":
    show(sys.argv[1] if len(sys.argv) > 1 else "spear_fishing",
         sys.argv[2] if len(sys.argv) > 2 else "Fraser River",
         int(sys.argv[3]) if len(sys.argv) > 3 else 0)
