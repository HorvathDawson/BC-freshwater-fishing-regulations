"""PROTOTYPE 10 — THE TABLE, WITH ITS WORKING SHOWN.

WHAT IT HANDLES
  Every cell the page would print, beside the book's own sentence that put it there. This is
  the compliance check made READABLE: if a number in the table is not traceable to a quoted
  rule, it is visible here as a number with nothing under it.
"""
import sys
from pipeline.regs.table.build import (build, section_rules, section_regions,
                                       section_label, name, D)

def trace(water, run=0):
    kind = "lake" if (D[water].get("kind") == "lake") else "stream"
    runs = D[water].get("runs") or []
    lbl = (runs[run].get("label") if run < len(runs) else "") or f"stretch {run+1}"
    rules = section_rules(water, run)
    rows = build(rules, kind, section_regions(water, run), section_label(water, run))
    print("\n" + "=" * 86)
    print(f"{water}  ·  {lbl}  ·  {kind}")
    print(f"{len(rules)} rules in  ->  {len(rows)} rows out")
    print("=" * 86)
    for r in rows:
        who, q = r.subject.words(name, is_release=(r.outcome.kind == 'release'))
        print(f"\n  ┌ {who}")
        # A SEASON THAT HEADS A ROW IS PRINTED WITH ITS DATES. The only time one does is when
        # nothing year-round speaks to the fish here (see `resolve(stranded=True)`).
        when = f"  — {r.governs.applies.detail} only" if not r.governs.applies.always else ""
        print(f"  │ KEEP: {r.outcome.word()}    {q}{when}")
        print(f"  │ means: {r.outcome.sentence()}")
        gov = [c for c in r.chain if c.status == "governs"]
        if gov:
            g = gov[0]
            print(f"  │")
            print(f"  │ set by {g.authority} — “{g.verbatim[:70]}”")
        others = [c for c in r.chain if c.status != "governs"]
        if others:
            print(f"  │ behind it:")
            for c in others[:4]:
                # WHOSE RULE IS THIS? Absorption merges the chains of rows about different fish,
                # so a rung under "Trout and char" can be a lake-trout closure or a steelhead
                # season. Shown as an outcome and a status alone it reads as a statement about
                # the whole row — 149 rungs.
                about = ""
                if c.subject != r.subject:
                    w, _ = c.subject.words(name, is_release=(c.outcome.kind == "release"))
                    about = f"  [{w[:26]}]"
                print(f"  │    {c.outcome.word():>7s}  {c.authority:11s} "
                      f"{c.status[:32]}{about}")
                print(f"  │            “{c.verbatim[:62]}”")
        for l in (r.limits or []):
            print(f"  │ OF WHICH: {l.sentence(name)}")
            print(f"  │            “{l.verbatim[:62]}”")
        for g in (r.gates or []):
            tag = f"  ({g.status})" if g.status else ""
            print(f"  │ SIZE: {g.sentence(name, r.subject)}  — {g.authority}{tag}")
            print(f"  │            “{g.verbatim[:62]}”")
        for c in (r.caveats or [])[:3]:
            print(f"  │ BUT: {c.outcome.word()} {c.applies.kind} — {c.applies.detail[:46]}")
            print(f"  │            “{c.verbatim[:62]}”")
        for e in (r.exemptions or [])[:2]:
            print(f"  │ EXCEPT: {e['note'][:60]}")
            print(f"  │            (nothing can draw where — so the rule above still stands)")
        print(f"  └")

if __name__ == "__main__":
    trace(sys.argv[1] if len(sys.argv) > 1 else "Fraser River",
          int(sys.argv[2]) if len(sys.argv) > 2 else 0)
