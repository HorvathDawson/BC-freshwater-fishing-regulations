"""PROTOTYPE 10 — THE TABLE, WITH ITS WORKING SHOWN.

WHAT IT HANDLES
  Every cell the page would print, beside the book's own sentence that put it there. This is
  the compliance check made READABLE: if a number in the table is not traceable to a quoted
  rule, it is visible here as a number with nothing under it.
"""
import sys
from pipeline.regs.table.build import build, section_rules, section_regions, name, D

def trace(water, run=0):
    kind = "lake" if (D[water].get("kind") == "lake") else "stream"
    runs = D[water].get("runs") or []
    lbl = (runs[run].get("label") if run < len(runs) else "") or f"stretch {run+1}"
    rules = section_rules(water, run)
    rows = build(rules, kind, section_regions(water, run))
    print("\n" + "=" * 86)
    print(f"{water}  ·  {lbl}  ·  {kind}")
    print(f"{len(rules)} rules in  ->  {len(rows)} rows out")
    print("=" * 86)
    for r in rows:
        who, q = r.subject.words(name, is_release=(r.outcome.kind == 'release'))
        print(f"\n  ┌ {who}")
        print(f"  │ KEEP: {r.outcome.word()}    {q}")
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
                print(f"  │    {c.outcome.word():>7s}  {c.authority:11s} {c.status[:40]}")
                print(f"  │            “{c.verbatim[:62]}”")
        for l in (r.limits or []):
            print(f"  │ OF WHICH: {l.sentence(name)}")
            print(f"  │            “{l.verbatim[:62]}”")
        for c in (r.caveats or [])[:3]:
            print(f"  │ BUT: {c.outcome.word()} {c.applies.kind} — {c.applies.detail[:46]}")
            print(f"  │            “{c.verbatim[:62]}”")
        print(f"  └")

if __name__ == "__main__":
    trace(sys.argv[1] if len(sys.argv) > 1 else "Fraser River",
          int(sys.argv[2]) if len(sys.argv) > 2 else 0)
