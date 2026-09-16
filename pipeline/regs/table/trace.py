"""THE TABLE, WITH ITS WORKING SHOWN — for a person reading a terminal.

Every counter on every row, beside the book's own sentence that put it there and the two
axes of where it came from. If a number in the table is not traceable to a quoted rule, it is
visible here as a number with nothing under it.
"""
import sys
from pipeline.regs.table.build import (ledger, base, section_rules, section_regions,
                                       section_label, section_kind, name, D)
from pipeline.regs.table.rows import rows


def show(L, title: str, on=None):
    print("\n" + "=" * 92)
    print(title)
    print(f"{sum(1 for a in L.allowances if a.derived_from is None)} counters")
    print("=" * 92)
    for r in rows(L, name):
        head = r.headline(on)
        q = r.qualifier()
        print(f"\n  ┌ {r.heading(name)}" + (f"  · {q}" if q else ""))
        print(f"  │ KEEP: {head.word() if head else '—'}"
              + (f"    {head.outcome.sentence()}" if head else "    (no standing number)"))
        for a in r.counters:
            tag = " (moot — nothing may be kept)" if r.moot(a, on) else ""
            live = "" if L.binds(a, r.species, r.origin if r.origin.value != "both" else __import__('pipeline.regs.table.subject', fromlist=['Origin']).Origin.wild, None, on) else "  [not today]" if on else "  [in season only]"
            who, ql = a.scope.words(name)
            carved = ", ".join(f"{c.scope.words(name)[0].lower()} → {c.source.who}"
                               for c in L.carves.get(a, []))
            print(f"  │   {a.period[:4]:4s} {a.word():>7s}  {who[:28]:28s} {ql[:30]:30s} "
                  f"{a.source.words()[:44]:44s}{tag}{live}")
            print(f"  │            “{a.source.verbatim.strip()[:70]}”"
                  + (f"  · {a.applies.detail[:30]}" if a.applies.detail else "")
                  + (f"  · other than: {carved}" if carved else ""))
        for a, st in r.behind:
            if a.applies.kind == "somewhere":
                continue
            print(f"  │   ~ {a.word():>7s}  {a.source.words()[:40]:40s} {st[:50]}")
            print(f"  │            “{a.source.verbatim.strip()[:70]}”")
        print(f"  └")
    som = [a for a in L.allowances if a.applies.kind == "somewhere"]
    if som:
        print("\n  somewhere in here, nothing can draw where:")
        for a in som:
            print(f"     {a.word():>7s}  {a.applies.detail[:60]}  ({a.source.who})")


def trace(water, run=0, on=None):
    kind = section_kind(water)
    runs = D[water].get("runs") or []
    lbl = (runs[run].get("label") if run < len(runs) else "") or f"stretch {run+1}"
    rules = section_rules(water, run)
    regions = section_regions(water, run)
    B = base(rules, kind)
    show(B, f"STAGE 1 · the standing table · Region {', '.join(sorted(regions))} · {kind}s")
    L = ledger(rules, kind, regions, section_label(water, run))
    show(L, f"STAGE 2 · {water}  ·  {lbl}  ·  {kind}  ·  {len(rules)} rules in", on)


if __name__ == "__main__":
    trace(sys.argv[1] if len(sys.argv) > 1 else "Fraser River",
          int(sys.argv[2]) if len(sys.argv) > 2 else 0,
          tuple(int(x) for x in sys.argv[3].split("-")) if len(sys.argv) > 3 else None)
