"""Print summary.md's tables from candidates.json (read only)."""
import json
from collections import Counter
from pathlib import Path

d = json.loads((Path(__file__).parent / "candidates.json").read_text())
C = d["candidates"]
src_of = lambda s: s["source"]
rows = Counter()
for c in C:
    for s in c["sources"]:
        rows[(src_of(s), s["confidence"])] += 1
srcs = sorted({k[0] for k in rows})
print("| source | high | medium | low | total |\n|---|---|---|---|---|")
for s in srcs:
    h, m, l = (rows[(s, x)] for x in ("high", "medium", "low"))
    print(f"| {s} | {h} | {m} | {l} | {h + m + l} |")
print()
print("| merged confidence | unnamed | different | generic_differs | spelling | total |\n|---|---|---|---|---|---|")
for cf in ("high", "medium", "low"):
    cs = [c for c in C if c["confidence"] == cf]
    r = Counter(c["relation"] for c in cs)
    print(f"| {cf} | {r['unnamed']} | {r['different']} | {r['generic_differs']} | {r['spelling']} | {len(cs)} |")
print()
print("auto_accept_eligible:", sum(c["auto_accept_eligible"] for c in C),
      "corroborated:", sum(1 for c in C if c["corroborated_by"]))
print()
for c in C:
    if c["confidence"] == "high" or c["corroborated_by"]:
        t = c["target"]
        tid = t.get("item_id") or (f"blk {t['blk']}" if t.get("blk") else f"wbk {t.get('wbk')}")
        ev = c["sources"][0]["evidence"]
        e = (f"{ev['blk_overlap']:.0%} of line / {ev['source_overlap']:.0%} of source" if "blk_overlap" in ev
             else f"station {ev.get('station')} ({ev.get('trust')}, {ev.get('distance_m')} m)" if "station" in ev else "")
        print(f"| {c['proposed']} | {tid} | {', '.join(c['current_names'][:2]) or '(unnamed)'} | "
              f"{', '.join(sorted({s['source'] for s in c['sources']}))} | {e} | {c['confidence']} | "
              f"{'yes' if c['auto_accept_eligible'] else 'no' + (' (rival: ' + ', '.join(c['rival_names']) + ')' if c['rival_names'] else '')} |")
