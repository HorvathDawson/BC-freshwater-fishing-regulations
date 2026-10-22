"""Read-only: province-wide counts of the three failure shapes, and the symptom the owner sees (a piece of a water
whose answer today is far from the answer on that water's own main channel), for the live panels and each variant.

    .venv/bin/python handoff/PLAN/app-data/side-channels/scripts/structural.py SCRATCH_DIR GRAPH.pkl
"""
import collections
import json
import pickle
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, ".")
from compare import BUNDLE, INDEX, estimate, load_bundle  # noqa: E402


def main(scratch, graph_pkl):
    scratch = Path(scratch)
    with open(graph_pkl, "rb") as fh:
        g = pickle.load(fh)
    node = {v["sid"]: v for v in g["nodes"].values() if v["sid"]}
    length = {s: v["length"] for s, v in node.items()}
    live, names, _ = load_bundle()
    idx = json.loads(INDEX.read_text())["stations"]
    db = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    item_of = collections.defaultdict(list)
    for sid, o, name in db.execute("SELECT s.sid, i.ord, i.name FROM item_section s JOIN item i USING(ord) "
                                   "WHERE i.kind='stream'"):
        item_of[(o, name)].append(sid)

    km = lambda ss: sum(length.get(s, 0) for s in ss) / 1000  # noqa: E731

    # (b) a donor UPSTREAM of a reach that drains more than the reach: only a distributary can sit below a gauge
    # and drain less than it, so the walk passed through a bigger river into a split-off piece
    under = [s for s, (ms, a) in live.items() if a and any(r == "up" and ad > 2 * a for _s, r, ad, *_ in ms)]
    under_side = [s for s in under if node.get(s, {}).get("side")]
    print(f"(b) reach drains < half of an UPSTREAM donor: {len(under):,} sections ({km(under):,.0f} km); "
          f"{len(under_side):,} flagged side channel, {len(under) - len(under_side):,} not")
    under_far = [s for s in under if all(not same for *_x, same in live[s][0])]
    print(f"    ...and every donor is on another river: {len(under_far):,} sections ({km(under_far):,.0f} km)")

    # (c) one river's own gauges outvoting the one standing on this water
    red = []
    for s, (ms, a) in live.items():
        if not a:
            continue
        same = [(max(a, ad) / min(a, ad), st) for st, _r, ad, _y, _g, sm in ms if sm]
        if any(r <= 1.5 for r, _ in same) and any(r > 5 for r, _ in same):
            red.append(s)
    print(f"(c) a same-river donor ON this water (<=1.5x) shares the vote with one >5x smaller/larger on the same "
          f"river: {len(red):,} sections ({km(red):,.0f} km)")

    # the SYMPTOM: pieces of one named water whose answer today is >=25 points from the water's longest piece
    variants = {"live": live}
    for v in ("same_wsc", "nested"):
        f = scratch / f"panels_{v}.pkl"
        if f.exists():
            variants[v] = pickle.loads(f.read_bytes())
    cache = {}

    def ans(e):
        if e is None:
            return None
        if e not in cache:
            cache[e] = estimate(e[0], e[1], idx)[0]
        return cache[e]

    for v, panels in variants.items():
        odd, odd_items, silent_split = [], [], 0
        for (_o, name), sids in item_of.items():
            if len(sids) < 2:
                continue
            main = max(sids, key=lambda s: length.get(s, 0))
            m = ans(panels.get(main))
            bad = []
            for s in sids:
                x = ans(panels.get(s))
                if (m is None) != (x is None):
                    silent_split += 1
                    bad.append(s)
                elif m is not None and abs(m - x) >= 25:
                    bad.append(s)
            if bad:
                odd += bad
                odd_items.append(name)
        print(f"[{v}] pieces whose answer today is >=25 points off, or silent/answered against, their water's "
              f"longest piece: {len(odd):,} sections ({km(odd):,.0f} km) on {len(odd_items):,} waters "
              f"({silent_split:,} of them the silent/answered kind)")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
