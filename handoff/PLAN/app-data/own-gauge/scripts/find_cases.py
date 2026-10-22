"""Read-only: find real waters for the two shapes the rule's exceptions exist for.

  long   a long river with ONE own gauge near its top: the most along-water km downstream of a single own gauge
  fork   a creek whose own gauge sits on a tiny upstream fork: the section drains many times what the gauge does

    .venv/bin/python handoff/PLAN/app-data/own-gauge/scripts/find_cases.py OWN.pkl
"""
import collections
import pickle
import sqlite3
import sys

BUNDLE = "/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/data/generated/bundle/bundle.sqlite"


def main(own_pkl):
    rows = pickle.loads(open(own_pkl, "rb").read())["rows"]
    db = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    names = dict(db.execute("SELECT item_id, name FROM item"))
    gname = dict(db.execute("SELECT station, name FROM gauge"))
    by_water = collections.defaultdict(lambda: dict(stations=set(), down_km=0.0, n=0))
    for r in rows.values():
        if not r["own"] or r["side"]:
            continue
        w = by_water[r["water"]]
        w["n"] += 1
        for o in r["own"]:
            w["stations"].add(o["station"])
            if o["direction"] == "down":
                w["down_km"] = max(w["down_km"], o["distance_m"] / 1000)
    print("LONG: one own gauge, most km of the same water below it")
    single = [(w["down_km"], k, w) for k, w in by_water.items() if len(w["stations"]) == 1]
    for km, k, w in sorted(single, key=lambda t: -t[0])[:15]:
        st = next(iter(w["stations"]))
        print(f"  {names.get(k, k)[:30]:30} {k:16} {st} {gname.get(st, '')[:44]:44} {km:6.0f} km below, {w['n']} sections")
    print("\nFORK: the section drains many times its own gauge")
    seen = set()
    fork = []
    for sid, r in rows.items():
        if r["side"] or not r["target"]:
            continue
        for o in r["own"]:
            ratio = r["target"] / o["area"]
            if ratio >= 5:
                fork.append((ratio, sid, r, o))
    fork.sort(key=lambda t: -t[0])
    for ratio, sid, r, o in fork:
        if r["water"] in seen:
            continue
        seen.add(r["water"])
        print(f"  {names.get(r['water'], r['water'])[:30]:30} sid {sid:8} {o['station']} "
              f"{gname.get(o['station'], '')[:40]:40} gauge {o['area']:9.1f} km2, section {r['target']:9.1f} km2 "
              f"({ratio:6.1f}x) {o['distance_m'] / 1000:6.1f} km {o['direction']}")
        if len(seen) >= 20:
            break


if __name__ == "__main__":
    main(sys.argv[1])
