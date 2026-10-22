"""Read-only: what each gate of the shipped `may_lead` does on real water, and how big the panels get.

Reads own_table.py's OWN.pkl (every own-gauge relation in today's option-A panels, with its along-water distance)
and rebuild.py's AFTER.pkl (the changed build).

    .venv/bin/python handoff/PLAN/app-data/own-gauge/scripts/gates.py OWN.pkl AFTER.pkl [--model-areas]
"""
import collections
import json
import pickle
import sys

sys.path.insert(0, ".")
from pipeline.atlas.gauges.panel import OWN_SMALLER_RATIO, area_scale, far_limit_km  # noqa: E402

MAIN = "/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations"


def main(own_pkl, after_pkl):
    rows = pickle.loads(open(own_pkl, "rb").read())["rows"]
    after = pickle.loads(open(after_pkl, "rb").read())
    roster = json.loads(open(MAIN + "/data/source/bc_hydrometric_stations.json").read())
    live = {s["station"] for s in roster if s.get("realtime")}
    published = {s["station"]: s["area_km2"] for s in roster if s.get("area_km2")}
    unscaled = "--model-areas" in sys.argv          # the far gate at the area model's figures, as first shipped
    why = collections.Counter()
    sections = collections.Counter()
    for r in rows.values():
        if not r["own"]:
            continue
        verdicts = set()
        for o in r["own"]:
            t, a, d = r["target"], o["area"], o["distance_m"]
            small = t > a * OWN_SMALLER_RATIO
            k = 1.0 if unscaled else area_scale(a, published.get(o["station"]))
            far = d > far_limit_km(t, a, k) * 1000
            k = ("not real-time" if o["station"] not in live else
                 "far and much smaller" if far and small else "far" if far else "much smaller" if small else "leads")
            why[k] += 1
            verdicts.add(k)
        sections["some own gauge may lead" if "leads" in verdicts else
                 "own gauges, none may lead"] += 1
    print("own-gauge relations in today's panels, by the first gate that stops them:")
    for k, n in why.most_common():
        print(f"  {k}: {n:,}")
    print("sections:", dict(sections))
    size = collections.Counter(len(e[0]) for e in after.values())
    leads = collections.Counter(sum(1 for m in e[0] if m[6]) for e in after.values())
    print("members per section's panel after:", dict(sorted(size.items())))
    print("own gauges that may lead, per section:", dict(sorted(leads.items())))


if __name__ == "__main__":
    main(*sys.argv[1:3])
