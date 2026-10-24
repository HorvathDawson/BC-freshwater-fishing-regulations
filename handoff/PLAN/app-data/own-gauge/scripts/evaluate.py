"""Read-only: the own-gauge rule on real water, today's feed. Distributions, a threshold grid, named examples.

The estimate is a port of `estimate()` in app/packages/core/src/trust.ts at horizon 0, per quantity (Flow =
discharge, Level = level; the `reading()` rule of app/packages/data/src/panel.ts), with the lead share applied the way
`leadShares` / `lead_shares` do. `--lead-share`, `--far-km`, `--smaller` override the proposed defaults.

    .venv/bin/python handoff/PLAN/app-data/own-gauge/scripts/evaluate.py OWN.pkl PANELS.pkl [grid|examples|bins]
"""
import collections
import json
import math
import pickle
import sqlite3
import sys

sys.path.insert(0, ".")
from pipeline.gauges.consume.panel import Contribution, answering, reserve_at  # noqa: E402

MAIN = "/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations"
BUNDLE = MAIN + "/data/generated/bundle/bundle.sqlite"
INDEX = MAIN + "/data/generated/gauges/feeds/index.json"


def reading(row, q):
    if not row:
        return None
    if row.get(q) is not None:
        return row[q]
    return row.get("percentile") if (row.get("parameter") or "discharge") == q else None


# THE APP'S ANSWER, imported — `answering` in pipeline/gauges/consume/panel.py, which app/tools/one-formula.test.ts
# runs beside `answering` in app/packages/core/src/trust.ts (T125 item 7). This script used to port it by hand.
def estimate(members, target, idx, q, lead_station=None, lead_share=0.7):
    """-> (percentile 0..100 | None, why, [(station, share)]); `lead_station` leads (rank 1) with `lead_share`."""
    cs, sts = [], []
    for rank, (st, role, area, years, _reg, same) in enumerate(members or ()):
        p = reading(idx.get(st), q)
        if p is None:
            continue
        cs.append(Contribution(p, role, area, years, bool(same), 1 if st == lead_station else 0,
                               reserve_at(rank)))
        sts.append(st)
    got, _stood = answering(target, cs, own_share=lead_share)
    if not got["ok"]:
        return None, got["why"], []
    return got["value"]["percentile"] * 100, "ok", [(sts[i], s) for i, s in sorted(got["value"]["shares"].items())]


def far_limit_km(far, target, gauge):
    """`far` is a fixed distance in km, or "hack:F[:MIN]" — F times the river's length at the larger of the two
    catchments by Hack's law (L = 1.27 A^0.6 km, A in km2), never less than MIN km."""
    if isinstance(far, str) and far.startswith("hack:"):
        parts = far.split(":")
        frac, floor = float(parts[1]), float(parts[2]) if len(parts) > 2 else 0.0
        return max(floor, frac * 1.27 * max(target or 0.0, gauge or 0.0) ** 0.6)
    return float(far)


def lead_of(row, q, far, smaller):
    """The own gauge that leads quantity `q` here, or None: nearest along the water among those measuring `q`,
    not farther than `far` (see far_limit_km), and not draining less than 1/`smaller` of the section."""
    best = None
    for o in row["own"]:
        if q not in o["measures"]:
            continue
        if o["distance_m"] > far_limit_km(far, row["target"], o["area"]) * 1000:
            continue
        if row["target"] and o["area"] * smaller < row["target"]:
            continue
        key = (o["distance_m"], abs(math.log(max(o["area"], 1e-9) / max(row["target"] or 1e-9, 1e-9))))
        if best is None or key < best[0]:
            best = (key, o["station"])
    return None if best is None else best[1]


def binned(a, b):
    if a is None and b is None:
        return "silent before and after"
    if a is None:
        return "gains an answer"
    if b is None:
        return "loses its answer"
    d = abs(a - b)
    return "moves 25 points or more" if d >= 25 else "moves 10 to 25 points" if d >= 10 else \
        "moves 5 to 10 points" if d >= 5 else "moves under 5 points" if d > 0.05 else "unchanged"


ORDER = ["unchanged", "moves under 5 points", "moves 5 to 10 points", "moves 10 to 25 points",
         "moves 25 points or more", "gains an answer", "loses its answer", "silent before and after"]


def count(own, panels, idx, q, far_km, smaller, lead_share):
    c, km = collections.Counter(), collections.Counter()
    leads = 0
    for sid, row in own["rows"].items():
        lead = lead_of(row, q, far_km, smaller)
        if lead is None:
            continue
        leads += 1
        ms, target = panels[sid]
        a = estimate(ms, target, idx, q)[0]
        b = estimate(ms, target, idx, q, lead, lead_share)[0]
        k = binned(a, b)
        c[k] += 1
        km[k] += row["length"] / 1000
    return leads, c, km


def main(own_pkl, panels_pkl, what="all", far_km=50.0, smaller=10.0, lead_share=0.7):
    own = pickle.loads(open(own_pkl, "rb").read())
    panels = pickle.loads(open(panels_pkl, "rb").read())
    idx = json.loads(open(INDEX).read())["stations"]
    rows = own["rows"]
    if what in ("all", "dist"):
        with_own = [r for r in rows.values() if r["own"]]
        d = sorted(min(o["distance_m"] for o in r["own"]) / 1000 for r in with_own)
        rt = sorted(min((r["target"] or 0) / o["area"] for o in r["own"]) for r in with_own)
        pct = lambda xs, p: xs[min(len(xs) - 1, int(p * len(xs)))]  # noqa: E731
        print(f"sections whose panel holds an own gauge: {len(with_own):,}")
        print("  nearest own gauge, km along the water: " +
              ", ".join(f"p{int(p*100)} {pct(d, p):.1f}" for p in (.1, .25, .5, .75, .9, .95, .99)))
        print("  section / own gauge catchment (best): " +
              ", ".join(f"p{int(p*100)} {pct(rt, p):.2f}" for p in (.1, .25, .5, .75, .9, .95, .99)))
        for t in (10, 25, 50, 100, 200):
            print(f"  within {t} km: {sum(1 for x in d if x <= t):,}")
        for t in (2, 3, 5, 10, 30):
            print(f"  section at most {t}x the gauge: {sum(1 for x in rt if x <= t):,}")
    if what in ("all", "grid"):
        for q in ("discharge", "level"):
            print(f"\n== {q}: threshold grid (sections where the own gauge leads; how today's answer moves)")
            for fk in (25, 50, 100, 1e9, "hack:0.1", "hack:0.2", "hack:0.3", "hack:0.2:10", "hack:0.2:25"):
                for sm in (3, 10, 1e9):
                    for ls in ((0.6, 0.7) if sm == 10 else (0.7,)):
                        leads, c, _km = count(own, panels, idx, q, fk, sm, ls)
                        fkl = fk if isinstance(fk, str) else "inf" if fk > 1e8 else f"{fk:g}"
                        sml = "inf" if sm > 1e8 else f"{sm:g}"
                        print(f"  far {fkl:>11}, smaller {sml:>4}x, share {ls}: leads {leads:,}; " +
                              ", ".join(f"{k} {c[k]:,}" for k in ORDER if c[k]))
    if what in ("all", "bins"):
        for q in ("discharge", "level"):
            leads, c, km = count(own, panels, idx, q, far_km, smaller, lead_share)
            print(f"\n== {q} at far {far_km} km / smaller {smaller}x / share {lead_share}: own gauge leads {leads:,}")
            for k in ORDER:
                if c[k]:
                    print(f"  | {k} | {c[k]:,} | {km[k]:,.0f} |")


if __name__ == "__main__":
    args = sys.argv[1:]
    far = args[3] if len(args) > 3 else 50.0
    main(args[0], args[1], args[2] if len(args) > 2 else "all",
         far if isinstance(far, float) or far.startswith("hack:") else float(far))
