"""PROTOTYPE, read-only: the mainstem half of the Widgeon complaint — a river's own far-upstream gauges outvoting the
gauge standing on this water. Re-scores TODAY's answer with one change to the weights and counts what moves.

THE CHANGE ("relative share"): a same-river donor's weight is multiplied by its catchment over the largest
same-river donor's catchment in the panel (both on the same blue line, so the smaller is a subset of the larger's
water). The best same-river donor keeps its full weight; Fraser at Hansard (8% of the flow at Hope by ECCC area)
stops counting as much as Hope. Tributary donors are untouched. Robust to a quiet gauge: the factor is computed
over the panel's members, and a panel whose big gauge is quiet still answers from the others.

    .venv/bin/python handoff/PLAN/app-data/side-channels/scripts/reweight.py SCRATCH_DIR GRAPH.pkl
"""
import collections
import json
import math
import pickle
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, ".")
from compare import INDEX, estimate, load_bundle  # noqa: E402


def rel_members(members):
    """The panel with each same-river donor's record years scaled to carry the relative-share factor.

    `weight_of` multiplies by min(1, years/20); scaling `years` is how this prototype injects a multiplicative
    factor without forking the estimate. Only valid for the comparison here (a real change goes in weight_of /
    weightFor as its own factor).
    """
    big = max((ad for _s, _r, ad, _y, _g, same in members if same), default=None)
    out = []
    for st, role, ad, yrs, reg, same in members:
        if same and big:
            rec = min(1.0, yrs / 20)
            f = ad / big
            yrs = rec * f * 20       # years such that min(1, years/20) == rec * f
        out.append((st, role, ad, yrs, reg, same))
    return tuple(out)


def main(scratch, graph_pkl):
    with open(graph_pkl, "rb") as fh:
        g = pickle.load(fh)
    length = {v["sid"]: v["length"] for v in g["nodes"].values() if v["sid"]}
    live, names, _ = load_bundle()
    idx = json.loads(INDEX.read_text())["stations"]
    sets = {"live": live}
    f = Path(scratch) / "panels_same_wsc.pkl"
    if f.exists():
        sets["same_wsc"] = pickle.loads(f.read_bytes())
    for label, panels in sets.items():
        moved, km, cache, by_name = collections.Counter(), collections.Counter(), {}, collections.Counter()
        for s, e in panels.items():
            if e not in cache:
                a = estimate(e[0], e[1], idx)[0]
                b = estimate(rel_members(e[0]), e[1], idx)[0]
                cache[e] = (a, b)
            a, b = cache[e]
            if a is None or b is None:
                k = "unchanged silence" if a is None and b is None else "silence changes"
            else:
                d = abs(a - b)
                k = ">=25" if d >= 25 else "10-25" if d >= 10 else "5-10" if d >= 5 else "<5"
            moved[k] += 1
            km[k] += length.get(s, 0) / 1000
            if k in (">=25", "10-25"):
                by_name[names.get(s) or "(unnamed)"] += 1
        print(f"[{label} panels] relative-share reweight, today's answer moves by: "
              + ", ".join(f"{k}: {moved[k]:,} sections / {km[k]:,.0f} km" for k in sorted(moved)))
        print("   waters with the most sections moving >=10 points:",
              ", ".join(f"{n} ({c})" for n, c in by_name.most_common(12)))
    for s in (315388, 60045):
        e = sets.get("same_wsc", live).get(s)
        a = estimate(e[0], e[1], idx)
        b = estimate(rel_members(e[0]), e[1], idx)
        print(f"sid {s} {names.get(s)}: {a[0]:.0f} -> {b[0]:.0f}  shares " +
              ", ".join(f"{st} {w * 100:.0f}%" for st, w in b[2]))


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
    _ = math
