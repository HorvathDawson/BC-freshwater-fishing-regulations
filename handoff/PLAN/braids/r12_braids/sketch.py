"""Sketches: current majority placement vs tangent split (±100 m; ±50/±200 dotted) for 3 braids."""
import math, pickle, sys
sys.path.insert(0, "/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations")
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from shapely.geometry import LineString, Point
from shapely.strtree import STRtree

OUTDIR = sys.argv[1]
D = pickle.load(open("/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/data/generated/scratch_r10/braids_sub.pkl", "rb"))
g, geo = D["graph"], D["geoms"]

CASES = [
    {"file": "dean_canyon.png", "title": "Dean River — signs ~0.5 km above the canyon (cut at 5,243 m)",
     "blk": "360883982", "cuts": [5243.0], "braids": ["360227736:0", "360227738:0"],
     "zones": ["below the signs: No fishing (r4, all year)", "above the signs: r3 No fishing Oct 1–May 31"],
     "pad": 700},
    {"file": "peace_site_c.png", "title": "Peace River — 1,200 m signs (1,595,781) and Site C dam (1,596,981)",
     "blk": "359572348", "cuts": [1595781.448, 1596981.448], "braids": ["359005110:0", "359013560:0"],
     "zones": ["below the 1,200 m signs: downstream row (open)", "signs → dam: No fishing (all year)",
               "above the dam: Hwy 29 → Site C row"], "pad": 900},
    {"file": "chowade_network.png", "title": "Chowade River — Horseshoe Road Bridge (cut at 20,884 m)",
     "blk": "359571076", "cuts": [20884.0], "braids": ["359002043:0", "359006635:0"],
     "zones": ["below the bridge: open", "above the bridge: No fishing Aug 15–Oct 15 (r1)"], "pad": 900},
]
COL = ["#2b6cb0", "#c53030", "#2f855a"]


def main_ids(blk):
    return sorted(s for s, n in g.nodes.items() if n.blk == blk and s in geo and geo[s].length)


def chowade_blk():
    # the Chowade main line: the blk of the on-line neighbour of 359002043's network
    for s, n in g.nodes.items():
        if n.display_name == "Chowade River" and n.down_m < 20884 < n.up_m and s in geo:
            return n.blk
    return None


for case in CASES:
    ids = main_ids(case["blk"])
    tree = STRtree([geo[i] for i in ids])

    def meas(pt):
        i = ids[int(tree.nearest(pt))]
        n, ln = g.nodes[i], geo[i]
        return n.down_m + ln.project(pt) / ln.length * (n.up_m - n.down_m)

    def point(m):
        for i in ids:
            n = g.nodes[i]
            if n.down_m <= m <= n.up_m:
                ln = geo[i]
                return ln.interpolate((m - n.down_m) / (n.up_m - n.down_m) * ln.length)
        j = min(ids, key=lambda i: min(abs(g.nodes[i].down_m - m), abs(g.nodes[i].up_m - m)))
        n = g.nodes[j]
        return Point(geo[j].coords[0] if abs(n.down_m - m) < abs(n.up_m - m) else geo[j].coords[-1])

    def tangent(c, W):
        a, b = point(c - W), point(c + W)
        tx, ty = b.x - a.x, b.y - a.y
        h = math.hypot(tx, ty)
        return point(c), (tx / h, ty / h)

    # everything to draw: braids, their network, main line nearby
    allb = [geo[p] for p in case["braids"]]
    minx = min(l.bounds[0] for l in allb) - case["pad"]; maxx = max(l.bounds[2] for l in allb) + case["pad"]
    miny = min(l.bounds[1] for l in allb) - case["pad"]; maxy = max(l.bounds[3] for l in allb) + case["pad"]
    others = [s for s, n in g.nodes.items() if s in geo and n.kind.value == "stream"
              and geo[s].bounds[2] > minx and geo[s].bounds[0] < maxx and geo[s].bounds[3] > miny and geo[s].bounds[1] < maxy]

    fig, axes = plt.subplots(1, 2, figsize=(13, 6.6))
    for k, ax in enumerate(axes):
        for s in others:
            if s in case["braids"]:
                continue
            l = geo[s]
            x, y = l.xy
            on = g.nodes[s].blk == case["blk"]
            ax.plot(x, y, color="#4a5568" if on else "#a0aec0", lw=2.6 if on else 1.0, zorder=1)
        for p in case["braids"]:
            l = geo[p]
            if k == 0:
                # current: the whole braid takes the zone holding most of its length (projection)
                n = max(2, int(l.length // 5))
                pts = [l.interpolate((j + .5) * l.length / n) for j in range(n)]
                zs = [sum(meas(q) > c for c in case["cuts"]) for q in pts]
                z = max(set(zs), key=zs.count)
                x, y = l.xy
                ax.plot(x, y, color=COL[z], lw=3.2, zorder=3)
                share = zs.count(z) / len(zs)
                mid = l.interpolate(0.5, normalized=True)
                ax.annotate(f"{p}\n{l.length:,.0f} m, {share:.0%} in zone", (mid.x, mid.y), fontsize=7.5,
                            xytext=(6, 6), textcoords="offset points", zorder=5)
            else:
                # tangent split: each point's zone = how many cut lines it lies above (±100 m tangent)
                n = max(2, int(l.length // 2))
                pts = [l.interpolate(j * l.length / n) for j in range(n + 1)]
                zs = []
                for q in pts:
                    zz = 0
                    for c in case["cuts"]:
                        P, (tx, ty) = tangent(c, 100.0)
                        zz += ((q.x - P.x) * tx + (q.y - P.y) * ty) > 0
                    zs.append(zz)
                for j in range(n):
                    ax.plot([pts[j].x, pts[j + 1].x], [pts[j].y, pts[j + 1].y], color=COL[zs[j]], lw=3.2,
                            zorder=3, solid_capstyle="butt")
        for c in case["cuts"]:
            P, _ = tangent(c, 100.0)
            ax.plot([P.x], [P.y], "k^", ms=9, zorder=6)
            if k == 1:
                for W, ls, lw in ((50.0, ":", 0.8), (100.0, "--", 1.3), (200.0, ":", 0.8)):
                    P, (tx, ty) = tangent(c, W)
                    R = 3000
                    ax.plot([P.x - R * -ty, P.x + R * -ty], [P.y - R * tx, P.y + R * tx], "k", ls=ls, lw=lw, zorder=2)
        ax.set_xlim(minx, maxx); ax.set_ylim(miny, maxy)
        ax.set_aspect("equal"); ax.set_xticks([]); ax.set_yticks([])
        ax.set_title(["Now (E15): whole braid on its majority side", "Tangent split: ±100 m (dashed), ±50/±200 (dotted)"][k], fontsize=10)
    hs = [plt.Line2D([0], [0], color=COL[i], lw=3) for i in range(len(case["zones"]))]
    hs += [plt.Line2D([0], [0], color="#4a5568", lw=2.6), plt.Line2D([0], [0], marker="^", color="k", ls="")]
    fig.legend(hs, case["zones"] + ["main line (the cut's own blue line)", "cut point"], loc="lower center",
               ncol=3, fontsize=8.5, frameon=False)
    fig.suptitle(case["title"], fontsize=12)
    fig.tight_layout(rect=(0, 0.07, 1, 0.95))
    fig.savefig(f"{OUTDIR}/{case['file']}", dpi=110)
    plt.close(fig)
    print("wrote", case["file"], case["blk"])
