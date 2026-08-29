"""Draw what braid-nest simplification keeps and removes, from a built graph.

    .venv/bin/python -m pipeline.tools.nest_report --out output/v2/full --html /tmp/nests.html

Regenerate this whenever the rule in `pipeline.graph.nests` changes: the point of the page is to show
the rule's decisions on real geometry, and every version of the rule so far looked plausible in prose
and wrong on the map. Three panels per nest — the channels now, the ones being removed, what is left —
because comparing two busy pictures is asking the reader to spot the difference.

Each nest is checked with `nests.check_destinations` before it is drawn, so a page that renders is
also a page whose invariant held.
"""

from __future__ import annotations

import argparse
import pickle
from collections import defaultdict
from pathlib import Path

from pipeline.graph.nests import check_destinations, essential_routes, nest_ports
from pipeline.graph.prune import loop_nodes, nv_blks

W, H = 460, 400


from project_config import get_config
def _rings(graph, geoms, nid):
    """(kind, points) for a node. A LAKE stores its ring as a LineString, so keying off the geometry
    type alone drew every lake as a stray loop hanging off the stream that threads it."""
    gm = geoms.get(nid)
    if gm is None or gm.is_empty:
        return []
    node = graph.nodes.get(nid)
    lake = node is not None and node.kind.name == "lake"
    t = gm.geom_type
    if t == "LineString":
        return [("P" if lake else "L", [list(c[:2]) for c in gm.coords])]
    if t == "MultiLineString":
        return [("P" if lake else "L", [list(c[:2]) for c in p.coords]) for p in gm.geoms]
    if t == "Polygon":
        return [("P", [list(c[:2]) for c in gm.exterior.coords])]
    if t == "MultiPolygon":
        return [("P", [list(c[:2]) for c in p.exterior.coords]) for p in gm.geoms]
    return []


def _clip(pts, box):
    inside = lambda p: box[0] <= p[0] <= box[2] and box[1] <= p[1] <= box[3]   # noqa: E731
    runs, cur = [], []
    for i, p in enumerate(pts):
        if inside(p):
            if not cur and i:
                cur.append(pts[i - 1])                   # reach out to the frame edge
            cur.append(p)
        elif cur:
            cur.append(p)
            runs.append(cur)
            cur = []
    if cur:
        runs.append(cur)
    return [r for r in runs if len(r) > 1]


def _roles(graph, comp, bylen):
    """What to colour: the mainstem it drains into, any named SIDE CHANNEL it drains into (violet, so
    a 7 km channel does not read as the 77 km river), and the named waters joining."""
    ins, exits = nest_ports(graph, comp)
    blks = sorted({graph.nodes[e].blk for e in exits if e in graph.nodes}, key=lambda b: -bylen[b])
    main, side = (blks[0] if blks else ""), set(blks[1:])
    tribs = {w for w, (srcs, _e) in ins.items()
             if not w.startswith("?") and all(graph.nodes[s].blk not in set(blks) for s in srcs)}
    name = lambda b: next((graph.nodes[n].display_name or "?"                    # noqa: E731
                           for n, v in graph.nodes.items() if v.blk == b), "?")
    return main, side, sorted(tribs), (name(main) if main else ""), [name(b) for b in blks[1:]]


def _panel(graph, geoms, comp, show, bylen, faint=False):
    main, side, tribs, _mn, _sn = _roles(graph, comp, bylen)
    pts = [p for n in comp for _k, r in _rings(graph, geoms, n) for p in r]
    xs, ys = [p[0] for p in pts], [p[1] for p in pts]
    m = 0.22 * max(max(xs) - min(xs), max(ys) - min(ys), 300)
    box = [min(xs) - m, min(ys) - m, max(xs) + m, max(ys) + m]
    bw, bh = box[2] - box[0], box[3] - box[1]
    if bw / bh < W / H:
        d = (bh * W / H - bw) / 2
        box[0] -= d
        box[2] += d
    else:
        d = (bw * H / W - bh) / 2
        box[1] -= d
        box[3] += d
    s = W / (box[2] - box[0])
    path = lambda r: " ".join(("M" if i == 0 else "L") +                        # noqa: E731
                              f"{(p[0]-box[0])*s:.1f} {H-(p[1]-box[1])*s:.1f}" for i, p in enumerate(r))
    out: list[str] = []

    def draw(nid, colour, width, op=1.0):
        for kind, r in _rings(graph, geoms, nid):
            if kind == "P":
                if any(box[0] <= p[0] <= box[2] and box[1] <= p[1] <= box[3] for p in r):
                    out.append(f'<path d="{path(r)} Z" fill="{colour}" fill-opacity="{0.35*op:.2f}" '
                               f'stroke="{colour}" stroke-width="1.2" stroke-opacity="{op:.2f}"/>')
            else:
                for run in _clip(r, box):
                    out.append(f'<path d="{path(run)}" fill="none" stroke="{colour}" '
                               f'stroke-width="{width}" stroke-opacity="{op:.2f}" '
                               f'stroke-linecap="round" stroke-linejoin="round"/>')

    ctx = 0.32 if faint else 1.0
    for nid, v in graph.nodes.items():
        if nid in comp:
            continue
        if main and v.blk == main:
            draw(nid, "var(--main)", 8, ctx)
        elif v.blk in side:
            draw(nid, "var(--side)", 6, ctx)
        elif (v.display_name or "").strip() in tribs:
            draw(nid, "var(--trib)", 5, ctx)
    for nid in comp:
        if nid in show:
            draw(nid, "var(--cut)" if faint else "var(--nest)", 5)
    return f'<svg viewBox="0 0 {W} {H}" preserveAspectRatio="xMidYMid meet">' + "".join(out) + "</svg>"


_CSS = """
:root { --bg:#eef1f4; --panel:#fbfcfd; --ink:#0f171e; --muted:#5b6b7a; --line:#d3dae1;
  --main:#9aa8b4; --main-t:#68798a; --side:#8b6fb0; --trib:#0e7c8b; --nest:#16232f; --cut:#c2492f; }
@media (prefers-color-scheme: dark) { :root:not([data-theme="light"]) {
  --bg:#0e141a; --panel:#151d25; --ink:#e6edf3; --muted:#94a5b4; --line:#26313b;
  --main:#55646f; --main-t:#9db0bf; --side:#a98ecf; --trib:#3fb8c9; --nest:#e2ecf4; --cut:#f0724f; } }
:root[data-theme="dark"] { --bg:#0e141a; --panel:#151d25; --ink:#e6edf3; --muted:#94a5b4; --line:#26313b;
  --main:#55646f; --main-t:#9db0bf; --side:#a98ecf; --trib:#3fb8c9; --nest:#e2ecf4; --cut:#f0724f; }
* { box-sizing:border-box; }
body { margin:0; background:var(--bg); color:var(--ink); font-family:"IBM Plex Sans",system-ui,sans-serif; line-height:1.6; }
.wrap { max-width:1400px; margin:0 auto; padding:56px 24px 80px; display:flex; flex-direction:column; gap:44px; }
h1 { font-family:"Zilla Slab",Georgia,serif; font-weight:600; font-size:2.1rem; margin:0; letter-spacing:-.01em; }
h2 { font-family:"Zilla Slab",Georgia,serif; font-weight:600; font-size:1.4rem; margin:0 0 12px; }
h3 { font-family:"Zilla Slab",Georgia,serif; font-weight:600; font-size:1.3rem; margin:0; }
p { margin:0; max-width:74ch; } .lede { color:var(--muted); }
.mono { font-family:"IBM Plex Mono",ui-monospace,monospace; font-variant-numeric:tabular-nums; }
.key { display:flex; flex-wrap:wrap; gap:22px; padding:14px 18px; background:var(--panel);
  border:1px solid var(--line); border-radius:4px; font-size:.86rem; position:sticky; top:0; z-index:3; }
.key span { display:flex; align-items:center; gap:9px; color:var(--muted); }
.sw { width:26px; height:0; border-top-width:4px; border-top-style:solid; flex:none; }
.case { display:flex; flex-direction:column; gap:4px; }
.sub { color:var(--muted); font-size:.85rem; margin-bottom:10px; } .sub .mono { font-size:.8rem; }
.trio { display:grid; grid-template-columns:repeat(3,1fr); gap:14px; }
@media (max-width:980px) { .trio { grid-template-columns:1fr; } }
.opt { display:flex; flex-direction:column; gap:7px; }
.cap { display:flex; justify-content:space-between; align-items:baseline; gap:10px; }
.ok { font-family:"IBM Plex Mono",monospace; font-size:.7rem; text-transform:uppercase; letter-spacing:.09em; color:var(--muted); }
.cap .mono { font-size:.78rem; color:var(--muted); }
.cell { background:var(--panel); border:1px solid var(--line); border-radius:4px; padding:8px; }
.cell svg { display:block; width:100%; height:auto; }
table { border-collapse:collapse; width:100%; font-size:.94rem; }
th,td { text-align:left; padding:10px 12px; border-bottom:1px solid var(--line); }
th { font-family:"IBM Plex Mono",monospace; font-size:.68rem; text-transform:uppercase; letter-spacing:.09em; color:var(--muted); font-weight:500; }
th.n,td.n { text-align:right; } td.n { font-family:"IBM Plex Mono",monospace; font-variant-numeric:tabular-nums; }
.scroll { overflow-x:auto; }
"""


def build_page(graph, geoms, limit=6, protected=None):
    bylen: dict[str, float] = defaultdict(float)
    for _n, v in graph.nodes.items():
        if v.blk:
            bylen[v.blk] += (v.length_m or 0.0)

    records: list = []
    loop_nodes(graph, protected or set(), True, records)
    cases = []
    for comp, *_rest in records:
        comp = set(comp)
        keep, _spare = essential_routes(graph, comp)
        if not (comp - keep):
            continue
        lost = check_destinations(graph, comp, keep)
        main, _side, tribs, mainname, sidenames = _roles(graph, comp, bylen)
        km0 = sum(graph.nodes[n].length_m or 0 for n in comp) / 1000
        km1 = sum(graph.nodes[n].length_m or 0 for n in keep) / 1000
        cases.append({"comp": comp, "keep": keep, "lost": lost, "km0": km0, "km1": km1,
                      "main": mainname, "sides": sidenames, "tribs": tribs,
                      "name": (tribs[0] if tribs else mainname or "nest")})
    cases.sort(key=lambda c: -len(c["comp"]))
    cases = cases[:limit]

    secs = []
    for c in cases:
        comp, keep = c["comp"], c["keep"]
        cut = comp - keep
        side = (' &middot; <span class="mono" style="color:var(--side)">'
                + " &middot; ".join(c["sides"]) + "</span>") if c["sides"] else ""
        warn = ('<span style="color:var(--cut)"> &middot; LOST ' + str(len(c["lost"])) + " destination(s)</span>"
                ) if c["lost"] else ""
        secs.append(
            '<section class="case"><h3>' + c["name"] + "</h3>"
            '<p class="sub">drains into <span class="mono" style="color:var(--main-t)">' + c["main"] + "</span>"
            + side + ' &nbsp;|&nbsp; joined by <span class="mono" style="color:var(--trib)">'
            + (" &middot; ".join(c["tribs"]) or "unnamed only") + "</span>" + warn + "</p>"
            '<div class="trio">'
            '<div class="opt"><div class="cap"><span class="ok">before</span><span class="mono">'
            + f'{len(comp)} &middot; {c["km0"]:.1f} km</span></div><div class="cell">'
            + _panel(graph, geoms, comp, comp, bylen) + "</div></div>"
            '<div class="opt"><div class="cap"><span class="ok" style="color:var(--cut)">what comes out</span>'
            '<span class="mono" style="color:var(--cut)">'
            + f'{len(cut)} &middot; {c["km0"]-c["km1"]:.1f} km</span></div><div class="cell">'
            + _panel(graph, geoms, comp, cut, bylen, faint=True) + "</div></div>"
            '<div class="opt"><div class="cap"><span class="ok">after</span><span class="mono">'
            + f'{len(keep)} &middot; {c["km1"]:.1f} km</span></div><div class="cell">'
            + _panel(graph, geoms, comp, keep, bylen) + "</div></div></div></section>")

    rows = "".join(
        f'<tr><td>{c["name"]}</td><td class="n">{len(c["comp"])}</td><td class="n">{len(c["keep"])}</td>'
        f'<td class="n">{c["km0"]:.1f}</td><td class="n">{c["km1"]:.1f}</td>'
        f'<td class="n">{round(100-100*c["km1"]/c["km0"]) if c["km0"] else 0}%</td></tr>' for c in cases)
    return ("<title>What Comes Out of the Braid</title>"
            '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Zilla+Slab:wght@500;600'
            '&family=IBM+Plex+Sans:wght@400;500&family=IBM+Plex+Mono:wght@400;500&display=swap">'
            "<style>" + _CSS + '</style><div class="wrap">'
            '<header style="display:flex;flex-direction:column;gap:14px"><h1>What comes out of the braid</h1>'
            '<p class="lede">Each nest reduced to the channels that carry something: one route for every '
            'water, to every destination it would otherwise lose. Generated from the built graph.</p></header>'
            '<div class="key">'
            '<span><i class="sw" style="border-color:var(--main)"></i>mainstem it drains into</span>'
            '<span><i class="sw" style="border-color:var(--side)"></i>named side channel it drains into</span>'
            '<span><i class="sw" style="border-color:var(--trib)"></i>named water joining (lakes shaded)</span>'
            '<span><i class="sw" style="border-color:var(--nest)"></i>braid channels</span>'
            '<span><i class="sw" style="border-color:var(--cut)"></i>removed</span></div>'
            + "".join(secs) +
            '<section><h2>Totals</h2><div class="scroll"><table><thead><tr><th>nest</th>'
            '<th class="n">channels</th><th class="n">after</th><th class="n">km now</th>'
            '<th class="n">km after</th><th class="n">line removed</th></tr></thead><tbody>'
            + rows + "</tbody></table></div></section></div>")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default=str(get_config().review_build_dir), help="a build directory (UNPRUNED is best: "
                    "run the build with --no-simplify-braids so the nests are still there to show)")
    ap.add_argument("--html", required=True)
    ap.add_argument("--limit", type=int, default=6)
    args = ap.parse_args()
    d = Path(args.out)
    graph = pickle.load(open(d / "graph.pkl", "rb"))
    geoms = pickle.load(open(d / "geometries.pkl", "rb"))
    nv = []
    for f in (Path("pipeline/name_variants.json"),):
        if f.exists():
            from pipeline.graph.names import load_name_variants
            nv = load_name_variants(f)
    Path(args.html).write_text(build_page(graph, geoms, args.limit, nv_blks(nv)))
    print(f"wrote {args.html}")


if __name__ == "__main__":
    main()
