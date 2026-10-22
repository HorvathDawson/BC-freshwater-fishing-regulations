"""Read-only: for every section with a donor panel, which members are its OWN gauges, how far along the water each
sits, and how big each is against the section — the facts the own-gauge rule decides on. Writes one pickle.

Inputs (all read-only):
  graph_small.pkl     the side-channels lane's one-load extract of the live atlas graph (extract_graph.py there):
                      per node sid, kind, wsc, length, model area; the edges (from = upstream, to = downstream)
  panels.pkl          `sid -> (members, target area)` as the bundle's panel pass builds them (prototype.py there;
                      `built/panels_same_wsc.pkl` is option A as merged, 0 sections differ from the shipped code)
  the live bundle     gauge (station -> sid, item_id), item_section/item (sid -> water), gauge_stats (quantities)

OWN = the station's water (gauge.item_id) is the section's water. DISTANCE = along that water only (stream sections of
the same item), shortest path, from the gauge (which stands at the DOWNSTREAM end of its own section) to the nearer
end of the target section, walking upstream or downstream (never both), the same pure-direction walk the panel's
candidate pass does. A side channel answers with its river's panel (`mainstem_of`, option A), so its own gauges are
its river reach's.

    .venv/bin/python handoff/PLAN/app-data/own-gauge/scripts/own_table.py GRAPH_SMALL.pkl PANELS.pkl OUT.pkl
"""
import collections
import heapq
import pickle
import sqlite3
import sys

BUNDLE = "/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/data/generated/bundle/bundle.sqlite"
CAP_M = 1_000_000          # walk this far at most: far past any threshold worth proposing
SIDE = {"1100", "1350"}
HOPS = 4


def same_wsc_parent(nodes, up, down):
    """`mainstem_of` as merged (option A), over the extract's dict nodes."""
    parent = {}
    for nid, n in nodes.items():
        if not (n["side"] and n["kind"] == "stream"):
            continue
        mine, best, seen, frontier = n["area"] or 0.0, None, {nid}, [nid]
        for _ in range(HOPS):
            nxt = []
            for cur in frontier:
                for other in (*up[cur], *down[cur]):
                    if other in seen:
                        continue
                    seen.add(other)
                    o = nodes.get(other)
                    if o is None:
                        continue
                    nxt.append(other)
                    if o["side"] or o["kind"] != "stream" or o["wsc"] != n["wsc"]:
                        continue
                    a = o["area"]
                    if a and a > mine and (best is None or a > best[0]):
                        best = (a, other)
            frontier = nxt
        if best is not None:
            parent[nid] = best[1]
    return parent


def main(graph_pkl, panels_pkl, out):
    g = pickle.loads(open(graph_pkl, "rb").read())
    nodes = g["nodes"]
    up, down = collections.defaultdict(list), collections.defaultdict(list)
    for a, b in g["edges"]:
        up[b].append(a)
        down[a].append(b)
    node_of = {v["sid"]: k for k, v in nodes.items() if v["sid"] is not None}
    panels = pickle.loads(open(panels_pkl, "rb").read())

    db = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    water = {}
    for sid, item in db.execute("SELECT s.sid, i.item_id FROM item_section s JOIN item i USING(ord) ORDER BY i.item_id"):
        water.setdefault(sid, item)
    gauges = {s: (sid, item) for s, sid, item in db.execute("SELECT station, sid, item_id FROM gauge WHERE sid IS NOT NULL")}
    measures = collections.defaultdict(set)
    for s, p in db.execute("SELECT station, parameter FROM gauge_stats"):
        measures[s].add(p)

    stations = {m[0] for ms, _a in panels.values() for m in ms}
    # station -> {node: (distance m, direction)} along its own water
    reach = {}
    for st in stations:
        if st not in gauges or not gauges[st][1]:
            continue
        gsid, item = gauges[st]
        start = node_of.get(gsid)
        if start is None:
            continue
        got = {start: (0.0, "at")}
        for adj, direction in ((up, "up"), (down, "down")):
            dist = {start: 0.0}
            heap = [(0.0, start)]
            while heap:
                d, cur = heapq.heappop(heap)
                if d > dist.get(cur, float("inf")) or d > CAP_M:
                    continue
                step = nodes[cur]["length"] or 0.0
                if direction == "down" and cur == start:
                    step = 0.0          # the gauge stands at its section's downstream end
                for nxt in adj[cur]:
                    n = nodes.get(nxt)
                    if n is None or n["kind"] != "stream" or water.get(n["sid"]) != item:
                        continue
                    nd = d + step
                    if nd < dist.get(nxt, float("inf")):
                        dist[nxt] = nd
                        heapq.heappush(heap, (nd, nxt))
            for k, d in dist.items():
                if k != start and (k not in got or d < got[k][0]):
                    got[k] = (d, direction)
        reach[st] = got

    parent = same_wsc_parent(nodes, up, down)
    rows = {}
    for sid, (members, target) in panels.items():
        nid = node_of.get(sid)
        if nid is None:
            continue
        ask = parent.get(nid, nid)       # a side channel answers as its river reach
        own = []
        for st, role, area, years, reg, same in members:
            hit = reach.get(st, {}).get(ask)
            if hit is None:
                continue
            own.append(dict(station=st, distance_m=hit[0], direction=hit[1], area=area, years=years,
                            same=same, role=role, measures=tuple(sorted(measures.get(st, ())))))
        rows[sid] = dict(own=own, target=target, length=nodes[nid]["length"] or 0.0,
                         water=water.get(nodes[ask]["sid"]), side=nid in parent)
    pickle.dump(dict(rows=rows, measures={k: tuple(sorted(v)) for k, v in measures.items()}), open(out, "wb"),
                protocol=pickle.HIGHEST_PROTOCOL)
    n_own = sum(1 for r in rows.values() if r["own"])
    print(f"{len(rows):,} panel sections; {n_own:,} hold an own gauge in their panel")


if __name__ == "__main__":
    main(*sys.argv[1:4])
