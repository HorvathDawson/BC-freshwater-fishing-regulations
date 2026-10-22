"""Read-only: for given sids, the node, whether it is a side channel, which reach `mainstem_of` adopted, and the
neighbours within two hops (from extract_graph.py's table).

    .venv/bin/python handoff/PLAN/app-data/side-channels/scripts/neighbours.py GRAPH.pkl 60045 317336
"""
import collections
import pickle
import sys


def load(path):
    with open(path, "rb") as fh:
        g = pickle.load(fh)
    by_sid = {v["sid"]: k for k, v in g["nodes"].items()}
    up, down = collections.defaultdict(list), collections.defaultdict(list)
    for a, b in g["edges"]:
        down[a].append(b)
        up[b].append(a)
    return g, by_sid, up, down


def line(g, nid):
    v = g["nodes"][nid]
    return (f"sid {v['sid']:>7} {nid:22} {v['kind']:6} {(v['name'] or '-')[:28]:28} mag={v['mag']} "
            f"et={','.join(v['et'])} side={int(v['side'])} area={v['area'] or 0:,.1f} len={v['length']:,.0f}m")


if __name__ == "__main__":
    g, by_sid, up, down = load(sys.argv[1])
    for s in map(int, sys.argv[2:]):
        nid = by_sid[s]
        print("==", line(g, nid))
        p = g["parent"].get(nid)
        print("   adopted ->", line(g, p) if p else None)
        for label, adj in (("down", down), ("up", up)):
            for o in adj[nid]:
                print(f"   {label:4} ", line(g, o))
                for o2 in adj[o]:
                    if o2 != nid:
                        print(f"   {label:4}2", line(g, o2))
