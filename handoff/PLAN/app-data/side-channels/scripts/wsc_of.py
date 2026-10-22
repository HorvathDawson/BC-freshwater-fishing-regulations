"""Read-only: the FWA watershed code of given sids and of the reach each adopted (extract_graph.py's table).

    .venv/bin/python handoff/PLAN/app-data/side-channels/scripts/wsc_of.py GRAPH.pkl 60045 60043
"""
import pickle
import sys

if __name__ == "__main__":
    with open(sys.argv[1], "rb") as fh:
        g = pickle.load(fh)
    by_sid = {v["sid"]: k for k, v in g["nodes"].items()}
    for s in map(int, sys.argv[2:]):
        nid = by_sid[s]
        v = g["nodes"][nid]
        p = g["parent"].get(nid)
        pv = g["nodes"][p] if p else {}
        print(f"sid {s} {v['name'] or '-'} blk={v['blk']} wsc={v['wsc']}\n"
              f"    -> {pv.get('name')} sid={pv.get('sid')} blk={pv.get('blk')} wsc={pv.get('wsc')}")
