"""Read-only: today's answer, donors and shares for given sections, live and under the same-WSC prototype, as
markdown rows for REPORT.md.

    .venv/bin/python handoff/PLAN/app-data/side-channels/scripts/examples.py SCRATCH_DIR GRAPH.pkl 60045 308351 ...
"""
import json
import pickle
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, ".")
from compare import INDEX, estimate, load_bundle  # noqa: E402


def fmt(v):
    return "none" if v is None else f"{v:.0f}th"


def main(scratch, graph_pkl, sids):
    with open(graph_pkl, "rb") as fh:
        g = pickle.load(fh)
    node = {v["sid"]: (k, v) for k, v in g["nodes"].items() if v["sid"]}
    live, names, gname = load_bundle()
    sw = pickle.loads((Path(scratch) / "panels_same_wsc.pkl").read_bytes())
    idx = json.loads(INDEX.read_text())["stations"]
    print("| sid | water (piece) | side ch. | own area km² | adopts (live) | live: target km², answer, donors today |"
          " same-WSC: target km², answer, donors today |")
    print("|---|---|---|---|---|---|---|")
    for s in sids:
        k, v = node[s]
        p = g["parent"].get(k)
        pv = g["nodes"][p] if p else None
        cells = []
        for panels in (live, sw):
            e = panels.get(s)
            if e is None:
                cells.append("no panel")
                continue
            pc, why, shares = estimate(e[0], e[1], idx)
            don = ", ".join(f"{st} {gname.get(st, '?').title()[:28]} {w * 100:.0f}%" for st, w in shares)
            cells.append(f"{e[1]:,.0f}; **{fmt(pc) if pc is not None else why}**; {don or '-'}")
        adopt = f"{pv['name'] or '(unnamed)'} ({pv['wsc']})" if pv else "-"
        print(f"| {s} | {names.get(s) or '(unnamed)'} | {'yes' if v['side'] else 'no'} ({v['wsc']}) | "
              f"{v['area'] or 0:,.1f} | {adopt} | {cells[0]} | {cells[1]} |")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], [int(x) for x in sys.argv[3:]])
