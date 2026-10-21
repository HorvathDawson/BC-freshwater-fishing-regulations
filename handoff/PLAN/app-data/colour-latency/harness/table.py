"""Colour-latency: fold the run summaries into the README's tables (markdown on stdout) and results.json.

  .venv/bin/python handoff/PLAN/app-data/colour-latency/harness/table.py <runs dir> <results.json>
"""
import json, sys
from pathlib import Path

d = Path(sys.argv[1])
MODES = ["none", "idle", "A", "AP", "Ds", "Dfull", "Bm", "E0", "E1", "E2", "E1A", "E1P", "E1AP"]
CONFIGS = [("gpu", "1280x800", "desktop, Metal GPU"), ("gpu_4g_cpu4", "390x844", "phone, Metal GPU, 4G + 4x CPU"),
           ("swiftshader", "390x844", "phone, SwiftShader (CPU GL)")]


def load(gpu, size, mode, kind):
    f = d / f"{gpu}-{size}-{mode}-{kind}.summary.json"
    return json.loads(f.read_text()) if f.exists() else None


out = {}
for gpu, size, label in CONFIGS:
    rows = []
    print(f"\n### {label} ({size})\n")
    print("| mode | pans grey while moving | pans grey after end | pan end -> all coloured, median / max ms | "
          "idle after end, median ms | colour work per pan, median / max ms | long tasks in pans, sum / max ms | "
          "frame gap p95 / max ms | day change: sync / settle ms, calls | setup ms | heap MB after pans / day |")
    print("|---|---|---|---|---|---|---|---|---|---|---|")
    for mode in MODES:
        p, c = load(gpu, size, mode, "probe"), load(gpu, size, mode, "clean")
        if not p and not c:
            continue
        s = c or p
        ps, pp = s["panSummary"], (p or {}).get("panSummary", {})
        day = s.get("day", {})
        setup = sum(x[1] for x in s["setup"] if not x[3].startswith("day")) if s.get("setup") else 0
        bset = [v.get("setPaintMs") for v in s.get("expr", {}).values()]
        if bset:
            setup = f"{setup:.0f} (+setPaint {bset[0]})"
        row = {
            "mode": mode,
            "greyMoving": pp.get("pansWithWrongMoving"), "greyAfter": pp.get("pansWithWrongAfter"),
            "coloured": pp.get("colouredMs"), "idle": ps["idleMs"]["median"],
            "work": ps["colourMs"], "lt": [ps["longtaskSum"], ps["longtaskMax"]],
            "gap": [ps.get("frameGapP95"), ps.get("frameGapMax")],
            "day": [day.get("syncMs"), day.get("settleMs"), day.get("calls")],
            "setup": setup, "mem": [s["mem"].get("afterPans"), s["mem"].get("afterDay")],
            "callsPerPan": ps["calls"], "expr": s.get("expr"),
        }
        rows.append(row)
        col = row["coloured"] or {}
        print(f"| {mode} | {row['greyMoving']}/12 | {row['greyAfter']}/12 | {col.get('median')} / {col.get('max')} | "
              f"{row['idle']} | {row['work']['median']} / {row['work']['max']} | {row['lt'][0]} / {row['lt'][1]} | "
              f"{row['gap'][0]} / {row['gap'][1]} | {row['day'][0]} / {row['day'][1]}, {row['day'][2]} | {setup} | "
              f"{row['mem'][0]} / {row['mem'][1]} |")
    out[f"{gpu}-{size}"] = rows
Path(sys.argv[2]).write_text(json.dumps(out, indent=1))
