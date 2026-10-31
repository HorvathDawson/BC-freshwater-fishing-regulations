"""T322 DRY RUN: what the confirmed-slough fold WOULD do on the live atlas — no atlas build, nothing live written.

Reads the promoted atlas (`graph.pkl`, `registry.json`) read-only and the applied calls
(`data/curated/waters/slough_calls.json`), runs `registry.flowing.fold_confirmed_sloughs` in memory, prints how many
fold (into which streams) and how many are ignored and why, and writes the full per-wbk report as JSON.

The graph is ~600 MB on disk; run it as a heavy job:

    scripts/heavy.sh t322-preview 30 -- .venv/bin/python -m pipeline.tools.audit.slough_fold_preview \
        [--atlas data/generated/atlas/full] [--out data/generated/scratch/slough_fold_preview.json]
"""
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path


def summary(rep: dict, top: int = 40) -> str:
    rows = rep["rows"]
    lines = [f"confirmed calls: {rep['confirmed']}",
             "outcomes: " + ", ".join(f"{k} {v}" for k, v in rep["counts"].items())]
    folded = [r for r in rows if r["outcome"] == "folded"]
    lines.append(f"FOLD: {len(folded)} polygon(s) into {len(rep['streams'])} stream(s); "
                 f"{sum(len(s['absorbed']) for s in rep['streams'].values())} registry item(s) absorbed")
    by = sorted(rep["streams"].items(), key=lambda kv: (-kv[1]["polygons"], kv[0]))
    for sid, s in by[:top]:
        ab = f"  absorbs {', '.join(s['absorbed'])}" if s["absorbed"] else ""
        lines.append(f"  {s['polygons']:4d}  {sid:<22} {s['name']}{ab}")
    if len(by) > top:
        lines.append(f"  ... {len(by) - top} more stream(s) (see the JSON)")
    why = Counter(r["why"].split(":")[0] for r in rows if r["outcome"] == "ignored")
    lines.append(f"IGNORED: {sum(why.values())}")
    for w, n in why.most_common():
        lines.append(f"  {n:4d}  {w}")
    st = [r for r in rows if r["outcome"] == "already stream"]
    if st:
        lines.append(f"ALREADY STREAM (no change): {len(st)} — " +
                     ", ".join(sorted({f"{r['stream']} {r['name']}" for r in st}))[:400])
    lines.append(f"other waters' lake edges that become confluences: {len(rep['boundaries_rewritten'])}")
    return "\n".join(lines)


def main(argv=None) -> int:
    from pipeline.atlas.registry import load_registry
    from pipeline.atlas.registry.flowing import fold_confirmed_sloughs
    from pipeline.common.curated import GENERATED
    from pipeline.common.io.serialize import read_artifact
    from pipeline.tools.slough_calls import load

    ap = argparse.ArgumentParser(prog="python -m pipeline.tools.audit.slough_fold_preview",
                                 description=__doc__.split("\n")[0])
    ap.add_argument("--atlas", type=Path, default=None, help="the atlas build dir (default: the promoted one)")
    ap.add_argument("--out", type=Path, default=None,
                    help="the JSON report (default data/generated/scratch/slough_fold_preview.json)")
    a = ap.parse_args(argv)
    atlas = Path(a.atlas or GENERATED.require_build())
    out = Path(a.out or GENERATED.base / "scratch" / "slough_fold_preview.json")
    wbks = load()
    registry = load_registry(atlas / "registry.json")
    graph = read_artifact(str(atlas / "graph.pkl"))
    rep: dict = {}
    fold_confirmed_sloughs(registry, graph, wbks, rep)
    rep["atlas"] = str(atlas)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(rep, indent=1) + "\n", encoding="utf-8")
    print(summary(rep))
    print(f"-> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
