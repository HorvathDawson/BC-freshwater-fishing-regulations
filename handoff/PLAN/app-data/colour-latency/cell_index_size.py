"""Colour-latency, option E: how big would a province section->cell index be? Measured on a patch of z14 tiles (every
section the atlas draws at its deepest zoom), aggregated to z10/z11/z12 cells; the (cell, id) pair count per section
is the duplication factor, scaled to the province's 1,957,202 sections.

  .venv/bin/python handoff/PLAN/app-data/colour-latency/cell_index_size.py <z14.json from harness/build-cell-index.mjs>
"""
import json, sys

d = json.load(open(sys.argv[1]))
ids = set()
for c in d["cells"].values():
    for v in c.values():
        ids.update(v)
print(f"patch: {len(d['cells'])} z14 tiles, {len(ids)} sections")
for cz in (10, 11, 12):
    sh = 14 - cz
    cells = {}
    for k, c in d["cells"].items():
        x, y = map(int, k.split("/"))
        s = cells.setdefault((x >> sh, y >> sh), set())
        for v in c.values():
            s.update(v)
    pairs = sum(len(s) for s in cells.values())
    dup = pairs / len(ids)
    # ~2.5 bytes per pair: delta-coded sorted ids per cell as varints (ids in a cell are scattered: handles are not
    # spatially ordered), before gzip
    print(f"z{cz} cells: {len(cells)}, pairs {pairs}, {dup:.2f} cells per section, ids per cell median "
          f"{sorted(len(s) for s in cells.values())[len(cells) // 2]} -> province ~{1957202 * dup / 1e6:.2f} M pairs"
          f" ~{1957202 * dup * 2.5 / 1e6:.1f} MB raw")
