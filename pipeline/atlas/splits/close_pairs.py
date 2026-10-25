"""THE CLOSE-PAIR REPORT (T179, owner 2026-10-25) — a report, never a gate.

Every CURATED cut standing 5–25 m from any other boundary on its blue line: another curated cut, a
gauge, area, border or length cut, a lake edge, or the line's own end. Under 5 m the sliver gate
already stops the build (`sliver_gate`); 5–25 m is a real piece, but short enough that the two are
often one place written twice (the Moyie "U.S. border" 12 m from the Region 4 line, before it was
anchored on the border) — or two real places (Clayhurst / 7B, Chilkoot / 6: area-vs-area 10–20 m,
left as they are). The owner reads the list; nothing here moves or merges a cut.

Read off the FINISHED graph, after the last cut pass, so every family is where it stands.
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

from pipeline.common.models import BoundaryKind, NodeKind, StreamGraph
from pipeline.common.tolerances import SLIVER_M

#: The band reported: [LO_M, HI_M).
LO_M = SLIVER_M          # 5 m — under it the sliver gate stops the build
HI_M = 25.0
FILENAME = "close_pairs.json"


def _family(b) -> str:
    """What a boundary is, for the report: curated / gauge / area / border / length / lake_edge / line_end."""
    from pipeline.atlas.splits.sectionizer import is_authored

    if b is None or b.kind in (BoundaryKind.outlet, BoundaryKind.headwaters):
        return "line_end"
    if b.kind == BoundaryKind.lake:
        return "lake_edge"
    return "curated" if is_authored(b.source) else str(b.source)


def close_pairs(graph: StreamGraph, lo_m: float = LO_M, hi_m: float = HI_M) -> list[dict]:
    """One row per curated place with another place on its blue line `lo_m <= distance < hi_m`: the
    nearest such place. A place = a piece end (every id merged there is one boundary already)."""
    by_blk: dict[str, dict[float, object]] = defaultdict(dict)
    for n in graph.nodes.values():
        if n.kind != NodeKind.stream or not n.blk:
            continue
        for m, bd in ((n.down_m, n.lower_bound), (n.up_m, n.upper_bound)):
            if by_blk[n.blk].get(m) is None:
                by_blk[n.blk][m] = bd
    out: list[dict] = []
    for blk in sorted(by_blk):
        places = sorted(by_blk[blk].items(), key=lambda kv: kv[0])
        for i, (m, bd) in enumerate(places):
            if bd is None or _family(bd) != "curated":
                continue
            near = []
            for js in (range(i - 1, -1, -1), range(i + 1, len(places))):
                for j in js:
                    d = abs(places[j][0] - m)
                    if d >= hi_m:
                        break
                    if d >= lo_m:
                        near.append((d, places[j][0], places[j][1]))
            if not near:
                continue
            d, om, ob = min(near, key=lambda t: (t[0], t[1]))
            out.append({
                "blk": blk, "distance_m": round(d, 2),
                "curated": str(bd.boundary_id), "label": bd.label or "", "at_m": round(m, 2),
                "aliases": list(bd.aliases or ()),
                "other": str(ob.boundary_id) if ob is not None else "end",
                "other_kind": _family(ob), "other_label": (ob.label if ob is not None else "") or "",
                "other_m": round(om, 2)})
    return out


def write(out_dir, rows: list[dict]) -> Path:
    path = Path(out_dir) / FILENAME
    path.write_text(json.dumps(rows, indent=1) + "\n")
    return path


def report(graph: StreamGraph, out_dir) -> list[dict]:
    """Write `close_pairs.json` and print the count by kind of the other boundary."""
    rows = close_pairs(graph)
    path = write(out_dir, rows)
    by: dict[str, int] = defaultdict(int)
    for r in rows:
        by[r["other_kind"]] += 1
    mix = ", ".join(f"{n} {k}" for k, n in sorted(by.items(), key=lambda kv: (-kv[1], kv[0])))
    print(f"  close pairs: {len(rows)} curated cut(s) {LO_M:g}-{HI_M:g} m from another boundary"
          f"{' (' + mix + ')' if mix else ''} -> {path}  (report only)")
    return rows
