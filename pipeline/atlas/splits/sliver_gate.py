"""THE SLIVER GATE — asserts, never repairs (BOUND round, 2026-10-06).

A cut that leaves a stream piece shorter than `SLIVER_M` is a defect at its SOURCE: two geometries
that should have been one (the B.C. border and a region edge, before the outline was made exact), a
crossing inside the positional noise of an edge (before the clean cut), or a boundary drawn a hair
from a blue line's end. The user's ruling (2026-10-06) is that slivers are prevented at the source:
no snap, no merge, no metre floor. So this module only LOOKS. `pipeline.atlas.build` calls `check`
after the last cut pass; if anything is found the build stops, every offender is named on screen and
in `sliver_gate.json`, and the fix goes where the cut came from.

What is NOT a sliver: a piece FWA itself drew that short (a 0.95 m blue line between two lakes),
taken before the first cut (`short_pieces`). A cut only ever shortens a piece, so every OTHER short
piece was made by a cut.

Also refused: a clean-cut stream piece straddling an end of the inside its cutter decided (a cut the
cutter asked for that did not happen — `border.mark_inside_areas` reports it instead of guessing).
"""

from __future__ import annotations

import json
from pathlib import Path

from pipeline.common.models import NodeKind, StreamGraph

#: The shortest piece a cut may leave. Two boundaries closer than this are one place by the
#: sectionizer's own definition (`sectionizer._MERGE_PROXIMITY_M`); a piece this short is never a
#: stretch anybody fishes or a regulation describes.
SLIVER_M = 5.0


def short_pieces(graph: StreamGraph, min_m: float = SLIVER_M) -> set[str]:
    """Stream pieces shorter than `min_m` — taken BEFORE the first cut pass, these are FWA's own."""
    return {nid for nid, n in graph.nodes.items()
            if n.kind == NodeKind.stream and (n.up_m - n.down_m) < min_m}


def slivers(graph: StreamGraph, before: set[str], min_m: float = SLIVER_M) -> list[dict]:
    """Every stream piece under `min_m` a cut made, with the boundaries at its ends."""
    out = []
    for nid in sorted(short_pieces(graph, min_m) - set(before)):
        n = graph.nodes[nid]
        out.append({"node": nid, "blk": n.blk, "name": n.display_name or "",
                    "length_m": round(n.up_m - n.down_m, 3),
                    "lower": getattr(n.lower_bound, "boundary_id", None),
                    "upper": getattr(n.upper_bound, "boundary_id", None)})
    return out


def check(graph: StreamGraph, before: set[str], straddlers: list, out_dir: Path | None = None,
          min_m: float = SLIVER_M) -> None:
    """Stop the build if a cut left a sliver or a clean-cut piece straddles its cutter's inside."""
    found = slivers(graph, before, min_m)
    strad = [{"area": s[0], "node": s[1], "piece": list(s[2:4]) if len(s) > 2 else None,
              "inside": [list(map(str, x)) for x in s[4]] if len(s) > 4 else None}
             for s in sorted(straddlers, key=lambda s: (s[0], s[1]))]
    if not found and not strad:
        print(f"  sliver gate: no cut left a piece under {min_m:g} m ({len(before):,} shorter pieces "
              f"are FWA's own); every clean-cut piece lies wholly in or out")
        return
    if out_dir is not None:
        Path(out_dir).mkdir(parents=True, exist_ok=True)
        (Path(out_dir) / "sliver_gate.json").write_text(
            json.dumps({"slivers": found, "straddlers": strad}, indent=1) + "\n")
    for s in found[:40]:
        print(f"  SLIVER {s['node']} {s['name']!r} {s['length_m']} m  {s['lower']} .. {s['upper']}")
    for s in strad[:40]:
        print(f"  STRADDLER {s['node']} {s['piece']} across the inside of {s['area']}: {s['inside']}")
    raise SystemExit(f"atlas build: sliver gate — {len(found)} piece(s) under {min_m:g} m made by a "
                     f"cut, {len(strad)} clean-cut straddler(s). Fix the SOURCE of each cut; the gate "
                     f"never repairs (see sliver_gate.json)")
