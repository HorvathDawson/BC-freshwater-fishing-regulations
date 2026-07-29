"""Load splits.json, resolve anchors, and write back splits.resolved.json (04).

Authored splits live in a curated splits.json (see splits.example.json / splits.schema.md).
Resolution (anchor -> route measure per targeted BLK) happens in the `sections` step, after
the topology graph exists; `resolve_splits` uses anchors.py and is a stub until then.
`load_split_defs` is implemented so the schema is validated at authoring time.
"""

from __future__ import annotations

import json
from pathlib import Path

from .models import BlkChain, SplitDef, SplitPoint


def load_split_defs(path: str) -> list[SplitDef]:
    """Parse + validate a splits.json (or splits.example.json). Fails loud on bad targets."""
    data = json.loads(Path(path).read_text())
    entries = data.get("splits", data) if isinstance(data, dict) else data
    defs = [SplitDef.from_dict(e) for e in entries if isinstance(e, dict) and e.get("id")]
    ids = [d.id for d in defs]
    dupes = {i for i in ids if ids.count(i) > 1}
    if dupes:
        raise ValueError(f"duplicate split ids: {sorted(dupes)}")
    return defs


def resolve_splits(defs: list[SplitDef], chains: list[BlkChain],
                   mu_polys: dict | None = None) -> list[SplitPoint]:
    """Resolve each SplitDef to SplitPoint(s) via anchors.py (geometry only, before the graph)."""
    from .anchors import resolve_split_defs
    return resolve_split_defs(defs, chains, mu_polys=mu_polys)


def write_resolved(points: list[SplitPoint], path: str) -> None:
    """Write resolved SplitPoints to a reviewable JSON sidecar (splits.resolved.json).
    Surfaces ``concern``/``picked_up`` so ambiguous or deduped splits are never silent."""
    rows = []
    for p in points:
        row = {"split_id": p.split_id, "blk": p.blk, "route_measure": round(p.route_measure, 2),
               "label": p.label, "anchor_type": p.anchor_type.value}
        if getattr(p, "offset_m", 0.0):
            row["offset_m"] = round(p.offset_m, 1)
        if getattr(p, "picked_up", False):
            row["picked_up"] = True
        if getattr(p, "concern", ""):
            row["concern"] = p.concern
        rows.append(row)
    Path(path).write_text(json.dumps(rows, indent=2))
