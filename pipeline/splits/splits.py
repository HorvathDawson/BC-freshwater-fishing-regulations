"""Load splits.json, resolve anchors, and write back splits.resolved.json (04).

Authored splits live in a curated splits.json (schema: the SplitDef fields consumed by `_from_dict`).
Resolution (anchor -> route measure per targeted BLK) happens in the `sections` step, after
the topology graph exists; `resolve_splits` uses anchors.py and is a stub until then.
`load_split_defs` is implemented so the schema is validated at authoring time.
"""

from __future__ import annotations

import json
from pathlib import Path

from pipeline.models import BlkChain, SplitDef, SplitPoint


def _norm_target(applies: dict | None) -> dict:
    """An ``applies_to`` block -> a flat {gnis_id|blk|wsc} target for SplitDef.from_dict."""
    applies = applies or {}
    tgt = {}
    for k in ("gnis_id", "blk", "wsc"):
        if applies.get(k):
            tgt[k] = applies[k]
    if not tgt and applies.get("gnis_ids"):
        tgt["gnis_id"] = applies["gnis_ids"][0]         # multi-gnis waterbody -> first (rare)
    return tgt


def _flatten_waterbodies(data: dict) -> tuple[list[dict], list[tuple[str, str]]]:
    """By-waterbody shape -> flat split dicts. Each split's target is its own ``applies_to`` if it
    has one, else the waterbody's ``applies_to``. Both use the same shape (gnis_id/blk/wsc/gnis_ids).
    Returns (flat_dicts, untargeted[(waterbody, split_id)]) — splits that resolve to NO target
    (waterbody applies_to is null and the split has none of its own)."""
    flat: list[dict] = []
    untargeted: list[tuple[str, str]] = []
    for wb in data.get("waterbodies", []):
        wb_tgt = _norm_target(wb.get("applies_to"))
        for s in wb.get("splits", []):
            if not isinstance(s, dict) or not s.get("id"):
                continue
            d = dict(s)
            tgt = _norm_target(s.get("applies_to")) or wb_tgt   # per-split override wins
            if not tgt:
                untargeted.append((wb.get("name", "?"), s["id"]))
            d.pop("applies_to", None)
            d.update(tgt)                                        # flat target for SplitDef.from_dict
            flat.append(d)
    return flat, untargeted


def load_split_defs(path: str) -> list[SplitDef]:
    """Parse + validate a splits.json. Accepts the by-waterbody shape ({"waterbodies": [...]})
    or the legacy flat shape ({"splits": [...]} / a bare list). Fails loud on bad targets,
    but skips (with a warning) splits whose waterbody has no ``applies_to`` yet."""
    data = json.loads(Path(path).read_text())
    if isinstance(data, dict) and "waterbodies" in data:
        entries, untargeted = _flatten_waterbodies(data)
        if untargeted:                       # a null-applies_to waterbody whose split has no own target
            raise ValueError(
                "splits.json validation: split(s) with no resolvable target (waterbody applies_to is "
                "null and the split has no applies_to of its own): "
                + ", ".join(f"{wb}:{sid}" for wb, sid in untargeted[:12]))
    elif isinstance(data, dict):
        entries = data.get("splits", [])
    else:
        entries = data
    defs: list[SplitDef] = []
    skipped: list[tuple[str, str]] = []
    for e in entries:
        if not isinstance(e, dict) or not e.get("id"):
            continue
        try:
            defs.append(SplitDef.from_dict(e))
        except ValueError as exc:
            skipped.append((e.get("id", "?"), str(exc)))
    if skipped:
        import logging
        logging.getLogger(__name__).warning(
            "load_split_defs: skipped %d split(s) with no resolvable target (needs applies_to): %s",
            len(skipped), ", ".join(i for i, _ in skipped[:8]))
    ids = [d.id for d in defs]
    dupes = {i for i in ids if ids.count(i) > 1}
    if dupes:
        raise ValueError(f"duplicate split ids: {sorted(dupes)}")
    return defs


def resolve_splits(defs: list[SplitDef], chains: list[BlkChain],
                   mu_polys: dict | None = None) -> list[SplitPoint]:
    """Resolve each SplitDef to SplitPoint(s) via anchors.py (geometry only, before the graph)."""
    from pipeline.splits.anchors import resolve_split_defs
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
