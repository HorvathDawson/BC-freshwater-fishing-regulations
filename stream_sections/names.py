"""Step 2 (03 S2): resolve (name, source) tuples per BLK.

Emits TAGGED NameTuples (never a scalar). Sources, priority high->low: override, gazette,
side-channel, upstream-inherited. Side-channel uses the shared-WSC main-channel BLK (the
Seabird channel gets (Fraser River, side-channel) alongside its own override name).

Reuses the curated pipeline/matching/feature_display_names.json override table.
upstream-inherited is deferred (needs the topology graph) — see TODO.
"""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Optional

from .models import BlkChain, NameSource, NameTuple

_PRIORITY = {
    NameSource.override: 0,
    NameSource.gazette: 1,
    NameSource.side_channel: 2,
    NameSource.upstream_inherited: 3,
}

_DEFAULT_OVERRIDES = Path(__file__).resolve().parents[1] / "pipeline" / "matching" / "feature_display_names.json"


def load_display_name_overrides(path: Optional[Path] = None) -> dict[str, dict[str, tuple]]:
    """Load feature_display_names.json into {'blk'|'fid': {key: (display_name, (variants...))}}."""
    path = Path(path) if path else _DEFAULT_OVERRIDES
    out: dict[str, dict[str, tuple]] = {"blk": {}, "fid": {}}
    if not path.exists():
        return out
    entries = json.loads(path.read_text())
    if isinstance(entries, dict):
        entries = entries.get("entries", []) or list(entries.values())
    for e in entries:
        if not isinstance(e, dict):
            continue
        name = e.get("display_name", "") or ""
        variants = tuple(e.get("name_variants", []) or [])
        payload = (name, variants)
        for blk in e.get("blue_line_keys", []) or []:
            out["blk"][str(blk)] = payload
        for fid in e.get("linear_feature_ids", []) or []:
            out["fid"][str(fid)] = payload
    return out


def _sorted_unique(tuples: list[NameTuple]) -> tuple[NameTuple, ...]:
    seen: set[tuple[str, str]] = set()
    ordered: list[NameTuple] = []
    for t in sorted(tuples, key=lambda t: _PRIORITY[t.source]):
        key = (t.name, t.source.value)
        if t.name and key not in seen:
            seen.add(key)
            ordered.append(t)
    return tuple(ordered)


def resolve_names(chains: list[BlkChain], overrides: Optional[dict] = None,
                  overrides_path: Optional[Path] = None) -> list[BlkChain]:
    """Return chains with ``name_tuples`` populated + priority-ordered."""
    if overrides is None:
        overrides = load_display_name_overrides(overrides_path)

    # Index named chains by WSC to find same-WSC main channels for side-channel inheritance.
    by_wsc: dict[str, list[BlkChain]] = {}
    for c in chains:
        by_wsc.setdefault(c.fwa_watershed_code, []).append(c)

    def _mag(c: BlkChain) -> tuple[int, int]:
        return (c.stream_magnitude or 0, c.stream_order or 0)

    out: list[BlkChain] = []
    for c in chains:
        tuples: list[NameTuple] = list(c.name_tuples)  # gazette tuple set in blk_chains

        # override (highest priority) — by blk, then any member fid
        ov = overrides["blk"].get(c.blk)
        if ov is None:
            ov = next((overrides["fid"][f.fid] for f in c.fids if f.fid in overrides["fid"]), None)
        if ov:
            name, variants = ov
            if name:
                tuples.append(NameTuple(name, NameSource.override))
            for v in variants:
                tuples.append(NameTuple(v, NameSource.override))

        # side-channel — the highest-magnitude DIFFERENT named BLK sharing this WSC
        siblings = [s for s in by_wsc.get(c.fwa_watershed_code, [])
                    if s.blk != c.blk and s.gnis_name]
        if siblings:
            main = max(siblings, key=_mag)
            if main.gnis_name and main.gnis_name != c.gnis_name:
                tuples.append(NameTuple(main.gnis_name, NameSource.side_channel))

        # TODO upstream-inherited: for still-unnamed chains, walk topology.up_adj to the nearest
        # named segment (needs the graph; run after topology in build.py).

        out.append(replace(c, name_tuples=_sorted_unique(tuples)))
    return out
