"""Step 2 (03 S2): resolve (name, source) tuples per BLK.

Emits TAGGED NameTuples (never a scalar). Sources, priority high->low: override, gazette,
side-channel, upstream-inherited. Side-channel uses the shared-WSC main-channel BLK (the
Seabird channel gets (Fraser River, side-channel) alongside its own override name).

Reuses the curated pipeline/matching/feature_display_names.json override table.
upstream-inherited is deferred (needs the topology graph) — see TODO.
"""

from __future__ import annotations

import json
import re
from dataclasses import replace
from pathlib import Path
from typing import Optional

from .models import BlkChain, NameSource, NameTuple, NodeKind, StreamGraph

# Display priority = the NameSource declaration order (override highest). Derived so it can
# never drift out of sync with the enum (a missing source used to KeyError in _sorted_unique).
_PRIORITY = {s: i for i, s in enumerate(NameSource)}

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

        # NOTE manual display-name overrides (feature_display_names.json) are NOT applied here
        # anymore — they moved into the compiled name_variants.json + names.apply_name_variants,
        # which runs on the GRAPH (reach-aware, so 'Two Forty-One Creek above Greyback Lake' hits
        # only the upper piece, not the whole shared BLK). resolve_names sets gazette + side-channel.

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


# ------------------------------------------------------------ name variations (docs/13, graph)

_ABBREV = [(re.compile(r"\bL\.$"), "Lake"), (re.compile(r"\bCr\.$"), "Creek"),
           (re.compile(r"\bR\.$"), "River")]


def _display_case(name: str) -> str:
    """Title-case a SHOUTING/abbreviated name (stocking/bathy/synopsis) for display; leave
    already-cased names (override/gazette like 'McArthur') untouched."""
    if not name or name != name.upper():
        return name
    s = name.title()
    for pat, full in _ABBREV:
        s = pat.sub(full, s)
    return s


def load_name_variants(path) -> list[dict]:
    p = Path(path)
    return json.loads(p.read_text()) if p.exists() else []


def _as_list(target: dict, singular: str, plural: str) -> list:
    """Accept either a singular key (blk) or a plural list (blks) in a target."""
    return list(target.get(plural, [])) + ([target[singular]] if target.get(singular) else [])


def _node_matches(node, target: dict, reach: Optional[dict]) -> bool:
    blks = _as_list(target, "blk", "blks")
    wbks = _as_list(target, "wbk", "wbks")
    gnis = _as_list(target, "gnis_id", "gnis_ids")
    wscs = _as_list(target, "wsc", "wscs")
    if blks and node.kind == NodeKind.stream and node.blk in blks:
        if reach:
            lo, hi = reach.get("from_m", node.down_m), reach.get("to_m", node.up_m)
            return node.up_m > lo and node.down_m < hi   # piece overlaps the reach window
        return True
    if wbks:
        # a lake NODE by its wbk, OR a stream piece OVERLAID by a wetland/river wbk (member_wbks
        # — the wetland is not a node/split/barrier; the name rides on the through-stream piece).
        if node.kind == NodeKind.lake and node.wbk in wbks:
            return True
        if node.kind == NodeKind.stream and any(w in node.member_wbks for w in wbks):
            return True
    if gnis and node.gnis_id and node.gnis_id in gnis:
        return True
    if wscs and node.wsc and node.wsc in wscs:
        return True
    return False


def apply_name_variants(graph: StreamGraph, entries: list[dict]) -> int:
    """Attach compiled name variants (docs/13) to graph nodes as (name, source, note) tuples and
    recompute display_name. A name flagged ``display: true`` in the file becomes the node's label
    even if its source ranks below gazette (e.g. the gauge-sourced 'Two Forty-One Creek' beats the
    inherited 'Penticton Creek'); otherwise the highest-priority tuple displays. Shouty
    stocking/gauge names are title-cased. Runs AFTER splits so reach targets hit pieces. Returns
    the number of (entry, node) applications."""
    authored: dict[str, str] = {}     # node_id -> explicit display name (display: true)
    touched: set[str] = set()
    applied = 0
    for entry in entries:
        target, reach = entry.get("target", {}), entry.get("reach")
        tuples, disp = [], ""
        for n in entry.get("names", []):
            nm = n.get("name")
            if not nm:
                continue
            try:
                src = NameSource(n.get("source", "regulation"))
            except ValueError:
                src = NameSource.alias                # unknown source -> searchable alias
            tuples.append(NameTuple(nm, src, n.get("note", "")))
            if n.get("display"):
                disp = nm
        if not tuples:
            continue
        for nid, node in graph.nodes.items():
            if not _node_matches(node, target, reach):
                continue
            graph.nodes[nid] = replace(node, name_tuples=_sorted_unique(list(node.name_tuples) + tuples))
            touched.add(nid)
            if disp:
                authored[nid] = disp
            applied += 1

    for nid in touched:                               # finalize display once, per touched node
        node = graph.nodes[nid]
        if nid in authored:
            display = _display_case(authored[nid])
        else:
            top = node.name_tuples[0] if node.name_tuples else None
            display = _display_case(top.name) if top else node.display_name
        graph.nodes[nid] = replace(node, display_name=display)
    return applied
