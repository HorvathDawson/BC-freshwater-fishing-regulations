"""Step 2 (03 S2): resolve (name, source) tuples per BLK.

Emits TAGGED NameTuples (never a scalar). Sources, priority high->low: override, gazette,
side-channel. Side-channel uses the shared-WSC main-channel BLK (the Seabird channel gets
(Fraser River, side-channel) alongside its own override name), carrying the main channel's gnis.

Manual display-name / variant overrides are NOT applied here — they live in the compiled
``name_variants.json`` and are applied to the GRAPH (reach-aware) by ``apply_name_variants`` below.
``resolve_names`` sets only the gazette + side-channel tuples.
"""

from __future__ import annotations

import json
import re
from dataclasses import replace
from pathlib import Path
from typing import Optional

from pipeline.common.models import (WATERBODY_KINDS, BlkChain, NameSource, NameTuple, NodeKind,
                             StreamGraph)

# Display priority = the NameSource declaration order (override highest). Derived so it can
# never drift out of sync with the enum (a missing source used to KeyError in _sorted_unique).
_PRIORITY = {s: i for i, s in enumerate(NameSource)}


def _sorted_unique(tuples: list[NameTuple]) -> tuple[NameTuple, ...]:
    seen: set[tuple[str, str]] = set()
    ordered: list[NameTuple] = []
    for t in sorted(tuples, key=lambda t: _PRIORITY[t.source]):
        key = (t.name, t.source.value)
        if t.name and key not in seen:
            seen.add(key)
            ordered.append(t)
    return tuple(ordered)


def resolve_names(chains: list[BlkChain]) -> list[BlkChain]:
    """Return chains with ``name_tuples`` populated + priority-ordered (gazette + side-channel;
    manual overrides are applied later on the graph by ``apply_name_variants``)."""
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

        # side-channel — the highest-magnitude DIFFERENT named BLK sharing this WSC. A side channel
        # is by definition SMALLER than its mainstem, so only inherit when that main is genuinely
        # bigger than c (`_mag(main) > _mag(c)`). Without this the mainstem grabbed its biggest
        # sibling's name too (Stave River wrongly got 'Blind Slough'), making both names ambiguous.
        siblings = [s for s in by_wsc.get(c.fwa_watershed_code, [])
                    if s.blk != c.blk and s.gnis_name]
        if siblings:
            main = max(siblings, key=_mag)
            if main.gnis_name and main.gnis_name != c.gnis_name and _mag(main) > _mag(c):
                # carry the main channel's gnis so the registry can group the whole river by it
                tuples.append(NameTuple(main.gnis_name, NameSource.side_channel, gnis_id=main.gnis_id))

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


_REACH_MIN_OVERLAP_M = 1.0   # a reach must overlap a section by more than this to name it (proximity)
_REACH_SNAP_M = 50.0         # snap an authored reach bound to a section boundary within this many
                             # route-metres (like split proximity pickup) so approximate/rounded
                             # authored measures land exactly on the cut instead of bleeding over


def _snap_reach(reach: dict, blks: list, idx: dict, graph: StreamGraph) -> dict:
    """Snap reach from_m/to_m onto the nearest section boundary (any node down_m/up_m on the target
    blks) within ``_REACH_SNAP_M``. Lets a reach be authored from an approximate coord/measure and
    still align to the real split cut. No-op if there are no candidate boundaries or none are close."""
    bounds: set[float] = set()
    for b in blks:
        for nid in idx["blk"].get(str(b), ()):
            n = graph.nodes[nid]
            bounds.add(n.down_m)
            bounds.add(n.up_m)
    if not bounds:
        return reach

    def snap(v):
        if v is None:
            return v
        near = min(bounds, key=lambda x: abs(x - v))
        return near if abs(near - v) <= _REACH_SNAP_M else v

    r = dict(reach)
    if r.get("from_m") is not None:
        r["from_m"] = snap(r["from_m"])
    if r.get("to_m") is not None:
        r["to_m"] = snap(r["to_m"])
    return r


def _as_list(target: dict, singular: str, plural: str) -> list:
    """Accept either a singular key (blk) or a plural list (blks) in a target."""
    return list(target.get(plural, [])) + ([target[singular]] if target.get(singular) else [])


def _node_matches(node, target: dict, reach: Optional[dict]) -> bool:
    # Optional kind filter. `blks` is stream-only by construction (below), but `wscs` is NOT: a LAKE
    # carries the wsc of the river threading it, so a wsc-targeted name lands on the lake as well as
    # the stream. That renamed Long Lake to "Docee River" — the Docee's wsc target caught the lake it
    # drains. Absent, the filter does nothing, so the ~3,600 existing entries are unaffected.
    kinds = {str(k).lower() for k in _as_list(target, "kind", "kinds")}
    if kinds and (("stream" if node.kind == NodeKind.stream else str(node.kind.value)) not in kinds):
        return False
    blks = _as_list(target, "blk", "blks")
    wbks = _as_list(target, "wbk", "wbks")
    gnis = _as_list(target, "gnis_id", "gnis_ids")
    wscs = _as_list(target, "wsc", "wscs")
    if blks and node.kind == NodeKind.stream and node.blk in blks:
        if reach:
            lo, hi = reach.get("from_m", node.down_m), reach.get("to_m", node.up_m)
            # proximity: require a REAL overlap, not a boundary touch. Authored measures are often
            # rounded, so a bound landing ~a few cm past a split would otherwise bleed the name into
            # the adjacent section. Ignore overlaps <= _REACH_MIN_OVERLAP_M.
            overlap = min(node.up_m, hi) - max(node.down_m, lo)
            return overlap > _REACH_MIN_OVERLAP_M
        return True
    if wbks:
        # a lake NODE by its wbk, OR a stream piece OVERLAID by a wetland/river wbk (member_wbks
        # — the wetland is not a node/split/barrier; the name rides on the through-stream piece).
        if node.kind in WATERBODY_KINDS and node.wbk in wbks:
            return True
        if node.kind == NodeKind.stream and any(w in node.member_wbks for w in wbks):
            return True
    if gnis and ((node.gnis_id and node.gnis_id in gnis)
                 or any(t.gnis_id and t.gnis_id in gnis for t in node.name_tuples)):
        return True  # lakes carry several gnis (GNIS_ID_1/2/3) on their tuples, not just the scalar
    if wscs and node.wsc and node.wsc in wscs:
        return True
    return False


def _build_target_index(graph: StreamGraph) -> dict[str, dict]:
    """One pass over the graph -> id -> [node_id] indexes (blk / wbk incl. member_wbks / gnis incl.
    name-tuple gnis / wsc), so an entry hits only the handful of nodes its target names instead of a
    full 2.1M-node scan per entry (the old O(entries x nodes) = the ~58-min registry-stage cost)."""
    from collections import defaultdict
    idx = {"blk": defaultdict(list), "wbk": defaultdict(list),
           "gnis": defaultdict(list), "wsc": defaultdict(list)}
    for nid, node in graph.nodes.items():
        if node.kind == NodeKind.stream:
            if node.blk:
                idx["blk"][node.blk].append(nid)
            for w in node.member_wbks:                # wetland/river overlay rides on the stream piece
                idx["wbk"][w].append(nid)
        elif node.kind in WATERBODY_KINDS and node.wbk:
            idx["wbk"][node.wbk].append(nid)
        for g in {node.gnis_id, *(t.gnis_id for t in node.name_tuples)}:
            if g:
                idx["gnis"][g].append(nid)
        if node.wsc:
            idx["wsc"][node.wsc].append(nid)
    return idx


def _candidate_nids(idx: dict, target: dict) -> set[str]:
    """Superset of node ids a target could name, from the index (then confirmed by _node_matches)."""
    out: set[str] = set()
    for key, (sing, plur) in (("blk", ("blk", "blks")), ("wbk", ("wbk", "wbks")),
                              ("gnis", ("gnis_id", "gnis_ids")), ("wsc", ("wsc", "wscs"))):
        for v in _as_list(target, sing, plur):
            out.update(idx[key].get(str(v), ()))
    return out


def apply_name_variants(graph: StreamGraph, entries: list[dict]) -> int:
    """Attach compiled name variants (docs/13) to graph nodes as (name, source, note) tuples and
    recompute display_name. A name flagged ``display: true`` in the file becomes the node's label
    even if its source ranks below gazette (e.g. the gauge-sourced 'Two Forty-One Creek' beats the
    inherited 'Penticton Creek'); otherwise the highest-priority tuple displays. Shouty
    stocking/gauge names are title-cased. Runs AFTER splits so reach targets hit pieces. Returns
    the number of (entry, node) applications.

    Uses a one-time target index (`_build_target_index`) so each entry visits only its candidate
    nodes — O(entries + hits) instead of O(entries x nodes)."""
    idx = _build_target_index(graph)
    authored: dict[str, str] = {}     # node_id -> explicit display name (display: true)
    touched: set[str] = set()
    applied = 0
    unaligned: list[str] = []         # reach variants painting a piece not cut at the reach bounds
    for entry in entries:
        target, reach = entry.get("target", {}), entry.get("reach")
        if reach:
            reach = _snap_reach(reach, _as_list(target, "blk", "blks"), idx, graph)
        # if the variant is scoped to a gnis, carry it so nodes it names group under that gnis
        _gids = _as_list(target, "gnis_id", "gnis_ids")
        gid = str(_gids[0]) if _gids else ""
        tuples, disp = [], ""
        for n in entry.get("names", []):
            nm = n.get("name")
            if not nm:
                continue
            try:
                src = NameSource(n.get("source", "regulation"))
            except ValueError:
                src = NameSource.alias                # unknown source -> searchable alias
            tuples.append(NameTuple(nm, src, n.get("note", ""), gnis_id=gid))
            if n.get("display"):
                disp = nm
        if not tuples:
            continue
        for nid in _candidate_nids(idx, target):
            node = graph.nodes[nid]
            if not _node_matches(node, target, reach):   # confirm (reach window / member_wbks nuance)
                continue
            if reach and node.kind == NodeKind.stream:
                lo, hi = reach.get("from_m", node.down_m), reach.get("to_m", node.up_m)
                if node.down_m < lo - 1.0 or node.up_m > hi + 1.0:   # piece spills past the reach
                    unaligned.append(f"'{tuples[0].name}' blk {node.blk} reach [{lo:.0f},{hi:.0f}] "
                                     f"paints piece [{node.down_m:.0f},{node.up_m:.0f}] — add a split at the reach bound")
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
    if unaligned:
        print(f"  [name_variants] WARNING: {len(unaligned)} reach variant(s) not aligned to a split:")
        for w in unaligned[:15]:
            print(f"    - {w}")
    return applied


def mint_waterbody_nodes(graph: StreamGraph, names: dict[str, tuple], source: NameSource,
                         kind: NodeKind = NodeKind.lake, allow_unnamed: bool = False,
                         wsc_of: dict[str, str] | None = None) -> int:
    """Mint an edgeless ``lake:{wbk}`` node for every NAMED waterbody in ``names`` that the graph does
    not already node. Returns the count minted.

    A waterbody only becomes a node when stream fids run THROUGH it. Two kinds of named, regulated
    water therefore never got one:

      - **isolated** — nothing flows in or out (Frazer Lake, Hall Road Pond, Kinglet Lake);
      - **overlaid** — a stream crosses the polygon but the polygon is a wetland/marsh, so the fids
        record it in ``member_wbks`` and no node is built for the waterbody itself (Cheam Lake,
        Minnekhada Marsh).

    Both cases produce a registry item with an EMPTY ``section_ids``, so ``op=whole`` resolves against
    an empty universe and the rule silently binds nothing — on waters people fish, several of them
    closures. Minting gives the item exactly one section to answer with.

    The node carries no edges (nothing flows through it, and for an overlay the stream keeps its own
    run) and no sidecar geometry — the client draws these from the FWA polygon layer by wbk, and
    ``add_mu_sets`` falls back to that same polygon. ``names`` is ``{wbk: ((name, gnis_id), ...)}`` as
    returned by ``get_lake_names`` / ``get_wetland_names``; the display name is the longest gazetted
    name, matching what ``add_waterbody_items`` used to put on the item so no name churns. ``kind``
    types the node and, through it, the registry item: ``NodeKind.wetland`` for the wetlands layer,
    ``lake`` otherwise.

    ``allow_unnamed`` mints a node for a waterbody with no gazetted name at all. That used to
    be pointless — "nothing could ever target it" — and it is now the opposite of true: a ZONE
    regulation targets water by where it is, not by what it is called, so an unnamed pond in a
    management unit with a spring closure is closed. Province-wide that is **417,111**
    waterbodies with no node: 333,468 of 333,526 wetlands, 82,201 lakes and 1,442 reservoirs.
    Without a node they carry no ``mus``, and every zone rule is invisible on exactly the small
    water people fish.

    Minting them here rather than stamping them in a side artifact keeps ONE source: membership
    is a property of a node, and every waterbody is a node.

    ``wsc_of`` is ``{wbk: trimmed FWA_WATERSHED_CODE}`` of the waterbody's own polygon (the same
    map `build_stream_graph` reads). A minted node used to carry NO code whatever its polygon said —
    11 of them: the split-lake leftovers (Williston `200-948755`, Kootenay, Shannon), Big Horn
    Reservoir and seven small lakes — so no watershed rule could see them.
    """
    from pipeline.common.models import StreamNode

    wsc_of = wsc_of or {}

    minted = 0
    for wbk, pairs in names.items():
        wbk = str(wbk)
        nid = f"lake:{wbk}"
        if nid in graph.nodes:
            continue
        nms = [nm for nm, _ in pairs if nm]
        if not nms and not allow_unnamed:
            continue
        graph.nodes[nid] = StreamNode(
            node_id=nid, kind=kind, wbk=wbk, wsc=wsc_of.get(wbk, ""),
            display_name=max(nms, key=len) if nms else "",
            name_tuples=tuple(NameTuple(nm, source, "", gid or "") for nm, gid in pairs if nm),
        )
        minted += 1
    return minted
