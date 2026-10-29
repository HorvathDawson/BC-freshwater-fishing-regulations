"""T224 MINED NAMES ARE LABELS, NOT GROUPING KEYS (owner 2026-10-28).

A name the owner accepted from a mined source (`data/curated/waters/mined_names.json`: OpenStreetMap, a city stream
layer, an ECCC station) says what people CALL a feature. It never says what the feature IS, so it may not change:
- what the braid prune keeps (a named piece is not a prune candidate — 409 braid pieces were kept for that reason);
- which item a feature belongs to (a named ditch stays in its river's item: the Quinsam diversion headpond joined
  the Quinsam River and took its rules);
- what a slough absorbs (`registry.flowing`: the unnamed lake polygon 329292348 became part of the OSM-named
  "Silverstream Slough" stream and took stream rules instead of lake rules).

So the names never reach the graph. They are attached here, to the FINISHED registry, after every grouping pass:
- a feature some item already owns: the name joins that item's `variants` + `name_sources` (search, "also X · source");
  the item's id, name, kind and sections do not move;
- a feature no item owns (an FWA-unnamed line): an item of its own over exactly that feature's sections, named by the
  mined name (T224: an FWA-unnamed water displays its best variant) — never merged with another feature by name;
- a feature that is not in the atlas (pruned, outside the build): the name is reported, not applied.
Only stream lines are targets (every mined entry names `blks`); a lake polygon is never touched.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import replace

from pipeline.common.models import WATERBODY_KINDS, NameRecord, RegistryItem, StreamGraph
from pipeline.common.name_sources import is_official, label_of


def _records(names: list[dict]) -> list[NameRecord]:
    return [NameRecord(name=n["name"], source=n.get("source") or "alias",
                       label=n.get("source_label") or label_of(n.get("source") or "alias"),
                       ref=n.get("source_ref") or "", official=is_official(n.get("source") or "alias"))
            for n in names if n.get("name")]


def add_mined_names(registry: dict[str, RegistryItem], graph: StreamGraph, entries: list[dict],
                    report: dict | None = None) -> dict[str, RegistryItem]:
    from pipeline.atlas.registry.build import _ref_ids
    rep = report if report is not None else {}
    rep.update(labelled=0, own_items=0, absent=0, absent_blks=[])
    by_blk: dict[str, list] = defaultdict(list)
    for n in graph.nodes.values():
        if n.kind not in WATERBODY_KINDS and n.blk:
            by_blk[str(n.blk)].append(n)
    owner: dict[str, list[str]] = defaultdict(list)          # section -> the non-area items that hold it
    for iid, it in registry.items():
        if it.kind != "area":
            for s in it.section_ids:
                owner[s].append(iid)
    out = dict(registry)
    for e in entries:
        recs = _records(e.get("names") or [])
        blks = [str(b) for b in ((e.get("target") or {}).get("blks") or [])]
        nodes = [n for b in blks for n in by_blk.get(b, ())]
        if not recs or not nodes:
            rep["absent"] += 1
            rep["absent_blks"].extend(blks)
            continue
        sids = sorted({n.node_id for n in nodes})
        owners = sorted({i for s in sids for i in owner.get(s, ())})
        if owners:
            for iid in owners:
                it = out[iid]
                have = {r.name for r in it.name_sources} | {it.name, *it.variants}
                new = [r for r in recs if r.name not in have]
                if new:
                    out[iid] = replace(it, variants=tuple(sorted({*it.variants, *(r.name for r in new)})),
                                       name_sources=(*it.name_sources, *new))
            rep["labelled"] += 1
            continue
        lowest = min(nodes, key=lambda n: (str(n.blk), n.node_id))
        iid = f"blk:{lowest.blk}"
        if iid in out:                                       # never fold two features into one item by a name
            iid = f"blk:{lowest.blk}:{lowest.node_id}"
        out[iid] = RegistryItem(id=iid, name=recs[0].name, kind="stream",
                                variants=tuple(sorted({r.name for r in recs})), name_sources=tuple(recs),
                                section_ids=tuple(sids), ref_ids=_ref_ids(nodes))
        for s in sids:
            owner[s].append(iid)
        rep["own_items"] += 1
    return out
