"""Lakes cut into parts — the parents no regulation may bind.

`added_lakes.geojson` carries `part_of: {"wbk": …}` on each part. The atlas ingest re-stamps the fids
inside a part to the part's own lake key, so the WATER moves to the parts — but the parent's node and
section survive with its whole polygon. That section is a ghost: Kootenay Lake's is 423 km² of water
that also belongs to the Main Body (389 km²), the Upper West Arm and the Lower West Arm; Williston's
and Shannon Lake's are the same. The parent ITEM stays in the registry, because it carries the lake's
name and its gauges, and "Kootenay Lake" must still find all three parts (`item.part_of`); but a
record that binds the parent binds the ghost and none of the parts. So a parent is refused as a
binding target at ingest (`validate_catalogue`) and again by the reach builder.

EVERY LAKE WITH PARTS IS A PARENT, whatever the parts' area. The re-stamp leaves the parent's own
section carrying its full polygon in every case, so binding it is binding the ghost even when the
parts do not tile it — a lake cut only partly needs a "proper" part for the rest (Shannon Lake has
one). Measured 2026-09-23: the parts tile their parent — Kootenay 389.20 + 18.40 + 15.57 = 423.18 km²
of 423.00; Williston 1728.1 of 1726.7.
"""

from __future__ import annotations

from functools import lru_cache


@lru_cache(maxsize=4)
def _parents(path: str | None) -> tuple[tuple[str, tuple[str, ...]], ...]:
    from pipeline.atlas.waters.added_lakes.ingest import load
    out: dict[str, list[str]] = {}
    for lake in load(path):
        parent = ((lake["props"].get("part_of") or {}).get("wbk") or "")
        if parent:
            out.setdefault(f"wbk:{parent}", []).append(f"wbk:{lake['wbk']}")
    return tuple((k, tuple(sorted(v))) for k, v in sorted(out.items()))


def split_parents(registry=None, path: str | None = None) -> dict[str, list[str]]:
    """{parent item_id: [part item_ids]} — every lake the curation cut into parts. With a
    `registry`, THE REGISTRY'S OWN `part_of` (written by the atlas build from the polygons it
    ingested): the build's answer, not the curated file's. Without one (the ingest-time validator,
    which has no build to hand), the curated polygons."""
    if registry is not None:
        out: dict[str, list[str]] = {}
        for iid in sorted(registry):
            parent = getattr(registry[iid], "part_of", "") or ""
            if parent:
                out.setdefault(parent, []).append(iid)
        return out
    return {k: list(v) for k, v in _parents(path)}


def refs_to_parents(entries, parents: dict[str, list[str]]) -> list[str]:
    """Every place an entry names a split parent — `matched`, an extent's `item_id`/`item_ids`
    (entry, rule and licensing), a `tributary_excludes` — as one line each. Empty = clean.

    `entries` are entry dicts; `parents` is `split_parents()`."""
    if not parents:
        return []
    out: list[str] = []

    def scan(exts, where):
        for ex in exts or []:
            if not isinstance(ex, dict):
                continue
            for i in ([ex["item_id"]] if ex.get("item_id") else []) + list(ex.get("item_ids") or []):
                if i in parents:
                    out.append(f"{where}: {i} is cut into {', '.join(parents[i])} — name the "
                               f"part(s)")

    for e in entries:
        eid = e.get("entry_id")
        for i in e.get("matched") or []:
            if i in parents:
                out.append(f"{eid} matched: {i} is cut into {', '.join(parents[i])} — match the "
                           f"part(s)")
        scan(e.get("extents"), f"{eid} extents")
        for r in e.get("rules") or []:
            if isinstance(r, dict):
                scan(r.get("extents"), f"{eid}/{r.get('rule_id')}")
                scan(r.get("tributary_excludes"), f"{eid}/{r.get('rule_id')} tributary_excludes")
        for x in e.get("licensing") or []:
            if isinstance(x, dict):
                scan(x.get("extents"), f"{eid}#{x.get('id')}")
                scan(x.get("tributary_excludes"), f"{eid}#{x.get('id')} tributary_excludes")
    return out
