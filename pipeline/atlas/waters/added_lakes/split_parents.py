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
    """Every place an entry BINDS a split parent — `matched`, an extent's `item_id`/`item_ids`
    (entry scope, rule and licensing, extents and carve-outs: `walk.iter_item_refs`) — as one line
    each. Empty = clean. An extent's `outside_items` may name a parent: subtracting "Kootenay Lake"
    from the Creston Valley WMA takes out its parts and its ghost (`reach.extent.
    whole_water_sections`), which is what the four Creston records do.

    `entries` are entry dicts; `parents` is `split_parents()`."""
    return [msg for e in entries for _path, msg in refs_to_parents_at(e, parents)]


def refs_to_parents_at(entry: dict, parents: dict[str, list[str]]) -> list[tuple[tuple, str]]:
    """`refs_to_parents` for one entry, each line with the PATH of the id it names (the entry's own
    keys, `walk.iter_item_refs`), so the review app addresses it to the field."""
    from pipeline.regs.parsing.catalogue_parts.walk import iter_item_refs, ref_key
    if not parents or not isinstance(entry, dict):
        return []
    eid = entry.get("entry_id")
    out: list[tuple[tuple, str]] = []
    for path, i in iter_item_refs(entry):
        if i not in parents or ref_key(path) == "outside_items":
            continue
        parts = ", ".join(parents[i])
        if path[0] == "matched":
            out.append((path, f"{eid} matched: {i} is cut into {parts} — match the part(s)"))
            continue
        out.append((path, f"{_where(entry, path)}: {i} is cut into {parts} — name the part(s)"))
    return out


def _where(entry: dict, path: tuple) -> str:
    """How a refusal names the list an extent sits in: `<entry> extents`, `<entry>/<rule id>`,
    `<entry>#<record id>`, with ` tributary_excludes` for a carve-out."""
    eid = entry.get("entry_id")
    if path[0] == "extents":
        return f"{eid} extents"
    rec = entry[path[0]][path[1]]
    out = f"{eid}/{rec.get('rule_id')}" if path[0] == "rules" else f"{eid}#{rec.get('id')}"
    return out + (" tributary_excludes" if path[2] == "tributary_excludes" else "")
