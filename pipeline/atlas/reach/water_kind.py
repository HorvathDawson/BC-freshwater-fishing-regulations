"""WHAT KIND OF WATER A SECTION IS, for `Extent.feature_types` — the ONE place it is decided.

A section's kind is the atlas graph's — `stream`, `lake`, `wetland` — EXCEPT that a section of a
LAKE-TYPED water whose name says it flows (`flows`: the Vedder Canal, Gravel Slough, Maria Slough,
the lake item named "Alouette River") IS A STREAM (`kind_of`; user ruling
2026-10-03: a slough is a stream everywhere — every zone and provincial "in streams" rule, spring
and winter stream closures, "from streams" quotas, stream bait bans, the steelhead rules). The
reach builder reads `kind_of` for every `feature_types` filter (`extent._kind_of`), and a rule's
`water` is enforced through those (`bundle.rules`: a rule with `water` places only by
`feature_types: [water]`), so every rule takes it up with no per-rule change. The drawn geometry
stays a polygon (the bundle's `item.kind`); the export names the regulatory kind (`flows`).

`flows` is also the known-waters generator's "no lakes" test (`pipeline.regs.steelhead.
known_waters`).
"""
from __future__ import annotations

import re

#: A name whose HEAD NOUN (its last word, before any "at …"/"near …" phrase) is one of these flows:
#: "Vedder Canal", "Gravel Slough", "Sumas Lake Canal", "Rancheria River" — but not "Bear Creek
#: Reservoir", "Corn Creek Marsh", "River Lakes", "OKANAGAN RIVER OXBOWS" or "Pete's Pond Unnamed
#: Lake At The Head Of San Juan River" (2026-10-03: the old any-word test made all of those flow).
FLOWING = re.compile(r"\b(slough|canal|channel|river|creek)$", re.I)
_LOCATIVE = re.compile(r"\s+(at|near)\b.*$", re.I)


def flows(kind, name) -> bool:
    """Flowing water: a stream, or a water of another kind whose name's head noun says it flows."""
    if str(getattr(kind, "value", kind) or "").lower() == "stream":
        return True
    head = _LOCATIVE.sub("", str(name or "").strip())
    return bool(FLOWING.search(head))


def _kind(x) -> str:
    k = getattr(x, "kind", "")
    return str(getattr(k, "value", k) or "").lower()


_FLOWING_LAKES: dict[int, tuple[object, frozenset]] = {}


def flowing_lake_sections(registry) -> frozenset:
    """Sections of the non-stream waters (`registry` items, areas excluded) whose name flows."""
    hit = _FLOWING_LAKES.get(id(registry))
    if hit is not None and hit[0] is registry:
        return hit[1]
    items = registry.items() if hasattr(registry, "items") else ()
    got = frozenset(s for k, it in items if not str(k).startswith("area:")
                    and _kind(it) != "stream" and flows(_kind(it), getattr(it, "name", ""))
                    for s in (getattr(it, "section_ids", None) or ()))
    _FLOWING_LAKES[id(registry)] = (registry, got)
    return got


def kind_of(graph, registry, section_id: str) -> str:
    """THE WATER KIND OF A SECTION for every regulation: the graph's kind (`stream`, `lake`,
    `wetland`), but `stream` for a section of a lake-typed water that flows (`flows`). '' when the
    graph has no such section."""
    n = graph.nodes.get(section_id)
    if n is None:
        return ""
    k = getattr(n, "kind", None)
    k = str(getattr(k, "value", k) or "").lower()
    if k != "stream" and registry is not None and section_id in flowing_lake_sections(registry):
        return "stream"
    return k
