"""WHAT KIND OF WATER A SECTION IS, for `Extent.feature_types` — read from the ONE place it is
decided, the registry.

A section's water kind is ITS OWNER'S `item.kind` — `stream`, `lake`, `wetland` — and the graph
node's kind only for a section no named water owns. The registry decides the kind once
(`pipeline.atlas.registry.flowing`, over `pipeline.common.water_kind.flows`): a lake-typed water
whose name says it flows (the Vedder Canal, Gravel Slough, Nicomen Slough's polygons, the Stellako's
wide reach) IS A STREAM, folded into its river's item or made a stream item of its own, so that
user ruling of 2026-10-03 — a slough is a stream everywhere: every zone and provincial "in streams"
rule, spring and winter stream closures, "from streams" quotas, stream bait bans, the steelhead
rules — holds with NO per-rule change and NO second computation. The reach builder reads `kind_of`
for every `feature_types` filter (`extent._kind_of`), the tributary walk and the confluence step
(`graph.tributaries.expand`, `reach.build._expander`), and a rule's `water` is enforced through
those (`bundle.rules`: a rule with `water` places only by `feature_types: [water]`).

The SHAPE a section is drawn as (a line, a polygon) is the graph node's kind and is a per-section
fact: the bundle writes it (`section_span.shape`), the tiles draw it. It never decides a rule.
"""
from __future__ import annotations

_OWNER_KINDS: dict[int, tuple[object, object, dict]] = {}


def _kind(x) -> str:
    k = getattr(x, "kind", "")
    return str(getattr(k, "value", k) or "").lower()


def owner_kinds(graph, registry) -> dict[str, str]:
    """{section: its owner's kind} for every section whose WATER kind differs from its graph
    node's — a river's polygon, a slough's — which is all a lookup needs: where the two agree,
    the node's kind is the owner's. Cached per (graph, registry) pair, by identity."""
    key = id(registry)
    hit = _OWNER_KINDS.get(key)
    if hit is not None and hit[0] is registry and hit[1] is graph:
        return hit[2]
    got: dict[str, str] = {}
    items = registry.items() if hasattr(registry, "items") else ()
    nodes = getattr(graph, "nodes", {}) or {}
    for iid, it in items:
        k = _kind(it)
        if k not in ("stream", "lake", "wetland"):
            continue
        for s in getattr(it, "section_ids", None) or ():
            n = nodes.get(s)
            if n is not None and _kind(n) != k:
                got[s] = k
    _OWNER_KINDS[key] = (registry, graph, got)
    return got


def kind_of(graph, registry, section_id: str) -> str:
    """THE WATER KIND OF A SECTION for every regulation: the kind of the registry item that owns
    it (`stream`, `lake`, `wetland`), else the graph node's. '' when the graph has no such
    section."""
    n = graph.nodes.get(section_id)
    if n is None:
        return ""
    if registry is not None:
        got = owner_kinds(graph, registry).get(section_id)
        if got:
            return got
    return _kind(n)
