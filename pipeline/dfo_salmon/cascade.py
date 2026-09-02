"""cascade — Region 6's lettered sections as a tree of SPATIAL scopes.

The sections are not labels. Each one is a real extent, and the letters describe a
containment hierarchy that decides which rule wins:

    A   All Region 6 waters                     the default, for anything not below
    ├── B   Skeena River Watershed              "Section A applies if … not listed below"
    │   ├── B(i)   the watershed ABOVE the CNR Railway Bridge at Terrace
    │   └── B(ii)  the watershed BELOW it
    ├── C   Nass River Watershed
    ├── D   Queen Charlotte Islands (Haida Gwaii) watersheds
    ├── F   Fraser River Watershed within Region 6      — closed outright
    └── E   Other Mainland Watersheds, EXCEPT the Fraser
        ├── streams draining to tidal Areas 3, 4, 5 and 6
        ├── streams draining to tidal Area 5
        └── streams draining to tidal Area 6

Two things a flat `inherits` list gets wrong, and this module fixes:

1. **B(i) and B(ii) are the watershed above and below one point.** They are the same
   primitive as a named water's reach — `upstream_of(anchor)` over an item whose
   tributaries are included — just applied to the whole Skeena. Nothing new is needed
   to bind them; they are the largest instance of the case the model already handles.
2. **E is the `else` branch**, not a shape to author. "Other Mainland Watersheds,
   except for the Fraser" reads like a set difference, but nothing has to compute one:
   a water *listed* in the table already declares its section, and an *unlisted* water
   is placed by testing its siblings in order — in the Skeena? in the Nass? Haida
   Gwaii? the Fraser? — and falling through to E. Those siblings must be bound anyway,
   so E costs nothing extra. Rendering "everywhere section E applies" is the same
   fallthrough evaluated over every section, so it is derived, never authored.

Resolution walks the tree upward: the narrowest scope holding a matching rule wins,
then its parent, up to A.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional

from pipeline.dfo_salmon.untangle import Untangled

#: How a scope's extent is defined. The curator binds each to real geometry; the kind
#: says WHICH operation, so the binding is a small decision rather than a blank page.
KINDS = (
    "region",           # the Region 6 polygon
    "watershed",        # every water draining to one named river
    "watershed_above",  # …restricted to above an anchor  (upstream_of + tributaries)
    "watershed_below",  # …restricted to below an anchor
    "island_group",     # a named archipelago's watersheds
    "residual",         # the else-branch: whatever its siblings do not claim
    "tidal_areas",      # streams whose OUTLET falls in the given PFMA areas
)

_RE_UP = re.compile(r"\b(?:waters\s+)?upstream of\s+(?P<a>[^.,;]+)", re.I)
_RE_DOWN = re.compile(r"\b(?:waters\s+)?downstream of\s+(?P<a>[^.,;]+)", re.I)
_RE_WATERSHED = re.compile(r"(?P<name>[A-Z][\w'’]*(?:\s+[A-Z][\w'’]*)*)\s+Watershed", re.I)
_RE_AREAS = re.compile(r"\bareas?\s+(\d{1,2}(?:(?:\s*[,&]\s*|\s+and\s+)+\d{1,2})*)", re.I)


@dataclass
class Scope:
    """One node of the cascade. `binding` is filled in by a curator, like a location."""

    scope_id: str
    parent: Optional[str]
    kind: str
    label: str
    #: watershed kinds — the river whose drainage this is.
    of: Optional[str] = None
    #: watershed_above / watershed_below — the cut point, verbatim.
    anchor: Optional[str] = None
    #: tidal_areas — PFMA area numbers. Sourced from the same layer the tidal boundary
    #: is built from (`dfo_bc_pfma_subareas_chs_v3`); a stream belongs to an area when
    #: its outlet at the tidal boundary falls inside that area's polygon.
    areas: List[int] = field(default_factory=list)
    #: residual — the siblings tested BEFORE falling through to this scope. Ordering
    #: information, not geometry to subtract: binding these is what places an unlisted
    #: water, and this scope needs no binding of its own.
    minus: List[str] = field(default_factory=list)
    #: True when this scope publishes no rules of its own (a pure container, like B).
    container_only: bool = False

    def to_dict(self) -> dict:
        return asdict(self)


def _letter_parent(key: str) -> Optional[str]:
    """`B(i)` -> `B`; a bare letter -> `A`; `A` -> None."""
    if key == "A":
        return None
    base = key.split("(")[0]
    return base if base != key else "A"


def _classify(key: str, label: str) -> dict:
    """Infer the spatial kind of one banner from its own words."""
    if key == "A":
        return {"kind": "region"}

    areas = []
    m = _RE_AREAS.search(label)
    if m:
        areas = [int(n) for n in re.findall(r"\d{1,2}", m.group(1))]
    if areas:
        return {"kind": "tidal_areas", "areas": areas}

    if re.search(r"\bother\b.*\bexcept\b", label, re.I):
        return {"kind": "residual"}

    if re.search(r"queen charlotte|haida gwaii", label, re.I):
        return {"kind": "island_group"}

    of = None
    ws = _RE_WATERSHED.search(label)
    if ws:
        of = ws.group("name").strip()
        # "B. Part (i): Skeena River Watershed-Waters upstream of …" names the parent
        # river; strip the leading section word if the regex over-reached.
        of = re.sub(r"^(?:Part|Section)\s+", "", of).strip()

    up, down = _RE_UP.search(label), _RE_DOWN.search(label)
    if up:
        return {"kind": "watershed_above", "of": of, "anchor": up.group("a").strip(" .")}
    if down:
        return {"kind": "watershed_below", "of": of, "anchor": down.group("a").strip(" .")}
    return {"kind": "watershed", "of": of}


def build_scopes(u: Untangled) -> List[Scope]:
    """Derive the scope tree for one region. Empty for regions with no sections."""
    scopes: List[Scope] = []
    by_key = {s.key: s for s in u.sections}
    if not by_key:
        return scopes

    with_rules = {d.key for d in u.defaults if d.rules} | {
        w.section for w in u.waters if w.section}

    for sec in u.sections:
        info = _classify(sec.key, sec.title)
        scopes.append(Scope(
            scope_id=sec.key,
            parent=_letter_parent(sec.key),
            label=sec.title,
            container_only=sec.key not in with_rules,
            **info,
        ))

    # Area sub-scopes hang off the section that published them, not off A.
    for d in u.defaults:
        if d.kind != "area":
            continue
        sid = f"{d.key}:areas-" + "-".join(str(a) for a in d.areas)
        scopes.append(Scope(
            scope_id=sid, parent=d.key, kind="tidal_areas",
            label=d.scope, areas=list(d.areas),
        ))

    # A residual scope subtracts its siblings — everything else under the same parent.
    for s in scopes:
        if s.kind == "residual":
            s.minus = sorted(x.scope_id for x in scopes
                             if x.parent == s.parent and x.scope_id != s.scope_id)
    return scopes


def resolution_chain(scopes: List[Scope], scope_id: Optional[str]) -> List[str]:
    """Narrowest first, widest last: `E:areas-5` -> `[E:areas-5, E, A]`.

    This is the order a lookup tries. A named water sits below the front of the chain:
    its own reach is consulted before any scope in it.
    """
    if not scope_id:
        return []
    index = {s.scope_id: s for s in scopes}
    chain, seen = [], set()
    cur: Optional[str] = scope_id
    while cur and cur in index and cur not in seen:
        seen.add(cur)
        chain.append(cur)
        cur = index[cur].parent
    if cur and cur not in seen:      # a parent that has no banner of its own
        chain.append(cur)
    return chain


def needs_binding(scopes: List[Scope]) -> List[Scope]:
    """Scopes a curator must actually author geometry for.

    Excludes:

    * ``container_only`` scopes with no rules of their own — but NOT the containers a
      residual falls through (B, C, D, F place unlisted waters, so they are bound);
    * ``residual`` scopes, which are the else-branch of their siblings (see above).
    """
    return [s for s in scopes if s.kind != "residual"]


def needs_new_machinery(scopes: List[Scope]) -> List[Scope]:
    """Scopes that cannot be bound with what the repo already has.

    Only `tidal_areas`: "streams flowing into tidal waters of Area N" is about a
    stream's OUTLET, which needs the DFO Pacific Fishery Management Area polygons.
    Everything else is a named water plus at most two cut points.
    """
    return [s for s in scopes if s.kind == "tidal_areas"]
