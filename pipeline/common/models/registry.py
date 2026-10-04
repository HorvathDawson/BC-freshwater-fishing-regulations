"""Registry datatypes (parser truth): a named waterbody item + its bindable boundaries."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RegistryBoundary:
    """One bindable cut-point on a registry item — what a rule's Extent selects against. Derived
    from the graph nodes' section bounds (curated splits + auto lake/outlet/headwaters)."""
    id: str                      # READABLE token a rule's Extent.splits references (split id | "tenas_lake")
    label: str                   # human-readable ("Goat Creek → Atnarko River", "Tenas Lake")
    kind: str                    # BoundaryKind value: split | confluence | lake | outlet | headwaters | mu | border
    ref: str = ""                # the graph boundary_id (stable): "split:{id}" | "lake:{wbk}" | "outlet" | "headwaters"
    wbk: str = ""                # robust key for lake boundaries
    aliases: tuple[str, ...] = ()  # other ids this same cut-point answers to (see SectionBoundary)


@dataclass(frozen=True)
class RegistryItem:
    """A regulated waterbody the matcher points an Entry at. Identity + names come straight from the
    graph node grouping (which already merged blk-chains + applied name_variants); `boundaries` is the
    parser's catalog of bindable cut-points on this item."""
    id: str                              # gnis:{} -> wsc:{} -> blk:{} (streams); wbk:{} (lakes/wetlands); area:{} (areas)
    name: str                            # display_name from the graph (or GNIS name for wetlands)
    kind: str                            # stream | lake | wetland | area
    variants: tuple[str, ...] = ()       # searchable name variants (name_tuples)
    mus: tuple[str, ...] = ()            # management units this item spans (07 overlay; may be empty pre-overlay)
    section_ids: tuple[str, ...] = ()    # its section node_ids
    boundaries: tuple[RegistryBoundary, ...] = ()
    ref_ids: tuple[str, ...] = ()        # EVERY FWA id this item answers to (gnis:/wbk:/wsc:/blk: of its
                                         # member nodes) — the matcher's id_index bridge so a curated
                                         # override id (incl. a lake's gnis) resolves to this item
    part_of: str = ""                    # the item this one is a curated PART of (a lake cut into parts
                                         # in `added_lakes.geojson`: Kootenay Lake's Main Body -> Kootenay
                                         # Lake) — written by the atlas build (`registry.add_lake_parts`),
                                         # so no reader opens the curated polygons to learn it
