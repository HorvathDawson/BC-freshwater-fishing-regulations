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
    names: tuple[str, ...] = ()    # the other NAMES at this cut-point (`SectionBoundary.names`): shown, never bound

    def answers_to(self) -> tuple[str, ...]:
        """EVERY ID THIS CUT-POINT ANSWERS TO, as a rule binds it: its own `id`, then each alias with
        the graph's `split:` ref prefix stripped (a rule's `Extent.splits` names bare ids), in order.
        The one reading (T125 item 3): the parse menu (`parse_context`), the resolver (`extent`),
        and the bundle's place names all ask it."""
        return (self.id, *(bare_split_id(a) for a in (self.aliases or ())))


def bare_split_id(alias: str) -> str:
    """A cut-point alias as a rule names it: the graph's `split:` ref prefix stripped (`split:goat_creek` ->
    `goat_creek`), anything else as it is. The ONE strip (T125 item 3, adversarial review): `answers_to` and the
    bundle's span tokens (`spans._authored`, which reads a section's graph aliases as well as a registry boundary's)
    ask it."""
    return alias[len("split:"):] if alias.startswith("split:") else alias


@dataclass(frozen=True)
class RegistryItem:
    """A regulated waterbody the matcher points an Entry at. Identity + names come straight from the
    graph node grouping (which already merged blk-chains + applied name_variants); `boundaries` is the
    parser's catalog of bindable cut-points on this item."""
    id: str                              # gnis:{} -> wsc:{} -> blk:{} (streams); wbk:{} (lakes/wetlands); area:{} (areas)
    name: str                            # display_name from the graph (or GNIS name for wetlands)
    kind: str                            # THE WATER KIND: stream | lake | wetland | area. Decided ONCE,
                                         # here, by the registry build (`common.water_kind.flows` +
                                         # `registry.flowing`): a slough, canal or river drawn as a
                                         # polygon is `stream`. The shape a SECTION is drawn as is the
                                         # graph node's kind, never this.
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
                                         # so no reader opens the curated polygons to learn it. A lake
                                         # with parts OWNS NO SECTION of its own (user ruling 2026-10-03):
                                         # its parts are the water, and it is found through them.
    aliases: tuple[str, ...] = ()        # item ids this item ABSORBED (`registry.flowing`: a river's
                                         # polygons folded into its line's item, a slough's polygons
                                         # into one item). The permanent record of what an old id names;
                                         # `flowing.canonical` maps one to this item at the corpus'
                                         # read points, and everything else refuses it.
