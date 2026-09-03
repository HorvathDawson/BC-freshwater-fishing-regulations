"""What a registry item IS, and the one question everything downstream keeps getting wrong.

THE REGISTRY HOLDS TWO KINDS OF THING AND ONE FIELD MEANS TWO DIFFERENT RELATIONS.

    kind = stream | lake | wetland     `section_ids` is IDENTITY
                                       these sections ARE this water
    kind = area                        `section_ids` is CONTAINMENT
                                       these sections are INSIDE this polygon

A park is not made of rivers. But both are stored the same way, so any code asking "which
item owns this section" gets identity and containment answers in one list — and containment
outnumbers identity 783,492 to 61,879, twelve to one. Worse, `area:` sorts first
alphabetically, so a `setdefault` or a "first wins" loop picks the park EVERY time.

That has now been written wrong three times independently, in three modules, by three
authors who each had to remember the conflation:

    bundle `_gauges`        123 of 220 lake_gauge rows named a park; 542 of 2,018 gauges
    bundle `_place_water`   35,133 of 352,666 rows offered a park as "water near this town"
    tiles  `_identity`      every tile feature inside a park took the park's id and its
                            slug as a search name

None of the three errored. The bad ids simply pointed at rows `_items` had never inserted.

SO THE CHECK LIVES HERE, ONCE, and reads `kind` rather than the id string. `kind == "area"`
and `id.startswith("area:")` agree exactly today (1,473 of 1,473, measured) — but the id is
a naming scheme and the kind is the fact, and a scheme can be renamed.

THE REAL FIX IS UPSTREAM and is not this. `section_ids` should not carry two relations: an
area's members belong in their own field (`contains`), or areas belong in their own
collection. Until then, every reader has to remember — and this exists so that "remember"
means one import instead of one comment.
"""

from __future__ import annotations

#: Items that are a body of water a person could fish, search for, or stand beside.
WATER_KINDS = frozenset({"stream", "lake", "wetland"})

#: Administrative geography: parks, reserves, indigenous land, closure polygons. Real, and
#: already on the TILE as the `areas` attribute of every feature inside them.
AREA_KINDS = frozenset({"area"})


def is_water(item) -> bool:
    """Is this item a water, rather than administrative geography?

    Accepts a dict (registry.json) or an object with `.kind` (RegistryItem), because the
    registry is read both ways and a helper only used by half the callers is no helper.
    """
    kind = item.get("kind") if isinstance(item, dict) else getattr(item, "kind", None)
    kind = kind.value if hasattr(kind, "value") else kind
    if kind is not None:
        return str(kind) not in AREA_KINDS
    # No kind at all: fall back to the id scheme rather than guessing "water" and letting a
    # park through, which is the failure this module exists to prevent.
    iid = item.get("id") if isinstance(item, dict) else getattr(item, "id", "")
    return not str(iid).startswith("area:")


def waters(items):
    """Just the waters, preserving order."""
    return [i for i in items if is_water(i)]
