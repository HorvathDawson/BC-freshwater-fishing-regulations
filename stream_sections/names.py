"""Step 2 (03 S2): resolve (name, source) tuples per BLK.

Port graph_builder.propagate_names_by_watershed + annotate_unnamed_context, but EMIT TAGGED
NameTuples instead of overwriting a scalar. Order: override > gazette > side-channel >
upstream-inherited (NameSource priority). Uses the (WSC, BLK) index as the discriminator.
The Seabird channel gets (override) + (Fraser, side-channel).
"""

from __future__ import annotations

from .models import BlkChain, NameTuple


def resolve_names(chains: list[BlkChain], display_name_overrides: dict) -> list[BlkChain]:
    """Return chains with ``name_tuples`` populated (chains are frozen -> rebuilt copies)."""
    raise NotImplementedError
