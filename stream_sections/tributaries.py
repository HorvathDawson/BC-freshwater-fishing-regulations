"""Tributary reachability over the single stream graph (03 S6 / 08).

Tributaries of a node = its ANCESTORS in the graph (walk incoming flow edges upstream).
``graph.ancestors`` gives the raw closure; this module adds the two REQUIRED guards from
spike 10 on top of it (directionality alone is not enough for a full walk, though the
inverted graph already avoids the confluence-parent leak by construction):

- WSC-descendant filter: keep only ancestors whose fwa_watershed_code is a descendant
  (prefix-extension) of the seed node's trimmed WSC — the drainage subtree.
- EDGE_TYPE=2300 barrier: do not traverse through connector/canal nodes (e.g. the
  Kootenay<->Columbia canal), so regs don't leak across systems.
- Lake barrier: once lakes are nodes (sectionizer), stop at regulated lakes.

Implemented against StreamGraph once the guards' node metadata (edge_type per node, lake
nodes) is populated by the sectionizer. Until then, ``graph.ancestors`` is the connectivity
closure used for validation.
"""

from __future__ import annotations

from .graph import ancestors  # noqa: F401  (re-export the raw closure)
from .models import StreamGraph


def tributary_node_ids(graph: StreamGraph, node_id: str,
                       wsc_filter: bool = True, block_2300: bool = True) -> tuple[str, ...]:
    """Guarded tributary closure of ``node_id``. TODO: apply WSC filter + 2300 barrier."""
    raise NotImplementedError("guarded tributary walk: sectionizer must populate node guards first")
