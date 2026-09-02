"""Stocking: which waters get fish, and when.

The same shape as gauges, and for the same reason. FIDQ publishes a stable `waterbody_id`,
so that id addresses the feed directly and the bundle holds only the LINK from it to a
registry item. A weekly job fetches releases by id and knows nothing else — no atlas, no
matching, no geometry.

See `pipeline/docs/15-live-data-flow.md`.
"""

from pipeline.stocking.identifiers import build_identifier_index
from pipeline.stocking.match import StockMatch, match_waterbodies

__all__ = ["StockMatch", "build_identifier_index", "match_waterbodies"]
