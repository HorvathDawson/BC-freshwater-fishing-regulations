"""Split anchor resolution (04): any AnchorType -> (blk, route_measure).

Each resolver normalizes to an absolute DOWNSTREAM_ROUTE_MEASURE on a BLK:
- lake            -> lake outlet/inlet fid measure (reuse atlas lake-outlet machinery)
- confluence      -> mainstem fid measure at the tributary junction node
- linear_feature_id -> that fid's route measure (exact; used today for Wigwam/Shuswap)
- landmark/point/border/mu_boundary -> project coordinate/boundary crossing onto the blue line
"""

from __future__ import annotations

from .models import BlkChain, SplitAnchor, SplitPoint


def resolve_anchor(anchor: SplitAnchor, chain: BlkChain, context: dict) -> SplitPoint:
    """Resolve one anchor to a SplitPoint. ``context`` carries lakes/MU polygons/graph as needed."""
    raise NotImplementedError
