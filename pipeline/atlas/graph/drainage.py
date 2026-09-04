"""How much country drains through a POINT — not through a section.

THE PROBLEM THIS EXISTS FOR. Every gauge weight in the conditions work is a ratio of
drainage areas, and a section carries ONE magnitude: the value at its downstream end. But
82.2% of tributary junctions land INSIDE a section rather than at either end, 361,756
sections have at least one, and among those the drainage DOUBLES OR MORE from top to bottom
in 90.8% of cases (median x2.00, p99 x54). So using the section's own number for a tap near
its top credits that spot with every creek joining below it — on the big rivers people
actually fish, because those are the sections with inner junctions.

Nothing new has to be fetched. The graph already knows where each tributary arrives
(`Edge.at_measure`) and how much it brings (`stream_magnitude`), so a section is not a
number, it is a STAIRCASE:

    m(t) = m_outlet - sum{ mag(u) : u joins at a measure below t }

TWO CONVERSIONS, AND THEY ARE NOT THE SAME ONE. This was the error a reviewer caught: the
plan used one exponent for both jobs.

    geomorphic   area = c * mag^b     magnitude -> area          (here)
    hydrologic   Q ratio = A ratio^f  area -> flow               (NOT here; the caller's)

`b` belongs to the landscape and `f` belongs to the water; the exponent on a magnitude
RATIO is their product. This module only ever does the first.

AND `c` IS NOT ONE NUMBER FOR THE PROVINCE. A wet coastal basin maps more headwater links
per square kilometre than a dry plateau, so a single constant is a bias that varies with
where you are. `AREA_HA` on the 11,580 named watersheds gives it directly, per basin, from
data already on disk:

    gauges     (2,037 points)   area = 1.488 * mag^0.868   R2 0.826
    watersheds (11,580 points)  area = 1.237 * mag^0.851   R2 0.859

Two unrelated datasets landing within 2% on the exponent is the best evidence available
that the relationship is real; the watersheds win on count, on directness (they measure
area, rather than inferring it through a gauge's own catchment) and on being splittable by
basin.
"""

from __future__ import annotations

import collections
import math
from dataclasses import dataclass

from pipeline.common.models import StreamGraph

#: Below this a fitted basin has too few watersheds to beat the province-wide constant.
MIN_BASIN_POINTS = 25

#: How many characters of an FWA watershed code name the basin. The code is hierarchical
#: and `-`-delimited: "100-000000-..." is the Fraser, "200-692231-..." the Liard. Three
#: characters is the major drainage; seven takes the first tributary level as well.
BASIN_PREFIX = 3


@dataclass(frozen=True)
class AreaModel:
    """`area_km2 = c * magnitude ** b`, with `c` per basin where there is enough to fit."""

    b: float
    c_default: float
    c_by_basin: dict[str, float]

    def area_km2(self, magnitude: int | None, wsc: str | None) -> float | None:
        """None when there is no magnitude — an absent count is not a small one."""
        if not magnitude or magnitude <= 0:
            return None
        c = self.c_by_basin.get(basin_of(wsc), self.c_default)
        return c * magnitude ** self.b

    def describe(self) -> str:
        return (f"area_km2 = c * mag^{self.b:.3f}, c={self.c_default:.3f} province-wide "
                f"with {len(self.c_by_basin)} basins fitted separately")


def basin_of(wsc: str | None) -> str:
    return (wsc or "")[:BASIN_PREFIX]


def fit_area_model(points: list[tuple[float, int, str]]) -> AreaModel:
    """Fit from `(area_km2, magnitude, wsc)` — the named watersheds.

    ORDINARY LEAST SQUARES IN LOG SPACE, and a reviewer's warning about it recorded here
    rather than silently accepted: OLS attenuates the slope when the predictor carries
    error, so a true exponent of 1.0 can read as 0.87 at this R-squared. The number is used
    only inside a RATIO between two points in the same basin, where a shared attenuation
    largely cancels; it must not be trusted as a statement about landscape scaling. If it
    is ever wanted for that, refit with reduced major axis.
    """
    xs = [math.log(m) for _, m, _ in points]
    ys = [math.log(a) for a, _, _ in points]
    n = len(xs)
    if n < 2:
        raise ValueError("need at least two points to fit an area model")
    mx, my = sum(xs) / n, sum(ys) / n
    denom = sum((x - mx) ** 2 for x in xs)
    b = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / denom if denom else 1.0
    c_default = math.exp(my - b * mx)

    by_basin: dict[str, list[tuple[float, int]]] = collections.defaultdict(list)
    for a, m, wsc in points:
        by_basin[basin_of(wsc)].append((a, m))
    # `b` is held FIXED across basins and only `c` is refitted. Letting both move gives
    # every basin its own curve shape from a few dozen points, which overfits the small
    # basins and makes two neighbouring catchments disagree about a ratio they share.
    c_by_basin = {}
    for key, pts in by_basin.items():
        if len(pts) < MIN_BASIN_POINTS or not key:
            continue
        c_by_basin[key] = math.exp(
            sum(math.log(a) - b * math.log(m) for a, m in pts) / len(pts))
    return AreaModel(b=b, c_default=c_default, c_by_basin=c_by_basin)


def primary_edges(graph: StreamGraph) -> set[int]:
    """Edge indices on a SPANNING TREE of the network — one outflow per node.

    A braid or a distributary sends the same water down two channels, and a magnitude
    summed over both counts those headwaters twice. Left alone that is measurable: 1.5% of
    sections end up with a staircase that runs below 1, which is arithmetically impossible
    and was the plan's one open defect.

    Keeping one outgoing edge per node makes the accumulation a tree, where every headwater
    is delivered exactly once. The one kept is the edge into the LARGEST downstream node,
    which is the mainstem — the secondary channel is the one that should not carry the
    count, and picking the smaller one would hand the whole river to a side channel.
    """
    out: dict[str, list[int]] = collections.defaultdict(list)
    for i, e in enumerate(graph.edges):
        out[e.from_node].append(i)
    keep: set[int] = set()
    for _node, idxs in out.items():
        if len(idxs) == 1:
            keep.add(idxs[0])
            continue
        best = max(idxs, key=lambda i: (
            getattr(graph.nodes.get(graph.edges[i].to_node), "stream_magnitude", 0) or 0,
            getattr(graph.nodes.get(graph.edges[i].to_node), "length_m", 0.0) or 0.0))
        keep.add(best)
    return keep


def staircase(graph: StreamGraph) -> dict[str, list[tuple[float, int]]]:
    """section id -> ascending `(measure, magnitude ARRIVING there)` for inner junctions.

    Only sections that actually have one get an entry; the rest are a constant and need no
    row. Measures are in the FWA's own along-line units, which run from the mouth upward —
    so a bigger measure is FURTHER UPSTREAM and the magnitude below any point is the
    section's own total minus everything joining above it.
    """
    keep = primary_edges(graph)
    ins: dict[str, list[tuple[float, int]]] = collections.defaultdict(list)
    for i, e in enumerate(graph.edges):
        if i not in keep:
            continue
        up = graph.nodes.get(e.from_node)
        if up is None or e.at_measure is None:
            continue
        ins[e.to_node].append((float(e.at_measure), int(up.stream_magnitude or 1)))

    out: dict[str, list[tuple[float, int]]] = {}
    for node_id, arrivals in ins.items():
        n = graph.nodes.get(node_id)
        if n is None:
            continue
        lo, hi = min(n.down_m, n.up_m), max(n.down_m, n.up_m)
        inner = sorted((m, g) for m, g in arrivals if lo + 1.0 < m < hi - 1.0)
        if inner:
            out[node_id] = inner
    return out


def magnitude_at(section_magnitude: int, steps: list[tuple[float, int]] | None,
                 measure: float) -> int:
    """Magnitude at `measure` along a section whose outlet carries `section_magnitude`.

    Everything joining BELOW the point is not yet upstream of it, so it comes off the total.
    Clamped at 1: a section always drains at least its own headwater, and where the braid
    tree has still double-counted, a floor is the honest degradation — it says "at least
    this much" rather than "a negative catchment".
    """
    if not steps:
        return max(1, section_magnitude)
    below = sum(g for m, g in steps if m < measure)
    return max(1, section_magnitude - below)
