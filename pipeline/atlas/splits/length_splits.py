"""Cut sections that are too long to say one thing, at the junctions where they stop saying it.

WHY A CAP AT ALL. Everything the app says about a section — its flow percentile, its donor
panel, its colour, the stretch that lights up when you tap it — is said about the WHOLE
section. That is fine for the median section (740 m) and a lie for the tail: named sections
run to a p99 of 44 km and a maximum of 379 km, and 35.6% of all named channel sits in a
section longer than 25 km. One number, one colour, one panel, for a river that gains an
order of magnitude of drainage along its length.

WHY JUNCTIONS AND NOT KILOMETRES. A plain length cap cuts at an arbitrary metre mark, and
the two halves it produces have the SAME drainage area, the same donors and therefore the
same colour — more sections, no more truth. A tributary confluence is the place where the
river actually becomes a different river, so cutting there is what gives each piece its own
catchment, its own panel, and a colour it has earned. 82.2% of confluences currently land
mid-section, so the material is already in the graph; this stage only spends it where a
section is long enough to need it.

WHAT IT CANNOT REACH. Five sections over 25 km have no interior confluence at all, and four
of them are out-of-BC reaches of the Kootenay, Beaver and Tatshenshini — FWA carries no
tributaries outside the province, so there is no junction to cut at and nothing known to
change along them. Leaving those long is the honest outcome, not a gap.

RUNS AFTER EVERY OTHER SPLIT and BEFORE the area/MU membership passes. After, because a cut
should only be spent where the curated, border, area and gauge boundaries have not already
divided the water. Before, because `_split_one` copies the parent's attributes onto the new
piece — a section split after the MU pass would inherit its parent's MU list across a
boundary it actually crosses.
"""

from __future__ import annotations

from collections import defaultdict

from pipeline.common.models import AnchorType, NodeKind, SplitPoint, StreamGraph

# The longest a section may be before this stage tries to divide it.
#
# MEASURED, over the 1,232,825 sections of the 3-hop build. The cap is only worth the
# channel it reaches, and section COUNT badly understates that — the long sections are few
# and hold most of the water anyone looks at:
#
#     cap    sections over it   share of NAMED channel   cuts   sections added   named cuts
#      5 km     29,809 (2.4%)            78.3%          54,317      +4.4%           15%
#     10 km      8,028 (0.7%)            62.5%          14,937      +1.2%           33%
#     25 km      1,550 (0.1%)            35.6%           2,615      +0.2%           68%
#     50 km        423 (0.0%)            18.6%             619      +0.1%           88%
#
# 25 km is the shipped default. 10 km reaches nearly twice the channel for 1.2% more
# sections and is the better setting if the map still reads coarse; both are one flag
# (`--max-section-km`) and neither is expensive.
#
# The last column is the share of cuts that land at a NAMED tributary and so can be
# described to a reader. It falls with the cap because a tighter cap has to take whatever
# junction is in range — another reason not to go below 10 km without a reason.
DEFAULT_CAP_M = 25_000.0

# Never mint a stub. A junction a few metres from an existing boundary produces a section
# too short to tap, too short to label, and indistinguishable on the map from the boundary
# it sits beside.
MIN_PIECE_M = 250.0

# A cut at a NAMED tributary can be described ("downstream of Sloquet Creek"); a cut at an
# unnamed one cannot, and reads as an unexplained break in the river. So a named junction is
# preferred — but only once the piece is already at least this fraction of the cap, so that
# preferring a describable cut cannot shred a river into short pieces.
PREFER_NAMED_FROM = 0.5

# Two tributaries arriving within this distance are one junction as far as a cut is
# concerned — cutting twice would mint a section shorter than its own label.
_SAME_JUNCTION_M = 0.5

# How far a cut may be moved to land exactly on a stream-segment boundary.
#
# `_repartition` assigns a segment to a piece with a strict `<`, so a cut a few centimetres
# off a boundary leaves that segment overlapping BOTH pieces — and where it is off in the
# downstream direction, the piece ABOVE a confluence inherits the drainage from below it.
# Measured: 99.9% of these junctions already sit within 5 cm of a boundary (they are the
# same feature), 27 of 2,615 sit just below one. Two metres is enough to close those and far
# too small to move a cut onto a different junction.
_SNAP_M = 2.0


def _interior_junctions(graph: StreamGraph) -> dict[str, list[tuple[float, str]]]:
    """{section id: [(measure, tributary name), ...]} for confluences INSIDE a section.

    An inbound edge carries the measure at which its tributary meets this section's blue
    line. Strictly between the section's own bounds means the confluence is interior — the
    junction the section is currently hiding.
    """
    seen: dict[str, list[float]] = defaultdict(list)
    out: dict[str, list[tuple[float, str]]] = defaultdict(list)
    for e in graph.edges:
        n = graph.nodes.get(e.to_node)
        if n is None or n.kind != NodeKind.stream:
            continue
        if not (n.down_m < e.at_measure < n.up_m):
            continue
        # THE EXACT MEASURE, NOT A ROUNDED ONE. This rounded to a decimetre to dedupe
        # two tributaries meeting at one point, and that rounding is the whole hazard:
        # 99.9% of these measures ARE a stream-segment boundary in FWA (measured, median
        # offset 0.000 m), and `_repartition` decides which piece a segment belongs to with
        # a strict `<`. A cut nudged a few centimetres off leaves the segment overlapping
        # BOTH pieces, and if it is nudged the wrong way the piece above a confluence
        # inherits the drainage from below it. Dedupe on proximity instead, and cut where
        # the junction actually is.
        near = seen[e.to_node]
        if any(abs(m - e.at_measure) <= _SAME_JUNCTION_M for m in near):
            continue
        near.append(e.at_measure)
        trib = graph.nodes.get(e.from_node)
        out[e.to_node].append((e.at_measure, trib.display_name if trib else ""))
    return out


def _cuts_in(node, junctions: list[tuple[float, str]], cap_m: float,
             min_piece_m: float) -> list[tuple[float, str]]:
    """Where to cut ONE over-long section, walking from its low end.

    Greedy on the LAST junction still inside the cap: it takes the longest piece the cap
    allows, so it makes the fewest cuts that do the job. Taking the first junction past the
    anchor instead would satisfy the cap just as well while cutting far more often — the cap
    is a ceiling on ignorance, not a target length.

    A named junction in the upper half of the window wins over a nearer unnamed one, because
    a boundary that can be named is a boundary a reader can locate. Where no junction fits
    at all the first one past the cap is taken: overshooting is better than leaving the
    whole run undivided, and it is the only option where tributaries are sparse.
    """
    js = sorted(m for m, _ in junctions
                if node.down_m + min_piece_m < m < node.up_m - min_piece_m)
    if not js:
        return []
    name_at = {m: nm for m, nm in junctions}

    out: list[tuple[float, str]] = []
    anchor, i = node.down_m, 0
    while node.up_m - anchor > cap_m and i < len(js):
        window = [m for m in js[i:] if m - anchor <= cap_m]
        if window:
            named = [m for m in window
                     if name_at.get(m) and m - anchor >= cap_m * PREFER_NAMED_FROM]
            pick = named[-1] if named else window[-1]
        else:
            pick = js[i]                       # nothing fits; overshoot to the next junction
        if pick - anchor < min_piece_m:
            i += 1
            continue
        out.append((pick, name_at.get(pick, "")))
        anchor = pick
        i = js.index(pick) + 1
    return out


def _snap(m: float, node, fid_index: dict | None) -> float:
    """Move a cut onto the exact segment boundary it is already all but sitting on."""
    if not fid_index:
        return m
    best = m
    gap = _SNAP_M
    for f in node.member_fids:
        info = fid_index.get(f)
        if info is None:
            continue
        for edge in (info[0], info[1]):
            d = abs(edge - m)
            if d < gap and node.down_m < edge < node.up_m:
                best, gap = edge, d
    return best


def junction_cuts(graph: StreamGraph, cap_m: float = DEFAULT_CAP_M,
                  min_piece_m: float = MIN_PIECE_M,
                  fid_index: dict | None = None) -> list[SplitPoint]:
    """Every cut needed to bring sections under ``cap_m``, at interior confluences only.

    Resolved against the graph AS IT STANDS, so a river already divided by a lake, a border,
    a closure or a gauge is measured in the pieces those left behind and is cut only where
    they did not reach.

    `fid_index` (fid -> (down_m, up_m, order, magnitude)) snaps each cut onto the exact
    stream-segment boundary at the confluence, so the segments repartition cleanly and each
    new piece gets its own order and magnitude rather than sharing its neighbour's. Omit it
    and the cuts still land, within centimetres, on the same places.

    The ids are `length:{blk}:{measure}` — derived from the blue line and the confluence, so
    a rebuild on the same FWA vintage mints the same ones. They are NOT part of any curated
    binding: a registry item is keyed on the stream's identity (gnis / wsc / blk / wbk) and
    never on its sections, so subdividing a section adds sections to an item without
    changing that item's id or any entry that resolves through it.
    """
    inner = _interior_junctions(graph)
    out: list[SplitPoint] = []
    for nid, n in graph.nodes.items():
        if n.kind != NodeKind.stream or n.length_m <= cap_m:
            continue
        for m, name in _cuts_in(n, inner.get(nid, []), cap_m, min_piece_m):
            # Snapped LAST, after the choice of junction is made: the choice is about which
            # tributary to cut at, and this only removes the centimetre of slop between the
            # confluence's measure and the segment boundary that is the same place.
            m = _snap(m, n, fid_index)
            out.append(SplitPoint(
                split_id=f"length:{n.blk}:{int(m)}", blk=n.blk, route_measure=m, fid="",
                # The label becomes the section boundary's label and so appears in
                # `location_identifier` ("downstream of Sloquet Creek"). An unnamed
                # tributary gives "", which that property already treats as no bound —
                # the cut still happens, it just goes undescribed rather than described
                # badly.
                label=name, anchor_type=AnchorType.confluence))
    return out
