"""Resolve ONE authored extent onto the section ids it covers.

The primitive the rest of `pipeline.atlas.reach` is built on: `extent` answers "what does this one
extent select", `tributaries` extends a reach upstream, `classify` turns that into an
outcome, and `build` runs the whole corpus. It lives here rather than under `registry`
because it is reach logic, not registry storage — the registry only supplies the
item -> sections map it reads.

Lifted out of `curation-review/backend/reuse.py` so the review app and the artifact builder run ONE
implementation. If the builder had its own, the bundle and the app would disagree — and the app is
where a human signed off, so the divergence would be invisible until a user hit a wrong reach.

Two things to preserve:

**The resolver never creates a section.** `_by_measure` only filters pre-existing graph nodes by route
measure; cutting happens upstream in the sectionizer from `pipeline/atlas/splits.json`. If resolution could
split, editing a regulation would silently change section geometry.

**Reaches are found by ROUTE MEASURE on the cut's own blue line, never by a flow walk.** A `between`
whose two cuts sit on different blue lines is the intersection of two half-lines instead
(`_between_across_lines`) — that is how a reach spanning a name change resolves.

The graph and registry are passed in rather than read from module state, so a caller can resolve
against any build. `curation-review/backend/reuse.py` supplies its cached pair; the builder supplies
its own.
"""

from __future__ import annotations

from pipeline.atlas.graph.windows import polygon_window
from pipeline.atlas.registry import regions as _regions
from pipeline.atlas.registry.basins import basin_code, basin_members
from pipeline.common.fwa_edge_types import is_main_flow
from pipeline.common.registry_kinds import area_key
from pipeline.common.utils.wsc import trim_wsc   # noqa: F401  (used by the moved body)
from pipeline.common.utils import wsc as _wsc
from pipeline.common import tolerances as _tol

#: Basins answered from the graph, per graph object — a sub-basin is a full scan of the graph. The
#: graph is held beside its answer and checked by identity, so a recycled `id()` never serves
#: another graph's basin.
_BASIN_CACHE: dict[tuple[int, str], tuple[object, frozenset]] = {}


def area_sections(reg, g, key: str) -> set[str] | None:
    """The sections of an `area:` id: the registry's item, or — for a watershed the registry did
    not mint (`area:basin:100-342455-`, the Chilcotin) — every graph node whose FWA code lies in
    that basin (`registry.basins`). None = no such area (the caller fails, naming it)."""
    if key in reg:
        if key.startswith(_regions.REGION_PREFIX):
            # A WATER TAKES THE ZONE RULES OF THE REGION IT LIES IN: a straddling stream piece is
            # its home region's only, a lake is in both (`registry.regions`, user ruling 2026-09-25
            # — Mara Lake). The ONE region membership (T115): `regions.region_sections`.
            return _regions.region_members(reg, g, key)
        return set(reg[key].section_ids)
    code = basin_code(key)
    if code is None or g is None or not hasattr(g, "nodes"):
        return None
    ck = (id(g), code)
    hit = _BASIN_CACHE.get(ck)
    if hit is None or hit[0] is not g:
        hit = _BASIN_CACHE[ck] = (g, frozenset(basin_members(g.nodes.values(), code)))
    return set(hit[1]) if hit[1] else None


#: A tributary whose mouth lies within this many metres of a watershed cut sits AT the cut, and is on
#: neither side (`Extent.watershed`). A curated confluence split lands on the confluence itself; this
#: only absorbs the rounding of FWA's millionths (1.4 m a unit on the Fraser).
WATERSHED_AT_CUT_M = _tol.WATERSHED_AT_CUT_M     # 5 m, pipeline/common/tolerances.py

#: POLICY (user ruling 2026-09-29, SP-9): a water coded to the RIVER ITSELF that touches no other
#: water (a floodplain lake) goes to the side of the cut the river piece nearest to it is on
#: (`position.nearest_pieces`) — where it lies, as its code would have said had FWA given it one.
CODE_LAKES_PLACED_BY_POSITION = True

#: {(id(graph), river code): (graph, ((node id, first group or None), ...))} — a basin's members
#: with the position each joins the river at. The graph is held and checked by identity (a recycled
#: id() never serves another graph).
_WS_MEMBERS: dict[tuple[int, str], tuple[object, tuple]] = {}
#: {(id(graph), blk): (graph, length)} — a blue line's FWA length, for code positions.
_LINE_LEN: dict[tuple[int, str], tuple[object, float]] = {}


def _first_group(code: str, river: str) -> int | None:
    """The position (in millionths of `river`'s length, from its mouth) at which the water coded
    `code` joins `river` — the group after the river's code. None when `code` IS the river's."""
    head = _wsc.group_after(code, river)
    return int(head) if head is not None and head.isdigit() else None


def _basin_members(g, river: str) -> tuple:
    """Every node of `river`'s basin (its code or `basin_wsc` at or below `river`), with the group
    it joins the river at (`_first_group`)."""
    from pipeline.atlas.registry.basins import node_basin_code
    ck = (id(g), river)
    hit = _WS_MEMBERS.get(ck)
    if hit is None or hit[0] is not g:
        out = []
        for nid in sorted(g.nodes):
            c = node_basin_code(g.nodes[nid])
            if _wsc.is_at_or_below(c, river):
                out.append((nid, _first_group(c, river)))
        hit = _WS_MEMBERS[ck] = (g, tuple(out))
    return hit[1]


def _line_length(g, blk: str, river: str) -> float:
    """The FWA length of blue line `blk`, which carries `river`'s code — the denominator of every
    group on it. Read from the tributaries that join the line directly (group = measure / length ×
    1e6, rounded down; on the Fraser the median of 3,362 agrees with the line's own length to 1 in
    10^6), because a line whose head lies inside a lake has no graph node at its top and its
    largest `up_m` falls short. With fewer than five direct confluences, the largest `up_m`."""
    ck = (id(g), blk)
    hit = _LINE_LEN.get(ck)
    if hit is not None and hit[0] is g:
        return hit[1]
    from pipeline.atlas.registry.basins import node_basin_code
    ups, ratios = [], []
    for nid, n in g.nodes.items():
        if n.blk != blk:
            continue
        ups.append(n.up_m)
        for ei in g.up_adj.get(nid, []):
            e = g.edges[ei]
            s = g.nodes.get(e.from_node)
            grp = _first_group(node_basin_code(s), river) if s is not None else None
            if grp and grp >= 1000 and e.at_measure > 0 and \
                    _wsc.is_strictly_below(node_basin_code(s), river):
                ratios.append(e.at_measure * 1e6 / (grp + 0.5))
    if len(ratios) >= 5:
        ratios.sort()
        length = ratios[len(ratios) // 2]
    else:
        length = max(ups) if ups else 0.0
    _LINE_LEN[ck] = (g, length)
    return length


def _watershed_part(g, universe: set[str], river_in: set[str], river_amb: set[str],
                    cuts: list[tuple[str, float]], op: str, reg=None) -> dict | str:
    """`Extent.watershed`: the river's basin on the op's side of the cut(s), by code position.

    `universe` is the scoped river's own sections; `river_in` / `river_amb` what the measure cut
    already gave for them (inside / straddling). Returns ``{"sections", "unplaced", "at_cut"}`` or a
    failure code.

    A member whose code joins the river at group `p` is placed by `p` against each cut's position
    `P` (= cut measure / line length × 1e6): above, below, or AT the cut (its mouth within
    `WATERSHED_AT_CUT_M`), which is neither side. A member coded to the river itself (`p` is None) is
    the river: a piece of the scoped item goes where the measure cut put it; any other (a floodplain
    lake, an unnamed side channel, a pond FWA placed only in the river's own named watershed) goes
    where the water it touches goes — when all of it agrees — and is otherwise UNPLACED: reported,
    never guessed onto a side."""
    from pipeline.atlas.registry.basins import node_basin_code
    # THE WATER KIND (T121, `_kind_of` -> `water_kind.kind_of`): the river's own pieces, a river
    # polygon or slough the registry calls the river included — never the node's shape
    codes = {node_basin_code(g.nodes[s]) for s in universe
             if s in g.nodes and _kind_of(g, s, reg) == "stream"}
    codes.discard("")
    if len(codes) != 1:
        return "watershed_river_code_ambiguous"
    river = codes.pop()
    pos: list[tuple[float, float]] = []                      # (P, tolerance in units)
    for blk, m in cuts:
        on = [s for s in universe if s in g.nodes and g.nodes[s].blk == blk]
        if not on or node_basin_code(g.nodes[on[0]]) != river:
            return "watershed_cut_off_the_river"
        length = _line_length(g, blk, river)
        if length <= 0:
            return "watershed_line_has_no_length"
        pos.append((m / length * 1e6, WATERSHED_AT_CUT_M * 1e6 / length))

    def side(p: int) -> str:
        """"in" / "out" / "at" for a tributary group at position `p`."""
        rel = []
        for P, tol in pos:
            d = (p + 0.5) - P
            if abs(d) <= 0.5 + tol:
                return "at"
            rel.append("above" if d > 0 else "below")
        if op == "upstream_of":
            return "in" if rel[0] == "above" else "out"
        if op == "downstream_of":
            return "in" if rel[0] == "below" else "out"
        return "in" if sorted(rel) == ["above", "below"] else "out"     # between the two cuts

    members = _basin_members(g, river)
    verdict: dict[str, str] = {}
    riverish: list[str] = []
    at_cut: set[int] = set()
    for nid, p in members:
        if p is None:
            if nid in universe and nid not in river_amb:
                verdict[nid] = "in" if nid in river_in else "out"
            else:
                # Off the item, or a piece of it the measure cut could not place (a braid that
                # touches no other piece of the river — the Fraser's side channels behind
                # Nicomen and Maria sloughs): placed with its group, by what the group touches.
                riverish.append(nid)
            continue
        s = side(p)
        if s == "at":
            at_cut.add(p)
        verdict[nid] = s
    # THE RIVER'S OWN CODE OFF THE ITEM: placed by what it touches, as one connected group.
    pending = set(riverish)
    seen: set[str] = set()
    for start in sorted(riverish):
        if start in seen:
            continue
        comp, stack, votes = set(), [start], set()
        seen.add(start)
        while stack:
            nid = stack.pop()
            comp.add(nid)
            nb = {g.edges[i].to_node for i in g.down_adj.get(nid, [])} | \
                 {g.edges[i].from_node for i in g.up_adj.get(nid, [])}
            for x in nb:
                if x in pending:
                    if x not in seen:
                        seen.add(x)
                        stack.append(x)
                elif x in verdict:
                    votes.add(verdict[x])
        v = votes.pop() if len(votes) == 1 and votes <= {"in", "out"} else "?"
        for nid in comp:
            verdict[nid] = v
    # THE RIVER'S OWN CODE, TOUCHING NOTHING: a floodplain lake no stream enters or leaves (151 of
    # the Fraser's in Region 5). Placed where it lies — with the river piece nearest its outline
    # (`position.nearest_pieces`), when that piece's side is known (`CODE_LAKES_PLACED_BY_POSITION`).
    if CODE_LAKES_PLACED_BY_POSITION:
        from pipeline.atlas.reach import position as _position
        alone = [n for n, v in verdict.items() if v == "?" and n in g.nodes
                 and not g.up_adj.get(n) and not g.down_adj.get(n)
                 and _kind_of(g, n, reg) != "stream"]
        placed = {s for s in universe if verdict.get(s) in ("in", "out")
                  and node_basin_code(g.nodes[s]) == river}
        for n, piece in _position.nearest_pieces(g, alone, placed).items():
            verdict[n] = verdict[piece]
    return {"sections": {n for n, v in verdict.items() if v == "in"},
            "unplaced": sorted(n for n, v in verdict.items() if v == "?"),
            "at_cut": sorted(f"{river}-{p:06d}" for p in at_cut),
            "river_code": river}


def _cut_at(g, refs: set[str], universe: set[str]):
    """(blk, measure, alternatives) of the cut, from the node bounds that carry it, or None if it is
    not in `universe`. `alternatives` are the OTHER measures the same cut lands at on that blue line.

    Two ways one split id maps to more than one place, and they need opposite treatment:

    * SEVERAL BLUE LINES — the mainstem plus every side channel the perpendicular sweep cut at the
      same place. Not ambiguous: they are the same cut across a braid. Take the PRINCIPAL channel (the
      line carrying the most length in this item, tie-broken on blk id) and let `_by_measure` place the
      side channels relative to it, which is what it is for.
    * SEVERAL MEASURES ON ONE LINE — genuinely ambiguous. An area boundary the stream crosses twice
      (Pinnacles Park's boundary crosses Baker Creek at 7,094 m and 8,067 m, entering and leaving)
      mints ONE split id for BOTH crossings, so "upstream of it" has two honest readings.

    Both cases used to be resolved by set-iteration order, so the same rule could return a different
    reach on two page loads. Now the order is fixed — the measure nearest the mouth first — and the
    alternatives are handed back: `resolve_extent` takes the candidate nearest the rule's other end
    (`AMBIGUOUS_CUT_NEAREST_THE_OTHER_END`) and records every candidate as an `ambiguous_cut`."""
    by_blk, at = _crossings(g, refs, universe)
    if not at:
        return None
    blk = max(at, key=lambda b: (by_blk.get(b, 0.0), b))
    ms = sorted(at[blk])
    return blk, ms[0], ms[1:]


def _crossings(g, refs: set[str], universe: set[str]
               ) -> tuple[dict[str, float], dict[str, set[float]]]:
    """`({blk: length in universe}, {blk: {measure}})` — every place a node bound carrying one of
    `refs` sits, per blue line. The one scan `_cut_at` (the principal line) and `_side_crossings`
    (the braid's other lines) both read."""
    by_blk: dict[str, float] = {}
    at: dict[str, set[float]] = {}
    for nid in universe:
        n = g.nodes.get(nid)
        if n is None:
            continue
        by_blk[n.blk] = by_blk.get(n.blk, 0.0) + (n.length_m or 0.0)
        for b in (n.lower_bound, n.upper_bound):
            if b is not None and b.boundary_id in refs:
                at.setdefault(n.blk, set()).add(b.route_measure)
    return by_blk, at


#: POLICY (T141, owner Q100 2026-10-23): A CUT ACROSS A BRAID REPORTS ITS SIDE-CHANNEL CROSSINGS. The
#: perpendicular sweep cuts the mainstem AND every side channel at the same place under one split id
#: (`_cut_at`); the reach is taken on the principal line, but the cut is just as much where each side
#: channel crosses it. Each used cut carries them as `side` — `[(blk, measure), …]` on the OTHER blue
#: lines — so every reader of the cuts (the tributary seeding, `build.seeding_cuts`; the confluence
#: test, `build.confluence_excludes`) sees the whole cut, not its principal line alone. False = no
#: side crossings (the mutation pin).
CUTS_REPORT_SIDE_CHANNEL_CROSSINGS = True


def _side_crossings(g, refs: set[str], universe: set[str], blk: str) -> list[tuple[str, float]]:
    """The cut's crossings on every blue line but `blk` (its principal line), sorted."""
    if not CUTS_REPORT_SIDE_CHANNEL_CROSSINGS:
        return []
    _, at = _crossings(g, refs, universe)
    return sorted((b, m) for b, ms in at.items() if b != blk for m in ms)


def _by_measure(g, universe: set[str], blk: str, lo: float, hi: float,
                seed_outside: frozenset = frozenset(),
                why: dict | None = None) -> tuple[set[str], set[str]]:
    """(sections in the reach, braided pieces that STRADDLE its end).

    Two passes. First the cut's own blue line, selected by ROUTE MEASURE — exact, no topology needed.
    Then the braided side channels on other blue lines, by where they connect: a braid whose ends BOTH
    attach inside the reach is inside it, one whose ends are both outside is outside, and only one that
    detaches inside and rejoins outside genuinely straddles the boundary. Repeated to a fixpoint so a
    braid hanging off a braid resolves too.

    The per-piece fixpoint alone is not enough. Two braid pieces that hang off each other each wait on
    the other to settle and NEITHER ever does — so a cluster whose external neighbours all agree gets
    reported as straddling when it plainly does not. (On the Chilliwack that was 6 of 21 sections: one
    pair whose only outside neighbour is inside the reach, one four-piece nest whose every outside
    neighbour is below the cut.) So whatever the fixpoint leaves is grouped into CONNECTED COMPONENTS
    and each judged as a unit on the neighbours outside itself — the same per-component rule the braid
    prune uses. Only a component that really does touch both sides is returned as straddling.

    (A flow-graph walk was tried first and is wrong here: `upstream_of` and `downstream_of` the same cut
    both returned 36 of the Chilliwack's 39 sections, which cannot both be true.)"""

    # THE CUT IS A BOUNDARY ON EVERY LINE IT CROSSES (T67): the atlas cuts a braid across the cut under
    # the split's own id (`splits.braid_sweep`), so two pieces of ANOTHER blue line meeting at a bound
    # that carries the window's cut are on opposite sides of it, not neighbours. Without this the two
    # halves of a cut braid hold each other across the cut and read as one straddling component.
    cut_ids: set[str] = set()
    lo_ids: set[str] = set()                               # the ids of the window's lower cut …
    hi_ids: set[str] = set()                               # … and of its upper cut
    if A_CUT_SEVERS_EVERY_LINE_IT_CROSSES:
        for nid in universe:
            n = g.nodes.get(nid)
            if n is None or n.blk != blk:
                continue
            for b in (n.lower_bound, n.upper_bound):
                if b is None or b.route_measure is None:
                    continue
                names = {str(b.boundary_id), *(b.aliases or ())}
                if abs(b.route_measure - lo) < 0.001:
                    lo_ids |= names
                if abs(b.route_measure - hi) < 0.001:
                    hi_ids |= names
        cut_ids = lo_ids | hi_ids

    def _across_the_cut(a: str, b: str) -> bool:
        na, nb = g.nodes.get(a), g.nodes.get(b)
        if na is None or nb is None or not na.blk or na.blk != nb.blk or na.blk == blk:
            return False
        bnd = na.upper_bound if abs(na.up_m - nb.down_m) < 0.001 else (
            na.lower_bound if abs(na.down_m - nb.up_m) < 0.001 else None)
        return bnd is not None and (str(bnd.boundary_id) in cut_ids
                                    or any(x in cut_ids for x in (bnd.aliases or ())))

    def _nbrs(nid: str) -> set[str]:
        out = {g.edges[i].to_node for i in g.down_adj.get(nid, [])}
        out |= {g.edges[i].from_node for i in g.up_adj.get(nid, [])}
        out &= universe
        if cut_ids:
            out = {x for x in out if not _across_the_cut(nid, x)}
        return out

    said: dict[str, str] = {}                              # this call's own reasons, always kept

    def _say_here(nid: str, reason: str) -> None:
        said.setdefault(nid, reason)
        _say(why, nid, reason)

    on_blk, others = set(), set()
    poly_out, poly_straddle = set(), set()
    for nid in universe:
        n = g.nodes.get(nid)
        if n is None:
            continue
        if not n.blk:
            # A WATERBODY NODE IS PLACED BY ITS MEASURE WINDOW on this blue line (`graph.windows`):
            # a river's own polygon is the river between the measures the line leaves and enters
            # it at, selected exactly like a line piece. A cut INSIDE the window (never from a
            # curated split — the sectionizer aliases a cut landing in a waterbody onto its edge)
            # leaves it straddling: reported, never guessed. A polygon this line does not pass
            # through is placed by its neighbours below, like any off-line piece.
            w = polygon_window(g, nid, blk)
            if w is None:
                others.add(nid)
                continue
            _, wlo, whi = w
            wlo = wlo if wlo is not None else whi
            whi = whi if whi is not None else wlo
            if wlo >= lo - 0.001 and whi <= hi + 0.001:
                on_blk.add(nid)
            elif whi <= lo + 0.001 or wlo >= hi - 0.001:
                poly_out.add(nid)
            else:
                poly_straddle.add(nid)
            continue
        if n.blk != blk:
            others.add(nid)
        elif n.down_m >= lo - 0.001 and n.up_m <= hi + 0.001:
            on_blk.add(nid)

    for nid in poly_straddle:
        _say(why, nid, "polygon_straddles_the_cut")
    inside = set(on_blk)
    outside = ({n for n in universe if g.nodes.get(n) and g.nodes[n].blk == blk} - on_blk) | poly_out
    # `outside` is normally seeded from this blue line’s own pieces that fall outside the window.
    # A window covering the WHOLE line leaves it empty, and an empty `outside` is not neutral: the
    # fixpoint below can then only ever move a piece INWARD (`nbrs <= outside` is unsatisfiable), so
    # it cascades across the junction and swallows water on the far side of the cut. That is how
    # "everything below the Talchako confluence" came to claim five Atnarko sections that are ABOVE
    # it, and it stayed hidden while a 58 m sliver sat above the cut acting as the seed. A caller
    # that knows which pieces sit across the cut passes them here.
    outside |= (set(seed_outside) & universe)
    pending = set(others) - outside

    def _settle(pending: set[str], nbrs_of) -> set[str]:
        """Settle every piece whose neighbours all lie on one side, to a fixpoint; return the rest."""
        while True:
            settled = set()
            for nid in sorted(pending):
                nbrs = nbrs_of(nid)
                if not nbrs:
                    continue                               # detached: cannot be placed
                if nbrs <= inside:
                    inside.add(nid); settled.add(nid)
                elif nbrs <= outside:
                    outside.add(nid); settled.add(nid)
            if not settled:
                return pending                             # fixpoint; judge what is left per component
            pending = pending - settled

    def _components(pending: set[str], nbrs_of) -> list[tuple[set[str], set[str]]]:
        """Connected components within `pending`, each with the neighbours outside itself."""
        out, seen = [], set()
        for start in sorted(pending):
            if start in seen:
                continue
            comp, stack = set(), [start]
            seen.add(start)
            while stack:
                nid = stack.pop()
                comp.add(nid)
                for x in nbrs_of(nid) & pending:
                    if x not in seen:
                        seen.add(x)
                        stack.append(x)
            out.append((comp, set().union(*(nbrs_of(n) for n in comp)) - comp))
        return out

    pending = _settle(pending, _nbrs)
    straddling: set[str] = set()
    detached: list[set[str]] = []
    for comp, ext in _components(pending, _nbrs):
        if ext and ext <= inside:
            inside |= comp
        elif ext and ext <= outside:
            outside |= comp
        elif not ext:
            detached.append(comp)                          # touches nothing in the water: below
        else:
            straddling |= comp                             # touches both sides: unplaceable
            for nid in comp:
                _say_here(nid, "straddles_the_cut")

    # DETACHED PIECES, PLACED BY THEIR OWN BLUE LINE. A side channel threading a chain of little
    # unnamed lakes (the Peace below Site C: blk 359004080 runs piece, lake, piece, lake…) touches
    # no section of the water at all — its neighbours are the lakes, which belong to no named item
    # — so it was reported as straddling and every row on the river dropped it. It is still ON a
    # blue line whose other pieces ARE placed, and route measure on that line says where it is:
    # the nearest placed piece of the same line below it and above it. Both inside → inside; both
    # outside → outside; only one side placed, or the two disagree → it stays unplaceable. Judged
    # against the placements made above and never against each other, so the order is irrelevant.
    lost: set[str] = set()
    for comp in detached:
        if 0.0 < lo and hi < float("inf") and _between_the_windows_cuts(g, comp, blk, lo_ids, hi_ids):
            inside |= comp                                 # T187: cut off by the window's own two cuts
            continue
        side = _bracket(g, comp, inside, outside, universe)
        if side == "in":
            inside |= comp
        elif side == "out":
            outside |= comp
        else:
            lost |= comp

    if lost and OFF_LINE_PIECES_FOLLOW_THEIR_FLOW:
        # WHAT IS STILL DETACHED IS PLACED BY ITS FLOW PATH (D5, owner 2026-10-12). `_nbrs` sees only
        # the scoped water, so a piece whose link to it runs through a lake, a pond, another named
        # water or the sea touched "nothing", and the bracket needs a placed piece of its own line on
        # BOTH sides — none exists above the Chilliwack's lake or below the Cowichan's delta pond.
        # Its water still goes somewhere: follow it (`_flow_reach`) to the first sections of the
        # scoped water it meets and judge it on those exactly as on neighbours — every one on one
        # side places it; touching a straddling braid or both sides leaves it unplaced. Only these
        # pieces are re-judged, so no placement made above can change.
        reach: dict[str, set[str]] = {}
        flags: dict[str, set[str]] = {}
        for nid in sorted(lost):
            reach[nid], flags[nid] = _flow_reach(g, nid, universe, blk, lo, hi,
                                                 cut_ids=frozenset(cut_ids), principal=blk)
        inside.add(_JUST_ABOVE_IN)                         # the window's side of "just above a cut"
        outside.add(_JUST_ABOVE_OUT)

        def _flow_nbrs(nid: str) -> set[str]:
            return _nbrs(nid) | reach.get(nid, set())
        rest = _settle(lost, _flow_nbrs)
        for comp, ext in _components(rest, _flow_nbrs):
            if ext and ext <= inside:
                inside |= comp
                continue
            if ext and ext <= outside:
                outside |= comp
                continue
            straddling |= comp
            if ext & straddling:
                reason = "joins_a_straddling_braid"
            elif ext:
                reason = "straddles_the_cut"
            else:
                # WHY its water could not place it — the flow path's own word, never a guess.
                seen_flags = set().union(*(flags.get(n, set()) for n in comp))
                reason = next((r for f, r in _UNJOINED if f in seen_flags),
                              "touches_none_of_its_water")
            for nid in comp:
                _say_here(nid, reason)
    else:
        straddling |= lost
        for nid in lost:
            _say_here(nid, "detached")

    inside.discard(_JUST_ABOVE_IN)
    outside.discard(_JUST_ABOVE_OUT)
    return inside, straddling | poly_straddle


#: T67 (owner 2026-10-24): A CUT SEVERS EVERY LINE IT CROSSES — adjacency across a bound carrying the
#: window's cut, on any blue line but the cut's principal one, is not adjacency (`_by_measure`). False is
#: the mutation pin (the cut braid reads as one straddling component again). POLICY_VERSION 29.
A_CUT_SEVERS_EVERY_LINE_IT_CROSSES = True

#: T187 (2026-10-24): A BRAID PIECE CUT AT BOTH ENDS BY THE WINDOW'S OWN TWO CUTS LIES INSIDE THE WINDOW.
#: The sweep cuts a braid at both cuts of a short `between` (the Peace's 1.84 km piece of blk 359005110
#: between "boundary signs 1200 m downstream of Site C dam" and "Site C dam"); the piece is then severed
#: from both its siblings (`A_CUT_SEVERS_EVERY_LINE_IT_CROSSES`), touches nothing of its water, and
#: `_bracket` judged it by its own line's placed neighbours — both across a cut, both outside — so it was
#: put outside and the closure lost it. A cut is a side change, so the bracket's two outside neighbours
#: are exactly what puts it inside. Narrow on purpose (`_between_the_windows_cuts`): only a detached
#: component on ONE other blue line, contiguous, whose LOWER end is the window's lower cut and whose
#: UPPER end is its upper cut (different ids) — a piece merely near a cut, ending at one cut only, or
#: lying the wrong way round is left to the bracket and the flow path as before. False is the mutation
#: pin. POLICY_VERSION 31.
A_PIECE_BETWEEN_THE_WINDOWS_CUTS_IS_INSIDE = True


def _between_the_windows_cuts(g, comp: set[str], principal: str, lo_ids: set[str],
                              hi_ids: set[str]) -> bool:
    """Is detached `comp` a run of one blue line (not `principal`) from the window's lower cut to its
    upper cut (`A_PIECE_BETWEEN_THE_WINDOWS_CUTS_IS_INSIDE`)?"""
    if not A_PIECE_BETWEEN_THE_WINDOWS_CUTS_IS_INSIDE or not lo_ids or not hi_ids:
        return False
    lo_only, hi_only = lo_ids - hi_ids, hi_ids - lo_ids    # one id naming both ends tells nothing
    nodes = [g.nodes.get(n) for n in comp]
    if any(n is None or not n.blk or n.blk == principal for n in nodes):
        return False
    if len({n.blk for n in nodes}) != 1:
        return False
    nodes.sort(key=lambda n: n.down_m)
    if any(abs(a.up_m - b.down_m) > 0.001 for a, b in zip(nodes, nodes[1:])):
        return False                                       # not one unbroken run of the line

    def _names(b) -> set[str]:
        return set() if b is None else {str(b.boundary_id), *(b.aliases or ())}
    return bool(_names(nodes[0].lower_bound) & lo_only) and bool(_names(nodes[-1].upper_bound) & hi_only)


#: D5 (owner 2026-10-12): a piece the direct neighbour pass cannot place is placed by its FLOW PATH —
#: through lakes, ponds, wetlands, other named waters and water outside B.C. (T11) — to the first
#: section of the scoped water it meets (`_flow_reach`). Guarded by the reach cache's POLICY_VERSION.
OFF_LINE_PIECES_FOLLOW_THEIR_FLOW = True

#: E15 IS GONE (T67, owner 2026-10-24, RULINGS E16): a braid straddling a cut is no longer placed on the
#: side holding most of its length and marked. The atlas CUTS it (`splits.braid_sweep`) wherever it
#: attaches to the host on both sides of the cut, and the reach reads the cut on every line it crosses
#: (`A_CUT_SEVERS_EVERY_LINE_IT_CROSSES`). What still straddles stays unplaced with its typed reason, and
#: the run's gate (`build.straddle_gate`) refuses it.

#: AN AMBIGUOUS CUT TAKES THE CANDIDATE NEAREST THE RULE'S OTHER END (owner 2026-10-22, decisions.md
#: "the audit plan"). One split id can land at two measures on one blue line — a lake the line runs
#: through is met at its outlet AND its inlet ("Harrison River … Harrison Lake": km 20 and km 84).
#: `_cut_at` hands back every candidate; the one taken is the one nearest the other end of the
#: stretch, "because this will never be a lake regulation and under-lake streams have no regs":
#: `downstream_of` the lowest (the other end is the mouth), `upstream_of` the highest (the source),
#: `between` on one line the pair nearest each other, `between` across two lines the upper cut's
#: lowest and the lower cut's highest that still keep the upper cut above the lower one. Every
#: candidate and the one taken are still recorded (`ambiguous_cut`, carried to the bundle's
#: `rule_ambiguous_cut`). False restores the old pick (nearest the mouth, whatever the rule).
#: Guarded by the reach cache's POLICY_VERSION (20; 21 for the still-ordered rule across lines).
AMBIGUOUS_CUT_NEAREST_THE_OTHER_END = True


def _take(at, m: float):
    """`at` = (blk, measure, alternatives) re-pointed at candidate `m`; the rest become alternatives."""
    blk, used, alts = at
    every = sorted({used, *alts})
    return blk, m, [x for x in every if x != m]


def _toward_end(at, upper: bool):
    """The candidate nearest the line's source (`upper`) or its mouth."""
    if not AMBIGUOUS_CUT_NEAREST_THE_OTHER_END or not at[2]:
        return at
    every = [at[1], *at[2]]
    return _take(at, max(every) if upper else min(every))


def _nearest_pair(a, b):
    """Two cuts on ONE line: the pair of candidates nearest each other (ties: the lower pair)."""
    if not AMBIGUOUS_CUT_NEAREST_THE_OTHER_END or not (a[2] or b[2]):
        return a, b
    pairs = [(x, y) for x in (a[1], *a[2]) for y in (b[1], *b[2])]
    x, y = min(pairs, key=lambda p: (abs(p[0] - p[1]), min(p), max(p)))
    return _take(a, x), _take(b, y)


#: A CURATED PICK OUTRANKS THE GENERAL RULE (owner 2026-10-22, decisions.md "Ambiguous cuts,
#: refinement"). Where a curated extent says which crossing a cut means (`Extent.cut_pick`: split id
#: -> "lower" | "upper" | a route measure), that candidate is taken BEFORE
#: `AMBIGUOUS_CUT_NEAREST_THE_OTHER_END` looks at the cut, and the general rule then sees only the
#: pinned candidate (the other cut of a `between` still follows the general rule, toward the pinned
#: one). The known case: skeena_river_2 starts at the Skeena River Ecological Reserve's LOWER
#: mainstem crossing (LS-1), a stand-in for the Exchamsiks confluence, where the general rule would
#: take the upper one. The diagnostic records every candidate, the one taken, `toward: "curated"`,
#: `reason: "curated"` and the curated `pick`. False ignores the field (the mutation pin). Not in
#: the reach cache's POLICY_VERSION: a rule's extents (`cut_pick` with them) are in its entry key,
#: and licensing records are placed fresh every run.
CURATED_CUT_PICK_OUTRANKS = True

#: How far a curated measure may sit from the candidate it names (metres along the line).
CUT_PICK_TOLERANCE_M = 25.0


def _curated_pick(at, pick):
    """`at` re-pointed at the curated `pick` ("lower", "upper" or a measure), or None when a measure
    names no candidate (stale: the atlas moved the crossing — refused, never snapped elsewhere)."""
    every = sorted({at[1], *at[2]})
    if pick == "lower":
        return _take(at, every[0])
    if pick == "upper":
        return _take(at, every[-1])
    m = min(every, key=lambda x: (abs(x - float(pick)), x))
    return _take(at, m) if abs(m - float(pick)) <= CUT_PICK_TOLERANCE_M else None


#: A flow path reaching the cut's line THROUGH A LAKE'S OUTLET exactly at a cut is just ABOVE that
#: cut — it drained into the lake, which lies wholly above its outlet (Sproat River pieces draining
#: into Sproat Lake, outside "between Sproat Lake and the signs"). Stand-ins for that position on the
#: window's inside / outside (`_flow_reach`).
_JUST_ABOVE_IN, _JUST_ABOVE_OUT = "\x00just-above-a-cut:in", "\x00just-above-a-cut:out"

#: The typed reasons a detached piece stays unplaced, strongest first (flag -> reason).
_UNJOINED = (("at_cut", "meets_the_line_at_the_cut"),
             ("leaves_bc", "leaves_bc_unjoined"),
             ("sea", "flows_to_the_sea_unjoined"))


def _say(why: dict | None, nid: str, reason: str) -> None:
    """Record the first reason a piece is unplaced (a caller that did not ask passes None)."""
    if why is not None:
        why.setdefault(nid, reason)


def _severed(g, a: str, b: str, principal: str, cut_ids) -> bool:
    """Do pieces `a` and `b` of one blue line (not the cut's principal one) meet at a bound carrying the
    window's cut? Then they are on opposite sides of it (`A_CUT_SEVERS_EVERY_LINE_IT_CROSSES`)."""
    if not cut_ids:
        return False
    na, nb = g.nodes.get(a), g.nodes.get(b)
    if na is None or nb is None or not na.blk or na.blk != nb.blk or na.blk == principal:
        return False
    bnd = na.upper_bound if abs(na.up_m - nb.down_m) < 0.001 else (
        na.lower_bound if abs(na.down_m - nb.up_m) < 0.001 else None)
    return bnd is not None and (str(bnd.boundary_id) in cut_ids
                                or any(x in cut_ids for x in (bnd.aliases or ())))


def _flow_reach(g, nid: str, universe: set[str], blk: str, lo: float, hi: float,
                cut_ids: frozenset = frozenset(), principal: str = ""):
    """(sections of the scoped water `nid`'s WATER reaches, flags) — the flow path through water
    that is not the scoped water's own.

    DOWNSTREAM through anything (a lake, a pond, another named water, water outside B.C. — T11: the
    path resumes where the stream re-enters) to the first scoped section on every branch; a branch
    that ends without one ends at the SEA (flag `sea`) or OUTSIDE B.C. (`leaves_bc`). UPSTREAM only
    through the piece's own water (its lakes and side channels): a tributary basin — a code that
    extends the piece's — never leads back to the river, so it is not climbed. A downstream path that
    meets the cut's own line AT the cut (within `WATERSHED_AT_CUT_M`) is on neither side: it is
    dropped and flagged `at_cut`, so the piece stays unplaced and says why — unless it arrives
    through a LAKE'S OUTLET, which puts it just above the cut (`_JUST_ABOVE_IN` / `_OUT`).

    THE WINDOW'S CUT IS NOT CROSSED (T67, `A_CUT_SEVERS_EVERY_LINE_IT_CROSSES`): neither walk steps between
    two pieces of a blue line that meet at a bound carrying the cut (a braid the atlas cut there: the
    Chowade's 26 m piece below Horseshoe Road Bridge climbed back over its own cut), and the upstream walk
    never climbs through the LAKE the cut is the edge of (`cut_ids` holds `lake:{wbk}`): a channel
    draining the window's cut lake is on that lake's downstream side (the Ash River's second outlet of
    Elsie Lake, outside "between Elsie Lake and Dickson Lake")."""
    if not A_CUT_SEVERS_EVERY_LINE_IT_CROSSES:
        cut_ids = frozenset()
    cuts = [c for c in (lo, hi) if 0.0 < c < float("inf")]
    from pipeline.atlas.registry.basins import node_basin_code
    hits: set[str] = set()
    flags: set[str] = set()
    seen, stack = {nid}, [nid]
    while stack:
        x = stack.pop()
        downs = g.down_adj.get(x, [])
        if not downs:                                      # the path ends: the sea, or out of B.C.
            n = g.nodes.get(x)
            flags.add("leaves_bc" if n is not None and n.out_of_bc else "sea")
        for i in downs:
            e = g.edges[i]
            y = e.to_node
            if _severed(g, x, y, principal, cut_ids):
                continue
            if y in universe:
                q = g.nodes.get(y)
                at = [c for c in cuts if abs(e.at_measure - c) <= WATERSHED_AT_CUT_M]
                if q is not None and q.blk == blk and at:
                    src = g.nodes.get(x)
                    if e.kind == "lake_out" and src is not None and not src.blk:
                        c = at[0]
                        hits.add(_JUST_ABOVE_IN if lo - WATERSHED_AT_CUT_M <= c < hi - WATERSHED_AT_CUT_M
                                 else _JUST_ABOVE_OUT)
                    else:
                        flags.add("at_cut")
                else:
                    hits.add(y)
            elif y not in seen:
                seen.add(y)
                stack.append(y)
    own = node_basin_code(g.nodes[nid]) if nid in g.nodes else ""
    seen, stack = {nid}, [nid]
    while stack:
        x = stack.pop()
        for i in g.up_adj.get(x, []):
            y = g.edges[i].from_node
            if _severed(g, x, y, principal, cut_ids) or y in cut_ids:
                continue
            if y in universe:
                hits.add(y)
                continue
            n = g.nodes.get(y)
            if y in seen or n is None or _wsc.is_strictly_below(node_basin_code(n), own):
                continue
            seen.add(y)
            stack.append(y)
    hits.discard(nid)
    return hits, flags


def _bracket(g, comp: set[str], inside: set[str], outside: set[str], universe: set[str]) -> str:
    """"in" / "out" / "" for a detached component: the side its members' nearest PLACED neighbours
    on their own blue line (by route measure, below and above) agree on."""
    sides: set[str] = set()
    for nid in comp:
        n = g.nodes.get(nid)
        if n is None or not n.blk:
            return ""
        below = above = None
        for o in universe:
            if o in comp or (o not in inside and o not in outside):
                continue
            on = g.nodes.get(o)
            if on is None or on.blk != n.blk:
                continue
            if on.up_m <= n.down_m + 0.001 and (below is None or on.up_m > g.nodes[below].up_m):
                below = o
            elif on.down_m >= n.up_m - 0.001 and (above is None or on.down_m < g.nodes[above].down_m):
                above = o
        if below is None or above is None:
            return ""
        sides |= {"in" if x in inside else "out" for x in (below, above)}
    return sides.pop() if len(sides) == 1 else ""


#: POLICY (T153, owner question Q124 — OPEN; built on its stated default, 2026-10-23): AN UNNAMED
#: LAKE POLYGON THE RIVER'S MAIN FLOW RUNS THROUGH IS THE RIVER for a rule whose extent covers BOTH
#: SIDES of it — the book's "river" includes its widenings unless it names the lake. FWA cuts a blue
#: line at every waterbody it crosses and the graph makes each one a `lake:{wbk}` node that no river
#: item holds, so a river row's reach stopped at every pond on its course and the pond fell back to
#: the zone rules (456 such polygons under a named row, census T148). A NAMED lake (a GNIS name on
#: the node) or one any registry water holds never takes the river's row: it is a water of its own.
#: Decided by ONE function, `river_widenings`; every extent that measures a river (`whole`, the cuts,
#: a `within` of a named water) calls it. False = the reach stops at the polygon (the mutation pin).
UNNAMED_LAKES_ON_THE_RIVER_ARE_THE_RIVER = True

#: How many unnamed polygons back to back one widening may chain through (a pond draining straight
#: into another pond on the same line). Measured: the longest chain under a regulated row is short;
#: the cap only bounds a pathological graph.
WIDENING_CHAIN_MAX = 8

#: {id(registry): (registry, frozenset of sections a registry WATER holds)} — checked by identity.
_HELD: dict[int, tuple[object, frozenset]] = {}


def _held_by_a_water(reg) -> frozenset:
    """Every section some registry water (stream, lake, wetland — never an area) holds."""
    hit = _HELD.get(id(reg))
    if hit is None or hit[0] is not reg:
        out: set[str] = set()
        for it in (reg.values() if hasattr(reg, "values") else ()):
            k = getattr(it, "kind", "")
            if str(getattr(k, "value", k) or "").lower() in ("stream", "lake", "wetland"):
                out.update(getattr(it, "section_ids", None) or ())
        hit = _HELD[id(reg)] = (reg, frozenset(out))
    return hit[1]


def river_widenings(g, reg, selected: set[str]) -> set[str]:
    """THE UNNAMED LAKE POLYGONS a reach takes as its river's widenings (T153,
    `UNNAMED_LAKES_ON_THE_RIVER_ARE_THE_RIVER`) — the one place it is decided.

    A polygon is taken when ALL hold:
      * it is a waterbody node with no GNIS name and no registry water holds it (unnamed, not a
        water of its own — every registry lake is named);
      * it is not already selected;
      * the river's MAIN FLOW runs through it: it drains (`lake_out`) into a SELECTED piece P, and
        a SELECTED piece on P's own blue line, from further up that line, flows into it
        (`lake_in`) — the extent covers both sides, as `graph.tributaries._lake_mid_reach` reads
        "a lake in the middle of the reach" — and both pieces carry a MAIN FLOW line
        (`fwa_edge_types.is_main_flow`: 1000/1050/1200/1250). A pond on a side channel (1100/1350,
        or a blue line of its own), a lake at the reach's top or mouth, or one holding a cut is
        never taken.
    Unnamed polygons back to back on the line (a pond draining into a pond) are taken together,
    every one on the path between the two sides (`WIDENING_CHAIN_MAX`); a NAMED polygon in the
    chain breaks it."""
    if not UNNAMED_LAKES_ON_THE_RIVER_ARE_THE_RIVER or g is None or not hasattr(g, "nodes"):
        return set()
    held = _held_by_a_water(reg)

    def _open(nid: str) -> bool:
        n = g.nodes.get(nid)
        return (n is not None and not n.blk and not (n.gnis_id or n.display_name)
                and nid not in held and nid not in selected)

    out: set[str] = set()
    for pid in sorted(selected):
        p = g.nodes.get(pid)
        if p is None or not p.blk or not is_main_flow(p):
            continue
        for ei in sorted(g.up_adj.get(pid, ())):
            e = g.edges[ei]
            if e.kind != "lake_out" or not _open(e.from_node):
                continue
            stack = [(e.from_node, (e.from_node,))]
            while stack:
                cur, path = stack.pop()
                for ej in sorted(g.up_adj.get(cur, ())):
                    f = g.edges[ej]
                    src = f.from_node
                    s = g.nodes.get(src)
                    if s is None:
                        continue
                    if s.blk:
                        if (f.kind == "lake_in" and src in selected and s.blk == p.blk
                                and s.down_m >= p.up_m - 1.0 and is_main_flow(s)):
                            out.update(path)
                    elif src not in path and len(path) < WIDENING_CHAIN_MAX and _open(src):
                        stack.append((src, path + (src,)))
    return out


def whole_water_sections(reg, g, item_id: str) -> set[str]:
    """Every section a water's id stands for when a rule names the WHOLE of it: its own, and — for
    a lake cut into parts, which owns none (user ruling 2026-10-03, `registry.add_lake_parts`) —
    its parts' and its own whole polygon (the ghost node the graph keeps). "Kootenay Lake" taken
    out of the Creston Valley WMA (`outside_items`, AGENTS 13) takes out the Main Body, the West
    Arms and the ghost, not nothing."""
    it = reg[item_id]
    out = set(it.section_ids)
    parts = [k for k, v in reg.items() if getattr(v, "part_of", "") == item_id]
    if parts:
        out |= {s for k in parts for s in reg[k].section_ids}
        wbk = item_id.split(":", 1)[1] if item_id.startswith("wbk:") else ""
        ghost = f"lake:{wbk}"
        if wbk and g is not None and hasattr(g, "nodes") and ghost in g.nodes:
            out.add(ghost)
    return out


def _kind_of(g, section_id: str, reg=None) -> str:
    """A section's water kind — `stream`, `lake`, `wetland` — for `feature_types`: the ONE
    definition, `water_kind.kind_of` (a lake-typed water whose name says it flows — a slough, a
    canal — is a stream; pass the registry `reg` so it can tell).

    Lower-cased and stringified because the graph stores an enum and the authored extent
    stores text, and the two only have to agree here.
    """
    from pipeline.atlas.reach.water_kind import kind_of
    return kind_of(g, reg, section_id)


def _waters(g, section_ids) -> list[str]:
    """The distinct named waters a reach actually lands on, in size order (biggest share first).

    A section id says nothing to a curator; "Chilliwack River" does. A rule's authored extent reads
    "downstream of Vedder Crossing Bridge", which does not reveal that it resolves onto the VEDDER —
    the water it is scoped to is exactly what a reader needs to check, and exactly what the extent
    text hides."""
    by_name: dict[str, float] = {}
    for nid in section_ids:
        n = g.nodes.get(nid)
        if n is None:
            continue
        nm = (n.display_name or "").strip() or "(unnamed)"
        by_name[nm] = by_name.get(nm, 0.0) + (n.length_m or 0.0)
    return [n for n, _ in sorted(by_name.items(), key=lambda kv: (-kv[1], kv[0]))]


def _on_line_end(g, universe: set[str], blk: str, m: float, upper: bool) -> bool:
    """Does this cut sit exactly on the END of its own blue line, within the scoped water?"""
    ms = [(g.nodes[n].up_m if upper else g.nodes[n].down_m)
          for n in universe if g.nodes.get(n) is not None and g.nodes[n].blk == blk]
    return bool(ms) and abs(m - (max(ms) if upper else min(ms))) < 0.01


def _half(g, universe: set[str], blk: str, m: float, upper: bool, why: dict | None = None):
    """One side of a cut — measure-selected, and by COMPLEMENT when the cut sits on the line's end.

    `_by_measure` seeds from nodes of the cut's OWN blue line that fall inside the window, then grows
    over the braid. A cut sitting exactly on that line's end leaves the window with nothing to seed
    from, so the half comes back EMPTY — not "nearly empty", but zero, no matter how the cut got
    there. That is a limit of the seeding, not a fact about the river: the Bella Coola's head IS the
    confluence where the Talchako meets the Atnarko, and the water above it is simply on the next
    blue line up. Before this, "between Goat Creek and the Talchako" intersected with that empty half
    and bound nothing — a rule that reads as "no regulation here", which is the failure this project
    can least afford.

    So when the measure half is empty AND the cut is on that end, take the other half's COMPLEMENT.
    A cut cleanly halves the water, which is the same assumption `_between_across_lines` already
    makes.

    The complement takes everything the other half did not, INCLUDING what that pass called
    straddling. When one side of the cut is empty there is nothing for the braid fixpoint to settle
    against — `outside` never gets a seed, so no piece can ever be placed outside, and whatever fails
    to settle is reported as straddling by default. On the Bella Coola that was all 17 Atnarko
    sections: the entire answer, labelled unplaceable. A piece cannot straddle the end of a line
    anyway — there is no water on the far side to straddle into."""
    INF = float("inf")
    lo, hi = (m, INF) if upper else (0.0, m)
    # Which end (if either) this cut sits on is a property of the CUT, not of the side being asked
    # for. Both sides need the junction seed: the empty side so its complement is taken from a
    # correct other half, and the populated side so it does not swallow the far water itself.
    at_top = _on_line_end(g, universe, blk, m, True)
    at_bottom = _on_line_end(g, universe, blk, m, False)
    if not (at_top or at_bottom):
        return _by_measure(g, universe, blk, lo, hi, why=why)
    seed = _across_the_junction(g, universe, blk, m, at_top)
    sec, straddling = _by_measure(g, universe, blk, lo, hi, seed_outside=seed, why=why)
    if sec:
        return sec, straddling
    other, _ = _by_measure(g, universe, blk, *((0.0, m) if upper else (m, INF)), seed_outside=seed)
    return universe - other, set()


def _across_the_junction(g, universe: set[str], blk: str, m: float, upper: bool) -> frozenset:
    """Pieces on OTHER blue lines meeting this line exactly at `m` — the far side of a cut that
    sits on this line’s end. Seeding them as `outside` is what stops the braid fixpoint walking up
    the next river and calling it downstream."""
    out: set[str] = set()
    for nid in universe:
        n = g.nodes.get(nid)
        if n is None or n.blk != blk:
            continue
        if abs((n.up_m if upper else n.down_m) - m) >= 0.01:
            continue
        for i in g.down_adj.get(nid, []):
            out.add(g.edges[i].to_node)
        for i in g.up_adj.get(nid, []):
            out.add(g.edges[i].from_node)
    return frozenset(x for x in (out & universe) if g.nodes.get(x) and g.nodes[x].blk != blk)


def _between_across_lines(g, universe: set[str], a, b, why: dict | None = None,
                          chosen: dict | None = None):
    """`between` when the two cuts sit on DIFFERENT blue lines, or None if they are not on one flow.

    A reach can span a name change: "downstream of Tamihi Rapids Bridge to Vedder Crossing Bridge" runs
    down the Chilliwack and onto the Vedder, which is a separate blue line, so there is no single route
    measure to compare the two cuts on. But each cut still cleanly halves the water, so the reach is
    just the INTERSECTION of the two halves:

        between(A, B) = downstream-of-the-upstream-cut  AND  upstream-of-the-downstream-cut

    Which cut is upstream is read off the halves themselves rather than by walking the flow graph
    (unreliable across braids): if B's own piece falls inside A's downstream half, then B is below A.
    Both cuts keep their braid handling, and a piece that straddles EITHER cut stays unplaceable.

    AN AMBIGUOUS CUT (`AMBIGUOUS_CUT_NEAREST_THE_OTHER_END`): the order is read with the cuts as
    given; then the UPPER cut takes its lowest candidate and the LOWER cut its highest — each the
    one nearest the other end — unless that pair is no longer ordered (a candidate past where the
    two lines join), when the nearest pair that is still ordered is taken; then the halves are
    taken again. `chosen`, when given, receives the
    two cuts actually used, as {"a": ..., "b": ...}."""

    def _below(a, b, why=None):
        """(below A, straddling A, below B, straddling B, B below A?, A below B?)."""
        (blk_a, m_a, _), (blk_b, m_b, _) = a, b
        down_a, s_a = _half(g, universe, blk_a, m_a, upper=False, why=why)  # below A
        down_b, s_b = _half(g, universe, blk_b, m_b, upper=False, why=why)  # below B
        b_below_a = any(n in down_a for n in universe if n.startswith(f"{blk_b}:"))
        a_below_b = any(n in down_b for n in universe if n.startswith(f"{blk_a}:"))
        return down_a, s_a, down_b, s_b, b_below_a, a_below_b

    if AMBIGUOUS_CUT_NEAREST_THE_OTHER_END and (a[2] or b[2]):
        *_, b_below_a, a_below_b = _below(a, b)
        if b_below_a == a_below_b:
            return None                 # neither above the other (parallel branches): no reach between
        upper_cut, lower_cut = (a, b) if b_below_a else (b, a)
        # Each cut's candidates nearest the other end first: the upper cut's lowest, the lower
        # cut's highest. A candidate past the junction of the two lines (a tributary entering the
        # lower line's lake: the lake's inlet is ABOVE where it joins) leaves the cuts unordered,
        # so the nearest pair that still keeps the upper cut above the lower one is taken; none
        # = the cuts as given (reviewer 2026-10-22).
        ups = sorted({upper_cut[1], *upper_cut[2]})
        downs = sorted({lower_cut[1], *lower_cut[2]}, reverse=True)
        pairs = sorted(((i, j) for i in range(len(ups)) for j in range(len(downs))),
                       key=lambda p: (p[0] + p[1], p[0]))
        for i, j in pairs:
            u, lw = _take(upper_cut, ups[i]), _take(lower_cut, downs[j])
            *_, lw_below_u, u_below_lw = _below(u, lw)
            if lw_below_u and not u_below_lw:
                upper_cut, lower_cut = u, lw
                break
        a, b = (upper_cut, lower_cut) if b_below_a else (lower_cut, upper_cut)
    if chosen is not None:
        chosen.update(a=a, b=b)
    down_a, s_a, down_b, s_b, b_below_a, a_below_b = _below(a, b, why=why)
    if b_below_a == a_below_b:
        return None                     # neither above the other (parallel branches): no reach between
    if chosen is not None:
        chosen["b_below_a"] = b_below_a     # which cut is the reach's LOWER end (`cuts`)
    (blk_a, m_a, _), (blk_b, m_b, _) = a, b
    up_a, _ = _half(g, universe, blk_a, m_a, upper=True)
    up_b, _ = _half(g, universe, blk_b, m_b, upper=True)
    sec = (down_a & up_b) if b_below_a else (down_b & up_a)
    return sec, (s_a | s_b) - sec


def resolve_extent(reg, g, covered_ids: list[str], ex: dict,
                   reasons: list | None = None) -> dict | None:
    """What one extent selects: ``{"sections": [...], "unclassified": [...], "ambiguous_cut": [...]}``.

    `sections` is exact — including braided pieces whose ends both attach inside the reach.
    `unclassified` holds only the pieces that genuinely STRADDLE the reach's end (in at one side, out
    at the other) — surfaced, never silently swallowed. `ambiguous_cut` names any split whose cut lands
    at more than one measure on the chosen blue line, so the curator can see that the reach shown is
    one of two readings (see `_cut_at`). None = not determinable at all (an `within(area)` scope, or a
    cut that is not on the scoped water).

    `cuts` (T111) are the cuts the reach was actually taken at — `{split, blk, m, end, kind, label,
    side}`, `end` "lower" or "upper", after `cut_pick` and the ambiguous-cut rule, `side` the same
    cut's crossings of the braid's other blue lines (T141, `CUTS_REPORT_SIDE_CHANNEL_CROSSINGS`);
    empty for `whole` and `within`. The tributary seeding (`build.seeding_cuts`) and the confluence test
    (`build.confluence_excludes`) read them; neither re-derives a cut.

    ``reasons`` (optional): a list the caller passes to collect ``(code, detail)`` for anything that
    could NOT be resolved. `None` alone says a rule binds nothing without saying why, and a rule that
    silently binds nothing is the failure this project can least afford — it is indistinguishable
    from a water with no regulation. Purely additive: omit it and behaviour is unchanged."""
    def _fail(code: str, detail: str = "") -> None:
        if reasons is not None:
            reasons.append((code, detail))
    # `item_ids` scopes an extent to SEVERAL items — a reach whose two cut-points sit on different
    # waters (Tamihi on the Chilliwack, Vedder Crossing on the Vedder). Scoping such a reach to either
    # one alone puts the other end out of scope and it cannot resolve at all.
    scope = ex.get("item_ids") or ([ex["item_id"]] if ex.get("item_id") else covered_ids)
    universe: set[str] = set()
    for i in scope:
        if i in reg:
            universe |= set(reg[i].section_ids)
    op = ex.get("op")
    if op == "rest":
        # A COMPLEMENT IS A STATEMENT ABOUT OTHER RULES' REACHES, so one extent alone cannot say
        # it: `build.build_reach` resolves it against the rule's siblings.
        _fail("rest_needs_its_siblings", ",".join(ex.get("siblings") or ()))
        return None
    if op == "steelhead_waters":
        # THE KNOWN STEELHEAD WATERS are a fact of the whole corpus (`build.build_reaches`).
        _fail("steelhead_waters_need_the_corpus", ",".join(ex.get("siblings") or ()))
        return None
    if not universe and op != "within":
        # `within` is the one op that can answer without an item scope — an admin-area closure names
        # the area, not a water. Every other op selects FROM a water, so no water means no answer.
        known = [i for i in scope if i in reg]
        _fail("no_sections_for_items", f"no scoped item is in the registry: {scope}" if not known
              else f"every scoped item has zero sections: {known}")
        return None
    # `within_area` LIMITS whatever this extent selects to a polygon. It is the one shape the
    # vocabulary could not express: "any stream in the Fraser River Watershed OF REGION 5" is a
    # watershed INTERSECTED with an administrative area, and every op here only ever ADDS water.
    #
    # A bounded reach cannot substitute, measured: walking the Fraser from its region-5 sections
    # leaks 2,074 sections outside the region and misses 23,052 inside it (waters that drain into
    # the Fraser somewhere else). And region 6 holds no Fraser mainstem at all, so there is no
    # reach to bound.
    #
    # Resolved HERE and carried forward, because the intersection must be applied AFTER the
    # tributary walk — the walk is what leaves the area, so filtering the seed would do nothing.
    limit_sections: set[str] | None = None
    limit_id = str(ex.get("within_area") or "")
    if limit_id:
        key = area_key(limit_id)
        limit_sections = area_sections(reg, g, key)
        if limit_sections is None:
            _fail("within_area_not_in_registry", limit_id)
            return None

    # `outside_area` SUBTRACTS a polygon — the mirror of `within_area`, and the shape a
    # regulation needs when its own header carves one out. The synopsis prints "Region 1 Daily
    # Quotas (excluding Haida Gwaii)" and then prints Haida Gwaii's table beside it; with no way
    # to say the exclusion, both bound the Yakoun River and the app stated Trout 4 and
    # Trout/char 5, Kokanee 5 and Kokanee 10, one above the other.
    #
    # Applied here, with `within_area`, so it lands AFTER the tributary walk — the walk is what
    # leaves the area, so subtracting from the seed would do nothing.
    # `outside_areas` is the same thing for SEVERAL carve-outs, unioned with `outside_area`. A
    # residual scope needs it: DFO Region 6 section E is "Other Mainland Watersheds", the region
    # minus the Skeena, the Nass, the Fraser and Haida Gwaii, and a rule's extents UNION so the
    # subtraction can never be written as more extents.
    drop_sections: set[str] = set()
    drop_ids = [str(x) for x in (ex.get("outside_areas") or []) if x]
    if ex.get("outside_area"):
        drop_ids.append(str(ex["outside_area"]))
    for drop_id in drop_ids:
        key = area_key(drop_id)
        got_area = area_sections(reg, g, key)
        if got_area is None:
            _fail("outside_area_not_in_registry", drop_id)
            return None
        drop_sections |= got_area
    # `outside_area_kind` SUBTRACTS A WHOLE FAMILY of areas, the mirror of `area_kind` on
    # `within`. "Basic and supplementary licences and stamps are not valid in National Parks" is
    # about all seven parks, and as `outside_areas` it would be a hand list that goes stale when
    # a park is gazetted. An empty family is a failure, never a no-op that leaves the parks in.
    drop_kind = str(ex.get("outside_area_kind") or "")
    if drop_kind:
        family = _regions.area_family_sections(reg, drop_kind, g)
        if family is None:
            _fail("outside_area_kind_matches_nothing", drop_kind)
            return None
        drop_sections |= family
    # `outside_items` SUBTRACTS WATERS by registry id, at the same point. A `within(area)` holds
    # every lake the polygon merely reaches into (lakes are never cut), so an area row needs a way
    # to take such a lake back out — and a water it names but does not have is a failure, never a
    # silent no-op that leaves the lake covered.
    for item_id in (ex.get("outside_items") or []):
        if item_id not in reg:
            _fail("outside_item_not_in_registry", str(item_id))
            return None
        drop_sections |= whole_water_sections(reg, g, item_id)

    def _limited(sec: set[str]) -> set[str]:
        out = sec if limit_sections is None else (sec & limit_sections)
        return (out - drop_sections) if drop_sections else out

    def _out(sec: set[str], *, widen: bool = False, inside: set[str] | None = None,
             **extra) -> dict:
        # THE RIVER'S UNNAMED WIDENINGS (T153, `river_widenings`): a measured river extent takes
        # every unnamed polygon its main flow runs through between two pieces it selected — a
        # `within` of a named water only those inside the area. Reported (`widened`), and kept out
        # of `waters`, which names the waters the reach lands on.
        wide: set[str] = river_widenings(g, reg, sec) if widen else set()
        if inside is not None:
            wide &= inside
        river = sec
        sec = sec | wide
        d = {"sections": sorted(_limited(sec)), "unclassified": [], "ambiguous_cut": [],
             "window": None, "cuts": [], "waters": _waters(g, _limited(river))}
        if wide:
            d["widened"] = sorted(_limited(wide))
        if not d["sections"]:
            # resolved, and selects nothing: said, so an unbound rule's reason is the resolver's
            if op == "between":
                _fail("between_selects_nothing", "both cuts resolve but the reach between them is "
                      "empty — the two may have collapsed to one measure (see the Mitchell alias bug)")
            else:
                _fail("selects_nothing", f"the {op} extent resolves and selects no section")
        if limit_sections is not None:
            d["within_area"] = sorted(limit_sections)
            if op != "within":
                # THE WALK'S SEED IS THE WHOLE NAMED WATER, NOT ITS PART INSIDE THE AREA. "The
                # Fraser River watershed in Region 6" walks the Fraser and keeps what lands in
                # Region 6; clipping first left an EMPTY seed, because no Fraser mainstem runs
                # through Region 6, and three Region 6 closures bound nothing. `sections` stays
                # limited — a rule that does not walk is the part inside the area, as before —
                # and `classify` walks from `seed`, then applies `within_area` to what it found.
                d["seed"] = sorted((sec - drop_sections) if drop_sections else sec)
        # THE KINDS THE RULE'S REACH IS LIMITED TO, carried forward like `within_area` and for
        # the same reason: "lakes of the Fraser watershed" is a filter on what the tributary walk
        # finds, so `classify` applies it after the walk. (On `within` the area's members were
        # already filtered below; applying it again after a walk is what keeps a walk from one
        # lake from adding every stream above it.)
        if ex.get("feature_types"):
            d["feature_types"] = sorted({str(t).lower() for t in ex["feature_types"]})
        d.update(extra)
        return d

    if op == "whole":
        return _out(universe, widen=True)
    if op == "within":
        # An `area:` registry item already carries every section its polygon covers (the build's
        # membership pass), so `within` needs no geometry here — it is a set operation.
        #
        # TWO READINGS, one rule. "Pitt River within Garibaldi Park" qualifies a named water: the
        # answer is the PART OF PITT RIVER inside the park, never every stream in Garibaldi. "No
        # fishing within X Ecological Reserve" names no water at all: the answer is EVERYTHING inside
        # the polygon. Both fall out of intersecting with the rule's item universe and taking the whole
        # area when there is no universe to intersect with — the rule's own scope decides, so nothing
        # has to classify areas into admin-vs-qualifying and the two cannot drift apart.
        aid = str(ex.get("area_id") or "")
        kind_of_area = str(ex.get("area_kind") or "")
        if not aid and not kind_of_area:
            _fail("within_without_area_id")
            return None
        if kind_of_area:
            # A WHOLE FAMILY OF AREAS, because some regulations are written against one.
            # "Freshwater fishing is prohibited in National Parks" is about all seven, and
            # the ecological-reserve closure is about all 120 — as `area_id` extents that
            # would be 127 hand-listed ids that go stale the moment the province gazettes
            # another reserve. `area_kind` was in the model for this and, like
            # `feature_types` beside it, no resolver had ever read it.
            #
            # The union, not each in turn: the rule is one rule and its sections are one
            # set. An empty family is a failure and not an empty closure — it means the
            # kind is misspelled or the atlas never built those polygons.
            family = _regions.area_family_sections(reg, kind_of_area, g)
            if family is None:
                _fail("area_kind_matches_nothing", kind_of_area)
                return None
            in_area = set(family)
        else:
            key = area_key(aid)
            in_area = area_sections(reg, g, key)
            if in_area is None:
                _fail("area_id_not_in_registry", aid)
                return None
        # FEATURE TYPES ARE APPLIED HERE, and were not applied anywhere at all.
        #
        # `Extent.feature_types` has been in the model, documented and validated, since the
        # model was written — and no resolver ever read it. The first zone rule to use it
        # ("No fishing in any STREAM in Management Units 1-1 to 1-6") bound all 23,391
        # sections inside the area: 16,528 streams, and also 4,151 lakes and 2,712 wetlands
        # that the regulation does not mention. A closure on water a rule never named is
        # the same failure as the Fording River, arrived at from the other direction.
        #
        # An unknown kind is EXCLUDED rather than kept. The alternative is a rule that says
        # "streams only" quietly covering something the atlas could not classify.
        kinds = {str(t).lower() for t in (ex.get("feature_types") or [])}
        if kinds:
            in_area = {s for s in in_area if _kind_of(g, s, reg) in kinds}
            if not in_area:
                _fail("area_has_no_features_of_type",
                      f"{aid or kind_of_area} / {sorted(kinds)}")
                return None
        sec = (universe & in_area) if universe else in_area
        if not sec:
            # The water and the area do not meet. Real curation signal, not a resolver failure: it
            # means the rule paired a water with an area it never enters.
            _fail("area_does_not_meet_this_water", aid or kind_of_area)
            return None
        return _out(sec, widen=bool(universe), inside=in_area)
    # The measure window this extent resolved to, as (blk, lo, hi) — None when there isn't
    # one (a `whole` extent, or a `between` spanning two blue lines). Returned rather than
    # discarded because the tributary walk needs the reach's lower cut to decide which
    # mouths sit ON it, and re-deriving that from node bounds is the same "two places must
    # agree" trap this module exists to avoid.
    window: tuple[str, float, float] | None = None
    ids = list(ex.get("splits") or [])
    if not ids:
        _fail("no_splits_on_extent", op or "")
        return None
    INF = float("inf")
    ambiguous: list[dict] = []
    #: piece -> why it stays unplaced (`_by_measure`), for every piece reported `unclassified`
    why: dict[str, str] = {}

    def _refs(bid: str) -> set[str]:
        """The section-boundary refs a bound boundary ID maps to.

        An extent binds a boundary by its ID, but the graph records the REF, and the two differ by
        kind: a curated cut is `split:{id}` while a lake outlet is `lake:{wbk}`. Assuming "split:"
        meant every reach bounded by a lake resolved to nothing — 46 extents, including most of the
        "river from the lake down to the bridge" regulations.

        The bound id may also be an ALIAS rather than a boundary's own id: a split that landed inside
        a lake run (Duncan Dam) exists only as another name for that lake's boundary, so matching on
        `b.id` alone still found nothing and the reach stayed unresolvable."""
        out = {f"split:{bid}"}
        for i in scope:
            it = reg.get(i)
            for b in (it.boundaries if it else ()):
                if bid in b.answers_to():           # `RegistryBoundary.answers_to`, T125 item 3
                    if b.ref:
                        out.add(b.ref)
                    out.update(b.aliases or ())     # the graph refs this cut-point also carries
        return out

    def _meta(bid: str) -> tuple[str, str]:
        """(boundary kind, label) of the registry boundary a bound id names — its own boundary
        first, else the one it is an alias of (as `_refs` finds them)."""
        alias = None
        for i in scope:
            it = reg.get(i)
            for b in (it.boundaries if it else ()):
                kind = getattr(b.kind, "value", b.kind)
                if b.id == bid:
                    return str(kind or ""), b.label or ""
                if alias is None and bid in b.answers_to()[1:]:
                    alias = (str(kind or ""), b.label or "")
        return alias or ("", "")

    #: THE CUTS THIS EXTENT ACTUALLY USED (T111): one per split id, at the candidate taken (after
    #: `cut_pick` and `AMBIGUOUS_CUT_NEAREST_THE_OTHER_END`), on the line it was taken on, with the
    #: END of the reach it bounds. The tributary seeding (every `lower` cut, `build.seeding_cuts`)
    #: and the confluence test (`build.confluence_excludes`) read these — neither re-derives a cut.
    cuts: list[dict] = []

    def _used(sid: str, at, end: str) -> None:
        kind, label = _meta(sid)
        cuts.append({"split": sid, "blk": at[0], "m": at[1], "end": end, "kind": kind,
                     "label": label,
                     "side": [list(x) for x in _side_crossings(g, _refs(sid), universe, at[0])]})

    def _why_of(pieces) -> dict[str, str]:
        """The typed reason for each reported piece (`unknown` only if no pass said one)."""
        return {p: why.get(p, "unknown") for p in sorted(pieces)}

    #: split id -> the cut as its CURATED pick left it, every candidate kept (`_pin`)
    curated: dict = {}
    picks = dict(ex.get("cut_pick") or {}) if CURATED_CUT_PICK_OUTRANKS else {}

    def _note(sid: str, at, toward: str) -> None:
        """Record an ambiguous cut: every candidate, the one used, and the end it was taken toward
        (`mouth`, `source`, or the split id of the stretch's other end) — or `curated`, with the
        curated `pick`, when a curated record decided it (`CURATED_CUT_PICK_OUTRANKS`)."""
        at = curated.get(sid, at)
        if at[2]:
            row = {"split_id": sid, "used": round(at[1], 2),
                   "also_at": [round(x, 2) for x in at[2]],
                   "candidates": sorted(round(x, 2) for x in (at[1], *at[2])),
                   "toward": toward}
            if sid in curated:
                row.update(toward="curated", reason="curated", pick=picks[sid])
            ambiguous.append(row)

    def _pin(sid: str, at):
        """The cut with its curated pick applied: the general rule then sees ONLY the pinned
        candidate. None = the pick names no candidate (reported, the extent fails)."""
        if at is None or sid not in picks:
            return at
        got = _curated_pick(at, picks[sid])
        if got is None:
            _fail("cut_pick_not_a_candidate",
                  f"{sid}: {picks[sid]} vs {sorted({at[1], *at[2]})}")
            return None
        curated[sid] = got
        return got[0], got[1], []

    if op in ("upstream_of", "downstream_of"):
        at = _cut_at(g, _refs(ids[0]), universe)
        if at is None:
            _fail("cut_not_on_this_water", f"cut(s) not on the scoped water: {ids[:1]}")
            return None
        at = _pin(ids[0], at)
        if at is None:
            return None
        at = _toward_end(at, upper=(op == "upstream_of"))
        _note(ids[0], at, "source" if op == "upstream_of" else "mouth")
        _used(ids[0], at, "lower" if op == "upstream_of" else "upper")
        blk, m = at[0], at[1]
        lo, hi = (m, INF) if op == "upstream_of" else (0.0, m)
        window = (blk, lo, hi)
        sec, braided = _half(g, universe, blk, m, upper=(op == "upstream_of"), why=why)
    elif op == "between" and len(ids) == 2:
        a = _cut_at(g, _refs(ids[0]), universe)
        b = _cut_at(g, _refs(ids[1]), universe)
        if a is None or b is None:
            _fail("cut_not_on_this_water", "cut(s) not on the scoped water: "
                  f"{[i for i, x in zip(ids, (a, b)) if x is None]}")
            return None
        a, b = _pin(ids[0], a), _pin(ids[1], b)
        if a is None or b is None:
            return None
        if a[0] == b[0]:
            a, b = _nearest_pair(a, b)
            _note(ids[0], a, ids[1])
            _note(ids[1], b, ids[0])
            if a[1] == b[1]:
                # Both ends landed on the SAME measure, so the range is empty and the rule would bind
                # nothing while looking resolved. This is the alias case: a lake is one boundary
                # occupying TWO positions on a blue line (where the river enters and where it leaves),
                # and an alias records that two ids name that boundary without recording which END each
                # meant. Mitchell River's "between Mitchell Lake and 100 m upstream" collapses exactly
                # here. Report it rather than return an empty reach.
                _fail("between_cuts_collapsed", f"{ids[0]} and {ids[1]} both at {a[1]:.1f}"
                      + (f"; alternatives at {a[2]}" if a[2] else ""))
                return None
            window = (a[0], min(a[1], b[1]), max(a[1], b[1]))
            _used(ids[0], a, "lower" if a[1] < b[1] else "upper")
            _used(ids[1], b, "lower" if b[1] < a[1] else "upper")
            sec, braided = _by_measure(g, universe, *window, why=why)
        else:
            chosen: dict = {}
            got = _between_across_lines(g, universe, a, b, why=why, chosen=chosen)
            if got is None:
                _note(ids[0], a, ids[1])
                _note(ids[1], b, ids[0])
                _fail("between_cuts_not_ordered", f"{ids[0]} / {ids[1]}")
                return None
            a, b = chosen["a"], chosen["b"]
            _note(ids[0], a, ids[1])
            _note(ids[1], b, ids[0])
            _used(ids[0], a, "upper" if chosen["b_below_a"] else "lower")
            _used(ids[1], b, "lower" if chosen["b_below_a"] else "upper")
            sec, braided = got
    else:
        _fail("unsupported_op", str(op))
        return None
    if ex.get("watershed"):
        # A PART OF THE RIVER'S WATERSHED, cut by FWA code (`Extent.watershed`). The river's own
        # pieces keep the measure cut just made; everything else in its basin is placed by the
        # position its code joins the river at. No `seed` and no `window`: a watershed is not
        # walked (`classify` keeps it out of the tributary walk).
        # (the positions as `_watershed_part` takes them — never rebinds `cuts`, the dicts above,
        # which this part reports like any other extent: T111 review)
        places = [(at[0], at[1])] if op in ("upstream_of", "downstream_of") else \
            [(a[0], a[1]), (b[0], b[1])]
        part = _watershed_part(g, universe, set(sec), set(braided), places, op, reg)
        if isinstance(part, str):
            _fail(part, ",".join(scope))
            return None
        d = _out(part["sections"], ambiguous_cut=ambiguous, window=None, cuts=cuts,
                 unclassified=sorted(braided), unclassified_why=_why_of(braided))
        d.pop("seed", None)
        d.update(watershed=True, river=sorted(universe),
                 unplaced=[s for s in part["unplaced"] if s not in braided],
                 at_cut=part["at_cut"], river_code=part["river_code"])
        return d
    return _out(sec, widen=True, unclassified=sorted(braided), ambiguous_cut=ambiguous,
                window=window,
                cuts=cuts, unclassified_why=_why_of(braided))
