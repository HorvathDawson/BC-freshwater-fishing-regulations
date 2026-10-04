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
from pipeline.common.utils.wsc import trim_wsc   # noqa: F401  (used by the moved body)

#: Basins answered from the graph, per graph object — a sub-basin is a full scan of the graph. The
#: graph is held beside its answer and checked by identity, so a recycled `id()` never serves
#: another graph's basin.
_BASIN_CACHE: dict[tuple[int, str], tuple[object, frozenset]] = {}


def area_sections(reg, g, key: str) -> set[str] | None:
    """The sections of an `area:` id: the registry's item, or — for a watershed the registry did
    not mint (`area:basin:100-342455-`, the Chilcotin) — every graph node whose FWA code lies in
    that basin (`registry.basins`). None = no such area (the caller fails, naming it)."""
    if key in reg:
        got = set(reg[key].section_ids)
        if key.startswith(_regions.REGION_PREFIX):
            # A WATER TAKES THE ZONE RULES OF THE REGION IT LIES IN: a lake (or a straddling
            # stream piece) touching two region polygons is its home region's only
            # (`registry.regions`, user ruling 2026-09-25 — Mara Lake).
            got = _regions.in_region(g, key[len(_regions.REGION_PREFIX):], got)
        return got
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
WATERSHED_AT_CUT_M = 5.0

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
    if code == river:
        return None
    head = code[len(river) + 1:].split("-", 1)[0]
    return int(head) if head.isdigit() else None


def _basin_members(g, river: str) -> tuple:
    """Every node of `river`'s basin (its code or `basin_wsc` at or below `river`), with the group
    it joins the river at (`_first_group`)."""
    from pipeline.atlas.registry.basins import node_basin_code
    ck = (id(g), river)
    hit = _WS_MEMBERS.get(ck)
    if hit is None or hit[0] is not g:
        out = []
        pre = river + "-"
        for nid in sorted(g.nodes):
            c = node_basin_code(g.nodes[nid])
            if c == river or c.startswith(pre):
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
                    node_basin_code(s).startswith(river + "-"):
                ratios.append(e.at_measure * 1e6 / (grp + 0.5))
    if len(ratios) >= 5:
        ratios.sort()
        length = ratios[len(ratios) // 2]
    else:
        length = max(ups) if ups else 0.0
    _LINE_LEN[ck] = (g, length)
    return length


def _watershed_part(g, universe: set[str], river_in: set[str], river_amb: set[str],
                    cuts: list[tuple[str, float]], op: str) -> dict | str:
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
    codes = {node_basin_code(g.nodes[s]) for s in universe
             if s in g.nodes and str(getattr(g.nodes[s].kind, "value", g.nodes[s].kind)) == "stream"}
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
                 and str(getattr(g.nodes[n].kind, "value", g.nodes[n].kind)) != "stream"]
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
    reach on two page loads. Now the choice is fixed — nearest the mouth — and the alternatives are
    handed back so `resolve_extent` can flag the extent instead of quietly picking for the curator."""
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
    if not at:
        return None
    blk = max(at, key=lambda b: (by_blk.get(b, 0.0), b))
    ms = sorted(at[blk])
    return blk, ms[0], ms[1:]
    return None


def _by_measure(g, universe: set[str], blk: str, lo: float, hi: float,
                seed_outside: frozenset = frozenset()) -> tuple[set[str], set[str]]:
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

    def _nbrs(nid: str) -> set[str]:
        out = {g.edges[i].to_node for i in g.down_adj.get(nid, [])}
        out |= {g.edges[i].from_node for i in g.up_adj.get(nid, [])}
        return out & universe

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
    while True:
        settled = set()
        for nid in pending:
            nbrs = _nbrs(nid)
            if not nbrs:
                continue                                   # detached: cannot be placed
            if nbrs <= inside:
                inside.add(nid); settled.add(nid)
            elif nbrs <= outside:
                outside.add(nid); settled.add(nid)
        if not settled:
            break                                          # fixpoint; judge what is left per component
        pending -= settled

    straddling: set[str] = set()
    detached: list[set[str]] = []
    seen: set[str] = set()
    for start in pending:
        if start in seen:
            continue
        comp, stack = set(), [start]                        # connected component within `pending`
        seen.add(start)
        while stack:
            nid = stack.pop()
            comp.add(nid)
            for x in _nbrs(nid) & pending:
                if x not in seen:
                    seen.add(x)
                    stack.append(x)
        ext = set().union(*(_nbrs(n) for n in comp)) - comp
        if ext and ext <= inside:
            inside |= comp
        elif ext and ext <= outside:
            outside |= comp
        elif not ext:
            detached.append(comp)                          # touches nothing in the water: below
        else:
            straddling |= comp                             # touches both sides: unplaceable

    # DETACHED PIECES, PLACED BY THEIR OWN BLUE LINE. A side channel threading a chain of little
    # unnamed lakes (the Peace below Site C: blk 359004080 runs piece, lake, piece, lake…) touches
    # no section of the water at all — its neighbours are the lakes, which belong to no named item
    # — so it was reported as straddling and every row on the river dropped it. It is still ON a
    # blue line whose other pieces ARE placed, and route measure on that line says where it is:
    # the nearest placed piece of the same line below it and above it. Both inside → inside; both
    # outside → outside; only one side placed, or the two disagree → it stays unplaceable. Judged
    # against the placements made above and never against each other, so the order is irrelevant.
    verdicts = [(comp, _bracket(g, comp, inside, outside, universe)) for comp in detached]
    for comp, side in verdicts:
        if side == "in":
            inside |= comp
        elif side != "out":
            straddling |= comp
    return inside, straddling | poly_straddle


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


def _half(g, universe: set[str], blk: str, m: float, upper: bool):
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
        return _by_measure(g, universe, blk, lo, hi)
    seed = _across_the_junction(g, universe, blk, m, at_top)
    sec, straddling = _by_measure(g, universe, blk, lo, hi, seed_outside=seed)
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


def _between_across_lines(g, universe: set[str], a, b):
    """`between` when the two cuts sit on DIFFERENT blue lines, or None if they are not on one flow.

    A reach can span a name change: "downstream of Tamihi Rapids Bridge to Vedder Crossing Bridge" runs
    down the Chilliwack and onto the Vedder, which is a separate blue line, so there is no single route
    measure to compare the two cuts on. But each cut still cleanly halves the water, so the reach is
    just the INTERSECTION of the two halves:

        between(A, B) = downstream-of-the-upstream-cut  AND  upstream-of-the-downstream-cut

    Which cut is upstream is read off the halves themselves rather than by walking the flow graph
    (unreliable across braids): if B's own piece falls inside A's downstream half, then B is below A.
    Both cuts keep their braid handling, and a piece that straddles EITHER cut stays unplaceable."""
    (blk_a, m_a, _), (blk_b, m_b, _) = a, b
    down_a, s_a = _half(g, universe, blk_a, m_a, upper=False)   # everything below cut A
    down_b, s_b = _half(g, universe, blk_b, m_b, upper=False)   # everything below cut B
    up_a, _ = _half(g, universe, blk_a, m_a, upper=True)
    up_b, _ = _half(g, universe, blk_b, m_b, upper=True)

    b_below_a = any(n in down_a for n in universe if n.startswith(f"{blk_b}:"))
    a_below_b = any(n in down_b for n in universe if n.startswith(f"{blk_a}:"))
    if b_below_a == a_below_b:
        return None                     # neither above the other (parallel branches): no reach between
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
        _fail("no_sections_for_items", ",".join(scope))
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
        key = limit_id if limit_id.startswith("area:") else f"area:{limit_id}"
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
        key = drop_id if drop_id.startswith("area:") else f"area:{drop_id}"
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
        prefix = f"area:{drop_kind}:"
        members = [i for k, i in reg.items() if k.startswith(prefix)]
        if not members:
            _fail("outside_area_kind_matches_nothing", drop_kind)
            return None
        drop_sections |= {s for i in members for s in i.section_ids}
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

    def _out(sec: set[str], **extra) -> dict:
        d = {"sections": sorted(_limited(sec)), "unclassified": [], "ambiguous_cut": [],
             "window": None, "waters": _waters(g, _limited(sec))}
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
        return _out(universe)
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
            prefix = f"area:{kind_of_area}:"
            members = [i for k, i in reg.items() if k.startswith(prefix)]
            if not members:
                _fail("area_kind_matches_nothing", kind_of_area)
                return None
            in_area = {s for i in members for s in i.section_ids}
        else:
            key = aid if aid.startswith("area:") else f"area:{aid}"
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
        return _out(sec)
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

    def _refs(bid: str) -> set[str]:
        """The section-boundary refs a bound boundary ID maps to.

        An extent binds a boundary by its ID, but the graph records the REF, and the two differ by
        kind: a curated cut is `split:{id}` while a lake outlet is `lake:{wbk}`. Assuming "split:"
        meant every reach bounded by a lake resolved to nothing — 46 extents, including most of the
        "river from the lake down to the bridge" regulations.

        The bound id may also be an ALIAS rather than a boundary's own id: a split that landed inside
        a lake run (Duncan Dam) exists only as another name for that lake's boundary, so matching on
        `b.id` alone still found nothing and the reach stayed unresolvable."""
        want = {bid, f"split:{bid}"}
        out = {f"split:{bid}"}
        for i in scope:
            it = reg.get(i)
            for b in (it.boundaries if it else ()):
                aliases = set(b.aliases or ())
                if b.id == bid or (aliases & want):
                    if b.ref:
                        out.add(b.ref)
                    out.update(aliases)             # ids this same cut-point also answers to
        return out

    def _note(sid: str, at) -> None:
        if at[2]:
            ambiguous.append({"split_id": sid, "used": round(at[1], 2),
                              "also_at": [round(x, 2) for x in at[2]]})

    if op in ("upstream_of", "downstream_of"):
        at = _cut_at(g, _refs(ids[0]), universe)
        if at is None:
            _fail("cut_not_on_this_water", ids[0])
            return None
        _note(ids[0], at)
        blk, m = at[0], at[1]
        lo, hi = (m, INF) if op == "upstream_of" else (0.0, m)
        window = (blk, lo, hi)
        sec, braided = _half(g, universe, blk, m, upper=(op == "upstream_of"))
    elif op == "between" and len(ids) == 2:
        a = _cut_at(g, _refs(ids[0]), universe)
        b = _cut_at(g, _refs(ids[1]), universe)
        if a is None or b is None:
            _fail("cut_not_on_this_water", ids[0] if a is None else ids[1])
            return None
        _note(ids[0], a)
        _note(ids[1], b)
        if a[0] == b[0]:
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
            sec, braided = _by_measure(g, universe, *window)
        else:
            got = _between_across_lines(g, universe, a, b)
            if got is None:
                _fail("between_cuts_not_ordered", f"{ids[0]} / {ids[1]}")
                return None
            sec, braided = got
    else:
        _fail("unsupported_op", str(op))
        return None
    if ex.get("watershed"):
        # A PART OF THE RIVER'S WATERSHED, cut by FWA code (`Extent.watershed`). The river's own
        # pieces keep the measure cut just made; everything else in its basin is placed by the
        # position its code joins the river at. No `seed` and no `window`: a watershed is not
        # walked (`classify` keeps it out of the tributary walk).
        cuts = [(at[0], at[1])] if op in ("upstream_of", "downstream_of") else \
            [(a[0], a[1]), (b[0], b[1])]
        part = _watershed_part(g, universe, set(sec), set(braided), cuts, op)
        if isinstance(part, str):
            _fail(part, ",".join(scope))
            return None
        d = _out(part["sections"], ambiguous_cut=ambiguous, window=None,
                 unclassified=sorted(braided))
        d.pop("seed", None)
        d.update(watershed=True, river=sorted(universe),
                 unplaced=[s for s in part["unplaced"] if s not in braided],
                 at_cut=part["at_cut"], river_code=part["river_code"])
        return d
    return _out(sec, unclassified=sorted(braided), ambiguous_cut=ambiguous, window=window)
