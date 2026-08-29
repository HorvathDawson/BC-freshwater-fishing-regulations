"""Resolve ONE authored extent onto the section ids it covers.

The primitive the rest of `pipeline.reach` is built on: `extent` answers "what does this one
extent select", `tributaries` extends a reach upstream, `classify` turns that into an
outcome, and `build` runs the whole corpus. It lives here rather than under `registry`
because it is reach logic, not registry storage — the registry only supplies the
item -> sections map it reads.

Lifted out of `curation-review/backend/reuse.py` so the review app and the artifact builder run ONE
implementation. If the builder had its own, the bundle and the app would disagree — and the app is
where a human signed off, so the divergence would be invisible until a user hit a wrong reach.

Two things to preserve:

**The resolver never creates a section.** `_by_measure` only filters pre-existing graph nodes by route
measure; cutting happens upstream in the sectionizer from `pipeline/splits.json`. If resolution could
split, editing a regulation would silently change section geometry.

**Reaches are found by ROUTE MEASURE on the cut's own blue line, never by a flow walk.** A `between`
whose two cuts sit on different blue lines is the intersection of two half-lines instead
(`_between_across_lines`) — that is how a reach spanning a name change resolves.

The graph and registry are passed in rather than read from module state, so a caller can resolve
against any build. `curation-review/backend/reuse.py` supplies its cached pair; the builder supplies
its own.
"""

from __future__ import annotations

from pipeline.utils.wsc import trim_wsc   # noqa: F401  (used by the moved body)


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


def _by_measure(g, universe: set[str], blk: str, lo: float, hi: float) -> tuple[set[str], set[str]]:
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
    for nid in universe:
        n = g.nodes.get(nid)
        if n is None:
            continue
        if n.blk != blk:
            others.add(nid)
        elif n.down_m >= lo - 0.001 and n.up_m <= hi + 0.001:
            on_blk.add(nid)

    inside, outside = set(on_blk), {n for n in universe if g.nodes.get(n) and g.nodes[n].blk == blk} - on_blk
    pending = set(others)
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
        else:
            straddling |= comp                             # touches both sides (or nothing): unplaceable
    return inside, straddling


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
    INF = float("inf")
    (blk_a, m_a, _), (blk_b, m_b, _) = a, b
    down_a, s_a = _by_measure(g, universe, blk_a, 0.0, m_a)     # everything below cut A
    down_b, s_b = _by_measure(g, universe, blk_b, 0.0, m_b)     # everything below cut B
    up_a, _ = _by_measure(g, universe, blk_a, m_a, INF)
    up_b, _ = _by_measure(g, universe, blk_b, m_b, INF)

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
    if not universe and op != "within":
        # `within` is the one op that can answer without an item scope — an admin-area closure names
        # the area, not a water. Every other op selects FROM a water, so no water means no answer.
        _fail("no_sections_for_items", ",".join(scope))
        return None
    if op == "whole":
        return {"sections": sorted(universe), "unclassified": [], "ambiguous_cut": [], "window": None,
                "waters": _waters(g, universe)}
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
        if not aid:
            _fail("within_without_area_id")
            return None
        key = aid if aid.startswith("area:") else f"area:{aid}"
        if key not in reg:
            _fail("area_id_not_in_registry", aid)
            return None
        area_sections = set(reg[key].section_ids)
        sec = (universe & area_sections) if universe else area_sections
        if not sec:
            # The water and the area do not meet. Real curation signal, not a resolver failure: it
            # means the rule paired a water with an area it never enters.
            _fail("area_does_not_meet_this_water", aid)
            return None
        return {"sections": sorted(sec), "unclassified": [], "ambiguous_cut": [],
                "waters": _waters(g, sec), "window": None}
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
        sec, braided = _by_measure(g, universe, blk, lo, hi)
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
    return {"sections": sorted(sec), "unclassified": sorted(braided), "ambiguous_cut": ambiguous,
            "waters": _waters(g, sec), "window": window}
