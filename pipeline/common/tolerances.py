"""Distance tolerances that more than one module must agree on — declared once (audit F22, plan 2.10).

Each was a literal in two or three files that happened to match; a later edit to one copy would have made two
stages disagree about whether a point is "the same place", "at the cut" or "touching" — silently, since every stage
still runs. The modules that use them import from here (and keep their old module-level names as aliases, so a
reader of `sliver_gate.SLIVER_M` still finds it). VALUES ARE UNCHANGED from the copies they replace.

Distances are metres along a route (measures) or in EPSG:3005, unless the name says otherwise.

ONE PLACE
    SAME_PLACE_M       two computations of ONE point agree to ~1e-9 m; this is float identity, not a positional
                       tolerance (`sectionizer._coincident`, `clean_cut`, `border`).
    SLIVER_M           the shortest piece a cut may leave; two boundaries closer than this are one place
                       (`sliver_gate.SLIVER_M`, `sectionizer.AUTHORED_MERGE_M`, `clean_cut.DIP_M`).

AT THE CUT
    AT_CUT_M           a join within this of a cut stands AT it (`at_cut`; the bundle's cut names, `spans`).
    WATERSHED_AT_CUT_M a tributary mouth this close to a watershed cut is on neither side (`reach.extent`): FWA's
                       millionths rounding (1.4 m a unit on the Fraser).

TOUCHING
    TOUCH_M            two parts of a water closer than this meet (`bundle.topology` part marks and
                       `tools.water_map_export` boundaries draw the same marks).

CONFLUENCES — three different questions, deliberately three values (the plan's "150/200/250"; the 250 m
`spans._MEET_M` was deleted by T108, which made `continuation.goes_on` the one answer):
    CONFLUENCE_NEAR_M  150: a point cut labelled "near the X confluence" IS at it when X's mouth lies this close,
                       on the cut's own blue line (`graph.tributaries`, `reach.build.confluence_excludes`).
    STUB_M             100: B's line may go on this far above A's mouth and A's mouth still be B's top.
    CONFLUENCE_M       200: another inflow this close to A's mouth (or above it) makes it a confluence.
    OWN_M              1000: a piece of B joining B's top piece this close below A's mouth is B going on as itself.
                       (`bundle.continuation.goes_on`)
"""

from __future__ import annotations

SAME_PLACE_M = 1e-6
SLIVER_M = 5.0

AT_CUT_M = 1.0
WATERSHED_AT_CUT_M = 5.0

TOUCH_M = 60.0

CONFLUENCE_NEAR_M = 150.0
STUB_M = 100.0
CONFLUENCE_M = 200.0
OWN_M = 1000.0


def at_cut(m: float, cut_m: float, tol_m: float = AT_CUT_M) -> bool:
    """Is measure `m` AT the cut at `cut_m` — strictly closer than `tol_m`?

    The one predicate for "this join stands at the cut" (a join exactly `tol_m` away is beside it, not at it)."""
    return abs(m - cut_m) < tol_m
