"""THE CONTINUATION LINK (owner 2026-10-21: "it should be able to navigate up if one stream comes directly from another,
like vedder from chilliwack"): `water_continue` in the release's water file — water A CONTINUES AS water B when A's
mouth is B's top on the same watercourse, and nothing else comes in there. Tributaries never do.

No data: the rule on synthetic topology, in the ATLAS GRAPH's terms (every inflow, with the measure on B's line where it
joins). The release-level check against the real Chilliwack / Vedder, the Cowichan, the Somass, the Thompson is in
`test_release.py` (`needs_bundle`).

REVIEW 2026-10-21 (why the graph): the first cut read the bundle's `section_down` for "the only inflow at B's top". That
table holds GAUGE-SHED members only, so it hides most inflows: it shipped 92 rows of which 89 were tributaries or
confluences (Oliver Creek "continued as" the Cowichan, the lake outlet missing; the Sproat as the Somass, the Stamp
missing; the South Thompson as the Thompson, the North Thompson missing) and it missed every continuation outside a
shed.
"""
from __future__ import annotations

from pipeline.deliver.release import continuation as C


def _line(**ids):
    """{sid: (blue line, measure)} from `sid=(blk, m)` keywords: `s1=("X", 0)`."""
    return {int(k[1:]): v for k, v in ids.items()}


#: A Chilliwack / Vedder shape. Water 1 (main line X, a braid piece Z) starts at measure 0 where it flows into water 2's
#: main line Y at its top piece s12 (Y 12723 .. 12950): it joins 25 m below the piece's top (the atlas's snapping), and
#: nothing else joins s12. Water 3 (line T) is a tributary joining s11 mid-piece. Water 2 also owns a side piece W.
LINE = _line(s1=("X", 0), s2=("X", 4000), s3=("X", 9000), s4=("Z", 0),
             s10=("Y", 0), s11=("Y", 4738), s12=("Y", 12723), s13=("W", 0),
             s20=("T", 0), s21=("T", 800))
SECS = {1: {1, 2, 3, 4}, 2: {10, 11, 12, 13}, 3: {20, 21}}
TOP = {12: 12950.0, 11: 12723.0, 10: 4738.0, 13: 300.0}
#: (from, to, at — the measure on `to`'s line where `from` joins)
EDGES = [(1, 12, 12925.0), (2, 1, 0.0), (3, 2, 0.0), (4, 3, 0.0), (12, 11, 12723.0), (11, 10, 4738.0),
         (13, 10, 2000.0), (20, 11, 8000.0), (21, 20, 0.0)]


def _cont(*, secs=SECS, edges=EDGES, line=LINE, top=TOP, streams=(1, 2, 3), item_down=((1, 2), (3, 2))):
    owners = {s: {o for o, ss in secs.items() if s in ss} for ss in secs.values() for s in ss}
    return C.continuations(set(streams), secs, list(item_down), line.get, C.Flow(edges, top, owners))


def test_a_stream_whose_mouth_is_the_top_of_anothers_line_continues_as_it():
    assert _cont() == [(1, 1, 2, 12)]


def test_a_tributary_joining_mid_river_does_not():
    # water 3 joins Y's piece s11 at 8000, far below that piece's top — and s11 is not water 2's top anyway
    assert [r for r in _cont() if r[0] == 3] == []
    # nor when it joins the TOP piece, but well below its top: water 2's own line goes on above it (a stub > 100 m)
    assert _cont(edges=[e for e in EDGES if e[0] != 1] + [(1, 12, 12500.0)]) == []


def test_a_confluence_is_no_continuation_whichever_river_is_bigger():
    # a second river joins the top piece 64 m below the first (the Stamp beside the Sproat, forming the Somass)
    assert _cont(edges=EDGES + [(30, 12, 12861.0)]) == []
    # 145 m apart is still one confluence (the North Thompson beside the South Thompson, forming the Thompson)
    assert _cont(edges=EDGES + [(30, 12, 12780.0)]) == []
    # a lake's outlet at the top, the creek just below it (Cowichan Lake, then Oliver Creek): the lake is the source
    assert _cont(edges=EDGES + [(31, 12, 12950.0)]) == []
    # a creek joining well down the top piece is just a tributary of B, and changes nothing
    assert _cont(edges=EDGES + [(30, 12, 12500.0)]) == [(1, 1, 2, 12)]


def test_a_water_whose_own_name_comes_in_lower_down_goes_on_as_itself():
    # the upper Cottonwood joins the Cottonwood's top piece 448 m below the Swift: the Cottonwood goes on as itself
    secs = {**SECS, 2: SECS[2] | {40}}
    assert _cont(secs=secs, edges=EDGES + [(40, 12, 12477.0)]) == []


def test_the_same_blue_line_is_the_same_watercourse():
    # A's mouth piece lies on B's own line: the FWA main stem, whatever else joins there
    line = {**LINE, 1: ("Y", 12950)}
    assert _cont(line=line, secs={**SECS, 1: {1}}, edges=EDGES + [(30, 12, 12940.0)]) == [(1, 1, 2, 12)]


def test_only_the_main_lines_ends_count():
    # A's braid piece (Z) flows into B's top, not A's main line: no continuation through a braid
    assert _cont(edges=[(1, 2, 0.0) if e[0] == 1 else e for e in EDGES] + [(4, 12, 12925.0)]) == []
    # A's main line flows into B's side piece (W), not B's main line
    assert _cont(edges=[(1, 13, 300.0) if e[0] == 1 else e for e in EDGES]) == []
    # A's main line does not START there (its lowest section on X is s1 at 0; s2 is above it)
    assert _cont(edges=[(1, 2, 0.0) if e[0] == 1 else e for e in EDGES] + [(2, 12, 12925.0)]) == []


def test_a_lake_never_continues_or_is_continued():
    assert _cont(streams=(2, 3)) == []
    assert _cont(streams=(1, 3)) == []


def test_only_waters_the_graph_already_says_flow_into_each_other():
    assert _cont(item_down=((3, 2),)) == []


def test_a_section_off_any_blue_line_is_no_end():
    assert C.line_of("380887781:0") == ("380887781", 0)
    assert C.line_of("lake:329707189") is None
    assert C.line_of("-2000000226:0") == ("-2000000226", 0)


def test_a_water_continued_twice_or_continuing_twice_is_refused():
    def refused(**kw):
        try:
            C.continuations(**kw)
        except SystemExit as e:
            return str(e)
        raise AssertionError("must be refused")
    # two waters whose mouths both sit on B's own line at its top: the data is wrong, and nothing is guessed
    secs = {**SECS, 1: {1}, 4: {30}}
    line = {**LINE, 1: ("Y", 13000), 30: ("Y", 13000)}
    owners = {s: {o for o, ss in secs.items() if s in ss} for ss in secs.values() for s in ss}
    flow = C.Flow(EDGES + [(30, 12, 12950.0)], TOP, owners)
    assert "continued by two waters" in refused(streams={1, 2, 3, 4}, secs=secs, item_down=[(1, 2), (4, 2)],
                                                line=line.get, flow=flow)
    # one mouth that is the top of two waters (a distributary): which one is not ours to guess
    secs = {**SECS, 5: {50}}
    line = {**LINE, 50: ("V", 100)}
    owners = {s: {o for o, ss in secs.items() if s in ss} for ss in secs.values() for s in ss}
    flow = C.Flow(EDGES + [(1, 50, 190.0)], {**TOP, 50: 200.0}, owners)
    assert "continues as two waters" in refused(streams={1, 2, 3, 5}, secs=secs, item_down=[(1, 2), (1, 5)],
                                                line=line.get, flow=flow)
