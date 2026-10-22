"""THE CONTINUATION LINK (owner 2026-10-21: "it should be able to navigate up if one stream comes directly from another,
like vedder from chilliwack"): `water_continue` in the release's water file — water A CONTINUES AS water B when A's
mouth meets B's top on the same watercourse. Tributaries (the Vedder's sloughs, a creek joining mid-river) never do.

No data: the rule on synthetic topology. The release-level check against the real Chilliwack / Vedder is in
`test_release.py` (`needs_bundle`), on the release that test builds.
"""
from __future__ import annotations

from pipeline.deliver.release import continuation as C


def _line(**ids):
    """{sid: (blue line, measure)} from `sid=(blk, m)` keywords: `s1=("X", 0)`."""
    return {int(k[1:]): v for k, v in ids.items()}


#: A Chilliwack / Vedder shape: A's main line X starts (measure 0) where it flows into B's main line Y at Y's top
#: (12723), and nothing else flows into that section. A also owns a braid piece (Z, one section) and B a side piece (W).
LINE = _line(s1=("X", 0), s2=("X", 4000), s3=("X", 9000), s4=("Z", 0),
             s10=("Y", 0), s11=("Y", 4738), s12=("Y", 12723), s13=("W", 0),
             s20=("T", 0), s21=("T", 800))
SECS = {1: {1, 2, 3, 4}, 2: {10, 11, 12, 13}, 3: {20, 21}}
DOWN = {1: 12, 2: 1, 3: 2, 4: 3, 12: 11, 11: 10, 13: 10, 20: 11, 21: 20}


def _cont(*, secs=SECS, down=DOWN, line=LINE, streams=(1, 2, 3), item_down=((1, 2), (3, 2))):
    return C.continuations(set(streams), secs, list(item_down), down, line.get)


def test_a_stream_whose_line_begins_at_the_top_of_anothers_continues_as_it():
    assert _cont() == [(1, 1, 2, 12)]


def test_a_tributary_joining_mid_river_does_not():
    # water 3 (line T) flows into Y at 4738, which has B's own section above it: a tributary
    assert (3, 21, 2, 11) not in _cont() and (3, 20, 2, 11) not in _cont()


def test_two_waters_meeting_at_the_top_neither_continues_unless_one_shares_the_line():
    down = {**DOWN, 21: 12}                       # T now also flows into Y's top: a confluence
    assert _cont(down=down) == []
    line = {**LINE, 1: ("Y", 13000)}              # ... unless A's mouth section is on Y itself (the FWA main stem)
    secs = {**SECS, 1: {1, 4}}
    assert _cont(down=down, line=line, secs=secs) == [(1, 1, 2, 12)]


def test_only_the_main_lines_ends_count():
    # A's braid piece (Z) flows into B's top, not A's main line: no continuation through a braid
    assert _cont(down={**DOWN, 1: 2, 4: 12}) == []
    # A's main line flows into B's side piece (W), not B's main line
    assert _cont(down={**DOWN, 1: 13}) == []
    # A's main line does not START there (its lowest section on X is s1 at 0; s2 is above it)
    assert _cont(down={**DOWN, 1: 2, 2: 12}) == []


def test_a_lake_never_continues_or_is_continued():
    assert _cont(streams=(2, 3)) == []
    assert _cont(streams=(1, 3)) == []


def test_only_waters_the_graph_already_says_flow_into_each_other():
    assert _cont(item_down=((3, 2),)) == []


def test_a_section_off_any_blue_line_is_no_end():
    assert C.line_of("380887781:0") == ("380887781", 0)
    assert C.line_of("lake:329707189") is None
    assert C.line_of("-2000000226:0") == ("-2000000226", 0)


def test_a_water_continued_twice_is_refused():
    # two waters whose mouths both sit on B's own line at its top: the data is wrong, and nothing is guessed
    secs = {**SECS, 1: {1}, 4: {30}}
    line = {**LINE, 1: ("Y", 13000), 30: ("Y", 13000)}
    down = {**DOWN, 30: 12}
    try:
        C.continuations({1, 2, 3, 4}, secs, [(1, 2), (4, 2)], down, line.get)
    except SystemExit as e:
        assert "continued by two waters" in str(e)
    else:
        raise AssertionError("a water continued by two waters must be refused")
