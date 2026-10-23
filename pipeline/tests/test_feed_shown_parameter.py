"""T106: the publisher is the one authority on which quantity a station's reading shows.

The app reads `now.parameter` through core `shownParameter` and never infers it, so a declaration with no value
in it would leave the sheet with nothing to show. Mutation pins: dropping the last line's level branch (back to
`else "discharge"`) fails `test_no_envelope_declares_what_it_reported` on the level-only row; swapping the first two
checks fails `test_discharge_first_where_both_rank` on 08ME030-with-both-envelopes.
"""
from pipeline.gauges.feed.publish import shown_parameter

FLOW = {"discharge": {}}
STAGE = {"level": {}}
BOTH = {"discharge": {}, "level": {}}


def test_discharge_first_where_both_rank():
    assert [shown_parameter(BOTH, 5.46, 4.162), shown_parameter(FLOW, 5.46, 4.162)] == ["discharge", "discharge"]


def test_level_where_only_level_ranks():
    # 08ME030 as published 2026-10-22: both quantities, a level envelope only
    assert [shown_parameter(STAGE, 5.46, 4.162), shown_parameter(BOTH, None, 4.162)] == ["level", "level"]


def test_no_envelope_declares_what_it_reported():
    assert [shown_parameter({}, None, 1.4), shown_parameter({}, 12.0, 1.4), shown_parameter({}, 12.0, None),
            shown_parameter({}, None, None), shown_parameter(FLOW, None, 1.4)] == \
        ["level", "discharge", "discharge", "discharge", "level"]
