"""T1053 part 1 (PROPOSAL, the owner's wording pick is open): a rule that reaches its water's tributaries says so on its
`applies` line, whole string — its own `includes_tributaries`, else its row's (AGENTS 9: three-valued), or
`tributaries_only`. Eagle Creek's sheet said only "This water's own row …: Eagle Creek." (report 3eaf5ac25fc39d73)."""

from __future__ import annotations

import pytest

from pipeline.deliver.answers.display.decided import BOOK_TITLE, applies_here, covers_tributaries

EAGLE = {"kind": "water", "name": "Eagle Creek", "symbols": ["Incl. Tribs"], "includes_tributaries": True,
         "pages": [23]}


def _rule(**fields):
    return {"id": "r2:eagle_creek@2-8::eagle_creek.r1", "entry_id": "r2:eagle_creek@2-8",
            "provenance": {"rank": 1}, "fields": {"extents": [{"op": "whole"}], **fields}}


def _doc(entry):
    return {"entries": {"r2:eagle_creek@2-8": entry}, "rules": {}, "waters": {}}


def test_eagle_creeks_own_sheet_says_it_covers_its_tributary_streams():
    say, trib = applies_here(_rule(), _doc(EAGLE))
    assert say == (f"This water’s own row in the {BOOK_TITLE}: Eagle Creek. "
                   "Covers Eagle Creek and its tributary streams (✱, p.4).")
    assert trib == "Inherited from Eagle Creek: its row covers its tributaries."


def test_the_rules_own_false_beats_the_rows_true():
    """AGENTS 9: a rule's own flag wins over its row's; false here means its water alone."""
    say, _ = applies_here(_rule(includes_tributaries=False), _doc(EAGLE))
    assert say == f"This water’s own row in the {BOOK_TITLE}: Eagle Creek."


def test_a_rules_own_true_on_a_row_that_says_nothing():
    row = {**EAGLE, "symbols": [], "includes_tributaries": None}
    assert covers_tributaries(_rule(includes_tributaries=True), row, "Eagle Creek") == \
        "Covers Eagle Creek and its tributary streams."      # no ✱ printed on the row: no key cited


def test_a_row_without_the_flag_says_nothing():
    row = {k: v for k, v in EAGLE.items() if k not in ("includes_tributaries", "symbols")}
    assert covers_tributaries(_rule(), row, "Eagle Creek") is None


def test_a_stretch_says_the_streams_joining_that_part():
    x = _rule(extents=[{"op": "downstream_of", "splits": ["x"]}])
    assert covers_tributaries(x, EAGLE, "Eagle Creek") == \
        "Covers the part of Eagle Creek it names and the tributary streams joining that part (✱, p.4)."


@pytest.mark.parametrize("extents, words", [
    ([{"op": "whole"}], "Covers only the tributary streams, not Eagle Creek itself (✱, p.4)."),
    ([{"op": "upstream_of", "splits": ["x"]}],
     "Covers only the tributary streams of the part it names, not Eagle Creek itself (✱, p.4)."),
])
def test_tributaries_only_says_not_the_water_itself(extents, words):
    assert covers_tributaries(_rule(tributaries_only=True, extents=extents), {**EAGLE, "includes_tributaries": None},
                              "Eagle Creek") == words


def test_an_undrawn_part_never_walks_and_says_nothing():
    """RULINGS T10: a rule held as a note on an undrawn part does not walk its row's tributaries."""
    assert covers_tributaries(_rule(undrawn_part="500 m below the dam"), EAGLE, "Eagle Creek") is None
