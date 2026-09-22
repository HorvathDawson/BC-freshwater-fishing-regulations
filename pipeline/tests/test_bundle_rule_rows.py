"""`pipeline.deliver.bundle.rules` — the row it writes, and the summary it prints about it.

The summary matters more than a printed line usually would: it is what a person reads to decide
whether a province-wide rebuild is healthy, and there is no second place to check.
"""

import json

from pipeline.deliver.bundle.rules import _rule_row


RULE = {
    "rule_id": "x.r1",
    "type": "retention_limit",
    "species": ["BT"],
    "take": 2,
    "verbatim": "Trout daily quota 2",
}


def _fields():
    """The row, by position — the thing that drifts."""
    return _rule_row("r1:x@1-1", dict(RULE), uncertain=True)


def test_the_uncertain_flag_is_the_only_field_that_says_uncertain():
    """**The bug this file exists for.** The summary counted `r[8]`, which is
    `json.dumps(species)` — a string that is never empty ("[]" at minimum) and so always
    truthy. Every build reported every rule as uncertain: 3,422 of 3,422. The column itself
    was always right, so nothing downstream broke and nothing failed; only the number a person
    reads to judge a build was meaningless, and it was wrong in the safe-looking direction.

    Pinned by VALUE rather than by index, so moving a field cannot quietly restore it.
    """
    on = _rule_row("r1:x@1-1", dict(RULE), uncertain=True)
    off = _rule_row("r1:x@1-1", dict(RULE), uncertain=False)
    differ = [i for i, (a, b) in enumerate(zip(on, off)) if a != b]
    assert len(differ) == 1, f"uncertain changed {len(differ)} fields: {differ}"
    assert on[differ[0]] == 1 and off[differ[0]] == 0


def test_an_empty_json_collection_is_truthy_and_so_can_never_stand_in_for_a_flag():
    """Why the old index was silently wrong rather than loudly wrong. Several fields are JSON
    strings, and a JSON string for an EMPTY collection is "[]" — two characters, and truthy.
    A `if r[i]` test on any of them is true for every rule in the corpus.

    This rule names no `species_except` and no windows, so those fields are as empty as the
    row ever gets; they are still truthy."""
    row = _rule_row("r1:x@1-1", dict(RULE), uncertain=False)
    blanks = [f for f in row if isinstance(f, str) and f in ("[]", "{}")]
    assert blanks, "expected an empty-collection JSON string in the row"
    assert all(bool(b) for b in blanks), "'[]' is truthy — that is the trap"


def test_the_row_is_the_length_the_insert_expects():
    """The INSERT names its columns because a positional list once wrote a 42 MB bundle with
    zero entries in it. This holds the row to the same count from the other side."""
    assert len(_fields()) == 16


def test_species_survive_as_json_not_python_repr():
    row = _rule_row("r1:x@1-1", dict(RULE), uncertain=False)
    assert json.loads(row[8]) == ["BT"]
